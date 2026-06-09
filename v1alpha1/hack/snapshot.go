// Copyright 2018 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package hack is a vendored, surgically modified fork of
// go.etcd.io/etcd/etcdutl/v3/snapshot. Upstream Restore bootstraps a *brand new*
// cluster: it recomputes member IDs deterministically and writes a fresh raft
// log starting at index 1. That can't be used to seed a node joining a *live*
// cluster — the leader already assigned the new member a timestamped ID, and the
// restored node's low-index log would conflict with the leader's (compacted)
// history, forcing the leader to send a raft snapshot.
//
// On Windows, receiving that snapshot panics the embedded host: applySnapshot ->
// OpenSnapshotBackend renames the snapshot db over the still-open bbolt backend,
// which Windows refuses ("Access is denied"). See the join flow in executor.go.
//
// This fork changes Restore so the seeded data directory impersonates a node
// that has *already applied* the leader's snapshot at the leader's live raft
// index: it keeps the leader-assigned member IDs verbatim (RestoreConfig.Members
// + SelfID), pins the leader's cluster ID, preserves learner status in the conf
// state, and anchors the raft snapshot at the copied db's own consistent
// index/term. The joiner then boots already caught up to that index, so the
// leader replicates forward over the log and never sends a snapshot.
package hack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"go.uber.org/zap"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/snapshot"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/etcd/server/v3/verify"
	"go.etcd.io/raft/v3/raftpb"
)

// Manager defines snapshot methods.
type Manager interface {
	// Save fetches snapshot from remote etcd server, saves data
	// to target path and returns server version. If the context "ctx" is canceled or timed out,
	// snapshot save stream will error out (e.g. context.Canceled,
	// context.DeadlineExceeded). Make sure to specify only one endpoint
	// in client configuration. Snapshot API must be requested to a
	// selected node, and saved snapshot is the point-in-time state of
	// the selected node.
	Save(ctx context.Context, cfg clientv3.Config, dbPath string) (version string, err error)

	// Status returns the snapshot file information.
	Status(dbPath string) (Status, error)

	// Restore restores a new etcd data directory from given snapshot
	// file. It returns an error if specified data directory already
	// exists, to prevent unintended data directory overwrites.
	Restore(cfg RestoreConfig) error
}

// NewV3 returns a new snapshot Manager for v3.x snapshot.
func NewV3(lg *zap.Logger) Manager {
	return &v3Manager{lg: lg}
}

type v3Manager struct {
	lg *zap.Logger

	name      string
	selfID    uint64
	srcDbPath string
	walDir    string
	snapDir   string
	cl        *membership.RaftCluster

	skipHashCheck   bool
	initialMmapSize uint64
}

// hasChecksum returns "true" if the file size "n"
// has appended sha256 hash digest.
func hasChecksum(n int64) bool {
	// 512 is chosen because it's a minimum disk sector size
	// smaller than (and multiplies to) OS page size in most systems
	return (n % 512) == sha256.Size
}

// Save fetches snapshot from remote etcd server and saves data to target path.
func (s *v3Manager) Save(ctx context.Context, cfg clientv3.Config, dbPath string) (version string, err error) {
	return snapshot.SaveWithVersion(ctx, s.lg, cfg, dbPath)
}

// Status is the snapshot file status.
type Status struct {
	Hash      uint32 `json:"hash"`
	Revision  int64  `json:"revision"`
	TotalKey  int    `json:"totalKey"`
	TotalSize int64  `json:"totalSize"`
	// Version is equal to storageVersion of the snapshot
	// Empty if server does not supports versioned snapshots (<v3.6)
	Version string `json:"version"`
}

// Status returns the snapshot file information.
func (s *v3Manager) Status(dbPath string) (ds Status, err error) {
	if _, err = os.Stat(dbPath); err != nil {
		return ds, err
	}

	db, err := bolt.Open(dbPath, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return ds, err
	}
	defer db.Close()

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	seenKeys := make(map[string]struct{})

	if err = db.View(func(tx *bolt.Tx) error {
		// check snapshot file integrity first
		var dbErrStrings []string
		for dbErr := range tx.Check() {
			dbErrStrings = append(dbErrStrings, dbErr.Error())
		}
		if len(dbErrStrings) > 0 {
			return fmt.Errorf("snapshot file integrity check failed. %d errors found.\n"+strings.Join(dbErrStrings, "\n"), len(dbErrStrings))
		}
		ds.TotalSize = tx.Size()
		v := schema.ReadStorageVersionFromSnapshot(tx)
		if v != nil {
			ds.Version = v.String()
		}
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("nil bucket: %q", string(next))
			}
			_, err = h.Write(next)
			if err != nil {
				return fmt.Errorf("cannot hash bucket name: %q err: %w", string(next), err)
			}

			iskeyb := (bytes.Equal(next, schema.Key.Name()))
			if err = b.ForEach(func(k, v []byte) error {
				_, err = h.Write(k)
				if err != nil {
					return fmt.Errorf("cannot hash bucket key: %q err: %w", k, err)
				}
				_, err = h.Write(v)
				if err != nil {
					return fmt.Errorf("cannot hash bucket key: %q value: %q err: %w", k, v, err)
				}
				if iskeyb {
					var rev mvcc.Revision
					rev, err = bytesToRev(k)
					if err != nil {
						return fmt.Errorf("cannot parse revision key: %q err: %w", k, err)
					}
					ds.Revision = rev.Main

					var kv mvccpb.KeyValue
					err = kv.Unmarshal(v)
					if err != nil {
						return fmt.Errorf("cannot unmarshal value, key: %q value: %q err: %w", k, v, err)
					}
					key := string(kv.Key)
					// refer to https://etcd.io/docs/v3.5/learning/data_model/
					if !mvcc.IsTombstone(k) {
						seenKeys[key] = struct{}{}
					} else {
						delete(seenKeys, key)
					}
				}
				return nil
			}); err != nil {
				return fmt.Errorf("error during bucket key iteration, name: %q err: %w", string(next), err)
			}
		}
		return nil
	}); err != nil {
		return ds, err
	}

	ds.TotalKey = len(seenKeys)
	ds.Hash = h.Sum32()
	return ds, nil
}

func bytesToRev(b []byte) (rev mvcc.Revision, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()
	return mvcc.BytesToRev(b), err
}

// MemberInfo describes a cluster member exactly as the leader knows it: the
// leader-assigned ID (verbatim, never recomputed), the advertised URLs, and
// whether it is a raft learner. The joining node is included (with its real
// name filled in — the leader records it with an empty name until the node
// starts and publishes).
type MemberInfo struct {
	ID         uint64
	Name       string
	PeerURLs   []string
	ClientURLs []string
	IsLearner  bool
}

// RestoreConfig configures a join-seed restore: it produces a data directory
// that boots as an existing member of a live cluster, already caught up to the
// snapshot's raft index, rather than bootstrapping a fresh cluster.
type RestoreConfig struct {
	// SnapshotPath is the path of the snapshot db pulled from the leader.
	SnapshotPath string

	// Name is this member's (the joiner's) human-readable name.
	Name string

	// SelfID is the leader-assigned member ID for the joining node.
	SelfID uint64
	// ClusterID is the live cluster's ID; the seeded node must agree on it.
	ClusterID uint64
	// Members is the full membership as the leader reports it (including self),
	// with leader-assigned IDs preserved verbatim.
	Members []MemberInfo

	// OutputDataDir is the target data directory. Must not already exist non-empty.
	OutputDataDir string
	// OutputWALDir is the target WAL directory. Defaults to OutputDataDir/member/wal.
	OutputWALDir string

	// SkipHashCheck ignores the snapshot integrity hash (set when the snapshot
	// was copied from a data directory rather than produced by the Snapshot API).
	SkipHashCheck bool

	// InitialMmapSize is the database initial memory map size.
	InitialMmapSize uint64
}

// Restore writes a seed data directory from the leader's snapshot db. The seeded
// node impersonates one that has already applied the leader's snapshot at the
// db's own consistent index/term, so on boot the leader catches it up over the
// raft log (never a MsgSnap).
func (s *v3Manager) Restore(cfg RestoreConfig) error {
	if cfg.SelfID == 0 {
		return fmt.Errorf("restore: SelfID required")
	}
	if cfg.ClusterID == 0 {
		return fmt.Errorf("restore: ClusterID required")
	}

	membs := make([]*membership.Member, 0, len(cfg.Members))
	for _, mi := range cfg.Members {
		membs = append(membs, &membership.Member{
			ID:             types.ID(mi.ID),
			RaftAttributes: membership.RaftAttributes{PeerURLs: mi.PeerURLs, IsLearner: mi.IsLearner},
			Attributes:     membership.Attributes{Name: mi.Name, ClientURLs: mi.ClientURLs},
		})
	}
	// Build the cluster with the leader's verbatim IDs and pin our local ID +
	// the live cluster ID (NewClusterFromMembers would otherwise derive its own).
	s.cl = membership.NewClusterFromMembers(s.lg, types.ID(cfg.ClusterID), membs)
	s.cl.SetID(types.ID(cfg.SelfID), types.ID(cfg.ClusterID))

	dataDir := cfg.OutputDataDir
	if dataDir == "" {
		dataDir = cfg.Name + ".etcd"
	}
	if fileutil.Exist(dataDir) && !fileutil.DirEmpty(dataDir) {
		return fmt.Errorf("data-dir %q not empty or could not be read", dataDir)
	}

	walDir := cfg.OutputWALDir
	if walDir == "" {
		walDir = filepath.Join(dataDir, "member", "wal")
	} else if fileutil.Exist(walDir) {
		return fmt.Errorf("wal-dir %q exists", walDir)
	}

	s.name = cfg.Name
	s.selfID = cfg.SelfID
	s.srcDbPath = cfg.SnapshotPath
	s.walDir = walDir
	s.snapDir = filepath.Join(dataDir, "member", "snap")
	s.skipHashCheck = cfg.SkipHashCheck
	s.initialMmapSize = cfg.InitialMmapSize

	s.lg.Info(
		"restoring join seed",
		zap.String("path", s.srcDbPath),
		zap.String("wal-dir", s.walDir),
		zap.String("data-dir", dataDir),
		zap.String("snap-dir", s.snapDir),
		zap.Uint64("self-id", cfg.SelfID),
		zap.Uint64("cluster-id", cfg.ClusterID),
	)

	if err := s.saveDB(); err != nil {
		return err
	}

	// The copied db's consistent index/term is the leader raft index this applied
	// state corresponds to — anchor the seeded raft snapshot there.
	index, term := s.readConsistentIndex()

	hardstate, err := s.saveWALAndSnap(index, term)
	if err != nil {
		return err
	}

	if err := s.updateCIndex(hardstate.Commit, hardstate.Term); err != nil {
		return err
	}

	s.lg.Info(
		"restored join seed",
		zap.String("data-dir", dataDir),
		zap.Uint64("snapshot-index", index),
		zap.Uint64("snapshot-term", term),
	)

	return verify.VerifyIfEnabled(verify.Config{
		ExactIndex: true,
		Logger:     s.lg,
		DataDir:    dataDir,
	})
}

func (s *v3Manager) outDbPath() string {
	return filepath.Join(s.snapDir, "db")
}

// saveDB copies the database snapshot to the snapshot directory
func (s *v3Manager) saveDB() error {
	err := s.copyAndVerifyDB()
	if err != nil {
		return err
	}

	be := backend.NewDefaultBackend(s.lg, s.outDbPath(), backend.WithMmapSize(s.initialMmapSize))
	defer be.Close()

	err = schema.NewMembershipBackend(s.lg, be).TrimMembershipFromBackend()
	if err != nil {
		return err
	}

	return nil
}

func (s *v3Manager) copyAndVerifyDB() error {
	srcf, ferr := os.Open(s.srcDbPath)
	if ferr != nil {
		return ferr
	}
	defer srcf.Close()

	// get snapshot integrity hash
	if _, err := srcf.Seek(-sha256.Size, io.SeekEnd); err != nil {
		return err
	}
	sha := make([]byte, sha256.Size)
	if _, err := srcf.Read(sha); err != nil {
		return err
	}
	if _, err := srcf.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := fileutil.CreateDirAll(s.lg, s.snapDir); err != nil {
		return err
	}

	outDbPath := s.outDbPath()

	db, dberr := os.OpenFile(outDbPath, os.O_RDWR|os.O_CREATE, 0o600)
	if dberr != nil {
		return dberr
	}
	defer db.Close()

	if _, err := io.Copy(db, srcf); err != nil {
		return err
	}

	// truncate away integrity hash, if any.
	off, serr := db.Seek(0, io.SeekEnd)
	if serr != nil {
		return serr
	}
	hasHash := hasChecksum(off)
	if hasHash {
		if err := db.Truncate(off - sha256.Size); err != nil {
			return err
		}
	}

	if !hasHash && !s.skipHashCheck {
		return fmt.Errorf("snapshot missing hash but --skip-hash-check=false")
	}

	if hasHash && !s.skipHashCheck {
		// check for match
		if _, err := db.Seek(0, io.SeekStart); err != nil {
			return err
		}
		h := sha256.New()
		if _, err := io.Copy(h, db); err != nil {
			return err
		}
		dbsha := h.Sum(nil)
		if !reflect.DeepEqual(sha, dbsha) {
			return fmt.Errorf("expected sha256 %v, got %v", sha, dbsha)
		}
	}

	// db hash is OK, can now modify DB so it can be part of a new cluster

	return nil
}

// readConsistentIndex reads the consistent index and term persisted in the
// copied snapshot db. That index is the leader raft index whose applied state
// the db reflects, which is exactly where the seeded node must claim to be.
func (s *v3Manager) readConsistentIndex() (uint64, uint64) {
	be := backend.NewDefaultBackend(s.lg, s.outDbPath(), backend.WithMmapSize(s.initialMmapSize))
	defer be.Close()

	tx := be.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	return schema.UnsafeReadConsistentIndex(tx)
}

// saveWALAndSnap writes a WAL + snapshot describing a member that has already
// applied a snapshot at (snapIndex, snapTerm): the membership is the leader's
// (verbatim IDs, learner status preserved), and there are no log entries after
// the snapshot. On boot the node is a follower at snapIndex and the leader
// replicates forward over the log — never a MsgSnap.
func (s *v3Manager) saveWALAndSnap(snapIndex, snapTerm uint64) (*raftpb.HardState, error) {
	if err := fileutil.CreateDirAll(s.lg, s.walDir); err != nil {
		return nil, err
	}

	// Persist the membership into the seeded backend.
	be := backend.NewDefaultBackend(s.lg, s.outDbPath(), backend.WithMmapSize(s.initialMmapSize))
	defer be.Close()
	s.cl.SetBackend(schema.NewMembershipBackend(s.lg, be))
	for _, m := range s.cl.Members() {
		s.cl.AddMember(m, true)
	}

	md := &etcdserverpb.Metadata{NodeID: s.selfID, ClusterID: uint64(s.cl.ID())}
	metadata, merr := md.Marshal()
	if merr != nil {
		return nil, merr
	}
	w, walerr := wal.Create(s.lg, s.walDir, metadata)
	if walerr != nil {
		return nil, walerr
	}
	defer w.Close()

	// Preserve the leader's voter/learner split in the conf state.
	var voters, learners []uint64
	for _, id := range s.cl.MemberIDs() {
		if s.cl.Member(id).IsLearner {
			learners = append(learners, uint64(id))
		} else {
			voters = append(voters, uint64(id))
		}
	}
	confState := raftpb.ConfState{Voters: voters, Learners: learners}

	// No entries after the snapshot: the node starts already applied to snapIndex.
	hardState := raftpb.HardState{Term: snapTerm, Commit: snapIndex}
	if err := w.Save(hardState, nil); err != nil {
		return nil, err
	}

	raftSnap := raftpb.Snapshot{
		Data: etcdserver.GetMembershipInfoInV2Format(s.lg, s.cl),
		Metadata: raftpb.SnapshotMetadata{
			Index:     snapIndex,
			Term:      snapTerm,
			ConfState: confState,
		},
	}
	sn := snap.New(s.lg, s.snapDir)
	if err := sn.SaveSnap(raftSnap); err != nil {
		return nil, err
	}
	walSnap := walpb.Snapshot{Index: snapIndex, Term: snapTerm, ConfState: &confState}
	return &hardState, w.SaveSnapshot(walSnap)
}

func (s *v3Manager) updateCIndex(commit uint64, term uint64) error {
	be := backend.NewDefaultBackend(s.lg, s.outDbPath(), backend.WithMmapSize(s.initialMmapSize))
	defer be.Close()

	cindex.UpdateConsistentIndexForce(be.BatchTx(), commit, term)
	return nil
}
