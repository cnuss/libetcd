# `snapshot.go` — vendored fork of etcd's `etcdutl` snapshot Restore

`snapshot.go` is a vendored, surgically modified copy of
[`go.etcd.io/etcd/etcdutl/v3/snapshot/v3_snapshot.go`](https://pkg.go.dev/go.etcd.io/etcd/etcdutl/v3@v3.6.12/snapshot)
(pinned at **v3.6.12** — the same version as the rest of the etcd modules in
`go.mod`). This document records exactly what was changed from upstream and why,
so the fork can be re-synced against a future etcd release.

## Why fork at all

`Join` ([`../executor.go`](../executor.go)) seeds a new node's data directory
from a leader snapshot before `Start`, so the node boots already caught up to
the leader's raft index. The leader then replicates forward over the log and
**never sends a raft snapshot** to the joiner.

That matters because applying a received raft snapshot panics the embedded host
on **Windows**: `etcdserver.applySnapshot` → `serverstorage.OpenSnapshotBackend`
does `os.Rename(<idx>.snap.db → member/snap/db)` over the still-open bbolt
backend, and Windows refuses to rename onto an open file (`"Access is denied"`).
The old backend is closed only *after* the rename (`etcdserver/server.go`,
`applySnapshot`). The bug is unchanged through `v3.8.0-alpha.0`; closest tracked
issue [etcd-io/etcd#18055](https://github.com/etcd-io/etcd/issues/18055).

Upstream `Restore` can't be used as-is to seed a node joining a **live** cluster:

- It **recomputes** member IDs deterministically (`NewClusterFromURLsMap` →
  `NewMember(..., now=nil)` = `sha1(peerURLs+token)`). But the live leader
  assigned the new learner a **timestamped** ID
  (`v3rpc/member.go`: `NewMemberAsLearner("", urls, "", &now)` =
  `sha1(peerURLs + "" + now.Unix())`). The two never match, and raft routes by
  member ID, so a restored node can't take the identity the cluster gave it.
- It writes a **fresh-bootstrap raft log starting at index 1**. Those low
  indices conflict with the leader's real history, whose prefix is compacted
  (`firstIndex=2` after the first memory snapshot) — so the leader would send a
  snapshot anyway, re-triggering the Windows panic.

The fork makes `Restore` seed a node that has *already applied* the leader's
snapshot at the leader's live raft index, with the leader's real member IDs.

## Changes from upstream

Line/structure references are against `v3_snapshot.go` @ v3.6.12.

### Mechanical

- `package snapshot` → `package hack`.
- Dropped now-unused imports: `encoding/json`, `go.etcd.io/etcd/server/v3/config`,
  `go.etcd.io/raft/v3` (`raftpb` is still used).
- Log messages `"restoring snapshot"` / `"restored snapshot"` →
  `"restoring join seed"` / `"restored join seed"` (and their fields).
- Deleted the revision-bump / mark-compacted feature, unused for join:
  `modifyLatestRevision`, `unsafeBumpBucketsRevision`,
  `unsafeMarkRevisionCompacted`, `unsafeGetLatestRevision`, and the
  `RevisionBump` / `MarkCompacted` config fields + their call in `Restore`.

`Save`, `Status`, `hasChecksum`, `bytesToRev`, `outDbPath`, `saveDB`,
`copyAndVerifyDB`, and `updateCIndex` are **unchanged** from upstream.

### `v3Manager` struct

- Added field `selfID uint64` (the leader-assigned member ID for this node).

### `RestoreConfig`

| Upstream | Fork |
| --- | --- |
| `PeerURLs []string` | removed |
| `InitialCluster string` | removed |
| `InitialClusterToken string` | removed |
| `RevisionBump uint64` | removed |
| `MarkCompacted bool` | removed |
| — | `SelfID uint64` — leader-assigned member ID for the joiner |
| — | `ClusterID uint64` — the live cluster's ID; the seed must agree on it |
| — | `Members []MemberInfo` — full membership as the leader reports it, IDs verbatim |

New type `MemberInfo { ID uint64; Name string; PeerURLs, ClientURLs []string; IsLearner bool }`.

### `Restore`

- Validates `SelfID != 0` and `ClusterID != 0`.
- **Cluster construction** — the central change:
  - Upstream: `types.NewURLs` + `types.NewURLsMap`, `config.ServerConfig.VerifyBootstrap()`,
    then `membership.NewClusterFromURLsMap(token, ics)` — **recomputes IDs**.
  - Fork: builds `[]*membership.Member` from `cfg.Members` (IDs, peer/client URLs,
    and learner flag **verbatim**), then
    `membership.NewClusterFromMembers(lg, types.ID(cfg.ClusterID), membs)` and
    `s.cl.SetID(self=cfg.SelfID, cluster=cfg.ClusterID)`. No recomputation, no
    `VerifyBootstrap`.
- Stashes `s.selfID = cfg.SelfID`.
- Reads the anchor index/term from the copied db (`readConsistentIndex`, new) and
  passes them to `saveWALAndSnap`.
- Dropped the `MarkCompacted && RevisionBump` block.

### `readConsistentIndex` (new)

Opens the copied db read-only and returns
`schema.UnsafeReadConsistentIndex(tx)` — the consistent `(index, term)` persisted
in the snapshot. That index is the leader raft index the applied db state
reflects, and is exactly where the seeded node must claim to be.

### `saveWALAndSnap` — now `saveWALAndSnap(snapIndex, snapTerm uint64)`

Upstream writes a **fresh cluster bootstrap**; the fork writes a node that has
**already applied a snapshot** at `(snapIndex, snapTerm)`:

| | Upstream | Fork |
| --- | --- | --- |
| Raft entries | one `ConfChangeAddNode` per member, indices `1..N`, term `1` | **none** (`w.Save(hardState, nil)`) |
| `HardState` | `{Term: 1, Vote: peers[0].ID, Commit: N}` | `{Term: snapTerm, Commit: snapIndex}` (no vote) |
| Snapshot metadata | `Index: N, Term: 1` | `Index: snapIndex, Term: snapTerm` |
| WAL `NodeID` | `s.cl.MemberByName(s.name).ID` | `s.selfID` (leader records the learner with an **empty name**, so the name lookup returns nil here) |
| Conf state | `Voters: <all member IDs>` (upstream literally notes `// TODO: This code ignores learners !!!`) | **splits** `Voters` / `Learners` by `Member.IsLearner`, preserving learner status |

Effect: on boot the node is a follower already applied to `snapIndex`, with the
leader's exact membership (learner still a learner). The leader's first append
finds agreement at `snapIndex` and replicates forward over the log — no
`MsgSnap`, so the Windows snapshot-apply path is never reached.

## Re-syncing against a newer etcd

1. Diff this file's `snapshot.go` against the new
   `etcdutl/v3/snapshot/v3_snapshot.go`:
   ```sh
   UTL=$(go env GOMODCACHE)/go.etcd.io/etcd/etcdutl/v3@<version>/snapshot/v3_snapshot.go
   diff <(tail -n +14 "$UTL") <(tail -n +35 snapshot.go) --unified=2
   ```
   (The `tail` offsets skip each file's license/package header so only the bodies
   diff.)
2. Re-apply the changes above onto the new upstream body.
3. Re-check the upstream bug is still present (the `os.Rename` in
   `serverstorage.OpenSnapshotBackend` before the old backend is closed). If etcd
   has fixed it, this whole fork — and the seeding step in `Join` — can be
   removed in favor of the stock learner-add flow.

## The patch (against upstream `v3_snapshot.go`)

The exact transform that turns upstream `etcdutl/v3/snapshot/v3_snapshot.go`
(@ v3.6.12) into this file. Regenerate with the diff in *Re-syncing* above.

Note: the first hunk (`package snapshot` → `package hack` + the package doc
comment) is a **vendoring** artifact, not functionality. To apply this as an
in-place patch to upstream, keep `package snapshot` and skip that doc comment;
every other hunk is the functional change.

```diff
--- a/snapshot/v3_snapshot.go
+++ b/snapshot/v3_snapshot.go
@@ -12,13 +12,31 @@
 // See the License for the specific language governing permissions and
 // limitations under the License.
 
-package snapshot
+// Package hack is a vendored, surgically modified fork of
+// go.etcd.io/etcd/etcdutl/v3/snapshot. Upstream Restore bootstraps a *brand new*
+// cluster: it recomputes member IDs deterministically and writes a fresh raft
+// log starting at index 1. That can't be used to seed a node joining a *live*
+// cluster — the leader already assigned the new member a timestamped ID, and the
+// restored node's low-index log would conflict with the leader's (compacted)
+// history, forcing the leader to send a raft snapshot.
+//
+// On Windows, receiving that snapshot panics the embedded host: applySnapshot ->
+// OpenSnapshotBackend renames the snapshot db over the still-open bbolt backend,
+// which Windows refuses ("Access is denied"). See the join flow in executor.go.
+//
+// This fork changes Restore so the seeded data directory impersonates a node
+// that has *already applied* the leader's snapshot at the leader's live raft
+// index: it keeps the leader-assigned member IDs verbatim (RestoreConfig.Members
+// + SelfID), pins the leader's cluster ID, preserves learner status in the conf
+// state, and anchors the raft snapshot at the copied db's own consistent
+// index/term. The joiner then boots already caught up to that index, so the
+// leader replicates forward over the log and never sends a snapshot.
+package hack
 
 import (
 	"bytes"
 	"context"
 	"crypto/sha256"
-	"encoding/json"
 	"fmt"
 	"hash/crc32"
 	"io"
@@ -36,7 +54,6 @@
 	"go.etcd.io/etcd/client/pkg/v3/types"
 	clientv3 "go.etcd.io/etcd/client/v3"
 	"go.etcd.io/etcd/client/v3/snapshot"
-	"go.etcd.io/etcd/server/v3/config"
 	"go.etcd.io/etcd/server/v3/etcdserver"
 	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
 	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
@@ -47,7 +64,6 @@
 	"go.etcd.io/etcd/server/v3/storage/wal"
 	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
 	"go.etcd.io/etcd/server/v3/verify"
-	"go.etcd.io/raft/v3"
 	"go.etcd.io/raft/v3/raftpb"
 )
 
@@ -80,6 +96,7 @@
 	lg *zap.Logger
 
 	name      string
+	selfID    uint64
 	srcDbPath string
 	walDir    string
 	snapDir   string
@@ -208,78 +225,75 @@
 	return mvcc.BytesToRev(b), err
 }
 
-// RestoreConfig configures snapshot restore operation.
+// MemberInfo describes a cluster member exactly as the leader knows it: the
+// leader-assigned ID (verbatim, never recomputed), the advertised URLs, and
+// whether it is a raft learner. The joining node is included (with its real
+// name filled in — the leader records it with an empty name until the node
+// starts and publishes).
+type MemberInfo struct {
+	ID         uint64
+	Name       string
+	PeerURLs   []string
+	ClientURLs []string
+	IsLearner  bool
+}
+
+// RestoreConfig configures a join-seed restore: it produces a data directory
+// that boots as an existing member of a live cluster, already caught up to the
+// snapshot's raft index, rather than bootstrapping a fresh cluster.
 type RestoreConfig struct {
-	// SnapshotPath is the path of snapshot file to restore from.
+	// SnapshotPath is the path of the snapshot db pulled from the leader.
 	SnapshotPath string
 
-	// Name is the human-readable name of this member.
+	// Name is this member's (the joiner's) human-readable name.
 	Name string
 
-	// OutputDataDir is the target data directory to save restored data.
-	// OutputDataDir should not conflict with existing etcd data directory.
-	// If OutputDataDir already exists, it will return an error to prevent
-	// unintended data directory overwrites.
-	// If empty, defaults to "[Name].etcd" if not given.
+	// SelfID is the leader-assigned member ID for the joining node.
+	SelfID uint64
+	// ClusterID is the live cluster's ID; the seeded node must agree on it.
+	ClusterID uint64
+	// Members is the full membership as the leader reports it (including self),
+	// with leader-assigned IDs preserved verbatim.
+	Members []MemberInfo
+
+	// OutputDataDir is the target data directory. Must not already exist non-empty.
 	OutputDataDir string
-	// OutputWALDir is the target WAL data directory.
-	// If empty, defaults to "[OutputDataDir]/member/wal" if not given.
+	// OutputWALDir is the target WAL directory. Defaults to OutputDataDir/member/wal.
 	OutputWALDir string
 
-	// PeerURLs is a list of member's peer URLs to advertise to the rest of the cluster.
-	PeerURLs []string
-
-	// InitialCluster is the initial cluster configuration for restore bootstrap.
-	InitialCluster string
-	// InitialClusterToken is the initial cluster token for etcd cluster during restore bootstrap.
-	InitialClusterToken string
-
-	// SkipHashCheck is "true" to ignore snapshot integrity hash value
-	// (required if copied from data directory).
+	// SkipHashCheck ignores the snapshot integrity hash (set when the snapshot
+	// was copied from a data directory rather than produced by the Snapshot API).
 	SkipHashCheck bool
 
 	// InitialMmapSize is the database initial memory map size.
 	InitialMmapSize uint64
-
-	// RevisionBump is the amount to increase the latest revision after restore,
-	// to allow administrators to trick clients into thinking that revision never decreased.
-	// If 0, revision bumping is skipped.
-	// (required if MarkCompacted == true)
-	RevisionBump uint64
-
-	// MarkCompacted is "true" to mark the latest revision as compacted.
-	// (required if RevisionBump > 0)
-	MarkCompacted bool
 }
 
-// Restore restores a new etcd data directory from given snapshot file.
+// Restore writes a seed data directory from the leader's snapshot db. The seeded
+// node impersonates one that has already applied the leader's snapshot at the
+// db's own consistent index/term, so on boot the leader catches it up over the
+// raft log (never a MsgSnap).
 func (s *v3Manager) Restore(cfg RestoreConfig) error {
-	pURLs, err := types.NewURLs(cfg.PeerURLs)
-	if err != nil {
-		return err
+	if cfg.SelfID == 0 {
+		return fmt.Errorf("restore: SelfID required")
 	}
-	var ics types.URLsMap
-	ics, err = types.NewURLsMap(cfg.InitialCluster)
-	if err != nil {
-		return err
+	if cfg.ClusterID == 0 {
+		return fmt.Errorf("restore: ClusterID required")
 	}
 
-	srv := config.ServerConfig{
-		Logger:              s.lg,
-		Name:                cfg.Name,
-		PeerURLs:            pURLs,
-		InitialPeerURLsMap:  ics,
-		InitialClusterToken: cfg.InitialClusterToken,
+	membs := make([]*membership.Member, 0, len(cfg.Members))
+	for _, mi := range cfg.Members {
+		membs = append(membs, &membership.Member{
+			ID:             types.ID(mi.ID),
+			RaftAttributes: membership.RaftAttributes{PeerURLs: mi.PeerURLs, IsLearner: mi.IsLearner},
+			Attributes:     membership.Attributes{Name: mi.Name, ClientURLs: mi.ClientURLs},
+		})
 	}
-	if err = srv.VerifyBootstrap(); err != nil {
-		return err
-	}
+	// Build the cluster with the leader's verbatim IDs and pin our local ID +
+	// the live cluster ID (NewClusterFromMembers would otherwise derive its own).
+	s.cl = membership.NewClusterFromMembers(s.lg, types.ID(cfg.ClusterID), membs)
+	s.cl.SetID(types.ID(cfg.SelfID), types.ID(cfg.ClusterID))
 
-	s.cl, err = membership.NewClusterFromURLsMap(s.lg, cfg.InitialClusterToken, ics)
-	if err != nil {
-		return err
-	}
-
 	dataDir := cfg.OutputDataDir
 	if dataDir == "" {
 		dataDir = cfg.Name + ".etcd"
@@ -296,6 +310,7 @@
 	}
 
 	s.name = cfg.Name
+	s.selfID = cfg.SelfID
 	s.srcDbPath = cfg.SnapshotPath
 	s.walDir = walDir
 	s.snapDir = filepath.Join(dataDir, "member", "snap")
@@ -303,25 +318,24 @@
 	s.initialMmapSize = cfg.InitialMmapSize
 
 	s.lg.Info(
-		"restoring snapshot",
+		"restoring join seed",
 		zap.String("path", s.srcDbPath),
 		zap.String("wal-dir", s.walDir),
 		zap.String("data-dir", dataDir),
 		zap.String("snap-dir", s.snapDir),
-		zap.Uint64("initial-memory-map-size", s.initialMmapSize),
+		zap.Uint64("self-id", cfg.SelfID),
+		zap.Uint64("cluster-id", cfg.ClusterID),
 	)
 
-	if err = s.saveDB(); err != nil {
+	if err := s.saveDB(); err != nil {
 		return err
 	}
 
-	if cfg.MarkCompacted && cfg.RevisionBump > 0 {
-		if err = s.modifyLatestRevision(cfg.RevisionBump); err != nil {
-			return err
-		}
-	}
+	// The copied db's consistent index/term is the leader raft index this applied
+	// state corresponds to — anchor the seeded raft snapshot there.
+	index, term := s.readConsistentIndex()
 
-	hardstate, err := s.saveWALAndSnap()
+	hardstate, err := s.saveWALAndSnap(index, term)
 	if err != nil {
 		return err
 	}
@@ -331,12 +345,10 @@
 	}
 
 	s.lg.Info(
-		"restored snapshot",
-		zap.String("path", s.srcDbPath),
-		zap.String("wal-dir", s.walDir),
+		"restored join seed",
 		zap.String("data-dir", dataDir),
-		zap.String("snap-dir", s.snapDir),
-		zap.Uint64("initial-memory-map-size", s.initialMmapSize),
+		zap.Uint64("snapshot-index", index),
+		zap.Uint64("snapshot-term", term),
 	)
 
 	return verify.VerifyIfEnabled(verify.Config{
@@ -368,70 +380,6 @@
 	return nil
 }
 
-// modifyLatestRevision can increase the latest revision by the given amount and sets the scheduled compaction
-// to that revision so that the server will consider this revision compacted.
-func (s *v3Manager) modifyLatestRevision(bumpAmount uint64) error {
-	be := backend.NewDefaultBackend(s.lg, s.outDbPath())
-	defer func() {
-		be.ForceCommit()
-		be.Close()
-	}()
-
-	tx := be.BatchTx()
-	tx.LockOutsideApply()
-	defer tx.Unlock()
-
-	latest, err := s.unsafeGetLatestRevision(tx)
-	if err != nil {
-		return err
-	}
-
-	latest = s.unsafeBumpBucketsRevision(tx, latest, int64(bumpAmount))
-	s.unsafeMarkRevisionCompacted(tx, latest)
-
-	return nil
-}
-
-func (s *v3Manager) unsafeBumpBucketsRevision(tx backend.UnsafeWriter, latest mvcc.Revision, amount int64) mvcc.Revision {
-	s.lg.Info(
-		"bumping latest revision",
-		zap.Int64("latest-revision", latest.Main),
-		zap.Int64("bump-amount", amount),
-		zap.Int64("new-latest-revision", latest.Main+amount),
-	)
-
-	latest.Main += amount
-	latest.Sub = 0
-	k := mvcc.NewRevBytes()
-	k = mvcc.RevToBytes(latest, k)
-	tx.UnsafePut(schema.Key, k, []byte{})
-
-	return latest
-}
-
-func (s *v3Manager) unsafeMarkRevisionCompacted(tx backend.UnsafeWriter, latest mvcc.Revision) {
-	s.lg.Info(
-		"marking revision compacted",
-		zap.Int64("revision", latest.Main),
-	)
-
-	mvcc.UnsafeSetScheduledCompact(tx, latest.Main)
-}
-
-func (s *v3Manager) unsafeGetLatestRevision(tx backend.UnsafeReader) (mvcc.Revision, error) {
-	var latest mvcc.Revision
-	err := tx.UnsafeForEach(schema.Key, func(k, _ []byte) (err error) {
-		rev := mvcc.BytesToRev(k)
-
-		if rev.GreaterThan(latest) {
-			latest = rev
-		}
-
-		return nil
-	})
-	return latest, err
-}
-
 func (s *v3Manager) copyAndVerifyDB() error {
 	srcf, ferr := os.Open(s.srcDbPath)
 	if ferr != nil {
@@ -503,15 +451,30 @@
 	return nil
 }
 
-// saveWALAndSnap creates a WAL for the initial cluster
-//
-// TODO: This code ignores learners !!!
-func (s *v3Manager) saveWALAndSnap() (*raftpb.HardState, error) {
+// readConsistentIndex reads the consistent index and term persisted in the
+// copied snapshot db. That index is the leader raft index whose applied state
+// the db reflects, which is exactly where the seeded node must claim to be.
+func (s *v3Manager) readConsistentIndex() (uint64, uint64) {
+	be := backend.NewDefaultBackend(s.lg, s.outDbPath(), backend.WithMmapSize(s.initialMmapSize))
+	defer be.Close()
+
+	tx := be.ReadTx()
+	tx.RLock()
+	defer tx.RUnlock()
+	return schema.UnsafeReadConsistentIndex(tx)
+}
+
+// saveWALAndSnap writes a WAL + snapshot describing a member that has already
+// applied a snapshot at (snapIndex, snapTerm): the membership is the leader's
+// (verbatim IDs, learner status preserved), and there are no log entries after
+// the snapshot. On boot the node is a follower at snapIndex and the leader
+// replicates forward over the log — never a MsgSnap.
+func (s *v3Manager) saveWALAndSnap(snapIndex, snapTerm uint64) (*raftpb.HardState, error) {
 	if err := fileutil.CreateDirAll(s.lg, s.walDir); err != nil {
 		return nil, err
 	}
 
-	// add members again to persist them to the backend we create.
+	// Persist the membership into the seeded backend.
 	be := backend.NewDefaultBackend(s.lg, s.outDbPath(), backend.WithMmapSize(s.initialMmapSize))
 	defer be.Close()
 	s.cl.SetBackend(schema.NewMembershipBackend(s.lg, be))
@@ -519,8 +482,7 @@
 		s.cl.AddMember(m, true)
 	}
 
-	m := s.cl.MemberByName(s.name)
-	md := &etcdserverpb.Metadata{NodeID: uint64(m.ID), ClusterID: uint64(s.cl.ID())}
+	md := &etcdserverpb.Metadata{NodeID: s.selfID, ClusterID: uint64(s.cl.ID())}
 	metadata, merr := md.Marshal()
 	if merr != nil {
 		return nil, merr
@@ -531,54 +493,28 @@
 	}
 	defer w.Close()
 
-	peers := make([]raft.Peer, len(s.cl.MemberIDs()))
-	for i, id := range s.cl.MemberIDs() {
-		ctx, err := json.Marshal((*s.cl).Member(id))
-		if err != nil {
-			return nil, err
+	// Preserve the leader's voter/learner split in the conf state.
+	var voters, learners []uint64
+	for _, id := range s.cl.MemberIDs() {
+		if s.cl.Member(id).IsLearner {
+			learners = append(learners, uint64(id))
+		} else {
+			voters = append(voters, uint64(id))
 		}
-		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
 	}
+	confState := raftpb.ConfState{Voters: voters, Learners: learners}
 
-	ents := make([]raftpb.Entry, len(peers))
-	nodeIDs := make([]uint64, len(peers))
-	for i, p := range peers {
-		nodeIDs[i] = p.ID
-		cc := raftpb.ConfChange{
-			Type:    raftpb.ConfChangeAddNode,
-			NodeID:  p.ID,
-			Context: p.Context,
-		}
-		d, err := cc.Marshal()
-		if err != nil {
-			return nil, err
-		}
-		ents[i] = raftpb.Entry{
-			Type:  raftpb.EntryConfChange,
-			Term:  1,
-			Index: uint64(i + 1),
-			Data:  d,
-		}
-	}
-
-	commit, term := uint64(len(ents)), uint64(1)
-	hardState := raftpb.HardState{
-		Term:   term,
-		Vote:   peers[0].ID,
-		Commit: commit,
-	}
-	if err := w.Save(hardState, ents); err != nil {
+	// No entries after the snapshot: the node starts already applied to snapIndex.
+	hardState := raftpb.HardState{Term: snapTerm, Commit: snapIndex}
+	if err := w.Save(hardState, nil); err != nil {
 		return nil, err
 	}
 
-	confState := raftpb.ConfState{
-		Voters: nodeIDs,
-	}
 	raftSnap := raftpb.Snapshot{
 		Data: etcdserver.GetMembershipInfoInV2Format(s.lg, s.cl),
 		Metadata: raftpb.SnapshotMetadata{
-			Index:     commit,
-			Term:      term,
+			Index:     snapIndex,
+			Term:      snapTerm,
 			ConfState: confState,
 		},
 	}
@@ -586,8 +522,8 @@
 	if err := sn.SaveSnap(raftSnap); err != nil {
 		return nil, err
 	}
-	snapshot := walpb.Snapshot{Index: commit, Term: term, ConfState: &confState}
-	return &hardState, w.SaveSnapshot(snapshot)
+	walSnap := walpb.Snapshot{Index: snapIndex, Term: snapTerm, ConfState: &confState}
+	return &hardState, w.SaveSnapshot(walSnap)
 }
 
 func (s *v3Manager) updateCIndex(commit uint64, term uint64) error {
```
