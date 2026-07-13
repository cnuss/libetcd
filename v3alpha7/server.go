package v3alpha7

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v3 "github.com/cnuss/libetcd/v3"
	"github.com/coreos/go-semver/semver"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/pkg/v3/notify"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api"
	httptypes "go.etcd.io/etcd/server/v3/etcdserver/api/etcdhttp/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	v2stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3alarm"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
	"go.etcd.io/etcd/server/v3/etcdserver/apply"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	servererrors "go.etcd.io/etcd/server/v3/etcdserver/errors"
	"go.etcd.io/etcd/server/v3/lease"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type serverImpl struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	src    v3.ServerConfig // builder; cfg() snapshots it on first use

	bemu sync.RWMutex

	// Raft-state mirrors maintained by the driver loop's ready handler and
	// the apply loop. Zero-value-ready atomics — no constructor wiring.
	lead         atomic.Uint64
	committedIdx atomic.Uint64
	appliedIdx   atomic.Uint64
	term         atomic.Uint64

	leadTimeMu      sync.RWMutex
	leadElectedTime time.Time

	// Backing stores for the lazy accessor methods below. Fallible accessors
	// (dirs, be) cancel s.ctx with the cause and cache the zero value, so a
	// failed bootstrap step stays failed and the context reports why.
	lazyCfg       lazy[config.ServerConfig]
	lazyLg        lazy[*zap.Logger]
	lazyCi        lazy[cindex.ConsistentIndexer]
	lazyBeHooks   lazy[*serverstorage.BackendHooks]
	lazyDataDir   lazy[string]
	lazyMemberDir lazy[string]
	lazySnapDir   lazy[string]
	lazySs        lazy[*snap.Snapshotter]
	lazyPrt       lazy[http.RoundTripper]
	lazyBeExist   lazy[bool]
	lazyBe        lazy[backend.Backend]

	lazyHaveWAL     lazy[bool]
	lazyWalSnap     lazy[*raftpb.Snapshot]
	lazyBwal        lazy[*bootWAL]
	lazyCl          lazy[*bootCluster]
	lazyRaftStorage lazy[*raft.MemoryStorage]
	lazyStorage     lazy[serverstorage.Storage]

	lazyAlarmStore    lazy[*v3alarm.AlarmStore]
	lazyLeaderChanged lazy[*notify.Notifier]

	lazyReqIDGen          lazy[*idutil.Generator]
	lazyW                 lazy[wait.Wait]
	lazyApplyWait         lazy[wait.WaitTime]
	lazyFirstCommitInTerm lazy[*notify.Notifier]
	lazyLessor            lazy[lease.Lessor]
	lazyTokenProvider     lazy[auth.TokenProvider]
	lazyKV                lazy[mvcc.WatchableKV]
	lazyAuthStore         lazy[auth.AuthStore]
	lazyUberApply         lazy[apply.UberApplier]
	lazyApplyLoop         lazy[*applyLoop]

	lazyBootRaft    lazy[*bootRaft]
	lazyRaftNode    lazy[raft.Node]
	lazyErrc        lazy[chan error]
	lazyServerStats lazy[*v2stats.ServerStats]
	lazyLeaderStats lazy[*v2stats.LeaderStats]
	lazyTransport   lazy[rafthttp.Transporter]
	lazyRaftDriver  lazy[*raftDriver]
}

var _ v3.Server = (*serverImpl)(nil)

func newServer(cfg v3.ServerConfig) v3.Server {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &serverImpl{ctx: ctx, cancel: cancel, src: cfg}
}

// cfg snapshots the source builder on first use — With* calls on the source
// builder land until then, never after.
func (s *serverImpl) cfg() config.ServerConfig { return s.lazyCfg.do(s.src.Build) }

func (s *serverImpl) lg() *zap.Logger {
	return s.lazyLg.do(func() *zap.Logger { return s.cfg().Logger })
}

// ci starts detached from any backend; be attaches the backend on first
// open, matching bootstrapBackend's NewConsistentIndex(nil)+SetBackend.
func (s *serverImpl) ci() cindex.ConsistentIndexer {
	return s.lazyCi.do(func() cindex.ConsistentIndexer { return cindex.NewConsistentIndex(nil) })
}

func (s *serverImpl) beHooks() *serverstorage.BackendHooks {
	return s.lazyBeHooks.do(func() *serverstorage.BackendHooks {
		return serverstorage.NewBackendHooks(s.lg(), s.ci())
	})
}

// The dir accessors create their directory on first use (bootstrap()'s
// TouchDirAll: 0700 + permission check) and return its path; "" means
// creation failed and s.ctx carries the cause. Each ensures its parent
// first: snapDir → memberDir → dataDir.

func (s *serverImpl) dataDir() string {
	return s.lazyDataDir.do(func() string {
		dir := s.cfg().DataDir
		if err := fileutil.TouchDirAll(s.lg(), dir); err != nil {
			s.cancel(err)
			return ""
		}
		return dir
	})
}

func (s *serverImpl) memberDir() string {
	return s.lazyMemberDir.do(func() string {
		if s.dataDir() == "" {
			return ""
		}
		cfg := s.cfg()
		dir := cfg.MemberDir()
		if err := fileutil.TouchDirAll(s.lg(), dir); err != nil {
			s.cancel(err)
			return ""
		}
		return dir
	})
}

func (s *serverImpl) snapDir() string {
	return s.lazySnapDir.do(func() string {
		if s.memberDir() == "" {
			return ""
		}
		cfg := s.cfg()
		dir := cfg.SnapDir()
		if err := fileutil.TouchDirAll(s.lg(), dir); err != nil {
			s.cancel(err)
			return ""
		}
		return dir
	})
}

// ss mirrors etcdserver's bootstrapSnapshot: snapDir ensures the directory
// exists, stale tmp* files from interrupted snapshot saves are swept (log-only
// on failure, like upstream), then the snapshotter opens over the dir. nil
// means the dir chain failed and s.ctx carries the cause.
func (s *serverImpl) ss() *snap.Snapshotter {
	return s.lazySs.do(func() *snap.Snapshotter {
		dir := s.snapDir()
		if dir == "" {
			return nil
		}
		if err := fileutil.RemoveMatchFile(s.lg(), dir, func(fileName string) bool {
			return strings.HasPrefix(fileName, "tmp")
		}); err != nil {
			s.lg().Error(
				"failed to remove temp file(s) in snapshot directory",
				zap.String("path", dir),
				zap.Error(err),
			)
		}
		return snap.New(s.lg(), dir)
	})
}

// prt is the peer round-tripper bootstrap() hands to cluster bootstrap and
// member probing — TLS from PeerTLSInfo, dial timeout derived from election
// ticks. nil means construction failed and s.ctx carries the cause.
func (s *serverImpl) prt() http.RoundTripper {
	return s.lazyPrt.do(func() http.RoundTripper {
		cfg := s.cfg()
		prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.PeerDialTimeout())
		if err != nil {
			s.cancel(err)
			return nil
		}
		return prt
	})
}

// beExist samples whether the backend file existed before be() first opened
// (and thereby created) it. be() samples it ahead of OpenBackend, so any
// later reader sees the pre-open truth.
func (s *serverImpl) beExist() bool {
	return s.lazyBeExist.do(func() bool {
		cfg := s.cfg()
		return fileutil.Exist(cfg.BackendPath())
	})
}

// be mirrors etcdserver's bootstrapBackend (bootstrap.go) using its exported
// pieces, including the haveWAL snapshot-recovery branch.
func (s *serverImpl) be() backend.Backend {
	return s.lazyBe.do(func() backend.Backend {
		cfg := s.cfg()
		// The backend lives at <data-dir>/member/snap/db; snapDir ensures
		// the whole directory chain (and cancels s.ctx on failure).
		if s.snapDir() == "" {
			return nil
		}
		beExist := s.beExist()
		be := serverstorage.OpenBackend(cfg, s.beHooks())
		s.ci().SetBackend(be)
		schema.CreateMetaBucket(be.BatchTx())
		if thresholdBytes := cfg.BootstrapDefragThresholdMegabytes * 1024 * 1024; thresholdBytes != 0 {
			if freeable := uint(be.Size() - be.SizeInUse()); freeable >= thresholdBytes {
				if err := be.Defrag(); err != nil {
					be.Close()
					s.cancel(err)
					return nil
				}
			}
		}
		// recoverSnapshot (bootstrap.go): restore the backend from the
		// newest snapshot recorded in the WAL, then sanity-check the
		// consistent index against it.
		if s.haveWAL() {
			if snapshot := s.walSnapshot(); snapshot != nil {
				recovered, err := serverstorage.RecoverSnapshotBackend(cfg, be, snapshot, beExist, s.beHooks())
				if err != nil {
					be.Close()
					s.cancel(fmt.Errorf("failed to recover v3 backend from snapshot: %w", err))
					return nil
				}
				be = recovered
				s.ci().SetBackend(be)
				if beExist {
					kvindex := s.ci().ConsistentIndex()
					if kvindex < snapshot.Metadata.GetIndex() {
						if kvindex != 0 {
							be.Close()
							s.cancel(fmt.Errorf("database file (%v index %d) does not match with snapshot (index %d)", cfg.BackendPath(), kvindex, snapshot.Metadata.GetIndex()))
							return nil
						}
						s.lg().Warn(
							"consistent index was never saved",
							zap.Uint64("snapshot-index", snapshot.Metadata.GetIndex()),
						)
					}
				}
			} else {
				if s.ctx.Err() != nil { // walSnapshot failed, not merely absent
					be.Close()
					return nil
				}
				s.lg().Info("No snapshot found. Recovering WAL from scratch!")
			}
		}
		if beExist {
			if err := schema.Validate(cfg.Logger, be.ReadTx()); err != nil {
				be.Close()
				s.cancel(err)
				return nil
			}
		}
		return be
	})
}

func (s *serverImpl) walDir() string {
	cfg := s.cfg()
	return cfg.WALDir()
}

// haveWAL samples once whether a WAL already exists — the fork in the road
// between fresh-bootstrap and restart paths. Sampled before any accessor
// creates a WAL, and memoized so every consumer sees the same branch.
func (s *serverImpl) haveWAL() bool {
	return s.lazyHaveWAL.do(func() bool { return wal.Exist(s.walDir()) })
}

// walSnapshot reconstructs the newest raft snapshot recorded in the WAL
// (recoverSnapshot's first half upstream). nil means no WAL or no snapshot
// yet; a WAL read failure cancels s.ctx.
func (s *serverImpl) walSnapshot() *raftpb.Snapshot {
	return s.lazyWalSnap.do(func() *raftpb.Snapshot {
		if !s.haveWAL() {
			return nil
		}
		walSnaps, err := wal.ValidSnapshotEntries(s.lg(), s.walDir())
		if err != nil {
			s.cancel(err)
			return nil
		}
		if len(walSnaps) == 0 {
			return nil
		}
		idx := len(walSnaps) - 1
		snapshot := &raftpb.Snapshot{
			Metadata: &raftpb.SnapshotMetadata{
				Term:  new(walSnaps[idx].GetTerm()),
				Index: new(walSnaps[idx].GetIndex()),
			},
		}
		if walSnaps[idx].ConfState != nil {
			snapshot.Metadata.ConfState = proto.Clone(walSnaps[idx].ConfState).(*raftpb.ConfState)
		}
		s.lg().Info("constructed a snapshot from WAL record",
			zap.Uint64("snapshot-index", snapshot.Metadata.GetIndex()),
			zap.Int("snapshot-size", proto.Size(snapshot)),
			zap.String("confState", snapshot.Metadata.ConfState.String()),
			zap.Int("walSnaps-count", len(walSnaps)),
		)
		return snapshot
	})
}

// bwal opens (restart) or creates (fresh bootstrap) the WAL — upstream's
// bootstrapWALFromSnapshot / bootstrapNewWAL, inlined. On restart the
// backend bootstraps first: snapshot recovery may replace it, and the
// ForceNewCluster rewrite reads the recovered consistent index (same order
// as upstream bootstrap()).
func (s *serverImpl) bwal() *bootWAL {
	return s.lazyBwal.do(func() *bootWAL {
		cfg := s.cfg()
		if s.haveWAL() {
			if s.be() == nil {
				return nil
			}
			if err := fileutil.IsDirWriteable(s.walDir()); err != nil {
				s.cancel(fmt.Errorf("cannot write to WAL directory: %w", err))
				return nil
			}
			s.lg().Info("Bootstrapping WAL from snapshot")

			// openWALFromSnapshot, inlined: open at the snapshot position,
			// repairing a torn tail (io.ErrUnexpectedEOF) at most once.
			var walsnap walpb.Snapshot
			snapshot := s.walSnapshot()
			if snapshot != nil {
				walsnap.Index, walsnap.Term = new(snapshot.Metadata.GetIndex()), new(snapshot.Metadata.GetTerm())
			}
			var (
				w    *wal.WAL
				st   *raftpb.HardState
				ents []*raftpb.Entry
				meta *snapMeta
			)
			repaired := false
			for {
				var err error
				w, err = wal.Open(s.lg(), s.walDir(), &walsnap)
				if err != nil {
					s.cancel(fmt.Errorf("failed to open WAL: %w", err))
					return nil
				}
				if cfg.UnsafeNoFsync {
					w.SetUnsafeNoFsync()
				}
				var wmetadata []byte
				wmetadata, st, ents, err = w.ReadAll()
				if err != nil {
					w.Close()
					if repaired || !errors.Is(err, io.ErrUnexpectedEOF) {
						s.cancel(fmt.Errorf("failed to read WAL, cannot be repaired: %w", err))
						return nil
					}
					if !wal.Repair(s.lg(), s.walDir()) {
						s.cancel(fmt.Errorf("failed to repair WAL: %w", err))
						return nil
					}
					s.lg().Info("repaired WAL", zap.Error(err))
					repaired = true
					continue
				}
				var metadata etcdserverpb.Metadata
				pbutil.MustUnmarshalMessage(&metadata, wmetadata)
				meta = &snapMeta{nodeID: types.ID(metadata.GetNodeID()), clusterID: types.ID(metadata.GetClusterID())}
				break
			}
			bw := &bootWAL{lg: s.lg(), haveWAL: true, w: w, st: st, ents: ents, snapshot: snapshot, meta: meta}

			if cfg.ForceNewCluster {
				consistentIndex := s.ci().ConsistentIndex()
				oldCommitIndex := bw.st.GetCommit()
				// Reset Commit to max(HardState.Commit, consistent_index) so
				// entries that were applied but whose commit index was never
				// persisted survive the force-restart. See upstream comment.
				bw.st.Commit = new(max(oldCommitIndex, consistentIndex))
				bw.ents = bw.committedEntries()
				if err := bw.appendAndCommitEntries(bw.newConfigChangeEntries()); err != nil {
					s.cancel(err)
					return nil
				}
				s.lg().Info(
					"forcing restart member",
					zap.String("cluster-id", meta.clusterID.String()),
					zap.String("local-member-id", meta.nodeID.String()),
					zap.Uint64("wal-commit-index", oldCommitIndex),
					zap.Uint64("commit-index", bw.st.GetCommit()),
				)
			} else {
				s.lg().Info(
					"restarting local member",
					zap.String("cluster-id", meta.clusterID.String()),
					zap.String("local-member-id", meta.nodeID.String()),
					zap.Uint64("commit-index", bw.st.GetCommit()),
				)
			}
			return bw
		}

		// bootstrapNewWAL: fresh WAL stamped with the cluster identity, so
		// the cluster must bootstrap first (reverse of the restart path).
		c := s.cl()
		if c == nil || s.memberDir() == "" {
			return nil
		}
		metadata := pbutil.MustMarshalMessage(
			&etcdserverpb.Metadata{
				NodeID:    new(uint64(c.nodeID)),
				ClusterID: new(uint64(c.cl.ID())),
			},
		)
		w, err := wal.Create(s.lg(), s.walDir(), metadata)
		if err != nil {
			s.cancel(fmt.Errorf("failed to create WAL: %w", err))
			return nil
		}
		if cfg.UnsafeNoFsync {
			w.SetUnsafeNoFsync()
		}
		return &bootWAL{lg: s.lg(), w: w}
	})
}

// cl bootstraps cluster membership — upstream's bootstrapCluster plus
// Finalize, folded together since be() is lazily reachable here.
func (s *serverImpl) cl() *bootCluster {
	return s.lazyCl.do(func() *bootCluster {
		cfg := s.cfg()
		var bc *bootCluster
		switch {
		case !s.haveWAL() && !cfg.NewCluster:
			// TODO: joining needs isCompatibleWithCluster — an unexported
			// version-probe chain (cluster_util.go). Refuse rather than join
			// without the safety check.
			s.cancel(errors.New("v3alpha7: joining an existing cluster is not yet supported"))
			return nil
		case !s.haveWAL() && cfg.NewCluster:
			if err := cfg.VerifyBootstrap(); err != nil {
				s.cancel(err)
				return nil
			}
			cl, err := membership.NewClusterFromURLsMap(s.lg(), cfg.InitialClusterToken, cfg.InitialPeerURLsMap, membership.WithMaxLearners(cfg.MaxLearners))
			if err != nil {
				s.cancel(err)
				return nil
			}
			m := cl.MemberByName(cfg.Name)
			// isMemberBootstrapped, approximated with the exported
			// GetClusterFromRemotePeers: fixed 10s timeout and logged fetch
			// errors, vs upstream's quiet BootstrapTimeoutEffective variant.
			var remoteURLs []string
			for _, mem := range cl.Members() {
				if mem.Name == cfg.Name {
					continue
				}
				remoteURLs = append(remoteURLs, mem.PeerURLs...)
			}
			sort.Strings(remoteURLs)
			if len(remoteURLs) > 0 {
				prt := s.prt()
				if prt == nil {
					return nil
				}
				if rcl, gerr := etcdserver.GetClusterFromRemotePeers(s.lg(), remoteURLs, prt); gerr == nil {
					if rm := rcl.Member(m.ID); rm != nil && len(rm.ClientURLs) > 0 {
						s.cancel(fmt.Errorf("member %s has already been bootstrapped", m.ID))
						return nil
					}
				}
			}
			if cfg.ShouldDiscover() {
				s.lg().Info("Bootstrapping cluster using v3 discovery.")
				str, jerr := v3discovery.JoinCluster(s.lg(), &cfg.DiscoveryCfg, m.ID, cfg.InitialPeerURLsMap.String())
				if jerr != nil {
					s.cancel(&servererrors.DiscoveryError{Op: "join", Err: jerr})
					return nil
				}
				urlsmap, uerr := types.NewURLsMap(str)
				if uerr != nil {
					s.cancel(uerr)
					return nil
				}
				if config.CheckDuplicateURL(urlsmap) {
					s.cancel(fmt.Errorf("discovery cluster %s has duplicate url", urlsmap))
					return nil
				}
				if cl, err = membership.NewClusterFromURLsMap(s.lg(), cfg.InitialClusterToken, urlsmap, membership.WithMaxLearners(cfg.MaxLearners)); err != nil {
					s.cancel(err)
					return nil
				}
			}
			bc = &bootCluster{remotes: nil, cl: cl, nodeID: m.ID}
		default: // haveWAL: recover identity from the WAL metadata
			bw := s.bwal()
			if bw == nil {
				return nil
			}
			if err := fileutil.IsDirWriteable(s.memberDir()); err != nil {
				s.cancel(fmt.Errorf("cannot write to member directory: %w", err))
				return nil
			}
			if cfg.ShouldDiscover() {
				s.lg().Warn(
					"discovery token is ignored since cluster already initialized; valid logs are found",
					zap.String("wal-dir", s.walDir()),
				)
			}
			cl := membership.NewCluster(s.lg(), membership.WithMaxLearners(cfg.MaxLearners))
			cl.SetID(bw.meta.nodeID, bw.meta.clusterID)
			bc = &bootCluster{cl: cl, nodeID: bw.meta.nodeID}
		}

		// Finalize (upstream bootstrappedCluster.Finalize): wire the cluster
		// to the backend, recover membership on restart, validate learners.
		be := s.be()
		if be == nil {
			return nil
		}
		if !s.haveWAL() {
			bc.cl.SetID(bc.nodeID, bc.cl.ID())
		}
		bc.cl.SetBackend(schema.NewMembershipBackend(s.lg(), be))
		if s.haveWAL() {
			bc.cl.Recover(api.UpdateCapability)
			// databaseFileMissing: a v3 cluster whose backend vanished.
			if v3Cluster := bc.cl.Version() != nil && !bc.cl.Version().LessThan(semver.Version{Major: 3}); v3Cluster && !s.beExist() {
				bepath := cfg.BackendPath()
				os.RemoveAll(bepath)
				s.cancel(fmt.Errorf("database file (%v) of the backend is missing", bepath))
				return nil
			}
		}
		if err := membership.ValidateMaxLearnerConfig(cfg.MaxLearners, bc.cl.Members(), false); err != nil {
			s.cancel(err)
			return nil
		}
		return bc
	})
}

// Derived getters over the cl() group. Zero value means cluster bootstrap
// failed and s.ctx carries the cause.

func (s *serverImpl) nodeID() types.ID {
	if bc := s.cl(); bc != nil {
		return bc.nodeID
	}
	return 0
}

func (s *serverImpl) remotes() []*membership.Member {
	if bc := s.cl(); bc != nil {
		return bc.remotes
	}
	return nil
}

func (s *serverImpl) raftCluster() *membership.RaftCluster {
	if bc := s.cl(); bc != nil {
		return bc.cl
	}
	return nil
}

// alarmStore mirrors EtcdServer.restoreAlarms: the alarm store restored
// from (or initialized in) the backend.
func (s *serverImpl) alarmStore() *v3alarm.AlarmStore {
	return s.lazyAlarmStore.do(func() *v3alarm.AlarmStore {
		be := s.be()
		if be == nil {
			return nil
		}
		as, err := v3alarm.NewAlarmStore(s.lg(), schema.NewAlarmBackend(s.lg(), be))
		if err != nil {
			s.cancel(err)
			return nil
		}
		return as
	})
}

// leaderChanged is the notifier the raft run loop fires on leadership
// change; consumers receive via LeaderChangedNotify. Exists (and is safe to
// hand out) before any raft activity — it simply hasn't fired yet.
func (s *serverImpl) leaderChanged() *notify.Notifier {
	return s.lazyLeaderChanged.do(notify.NewNotifier)
}

// raftStorage seeds raft's in-memory storage from the WAL (upstream calls
// bootstrappedWAL.MemoryStorage inside bootstrapRaft*).
func (s *serverImpl) raftStorage() *raft.MemoryStorage {
	return s.lazyRaftStorage.do(func() *raft.MemoryStorage {
		bw := s.bwal()
		if bw == nil {
			return nil
		}
		return bw.memoryStorage()
	})
}

// bootRaft assembles the raft launch parameters — upstream's bootstrapRaft
// (bootstrapRaftFromCluster / bootstrapRaftFromWAL) plus raftConfig, inlined.
func (s *serverImpl) bootRaft() *bootRaft {
	return s.lazyBootRaft.do(func() *bootRaft {
		cfg := s.cfg()
		bw := s.bwal()
		if bw == nil {
			return nil
		}
		ms := s.raftStorage()
		if ms == nil {
			return nil
		}
		br := &bootRaft{
			heartbeat: time.Duration(cfg.TickMs) * time.Millisecond,
			storage:   ms,
		}

		var id types.ID
		if s.haveWAL() {
			// bootstrapRaftFromWAL: identity from the WAL metadata, no
			// peers — RestartNode replays the log.
			id = bw.meta.nodeID
		} else {
			// bootstrapRaftFromCluster: fresh start, peers from the
			// bootstrapped membership (nil peer list on join — upstream
			// passes nil ids; our cl() refuses join for now anyway).
			bc := s.cl()
			if bc == nil {
				return nil
			}
			member := bc.cl.MemberByName(cfg.Name)
			id = member.ID
			if cfg.NewCluster {
				ids := bc.cl.MemberIDs()
				br.peers = make([]raft.Peer, len(ids))
				for i, pid := range ids {
					ctx, err := json.Marshal(bc.cl.Member(pid))
					if err != nil {
						s.cancel(fmt.Errorf("failed to marshal member: %w", err))
						return nil
					}
					br.peers[i] = raft.Peer{ID: uint64(pid), Context: ctx}
				}
			}
			s.lg().Info(
				"starting local member",
				zap.String("local-member-id", member.ID.String()),
				zap.String("cluster-id", bc.cl.ID().String()),
			)
		}

		br.config = &raft.Config{
			ID:              uint64(id),
			ElectionTick:    cfg.ElectionTicks,
			HeartbeatTick:   1,
			Storage:         ms,
			MaxSizePerMsg:   1 * 1024 * 1024,
			MaxInflightMsgs: 4096 / 8,
			CheckQuorum:     true,
			PreVote:         cfg.PreVote,
			Logger:          etcdserver.NewRaftLoggerZap(s.lg().Named("raft")),
		}
		return br
	})
}

// raftNode mints the raft.Node — the exact seam a forked raft implementation
// will plug into later: everything upstream funnels through these two
// constructors (bootstrap.go:557/559), so swapping the module (or this body)
// swaps the consensus engine wholesale. Note StartNode/RestartNode spawn the
// node's run goroutine; it stays quiescent until something calls Tick.
func (s *serverImpl) raftNode() raft.Node {
	return s.lazyRaftNode.do(func() raft.Node {
		br := s.bootRaft()
		if br == nil {
			return nil
		}
		if len(br.peers) == 0 {
			return raft.RestartNode(br.config)
		}
		return raft.StartNode(br.config, br.peers)
	})
}

// reqIDGen mints unique request IDs for proposals (member ID + timestamp
// seeded, like NewServer).
func (s *serverImpl) reqIDGen() *idutil.Generator {
	return s.lazyReqIDGen.do(func() *idutil.Generator {
		return idutil.NewGenerator(uint16(s.nodeID()), time.Now())
	})
}

// errc carries critical errors from the transport (and later the serve
// loops). Buffered so reporters never block; upstream sizes it from
// listener counts, which don't exist yet.
func (s *serverImpl) errc() chan error {
	return s.lazyErrc.do(func() chan error { return make(chan error, 16) })
}

func (s *serverImpl) serverStats() *v2stats.ServerStats {
	return s.lazyServerStats.do(func() *v2stats.ServerStats {
		cfg := s.cfg()
		bc := s.cl()
		if bc == nil {
			return nil
		}
		return v2stats.NewServerStats(cfg.Name, bc.cl.String())
	})
}

func (s *serverImpl) leaderStats() *v2stats.LeaderStats {
	return s.lazyLeaderStats.do(func() *v2stats.LeaderStats {
		bc := s.cl()
		if bc == nil {
			return nil
		}
		return v2stats.NewLeaderStats(s.lg(), bc.nodeID.String())
	})
}

// transport builds and starts the raft peer transport (StartEtcd's tr block
// in etcdserver.NewServer). serverImpl itself is the rafthttp.Raft the
// transport delivers inbound messages to. Peer/remote goroutines spawn here,
// on first pull — lazy like everything else, no separate Start.
func (s *serverImpl) transport() rafthttp.Transporter {
	return s.lazyTransport.do(func() rafthttp.Transporter {
		cfg := s.cfg()
		bc := s.cl()
		ss := s.ss()
		sstats := s.serverStats()
		lstats := s.leaderStats()
		if bc == nil || ss == nil || sstats == nil || lstats == nil {
			return nil
		}
		tr := &rafthttp.Transport{
			Logger:      s.lg(),
			TLSInfo:     cfg.PeerTLSInfo,
			DialTimeout: cfg.PeerDialTimeout(),
			ID:          bc.nodeID,
			URLs:        cfg.PeerURLs,
			ClusterID:   bc.cl.ID(),
			Raft:        s,
			Snapshotter: ss,
			ServerStats: sstats,
			LeaderStats: lstats,
			ErrorC:      s.errc(),
		}
		if err := tr.Start(); err != nil {
			s.cancel(err)
			return nil
		}
		for _, m := range bc.remotes {
			if m.ID != bc.nodeID {
				tr.AddRemote(m.ID, m.PeerURLs)
			}
		}
		for _, m := range bc.cl.Members() {
			if m.ID != bc.nodeID {
				tr.AddPeer(m.ID, m.PeerURLs)
			}
		}
		return tr
	})
}

// raftDriver assembles and starts our raftNode-equivalent loop. Everything
// is pulled lazily right here — raft.Node (StartNode/RestartNode), the
// transport, storage — and the loop goroutine launches on first pull. There
// is deliberately no Start(): components run when first needed.
func (s *serverImpl) raftDriver() *raftDriver {
	return s.lazyRaftDriver.do(func() *raftDriver {
		br := s.bootRaft()
		n := s.raftNode()
		st := s.storage()
		cl := s.raftCluster()
		tr := s.transport()
		if br == nil || n == nil || st == nil || cl == nil || tr == nil {
			return nil
		}
		d := &raftDriver{
			lg:           s.lg(),
			latestTickTs: time.Now(),
			node:         n,
			raftStorage:  br.storage,
			storage:      st,
			heartbeat:    br.heartbeat,
			transport:    tr,
			isIDRemoved:  func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
			fail: func(err error) {
				s.lg().Error("raft driver failed", zap.Error(err))
				s.cancel(err)
			},
			td:         contention.NewTimeoutDetector(2 * br.heartbeat),
			msgSnapC:   make(chan *raftpb.Message, maxInFlightMsgSnap),
			applyc:     make(chan toApply),
			readStateC: make(chan raft.ReadState, 1),
			stopped:    make(chan struct{}),
			done:       make(chan struct{}),
		}
		if d.heartbeat == 0 {
			d.ticker = &time.Ticker{}
		} else {
			d.ticker = time.NewTicker(d.heartbeat)
		}
		d.start(&raftReadyHandler{
			getLead:    s.lead.Load,
			updateLead: s.lead.Store,
			updateLeadership: func(newLeader bool) {
				// TODO: compactor pause/resume and lessor demote once those
				// paths exist (EtcdServer.run upstream).
				if newLeader {
					s.leadTimeMu.Lock()
					s.leadElectedTime = time.Now()
					s.leadTimeMu.Unlock()
					s.leaderChanged().Notify()
				}
			},
			updateCommittedIndex: func(ci uint64) {
				// monotonic, mirroring EtcdServer.run's handler
				for {
					cur := s.committedIdx.Load()
					if ci <= cur || s.committedIdx.CompareAndSwap(cur, ci) {
						return
					}
				}
			},
		})
		return d
	})
}

// storage assembles the etcdserver Storage over the WAL and snapshotter
// (bootstrapStorage upstream).
func (s *serverImpl) storage() serverstorage.Storage {
	return s.lazyStorage.do(func() serverstorage.Storage {
		bw := s.bwal()
		ss := s.ss()
		if bw == nil || ss == nil {
			return nil
		}
		return serverstorage.NewStorage(s.lg(), bw.w, ss)
	})
}

func (s *serverImpl) Alarms() []*etcdserverpb.AlarmMember {
	as := s.alarmStore()
	if as == nil {
		return nil
	}
	return as.Get(etcdserverpb.AlarmType_NONE)
}

func (s *serverImpl) AppliedIndex() uint64 {
	return s.appliedIdx.Load()
}

func (s *serverImpl) Cluster() api.Cluster {
	// Explicit nil check: returning a nil *membership.RaftCluster directly
	// would produce a non-nil api.Cluster wrapping a nil pointer.
	if cl := s.raftCluster(); cl != nil {
		return cl
	}
	return nil
}

func (s *serverImpl) ClusterVersion() *semver.Version {
	cl := s.raftCluster()
	if cl == nil {
		return nil
	}
	return cl.Version()
}

func (s *serverImpl) CommittedIndex() uint64 {
	return s.committedIdx.Load()
}

func (s *serverImpl) Leader() types.ID {
	return types.ID(s.lead.Load())
}

func (s *serverImpl) LeaderChangedNotify() <-chan struct{} {
	return s.leaderChanged().Receive()
}

func (s *serverImpl) MemberID() types.ID {
	return s.nodeID()
}

func (s *serverImpl) StorageVersion() *semver.Version {
	// `applySnapshot` sets a new backend instance, so we need to acquire the bemu lock.
	s.bemu.RLock()
	defer s.bemu.RUnlock()

	be := s.be()
	if be == nil {
		s.lg().Warn("Failed to open backend", zap.Error(context.Cause(s.ctx)))
		return nil
	}
	v, err := schema.DetectSchemaVersion(s.lg(), be.ReadTx())
	if err != nil {
		s.lg().Warn("Failed to detect schema version", zap.Error(err))
		return nil
	}
	return &v
}

func (s *serverImpl) Term() uint64 {
	return s.term.Load()
}

// rafthttp.Raft implementation — the transport delivers inbound peer
// traffic here (EtcdServer implements the same quartet upstream).

func (s *serverImpl) Process(ctx context.Context, m *raftpb.Message) error {
	if cl := s.raftCluster(); cl != nil && cl.IsIDRemoved(types.ID(m.GetFrom())) {
		s.lg().Warn(
			"rejected Raft message from removed member",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("removed-member-id", types.ID(m.GetFrom()).String()),
		)
		return httptypes.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}
	if s.MemberID() != types.ID(m.GetTo()) {
		s.lg().Warn(
			"rejected Raft message to mismatch member",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("mismatch-member-id", types.ID(m.GetTo()).String()),
		)
		return httptypes.NewHTTPError(http.StatusForbidden, "cannot process message to mismatch member")
	}
	if m.GetType() == raftpb.MsgApp {
		if sstats := s.serverStats(); sstats != nil {
			sstats.RecvAppendReq(types.ID(m.GetFrom()).String(), proto.Size(m))
		}
	}
	n := s.raftNode()
	if n == nil {
		return context.Cause(s.ctx)
	}
	return n.Step(ctx, m)
}

func (s *serverImpl) IsIDRemoved(id uint64) bool {
	if cl := s.raftCluster(); cl != nil {
		return cl.IsIDRemoved(types.ID(id))
	}
	return false
}

func (s *serverImpl) ReportUnreachable(id uint64) {
	if n := s.raftNode(); n != nil {
		n.ReportUnreachable(id)
	}
}

// ReportSnapshot reports snapshot sent status to the raft state machine.
func (s *serverImpl) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	if n := s.raftNode(); n != nil {
		n.ReportSnapshot(id, status)
	}
}
