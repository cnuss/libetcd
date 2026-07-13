package v3alpha7

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/notify"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/pkg/v3/schedule"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/apply"
	servererrors "go.etcd.io/etcd/server/v3/etcdserver/errors"
	"go.etcd.io/etcd/server/v3/features"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// etcdProgress mirrors upstream: apply-loop bookkeeping of what has been
// applied and snapshotted.
type etcdProgress struct {
	confState           *raftpb.ConfState
	diskSnapshotIndex   uint64
	memorySnapshotIndex uint64
	appliedt            uint64
	appliedi            uint64
}

// confChangeResponse mirrors upstream: the value Triggered to a waiter of a
// configuration-change proposal.
type confChangeResponse struct {
	membs         []*membership.Member
	raftAdvancedC <-chan struct{}
	err           error
}

// applyLoop is the handle for the run/apply goroutine; the goroutine owns
// consuming raftDriver.apply() and dispatching applyAll in FIFO order.
type applyLoop struct {
	stopc chan struct{}
	donec chan struct{}
}

// --- infrastructure accessors ---

// w is the proposal wait registry: raft request issuers park on an ID, the
// apply path Triggers it with the Result.
func (s *serverImpl) w() wait.Wait {
	return s.lazyW.do(wait.New)
}

// applyWait wakes waiters when the applied index passes their threshold
// (token provider and linearizable reads park here).
func (s *serverImpl) applyWait() wait.WaitTime {
	return s.lazyApplyWait.do(func() wait.WaitTime { return wait.NewTimeList() })
}

// firstCommitInTerm fires when the leader commits its first (noop) entry of
// a new term.
func (s *serverImpl) firstCommitInTerm() *notify.Notifier {
	return s.lazyFirstCommitInTerm.do(notify.NewNotifier)
}

// --- storage / auth / apply component accessors (NewServer tail upstream) ---

// lessor must recover before the KV: mvcc.New reattaches keys to leases, and
// recovering the KV first would attach them to the wrong lessor. The lazy
// chain enforces it — kv() pulls lessor() before mvcc.New runs.
func (s *serverImpl) lessor() lease.Lessor {
	return s.lazyLessor.do(func() lease.Lessor {
		be := s.be()
		cl := s.raftCluster()
		if be == nil || cl == nil {
			return nil
		}
		cfg := s.cfg()
		heartbeat := time.Duration(cfg.TickMs) * time.Millisecond
		minTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat
		return lease.NewLessor(s.lg(), be, cl, lease.LessorConfig{
			MinLeaseTTL:                int64(math.Ceil(minTTL.Seconds())),
			CheckpointInterval:         cfg.LeaseCheckpointInterval,
			CheckpointPersist:          cfg.ServerFeatureGate.Enabled(features.LeaseCheckpointPersist),
			ExpiredLeasesRetryInterval: cfg.ReqTimeout(),
		})
		// TODO: SetCheckpointer (LeaseCheckpoint feature) once the raft
		// proposal path exists.
	})
}

func (s *serverImpl) tokenProvider() auth.TokenProvider {
	return s.lazyTokenProvider.do(func() auth.TokenProvider {
		cfg := s.cfg()
		tp, err := auth.NewTokenProvider(s.lg(), cfg.AuthToken,
			func(index uint64) <-chan struct{} {
				return s.applyWait().Wait(index)
			},
			time.Duration(cfg.TokenTTL)*time.Second,
		)
		if err != nil {
			s.cancel(fmt.Errorf("failed to create token provider: %w", err))
			return nil
		}
		return tp
	})
}

func (s *serverImpl) kv() mvcc.WatchableKV {
	return s.lazyKV.do(func() mvcc.WatchableKV {
		be := s.be()
		le := s.lessor() // must recover before the KV — see lessor()
		if be == nil || le == nil {
			return nil
		}
		cfg := s.cfg()
		return mvcc.New(s.lg(), be, le, mvcc.StoreConfig{
			CompactionBatchLimit:    cfg.CompactionBatchLimit,
			CompactionSleepInterval: cfg.CompactionSleepInterval,
		})
	})
}

func (s *serverImpl) authStore() auth.AuthStore {
	return s.lazyAuthStore.do(func() auth.AuthStore {
		be := s.be()
		tp := s.tokenProvider()
		if be == nil || tp == nil {
			return nil
		}
		cfg := s.cfg()
		return auth.NewAuthStore(s.lg(), schema.NewAuthBackend(s.lg(), be), tp, int(cfg.BcryptCost))
	})
}

// uberApply is the request applier stack (quota → auth → backend). Building
// it also installs the tx post-lock hook, like NewServer does after init.
func (s *serverImpl) uberApply() apply.UberApplier {
	return s.lazyUberApply.do(func() apply.UberApplier {
		kv := s.kv()
		as := s.alarmStore()
		auths := s.authStore()
		le := s.lessor()
		cl := s.raftCluster()
		be := s.be()
		if kv == nil || as == nil || auths == nil || le == nil || cl == nil || be == nil {
			return nil
		}
		cfg := s.cfg()
		ua := apply.NewUberApplier(apply.ApplierOptions{
			Logger:                       s.lg(),
			KV:                           kv,
			AlarmStore:                   as,
			AuthStore:                    auths,
			Lessor:                       le,
			Cluster:                      cl,
			RaftStatus:                   s,
			SnapshotServer:               s,
			ConsistentIndex:              s.ci(),
			TxnModeWriteWithSharedBuffer: cfg.ServerFeatureGate.Enabled(features.TxnModeWriteWithSharedBuffer),
			Backend:                      be,
			QuotaBackendBytesCfg:         cfg.QuotaBackendBytes,
			WarningApplyDuration:         cfg.WarningApplyDuration,
		})
		// Set the hook after initialization so it isn't called during it
		// (getTxPostLockInsideApplyHook upstream).
		be.SetTxPostLockInsideApplyHook(func() {
			applyingIdx, applyingTerm := s.ci().ConsistentApplyingIndex()
			if applyingIdx > s.ci().UnsafeConsistentIndex() {
				s.ci().SetConsistentIndex(applyingIdx, applyingTerm)
			}
		})
		return ua
	})
}

// ForceSnapshot satisfies apply.SnapshotServer.
// TODO: flip the force-disk-snapshot flag once snapshotting exists.
func (s *serverImpl) ForceSnapshot() {}

// --- the loop itself ---

// runLoop starts the run/apply goroutine on first pull (no Start, as
// always): it consumes the raft driver's apply channel and dispatches
// applyAll jobs in FIFO order. Pulling this forces the whole chain up
// through the raft driver.
func (s *serverImpl) runLoop() *applyLoop {
	return s.lazyApplyLoop.do(func() *applyLoop {
		d := s.raftDriver()
		ua := s.uberApply()
		if d == nil || ua == nil {
			return nil
		}
		sn, err := s.raftStorage().Snapshot()
		if err != nil {
			s.cancel(fmt.Errorf("failed to get snapshot from Raft storage: %w", err))
			return nil
		}
		ep := etcdProgress{
			confState:           sn.Metadata.ConfState,
			diskSnapshotIndex:   sn.Metadata.GetIndex(),
			memorySnapshotIndex: sn.Metadata.GetIndex(),
			appliedt:            sn.Metadata.GetTerm(),
			appliedi:            sn.Metadata.GetIndex(),
		}
		if ep.confState == nil {
			s.cancel(stderrors.New("empty confstate on apply-loop start"))
			return nil
		}

		l := &applyLoop{stopc: make(chan struct{}), donec: make(chan struct{})}
		sched := schedule.NewFIFOScheduler(s.lg())
		go func() {
			defer func() {
				sched.Stop()
				d.stop()
				close(l.donec)
			}()
			// TODO: expiredLeaseC revocation needs the raft proposal path
			// (LeaseRevoke); leases don't expire-revoke yet.
			for {
				select {
				case ap := <-d.apply():
					f := schedule.NewJob("server_applyAll", func(ctx context.Context) { s.applyAll(&ep, &ap) })
					sched.Schedule(f)
				case err := <-s.errc():
					s.lg().Warn("server error", zap.Error(err))
					s.lg().Warn("data-dir used by this member must be removed")
					s.cancel(err)
					return
				case <-l.stopc:
					return
				case <-s.ctx.Done():
					return
				}
			}
		}()
		return l
	})
}

func (s *serverImpl) applyAll(ep *etcdProgress, ap *toApply) {
	if !raft.IsEmptySnap(ap.snapshot) {
		// TODO: applySnapshot (follower receiving a leader snapshot) needs
		// the backend-swap machinery; fail loud rather than corrupt.
		s.cancel(fmt.Errorf("v3alpha7: applying an incoming raft snapshot is not yet supported (index %d)", ap.snapshot.Metadata.GetIndex()))
		return
	}
	s.applyEntries(ep, ap)
	s.applyWait().Trigger(ep.appliedi)

	// Wait for the raft routine to finish disk writes before any snapshot
	// trigger: applied index must not pass raft storage's last index.
	<-ap.notifyc

	// TODO: snapshotIfNeededAndCompactRaftLog + msgSnapC merged-snapshot
	// send, once snapshot creation exists.
}

func (s *serverImpl) applyEntries(ep *etcdProgress, ap *toApply) {
	if len(ap.entries) == 0 {
		return
	}
	firsti := ap.entries[0].GetIndex()
	if firsti > ep.appliedi+1 {
		s.lg().Panic(
			"unexpected committed entry index",
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("first-committed-entry-index", firsti),
		)
	}
	var ents []*raftpb.Entry
	if ep.appliedi+1-firsti < uint64(len(ap.entries)) {
		ents = ap.entries[ep.appliedi+1-firsti:]
	}
	if len(ents) == 0 {
		return
	}
	var shouldstop bool
	if ep.appliedt, ep.appliedi, shouldstop = s.applyEnts(ents, ep, ap.raftAdvancedC); shouldstop {
		go func() {
			time.Sleep(10 * 100 * time.Millisecond)
			s.cancel(stderrors.New("the member has been permanently removed from the cluster"))
		}()
	}
}

// applyEnts is upstream's EtcdServer.apply: dispatch entries to the normal
// or conf-change path, tracking consistent-index dedup across restarts.
func (s *serverImpl) applyEnts(es []*raftpb.Entry, ep *etcdProgress, raftAdvancedC <-chan struct{}) (appliedt uint64, appliedi uint64, shouldStop bool) {
	for i := range es {
		e := es[i]
		index := s.ci().ConsistentIndex()

		// Apply all WAL entries on top of the v2store; only 'unapplied'
		// entries (index > backend consistent index) hit the backend.
		shouldApplyV3 := membership.ApplyV2storeOnly
		if e.GetIndex() > index {
			shouldApplyV3 = membership.ApplyBoth
			s.ci().SetConsistentApplyingIndex(e.GetIndex(), e.GetTerm())
		}
		switch e.GetType() {
		case raftpb.EntryNormal:
			s.applyEntryNormal(e, shouldApplyV3)
			s.appliedIdx.Store(e.GetIndex())
			s.term.Store(e.GetTerm())

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			pbutil.MustUnmarshalMessage(&cc, e.Data)
			removedSelf, err := s.applyConfChange(&cc, ep, shouldApplyV3)
			s.appliedIdx.Store(e.GetIndex())
			s.term.Store(e.GetTerm())
			shouldStop = shouldStop || removedSelf
			s.w().Trigger(cc.GetId(), &confChangeResponse{s.raftCluster().Members(), raftAdvancedC, err})

		default:
			s.lg().Panic(
				"unknown entry type; must be either EntryNormal or EntryConfChange",
				zap.String("type", e.GetType().String()),
			)
		}
		appliedi, appliedt = e.GetIndex(), e.GetTerm()
	}
	return appliedt, appliedi, shouldStop
}

func (s *serverImpl) applyEntryNormal(e *raftpb.Entry, shouldApplyV3 membership.ShouldApplyV3) {
	if shouldApplyV3 {
		defer func() {
			// The tx post-lock hook may not run for this entry; move the
			// consistent index forward directly if so.
			newIndex := s.ci().ConsistentIndex()
			if newIndex < e.GetIndex() {
				s.ci().SetConsistentIndex(e.GetIndex(), e.GetTerm())
			}
		}()
	}

	// Raft may generate a noop entry on leader confirmation; skip early.
	if len(e.Data) == 0 {
		s.firstCommitInTerm().Notify()
		if s.isLeader() {
			cfg := s.cfg()
			s.lessor().Promote(cfg.ElectionTimeout())
		}
		return
	}

	ar, id := apply.Apply(s.lg(), e, s.uberApply(), s.w(), shouldApplyV3)

	if !shouldApplyV3 || ar == nil {
		return
	}

	if !stderrors.Is(ar.Err, servererrors.ErrNoSpace) || len(s.alarmStore().Get(etcdserverpb.AlarmType_NOSPACE)) > 0 {
		s.w().Trigger(id, ar)
		return
	}

	// TODO: raise the NOSPACE alarm through a raft proposal once the
	// proposal path exists; upstream does raftRequest(AlarmRequest) here.
	s.lg().Warn(
		"message exceeded backend quota; NOSPACE alarm not yet proposable",
		zap.Int64("quota-size-bytes", s.cfg().QuotaBackendBytes),
		zap.Error(ar.Err),
	)
	s.w().Trigger(id, ar)
}

// applyConfChange applies a ConfChange already committed through raft.
func (s *serverImpl) applyConfChange(cc *raftpb.ConfChange, ep *etcdProgress, shouldApplyV3 membership.ShouldApplyV3) (bool, error) {
	cl := s.raftCluster()
	if err := cl.ValidateConfigurationChange(cc, shouldApplyV3); err != nil {
		s.lg().Error("Validation on configuration change failed", zap.Bool("shouldApplyV3", bool(shouldApplyV3)), zap.Error(err))
		cc.NodeId = new(raft.None)
		s.raftNode().ApplyConfChange(cc)

		// The tx post-lock hook won't run in this case; set the consistent
		// index directly.
		if membership.ApplyBoth == shouldApplyV3 {
			applyingIndex, applyingTerm := s.ci().ConsistentApplyingIndex()
			s.ci().SetConsistentIndex(applyingIndex, applyingTerm)
		}
		return false, err
	}

	// Don't apply an unvalidated conf change to raft on bootstrap replay.
	if shouldApplyV3 {
		ep.confState = s.raftNode().ApplyConfChange(cc)
	}
	s.beHooks().SetConfState(ep.confState)
	switch cc.GetType() {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		confChangeContext := new(membership.ConfigChangeContext)
		if err := json.Unmarshal(cc.Context, confChangeContext); err != nil {
			s.lg().Panic("failed to unmarshal member", zap.Error(err))
		}
		if cc.GetNodeId() != uint64(confChangeContext.Member.ID) {
			s.lg().Panic(
				"got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.GetNodeId()).String()),
				zap.String("member-id-from-message", confChangeContext.Member.ID.String()),
			)
		}
		if confChangeContext.IsPromote {
			cl.PromoteMember(confChangeContext.Member.ID, shouldApplyV3)
		} else {
			cl.AddMember(&confChangeContext.Member, shouldApplyV3)
			if confChangeContext.Member.ID != s.MemberID() {
				s.transport().AddPeer(confChangeContext.Member.ID, confChangeContext.PeerURLs)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.GetNodeId())
		cl.RemoveMember(id, shouldApplyV3)
		if id == s.MemberID() {
			return true, nil
		}
		s.transport().RemovePeer(id)

	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			s.lg().Panic("failed to unmarshal member", zap.Error(err))
		}
		if cc.GetNodeId() != uint64(m.ID) {
			s.lg().Panic(
				"got different member ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.GetNodeId()).String()),
				zap.String("member-id-from-message", m.ID.String()),
			)
		}
		cl.UpdateRaftAttributes(m.ID, m.RaftAttributes, shouldApplyV3)
		if m.ID != s.MemberID() {
			s.transport().UpdatePeer(m.ID, m.PeerURLs)
		}
	}
	return false, nil
}

func (s *serverImpl) isLeader() bool {
	return uint64(s.MemberID()) == s.lead.Load()
}
