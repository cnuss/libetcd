package v3alpha7

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// maxInFlightMsgSnap is the max number of in-flight snapshot messages
	// the driver allows to have. This number is more than enough for most
	// clusters with 5 machines. (etcdserver/server.go upstream)
	maxInFlightMsgSnap = 16

	// internalTimeout bounds the read-state handoff (raftNode.start upstream).
	internalTimeout = time.Second
)

// toApply is a batch of committed entries and/or a snapshot handed to the
// apply loop. notifyc synchronizes the applier with the driver's disk
// writes; raftAdvancedC signals that Advance ran (conf-change batches only).
type toApply struct {
	entries  []*raftpb.Entry
	snapshot *raftpb.Snapshot
	notifyc  chan struct{}
	//lint:ignore U1000 consumed by the apply loop once it exists
	raftAdvancedC <-chan struct{}
}

// raftReadyHandler is the server-side surface the driver updates as raft
// state changes (mirrors etcdserver's raftReadyHandler).
type raftReadyHandler struct {
	getLead              func() uint64
	updateLead           func(uint64)
	updateLeadership     func(newLeader bool)
	updateCommittedIndex func(uint64)
}

// raftDriver is our replacement for etcdserver's unexported raftNode: the
// loop that ticks raft.Node, persists Ready batches (snapshot → WAL entries
// → fsync ordering), forwards messages to the transport, and hands committed
// entries to the apply loop. Upstream Fatal exits become fail() so the
// owning server context reports the cause instead of killing the process.
//
// transport is late-bound (upstream does the same: srv.r.transport = tr in
// NewServer) and must be set before start; start panics on nil transport,
// matching the upstream contract.
type raftDriver struct {
	lg *zap.Logger

	tickMu       sync.RWMutex
	latestTickTs time.Time

	node        raft.Node
	raftStorage *raft.MemoryStorage
	storage     serverstorage.Storage
	heartbeat   time.Duration
	transport   rafthttp.Transporter
	isIDRemoved func(id uint64) bool
	fail        func(error)

	// td detects contention on the raft heartbeat: expect to send one
	// within 2 heartbeat intervals.
	td *contention.TimeoutDetector

	msgSnapC   chan *raftpb.Message
	applyc     chan toApply
	readStateC chan raft.ReadState

	ticker  *time.Ticker
	stopped chan struct{}
	done    chan struct{}
}

// raft.Node has no internal tick lock; serialize ticks ourselves.
func (d *raftDriver) tick() {
	d.tickMu.Lock()
	d.node.Tick()
	d.latestTickTs = time.Now()
	d.tickMu.Unlock()
}

func (d *raftDriver) getLatestTickTs() time.Time {
	d.tickMu.RLock()
	defer d.tickMu.RUnlock()
	return d.latestTickTs
}

func (d *raftDriver) apply() chan toApply {
	return d.applyc
}

// start runs the driver loop in a new goroutine. It is no longer safe to
// modify the fields after it has been started. Faithful port of
// raftNode.start (etcdserver/raft.go); prometheus counters dropped for now.
func (d *raftDriver) start(rh *raftReadyHandler) {
	if d.transport == nil {
		panic("raftDriver.start: transport not set")
	}

	go func() {
		defer d.onStop()
		islead := false

		for {
			select {
			case <-d.ticker.C:
				d.tick()
			case rd := <-d.node.Ready():
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					rh.updateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					rh.updateLeadership(newLeader)
					d.td.Reset()
				}

				if len(rd.ReadStates) != 0 {
					select {
					case d.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						d.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
					case <-d.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				raftAdvancedC := make(chan struct{}, 1)
				raftSnap := proto.Clone(rd.Snapshot).(*raftpb.Snapshot)
				ap := toApply{
					entries:       rd.CommittedEntries,
					snapshot:      proto.Clone(rd.Snapshot).(*raftpb.Snapshot),
					notifyc:       notifyc,
					raftAdvancedC: raftAdvancedC,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case d.applyc <- ap:
				case <-d.stopped:
					return
				}

				// The leader can write to its disk in parallel with
				// replicating to the followers and then writing to their
				// disks. See raft thesis 10.2.1.
				if islead {
					d.transport.Send(d.processMessages(rd.Messages))
				}

				// Save the snapshot file and WAL snapshot entry before any
				// other entries or hardstate, to ensure recovery after a
				// snapshot restore is possible.
				if !raft.IsEmptySnap(raftSnap) {
					if err := d.storage.SaveSnap(raftSnap); err != nil {
						d.fail(fmt.Errorf("failed to save Raft snapshot: %w", err))
						return
					}
				}

				if err := d.storage.Save(rd.HardState, rd.Entries); err != nil {
					d.fail(fmt.Errorf("failed to save Raft hard state and entries: %w", err))
					return
				}

				if !raft.IsEmptySnap(raftSnap) {
					// Force WAL to fsync its hard state before Release()
					// drops old data, or a restart may see a truncated log.
					// See etcd-io/etcd#10219.
					if err := d.storage.Sync(); err != nil {
						d.fail(fmt.Errorf("failed to sync Raft snapshot: %w", err))
						return
					}

					// The snapshot is now claimed persisted to disk.
					notifyc <- struct{}{}

					d.raftStorage.ApplySnapshot(raftSnap)
					d.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", raftSnap.Metadata.GetIndex()))

					if err := d.storage.Release(raftSnap); err != nil {
						d.fail(fmt.Errorf("failed to release Raft wal: %w", err))
						return
					}
				}

				d.raftStorage.Append(rd.Entries)

				confChanged := false
				for _, ent := range rd.CommittedEntries {
					if ent.GetType() == raftpb.EntryConfChange {
						confChanged = true
						break
					}
				}

				if !islead {
					// Finish processing incoming messages before signaling
					// notifyc.
					msgs := d.processMessages(rd.Messages)

					// Unblock the applier waiting on raft log disk writes.
					notifyc <- struct{}{}

					// Candidate or follower waits for all pending conf
					// changes to be applied before sending messages, so
					// votes from removed members aren't counted and a slow
					// follower can't self-elect before the conf change
					// applies. (Assumes notifyc has cap 1.)
					if confChanged {
						select {
						case notifyc <- struct{}{}:
						case <-d.stopped:
							return
						}
					}

					d.transport.Send(msgs)
				} else {
					// Leader already processed MsgSnap and signaled.
					notifyc <- struct{}{}
				}

				d.node.Advance()

				if confChanged {
					raftAdvancedC <- struct{}{}
				}
			case <-d.stopped:
				return
			}
		}
	}()
}

func updateCommittedIndex(ap *toApply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].GetIndex()
	}
	if ap.snapshot != nil && ap.snapshot.Metadata.GetIndex() > ci {
		ci = ap.snapshot.Metadata.GetIndex()
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

// processMessages filters outbound messages: drops sends to removed members,
// collapses duplicate MsgAppResp, reroutes MsgSnap through msgSnapC for
// KV-snapshot merging, and observes heartbeat latency.
func (d *raftDriver) processMessages(ms []*raftpb.Message) []*raftpb.Message {
	sentAppResp := false
	var messages []*raftpb.Message
	for i := len(ms) - 1; i >= 0; i-- {
		m := ms[i]
		if d.isIDRemoved(m.GetTo()) {
			continue
		}

		if m.GetType() == raftpb.MsgAppResp {
			if sentAppResp {
				continue
			}
			sentAppResp = true
		}

		if m.GetType() == raftpb.MsgSnap {
			// MsgSnap only carries the raft snapshot; the server main loop
			// merges in the KV snapshot before sending, so reroute it.
			select {
			case d.msgSnapC <- m:
			default:
				// drop msgSnap if the inflight chan is full
			}
			continue
		}
		if m.GetType() == raftpb.MsgHeartbeat {
			if ok, exceed := d.td.Observe(m.GetTo()); !ok {
				d.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", m.GetTo())),
					zap.Duration("heartbeat-interval", d.heartbeat),
					zap.Duration("expected-duration", 2*d.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
			}
		}
		messages = append(messages, m)
	}
	return messages
}

// stop signals the loop and blocks until it acknowledges.
func (d *raftDriver) stop() {
	select {
	case d.stopped <- struct{}{}:
		// not already stopped — triggered now
	case <-d.done:
		return // already stopped
	}
	<-d.done
}

func (d *raftDriver) onStop() {
	d.node.Stop()
	d.ticker.Stop()
	if d.transport != nil {
		d.transport.Stop()
	}
	if err := d.storage.Close(); err != nil {
		d.fail(fmt.Errorf("failed to close Raft storage: %w", err))
	}
	close(d.done)
}
