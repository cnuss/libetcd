package v3alpha7

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Local equivalents of etcdserver's unexported bootstrapped* carriers
// (bootstrap.go upstream). Pure data plus small methods over exported APIs;
// methods that upstream terminates with Fatal return errors instead so the
// owning accessor can cancel the server context.

type snapMeta struct {
	nodeID, clusterID types.ID
}

type bootWAL struct {
	lg *zap.Logger

	haveWAL  bool
	w        *wal.WAL
	st       *raftpb.HardState
	ents     []*raftpb.Entry
	snapshot *raftpb.Snapshot
	meta     *snapMeta
}

// memoryStorage seeds raft's in-memory storage from the WAL-recovered
// snapshot, hard state and entries (bootstrappedWAL.MemoryStorage upstream).
func (bw *bootWAL) memoryStorage() *raft.MemoryStorage {
	ms := raft.NewMemoryStorage()
	if bw.snapshot != nil {
		ms.ApplySnapshot(bw.snapshot)
	}
	if bw.st != nil {
		ms.SetHardState(bw.st)
	}
	if len(bw.ents) != 0 {
		ms.Append(bw.ents)
	}
	return ms
}

func (bw *bootWAL) committedEntries() []*raftpb.Entry {
	for i, ent := range bw.ents {
		if ent.GetIndex() > bw.st.GetCommit() {
			bw.lg.Info(
				"discarding uncommitted WAL entries",
				zap.Uint64("entry-index", ent.GetIndex()),
				zap.Uint64("commit-index-from-wal", bw.st.GetCommit()),
				zap.Int("number-of-discarded-entries", len(bw.ents)-i),
			)
			return bw.ents[:i]
		}
	}
	return bw.ents
}

func (bw *bootWAL) newConfigChangeEntries() []*raftpb.Entry {
	return serverstorage.CreateConfigChangeEnts(
		bw.lg,
		serverstorage.GetEffectiveNodeIDsFromWALEntries(bw.lg, bw.snapshot, bw.ents),
		uint64(bw.meta.nodeID),
		bw.st.GetTerm(),
		bw.st.GetCommit(),
	)
}

func (bw *bootWAL) appendAndCommitEntries(ents []*raftpb.Entry) error {
	bw.ents = append(bw.ents, ents...)
	if err := bw.w.Save(&raftpb.HardState{}, ents); err != nil {
		return fmt.Errorf("failed to save hard state and entries: %w", err)
	}
	if len(bw.ents) != 0 {
		bw.st.Commit = bw.ents[len(bw.ents)-1].Index
	}
	return nil
}

type bootCluster struct {
	remotes []*membership.Member
	cl      *membership.RaftCluster
	nodeID  types.ID
}

// bootRaft carries the raft launch parameters (bootstrappedRaft upstream):
// the config and peer list StartNode/RestartNode consume, plus the heartbeat
// interval the raft driver loop will tick at.
type bootRaft struct {
	heartbeat time.Duration
	peers     []raft.Peer
	config    *raft.Config
	storage   *raft.MemoryStorage
}
