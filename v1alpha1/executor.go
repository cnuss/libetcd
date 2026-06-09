package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1/hack"
)

// Start mints and starts the server (at most once) and serves the client and
// peer HTTP servers on their listeners in the background. Listeners not supplied
// via WithClientListener/WithPeerListener are auto-bound to a free loopback
// port. It returns the latched configuration error if the server can't be
// minted.
func (b *EtcdImpl) Start() error {
	if err := b.ensureListeners(); err != nil {
		return err
	}
	srv := b.Server()
	if srv == nil {
		return context.Cause(b.ctx)
	}
	b.startOnce.Do(func() {
		srv.Start()
		b.started.Store(true) // run loop active; Stop must HardStop from here

		b.mu.Lock()
		cl, pl, uctx := b.clientListener, b.peerListener, b.userCtx
		b.mu.Unlock()

		// Serve the peer + client listeners *before* waiting for ready: a joining
		// member needs its peer server up to receive raft and catch up, or
		// ReadyNotify never fires.
		if pl != nil {
			ph := b.PeerHTTP()
			go func() { _ = ph.Serve(pl) }()
		}
		if cl != nil {
			ch := b.ClientHTTP()
			go func() { _ = ch.Serve(cl) }()
		}

		// Block until the node is ready to serve, bounded by the caller's
		// context (WithContext) so it can't hang forever.
		waitCtx := b.ctx
		if uctx != nil {
			waitCtx = uctx
		}
		select {
		case <-srv.ReadyNotify():
		case <-waitCtx.Done():
		}

		// Graceful shutdown when the caller's context (WithContext) is cancelled.
		if uctx != nil {
			context.AfterFunc(uctx, func() { _ = b.Stop() })
		}
	})
	return nil
}

// Join brings the node up as a member of an existing cluster, fully managed on
// the joiner side: it binds its listeners, adds itself to the cluster as a
// learner (non-voting, so it doesn't disturb quorum while catching up), starts,
// and promotes itself to a voting member once caught up. It blocks until the
// node is a voting member, or the bounding context elapses.
func (b *EtcdImpl) Join(with v1.Client) error {
	if with == nil {
		return errors.New("join: nil client")
	}
	// Bind listeners first so the self peer URL is concrete before member-add.
	if err := b.ensureListeners(); err != nil {
		return err
	}

	b.mu.Lock()
	selfPeer := b.cfg.AdvertisePeerUrls[0].String()
	name := b.cfg.Name
	uctx := b.userCtx
	b.mu.Unlock()

	ctx := context.Background()
	if uctx != nil {
		ctx = uctx
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
	}

	// A leader-pinned client to the existing cluster for the membership changes
	// (clientv3 would forward anyway, but pinning avoids the extra hop).
	mc := with.Leader()
	if mc == nil {
		mc = with.Voters()
	}
	if mc == nil {
		return errors.New("join: no usable client from peer")
	}
	defer mc.Close()

	// Existing members, for this node's initial-cluster string.
	ml, err := mc.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("join: member list: %w", err)
	}
	parts := make([]string, 0, len(ml.Members)+1)
	for _, m := range ml.Members {
		if m.Name == "" {
			continue // not-yet-started member; has no usable name
		}
		if m.IsLearner {
			continue // learners don't serve raft, so ignore them in the initial cluster
		}
		for _, pu := range m.PeerURLs {
			parts = append(parts, m.Name+"="+pu)
		}
	}

	// Add self as a learner, retrying transient errors (e.g. "unhealthy cluster"
	// during a concurrent reconfig, or a leader change) until it succeeds.
	var id uint64
	if err := retry(ctx, func() bool {
		add, err := mc.MemberAddAsLearner(ctx, []string{selfPeer})
		if err != nil {
			return false
		}
		id = add.Member.ID
		return true
	}); err != nil {
		return fmt.Errorf("join: add member as learner: %w", err)
	}

	parts = append(parts, name+"="+selfPeer)
	initialCluster := strings.Join(parts, ",")

	// Pin the cluster config: existing members + self, joining (not bootstrapping).
	b.mutate(func() error {
		b.cfg.Name = name
		b.cfg.InitialCluster = initialCluster
		b.cfg.ClusterState = embed.ClusterStateFlagExisting
		b.clusterSet.Store(true)
		return nil
	})

	// Seed this node's data directory from a leader snapshot so it boots already
	// caught up to the leader's current raft index. Without this, a from-empty
	// joiner into a cluster that has applied >100 entries is bootstrapped by the
	// leader with a *raft snapshot* — and applying that snapshot panics the
	// embedded host on Windows (etcd renames the snapshot db over the still-open
	// backend; "Access is denied"). Seeding makes the leader catch us up over the
	// log instead. See v1alpha1/hack/snapshot.go.
	if err := b.seedFromLeader(ctx, mc, id, name); err != nil {
		return fmt.Errorf("join: seed from leader: %w", err)
	}

	if err := b.Start(); err != nil {
		return fmt.Errorf("join: start: %w", err)
	}

	// Promote learner -> voting. The seed left this node already caught up to the
	// leader's raft index, so promotion succeeds promptly. etcd rejects promotion
	// of a learner that isn't in sync with the leader, so simply retrying the
	// transient rejection is sufficient — no need to poll raft indices.
	//
	// We deliberately do NOT compare Maintenance.Status here: under heavy write
	// load, Status -> StorageVersion reads the backend's shared read tx, which etcd
	// transiently nils during a commit, and a loopback Status racing that window
	// panics the host (a separate etcd bug). MemberPromote alone is the gate.
	if err := retry(ctx, func() bool {
		_, err := mc.MemberPromote(ctx, id)
		return err == nil
	}); err != nil {
		return fmt.Errorf("join: promote member %x: %w", id, err)
	}
	return nil
}

// seedFromLeader pulls a point-in-time db snapshot from the leader and restores
// it into this node's data directory, pre-seeded with the leader-assigned member
// ID (selfID), the live cluster ID, and the full membership (learner status
// preserved). The seeded node boots as a follower already applied to the
// snapshot's raft index, so the leader replicates forward over the log and never
// sends a raft snapshot. It must run after the learner-add (so selfID and the
// membership are known) and before Start.
func (b *EtcdImpl) seedFromLeader(ctx context.Context, mc *clientv3.Client, selfID uint64, selfName string) error {
	b.mu.Lock()
	lg := b.cfg.GetLogger()
	dir := b.cfg.Dir
	b.mu.Unlock()

	// A concrete, empty data directory for the restore target.
	if dir == "" {
		d, err := os.MkdirTemp("", "libetcd-")
		if err != nil {
			return fmt.Errorf("data dir: %w", err)
		}
		dir = d
		b.mutate(func() error { b.cfg.Dir = dir; return nil })
	}

	// Full membership (including this node, still a learner) and the cluster ID,
	// taken verbatim from the leader so the seed agrees on every ID.
	ml, err := mc.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("member list: %w", err)
	}
	members := make([]hack.MemberInfo, 0, len(ml.Members))
	for _, m := range ml.Members {
		name := m.Name
		if m.ID == selfID {
			name = selfName // the leader records the new learner with an empty name
		}
		members = append(members, hack.MemberInfo{
			ID:         m.ID,
			Name:       name,
			PeerURLs:   m.PeerURLs,
			ClientURLs: m.ClientURLs,
			IsLearner:  m.IsLearner,
		})
	}

	eps := mc.Endpoints()
	if len(eps) == 0 {
		return errors.New("leader client has no endpoint")
	}

	// Pull the snapshot into a scratch dir (kept out of the restore target, which
	// must be empty).
	scratch, err := os.MkdirTemp("", "libetcd-seed-")
	if err != nil {
		return fmt.Errorf("scratch dir: %w", err)
	}
	defer os.RemoveAll(scratch)
	dbPath := filepath.Join(scratch, "leader.db")

	mgr := hack.NewV3(lg)
	leaderCfg := clientv3.Config{Endpoints: []string{eps[0]}, DialTimeout: 5 * time.Second, Logger: lg}
	if _, err := mgr.Save(ctx, leaderCfg, dbPath); err != nil {
		return fmt.Errorf("snapshot save: %w", err)
	}

	return mgr.Restore(hack.RestoreConfig{
		SnapshotPath:  dbPath,
		Name:          selfName,
		SelfID:        selfID,
		ClusterID:     ml.Header.ClusterId,
		Members:       members,
		OutputDataDir: dir,
	})
}

// retry calls fn every 500ms until it returns true, or ctx is done (whose error
// it then returns).
func retry(ctx context.Context, fn func() bool) error {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		if fn() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// ensureListeners binds a free loopback listener for any side (client/peer) that
// wasn't given one via WithClientListener/WithPeerListener. It must run before
// the server is minted so the advertised URLs match the bound ports.
func (b *EtcdImpl) ensureListeners() error {
	if b.ClientListener() == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("client listener: %w", err)
		}
		b.WithClientListener(l)
	}
	if b.PeerListener() == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("peer listener: %w", err)
		}
		b.WithPeerListener(l)
	}
	return nil
}

// Stop shuts down the HTTP servers and stops the etcd server, at most once and
// best-effort, returning the joined error. A started server is HardStopped; an
// only-minted one is Cleaned up (its backend released without a run loop).
func (b *EtcdImpl) Stop() error {
	var errs []error
	b.stopOnce.Do(func() {
		b.mu.Lock()
		ch, ph, srv := b.clientHTTP, b.peerHTTP, b.srv
		b.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if ch != nil {
			if err := ch.Shutdown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("shutdown client http: %w", err))
			}
		}
		if ph != nil {
			if err := ph.Shutdown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("shutdown peer http: %w", err))
			}
		}
		if srv != nil {
			if b.started.Load() {
				srv.HardStop()
			} else {
				srv.Cleanup()
			}
		}
	})
	return errors.Join(errs...)
}
