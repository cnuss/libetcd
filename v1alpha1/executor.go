package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"go.etcd.io/etcd/server/v3/embed"

	v1 "github.com/cnuss/libetcd/v1"
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
		b.started.Store(true)

		b.mu.Lock()
		cl, pl, uctx := b.clientListener, b.peerListener, b.userCtx
		b.mu.Unlock()

		if pl != nil {
			ph := b.PeerHTTP()
			go func() { _ = ph.Serve(pl) }()
		}
		if cl != nil {
			ch := b.ClientHTTP()
			go func() { _ = ch.Serve(cl) }()
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
	// during a concurrent reconfig, or a leader change) until it succeeds or ctx
	// elapses.
	var id uint64
	addTicker := time.NewTicker(500 * time.Millisecond)
	for {
		add, addErr := mc.MemberAddAsLearner(ctx, []string{selfPeer})
		if addErr == nil {
			id = add.Member.ID
			break
		}
		select {
		case <-ctx.Done():
			addTicker.Stop()
			return fmt.Errorf("join: member add: %w", ctx.Err())
		case <-addTicker.C:
		}
	}
	addTicker.Stop()

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

	if err := b.Start(); err != nil {
		return fmt.Errorf("join: start: %w", err)
	}

	// Wait until this learner's raft index is within 90% of the leader's before
	// attempting promotion, so etcd doesn't reject it for being out of sync.
	self := b.Self()
	leaderEP := ""
	if eps := mc.Endpoints(); len(eps) > 0 {
		leaderEP = eps[0]
	}
	syncTicker := time.NewTicker(500 * time.Millisecond)
	for self != nil {
		leaderSt, lErr := mc.Status(ctx, leaderEP)
		selfSt, sErr := self.Status(ctx, "") // loopback ignores the endpoint arg
		if lErr == nil && sErr == nil && selfSt.RaftIndex*100 >= leaderSt.RaftIndex*90 {
			break
		}
		select {
		case <-ctx.Done():
			syncTicker.Stop()
			return fmt.Errorf("join: wait for sync: %w", ctx.Err())
		case <-syncTicker.C:
		}
	}
	syncTicker.Stop()

	// Promote learner -> voting. With the sync wait above this usually succeeds
	// first try, but retry until it does or ctx elapses. Blocks until voting.
	promoteTicker := time.NewTicker(500 * time.Millisecond)
	defer promoteTicker.Stop()
	for {
		if _, err := mc.MemberPromote(ctx, id); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("join: promote member %x: %w", id, ctx.Err())
		case <-promoteTicker.C:
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
