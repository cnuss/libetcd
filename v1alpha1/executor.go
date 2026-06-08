package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
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
func (b *EtcdImpl) Join(with *clientv3.Client) error {
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

	// Existing members, for this node's initial-cluster string.
	ml, err := with.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("join: member list: %w", err)
	}
	parts := make([]string, 0, len(ml.Members)+1)
	for _, m := range ml.Members {
		if m.Name == "" {
			continue // not-yet-started member; has no usable name
		}
		for _, pu := range m.PeerURLs {
			parts = append(parts, m.Name+"="+pu)
		}
	}

	// Add self as a learner.
	add, err := with.MemberAddAsLearner(ctx, []string{selfPeer})
	if err != nil {
		return fmt.Errorf("join: member add: %w", err)
	}
	id := add.Member.ID

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

	// Promote learner -> voting once it's caught up (blocks until done).
	return b.promote(ctx, with, id)
}

// promote retries MemberPromote until it succeeds (etcd rejects promotion until
// the learner is in sync with the leader) or ctx elapses.
func (b *EtcdImpl) promote(ctx context.Context, with *clientv3.Client, id uint64) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := with.MemberPromote(ctx, id); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("join: promote member %x: %w", id, ctx.Err())
		case <-ticker.C:
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
