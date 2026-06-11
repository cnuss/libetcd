package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// Start mints and starts the server (at most once) and serves the client and
// peer HTTP servers on their listeners in the background. Listeners not supplied
// via WithClientServing/WithPeerServing are auto-bound to a free loopback
// port. It returns the latched configuration error if the server can't be
// minted.
//
// Over a data dir that already holds a member's data, the minted server boots
// that member from its WAL — a restart, with the config's name/initial-cluster/
// cluster-state ignored in favor of the on-disk identity. Start/Stop are
// once-guarded, so a restart is always a fresh builder over the old dir; see
// the v1.Executor contract.
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
		// ReadyNotify never fires. PeerHTTP/ClientHTTP resolve the supplied or
		// default http.Server (and mux any application handler onto the raft paths).
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

// ensureListeners binds a free loopback listener for any side (client/peer) that
// wasn't given one via WithClientServing/WithPeerServing. It must run before
// the server is minted so the advertised URLs match the bound ports.
func (b *EtcdImpl) ensureListeners() error {
	if b.ClientListener() == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("client listener: %w", err)
		}
		b.WithClientServing(l, nil)
	}
	if b.PeerListener() == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("peer listener: %w", err)
		}
		b.WithPeerServing(l, nil)
	}
	return nil
}

// Stop stops the etcd server and shuts down the HTTP servers, at most once and
// best-effort, returning the joined error. A started server is HardStopped; an
// only-minted one is Cleaned up (its backend released without a run loop).
// Stop returning means the data dir is released (backend and WAL closed), so a
// fresh builder can be constructed over the same dir — see Start on restarts.
//
// The etcd server stops first, deliberately: on a multi-member cluster the
// peer http.Server holds the other members' long-lived raft stream
// connections, which only terminate when the raft transport stops (HardStop's
// run-loop exit does that). Shutting the HTTP servers down first would wait
// out the full shutdown timeout on those never-idle streams and report a
// spurious "context deadline exceeded".
func (b *EtcdImpl) Stop() error {
	var errs []error
	b.stopOnce.Do(func() {
		b.mu.Lock()
		ch, ph, srv := b.clientHTTP, b.peerHTTP, b.srv
		b.mu.Unlock()

		if srv != nil {
			if b.started.Load() {
				srv.HardStop()
			} else {
				srv.Cleanup()
			}
		}

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
	})
	return errors.Join(errs...)
}
