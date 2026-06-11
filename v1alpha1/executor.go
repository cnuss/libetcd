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
// port; a side opted out via WithoutClientServing/WithoutPeerServing is
// neither bound nor served. It returns the latched configuration error if the
// server can't be minted.
func (b *EtcdImpl) Start() error {
	return b.start(nil)
}

// start is Start with an optional wait bound: when waitCtx is non-nil, the
// ready wait is bounded by it instead of the user context, and its expiry is
// returned as an error. Join passes its own deadline here — the user context
// often has none (WithContext with a plain cancel), and an unready joiner must
// surface within the join budget so the rollback can run, not hang forever on
// ReadyNotify.
func (b *EtcdImpl) start(waitCtx context.Context) (err error) {
	if lerr := b.ensureListeners(); lerr != nil {
		return lerr
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

		// Block until the node is ready to serve, bounded by the supplied wait
		// context, falling back to the caller's context (WithContext) so it
		// can't hang forever. Only the explicit wait bound reports expiry as an
		// error: Start's historical contract is to return nil after a user-
		// context cancel (shutdown is the AfterFunc's job).
		wctx := b.ctx
		if uctx != nil {
			wctx = uctx
		}
		if waitCtx != nil {
			wctx = waitCtx
		}
		select {
		case <-srv.ReadyNotify():
		case <-wctx.Done():
			if waitCtx != nil {
				err = fmt.Errorf("server not ready: %w", context.Cause(wctx))
			}
		}

		// Graceful shutdown when the caller's context (WithContext) is cancelled.
		if uctx != nil {
			context.AfterFunc(uctx, func() { _ = b.Stop() })
		}
	})
	return err
}

// ensureListeners binds a free loopback listener for any side (client/peer)
// that wasn't given one via WithClientServing/WithPeerServing — unless that
// side was opted out via WithoutClientServing/WithoutPeerServing, in which case
// nothing is bound (and, the listener staying nil, Start serves nothing on it).
// It must run before the server is minted so the advertised URLs match the
// bound ports.
func (b *EtcdImpl) ensureListeners() error {
	b.mu.Lock()
	clientOff, peerOff := b.clientServingOff, b.peerServingOff
	b.mu.Unlock()

	if !clientOff && b.ClientListener() == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("client listener: %w", err)
		}
		b.WithClientServing(l, nil)
	}
	if !peerOff && b.PeerListener() == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("peer listener: %w", err)
		}
		b.WithPeerServing(l, nil)
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
