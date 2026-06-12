package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Start mints and starts the server (at most once) and serves the client and
// peer HTTP servers on their listeners in the background. Listeners not
// supplied via WithClientListener/WithPeerListener materialize from the
// auto-bind defaults (a free loopback port each); a headless client side
// (WithClientListener(nil)) binds and serves nothing. It returns the latched
// configuration error if the server can't be minted.
//
// Over a data dir that already holds a member's data, the minted server boots
// that member from its WAL — a restart, with the config's name/initial-cluster/
// cluster-state ignored in favor of the on-disk identity. Start/Stop are
// once-guarded, so a restart is always a fresh builder over the old dir; see
// the v1.Executor contract.
//
// When startWaitCtx is set (Join sets it before calling Start), the ready wait
// is bounded by it instead of the user context and its expiry is returned as
// an error — an unready joiner must surface within the join budget so the
// rollback can run, not hang forever on ReadyNotify.
func (b *EtcdImpl) Start() (err error) {
	// Server() materializes the listeners on the way (each factory invoked
	// once; nil factories do nothing) before minting from the derived URLs.
	srv := b.Server()
	if srv == nil {
		return context.Cause(b.ctx)
	}
	b.startOnce.Do(func() {
		srv.Start()
		b.started.Store(true) // run loop active; Stop must HardStop from here

		b.mu.Lock()
		cl, pl, uctx, waitCtx := b.clientListener, b.peerListener, b.userCtx, b.startWaitCtx
		b.mu.Unlock()

		// Serve the peer + client listeners *before* waiting for ready: a joining
		// member needs its peer server up to receive raft and catch up, or
		// ReadyNotify never fires. peerHTTP/clientHTTP resolve each side's
		// http factory; a side with no listener serves nothing.
		if pl != nil {
			if ph := b.peerServer(); ph != nil {
				go func() { _ = ph.Serve(pl) }()
			}
		}
		if cl != nil {
			if ch := b.clientServer(); ch != nil {
				go func() { _ = ch.Serve(cl) }()
			}
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
