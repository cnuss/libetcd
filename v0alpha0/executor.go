package v0alpha0

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/server/v3/etcdserver"
)

// leaveTimeout/leaveRetry bound the auto-leave MemberRemove a discovery node
// attempts on Stop. etcd rejects removing a healthy voter until the cluster has
// been healthy for its HealthInterval — "unhealthy cluster" on a freshly-formed
// one — so the attempt is retried until that window opens or the budget runs
// out. Two HealthIntervals gives the window time to open with margin; an
// already-settled node leaves on the first try, a node leaving seconds after
// forming waits it out. Best-effort throughout: if it never succeeds (quorum
// already lost, last member), Stop tears down anyway.
const (
	leaveTimeout = 2 * etcdserver.HealthInterval
	leaveRetry   = 500 * time.Millisecond
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
// the v0.Executor contract.
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

		// Auto-leave (discovery nodes): remove self from membership while raft
		// still serves, so the cluster's quorum shrinks with us — a survivor never
		// wedges on a stopped-but-still-counted voter. Best-effort and bounded; a
		// failure (no quorum, or we're the last member) just proceeds to the stop
		// below, which for a node going away anyway is harmless. Must run before
		// HardStop: once the server is torn down a reconfig can't be proposed
		// (configure returns ErrStopped).
		if srv != nil && b.started.Load() && b.leaveOnStop.Load() {
			if cli := b.Self(); cli != nil {
				lctx, lcancel := context.WithTimeout(context.Background(), leaveTimeout)
				id := uint64(srv.MemberID())
				for {
					if _, err := cli.MemberRemove(lctx, id); err == nil {
						break // left cleanly — cluster quorum shrinks with us
					}
					// Most likely etcd's HealthInterval gate ("unhealthy cluster")
					// on a freshly-formed cluster; retry until it opens or we run
					// out of budget, then fall through to the teardown.
					select {
					case <-lctx.Done():
					case <-time.After(leaveRetry):
						continue
					}
					break
				}
				lcancel()
			}
		}

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
