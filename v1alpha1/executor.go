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
