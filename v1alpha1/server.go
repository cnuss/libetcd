package v1alpha1

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/etcdhttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3rpc"
	"go.etcd.io/etcd/server/v3/lease/leasehttp"
	"google.golang.org/grpc"
)

// Server mints the etcdserver.EtcdServer from the validated configuration,
// exactly once, and returns it (cached on subsequent calls). It returns nil if
// the configuration was latched as invalid or etcdserver.NewServer fails; the
// underlying error is latched as the builder context cause.
func (b *EtcdImpl) Server() *etcdserver.EtcdServer {
	b.serverOnce.Do(func() {
		// Default an unset data dir to a temp dir named for the node, so a node
		// minted without WithDir doesn't land on a relative/shared path.
		b.mu.Lock()
		emptyDir, name := b.cfg.Dir == "", b.cfg.Name
		b.mu.Unlock()
		if emptyDir {
			dir, err := os.MkdirTemp("", name+"-")
			if err != nil {
				b.cancel(fmt.Errorf("create data dir: %w", err))
				return
			}
			b.WithDir(dir)
		}

		b.mu.Lock()
		srvcfg, cause := b.srvcfg, context.Cause(b.ctx)
		b.mu.Unlock()

		if cause != nil {
			return
		}
		srv, err := etcdserver.NewServer(srvcfg)
		if err != nil {
			b.cancel(fmt.Errorf("new server: %w", err))
			return
		}
		b.srv = srv
	})
	return b.srv
}

// GrpcServer returns the v3 gRPC server for the minted server — with the
// election and lock services registered — minted at most once. Returns nil if
// the server can't be minted.
func (b *EtcdImpl) GrpcServer() *grpc.Server {
	srv := b.Server()
	if srv == nil {
		return nil
	}
	b.grpcOnce.Do(func() {
		gs := v3rpc.Server(srv, nil, nil)
		v3c := b.Self()
		v3electionpb.RegisterElectionServer(gs, v3election.NewElectionServer(v3c))
		v3lockpb.RegisterLockServer(gs, v3lock.NewLockServer(v3c))
		b.grpcSrv = gs
	})
	return b.grpcSrv
}

// ClientListener returns the listener set by WithClientServing, or nil.
func (b *EtcdImpl) ClientListener() net.Listener {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientListener
}

// PeerListener returns the listener set by WithPeerServing, or nil.
func (b *EtcdImpl) PeerListener() net.Listener {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peerListener
}

// PeerHandler returns an http.Handler serving the peer (raft) protocol for the
// minted server, or nil if the server can't be minted.
func (b *EtcdImpl) PeerHandler() http.Handler {
	srv := b.Server()
	if srv == nil {
		return nil
	}
	b.mu.Lock()
	lg := b.cfg.GetLogger()
	b.mu.Unlock()
	return etcdhttp.NewPeerHandler(lg, srv)
}

// PeerPaths returns the URL path prefixes the peer (raft) protocol must serve —
// the same set etcdhttp.NewPeerHandler registers: raft messages, membership,
// lease forwarding, version, and downgrade. PeerHTTP routes these to PeerHandler
// when WithPeerServing was given a server carrying its own handler, so raft and
// the application's routes can share one listener.
//
// This list is hand-maintained against etcd's peer mux; if a future etcd version
// adds a peer route, add it here too.
func (b *EtcdImpl) PeerPaths() []string {
	return []string{
		rafthttp.RaftPrefix, rafthttp.RaftPrefix + "/",
		"/members", "/members/promote/",
		leasehttp.LeasePrefix, leasehttp.LeaseInternalPrefix,
		etcdserver.DowngradeEnabledPath,
		etcdserver.PeerHashKVPath,
		"/version",
	}
}

// ClientHandler returns an http.Handler serving the etcd v3 client API for the
// minted server, or nil if the server can't be minted. It mirrors embed's
// serveClients: a v3rpc gRPC server (with election and lock services) wrapped as
// an HTTP/2 handler.
//
// When a cleartext client listener was provided (WithClientServing, non-TLS),
// the REST/JSON grpc-gateway is also wired — backed by a lazy gRPC connection to
// that listener's address — and multiplexed with gRPC, and the result is h2c-
// wrapped so a plaintext listener serves HTTP/2. A TLS listener gets the gRPC-
// only handler (HTTP/2 via ALPN). With no listener, a gRPC-only, h2c-wrapped
// handler is returned. Mount it on the client listener.
func (b *EtcdImpl) ClientHandler() http.Handler {
	gs := b.GrpcServer()
	if gs == nil {
		return nil
	}

	b.mu.Lock()
	cl := b.clientListener
	b.mu.Unlock()

	// Cleartext when there's no listener or it's not TLS-wrapped.
	cleartext := cl == nil || !isTLS(cl)

	var handler http.Handler = grpcHandlerFunc(gs, nil)
	// REST gateway only for a cleartext listener: it dials the listener address
	// over insecure gRPC, which a TLS listener wouldn't accept.
	if cl != nil && cleartext {
		if gwmux, err := gatewayMux(cl.Addr().String()); err == nil {
			handler = grpcHandlerFunc(gs, gwmux)
		}
		// On gateway wiring failure, fall through to the gRPC-only handler.
	}
	if cleartext {
		handler = h2cHandler(handler)
	}
	return handler
}

// ClientHTTP returns the http.Server for the client (v3 API) listener, resolved
// at most once: the one supplied to WithClientServing, or a default whose Handler
// is ClientHandler. A supplied server with no Handler is given ClientHandler; one
// that carries its own Handler is served as-is (no path-mux — see WithClientServing).
func (b *EtcdImpl) ClientHTTP() *http.Server {
	b.clientHTTPOnce.Do(func() {
		b.mu.Lock()
		srv := b.clientHTTP
		b.mu.Unlock()

		switch {
		case srv == nil:
			srv = &http.Server{Handler: b.ClientHandler()}
		case srv.Handler == nil:
			srv.Handler = b.ClientHandler()
		}

		b.mu.Lock()
		b.clientHTTP = srv
		b.mu.Unlock()
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientHTTP
}

// PeerHTTP returns the http.Server for the peer (raft) listener, resolved at
// most once: the one supplied to WithPeerServing, or a default whose Handler is
// PeerHandler.
//
// When the supplied server carries its own Handler (an application sharing the
// peer port), the raft PeerPaths are muxed onto PeerHandler and everything else
// falls through to that handler, so raft keeps working alongside it. Resolving
// the handler mints the server, so this must be called after Start has bound the
// listeners (Start calls it); calling it earlier freezes the config.
func (b *EtcdImpl) PeerHTTP() *http.Server {
	b.peerHTTPOnce.Do(func() {
		b.mu.Lock()
		srv := b.peerHTTP
		b.mu.Unlock()

		ph := b.PeerHandler()
		switch {
		case srv == nil:
			srv = &http.Server{Handler: ph}
		case srv.Handler == nil:
			srv.Handler = ph
		default:
			// Application handler on the peer port: route raft paths to the peer
			// handler, everything else to the supplied handler.
			mux := http.NewServeMux()
			for _, p := range b.PeerPaths() {
				mux.Handle(p, ph)
			}
			mux.Handle("/", srv.Handler)
			srv.Handler = mux
		}

		b.mu.Lock()
		b.peerHTTP = srv
		b.mu.Unlock()
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peerHTTP
}
