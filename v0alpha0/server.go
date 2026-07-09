package v0alpha0

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"

	"github.com/cnuss/libetcd/v0alpha0/join"
	"github.com/cnuss/libetcd/v0alpha0/lock"
	"github.com/cnuss/libetcd/v0alpha0/stream"
	clientv3 "go.etcd.io/etcd/client/v3"
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
		// Materialize the listeners before the config is read: minting bakes
		// the advertise URLs into the server, so the factories must have run
		// (binding sockets and deriving URLs) by now. Headless sides stay nil.
		b.ClientListener()
		b.PeerListener()

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
		srvcfg, cause, lg := b.srvcfg, context.Cause(b.ctx), b.cfg.GetLogger()
		b.mu.Unlock()

		if cause != nil {
			return
		}
		srv, err := etcdserver.NewServer(srvcfg)
		if err != nil {
			b.cancel(fmt.Errorf("new server: %w", err))
			return
		}
		// Wrap the raft stream RoundTripper so the serve-side 206 is accepted by
		// the stock reader (issue #8). Done here — after NewServer mints it,
		// before Start fires the raft/apply loops. On the join path NewServer
		// has already started reader goroutines, so Intercept quiesces them
		// around the swap (issue #52) — see stream.Intercept.
		stream.Intercept(srv, lg)
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

// ClientListener materializes and returns the client listener: on first call
// it invokes the client listener factory (binding the auto-bind default or
// handing out the WithClientListener socket) and derives the client
// listen/advertise URLs from its address. Nil when the client side is headless
// (WithClientListener(nil) cleared the factory) or the factory failed (the
// error is latched as the config cause).
func (b *EtcdImpl) ClientListener() net.Listener {
	b.clientListenerOnce.Do(func() {
		b.clientListenerMaterialized.Store(true)
		b.mu.Lock()
		f, latched := b.clientListenerFactory, context.Cause(b.ctx)
		b.mu.Unlock()
		// A latched config error makes the mutate below a no-op, so binding now
		// would leak the socket (never stored, never closed). Don't bind.
		if f == nil || latched != nil {
			return
		}
		lis, err := f()
		if err != nil {
			b.cancel(fmt.Errorf("client listener: %w", err))
			return
		}
		b.mutate(func() error {
			b.clientListener = lis
			u := listenerURL(lis)
			b.cfg.ListenClientUrls = []url.URL{u}
			b.cfg.AdvertiseClientUrls = []url.URL{u}
			return nil
		})
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientListener
}

// PeerListener materializes and returns the peer listener, exactly as
// ClientListener does for the client side. Nil only when the factory failed
// (latched as the config cause) — the peer side cannot be turned off.
func (b *EtcdImpl) PeerListener() net.Listener {
	b.peerListenerOnce.Do(func() {
		b.peerListenerMaterialized.Store(true)
		b.mu.Lock()
		f, latched := b.peerListenerFactory, context.Cause(b.ctx)
		b.mu.Unlock()
		// See ClientListener: don't bind into a latched (no-op mutate) config.
		if f == nil || latched != nil {
			return
		}
		lis, err := f()
		if err != nil {
			b.cancel(fmt.Errorf("peer listener: %w", err))
			return
		}
		b.mutate(func() error {
			b.peerListener = lis
			// applyPeerURLs honors the WithPeerListener advertise override, so
			// a proxy/tunnel address survives materialization instead of being
			// re-derived from the bound listener.
			b.applyPeerURLs(lis)
			return nil
		})
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peerListener
}

// PeerHandler returns an http.Handler serving the peer (raft) protocol plus the
// libetcd join resource for the minted server, or nil if the server can't be
// minted.
func (b *EtcdImpl) PeerHandler() http.Handler {
	srv := b.Server()
	if srv == nil {
		return nil
	}
	b.mu.Lock()
	lg, token, userinfo := b.cfg.GetLogger(), b.cfg.InitialClusterToken, b.joinUserinfo
	b.mu.Unlock()

	js := &join.Server{
		Self:     b.Self,
		Token:    token,
		Userinfo: userinfo, // non-empty (discovery) switches /join to JWT mode
		Acquire: func(ctx context.Context, cli *clientv3.Client) (func() error, error) {
			lk, err := lock.Acquire(ctx, cli, "peer-join")
			if err != nil {
				return nil, err
			}
			return lk.Release, nil
		},
		Logger: lg,
	}
	// etcdhttp.NewPeerHandler promises only http.Handler, so mount it under our
	// own mux rather than type-asserting it to *http.ServeMux (which would panic
	// if a future etcd version changed the type). The join resource takes its
	// own path; everything else falls through to the etcd peer handler.
	mux := http.NewServeMux()
	mux.Handle(join.Path, js)
	mux.Handle("/", etcdhttp.NewPeerHandler(lg, srv))
	// Wrap so the raft stream's success status goes out as 206 on the wire; the
	// dial side (stream.Intercept, called from Server) rewrites it back to 200
	// before the stock reader. See package stream (issue #8).
	return stream.Handler(mux)
}

// PeerPaths returns the URL path prefixes the peer (raft) protocol must serve —
// the set etcdhttp.NewPeerHandler registers (raft messages, membership, lease
// forwarding, version, downgrade) plus the libetcd join resource.
//
// This list is hand-maintained against etcd's peer mux; if a future etcd version
// adds a peer route, add it here too.
func (b *EtcdImpl) PeerPaths() []string {
	paths := []string{
		rafthttp.RaftPrefix, rafthttp.RaftPrefix + "/",
		"/members", "/members/promote/",
		leasehttp.LeasePrefix, leasehttp.LeaseInternalPrefix,
		etcdserver.DowngradeEnabledPath,
		etcdserver.PeerHashKVPath,
		"/version",
	}
	return append(paths, join.Paths()...)
}

// ClientHandler returns an http.Handler serving the etcd v3 client API for the
// minted server, or nil if the server can't be minted. It mirrors embed's
// serveClients: a v3rpc gRPC server (with election and lock services) wrapped as
// an HTTP/2 handler.
//
// When a cleartext client listener materialized (WithClientListener, non-TLS),
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

// clientServer resolves (at most once) the http.Server libetcd serves the client
// (v3 API) listener with: the client http factory's output, whose Handler is
// ClientHandler. Nil when the side's factory is nil (headless client side —
// nothing to serve). Resolving the handler mints the server, so Start calls
// this after the listeners have materialized. It is unexported deliberately:
// libetcd owns serving (you set the listener via WithClientListener, libetcd
// serves it), so the server isn't handed out for callers to mutate.
func (b *EtcdImpl) clientServer() *http.Server {
	b.clientHTTPOnce.Do(func() {
		b.mu.Lock()
		f := b.clientHTTPFactory
		b.mu.Unlock()
		if f == nil {
			return
		}
		srv := f()
		b.mu.Lock()
		b.clientHTTP = srv
		b.mu.Unlock()
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientHTTP
}

// peerServer resolves (at most once) the http.Server libetcd serves the peer
// (raft) listener with, exactly as clientServer does for the client side, and is
// unexported for the same reason: libetcd owns serving.
func (b *EtcdImpl) peerServer() *http.Server {
	b.peerHTTPOnce.Do(func() {
		b.mu.Lock()
		f := b.peerHTTPFactory
		b.mu.Unlock()
		if f == nil {
			return
		}
		srv := f()
		b.mu.Lock()
		b.peerHTTP = srv
		b.mu.Unlock()
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peerHTTP
}
