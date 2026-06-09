package v1alpha1

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/etcdhttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3rpc"
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

// Self returns an in-process clientv3.Client wired to this node's minted server
// (via v3client), minted at most once. Returns nil if the server can't be
// minted.
func (b *EtcdImpl) Self() *clientv3.Client {
	srv := b.Server()
	if srv == nil {
		return nil
	}
	b.loopbackOnce.Do(func() {
		b.loopbackCli = v3client.New(srv)
	})
	return b.loopbackCli
}

// Leader returns a clientv3.Client pinned to the cluster leader's client URLs,
// discovered via this node's Self client, or nil if it can't be determined. The
// caller closes the returned client.
func (b *EtcdImpl) Leader() *clientv3.Client {
	self := b.Self()
	if self == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()
	ml, err := self.MemberList(ctx)
	if err != nil {
		return nil
	}
	return b.leaderClientFrom(ctx, self, ml)
}

// Voters returns a networked clientv3.Client that dials the cluster's voting
// members (learners excluded). It discovers the voters via the in-process Self
// client's MemberList; if that's unavailable it falls back to this node's own
// client URLs. Returns nil if the configuration is invalid or the client can't
// be built (the underlying error is latched as the builder cause).
func (b *EtcdImpl) Voters() *clientv3.Client {
	b.mu.Lock()
	cause := context.Cause(b.ctx)
	eps := urlsToEndpoints(b.cfg.AdvertiseClientUrls) // fallback: self
	lg := b.cfg.GetLogger()
	b.mu.Unlock()

	if cause != nil {
		return nil
	}

	// Prefer the cluster's voting members.
	if lb := b.Self(); lb != nil {
		ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
		ml, err := lb.MemberList(ctx)
		cancel()
		if err == nil {
			var voters []string
			for _, m := range ml.Members {
				if m.IsLearner {
					continue
				}
				voters = append(voters, m.ClientURLs...)
			}
			if len(voters) > 0 {
				eps = voters
			}
		}
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: 5 * time.Second,
		// Use the server's configured logger so the client honors WithLogLevel
		// (default "fatal") instead of clientv3's default warn-level logger.
		Logger: lg,
	})
	if err != nil {
		b.cancel(fmt.Errorf("dial client: %w", err))
		return nil
	}
	return cli
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

// ClientListener returns the listener set by WithClientListener, or nil.
func (b *EtcdImpl) ClientListener() net.Listener {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientListener
}

// PeerListener returns the listener set by WithPeerListener, or nil.
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

// ClientHandler returns an http.Handler serving the etcd v3 client API for the
// minted server, or nil if the server can't be minted. It mirrors embed's
// serveClients: a v3rpc gRPC server (with election and lock services) wrapped as
// an HTTP/2 handler.
//
// When a cleartext client listener was provided (WithClientListener, non-TLS),
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
	cleartext := cl == nil || listenerScheme(cl) == "http"

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
// at most once: the one supplied via WithClientHTTP, or a default whose Handler
// is ClientHandler.
func (b *EtcdImpl) ClientHTTP() *http.Server {
	b.clientHTTPOnce.Do(func() {
		b.mu.Lock()
		missing := b.clientHTTP == nil
		b.mu.Unlock()
		if missing {
			h := &http.Server{Handler: b.ClientHandler()}
			b.mu.Lock()
			b.clientHTTP = h
			b.mu.Unlock()
		}
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clientHTTP
}

// PeerHTTP returns the http.Server for the peer (raft) listener, resolved at
// most once: the one supplied via WithPeerHTTP, or a default whose Handler is
// PeerHandler.
func (b *EtcdImpl) PeerHTTP() *http.Server {
	b.peerHTTPOnce.Do(func() {
		b.mu.Lock()
		missing := b.peerHTTP == nil
		b.mu.Unlock()
		if missing {
			h := &http.Server{Handler: b.PeerHandler()}
			b.mu.Lock()
			b.peerHTTP = h
			b.mu.Unlock()
		}
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peerHTTP
}
