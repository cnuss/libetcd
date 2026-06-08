// Package v1 is the stable public surface for libetcd. The Builder interface
// here is the contract callers depend on across releases; the implementation
// lives in v1alpha1 and may change between alpha revisions.
//
// libetcd is a thin, developer-friendly SDK for embedded etcd: configure a node
// with a fluent builder, then mint an etcdserver.EtcdServer from the validated
// configuration.
package v1

import (
	"context"
	"net"
	"net/http"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"google.golang.org/grpc"
)

// Accessor exposes read-side handles derived from a configured builder: the
// minted server and the listeners it was given.
type Accessor interface {
	// Server mints the etcdserver.EtcdServer from the validated configuration,
	// exactly once (cached on subsequent calls). Returns nil if the configuration
	// is invalid or minting fails.
	Server() *etcdserver.EtcdServer
	// ClientListener returns the listener set by WithClientListener, or nil.
	ClientListener() net.Listener
	// PeerListener returns the listener set by WithPeerListener, or nil.
	PeerListener() net.Listener
	// PeerHandler returns an http.Handler serving the peer (raft) protocol for
	// the minted server, or nil if the server can't be minted.
	PeerHandler() http.Handler
	// ClientHandler returns an http.Handler serving the gRPC client (v3) API for
	// the minted server — mount it on an HTTP/2 listener. Returns nil if the
	// server can't be minted.
	ClientHandler() http.Handler
	// GrpcServer returns the v3 gRPC server (election and lock services
	// registered) for the minted server, minted at most once. Nil if the server
	// can't be minted.
	GrpcServer() *grpc.Server
	// Loopback returns an in-process clientv3.Client wired to the minted server,
	// minted at most once. Nil if the server can't be minted.
	Loopback() *clientv3.Client
	// Client returns a networked clientv3.Client dialing the node's client URLs,
	// or nil if the configuration is invalid or the client can't be built.
	Client() *clientv3.Client
	// ClientHTTP returns the http.Server for the client (v3 API) listener,
	// resolved at most once: the one supplied via WithClientHTTP, or a default
	// whose Handler is ClientHandler.
	ClientHTTP() *http.Server
	// PeerHTTP returns the http.Server for the peer (raft) listener, resolved at
	// most once: the one supplied via WithPeerHTTP, or a default whose Handler is
	// PeerHandler.
	PeerHTTP() *http.Server
}

// Builder configures an embedded etcd node. Configure it with the With* methods
// (each returns the node, an Etcd, for chaining), then Start it. Obtain one from
// libetcd.New.
//
// Each With* mutates an underlying embed.Config, revalidates it, and regenerates
// a derived config.ServerConfig. The first validation failure is latched and
// surfaced by the accessors (e.g. Server returns nil) and by Start.
//
// Defaults (no method calls): name "default", a temp data dir, client URL
// http://localhost:2379, peer URL http://localhost:2380, a new cluster, and log
// level "fatal".
type Builder interface {
	Accessor
	// WithName sets the node (member) name. Default "default".
	WithName(name string) Etcd
	// WithDir sets the data directory. Default: a fresh temp directory.
	WithDir(dir string) Etcd
	// WithClientListener sets the client (v3 API) listen+advertise URL from a
	// net.Listener's address (scheme inferred: https if TLS-wrapped, else http).
	// Pass a listener bound to :0 to claim a concrete free port.
	WithClientListener(l net.Listener) Etcd
	// WithPeerListener sets the peer (raft) listen+advertise URL from a
	// net.Listener's address.
	WithPeerListener(l net.Listener) Etcd
	// WithClusterToken sets the initial-cluster token. Default "libetcd-cluster".
	WithClusterToken(token string) Etcd
	// WithLogLevel sets the server log level (e.g. "error", "warn", "info").
	WithLogLevel(level string) Etcd
	// WithContext ties the node's lifetime to ctx: when ctx is cancelled, the
	// node is gracefully Stopped. Without it, the node runs until Stop is called.
	WithContext(ctx context.Context) Etcd
	// WithClientHTTP supplies the http.Server for the client (v3 API) listener.
	// Optional; ClientHTTP creates a default if unset.
	WithClientHTTP(srv *http.Server) Etcd
	// WithPeerHTTP supplies the http.Server for the peer (raft) listener.
	// Optional; PeerHTTP creates a default if unset.
	WithPeerHTTP(srv *http.Server) Etcd
}

type Executor interface {
	// Start mints + starts a fresh single-member node (auto-binding listeners and
	// serving the HTTP servers).
	Start() error
	// Stop shuts the node down, best-effort and idempotent.
	Stop() error
	// Join brings the node up as a member of an existing cluster, fully managed
	// on the joiner side: given a client to any current member, it adds itself as
	// a learner, starts, catches up, and promotes itself to a voting member. It
	// blocks until the node is a voting member (so reads work immediately) or the
	// WithContext context / an internal deadline elapses.
	Join(with *clientv3.Client) error
}

type Etcd interface {
	Accessor
	Builder
	Executor
}
