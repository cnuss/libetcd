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

// Server exposes the server-side handles minted from a configured node: the raw
// etcdserver, its listeners, HTTP handlers, http.Servers, and the gRPC server.
type Server interface {
	// Server mints the etcdserver.EtcdServer from the validated configuration,
	// exactly once (cached on subsequent calls). Returns nil if the configuration
	// is invalid or minting fails.
	Server() *etcdserver.EtcdServer
	// GrpcServer returns the v3 gRPC server (election and lock services
	// registered) for the minted server, minted at most once. Nil if the server
	// can't be minted.
	GrpcServer() *grpc.Server
	// ClientHandler returns an http.Handler serving the gRPC client (v3) API for
	// the minted server — mount it on an HTTP/2 listener. Returns nil if the
	// server can't be minted.
	ClientHandler() http.Handler
	// PeerHandler returns an http.Handler serving the peer (raft) protocol for
	// the minted server, or nil if the server can't be minted.
	PeerHandler() http.Handler
	// ClientHTTP returns the http.Server for the client (v3 API) listener,
	// resolved at most once: the one supplied via WithClientHTTP, or a default
	// whose Handler is ClientHandler.
	ClientHTTP() *http.Server
	// PeerHTTP returns the http.Server for the peer (raft) listener, resolved at
	// most once: the one supplied via WithPeerHTTP, or a default whose Handler is
	// PeerHandler.
	PeerHTTP() *http.Server
	// ClientListener returns the listener set by WithClientListener, or nil.
	ClientListener() net.Listener
	// PeerListener returns the listener set by WithPeerListener, or nil.
	PeerListener() net.Listener
}

// Client exposes clientv3.Clients to the cluster from a running node.
type Client interface {
	// Self returns an in-process clientv3.Client wired to this node's minted
	// server, minted at most once. Nil if the server can't be minted.
	Self() *clientv3.Client
	// Leader returns a clientv3.Client pinned to the cluster leader's client
	// URLs (discovered via Self), or nil if it can't be determined. The caller
	// closes it.
	Leader() *clientv3.Client
	// Voters returns a networked clientv3.Client dialing the cluster's voting
	// members (discovered via Self's MemberList; learners excluded), or nil if
	// the configuration is invalid or the client can't be built.
	Voters() *clientv3.Client
}

// Builder configures an embedded etcd node. Configure it with the With* methods
// (each returns the node, an Etcd, for chaining), then Start it. Obtain one from
// libetcd.New.
//
// Each With* mutates an underlying embed.Config, revalidates it, and regenerates
// a derived config.ServerConfig. The first validation failure is latched and
// surfaced by the Server/Client accessors (e.g. Server returns nil) and by Start.
//
// Defaults (no method calls): a temp data dir, client URL
// http://localhost:2379, peer URL http://localhost:2380, a new cluster, and log
// level "fatal". New also applies opinionated tuning for embedded use (longer
// raft tick/election, generous snapshot retention).
type Builder interface {
	// WithName sets the node (member) name. Default: a unique generated name.
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
	// on the joiner side: given any current member (a Client), it adds itself as
	// a learner, starts, catches up, and promotes itself to a voting member. It
	// blocks until the node is a voting member (so reads work immediately) or the
	// WithContext context / an internal deadline elapses.
	Join(with Client) error
}

type Etcd interface {
	Server
	Client
	Builder
	Executor
}
