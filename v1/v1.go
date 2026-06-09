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
	"io"
	"net"
	"net/http"
	"net/url"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"google.golang.org/grpc"
)

// Peers is a flat list of member peer (raft) URLs. Client.Peers returns one for
// a running cluster; From takes one to join a new node to that cluster.
type Peers []*url.URL

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
	// PeerPaths returns the URL path prefixes the peer (raft) protocol must
	// serve: the raft message endpoints plus the membership, lease-forwarding,
	// version, and downgrade paths. WithPeerServing routes these to PeerHandler
	// when a caller supplies their own handler on the same listener, so raft keeps
	// working alongside application routes. Callers serving the peer protocol
	// themselves can use it to mount the same set.
	PeerPaths() []string
	// ClientHTTP returns the http.Server for the client (v3 API) listener,
	// resolved at most once: the one supplied to WithClientServing, or a default
	// whose Handler is ClientHandler.
	ClientHTTP() *http.Server
	// PeerHTTP returns the http.Server for the peer (raft) listener, resolved at
	// most once: the one supplied to WithPeerServing, or a default whose Handler
	// is PeerHandler.
	PeerHTTP() *http.Server
	// ClientListener returns the listener set by WithClientServing, or nil.
	ClientListener() net.Listener
	// PeerListener returns the listener set by WithPeerServing, or nil.
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
	// Peers returns the flat list of every member's peer (raft) URLs, discovered
	// via Self's MemberList (learners included). Pass it to From to join another
	// node to this cluster. Empty if the server can't be minted or the member
	// list is unavailable.
	Peers() Peers
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
// http://localhost:2379, peer URL http://localhost:2380, a new cluster, and no
// logging (a silent node; call WithLog to enable it). New also applies
// opinionated tuning for embedded use (longer raft tick/election, generous
// snapshot retention).
type Builder[T any] interface {
	// WithName sets the node (member) name. Default: a unique generated name.
	WithName(name string) T
	// WithDir sets the data directory. Default: a fresh temp directory.
	WithDir(dir string) T
	// WithClusterToken sets the initial-cluster token. Default "libetcd-cluster".
	WithClusterToken(token string) T
	// WithLog routes the server's logs to writer at the given level (e.g.
	// "debug", "info", "warn", "error"). A node is silent by default.
	WithLog(level string, writer io.Writer) T
	// WithContext ties the node's lifetime to ctx: when ctx is cancelled, the
	// node is gracefully Stopped. Without it, the node runs until Stop is called.
	WithContext(ctx context.Context) T
	// WithClientServing configures how the client (v3 API) is served, unifying the
	// listener and the http.Server in one call.
	//
	//   - lis sets the client listen+advertise URL from the listener's address
	//     (https if TLS-wrapped). Pass a listener bound to :0 to claim a free
	//     port. If nil, Start auto-binds a free loopback listener.
	//   - srv supplies the client http.Server. If nil, a default server whose
	//     Handler is ClientHandler is used. If srv carries its own Handler, it is
	//     served as-is and replaces the client API — unlike the peer side, the
	//     client handler is content-type-multiplexed gRPC and cannot be path-muxed,
	//     so a caller wanting both must compose ClientHandler themselves.
	//
	// Both nil is equivalent to not calling it. Replaces the former
	// WithClientListener + WithClientHTTP pair.
	WithClientServing(lis net.Listener, srv *http.Server) T
	// WithPeerServing configures how the peer (raft) protocol is served, unifying
	// the listener and the http.Server in one call.
	//
	//   - lis sets the peer listen+advertise URL from the listener's address
	//     (https if TLS-wrapped). Pass a listener bound to :0 to claim a free
	//     port. If nil, Start auto-binds a free loopback listener.
	//   - srv supplies the peer http.Server (custom timeouts, TLSConfig, …). If
	//     nil, a default server is used. If srv carries its own Handler — e.g. an
	//     application mux sharing the peer port — the raft PeerPaths are routed to
	//     PeerHandler and all other paths to srv.Handler, so raft keeps working
	//     alongside the application routes.
	//
	// Both nil is equivalent to not calling it: a free loopback listener serving
	// the raft handler. Replaces the former WithPeerListener + WithPeerHTTP pair.
	WithPeerServing(lis net.Listener, srv *http.Server) T
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
	Builder[Etcd]
	Executor
}

// EtcdPeer is the join-only surface returned by From: a node configured to join
// an existing cluster (reachable at a set of peer URLs) rather than to bootstrap
// a fresh one. It exposes the Client accessors and the Builder setters (which
// chain back to EtcdPeer), but not Start — the only lifecycle entry is Join.
type EtcdPeer interface {
	Client
	Builder[EtcdPeer]

	// Join brings the node into the cluster at the configured peer URLs: it
	// discovers a client endpoint from those peers, adds itself as a learner,
	// seeds from a leader snapshot, and promotes itself to a voting member. It
	// blocks until the node is voting or the bounding context elapses.
	Join() error
}
