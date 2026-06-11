// Package v1 is the intended-stable public surface for libetcd. The Builder
// interface here is the contract callers should depend on, but until the
// module reaches v1.0.0 breaking changes can still land here; the
// implementation lives in v1alpha1 and may change between alpha revisions.
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
	Peers() []string
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
	//
	// A dir that already holds a member's data restarts that member: the node
	// boots from the dir's WAL, which carries its identity (member ID, cluster
	// ID, membership) — the dir is the only thing a restart must reuse from
	// the previous incarnation. On a multi-member cluster the restarted node
	// must also advertise the peer URL the membership registered for it, since
	// the other members dial that URL: bind a listener on the same address and
	// pass it via WithPeerServing (and pin the client address the same way if
	// anything dials the member's registered client URL).
	WithDir(dir string) T
	// WithClusterToken sets the initial-cluster token. Default "etcd-cluster"
	// (embed's default).
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

// Executor is the lifecycle of one node incarnation. A handle is single-use:
// Start and Stop each run at most once, so a "restart" is always a brand-new
// builder constructed over the previous incarnation's data dir (see WithDir
// for what else a restart must hold constant).
type Executor interface {
	// Start mints + starts the node (auto-binding listeners and serving the
	// HTTP servers), at most once per handle. Over an empty (or unset) data dir
	// it bootstraps a fresh single-member cluster; over a dir that already
	// holds a member's data it boots that member from its WAL — a restart —
	// and etcd then ignores the builder's name, initial-cluster string,
	// cluster token, and cluster state (the on-disk identity wins; the name
	// and client URLs are republished from the new config). Start blocks until
	// the node can serve, which for a restarted multi-member cluster requires
	// quorum: restart the members concurrently, not serially.
	Start() error
	// Stop shuts the node down, best-effort and idempotent. When Stop returns,
	// the data dir is released (backend and WAL closed), so a fresh builder
	// can be constructed over it.
	Stop() error
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
// chain back to EtcdPeer), but not Start — the lifecycle entries are Join and
// Stop.
type EtcdPeer interface {
	Client
	Builder[EtcdPeer]

	// Join brings the node into the cluster at the configured peer URLs: it
	// discovers a client endpoint from those peers, takes a cluster-wide join
	// lock so concurrent joiners serialize, adds itself as a learner, starts,
	// and promotes itself to a voting member once caught up. It blocks until the
	// node is voting or the bounding context elapses; on failure after the
	// member-add it rolls the half-joined member back out of the cluster.
	//
	// Join is for first-time membership only. Restarting a member that already
	// joined is Start's job: construct a fresh New() builder over its data dir
	// (it boots from the WAL and rejoins raft; see Executor and WithDir) —
	// joining again would collide with the member the cluster already has.
	//
	// The join lock writes transient coordination keys under the
	// "libetcd/lock/" prefix in the target cluster's keyspace; they are visible
	// to scans, watchers, and backups, and applications should avoid colliding
	// keys under that prefix.
	//
	// A failure before the local server started leaves the handle reusable:
	// Join may simply be called again. A failure after the server started
	// exhausts the handle (the embedded server is single-use) — further Join
	// calls fail immediately; build a fresh From handle to try again.
	Join() error
	// Stop shuts the node down, best-effort and idempotent.
	Stop() error
}
