// Package v0 is the intended-stable public surface for libetcd. The Builder
// interface here is the contract callers should depend on, but until the
// module reaches v1.0.0 breaking changes can still land here; the
// implementation lives in v0alpha0 and may change between alpha revisions.
//
// libetcd is a thin, developer-friendly SDK for embedded etcd: configure a node
// with a fluent builder, then mint an etcdserver.EtcdServer from the validated
// configuration.
package v0

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
	// version, and downgrade paths. Callers mounting PeerHandler elsewhere (e.g.
	// for metrics or inspection) can use it to map the same set.
	PeerPaths() []string
	// ClientListener returns the materialized client listener — the one passed
	// to WithClientListener, or the auto-bound default — or nil before
	// Start/Join binds it or when the client side is headless.
	ClientListener() net.Listener
	// PeerListener returns the materialized peer listener — the one passed to
	// WithPeerListener, or the auto-bound default — or nil before Start/Join
	// binds it, or when the peer side is BYO-served
	// (WithPeerListener(nil, advertiseURLs...)) and libetcd binds nothing.
	PeerListener() net.Listener

	// Init initializes this member's data directory offline — no snapshot
	// file, no running server. The produced directory contains an empty
	// keyspace and the full initial cluster membership, so the member can
	// afterwards be started with just its name and data dir. Idempotent: an
	// already-initialized directory is validated (same member identity,
	// offline consistency check) rather than clobbered. For multi-member
	// bootstrap, run once per member with the same WithInitialCluster.
	Init() error
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
	// Client returns a networked clientv3.Client dialing the cluster's voting
	// members (discovered via Self's MemberList; learners excluded), or nil if
	// the configuration is invalid or the client can't be built. This is the
	// general handle for talking to the cluster from outside a single member,
	// as opposed to Self (in-process) or Leader (pinned to the leader).
	Client() *clientv3.Client
	// Peers returns the flat list of every member's peer (raft) URLs, discovered
	// via Self's MemberList (learners included). Pass it to From to join another
	// node to this cluster. Empty if the server can't be minted or the member
	// list is unavailable.
	Peers() []string
	// Endpoints returns this node's advertised client (v3 API) URLs — the
	// addresses a networked client dials to reach this member. Empty when the
	// client side is headless (WithClientListener(nil)) or the config is
	// invalid. Unlike Peers, this is just this node's own advertised endpoints,
	// not a cluster-wide list.
	Endpoints() []string
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
	// pass it via WithPeerListener (and pin the client address the same way if
	// anything dials the member's registered client URL).
	WithDir(dir string) T
	// WithClusterToken sets the initial-cluster token. Default "etcd-cluster"
	// (embed's default).
	WithClusterToken(token string) T
	// WithInitialCluster sets the full initial cluster membership
	// ("name1=peerURL1,name2=peerURL2,..."), pinning it: the single-member
	// auto-sync that normally keeps InitialCluster pointing at this node
	// stops, exactly as when Join pins the membership. For offline
	// multi-member bootstrap (Init); this node's name and advertised peer
	// URLs must appear in the map, so set WithName (and the peer advertise
	// URLs) first.
	WithInitialCluster(cluster string) T
	// WithLog routes the server's logs to writer at the given level (e.g.
	// "debug", "info", "warn", "error"). A node is silent by default.
	WithLog(level string, writer io.Writer) T
	// WithContext ties the node's lifetime to ctx: when ctx is cancelled, the
	// node is gracefully Stopped. Without it, the node runs until Stop is called.
	WithContext(ctx context.Context) T
	// WithClientListener sets the socket the client (v3 API) is served on. The
	// listener is the only serving knob — libetcd serves everything it binds:
	//
	//   - Not called: Start auto-binds a free loopback listener and serves it
	//     (the default).
	//   - Non-nil lis: the client listen+advertise URLs derive from the
	//     listener's address (https if TLS-wrapped — wrap with tls.NewListener
	//     to serve TLS). Pass a listener bound to :0 to claim a free port.
	//   - Nil: the client side is headless — no listener is bound, no client
	//     URLs are registered with the cluster, and nothing serves the v3 API
	//     on this node. Self (the in-process client) still works; networked
	//     clients reach the keyspace through other, serving members.
	//
	// Last call wins. Replaces WithClientServing; there is no separate
	// http.Server knob — compose ClientHandler yourself if you need one.
	WithClientListener(lis net.Listener) T
	// WithPeerListener sets the socket the peer (raft) protocol is served on,
	// and optionally the URLs to advertise for it:
	//
	//   - Not called: Start auto-binds a free loopback listener and serves it
	//     (the default — fine for same-host clusters; remote clusters need a
	//     routable address).
	//   - Non-nil lis: libetcd serves the peer protocol on lis.
	//   - advertiseURLs given: those are advertised to the cluster (the
	//     addresses other members dial), while libetcd still serves lis. This
	//     separates the advertised address from the bound socket — for a
	//     reverse proxy, load balancer, or tunnel (e.g. a NAT-traversing
	//     listener fronted by a public URL). Unparseable entries are dropped;
	//     if none parse, the listener's own address is advertised as a
	//     fallback.
	//   - advertiseURLs omitted: the peer listen+advertise URLs both derive
	//     from the listener's address (https if TLS-wrapped). Pass a listener
	//     bound to :0 to claim a free port.
	//   - Nil lis with advertiseURLs: BYO peer serving — libetcd binds and
	//     serves nothing on the peer side; something else serves PeerHandler()
	//     (over PeerPaths()) at the advertised URLs (a custom mux, a shared
	//     server, a transport libetcd doesn't manage), which is what the
	//     cluster dials. PeerListener returns nil. libetcd still drives raft
	//     membership and promotion; the caller owns the peer HTTP server. The
	//     peer side can't go fully dark (raft must be reachable), so nil
	//     delegates serving rather than turning it off — unlike
	//     WithClientListener(nil). Mount PeerHandler only after Start/Join
	//     returns (mounting earlier mints the server prematurely); a joining
	//     node needs no inbound raft during Join, so serving after Join is in
	//     time for steady-state replication. Stop serving (close your
	//     http.Server) before Stop — Stop closes the backend, and a peer
	//     request reaching the still-mounted handler afterwards panics in
	//     etcd's handler.
	//   - Nil lis with no advertiseURLs: a configuration error — nothing to
	//     bind and nothing to advertise leaves a raft member with no peer
	//     address.
	//
	// Last call wins. Replaces WithPeerServing.
	WithPeerListener(lis net.Listener, advertiseURLs ...string) T
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

// EtcdPeer is the surface returned by From: a node that joins an existing
// cluster (when From was given peer URLs) or bootstraps a fresh one (when it
// wasn't). It exposes the Client accessors, the Server handles, and the Builder
// setters (which chain back to EtcdPeer); the lifecycle entries are Join and
// Stop, not Start (Join handles both joining and bootstrap).
//
// The Server handles (Server/GrpcServer/ClientHandler/PeerHandler/…) are for use
// after Join has started the node — reading the running server, registering
// extra gRPC services, inspecting listeners. Calling them before Join mints the
// server prematurely from the bootstrap config, which Join then rejects ("server
// already minted"); build a fresh From handle in that case.
type EtcdPeer interface {
	Client
	Server
	Builder[EtcdPeer]

	// Join brings the node into the cluster at the configured peer URLs, over
	// the peer (raft) listener: it POSTs itself to a peer's /libetcd/v1/join
	// endpoint (which adds it as a learner under a cluster-wide join lock and
	// streams back a seed snapshot), restores the snapshot, starts, and
	// promotes itself to a voting member once caught up. No networked client is
	// dialed; the join is authorized by the cluster token (WithClusterToken),
	// so it is libetcd-to-libetcd. It blocks until the node is voting or the
	// bounding context elapses; on failure after the member-add it rolls the
	// half-joined member back out of the cluster.
	//
	// When From was called with no peer URLs, there is nothing to join: Join
	// bootstraps a fresh single-member cluster instead, behaving exactly like
	// New().Start() (none of the join-failure semantics below apply). This is
	// how a first node and the nodes that join it can share one From()/Join()
	// call site.
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
