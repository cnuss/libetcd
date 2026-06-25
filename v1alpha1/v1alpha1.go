// Package v1alpha1 is the current implementation behind the v1 interfaces. The
// root libetcd façade wraps this; callers reaching directly into v1alpha1 use it
// for the concrete types. Anything here may change between alpha revisions —
// depend on the v1 contract, not these internals.
//
// The implementation is split by interface: v1alpha1.go (the EtcdImpl type and
// New), builder.go (the With* setters and the validate/derive machinery),
// server.go (Server handles), client.go (Client handles), and executor.go (the
// Executor lifecycle: Start/Stop).
package v1alpha1

import (
	"context"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	v1 "github.com/cnuss/libetcd/v1"
)

// EtcdImpl implements the full Etcd surface (Server + Client + Builder + Executor).
var _ v1.Etcd = (*EtcdImpl)(nil)

// EtcdImpl is the default implementation. It holds a live embed.Config that each
// With* mutates in place under mu; after every mutation the config is
// revalidated and a fresh config.ServerConfig is derived (the old one discarded).
// The first validation failure is latched as the cause of ctx, and the
// Server/Client accessors surface it.
type EtcdImpl struct {
	mu     sync.Mutex
	cfg    *embed.Config
	srvcfg config.ServerConfig // regenerated on each successful mutation
	ctx    context.Context
	cancel context.CancelCauseFunc

	// userCtx is the optional caller context from WithContext; when set and
	// cancelled, Start arranges a graceful Stop. Distinct from ctx, which carries
	// the config-validity cause.
	userCtx context.Context

	// startWaitCtx, set by Join right before it calls Start, bounds Start's
	// ready wait and reports its expiry as an error — the user context often
	// has no deadline (a plain cancel), and an unready joiner must fail within
	// the join budget so the rollback can run rather than hang on ReadyNotify.
	// Plain Start (field unset) keeps the historical contract: nil after a
	// user-context cancel, shutdown left to the AfterFunc.
	startWaitCtx context.Context

	// clientListenerFactory/peerListenerFactory produce each side's listener,
	// invoked lazily (exactly once) by the ClientListener/PeerListener
	// accessors. newImpl seeds auto-bind defaults; WithClientListener/
	// WithPeerListener replace them with factories handing out the caller's
	// listener. A nil factory means "do nothing": the headless client side
	// (WithClientListener(nil)) binds and serves nothing and registers no
	// client URLs.
	clientListenerFactory func() (net.Listener, error)
	peerListenerFactory   func() (net.Listener, error)

	// peerAdvertise overrides the peer advertise URLs (WithPeerListener's
	// variadic arg): the addresses the cluster dials, distinct from the bound
	// listener — for a proxy, load balancer, or tunnel. Empty means advertise
	// the listener's own address. Applied both at config time and at listener
	// materialization (see applyPeerURLs) so it survives the latter.
	peerAdvertise []url.URL

	// clientHTTPFactory/peerHTTPFactory produce the http.Servers libetcd
	// serves each side's listener with (Handler = ClientHandler/PeerHandler),
	// invoked lazily (exactly once) by clientServer/peerServer. A nil factory
	// serves nothing.
	clientHTTPFactory func() *http.Server
	clientHTTPOnce    sync.Once
	peerHTTPFactory   func() *http.Server
	peerHTTPOnce      sync.Once

	// Factory invocation is lazy and once-guarded, driven by the accessors:
	// ClientListener/PeerListener invoke their listener factory on first call
	// (binding the socket and deriving the listen/advertise URLs), and
	// clientServer/peerServer invoke their http factory on first call. These fields
	// cache the results; nil-factory sides stay nil. There is no eager
	// ensure/materialize step — Server() and Start() simply call the accessors.
	clientListenerOnce sync.Once
	clientListener     net.Listener
	peerListenerOnce   sync.Once
	peerListener       net.Listener
	clientHTTP         *http.Server
	peerHTTP           *http.Server

	// {client,peer}ListenerMaterialized latch true when the listener factory
	// has been invoked (even for a nil/headless or latched-config result, which
	// leaves the listener nil). Once set, the listener setters refuse — the
	// sync.Once is spent, so a later setter could change the advertised URLs
	// without the factory ever binding them.
	clientListenerMaterialized atomic.Bool
	peerListenerMaterialized   atomic.Bool

	// clusterSet records that the cluster membership has been pinned (by Join,
	// which joins an existing cluster). Until then, mutate auto-syncs
	// InitialCluster to a single-member string. Once pinned, Join owns it.
	clusterSet atomic.Bool

	// joinUserinfo/joinCredential carry discovery JWT-mode wiring, set by
	// joinViaDiscovery (both empty in plain shared-secret mode). joinUserinfo is
	// the seed's userinfo URL the peer-join handler delegates JWT verification
	// to; joinCredential is this node's own JWT — the credential it presents
	// when joining another member, distinct from the cluster token, which
	// discovery pins to the JWT's sub. Guarded by mu.
	joinUserinfo   string
	joinCredential string

	// serverOnce guards minting srv exactly once from the current srvcfg; a mint
	// failure is latched as the context cause.
	serverOnce sync.Once
	srv        *etcdserver.EtcdServer

	// startOnce/stopOnce make Start/Stop run at most once; started records
	// whether the server's run loop was started (so Stop knows HardStop vs
	// Cleanup).
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool

	// grpcOnce/grpcSrv and loopbackOnce/loopbackCli cache the gRPC server and the
	// in-process loopback client, each minted at most once.
	grpcOnce     sync.Once
	grpcSrv      *grpc.Server
	loopbackOnce sync.Once
	loopbackCli  *clientv3.Client
}

// New returns an EtcdImpl with default configuration and a unique generated
// member name. The root libetcd.New façade wraps this and returns it as the
// v1.Etcd interface.
//
// The builder starts from embed.NewConfig() — the minimum configuration that
// passes embed.Config.Validate() — and revalidates after every With* call.
func New() v1.Etcd {
	return newImpl()
}

// newImpl builds a concrete *EtcdImpl. New returns it as the full v1.Etcd
// surface; From wraps it as the join-only v1.EtcdPeer surface.
func newImpl() *EtcdImpl {
	ctx, cancel := context.WithCancelCause(context.Background())
	cfg := embed.NewConfig()
	cfg.Name = defaultName()

	// Opinionated defaults
	cfg.InitialElectionTickAdvance = false
	cfg.ElectionMs = 10000
	cfg.MaxLearners = math.MaxInt
	cfg.SnapshotCatchUpEntries = 100000
	cfg.SnapshotCount = 100000
	cfg.TickMs = 1000

	// Silent by default: install a no-op logger. WithLog replaces this builder to
	// route logs to a writer. Setting the builder (not just LogLevel) is what makes
	// the choice stick — setupLogging only auto-builds when ZapLoggerBuilder is nil.
	cfg.Logger = "zap"
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())

	// Cluster token from the environment, so a node (or a whole discovery
	// cluster) can be configured without code — e.g. LIBETCD_CLUSTER_TOKEN set to
	// a GitHub OIDC token in CI. An explicit WithClusterToken later overrides it.
	if tok := os.Getenv(ClusterTokenEnv); tok != "" {
		cfg.InitialClusterToken = tok
	}

	b := &EtcdImpl{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
	// Factory defaults: a free loopback port per side, bound lazily at
	// materialization (Start/Join) rather than construction, each served with
	// the default handler. A nil factory means "do nothing" for that piece;
	// the With*Listener setters install, replace, or nil these.
	b.clientListenerFactory = func() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }
	b.clientHTTPFactory = func() *http.Server { return &http.Server{Handler: b.ClientHandler()} }
	b.peerListenerFactory = func() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }
	b.peerHTTPFactory = func() *http.Server { return &http.Server{Handler: b.PeerHandler()} }
	// Validate the defaults and seed srvcfg, so a builder with no With* calls
	// can still mint a server from embed.NewConfig()'s baseline.
	b.mutate(func() error { return nil })
	return b
}

// From returns a join-only builder for a node that will join the cluster
// reachable at the given peer URLs. Configure it with the With* methods, then
// call Join; see peerJoiner.
//
// The arguments are unioned with the LIBETCD_PEERS environment variable
// (PeersEnv) — a comma-separated list or a JSON array of strings — so a node can
// be aimed at a cluster by environment alone. Join normalizes and de-duplicates
// the result; with no peers from either source, From()...Join() bootstraps.
func From(peers ...string) v1.EtcdPeer {
	// Union the arguments with the env peers before Join sees them. Copy first
	// so we never append into the caller's backing array (From(slice...) aliases
	// it). Raw strings — Join's sanitizePeers trims, default-schemes, and dedups.
	peers = append(append([]string(nil), peers...), envPeers(os.Getenv(PeersEnv))...)
	return &peerJoiner{EtcdImpl: newImpl(), peers: peers}
}
