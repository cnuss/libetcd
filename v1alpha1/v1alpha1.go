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

	// clientListener and peerListener retain the listeners passed to
	// WithClientServing / WithPeerServing (nil until set), exposed via the
	// ClientListener / PeerListener accessors.
	clientListener net.Listener
	peerListener   net.Listener

	// clientServingOff/peerServingOff record the WithoutClientServing /
	// WithoutPeerServing opt-outs. ensureListeners skips auto-binding an
	// opted-out side (and with no listener, Start serves nothing on it). Guarded
	// by mu; a later With*Serving with a non-nil listener clears the flag.
	clientServingOff bool
	peerServingOff   bool

	// clientHTTP/peerHTTP are the http.Servers returned by ClientHTTP/PeerHTTP:
	// the ones supplied via WithClientServing/WithPeerServing, or defaults created
	// (once each) if none were provided.
	clientHTTP     *http.Server
	clientHTTPOnce sync.Once
	peerHTTP       *http.Server
	peerHTTPOnce   sync.Once

	// clusterSet records that the cluster membership has been pinned (by Join,
	// which joins an existing cluster). Until then, mutate auto-syncs
	// InitialCluster to a single-member string. Once pinned, Join owns it.
	clusterSet atomic.Bool

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

	b := &EtcdImpl{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
	// Validate the defaults and seed srvcfg, so a builder with no With* calls
	// can still mint a server from embed.NewConfig()'s baseline.
	b.mutate(func() error { return nil })
	return b
}

// From returns a join-only builder for a node that will join the cluster
// reachable at the given peer URLs. Configure it with the With* methods, then
// call Join; see peerJoiner.
func From(peers ...string) v1.EtcdPeer {
	return &peerJoiner{EtcdImpl: newImpl(), peers: peers}
}
