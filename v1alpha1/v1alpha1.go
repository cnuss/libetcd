// Package v1alpha1 is the current implementation behind the v1 interfaces. The
// root libetcd façade wraps this; callers reaching directly into v1alpha1 use it
// for the concrete types. Anything here may change between alpha revisions —
// depend on the v1 contract, not these internals.
//
// The implementation is split by interface: v1alpha1.go (the EtcdImpl type and
// New), builder.go (the With* setters and the validate/derive machinery),
// accessor.go (the read-side Accessor handles), and executor.go (the Executor
// lifecycle: Start/Stop).
package v1alpha1

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"google.golang.org/grpc"

	v1 "github.com/cnuss/libetcd/v1"
)

// EtcdImpl implements the full Etcd surface (Accessor + Builder + Executor).
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
	// WithClientListener / WithPeerListener (nil until set), exposed via the
	// ClientListener / PeerListener accessors.
	clientListener net.Listener
	peerListener   net.Listener

	// clientHTTP/peerHTTP are the http.Servers returned by ClientHTTP/PeerHTTP:
	// the ones supplied via WithClientHTTP/WithPeerHTTP, or defaults created
	// (once each) if none were provided.
	clientHTTP     *http.Server
	clientHTTPOnce sync.Once
	peerHTTP       *http.Server
	peerHTTPOnce   sync.Once

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

// New returns an unconfigured EtcdImpl. The root libetcd.New façade wraps this
// and returns it as the v1.Builder interface.
//
// The builder starts from embed.NewConfig() — the minimum configuration that
// passes embed.Config.Validate() — and revalidates after every With* call.
func New() *EtcdImpl {
	ctx, cancel := context.WithCancelCause(context.Background())
	cfg := embed.NewConfig()
	cfg.LogLevel = "fatal" // quiet by default; override with WithLogLevel
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
