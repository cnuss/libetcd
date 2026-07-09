package v1alpha1

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	v1 "github.com/cnuss/libetcd/v1"
)

// WithName sets the node (member) name.
func (b *EtcdImpl) WithName(name string) v1.Etcd {
	b.mutate(func() error { b.cfg.Name = name; return nil })
	return b
}

// WithDir sets the data directory.
func (b *EtcdImpl) WithDir(dir string) v1.Etcd {
	b.mutate(func() error { b.cfg.Dir = dir; return nil })
	return b
}

// WithClientListener sets the socket the client (v3 API) is served on; the
// listener is the only serving knob (libetcd serves everything it binds).
//
//   - Non-nil lis: the client listen+advertise URLs derive from its address
//     (https if TLS-wrapped); the factory will hand it out at materialization.
//   - Nil: headless client side — the listener and http factories are cleared
//     ("do nothing"), no socket is bound, nothing is served, and no client
//     URLs are registered with the cluster. Self() still works in-process.
//
// Last call wins, but only until the listener has been materialized (the
// first ClientListener() call, typically inside Start/Join): after that a
// changed listener can't reach the running config, so the call latches an
// error instead of lying.
func (b *EtcdImpl) WithClientListener(lis net.Listener) v1.Etcd {
	b.mutate(func() error {
		if b.clientListenerMaterialized.Load() {
			return fmt.Errorf("client listener already materialized; configure before Start/Join")
		}
		if lis == nil {
			b.clientListenerFactory = nil
			b.clientHTTPFactory = nil
			b.cfg.ListenClientUrls = nil
			b.cfg.AdvertiseClientUrls = nil
			return nil
		}
		b.clientListenerFactory = func() (net.Listener, error) { return lis, nil }
		u := listenerURL(lis)
		b.cfg.ListenClientUrls = []url.URL{u}
		b.cfg.AdvertiseClientUrls = []url.URL{u}
		return nil
	})
	return b
}

// WithClusterToken sets the initial-cluster token.
func (b *EtcdImpl) WithClusterToken(token string) v1.Etcd {
	b.mutate(func() error { b.cfg.InitialClusterToken = token; return nil })
	return b
}

// WithLog routes the server's logs to writer at the given level (e.g. "debug",
// "info", "warn", "error"). By default a node is silent (a no-op logger); call
// WithLog to see etcd's internal logging, e.g. WithLog("debug", os.Stderr).
//
// It installs a fresh zap logger and replaces the config's ZapLoggerBuilder, so
// it takes effect even after New() has seeded the default (silent) logger —
// unlike setting LogLevel alone, which setupLogging ignores once a builder is set.
func (b *EtcdImpl) WithLog(level string, writer io.Writer) v1.Etcd {
	b.mutate(func() error {
		lg, err := buildLogger(level, writer)
		if err != nil {
			return err
		}
		b.cfg.Logger = "zap"
		b.cfg.LogLevel = level
		b.cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(lg)
		return nil
	})
	return b
}

// buildLogger constructs a zap logger writing to w at the given level. A nil
// writer discards output. An unrecognized level is an error (latched as the
// config cause), keeping the no-panic builder contract.
func buildLogger(level string, w io.Writer) (*zap.Logger, error) {
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	if w == nil {
		w = io.Discard
	}
	enc := zapcore.NewConsoleEncoder(logutil.DefaultZapLoggerConfig.EncoderConfig)
	return zap.New(zapcore.NewCore(enc, zapcore.AddSync(w), lvl)), nil
}

// WithContext ties the node's lifetime to ctx: Start arranges a graceful Stop
// when ctx is cancelled.
func (b *EtcdImpl) WithContext(ctx context.Context) v1.Etcd {
	b.mutate(func() error { b.userCtx = ctx; return nil })
	return b
}

// WithPeerListener sets the socket the peer (raft) protocol is served on, and
// optionally the URLs to advertise for it.
//
//   - Non-nil lis: libetcd serves the peer protocol on lis. The factory hands
//     it out at materialization.
//
//   - advertiseURLs given: those are advertised to the cluster (what other
//     members dial) while libetcd still serves lis — the proxy/LB/tunnel case
//     where the advertised address differs from the bound socket. Unparseable
//     entries are dropped; if none parse, the listener's address is advertised
//     as a fallback (with a warning).
//
//   - advertiseURLs omitted: the peer listen+advertise URLs both derive from
//     the listener's address (https if TLS-wrapped).
//
//   - Nil lis + advertiseURLs given: BYO peer serving. libetcd binds and
//     serves nothing on the peer side; something else serves PeerHandler()
//     (over PeerPaths()) at the advertised URLs — a custom mux, a
//     shared/already-running server, a transport libetcd doesn't manage —
//     which is what the cluster dials. PeerListener() returns nil. libetcd
//     still drives raft membership and promotion; the caller owns the peer
//     HTTP server. The peer side can't go fully dark (raft must be reachable),
//     so nil means delegate serving, not turn off — unlike
//     WithClientListener(nil).
//
//     Mount PeerHandler() only after Start/Join returns: it mints the server,
//     so calling it earlier mints prematurely (Join then rejects the handle,
//     and the join's snapshot seed can't run over an already-booted dir). A
//     joining node needs no inbound raft during Join — the snapshot seed boots
//     it caught up and it dials out to promote — so serving after Join is in
//     time for steady-state replication. Symmetrically, stop serving (close
//     your http.Server) before Stop: Stop closes the etcd backend, and a peer
//     request that lands on the still-mounted PeerHandler afterwards
//     dereferences the closed backend and panics inside etcd's handler.
//
//   - Nil lis + no advertiseURLs: a configuration error, latched — with
//     nothing to bind and nothing to advertise, a raft member has no peer
//     address at all.
//
// Last call wins, but only until the listener has been materialized (the first
// PeerListener() call, typically inside Start/Join): after that a changed
// listener can't reach the running config, so the call latches an error.
func (b *EtcdImpl) WithPeerListener(lis net.Listener, advertiseURLs ...string) v1.Etcd {
	b.mutate(func() error {
		if b.peerListenerMaterialized.Load() {
			return fmt.Errorf("peer listener already materialized; configure before Start/Join")
		}
		// b.cfg.GetLogger() (not b.Logger(), which would re-lock b.mu while
		// mutate already holds it) — parse runs under the lock.
		adv := parseAdvertiseURLs(advertiseURLs, b.cfg.GetLogger())
		if lis == nil {
			// BYO peer serving: libetcd binds and serves nothing on the peer
			// side; something else serves PeerHandler() at the advertised URLs,
			// which is what the cluster dials. Clear the factories ("do
			// nothing", as WithClientListener(nil) does) and set the peer URLs
			// directly from the advertise override, since there's no listener
			// to derive them from. With no advertise URLs there's nothing to
			// bind and nothing to advertise — a raft member must do one.
			if len(adv) == 0 {
				return fmt.Errorf("peer listener cannot be nil without advertise URLs: a raft member must serve a peer socket or advertise where its peer protocol is served")
			}
			b.peerListenerFactory = nil
			b.peerHTTPFactory = nil
			b.peerAdvertise = adv
			b.cfg.AdvertisePeerUrls = adv
			// ListenPeerUrls is the bind set; etcd's checkBindURLs rejects a
			// hostname there (only IP/localhost), and the advertise URLs are
			// typically hostnames. libetcd binds nothing in BYO mode (the
			// caller serves PeerHandler), so this is inert metadata that just
			// has to pass validation — hardcode a loopback placeholder.
			b.cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}}
			return nil
		}
		b.peerListenerFactory = func() (net.Listener, error) { return lis, nil }
		b.peerAdvertise = adv
		b.applyPeerURLs(lis)
		return nil
	})
	return b
}

// mutate applies f to the config under the lock, then revalidates and
// regenerates the derived ServerConfig. Once the builder context has been
// cancelled (a prior failure), further mutations are no-ops so the first error
// is the one the accessors report. Any failure — from f, from Validate, or from
// deriving the ServerConfig — is latched as the context cause.
func (b *EtcdImpl) mutate(f func() error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.ctx.Err() != nil {
		return
	}
	if err := f(); err != nil {
		b.cancel(err)
		return
	}
	// Single-member auto-sync: keep InitialCluster pointing at this node so a
	// changed name or peer URL doesn't break minting. It must list *every*
	// advertise peer URL (not just the first) — etcd's VerifyBootstrap requires
	// this node's initial-cluster URL set to equal its advertise-peer-urls set,
	// so a member advertising several (e.g. multiple tunnels) needs them all
	// here. Join pins the cluster (clusterSet) for a multi-member join and takes
	// over InitialCluster.
	if !b.clusterSet.Load() && len(b.cfg.AdvertisePeerUrls) > 0 {
		parts := make([]string, len(b.cfg.AdvertisePeerUrls))
		for i, u := range b.cfg.AdvertisePeerUrls {
			parts[i] = b.cfg.Name + "=" + u.String()
		}
		b.cfg.InitialCluster = strings.Join(parts, ",")
	}
	if err := b.validate(); err != nil {
		b.cancel(err)
		return
	}
	srvcfg, err := b.serverConfig()
	if err != nil {
		b.cancel(err)
		return
	}
	b.srvcfg = srvcfg
}

// validate runs embed.Config.Validate, recovering any panic into an error.
// Validate panics rather than returning an error on some bad inputs (e.g. an
// unknown log level, which etcd's logutil.ConvertToZapLevel panics on), so the
// recover keeps the builder's no-panic, latch-the-cause contract intact.
func (b *EtcdImpl) validate() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("config validation panicked: %v", r)
		}
	}()
	return b.cfg.Validate()
}

// serverConfig derives a config.ServerConfig from the current embed.Config,
// mirroring what embed.StartEtcd builds internally. A few fields are
// intentionally omitted because they depend on unexported embed helpers:
// AutoCompactionRetention and BackendFreelistType (parsed by unexported funcs)
// and TracerOptions. Logger is taken from cfg.GetLogger(), which Validate's
// setupLogging populates.
func (b *EtcdImpl) serverConfig() (config.ServerConfig, error) {
	urlsmap, token, err := b.cfg.PeerURLsMapAndToken("etcd")
	if err != nil {
		return config.ServerConfig{}, fmt.Errorf("initial cluster: %w", err)
	}
	srvcfg := config.ServerConfig{
		// GetLogger returns the zap logger that Validate's setupLogging wired up;
		// etcdserver.NewServer panics on a nil Logger.
		Logger:                            b.cfg.GetLogger(),
		Name:                              b.cfg.Name,
		ClientURLs:                        b.cfg.AdvertiseClientUrls,
		PeerURLs:                          b.cfg.AdvertisePeerUrls,
		DataDir:                           b.cfg.Dir,
		DedicatedWALDir:                   b.cfg.WalDir,
		SnapshotCount:                     b.cfg.SnapshotCount,
		SnapshotCatchUpEntries:            b.cfg.SnapshotCatchUpEntries,
		MaxSnapFiles:                      b.cfg.MaxSnapFiles,
		MaxWALFiles:                       b.cfg.MaxWalFiles,
		InitialPeerURLsMap:                urlsmap,
		InitialClusterToken:               token,
		DiscoveryCfg:                      b.cfg.DiscoveryCfg,
		NewCluster:                        b.cfg.IsNewCluster(),
		PeerTLSInfo:                       b.cfg.PeerTLSInfo,
		TickMs:                            b.cfg.TickMs,
		ElectionTicks:                     b.cfg.ElectionTicks(),
		InitialElectionTickAdvance:        b.cfg.InitialElectionTickAdvance,
		AutoCompactionMode:                b.cfg.AutoCompactionMode,
		QuotaBackendBytes:                 b.cfg.QuotaBackendBytes,
		BackendBatchLimit:                 b.cfg.BackendBatchLimit,
		BackendBatchInterval:              b.cfg.BackendBatchInterval,
		MaxTxnOps:                         b.cfg.MaxTxnOps,
		MaxRequestBytes:                   b.cfg.MaxRequestBytes,
		MaxConcurrentStreams:              b.cfg.MaxConcurrentStreams,
		SocketOpts:                        b.cfg.SocketOpts,
		StrictReconfigCheck:               b.cfg.StrictReconfigCheck,
		ClientCertAuthEnabled:             b.cfg.ClientTLSInfo.ClientCertAuth,
		AuthToken:                         b.cfg.AuthToken,
		BcryptCost:                        b.cfg.BcryptCost,
		TokenTTL:                          b.cfg.AuthTokenTTL,
		CORS:                              b.cfg.CORS,
		HostWhitelist:                     b.cfg.HostWhitelist,
		CorruptCheckTime:                  b.cfg.CorruptCheckTime,
		CompactHashCheckTime:              b.cfg.CompactHashCheckTime,
		PreVote:                           b.cfg.PreVote,
		ForceNewCluster:                   b.cfg.ForceNewCluster,
		EnableGRPCGateway:                 b.cfg.EnableGRPCGateway,
		EnableDistributedTracing:          b.cfg.EnableDistributedTracing,
		UnsafeNoFsync:                     b.cfg.UnsafeNoFsync,
		CompactionBatchLimit:              b.cfg.CompactionBatchLimit,
		CompactionSleepInterval:           b.cfg.CompactionSleepInterval,
		WatchProgressNotifyInterval:       b.cfg.WatchProgressNotifyInterval,
		DowngradeCheckTime:                b.cfg.DowngradeCheckTime,
		WarningApplyDuration:              b.cfg.WarningApplyDuration,
		WarningUnaryRequestDuration:       b.cfg.WarningUnaryRequestDuration,
		MemoryMlock:                       b.cfg.MemoryMlock,
		BootstrapDefragThresholdMegabytes: b.cfg.BootstrapDefragThresholdMegabytes,
		MaxLearners:                       b.cfg.MaxLearners,
		V2Deprecation:                     b.cfg.V2DeprecationEffective(),
		LocalAddress:                      b.cfg.InferLocalAddr(),
		ServerFeatureGate:                 b.cfg.ServerFeatureGate,
		Metrics:                           b.cfg.Metrics,
	}
	srvcfg.PeerTLSInfo.LocalAddr = srvcfg.LocalAddress
	return srvcfg, nil
}
