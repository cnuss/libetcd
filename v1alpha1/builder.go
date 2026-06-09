package v1alpha1

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"

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

// WithClientListener sets the client URL from a net.Listener's address and
// retains the listener (see ClientListener).
func (b *EtcdImpl) WithClientListener(l net.Listener) v1.Etcd {
	b.mutate(func() error {
		u := listenerURL(l)
		b.cfg.ListenClientUrls = []url.URL{u}
		b.cfg.AdvertiseClientUrls = []url.URL{u}
		b.clientListener = l
		return nil
	})
	return b
}

// WithPeerListener sets the peer URL from a net.Listener's address and retains
// the listener (see PeerListener).
func (b *EtcdImpl) WithPeerListener(l net.Listener) v1.Etcd {
	b.mutate(func() error {
		u := listenerURL(l)
		b.cfg.ListenPeerUrls = []url.URL{u}
		b.cfg.AdvertisePeerUrls = []url.URL{u}
		b.peerListener = l
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

// WithClientHTTP supplies the http.Server for the client (v3 API) listener.
func (b *EtcdImpl) WithClientHTTP(srv *http.Server) v1.Etcd {
	b.mutate(func() error { b.clientHTTP = srv; return nil })
	return b
}

// WithPeerHTTP supplies the http.Server for the peer (raft) listener.
func (b *EtcdImpl) WithPeerHTTP(srv *http.Server) v1.Etcd {
	b.mutate(func() error { b.peerHTTP = srv; return nil })
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
	// changed name or peer URL doesn't break minting. Join pins the cluster
	// (clusterSet) for a multi-member join and takes over InitialCluster.
	if !b.clusterSet.Load() && len(b.cfg.AdvertisePeerUrls) > 0 {
		b.cfg.InitialCluster = b.cfg.Name + "=" + b.cfg.AdvertisePeerUrls[0].String()
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
	return config.ServerConfig{
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
		DiscoveryURL:                      b.cfg.Durl,
		DiscoveryProxy:                    b.cfg.Dproxy,
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
		ExperimentalLocalAddress:          b.cfg.InferLocalAddr(),
		ServerFeatureGate:                 b.cfg.ServerFeatureGate,
		Metrics:                           b.cfg.Metrics,
	}, nil
}
