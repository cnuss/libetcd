package v3alpha7

import (
	"net/netip"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/featuregate"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
	"go.etcd.io/etcd/server/v3/features"

	v3 "github.com/cnuss/libetcd/v3"
)

// serverConfigImpl is the etcd 3.7 implementation of v3.ServerConfig. It
// accumulates fields into an underlying config.ServerConfig; Build returns
// a copy of the accumulated value.
type serverConfigImpl struct {
	cfgMu sync.Mutex
	cfg   config.ServerConfig
}

var _ v3.ServerConfig = (*serverConfigImpl)(nil)

// NewServerConfig returns an empty builder.
func newServerConfig() v3.ServerConfig {
	cfg := embed.NewConfig()
	cfg.LogLevel = "fatal"
	if cfg.Validate() != nil {
		panic("invalid default config")
	}

	// embed.StartEtcd generates the next three values from other embed.Config
	// fields instead of copying them; mirror that inline.
	urlsmap, token, err := cfg.PeerURLsMapAndToken("etcd")
	if err != nil {
		panic("invalid default initial cluster: " + err.Error())
	}

	// embed.parseCompactionRetention: bare integers are revision counts in
	// revision mode or hours in periodic mode; anything else is a duration.
	retention := cfg.AutoCompactionRetention
	if retention == "" {
		retention = "0"
	}
	var autoCompactionRetention time.Duration
	if h, herr := strconv.Atoi(retention); herr == nil && h >= 0 {
		switch cfg.AutoCompactionMode {
		case embed.CompactorModeRevision:
			autoCompactionRetention = time.Duration(int64(h))
		case embed.CompactorModePeriodic:
			autoCompactionRetention = time.Duration(int64(h)) * time.Hour
		}
	} else {
		if autoCompactionRetention, err = time.ParseDuration(retention); err != nil {
			panic("invalid default auto-compaction retention: " + err.Error())
		}
	}

	// embed.parseBackendFreelistType: everything but "array" is the map type.
	backendFreelistType := bolt.FreelistMapType
	if cfg.BackendFreelistType == "array" {
		backendFreelistType = bolt.FreelistArrayType
	}

	return &serverConfigImpl{cfg: config.ServerConfig{
		Name:                              cfg.Name,
		ClientURLs:                        cfg.AdvertiseClientUrls,
		PeerURLs:                          cfg.AdvertisePeerUrls,
		DataDir:                           cfg.Dir,
		DedicatedWALDir:                   cfg.WalDir,
		SnapshotCount:                     cfg.SnapshotCount,
		SnapshotCatchUpEntries:            cfg.SnapshotCatchUpEntries,
		MaxSnapFiles:                      cfg.MaxSnapFiles,
		MaxWALFiles:                       cfg.MaxWalFiles,
		InitialPeerURLsMap:                urlsmap,
		InitialClusterToken:               token,
		DiscoveryCfg:                      cfg.DiscoveryCfg,
		NewCluster:                        cfg.IsNewCluster(),
		PeerTLSInfo:                       cfg.PeerTLSInfo,
		TickMs:                            cfg.TickMs,
		ElectionTicks:                     cfg.ElectionTicks(),
		InitialElectionTickAdvance:        cfg.InitialElectionTickAdvance,
		AutoCompactionRetention:           autoCompactionRetention,
		AutoCompactionMode:                cfg.AutoCompactionMode,
		QuotaBackendBytes:                 cfg.QuotaBackendBytes,
		BackendBatchLimit:                 cfg.BackendBatchLimit,
		BackendFreelistType:               backendFreelistType,
		BackendBatchInterval:              cfg.BackendBatchInterval,
		MaxTxnOps:                         cfg.MaxTxnOps,
		MaxRequestBytes:                   cfg.MaxRequestBytes,
		MaxConcurrentStreams:              cfg.MaxConcurrentStreams,
		SocketOpts:                        cfg.SocketOpts,
		StrictReconfigCheck:               cfg.StrictReconfigCheck,
		ClientCertAuthEnabled:             cfg.ClientTLSInfo.ClientCertAuth,
		AuthToken:                         cfg.AuthToken,
		BcryptCost:                        cfg.BcryptCost,
		TokenTTL:                          cfg.AuthTokenTTL,
		CORS:                              cfg.CORS,
		HostWhitelist:                     cfg.HostWhitelist,
		CorruptCheckTime:                  cfg.CorruptCheckTime,
		CompactHashCheckTime:              cfg.CompactHashCheckTime,
		PreVote:                           cfg.PreVote,
		Logger:                            cfg.GetLogger(),
		ForceNewCluster:                   cfg.ForceNewCluster,
		EnableGRPCGateway:                 cfg.EnableGRPCGateway,
		EnableDistributedTracing:          cfg.EnableDistributedTracing,
		UnsafeNoFsync:                     cfg.UnsafeNoFsync,
		CompactionBatchLimit:              cfg.CompactionBatchLimit,
		CompactionSleepInterval:           cfg.CompactionSleepInterval,
		WatchProgressNotifyInterval:       cfg.WatchProgressNotifyInterval,
		DowngradeCheckTime:                cfg.DowngradeCheckTime,
		WarningApplyDuration:              cfg.WarningApplyDuration,
		WarningUnaryRequestDuration:       cfg.WarningUnaryRequestDuration,
		MemoryMlock:                       cfg.MemoryMlock,
		BootstrapDefragThresholdMegabytes: cfg.BootstrapDefragThresholdMegabytes,
		MaxLearners:                       cfg.MaxLearners,
		V2Deprecation:                     cfg.V2DeprecationEffective(),
		LocalAddress:                      cfg.InferLocalAddr(),
		ServerFeatureGate:                 cfg.ServerFeatureGate,
		Metrics:                           cfg.Metrics,
	}}
}

func (s *serverConfigImpl) WithName(name string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.Name = name
	return s
}

func (s *serverConfigImpl) WithDiscoveryCfg(cfg v3discovery.DiscoveryConfig) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.DiscoveryCfg = cfg
	return s
}

func (s *serverConfigImpl) WithClientURLs(urls types.URLs) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.ClientURLs = urls
	return s
}

func (s *serverConfigImpl) WithPeerURLs(urls types.URLs) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.PeerURLs = urls
	return s
}

func (s *serverConfigImpl) WithDataDir(dir string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.DataDir = dir
	return s
}

func (s *serverConfigImpl) WithDedicatedWALDir(dir string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.DedicatedWALDir = dir
	return s
}

func (s *serverConfigImpl) WithSnapshotCount(count uint64) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.SnapshotCount = count
	return s
}

func (s *serverConfigImpl) WithSnapshotCatchUpEntries(entries uint64) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.SnapshotCatchUpEntries = entries
	return s
}

func (s *serverConfigImpl) WithMaxSnapFiles(max uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MaxSnapFiles = max
	return s
}

func (s *serverConfigImpl) WithMaxWALFiles(max uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MaxWALFiles = max
	return s
}

func (s *serverConfigImpl) WithBackendBatchInterval(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.BackendBatchInterval = interval
	return s
}

func (s *serverConfigImpl) WithBackendBatchLimit(limit int) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.BackendBatchLimit = limit
	return s
}

func (s *serverConfigImpl) WithBackendFreelistType(t bolt.FreelistType) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.BackendFreelistType = t
	return s
}

func (s *serverConfigImpl) WithInitialPeerURLsMap(urlsMap types.URLsMap) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.InitialPeerURLsMap = urlsMap
	return s
}

func (s *serverConfigImpl) WithInitialClusterToken(token string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.InitialClusterToken = token
	return s
}

func (s *serverConfigImpl) WithNewCluster(newCluster bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.NewCluster = newCluster
	return s
}

func (s *serverConfigImpl) WithPeerTLSInfo(info transport.TLSInfo) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.PeerTLSInfo = info
	return s
}

func (s *serverConfigImpl) WithCORS(cors map[string]struct{}) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.CORS = cors
	return s
}

func (s *serverConfigImpl) WithHostWhitelist(whitelist map[string]struct{}) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.HostWhitelist = whitelist
	return s
}

func (s *serverConfigImpl) WithTickMs(tickMs uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.TickMs = tickMs
	return s
}

func (s *serverConfigImpl) WithElectionTicks(ticks int) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.ElectionTicks = ticks
	return s
}

func (s *serverConfigImpl) WithInitialElectionTickAdvance(advance bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.InitialElectionTickAdvance = advance
	return s
}

func (s *serverConfigImpl) WithBootstrapTimeout(timeout time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.BootstrapTimeout = timeout
	return s
}

func (s *serverConfigImpl) WithAutoCompactionRetention(retention time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.AutoCompactionRetention = retention
	return s
}

func (s *serverConfigImpl) WithAutoCompactionMode(mode string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.AutoCompactionMode = mode
	return s
}

func (s *serverConfigImpl) WithCompactionBatchLimit(limit int) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.CompactionBatchLimit = limit
	return s
}

func (s *serverConfigImpl) WithCompactionSleepInterval(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.CompactionSleepInterval = interval
	return s
}

func (s *serverConfigImpl) WithQuotaBackendBytes(quota int64) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.QuotaBackendBytes = quota
	return s
}

func (s *serverConfigImpl) WithMaxTxnOps(max uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MaxTxnOps = max
	return s
}

func (s *serverConfigImpl) WithMaxRequestBytes(max uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MaxRequestBytes = max
	return s
}

func (s *serverConfigImpl) WithMaxConcurrentStreams(max uint32) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MaxConcurrentStreams = max
	return s
}

func (s *serverConfigImpl) WithWarningApplyDuration(duration time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.WarningApplyDuration = duration
	return s
}

func (s *serverConfigImpl) WithWarningUnaryRequestDuration(duration time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.WarningUnaryRequestDuration = duration
	return s
}

func (s *serverConfigImpl) WithStrictReconfigCheck(strict bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.StrictReconfigCheck = strict
	return s
}

func (s *serverConfigImpl) WithClientCertAuthEnabled(enabled bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.ClientCertAuthEnabled = enabled
	return s
}

func (s *serverConfigImpl) WithAuthToken(token string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.AuthToken = token
	return s
}

func (s *serverConfigImpl) WithBcryptCost(cost uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.BcryptCost = cost
	return s
}

func (s *serverConfigImpl) WithTokenTTL(ttl uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.TokenTTL = ttl
	return s
}

func (s *serverConfigImpl) WithInitialCorruptCheck(check bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.InitialCorruptCheck = check
	return s
}

func (s *serverConfigImpl) WithCorruptCheckTime(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.CorruptCheckTime = interval
	return s
}

func (s *serverConfigImpl) WithCompactHashCheckTime(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.CompactHashCheckTime = interval
	return s
}

func (s *serverConfigImpl) WithPreVote(preVote bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.PreVote = preVote
	return s
}

func (s *serverConfigImpl) WithSocketOpts(opts transport.SocketOpts) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.SocketOpts = opts
	return s
}

func (s *serverConfigImpl) WithLogger(logger *zap.Logger) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.Logger = logger
	return s
}

func (s *serverConfigImpl) WithForceNewCluster(force bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.ForceNewCluster = force
	return s
}

func (s *serverConfigImpl) WithLeaseCheckpointInterval(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.LeaseCheckpointInterval = interval
	return s
}

func (s *serverConfigImpl) WithEnableGRPCGateway(enable bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.EnableGRPCGateway = enable
	return s
}

func (s *serverConfigImpl) WithEnableDistributedTracing(enable bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.EnableDistributedTracing = enable
	return s
}

func (s *serverConfigImpl) WithTracerOptions(opts []otelgrpc.Option) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.TracerOptions = opts
	return s
}

func (s *serverConfigImpl) WithWatchProgressNotifyInterval(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.WatchProgressNotifyInterval = interval
	return s
}

func (s *serverConfigImpl) WithUnsafeNoFsync(noFsync bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.UnsafeNoFsync = noFsync
	return s
}

func (s *serverConfigImpl) WithDowngradeCheckTime(interval time.Duration) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.DowngradeCheckTime = interval
	return s
}

func (s *serverConfigImpl) WithMemoryMlock(mlock bool) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MemoryMlock = mlock
	return s
}

func (s *serverConfigImpl) WithBootstrapDefragThresholdMegabytes(threshold uint) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.BootstrapDefragThresholdMegabytes = threshold
	return s
}

func (s *serverConfigImpl) WithMaxLearners(max int) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.MaxLearners = max
	return s
}

func (s *serverConfigImpl) WithV2Deprecation(phase config.V2DeprecationEnum) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.V2Deprecation = phase
	return s
}

func (s *serverConfigImpl) WithLocalAddress(address string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.LocalAddress = address
	return s
}

func (s *serverConfigImpl) WithServerFeatureGate(gate featuregate.FeatureGate) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.ServerFeatureGate = gate
	return s
}

func (s *serverConfigImpl) WithMetrics(metrics string) v3.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.Metrics = metrics
	return s
}

func (s *serverConfigImpl) Build() config.ServerConfig {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	cfg := s.cfg

	// The fields below derive from final field values, so they belong here
	// rather than in the constructor.

	// etcdmain convention: unset data dir becomes "<name>.etcd". Deriving at
	// build time keeps it coupled to WithName.
	if cfg.DataDir == "" {
		cfg.DataDir = cfg.Name + ".etcd"
	}

	// Config.InferLocalAddr: first non-loopback, non-unspecified IP among the
	// advertised peer URLs, gated on the SetMemberLocalAddr feature.
	if cfg.LocalAddress == "" && cfg.ServerFeatureGate != nil && cfg.ServerFeatureGate.Enabled(features.SetMemberLocalAddr) {
		for _, u := range cfg.PeerURLs {
			if addr, err := netip.ParseAddr(u.Hostname()); err == nil && !addr.IsLoopback() && !addr.IsUnspecified() {
				cfg.LocalAddress = addr.String()
				break
			}
		}
	}
	cfg.PeerTLSInfo.LocalAddr = cfg.LocalAddress

	return cfg
}
