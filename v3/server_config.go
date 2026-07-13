package v3

import (
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/featuregate"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
)

// ServerConfig is a fluent builder over etcd's config.ServerConfig. Each
// With* method sets the field of the same name and returns the builder for
// chaining; Build materializes the accumulated configuration.
type ServerConfig interface {
	// WithName sets the node (member) name.
	WithName(name string) ServerConfig
	// WithDiscoveryCfg sets the v3 discovery configuration.
	WithDiscoveryCfg(cfg v3discovery.DiscoveryConfig) ServerConfig
	// WithClientURLs sets the advertised client URLs.
	WithClientURLs(urls types.URLs) ServerConfig
	// WithPeerURLs sets the advertised peer URLs.
	WithPeerURLs(urls types.URLs) ServerConfig
	// WithDataDir sets the data directory.
	WithDataDir(dir string) ServerConfig
	// WithDedicatedWALDir makes etcd write the WAL to this directory rather
	// than dataDir/member/wal.
	WithDedicatedWALDir(dir string) ServerConfig
	// WithSnapshotCount sets the number of committed transactions to trigger
	// a snapshot to disk.
	WithSnapshotCount(count uint64) ServerConfig
	// WithSnapshotCatchUpEntries sets the number of entries for a slow
	// follower to catch up after compacting the raft storage entries.
	WithSnapshotCatchUpEntries(entries uint64) ServerConfig
	// WithMaxSnapFiles sets the maximum number of snapshot files to retain.
	WithMaxSnapFiles(max uint) ServerConfig
	// WithMaxWALFiles sets the maximum number of WAL files to retain.
	WithMaxWALFiles(max uint) ServerConfig
	// WithBackendBatchInterval sets the maximum time before committing the
	// backend transaction.
	WithBackendBatchInterval(interval time.Duration) ServerConfig
	// WithBackendBatchLimit sets the maximum operations before committing
	// the backend transaction.
	WithBackendBatchLimit(limit int) ServerConfig
	// WithBackendFreelistType sets the type of the backend boltdb freelist.
	WithBackendFreelistType(t bolt.FreelistType) ServerConfig
	// WithInitialPeerURLsMap sets the initial cluster name-to-peer-URLs map.
	WithInitialPeerURLsMap(urlsMap types.URLsMap) ServerConfig
	// WithInitialClusterToken sets the initial cluster token.
	WithInitialClusterToken(token string) ServerConfig
	// WithNewCluster marks whether this member bootstraps a new cluster.
	WithNewCluster(newCluster bool) ServerConfig
	// WithPeerTLSInfo sets the TLS configuration for peer traffic.
	WithPeerTLSInfo(info transport.TLSInfo) ServerConfig
	// WithCORS sets the set of allowed CORS origins.
	WithCORS(cors map[string]struct{}) ServerConfig
	// WithHostWhitelist sets acceptable hostnames from client requests when
	// the server is insecure (no TLS).
	WithHostWhitelist(whitelist map[string]struct{}) ServerConfig
	// WithTickMs sets the heartbeat tick interval in milliseconds.
	WithTickMs(tickMs uint) ServerConfig
	// WithElectionTicks sets the number of ticks before an election fires.
	WithElectionTicks(ticks int) ServerConfig
	// WithInitialElectionTickAdvance fast-forwards election ticks on boot to
	// speed up initial leader election.
	WithInitialElectionTickAdvance(advance bool) ServerConfig
	// WithBootstrapTimeout sets the bootstrap timeout.
	WithBootstrapTimeout(timeout time.Duration) ServerConfig
	// WithAutoCompactionRetention sets the auto-compaction retention window.
	WithAutoCompactionRetention(retention time.Duration) ServerConfig
	// WithAutoCompactionMode sets the auto-compaction mode ("periodic" or
	// "revision").
	WithAutoCompactionMode(mode string) ServerConfig
	// WithCompactionBatchLimit sets the maximum revisions deleted per
	// compaction batch.
	WithCompactionBatchLimit(limit int) ServerConfig
	// WithCompactionSleepInterval sets the sleep interval between compaction
	// batches.
	WithCompactionSleepInterval(interval time.Duration) ServerConfig
	// WithQuotaBackendBytes sets the backend storage quota in bytes.
	WithQuotaBackendBytes(quota int64) ServerConfig
	// WithMaxTxnOps sets the maximum number of operations per transaction.
	WithMaxTxnOps(max uint) ServerConfig
	// WithMaxRequestBytes sets the maximum request size to send over raft.
	WithMaxRequestBytes(max uint) ServerConfig
	// WithMaxConcurrentStreams sets the maximum number of concurrent streams
	// each client can open at a time.
	WithMaxConcurrentStreams(max uint32) ServerConfig
	// WithWarningApplyDuration sets the apply-duration threshold that
	// triggers a warning log.
	WithWarningApplyDuration(duration time.Duration) ServerConfig
	// WithWarningUnaryRequestDuration sets the unary-request duration
	// threshold that triggers a warning log.
	WithWarningUnaryRequestDuration(duration time.Duration) ServerConfig
	// WithStrictReconfigCheck rejects reconfiguration requests that would
	// cause quorum loss.
	WithStrictReconfigCheck(strict bool) ServerConfig
	// WithClientCertAuthEnabled marks that client certs are signed by the
	// client CA.
	WithClientCertAuthEnabled(enabled bool) ServerConfig
	// WithAuthToken sets the auth token spec (e.g. "simple" or "jwt,...").
	WithAuthToken(token string) ServerConfig
	// WithBcryptCost sets the bcrypt cost for hashing auth passwords.
	WithBcryptCost(cost uint) ServerConfig
	// WithTokenTTL sets the simple-token TTL in seconds.
	WithTokenTTL(ttl uint) ServerConfig
	// WithInitialCorruptCheck checks data corruption on boot before serving
	// any peer/client traffic.
	WithInitialCorruptCheck(check bool) ServerConfig
	// WithCorruptCheckTime sets the interval between periodic corruption
	// checks.
	WithCorruptCheckTime(interval time.Duration) ServerConfig
	// WithCompactHashCheckTime sets the interval between periodic compact
	// hash checks.
	WithCompactHashCheckTime(interval time.Duration) ServerConfig
	// WithPreVote enables Raft Pre-Vote.
	WithPreVote(preVote bool) ServerConfig
	// WithSocketOpts sets socket options passed to listener config.
	WithSocketOpts(opts transport.SocketOpts) ServerConfig
	// WithLogger sets the logger for server-side operations.
	WithLogger(logger *zap.Logger) ServerConfig
	// WithForceNewCluster forces the creation of a new one-member cluster
	// from existing data.
	WithForceNewCluster(force bool) ServerConfig
	// WithLeaseCheckpointInterval sets the wait duration between lease
	// checkpoints.
	WithLeaseCheckpointInterval(interval time.Duration) ServerConfig
	// WithEnableGRPCGateway enables the gRPC gateway (REST proxy).
	WithEnableGRPCGateway(enable bool) ServerConfig
	// WithEnableDistributedTracing enables distributed tracing using the
	// OpenTelemetry protocol.
	WithEnableDistributedTracing(enable bool) ServerConfig
	// WithTracerOptions sets options for the OpenTelemetry gRPC interceptor.
	WithTracerOptions(opts []otelgrpc.Option) ServerConfig
	// WithWatchProgressNotifyInterval sets the interval between watch
	// progress notifications.
	WithWatchProgressNotifyInterval(interval time.Duration) ServerConfig
	// WithUnsafeNoFsync disables all uses of fsync. Setting this is unsafe
	// and will cause data loss.
	WithUnsafeNoFsync(noFsync bool) ServerConfig
	// WithDowngradeCheckTime sets the interval between downgrade status
	// checks.
	WithDowngradeCheckTime(interval time.Duration) ServerConfig
	// WithMemoryMlock enables mlocking of etcd owned memory pages.
	WithMemoryMlock(mlock bool) ServerConfig
	// WithBootstrapDefragThresholdMegabytes sets the minimum number of
	// megabytes needed to be freed for etcd to run defrag during bootstrap.
	WithBootstrapDefragThresholdMegabytes(threshold uint) ServerConfig
	// WithMaxLearners limits the number of learner members in the cluster.
	WithMaxLearners(max int) ServerConfig
	// WithV2Deprecation sets the phase of the v2store deprecation process.
	WithV2Deprecation(phase config.V2DeprecationEnum) ServerConfig
	// WithLocalAddress sets the local IP address to use when communicating
	// with a peer.
	WithLocalAddress(address string) ServerConfig
	// WithServerFeatureGate sets the server-level feature gate.
	WithServerFeatureGate(gate featuregate.FeatureGate) ServerConfig
	// WithMetrics sets the metrics level, either "basic" or "extensive".
	WithMetrics(metrics string) ServerConfig

	// Build returns the accumulated configuration.
	Build() config.ServerConfig
}
