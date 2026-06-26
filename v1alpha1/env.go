package v1alpha1

import (
	"os"
	"path"
)

// Environment variables libetcd reads. Each lets a node (or a whole discovery
// cluster) be configured without code — set in the process environment, unioned
// with or overridden by the equivalent With* call. Names are LIBETCD_-prefixed
// to namespace them; the consts are the contract, not the raw strings.
const (
	// PeersEnv is the environment variable Join unions into its peer list, on
	// top of From's arguments: a comma-separated list or a JSON array of
	// strings.
	PeersEnv = "LIBETCD_PEERS"

	// ClusterTokenEnv sets the cluster token (WithClusterToken) from the
	// environment — e.g. a GitHub OIDC token for the discovery join. Applied at
	// construction; an explicit WithClusterToken overrides it.
	ClusterTokenEnv = "LIBETCD_CLUSTER_TOKEN"

	// DataDirEnv sets the data directory (WithDir) from the environment — so a
	// node can be pointed at a persistent dir (a mounted volume, EFS) without
	// code, which is what lets a crashed node resume over its surviving WAL.
	// Applied at construction; an explicit WithDir overrides it.
	DataDirEnv = "LIBETCD_DATA_DIR"
)

var (
	DefaultDataDir = func() string {
		p := func() string {
			if d := os.Getenv(DataDirEnv); d != "" {
				return d
			}
			if d, err := os.UserCacheDir(); err == nil {
				return path.Join(d, ".libetcd")
			}
			if d, err := os.UserHomeDir(); err == nil {
				return path.Join(d, ".libetcd")
			}
			if d, err := os.MkdirTemp(os.TempDir(), "libetcd-"); err == nil {
				return d
			}
			return path.Join(os.TempDir(), ".libetcd")
		}()
		// Ensure it exists (MkdirTemp already did; the env/cache/home arms may
		// not have). Ignore the error — server.go surfaces a real open failure
		// when it mints the backend.
		if _, err := os.Stat(p); err != nil {
			_ = os.MkdirAll(p, 0o700)
		}
		return p
	}()
)
