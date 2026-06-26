package e2e

// Environment variables the e2e suite reads to gate and parameterize itself.
// These are test-harness knobs (set by CI or a local run), distinct from the
// library's own LIBETCD_* contract in v1alpha1 — the fleet also reads
// v1alpha1.ClusterTokenEnv (LIBETCD_CLUSTER_TOKEN) and the GitHub-provided
// GITHUB_RUN_ID, which are not redeclared here.
const (
	// E2EEnv force-enables the e2e suite on a CI cell that otherwise skips it
	// (the suite runs on only a few variants); set to "1".
	E2EEnv = "LIBETCD_E2E"

	// FleetEnv gates the fleet acceptance test (TestDiscoveryFleet); set to "1"
	// alongside NodeCountEnv and LIBETCD_CLUSTER_TOKEN by .github/workflows/e2e.yml.
	FleetEnv = "LIBETCD_E2E_FLEET"

	// NodeCountEnv tells a fleet node the cluster size N. Interim — N will later
	// be derived a different way (e.g. from the seed); see TODO at fleetGate.
	NodeCountEnv = "NODE_COUNT"
)

// GitHub Actions environment variables the e2e suite reads. Package-private:
// these are provided by the runner, not part of any libetcd contract.
const (
	// ciEnv is "true" on a CI runner; the e2e suite self-gates off CI unless
	// E2EEnv forces it on.
	ciEnv = "CI"

	// runIDEnv namespaces fleet keys per workflow run, so concurrent runs don't
	// collide in the cluster keyspace.
	runIDEnv = "GITHUB_RUN_ID"
)
