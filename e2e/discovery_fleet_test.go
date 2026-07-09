package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
	"github.com/cnuss/libetcd/v0alpha0"
	"github.com/cnuss/libtunnel"
)

// discoSeed is the live discovery seed the fleet test rendezvouses through.
const discoSeed = "https://disco.nuss.io"

// fleetGate skips unless the multi-runner fleet env is present — set only by
// .github/workflows/e2e.yml, which fans this test across N runners. It returns
// the fleet size and the cluster token (a GitHub OIDC JWT). This node's identity
// comes from its tunnel hostname (unique per runner), so no index is needed. The
// plain e2e suite (and a local `go test`) never carries these, so the test is
// inert outside the fleet workflow.
func fleetGate(t *testing.T) (count int, token string) {
	t.Helper()
	if os.Getenv(FleetEnv) != "1" {
		t.Skip("fleet e2e gated off — set LIBETCD_E2E_FLEET=1 plus NODE_COUNT/LIBETCD_CLUSTER_TOKEN (run by .github/workflows/e2e.yml)")
	}
	token = os.Getenv(v0alpha0.ClusterTokenEnv)
	// TODO: NODE_COUNT is the interim way to tell a node the fleet size — we'll
	// derive N a different way later (e.g. from the seed). Validate this path first.
	count, _ = strconv.Atoi(os.Getenv(NodeCountEnv))
	if token == "" || count < 1 {
		t.Fatalf("fleet env incomplete: NODE_COUNT=%q token-set=%v", os.Getenv(NodeCountEnv), token != "")
	}
	return count, token
}

// TestDiscoveryFleet is the #108/#122 acceptance test, run for real: this runner
// is one node of an N-node cluster that forms across real Cloudflare tunnels
// through the live discovery seed, with zero prior topology knowledge. Every
// runner makes the identical call — From(disco).WithClusterToken(jwt).Join() —
// carrying its OWN GitHub OIDC token: distinct JWTs, one shared sub, so the seed
// namespaces them into a single cluster (the bug #122 fixed). The seed's
// claim/roster picks exactly one bootstrapper and feeds the joiners the peer
// set; no node is special-cased and no peer URL is shared between runners.
//
// It then proves convergence + replication with no cross-runner messaging: each
// node writes its own key and reads back all N nodes' keys. Seeing all N means
// every node joined and every write replicated here.
func TestDiscoveryFleet(t *testing.T) {
	count, token := fleetGate(t)
	t.Setenv("LIBTUNNEL_CACHE_DIR", t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// This node behind its own Cloudflare tunnel; advertise the tunnel URL as the
	// peer address (BYO peer serving — we own the listener the tunnel fronts).
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tlog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	tun := libtunnel.New(libtunnel.Cloudflare()).WithContext(ctx).WithLogger(tlog).WithListener(l)

	// The tunnel hostname is this node's unique identity — no index needed, every
	// runner gets a distinct tunnel.
	self := tun.URL()
	tag := self.Hostname()

	etcd := libetcd.From(discoSeed).
		WithClusterToken(token).
		WithName(tag).
		WithPeerListener(nil, self.String()).
		WithContext(ctx).
		WithLog("info", os.Stderr)
	if err := etcd.Join(); err != nil {
		t.Fatalf("node %s Join via discovery: %v", tag, err)
	}
	t.Cleanup(func() { _ = etcd.Stop() })

	// Serve the peer (raft + join) protocol on the tunnel-fronted listener — only
	// after Join, so the server isn't minted prematurely.
	mux := http.NewServeMux()
	for _, p := range etcd.PeerPaths() {
		mux.Handle(p, etcd.PeerHandler())
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(l) }()
	// Close the peer server before Stop: a peer request reaching a still-serving
	// handler after the backend closes panics in etcd's handler.
	t.Cleanup(func() { _ = srv.Close() })

	cli := etcd.Self()
	if cli == nil {
		t.Fatal("nil in-process client after Join")
	}

	// Converge: every node must see all N members (nodes join over time).
	if err := fleetWaitMembers(ctx, cli, count); err != nil {
		t.Fatalf("node %s membership: %v", tag, err)
	}

	// Replication without coordination: write this node's key under a run-scoped
	// prefix, then read back all N nodes' keys.
	prefix := "fleet/" + os.Getenv(runIDEnv) + "/"
	if _, err := cli.Put(ctx, prefix+tag, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("node %s put: %v", tag, err)
	}
	if err := fleetWaitKeys(ctx, cli, prefix, count); err != nil {
		t.Fatalf("node %s replication: %v", tag, err)
	}
	t.Logf("node %s: cluster of %d converged + replicated across real tunnels", tag, count)
}

// fleetWaitMembers polls until the cluster reports exactly n voting members.
func fleetWaitMembers(ctx context.Context, cli *clientv3.Client, n int) error {
	var last error
	for {
		ml, err := cli.MemberList(ctx)
		last = err
		if err == nil && len(ml.Members) == n {
			voters := 0
			for _, m := range ml.Members {
				if !m.IsLearner {
					voters++
				}
			}
			if voters == n {
				return nil
			}
			last = fmt.Errorf("%d/%d voters", voters, n)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("members != %d before deadline: %w", n, last)
		case <-time.After(2 * time.Second):
		}
	}
}

// fleetWaitKeys polls until at least n keys exist under prefix (every node's key
// has replicated here).
func fleetWaitKeys(ctx context.Context, cli *clientv3.Client, prefix string, n int) error {
	var last error
	for {
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
		last = err
		if err == nil && len(resp.Kvs) >= n {
			return nil
		}
		if err == nil {
			last = fmt.Errorf("saw %d/%d keys", len(resp.Kvs), n)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("keys under %q < %d before deadline: %w", prefix, n, last)
		case <-time.After(2 * time.Second):
		}
	}
}
