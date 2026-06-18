package e2e

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cnuss/libetcd"
	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libtunnel"
)

// TestMultiNodeTunnel forms a three-node libetcd cluster across NAT with
// libtunnel using one identical call per node. It mints n tunnels up front (just
// to cache their specs — Hostname() registers without connecting), then uses
// libtunnel.Hosts() as the peer set handed to every node. Each node replays its
// own tunnel by host with libtunnel.From(...) inside newTunnelNode and calls
// From(Hosts()...).Join() concurrently — no node special-cased. The
// uniform-config election picks the lowest URL to bootstrap and the other two
// join it (issue #98). BYO peer serving (WithPeerListener(nil, tunnelURL)) fronts
// each node's raft HTTP with its tunnel.
//
// In-process, dials real Cloudflare tunnels — gated like the rest of the suite
// (gateE2E), needs outbound network.
func TestMultiNodeTunnel(t *testing.T) {
	gateE2E(t)

	// Isolate the tunnel cache so Hosts() enumerates exactly this test's mints,
	// not specs left by other runs.
	t.Setenv("LIBTUNNEL_CACHE_DIR", t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const n = 3

	// Mint n distinct tunnels — Hostname() registers and caches each spec without
	// starting a connection — so Hosts() enumerates them and From can replay each.
	for i := range n {
		if h := libtunnel.New(libtunnel.Cloudflare()).Hostname(); h == "" {
			t.Fatalf("mint %d: empty hostname", i)
		}
	}
	peers := libtunnel.Hosts()
	if len(peers) != n {
		t.Fatalf("Hosts() = %d entries, want %d", len(peers), n)
	}

	nodes := make([]v1.EtcdPeer, n)
	servers := make([]*http.Server, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodes[i], servers[i], errs[i] = newTunnelNode(ctx, peers[i])
		}(i)
	}
	wg.Wait()

	// BYO peer serving: we own the peer HTTP servers, so close them before
	// stopping the nodes — Stop() closes the etcd backend, and a peer request
	// reaching a still-serving handler afterwards panics in etcd's handler.
	t.Cleanup(func() {
		for _, s := range servers {
			if s != nil {
				_ = s.Close()
			}
		}
		for _, nd := range nodes {
			if nd != nil {
				_ = nd.Stop()
			}
		}
	})

	for i, e := range errs {
		if e != nil {
			t.Fatalf("node %d (%s) Join: %v", i, peers[i], e)
		}
	}

	// Every node converged on the same three-member, all-voting cluster.
	for i, nd := range nodes {
		ml, err := nd.Self().MemberList(ctx)
		if err != nil {
			t.Fatalf("node %d MemberList: %v", i, err)
		}
		if len(ml.Members) != n {
			t.Fatalf("node %d sees %d members, want %d", i, len(ml.Members), n)
		}
		st, err := nd.Self().Status(ctx, "")
		if err != nil {
			t.Fatalf("node %d Status: %v", i, err)
		}
		if st.IsLearner {
			t.Errorf("node %d (%s) still a learner; want voter", i, peers[i])
		}
	}
	t.Logf("multi-node tunnel: %d voting members across the tunnels", n)
}

// newTunnelNode brings up one BYO-peer-serving node in the uniform-config set:
// replay this node's tunnel by host with libtunnel.From, advertise its URL, and
// From(libtunnel.Hosts()...).Join() — the peer set is read from the shared
// tunnel cache, so every node gets the same self-inclusive list and the election
// decides bootstrap vs join. Then serve PeerHandler() on the tunnel's listener,
// only after Join returns, so the server isn't minted prematurely. No
// WithContext on the node: each Join is bounded by the library default and
// returns a real error on failure rather than hanging until the test context
// fires. The caller closes the returned *http.Server before stopping the node.
func newTunnelNode(ctx context.Context, selfURL string) (v1.EtcdPeer, *http.Server, error) {
	u, err := url.Parse(selfURL)
	if err != nil {
		return nil, nil, err
	}
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, nil, err
	}
	tun := libtunnel.From(u.Hostname()).WithContext(ctx).WithListener(l)

	etcd := libetcd.From(libtunnel.Hosts()...).WithPeerListener(nil, tun.URL().String())
	if err := etcd.Join(); err != nil {
		return nil, nil, err
	}

	mux := http.NewServeMux()
	for _, path := range etcd.PeerPaths() {
		mux.Handle(path, etcd.PeerHandler())
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(l)

	return etcd, srv, nil
}
