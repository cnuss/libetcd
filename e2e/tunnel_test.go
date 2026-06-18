package e2e

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/cnuss/libetcd"
	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libtunnel"
)

// TestMultiNodeTunnel forms a three-node libetcd cluster across NAT with
// libtunnel using one identical call per node: every node is handed the same
// self-inclusive peer set (all three tunnel URLs) and calls From(set...).Join()
// concurrently. No node is special-cased — the uniform-config election picks the
// lowest URL to bootstrap and the other two join it (issue #98). Each node uses
// BYO peer serving (WithPeerListener(nil, tunnelURL)), serving its own raft HTTP
// on a socket fronted by a public Cloudflare tunnel and advertising that URL.
//
// In-process (no subprocess), so assertions ride testing. It dials real
// Cloudflare tunnels, so it's gated like the rest of the suite (gateE2E) and
// needs outbound network.
func TestMultiNodeTunnel(t *testing.T) {
	gateE2E(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const n = 3

	// Bind every listener and mint every tunnel first, so all URLs exist before
	// any node starts; then hand the same set to all three. WithContext makes
	// URL() block until the tunnel is routable.
	listeners := make([]net.Listener, n)
	tunnels := make([]libtunnel.TunneledV1, n)
	urls := make([]string, n)
	for i := range n {
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		tunnels[i] = libtunnel.New(libtunnel.Cloudflare()).WithContext(ctx).WithListener(l)
		urls[i] = tunnels[i].URL().String()
	}

	nodes := make([]v1.EtcdPeer, n)
	servers := make([]*http.Server, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodes[i], servers[i], errs[i] = newTunnelNode(listeners[i], urls[i], urls...)
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
			t.Fatalf("node %d (%s) Join: %v", i, urls[i], e)
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
			t.Errorf("node %d (%s) still a learner; want voter", i, urls[i])
		}
	}
	t.Logf("multi-node tunnel: %d voting members across the tunnels", n)
}

// newTunnelNode brings up one BYO-peer-serving node in the uniform-config set:
// From(peers...) advertising its own tunnel URL, Join (the election decides
// bootstrap vs join), then serve PeerHandler() on the tunnel's listener — only
// after Join returns, so the server isn't minted prematurely. The caller closes
// the returned *http.Server before stopping the node.
func newTunnelNode(listener net.Listener, selfURL string, peers ...string) (v1.EtcdPeer, *http.Server, error) {
	// No WithContext: each Join is bounded by the library's default join timeout
	// and returns a real error on failure, rather than hanging until the test
	// context fires (which would Stop a serving node mid-flight and panic in
	// etcd's handler). Teardown is the caller's t.Cleanup.
	etcd := libetcd.From(peers...).WithPeerListener(nil, selfURL)
	if err := etcd.Join(); err != nil {
		return nil, nil, err
	}

	mux := http.NewServeMux()
	for _, path := range etcd.PeerPaths() {
		mux.Handle(path, etcd.PeerHandler())
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)

	return etcd, srv, nil
}
