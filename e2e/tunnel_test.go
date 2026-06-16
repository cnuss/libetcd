package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/cnuss/libetcd"
	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libtunnel"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestMultiNodeTunnel forms a three-node libetcd cluster across NAT with
// libtunnel: each node uses BYO peer serving — WithPeerListener(nil, tunnelURL)
// — serving its own peer (raft) HTTP on a local socket fronted by a public
// Cloudflare tunnel, while advertising the tunnel URL the other members dial.
// From()/Join() bootstraps the first node and joins the rest over the same call
// site. It asserts all three end up voting and every node's write replicated
// cluster-wide (3 voters, 3 keys).
//
// In-process (no subprocess), so the assertions ride testing, not an exit code
// + stdout grep. It dials real Cloudflare tunnels, so it's gated like the rest
// of the suite (see gateE2E) and needs outbound network.
func TestMultiNodeTunnel(t *testing.T) {
	gateE2E(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const num = 3

	// BYO peer serving means we own the peer HTTP servers, so we must also stop
	// them — before stopping the nodes. Stop() closes the etcd backend, and a
	// peer request (another member's /version poll, a tunnel health check) that
	// lands on a still-serving handler afterwards dereferences the closed backend
	// and panics in etcd's handler. Close the servers first, then stop the nodes.
	var nodes []v1.EtcdPeer
	var servers []*http.Server
	t.Cleanup(func() {
		for _, s := range servers {
			_ = s.Close()
		}
		for _, n := range nodes {
			_ = n.Stop()
		}
	})

	var node v1.EtcdPeer
	var cli *clientv3.Client
	for i := range num {
		var peers []string
		if node != nil {
			peers = node.Peers()
		}

		n, c, srv, err := newTunnelNode(ctx, peers...)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		nodes = append(nodes, n)
		servers = append(servers, srv)
		node, cli = n, c
		t.Logf("node %d peers: %v", i, node.Peers())

		if _, err := cli.Put(ctx, fmt.Sprintf("peers-%d", i), fmt.Sprintf("%v", node.Peers())); err != nil {
			t.Fatalf("node %d put: %v", i, err)
		}
	}

	members, err := cli.MemberList(ctx)
	if err != nil {
		t.Fatalf("member list: %v", err)
	}
	voters := 0
	for _, m := range members.Members {
		status, err := cli.Status(ctx, m.ClientURLs[0])
		if err != nil {
			t.Fatalf("member %d at %v: status: %v", m.ID, m.ClientURLs, err)
		}
		t.Logf("member %d at %v is voter: %v", m.ID, m.ClientURLs, !status.IsLearner)
		if !status.IsLearner {
			voters++
		}
	}

	allKvs, err := cli.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get all: %v", err)
	}

	if voters != num {
		t.Errorf("voters = %d, want %d", voters, num)
	}
	if len(allKvs.Kvs) != num {
		t.Errorf("keys = %d, want %d", len(allKvs.Kvs), num)
	}
	t.Logf("multi-node tunnel: %d voting members, %d keys replicated across the tunnels", voters, len(allKvs.Kvs))
}

// newTunnelNode brings up one BYO-peer-serving node behind a fresh tunnel: bind
// a local listener, front it with a Cloudflare tunnel, advertise the tunnel URL,
// Join (bootstrap when peers is empty), then serve PeerHandler() on the listener
// — only after Join returns, so the server isn't minted prematurely. The caller
// closes the returned *http.Server before stopping the node.
func newTunnelNode(ctx context.Context, peers ...string) (v1.EtcdPeer, *clientv3.Client, *http.Server, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, nil, nil, err
	}

	// WithContext makes tunnel.URL() block until the tunnel is up and routable,
	// so the advertised URL is dialable by the time Join needs it.
	tunnel := libtunnel.New(libtunnel.Cloudflare()).
		WithContext(ctx).
		WithListener(listener)

	etcd := libetcd.From(peers...).WithPeerListener(nil, tunnel.URL().String())
	if err := etcd.Join(); err != nil {
		return nil, nil, nil, err
	}

	mux := http.NewServeMux()
	for _, path := range etcd.PeerPaths() {
		mux.Handle(path, etcd.PeerHandler())
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)

	return etcd, etcd.Client(), srv, nil
}
