// Command with-tunnel forms a two-node libetcd cluster across NAT using
// libtunnel: each node serves its peer (raft) listener on a local socket but
// advertises a public Cloudflare tunnel URL, so the members can reach each
// other without a routable address of their own.
//
// It exercises three libetcd features together:
//   - From() with no peers bootstraps a fresh cluster; From(peers...) joins one
//     — the same call site for both roles (the first node and the joiner).
//   - WithPeerListener(lis, advertiseURL) serves lis but advertises the tunnel
//     URL, separating the advertised address from the bound socket.
//   - Client() is the handle for talking to the cluster.
//
// This is a network-dependent demo: it opens real Cloudflare tunnels, so it
// needs outbound network (no external binary — libtunnel embeds the client).
// It is wired into the e2e suite and runnable by hand with `make run with-tunnel`.
package main

import (
	"context"
	"log"
	"net"

	"github.com/cnuss/libetcd"
	"github.com/cnuss/libtunnel"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Node 1: bind a local peer listener, front it with a tunnel, and bootstrap
	// the cluster (From() with no peers → Join() starts a fresh single member).
	lis1, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("node 1 listening on %s", lis1.Addr())

	// WithContext makes tun.URL() block until the tunnel is established and
	// routable, so the join below dials a tunnel that's actually up.
	tun1 := libtunnel.New(libtunnel.Cloudflare()).WithContext(ctx).WithListener(lis1)
	log.Printf("node 1 tunnel hostname %s", tun1.Hostname())

	etcd1 := libetcd.From().WithPeerListener(lis1, tun1.URL().String()).WithContext(ctx)
	if err := etcd1.Join(); err != nil {
		log.Fatal(err)
	}
	defer etcd1.Stop()
	log.Printf("node 1 peers: %v", etcd1.Peers())

	put1, err := etcd1.Client().Put(ctx, "hello", "world")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("node 1 put revision %d", put1.Header.Revision)

	// Node 2: same shape, but joins node 1 through its advertised tunnel URL.
	lis2, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("node 2 listening on %s", lis2.Addr())

	tun2 := libtunnel.New(libtunnel.Cloudflare()).WithContext(ctx).WithListener(lis2)
	log.Printf("node 2 tunnel hostname %s", tun2.Hostname())

	// This Join dials node 1's advertised tunnel URL over the network — the slow,
	// failure-prone step; log what it's reaching for so a stall is diagnosable.
	log.Printf("node 2 joining via %v", etcd1.Peers())
	etcd2 := libetcd.From(etcd1.Peers()...).WithPeerListener(lis2, tun2.URL().String()).WithContext(ctx)
	if err := etcd2.Join(); err != nil {
		log.Fatal(err)
	}
	defer etcd2.Stop()
	log.Printf("node 2 joined; peers: %v", etcd2.Peers())

	// The write made on node 1 replicated across the tunnels to node 2.
	resp, err := etcd2.Client().Get(ctx, "hello")
	if err != nil {
		log.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "world" {
		log.Fatalf("read on node 2 = %v, want %q", resp.Kvs, "world")
	}
	log.Printf("with-tunnel success: node 2 read %q across the tunnels", resp.Kvs[0].Value)
}
