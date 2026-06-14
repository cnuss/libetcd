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
// This is a network-dependent demo: it opens real Cloudflare tunnels, so it is
// not part of the hermetic e2e suite — run it by hand (`go run .`).
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
	tun1 := libtunnel.New(libtunnel.Cloudflare()).WithListener(lis1)

	etcd1 := libetcd.From().WithPeerListener(lis1, tun1.URL().String()).WithContext(ctx)
	if err := etcd1.Join(); err != nil {
		log.Fatal(err)
	}
	defer etcd1.Stop()

	if _, err := etcd1.Client().Put(ctx, "hello", "world"); err != nil {
		log.Fatal(err)
	}

	// Node 2: same shape, but joins node 1 through its advertised tunnel URL.
	lis2, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		log.Fatal(err)
	}
	tun2 := libtunnel.New(libtunnel.Cloudflare()).WithListener(lis2)

	etcd2 := libetcd.From(etcd1.Peers()...).WithPeerListener(lis2, tun2.URL().String()).WithContext(ctx)
	if err := etcd2.Join(); err != nil {
		log.Fatal(err)
	}
	defer etcd2.Stop()

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
