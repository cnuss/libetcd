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
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/cnuss/libetcd"
	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libtunnel"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var node v1.EtcdPeer = nil
	var cli *clientv3.Client
	var err error
	var num = 3

	// BYO peer serving means we own the peer HTTP servers, so we must also stop
	// them — before stopping the nodes. node.Stop() closes the etcd backend;
	// a peer request (another member's /version poll, a tunnel health check)
	// that lands on our still-serving handler after that dereferences a closed
	// backend and panics in etcd's handler. Shut the servers first, then stop
	// the nodes. One deferred teardown, registered before the loop so it runs
	// last (after all the work below).
	var nodes []v1.EtcdPeer
	var servers []*http.Server
	defer func() {
		for _, s := range servers {
			_ = s.Close()
		}
		for _, n := range nodes {
			_ = n.Stop()
		}
	}()

	for i := range num {
		fmt.Printf("starting node %d\n", i)
		peers := []string{}

		if node != nil {
			peers = node.Peers()
		}

		var srv *http.Server
		node, cli, srv, err = newEtcd(ctx, peers...)
		if err != nil {
			log.Fatal(err)
		}
		nodes = append(nodes, node)
		servers = append(servers, srv)

		log.Printf("peers: %v", node.Peers())
		if put, err := cli.Put(ctx, fmt.Sprintf("peers-%d", i), fmt.Sprintf("%v", node.Peers())); err != nil {
			log.Fatal(err)
		} else {
			log.Printf("put revision %d", put.Header.Revision)
		}
	}

	members, err := cli.MemberList(ctx)
	if err != nil {
		log.Fatal(err)
	}
	voters := 0
	for _, m := range members.Members {
		if status, err := cli.Status(ctx, m.ClientURLs[0]); err != nil {
			log.Fatalf("member %d at %v: status error: %v", m.ID, m.ClientURLs, err)
		} else {
			log.Printf("member %d at %v is voter: %v", m.ID, m.ClientURLs, !status.IsLearner)
			if !status.IsLearner {
				voters++
			}
		}
	}

	allKvs, err := cli.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range allKvs.Kvs {
		log.Printf("key %q value %q", kv.Key, kv.Value)
	}

	// Every node joined as a voter and every node's write replicated cluster-wide
	// over the BYO peer servers fronted by the tunnels: num voting members, num
	// keys. Anything short of that is a failed run.
	if voters != num || len(allKvs.Kvs) != num {
		log.Fatalf("with-tunnel FAILED: %d/%d voters, %d/%d keys", voters, num, len(allKvs.Kvs), num)
	}
	log.Printf("with-tunnel success: %d voting members, %d keys replicated across the tunnels", voters, len(allKvs.Kvs))
}

func newEtcd(ctx context.Context, peers ...string) (v1.EtcdPeer, *clientv3.Client, *http.Server, error) {
	log.Printf("new etcd from peers: %v", peers)
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, nil, nil, err
	}

	tunnel := libtunnel.New(libtunnel.Cloudflare()).
		WithContext(ctx).
		WithListener(listener)
	log.Printf("new etcd hostname: %s", tunnel.Hostname())

	etcd := libetcd.From(peers...).
		// WithLog("info", os.Stderr).
		WithPeerListener(nil, tunnel.URL().String())

	if err := etcd.Join(); err != nil {
		return nil, nil, nil, err
	}
	log.Printf("new etcd: peers: %v, endpoints: %v", etcd.Peers(), etcd.Endpoints())

	// BYO peer serving: we own the peer (raft) HTTP server. Mount PeerHandler()
	// only now, after Join returns — mounting it earlier mints the server before
	// Join and is rejected. The caller (main) closes this server before stopping
	// the node; see the teardown note there.
	mux := http.NewServeMux()
	mux.Handle("/hello", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("world"))
	}))
	for _, path := range etcd.PeerPaths() {
		mux.Handle(path, etcd.PeerHandler())
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)

	return etcd, etcd.Client(), srv, nil
}
