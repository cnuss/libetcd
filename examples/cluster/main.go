// Command cluster brings up a 3-node embedded etcd cluster in one process using
// WithPeers, writes a key through node 1's client, reads it back through node
// 3's client, and prints the replicated value — demonstrating that the nodes
// form a real raft cluster.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/cnuss/libetcd"
	v1 "github.com/cnuss/libetcd/v1"
)

// node describes one member: its name and its fixed client/peer ports. Ports are
// fixed (not auto) because every member must know every peer URL up front to
// bootstrap the initial cluster.
type node struct {
	name       string
	clientPort int
	peerPort   int
}

func main() {
	nodes := []node{
		{"n1", 23791, 23801},
		{"n2", 23792, 23802},
		{"n3", 23793, 23803},
	}

	// Shared initial-cluster map: member name -> peer URL.
	peers := make(map[string]string, len(nodes))
	for _, n := range nodes {
		peers[n.name] = fmt.Sprintf("http://localhost:%d", n.peerPort)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A new multi-member cluster only reports ready once a quorum has formed, so
	// every node must be starting concurrently — boot them in parallel.
	handles := make([]v1.Etcd, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n node) {
			defer wg.Done()
			dir, err := os.MkdirTemp("", "libetcd-"+n.name+"-")
			if err != nil {
				errs[i] = err
				return
			}
			defer os.RemoveAll(dir)
			handles[i], errs[i] = libetcd.New().
				WithName(n.name).
				WithDir(dir).
				WithClientPort(n.clientPort).
				WithPeerPort(n.peerPort).
				WithPeers(peers).
				Start(ctx)
		}(i, n)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			log.Fatalf("node %s: %v", nodes[i].name, err)
		}
		defer handles[i].Close()
	}

	// Write on node 1, read on node 3: proves replication across the cluster.
	if _, err := handles[0].Client().Put(ctx, "color", "blue"); err != nil {
		log.Fatal(err)
	}
	resp, err := handles[2].Client().Get(ctx, "color")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("cluster: replicated color = %s\n", resp.Kvs[0].Value) // cluster: replicated color = blue
}
