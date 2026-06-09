// Command join-from-peers joins a second node to a running one using only the first
// node's peer URLs. libetcd.From(peers).Join() discovers a client endpoint by
// scraping each peer's /members handler over HTTP, then runs a managed
// learner-join (add → seed from leader → promote).
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the nodes

	// Node 1: a fresh single-node cluster serving the peer protocol on a known
	// listener, with a key to replicate.
	node1 := libetcd.New().WithContext(ctx).WithPeerServing(listener(), nil)
	if err := node1.Start(); err != nil {
		log.Fatal(err)
	}
	if _, err := node1.Voters().Put(ctx, "greeting", "hello from the cluster"); err != nil {
		log.Fatal(err)
	}

	// Node 2: join using only node 1's peer URLs — no pre-dialed client.
	node2 := libetcd.From(node1.Peers()).WithContext(ctx)
	if err := node2.Join(); err != nil {
		log.Fatal(err)
	}

	resp, err := node2.Self().Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("node 2 joined and read: %s\n", resp.Kvs[0].Value)
	// Output: node 2 joined and read: hello from the cluster
}

func listener() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	return l
}
