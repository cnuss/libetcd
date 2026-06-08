// Command multi-node starts a node, then brings up a second node that Joins the
// first and reads back the replicated key.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops both nodes

	// Node 1: a fresh single-member cluster.
	e1 := libetcd.New().WithContext(ctx)
	if err := e1.Start(); err != nil {
		log.Fatal(err)
	}

	cli := e1.Client()
	if _, err := cli.Put(ctx, "greeting", "hello world"); err != nil {
		log.Fatal(err)
	}

	// Node 2: join the cluster via node 1's client — fully managed.
	e2 := libetcd.New().WithContext(ctx)
	if err := e2.Join(cli); err != nil {
		log.Fatal(err)
	}

	resp, err := e2.Loopback().Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("greeting from node 2: %s\n", resp.Kvs[0].Value)
}
