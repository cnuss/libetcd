// Command multi-node starts a node, then brings up a second node that Joins the
// first and reads back the replicated key.
package main

import (
	"context"
	"fmt"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"

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

	printMembers(ctx, "before join", cli)

	// Node 2: join the cluster from node 1's peer URLs — fully managed.
	e2 := libetcd.From(e1.Peers()...).WithContext(ctx)
	if err := e2.Join(); err != nil {
		log.Fatal(err)
	}

	printMembers(ctx, "after join", cli)

	resp, err := e2.Self().Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("greeting from node 2: %s\n", resp.Kvs[0].Value)
}

func printMembers(ctx context.Context, label string, cli *clientv3.Client) {
	members, err := cli.MemberList(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("members %s:\n", label)
	for _, m := range members.Members {
		fmt.Printf("  %16x  %-12s learner=%-5v  peers=%v\n", m.ID, m.Name, m.IsLearner, m.PeerURLs)
	}
}
