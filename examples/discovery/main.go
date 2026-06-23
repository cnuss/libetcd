// Command discovery brings up a three-node cluster through a discovery seed.
// Every node makes the same From(seed).Join() call — the seed elects one node
// to bootstrap and the rest join it, so none is special-cased. Compare
// examples/multi-node, which wires the second node to the first by hand.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/cnuss/libetcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops every node

	// Mint one disco-native token; its sub names the cluster, so every node using
	// it lands in the same one. (In CI, hand each node a GitHub OIDC token —
	// same sub across a workflow's runners.)
	resp, err := http.Get("https://disco.nuss.io/token")
	if err != nil {
		log.Fatal(err)
	}
	var tok map[string]any
	json.NewDecoder(resp.Body).Decode(&tok)
	resp.Body.Close()

	token := tok["token"].(string)

	// Three identical calls — the seed elects one to bootstrap, the rest join.
	log.Printf("joining e1 using disco.nuss.io")
	e1 := libetcd.From("https://disco.nuss.io").WithClusterToken(token).WithContext(ctx)
	if err := e1.Join(); err != nil {
		log.Fatal(err)
	}
	e1.Self().Put(ctx, "e1", fmt.Sprintf("hello from %v", e1.Peers()))

	log.Printf("joining e2 using disco.nuss.io")
	e2 := libetcd.From("https://disco.nuss.io").WithClusterToken(token).WithContext(ctx)
	if err := e2.Join(); err != nil {
		log.Fatal(err)
	}
	e2.Self().Put(ctx, "e2", fmt.Sprintf("hello from %v", e2.Peers()))

	log.Printf("joining e3 using disco.nuss.io")
	e3 := libetcd.From("https://disco.nuss.io").WithClusterToken(token).WithContext(ctx)
	if err := e3.Join(); err != nil {
		log.Fatal(err)
	}
	e3.Self().Put(ctx, "e3", fmt.Sprintf("hello from %v", e3.Peers()))

	members, err := e1.Client().MemberList(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("members:\n")
	for _, m := range members.Members {
		fmt.Printf("  %16x  %-12s learner=%-5v  peers=%v\n", m.ID, m.Name, m.IsLearner, m.PeerURLs)
	}

	kvs, err := e1.Client().Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("kvs:\n")
	for _, kv := range kvs.Kvs {
		fmt.Printf("  %q = %q\n", kv.Key, kv.Value)
	}
	log.Printf("disco: done")
}
