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
	e1 := libetcd.From("https://disco.nuss.io").WithClusterToken(token).WithContext(ctx)
	if err := e1.Join(); err != nil {
		log.Fatal(err)
	}
	e2 := libetcd.From("https://disco.nuss.io").WithClusterToken(token).WithContext(ctx)
	if err := e2.Join(); err != nil {
		log.Fatal(err)
	}
	e3 := libetcd.From("https://disco.nuss.io").WithClusterToken(token).WithContext(ctx)
	if err := e3.Join(); err != nil {
		log.Fatal(err)
	}

	// Write on one node, read it back from another — replication works.
	if _, err := e1.Self().Put(ctx, "greeting", "hello world"); err != nil {
		log.Fatal(err)
	}
	got, err := e3.Self().Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("greeting from node 3: %s\n", got.Kvs[0].Value)
}
