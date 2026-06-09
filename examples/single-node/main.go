// Command single-node starts an embedded etcd node and round-trips a key.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the node

	e := libetcd.New().WithContext(ctx)
	if err := e.Start(); err != nil {
		log.Fatal(err)
	}

	cli := e.Voters()
	cli.Put(ctx, "greeting", "hello world")
	resp, _ := cli.Get(ctx, "greeting")

	fmt.Printf("greeting: %s\n", resp.Kvs[0].Value)
}
