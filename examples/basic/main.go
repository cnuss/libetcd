// Command basic is the smallest libetcd example: start a single embedded etcd
// node on auto-selected ports, round-trip a key through its client, and print
// the value.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir, err := os.MkdirTemp("", "libetcd-basic-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Port 0 lets the SDK pick free ports, so the example never collides with a
	// local etcd or another test binary.
	e, err := libetcd.New().
		WithName("basic").
		WithDir(dir).
		WithClientPort(0).
		WithPeerPort(0).
		Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	if _, err := e.Client().Put(ctx, "greeting", "hello world"); err != nil {
		log.Fatal(err)
	}
	resp, err := e.Client().Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("greeting: %s\n", resp.Kvs[0].Value) // greeting: hello world
}
