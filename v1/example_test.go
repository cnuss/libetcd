package v1_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cnuss/libetcd"
)

// New returns an unconfigured Builder. Configure it with the With* methods, then
// Start the node and use its Client.
func ExampleNew() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Port 0 picks a free port, so the example never collides with a local etcd.
	e, err := libetcd.New().
		WithName("greeter").
		WithClientPort(0).
		WithPeerPort(0).
		Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	if _, err := e.Client().Put(ctx, "greeting", "hello"); err != nil {
		log.Fatal(err)
	}
	resp, err := e.Client().Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("greeting = %q\n", resp.Kvs[0].Value)
	// Output: greeting = "hello"
}
