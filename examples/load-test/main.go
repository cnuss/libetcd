// Command load-test brings up a node, kicks off read/write load as soon as it's
// up, joins a second node under load, and runs for 15 seconds — printing
// throughput, latency, and the member list every couple seconds.
package main

import (
	"context"
	"log"
	"time"

	"github.com/cnuss/libetcd"
	"github.com/cnuss/libetcd/examples"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	load := examples.NewLoad(ctx, 2*time.Second)

	// Node 1 up — registering it kicks off load immediately.
	e1 := libetcd.New().WithContext(ctx)
	if err := e1.Start(); err != nil {
		log.Fatal(err)
	}
	load.WithEtcd(e1)

	// Bring up a second node (joins under load) and load it too.
	e2 := libetcd.New().WithContext(ctx)
	if err := e2.Join(e1.Client()); err != nil {
		log.Fatal(err)
	}
	load.WithEtcd(e2)

	// Run until the 15s deadline; cancelling ctx gracefully stops both nodes.
	<-ctx.Done()
}
