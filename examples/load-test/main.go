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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	load := examples.NewLoad(ctx, 2*time.Second)

	// A high snapshot count keeps the run from triggering raft snapshots, so
	// nodes joining under load catch up by log replay instead of snapshot
	// transfer (embedded snapshot apply can't reopen its mmap'd bbolt db on
	// Windows).
	const snapshotCount = 1_000_000

	// Node 1 up — registering it kicks off load immediately.
	e1 := libetcd.New().WithSnapshotCount(snapshotCount).WithContext(ctx)
	if err := e1.Start(); err != nil {
		log.Fatal(err)
	}
	load.WithEtcd(e1)

	// Grow the cluster under load: join a new node every 2s until the deadline.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return // cancelling ctx gracefully stops every node
		case <-ticker.C:
			n := libetcd.New().WithSnapshotCount(snapshotCount).WithContext(ctx)
			if err := n.Join(e1); err != nil {
				if ctx.Err() != nil {
					return // test over
				}
				log.Printf("join: %v", err)
				continue
			}
			load.WithEtcd(n)
		}
	}
}
