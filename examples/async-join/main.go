// Command async-join grows a cluster by joining three peers concurrently —
// each goroutine calls From(peers).Join(), and the library serializes the
// membership changes internally — then proves no data was lost: every joiner
// writes a key through its own node right after joining, and the leader reads
// all of them back.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
)

const peerCount = 3

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the nodes

	var wg sync.WaitGroup

	fmt.Println("starting leader...")
	leader := libetcd.New().WithContext(ctx)
	if err := leader.Start(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("leader started with peer URLs: %v\n", leader.Peers())

	for i := range peerCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Printf("joining peer %d to cluster...", i)
			peer := libetcd.From(leader.Peers()...).WithContext(ctx)
			if err := peer.Join(); err != nil {
				log.Fatalf("join %d: %v", i, err)
			}
			// Write through the freshly joined node itself: if the join left it
			// healthy, this commits cluster-wide.
			key := fmt.Sprintf("joined/%d", i)
			if _, err := peer.Self().Put(ctx, key, fmt.Sprintf("hello from peer %d", i)); err != nil {
				log.Fatalf("put %s: %v", key, err)
			}
			log.Printf("peer %d joined and wrote %s", i, key)
		}(i)
	}
	wg.Wait()

	// Read every joiner's key back through the leader. A missing key means a
	// put was lost — fail loudly.
	resp, err := leader.Self().Get(ctx, "joined/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("get joined/: %v", err)
	}
	for _, kv := range resp.Kvs {
		fmt.Printf("%s: %s\n", kv.Key, kv.Value)
	}
	if got := len(resp.Kvs); got != peerCount {
		log.Fatalf("data loss: %d/%d keys survived", got, peerCount)
	}

	fmt.Printf("voters: %v\n", leader.Voters().Endpoints())
	fmt.Printf("all %d puts survived\n", peerCount)
}
