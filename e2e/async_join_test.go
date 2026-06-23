package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
)

// TestAsyncJoin grows a cluster by joining three peers concurrently — each
// goroutine calls From(peers).Join() and the library serializes the membership
// changes internally — then proves no data was lost: every joiner writes a key
// through its own node right after joining, and the leader reads them all back.
func TestAsyncJoin(t *testing.T) {
	gateE2E(t)

	const peerCount = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the nodes

	leader := libetcd.New().WithContext(ctx)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	peerURLs := leader.Peers() // one MemberList RPC, reused by every joiner

	// Collect failures and fail from the test goroutine: a t.Fatal inside a
	// joiner goroutine would race teardown mid-membership-change.
	joinErrs := make(chan error, peerCount)
	var wg sync.WaitGroup
	for i := range peerCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			peer := libetcd.From(peerURLs...).WithContext(ctx)
			if err := peer.Join(); err != nil {
				joinErrs <- fmt.Errorf("join %d: %w", i, err)
				return
			}
			// Write through the freshly joined node itself: if the join left it
			// healthy, this commits cluster-wide.
			key := fmt.Sprintf("joined/%d", i)
			if _, err := peer.Self().Put(ctx, key, fmt.Sprintf("hello from peer %d", i)); err != nil {
				joinErrs <- fmt.Errorf("put %s: %w", key, err)
			}
		}(i)
	}
	wg.Wait()
	close(joinErrs)
	for err := range joinErrs {
		if err != nil {
			t.Fatal(err)
		}
	}

	// Every joiner's key reads back through the leader; a missing one is data loss.
	resp, err := leader.Self().Get(ctx, "joined/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get joined/: %v", err)
	}
	if got := len(resp.Kvs); got != peerCount {
		t.Fatalf("data loss: %d/%d keys survived", got, peerCount)
	}
}
