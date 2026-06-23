package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
	"github.com/cnuss/libetcd/v1"
)

// TestLoadJoin stress-tests concurrent joins under sustained writes: while N
// writers continuously put sequenced keys through the leader, several peers
// join concurrently. After joins complete, it verifies zero data loss: every
// acknowledged put exists with the exact value, and reads agree across leader
// and joiners.
func TestLoadJoin(t *testing.T) {
	gateE2E(t)

	const (
		joinerCount = 3
		writerCount = 4
		keyPrefix   = "load/"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader := libetcd.New().WithName("leader").WithContext(ctx)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}

	leaderClient := leader.Self()
	if leaderClient == nil {
		t.Fatal("leader self client is nil")
	}

	var (
		wgWriters    sync.WaitGroup
		wgJoiners    sync.WaitGroup
		seq          atomic.Uint64
		ackMu        sync.Mutex
		acknowledged = make(map[string]string)
		stopWriters  = make(chan struct{})
		peersMu      sync.Mutex
		peers        = make([]v1.EtcdPeer, 0, joinerCount)
		joinErrs     = make(chan error, joinerCount)
	)

	for writerID := range writerCount {
		wgWriters.Add(1)
		go func(writerID int) {
			defer wgWriters.Done()
			for {
				select {
				case <-stopWriters:
					return
				default:
				}

				n := seq.Add(1)
				key := fmt.Sprintf("%s%02d/%012d", keyPrefix, writerID, n)
				val := fmt.Sprintf("v:%02d:%012d", writerID, n)

				_, err := leaderClient.Put(ctx, key, val)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					continue
				}

				ackMu.Lock()
				acknowledged[key] = val
				ackMu.Unlock()
			}
		}(writerID)
	}

	for i := range joinerCount {
		wgJoiners.Add(1)
		go func(i int) {
			defer wgJoiners.Done()

			peerNode := libetcd.From(leader.Peers()...).
				WithName(fmt.Sprintf("joiner-%d", i)).
				WithContext(ctx)
			if err := peerNode.Join(); err != nil {
				joinErrs <- fmt.Errorf("join %d: %w", i, err)
				return
			}

			peersMu.Lock()
			peers = append(peers, peerNode)
			peersMu.Unlock()
		}(i)
	}

	wgJoiners.Wait()
	close(joinErrs)
	for err := range joinErrs {
		if err != nil {
			t.Fatal(err)
		}
	}

	close(stopWriters)
	wgWriters.Wait()

	ackMu.Lock()
	expected := make(map[string]string, len(acknowledged))
	for k, v := range acknowledged {
		expected[k] = v
	}
	ackMu.Unlock()
	if len(expected) == 0 {
		t.Fatal("no acknowledged writes to verify")
	}

	nodes := make([]*clientv3.Client, 0, 1+len(peers))
	nodes = append(nodes, leaderClient)
	for _, peerNode := range peers {
		self := peerNode.Self()
		if self == nil {
			t.Fatal("joined peer self client is nil")
		}
		nodes = append(nodes, self)
	}

	for i, node := range nodes {
		got, err := loadJoinGetByPrefix(ctx, node, keyPrefix)
		if err != nil {
			t.Fatalf("verify node %d: %v", i, err)
		}
		if len(got) != len(expected) {
			t.Fatalf("verify node %d: got %d keys, want %d", i, len(got), len(expected))
		}
		for k, want := range expected {
			if got[k] != want {
				t.Fatalf("verify node %d: key %q got %q, want %q", i, k, got[k], want)
			}
		}
	}
}

func loadJoinGetByPrefix(ctx context.Context, cli *clientv3.Client, prefix string) (map[string]string, error) {
	getCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := cli.Get(getCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out[string(kv.Key)] = string(kv.Value)
	}
	return out, nil
}
