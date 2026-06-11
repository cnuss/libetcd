// Command dir-handoff exercises data-dir reuse across builder instances — the
// process-restart story: start a node over a dir, write keys, stop it, then
// construct a brand-new builder over the same dir (no carried-over in-memory
// state) and verify every key survived.
//
// The only thing held constant is the data dir. The member's identity (ID,
// cluster ID, data) lives on disk; the second incarnation boots from the WAL,
// which makes etcd ignore the fresh builder's generated name, initial-cluster
// string, and cluster state. Listeners are auto-bound to new free ports both
// times — fine for a single-member cluster, where no other member dials the
// registered peer URL (a multi-member restart must re-bind the same peer
// addresses; see examples/restart-cycle).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
)

const (
	keyPrefix = "handoff/"
	keyCount  = 16
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "libetcd-dir-handoff-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// First incarnation: a fresh node over the (empty) dir.
	first := libetcd.New().WithDir(dir)
	if err := first.Start(); err != nil {
		log.Fatalf("first Start: %v", err)
	}

	expected := make(map[string]string, keyCount)
	cli := first.Self()
	for i := range keyCount {
		k := fmt.Sprintf("%s%03d", keyPrefix, i)
		v := fmt.Sprintf("value-%03d", i)
		if _, err := cli.Put(ctx, k, v); err != nil {
			log.Fatalf("Put %s: %v", k, err)
		}
		expected[k] = v
	}

	if err := first.Stop(); err != nil {
		log.Fatalf("first Stop: %v", err)
	}

	// Second incarnation: a brand-new builder over the same dir. Builder
	// handles are single-use (Start/Stop run at most once), so a "restart" is
	// always a new builder; nothing survives in memory.
	second := libetcd.New().WithDir(dir)
	if err := second.Start(); err != nil {
		log.Fatalf("second Start: %v", err)
	}

	got, err := getByPrefix(ctx, second.Self(), keyPrefix)
	if err != nil {
		log.Fatalf("verify Get: %v", err)
	}
	if len(got) != len(expected) {
		log.Fatalf("verify: got %d keys, want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			log.Fatalf("verify: key %q got %q, want %q", k, got[k], want)
		}
	}

	if err := second.Stop(); err != nil {
		log.Fatalf("second Stop: %v", err)
	}

	fmt.Printf("dir-handoff success: verified %d/%d keys after data-dir handoff\n", len(got), keyCount)
}

// getByPrefix reads every key under prefix into a map.
func getByPrefix(ctx context.Context, cli *clientv3.Client, prefix string) (map[string]string, error) {
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
