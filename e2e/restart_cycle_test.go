package e2e

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
	v0 "github.com/cnuss/libetcd/v0"
)

const (
	restartCycleKeyPrefix = "cycle/"
	restartCycleKeyCount  = 24
	restartCycleCycles    = 2
)

// restartCycleMember is what survives between generations: the on-disk identity
// (dir) and the addresses the cluster registered for it.
type restartCycleMember struct {
	name       string
	dir        string
	peerAddr   string
	clientAddr string
}

// TestRestartCycle exercises full restart cycles of a cluster: bootstrap a
// leader, join a peer, write data, stop ALL nodes, then bring every member back
// with brand-new builders over the same data dirs and verify zero loss — twice,
// because some breakage (WAL replay, listener reuse, bootstrap re-entry) only
// shows up on the second restart.
//
// What a restart must hold constant, and why:
//
//   - The data dir — the member's identity (ID, cluster ID, keyspace) lives
//     there; a restarted member boots from its WAL and etcd ignores the fresh
//     builder's initial-cluster string and cluster state.
//   - The peer (raft) address — the cluster's membership stores each member's
//     advertised peer URL and other members dial it, so each generation binds
//     a listener on the same address and passes it via WithPeerListener. The
//     first generation binds 127.0.0.1:0 to claim a free port; later
//     generations re-bind the recorded address (retrying briefly while the
//     previous generation's listener finishes closing).
//   - The client address is pinned the same way so the member's registered
//     client URL stays dialable for networked clients across generations.
//
// Builder handles are single-use (Start/Stop run at most once), so every
// generation constructs fresh builders. A restarted member's Start blocks until
// the cluster has quorum — which needs the other members up too — so a
// generation's members are started concurrently.
func TestRestartCycle(t *testing.T) {
	gateE2E(t)

	ctx := context.Background()

	// Generation 0: bootstrap node-a, claim free loopback ports for both
	// members, and record the chosen addresses for every later generation.
	a0Peer, a0Client := restartCycleMustListen(t, "127.0.0.1:0"), restartCycleMustListen(t, "127.0.0.1:0")
	b0Peer, b0Client := restartCycleMustListen(t, "127.0.0.1:0"), restartCycleMustListen(t, "127.0.0.1:0")
	members := []restartCycleMember{
		{name: "node-a", dir: t.TempDir(), peerAddr: a0Peer.Addr().String(), clientAddr: a0Client.Addr().String()},
		{name: "node-b", dir: t.TempDir(), peerAddr: b0Peer.Addr().String(), clientAddr: b0Client.Addr().String()},
	}

	nodeA := libetcd.New().
		WithName(members[0].name).
		WithDir(members[0].dir).
		WithPeerListener(a0Peer).
		WithClientListener(a0Client)
	if err := nodeA.Start(); err != nil {
		t.Fatalf("node-a Start: %v", err)
	}

	nodeB := libetcd.From(nodeA.Peers()...).
		WithName(members[1].name).
		WithDir(members[1].dir).
		WithPeerListener(b0Peer).
		WithClientListener(b0Client)
	if err := nodeB.Join(); err != nil {
		t.Fatalf("node-b Join: %v", err)
	}

	// Write the dataset through the leader; with two voters, every Put is
	// committed on both members before it is acknowledged.
	expected := make(map[string]string, restartCycleKeyCount)
	cli := nodeA.Self()
	for i := range restartCycleKeyCount {
		k := fmt.Sprintf("%s%03d", restartCycleKeyPrefix, i)
		v := fmt.Sprintf("value-%03d", i)
		if _, err := cli.Put(ctx, k, v); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		expected[k] = v
	}

	restartCycleStopAll(t, nodeA, nodeB)

	// Restart cycles: every generation recreates each member with a fresh
	// builder over the same dir + addresses, verifies the dataset on every
	// member, and stops them all again.
	for cycle := 1; cycle <= restartCycleCycles; cycle++ {
		nodes := restartCycleStartGeneration(t, members)
		for i, n := range nodes {
			restartCycleVerify(t, ctx, members[i].name, n.Self(), expected)
		}
		restartCycleStopAll(t, restartCycleNodesToStoppers(nodes)...)
	}
}

// restartCycleStartGeneration recreates every member with a fresh builder over
// its dir and recorded addresses, and starts them concurrently — a restarted
// member's Start blocks until the cluster has quorum, so serial starts would
// deadlock.
func restartCycleStartGeneration(t *testing.T, members []restartCycleMember) []v0.Etcd {
	t.Helper()
	nodes := make([]v0.Etcd, len(members))
	for i, m := range members {
		nodes[i] = libetcd.New().
			WithName(m.name).
			WithDir(m.dir).
			WithPeerListener(restartCycleRelisten(t, m.peerAddr)).
			WithClientListener(restartCycleRelisten(t, m.clientAddr))
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(nodes))
	for i, n := range nodes {
		wg.Add(1)
		go func(name string, n v0.Etcd) {
			defer wg.Done()
			if err := n.Start(); err != nil {
				errs <- fmt.Errorf("%s Start: %w", name, err)
			}
		}(members[i].name, n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return nodes
}

// restartCycleVerify reads the whole prefix through cli and compares it against
// expected, key count and values both.
func restartCycleVerify(t *testing.T, ctx context.Context, name string, cli *clientv3.Client, expected map[string]string) {
	t.Helper()
	if cli == nil {
		t.Fatalf("verify %s: nil client", name)
	}
	getCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := cli.Get(getCtx, restartCycleKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("verify %s: Get: %v", name, err)
	}
	got := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		got[string(kv.Key)] = string(kv.Value)
	}
	if len(got) != len(expected) {
		t.Fatalf("verify %s: got %d keys, want %d", name, len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			t.Fatalf("verify %s: key %q got %q, want %q", name, k, got[k], want)
		}
	}
}

func restartCycleStopAll(t *testing.T, nodes ...interface{ Stop() error }) {
	t.Helper()
	// Concurrently: a member's peer server holds live raft streams from the
	// others, so stopping in parallel lets the shutdowns unblock each other.
	var wg sync.WaitGroup
	stopErrs := make(chan error, len(nodes))
	for _, n := range nodes {
		wg.Add(1)
		go func(n interface{ Stop() error }) {
			defer wg.Done()
			if err := n.Stop(); err != nil {
				stopErrs <- fmt.Errorf("Stop: %w", err)
			}
		}(n)
	}
	wg.Wait()
	close(stopErrs)
	for err := range stopErrs {
		t.Fatal(err)
	}
}

func restartCycleNodesToStoppers(nodes []v0.Etcd) []interface{ Stop() error } {
	out := make([]interface{ Stop() error }, len(nodes))
	for i, n := range nodes {
		out[i] = n
	}
	return out
}

// restartCycleRelisten re-binds addr, retrying briefly: the previous
// generation's listener (or its connections) may still be releasing the port.
func restartCycleRelisten(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebind %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func restartCycleMustListen(t *testing.T, addr string) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	return l
}
