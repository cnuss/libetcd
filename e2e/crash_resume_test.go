package e2e

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
)

const (
	crashResumeKeyPrefix = "crash/"
	crashResumePreKeys   = 12 // written before the crash, on all three voters
	crashResumeDownKeys  = 12 // written while the crashed node is down (survivors only)
)

// TestCrashResume is the #135 resumability case the BYO restart tests don't
// cover: a single voter goes down UNGRACEFULLY — it leaves the cluster without
// removing itself from membership (no auto-leave) — while the surviving voters
// keep quorum, then it comes back over its own data dir and rejoins by booting
// from its WAL, catching up everything it missed. It is the same member, never
// re-added.
//
// The crash is modeled by Stop on a non-discovery node: that releases the
// backend and tears down raft but, unlike a discovery node, runs no auto-leave
// MemberRemove — so membership still lists the node, exactly as a SIGKILL would
// leave it. (A discovery node's graceful Stop auto-leaves and so can't resume;
// crash-resume is what a discovery node gets only by dying without Stop. The
// cluster-level behavior under test — membership retained, survivors hold
// quorum, WAL restart — is identical either way.)
//
// Three voters so quorum (2) survives one down member. The returning node is
// rebuilt with a fresh builder over the same dir + peer/client addresses (a
// restart boots from the WAL and ignores the builder's initial-cluster); its
// Start blocks until it has caught up to the quorum that stayed up.
func TestCrashResume(t *testing.T) {
	gateE2E(t)

	ctx := context.Background()

	// Claim free loopback ports up front and record them: the returning node must
	// re-advertise node-c's original peer address (membership stored it) and keep
	// its client address dialable.
	type member struct {
		name       string
		dir        string
		peerAddr   string
		clientAddr string
	}
	peers := [3]net.Listener{crashMustListen(t), crashMustListen(t), crashMustListen(t)}
	clients := [3]net.Listener{crashMustListen(t), crashMustListen(t), crashMustListen(t)}
	members := [3]member{
		{"node-a", t.TempDir(), peers[0].Addr().String(), clients[0].Addr().String()},
		{"node-b", t.TempDir(), peers[1].Addr().String(), clients[1].Addr().String()},
		{"node-c", t.TempDir(), peers[2].Addr().String(), clients[2].Addr().String()},
	}

	// node-a bootstraps; node-b and node-c join it. All three are plain
	// peer-list voters (no discovery), so Stop keeps membership.
	nodeA := libetcd.New().
		WithName(members[0].name).WithDir(members[0].dir).
		WithPeerListener(peers[0]).WithClientListener(clients[0])
	if err := nodeA.Start(); err != nil {
		t.Fatalf("node-a Start: %v", err)
	}
	nodeB := libetcd.From(nodeA.Peers()...).
		WithName(members[1].name).WithDir(members[1].dir).
		WithPeerListener(peers[1]).WithClientListener(clients[1])
	if err := nodeB.Join(); err != nil {
		t.Fatalf("node-b Join: %v", err)
	}
	nodeC := libetcd.From(nodeA.Peers()...).
		WithName(members[2].name).WithDir(members[2].dir).
		WithPeerListener(peers[2]).WithClientListener(clients[2])
	if err := nodeC.Join(); err != nil {
		t.Fatalf("node-c Join: %v", err)
	}
	// node-b is never crashed; ensure it's torn down at the end. node-a and the
	// two node-c incarnations are stopped explicitly below.
	t.Cleanup(func() { _ = nodeB.Stop() })

	// Dataset written while all three are up: committed on every voter.
	cli := nodeA.Self()
	expected := make(map[string]string, crashResumePreKeys+crashResumeDownKeys)
	crashPut(t, ctx, cli, expected, 0, crashResumePreKeys)

	// node-c's member ID before the crash — resume must return the SAME id.
	cID := crashMemberID(t, ctx, cli, "node-c")

	// Crash node-c: backend released, raft down, but membership keeps it (no
	// auto-leave). node-a and node-b carry quorum.
	if err := nodeC.Stop(); err != nil {
		t.Fatalf("node-c crash (Stop): %v", err)
	}

	// Membership still lists node-c (no auto-leave) — that retention is what makes
	// the resume a WAL boot rather than a fresh join. Asserted from a survivor.
	if downID := crashMemberID(t, ctx, cli, "node-c"); downID != cID {
		t.Fatalf("node-c member id changed while down: %x, want %x", downID, cID)
	}
	if n := crashVoterCount(t, ctx, cli); n != 3 {
		t.Fatalf("while node-c down: %d voters, want 3 (membership must retain the down node)", n)
	}

	// The cluster is still writable on the two survivors — proof quorum held
	// through the down window. These keys land while node-c is absent, so its
	// resume has something to catch up on.
	crashPut(t, ctx, cli, expected, crashResumePreKeys, crashResumeDownKeys)

	// Bring node-c back over its own dir + addresses. A restart boots from the
	// WAL, so a plain New() (not From/Join) is correct; Start blocks until it has
	// rejoined and caught up to the survivors.
	nodeC2 := libetcd.New().
		WithName(members[2].name).WithDir(members[2].dir).
		WithPeerListener(crashRelisten(t, members[2].peerAddr)).
		WithClientListener(crashRelisten(t, members[2].clientAddr))
	if err := nodeC2.Start(); err != nil {
		t.Fatalf("node-c resume Start: %v", err)
	}
	t.Cleanup(func() { _ = nodeC2.Stop() })

	// node-c resumed as the SAME member (booted from WAL, never re-added) and the
	// cluster still has exactly three voters.
	resumedID := crashMemberID(t, ctx, nodeC2.Self(), "node-c")
	if resumedID != cID {
		t.Fatalf("node-c resumed with member id %x, want original %x (re-added, not resumed)", resumedID, cID)
	}
	if n := crashVoterCount(t, ctx, nodeC2.Self()); n != 3 {
		t.Fatalf("after resume: %d voters, want 3", n)
	}

	// node-c caught up everything — both the pre-crash dataset and the writes it
	// missed while down. Poll: catch-up is asynchronous after Start returns ready.
	crashWaitData(t, ctx, nodeC2.Self(), expected)

	if err := nodeA.Stop(); err != nil {
		t.Fatalf("node-a Stop: %v", err)
	}
}

// crashPut writes count keys [start, start+count) through cli and records them
// in expected. With a quorum of voters up, each Put is committed before it's
// acknowledged.
func crashPut(t *testing.T, ctx context.Context, cli *clientv3.Client, expected map[string]string, start, count int) {
	t.Helper()
	for i := start; i < start+count; i++ {
		k := fmt.Sprintf("%s%03d", crashResumeKeyPrefix, i)
		v := fmt.Sprintf("value-%03d", i)
		putCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := cli.Put(putCtx, k, v)
		cancel()
		if err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		expected[k] = v
	}
}

// crashMemberID returns the member ID MemberList reports for the named member.
func crashMemberID(t *testing.T, ctx context.Context, cli *clientv3.Client, name string) uint64 {
	t.Helper()
	mlCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ml, err := cli.MemberList(mlCtx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	for _, m := range ml.Members {
		if m.Name == name {
			return m.ID
		}
	}
	t.Fatalf("member %q not found in MemberList", name)
	return 0
}

// crashVoterCount counts non-learner members.
func crashVoterCount(t *testing.T, ctx context.Context, cli *clientv3.Client) int {
	t.Helper()
	mlCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ml, err := cli.MemberList(mlCtx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	n := 0
	for _, m := range ml.Members {
		if !m.IsLearner {
			n++
		}
	}
	return n
}

// crashWaitData polls the whole prefix through cli until it matches expected
// (count and values), or fails after a deadline — catch-up after a resume is
// asynchronous.
func crashWaitData(t *testing.T, ctx context.Context, cli *clientv3.Client, expected map[string]string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		getCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		resp, err := cli.Get(getCtx, crashResumeKeyPrefix, clientv3.WithPrefix())
		cancel()
		if err == nil {
			got := make(map[string]string, len(resp.Kvs))
			for _, kv := range resp.Kvs {
				got[string(kv.Key)] = string(kv.Value)
			}
			if crashDataMatch(got, expected) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-c did not catch up: have %d/%d keys", len(latestGot(ctx, cli)), len(expected))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func crashDataMatch(got, expected map[string]string) bool {
	if len(got) != len(expected) {
		return false
	}
	for k, want := range expected {
		if got[k] != want {
			return false
		}
	}
	return true
}

// latestGot is a best-effort read for the failure message only.
func latestGot(ctx context.Context, cli *clientv3.Client) map[string]string {
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cli.Get(getCtx, crashResumeKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil
	}
	got := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		got[string(kv.Key)] = string(kv.Value)
	}
	return got
}

func crashMustListen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return l
}

// crashRelisten re-binds addr, retrying briefly while the crashed incarnation's
// listener finishes releasing the port.
func crashRelisten(t *testing.T, addr string) net.Listener {
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
