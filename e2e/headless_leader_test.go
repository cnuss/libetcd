package e2e

import (
	"context"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd"
)

const (
	headlessKey = "greeting"
	headlessVal = "hello from a headless cluster"
)

// TestHeadlessLeader exercises a cluster whose bootstrap node serves no client
// (v3 API) traffic at all — WithClientListener(nil) — yet is still joinable.
//
// This is what the peer-port join protocol unlocks: a joining node drives its
// whole join (add → snapshot seed → promote) over the cluster's peer (raft)
// listener, never over a client endpoint. So a cluster with no client endpoint
// anywhere — a headless leader, before any serving member has joined — is
// joinable, which a networked-clientv3 join could never do (nothing to dial).
//
// The headless leader still reads and writes its own keyspace in-process via
// Self (the loopback client needs no listener); networked clients reach the
// data through the serving members that join it.
func TestHeadlessLeader(t *testing.T) {
	gateE2E(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops every node

	// The bootstrap node is headless: it participates in raft and serves the
	// peer listener, but binds no client listener and registers no client URL.
	leader := libetcd.New().WithName("headless").WithClientListener(nil).WithContext(ctx)
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}

	// It has no client endpoint — confirm it registered none — but Self (the
	// in-process client) works regardless, so it can write its own keyspace.
	headlessMustHaveNoClientURL(ctx, t, leader.Self(), "headless")
	if _, err := leader.Self().Put(ctx, headlessKey, headlessVal); err != nil {
		t.Fatal(err)
	}

	// Two serving members join — through the headless leader's peer port. The
	// join never touches a client endpoint, so the headless bootstrap is no
	// obstacle. Each is a normal (client-serving) node.
	m1 := libetcd.From(leader.Peers()...).WithName("member-1").WithContext(ctx)
	if err := m1.Join(); err != nil {
		t.Fatal(err)
	}

	// member-2 joins through whichever peer answers first — leader or member-1;
	// both are reachable on their peer listeners.
	m2 := libetcd.From(leader.Peers()...).WithName("member-2").WithContext(ctx)
	if err := m2.Join(); err != nil {
		t.Fatal(err)
	}

	// The write made before any client-serving member existed replicated to
	// both joiners; read it back from each, in-process.
	headlessMustRead(ctx, t, m1.Self(), "member-1")
	headlessMustRead(ctx, t, m2.Self(), "member-2")

	// The headless leader still reads its own keyspace in-process.
	headlessMustRead(ctx, t, leader.Self(), "headless")

	// Membership: three voters, exactly one of them headless (no client URLs).
	headless, voters := headlessSurveyMembers(ctx, t, m1.Self())
	if voters != 3 {
		t.Fatalf("voters = %d, want 3", voters)
	}
	if headless != 1 {
		t.Fatalf("headless members = %d, want 1 (only the leader)", headless)
	}
}

// headlessMustHaveNoClientURL fails unless the named member registered no client URLs.
func headlessMustHaveNoClientURL(ctx context.Context, t *testing.T, cli *clientv3.Client, name string) {
	t.Helper()
	ml, err := cli.MemberList(ctx)
	if err != nil {
		t.Fatalf("%s MemberList: %v", name, err)
	}
	for _, m := range ml.Members {
		if m.Name == name && len(m.ClientURLs) != 0 {
			t.Fatalf("%s registered client URLs %v, want none (headless)", name, m.ClientURLs)
		}
	}
}

// headlessMustRead fails unless cli reads headlessKey==headlessVal.
func headlessMustRead(ctx context.Context, t *testing.T, cli *clientv3.Client, name string) {
	t.Helper()
	resp, err := cli.Get(ctx, headlessKey)
	if err != nil {
		t.Fatalf("%s Get: %v", name, err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != headlessVal {
		t.Fatalf("%s read %v, want %q", name, resp.Kvs, headlessVal)
	}
}

// headlessSurveyMembers returns (headless count, voter count) from the cluster
// membership as cli sees it.
func headlessSurveyMembers(ctx context.Context, t *testing.T, cli *clientv3.Client) (headless, voters int) {
	t.Helper()
	ml, err := cli.MemberList(ctx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	for _, m := range ml.Members {
		if m.IsLearner {
			continue
		}
		voters++
		if len(m.ClientURLs) == 0 {
			headless++
		}
	}
	return headless, voters
}
