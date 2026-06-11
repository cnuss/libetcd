// Command headless-leader runs a cluster whose leader serves raft only — no
// client (v3 API) endpoint — via WithoutClientServing.
//
// A serving node bootstraps the cluster first: a cluster whose every member is
// headless would expose no client endpoint at all, so From's discovery (and any
// networked client) could never reach it. The headless node then joins as a
// quorum-only member, leadership is moved onto it, and a third, serving node
// joins *through* the headless leader — exercising the join path's catch-up
// gate when the leader has no client URL to ask. Reads and writes flow through
// the serving peers; the headless leader still reads the replicated keyspace
// in-process via Self, which needs no listener.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops every node

	// Node 1: a normal serving node bootstraps the cluster.
	n1 := libetcd.New().WithContext(ctx)
	if err := n1.Start(); err != nil {
		log.Fatal(err)
	}

	if _, err := n1.Voters().Put(ctx, "greeting", "hello world"); err != nil {
		log.Fatal(err)
	}

	// Node 2: headless — participates in raft, serves no client traffic.
	headless := libetcd.From(n1.Peers()...).WithName("quorum-node").WithoutClientServing().WithContext(ctx)
	if err := headless.Join(); err != nil {
		log.Fatal(err)
	}

	// The headless member registers no client URLs: nothing to dial.
	ml, err := n1.Voters().MemberList(ctx)
	if err != nil {
		log.Fatal(err)
	}
	var headlessID uint64
	for _, m := range ml.Members {
		fmt.Printf("member %-12s learner=%-5v clientURLs=%v\n", m.Name, m.IsLearner, m.ClientURLs)
		if m.Name == "quorum-node" {
			headlessID = m.ID
			if len(m.ClientURLs) != 0 {
				log.Fatalf("headless member registered client URLs: %v", m.ClientURLs)
			}
		}
	}
	if headlessID == 0 {
		log.Fatal("headless member not found in the membership")
	}

	// Move leadership onto the headless member. MoveLeader must be addressed to
	// the current leader — n1, reachable in-process — then poll until the
	// transfer lands.
	if _, err := n1.Self().MoveLeader(ctx, headlessID); err != nil {
		log.Fatal(err)
	}
	for deadline := time.Now().Add(30 * time.Second); ; {
		st, err := n1.Self().Status(ctx, "")
		if err == nil && st.Leader == headlessID {
			break
		}
		if time.Now().After(deadline) {
			log.Fatal("leadership did not move to the headless member")
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("leader is now the headless member")

	// Node 3: a serving node joins through the headless leader — the catch-up
	// gate can't Status a leader with no client URL, so it measures against a
	// serving voter instead.
	n3 := libetcd.From(n1.Peers()...).WithContext(ctx)
	if err := n3.Join(); err != nil {
		log.Fatal(err)
	}

	// Writes and reads flow through the serving peers' client endpoints (the
	// networked Voters client dials voting members' client URLs — the headless
	// leader contributes none).
	cli := n1.Voters()
	if _, err := cli.Put(ctx, "via-peers", "ok"); err != nil {
		log.Fatal(err)
	}
	resp, err := cli.Get(ctx, "greeting")
	if err != nil || len(resp.Kvs) != 1 {
		log.Fatalf("read through serving peers: %v %v", resp, err)
	}
	fmt.Printf("read through serving peers: %s\n", resp.Kvs[0].Value)

	// The headless leader has no client endpoint, but Self is in-process and
	// still reads the replicated keyspace.
	resp, err = headless.Self().Get(ctx, "via-peers")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "ok" {
		log.Fatalf("read in-process on the headless leader: %v %v", resp, err)
	}
	fmt.Println("headless leader read its keyspace in-process via Self")

	fmt.Println("headless-leader success: verified")
}
