package e2e

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"

	v3 "github.com/cnuss/libetcd/v3"
	"github.com/cnuss/libetcd/v3alpha7"
)

// TestV3CoreBootstrap exercises the v3alpha7 lazy server core through its
// public surface only: build a ServerConfig, mint a Server, and let the
// first membership operation boot the whole pipeline (backend, WAL, raft
// node, driver loop, apply loop — there is no Start in the v3 design; the
// first call that needs consensus pulls it up).
//
// The server has no client serving or Stop yet, so the test asserts through
// the ServerV3 surface: member add/remove round-trips through raft, and the
// raft-state getters go live.
func TestV3CoreBootstrap(t *testing.T) {
	gateE2E(t)

	dir, err := os.MkdirTemp("", "libetcd-v3-core-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var srv v3.Server = v3alpha7.NewServer(
		v3alpha7.NewServerConfig().WithDataDir(dir),
	)

	// Identity is available before anything runs — pulling it bootstraps
	// membership (and the backend under it), but spins no consensus.
	self := srv.MemberID()
	if self == 0 {
		t.Fatal("zero member id")
	}
	if srv.Leader() != 0 {
		t.Fatal("leader set before anything ran")
	}

	// First conf-change proposal boots the pipeline. Until the node elects
	// itself raft drops proposals, so retry until the deadline.
	u, _ := url.Parse("http://127.0.0.1:22380")
	learner := membership.NewMember("learner1", types.URLs{*u}, "etcd-cluster", nil)
	learner.IsLearner = true

	var membs []*membership.Member
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		membs, err = srv.AddMember(ctx, *learner)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("AddMember never succeeded: %v", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if len(membs) != 2 {
		t.Fatalf("expected 2 members after add, got %d", len(membs))
	}

	// Consensus ran: the raft-state getters must be live and agree.
	if srv.Leader() != self {
		t.Fatalf("leader %s, want self %s", srv.Leader(), self)
	}
	if srv.AppliedIndex() == 0 || srv.CommittedIndex() == 0 || srv.Term() == 0 {
		t.Fatalf("raft getters not live: applied=%d committed=%d term=%d",
			srv.AppliedIndex(), srv.CommittedIndex(), srv.Term())
	}
	if srv.AppliedIndex() > srv.CommittedIndex() {
		t.Fatalf("applied %d ahead of committed %d", srv.AppliedIndex(), srv.CommittedIndex())
	}

	// Cluster view agrees with the response.
	if got := len(srv.Cluster().Members()); got != 2 {
		t.Fatalf("cluster reports %d members, want 2", got)
	}

	// Remove the learner again.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	membs, err = srv.RemoveMember(ctx, uint64(learner.ID))
	if err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	if len(membs) != 1 {
		t.Fatalf("expected 1 member after remove, got %d", len(membs))
	}
	if got := len(srv.Cluster().Members()); got != 1 {
		t.Fatalf("cluster reports %d members, want 1", got)
	}

	// Housekeeping surface stays sane on a live server.
	if alarms := srv.Alarms(); len(alarms) != 0 {
		t.Fatalf("unexpected alarms: %v", alarms)
	}
}
