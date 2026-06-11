package v1alpha1_test

import (
	"net"
	"testing"

	"github.com/cnuss/libetcd/v1alpha1"
)

// TestServerNilOnBadConfig checks Server returns nil when the config was latched
// invalid (recover guard turns the Validate panic into a latched cause).
func TestServerNilOnBadConfig(t *testing.T) {
	e := v1alpha1.New()
	e.WithLog("not-a-level", nil)
	if e.Server() != nil {
		t.Fatal("expected nil server for invalid config")
	}
}

// TestWithoutPeerServingRequiresURL checks that WithoutPeerServing with no
// advertise URL latches a configuration error: a raft member with no
// advertise-peer-URL can't be dialed by the rest of the cluster.
func TestWithoutPeerServingRequiresURL(t *testing.T) {
	e := v1alpha1.New()
	e.WithoutPeerServing()
	if e.Server() != nil {
		t.Fatal("expected nil server when WithoutPeerServing was given no advertise URL")
	}
	if err := e.Start(); err == nil {
		t.Fatal("expected Start to surface the latched advertise-URL error")
	}
}

// TestWithoutPeerServingBadURL checks an unparseable advertise URL is an error
// (latched), not silently dropped like From's peer sanitization.
func TestWithoutPeerServingBadURL(t *testing.T) {
	e := v1alpha1.New()
	e.WithoutPeerServing("ftp://nope:1")
	if e.Server() != nil {
		t.Fatal("expected nil server for an unparseable advertise URL")
	}
}

// TestAutoSyncInitialCluster checks the single-member auto-sync keeps the
// InitialCluster consistent when the peer URL changes, so the node starts
// (without it, NewServer fails: local name not in InitialCluster).
func TestAutoSyncInitialCluster(t *testing.T) {
	lp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithPeerServing(lp, nil)
	if err := e.Start(); err != nil {
		t.Fatalf("auto-sync failed, Start: %v", err)
	}
	t.Cleanup(func() { _ = e.Stop() })

	if e.Server() == nil {
		t.Fatal("nil server after WithPeerServing")
	}
}
