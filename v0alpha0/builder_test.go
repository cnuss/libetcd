package v0alpha0_test

import (
	"net"
	"testing"

	"github.com/cnuss/libetcd/v0alpha0"
)

// TestServerNilOnBadConfig checks Server returns nil when the config was latched
// invalid (recover guard turns the Validate panic into a latched cause).
func TestServerNilOnBadConfig(t *testing.T) {
	e := v0alpha0.New()
	e.WithLog("not-a-level", nil)
	if e.Server() != nil {
		t.Fatal("expected nil server for invalid config")
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

	e := v0alpha0.New()
	e.WithDir(t.TempDir()).WithPeerListener(lp)
	if err := e.Start(); err != nil {
		t.Fatalf("auto-sync failed, Start: %v", err)
	}
	t.Cleanup(func() { _ = e.Stop() })

	if e.Server() == nil {
		t.Fatal("nil server after WithPeerListener")
	}
}
