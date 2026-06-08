package v1alpha1_test

import (
	"net"
	"testing"

	"github.com/cnuss/libetcd/v1alpha1"
)

// TestServerNilOnBadConfig checks Server returns nil when the config was latched
// invalid (recover guard turns the Validate panic into a latched cause).
func TestServerNilOnBadConfig(t *testing.T) {
	b := v1alpha1.New()
	b.WithLogLevel("not-a-level")
	if b.Server() != nil {
		t.Fatal("expected nil server for invalid config")
	}
}

// TestAutoSyncInitialCluster checks the single-member auto-sync keeps the
// InitialCluster consistent when the name and peer URL change, so minting still
// succeeds (without it, NewServer fails: local name not in InitialCluster).
func TestAutoSyncInitialCluster(t *testing.T) {
	lp, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lp.Close()

	b := v1alpha1.New()
	b.WithDir(t.TempDir()).WithName("n1").WithPeerListener(lp)
	srv := b.Server()
	if srv == nil {
		t.Fatal("auto-sync failed: nil server after WithName + WithPeerListener")
	}
	srv.Cleanup()
}
