package v1alpha1_test

import (
	"testing"

	"github.com/cnuss/libetcd/v1alpha1"
)

// startedNode builds a node on a temp dir, starts it (auto-binding loopback
// listeners), and registers Stop for cleanup. Going through Start/Stop releases
// the backend and WAL file handles, which matters on Windows where t.TempDir's
// RemoveAll fails if the WAL is still open.
func startedNode(t *testing.T) *v1alpha1.EtcdImpl {
	t.Helper()
	e := v1alpha1.New()
	e.WithDir(t.TempDir())
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = e.Stop() })
	return e
}

// TestServerMints checks the config pipeline produces a non-nil server.
func TestServerMints(t *testing.T) {
	if startedNode(t).Server() == nil {
		t.Fatal("nil server")
	}
}

// TestServerOnce checks Server mints once and caches (serverOnce).
func TestServerOnce(t *testing.T) {
	e := startedNode(t)
	if a, c := e.Server(), e.Server(); a == nil {
		t.Fatal("nil server")
	} else if a != c {
		t.Fatal("Server minted twice; expected serverOnce cache")
	}
}

// TestLoopbackAndHandlers checks the read-side accessors mint non-nil.
func TestLoopbackAndHandlers(t *testing.T) {
	e := startedNode(t)
	if e.Loopback() == nil {
		t.Fatal("nil Loopback")
	}
	if e.GrpcServer() == nil {
		t.Fatal("nil GrpcServer")
	}
	if e.PeerHandler() == nil {
		t.Fatal("nil PeerHandler")
	}
	if e.ClientHandler() == nil {
		t.Fatal("nil ClientHandler")
	}
}
