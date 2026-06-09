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

// TestPeers checks Peers returns the configured peer topology: one entry for a
// single-member node, keyed by name, with a non-empty peer URL.
func TestPeers(t *testing.T) {
	e := startedNode(t)
	peers := e.Peers()
	if len(peers) != 1 {
		t.Fatalf("Peers: got %d entries, want 1: %v", len(peers), peers)
	}
	for name, urls := range peers {
		if name == "" {
			t.Errorf("Peers: empty member name")
		}
		if len(urls) == 0 {
			t.Errorf("Peers: member %q has no peer URLs", name)
		}
	}
}

// TestSelfAndHandlers checks the read-side accessors mint non-nil.
func TestSelfAndHandlers(t *testing.T) {
	e := startedNode(t)
	if e.Self() == nil {
		t.Fatal("nil Self")
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
