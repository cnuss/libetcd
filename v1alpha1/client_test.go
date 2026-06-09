package v1alpha1_test

import (
	"testing"
)

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
