package v1alpha1_test

import (
	"testing"
)

// TestPeers checks Peers returns the node's peer URLs: one entry for a
// single-member node, a non-empty URL.
func TestPeers(t *testing.T) {
	e := startedNode(t)
	peers := e.Peers()
	if len(peers) != 1 {
		t.Fatalf("Peers: got %d entries, want 1: %v", len(peers), peers)
	}
	for _, u := range peers {
		if u == nil {
			t.Error("Peers: nil URL entry")
		} else if u.String() == "" {
			t.Error("Peers: empty URL")
		}
	}
}
