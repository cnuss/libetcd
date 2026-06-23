package kvdb

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// TestKVDBLive exercises the three store ops against the real kvdb.io bucket.
// Gated: skips unless DISCO_KVDB_BUCKET + DISCO_KVDB_SECRET are set, so it runs
// only where the discovery secret is available (local dev, the disco CI job).
// Each run uses a unique sub so the one-shot claim and the roster don't collide
// across runs; entries carry the package TTL and expire on their own.
func TestKVDBLive(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Skipf("kvdb not configured: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sub := fmt.Sprintf("disco-test-%d", time.Now().UnixNano())

	// First claim wins; a second claim on the same sub loses.
	won, err := s.Claim(ctx, sub)
	if err != nil {
		t.Fatalf("claim 1: %v", err)
	}
	if !won {
		t.Fatalf("claim 1: won=false, want true (first claim should win)")
	}
	won, err = s.Claim(ctx, sub)
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if won {
		t.Fatalf("claim 2: won=true, want false (claim already held)")
	}

	// Register two members; the roster lists both. Re-register n1 to prove
	// idempotency (same id overwrites, not duplicates).
	for _, m := range []struct{ id, url string }{
		{"n1", "http://node1:2380"},
		{"n2", "http://node2:2380"},
		{"n1", "http://node1:2380"},
	} {
		if err := s.Register(ctx, sub, m.id, m.url); err != nil {
			t.Fatalf("register %s: %v", m.id, err)
		}
	}

	urls, err := s.Roster(ctx, sub)
	if err != nil {
		t.Fatalf("roster: %v", err)
	}
	slices.Sort(urls)
	want := []string{"http://node1:2380", "http://node2:2380"}
	if !slices.Equal(urls, want) {
		t.Fatalf("roster = %v, want %v", urls, want)
	}
}
