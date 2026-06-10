package v1alpha1_test

import (
	"testing"

	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1"
	"github.com/cnuss/libetcd/v1alpha1/stream"
)

// startedNode builds a node on a temp dir, starts it (auto-binding loopback
// listeners), and registers Stop for cleanup. Going through Start/Stop releases
// the backend and WAL file handles, which matters on Windows where t.TempDir's
// RemoveAll fails if the WAL is still open.
func startedNode(t *testing.T) v1.Etcd {
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

// TestRaftStreamIntercepted is the layout guard for the reflection in
// stream.Intercept: minting a server runs Intercept, and stream.Intercepted
// re-walks the same unexported path (EtcdServer.r → raftNodeConfig.transport →
// Transport.streamRt) to confirm the raft stream RoundTripper is now wrapped. If
// a future etcd bump renames or restructures any hop, this fails loudly in CI
// instead of silently dropping the 206 rewrite at a consumer's runtime.
func TestRaftStreamIntercepted(t *testing.T) {
	if !stream.Intercepted(startedNode(t).Server()) {
		t.Fatal("raft stream RoundTripper not intercepted — etcd internal layout likely changed")
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
