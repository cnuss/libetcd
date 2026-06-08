package v1alpha1_test

import (
	"testing"

	"github.com/cnuss/libetcd/v1alpha1"
)

// TestServerMints checks the config pipeline produces a server: a valid chain
// mints a non-nil *etcdserver.EtcdServer (default name matches the default
// InitialCluster).
func TestServerMints(t *testing.T) {
	b := v1alpha1.New()
	b.WithDir(t.TempDir())
	srv := b.Server()
	if srv == nil {
		t.Fatal("nil server")
	}
	srv.Cleanup()
}

// TestServerOnce checks Server mints once and caches (serverOnce).
func TestServerOnce(t *testing.T) {
	b := v1alpha1.New()
	b.WithDir(t.TempDir())
	a := b.Server()
	c := b.Server()
	if a == nil {
		t.Fatal("nil server")
	}
	if a != c {
		t.Fatal("Server minted twice; expected serverOnce cache")
	}
	a.Cleanup()
}

// TestLoopbackAndHandlers checks the read-side accessors mint non-nil from a
// valid builder.
func TestLoopbackAndHandlers(t *testing.T) {
	b := v1alpha1.New()
	b.WithDir(t.TempDir())
	defer b.Server().Cleanup()

	if b.Loopback() == nil {
		t.Fatal("nil Loopback")
	}
	if b.GrpcServer() == nil {
		t.Fatal("nil GrpcServer")
	}
	if b.PeerHandler() == nil {
		t.Fatal("nil PeerHandler")
	}
	if b.ClientHandler() == nil {
		t.Fatal("nil ClientHandler")
	}
}
