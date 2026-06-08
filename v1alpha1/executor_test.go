package v1alpha1_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cnuss/libetcd/v1alpha1"
)

// TestStartStopRoundTrip starts a node, round-trips a key through the in-process
// loopback client, and stops cleanly.
func TestStartStopRoundTrip(t *testing.T) {
	lc, _ := net.Listen("tcp", "127.0.0.1:0")
	lp, _ := net.Listen("tcp", "127.0.0.1:0")

	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithClientListener(lc).WithPeerListener(lp)

	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := e.Loopback()
	if _, err := cli.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := cli.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("Get = %v, want value %q", resp.Kvs, "v")
	}
}

// TestStopIdempotent checks Stop is safe to call more than once.
func TestStopIdempotent(t *testing.T) {
	e := v1alpha1.New()
	e.WithDir(t.TempDir())
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := e.Stop(); err != nil {
		t.Fatalf("Stop #1: %v", err)
	}
	if err := e.Stop(); err != nil {
		t.Fatalf("Stop #2: %v", err)
	}
}
