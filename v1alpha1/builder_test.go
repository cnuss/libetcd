package v1alpha1_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cnuss/libetcd/v1alpha1"
)

// startNode boots a single-node cluster on auto-selected ports under a temp data
// dir, returning the running handle. Port 0 keeps concurrent tests from
// colliding on a fixed port.
func startNode(t *testing.T) interface {
	Endpoints() []string
	Close() error
} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	e, err := v1alpha1.New().
		WithName("test").
		WithDir(t.TempDir()).
		WithClientPort(0).
		WithPeerPort(0).
		Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return e
}

func TestPutGetRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e, err := v1alpha1.New().
		WithName("test").
		WithDir(t.TempDir()).
		WithClientPort(0).
		WithPeerPort(0).
		Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Close()

	if _, err := e.Client().Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := e.Client().Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("Get returned %v, want value %q", resp.Kvs, "v")
	}
}

func TestEndpointsResolvedFromAutoPort(t *testing.T) {
	e := startNode(t)
	defer e.Close()

	eps := e.Endpoints()
	if len(eps) == 0 {
		t.Fatal("Endpoints() empty")
	}
	for _, ep := range eps {
		// Port 0 must have been resolved to a concrete bound port.
		if strings.HasSuffix(ep, ":0") {
			t.Errorf("endpoint %q still has unresolved :0 port", ep)
		}
	}
}

func TestCloseIsClean(t *testing.T) {
	e := startNode(t)
	if err := e.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
