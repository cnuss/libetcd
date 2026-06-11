package v1alpha1_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
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

	cli := e.Self()
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

// TestJoin brings up a node, joins a second one to it from its peer URLs, and
// reads the replicated key from the joiner — exercising the full learner-add →
// promote → voting flow on the single From(...).Join() path.
func TestJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	e1 := v1alpha1.New()
	e1.WithDir(t.TempDir()).WithContext(ctx)
	if err := e1.Start(); err != nil {
		t.Fatalf("node1 Start: %v", err)
	}
	defer e1.Stop()

	if _, err := e1.Voters().Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	e2 := v1alpha1.From(e1.Peers()...)
	e2.WithDir(t.TempDir()).WithContext(ctx)
	if err := e2.Join(); err != nil {
		t.Fatalf("node2 Join: %v", err)
	}
	defer e2.Stop()

	resp, err := e2.Self().Get(ctx, "k")
	if err != nil {
		t.Fatalf("node2 Get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("node2 Get = %v, want value %q", resp.Kvs, "v")
	}

	// Both members must stop cleanly. Stop stops the etcd server before
	// shutting down the HTTP servers; the old order waited out the HTTP
	// shutdown timeout on the other member's live raft streams and returned a
	// spurious "shutdown peer http: context deadline exceeded".
	if err := e2.Stop(); err != nil {
		t.Errorf("node2 Stop: %v", err)
	}
	if err := e1.Stop(); err != nil {
		t.Errorf("node1 Stop: %v", err)
	}
}

// httpGet fetches http://addr+path on the peer listener and returns the body.
func httpGet(t *testing.T, addr, path string) string {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// TestWithPeerListener exercises the listener-as-switch states on the peer
// side: the auto-bind default and a provided listener both end up with the
// raft protocol served by libetcd on the materialized listener.
func TestWithPeerListener(t *testing.T) {
	cases := []struct {
		name       string
		provideLis bool
	}{
		{"default_autobind", false},
		{"provided_listener", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var lis net.Listener
			e := v1alpha1.New()
			e.WithDir(t.TempDir())
			if tc.provideLis {
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("listen: %v", err)
				}
				lis = l
				e.WithPeerListener(lis)
			}
			if err := e.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer e.Stop()

			// Listener: the provided one is handed out by its factory; otherwise
			// the default factory auto-bound one at materialization.
			pl := e.PeerListener()
			if pl == nil {
				t.Fatal("PeerListener nil after Start")
			}
			if tc.provideLis && pl != lis {
				t.Errorf("PeerListener = %v, want the provided listener", pl.Addr())
			}

			if srv := e.PeerHTTP(); srv == nil || srv.Handler == nil {
				t.Fatal("PeerHTTP server missing or has a nil Handler")
			}

			// Node is healthy: round-trip a key through the loopback client.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := e.Self().Put(ctx, "k", "v"); err != nil {
				t.Fatalf("Put: %v", err)
			}

			// The peer (raft) protocol is served on the listener: /version is a
			// PeerPath, so it reaches the peer handler.
			if got := httpGet(t, pl.Addr().String(), "/version"); got == "" {
				t.Error("/version empty; want the peer handler's version response")
			}
		})
	}
}

// TestWithPeerListenerNil pins the peer side's no-off rule: a nil peer
// listener latches a config error (a raft member must advertise a peer URL)
// and Start surfaces it instead of booting.
func TestWithPeerListenerNil(t *testing.T) {
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithPeerListener(nil)
	t.Cleanup(func() { _ = e.Stop() })

	err := e.Start()
	if err == nil || !strings.Contains(err.Error(), "peer listener cannot be nil") {
		t.Fatalf("Start = %v, want latched nil-peer-listener config error", err)
	}
}

// TestWithClientListenerNil pins the headless client side: no listener bound,
// nothing served, no client URLs registered — while the in-process Self client
// still reads and writes the keyspace.
func TestWithClientListenerNil(t *testing.T) {
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithClientListener(nil)
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if l := e.ClientListener(); l != nil {
		t.Errorf("ClientListener = %v, want nil on a headless client side", l.Addr())
	}
	if srv := e.ClientHTTP(); srv != nil {
		t.Error("ClientHTTP non-nil on a headless client side; nothing should be served")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli := e.Self()
	if cli == nil {
		t.Fatal("Self nil on a headless node; the in-process client needs no listener")
	}
	if _, err := cli.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put through Self: %v", err)
	}

	ml, err := cli.MemberList(ctx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	if n := len(ml.Members); n != 1 {
		t.Fatalf("MemberList = %d members, want 1", n)
	}
	if urls := ml.Members[0].ClientURLs; len(urls) != 0 {
		t.Errorf("headless member registered client URLs %v, want none", urls)
	}
}
