package v1alpha1_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"slices"
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

	if _, err := e1.Client().Put(ctx, "k", "v"); err != nil {
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

// TestWithPeerListenerAdvertiseURL: an explicit advertise URL is registered
// with the cluster instead of the listener's own address (the proxy/tunnel
// case), while libetcd still serves the bound listener.
func TestWithPeerListenerAdvertiseURL(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const advertised = "http://node.example:2380"

	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithPeerListener(lis, advertised)
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	// The single member advertises the given URL, not the bound 127.0.0.1 port.
	peers := e.Peers()
	if len(peers) != 1 || peers[0] != advertised {
		t.Fatalf("Peers() = %v, want [%q]", peers, advertised)
	}
	if strings.Contains(peers[0], lis.Addr().String()) {
		t.Errorf("advertised the listener address %q; want the explicit URL", lis.Addr())
	}
	// libetcd still serves raft on the bound listener: /version reaches the peer handler.
	if got := httpGet(t, lis.Addr().String(), "/version"); got == "" {
		t.Error("/version empty on the bound listener; peer handler should serve it")
	}
}

// TestWithPeerListenerAdvertiseNormalize: an advertise URL without an explicit
// port (a public tunnel URL like https://host/) is normalized to host:port with
// the scheme's default port and no path, since etcd peer URLs require host:port.
func TestWithPeerListenerAdvertiseNormalize(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithPeerListener(lis, "https://node.example.com/")
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	peers := e.Peers()
	want := "https://node.example.com:443"
	if len(peers) != 1 || peers[0] != want {
		t.Fatalf("Peers() = %v, want [%q] (port filled, path dropped)", peers, want)
	}
}

// TestWithPeerListenerAdvertiseFallback: when every advertise URL is
// unparseable, the node falls back to the listener's own address (and the
// fallback warning runs under the builder lock without deadlocking).
func TestWithPeerListenerAdvertiseFallback(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithPeerListener(lis, "://nonsense", "\x7f://bad")
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	peers := e.Peers()
	want := "http://" + lis.Addr().String()
	if len(peers) != 1 || peers[0] != want {
		t.Fatalf("Peers() = %v, want [%q] (listener fallback)", peers, want)
	}
}

// TestEndpoints: a serving node reports its advertised client URL; a headless
// client side reports none.
func TestEndpoints(t *testing.T) {
	e := v1alpha1.New()
	e.WithDir(t.TempDir())
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	addr := e.ClientListener().Addr().String()
	eps := e.Endpoints()
	if len(eps) != 1 || !strings.Contains(eps[0], addr) {
		t.Fatalf("Endpoints() = %v, want one URL containing %q", eps, addr)
	}

	h := v1alpha1.New()
	h.WithDir(t.TempDir()).WithClientListener(nil)
	if err := h.Start(); err != nil {
		t.Fatalf("headless Start: %v", err)
	}
	defer h.Stop()
	if eps := h.Endpoints(); len(eps) != 0 {
		t.Errorf("headless Endpoints() = %v, want none", eps)
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

// TestWithListenerAfterMaterializeErrors: a listener setter called after the
// listener has materialized (here a headless client side, whose factory ran to
// a nil result) must latch — the sync.Once is spent, so a later setter could
// change the advertised URLs without the factory ever binding them.
func TestWithListenerAfterMaterializeErrors(t *testing.T) {
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithClientListener(nil)
	if l := e.ClientListener(); l != nil { // materializes the (nil) client side
		t.Fatalf("ClientListener = %v, want nil", l.Addr())
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	e.WithClientListener(lis) // too late — must latch a config error

	if e.Server() != nil {
		t.Fatal("Server non-nil after a post-materialization listener setter; expected the latched config error")
	}
}

// TestFromBootstrap: From() with no peers is a bootstrap — Join short-circuits
// to Start and the node is a usable single-member cluster (issue #77 NEED 2).
func TestFromBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p := v1alpha1.From() // no peers
	p.WithDir(t.TempDir()).WithContext(ctx)
	if err := p.Join(); err != nil {
		t.Fatalf("From().Join() bootstrap: %v", err)
	}
	defer p.Stop()

	if _, err := p.Client().Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put via Client(): %v", err)
	}
	resp, err := p.Self().Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get via Self(): %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("Get = %v, want value %q", resp.Kvs, "v")
	}
}

// TestFromBadPeersStillErrors: From with peers that all sanitize to nothing is
// a bad-input error, not a silent bootstrap — the bootstrap is keyed on the raw
// argument count, not the sanitized result.
func TestFromBadPeersStillErrors(t *testing.T) {
	p := v1alpha1.From("htp://bad") // one unparseable peer (wrong scheme)
	p.WithDir(t.TempDir())
	t.Cleanup(func() { _ = p.Stop() })

	err := p.Join()
	if err == nil || !strings.Contains(err.Error(), "no valid peer URLs") {
		t.Fatalf("Join = %v, want no-valid-peer-URLs error (not a bootstrap)", err)
	}
}

// TestWithPeerListenerMultiAdvertiseBootstrap: a single bootstrap member can
// advertise multiple peer URLs — the single-member auto-sync lists them all in
// initial-cluster, satisfying etcd's VerifyBootstrap (advertise set must equal
// the initial-cluster URL set for this member).
func TestWithPeerListenerMultiAdvertiseBootstrap(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithPeerListener(lis,
		"https://a.example.com:2380",
		"https://b.example.com:2380",
	)
	if err := e.Start(); err != nil {
		t.Fatalf("Start with two advertise URLs: %v", err)
	}
	defer e.Stop()

	got := e.Peers()
	slices.Sort(got)
	want := []string{"https://a.example.com:2380", "https://b.example.com:2380"}
	if !slices.Equal(got, want) {
		t.Fatalf("Peers() = %v, want %v", got, want)
	}
}
