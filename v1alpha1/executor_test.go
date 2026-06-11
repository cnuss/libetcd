package v1alpha1_test

import (
	"context"
	"io"
	"net"
	"net/http"
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
	e.WithDir(t.TempDir()).WithClientServing(lc, nil).WithPeerServing(lp, nil)

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
}

// TestWithoutClientServing starts a headless node: Start binds and serves no
// client listener, the member registers no client URLs, the in-process Self
// client still works, and Voters — with no serving member anywhere — returns
// nil without poisoning the handle.
func TestWithoutClientServing(t *testing.T) {
	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithoutClientServing()
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if e.ClientListener() != nil {
		t.Error("ClientListener bound despite WithoutClientServing")
	}
	if e.PeerListener() == nil {
		t.Error("PeerListener nil; the peer side should still auto-bind")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := e.Self()
	if cli == nil {
		t.Fatal("Self nil on a headless node; the in-process client needs no listener")
	}
	if _, err := cli.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put via Self: %v", err)
	}
	resp, err := cli.Get(ctx, "k")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("Get via Self = %v, %v; want value %q", resp, err, "v")
	}

	ml, err := cli.MemberList(ctx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	if len(ml.Members) != 1 || len(ml.Members[0].ClientURLs) != 0 {
		t.Errorf("members = %+v; want a single member with no client URLs", ml.Members)
	}

	if v := e.Voters(); v != nil {
		t.Error("Voters non-nil with no serving member anywhere")
	}
	// And the nil Voters above must not have latched an error: Self still works.
	if _, err := e.Self().Get(ctx, "k"); err != nil {
		t.Errorf("Self after Voters: %v (handle poisoned?)", err)
	}
}

// TestWithoutPeerServing starts a node whose raft transport is caller-owned:
// libetcd binds and serves no peer listener, the advertise-peer-URL is the
// caller's address (so the membership reports it), and the peer protocol works
// once the caller mounts PeerHandler on the PeerPaths of their own server.
func TestWithoutPeerServing(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithoutPeerServing(lis.Addr().String())
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if e.PeerListener() != nil {
		t.Error("PeerListener bound despite WithoutPeerServing")
	}

	want := "http://" + lis.Addr().String()
	if peers := e.Peers(); len(peers) != 1 || peers[0] != want {
		t.Errorf("Peers() = %v, want [%s] (the caller-owned server's address)", peers, want)
	}

	// The caller serves the raft transport: mount the peer handler on the raft
	// paths of an owned mux, post-Start.
	mux := http.NewServeMux()
	ph := e.PeerHandler()
	for _, p := range e.PeerPaths() {
		mux.Handle(p, ph)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Close()

	if got := httpGet(t, lis.Addr().String(), "/version"); got == "" {
		t.Error("/version empty; peer protocol not served on the caller-owned server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.Self().Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// TestServingOptOutLastCallWins checks a later WithClientServing with a real
// listener overrides an earlier WithoutClientServing (last call wins).
func TestServingOptOutLastCallWins(t *testing.T) {
	lc, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	e := v1alpha1.New()
	e.WithDir(t.TempDir()).WithoutClientServing()
	e.WithClientServing(lc, nil)
	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop()

	if e.ClientListener() != lc {
		t.Error("ClientListener is not the provided listener; WithClientServing should win over the earlier opt-out")
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

// TestWithPeerServing exercises the ternary nature of WithPeerServing across the
// listener {nil, provided} x server {nil, provided} x server-handler {nil,
// provided} matrix. For each combination it asserts the listener/server are
// honored and that the peer (raft) protocol stays served — and, when a custom
// handler is supplied, that raft paths still reach the peer handler while other
// paths fall through to the supplied handler (the single-port merge).
func TestWithPeerServing(t *testing.T) {
	const appBody = "APP-OK"
	appHandler := func() http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, appBody)
		})
	}

	cases := []struct {
		name           string
		provideLis     bool
		provideSrv     bool
		provideHandler bool
	}{
		{"nil_lis__nil_srv", false, false, false},
		{"lis__nil_srv", true, false, false},
		{"nil_lis__srv_no_handler", false, true, false},
		{"lis__srv_no_handler", true, true, false},
		{"nil_lis__srv_with_handler", false, true, true},
		{"lis__srv_with_handler", true, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var lis net.Listener
			if tc.provideLis {
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("listen: %v", err)
				}
				lis = l
			}
			var srv *http.Server
			if tc.provideSrv {
				srv = &http.Server{}
				if tc.provideHandler {
					srv.Handler = appHandler()
				}
			}

			e := v1alpha1.New()
			e.WithDir(t.TempDir()).WithPeerServing(lis, srv)
			if err := e.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer e.Stop()

			// Listener: the provided one is retained; otherwise Start auto-bound one.
			pl := e.PeerListener()
			if pl == nil {
				t.Fatal("PeerListener nil after Start")
			}
			if tc.provideLis && pl != lis {
				t.Errorf("PeerListener = %v, want the provided listener", pl.Addr())
			}

			// Server: the provided one is reused as-is; its Handler is always
			// resolved to something (raft handler, or the merge mux).
			if tc.provideSrv && e.PeerHTTP() != srv {
				t.Error("PeerHTTP did not return the provided server")
			}
			if e.PeerHTTP().Handler == nil {
				t.Error("PeerHTTP server has a nil Handler")
			}

			// Node is healthy: round-trip a key through the loopback client.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := e.Self().Put(ctx, "k", "v"); err != nil {
				t.Fatalf("Put: %v", err)
			}

			// The peer (raft) protocol is served on the listener in every case:
			// /version is a PeerPath, so it reaches the peer handler, not the app.
			addr := pl.Addr().String()
			if got := httpGet(t, addr, "/version"); got == "" || got == appBody {
				t.Errorf("/version = %q; want the peer handler's version response", got)
			}

			// A non-raft path reaches the supplied handler only when one was given.
			got := httpGet(t, addr, "/__app__")
			if tc.provideHandler && got != appBody {
				t.Errorf("/__app__ = %q, want %q (merge should route non-raft paths to the app handler)", got, appBody)
			}
			if !tc.provideHandler && got == appBody {
				t.Errorf("/__app__ = %q with no app handler; peer handler should own all paths", got)
			}
		})
	}
}
