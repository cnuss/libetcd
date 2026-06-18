package v1alpha1

import (
	"context"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/cnuss/libetcd/v1"
)

// TestEnvPeers covers parsing the LIBETCD_PEERS value: empty, CSV, JSON array,
// malformed JSON falling back to CSV, and whitespace/empty-entry trimming. The
// result is not otherwise normalized — sanitizePeers does that in Join.
func TestEnvPeers(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{"single", "a:2380", []string{"a:2380"}},
		{"csv", "a:2380,https://b:2380,c:2380", []string{"a:2380", "https://b:2380", "c:2380"}},
		{"csv trims + drops empties", " a:2380 , ,b:2380,", []string{"a:2380", "b:2380"}},
		{"json array", `["a:2380","https://b:2380"]`, []string{"a:2380", "https://b:2380"}},
		{"json array trims + drops empties", `[" a:2380 ","",  "b:2380"]`, []string{"a:2380", "b:2380"}},
		{"malformed json falls back to csv", "[a:2380,b:2380", []string{"[a:2380", "b:2380"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := envPeers(tc.in); !slices.Equal(got, tc.want) {
				t.Fatalf("envPeers(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestJoinFoldsEnvPeers: Join unions LIBETCD_PEERS into its targets, so From()
// with no arguments but the env set is a join, not a bootstrap. The env peer is
// non-loopback while the auto-bound advertise URL is loopback, so Join fails
// fast with the loopback-mismatch error — proving the env peer drove a join
// (a bootstrap would have returned nil) and reached the reachability check.
func TestJoinFoldsEnvPeers(t *testing.T) {
	t.Setenv(PeersEnv, "10.255.255.1:2380")
	p := From() // no arguments; the peer comes from LIBETCD_PEERS
	p.WithDir(t.TempDir())
	t.Cleanup(func() { _ = p.Stop() })

	start := time.Now()
	err := p.Join()
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Join = %v, want loopback-mismatch (env peer folded into a join, not a bootstrap)", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("Join took %v, want fail-fast", elapsed)
	}
}

// TestBootstrapRace brings up three nodes with identical config — every node
// gets the same peer set (its own advertised loopback URL included) and calls
// Join concurrently. No node is special-cased: the lowest URL bootstraps, the
// other two join it, and they converge on one three-member voting cluster. This
// is the uniform-config path end to end (in-process, loopback, no tunnels).
func TestBootstrapRace(t *testing.T) {
	const n = 3
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Bind every peer listener first so all URLs are known up front, then hand
	// the same set to all three nodes.
	lis := make([]net.Listener, n)
	urls := make([]string, n)
	for i := range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[i] = l
		urls[i] = "http://" + l.Addr().String()
	}

	nodes := make([]v1.EtcdPeer, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		nodes[i] = From(urls...).WithDir(t.TempDir()).WithPeerListener(lis[i]).WithContext(ctx)
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = nodes[i].Join() }(i)
	}
	wg.Wait()
	t.Cleanup(func() {
		for _, nd := range nodes {
			if nd != nil {
				_ = nd.Stop()
			}
		}
	})

	for i, e := range errs {
		if e != nil {
			t.Fatalf("node %d (%s) Join: %v", i, urls[i], e)
		}
	}

	// Every node sees the same three-member, all-voting cluster.
	for i, nd := range nodes {
		ml, err := nd.Self().MemberList(ctx)
		if err != nil {
			t.Fatalf("node %d MemberList: %v", i, err)
		}
		if len(ml.Members) != n {
			t.Fatalf("node %d sees %d members, want %d", i, len(ml.Members), n)
		}
		st, err := nd.Self().Status(ctx, "")
		if err != nil {
			t.Fatalf("node %d Status: %v", i, err)
		}
		if st.IsLearner {
			t.Errorf("node %d (%s) still a learner; want voter", i, urls[i])
		}
	}
}

// TestCanonicalPeerURL covers the membership-comparison normalization: default
// scheme, lowercase host, default-port fill, path drop, and rejection of
// non-http(s)/hostless input.
func TestCanonicalPeerURL(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"   ", ""},
		{"a:2380", "http://a:2380"},
		{"http://a:2380", "http://a:2380"},
		{"https://b", "https://b:443"},
		{"http://b", "http://b:80"},
		{"https://b:2380/", "https://b:2380"},
		{"HTTP://Host.Example:2380", "http://host.example:2380"},
		{"https://x.trycloudflare.com", "https://x.trycloudflare.com:443"},
		{"ftp://x:2380", ""},
		{"http://", ""},
	}
	for _, tc := range cases {
		if got := canonicalPeerURL(tc.in); got != tc.want {
			t.Errorf("canonicalPeerURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestBootstrapRole covers the uniform-config election: not-in-set is an
// ordinary join (inactive), the canonical-minimum self bootstraps, every other
// self joins, and canonicalization makes the self-vs-set match robust across URL
// forms (port-fill, case).
func TestBootstrapRole(t *testing.T) {
	cases := []struct {
		name          string
		peers         []string
		self          []string
		wantBootstrap bool
		wantActive    bool
	}{
		{"self not in set: ordinary join", []string{"a:2380", "b:2380"}, []string{"c:2380"}, false, false},
		{"no self urls: inactive", []string{"a:2380", "b:2380"}, nil, false, false},
		{"self is the minimum: bootstrap", []string{"c:2380", "a:2380", "b:2380"}, []string{"a:2380"}, true, true},
		{"self not minimum: join", []string{"c:2380", "a:2380", "b:2380"}, []string{"b:2380"}, false, true},
		{"single self-only set: bootstrap", []string{"a:2380"}, []string{"a:2380"}, true, true},
		{"canonical match across forms (port-fill)", []string{"https://b:443", "https://a"}, []string{"https://a:443"}, true, true},
		{"canonical match across case", []string{"http://z:2380", "http://A:2380"}, []string{"http://a:2380"}, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bootstrap, active := bootstrapRole(tc.peers, tc.self)
			if bootstrap != tc.wantBootstrap || active != tc.wantActive {
				t.Fatalf("bootstrapRole(%q, %q) = (bootstrap %v, active %v), want (%v, %v)",
					tc.peers, tc.self, bootstrap, active, tc.wantBootstrap, tc.wantActive)
			}
		})
	}
}

// TestRemoveSelf: a joiner drops its own URL (by canonical match) from the dial
// set while keeping the others in their original form.
func TestRemoveSelf(t *testing.T) {
	got := removeSelf([]string{"a:2380", "http://b:2380", "https://c:443"}, []string{"http://b:2380"})
	if want := []string{"a:2380", "https://c:443"}; !slices.Equal(got, want) {
		t.Fatalf("removeSelf = %q, want %q", got, want)
	}
	// Canonical match: self given without scheme/port still removes the entry.
	got = removeSelf([]string{"https://b:443", "https://c:443"}, []string{"https://b"})
	if want := []string{"https://c:443"}; !slices.Equal(got, want) {
		t.Fatalf("removeSelf (canonical) = %q, want %q", got, want)
	}
}

// TestSanitizePeers covers the normalization From applies to a caller's peer
// URLs: trim, drop-empty, default-scheme, dedup, the preserved first-seen
// order, and the reporting of dropped unparseable entries (empties and dups
// are removed without being reported).
func TestSanitizePeers(t *testing.T) {
	cases := []struct {
		name        string
		in          []string
		want        []string
		wantDropped []string
	}{
		{
			name: "bare host:port gets http scheme",
			in:   []string{"10.0.0.1:2380"},
			want: []string{"http://10.0.0.1:2380"},
		},
		{
			name: "trims surrounding whitespace",
			in:   []string{"  http://10.0.0.1:2380  ", "\thttp://10.0.0.2:2380\n"},
			want: []string{"http://10.0.0.1:2380", "http://10.0.0.2:2380"},
		},
		{
			name: "drops empty and whitespace-only entries",
			in:   []string{"", "   ", "http://10.0.0.1:2380"},
			want: []string{"http://10.0.0.1:2380"},
		},
		{
			name: "dedups, preserving first-seen order",
			in:   []string{"http://b:2380", "http://a:2380", "http://b:2380", "b:2380"},
			want: []string{"http://b:2380", "http://a:2380"},
		},
		{
			name: "trailing slash normalized away (dedups with bare)",
			in:   []string{"http://a:2380/", "http://a:2380"},
			want: []string{"http://a:2380"},
		},
		{
			name: "keeps https as-is",
			in:   []string{"https://a:2380"},
			want: []string{"https://a:2380"},
		},
		{
			name:        "drops bad entries, keeps the good ones",
			in:          []string{"ftp://a:2380", "http://", "://2380", "http://good:2380"},
			want:        []string{"http://good:2380"},
			wantDropped: []string{"ftp://a:2380", "http://", "://2380"},
		},
		{
			name:        "all-bad-or-empty yields empty",
			in:          []string{"", "  ", "ftp://x:2380", "http://"},
			want:        nil,
			wantDropped: []string{"ftp://x:2380", "http://"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, dropped := sanitizePeers(tc.in)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("sanitizePeers(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if !slices.Equal(dropped, tc.wantDropped) {
				t.Fatalf("sanitizePeers(%q) dropped %q, want %q", tc.in, dropped, tc.wantDropped)
			}
		})
	}
}

// TestIsLoopbackHost pins the loopback classification the pre-join
// reachability check builds on: names and IPs that are loopback, and the
// deliberate non-resolution of other hostnames.
func TestIsLoopbackHost(t *testing.T) {
	cases := []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"LOCALHOST", true},
		{"127.0.0.1", true},
		{"127.8.8.8", true},
		{"::1", true},
		{"10.0.0.1", false},
		{"192.168.1.10", false},
		{"example.com", false},
		{"etcd-0.internal", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isLoopbackHost(tc.host); got != tc.want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

// TestJoinFailsFastOnLatchedConfigError: a builder error latched before Join
// (here a bad log level) must fail Join immediately — before listeners are
// bound or the remote cluster is touched — not after the full join timeout.
func TestJoinFailsFastOnLatchedConfigError(t *testing.T) {
	p := From("10.0.0.1:2380").WithLog("not-a-level", nil)
	start := time.Now()
	err := p.Join()
	if err == nil || !strings.Contains(err.Error(), "configuration error") {
		t.Fatalf("Join = %v, want latched configuration error", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Join took %v, want fail-fast", elapsed)
	}
}

// TestJoinFailsFastWhenServerMinted: calling a client accessor before Join
// mints the server from the bootstrap config, which Join's config mutations
// can no longer reach; Join must reject the handle instead of failing slow.
func TestJoinFailsFastWhenServerMinted(t *testing.T) {
	// Not t.TempDir(): a minted-but-never-started server holds its WAL open
	// until process exit (the deferred WAL close lives in run(), which never
	// starts; Stop's Cleanup path doesn't reach it), and Windows can't delete
	// open files — t.TempDir's cleanup would fail the test. Best-effort
	// removal instead.
	dir, err := os.MkdirTemp("", "libetcd-minted-")
	if err != nil {
		t.Fatal(err)
	}
	p := From("10.0.0.1:2380")
	p.WithDir(dir)
	t.Cleanup(func() {
		_ = p.Stop()
		_ = os.RemoveAll(dir)
	})
	_ = p.Self() // accessor mints the server pre-Join

	err = p.Join()
	if err == nil || !strings.Contains(err.Error(), "already minted") {
		t.Fatalf("Join = %v, want already-minted error", err)
	}
}

// TestJoinHandleExhausted pins the single-use contract: once a failed join's
// rollback stopped a started server (latched by abortJoin), the handle refuses
// further Join calls immediately instead of re-adding a member to the remote
// cluster and spinning the whole budget against a server that can never start.
func TestJoinHandleExhausted(t *testing.T) {
	p := From("10.0.0.1:2380").(*peerJoiner)
	p.exhausted.Store(true) // what abortJoin does when started

	start := time.Now()
	err := p.Join()
	if err == nil || !strings.Contains(err.Error(), "cannot be reused") {
		t.Fatalf("Join = %v, want exhausted-handle error", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Join took %v, want immediate refusal", elapsed)
	}
}

// TestJoinFailsFastOnLoopbackMismatch: with no WithPeerListener the advertise
// peer URL is an auto-bound loopback address, which a remote (non-loopback)
// cluster can never dial back; Join must fail before the member-add rather
// than at the promote timeout.
func TestJoinFailsFastOnLoopbackMismatch(t *testing.T) {
	p := From("10.255.255.1:2380")
	p.WithDir(t.TempDir())
	t.Cleanup(func() { _ = p.Stop() })

	start := time.Now()
	err := p.Join()
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Join = %v, want loopback-mismatch error", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("Join took %v, want fail-fast before discovery", elapsed)
	}
}

// TestParseAdvertiseURLs covers the WithPeerListener advertise-URL
// normalization: a missing port is filled from the scheme, a path/trailing
// slash is dropped, entries that normalize to the same value dedup, the result
// is sorted, and unparseable/hostless entries are discarded.
func TestParseAdvertiseURLs(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"empty", nil, nil},
		{"fill default https port + drop path", []string{"https://h.example/"}, []string{"https://h.example:443"}},
		{"fill default http port", []string{"http://h.example"}, []string{"http://h.example:80"}},
		{"explicit port kept", []string{"http://h.example:2380"}, []string{"http://h.example:2380"}},
		{
			"dedup after normalize, sorted",
			[]string{"https://b.example", "https://a.example:443/", "https://a.example"},
			[]string{"https://a.example:443", "https://b.example:443"},
		},
		{"drop unparseable + hostless", []string{"://nope", "https://", "http://ok:7"}, []string{"http://ok:7"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := urlsToEndpoints(parseAdvertiseURLs(tc.in, nil))
			if !slices.Equal(got, tc.want) {
				t.Fatalf("parseAdvertiseURLs(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
