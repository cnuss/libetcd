package v1alpha1

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

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

// TestJoinFailsFastOnLoopbackMismatch: with no WithPeerServing the advertise
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
