package v0alpha0

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

const validDescriptorJSON = `{"discovery":"v1","token":"/token","userinfo":"/userinfo","claim":"/claim","register":"/register","roster":"/roster"}`

// TestProbeSeed: a valid descriptor is recognized; everything else falls through
// as "not a seed".
func TestProbeSeed(t *testing.T) {
	cases := []struct {
		name string
		code int
		body string
		want bool
	}{
		{"valid", 200, validDescriptorJSON, true},
		{"valid without token", 200, `{"discovery":"v1","userinfo":"/u","claim":"/c","register":"/r","roster":"/ro"}`, true},
		{"not found", 404, "", false},
		{"server error", 500, "", false},
		{"junk json", 200, "not json", false},
		{"missing fields", 200, `{"discovery":"v1"}`, false},
		{"missing userinfo", 200, `{"discovery":"v1","claim":"/c","register":"/r","roster":"/ro"}`, false},
		{"empty version", 200, `{"discovery":"","claim":"/c","register":"/r","roster":"/ro"}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != discoveryWellKnown {
					w.WriteHeader(404)
					return
				}
				if r.Header.Get("Cache-Control") != "no-cache" {
					t.Errorf("probe missing Cache-Control: no-cache")
				}
				w.WriteHeader(c.code)
				_, _ = w.Write([]byte(c.body))
			}))
			defer ts.Close()
			_, ok := probeSeed(context.Background(), ts.URL, ts.Client())
			if ok != c.want {
				t.Fatalf("probeSeed ok=%v, want %v", ok, c.want)
			}
		})
	}
}

// TestProbeSeedUnreachable: a dead address is "not a seed", not an error.
func TestProbeSeedUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, ok := probeSeed(ctx, "http://127.0.0.1:1", http.DefaultClient); ok {
		t.Fatal("probeSeed ok=true for an unreachable address")
	}
}

// mockSeed is an httptest discovery seed that records what the client sent and
// returns scripted responses.
type mockSeed struct {
	ts          *httptest.Server
	claimStatus int

	mu           sync.Mutex
	sub          string // the sub /userinfo vends for the presented bearer
	gotAuth      string
	gotNoCache   bool
	registerBody map[string]string
	rosterURLs   []string
	rosterHits   int
}

func newMockSeed(t *testing.T, claimStatus int) *mockSeed {
	t.Helper()
	m := &mockSeed{claimStatus: claimStatus, sub: "test-sub"}
	mux := http.NewServeMux()
	mux.HandleFunc(discoveryWellKnown, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(validDescriptorJSON))
	})
	// userinfo verifies the bearer and returns the full payload (sub among the
	// claims), mirroring the live seed.
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		m.record(r)
		m.mu.Lock()
		sub := m.sub
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"sub": sub, "iss": "mock", "aud": "disco"})
	})
	mux.HandleFunc("/claim", func(w http.ResponseWriter, r *http.Request) {
		m.record(r)
		w.WriteHeader(m.claimStatus)
	})
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		m.record(r)
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		m.mu.Lock()
		m.registerBody = body
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/roster", func(w http.ResponseWriter, r *http.Request) {
		m.record(r)
		m.mu.Lock()
		m.rosterHits++
		urls := m.rosterURLs
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string][]string{"urls": urls})
	})
	m.ts = httptest.NewServer(mux)
	t.Cleanup(m.ts.Close)
	return m
}

func (m *mockSeed) record(r *http.Request) {
	m.mu.Lock()
	m.gotAuth = r.Header.Get("Authorization")
	m.gotNoCache = r.Header.Get("Cache-Control") == "no-cache"
	m.mu.Unlock()
}

func (m *mockSeed) seed(token string) *discoverySeed {
	s, ok := probeSeed(context.Background(), m.ts.URL, m.ts.Client())
	if !ok {
		panic("mock seed did not probe as a seed")
	}
	s.token = token
	return s
}

// TestSeedUserinfo: userinfo verifies the bearer (carries it) and returns the
// sub from the payload; userinfoURL is the absolute endpoint /join delegates to.
func TestSeedUserinfo(t *testing.T) {
	m := newMockSeed(t, 200)
	m.sub = "cluster-sub-123"
	s := m.seed("jwt-xyz")

	got, err := s.userinfo(context.Background())
	if err != nil {
		t.Fatalf("userinfo: %v", err)
	}
	if got != m.sub {
		t.Fatalf("userinfo sub = %q, want %q", got, m.sub)
	}
	m.mu.Lock()
	auth, noCache := m.gotAuth, m.gotNoCache
	m.mu.Unlock()
	if auth != "Bearer jwt-xyz" || !noCache {
		t.Fatalf("userinfo headers: auth=%q no-cache=%v", auth, noCache)
	}
	if want := m.ts.URL + "/userinfo"; s.userinfoURL() != want {
		t.Fatalf("userinfoURL = %q, want %q", s.userinfoURL(), want)
	}
}

// TestSeedUserinfoEmptySub fails closed when the seed returns no sub.
func TestSeedUserinfoEmptySub(t *testing.T) {
	m := newMockSeed(t, 200)
	m.sub = ""
	if _, err := m.seed("jwt").userinfo(context.Background()); err == nil {
		t.Fatal("userinfo: want error on empty sub")
	}
}

// TestSeedClaim maps 200 -> won, 409 -> lost, and carries the bearer + no-cache.
func TestSeedClaim(t *testing.T) {
	for _, c := range []struct {
		status int
		won    bool
	}{{200, true}, {409, false}} {
		m := newMockSeed(t, c.status)
		won, err := m.seed("jwt-abc").claim(context.Background())
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if won != c.won {
			t.Fatalf("status %d: won=%v, want %v", c.status, won, c.won)
		}
		m.mu.Lock()
		if m.gotAuth != "Bearer jwt-abc" || !m.gotNoCache {
			t.Fatalf("claim headers: auth=%q no-cache=%v", m.gotAuth, m.gotNoCache)
		}
		m.mu.Unlock()
	}
}

// TestSeedClaimError surfaces an unexpected status.
func TestSeedClaimError(t *testing.T) {
	m := newMockSeed(t, 500)
	if _, err := m.seed("jwt").claim(context.Background()); err == nil {
		t.Fatal("claim: want error on 500")
	}
}

// TestSeedRegisterRoster: register posts {id,url}; roster returns the urls.
func TestSeedRegisterRoster(t *testing.T) {
	m := newMockSeed(t, 200)
	m.rosterURLs = []string{"http://a:2380", "http://b:2380"}
	s := m.seed("jwt")

	if err := s.register(context.Background(), "node-1", "http://a:2380"); err != nil {
		t.Fatalf("register: %v", err)
	}
	m.mu.Lock()
	got := m.registerBody
	m.mu.Unlock()
	if got["id"] != "node-1" || got["url"] != "http://a:2380" {
		t.Fatalf("register body = %v", got)
	}

	urls, err := s.roster(context.Background())
	if err != nil {
		t.Fatalf("roster: %v", err)
	}
	if len(urls) != 2 || urls[0] != "http://a:2380" {
		t.Fatalf("roster = %v", urls)
	}
}

// TestRosterWait polls until the roster is non-empty.
func TestRosterWait(t *testing.T) {
	m := newMockSeed(t, 200)
	s := m.seed("jwt")
	go func() {
		time.Sleep(1500 * time.Millisecond)
		m.mu.Lock()
		m.rosterURLs = []string{"http://late:2380"}
		m.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	urls, err := s.rosterWait(ctx)
	if err != nil {
		t.Fatalf("rosterWait: %v", err)
	}
	if len(urls) != 1 || urls[0] != "http://late:2380" {
		t.Fatalf("rosterWait = %v", urls)
	}
}

// TestRosterWaitDeadline errors when the roster stays empty.
func TestRosterWaitDeadline(t *testing.T) {
	m := newMockSeed(t, 200) // rosterURLs stays empty
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := m.seed("jwt").rosterWait(ctx); err == nil {
		t.Fatal("rosterWait: want error on empty-until-deadline")
	}
}

// TestSeedFromPeersMiss: a non-seed URL (404 on the well-known) doesn't sniff as
// a seed, so Join falls through to the plain-peer path.
func TestSeedFromPeersMiss(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
	}))
	defer ts.Close()
	pj := From(ts.URL).(*peerJoiner)
	if _, ok := pj.seedFromPeers(); ok {
		t.Fatal("seedFromPeers ok=true for a non-seed URL")
	}
}

// TestSeedFromPeersMultiPeerSkips: discovery is gated on exactly one URL, so a
// multi-peer set is never probed — even if one entry is a real seed it's treated
// as a plain peer list.
func TestSeedFromPeersMultiPeerSkips(t *testing.T) {
	probed := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == discoveryWellKnown {
			probed = true
			_, _ = w.Write([]byte(validDescriptorJSON))
			return
		}
		w.WriteHeader(404)
	}))
	defer ts.Close()

	pj := From(ts.URL, "http://other:2380").(*peerJoiner)
	if _, ok := pj.seedFromPeers(); ok {
		t.Fatal("seedFromPeers ok=true for a multi-URL set")
	}
	if probed {
		t.Fatal("multi-URL set was probed; discovery must be gated on len==1")
	}
}

// TestJoinViaDiscoveryBootstrap: when claim is won, the node bootstraps a fresh
// cluster and registers its advertised peer URL with the seed (exercising the
// sniff -> claim -> Start -> register path and the Stop teardown of keepalive).
func TestJoinViaDiscoveryBootstrap(t *testing.T) {
	m := newMockSeed(t, 200) // claim won
	m.sub = "repo:cnuss/libetcd:ref:refs/heads/main"

	ctx := t.Context()
	e := From(m.ts.URL).
		WithClusterToken("jwt-cluster").
		WithContext(ctx).
		WithDir(t.TempDir())

	if err := e.Join(); err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer e.Stop()

	// Discovery (JWT) mode pinned the verified sub as the cluster token, stashed
	// the JWT as the join credential, and recorded the seed's userinfo URL — so
	// this node's own /join now verifies joiners against sub, not the raw JWT.
	pj := e.(*peerJoiner)
	pj.mu.Lock()
	gotToken, gotCred, gotUserinfo := pj.cfg.InitialClusterToken, pj.joinCredential, pj.joinUserinfo
	pj.mu.Unlock()
	if gotToken != m.sub {
		t.Fatalf("cluster token = %q, want sub %q", gotToken, m.sub)
	}
	if gotCred != "jwt-cluster" {
		t.Fatalf("join credential = %q, want the JWT", gotCred)
	}
	if want := m.ts.URL + "/userinfo"; gotUserinfo != want {
		t.Fatalf("join userinfo = %q, want %q", gotUserinfo, want)
	}

	// It bootstrapped: a single-member cluster reachable in-process.
	ml, err := e.Self().MemberList(ctx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	if len(ml.Members) != 1 {
		t.Fatalf("members = %d, want 1 (bootstrap)", len(ml.Members))
	}

	// It registered its advertised peer URL (claim used the bearer too).
	m.mu.Lock()
	body, auth := m.registerBody, m.gotAuth
	m.mu.Unlock()
	if auth != "Bearer jwt-cluster" {
		t.Fatalf("seed auth = %q", auth)
	}
	if body == nil || body["url"] == "" || body["url"] != body["id"] {
		t.Fatalf("register body = %v", body)
	}
}

// clusterSeed is a stateful in-memory discovery seed: claim elects exactly one
// bootstrapper, register/roster accumulate live peers, and userinfo vends one
// fixed sub for any bearer. It's enough to drive real nodes through the full
// discovery + JWT-verified join in-process.
type clusterSeed struct {
	ts  *httptest.Server
	sub string

	mu      sync.Mutex
	claimed bool
	roster  []string
}

func newClusterSeed(t *testing.T, sub string) *clusterSeed {
	t.Helper()
	c := &clusterSeed{sub: sub}
	mux := http.NewServeMux()
	mux.HandleFunc(discoveryWellKnown, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(validDescriptorJSON))
	})
	// userinfo verifies the bearer (any non-empty token here) and returns the
	// one cluster sub — every node bearing a token for this cluster shares it,
	// regardless of which distinct JWT it actually presents.
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sub": c.sub, "iss": "mock"})
	})
	mux.HandleFunc("/claim", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.claimed {
			w.WriteHeader(http.StatusConflict) // a winner already holds it
			return
		}
		c.claimed = true
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		c.mu.Lock()
		if url := body["url"]; url != "" && !slices.Contains(c.roster, url) {
			c.roster = append(c.roster, url)
		}
		c.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/roster", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		urls := append([]string(nil), c.roster...)
		c.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string][]string{"urls": urls})
	})
	c.ts = httptest.NewServer(mux)
	t.Cleanup(c.ts.Close)
	return c
}

// TestDiscoveryFormsClusterJWT is the #122 end-to-end: two nodes carry distinct
// JWTs but the seed maps both to the same sub. The first wins the claim and
// bootstraps; the second loses, reads the roster, and joins over the peer
// transport. The bootstrapper's /join forwards the joiner's (different) JWT to
// the seed's userinfo, gets back the shared sub, and admits it — proving a raw
// per-node token never has to match, only the verified sub.
func TestDiscoveryFormsClusterJWT(t *testing.T) {
	seed := newClusterSeed(t, "repo:cnuss/libetcd:ref:refs/heads/main")
	ctx := t.Context()

	// Node A: wins the claim, bootstraps, serves /join in JWT mode.
	a := From(seed.ts.URL).
		WithClusterToken("jwt-node-A").
		WithContext(ctx).
		WithDir(t.TempDir())
	if err := a.Join(); err != nil {
		t.Fatalf("node A Join: %v", err)
	}
	defer a.Stop()

	// Node B: a *different* JWT, same cluster. Loses the claim and joins A.
	b := From(seed.ts.URL).
		WithClusterToken("jwt-node-B").
		WithContext(ctx).
		WithDir(t.TempDir())
	if err := b.Join(); err != nil {
		t.Fatalf("node B Join (different JWT, same sub): %v", err)
	}
	defer b.Stop()

	// Both members are in the cluster A bootstrapped.
	ml, err := a.Self().MemberList(ctx)
	if err != nil {
		t.Fatalf("MemberList: %v", err)
	}
	if len(ml.Members) != 2 {
		t.Fatalf("members = %d, want 2 (A bootstrapped, B joined via JWT verify)", len(ml.Members))
	}

	// Both pinned the same verified sub as the cluster token; neither kept its
	// raw JWT there.
	for name, pj := range map[string]*peerJoiner{"A": a.(*peerJoiner), "B": b.(*peerJoiner)} {
		pj.mu.Lock()
		tok := pj.cfg.InitialClusterToken
		pj.mu.Unlock()
		if tok != seed.sub {
			t.Fatalf("node %s cluster token = %q, want sub %q", name, tok, seed.sub)
		}
	}

	// Auto-leave: stopping a discovery node removes it from membership (it's
	// ephemeral), so A's view shrinks to one. Quorum follows the live set instead
	// of wedging on a stopped-but-still-counted voter — without auto-leave, A
	// would be 1-of-2 with no quorum and MemberList would hang/stay at 2.
	if err := b.Stop(); err != nil {
		t.Fatalf("stop B: %v", err)
	}
	members := -1
	for range 30 {
		mlctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ml, err := a.Self().MemberList(mlctx)
		cancel()
		if err == nil {
			if members = len(ml.Members); members == 1 {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if members != 1 {
		t.Fatalf("after B auto-leave on Stop, A sees %d members, want 1", members)
	}
}
