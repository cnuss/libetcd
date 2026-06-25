package v1alpha1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

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
		if url := body["url"]; url != "" && !contains(c.roster, url) {
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

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
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
}
