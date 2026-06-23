package v1alpha1

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v1 "github.com/cnuss/libetcd/v1"
)

// statefulSeed is an in-memory discovery seed implementing the wire protocol end
// to end: an atomic claim that mints a per-cluster secret on the first call, a
// roster, and the secret returned on claim/roster. It skips JWT verification
// (the resolver under test only forwards the bearer), namespacing everything
// under one cluster.
type statefulSeed struct {
	ts *httptest.Server

	mu     sync.Mutex
	claims int
	secret string
	roster map[string]string // id -> url
}

func newStatefulSeed(t *testing.T) *statefulSeed {
	t.Helper()
	s := &statefulSeed{roster: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc(discoveryWellKnown, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(validDescriptorJSON))
	})
	mux.HandleFunc("/claim", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.claims++
		if s.claims == 1 { // first caller wins and mints the cluster secret
			var b [16]byte
			_, _ = rand.Read(b[:])
			s.secret = hex.EncodeToString(b[:])
			_ = json.NewEncoder(w).Encode(map[string]any{"won": true, "secret": s.secret})
			return
		}
		w.WriteHeader(http.StatusConflict)
	})
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.roster[body["id"]] = body["url"]
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/roster", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		urls := make([]string, 0, len(s.roster))
		for _, u := range s.roster {
			urls = append(urls, u)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"urls": urls, "secret": s.secret})
	})
	s.ts = httptest.NewServer(mux)
	t.Cleanup(s.ts.Close)
	return s
}

// TestDiscoveryFormsCluster brings up three real nodes through one in-memory
// seed: the winner mints the cluster secret, the losers read it from the roster,
// and every node pins it as its etcd cluster token. They form one three-member
// cluster — which only works if the seed-vended secret (not the per-node JWT) is
// the join credential, the #120 invariant.
func TestDiscoveryFormsCluster(t *testing.T) {
	seed := newStatefulSeed(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const n = 3
	nodes := make([]v1.EtcdPeer, 0, n)
	for i := range n {
		e := From(seed.ts.URL).WithClusterToken("jwt").WithContext(ctx).WithDir(t.TempDir())
		if err := e.Join(); err != nil {
			t.Fatalf("node %d join: %v", i, err)
		}
		nodes = append(nodes, e)
		t.Cleanup(func() { _ = e.Stop() })
	}

	// Every node converged on the same three-member, all-voting cluster.
	for i, nd := range nodes {
		ml, err := nd.Self().MemberList(ctx)
		if err != nil {
			t.Fatalf("node %d members: %v", i, err)
		}
		if len(ml.Members) != n {
			t.Fatalf("node %d sees %d members, want %d", i, len(ml.Members), n)
		}
		st, err := nd.Self().Status(ctx, "")
		if err != nil {
			t.Fatalf("node %d status: %v", i, err)
		}
		if st.IsLearner {
			t.Fatalf("node %d still a learner; want voter", i)
		}
	}

	// The seed vended exactly one secret, used by all three (the second/third
	// claims lost and read it from the roster).
	seed.mu.Lock()
	defer seed.mu.Unlock()
	if seed.secret == "" || seed.claims < n || len(seed.roster) != n {
		t.Fatalf("seed state: secret=%q claims=%d roster=%d", seed.secret, seed.claims, len(seed.roster))
	}
}
