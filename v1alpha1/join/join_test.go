package join_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd/v1alpha1"
	"github.com/cnuss/libetcd/v1alpha1/join"
	"github.com/cnuss/libetcd/v1alpha1/lock"
)

const testToken = "test-cluster-token"

// noopAcquire is a lock that does nothing — the auth/dispatch tests never reach
// a real reconfig, so they don't need cluster coordination.
func noopAcquire(context.Context, *clientv3.Client) (func() error, error) {
	return func() error { return nil }, nil
}

// TestAuthAndDispatch covers the wire gate without a live node: token
// rejection, the not-ready (nil client) path, and method dispatch. The token
// is checked before the in-process client is touched, so Self can be nil here.
func TestAuthAndDispatch(t *testing.T) {
	srv := &join.Server{
		Self:    func() *clientv3.Client { return nil },
		Token:   testToken,
		Acquire: noopAcquire,
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	post := func(token string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+join.Path,
			strings.NewReader("peerURLs=http://127.0.0.1:32380"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set(join.TokenHeader, token)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		return resp
	}

	t.Run("wrong token forbidden", func(t *testing.T) {
		resp := post("nope")
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
	})

	t.Run("missing token forbidden", func(t *testing.T) {
		resp := post("")
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
	})

	t.Run("correct token but server not ready", func(t *testing.T) {
		resp := post(testToken)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
	})

	t.Run("unknown method not allowed", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+join.Path, nil)
		req.Header.Set(join.TokenHeader, testToken)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", resp.StatusCode)
		}
	})
}

// TestRoundTrip drives the protocol against a real single node: Add registers a
// learner and streams a usable snapshot; Remove is idempotent; Promote of an
// unknown member is permanent while a real-but-not-ready learner is retryable.
func TestRoundTrip(t *testing.T) {
	node := v1alpha1.New()
	node.WithDir(t.TempDir()).WithClusterToken(testToken)
	if err := node.Start(); err != nil {
		t.Fatalf("node Start: %v", err)
	}
	t.Cleanup(func() { _ = node.Stop() })

	srv := &join.Server{
		Self:  node.Self,
		Token: testToken,
		Acquire: func(ctx context.Context, cli *clientv3.Client) (func() error, error) {
			lk, err := lock.Acquire(ctx, cli, "peer-join")
			if err != nil {
				return nil, err
			}
			return lk.Release, nil
		},
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	cli := &join.Client{HTTP: ts.Client(), Token: testToken}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const learnerURL = "http://127.0.0.1:32380"
	add, err := cli.Add(ctx, ts.URL, []string{learnerURL})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	defer add.Snapshot.Close()

	if add.SelfID == 0 || add.ClusterID == 0 {
		t.Errorf("Add result: SelfID=%d ClusterID=%d, want both non-zero", add.SelfID, add.ClusterID)
	}
	// Membership: the bootstrap voter plus the new learner advertising our URL.
	var sawLearner bool
	for _, m := range add.Members {
		if m.IsLearner {
			for _, u := range m.PeerURLs {
				if u == learnerURL {
					sawLearner = true
				}
			}
		}
	}
	if !sawLearner {
		t.Errorf("Add membership %+v missing the learner with %s", add.Members, learnerURL)
	}
	// Snapshot body is a real, non-empty db stream.
	n, err := io.Copy(io.Discard, add.Snapshot)
	if err != nil {
		t.Fatalf("reading snapshot: %v", err)
	}
	if n == 0 {
		t.Error("snapshot body empty")
	}

	// Promote of an unknown member is permanent (404 -> ErrPermanent).
	if err := cli.Promote(ctx, ts.URL, 0xdeadbeef); !errors.Is(err, join.ErrPermanent) {
		t.Errorf("Promote(unknown) = %v, want ErrPermanent", err)
	}

	// Promote of the real learner: it never started, so it is not ready —
	// retryable, not permanent.
	if err := cli.Promote(ctx, ts.URL, add.SelfID); err == nil || errors.Is(err, join.ErrPermanent) {
		t.Errorf("Promote(unready learner) = %v, want a retryable error", err)
	}

	// Remove the learner, then removing it again is idempotent success.
	if err := cli.Remove(ctx, ts.URL, add.SelfID); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := cli.Remove(ctx, ts.URL, add.SelfID); err != nil {
		t.Errorf("Remove (idempotent) = %v, want nil", err)
	}
}
