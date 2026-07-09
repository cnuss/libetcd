package stream

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
)

// --- serve side: Handler rewrites 200 -> 206 on the stream path, but only for
// dialers that negotiated it via acceptHeader ---

func TestHandlerRewritesNegotiatedStreamPath(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, rafthttp.RaftStreamPrefix+"/message/1", nil)
	req.Header.Set(acceptHeader, acceptValue)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent {
		t.Errorf("status = %d, want 206 (rewritten from 200)", rec.Code)
	}
}

// TestHandlerLeavesUnnegotiated is the regression guard for the join hang: a
// dialer that did NOT negotiate (stock etcd, or a reader whose first dial beat
// Intercept's swap) must get a stock 200 — a 206 would wedge it forever in
// streamReader.dial's GracefulClose, draining a stream body that never ends.
func TestHandlerLeavesUnnegotiated(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, rafthttp.RaftStreamPrefix+"/message/1", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (no negotiation header)", rec.Code)
	}
}

func TestHandlerLeavesOffPath(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/members", nil)
	req.Header.Set(acceptHeader, acceptValue)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (off the stream path)", rec.Code)
	}
}

// --- dial side: accept206 negotiates via acceptHeader and rewrites 206 -> 200
// on the stream path ---

// fakeRT records the request it saw and answers with a fixed status.
type fakeRT struct {
	code int
	seen *http.Request
}

func (f *fakeRT) RoundTrip(req *http.Request) (*http.Response, error) {
	f.seen = req
	return &http.Response{StatusCode: f.code, Status: "x", Body: http.NoBody}, nil
}

func TestAccept206NegotiatesAndRewritesStreamPath(t *testing.T) {
	inner := &fakeRT{code: http.StatusPartialContent}
	rt := accept206{inner: inner}
	orig := httptest.NewRequest(http.MethodGet, rafthttp.RaftStreamPrefix+"/message/1", nil)
	resp, err := rt.RoundTrip(orig)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (rewritten from 206)", resp.StatusCode)
	}
	if got := inner.seen.Header.Get(acceptHeader); got != acceptValue {
		t.Errorf("outgoing %s = %q, want %q (negotiation header stamped)", acceptHeader, got, acceptValue)
	}
	if orig.Header.Get(acceptHeader) != "" {
		t.Error("caller's request was mutated; RoundTrip must clone before stamping")
	}
}

func TestAccept206LeavesOffPath(t *testing.T) {
	inner := &fakeRT{code: http.StatusPartialContent}
	rt := accept206{inner: inner}
	resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "/members", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206 (off the stream path)", resp.StatusCode)
	}
	if got := inner.seen.Header.Get(acceptHeader); got != "" {
		t.Errorf("outgoing %s = %q, want unset off the stream path", acceptHeader, got)
	}
}
