package stream

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
)

// --- serve side: Handler rewrites 200 -> 206 on the stream path ---

func TestHandlerRewritesStreamPath(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, rafthttp.RaftStreamPrefix+"/message/1", nil))
	if rec.Code != http.StatusPartialContent {
		t.Errorf("status = %d, want 206 (rewritten from 200)", rec.Code)
	}
}

func TestHandlerLeavesOffPath(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/members", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (off the stream path)", rec.Code)
	}
}

// --- dial side: accept206 rewrites 206 -> 200 on the stream path ---

type fakeRT struct{ code int }

func (f fakeRT) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: f.code, Status: "x", Body: http.NoBody}, nil
}

func TestAccept206RewritesStreamPath(t *testing.T) {
	rt := accept206{inner: fakeRT{code: http.StatusPartialContent}}
	resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, rafthttp.RaftStreamPrefix+"/message/1", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (rewritten from 206)", resp.StatusCode)
	}
}

func TestAccept206LeavesOffPath(t *testing.T) {
	rt := accept206{inner: fakeRT{code: http.StatusPartialContent}}
	resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "/members", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206 (off the stream path)", resp.StatusCode)
	}
}
