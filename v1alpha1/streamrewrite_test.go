package v1alpha1

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
)

type fakeRT struct{ code int }

func (f fakeRT) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: f.code, Status: "x", Body: http.NoBody}, nil
}

// TestStreamAccept206RewritesStreamPath: a 206 on /raft/stream becomes 200.
func TestStreamAccept206RewritesStreamPath(t *testing.T) {
	rt := streamAccept206{inner: fakeRT{code: http.StatusPartialContent}}
	resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, rafthttp.RaftStreamPrefix+"/message/1", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (rewritten from 206)", resp.StatusCode)
	}
}

// TestStreamAccept206LeavesOffPath: a 206 off the stream path is untouched.
func TestStreamAccept206LeavesOffPath(t *testing.T) {
	rt := streamAccept206{inner: fakeRT{code: http.StatusPartialContent}}
	resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "/members", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206 (off the stream path)", resp.StatusCode)
	}
}

// TestServerInterceptsStreamRt is the layout guard: it mints a real server (which
// runs interceptRaftStream) and walks the same reflection path to confirm the
// raft stream RoundTripper is now wrapped. If a future etcd bump renames or
// restructures any hop (EtcdServer.r → raftNodeConfig.transport → Transport.streamRt),
// this fails loudly in CI instead of silently dropping the 206 rewrite at a
// consumer's runtime.
func TestServerInterceptsStreamRt(t *testing.T) {
	e := newImpl()
	e.WithDir(t.TempDir())
	srv := e.Server()
	if srv == nil {
		t.Fatal("nil server")
	}
	t.Cleanup(func() { _ = e.Stop() })

	transport := unexportedField(reflect.ValueOf(srv).Elem().FieldByName("r"), "transport")
	if !transport.IsValid() {
		t.Fatal("no raftNode.transport field — etcd internal layout changed")
	}
	rt, ok := transport.Interface().(*rafthttp.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *rafthttp.Transport", transport.Interface())
	}
	streamRt := unexportedField(reflect.ValueOf(rt).Elem(), "streamRt")
	if !streamRt.IsValid() {
		t.Fatal("no Transport.streamRt field — etcd internal layout changed")
	}
	if _, ok := streamRt.Interface().(streamAccept206); !ok {
		t.Fatalf("streamRt = %T, want streamAccept206 (intercept failed / layout changed)", streamRt.Interface())
	}
}
