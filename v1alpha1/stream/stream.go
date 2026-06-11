// Package stream carries the special handling that makes etcd's raft peer
// stream report HTTP 206 Partial Content on the wire while both (stock) ends
// still agree on 200 OK — issue #8. A buffering intermediary streams a 206 body
// instead of holding it, so this lets a peer stream pass cleanly through one.
//
// It has two halves that must be used together:
//
//   - Handler (serve side) wraps the peer handler so the stream's success status
//     goes out as 206 instead of 200.
//   - Intercept (dial side) wraps the reader's RoundTripper so that 206 is
//     rewritten back to 200 just before etcd's streamReader.dial, which accepts
//     only 200.
//
// The dial side uses reflection + unsafe to reach an unexported, uninjectable
// RoundTripper inside the etcd server; that field path is version-coupled and
// guarded by tests. See Intercept.
package stream

import (
	"net/http"
	"reflect"
	"strings"
	"unsafe"

	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"
)

// acceptHeader/acceptValue negotiate the 206 rewrite. The dial side (accept206)
// stamps the header on stream dials it can translate back; the serve side
// (Handler) rewrites 200 → 206 only for requests carrying it. A dialer without
// the interceptor — stock etcd, or a libetcd node whose reader fires its first
// dial before Intercept's swap lands — therefore gets a stock 200 stream
// instead of a 206 it would fatally mishandle: etcd's streamReader.dial treats
// 206 as an unhandled status and drains the response body via
// httputil.GracefulClose, and a raft stream body never ends, so that reader
// goroutine would hang forever and the peer link never form.
const (
	acceptHeader = "X-Libetcd-Stream"
	acceptValue  = "206"
)

// Handler wraps a peer (raft) handler so that, on the /raft/stream path only and
// only for dialers that negotiated it (acceptHeader), the long-lived stream's
// success status is rewritten from 200 OK to 206 Partial Content — the serve-side
// half of issue #8. The stream handler is the sole 200 on the peer mux; pipeline
// writes 204 and /members, /version, lease, etc. return real 200+body that must
// pass through untouched, hence the path scope. The dial side (Intercept)
// rewrites the 206 back to 200 before the stock reader.
func Handler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, rafthttp.RaftStreamPrefix) &&
			r.Header.Get(acceptHeader) == acceptValue {
			w = &statusRewriter{ResponseWriter: w}
		}
		inner.ServeHTTP(w, r)
	})
}

// statusRewriter rewrites a 200 OK status to 206 Partial Content. It preserves
// http.Flusher because rafthttp's streamHandler type-asserts the ResponseWriter
// to Flusher (and would panic without it).
type statusRewriter struct {
	http.ResponseWriter
}

func (w *statusRewriter) WriteHeader(code int) {
	if code == http.StatusOK {
		code = http.StatusPartialContent
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRewriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// accept206 is the dial-side counterpart to statusRewriter. On raft stream
// dials it stamps acceptHeader — telling the serving peer this dialer can
// translate a 206 — and rewrites the 206 that comes back to 200 just as the
// response arrives, so etcd's streamReader.dial — which switches only on 200 —
// accepts it, while the wire still carried 206 for a buffering intermediary.
type accept206 struct{ inner http.RoundTripper }

func (s accept206) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasPrefix(req.URL.Path, rafthttp.RaftStreamPrefix) {
		req = req.Clone(req.Context()) // RoundTrippers must not mutate the caller's request
		req.Header.Set(acceptHeader, acceptValue)
	}
	resp, err := s.inner.RoundTrip(req)
	if err == nil && resp != nil &&
		resp.StatusCode == http.StatusPartialContent &&
		strings.HasPrefix(req.URL.Path, rafthttp.RaftStreamPrefix) {
		resp.StatusCode = http.StatusOK
		resp.Status = "200 OK"
	}
	return resp, err
}

// Intercept wraps the minted server's raft stream RoundTripper with accept206.
// etcd's peer transport exposes no injection seam — the Transport is built inside
// etcdserver.NewServer and the stream RoundTripper is minted privately in
// Transport.Start from (TLSInfo, DialTimeout) — and the only egress hook,
// HTTP_PROXY, unconditionally skips loopback (every test and example). So the
// RoundTripper is reached by reflection:
//
//	*EtcdServer.r (raftNode) → .raftNodeConfig.transport (*rafthttp.Transport) → .streamRt
//
// Every hop is unexported; unsafe lifts reflect's read/set ban. It must run after
// NewServer (so streamRt exists) and before the raft loop dials peers. The single
// concrete-type assert on streamRt upstream (Transport.Stop's CloseIdleConnections)
// already ran in Transport.Start before this swap, so wrapping it only forgoes
// closing idle stream conns at Stop, which teardown reclaims anyway.
//
// On any layout change it logs and leaves the node unwrapped rather than
// panicking: single-node is unaffected (no peer dials) and a multi-node join
// would then fail to sync — which the warning, and the layout-guard test, flag.
func Intercept(srv *etcdserver.EtcdServer, lg *zap.Logger) {
	field, inner, ok := streamRtField(srv)
	if !ok {
		if lg != nil {
			lg.Warn("libetcd: raft stream 206 intercept skipped; multi-node joins may not sync",
				zap.String("reason", "could not reach streamRt (etcd internal layout changed?)"))
		}
		return
	}
	if _, done := inner.(accept206); done {
		return // idempotent
	}
	field.Set(reflect.ValueOf(accept206{inner}))
}

// Intercepted reports whether srv's raft stream RoundTripper is currently wrapped
// by Intercept. It is the read-only predicate behind the layout-guard test.
func Intercepted(srv *etcdserver.EtcdServer) bool {
	_, inner, ok := streamRtField(srv)
	if !ok {
		return false
	}
	_, done := inner.(accept206)
	return done
}

// streamRtField walks *EtcdServer to the settable streamRt field and its current
// value, via unsafe (the whole path is unexported). On any layout mismatch it
// recovers and reports ok=false rather than panicking.
func streamRtField(srv *etcdserver.EtcdServer) (field reflect.Value, inner http.RoundTripper, ok bool) {
	defer func() { _ = recover() }() // layout changed → zero values, ok=false

	transport := unexportedField(reflect.ValueOf(srv).Elem().FieldByName("r"), "transport")
	if !transport.IsValid() {
		return
	}
	rt, isT := transport.Interface().(*rafthttp.Transport)
	if !isT || rt == nil {
		return
	}
	f := unexportedField(reflect.ValueOf(rt).Elem(), "streamRt")
	if !f.IsValid() {
		return
	}
	in, isRT := f.Interface().(http.RoundTripper)
	if !isRT || in == nil {
		return
	}
	return f, in, true
}

// unexportedField returns an addressable, settable reflect.Value for the named
// (promoted or direct) field of v — including unexported ones — using unsafe to
// bypass reflect's export restriction. v must be addressable.
func unexportedField(v reflect.Value, name string) reflect.Value {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return f
	}
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
}
