package v1alpha1

import (
	"net/http"
	"reflect"
	"strings"
	"unsafe"

	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"
)

// streamAccept206 is the dial-side half of issue #8 (the serve side lives in
// streamStatusRewriter, server.go). etcd's streamReader.dial switches only on a
// 200 OK; this rewrites the 206 Partial Content the serve side emits back to 200
// just as the response arrives, so a buffering intermediary still sees 206 on the
// wire while the stock reader stays happy. Scoped to /raft/stream — the only
// response that carries a 206.
type streamAccept206 struct{ inner http.RoundTripper }

func (s streamAccept206) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := s.inner.RoundTrip(req)
	if err == nil && resp != nil &&
		resp.StatusCode == http.StatusPartialContent &&
		strings.HasPrefix(req.URL.Path, rafthttp.RaftStreamPrefix) {
		resp.StatusCode = http.StatusOK
		resp.Status = "200 OK"
	}
	return resp, err
}

// interceptRaftStream wraps the minted server's raft stream RoundTripper with
// streamAccept206. etcd's peer transport exposes no injection seam — it is built
// inside etcdserver.NewServer and the stream RoundTripper is minted privately in
// Transport.Start from (TLSInfo, DialTimeout) — so we reach it by reflection:
//
//	*EtcdServer.r (raftNode) → .raftNodeConfig.transport (*rafthttp.Transport) → .streamRt
//
// Every hop is unexported; unsafe lifts reflect's read/set ban. It must run after
// NewServer (so streamRt exists) and before the raft loop dials peers — Server
// runs it before Start. The single concrete-type assert on streamRt in upstream
// (Transport.Stop's CloseIdleConnections) already ran in Transport.Start before
// this swap, so wrapping it only forgoes closing idle stream conns at Stop, which
// teardown reclaims anyway.
//
// On any layout change it logs and leaves the node unwrapped rather than
// panicking: single-node is unaffected (no peer dials), and a multi-node join
// would then fail to sync — which the warning flags. This is the cost of reaching
// etcd internals without forking; the field path is version-coupled.
func interceptRaftStream(srv *etcdserver.EtcdServer, lg *zap.Logger) {
	warn := func(reason string) {
		if lg != nil {
			lg.Warn("libetcd: raft stream 206 intercept skipped; multi-node joins may not sync",
				zap.String("reason", reason))
		}
	}
	defer func() {
		if r := recover(); r != nil {
			warn("panic walking etcd internals")
		}
	}()

	transport := unexportedField(reflect.ValueOf(srv).Elem().FieldByName("r"), "transport")
	if !transport.IsValid() {
		warn("no raftNode.transport field")
		return
	}
	rt, ok := transport.Interface().(*rafthttp.Transport)
	if !ok || rt == nil {
		warn("transport is not *rafthttp.Transport")
		return
	}
	streamRt := unexportedField(reflect.ValueOf(rt).Elem(), "streamRt")
	if !streamRt.IsValid() {
		warn("no Transport.streamRt field")
		return
	}
	inner, ok := streamRt.Interface().(http.RoundTripper)
	if !ok || inner == nil {
		warn("streamRt is not an http.RoundTripper")
		return
	}
	if _, done := inner.(streamAccept206); done {
		return // idempotent
	}
	streamRt.Set(reflect.ValueOf(streamAccept206{inner}))
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
