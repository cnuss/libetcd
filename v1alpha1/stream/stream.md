# `stream` — HTTP 206 on the raft peer stream (issue #8)

This package makes etcd's raft peer **stream** report `206 Partial Content` on
the wire, while both (stock, unmodified) etcd endpoints still behave as if it
were `200 OK`.

## Why

A raft peer stream is a single long-lived, chunked HTTP response: the serving
peer holds the connection open and writes message frames as they occur. Some
buffering intermediaries (proxies, service meshes, load balancers) will **hold**
a `200 OK` chunked body until it completes — which, for a stream that never
completes, means the bytes never flow and the follower never catches up. Many of
those same intermediaries **stream** a `206 Partial Content` body through
immediately.

So the goal: emit `206` on the stream so it survives a buffering hop. The catch:
both ends of the raft protocol must agree on the status, and we want **no fork**
of etcd — it has to keep working against stock `go.etcd.io/etcd`.

## The two halves

```
serving peer A                         reading peer B
┌──────────────┐                       ┌──────────────┐
│ etcd emits   │   206 on the wire     │ accept206    │
│ 200 on       │──────────────────────▶│ rewrites     │
│ /raft/stream │   (intermediary       │ 206 → 200    │
│              │    streams it)        │ before the   │
│ Handler      │                       │ stock reader │
│ rewrites     │                       │              │
│ 200 → 206    │                       │ etcd reader  │
└──────────────┘                       │ sees 200 ✓   │
                                       └──────────────┘
```

- **Serve side — `Handler`.** Wraps the peer (raft) `http.Handler`. On the
  `/raft/stream` path only, it rewrites the stream's success status `200 → 206`.
  Scoped to that path because the stream handler is the *only* `200` on the peer
  mux — pipeline writes `204`, and `/members`, `/version`, lease, etc. return
  real `200 + body` that must pass through untouched. The wrapper preserves
  `http.Flusher` (rafthttp's `streamHandler` type-asserts the `ResponseWriter`
  to `Flusher` and would panic without it).

- **Dial side — `Intercept`.** etcd's `streamReader.dial` switches only on
  `http.StatusOK`; a `206` falls through and is rejected, so the stream never
  establishes. `Intercept` wraps the reader's `RoundTripper` with `accept206`,
  which rewrites `206 → 200` the instant the response arrives — *after* it
  crossed the wire as `206`, *before* the stock reader inspects it.

Both halves are required. Serve-side `206` alone breaks a stock reader; dial-side
alone never sees a `206` to rewrite.

## Why the dial side needs reflection (and `unsafe`)

The serve side is easy — libetcd owns the peer handler. The dial side is not:
etcd's raft egress has **no injection seam**.

```
etcdserver.NewServer
  server.go    tr := &rafthttp.Transport{ TLSInfo, DialTimeout, URLs, … }   ← built internally, struct literal
  server.go    tr.Start()
                 transport.go   t.streamRt = newStreamRoundTripper(TLSInfo, DialTimeout)   ← minted privately
  server.go    tr.AddPeer(m.ID, m.PeerURLs)                                  ← dial target = membership URLs
                 peer.go   startPeer → &streamReader{ tr }
                   stream.go   streamReader.dial → cr.tr.streamRt.RoundTrip(req)
                   stream.go   switch resp.StatusCode { case http.StatusOK: … }   ← 206 rejected here
```

Every way in is blocked:

| Seam | Why it fails |
| --- | --- |
| Pass a `Transport`/`RoundTripper`/`Dialer` via config | `Transport` is built inside `NewServer`, stored in the unexported `srv.r.transport`; no `ServerConfig`/`embed` hook. Only `TLSInfo`/`DialTimeout`/`URLs` are inputs. |
| Wrap `streamRt` through a public API | It's minted privately in `Transport.Start` from `(TLSInfo, DialTimeout)`. |
| Set the transport's `Proxy` (`HTTP_PROXY`) | etcd's stream transport *does* honor `HTTP_PROXY`, but `x/net/http/httpproxy` **unconditionally skips `localhost` and loopback** — which is every test, example, and CI run. |
| Rewrite the dial **target** URL | The target comes from `AddPeer(m.PeerURLs)`, i.e. raft-replicated membership; changing it leaks a bogus URL cluster-wide. |
| Override the clientv3 dial | Wrong transport entirely — clientv3 is the v3 gRPC client API, it never carries a raft stream. |

With no seam, the only remaining handle is the live object graph. We hold the
`*etcdserver.EtcdServer`, so `Intercept` reflects down to the stream
`RoundTripper` and swaps it:

```
*EtcdServer.r (raftNode) → .raftNodeConfig.transport (*rafthttp.Transport) → .streamRt
```

Every hop is unexported, so `unsafe` (`reflect.NewAt(field.Type(),
unsafe.Pointer(field.UnsafeAddr())).Elem()`) lifts reflect's read/set ban.

### Timing

`Intercept` runs in `EtcdImpl.Server()` — **after** `NewServer` mints `streamRt`,
**before** `Start` fires the raft loop and peers dial. No reader races the swap.
(For a join, `NewServer`'s own `AddPeer` may start a reader that makes a first
dial against the unwrapped transport; it's rejected, retried ~100ms later against
the wrapped one, and self-heals — raft tolerates a few rejected dials.)

### What the swap costs

etcd asserts `streamRt.(*http.Transport)` in exactly one place —
`Transport.Stop`'s `CloseIdleConnections`. That assert ran in `Transport.Start`
(before our swap), so wrapping only forgoes closing idle stream conns at `Stop`,
which teardown reclaims anyway. The `streamProber` keeps the unwrapped transport,
which is correct — probes want plain `200`.

## The risk, and how it's guarded

This is a fork by other means, and **more fragile** than a source fork: the field
path (`r` / `raftNodeConfig` / `transport` / `streamRt`) is coupled to etcd's
internal layout. A source patch fails to apply at *rebase* time (loud, expected);
this walk compiles clean and could break at a *consumer's runtime*.

Two mitigations:

1. **Fail soft.** `streamRtField` recovers from any layout mismatch and reports
   `ok=false`; `Intercept` then logs a warning and leaves the node unwrapped.
   Single-node is unaffected (it never dials a peer); a multi-node join would
   fail to sync — which the warning flags.
2. **Fail loud in CI.** `Intercepted` is a read-only predicate that re-walks the
   same path. `TestRaftStreamIntercepted` (in `v1alpha1/server_test.go`) mints a
   real server and asserts the stream `RoundTripper` is wrapped, so an etcd bump
   that moves a field turns into a red build, not a silent prod regression.

## Public surface

| Symbol | Use |
| --- | --- |
| `Handler(inner http.Handler) http.Handler` | serve side; wrap the peer handler |
| `Intercept(srv *etcdserver.EtcdServer, lg *zap.Logger)` | dial side; wrap `streamRt` after mint, before start |
| `Intercepted(srv *etcdserver.EtcdServer) bool` | predicate; backs the layout-guard test |

## Alternatives considered (and rejected)

- **Fork the `server` module** (publish `github.com/cnuss/etcd/server/v3`):
  correct and the most legible, but a maintenance burden (rebase per etcd
  release) and requires standing up + publishing a fork repo. The reflection
  route keeps issue #8 fork-free and propagates to `go get` consumers as ordinary
  library code.
- **Local `replace` of the server module:** doesn't propagate to consumers
  (`replace` is main-module only), so not shippable.
- **HTTP forward proxy via `HTTP_PROXY`:** killed by the loopback bypass above,
  plus it's process-global and would route gRPC (which our clients would then
  have to opt out of with `WithNoProxy`).
