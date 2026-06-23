# libetcd

[![Go Reference](https://pkg.go.dev/badge/github.com/cnuss/libetcd.svg)](https://pkg.go.dev/github.com/cnuss/libetcd)
[![Go Report Card](https://goreportcard.com/badge/github.com/cnuss/libetcd)](https://goreportcard.com/report/github.com/cnuss/libetcd)
[![CI](https://github.com/cnuss/libetcd/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cnuss/libetcd/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cnuss/libetcd/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/cnuss/libetcd/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cnuss/libetcd/badge)](https://scorecard.dev/viewer/?uri=github.com/cnuss/libetcd)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

`libetcd` is a thin, developer-friendly SDK for running **embedded etcd**: it
wraps [`go.etcd.io/etcd/server/v3/embed`](https://pkg.go.dev/go.etcd.io/etcd/server/v3/embed)
behind a fluent builder so a Go program can spin up a real etcd node in-process
and get back a ready-to-use `clientv3.Client`.

It ships as stable/alpha versioned packages (`v1` stable contract, `v1alpha1`
mutable implementation), with CI, CodeQL, OpenSSF Scorecard, cosign-signed
releases, Dependabot, examples, and an e2e harness.

The API is a fluent builder: `New()` configures a node with `With*` methods,
`Start()` boots it, and `Stop()` shuts it down. The `With*` setters mutate the
node in place and chain.

## Quick Start

```sh
go get github.com/cnuss/libetcd
```

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the node

	// Defaults everything: a temp data dir and free loopback ports auto-bound by Start.
	e := libetcd.New().WithContext(ctx)
	if err := e.Start(); err != nil {
		log.Fatal(err)
	}

	cli := e.Client()
	cli.Put(ctx, "greeting", "hello world")
	resp, _ := cli.Get(ctx, "greeting")

	fmt.Printf("greeting: %s\n", resp.Kvs[0].Value) // greeting: hello world
}
```

(Full source: [`examples/single-node/main.go`](./examples/single-node/main.go).)

### Join an existing cluster

Give `From` the peer (raft) URLs of any current members — a hardcoded list, from
config, or another node's `Peers()`. `Join` runs entirely over the peer (raft)
listener: it POSTs the node to a peer's `/libetcd/v1/join` endpoint, restores the
snapshot the peer streams back, starts, and promotes the node to a voting member.
No client endpoint is dialed, so a node needs only the peer transport to join —
and a fully headless cluster (serving no client traffic anywhere) is joinable.
The join is authorized by the cluster token (`WithClusterToken`), so it is
libetcd-to-libetcd; stock etcd doesn't serve the endpoint:

```go
package main

import (
	"context"
	"log"
	"net"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx := context.Background()

	// Peer (raft) URLs of members already in the cluster. Plain strings —
	// bare host:port, http://, or https:// all work; From normalizes them and
	// drops any it can't parse. A hardcoded list here; in practice from config
	// or another node's Peers().
	peers := []string{"10.0.0.1:2380", "http://10.0.0.2:2380"}

	// A remote cluster must be able to dial this node back: the peer
	// listener's address is what Join advertises. Without WithPeerListener the
	// node auto-binds a loopback port, which only works when the whole
	// cluster runs on this host — Join rejects that combination for remote
	// peers.
	lis, err := net.Listen("tcp", "10.0.0.3:2380")
	if err != nil {
		log.Fatal(err)
	}

	node := libetcd.From(peers...).WithPeerListener(lis).WithContext(ctx)
	if err := node.Join(); err != nil {
		log.Fatal(err)
	}

	// node is now a voting member; Self reads the replicated keyspace.
	node.Self().Put(ctx, "joined", "true")
}
```

`From()` with no peers bootstraps instead of joining: `Join` short-circuits to a
fresh single-member start (exactly `New().Start()`), so a first node and the
nodes that join it can share one `From(...).Join()` call site.

Peers can also come from the `LIBETCD_PEERS` environment variable — a
comma-separated list or a JSON array of strings — which `Join` unions with the
arguments to `From` (either alone works; together they combine). So a node can
be aimed at a cluster purely by environment, with no code change. With no peers
from either source, `From()...Join()` still bootstraps.

A whole cluster can also come up from **one identical call on every node**. When
a node's own advertised peer URL (`WithPeerListener`) is part of the peer set —
every node handed the same self-inclusive list (e.g. the same `LIBETCD_PEERS`) —
`Join` elects a single bootstrapper with no coordination: the node whose URL
sorts first bootstraps, the rest join it. No node is special-cased; bring them
up concurrently. It's split-brain-safe (exactly one node is the canonical
minimum), though the lowest node must come up for the others to join. And
because a non-empty data dir makes `Join` boot from the WAL (a restart), the
same call is safe to re-run when a node restarts.

Joins are safe to run concurrently — `Join` serializes membership changes
through a lock held in the target cluster, so several nodes (even in separate
processes) can call it at once. The lock writes transient coordination keys
under the `libetcd/lock/` prefix in the target cluster's keyspace — visible to
scans, watchers, and backups — so applications should avoid keys under that
prefix. Working examples:
[`examples/multi-node/main.go`](./examples/multi-node/main.go) (one join) and
[`examples/discovery/main.go`](./examples/discovery/main.go) (a whole cluster
from one identical call per node, via a discovery seed). The `e2e` suite
exercises more: concurrent joins (`TestAsyncJoin`), joins under sustained writes
with zero-loss verification (`TestLoadJoin`), and a bootstrap node serving no
client traffic — `WithClientListener(nil)` — that nodes still join over the peer
transport (`TestHeadlessLeader`).

### Restarts and data-dir reuse

A builder handle is **single-use**: `Start`, `Join`, and `Stop` each run at
most once per handle. A restart — whether the process restarted or the node is
being cycled in-process — is always a brand-new builder constructed over the
previous incarnation's data dir:

```go
node := libetcd.New().WithDir(dir) // dir: the stopped node's data dir
if err := node.Start(); err != nil {
	log.Fatal(err)
}
```

What a restart must hold constant:

- **The data dir.** The member's identity (member ID, cluster ID, keyspace,
  membership) lives there. A node started over a dir with data boots from its
  WAL, and etcd then ignores the new builder's name, initial-cluster string,
  cluster token, and cluster state — they don't need to match the original
  boot. The name and client URLs are republished from the new config.
- **The peer (raft) address, on multi-member clusters.** The membership stores
  each member's advertised peer URL and the other members dial it: bind a
  listener on the original address and pass it via `WithPeerListener` on every
  restart (bind `127.0.0.1:0` the first time, record the port). Pin the client
  address the same way if anything dials the member's registered client URL.
  A single-member cluster can let `Start` auto-bind fresh ports.

A restarted member that already joined never calls `Join` again — `Start`
boots it from its WAL and it rejoins raft. And a restarted multi-member node's
`Start` blocks until the cluster has quorum, so restart a stopped cluster's
members concurrently, not serially.

The `e2e` suite covers this: `TestDirHandoff` (a single node, a new builder over
an old dir) and `TestRestartCycle` (two full stop-everything/restart cycles of a
cluster, zero-loss verified each time).

## Layout

Three packages, stable/alpha versioning:

```
github.com/cnuss/libetcd           — root façade. Stable surface (New).
github.com/cnuss/libetcd/v1        — stable Builder + Etcd interfaces.
github.com/cnuss/libetcd/v1alpha1  — current implementation. May change
                                   between alpha revisions.
```

Application code imports the root (`libetcd.New()…`). Code that needs to declare
types against the interfaces imports `v1`. Direct access to the `EtcdImpl` struct
lives in `v1alpha1`.

For the file-by-file map, see
[CONTRIBUTING.md → Where to find things](./CONTRIBUTING.md#where-to-find-things).

## API at a glance

`New()` returns the full node (an `Etcd`); `From()` returns a join-only node (an
`EtcdPeer`):

```go
func New() Etcd                 // a fresh, startable node
func From(peers ...string) EtcdPeer // a node that joins the cluster at those peer URLs

type Etcd interface {
    Server        // server-side handles
    Client        // clientv3 clients
    Builder[Etcd] // configuration (setters chain back to Etcd)
    Executor      // lifecycle
}

// Builder[T] — configure; each setter returns T (Etcd or EtcdPeer), mutating in place.
type Builder[T any] interface {
    WithName(name string) T                // member name; default: a unique generated name
    WithDir(dir string) T                  // data dir; default: a fresh temp dir
    WithClusterToken(token string) T       // initial-cluster token; default "etcd-cluster"
    WithLog(level string, w io.Writer) T   // route logs to w at level; silent by default
    WithContext(ctx context.Context) T     // cancel ctx => graceful Stop
    WithClientListener(l net.Listener) T   // client (v3 API) socket; nil = headless (no client serving/URLs)
    WithPeerListener(l net.Listener, advertiseURLs ...string) T // peer (raft) socket; nil+URLs = BYO serving; nil+none is a config error
}

// Executor — lifecycle.
type Executor interface {
    Start() error                      // mint + start; auto-binds any unset listener; serves HTTP
    Stop() error                       // best-effort, idempotent shutdown
}

// Server — server-side handles, minted lazily and cached.
type Server interface {
    Server() *etcdserver.EtcdServer  // the minted server (nil on bad config)
    GrpcServer() *grpc.Server        // v3 gRPC server (election + lock registered)
    ClientHandler() http.Handler     // gRPC (+REST gateway) handler, h2c-wrapped
    PeerHandler() http.Handler       // raft peer protocol + join handler
    ClientListener() net.Listener    // materialized client listener (nil when headless)
    PeerListener() net.Listener      // materialized peer listener
    PeerPaths() []string             // raft path prefixes (mount PeerHandler elsewhere to inspect)
}

// Client — clientv3 clients to the cluster.
type Client interface {
    Self() *clientv3.Client    // in-process client to this node
    Leader() *clientv3.Client  // client pinned to the leader
    Client() *clientv3.Client  // networked client (dials voting members)
    Peers() []string           // members' peer (raft) URLs; feed to From
}
```

`EtcdPeer` (from `From`) is a join-only node: the `Client` accessors, the
`Server` handles, and the `Builder` setters (chaining back to `EtcdPeer`), plus
`Join`/`Stop` — but no `Start`. `From(...).Join()` is the only way to join an
existing cluster. The `Server` handles are for use after `Join` has started the
node; calling them before `Join` mints the server prematurely from the bootstrap
config, which `Join` then rejects ("server already minted").

```go
// EtcdPeer — join an existing cluster from a list of peer URLs.
type EtcdPeer interface {
    Client            // Self / Leader / Client / Peers / Endpoints
    Server            // Server / GrpcServer / ClientHandler / PeerHandler / listeners
    Builder[EtcdPeer] // same setters as Etcd, chaining back to EtcdPeer
    Join() error      // join over the peer listener: POST /libetcd/v1/join, restore the
                      // streamed snapshot, start, promote to voting; token-authorized,
                      // rolls back the half-joined member on failure
    Stop() error      // best-effort, idempotent shutdown
}
```

Peer URLs are plain strings — bare `host:port`, `http://`, or `https://`
entries are accepted. At `Join` time `From` trims them, defaults a missing
scheme to `http`, de-duplicates, and silently drops any it can't parse. The
join rides the peer (raft) listener and is gated by the cluster token; that
gate is only meaningful over a TLS peer listener (a cleartext one carries the
token in the clear), so peer-listener TLS for the join path is still being
worked out ([#74](https://github.com/cnuss/libetcd/issues/74)).

**BYO peer serving.** `WithPeerListener(nil, advertiseURLs...)` tells libetcd to
bind no peer socket of its own: something else serves the peer (raft) protocol —
mount `PeerHandler()` over `PeerPaths()` on your own `http.Server`, reachable at
the advertised URLs the cluster dials. Use it to share a listener, multiplex
raft alongside your own routes, or front the peer protocol with a tunnel/proxy
(see `TestSingleNodeTunnel` / `TestMultiNodeTunnel` in `e2e`). Mount the handler
only **after** `Start`/`Join` returns —
calling `PeerHandler()` earlier mints the server prematurely. A joining node
needs no inbound raft during `Join` (the snapshot seed boots it caught up and it
dials out to promote), so serving right after `Join` returns is in time for
steady-state replication. Symmetrically, **stop serving (close your
`http.Server`) before `Stop`** — `Stop` closes the etcd backend, and a peer
request reaching the still-mounted handler afterwards panics inside etcd's
handler. `WithPeerListener(nil)` with no advertise URLs is a config error — a
raft member must serve a socket or advertise where its peer protocol is served.

## Examples

Self-contained programs in [`./examples`](./examples):

| Example       | Demonstrates                                                          |
| ------------- | --------------------------------------------------------------------- |
| `single-node` | Start a node (defaults everything), `Put`/`Get`, `Stop`.              |
| `multi-node`  | Bring up a node, `Join` a second to it, read the replicated key.      |
| `discovery`   | A three-node cluster from one identical `From(seed).Join()` call per node, via a discovery seed (`https://disco.nuss.io`). |

Everything else moved into the `e2e` suite as native tests — concurrent and
under-load joins (`TestAsyncJoin`, `TestLoadJoin`), restarts (`TestDirHandoff`,
`TestRestartCycle`), a headless bootstrap (`TestHeadlessLeader`), and NAT-
crossing over a Cloudflare tunnel (`TestSingleNodeTunnel`, `TestMultiNodeTunnel`).

Run one locally:

```sh
make run single-node
make run multi-node
make run discovery     # needs the live seed at disco.nuss.io
```

## Testing

```sh
make test   # library unit tests (fast, in-package)
make e2e    # native end-to-end tests (boot real nodes) + the example binaries
```

`make e2e` runs `go test -count=1 -v ./e2e` — native tests that boot real nodes,
plus a harness that builds and runs the remaining example binaries and asserts
their output. The `-count=1` defeats the test cache, since the harness builds
the example binaries at runtime and the cache key wouldn't otherwise pick up
example source changes.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the local dev loop, release
process, and what makes a good example.

## License

[MIT](./LICENSE)
