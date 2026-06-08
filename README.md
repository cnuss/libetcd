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

`New()` returns the full node — an `Etcd`, composed of three interfaces:

```go
func New() Etcd

type Etcd interface {
    Accessor // read-side handles
    Builder  // configuration
    Executor // lifecycle
}

// Builder — configure; each returns Builder, mutating the node in place.
type Builder interface {
    WithName(name string) Builder                // member name; default "default"
    WithDir(dir string) Builder                  // data dir; default: a fresh temp dir
    WithClientListener(l net.Listener) Builder   // client URL from a listener (https if TLS-wrapped)
    WithPeerListener(l net.Listener) Builder      // peer URL from a listener
    WithClusterToken(token string) Builder        // initial-cluster token; default "libetcd-cluster"
    WithLogLevel(level string) Builder            // server log level; default "fatal"
    WithClientHTTP(srv *http.Server) Builder      // supply the client (v3 API) http.Server
    WithPeerHTTP(srv *http.Server) Builder        // supply the peer (raft) http.Server
}

// Executor — lifecycle.
type Executor interface {
    Start() error // mint + start; auto-binds any unset listener; serves client/peer HTTP
    Stop() error  // best-effort shutdown
}

// Accessor — read-side handles, minted lazily and cached.
type Accessor interface {
    Server() *etcdserver.EtcdServer     // the minted server (nil on bad config)
    GrpcServer() *grpc.Server           // v3 gRPC server (election + lock registered)
    Loopback() *clientv3.Client         // in-process client
    Client() (*clientv3.Client, error)  // networked client (dials client URLs)
    ClientHandler() http.Handler        // gRPC (+REST gateway) handler, h2c-wrapped
    PeerHandler() http.Handler          // raft peer protocol handler
    ClientHTTP() *http.Server           // client http.Server (provided or default)
    PeerHTTP() *http.Server             // peer http.Server (provided or default)
    ClientListener() net.Listener       // listener set via WithClientListener, or nil
    PeerListener() net.Listener         // listener set via WithPeerListener, or nil
}
```

Single-node only for now; multi-node cluster bootstrap lands in a follow-up.

## Examples

Self-contained programs in [`./examples`](./examples):

| Example       | Demonstrates                                            |
| ------------- | ------------------------------------------------------ |
| `single-node` | Start a node (defaults everything), `Put`/`Get`, `Stop`. |

Run it locally:

```sh
make run single-node
```

## Testing

```sh
make test   # library unit tests (fast, in-package)
make e2e    # builds and runs every example binary, asserts its output
```

`make e2e` runs `go test -count=1 -v ./e2e`. The `-count=1` defeats the test
cache, since the harness builds the example binaries at runtime and the cache
key wouldn't otherwise pick up example source changes.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the local dev loop, release
process, and what makes a good example.

## License

[MIT](./LICENSE)
