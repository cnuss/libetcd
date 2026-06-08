# libetcd

[![Go Reference](https://pkg.go.dev/badge/github.com/cnuss/libetcd.svg)](https://pkg.go.dev/github.com/cnuss/libetcd)
[![Go Report Card](https://goreportcard.com/badge/github.com/cnuss/libetcd)](https://goreportcard.com/report/github.com/cnuss/libetcd)
[![CI](https://github.com/cnuss/libetcd/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cnuss/libetcd/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cnuss/libetcd/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/cnuss/libetcd/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cnuss/libetcd/badge)](https://scorecard.dev/viewer/?uri=github.com/cnuss/libetcd)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

`libetcd` is a thin, developer-friendly SDK for running **embedded etcd**: it
wraps [`go.etcd.io/etcd/server/v3/embed`](https://pkg.go.dev/go.etcd.io/etcd/server/v3/embed)
behind a fluent builder so a Go program can spin up a real etcd node — or a
multi-node cluster — in-process and get back a ready-to-use `clientv3.Client`.

It ships as stable/alpha versioned packages (`v1` stable contract, `v1alpha1`
mutable implementation), with CI, CodeQL, OpenSSF Scorecard, cosign-signed
releases, Dependabot, examples, and an e2e harness.

The API is a fluent builder: `New()` configures a node with `With*` methods and
`Start(ctx)` boots it.

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
	ctx := context.Background()

	// Port 0 picks a free port; omit WithDir for a throwaway temp data dir.
	e, err := libetcd.New().
		WithName("greeter").
		WithClientPort(0).
		WithPeerPort(0).
		Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	e.Client().Put(ctx, "greeting", "hello world")
	resp, _ := e.Client().Get(ctx, "greeting")

	fmt.Printf("greeting: %s\n", resp.Kvs[0].Value) // greeting: hello world
}
```

(Full source: [`examples/basic/main.go`](./examples/basic/main.go).)

## Layout

Three packages, stable/alpha versioning:

```
github.com/cnuss/libetcd           — root façade. Stable surface (New).
github.com/cnuss/libetcd/v1        — stable Builder + Etcd interfaces.
github.com/cnuss/libetcd/v1alpha1  — current implementation. May change
                                   between alpha revisions.
```

Application code imports the root (`libetcd.New()…`). Code that needs to declare
types against the interfaces imports `v1`. Direct access to the `BuilderImpl`
struct lives in `v1alpha1`.

For the file-by-file map, see
[CONTRIBUTING.md → Where to find things](./CONTRIBUTING.md#where-to-find-things).

## API at a glance

```go
type Builder interface {
    WithName(name string) Builder              // node (member) name; default "default"
    WithDir(dir string) Builder                // data dir; default: a fresh temp dir
    WithClientPort(port int) Builder           // localhost client port; 0 = pick free; default 2379
    WithPeerPort(port int) Builder             // localhost peer port;  0 = pick free; default 2380
    WithClientURL(urls ...string) Builder      // advanced: explicit client URLs
    WithPeerURL(urls ...string) Builder        // advanced: explicit peer URLs
    WithPeers(peers map[string]string) Builder // multi-node initial cluster: name -> peer URL
    WithClusterToken(token string) Builder     // initial-cluster token; default "libetcd-cluster"
    WithExistingCluster() Builder              // join an existing cluster instead of bootstrapping
    WithLogLevel(level string) Builder         // server log level; default "error"
    Start(ctx context.Context) (Etcd, error)   // terminal: boots, waits ready, dials a client
}

type Etcd interface {
    Client() *clientv3.Client // in-process-wired client to this node
    Endpoints() []string      // actual bound client endpoints (ports resolved)
    Server() *embed.Etcd      // escape hatch to the raw embed handle
    Close() error             // closes the client, then stops the server
}

func New() Builder   // unconfigured builder
```

Bring up a multi-node cluster by listing every member's peer URL with
`WithPeers` (see [`examples/cluster`](./examples/cluster/main.go)); a node with
no peers starts as a single-member cluster.

## Examples

Self-contained programs in [`./examples`](./examples):

| Example   | Demonstrates                                                       |
| --------- | ----------------------------------------------------------------- |
| `basic`   | Smallest wiring — start one node, `Put`/`Get`, `Close`.           |
| `cluster` | A 3-node in-process cluster via `WithPeers`; write one, read another. |

Run one locally:

```sh
make run basic
make run cluster
```

## Testing

```sh
make test   # library unit tests + godoc examples (fast, in-package)
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
