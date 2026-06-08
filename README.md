<!--
Made from this template? Find/replace "golib" → your library name across the
repo, update lib.go's `package golib` clause, then delete this comment.
`make all` should stay green. (Workflows read GITHUB_REPOSITORY at runtime, so
they need no edits.)
-->

# golib

[![Go Reference](https://pkg.go.dev/badge/github.com/cnuss/golib.svg)](https://pkg.go.dev/github.com/cnuss/golib)
[![Go Report Card](https://goreportcard.com/badge/github.com/cnuss/golib)](https://goreportcard.com/report/github.com/cnuss/golib)
[![CI](https://github.com/cnuss/golib/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cnuss/golib/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cnuss/golib/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/cnuss/golib/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cnuss/golib/badge)](https://scorecard.dev/viewer/?uri=github.com/cnuss/golib)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

`golib` is a thin, stable façade over stable/alpha versioned packages
(`v1` stable contract, `v1alpha1` mutable implementation), with CI, CodeQL,
OpenSSF Scorecard, cosign-signed releases, Dependabot, examples, and an e2e
harness.

The API is a generic builder: `New[T]()` configures with `With*` methods and
finalizes with `Build()`.

## Quick Start

```sh
go get github.com/cnuss/golib
```

```go
package main

import (
	"fmt"

	"github.com/cnuss/golib"
)

func main() {
	res := golib.New[string]().
		WithName("greeting").
		WithValue("hello world").
		Build()

	fmt.Printf("%s: %s\n", res.Name, res.Value) // greeting: hello world
}
```

(Full source: [`examples/basic/main.go`](./examples/basic/main.go).)

## Layout

Three packages, stable/alpha versioning:

```
github.com/cnuss/golib           — root façade. Stable surface (New).
github.com/cnuss/golib/v1        — stable Builder[T] interface + Result[T].
github.com/cnuss/golib/v1alpha1  — current implementation. May change
                                   between alpha revisions.
```

Application code imports the root (`golib.New[T]()…`). Code that needs to
declare types against the interface imports `v1`. Direct access to the
`BuilderImpl[T]` struct lives in `v1alpha1`.

For the file-by-file map, see
[CONTRIBUTING.md → Where to find things](./CONTRIBUTING.md#where-to-find-things).

## API at a glance

```go
type Builder[T any] interface {
    WithName(name string) Builder[T]   // display name carried into the Result
    WithValue(v T) Builder[T]          // the payload Build produces
    Build() Result[T]                  // terminal: assembles and returns
    Name() string                      // configured name (empty if unset)
}

type Result[T any] struct {
    Name  string `json:"name,omitempty"`
    Value T      `json:"value"`
}

func New[T any]() Builder[T]   // unconfigured builder
```

## Examples

Self-contained programs in [`./examples`](./examples):

| Example | Demonstrates                                          |
| ------- | ----------------------------------------------------- |
| `basic` | Smallest wiring — `New` + `WithValue` + `Build`.      |
| `named` | A typed struct payload carried through `WithValue`.   |

Run one locally:

```sh
make run basic
make run named
```

## Testing

```sh
make test   # library unit + fuzz tests (fast, in-package)
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
