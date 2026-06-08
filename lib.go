// Package libetcd is a thin, developer-friendly SDK for running embedded etcd.
//
// It wraps go.etcd.io/etcd/server/v3/embed behind a fluent builder: configure a
// node with With* methods, Start it, and use the in-process loopback client.
//
//	e := libetcd.New()
//	e.WithDir("/tmp/data").WithClientListener(lc)
//	if err := e.Start(); err != nil { /* ... */ }
//	defer e.Stop()
//	e.Loopback().Put(ctx, "k", "v")
//
// The package is split into three pieces:
//
//   - libetcd (this package) — thin façade exposing New. Stable surface for
//     application code.
//   - github.com/cnuss/libetcd/v1 — the stable Accessor, Builder, Executor, and
//     Etcd interfaces. Application code that wants to declare types against the
//     contract imports this.
//   - github.com/cnuss/libetcd/v1alpha1 — the current implementation. Internals
//     may change between alpha revisions; pin only if you need the concrete
//     types.
package libetcd

import (
	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1"
)

// New returns an embedded etcd node. Configure it with the With* methods (they
// mutate in place and chain), then call Start; Stop shuts it down.
//
//	e := libetcd.New()
//	e.WithDir("/tmp/data")
//	if err := e.Start(); err != nil { /* ... */ }
//	defer e.Stop()
func New() v1.Etcd {
	return v1alpha1.New()
}
