// Package libetcd is a thin, developer-friendly SDK for running embedded etcd.
//
// It wraps go.etcd.io/etcd/server/v3/embed behind a fluent builder: configure a
// node with With* methods, Start it, and get back a running handle that exposes
// a ready-to-use clientv3.Client wired to the in-process server.
//
//	e, err := libetcd.New().WithDir("/tmp/data").Start(ctx)
//	if err != nil { /* ... */ }
//	defer e.Close()
//	e.Client().Put(ctx, "k", "v")
//
// The package is split into three pieces:
//
//   - libetcd (this package) — thin façade exposing New. Stable surface for
//     application code.
//   - github.com/cnuss/libetcd/v1 — the stable Builder and Etcd interfaces.
//     Application code that wants to declare types against the contract imports
//     this.
//   - github.com/cnuss/libetcd/v1alpha1 — the current implementation. Internals
//     may change between alpha revisions; pin only if you need the concrete
//     types.
//
// Multi-node clusters bootstrap by listing every member's peer URL with
// WithPeers; a node with no peers configured starts as a single-member cluster.
package libetcd

import (
	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1"
)

// New returns an unconfigured Builder for an embedded etcd node. Configure it
// with the With* methods, then call Start.
//
//	e, err := libetcd.New().WithClientPort(2379).Start(ctx)
func New() v1.Builder {
	return v1alpha1.New()
}
