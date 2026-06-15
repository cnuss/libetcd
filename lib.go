// Package libetcd is a thin, developer-friendly SDK for running embedded etcd.
//
// It wraps go.etcd.io/etcd/server/v3/embed behind a fluent builder: configure a
// node with With* methods, Start it, and use the in-process loopback client.
//
//	e := libetcd.New()
//	e.WithDir("/tmp/data").WithClientListener(lc)
//	if err := e.Start(); err != nil { /* ... */ }
//	defer e.Stop()
//	e.Self().Put(ctx, "k", "v")
//
// The package is split into three pieces:
//
//   - libetcd (this package) — thin façade exposing New. Stable surface for
//     application code.
//   - github.com/cnuss/libetcd/v1 — the stable Server, Client, Builder, Executor, and
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

// Etcd is the node handle returned by New — the stable v1.Etcd surface
// (Server, Client, Builder, Executor) re-exported at the façade so callers can
// name it as libetcd.Etcd without importing the v1 package.
type Etcd = v1.Etcd

// EtcdPeer is the join handle returned by From — the stable v1.EtcdPeer surface
// re-exported at the façade so callers can name it as libetcd.EtcdPeer without
// importing the v1 package.
type EtcdPeer = v1.EtcdPeer

// New returns an embedded etcd node. Configure it with the With* methods (they
// mutate in place and chain), then call Start; Stop shuts it down.
//
//	e := libetcd.New()
//	e.WithDir("/tmp/data")
//	if err := e.Start(); err != nil { /* ... */ }
//	defer e.Stop()
func New() Etcd {
	return v1alpha1.New()
}

// From returns a node for an existing cluster reachable at the given peer URLs
// (e.g. another node's Peers()), or — called with no peer URLs — a fresh node
// that bootstraps a new cluster. Either way it's the same join-only handle:
// configure it with the With* methods, then call Join. With peers, Join brings
// the node into that cluster; with none, Join bootstraps (see EtcdPeer.Join),
// so From() ... Join() is the one entry point for both roles.
//
// Peers are plain strings — bare host:port, http://, or https:// entries are
// accepted; at Join time the library trims them, defaults a missing scheme to
// http, de-duplicates, and silently drops any it can't parse.
//
// Join runs entirely over the cluster's peer (raft) listener: the node POSTs
// itself to a peer's /libetcd/v1/join endpoint, restores the snapshot the peer
// streams back, starts, and promotes itself to a voting member — no networked
// client is dialed, so a node needs only the peer transport to join (and a
// fully headless cluster, serving no client traffic anywhere, is joinable).
// The join is authorized by the cluster token (WithClusterToken), so it is
// libetcd-to-libetcd: a stock etcd cluster doesn't serve the endpoint. The
// token gate is only meaningful over a TLS peer listener; see issue #74.
func From(peers ...string) EtcdPeer {
	return v1alpha1.From(peers...)
}
