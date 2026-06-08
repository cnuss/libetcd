// Package v1 is the stable public surface for libetcd. The Builder and Etcd
// interfaces here are the contract callers depend on across releases; the
// implementation lives in v1alpha1 and may change between alpha revisions.
//
// libetcd is a thin, developer-friendly SDK for running embedded etcd: configure
// a node with a fluent builder, Start it, and get back a ready-to-use
// clientv3.Client wired to the in-process server.
package v1

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// Builder configures and starts an embedded etcd node. Configure it with the
// With* methods (each returns the Builder for chaining), then call the terminal
// Start to boot the server and obtain a running Etcd. Obtain one from
// libetcd.New.
//
// Defaults (single node, no method calls): name "default", a fresh temp data
// dir, client URL http://localhost:2379, peer URL http://localhost:2380, a new
// cluster, and log level "error".
type Builder interface {
	// WithName sets the node (member) name. Default "default". With WithPeers,
	// the name must match this node's key in the peers map.
	WithName(name string) Builder
	// WithDir sets the data directory. Default: a fresh os.MkdirTemp directory,
	// which the caller is responsible for removing after Close.
	WithDir(dir string) Builder
	// WithClientPort sets the localhost client (v3 API) port. 0 picks a free
	// port at Start. Default 2379. Ignored if WithClientURL is set.
	WithClientPort(port int) Builder
	// WithPeerPort sets the localhost peer (raft) port. 0 picks a free port at
	// Start. Default 2380. Ignored if WithPeerURL is set.
	WithPeerPort(port int) Builder
	// WithClientURL overrides the client port with explicit listen+advertise
	// client URLs (e.g. "http://0.0.0.0:2379"). Advanced; most callers use
	// WithClientPort.
	WithClientURL(urls ...string) Builder
	// WithPeerURL overrides the peer port with explicit listen+advertise peer
	// URLs. Advanced; most callers use WithPeerPort.
	WithPeerURL(urls ...string) Builder
	// WithPeers declares a multi-node initial cluster as a map of member name to
	// peer URL. The map must include this node (its WithName key -> its peer
	// URL). Unset, the node bootstraps a single-member cluster.
	WithPeers(peers map[string]string) Builder
	// WithClusterToken sets the initial-cluster token, which scopes peer
	// discovery so unrelated clusters don't cross-join. Default
	// "libetcd-cluster".
	WithClusterToken(token string) Builder
	// WithExistingCluster marks the node as joining an already-bootstrapped
	// cluster (ClusterState "existing") rather than bootstrapping a new one.
	WithExistingCluster() Builder
	// WithLogLevel sets the server log level (e.g. "error", "warn", "info").
	// Default "error".
	WithLogLevel(level string) Builder
	// Start boots the embedded server, waits until it is ready (or ctx is done),
	// dials a client to it, and returns the running Etcd. The ctx bounds only
	// startup; the server runs until Close.
	Start(ctx context.Context) (Etcd, error)
}

// Etcd is a running embedded etcd node together with its lifecycle. Close it
// when done.
type Etcd interface {
	// Client returns a clientv3.Client wired to this node, ready for Put/Get/etc.
	// It is owned by the Etcd and closed by Close; do not close it yourself.
	Client() *clientv3.Client
	// Endpoints returns the node's actual bound client endpoints, with any
	// auto-selected (port 0) ports resolved to concrete ports.
	Endpoints() []string
	// Server returns the underlying embed.Etcd handle as an escape hatch for
	// advanced configuration not exposed by the Builder.
	Server() *embed.Etcd
	// Close closes the client, then stops the embedded server. It is safe to call
	// once; the data directory is not removed.
	Close() error
}
