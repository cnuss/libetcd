// Package store defines the discovery seed's backing-state contract — the two
// primitives the protocol needs, namespaced per cluster identity
// (the verified JWT sub). Implementations live in subpackages (see store/kvdb).
//
// The interface is deliberately narrow so the backing is swappable: kvdb.io
// today (atomic PATCH "+1" = the claim; prefix list + per-key TTL = the
// roster); DynamoDB (atomic ADD, query, TTL) or a loopback etcd (txn, lease)
// would each satisfy it without touching the seed's handlers.
package store

import (
	"context"
	"errors"
)

// ErrNotImplemented marks a scaffold stub; the seed maps it to HTTP 501.
var ErrNotImplemented = errors.New("not implemented")

// Store is the discovery seed's backing state.
type Store interface {
	// Claim atomically attempts the bootstrap claim for cluster sub. It returns
	// won=true to exactly one caller — the first — and false to the rest. The
	// claim carries a TTL refreshed on each call, so a bootstrapper that dies
	// before the cluster forms frees it and a later arrival can re-win. Once any
	// node has registered, the roster is non-empty and callers join instead of
	// claiming, so the claim is never re-won under a live cluster.
	//
	// The winner also mints the cluster's secret — a random join credential
	// keyed by sub — and Claim returns it. This binds the cluster to its sub: a
	// node identifies the cluster by this secret (not its own JWT), so every
	// node with the same sub shares one credential and a node with a different
	// sub gets a different one. A loser gets won=false and an empty secret (it
	// reads the secret from Roster instead, by which point the winner has minted
	// it).
	Claim(ctx context.Context, sub string) (won bool, secret string, err error)

	// Register advertises url as a live join target for cluster sub under a
	// stable id, with a TTL. Idempotent: re-calling refreshes the TTL
	// (keepalive-as-re-register), so a dead member's entry expires and is pruned
	// from the roster.
	Register(ctx context.Context, sub, id, url string) error

	// Roster returns the current live join-target URLs for cluster sub, plus the
	// cluster secret (see Claim). Once the roster is non-empty the winner has
	// minted the secret, so a joiner gets both in one call.
	Roster(ctx context.Context, sub string) (urls []string, secret string, err error)
}
