# `snapshot.go` — vendored fork of etcd's `etcdutl` snapshot Restore

`snapshot.go` is a vendored, surgically modified copy of
[`go.etcd.io/etcd/etcdutl/v3/snapshot/v3_snapshot.go`](https://pkg.go.dev/go.etcd.io/etcd/etcdutl/v3@v3.6.12/snapshot)
(pinned at **v3.6.12** — the same version as the rest of the etcd modules in
`go.mod`). This document records exactly what was changed from upstream and why,
so the fork can be re-synced against a future etcd release.

## Why fork at all

`Join` ([`../executor.go`](../executor.go)) seeds a new node's data directory
from a leader snapshot before `Start`, so the node boots already caught up to
the leader's raft index. The leader then replicates forward over the log and
**never sends a raft snapshot** to the joiner.

That matters because applying a received raft snapshot panics the embedded host
on **Windows**: `etcdserver.applySnapshot` → `serverstorage.OpenSnapshotBackend`
does `os.Rename(<idx>.snap.db → member/snap/db)` over the still-open bbolt
backend, and Windows refuses to rename onto an open file (`"Access is denied"`).
The old backend is closed only *after* the rename (`etcdserver/server.go`,
`applySnapshot`). The bug is unchanged through `v3.8.0-alpha.0`; closest tracked
issue [etcd-io/etcd#18055](https://github.com/etcd-io/etcd/issues/18055).

Upstream `Restore` can't be used as-is to seed a node joining a **live** cluster:

- It **recomputes** member IDs deterministically (`NewClusterFromURLsMap` →
  `NewMember(..., now=nil)` = `sha1(peerURLs+token)`). But the live leader
  assigned the new learner a **timestamped** ID
  (`v3rpc/member.go`: `NewMemberAsLearner("", urls, "", &now)` =
  `sha1(peerURLs + "" + now.Unix())`). The two never match, and raft routes by
  member ID, so a restored node can't take the identity the cluster gave it.
- It writes a **fresh-bootstrap raft log starting at index 1**. Those low
  indices conflict with the leader's real history, whose prefix is compacted
  (`firstIndex=2` after the first memory snapshot) — so the leader would send a
  snapshot anyway, re-triggering the Windows panic.

The fork makes `Restore` seed a node that has *already applied* the leader's
snapshot at the leader's live raft index, with the leader's real member IDs.

## Changes from upstream

Line/structure references are against `v3_snapshot.go` @ v3.6.12.

### Mechanical

- `package snapshot` → `package hack`.
- Dropped now-unused imports: `encoding/json`, `go.etcd.io/etcd/server/v3/config`,
  `go.etcd.io/raft/v3` (`raftpb` is still used).
- Log messages `"restoring snapshot"` / `"restored snapshot"` →
  `"restoring join seed"` / `"restored join seed"` (and their fields).
- Deleted the revision-bump / mark-compacted feature, unused for join:
  `modifyLatestRevision`, `unsafeBumpBucketsRevision`,
  `unsafeMarkRevisionCompacted`, `unsafeGetLatestRevision`, and the
  `RevisionBump` / `MarkCompacted` config fields + their call in `Restore`.

`Save`, `Status`, `hasChecksum`, `bytesToRev`, `outDbPath`, `saveDB`,
`copyAndVerifyDB`, and `updateCIndex` are **unchanged** from upstream.

### `v3Manager` struct

- Added field `selfID uint64` (the leader-assigned member ID for this node).

### `RestoreConfig`

| Upstream | Fork |
| --- | --- |
| `PeerURLs []string` | removed |
| `InitialCluster string` | removed |
| `InitialClusterToken string` | removed |
| `RevisionBump uint64` | removed |
| `MarkCompacted bool` | removed |
| — | `SelfID uint64` — leader-assigned member ID for the joiner |
| — | `ClusterID uint64` — the live cluster's ID; the seed must agree on it |
| — | `Members []MemberInfo` — full membership as the leader reports it, IDs verbatim |

New type `MemberInfo { ID uint64; Name string; PeerURLs, ClientURLs []string; IsLearner bool }`.

### `Restore`

- Validates `SelfID != 0` and `ClusterID != 0`.
- **Cluster construction** — the central change:
  - Upstream: `types.NewURLs` + `types.NewURLsMap`, `config.ServerConfig.VerifyBootstrap()`,
    then `membership.NewClusterFromURLsMap(token, ics)` — **recomputes IDs**.
  - Fork: builds `[]*membership.Member` from `cfg.Members` (IDs, peer/client URLs,
    and learner flag **verbatim**), then
    `membership.NewClusterFromMembers(lg, types.ID(cfg.ClusterID), membs)` and
    `s.cl.SetID(self=cfg.SelfID, cluster=cfg.ClusterID)`. No recomputation, no
    `VerifyBootstrap`.
- Stashes `s.selfID = cfg.SelfID`.
- Reads the anchor index/term from the copied db (`readConsistentIndex`, new) and
  passes them to `saveWALAndSnap`.
- Dropped the `MarkCompacted && RevisionBump` block.

### `readConsistentIndex` (new)

Opens the copied db read-only and returns
`schema.UnsafeReadConsistentIndex(tx)` — the consistent `(index, term)` persisted
in the snapshot. That index is the leader raft index the applied db state
reflects, and is exactly where the seeded node must claim to be.

### `saveWALAndSnap` — now `saveWALAndSnap(snapIndex, snapTerm uint64)`

Upstream writes a **fresh cluster bootstrap**; the fork writes a node that has
**already applied a snapshot** at `(snapIndex, snapTerm)`:

| | Upstream | Fork |
| --- | --- | --- |
| Raft entries | one `ConfChangeAddNode` per member, indices `1..N`, term `1` | **none** (`w.Save(hardState, nil)`) |
| `HardState` | `{Term: 1, Vote: peers[0].ID, Commit: N}` | `{Term: snapTerm, Commit: snapIndex}` (no vote) |
| Snapshot metadata | `Index: N, Term: 1` | `Index: snapIndex, Term: snapTerm` |
| WAL `NodeID` | `s.cl.MemberByName(s.name).ID` | `s.selfID` (leader records the learner with an **empty name**, so the name lookup returns nil here) |
| Conf state | `Voters: <all member IDs>` (upstream literally notes `// TODO: This code ignores learners !!!`) | **splits** `Voters` / `Learners` by `Member.IsLearner`, preserving learner status |

Effect: on boot the node is a follower already applied to `snapIndex`, with the
leader's exact membership (learner still a learner). The leader's first append
finds agreement at `snapIndex` and replicates forward over the log — no
`MsgSnap`, so the Windows snapshot-apply path is never reached.

## Re-syncing against a newer etcd

1. Diff this file's `snapshot.go` against the new
   `etcdutl/v3/snapshot/v3_snapshot.go`:
   ```sh
   UTL=$(go env GOMODCACHE)/go.etcd.io/etcd/etcdutl/v3@<version>/snapshot/v3_snapshot.go
   diff <(tail -n +14 "$UTL") <(tail -n +35 snapshot.go) --unified=2
   ```
   (The `tail` offsets skip each file's license/package header so only the bodies
   diff.)
2. Re-apply the changes above onto the new upstream body.
3. Re-check the upstream bug is still present (the `os.Rename` in
   `serverstorage.OpenSnapshotBackend` before the old backend is closed). If etcd
   has fixed it, this whole fork — and the seeding step in `Join` — can be
   removed in favor of the stock learner-add flow.
