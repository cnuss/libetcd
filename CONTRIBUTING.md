# Contributing

This document is for everyone working on `libetcd` — humans and AI agents alike.
It covers the layout, the local dev loop, the conventions that bite, and how a
change gets from an issue to a release.

## Where to find things

Deep-link by filename; line numbers will drift.

| Topic                                          | Source                                                           |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| Façade (`New`)                                 | [`lib.go`](./lib.go)                                             |
| Interfaces (`Server`/`Client`/`Builder`/`Executor`/`Etcd`/`EtcdPeer`) | [`v1/v1.go`](./v1/v1.go)                             |
| Type (`EtcdImpl`) + `New` + `From`             | [`v1alpha1/v1alpha1.go`](./v1alpha1/v1alpha1.go)                 |
| Builder methods + config machinery             | [`v1alpha1/builder.go`](./v1alpha1/builder.go)                   |
| Server handles                                 | [`v1alpha1/server.go`](./v1alpha1/server.go)                     |
| Client accessors (`Self`/`Leader`/`Voters`/`Peers`) | [`v1alpha1/client.go`](./v1alpha1/client.go)               |
| Executor lifecycle (`Start`/`Stop`)            | [`v1alpha1/executor.go`](./v1alpha1/executor.go)                |
| Join-from-peer-URLs (`From`/`EtcdPeer`)        | [`v1alpha1/peer.go`](./v1alpha1/peer.go)                         |
| Peer-port join protocol (`/libetcd/v1/join`)   | [`v1alpha1/join/`](./v1alpha1/join)                              |
| Cross-process join lock (etcd lease + mutex)   | [`v1alpha1/lock/`](./v1alpha1/lock)                              |
| Snapshot seed fork (restores the join snapshot) | [`v1alpha1/snapshot/`](./v1alpha1/snapshot)                     |
| Raft stream 206 handling (issue #8)            | [`v1alpha1/stream/`](./v1alpha1/stream)                          |
| gRPC/REST gateway wiring                       | [`v1alpha1/gateway.go`](./v1alpha1/gateway.go)                  |
| Unit tests (per interface)                     | [`v1alpha1/*_test.go`](./v1alpha1)                              |
| e2e harness + runner                           | [`e2e/e2e_test.go`](./e2e/e2e_test.go)                           |
| Worked examples                                | [`examples/`](./examples)                                        |
| Build / lint / test commands                   | [`Makefile`](./Makefile)                                         |
| Release + skip release regex                   | [`.github/workflows/ci.yml`](./.github/workflows/ci.yml)         |
| CodeQL scan                                    | [`.github/workflows/codeql.yml`](./.github/workflows/codeql.yml) |
| OpenSSF Scorecard scan                         | [`.github/workflows/scorecard.yml`](./.github/workflows/scorecard.yml) |
| Dependabot config                              | [`.github/dependabot.yml`](./.github/dependabot.yml)             |
| Cosign verification recipe                     | [`SECURITY.md`](./SECURITY.md)                                   |
| Orientation for AI agents                      | [`CLAUDE.md`](./CLAUDE.md)                                       |

## Module layout

Three packages, stable/alpha versioning:

```
github.com/cnuss/libetcd           — root façade. Stable surface (New).
github.com/cnuss/libetcd/v1        — stable Builder + Etcd interfaces.
github.com/cnuss/libetcd/v1alpha1  — current implementation. May change
                                   between alpha revisions.
```

Application code imports the root (`libetcd.New(name)…`). Code that needs to declare
types against the interfaces imports `v1`. Direct access to the `EtcdImpl`
struct lives in `v1alpha1`. The `v1alpha1` package wraps
`go.etcd.io/etcd/server/v3/embed` — keep the `embed`-specific glue there, behind
the stable `v1` surface.

## Local development

Requires Go 1.23 or later (the floor etcd's `embed` package needs).

```sh
git clone https://github.com/cnuss/libetcd.git
cd libetcd
make test   # library unit tests (fast, in-package)
make e2e    # builds and runs every example binary
```

Run a specific example locally:

```sh
make run single-node
```

## Before you push

- `gofmt -w .`
- `go vet ./...`
- `make test`
- `make e2e`

CI runs the same on every PR.

## Conventions that bite

Easy to get wrong from the diff alone:

- **`examples/` is intentionally duplicated.** Each `main.go` is a
  copy-pasteable starter; no shared internal package. Don't refactor it into
  one.
- **`v1alpha1` tests start a real node.** They mint and (in `executor_test.go`)
  start an embedded etcd, so they take a second or two and need a free loopback
  port. Use `WithDir(t.TempDir())` and let `Start` auto-bind listeners.
- **e2e builds binaries at runtime**, so the test cache can't see example
  source changes — `make e2e` passes `-count=1` to force a rebuild.
- **Skip-release token must be line-anchored.** The regex in
  [`ci.yml`](./.github/workflows/ci.yml) (`resolve tag` step) is
  `^[[:space:]]*\[skip release\][[:space:]]*$`. Inline prose mentions are safe;
  a standalone line in the commit body opts out.
- **Cosign / Scorecard tags are annotated.** `ossf/scorecard-action` publishes
  annotated tags; pinning the tag-object SHA fails Sigstore verification
  ("imposter commit"). Pin to the commit underneath (see existing entries in
  [`scorecard.yml`](./.github/workflows/scorecard.yml)).

## Adding an example

Examples live in `./examples/<name>/main.go`. Keep each example self-contained
(there's no shared internal package — the duplication is intentional, so each
example is copy-pasteable on its own).

Print a single recognizable line so the e2e harness can assert on it, then add
a row to the `cases` table in `e2e/e2e_test.go` (name + expected substring) and
to the README's example table.

## Branch / PR flow

**Every change starts with an issue** — no exceptions, including retroactive
cleanups. The PR body always carries a `Closes #<n>` line so the merge
auto-closes the tracking issue and leaves a paper trail.

```sh
gh issue create --title "…" --body "…"                    # 1. issue first
git switch -c <type>/<topic>                              # 2. branch
# ... edits, commit ...
git push -u origin <type>/<topic>
gh pr create --title "<type>: …" --body "Closes #<n>. …"  # 3. PR refs the issue
# CI green ⇒
gh pr merge <pr#> --squash --delete-branch
```

`main` is protected (`ci` required; no force-push). Don't push directly to it
for routine work — PR flow gives CI + auto-release a clean audit trail. Pushing
to `main` auto-bumps a patch tag and signs the release (see Releasing below).

Don't commit secrets. [`.gitignore`](./.gitignore) covers `.env*`, `.claude/`,
etc.

## Pull requests

- Keep PRs focused. One feature or fix per PR.
- Include test coverage for behavior changes — lib tests (`v1alpha1/`) for API
  changes, e2e tests (`e2e/e2e_test.go`) for example-visible changes.
- **Keep the README in sync with the façade.** The README mirrors the public
  surface, so any change to it must update the README in the same PR:
  - a new/changed/removed method on `Builder` (or the `v1` surface) → update
    the **API at a glance** block and the **Quick Start** snippet;
  - a new example → add a row to the **Examples** table;
  - a renamed package/version tier → update the **Layout** tree.
  Treat the README's code blocks as documentation that must compile against the
  current API — stale snippets are a review blocker.
- Signed commits preferred. The repo enables commit signing locally; CI does
  not enforce signatures.

## Commit messages

Short subject (≤ 72 chars), imperative mood ("Add X", not "Added X").
Wrap body at ~72 cols. Explain the *why*; the diff covers the *what*.

## Releasing

Patch releases are automatic. Every push to `main` runs the `Release`
workflow, which bumps the patch component of the latest `v*` tag,
re-runs `go vet`, `go build`, `make test`, and `make e2e` against that
ref, then:

- pushes the new tag,
- creates a GitHub Release with auto-generated notes, and
- warms `proxy.golang.org` so [pkg.go.dev](https://pkg.go.dev/github.com/cnuss/libetcd)
  surfaces the new version without manual prodding.

To opt a commit out of the auto-bump, put `[skip release]` on its own
line in the commit body. (It must be the only thing on its line, so
prose mentioning the token inline doesn't accidentally suppress.)

For a minor or major bump, tag locally and push the tag — the workflow
treats a manual tag as the version of record and skips the bump:

```sh
git tag v0.2.0
git push --tags
```

Tags must follow `vMAJOR.MINOR.PATCH` (Go module semver).

## License

By contributing you agree your contributions are licensed under the
[MIT License](./LICENSE).
