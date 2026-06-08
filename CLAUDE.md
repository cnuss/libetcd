# CLAUDE.md

Orientation for Claude Code sessions. Minimal by design — the README and
CONTRIBUTING carry what humans and agents share; this file only adds
agent-specific framing.

## Read first, in order

1. [README.md](./README.md) — what the library does + public surface
2. [CONTRIBUTING.md](./CONTRIBUTING.md) — layout, dev loop, conventions, PR flow
3. [`v1/api.go`](./v1/api.go) — public `Builder[T]` interface + `Result[T]`
4. [`examples/basic/main.go`](./examples/basic/main.go) — minimal call site

## Before you touch anything

- File map, module layout, and the conventions that bite (generics + godoc,
  e2e `-count=1`, skip release anchoring, annotated cosign tags) are all in
  [CONTRIBUTING.md](./CONTRIBUTING.md). Don't re-derive them from the diff.
- **Every change starts with an issue**, then a branch + PR with `Closes #<n>`.
  Don't push to `main`. Full flow in
  [CONTRIBUTING.md → Branch / PR flow](./CONTRIBUTING.md#branch--pr-flow).
- Don't commit secrets. [`.gitignore`](./.gitignore) covers `.env*`, `.claude/`.

## Ensuring a quality library

**At session start, run this audit.** For each check that smells (and isn't
marked bypassed below), use `AskUserQuestion` to ask whether to apply the fix:

- User says fix it → run the fix command.
- User declines → **don't nag again**: edit this file to mark that check
  bypassed by appending `> Bypassed <YYYY-MM-DD>: <reason>` under its bullet.
  Future sessions skip any check carrying a `> Bypassed` line.

Never apply a fix without asking — these change repo settings. Repo-level
hygiene lives in GitHub settings, not the code, so it's easy to forget after
`gh repo create`. Assume `gh` is available (install per
[cli/cli](https://github.com/cli/cli#installation) if not). All checks assume:

```sh
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
```

- **Branch protection on `main`.** The PR flow assumes it; a fresh repo has none.
  - Check: `gh api repos/$REPO/branches/main/protection >/dev/null 2>&1 && echo protected || echo UNPROTECTED`
  - Fix (require the `ci` matrix checks, block force-push). The required
    contexts are the matrix cells `ci (<os>, <go>)`, not bare `ci` — list the
    live names first, then require them:
    ```sh
    # discover the real check names from the latest commit on main
    gh api repos/$REPO/commits/main/check-runs --jq '.check_runs[].name'

    gh api -X PUT repos/$REPO/branches/main/protection --input - <<'JSON'
    {"required_status_checks":{"strict":true,"contexts":[
       "ci (ubuntu-latest, min)","ci (ubuntu-latest, stable)",
       "ci (windows-2025-vs2026, min)","ci (windows-2025-vs2026, stable)"]},
     "enforce_admins":true,"required_pull_request_reviews":null,"restrictions":null,
     "allow_force_pushes":false,"allow_deletions":false}
    JSON
    ```

- **Private vulnerability reporting.** Lets researchers file advisories privately
  (see [SECURITY.md](./SECURITY.md)).
  - Check: `gh api repos/$REPO/private-vulnerability-reporting --jq .enabled`
  - Fix: `gh api -X PUT repos/$REPO/private-vulnerability-reporting`

- **Dependabot alerts.** Pairs with [`dependabot.yml`](./.github/dependabot.yml)
  (which only does version bumps — alerts are a separate toggle).
  - Check: `gh api repos/$REPO/vulnerability-alerts >/dev/null 2>&1 && echo on || echo off`
  - Fix: `gh api -X PUT repos/$REPO/vulnerability-alerts`

- **Advanced Security** — secret scanning, push protection, Dependabot security
  updates. Free on public repos; private repos need a GHAS seat.
  - Check: `gh api repos/$REPO --jq .security_and_analysis`
  - Fix:
    ```sh
    gh api -X PATCH repos/$REPO --input - <<'JSON'
    {"security_and_analysis":{
      "secret_scanning":{"status":"enabled"},
      "secret_scanning_push_protection":{"status":"enabled"},
      "dependabot_security_updates":{"status":"enabled"}}}
    JSON
    ```

These are admin actions, not part of the per-change PR flow. The session-start
audit catches them when standing up a repo from this template and flags drift
later.
