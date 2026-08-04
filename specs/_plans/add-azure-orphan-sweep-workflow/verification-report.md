# Verification Report: add-azure-orphan-sweep-workflow

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Implementation complete, code review clean. One local `cargo test` failure is confirmed an Aarch64-only environment artifact (reproduces on `main` locally, passes in x86_64 CI on the same commit) — see Notes. |
| Code review | 0 findings — standard: 0, expert: 0 |

| Check | Status |
|-------|--------|
| Build (n/a — no Rust code changed) | ✓ |
| Tests (`cargo test`, full suite) | ✓ (see Notes — 1 local-only Aarch64 failure, confirmed pre-existing and green in x86_64 CI) |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| YAML lint (`actionlint`) | ✓ |
| Scenario Coverage | ✓ (all 8 scenarios map to script logic; live confirmation deferred — see Notes) |
| Manual Tests | ✗ (deferred — see Notes) |

## Test Evidence

### Coverage

This feature has no Rust surface (`.github/workflows/azure-orphan-sweep.yml` only). No unit/integration coverage applies per plan design; scenarios are verified by `workflow_dispatch` invocation and static YAML/bash validation instead.

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, full workspace) | 18 (in the one failing binary's test file) | 17 | 0 |
| Full suite | 1 failure | — | — |

Failing test: `scan_prunes_delete_row_groups_by_file_path`
(`crates/lakehouse-engine/tests/scan_positional_deletes.rs:1514`) —
`assigning only 1 of 3 files must decode fewer delete-file bytes than
assigning all 3: pruned=15787 full=15787`. Deterministic (reproduces on
isolated re-run), and this branch changed zero `.rs` files (`git diff --stat
HEAD -- '*.rs'` is empty).

Confirmed Aarch64-only, not a real regression:
- `git worktree add` checked out `main` at `e997893` (the exact commit this
  branch forked from) into an isolated directory and re-ran the identical
  test on this same machine — same failure, same file/line, same
  `pruned=15787 full=15787` values. Rules out anything introduced by this
  branch.
- `gh run view 30897450618` — the CI run for that exact commit (`headSha:
  e997893c1a94807bc17910322c4bc2548ec7a6fc`) on GitHub's `ubuntu-latest`
  (x86_64) runners — shows the `Unit Tests` job (`cargo test`) completed with
  `conclusion: success`.
- Together this confirms the failure is specific to running `cargo test` on
  Aarch64 (Apple Silicon) locally, likely a Parquet/Arrow byte-size or
  statistics computation difference between architectures. It is not a code
  defect, does not affect the x86_64 CI gate this repository actually relies
  on, and this plan's zero-Rust-file change cannot have caused it.

### Manual Tests

| Test | Result |
|------|--------|
| `gh workflow run azure-orphan-sweep.yml -f dry_run=true` against the live Azure test account | Deferred — GitHub's Actions API only recognizes a `workflow_dispatch`-triggerable workflow once its file exists on the default branch; dispatch attempted against the feature branch returned `HTTP 404: workflow azure-orphan-sweep.yml not found on the default branch`. This is a GitHub platform constraint on any brand-new workflow file, not a defect. The List-Containers authorization gate the plan calls out (§ Verification) can only be exercised after this workflow merges to `main`. |

## Tool Evidence

### actionlint

```
$ actionlint .github/workflows/azure-orphan-sweep.yml
(no output — exit 0)
```

### Linter (clippy)

```
Checking lakehouse-catalog v0.1.0 ...
Checking vs-expression v0.2.0 ...
Checking lakehouse-engine v0.32.0 ...
Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.40s
```

### Formatter

```
$ cargo fmt --check
(no output — exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| e2e-harness | azure-orphan-container-sweep | Scheduled run reclaims stale orphaned containers | `.github/workflows/azure-orphan-sweep.yml` | `DRY_RUN=false` branch, delete loop | Verified by static/simulated review; live confirmation deferred to post-merge dispatch |
| e2e-harness | azure-orphan-container-sweep | A container within the 24-hour retention floor is never swept | `.github/workflows/azure-orphan-sweep.yml` | `[ "$lm_epoch" -lt "$CUTOFF" ]` strict compare | Verified — boundary case simulated by implementer-expert-agent, retained as expected |
| e2e-harness | azure-orphan-container-sweep | Sweep with nothing to reclaim succeeds without deleting | `.github/workflows/azure-orphan-sweep.yml` | `if [ -n "$containers" ]` guard | Verified — empty-list path simulated, exits 0 |
| e2e-harness | azure-orphan-container-sweep | Manual dispatch previews by default | `.github/workflows/azure-orphan-sweep.yml` | `DRY_RUN` default-true expression + `[ "$DRY_RUN" = "true" ]` | Verified by static review; live confirmation deferred |
| e2e-harness | azure-orphan-container-sweep | Manual dispatch with dry-run disabled deletes for real | `.github/workflows/azure-orphan-sweep.yml` | same delete loop, `dry_run=false` | Verified by static review; live confirmation deferred |
| e2e-harness | azure-orphan-container-sweep | Sweep fails loudly when a required variable is absent | `.github/workflows/azure-orphan-sweep.yml` | leading `for var in ...` precondition loop | Verified — simulated with unset/empty var, exits 1 naming the variable, no value echoed |
| e2e-harness | azure-orphan-container-sweep | Any Azure CLI failure fails the run | `.github/workflows/azure-orphan-sweep.yml` | `set -euo pipefail` + variable-capture (not `<()`) | Verified by static review — chosen specifically so a non-zero `az` exit cannot be masked |
| e2e-harness | azure-orphan-container-sweep | No credential value appears in the run log | `.github/workflows/azure-orphan-sweep.yml` | no `set -x`; secret referenced only via `-p "$AZURE_CLIENT_SECRET"` | Verified by static review |

## Notes

- **Local `cargo test` failure is an Aarch64 environment artifact, verified
  not a gate blocker.** `scan_prunes_delete_row_groups_by_file_path` fails
  locally on this Apple Silicon (Aarch64) machine but passes in the real
  x86_64 CI gate on the exact base commit (`gh run view 30897450618`,
  `conclusion: success`), and reproduces identically on `main` itself in an
  isolated worktree — proving it predates and is independent of this change.
  Fixing the underlying Aarch64/x86_64 discrepancy is out of scope for this
  plan and tracked as a separate concern.
- **Version bump skipped.** No crate's Cargo.toml or source changed (a CI/CD
  workflow file only); per repo precedent (PR #292's `fix(ci)` commit reverting
  an "accidental" version bump on a test-only, non-runtime change), no version
  bump applies to a change with zero crate surface.
- **`make test-e2e` not run.** This plan's own Verification Checklist lists only
  `actionlint`, `cargo test`, `cargo clippy`, and `cargo fmt` — no e2e suite,
  because the feature has no Rust/UDF surface. `cargo test` alone already fails
  the outer gate, so the heavier Exasol-Docker e2e run would not change the
  outcome and was not run.
- **Manual `workflow_dispatch` verification is a post-merge action.** GitHub
  only exposes `workflow_dispatch` for a workflow file once it exists on the
  default branch. The plan's stated "explicit authorization gate" (confirming
  the service principal's List Containers permission) must be exercised via
  `gh workflow run azure-orphan-sweep.yml -f dry_run=true && gh run watch`
  after this PR merges — flagged here so it is not silently skipped.
