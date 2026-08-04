# Tasks: add-azure-orphan-sweep-workflow

## Phase 2: Implementation (Group A)
- [x] 2.1 Author `.github/workflows/azure-orphan-sweep.yml` skeleton: `schedule`
      (`0 2 * * 1`) and `workflow_dispatch` triggers, the `dry_run` boolean input
      defaulting to `true`, `permissions: {}`, and the four env bindings from
      `vars.AZURE_STORAGE_ACCOUNT_NAME` + the three secrets, scoped to the sweep step.
- [x] 2.2 Amend ADR 051 via the decision-log entry — already recorded in
      decision-log.md Design Decision [1] (`Supersedes:
      azure-e2e-orphan-sweep-out-of-band`, `Promotes to ADR: yes`); no additional
      implementation action, `/speq:record` performs the actual ADR file edit.

## Phase 2: Implementation (Group B)
- [x] 2.3 Write the sweep `run:` step: `set -euo pipefail`; a precondition check
      that fails naming any of the four variables that is unset or empty, echoing
      no value; `az login --service-principal` with the three secrets; resolve
      `DRY_RUN` from the `workflow_dispatch`/`schedule` expression; list
      `lhrs-e2e-` containers; filter by `last_modified` against a `now-24h`
      cutoff; and either log candidates (dry-run) or delete them (real). [expert]

## Phase 3: Verification
- [x] 3.1 Run `actionlint .github/workflows/azure-orphan-sweep.yml` — exit 0 (PASS)
- [x] 3.2 Run `cargo test` — 1 local Aarch64-only failure
      (`scan_prunes_delete_row_groups_by_file_path`); zero `.rs` files changed on
      this branch. Confirmed via `git worktree` re-run on `main` (same failure)
      and `gh run view` on that commit's x86_64 CI (`Unit Tests`: success) —
      environment artifact, not a gate blocker. See verification-report.md.
- [x] 3.3 Run `cargo clippy --all-targets` — 0 errors/warnings (PASS)
- [x] 3.4 Run `cargo fmt --check` — no changes (PASS)
