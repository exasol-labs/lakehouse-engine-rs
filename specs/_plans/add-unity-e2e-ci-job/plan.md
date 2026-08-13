# Plan: add-unity-e2e-ci-job

## Summary

Add an `e2e-unity` CI job that runs the Unity Catalog E2E suite against the local OSS Unity
Catalog + MinIO stack on every CI run, gating `release` alongside `e2e-lakekeeper`, and add
`--features unity-e2e` to the lint step so the suite is type-checked even when no stack runs.

## Design

### Context

The Unity Catalog E2E suite shipped in #327 runs only on a developer machine. Two independent CI
gaps let it rot, closed together by issue #328:

1. **No job executes it.** `ci.yml` has `e2e`, `e2e-lakekeeper`, and `e2e-azure`, but nothing
   invokes the `unity-e2e` suite. A regression in UC-native `createVirtualSchema` listing or
   column mapping reaches `main` unnoticed. Verified: `grep -i unity .github/workflows/ci.yml`
   returns zero hits.
2. **No job compiles it.** `crates/lakehouse-engine/tests/e2e_unity_test.rs:18` is
   `#![cfg(feature = "unity-e2e")]`, and `cargo clippy --workspace --all-targets` (`ci.yml:216`)
   passes no `--features`. The file compiles to an empty crate, so it can stop compiling with no
   job turning red.

Gap 2 is the cheaper one and needs no Docker stack: clippy only type-checks, and `unity-e2e = []`
pulls in no dependencies.

For Lakekeeper, the equivalent CI-job decision was tracked as its own ADR
(`verify-lakekeeper-continuously-dedicated-ci-job`, `specs/_decision/021-add-lakekeeper-e2e.md`),
separate from the suite that ADR's plan delivered. Unity has the suite but not the decision. This
plan records it.

- **Goals** — run the Unity suite continuously; gate `release` on it; type-check the suite on every
  CI run; keep the stale coverage-step comment accurate.
- **Non-Goals** — no change to the suite's assertions, the compose overlay, `scripts/unity/seed.sh`,
  or the `unity-up` / `test-e2e-unity` Makefile targets. No change to the coverage step's behavior
  (see § Consequences). No fix for the identical lint gap affecting the other four e2e features
  (see § Consequences).

### Decision

Mirror `e2e-lakekeeper` (`ci.yml:563-669`) as closely as Unity's stack allows, and diverge only
where Unity's fixture provisioning genuinely differs.

#### Architecture

```
build-so ──▶ e2e            ─┐
        ├──▶ e2e-lakekeeper ─┤──▶ release   (gating)
        ├──▶ e2e-unity      ─┘
        └──▶ e2e-azure  ─────X  (deliberately non-gating: live third-party account)
```

The new job's bring-up sequence, and why it is not `make unity-up`:

```
$COMPOSE up -d minio-init   →  docker wait  →  exit-code check   (bucket `warehouse` ready)
$COMPOSE up -d --wait minio exasol unitycatalog                  (compose healthchecks gate)
./scripts/unity/seed.sh                                          (own step, own failure attribution)
cargo test -p lakehouse-engine --features unity-e2e ...          (flag-identical to Makefile:250)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| One-shot container `docker wait`-ed, then long-running services with `--wait` | bring-up step | `up --wait` does not reliably hold a one-shot in the same wait set — the rationale already written at `ci.yml:592-600` |
| Explicit exit-code check on `minio-init` | bring-up step | `Makefile:238` starts `minio-init` fire-and-forget; a failed bucket creation would otherwise surface as a confusing seed failure |
| `seed.sh` as its own CI step | bring-up | Own step name in the Actions UI; the script's `set -euo pipefail` already fails loud |
| Inline `cargo test` with a flag-identical comment naming the Makefile authority | test step | Same idiom the coverage step already uses (`ci.yml:277-278`); calling `make test-e2e-unity` would re-run the bring-up this job just performed |
| Failure diagnostics: `ps`, `free -m`, host `dmesg`, per-service logs, Exasol crash logs, artifact upload | failure steps | A UDF sandbox crash appears only in the host kernel ring buffer, not DB-side logs |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `e2e-unity` gates `release` | Omit it, as `e2e-azure` is omitted | `e2e-azure`'s exclusion rationale (`ci.yml:791-792`) is a live third-party account whose outage could block every release. Unity's stack is entirely local Docker, like Lakekeeper's, so that rationale does not transfer. The image is pinned to `unitycatalog/unitycatalog:v0.5.0` (`docker-compose.unity.yml:40`), so no upstream push can break a release either |
| `timeout-minutes: 45` | A larger budget for the ~5.7 GB image pull | Matches `e2e-lakekeeper` exactly. Unity brings up three services against Lakekeeper's five and runs one one-shot against its three, so pull weight is offset by less bring-up. A wall-clock backstop only needs to beat a hang, not to fit tightly |
| Bring the stack up with explicit CI steps, not `make unity-up` | `run: make unity-up` | `unity-up` (`Makefile:235-239`) does not check `minio-init`'s exit code, so a failed bucket creation would surface only later as a seed error. Explicit steps also give per-phase attribution in the Actions UI, matching `e2e-lakekeeper`'s rigor |
| Inline the `cargo test` line rather than adding a bring-up-free Makefile target | Split `test-e2e-unity` into a `unity-up` half and a cargo-only half that CI calls | The split is cleaner on paper — single owner of the flags — but adds a Makefile target the issue never asked for. The repo already has an idiom for exactly this duplication: `ci.yml:277-278` names the CI step as the authority and requires flag-identity with the Makefile. Reusing that idiom costs one comment instead of a new public target. Noted: this makes `e2e-unity` the only e2e job whose test step is not `make test-e2e-*` |
| Add `--features unity-e2e` to the lint step only | Also add it to the coverage step | `cargo llvm-cov` wraps `cargo test` and *executes* every compiled test binary. Adding the feature there would run `e2e_unity_test` inside `unit-tests`, which has no Unity stack. The coverage step enables no e2e feature for any suite today; this plan keeps that invariant |
| Leave `exasol-e2e`, `cloud-e2e`, `lakekeeper-e2e`, and `azure-e2e` out of the lint step | Add all five features at once | The identical lint gap affects 14 other e2e test files, and `--features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e,unity-e2e` was verified clippy-clean (exit 0). Widening the fix is safe but is the same out-of-scope widening already declined for the coverage step. Tracked as a follow-up instead (§ Dependencies) |
| Reuse the hardcoded `lakehouse-engine-rs-minio-init-1` container name | Derive it via `docker compose ps -q minio-init` | No compose file pins a project `name:`, so the name is the checkout directory. All three existing jobs hardcode it and work because `actions/checkout` clones into a directory named after the repo. Introducing a second pattern for one job would leave two conventions in one file; the coupling is pre-existing and repo-wide (§ Dependencies) |
| Insert `e2e-unity` after `e2e-lakekeeper`, before `e2e-azure` | Append after `e2e-azure` | Keeps the three release-gating e2e jobs contiguous and leaves the deliberately non-gating `e2e-azure` last, adjacent to the `release` comment explaining its absence |

## Features

None. This plan adds no scenario and changes no existing one, so it carries no spec delta.

The suite's own behavior spec — `e2e-harness/unity-catalog-e2e-harness` — is untouched: fixture
bring-up, `createVirtualSchema` listing and column-mapping assertions, the fail-not-skip contract,
and the no-credential-leak contract all keep their current normative text and their current tests.
This plan changes only *where* that suite runs, which no `## Scenarios` clause states. The spec
library has no CI-job domain, and the established way to track a CI-job decision here is a
decision-log ADR — the precedent set by `verify-lakekeeper-continuously-dedicated-ci-job`,
`azure-e2e-ci-no-fork-coverage-goal` (plan `azure-e2e-ci-scope-simplification`, which recorded zero
spec deltas), and `azure-orphan-sweep-in-repo-workflow`.

The fail-not-skip contract needs no restatement: the new job inherits it from the suite itself.
`e2e_unity_test.rs` fails when the stack is unreachable, so an unreachable stack fails the job.

## Impact

Operator-facing, in three parts:

1. **Every CI run gains one job.** `E2E (Unity)` brings up MinIO, Exasol, and Unity Catalog OSS,
   seeds the Delta fixtures, and runs the suite. Cost: one additional `ubuntu-latest` runner for up
   to 45 minutes, including a ~5.7 GB image pull.
2. **A red `E2E (Unity)` now blocks the release job.** On a `main` push, a Unity regression stops
   tag and Release creation. It does **not** block a pull-request merge yet: `main`'s ruleset
   requires seven checks — `Check & Lint`, `Unit Tests`, `License Check`, `E2E`,
   `E2E (Lakekeeper)`, `Build Host Workspace`, `Sonar Analysis` — and `E2E (Unity)` is not among
   them. Adding it requires a repository-settings change no file in this plan can make. Task 5.1
   records that follow-up; until it is done, Unity parity with Lakekeeper is partial.
3. **The lint job compiles more code.** `cargo clippy` now type-checks `e2e_unity_test.rs` and the
   four `tests/common/` harness modules it pulls in. Verified green today, so the step does not turn
   red on merge. Because the restored `workspace-rlib` cache was written without the feature, lint
   recompiles the `lakehouse-engine` targets on top of it — measured at 16 s locally. The lint job
   never saves the cache, so no other job is affected.

No production crate behavior changes. No test assertion changes.

## Dependencies

None blocking. Three follow-ups this plan deliberately does not take:

| Follow-up | Why deferred |
|-----------|--------------|
| Add `E2E (Unity)` to `main`'s required status checks | Repository-settings change, not a file change; see Impact item 2 and task 5.1 |
| Extend the lint step to the other four e2e features | Same out-of-scope widening already declined for the coverage step; verified safe, so a follow-up is mechanical |
| Pin a compose project `name:` so container names stop tracking the checkout directory | Pre-existing and repo-wide; affects `e2e` and `e2e-lakekeeper` equally |

## Implementation Tasks

Tasks 1-4 all edit `.github/workflows/ci.yml`. Run them in the listed order so line numbers stay
predictable, and re-derive each anchor from the current file rather than trusting the numbers below
if an earlier task has already shifted them.

### 1. Type-check the suite in the lint job

- [ ] 1.1 Change `ci.yml:216` from
  `run: cargo clippy --workspace --all-targets -- -D warnings` to
  `run: cargo clippy --workspace --all-targets --features unity-e2e -- -D warnings`.
  Add a comment above the step stating why one feature of five is enabled: `unity-e2e` gates
  `crates/lakehouse-engine/tests/e2e_unity_test.rs` entirely
  (`#![cfg(feature = "unity-e2e")]`), so without the flag that file compiles to an empty crate and
  can silently stop compiling. Note that the flag costs no Docker stack and no dependencies
  (`unity-e2e = []`), and that the other four e2e features remain unchecked as a tracked follow-up.
  Do **not** change `cargo fmt` (`ci.yml:213`) or the `Makefile` `lint` target.
- [ ] 1.2 Fix the stale enumeration at `ci.yml:278`. It reads
  `# the authority. exasol-e2e, cloud-e2e, lakekeeper-e2e and azure-e2e are` and omits `unity-e2e`,
  which #327 added. Include `unity-e2e` so the claim on lines 279-280 — that this step compiles
  none of them and never touches a live stack — stays true and complete. This comment governs the
  **coverage** step; the coverage command itself (`ci.yml:285`) MUST NOT change, and MUST stay
  byte-identical to the `Makefile` `coverage` target (`Makefile:201-202`) as that target's own
  comment requires.

### 2. Add the `e2e-unity` job

- [ ] 2.1 Insert a complete `e2e-unity` job immediately after the `e2e-lakekeeper` job ends
  (currently `ci.yml:669`, before the `e2e-azure` comment block at 671), modelled on
  `ci.yml:563-669`. This is one contiguous insertion — do not interleave it with tasks 1, 3, or 4.
  Required elements:
  - A leading comment block, in the style of `ci.yml:563-568`: third viability gate, native Unity
    Catalog OSS; the overlay adds only `unitycatalog` plus an `exasol` `extra_hosts` override on
    top of the base stack and reuses base `minio`; independent of `e2e` and `e2e-lakekeeper` (same
    `needs`, no dependency between them) so all three run in parallel; gates `release`.
  - `name: E2E (Unity)` — this exact string becomes the status-check name in task 5.1.
  - `runs-on: ubuntu-latest`, `needs: [build-so]`, `timeout-minutes: 45`,
    `env: LH_EXASOL_CPUS: "2"`, each matching `e2e-lakekeeper`.
  - `- uses: actions/checkout@v7` then `- uses: ./.github/actions/e2e-setup`. The composite action
    supplies the free-disk step the ~5.7 GB UC image needs, the `lakehouse-engine-so` artifact
    download, the Rust 1.94 toolchain, and the `apparmor_restrict_unprivileged_userns` sysctl.
  - A `Pull stack images` step:
    `docker compose -f docker-compose.yml -f docker-compose.unity.yml pull --quiet`.
  - A `Start stack (Exasol + MinIO + Unity Catalog)` step whose script is exactly:
    ```bash
    COMPOSE="docker compose -f docker-compose.yml -f docker-compose.unity.yml"

    $COMPOSE up -d minio-init
    INIT_EXIT=$(docker wait lakehouse-engine-rs-minio-init-1)
    if [ "$INIT_EXIT" != "0" ]; then
      echo "::error::minio-init exited $INIT_EXIT (bucket creation failed)"
      $COMPOSE logs minio-init
      exit 1
    fi

    $COMPOSE up -d --wait minio exasol unitycatalog
    ```
    Precede it with a comment carrying the two-phase rationale, cross-referencing
    `ci.yml:592-600`, and adding what is specific here: `minio-init` runs first because
    `scripts/unity/seed.sh` requires the `warehouse` bucket, and its exit code is checked because
    `Makefile:238` does not check it.
  - A separate `Seed Unity Catalog fixtures` step: `run: ./scripts/unity/seed.sh`. The script is
    already executable (mode 775) and needs no environment variable: `LH_NETWORK` defaults to
    `lakehouse-engine`, matching the compose network's pinned `name:` (`docker-compose.yml:175`),
    and `LH_UNITY_PORT` defaults to `18080`, matching `docker-compose.unity.yml:64`. It needs only
    `docker` and `python3`, both present on `ubuntu-latest`. Its own `set -euo pipefail` makes it
    fail loud.
  - A `Run Unity Catalog E2E suite` step:
    `run: cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1`.
    This MUST stay flag-identical to `Makefile:250`; state that in a comment above the step, name
    this step as the authority, and explain that `make test-e2e-unity` is not used because it calls
    `$(MAKE) unity-up`, re-running the bring-up the two steps above just completed.
  - A `Dump Unity stack logs on failure` step, `if: failure()`, mirroring `ci.yml:636-657` with
    Unity's services: `$COMPOSE ps`, `free -m`, the same `sudo dmesg --ctime` grep and 40-line
    fallback, then `logs | tail -100` for `minio-init`, `unitycatalog`, and `minio`, then the same
    Exasol `/exa/logs/cored` crash-log `find`, then collect `/exa/logs` into `exa-logs-unity/`.
    Every command keeps its `|| true` so one missing service cannot mask the rest.
  - An `Upload Unity Exasol logs` step, `if: failure()`, `actions/upload-artifact@v7`,
    `name: exa-logs-unity`, `path: exa-logs-unity/`, `if-no-files-found: ignore`.
  - A `Stop stack` step, `if: always()`:
    `docker compose -f docker-compose.yml -f docker-compose.unity.yml down -v`.

### 3. Gate `release` on the new job

- [ ] 3.1 Add `e2e-unity` to the `release` job's `needs` at `ci.yml:793`, giving
  `needs: [e2e, e2e-lakekeeper, e2e-unity, lint, unit-tests, licenses, install-script, install-script-e2e]`.
  Extend the rationale comment at `ci.yml:791-792` so it explains the contrast rather than only
  `e2e-azure`'s absence: `e2e-unity` gates releases because its stack is entirely local containers,
  on a pinned image; `e2e-azure` does not, because a live third-party account there would let an
  outage or a rotated secret block every release.

### 4. List the new caller on the shared setup action

- [ ] 4.1 Update `.github/actions/e2e-setup/action.yml:3` from
  `Common preparation shared by the e2e, e2e-lakekeeper and e2e-azure jobs.` to name `e2e-unity`
  as a fourth caller. Change nothing else in the action — `e2e-unity` uses it unmodified.

### 5. Record the repository-settings follow-up

- [ ] 5.1 This plan cannot make `E2E (Unity)` a required status check: `main`'s ruleset
  (`repos/exasol-labs/lakehouse-engine-rs/rulesets/18435862`) lives in repository settings, not in
  the tree. Open a GitHub issue asking for `E2E (Unity)` to be added to that ruleset's required
  checks alongside `E2E (Lakekeeper)`, stating the consequence of not doing it: a red `E2E (Unity)`
  blocks the release job on a `main` push but does not block a pull-request merge. Reference the
  issue number from the `e2e-unity` job's leading comment so the partial-parity state is visible in
  `ci.yml` itself.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 2.1, 3.1 — all `.github/workflows/ci.yml`, strictly sequential in that order to avoid overlapping edits and stale line anchors |
| Group B | 4.1 — `.github/actions/e2e-setup/action.yml`, disjoint from Group A |
| Group C | 5.1 — GitHub issue only, touches no file |

No cross-group dependency. Group A's internal order is a hard requirement, not a preference: each
task's line anchors shift the ones after it.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | This plan is purely additive to `ci.yml` plus two comment corrections. No job, step, target, or test becomes obsolete |

The stale enumeration at `ci.yml:278` is corrected in place by task 1.2, not removed — the comment
still governs the coverage step's behavior, which this plan leaves unchanged.

## Verification

This plan changes CI wiring and no Rust behavior, so there is no failing-test-first cycle to run.
The proof is the workflow itself executing: a green `E2E (Unity)` on the pull request, and a red one
when the stack is broken.

### Scenario Coverage

No scenarios are added or changed (see § Features), so no scenario-to-test mapping applies. The
Unity suite's existing scenarios keep the tests they already have in
`crates/lakehouse-engine/tests/e2e_unity_test.rs`; this plan changes only where those tests run.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Lint gap closed | `cargo clippy --workspace --all-targets --features unity-e2e -- -D warnings` | Exit 0, zero warnings — verified green before planning |
| Lint gap closed (proof the file body is checked) | `cargo check -p lakehouse-engine --all-targets --features unity-e2e --message-format=json 2>&1 \| grep -o '"features":\["unity-e2e"\]' \| head -1` | Matches — the `e2e_unity_test` target builds with the feature on, so its `#![cfg]`-gated body and its `tests/common/` imports are type-checked |
| Coverage step untouched | `git diff -- .github/workflows/ci.yml \| grep llvm-cov` | No output — the coverage command is unchanged |
| Workflow is valid YAML | `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` | Exit 0, no exception |
| `e2e-unity` job present and gating | `grep -n 'e2e-unity\|E2E (Unity)' .github/workflows/ci.yml` | Hits in the job key, its `name:`, and the `release` job's `needs:` |
| Setup action lists the new caller | `grep -n e2e-unity .github/actions/e2e-setup/action.yml` | One hit, on the description line |
| Bring-up sequence works locally | Run the job's three bring-up commands by hand from a clean state (`make unity-down` first), then `docker compose -f docker-compose.yml -f docker-compose.unity.yml ps` | `minio-init` exits 0; `minio`, `exasol`, `unitycatalog` all healthy; `seed.sh` exits 0 |
| Suite passes over that stack | `cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1` | 0 failures. Note: locally the `docker wait` name is `lakehouse-engine-rs-2-minio-init-1` in this checkout — the job's hardcoded `lakehouse-engine-rs-minio-init-1` is correct for CI, where the checkout directory is the repository name |
| Fail-not-skip contract holds | `make unity-down`, then rerun the suite command above | Fails naming the unreachable stack; never reports 0 tests run or a skip |
| Job actually runs in CI | Push the branch, open the pull request, then `gh run view --log --job "E2E (Unity)"` | The job schedules, brings the stack up, seeds, and the suite passes |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets --features unity-e2e -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| E2E (Unity, local) | `make test-e2e-unity` | 0 failures — confirms the Makefile path still works unchanged |
| E2E (Lakekeeper, regression) | `make test-e2e-lakekeeper` | 0 failures — confirms the shared `minio-init` bring-up was not disturbed |
| CI (authoritative) | Pull-request checks | `Check & Lint` green with the new flag; `E2E (Unity)` green; `E2E`, `E2E (Lakekeeper)`, `Unit Tests` unchanged |
