# Decision Log: add-unity-e2e-ci-job

## Interview

**Q:** Should the new `e2e-unity` job gate the `release` job, like `e2e-lakekeeper` does?
**A:** Yes — add `e2e-unity` to `release.needs`. `e2e-azure` is deliberately excluded (comment at
`ci.yml:791-792`) because it depends on a live third-party Azure account whose outage or rotated
secret could block every release. Unity's stack is fully local Docker, like Lakekeeper's, so that
exclusion rationale does not apply. Mirror Lakekeeper exactly.

**Q:** What `timeout-minutes` budget should the `e2e-unity` job use? Lakekeeper uses 45; Unity pulls
a larger ~5.7 GB `unitycatalog/unitycatalog` image and seeds Delta fixtures onto MinIO.
**A:** 45 minutes — the same value as `e2e-lakekeeper`. No extra headroom requested.

**Q:** Should `cargo llvm-cov --workspace` (`ci.yml:285`, the coverage step in `unit-tests`) also
get `--features unity-e2e`, alongside the clippy fix? Unlike clippy, which only type-checks,
`cargo llvm-cov` wraps `cargo test` and executes every compiled test binary, so the feature would
newly run `e2e_unity_test.rs` inside a job with no Unity stack. Confirmed by grep that the step
passes no `--features` today, so `lakekeeper-e2e` has the identical untouched gap already.
**A:** Skip the coverage step entirely. Fix only the lint step (`ci.yml:216`) by adding
`--features unity-e2e` there. This matches the existing precedent — the coverage step enables no
e2e feature for any suite — and adds no execution risk. Fixing Lakekeeper's identical coverage gap
is explicitly out of scope.

**Q:** How should the CI job bring up the Unity stack, given that Unity's one-shot seed is not a
compose service? Lakekeeper's `minio-init` / `lakekeeper-migrate` are compose one-shots the CI job
`docker wait`s and exit-code-checks; Unity's seed is a plain script (`scripts/unity/seed.sh`), and
`make unity-up` does not exit-code-check its own `minio-init` call.
**A:** An explicit multi-step CI job mirroring Lakekeeper's rigor, not a bare `make unity-up` /
`make test-e2e-unity`. Concretely: (1) `pull --quiet` over both compose files; (2) `up -d --wait`
the health-gated long-running services; (3) `up -d minio-init` plus an explicit `docker wait` and
non-zero-exit check with a log dump and `exit 1`, for parity with how Lakekeeper's job treats its
one-shots; (4) run `./scripts/unity/seed.sh` as its own step so it gets its own attribution in the
Actions UI; (5) run the suite directly with the cargo line from `test-e2e-unity`, rather than
through the Makefile target, because `make test-e2e-unity` calls `$(MAKE) unity-up` and would re-run
the bring-up. The exact container name to `docker wait` is a planner implementation detail, not an
open question.

## Design Decisions

### [1] Record the CI-job decision as an ADR with no spec delta

- **Decision:** Author `verify-unity-catalog-continuously-dedicated-ci-job` in this decision log and
  write no `specs/_plans/add-unity-e2e-ci-job/<domain>/<feature>/spec.md` file. This plan carries
  zero spec deltas.
- **Alternatives:** Add a delta to `e2e-harness/unity-catalog-e2e-harness` restating the CI wiring.
  Rejected: the delta mechanism merges by scenario name, and there is no scenario to append,
  replace, or remove. That feature's normative text covers suite *behavior* — fixture bring-up,
  `createVirtualSchema` listing and column-mapping assertions, the fail-not-skip contract, the
  no-credential-leak contract — none of which changes. Adding a scenario asserting "CI runs this
  suite" would create a normative clause no test can verify from inside the suite.
- **Rationale:** Three in-repo precedents establish the ADR-only shape for a CI decision:
  `verify-lakekeeper-continuously-dedicated-ci-job` (the direct analogue, and the one issue #328
  names), `azure-orphan-sweep-in-repo-workflow` (a workflow-only plan), and
  `azure-e2e-ci-no-fork-coverage-goal`, whose plan `azure-e2e-ci-scope-simplification` recorded and
  archived with no spec-delta directory at all. Verified: `specs/_recorded/009-azure-e2e-ci-scope-simplification/`
  contains only `plan.md`, `decision-log.md`, `tasks.md`, `review-findings.md`, and
  `verification-report.md`. The spec library has no CI-job domain, and inventing one for a single
  job would spread the CI configuration's ownership across two artifacts. This entry is the judgment
  call about *where* to record, not the decision being recorded; entry [2] is the ADR.
- **Promotes to ADR:** no

### [2] Verify Unity Catalog continuously in a dedicated CI job

- **Decision:** Add an always-on `e2e-unity` job mirroring `e2e-lakekeeper` — `needs: [build-so]`,
  `runs-on: ubuntu-latest`, `timeout-minutes: 45`, `LH_EXASOL_CPUS: "2"`, the shared
  `./.github/actions/e2e-setup` composite, a phased bring-up with an exit-code-checked `minio-init`,
  `scripts/unity/seed.sh` as its own step, failure log dumping, artifact upload, and
  `down -v` cleanup — and gate `release` on it alongside `e2e` and `e2e-lakekeeper`.
- **Alternatives:** Opt-in only, never in CI, as `cloud-e2e` is. Rejected: the whole point of #328
  is continuous verification, and unlike `cloud-e2e` the Unity stack needs no credentials. Fold the
  suite into the existing `e2e` job. Rejected: couples the baseline job's runtime and stability to a
  ~5.7 GB image pull, the same reasoning that gave Lakekeeper its own job. Add the job but leave
  `release` ungated, as `e2e-azure` is. Rejected: `e2e-azure`'s exclusion turns on a live
  third-party account, and Unity's stack is entirely local containers on a pinned image
  (`unitycatalog/unitycatalog:v0.5.0`), so no external party can block a release.
- **Rationale:** MinIO, Exasol, and Unity Catalog OSS all run as local Docker containers, so
  continuous verification costs one runner and no external dependency. Without it, a regression in
  UC-native `createVirtualSchema` listing or column mapping reaches `main` unnoticed. Gating
  `release` matches Lakekeeper and is what makes the verification load-bearing rather than advisory.
  Record this ADR under the ID `verify-unity-catalog-continuously-dedicated-ci-job`, mirroring
  `verify-lakekeeper-continuously-dedicated-ci-job` in
  `specs/_decision/021-add-lakekeeper-e2e.md`.
- **Promotes to ADR:** yes

### [3] Bring the stack up with explicit CI steps rather than `make unity-up`

- **Decision:** The job runs `up -d minio-init` with a `docker wait` exit-code check, then
  `up -d --wait minio exasol unitycatalog`, then `./scripts/unity/seed.sh` as a separate step.
- **Alternatives:** `run: make unity-up`. Rejected: `Makefile:238` starts `minio-init`
  fire-and-forget, so a failed `warehouse` bucket creation would surface later as a confusing seed
  error instead of an attributed failure. Also fix `unity-up` to check that exit code. Rejected as
  scope creep — the interview settled on the CI job owning the rigor, and the Makefile target keeps
  working unchanged for local runs.
- **Rationale:** `minio-init` runs before the long-running services because `seed.sh` requires the
  bucket; `minio-init`'s own `depends_on: minio: service_healthy` pulls MinIO up and gates on its
  healthcheck, so nothing is lost by ordering it first. This is also the exact idiom
  `ci.yml:592-600` already documents: `up --wait` does not reliably hold a one-shot container in the
  same wait set as long-running services. Scope: local step sequencing within one job.
- **Promotes to ADR:** no

### [4] Inline the `cargo test` line instead of adding a bring-up-free Makefile target

- **Decision:** The test step runs
  `cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1`
  inline, with a comment naming the CI step as the authority and requiring flag-identity with
  `Makefile:250`.
- **Alternatives:** Split `test-e2e-unity` into a bring-up half and a cargo-only half that CI calls.
  Rejected, though it is the cleaner design on the merits: it would give the flags a single owner and
  keep every e2e job's test step a `make` call. It also adds a public Makefile target the issue never
  asked for, and the repo already has an idiom for precisely this duplication — `ci.yml:277-278`
  names the CI step as the authority and requires flag-identity with the `Makefile` `coverage`
  target. Calling `make test-e2e-unity` unmodified was also considered and rejected: it invokes
  `$(MAKE) unity-up`, re-running the bring-up and re-seeding the fixtures.
- **Rationale:** Reusing an established in-repo convention costs one comment instead of a new
  target. The honest cost, recorded here so it is not mistaken for an oversight: `e2e-unity` becomes
  the only e2e job whose test step is not `make test-e2e-*` (`ci.yml:531`, `632`, `738` are the
  others). Note also that the issue's own Scope section says "Run the suite with
  `make test-e2e-unity`"; the interview superseded that, because `test-e2e-unity` bundles a bring-up
  the CI job performs itself. Scope: a local wiring choice, reversible in one edit.
- **Promotes to ADR:** no

### [5] Fix the lint gap for `unity-e2e` only, not for all five e2e features

- **Decision:** Add `--features unity-e2e` to `ci.yml:216` and stop there. `exasol-e2e`,
  `cloud-e2e`, `lakekeeper-e2e`, and `azure-e2e` stay unchecked by clippy.
- **Alternatives:** Use `--features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e,unity-e2e`,
  covering all 15 e2e test files. Verified safe — that command exits 0 with zero warnings today — and
  it costs no extra CI minutes beyond the compile. Rejected for this plan as the same out-of-scope
  widening the interview already declined for the coverage step: issue #328 is about Unity, and the
  four other features carry an identical pre-existing gap that predates it.
- **Rationale:** Consistency with the user's stated scope boundary. The finding is recorded in
  `plan.md` § Dependencies as a mechanical follow-up so it is not lost. Verified empirically before
  planning: bare `--features unity-e2e` is accepted alongside `--workspace` on cargo 1.94.1 with
  resolver 2 even though only one member declares the feature, so no `package/feature` syntax is
  needed; and the depfile for the feature-enabled `e2e_unity_test` target reads all four
  `tests/common/` modules while the feature-off depfile reads none, proving the `#![cfg]`-gated body
  is genuinely type-checked rather than compiled away. Scope: a trim with a named follow-up.
- **Promotes to ADR:** no

### [6] Reuse the existing hardcoded `minio-init` container name

- **Decision:** `docker wait lakehouse-engine-rs-minio-init-1`, the same literal `e2e`
  (`ci.yml:507`) and `e2e-lakekeeper` (`ci.yml:606`) already use.
- **Alternatives:** Derive the name with `docker compose ... ps -q minio-init`, which is
  directory-independent and strictly more robust. Rejected: it would leave two conventions for one
  operation in a single file, and the fix belongs repo-wide rather than in the one new job.
- **Rationale:** No compose file pins a project `name:` and none sets `container_name:`, so Docker
  derives the project from the compose-file directory. The literal works in CI only because
  `actions/checkout` clones into a directory named after the repository. Verified: containers in
  this checkout are named `lakehouse-engine-rs-2-minio-init-1`, so the CI literal does not match a
  local run from a differently-named directory. The coupling is pre-existing and affects three jobs
  equally; `plan.md` § Dependencies records pinning a compose `name:` as the repo-wide follow-up.
  The compose network is already pinned (`name: lakehouse-engine`, `docker-compose.yml:175`), so
  `seed.sh`'s `LH_NETWORK` default is stable regardless of directory. This mirrors existing code; the
  underlying fragility is tracked separately.
- **Promotes to ADR:** no

### [7] Treat the required-status-check addition as an out-of-tree follow-up

- **Decision:** Task 5.1 opens an issue asking for `E2E (Unity)` to be added to `main`'s ruleset
  required checks, and the `e2e-unity` job's comment references it. No file in this plan can make
  the change.
- **Alternatives:** Say nothing and let `release.needs` stand as the whole story. Rejected: it would
  leave a silent asymmetry — a red `E2E (Unity)` blocks the release job on a `main` push but not a
  pull-request merge — and readers would reasonably assume full Lakekeeper parity.
- **Rationale:** Verified against the live ruleset
  (`repos/exasol-labs/lakehouse-engine-rs/rulesets/18435862`): the required checks are
  `Check & Lint`, `Unit Tests`, `License Check`, `E2E`, `E2E (Lakekeeper)`, `Build Host Workspace`,
  and `Sonar Analysis`. `E2E (Lakekeeper)` is required and `E2E (Azure)` is not, so Unity parity
  with Lakekeeper needs the ruleset entry as well as the `release.needs` entry. Scope: an operational
  follow-up, not a design decision.
- **Promotes to ADR:** no

### [8] Insert the job between `e2e-lakekeeper` and `e2e-azure`

- **Decision:** Place `e2e-unity` immediately after the `e2e-lakekeeper` job ends.
- **Alternatives:** Append after `e2e-azure`, the least-disruptive diff. Rejected for a weaker
  reason than the others: file order carries no semantics in GitHub Actions.
- **Rationale:** Grouping keeps the three release-gating e2e jobs contiguous and leaves the
  deliberately non-gating `e2e-azure` last, next to the `release` comment that explains its absence.
  Scope: cosmetic ordering.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated in Revision Mode after plan-reviewer blockers, and by speq-implement after code review. -->
