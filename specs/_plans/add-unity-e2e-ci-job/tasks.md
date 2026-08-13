# Tasks: add-unity-e2e-ci-job

## Phase 2: Implementation (Group A) — .github/workflows/ci.yml, strictly sequential
- [x] 2.1 (plan 1.1) Add `--features unity-e2e` to the clippy step at `ci.yml:216`; add a short
  comment above it explaining why this one feature of five is enabled.
- [x] 2.2 (plan 1.2) Fix the stale enumeration comment at `ci.yml:278` to include `unity-e2e`.
- [x] 2.3 (plan 2.1) Insert a complete `e2e-unity` job after `e2e-lakekeeper` (currently ending
  `ci.yml:669`), modelled on `ci.yml:563-669`, per plan.md's full task-2.1 spec (leading comment,
  `name: E2E (Unity)`, bring-up steps, seed step, test step, failure diagnostics, artifact upload,
  stop step).
- [x] 2.4 (plan 3.1) Add `e2e-unity` to the `release` job's `needs` at `ci.yml:793` and extend the
  rationale comment at `ci.yml:791-792`.

## Phase 2: Implementation (Group B) — .github/actions/e2e-setup/action.yml, disjoint from A
- [x] 2.5 (plan 4.1) Update the description at `.github/actions/e2e-setup/action.yml:3` to name
  `e2e-unity` as a fourth caller.

## Phase 2: Implementation (Group C) — GitHub issue, runs after Group A (needs the job comment 2.3 writes)
- [x] 2.6 (plan 5.1) Open a GitHub issue asking for `E2E (Unity)` to be added to `main`'s ruleset
  required checks (`repos/exasol-labs/lakehouse-engine-rs/rulesets/18435862`) alongside
  `E2E (Lakekeeper)`; state the consequence of not doing it. Reference the issue number from the
  `e2e-unity` job's leading comment (written in 2.3) so the partial-parity state is visible in
  `ci.yml` itself.

## Phase 4: Review Fixes
- [x] 4.1 In `.github/workflows/ci.yml` line 770, change `# ── third viability gate: REAL
  Azure ADLS Gen2 storage, local catalog ────` to `# ── fourth viability gate: REAL Azure
  ADLS Gen2 storage, local catalog ───`, keeping the trailing box-drawing rule the same
  total width as the neighbouring banners.
- [x] 4.2 In `.github/workflows/ci.yml` line 38, change
  `# e2e / e2e-lakekeeper / e2e-azure cascade-skip via `needs: [build-so]`.` to
  `# e2e / e2e-lakekeeper / e2e-unity / e2e-azure cascade-skip via `needs: [build-so]`.`
- [x] 4.3 In `.github/workflows/ci.yml`, change line 220 from
  `# unchecked here — a tracked follow-up, not fixed by this change.` to
  `# unchecked here — tracked in #337.` (issue already exists; do not create a new one).
- [x] 4.4 In `.github/workflows/ci.yml`, delete the parenthetical enumeration on line 219 so
  lines 218-219 read `# Docker stack, no dependencies (`unity-e2e = []`). The other four
  e2e` / `# features remain`, leaving the coverage-step comment at lines 281-283 as the
  file's single enumeration of the e2e features. Reflow the block to the surrounding
  ~76-column comment width; do not touch the `- name: Clippy` step or its `run:` line.
- [x] 4.5 In `.github/workflows/ci.yml`, replace lines 683-684 with the single line
  `  # Not yet in `main`'s ruleset required checks — tracked in #336.` Keep the blank
  comment separator line 682 and the `e2e-unity:` job key on the following line unchanged.
- [x] 4.6 In `Makefile`, add the reciprocal notice immediately above `test-e2e-unity:` (line 248),
  inside the existing comment block, stating that the `cargo test` line MUST stay flag-identical to
  the `Run Unity Catalog E2E suite` step in `ci.yml`'s `e2e-unity` job, which is the authority —
  mirroring the wording of the `coverage` target's comment at `Makefile:192-194`. Comment-only: add
  no target, change no recipe line, leave `unity-up` and the `test-e2e-unity` recipe bodies
  byte-identical. [expert]

## Phase 3: Verification
- [x] 3.1 Run automated checks from plan.md § Verification > Checklist (build, test, lint, format).
- [x] 3.2 Scenario coverage audit (plan.md states none apply — no scenarios added/changed).
- [x] 3.3 Manual testing per plan.md § Verification > Manual Testing (YAML validity, grep checks,
  local bring-up, suite pass, fail-not-skip contract). CI-only rows (job actually running in a real
  PR) are out of scope for this phase and are covered by the outer implement-pr pipeline once pushed.
