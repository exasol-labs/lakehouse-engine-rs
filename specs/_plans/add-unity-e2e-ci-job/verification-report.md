# Verification Report: add-unity-e2e-ci-job

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 6 checklist commands and 5 manual spot-checks green (11/11). New `e2e-unity` CI job mirrors `e2e-lakekeeper`, gates `release`, and the local Unity + Lakekeeper E2E suites both pass over the actual bring-up sequence the new job runs. |
| Code review | 6 findings — standard: 5, expert: 1 — 6 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ (N/A — plan carries no scenario, see below) |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

No production Rust code changed (this plan edits `.github/workflows/ci.yml`,
`.github/actions/e2e-setup/action.yml`, and `Makefile` comments only), so no coverage delta applies.

| Type | Coverage % |
|------|------------|
| Unit | unchanged |
| Integration | unchanged |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`) | all | all | 0 failures |
| E2E — Unity (`make test-e2e-unity`) | 10 | 10 | 0 |
| E2E — Lakekeeper (`make test-e2e-lakekeeper`, regression) | 23 | 23 | 0 |
| E2E — Unity, fail-not-skip contract (`make unity-down` then rerun) | 10 | 8 | 2 named failures — expected: stack unreachable, not a skip |

### Manual Tests

| Test | Result |
|------|--------|
| `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` — workflow is valid YAML | ✓ |
| `git diff -- ci.yml \| grep llvm-cov` — coverage step byte-unchanged (only unchanged diff context matched; no `+`/`-` line touches `llvm-cov`) | ✓ |
| `grep -n 'e2e-unity\|E2E (Unity)' ci.yml` — hits in job key, `name:`, and `release`'s `needs:` | ✓ |
| `grep -n e2e-unity .github/actions/e2e-setup/action.yml` — one hit, description line | ✓ |
| Fail-not-skip contract (`make unity-down`, rerun suite) — fails naming the unreachable stack, never reports 0 tests or a skip | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --features unity-e2e -- -D warnings
Finished dev profile — 0 warnings, 0 errors
```

### Formatter

```
cargo fmt --all --check
(empty diff — no changes needed)
```

## Scenario Coverage

None. Per plan.md § Features: "This plan adds no scenario and changes no existing one, so it
carries no spec delta." The suite's own behavior spec (`e2e-harness/unity-catalog-e2e-harness`)
is unchanged; this plan changes only *where* that suite runs (CI wiring), which no scenario states.

## Notes

- **Code review (6 findings, all fixed):**
  - Standard (5, all in `ci.yml`): renumbered the `e2e-azure` "third viability gate" banner to
    "fourth" (Unity now owns "third"); extended the `build-so` cascade-skip enumeration comment to
    include `e2e-unity`; replaced an untracked "a tracked follow-up" claim with a reference to a
    newly opened issue, #337 ("Type-check the remaining e2e test suites in the lint job"); removed a
    redundant e2e-feature enumeration that duplicated the coverage-step comment; merged an orphaned
    two-line issue-reference comment (`#336`) into one inline sentence.
  - Expert (1, in `Makefile`): added the reciprocal half of the flag-identity contract between
    `Makefile`'s `test-e2e-unity` target and `ci.yml`'s `Run Unity Catalog E2E suite` step — comment
    only, no recipe change, verified byte-identical cargo invocations across both files.
- **Two follow-up GitHub issues opened during this run** (both out of this plan's scope per
  plan.md § Dependencies, tracked so they aren't lost):
  - #336 — add `E2E (Unity)` to `main`'s ruleset required checks (plan task 5.1).
  - #337 — type-check the remaining four e2e test suites (`exasol-e2e`, `cloud-e2e`,
    `lakekeeper-e2e`, `azure-e2e`) in the lint job (surfaced by code review, already flagged as a
    follow-up in plan.md § Dependencies).
- **Verification agent's own operator error, not a plan defect:** the first two
  `make test-e2e-lakekeeper` attempts failed 8/23 because the manual bring-up omitted the
  `minio-lakekeeper-init` one-shot service (a documented prerequisite, `Makefile:83-85`, unrelated to
  this plan's changes). After including it, the suite passed 23/23 — confirming this plan's shared
  `minio-init` bring-up was not disturbed.
- **CI-only row deferred:** plan.md's Manual Testing table also lists "Job actually runs in CI" (push
  the branch, open the PR, `gh run view --log --job "E2E (Unity)"`). That check requires a live PR
  and is covered once this branch is pushed and its checks run — out of scope for local verification.
