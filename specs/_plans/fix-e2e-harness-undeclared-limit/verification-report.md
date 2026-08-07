# Verification Report: fix-e2e-harness-undeclared-limit

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 38 plan tasks + 7 code-review fixes landed; every checklist command is green; a mid-implementation live measurement disproved the plan's original core premise (a declared row cap reaches the adapter as a pushdown `limit`) and every affected artifact (doc comments, one regression test, one spec-delta scenario) was corrected to state the measured truth, recorded in `decision-log.md`. |
| Code review | 7 findings — 7 fixed (6 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| Lint (`cargo clippy --all-targets`, + 4 per-feature gates) | ✓ |
| Format (`cargo fmt`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, host) | 1081 | 1081 | 2 |
| E2E — `make test-e2e` (9 binaries incl. new `e2e_harness_row_cap_test`) | 240 | 240 | 0 |
| E2E — `make test-e2e-lakekeeper` | 23 | 23 | 0 |
| E2E — `azure-e2e` / `cloud-e2e` gates | compile-checked only (clippy + check, exit 0 each) | — | — |

`azure-e2e` (`e2e_azure_test`) and `cloud-e2e` (`cloud_e2e_test`) were not executed — no Azure Blob
Storage or SaaS staging credentials are available in this environment. Both were reviewed by
inspection against the Phase 1 measurement (task 4.10/4.11): no assertion in either binary depends
on the disproven cap→pushdown-limit mechanism, and neither references a fixture exceeding the old
10,000-row cap. `azure_e2e_test` is CI-only proof; `cloud_e2e_test` needs a manual SaaS run.

### Manual Tests

| Test | Result |
|------|--------|
| `scripts/capture-pushdown-payload.sh 'SELECT c_varchar FROM {table}'` — uncapped capture carries no `limit` | ✓ (task 1.2 measurement, shape 1) |
| `CAPTURE_RESULT_SET_MAX_ROWS=5 scripts/capture-pushdown-payload.sh '...'` — capped capture, scan-spec comparison | ✓ (task 1.2 control c1: cap is delivered/honored — 5 of 12 rows returned; scan spec unchanged) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets                         → exit 0
cargo clippy --all-targets --features exasol-e2e    → exit 0
cargo clippy --all-targets --features lakekeeper-e2e → exit 0
cargo clippy --all-targets --features azure-e2e      → exit 0
cargo clippy --all-targets --features cloud-e2e      → exit 0
```

### Formatter

```
cargo fmt --check → exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| e2e-harness | e2e-harness | Harness statements carry no row cap the test did not declare | `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` | `undeclared_cap_pushes_no_limit` | Pass |
| e2e-harness | e2e-harness | A declared row cap truncates the delivered result set, not the pushdown request (renamed — see Notes) | `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` | `declared_cap_truncates_delivered_result_set_not_pushdown_request` | Pass |
| e2e-harness | e2e-harness | Harness returns every row of a result set larger than one fetch response | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `harness_reads_high_cardinality_result_set_to_completion` | Pass |
| e2e-harness | lakekeeper-e2e-harness | A two-table broadcast join over a vended-credential warehouse returns correct rows (CHANGED — connection no longer opts out) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_broadcast_join_result_correct` | Pass |

## Notes

**Core premise correction (the most important finding of this implementation run).** The plan's
Context/Design sections asserted that Exasol converts a declared `resultSetMaxRows` cap into a
`pushdownRequest` `limit`. Live measurement (task 1.2: 20 captures across all 7 statement shapes,
plus 6 correctness controls) found this false on Exasol 2025.2.1 — a cap only truncates the
*delivered* result set; the adapter never sees it. This was verified, not assumed, per this
project's own verification-discipline rule. Consequences, all applied:

- `plan.md`'s Scenario Coverage row and `tasks.md` task 5.3 originally specified
  `declared_cap_reaches_adapter_as_pushdown_limit`, asserting the capped scan spec carries `limit
  n`. That would have asserted a falsehood. The test was rewritten as
  `declared_cap_truncates_delivered_result_set_not_pushdown_request`, proven RED against the literal
  original assertion (scan spec genuinely carries no `limit` under a cap) before being written GREEN
  against the measured behavior.
- Two doc comments (`ExaConn::capped_result_sets`, `ExaConn::unbounded_result_sets`) and two spec
  deltas (`e2e-harness/e2e-harness`, `e2e-harness/lakekeeper-e2e-harness`) were corrected to state the
  measured mechanism instead of the original claim.
- `docs/debugging-pushdown.md` records the permanent, operator-facing shape matrix, including an
  explicit hedge on the broadcast-join shape (Exasol's `EXPLAIN VIRTUAL` echo cannot fully exclude a
  limit reaching only a directly-executed join statement; result-value controls bound the other six
  shapes but not this one via the echo alone).
- `decision-log.md` records the correction's full rationale and evidence trail; `injection-surface.md`
  preserves the original claim and the measurement that disproved it, so the paper trail survives
  `/speq:record`'s archive.
- Phase 4's predicted blast radius (originally: any pushdown-shape assertion) collapsed to the
  truncation axis only. All 11 per-binary checks (4.1–4.11) came back with **zero newly-failing
  tests** — no GitHub issues were filed and no `capped_result_sets(n)` opt-ins were needed anywhere,
  since no fixture in the whole suite exceeds the old 10,000-row cap except `high_card_probe`, which
  is only reached through a cap-invariant `COUNT(DISTINCT)` (existing test) and the new
  chunked-fetch regression test (task 2.1).

**A second, independent defect surfaced during Phase 2 (task 2.2).** The pre-existing
`fetch_result_columns` read `fetch` responses at the wrong JSON path (`data` instead of nested under
`responseData`), so any handle-backed (non-inlined) result set silently returned **zero rows**, not
merely a truncated prefix. This was latent and never triggered by any pre-existing test (every
current fixture is small enough to return inline). Fixed as part of the read-to-completion loop.

**Code review found one related gap the fix didn't fully close (expert finding, now fixed).** The
initial completeness fix left the *inline-data* branch of `fetch_result_columns_with_num_bytes`
without the same completeness guarantee as the handle-loop branch. Not a live bug on Exasol
2025.2.1 (this server never pairs inline `data` with a handle), but the doc comment and the
`e2e-harness` spec delta both claimed unconditional completeness. Restructured so both branches
assert against the advertised `numRows`; re-verified against all 12 existing call sites plus both
local E2E suites, exit 0.

**Environmental gotchas discovered and worked around (no plan-scope code changed):**
- `e2e_positional_deletes_test`, `e2e_int96_timestamp_test` need the one-shot
  `spark-iceberg-fixtures` Compose job beyond the generic `exasol minio iceberg-rest` bring-up.
- The Lakekeeper overlay's `exasol` container needs to be *recreated* under the merged compose files
  (`docker-compose.yml` + `docker-compose.lakekeeper.yml`) to pick up the `/etc/hosts` patch for
  `keycloak`/`lakekeeper` DNS — starting only the base stack first and then adding the overlay
  services does not retrofit an already-running `exasol` container.

**Out of scope, confirmed unaffected:** #307 (a pushed `limit`/`orderBy` suppressing broadcast-join
pushdown) — this plan does not change `join_requires_exasol_postprocessing`; the broadcast-join test
pair was confirmed green under the flip (task 4.3), consistent with the disproven-premise finding.

Ready for `/speq:record fix-e2e-harness-undeclared-limit`.
