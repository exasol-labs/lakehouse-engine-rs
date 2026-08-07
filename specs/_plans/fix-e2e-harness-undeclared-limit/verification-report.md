# Verification Report: fix-e2e-harness-undeclared-limit

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 38 plan tasks + 7 code-review fixes + a second correction round (Phase 7, 5 tasks) landed; every checklist command is green. A mid-implementation `EXPLAIN VIRTUAL`-based measurement appeared to disprove the plan's original premise (a declared row cap reaches the adapter as a pushdown `limit`); a later direct capture of the REAL adapter request found that premise was correct all along — `EXPLAIN VIRTUAL` simply cannot observe it. Every affected artifact was corrected a second time to state the now-confirmed mechanism, recorded in `decision-log.md`'s second correction entry. |
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
| E2E — `make test-e2e` (9 binaries incl. new `e2e_harness_row_cap_test`) | 241 | 241 | 0 |
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
| e2e-harness | e2e-harness | A declared row cap truncates the returned row count (renamed twice — see Notes) | `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` | `declared_cap_truncates_returned_row_count` | Pass |
| e2e-harness | e2e-harness | Harness returns every row of a result set larger than one fetch response | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `harness_reads_high_cardinality_result_set_to_completion` | Pass |
| e2e-harness | lakekeeper-e2e-harness | A two-table broadcast join over a vended-credential warehouse returns correct rows (CHANGED — connection no longer opts out) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_broadcast_join_result_correct` | Pass |

## Notes

**Core premise correction, corrected a second time (the most important finding across this
implementation run and its follow-up correction round).** The plan's Context/Design sections
asserted that Exasol converts a declared `resultSetMaxRows` cap into a `pushdownRequest` `limit`.
A mid-implementation measurement (task 1.2: 20 `EXPLAIN VIRTUAL` captures across all 7 statement
shapes, plus 6 correctness controls) appeared to find this false on Exasol 2025.2.1 — every
`EXPLAIN VIRTUAL` capture came back byte-identical between a capped and uncapped connection. That
conclusion was itself wrong, and the error was in the tool, not the mechanism: `EXPLAIN VIRTUAL`
and a real query execution are different exchanges with the adapter, and `resultSetMaxRows` is an
attribute of whichever statement is actually sent — an `EXPLAIN VIRTUAL` wrapper is never the
statement a declared cap targets, so its echo was structurally incapable of showing this, for any
shape, regardless of the cap's value. A domain expert's challenge to the first correction prompted
a second measurement: directly capturing the adapter's raw incoming request during a REAL query
execution (bypassing `EXPLAIN VIRTUAL` entirely) for all 7 shapes. That capture found a declared
cap DOES reach the adapter as a pushdown `limit` on every shape — applied safely for raw scans
(per-shard limit plus an outer `LIMIT`), correctly withheld from beneath aggregates (outer `LIMIT`
only), and, for the broadcast-eligible join, disqualifying broadcast pushdown via
`join_requires_exasol_postprocessing` (existing, unchanged production code — this plan does not
touch it). This was verified against a live capture both times, per this project's own
verification-discipline rule; the first verification's tool had a blind spot the second one closed.
Consequences of the second correction, all applied:

- `plan.md`'s Scenario Coverage row and `tasks.md` task 5.3 originally specified
  `declared_cap_reaches_adapter_as_pushdown_limit`. The first (wrong) correction renamed it to
  `declared_cap_truncates_delivered_result_set_not_pushdown_request`, asserting the two `EXPLAIN
  VIRTUAL` plans were identical. The second correction renamed it again to
  `declared_cap_truncates_returned_row_count` and dropped the `EXPLAIN VIRTUAL`-equality assertion
  entirely — the test now asserts only the delivered row count (`n` for a capped connection, the
  full fixture count for an uncapped one), which is what it can actually observe; no assertion in
  this file claims anything about the pushdown request.
- A new regression test, `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`
  (`crates/lakehouse-engine/tests/e2e_join_test.rs`), proves the join-disqualification mechanism
  directly via a SQL `LIMIT` — the fully `EXPLAIN VIRTUAL`-observable form of the same
  `join_requires_exasol_postprocessing` check a declared `resultSetMaxRows` cap triggers less
  visibly. The comment above `e2e_broadcast_join_pushdown_shape`/`e2e_broadcast_join_result_correct`
  explaining why those two tests must stay uncapped was restored and corrected to state the
  confirmed mechanism, replacing the version Phase 3 had deleted as "stale."
- Two doc comments (`ExaConn::capped_result_sets` in `exasol_ws.rs`, and the `e2e_join_test.rs`
  comment above) and two spec deltas (`e2e-harness/e2e-harness`, `e2e-harness/lakekeeper-e2e-harness`)
  were corrected a second time to state the now-confirmed mechanism.
- `docs/debugging-pushdown.md` was rewritten: the shape matrix now states that a declared cap DOES
  reach a real request as a `limit`, for every shape, and that `EXPLAIN VIRTUAL` can never show
  this because it is a separate exchange — explicitly warning an operator that
  `scripts/capture-pushdown-payload.sh` (which uses `EXPLAIN VIRTUAL`) will show a broadcast join
  plan regardless of a declared cap.
- `decision-log.md` records both corrections' full rationale and evidence trail as separate,
  explicitly-superseding entries — neither the original claim nor the first (wrong) correction was
  deleted. `injection-surface.md` likewise adds the real-execution-path capture as a new section
  that marks the earlier `EXPLAIN VIRTUAL`-based matrix superseded without removing it, so the full
  paper trail survives `/speq:record`'s archive.
- Phase 4's predicted blast radius is unaffected by the second correction: it was scoped to what the
  *default flip* (10000 → 0) changes about existing tests' own connections, none of which declare a
  cap, so none of them are affected by what a *declared* cap does. All 11 per-binary checks
  (4.1–4.11) still came back with **zero newly-failing tests** — no GitHub issues were filed and no
  `capped_result_sets(n)` opt-ins were needed anywhere, since no fixture in the whole suite exceeds
  the old 10,000-row cap except `high_card_probe`, which is only reached through a cap-invariant
  `COUNT(DISTINCT)` (existing test) and the new chunked-fetch regression test (task 2.1).

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
pushdown) — this plan does not change `join_requires_exasol_postprocessing`; it is existing,
unchanged production code, per decision `[9]`. The broadcast-join test pair was confirmed green
under the default flip (task 4.3) because neither test declares a cap either way — the flip
(10000 → 0) changes nothing about a connection that already sends no cap. This is independent of,
and unaffected by, the second correction's finding that a *declared* cap does reach the adapter as
a pushdown `limit` and would disqualify broadcast pushdown if one were declared — which is exactly
why these two tests, and the lakekeeper suite's broadcast join test, must keep declaring none.

Ready for `/speq:record fix-e2e-harness-undeclared-limit`.
