# Plan: fix-e2e-harness-undeclared-limit

> Tracked in issue [#312](https://github.com/exasol-labs/lakehouse-engine-rs/issues/312).

## Summary

Stop the E2E WebSocket harness from attaching an invented `resultSetMaxRows: 10000` to every
statement, so an E2E assertion describes the request it actually runs. Measure which pushdown
shapes the injected cap changes, flip the default to Exasol's own `0` (no limit), fix the latent
single-`fetch` truncation the cap was hiding, and repair every E2E assertion the flip unmasks.

## Context

`ExaConn::connect_inner` sets `result_set_max_rows: 10000`
(`crates/lakehouse-engine/tests/common/exasol_ws.rs:92`). Both `execute` (`:101`) and
`try_execute` (`:130`) attach it as `{"attributes": {"resultSetMaxRows": …}}` on every statement.
Exasol converts that cap into a `pushdownRequest` `limit` the test never wrote, so assertions about
pushdown shape, plan shape, and result content are made against a different request than the one
the assertion describes.

`0` is Exasol's own documented default for `resultSetMaxRows`. The `10000` is a harness invention.
No test asserts on it.

Three consequences make this a defect rather than a quirk:

- **A test that means "no limit is pushed" runs a limit-carrying request.** The only record of this
  is a code comment at `crates/lakehouse-engine/tests/e2e_join_test.rs:113-117`, written because
  the cap disqualified broadcast-join pushdown and changed the plan under test. The workaround was
  to opt five individual tests out.
- **The pushdown debugging tool is capped too.** `e2e_capture_pushdown.rs:52` opens a plain
  `exa_conn()`. Its stated purpose is showing what a statement pushes down. Every payload captured
  with it carried an operator-invisible limit.
- **The cap masks a truncating result reader.** `fetch_result_columns`
  (`crates/lakehouse-engine/tests/common/exasol_ws.rs`) issues exactly one `fetch` at
  `startPosition: 0` with `numBytes: 67108864`, ignores how many rows that response returned,
  then closes the result set. Its correctness rests on an upstream row bound, not on the reader.
  Whether present-day fixtures actually exceed one response at that 64 MiB budget is measured in
  task 1.6, not assumed here — 30,000 rows of ~100 bytes is roughly 3 MB. Either way, removing the
  cap removes the bound the reader silently relies on, so the reader is completed before the flip.

### Measured census

Verified with Serena and repository search, not from the issue's approximations:

| Item | Count | Where |
|---|---|---|
| `exa_conn()` call sites | 186 | 11 E2E binaries |
| Direct `ExaConn::connect*` sites that execute statements | 7 | `cloud_e2e_test.rs` (5), `e2e_azure_test.rs` (1), `e2e_lakekeeper_test.rs` (1) |
| Direct `ExaConn::connect` sites asserting connect-failure only | 2 | `e2e_scan_test.rs:692`, `e2e_positional_deletes_test.rs:780` |
| Sites declaring the existing opt-out | 6 | `e2e_join_test.rs` (118, 139, 193, 1174, 1362), `e2e_lakekeeper_test.rs:884` |

Binaries constructing an `ExaConn`, by how they are run:

| Runner | Binaries |
|---|---|
| `make test-e2e` | `e2e_scan_test`, `e2e_capability_test`, `e2e_count_distinct_test`, `e2e_join_test`, `e2e_positional_deletes_test`, `e2e_int96_timestamp_test`, `e2e_refresh_test`, `e2e_non_ascii_identifier_test` |
| `make test-e2e-lakekeeper` | `e2e_lakekeeper_test` |
| `make test-e2e-azure` (needs real Azure credentials) | `e2e_azure_test` |
| `cloud-e2e` feature, SaaS only, not in CI | `cloud_e2e_test` |
| Manual diagnostic, not in any suite | `e2e_capture_pushdown` |

`e2e_capability_test` (69 sites) and `e2e_scan_test` (54 sites) carry the largest blast radius.

The one seeded table exceeding 10000 rows is `high_card_probe` (`HIGH_CARD_ROWS = 30_000`,
`crates/lakehouse-engine/tests/common/seed.rs:2267`). No test raw-scans it today; it is reached
only through `COUNT(DISTINCT)`, which returns one row. That makes it the right fixture for proving
the multi-fetch fix, and it is a strong prior that Exasol does not push the cap beneath an
aggregate — `high_cardinality_count_distinct_completes` asserts `30000` on a capped connection and
passes.

## Design

### Context

The harness owns one decision the rest of the test suite must not have to think about: what row
cap, if any, a statement declares. Today it makes that decision invisibly and wrongly, and the
decision leaks — as a pushdown `limit` into the system under test, and as scattered explanatory
comments in test files that each re-derive the same fact.

- **Goals** — the harness declares no cap unless a call site asks; a cap that is asked for is
  visible at the asking call site; one documented home explains that a cap becomes a pushdown
  `limit`; the result reader is complete without a cap holding it up.
- **Non-Goals** — changing whether a pushed `limit` suppresses broadcast-join pushdown (that is
  #307); adding a cap where no present-day caller needs one; making any production-code fix in
  response to a Phase-4 test failure. Every test the flip turns red is resolved the same way, with no
  exception: file a GitHub issue recording what the flip exposed, then declare an explicit
  `capped_result_sets(n)` at that one test's call site. `tasks.md` § Phase 4 states the rule.

### Decision

Default to the protocol's own default; make a cap an explicit call-site declaration.

```
BEFORE                                    AFTER
ExaConn { result_set_max_rows: u32 }      ExaConn { result_set_max_rows: u32 }
  connect_inner -> 10000  (invented)        connect_inner -> 0  (Exasol's default)
  execute -> always sends the cap           execute -> sends 0 unless declared
  .unbounded_result_sets() -> 0             .capped_result_sets(n) -> n
     (6 call sites opt OUT of a cap)           (a call site opts IN to a cap)
```

The field and the attribute stay. What changes is which value the harness chooses on the caller's
behalf: the protocol's documented "no limit", not a number the harness made up.

`capped_result_sets` carries the doc comment that owns the leaked decision: a declared cap reaches
the adapter as a pushdown `limit`, so a capped session exercises a different plan, and the method
is for tests whose assertion is about that capped plan.

`fetch_result_columns` loops until it has collected the `numRows` the result-set metadata reports,
advancing `startPosition` by the rows each response returned, and panics if a response returns
zero rows while rows remain outstanding.

`e2e_capture_pushdown` reads an optional `CAPTURE_RESULT_SET_MAX_ROWS`; unset means uncapped. The
binary is already "driven entirely by env vars", so `scripts/capture-pushdown-payload.sh` needs no
change — it inherits the variable. This makes the capped-versus-uncapped comparison reproducible
after this plan lands, not a one-off measurement.

#### Patterns

| Pattern | Where | Why |
|---|---|---|
| Default to the protocol default | `ExaConn::connect_inner` | The harness stops making a decision it has no basis to make |
| Opt in, not opt out | `capped_result_sets(n)` | A cap is visible where it is used, absent where it is not |
| One documented owner for a leaked fact | `capped_result_sets` doc comment | Replaces per-test comments that each re-derive "a cap becomes a pushdown limit" |
| Read to completion | `fetch_result_columns` | Correctness no longer depends on an upstream cap bounding the result |

### Consequences

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| Default `result_set_max_rows` to `0` | `Option<u32>`, omitting the `attributes` object entirely when no cap is declared | `0` is Exasol's documented default and is already proven against this server by the six existing opt-out call sites. Omitting the attribute is unproven here and adds a second variable to a change whose whole purpose is a clean unmasked-failure signal. Revisit only if the measurement shows `0` and omitted differ. |
| Keep an explicit cap knob | Delete the knob entirely (YAGNI) | Two present-day callers need it: the regression test that pins the measured injection surface, and `e2e_capture_pushdown`'s reproducible comparison. Not speculative. |
| Fix `fetch_result_columns` before flipping the default | Flip first, fix truncation as a follow-up | The reader's correctness rests on an upstream row bound, not on the reader. Removing that bound while the reader is still single-fetch would make this plan's unmasked-failure signal depend on a client that can short-read silently. Task 1.6 measures whether truncation is reachable with present fixtures; the ordering holds either way, as hardening that precedes the flip. |
| Record the measured injection surface in `docs/debugging-pushdown.md` | Leave it in the plan's evidence file only | `/speq:record` archives the plan directory. The operator-facing capture-tool doc is the permanent home for what the capture tool shows. |
| No Iceberg-spec compliance check | Run the check per CLAUDE.md | The change touches a test-only WebSocket client and its call sites. No scanning, pushdown, or schema/type production code changes. See decision-log entry 6. |

## Features

| Feature | Status | Spec |
|---|---|---|
| `e2e-harness/e2e-harness` | CHANGED | `e2e-harness/e2e-harness/spec.md` |
| `e2e-harness/lakekeeper-e2e-harness` | CHANGED | `e2e-harness/lakekeeper-e2e-harness/spec.md` |

## Impact

No product impact. The `.so`, the adapter, and the scan path are unchanged; nothing ships to an
operator differently.

Contributor-facing impact is real. `ExaConn::unbounded_result_sets` disappears; a test wanting a
capped session calls `capped_result_sets(n)` instead. Any E2E assertion that passed only because a
`limit` was pushed starts failing. This plan closes every one of those failures before it lands, each
by the same route: a filed issue recording what the flip exposed, plus an explicit
`capped_result_sets(n)` at that test's own call site — so no red test survives and no exposed
behavior goes untracked. Payloads captured with `scripts/capture-pushdown-payload.sh` before this
change carried a limit the operator did not write, so a conclusion drawn from one of them may not
hold.

## Dependencies

- Local Docker stack: Exasol, MinIO, Iceberg REST. `make test-e2e` does **not** start it. Bring it
  up first, or every DB-backed test fails and mimics a real regression.
- Lakekeeper overlay stack for `e2e_lakekeeper_test`.
- Real Azure Blob Storage credentials for `e2e_azure_test`. Absent locally, this binary is verified
  by CI only.
- `cloud_e2e_test` runs against SaaS staging under the `cloud-e2e` feature and is in no CI job. It
  is changed by inspection and compile-checked, not executed.

## Relationship to #307

[#307](https://github.com/exasol-labs/lakehouse-engine-rs/issues/307) — a pushed `limit` or
`orderBy` suppressing broadcast-join pushdown — is the bug that made this harness behavior visible.
The two defects are independent and either order works. This plan does not change
`join_requires_exasol_postprocessing` or any other adapter behavior.

## Migration

| Current | New |
|---|---|
| `exa_conn()` — silently capped at 10000 | `exa_conn()` — no cap declared |
| `exa_conn().unbounded_result_sets()` | `exa_conn()` |
| no way to declare a cap | `exa_conn().capped_result_sets(n)` |
| `e2e_capture_pushdown` always capped | uncapped unless `CAPTURE_RESULT_SET_MAX_ROWS` is set |

## Implementation Tasks

Full breakdown with `[expert]` routing in `tasks.md`. Five phases:

1. **Measure the injection surface** against the live Docker stack — capped versus uncapped
   `pushdownRequest` for seven statement shapes, plus the rows-per-`fetch`-response figure phase 2
   rests on. The declarable-cap knob lands first, as task 1.0: `result_set_max_rows` is private and
   the existing opt-out expresses only `0`, so the measurement tasks cannot declare a small
   distinguishable cap until `capped_result_sets` exists. Task 1.0 is purely additive — the old
   default and the old opt-out stay untouched until phase 3. Gates everything downstream. Nothing
   about the result is pre-decided in this plan.
2. **Make the result reader complete** — loop `fetch_result_columns`, behind a
   `numBytes`-parameterized entry point so a test can force chunking. Lands before the flip.
3. **Flip the default and drop the old knob** — `0` default, delete `unbounded_result_sets` and its
   6 call sites, delete the stale `e2e_join_test.rs` comment, update `docs/debugging-pushdown.md`,
   land the Lakekeeper spec delta.
4. **Fix every unmasked failure** — one task per E2E binary, open-ended by construction. The
   number of failing assertions is unknown until phase 3 lands and cannot be sized here. Every
   newly-red test takes one uniform route, with no test classification and no exceptions: a filed
   GitHub issue referencing #312, plus an explicit `capped_result_sets(n)` at that test's own call
   site. No production-code fix happens in this phase.
5. **Pin the new behavior** — regression tests for the three new scenarios.

## Parallelization

| Parallel Group | Tasks |
|---|---|
| Group A | 1.x measurement |
| Group B | 2.x result-reader completeness |
| Group C | 3.x flip, knob, dead code, docs |
| Group D | 4.1 … 4.10 per-binary remediation (one task per binary, mutually independent) |
| Group E | 5.x regression tests |

Sequential dependencies:

- Task 1.0 → Group A (the measurement tasks need a declarable cap; `result_set_max_rows` is private
  and the existing opt-out expresses only `0`).
- Task 1.6 → Task 2.1 (task 2.1's multi-response expectation cites 1.6's measured rows-per-response
  figure as the basis for its `numBytes` choice).
- Group A and Group B are otherwise independent and MAY run concurrently.
- Group A → Group C (the measurement records what the flip is expected to change).
- Group B → Group C (a complete reader must precede an unbounded result set).
- Group C → Group D (failures cannot be observed before the flip).
- Group C → Group E (the regression tests assert post-flip behavior).
- Group D tasks are mutually independent across binaries and MAY run concurrently, but each one
  needs the shared Docker stack, so run them serially against one stack unless separate stacks are
  available.

## Dead Code Removal

| Type | Location | Reason |
|---|---|---|
| Method | `ExaConn::unbounded_result_sets`, `crates/lakehouse-engine/tests/common/exasol_ws.rs:142` | Becomes a no-op once the default is `0` |
| Call | `.unbounded_result_sets()` at `e2e_join_test.rs` 118, 139, 193, 1174, 1362 and `e2e_lakekeeper_test.rs:884` | Method removed; the default already declares no cap |
| Comment | `crates/lakehouse-engine/tests/e2e_join_test.rs:113-117` | Its premise — "the default 10000-row cap reaches the adapter as a pushdown `limit`" — no longer holds for a default connection |

Any per-test comment discovered during phase 4 that explains the injected cap is dead for the same
reason and MUST be removed rather than reworded.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|---|---|---|---|
| Harness statements carry no row cap the test did not declare | Integration | `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` | `undeclared_cap_pushes_no_limit` |
| A declared row cap truncates the returned row count (RENAMED a second time — see decision-log.md's second correction entry: a live capture of the REAL adapter request, not `EXPLAIN VIRTUAL`, showed a declared cap DOES reach the adapter as a pushdown `limit`; this scenario now asserts only the delivered row count, which is what the test can actually observe) | Integration | `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` | `declared_cap_truncates_returned_row_count` |
| Harness returns every row of a result set larger than one fetch response | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `harness_reads_high_cardinality_result_set_to_completion` |
| A two-table broadcast join over a vended-credential warehouse returns correct rows (CHANGED — the connection no longer opts out) | Integration | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_broadcast_join_result_correct` |

The new join regression test `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`
(`crates/lakehouse-engine/tests/e2e_join_test.rs`) is not added as its own Scenario Coverage row: it
proves the join-disqualification mechanism itself (any pushed `limit`, via a SQL `LIMIT` that
`EXPLAIN VIRTUAL` can show directly) rather than backing a new normative scenario in either spec
delta above — it is closer to the second-correction regression evidence recorded in
`decision-log.md` and `verification-report.md` than to a Phase 5 scenario.

The fetch-completeness test lives in `e2e_count_distinct_test` because that binary already seeds
`high_card_probe` (30,000 rows of ~100-byte tokens); seeding it a second time in a new binary would
add stack time for no additional coverage. Whether that fixture exceeds one `fetch` response at the
harness's present 64 MiB `numBytes` budget is measured in task 1.6, not assumed here — the test forces
chunking with a small `numBytes` instead of depending on the answer.

### Manual Testing

| Feature | Command | Expected Output |
|---|---|---|
| `e2e-harness/e2e-harness` | `scripts/capture-pushdown-payload.sh 'SELECT c_varchar FROM {table}'` | The `EXPLAIN VIRTUAL` scan-spec JSON carries no `limit` key |
| `e2e-harness/e2e-harness` | `CAPTURE_RESULT_SET_MAX_ROWS=5 scripts/capture-pushdown-payload.sh 'SELECT c_varchar FROM {table}'` | The same scan-spec JSON carries `limit` `5`, and no other field differs from the uncapped capture |

### Checklist

| Step | Command | Expected |
|---|---|---|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Unit test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Lint, E2E gates | `cargo clippy --all-targets` once per feature: `--features exasol-e2e`, `--features lakekeeper-e2e`, `--features azure-e2e`, `--features cloud-e2e` | 0 warnings each. Each E2E binary is gated by its own crate-root `#![cfg(feature = "…")]`, so the wrong flag compiles an empty binary and exits 0 |
| Format | `cargo fmt` | No changes |
| Stack up | `docker compose up -d --wait exasol minio iceberg-rest` | All services healthy |
| E2E | `make test-e2e` | 0 failures, exit 0 |
| Lakekeeper E2E | `make test-e2e-lakekeeper` (overlay stack up) | 0 failures, exit 0 |

Check the exit code, not the tail of the output. `make test-e2e | tail` masks a non-zero exit.
Before debugging a hung run, check for a stray `bench/.env`.
