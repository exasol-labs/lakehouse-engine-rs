# Plan: fix-order-by-constant-select-item

## Summary

The adapter removes every constant sort key from a pushdown request once, when it parses the request, so no path renders a constant as a bare number that Exasol reads as a select-list position (#620). An `ORDER BY` of constants only runs as an unordered `LIMIT`, or orders by one `FALSE` key when it carries an `OFFSET`.

## Context

- Exasol replaces an `ORDER BY` position with the select item it names before it calls the adapter, so `ORDER BY 2` over `SELECT id, 1` arrives as a `literal_exactnumeric` sort key (#620). Exasol also folds a constant expression such as `1+0` or `CAST(1 AS DECIMAL(3,0))` into a literal sort key.
- Exasol reads a bare integer in a rendered `ORDER BY` as a select-list position. Parentheses do not change that.
- Planning reproduced the issue on the Docker Exasol with a build of current main. The issue's three row-scan queries already return the native rows, because the row-scan wrapper maps a key equal to a select item to that item's emitted column (`sort_by_select_items`, #617). Three shapes still render the bare number and return wrong rows, or fail with `Too large value 5` (SQL state 04000) when the constant exceeds the select-list item count: a folded constant (`ORDER BY 1+0 DESC, id LIMIT 3`), the grouped merge (`GROUP BY id ORDER BY 2 DESC, 1 LIMIT 3`), and the N-scan join wrapper (`events JOIN labels … ORDER BY 2 DESC, 1 LIMIT 3`).
- Every reader of `orderBy` takes the request from `PushdownRequest::parse`, which is the only constructor of the request type and the only reader of the raw `pushdownRequest`. The readers are the row-scan top-N check and declined wrapper (`topn.rs`), the per-shard limit withholding (`order_by_present`), the grouped merge (`grouped_agg.rs`), the broadcast join window (`joins/planning.rs`), and the N-scan join wrapper (`joins/sql_builders.rs`).
- Exasol rejects `LIMIT … OFFSET` without `ORDER BY` and accepts `ORDER BY FALSE`. Exasol pushes a non-zero offset together with an all-constant `orderBy` (`SELECT id, 1 … ORDER BY 2 LIMIT 3 OFFSET 2`). Both were verified on the Docker Exasol.
- Exasol keeps `ORDER BY RANDOM()` itself and pushes neither the ordering nor the `LIMIT`.
- The broadcast join window (`classify_join_window`, `joins/planning.rs`) declines every non-`column` sort key, so on main a join ordered by a constant runs on the N-scan wrapper. Once the parse step drops the constants, a join ordered by constants and bare projected columns takes the ordered broadcast window, and a join ordered by constants only with a `LIMIT` and no `OFFSET` takes the bare-`LIMIT` broadcast shape. The `FALSE` key of decision [1] still declines to the N-scan wrapper.
- Iceberg and Delta compliance: checked, not engaged. The Iceberg table spec and the Delta Lake protocol define how a reader finds a table's data files and rows, and neither defines the row order of a query result. The adapter reads no Iceberg sort order. This plan changes only which sort keys the adapter renders, so file pruning, delete application, and type mapping do not change.
- Architecture: no change. The fix stays inside the adapter's request parsing.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| Pushdown Planning, Ordered Sort-Key Capability | CHANGED | `pushdown-capabilities/pushdown-planning-order-by-capability/spec.md` |
| Pushdown Planning, Ordered Top-N | CHANGED | `pushdown-capabilities/pushdown-planning-topn/spec.md` |
| Pushdown Planning, Broadcast Inner Equi-Join | CHANGED | `pushdown-joins/pushdown-planning-join/spec.md` |
| Pushdown Planning, Unified Unaccelerated Join Fallback | CHANGED | `pushdown-joins/pushdown-planning-join-fallback/spec.md` |

## Impact

- A query that orders by a constant select item or a constant expression returns the native rows in the native order on the row-scan, grouped, and join paths. A constant above the select-list item count no longer fails the query.
- An `ORDER BY` that mixes constants with bare projected columns and carries a `LIMIT` runs as a per-shard bounded top-N instead of a full declined scan.
- An `ORDER BY` of constants only with a `LIMIT` and no `OFFSET` runs as a bare `LIMIT`, so each shard stops early.
- A broadcast-eligible join ordered by constants and bare projected columns, or by constants only with a `LIMIT` and no `OFFSET`, runs on the broadcast path instead of the N-scan wrapper.
- No breaking change. No property, capability, CONNECTION field, or scan-spec field changes.

## Implementation Tasks

### 1. Reproduce and pin the defect with E2E tests

- [ ] 1.1 Add `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` and declare it in `main.rs`. Use `lakehouse_vs()`, `events_table()`, `vs_table(LAKEHOUSE_VS, E2E_TABLE_2)` for `labels`, `explain_virtual_sql`, and the harness route checks `has_broadcast_join_block` and `has_two_scan_wrapper`. The seeded ids are the row oracle (`specs/testing.md` § Correctness oracles). Write five tests, each one case table:
  - `constant_sort_key_returns_the_native_order`, tagged `A constant sort key does not become a select-list position` and `An ORDER BY the adapter cannot bound as a top-N remains correctness-safe`. Rows: the issue's three row-scan queries (the no-`LIMIT` one with `WHERE id <= 5`), the folded `1+0` key, and the grouped query with `1` and with `5`, each expecting ids 1, 2, 3 (or 1 to 5) in that order. One more row runs `SELECT id FROM t ORDER BY RANDOM() LIMIT 3` and expects three distinct seeded ids, as the guard for the #620 acceptance criterion.
  - `constant_sort_key_planning_follows_the_other_keys`, tagged `A constant sort key does not decide the bounded top-N path`, `Per-shard row limit is emitted only alongside the matching per-shard sort`, `Unsupported ordered-query shapes decline the ordered-top-N path`, `A constant sort key does not become a select-list position`, and `An ORDER BY the adapter cannot bound as a top-N remains correctness-safe`. Row 1, `SELECT id, 1 FROM t ORDER BY 2 DESC, 1 LIMIT 3`: the scan spec carries `"order_by":` and `"limit":3`. Row 2, `SELECT id, 1 FROM t ORDER BY 2 DESC, ABS(score) LIMIT 3`: the scan spec carries neither field, and the result has exactly two columns. Both rows return ids 1, 2, 3.
  - `constant_sort_key_join_routes_by_the_other_keys`, tagged `Constant sort keys leave a broadcast-eligible join on the broadcast path`, `Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback`, and `A constant sort key does not become a select-list position`. On `events e JOIN labels l ON e.ID = l.ID`: `SELECT e.ID, 1 … ORDER BY 2 DESC, 1 LIMIT 3` and its `5` variant take the broadcast route (`has_broadcast_join_block` true, `has_two_scan_wrapper` false) and return ids 1, 2, 3 in that order. `SELECT e.ID, 1 … ORDER BY 2 LIMIT 3` takes the broadcast route and returns 3 rows of the join. `SELECT e.ID, 1 … ORDER BY 2 DESC, ABS(e.SCORE) LIMIT 3` takes the two-scan route (`has_two_scan_wrapper` true, `has_broadcast_join_block` false) and returns ids 1, 2, 3.
  - `constant_only_order_by_runs_as_an_unordered_limit`, tagged `An ORDER BY of constants only runs as an unordered LIMIT`. `SELECT id, 1 FROM t ORDER BY 2 LIMIT 3`: the scan spec carries `"limit":3` and no `"order_by":`, the pushed SQL holds no `ORDER BY`, and the result holds three distinct seeded ids, each with the constant 1.
  - `constant_only_order_by_keeps_its_offset_window`, tagged `An ORDER BY of constants only keeps its OFFSET window` and `Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback`. Rows for the row scan, the grouped query (`GROUP BY id`), and the join, each with `ORDER BY 2 LIMIT 3 OFFSET 18`, each expecting 2 rows of distinct seeded ids. The join row also asserts the two-scan route.

  In `crates/lakehouse-e2e/tests/e2e_local/join.rs`, beside its `VS_NAME_LOW` virtual schema (`JOIN_BROADCAST_MAX_BYTES = '1'`), add `e2e_join_fallback_constant_sort_key_returns_the_native_order`, tagged `A constant sort key does not become a select-list position`, so the N-scan wrapper stays proven fixed. It joins `vs_table(VS_NAME_LOW, E2E_TABLE)` and `vs_table(VS_NAME_LOW, E2E_TABLE_2)` with `SELECT e.ID, 1 … ORDER BY 2 DESC, 1 LIMIT 3` and its `5` variant, asserts `has_two_scan_wrapper`, and expects ids 1, 2, 3 in that order.

  Remove the `/// Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe` line from `declined_order_by_renders_every_ordering_in_full` (`crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`), because the two E2E tests above now prove that scenario (`specs/testing.md` § Coverage rule). The unit test stays, untagged.
- [ ] 1.2 Reproduce on main: start the stack with `make e2e-up`, build main with `make cross-udf-build`, and run the new tests with `cargo test --workspace --features lakehouse-e2e/exasol-e2e --test e2e_local -- order_by_constant join_fallback_constant_sort_key --test-threads=1`. The harness uploads `target/release/liblakehouse_engine.so`. Against that deployment, run `EXPLAIN VIRTUAL` and the query through `exapump` for `MY_LAKEHOUSE.EVENTS` (ids 1 to 20) and `MY_LAKEHOUSE.EVENTS e JOIN MY_LAKEHOUSE.LABELS l ON e.ID = l.ID`: the issue's three queries, `SELECT id FROM t ORDER BY 1+0 DESC, id LIMIT 3`, `SELECT id, 1, COUNT(*) FROM t GROUP BY id ORDER BY 2 DESC, 1 LIMIT 3` and its `5` variant, the join `SELECT e.id, 1 … ORDER BY 2 DESC, 1 LIMIT 3` and its `5` variant, and `SELECT id, 1 FROM t ORDER BY 2 LIMIT 3 OFFSET 18`. Record each pushed `ORDER BY` clause, each result, and the failing test rows in `tasks.md`. The planning repro predicts that the issue's row-scan queries pass, that the folded, grouped, and join rows return wrong rows or fail with SQL state 04000, that both planning tests fail, and that the join route test fails its broadcast-route rows, because a constant key sends the join to the N-scan wrapper on main. The offset test passes on main, because main still renders a constant key there, and it guards task 2.2's `FALSE` key.

### 2. Remove constant sort keys when the request is parsed

- [ ] 2.1 Write the failing unit test first in `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs`: one case table over `PushdownRequest::parse(..).json()["orderBy"]`, built with `pushdown_request`, `order_by_element`, `col`, and the `vs-expression` literal builders (`num_lit`, `lit`). Rows: a constant before a column key leaves only the column key; each literal type Exasol sends (`literal_exactnumeric`, `literal_double`, `literal_string`, `literal_null`, `literal_bool`, `literal_date`, `literal_timestamp`) is removed; a `literal_interval` key built with `lit`, which the select-list literal list omits, is removed (decision [2]); a `function_scalar` `RAND` key stays; constants only without an offset remove the `orderBy` key; constants only with a non-zero offset leave one `literal_bool` `false` element with the first element's direction and NULL placement; a request without `orderBy` is unchanged. The test carries no `/// Scenario:` line, because the E2E tests prove the scenarios.
- [ ] 2.2 Implement the rule in `PushdownRequest::parse` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) through one private helper. It removes every `orderBy` element whose `expression` node type starts with `literal_` (decision [2]). When that empties the list, it removes the `orderBy` key, unless `extract_offset` is non-zero, in which case it keeps one `FALSE` element (decision [1]). Extend the `PushdownRequest` doc comment with the invariant: an `orderBy` holds no constant key that Exasol pushed, and an `orderBy` of constants only with a non-zero offset holds one `FALSE` key. Update the `json()` doc comment to state that `parse` drops constant sort keys. Keep each comment to 1 or 2 lines. No `orderBy` reader changes. `sort_by_select_items` and both "fact 5" `debug_assert!`s stay.
- [ ] 2.3 Run the whole local suite with `make e2e-up test-e2e`, which rebuilds the `.so` before the harness deploys it. It includes the new module and the existing `ORDER BY` tests in `capability.rs`, `scan.rs`, and `join.rs`. Record the result in `tasks.md`.

### 3. Documentation

- [ ] 3.1 In `docs/capabilities.md`, § the `ORDER BY` paragraphs: the bounded top-N condition "every sort key must be a bare projected column" exempts a constant sort key, and the sentence "the adapter renders every sort key Exasol pushes in Exasol SQL" states the exception. The adapter drops a constant sort key, because Exasol reads a bare number in `ORDER BY` as a select-list position and a constant does not change the order. An `ORDER BY` of constants only with no `OFFSET` runs as a bare `LIMIT`. In § Joins, the broadcast condition "sort keys that are projected columns" becomes "sort keys that are projected columns or constants".

### 4. Verification

- [ ] 4.1 Run `make fmt-check lint` and `cargo test --workspace --lib`. Measure changed-line coverage with `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` and keep it at 90% or above. Report the changed-line and workspace coverage in the PR description.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Constant sort keys | 1.1-1.2, 2.1-2.3, 3.1, 4.1 | none | spec deltas `pushdown-capabilities/pushdown-planning-order-by-capability`, `pushdown-capabilities/pushdown-planning-topn`, `pushdown-joins/pushdown-planning-join`, `pushdown-joins/pushdown-planning-join-fallback`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (`PushdownRequest`, `extract_offset`, `order_by_present`), `support_tests.rs`, `pushdown_tests.rs`, `topn.rs` and `joins/planning.rs` (read only); `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs`, `join.rs`, `main.rs`, `fixtures.rs`; `docs/capabilities.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | none | `sort_by_select_items` still serves a non-constant expression key equal to a select item, such as `JSON_VALUE` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A constant sort key does not become a select-list position | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs`, `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `constant_sort_key_returns_the_native_order`, `constant_sort_key_planning_follows_the_other_keys`, `constant_sort_key_join_routes_by_the_other_keys`, `e2e_join_fallback_constant_sort_key_returns_the_native_order` |
| An ORDER BY of constants only keeps its OFFSET window | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_only_order_by_keeps_its_offset_window` |
| An ORDER BY the adapter cannot bound as a top-N remains correctness-safe | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_sort_key_returns_the_native_order`, `constant_sort_key_planning_follows_the_other_keys` |
| Constant sort keys leave a broadcast-eligible join on the broadcast path | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_sort_key_join_routes_by_the_other_keys` |
| Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_sort_key_join_routes_by_the_other_keys`, `constant_only_order_by_keeps_its_offset_window` |
| A constant sort key does not decide the bounded top-N path | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_sort_key_planning_follows_the_other_keys` |
| An ORDER BY of constants only runs as an unordered LIMIT | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_only_order_by_runs_as_an_unordered_limit` |
| Per-shard row limit is emitted only alongside the matching per-shard sort | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs` | `constant_sort_key_planning_follows_the_other_keys` |
| Unsupported ordered-query shapes decline the ordered-top-N path | E2E | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs`, `crates/lakehouse-e2e/tests/e2e_local/scan.rs` (existing) | `constant_sort_key_planning_follows_the_other_keys`, `order_by_without_limit_falls_back_correctly` |

The constant-key rule itself has one unit test in `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` (task 2.1). The unit test `declined_order_by_renders_every_ordering_in_full` stays, untagged. The join-fallback delta changes only its Background, which no scenario test maps.

### Manual Testing

`DSN='exasol://sys:exasol@localhost:28563?validateservercertificate=0'`, after `make e2e-up test-e2e` deployed the fixed build.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Pushdown Planning, Ordered Sort-Key Capability | `exapump sql -d "$DSN" "SELECT ID, 5 FROM MY_LAKEHOUSE.EVENTS ORDER BY 2 DESC, 1 LIMIT 3"` | Rows `1,5`, `2,5`, `3,5`; no `Too large value` error |
| Pushdown Planning, Ordered Sort-Key Capability | `exapump sql -d "$DSN" "SELECT ID, 1, COUNT(*) FROM MY_LAKEHOUSE.EVENTS GROUP BY ID ORDER BY 2 DESC, 1 LIMIT 3"` | Rows with ids 1, 2, 3 in that order, each count 1 |
| Pushdown Planning, Ordered Sort-Key Capability | `exapump sql -d "$DSN" "SELECT e.ID, 1 FROM MY_LAKEHOUSE.EVENTS e JOIN MY_LAKEHOUSE.LABELS l ON e.ID = l.ID ORDER BY 2 DESC, 1 LIMIT 3"` | Rows with ids 1, 2, 3 in that order |
| Pushdown Planning, Ordered Sort-Key Capability | `exapump sql -d "$DSN" "SELECT ID, 1 FROM MY_LAKEHOUSE.EVENTS ORDER BY 2 LIMIT 3 OFFSET 18"` | Exactly 2 rows |
| Pushdown Planning, Ordered Top-N | `exapump sql -d "$DSN" "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.EVENTS ORDER BY 1+0 DESC, ID LIMIT 3"` | The scan spec carries `"order_by":` on `ID` and `"limit":3`; the pushed SQL holds no `ORDER BY 1` |
| Pushdown Planning, Ordered Top-N | `exapump sql -d "$DSN" "EXPLAIN VIRTUAL SELECT ID, 1 FROM MY_LAKEHOUSE.EVENTS ORDER BY 2 LIMIT 3"` | The scan spec carries `"limit":3` and no `"order_by":`; the pushed SQL holds no `ORDER BY` |
| Pushdown Planning, Broadcast Inner Equi-Join | `exapump sql -d "$DSN" "EXPLAIN VIRTUAL SELECT e.ID, 1 FROM MY_LAKEHOUSE.EVENTS e JOIN MY_LAKEHOUSE.LABELS l ON e.ID = l.ID ORDER BY 2 DESC, 1 LIMIT 3"` | One `LAKEHOUSE_SCAN` fan-out whose scan spec carries a join block; no `LHS_T0` or `LHS_T1` leg |
| Pushdown Planning, Unified Unaccelerated Join Fallback | `exapump sql -d "$DSN" "EXPLAIN VIRTUAL SELECT e.ID, 1 FROM MY_LAKEHOUSE.EVENTS e JOIN MY_LAKEHOUSE.LABELS l ON e.ID = l.ID ORDER BY 2 LIMIT 3 OFFSET 18"` | Two scan legs `LHS_T0` and `LHS_T1` joined in Exasol SQL; the outer wrapper orders by `FALSE` and carries `LIMIT 3 OFFSET 18` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test (unit) | `cargo test --workspace --lib` | 0 failures |
| Test (E2E) | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |
