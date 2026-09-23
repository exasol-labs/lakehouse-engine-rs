# Plan: add-broadcast-join-topn

## Summary

A broadcast-eligible inner equi-join with `ORDER BY … LIMIT n` and a zero offset gets a per-shard post-join top-N: each fact shard sorts and truncates its OWN joined output to `n` rows inside DataFusion, and the unchanged outer wrapper merges at most `G × n` rows. This closes issue #309 and gives the ordered case the per-shard join-work savings that a bare `LIMIT` already gets (issue #307).

## Design

### Context

Today an ordered broadcast join leaves every shard unbounded. Each shard computes and emits its complete local join, and one Exasol-side wrapper (`wrap_declined_order_by`) applies `ORDER BY <keys> [LIMIT n [OFFSET m]]` over the merged rows. The flat single-table path already bounds each shard with a local top-N (`vs-adapter/pushdown-planning-topn`), and the broadcast bare-`LIMIT` path already caps each shard's joined output (`JoinSpec::post_join_limit`).

One correctness constraint shapes the design, and issue #309 does not state it. The join scan emits some columns in a representation that differs from the native value it reads: a nested or out-of-range column as JSON text, and an empty string as NULL in Exasol. A shard that ranks the native value can cut a row that the wrapper's global top-N needs (decision [2]).

- **Goals**
  - A zero-offset `ORDER BY … LIMIT n` over a broadcast join bounds each shard to its own post-join top-`n`, computed by a DataFusion TopK above the join and its `WHERE`.
  - The returned rows equal the same query evaluated on a single node, for every key type the broadcast path admits.
  - Every other window shape keeps its current SQL.
- **Non-Goals**
  - A per-shard bound for a non-zero offset (top-(n + m)): out of scope per the interview.
  - Any change to the outer wrapper, or a direct-attach merge `ORDER BY … LIMIT` on the fan-out: out of scope per the interview.
  - The same ranking divergence in the flat-scan top-N path (decision [6]).

### Decision

#### Architecture

```
pushdown request: orderBy = bare projected columns, limit n, offset 0
        │
        ▼
classify_join_window ──► JoinWindowPlan::Ordered { keys, limit: Some(n), offset: 0 }   (unchanged)
        │
        ▼
build_broadcast_join_sql (VS, sql_builders.rs)
   ├── bound_sort_key(key, projection)      admits each key and yields its SortKey
   ├── JoinSpec.post_join_order_by = keys   NEW
   ├── JoinSpec.post_join_limit    = n      (now also set for this shape)
   ├── fan-out outer scalar select: NO LIMIT
   └── wrap_declined_order_by: SELECT <visible> FROM (<fan-out>) ORDER BY <keys> LIMIT n   (unchanged)
        │
        ▼  one invocation per fact shard (LAKEHOUSE_SCAN UDF)
build_join_sql (join_scan.rs)
   SELECT <items> FROM (<dim>) INNER JOIN (<fact>) ON <cond> [WHERE <filter>]
   ORDER BY <emitted-value target> ASC|DESC NULLS FIRST|LAST, …   NEW
   LIMIT n                                                    ──► DataFusion SortExec TopK(fetch=n)
```

The emitted-value target for a key is the key column's `render_join_select_item` output. It is wrapped in `nullif(<expr>, '')` when the emitted value is a string: an Arrow `Utf8`, `LargeUtf8`, or `Utf8View` column, or a column that `needs_nested_json_rendering` or `needs_json_fallback` covers. For a `Float32` or `Float64` key, the target is `CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END`, because the `emit_batch` path emits a stored `NaN` as NULL (#246).

#### Window placement per `JoinWindowPlan` variant

| Window | `join.post_join_order_by` | `join.post_join_limit` | Fan-out merge `LIMIT` | Outer wrapper |
|---|---|---|---|---|
| `Unbounded` | empty | none | none | none |
| `BareLimit(n)` | empty | `n` | `n` | none |
| `Ordered`, limit `n`, offset 0 | keys | `n` | none | `ORDER BY keys LIMIT n` |
| `Ordered`, limit `n`, offset `m > 0` | empty | none | none | `ORDER BY keys LIMIT n OFFSET m` |
| `Ordered`, no limit | empty | none | none | `ORDER BY keys` |

Only the third row changes. The rows for `Ordered` with a non-zero offset or no limit keep their current SQL, because both new join-block keys are omitted when empty.

#### Key Interfaces

```rust
// crates/lakehouse-engine/src/scan/spec.rs
pub struct JoinSpec {
    // ... existing fields ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_join_limit: Option<u64>,
    /// Sort keys applied AFTER the node-local join and its WHERE, never to a side's scan.
    /// Set only together with `post_join_limit`. The scan ranks each key by the value it
    /// emits for that column, so the shard's cut agrees with the adapter's merge ranking.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_join_order_by: Vec<SortKey>,
    // ...
}

// crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs
// Replaces `binds_to_projection(..) -> bool`.
fn bound_sort_key<'k>(key: &'k ParsedSortKey, projection: &[ProjectionItem]) -> Option<&'k SortKey>;
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Distributed top-N (per-shard `ORDER BY … LIMIT n`, global merge `ORDER BY … LIMIT n`) | `build_join_sql` plus the unchanged wrapper | The recorded flat-scan argument: a global top-n row survives its own shard's cut |
| Bound lives in the join block | `JoinSpec::post_join_order_by` | Corollary of ADR `join-post-limit-lives-in-join-block`: a join-less spec cannot express a post-join bound |
| One direction/NULL-placement seam | `SortKey::render_ordered` on the shard, `ParsedSortKey::render_order_by_element` on the wrapper | Both end in `render_ordered`, so direction and NULL placement cannot drift |
| The emitting module owns ranking equivalence | `build_join_sql` | Only the join scan knows how each column is emitted (decision [2]) |

#### Quick Diagnostic (new `JoinSpec` field, replaced `binds_to_projection`)

| Question | Answer |
|----------|--------|
| One-sentence responsibility | `post_join_order_by` carries the per-shard post-join ordering. `bound_sort_key` admits a sort key and yields the bare-column key it binds. |
| Easier to call than to reimplement | Yes. The adapter hands over `SortKey` values. The scan alone decides how to rank them against the emitted representation. |
| Internal change forces an edit elsewhere | No. How the scan builds the ordering target stays private to `join_scan.rs`. |
| Doc comment states intent | Yes. It states the post-join placement, the pairing with the cap, and the emitted-value ranking rule. |
| One owner per decision | Yes. Which shape is bounded lives in `build_broadcast_join_sql`. How a shard ranks lives in `build_join_sql`. |
| Boundary visible without reading internals | Yes. The join block is the wire boundary between VS and UDF. |
| Tactical shortcut with follow-up | The flat-scan divergence is not fixed here. A follow-up issue is recommended (decision [6]). |
| Business logic depends inward only | Yes. No new framework or storage dependency. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| New `JoinSpec::post_join_order_by` field (decision [1]) | Reuse `CommonScanSpec::order_by`, or a new key type | The shard-invariant field means a pre-join, one-side ordering |
| Rank each key by its emitted value, with `''` as NULL (decision [2]) | Bare-column ranking, a plan-time JSON-fallback guard, or no bound for string keys | The only rule under which the per-shard cut agrees with the wrapper for every admitted key type |
| Zero offset only (decision [3]) | Per-shard top-(n + m) | Interview scope |
| Wrapper unchanged, no merge `LIMIT` on an ordered fan-out (decision [4]) | Direct-attach merge, or one shared cap value | Interview scope, and a fan-out `LIMIT` before the sort returns wrong rows |

### Iceberg and Delta compliance gate

This plan touches pushdown and scan execution, so the CLAUDE.md check applies. No normative section of the Iceberg table spec or the Delta protocol governs the order of query results. Iceberg § Sorting defines a sort order as per-file writer metadata, and Delta § Clustered Table states writer requirements only. The per-shard top-N sorts rows that the scan already read, and it reads no table-format metadata. Decision [5] quotes both sections. The Exasol-side type limitation that matters here (no empty string in the VARCHAR domain) is named as a deliberate rule in the `datafusion-scan/scan-execution-join` delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-join | CHANGED | `specs/_plans/add-broadcast-join-topn/vs-adapter/pushdown-planning-join/spec.md` |
| datafusion-scan/scan-execution-join | CHANGED | `specs/_plans/add-broadcast-join-topn/datafusion-scan/scan-execution-join/spec.md` |

Checked and left unchanged: `vs-adapter/pushdown-planning-topn` (single-table scope, and its one-seam Background bullet stays true), `datafusion-scan/scan-execution-plan-shape` (raw-row path only), and `vs-adapter/pushdown-planning-join-fallback` (decision [7]).

## Impact

- **Performance.** A zero-offset `ORDER BY … LIMIT n` over a broadcast join emits at most `n` rows per shard instead of the shard's full join. The UDF-to-Exasol row volume drops from the full join to at most `G × n` rows.
- **Results.** Unchanged. The returned rows equal single-node evaluation, as before.
- **Observable plan.** `EXPLAIN VIRTUAL` for that shape shows `"post_join_order_by"` and `"post_join_limit"` inside the common blob's `"join"` block. Every other shape shows the same SQL as before.
- **Wire format.** Additive and backward-compatible. The VS adapter and the scan UDF ship in one `.so`, so no mixed-version deployment exists.
- **Breaking changes.** None.

## Dependencies

- None new. DataFusion 54 already plans `ORDER BY … LIMIT n` as a fetch-limited `SortExec` (TopK), which `tests/scan_plan_shape.rs::order_by_spec_emits_bounded_topk_not_global_sort` pins for the raw scan.
- The implementing commit references `Closes #309`.

## Implementation Tasks

### 1. Join scan: post-join top-N (`datafusion-scan/scan-execution-join`)

- [ ] 1.1 Add `post_join_order_by: Vec<SortKey>` to `JoinSpec` in `crates/lakehouse-engine/src/scan/spec.rs`, with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`. Write the doc comment shown in Key Interfaces. Rewrite the `post_join_limit` doc sentences that say only an unordered cap is pushed and every ordered shard stays unbounded. [expert]
- [ ] 1.2 Add `post_join_order_by: Vec::new()` to every `JoinSpec { .. }` literal. Sites: `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:757` (production, which task 2.2 replaces), `src/scan/spec_tests.rs` (lines 1320, 1413, 1458), `src/scan/join_scan_tests.rs:89`, `src/scan/storage_ref_tests.rs:155`, `src/scan/raw_scan_tests.rs:128`, `src/scan/object_store_tests.rs` (lines 51, 404), `tests/scan_join_test.rs` (lines 189, 457, 810), `tests/scan_plan_shape.rs:630`, and `tests/scan_positional_deletes.rs` (lines 1263, 2121). Run `cargo test --no-run` to confirm that no literal is missed.
- [ ] 1.3 In `src/scan/spec_tests.rs`, add `join_spec_omitting_post_join_order_by_deserializes_to_empty`. Mirror `join_spec_omitting_post_join_limit_deserializes_to_none`: an empty ordering emits no key, a missing key deserializes to an empty ordering, and a set ordering survives `ScanSpec::from_parts_json`.
- [ ] 1.4 In `build_join_sql` (`src/scan/join_scan.rs`), splice ` ORDER BY <elements>` after the `WHERE` and before the `LIMIT` when `join.post_join_order_by` is non-empty. Render each element with `SortKey::render_ordered` over the emitted-value target that Architecture defines, built by a private helper beside `render_join_select_item`. For a `Float32` or `Float64` key, the target is `CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END`, so a stored `NaN` ranks as the NULL that `emit_batch` emits (#246). Update the doc comments of `build_join_sql` and `run_join_scan_with_session` to name the ordering. [expert]
- [ ] 1.5 In `src/scan/join_scan_tests.rs`, add `build_join_sql_renders_no_order_by_without_a_post_join_ordering`. Add `build_join_sql_ranks_a_json_rendered_key_by_its_emitted_text`, using `MemTable` sides as `build_join_sql_renders_a_nested_column_as_valid_json_end_to_end` does. Its fact side carries a `List<Int64>` key with the values `[9]` and `[10]`. With `ASC NULLS LAST` and a cap of 1, assert that the one emitted row is `[10]`: the emitted text `"[10]"` sorts before `"[9]"`, while native list order puts `[9]` first. Add `build_join_sql_ranks_a_nan_float_key_as_null` on the same `MemTable` pattern. Its fact side carries a `Float64` key with the values `NaN` and `1.0`. With `DESC NULLS LAST` and a cap of 1, assert that the one emitted row is `1.0`. Native DataFusion ranking puts `NaN` first under `DESC`, so the assertion fails without the rule.
- [ ] 1.6 In `tests/scan_join_test.rs`, add `join_order_by_limit_emits_bounded_top_n_after_join` over the existing `write_orders` and `write_customer` fixtures. Order by `C_NAME DESC NULLS FIRST, O_ORDERKEY ASC NULLS LAST` (one key per side) with a cap of 2, and assert the exact rows `(3, Carol)` and `(2, Bob)`. Set a conflicting `common.order_by` to prove that the scan ignores that field. Assert that the `build_join_physical_plan` display contains `TopK(fetch=` and no `SortExec: expr=[`. Assert `has_no_fetch_below` and a new `has_no_sort_below` on both `HashJoinExec` inputs. [expert]
- [ ] 1.7 In `tests/scan_join_test.rs`, add a `write_customer_with_blank_names` fixture (custkey 10 named `""`, 20 `Bob`, 30 `Carol`) and `join_top_n_ranks_an_empty_string_key_as_null`, joined against `write_orders`. With `C_NAME ASC NULLS LAST, O_ORDERKEY ASC NULLS LAST` and a cap of 2, assert the rows `(2, Bob)` and `(5, Bob)`. With `C_NAME DESC NULLS FIRST, O_ORDERKEY ASC NULLS LAST` and a cap of 2, assert the rows `(1, "")` and `(4, "")`. Native string ranking returns the two empty-name rows for the first query and `Carol` first for the second, so both assertions fail without the rule.

### 2. Broadcast planner: per-shard top-N (`vs-adapter/pushdown-planning-join`)

- [ ] 2.1 In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, replace `binds_to_projection` with `bound_sort_key` (signature in Key Interfaces). It returns the key's `SortKey` when the key is a bare column that the rendered projection carries as a bare-column item. The per-shard key list then comes from the same check that admits the key.
- [ ] 2.2 In `build_broadcast_join_sql`, map each `JoinWindowPlan` variant to its join-block bounds and its fan-out merge limit, per the Window placement table. Pass `Some(n)` as the merge limit to `build_scan_driving_sql` only for `BareLimit(n)`. Keep the wrapper call and its `debug_assert_ne!` unchanged. Update the doc comments of `build_broadcast_join_sql` and of `JoinWindowPlan::Ordered` (`joins/planning.rs`) to describe the zero-offset per-shard bound.
- [ ] 2.3 In `joins/sql_builders_tests.rs`, replace `broadcast_ordered_wraps_fan_out_and_leaves_shards_unbounded` with `broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n`. Assert that `join.post_join_order_by` equals the key with its flags and that `join.post_join_limit` is 5. Assert that the common blob has no `limit` and no `order_by`. Assert that the SQL ends with `) ORDER BY "L_ORDERKEY" ASC NULLS LAST LIMIT 5` and contains ` LIMIT ` exactly once. Extend `assert_shards_carry_no_window` to assert that `join.post_join_order_by` is absent.
- [ ] 2.4 In `joins/sql_builders_tests.rs`, add `broadcast_ordered_bounds_shards_with_keys_from_either_side`. Use a fixture variant that projects `L_ORDERKEY` and `O_ORDERDATE` and orders by `O_ORDERDATE DESC NULLS FIRST, L_ORDERKEY ASC NULLS LAST`. Assert that the join block carries both keys in order, each with its flags.
- [ ] 2.5 In `joins/sql_builders_tests.rs`, add `broadcast_zero_offset_request_takes_the_bounded_path`. Build the window through `classify_join_window` from two request fixtures, `"limit": {"numElements": 5, "offset": 0}` and `"limit": {"numElements": 5}`. Assert that `build_broadcast_join_sql` returns byte-identical SQL for both, and that the SQL carries `post_join_order_by`.
- [ ] 2.6 In `crates/lakehouse-engine/tests/e2e_join_test.rs`, extend `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct` to assert that the pushed SQL contains `"post_join_order_by"` and `"post_join_limit":3`. Extend `e2e_broadcast_join_order_by_without_limit_and_with_offset_stay_broadcast` to assert that neither key appears in either pushed SQL.
- [ ] 2.7 In `crates/lakehouse-engine/tests/common/seed.rs`, add a `dim_blank_label` table (`B_CUSTKEY` Int64, `B_LABEL` String, both required) with keys 1 to 5 and the labels `""`, `""`, `"alpha"`, `"bravo"`, `"charlie"` in one file. Seed it from `seed_events`, following the pattern of `seed_multi_table_join_extension`. Confirm that no Iceberg E2E test enumerates the namespace's tables.
- [ ] 2.8 In `e2e_join_test.rs`, add `e2e_broadcast_join_top_n_ranks_empty_string_as_null`. Run `SELECT b.B_LABEL, o.O_ORDERKEY FROM <vs>.FACT_ORDERS o JOIN <vs>.DIM_BLANK_LABEL b ON o.O_CUSTKEY = b.B_CUSTKEY ORDER BY b.B_LABEL ASC NULLS LAST, o.O_ORDERKEY ASC LIMIT 2` and its `DESC NULLS FIRST` variant against `VS_NAME` and `VS_NAME_LOW`. Assert that the `VS_NAME` plan is broadcast and carries `"post_join_order_by"`, and that the `VS_NAME_LOW` plan is the two-scan wrapper. Assert that both results are equal in query order, and that the ascending result is the `alpha` rows of orders 3 and 8. If the two-scan result ranks an emitted empty string as non-NULL, stop and escalate: the `nullif` rule of decision [2] is then wrong. Add a tie case with the same select list and `ORDER BY b.B_LABEL ASC NULLS LAST LIMIT 3` (decision [8]). Its third row is one of the two `bravo` rows, orders 4 and 9, which sit in different fact files. Against `VS_NAME`, assert that the plan carries `"post_join_order_by"` and that the label sequence is `alpha`, `alpha`, `bravo`. Assert that the first two `O_ORDERKEY` values are 3 and 8 in either order, and that the third is 4 or 9. Assert that `VS_NAME_LOW` returns the same label sequence.
- [ ] 2.9 In `docs/debugging-pushdown.md`, update the broadcast row of the declared-cap table (line 84). State that a zero-offset `ORDER BY … LIMIT n` also bounds each shard through `JoinSpec::post_join_order_by` plus `post_join_limit`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Join scan post-join top-N and its wire field | 1.1-1.7 | — | spec delta `datafusion-scan/scan-execution-join`; `crates/lakehouse-engine/src/scan/spec.rs`, `spec_tests.rs`, `join_scan.rs`, `join_scan_tests.rs`, `crates/lakehouse-engine/tests/scan_join_test.rs`, and the `JoinSpec` literal sites of task 1.2 |
| B: Broadcast planner per-shard top-N and live proof | 2.1-2.9 | A (reads `JoinSpec::post_join_order_by`, and replaces A's placeholder in the `sql_builders.rs` literal) | spec delta `vs-adapter/pushdown-planning-join`; `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, `sql_builders_tests.rs`, `joins/planning.rs`, `crates/lakehouse-engine/tests/e2e_join_test.rs`, `crates/lakehouse-engine/tests/common/seed.rs`, `docs/debugging-pushdown.md` |

The groups run in sequence. Group A holds the scan-side model: the join SQL, the emitted representation of each column, and DataFusion's physical plan. Group B holds the planner-side model: window classification, the fan-out, the wrapper, and the E2E stack. The only shared file is the production `JoinSpec` literal in `sql_builders.rs`, which A touches with `Vec::new()` and B then sets.

Tasks 1.1, 1.4, and 1.6 carry `[expert]`, so group A routes to the expert implementer. Task 1.4 carries the ranking-equivalence rule, task 1.6 carries the TopK plan-shape proof, and task 1.1 carries the wire-format contract. Group B is untagged. Its one subtle rule (no merge `LIMIT` on an ordered fan-out) is pinned by task 2.3's exactly-once `LIMIT` assertion.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` `binds_to_projection` | Replaced by `bound_sort_key`, which also yields the bound `SortKey` |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` `broadcast_ordered_wraps_fan_out_and_leaves_shards_unbounded` | Asserts the superseded unbounded-shard behavior for a zero-offset `LIMIT`. Replaced by task 2.3 |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| pushdown-planning-join: A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n`, `broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only`, `broadcast_ordered_plan_rendering_no_order_by_is_a_programming_error` |
| pushdown-planning-join: A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct` |
| pushdown-planning-join: A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n`, `broadcast_ordered_bounds_shards_with_keys_from_either_side`, `broadcast_zero_offset_request_takes_the_bounded_path` |
| pushdown-planning-join: A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct`, `e2e_broadcast_join_top_n_ranks_empty_string_as_null` |
| pushdown-planning-join: An ordered broadcast window with a non-zero offset or no LIMIT leaves every shard unbounded | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `broadcast_ordered_without_limit_wraps_fan_out_with_no_window`, `broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only` |
| pushdown-planning-join: An ordered broadcast window with a non-zero offset or no LIMIT leaves every shard unbounded | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_without_limit_and_with_offset_stay_broadcast` |
| scan-execution-join: A post-join ordering and cap bound each join shard to its own top-N over the joined output | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_order_by_limit_emits_bounded_top_n_after_join` |
| scan-execution-join: A join shard ranks each sort key by the value it emits | Integration | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_top_n_ranks_an_empty_string_key_as_null` |
| scan-execution-join: A join shard ranks each sort key by the value it emits | Unit | `crates/lakehouse-engine/src/scan/join_scan_tests.rs` | `build_join_sql_ranks_a_json_rendered_key_by_its_emitted_text`, `build_join_sql_ranks_a_nan_float_key_as_null` |
| scan-execution-join: A join shard ranks each sort key by the value it emits | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_top_n_ranks_empty_string_as_null` |
| scan-execution-join: A join block without a post-join ordering keeps its encoding | Unit | `crates/lakehouse-engine/src/scan/spec_tests.rs`, `crates/lakehouse-engine/src/scan/join_scan_tests.rs` | `join_spec_omitting_post_join_order_by_deserializes_to_empty`, `build_join_sql_renders_no_order_by_without_a_post_join_ordering` |

The SQL-builder and serde tests are unit tests because they are pure string and JSON computation with no I/O.

### Manual Testing

Run `make test-e2e` first. It provisions the Docker Exasol, seeds the tables, and creates the virtual schemas `MY_LAKEHOUSE_JOIN` and `MY_LAKEHOUSE_JOIN_LOW`. Set `DSN="exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0"`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-join (zero offset) | `exapump sql "EXPLAIN VIRTUAL SELECT c.C_NAME, o.O_ORDERDATE FROM MY_LAKEHOUSE_JOIN.FACT_ORDERS o JOIN MY_LAKEHOUSE_JOIN.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY ORDER BY o.O_ORDERDATE DESC LIMIT 3" -d "$DSN"` | The pushed SQL contains `"post_join_order_by":[{"column":"O_ORDERDATE","ascending":false` and `"post_join_limit":3` inside `"join":{`. It ends with `ORDER BY "O_ORDERDATE" DESC`, a NULL placement, and ` LIMIT 3`. ` LIMIT ` occurs once. |
| vs-adapter/pushdown-planning-join (non-zero offset) | The same command with `LIMIT 3 OFFSET 1` | The pushed SQL contains `"join":{` but neither `post_join_order_by` nor `post_join_limit`. It ends with ` LIMIT 3 OFFSET 1`. |
| datafusion-scan/scan-execution-join | `exapump sql "SELECT b.B_LABEL, o.O_ORDERKEY FROM MY_LAKEHOUSE_JOIN.FACT_ORDERS o JOIN MY_LAKEHOUSE_JOIN.DIM_BLANK_LABEL b ON o.O_CUSTKEY = b.B_CUSTKEY ORDER BY b.B_LABEL ASC NULLS LAST, o.O_ORDERKEY ASC LIMIT 2" -d "$DSN"` | Two rows: `alpha, 3` and `alpha, 8`. No row carries a NULL label. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
