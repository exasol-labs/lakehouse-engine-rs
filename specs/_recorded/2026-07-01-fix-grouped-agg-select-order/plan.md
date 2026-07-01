# Plan: fix-grouped-agg-select-order

## Summary

Fix the grouped-aggregate pushdown bug (GitHub issue #33) where the adapter always
emits group keys before aggregates in the outer merge SELECT, transposing columns
relative to the user's `selectList` and failing Exasol's positional pushdown
column-type check whenever an aggregate precedes or interleaves with a group key.

## Design

### Context

`crates/lakehouse-engine/src/adapter/pushdown.rs` handles grouped-aggregate
pushdown. `detect_group_by_aggregates` (L713-766) walks `pushdownRequest.selectList`
and splits it into two disjoint lists — `Vec<String> group_keys` and
`Vec<AggregatePlan> plans` — **discarding each item's original select-list index**.
`build_grouped_aggregate_scan_sql` (L1079-1163) then assembles the outer merge SELECT
unconditionally as `gk_select.chain(merge_items)` (L1116-1121), i.e. group keys always
first. Exasol validates the outer merge SELECT positionally against
`selectListDataTypes`; when an aggregate precedes a key in the original select list,
the adapter's keys-first output is transposed → `SQL Error [04000]: ... Data type
mismatch in column number 1`.

This is a single defect with three broken sub-cases (only sub-case 1 is reported in #33):
1. Aggregate before a single group key — `SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`
2. Interleaved multi-key GROUP BY — `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`
3. Expression group key after an aggregate — `SELECT COUNT(*), MOD(id,4) ... GROUP BY MOD(id,4)`

A secondary fragility travels with the same code: `group_key_exasol_types` (L784)
resolves a key's declared type by **comparing rendered SQL strings**
(`render_expression(item).ok().as_deref() == Some(key)`), and the detection
expression-key fallback (L757-760) accepts a select-list item as a group-key
projection via `group_keys.contains(&sql)`. Both are correct today but silently miss
if an expression key's two renderings (in `groupBy` vs `selectList`) differ by
whitespace or casing, defaulting to `VARCHAR(2000000)` with no CAST and a wrong result
type with no error.

- **Goals** — Assemble the outer wrapper SELECT, its cast list, and its GROUP BY list
  in the user's `selectList` order for ANY interleaving of keys and aggregates; replace
  string-equality type/classification lookups with index-based matching; cover all three
  sub-cases plus HAVING in one change.
- **Non-Goals** — No change to the scan UDF side, the wire `ScanSpec`/`AggregatePlan`
  shape, the inner fan-out EMITS clause, or the per-shard partial-aggregate SELECT.
  No change to aggregate-merge decomposition, sharding, or LIMIT/HAVING placement
  semantics beyond ordering.

### Decision

Thread each `selectList` item's **original index and classification** (group-key
projection vs aggregate) through `detect_group_by_aggregates`, then have
`build_grouped_aggregate_scan_sql` place the already-computed, already-typed SQL
fragments (`gk_select[i]` cast expressions and `merge_items[j]` merged aggregates)
into the outer SELECT / GROUP BY / order slots dictated by that index. The inner
fan-out (EMITS + per-shard scan) stays keys-first and unchanged — it is matched only
against itself, never against the user's select list. Reuse the same per-item index
to resolve group-key types by index instead of by rendered-string comparison.

#### Architecture

```
pushdownRequest.selectList  [SUM(score)@0, MOD(id,4)@1]
        │
        ▼  detect_group_by_aggregates  (thread index + classification)
   ordered items: [Agg(plan0)@0, Key(gk0)@1]   group_keys=[gk0]  plans=[plan0]
        │
        ▼  build_grouped_aggregate_scan_sql
   inner fan-out EMITS  ("GK_0" …, PARTIAL_* …)   ← UNCHANGED, keys-first
   outer SELECT  [ merge_items[0] , CAST("GK_0" AS ty) ]  ← reordered by index
   outer GROUP BY [ "GK_0" ]
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Index-carrying detection result | `detect_group_by_aggregates` return shape | Preserves select-list order without recomputing classification downstream |
| Positional assembly by original index | `build_grouped_aggregate_scan_sql` outer SELECT | Matches Exasol's positional `selectListDataTypes` check for any ordering |
| Index-based (not string-based) type lookup | `group_key_exasol_types`, detection expression-key fallback | Removes silent `VARCHAR` fallback on whitespace/case drift |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Full positional reorder threading select-list index | Narrow patch handling only "aggregate before one key" | The narrow patch leaves interleaved multi-key and expression-key-after-aggregate broken; #33's root cause is general transposition, so fix it generally |
| Keep `ScanSpec`/`AggregatePlan` disjoint-list wire shape unchanged | Add ordering to the wire spec | Scan UDF SELECT + emit loop + fan-out EMITS are self-consistent keys-first and never see the user's select order; verified in `scan/mod.rs` (`build_grouped_partial_agg_sql`, emit loop). Changing the wire shape would be churn with no correctness benefit |
| Fold index-based type/classification lookup into the same refactor | Separate follow-up change for the string-match fragility | The refactor already carries per-item index + classification; reusing it removes the fragile string lookup for free rather than adding a second mechanism |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-grouped-agg | CHANGED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |
| packaging/e2e-harness | CHANGED | `packaging/e2e-harness/spec.md` |

`datafusion-scan/scan-execution-grouped-agg` is intentionally **NOT** in scope:
independently re-verified via `crates/lakehouse-engine/src/scan/mod.rs`
(`build_grouped_partial_agg_sql` L390-423 and the emit loop L344-368) that the scan
side is keys-first on both the DataFusion SELECT and the emit order, matched only
against the fan-out EMITS clause and never against the user `selectList` — no change
needed there.

## Implementation Tasks

1. **Refactor detection + assembly to preserve select-list order.**
   1.1 Change `detect_group_by_aggregates` to return, in addition to `group_keys` and
       `plans`, an ordered classification of each `selectList` item carrying its
       original index and whether it is a group-key projection (with which group-key
       slot) or an aggregate (with which aggregate-plan slot). [expert]
   1.2 Rewrite `build_grouped_aggregate_scan_sql` outer-SELECT assembly to place each
       group-key cast expression and each merged-aggregate expression at its original
       select-list ordinal, replacing the unconditional `gk_select.chain(merge_items)`
       (leave the inner fan-out EMITS `gk_emits.chain(partial_items)` untouched). [expert]
   1.3 Replace `group_key_exasol_types`' rendered-string `position` lookup (L784) and
       the detection expression-key fallback's `group_keys.contains(&sql)` (L757-760)
       with index-based matching using the classification from 1.1. [expert]
   1.4 Update `handle_pushdown`'s call site (L1516-1568) to pass the new
       ordering/classification data through to `build_grouped_aggregate_scan_sql`.

2. **Unit tests in `pushdown.rs` test module** (run without Docker; fastest regression net).
   2.1 Extend the `make_group_by_request` helper (or add a variant) to accept
       `selectListDataTypes` so ordering + type-position assertions are possible.
   2.2 Add `detect_group_by_aggregates` tests for all four orderings: aggregate-before-key,
       interleaved multi-key, expression-key-after-aggregate, aggregate-first-with-HAVING —
       asserting the returned classification preserves original indices.
   2.3 Add `build_grouped_aggregate_scan_sql` tests asserting the outer SELECT column
       order and per-item CAST type match the input `selectList` order for the same four
       arrangements (e.g. aggregate expression appears at position 0, `CAST("GK_0" AS DECIMAL...)`
       at position 1 for the #33 repro).
   2.4 Add a regression test that an expression group key whose `groupBy` vs `selectList`
       renderings differ only by whitespace/casing still resolves its declared type by
       index (no silent `VARCHAR(2000000)` fallback).

3. **E2E tests in `crates/lakehouse-engine/tests/e2e_scan_test.rs`** (Phase 5 GROUP BY
   region, ~L905+). Must FAIL (not skip) if the Exasol Docker + MinIO stack is
   unavailable, following the existing `setup_e2e()` / `exa_conn()` / `query_columns` /
   `parse_int` / `parse_numeric` conventions. New cases must NOT put the key first.
   3.1 Aggregate before a single group key (literal #33 repro): `SELECT SUM(score), MOD(id,4)
       FROM {vs_table} GROUP BY MOD(id,4)` — assert success and per-group values match the
       already-correct key-first ordering.
   3.2 Interleaved multi-key GROUP BY: `SELECT MOD(id,4), SUM(score), MOD(id,2) FROM {vs_table}
       GROUP BY MOD(id,4), MOD(id,2)`.
   3.3 Expression group key after an aggregate: `SELECT COUNT(*), MOD(id,4) FROM {vs_table}
       GROUP BY MOD(id,4)` — assert the key column carries its DECIMAL type, not VARCHAR.
   3.4 Aggregate-first + HAVING: `SELECT SUM(score), MOD(id,4) FROM {vs_table} GROUP BY
       MOD(id,4) HAVING SUM(score) > n` — exercises the HAVING-present outer-wrapper path.

4. **Validate & finalize.** Run `cargo test` (host unit), `cargo clippy --all-targets`,
   `cargo fmt`; reference `Closes #33` in the implementing commit per CLAUDE.md.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3, 1.4 (single cohesive refactor — one worker) |
| Group B | 2.1, 2.2, 2.3, 2.4 (unit tests) |
| Group C | 3.1, 3.2, 3.3, 3.4 (E2E tests) |

Sequential dependencies:
- Group A → Group B (tests assert the new ordered behavior)
- Group A → Group C (E2E cases require the fix to pass)
- Group B and Group C are independent of each other (both depend only on A)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Logic | `group_key_exasol_types` string-`position` lookup (`pushdown.rs` L784) | Replaced by index-based matching (task 1.3) |
| Logic | detection expression-key `group_keys.contains(&sql)` branch (`pushdown.rs` L757-760) | Superseded by index-based classification (task 1.3) |

No whole functions or tests are removed; existing keys-first unit/E2E tests remain
valid (keys-first is one legal ordering under the fix).

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Grouped aggregate query is detected and translated to a grouped scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `detect_group_by_aggregates_preserves_select_list_order` |
| Grouped aggregate wrapper SQL re-groups partial results per user group key | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_wrapper_outer_select_follows_select_list_order` |
| Outer wrapper SELECT preserves user select-list order for interleaved keys and aggregates | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_wrapper_agg_before_key_ordering`, `grouped_wrapper_interleaved_multi_key_ordering`, `grouped_wrapper_expr_key_after_agg_ordering` |
| Grouped scan spec carries group-key rendered SQL fragments | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `group_key_type_resolved_by_index_not_string_match` |
| End-to-end grouped aggregate with an aggregate before the group key returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_agg_before_key` |
| End-to-end interleaved multi-key GROUP BY with an aggregate between the keys returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_interleaved_multi_key` |
| End-to-end expression group key placed after an aggregate returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_expr_key_after_agg` |
| End-to-end aggregate-first GROUP BY with HAVING returns correct results | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_agg_first_with_having` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-grouped-agg | `SELECT SUM(score), MOD(id,4) FROM <vs>.<table> GROUP BY MOD(id,4)` (via Exasol Docker + MinIO after `make cross-musl-udf-build` + deploy) | Query succeeds (no "Data type mismatch in column number 1"); per-group sums match the key-first form |
| packaging/e2e-harness | `make test-e2e` | All four new grouped-order cases pass; suite fails (not skips) if the stack is down |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, without the stack) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
