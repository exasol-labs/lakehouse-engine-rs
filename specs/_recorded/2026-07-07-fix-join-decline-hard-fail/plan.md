# Plan: fix-join-decline-hard-fail

## Summary

Fix issue #76: a pushdown over an inner join spanning three or more tables (Q1 `supplier⋈nation⋈region`, Q2 `customer⋈orders⋈lineitem`, NQ3 `part⋈partsupp⋈supplier⋈nation`) currently hard-fails with `F-UDF-CL-RUST-9001: join pushdown declined: the join spans more than two tables …` instead of falling back to unaccelerated execution. Generalize the already-specified unaccelerated fallback from two tables to N tables so a 3+ table inner join is served by an N-scan wrapper (never an error), closing the spec-vs-implementation gap that PR #70 left open.

## Design

### Context

PR #70 (`add-join-pushdown-broadcast`, merged `0e2fe9b`) introduced `JoinShape::Ineligible(IneligibleJoinReason::TooManyTables)`, which `handle_pushdown` (`pushdown.rs:2097`) turns into `Err(ineligible_join_decline(reason))`. Every `UdfError` variant is erased by the `exasol-udf-macros` 0.20.3 FFI shim to return code 1, which the Exasol UDF host surfaces as a hard SQL error — there is **no** code path in this repo or the SDK that makes Exasol retry a declined pushdown natively. This is the exact false premise ADR-083 rejected and ADR-085/086 fixed for the two-table case; the >2-table case was left declining because the fallback builder (`build_unaccelerated_join_sql`, `resolve_join_sides`, `build_two_scan_join_sql`, `build_join_alias_map`) is structurally two-sided.

The feature spec already requires the correct behavior ("spans more than two involved tables … SHALL instead emit the unaccelerated … join SQL when it can build one"). This plan is therefore a `fix-` (spec-vs-implementation mismatch), not new spec authoring — it generalizes the fallback to N tables and closes the runtime-behavior test gap.

- **Goals**
  - A 3+ table inner-join pushdown returns a valid `{"type":"pushdown","sql":…}` response (an N-scan wrapper), never an `Err`.
  - The N-scan wrapper's result equals single-node evaluation (correctness-first; "never wrong, only unaccelerated").
  - Close the test gap: a runtime (E2E) test proving Q1/Q2/NQ3-shape joins succeed and return correct results — not merely asserting the decline message text.
- **Non-Goals**
  - No change to the two-table broadcast path or the two-table two-scan fallback (ADR-081..086 behavior stays byte-for-byte; all existing 2-table host + E2E tests keep passing).
  - No N-table broadcast / node-local N-way DataFusion join (broadcast stays strictly two-table). No BL-001 Phase-2 broadcast work.
  - No new join capabilities advertised (outer joins stay unadvertised; the capability surface is unchanged).
  - No unrelated lc-rs/perf content from the sibling PR #74 this branch stacks on.

### Decision

Add an **additive** N-table (N≥3) inner-join fallback path that reuses the ADR-085/086 qualified-rendering machinery wholesale, leaving the two-table path untouched.

#### Architecture

```
handle_pushdown
  └─ detect_join(from tree)
       ├─ NotAJoin                → single-table path         (unchanged)
       ├─ Eligible(2-table equi)  → plan_eligible_join         (unchanged: broadcast | 2-scan)
       ├─ MultiTable(N≥3 inner)   → plan_multi_table_join       ★ NEW
       └─ Ineligible(reason)      → Err (native retry)          (only genuinely unbuildable)

plan_multi_table_join
  ├─ resolve each of N sides once  (resolve_one_join_side, side-local pruning)   [reused]
  ├─ any side empty → shape-correct empty result over combined N-table columns   [generalized]
  └─ build_n_scan_join_sql
       ├─ N sharded fan-out subqueries  (build_side_fan_out_sql)                  [reused]
       ├─ N-entry alias map  LHS_T0..LHS_T{N-1}  (uppercased tableName → alias)   ★ NEW
       ├─ every join-tree condition + WHERE + select/GROUP BY/HAVING/ORDER BY
       │   rendered table-qualified   (render_*_qualified, ADR-085/086)          [reused]
       └─ FROM (fan0) "LHS_T0", …, (fanK) "LHS_TK" WHERE <ANDed conditions + filter>
```

The genuinely new code is confined to (1) walking the nested-join `from` tree in `detect_join` to collect the N leaf tables and every join node's condition while asserting all nodes are inner, and (2) assembling the N-scan wrapper (N-entry alias map + cross-join FROM + conjunctive qualified WHERE). Everything else — per-side resolution, per-side fan-out, and all qualified rendering of conditions/filter/projection/aggregate/GROUP BY/HAVING/ORDER BY/LIMIT — is the existing two-table machinery, which is already alias-map-driven and table-count-agnostic.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Cross-join + conjunctive qualified WHERE | `build_n_scan_join_sql` FROM/WHERE | For all-inner nodes, `FROM a,b,c WHERE c1 AND c2 AND …` is provably equivalent to a chained `INNER JOIN … ON` tree but is order-agnostic — no ON-scoping bookkeeping across the join tree; Exasol's optimizer turns equi-WHERE into hash joins |
| Reuse ADR-085 `tableAlias` annotation | N-entry `alias_of` map | The qualified renderers already emit `"ALIAS"."COL"` from a `tableName→alias` map; extending the map to N entries makes shared column names across any pair correct with zero renderer changes |
| Additive path, two-table path frozen | `JoinShape::MultiTable` + `plan_multi_table_join` | Isolates regression risk; the `LHS_FACT`/`LHS_DIM` two-scan wrapper and all its tests/ADRs are unchanged |
| Last-resort error only | `detect_join` / `build_n_scan_join_sql` | Matches ADR-083: hard error reserved for a shape whose fallback genuinely cannot be built |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| N-table fallback via cross-join + conjunctive qualified WHERE | Chained `INNER JOIN … ON` reproducing the tree | Cross-join+WHERE is order-agnostic for all-inner joins and sidesteps ON-scope ordering; semantically identical, Exasol optimizes it to hash joins |
| Add `JoinShape::MultiTable` (N≥3); freeze the two-table path | Retrofit N-tables into `EligibleJoin`/`JoinSides`/`build_unaccelerated_join_sql` | Additive isolation keeps every ADR-081..086 two-table test green and confines the change to new, testable units |
| Broadcast stays strictly two-table | Node-local N-way DataFusion join in the UDF | Out of scope (BL-001 / mission "join pushdown beyond broadcast is out of scope"); the fallback already makes 3+ table joins correct, just unaccelerated |
| Error only for non-inner node / missing TABLE_MAP entry / unrenderable clause | Keep declining all `TooManyTables` | The spec already mandates fallback for >2 tables; an all-inner nested tree over resolvable tables is always buildable |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-join | CHANGED | `vs-adapter/pushdown-planning-join/spec.md` |

Delta: one CHANGED scenario ("A join outside the broadcast contract is declined safely" — clarifies >2 tables never errors), one NEW scenario ("A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper"), plus one NEW Background bullet.

## Dependencies

None new. Reuses existing `resolve_one_join_side`, `build_side_fan_out_sql`, `build_scan_driving_sql`, `annotate_columns_with_alias`, `render_expression_qualified`, `render_df_filter_qualified`, `qualified_join_select_items`/`_group_by`/`_having`/`_order_by`, `side_local_filter`, `referenced_side_columns`, `involved_table_columns`, `empty_result_sql`. Stacked on branch `feat/change-lc-rs-sdk-0-20-3` (PR #74); touches only join-pushdown code.

## Implementation Tasks

1. Detection — generalize `detect_join`
   - [ ] 1.1 Walk the nested-join `from` tree collecting every base-table leaf (in a stable order) and every join node's `condition`; assert every join node is `join_type = "inner"`. Introduce `JoinShape::MultiTable(MultiTableJoin)` carrying the N involved tables (Exasol name + original-cased `TABLE_MAP` Iceberg ident) and the N-1 collected condition nodes. [expert]
   - [ ] 1.2 Classify the boundaries: N==2 stays `Eligible`/`Ineligible` exactly as today; N≥3 all-inner → `MultiTable`; a non-inner node in the tree → `Ineligible(NotInnerJoinType)`; a non-table leaf / malformed node → `Ineligible(UnsupportedShape)`; a leaf table absent from `TABLE_MAP` → hard `Err` (stale VS), identical to the two-table path.
2. Routing — `handle_pushdown`
   - [ ] 2.1 Add `JoinShape::MultiTable(m) => return plan_multi_table_join(...).await;` next to the existing `Eligible` arm; leave `NotAJoin`, `Eligible`, and `Ineligible` arms unchanged.
3. N-side resolution — `plan_multi_table_join`
   - [ ] 3.1 Resolve each of the N sides once via `resolve_one_join_side`, forwarding each side's side-local WHERE conjuncts (`side_local_filter`) for Iceberg pruning, exactly as `resolve_join_sides` does per side.
   - [ ] 3.2 If ANY side has zero files, emit the shape-correct empty result (`empty_result_sql`) over the combined N-table projected column universe (generalize the two-side `involved_table_columns` extend to N tables). [expert]
4. N-scan SQL builder — `build_n_scan_join_sql`
   - [ ] 4.1 Build the N-entry alias map (`LHS_T0..LHS_T{N-1}`, uppercased `tableName` → alias) and render every collected join condition table-qualified via `render_expression_qualified`, AND-conjoining them with the qualified residual WHERE. [expert]
   - [ ] 4.2 Build one sharded fan-out per side (`build_side_fan_out_sql` with each side's referenced-column narrowing and side-local filter), then assemble `SELECT <qualified select list> FROM (fan0) "LHS_T0", … WHERE <ANDed conditions+filter> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`, reusing `qualified_join_select_items`/`_group_by`/`_having`/`_order_by` and an N-table `full_row_qualified_items`. Return `Err` (native retry) only when a condition/clause cannot be rendered or an involved table carries no column metadata. [expert]
5. Host unit tests (`crates/lakehouse-engine/src/adapter/pushdown.rs` `#[cfg(test)]`)
   - [ ] 5.1 `detect_join` over a 3-table and a 4-table all-inner nested tree → `MultiTable` with the right table count and condition count; a non-inner node in the tree → `Ineligible(NotInnerJoinType)`; a leaf missing from `TABLE_MAP` → `Err`.
   - [ ] 5.2 `build_n_scan_join_sql` for the Q1/Q2/NQ3 shapes yields an N-scan wrapper (N distinct `LHS_T*` aliases, all N-1 conditions present, table-qualified) — not an `Err`; a shared-column-name pair across three tables renders qualified (no bare-name ambiguity).
   - [ ] 5.3 Update `join_outside_contract_declined_safely`: `TooManyTables` is no longer asserted as a decline-to-error (it now routes to `MultiTable`); retire the `TooManyTables` decline facet / dead reason path if `detect_join` no longer produces it.
6. E2E test (`crates/lakehouse-engine/tests/e2e_join_test.rs`, against local Exasol Docker)
   - [ ] 6.1 Seed a third (and, for the 4-table shape, fourth) small Iceberg table in the join E2E namespace (extend `tests/common/seed.rs` join fixtures).
   - [ ] 6.2 Add `e2e_three_table_join_result_correct` (customer⋈orders⋈lineitem or supplier⋈nation⋈region) and `e2e_four_table_join_result_correct` (part⋈partsupp⋈supplier⋈nation): assert the query SUCCEEDS (no `F-UDF-CL-RUST-9001`), the pushed SQL is the N-scan wrapper (multiple `LHS_T*` aliases, not a native decline), and the result equals the same join computed independently — mirroring the existing `e2e_broadcast_join_result_correct` / `has_two_scan_wrapper` helper style.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 (detection) |
| Group B | 2.1 (routing), 3.1, 3.2 (resolution) |
| Group C | 4.1, 4.2 (SQL builder) |
| Group D | 5.1, 5.2, 5.3 (host tests), 6.1 (seed) |
| Group E | 6.2 (E2E behavior) |

Sequential dependencies:
- Group A → Group B (routing/resolution consume the `MultiTable` shape)
- Group B → Group C (builder consumes resolved sides)
- Group C → Group D (host tests exercise detection + builder; 5.1 can start with Group A)
- Group D → Group E (E2E needs the built path + seeded tables)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Enum variant / branch | `IneligibleJoinReason::TooManyTables` and its `detect_join` producers (`pushdown.rs:3546,3583`) | `TooManyTables` no longer routes to a decline; remove the variant (and its `ineligible_join_decline` arm + `join_outside_contract_declined_safely` facet) if `detect_join` stops producing it, or keep it only if still reachable as a genuinely-unbuildable defensive case — the implementer decides during task 5.3 |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A join outside the broadcast contract is declined safely (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `join_outside_contract_declined_safely` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — detection | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `detect_join_multi_table_inner_is_multitable` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — SQL shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_n_scan_join_sql_renders_qualified_wrapper` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — runtime, 3 tables | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_three_table_join_result_correct` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (NEW) — runtime, 4 tables | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_four_table_join_result_correct` |

Existing scenarios (broadcast, threshold, projection/EMITS, condition rendering, shared-column two-scan, aggregate-over-join, capabilities) keep their current passing tests unchanged — this plan must not regress them.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-join | `EXPLAIN VIRTUAL SELECT n.N_NAME, r.R_NAME FROM VS.SUPPLIER s JOIN VS.NATION n ON s.S_NATIONKEY=n.N_NATIONKEY JOIN VS.REGION r ON n.N_REGIONKEY=r.R_REGIONKEY;` | Pushed SQL is an N-scan wrapper with three `LHS_T*` fan-out subqueries joined by Exasol; no `F-UDF-CL-RUST-9001` error |
| vs-adapter/pushdown-planning-join | `SELECT COUNT(*) FROM VS.CUSTOMER c JOIN VS.ORDERS o ON c.C_CUSTKEY=o.O_CUSTKEY JOIN VS.LINEITEM l ON o.O_ORDERKEY=l.L_ORDERKEY;` | Query succeeds and returns the same count as the identical join over the source tables (single-node) |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E, Docker) | `make test-e2e` | 0 failures (fails, not skips, if no DB) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |

Implementing commit references `Closes #76` (the actual commit happens in `speq-implement-pr`, not here).
