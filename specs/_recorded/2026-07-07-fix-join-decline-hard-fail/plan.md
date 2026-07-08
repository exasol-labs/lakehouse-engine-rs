# Plan: fix-join-decline-hard-fail

## Summary

Correct the join-pushdown fix in PR #78. The first cut fixed #76 (3+ table inner joins hard-failing) by adding a *second, additive* N-table join path alongside the existing two-table path. Code review found that fix treats a symptom, not the cause, and leaves three latent defects:

1. **Root cause is in `crates/vs-expression`, not join arity.** A grouped-aggregate select list over a join whose select item is a *scalar function wrapping aggregates* — e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)` — declines at ALL arities (single-table, two-table, N-table). `render_selectlist_item_qualified` (`pushdown.rs:4486`) only special-cases a *top-level* `function_aggregate`; anything else recurses into `vs_expression::render_expression_safe` → `render_expression_inner` (`vs-expression/src/lib.rs:100`), which has NO `function_aggregate` arm, so a nested `SUM`/`COUNT` hits the catch-all (`lib.rs:728`) → `Err` → swallowed to `None` → decline. The fix belongs at that shared seam.
2. **Two parallel join implementations, not one.** Two-table (`plan_eligible_join`/`build_unaccelerated_join_sql`/`build_two_scan_join_sql`, `LHS_FACT`/`LHS_DIM`) and N≥3 (`plan_multi_table_join`/`build_n_scan_join_sql`, `LHS_T0..`) render the fallback twice. The bug shipped precisely because the rendering gap was in both and the fix landed in one. Collapse to a SINGLE N≥2 fallback renderer (two-table = N=2); broadcast stays an optimization selected *within* that one path.
3. **The "Exasol will retry natively" fiction.** 15 constructed `UdfError::User` decline sites in `pushdown.rs` claim Exasol re-plans on an adapter error. It does not (ADR-083, ADR-085; the `exasol-udf-macros` FFI shim erases `UdfError::User` into a hard `F-UDF-CL-RUST-9001`). Purge the framing; where a genuine last-resort error remains, state plainly it is a hard error with no native retry.
4. **No E2E for the failing shape.** Add end-to-end coverage of the reported scalar-over-aggregate grouped join (with `SUM`, `SUM(CASE …)`, `AVG`, `ROUND(… SUM(…)/COUNT(*) …)`, HAVING, ORDER BY, LIMIT) at BOTH N=2 and N≥3, plus host unit tests for vs-expression aggregate rendering and for `render_selectlist_item_qualified` on a scalar-over-aggregate item.

This plan supersedes the additive-two-path design (ADR-094) with a single unified renderer and moves the correctness fix to its true seam.

## Design

### Context

The adapter advertises `JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` statically, once, at `getCapabilities` (`capabilities.rs:152`). There is no per-query opt-out: once a capability is advertised, Exasol pushes every inner equi-join of any arity and expects the adapter to serve it. There is no protocol response for "decline, run this natively" and no native re-plan on an adapter error — a declined pushdown is a hard client-facing SQL error. The governing principle, which the first cut violated and this plan adopts explicitly:

> **For each advertised capability the adapter MUST always be able to render what Exasol may push, or MUST NOT advertise it. "Decline at runtime and hope Exasol retries" is not a valid third option.**

The reported failure is not really about join arity. `render_selectlist_item_qualified` dispatches a top-level `function_aggregate` to `render_aggregate_qualified` (which splices the Exasol aggregate name verbatim and qualifies its column argument), and sends everything else to `render_expression_qualified` → `vs_expression`. `vs_expression` renders `function_scalar` (ROUND, arithmetic, CASE, …) by recursing into its arguments, but has no `function_aggregate` arm — so the moment recursion reaches a nested `SUM`/`COUNT`, it errors. Any select item that is a *scalar expression over aggregates* therefore declines, at every arity. Fixing the seam repairs single-table, two-table, and N-table simultaneously.

- **Goals**
  - `vs_expression` renders `function_aggregate` nodes (Exasol aggregate name spliced verbatim; argument(s) rendered by recursion; `COUNT(*)`/star and `DISTINCT` handled), so a scalar-function-wrapping-aggregates select item over a join renders instead of declining.
  - Top-level and nested aggregate rendering are made consistent on the shared `vs_expression` path (no divergence between `render_aggregate_qualified` and the recursive renderer).
  - ONE unaccelerated join renderer for all inner joins N≥2 (two-table = N=2); broadcast is an optimization chosen inside that single path, not a second implementation.
  - Every `UdfError::User` join/aggregate decline site states the truth: a hard error with no native retry, raised only when the adapter genuinely cannot render what it advertised.
  - E2E proof of the reported shape at N=2 and N≥3 (fails before, passes after), plus host unit tests at the seam.
- **Non-Goals**
  - No new join capabilities advertised (outer joins stay unadvertised; capability surface unchanged).
  - No node-local N-way DataFusion join / N-table broadcast (broadcast stays strictly two-table, per mission non-goal); the unified fallback makes all inner joins correct, only unaccelerated beyond broadcast.
  - No change to the single-table partial/merge aggregate decomposition paths (`pushdown-planning`, `-count-distinct`, `-expression-aggregate`, `-grouped-agg`): those detect a top-level `function_aggregate` *before* recursing and remain behavior-compatible. The new `vs_expression` arm only affects aggregates that appear *nested inside another expression*, which previously errored.
  - No unrelated lc-rs/perf content.

### Decision

Three coordinated changes, plus tests.

#### 1. Render aggregate nodes at the shared seam (root cause)

Add a `function_aggregate` arm to `render_expression_inner` (`vs-expression/src/lib.rs`):
- Splice the Exasol aggregate `name` verbatim (uppercased; it is not a translated function — `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and the STDDEV/VARIANCE family pass through as-is, matching the existing top-level `render_aggregate_qualified` discipline).
- `COUNT(*)` (empty `arguments` / star) renders as `COUNT(*)`.
- Render each argument by recursion (so `SUM(CASE WHEN … END)`, `SUM(a*b)`, `COUNT(expr)` all work).
- Honor `distinct: true` → `COUNT(DISTINCT <arg>)`.
- Column arguments carry a `tableAlias` (the ADR-085 alias annotation), so nested aggregate arguments qualify correctly over a join.

Then make `render_selectlist_item_qualified` consistent: a top-level `function_aggregate` and a nested one render through the SAME `vs_expression` aggregate arm. Keep `render_aggregate_qualified`'s observable output identical (it becomes a thin wrapper over — or is unified with — the recursive path) so the existing "Aggregate over a join routes through the qualified wrapper" behavior is byte-compatible for the shapes it already handled.

#### 2. One unified N≥2 unaccelerated join renderer

```
handle_pushdown
  └─ detect_join(from tree)
       ├─ NotAJoin                     → single-table path            (unchanged)
       ├─ Join(N≥2, all inner, equi)   → plan_join                     ★ UNIFIED
       │      ├─ N==2 AND broadcast-eligible AND no Exasol postprocessing
       │      │        → broadcast fan-out            (optimization, within the one path)
       │      └─ else  → build_n_scan_join_sql (N≥2)  (single fallback renderer)
       └─ non-inner node / non-equi / unbuildable → hard Err (no native retry)
```

`build_n_scan_join_sql` becomes the ONLY fallback renderer (`LHS_T0..LHS_T{N-1}`, cross-join + conjunctive table-qualified WHERE, per-side sharded fan-out, ADR-091 per-side predicate pushdown, ADR-085 qualified rendering). The two-table case is simply N=2. `build_unaccelerated_join_sql`/`build_two_scan_join_sql`/`resolve_join_sides` and the `LHS_FACT`/`LHS_DIM` two-scan renderer are removed; the `Eligible`/`MultiTable` `JoinShape` split collapses into one join shape carrying the N resolved tables + N-1 conditions, with broadcast eligibility computed as a property inside `plan_join`.

#### 3. Purge the retry fiction

Remove "Exasol will retry natively / retry the query natively" from all 15 sites (`pushdown.rs:2211, 2231, 2253, 2614, 4161, 4819, 4867, 4883, 5168, 5181, 5277, 5309, 5329, 5358, 5415`). Reword the genuine last-resort errors (non-inner join node in the tree, involved table absent from `TABLE_MAP`, involved table carrying no column metadata, a clause the translator cannot render) as plain hard errors with no retry. The sites that vanish because their shape now renders (the aggregate/expression declines fixed by change #1 and the fallback made total by change #2) are deleted, not reworded.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Render aggregate at the shared translator seam | `render_expression_inner` `function_aggregate` arm | One fix repairs every arity; nested-in-scalar aggregates were the only gap |
| Verbatim aggregate-name splice + recursive args | `vs_expression` aggregate arm | Matches existing `render_aggregate_qualified` behavior; keeps the top-level path byte-compatible |
| Single fallback renderer, broadcast as inner optimization | `plan_join` / `build_n_scan_join_sql` | One implementation cannot drift from the other; the shipped bug was a two-copies divergence |
| Cross-join + conjunctive qualified WHERE for N≥2 | `build_n_scan_join_sql` | Order-agnostic for all-inner joins; no ON-scope bookkeeping; Exasol optimizes equi-WHERE to hash joins |
| Advertised capability must render, or not be advertised | all decline sites | There is no native retry; a decline is a hard client error |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Fix aggregate rendering in `vs-expression`, not by arity | Special-case scalar-over-aggregate in `render_selectlist_item_qualified` only | The seam is shared by all arities and by any future caller; fixing only the join select-list path would leave the same gap for single-table nested aggregates |
| Collapse to one N≥2 renderer; broadcast stays an inner optimization | Keep the additive two-path design (ADR-094) | Two copies drifted and shipped the bug; a single renderer makes "two-table = N=2" structural, not a coincidence. Supersedes ADR-094 |
| Purge retry framing; hard error only when truly unrenderable | Keep "retry natively" wording | It is false and the codebase already says so (ADR-083/085); the FFI shim makes every decline a hard error |
| Broadcast stays strictly two-table | N-way node-local join | Out of scope (mission non-goal); the unified fallback already makes all inner joins correct |
| Existing two-scan tests (`has_two_scan_wrapper`, `LHS_FACT`/`LHS_DIM`) migrate to `LHS_T0`/`LHS_T1` | Keep the old alias names for N=2 | The unified renderer uses one alias scheme; N=2 output changes alias names only, result is identical |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-join | CHANGED | `vs-adapter/pushdown-planning-join/spec.md` |
| sql-comprehension/vs-expression-translator | CHANGED | `sql-comprehension/vs-expression-translator/spec.md` |

- **pushdown-planning-join** delta: CHANGED "A join outside the broadcast contract is declined safely" (single unified path; hard error with NO native retry); CHANGED "A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper" (reframed as the unified N≥2 renderer, N=2 included); CHANGED "Aggregate over a join routes through the qualified … wrapper" (unified wrapper + nested-aggregate rendering via `vs_expression`); NEW "A scalar function wrapping aggregates in a grouped join select list is rendered, not declined"; revised Background bullets for the unified renderer and the capability-must-render principle.
- **vs-expression-translator** delta: NEW "Aggregate function nodes render with the aggregate name spliced verbatim"; CHANGED Background to list `function_aggregate` among supported node types.

## Dependencies

None new. Reuses `resolve_one_join_side`, `build_side_fan_out_sql`, `build_scan_driving_sql`, `annotate_columns_with_alias`, `render_expression_qualified`, `render_df_filter_qualified`, `qualified_join_select_items`/`_group_by`/`_having`/`_order_by`, `side_local_filter`, `referenced_side_columns`, `involved_table_columns`, `empty_result_sql`, and the ADR-091 per-side predicate attribution.

## Implementation Tasks

1. Root-cause fix — aggregate nodes in `crates/vs-expression`
   - [ ] 1.1 Add a `"function_aggregate"` arm to `render_expression_inner` (`vs-expression/src/lib.rs`, near the `function_scalar` arm at :346): splice `name` verbatim (uppercased), handle empty-args / star as `COUNT(*)`, render each argument recursively, and honor `distinct: true` → `COUNT(DISTINCT <arg>)`; render column-node `tableAlias` qualification for arguments. Remove the fall-through-to-catch-all for aggregate nodes (`lib.rs:728`). [expert]
   - [ ] 1.2 Unify `render_selectlist_item_qualified` (`pushdown.rs:4486`) and `render_aggregate_qualified` (`pushdown.rs:4469`) onto the new `vs_expression` aggregate path so a top-level aggregate and a nested one render identically; assert the top-level output is byte-compatible with the pre-change `render_aggregate_qualified` for the shapes it already handled. [expert]
2. Single unified N≥2 join renderer
   - [ ] 2.1 Collapse the `JoinShape` variants: fold `Eligible` (2-table) and `MultiTable` (N≥3) into one shape carrying the N resolved involved tables (Exasol name + original-cased `TABLE_MAP` Iceberg ident) and the N-1 join conditions; `detect_join` asserts every join node is `join_type = "inner"` and equi, over N≥2 base-table leaves. [expert]
   - [ ] 2.2 Route `handle_pushdown` through a single `plan_join`; compute broadcast eligibility (N==2, small side ≤ `JOIN_BROADCAST_MAX_BYTES`, no Exasol postprocessing) as a property *inside* `plan_join`; on eligibility take the broadcast fan-out, otherwise call `build_n_scan_join_sql`.
   - [ ] 2.3 Make `build_n_scan_join_sql` the sole fallback renderer for N≥2 (`LHS_T0..LHS_T{N-1}`), resolving each side once (ADR-091 per-side predicate pushdown), emitting the shape-correct empty result when any side has zero files, and rendering the whole select/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT table-qualified. Remove `build_unaccelerated_join_sql`, `build_two_scan_join_sql`, `resolve_join_sides`, and the `LHS_FACT`/`LHS_DIM` scheme. [expert]
3. Purge the "Exasol will retry natively" fiction
   - [ ] 3.1 Delete the retry framing from all 15 sites (`pushdown.rs:2211, 2231, 2253, 2614, 4161, 4819, 4867, 4883, 5168, 5181, 5277, 5309, 5329, 5358, 5415`). Reword the genuine last-resort errors (non-inner join node, table absent from `TABLE_MAP`, table with no column metadata, unrenderable clause) as hard errors with no native retry; delete the sites whose shape now always renders after tasks 1–2.
   - [ ] 3.2 Update the unit test asserting `msg.contains("retry")` (`pushdown.rs:7936`) to assert the corrected hard-error wording; retire the `TooManyTables` decline facet / any dead reason path no longer produced.
4. Host unit tests (`crates/vs-expression/src/lib.rs` + `crates/lakehouse-engine/src/adapter/pushdown.rs` `#[cfg(test)]`)
   - [ ] 4.1 `vs-expression`: `render_expression` over `SUM(col)`, `COUNT(*)`, `COUNT(DISTINCT col)`, `AVG(col)`, and a scalar-wrapping-aggregate `ROUND(100.0 * SUM(CASE WHEN … END) / COUNT(*), 2)` returns the expected SQL (aggregate name verbatim, args recursed) instead of `None`/`Err`.
   - [ ] 4.2 `pushdown`: `render_selectlist_item_qualified` on a scalar-over-aggregate item over a join renders table-qualified SQL (not `None`); a top-level bare aggregate still renders byte-identically to the pre-change output.
   - [ ] 4.3 `pushdown`: `detect_join` over a 2-, 3-, and 4-table all-inner tree yields the unified join shape with the right table/condition counts; a non-inner node → hard `Err` (reworded, no "retry"); a leaf missing from `TABLE_MAP` → hard `Err`. `build_n_scan_join_sql` for N=2/3/4 emits `LHS_T*` aliases with all conditions qualified; a shared-column-name triple renders qualified.
5. E2E (`crates/lakehouse-engine/tests/e2e_join_test.rs`, local Exasol Docker)
   - [ ] 5.1 Extend the join E2E seed (`tests/common/seed.rs`) so the scalar-over-aggregate shape can run at N=2 and N≥3 (reuse/extend the existing three/four-table fixtures with the columns the reported query needs, e.g. an `l_returnflag`-like discriminator).
   - [ ] 5.2 Add `e2e_scalar_over_aggregate_grouped_join_result_correct` at N=2 and `e2e_scalar_over_aggregate_grouped_join_n_table_result_correct` at N≥3: run a grouped join with `SUM(expr)`, `SUM(CASE …)`, `AVG`, `ROUND(100.0 * SUM(…) / COUNT(*), 2)`, plus HAVING, ORDER BY, LIMIT; assert the query SUCCEEDS (no `F-UDF-CL-RUST-9001`), the pushed SQL is the unified N-scan wrapper, and the result equals the same query on a single node. Must fail before the fix, pass after.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 (vs-expression aggregate arm), 4.1 (its unit tests) |
| Group B | 1.2 (seam unification), 4.2 (seam tests) |
| Group C | 2.1, 2.2, 2.3 (unified join renderer) |
| Group D | 3.1, 3.2 (purge retry) — after 2.x settles the surviving error sites |
| Group E | 4.3 (detection/builder host tests), 5.1 (seed) |
| Group F | 5.2 (E2E behavior) |

Sequential dependencies:
- Group A → Group B (the seam unification consumes the new aggregate arm)
- Group B, Group C → Group D (retry-site purge follows once the renderer/seam settle which errors survive)
- Group C → Group E → Group F (E2E needs the unified path + seeded tables)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `build_unaccelerated_join_sql`, `build_two_scan_join_sql`, `resolve_join_sides` (`pushdown.rs`) | Replaced by the single `build_n_scan_join_sql` (N≥2); the two-table fallback is now N=2 |
| Enum variant | `JoinShape::Eligible` / `JoinShape::MultiTable` split; `IneligibleJoinReason::TooManyTables` | Folded into one join shape; `TooManyTables` no longer a decline reason |
| Alias scheme | `LHS_FACT` / `LHS_DIM` two-scan rendering | Unified on `LHS_T0..LHS_T{N-1}` |
| Error framing | "Exasol will retry natively" at 15 sites | False; there is no native retry |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Aggregate function nodes render with the aggregate name spliced verbatim (NEW, vs-expression) | Unit | `crates/vs-expression/src/lib.rs` | `render_expression_renders_aggregate_nodes` |
| A scalar function wrapping aggregates in a grouped join select list is rendered, not declined (NEW) — seam | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `render_selectlist_item_qualified_renders_scalar_over_aggregate` |
| A scalar function wrapping aggregates in a grouped join select list is rendered, not declined (NEW) — runtime N=2 | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_scalar_over_aggregate_grouped_join_result_correct` |
| A scalar function wrapping aggregates in a grouped join select list is rendered, not declined (NEW) — runtime N≥3 | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_scalar_over_aggregate_grouped_join_n_table_result_correct` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (CHANGED — unified N≥2) — detection | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `detect_join_unifies_two_and_multi_table` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper (CHANGED — unified N≥2) — SQL shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `build_n_scan_join_sql_renders_qualified_wrapper` |
| A join outside the broadcast contract is declined safely (CHANGED — no native retry) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `join_outside_contract_declined_safely` |
| Aggregate over a join routes through the qualified wrapper (CHANGED — unified + nested aggregate) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_aggregate_over_join_result_correct` |

Existing scenarios (broadcast, threshold, projection/EMITS, condition rendering, shared-column, capabilities; and every single-table aggregate-pushdown scenario in `pushdown-planning`, `-count-distinct`, `-expression-aggregate`, `-grouped-agg`, `-nested-aggregate-fallback`) keep their current passing tests — this plan must not regress them. The two-scan alias assertions migrate from `LHS_FACT`/`LHS_DIM` to `LHS_T0`/`LHS_T1` with identical results.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-join | `SELECT l_returnflag, SUM(l_quantity), SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END), AVG(l_extendedprice), ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2) FROM VS.CUSTOMER c JOIN VS.ORDERS o ON c.C_CUSTKEY=o.O_CUSTKEY JOIN VS.LINEITEM l ON o.O_ORDERKEY=l.L_ORDERKEY GROUP BY l_returnflag HAVING COUNT(*) > 0 ORDER BY 1 LIMIT 10;` | Query SUCCEEDS (no `F-UDF-CL-RUST-9001`); result equals the same query over the source tables single-node |
| pushdown-planning-join | Same query with only `VS.ORDERS o JOIN VS.LINEITEM l` (N=2) | SUCCEEDS; unified N-scan wrapper (`LHS_T0`/`LHS_T1`); result matches single-node |
| vs-expression | (unit) render `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)` | A DataFusion SQL fragment with `SUM(...)`, `COUNT(*)` spliced verbatim; no `None`/`Err` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E, Docker) | `make test-e2e` | 0 failures (fails, not skips, if no DB) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |

Implementing commit references `Closes #76` (the actual commit happens in `speq-implement-pr`, not here).
