# Plan: fix-join-fallback-self-join-attribution

## Summary

Fix issue #361 by attributing every pushdown column reference to a JOIN LEG — one occurrence of a table in the FROM tree — instead of to a bare `tableName`, which collides whenever a table is joined to itself. One binding, derived from the FROM-tree leaf aliases the join walk currently discards, replaces four independent `tableName`-keyed re-derivations that each produced a distinct wrong-results defect.

## Design

### Context

A self-join returns a cross product. Captured live from `EXPLAIN VIRTUAL`'s `PUSHDOWN_JSON` column against the Docker container, `SELECT a.O_ORDERKEY, a.O_CUSTKEY FROM FACT_ORDERS a JOIN FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY` over a 10-row table returned 100 rows and generated:

```sql
SELECT "LHS_T1"."O_ORDERKEY", "LHS_T1"."O_CUSTKEY"
FROM (…) AS "LHS_T0"
INNER JOIN (…) AS "LHS_T1"
  ON (("LHS_T1"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY"))
```

The `ON` is a tautology, `LHS_T0` is unconstrained, and the select list is wrongly all-`LHS_T1`. The three-leg shape returned 1000 rows instead of 10, rendering `ON 1=1` at one join point plus the same tautology twice at the next.

The root cause is one decision taken in the wrong currency. `tableName` names a TABLE; a wrapper leg is an OCCURRENCE of a table. The two coincide only while no table appears twice. Every attribution site keyed on `tableName`, so a self-join's two legs became one map entry under last-write-wins.

Exasol already supplies the per-occurrence signal, and the join walk throws it away. A FROM-tree leaf carries its SQL alias under `alias`; a `column` node carries the same alias under `tableAlias`. `collect_join_tree` reads only the leaf's `name`, and `JoinLeaf` has no field to hold the alias — so identity is lost at COLLECTION and then reconstructed from `tableName` at RENDER.

- **Goals** — one owner for column-to-leg attribution; exact per-occurrence resolution from the leaf alias; byte-identical SQL for every request in which no table occurs twice; a loud failure, never a silent wrong answer, for a reference no leg key matches.
- **Non-Goals** — accelerating self-joins (they stay on the unaccelerated fallback); extending the broadcast contract to same-table joins; changing which requests reach the wrapper; touching the N = 1 qualified wrapper's deliberate name collapse; re-keying the refused-column check, whose name-keyed over-charging is already the fail-safe direction.

### Decision

Introduce `JoinLegs`, the single resolver of column-to-leg attribution, in a new `joins/attribution.rs`. It is derived from the detected join's leaves and is the only thing any call site consults for leg identity.

#### Architecture

```
pushdown request (raw, tableAlias intact)
        │
        ▼
collect_join_tree ──► JoinLeaf { table_name, table_alias, table_identifier }   (leaf `alias` RETAINED)
        │
        ▼
DetectedJoin ──► legs() ──► JoinLegs          ◄── the ONE owner of leg identity
                               │
        ┌──────────────────────┼──────────────────────┬─────────────────────────┐
        ▼                      ▼                      ▼                         ▼
 qualify(expr)         conjunct_single_leg     legs_referenced(expr)     leg_columns(...)
 (render sites)        (leg-local WHERE)       (FROM-chain attach)       (leg projection)
```

`JoinLegs` resolves a `column` node by matching the pair (`tableName`, `tableAlias`) against the leaves' (`name`, `alias`) pairs, comparing the alias verbatim. Where a `tableName` names exactly ONE leg it resolves by name alone and never consults an alias — which is what keeps the common unaliased join byte-identical, since Exasol stamps no `tableAlias` at all on an unaliased FROM clause.

The pair is injective by SQL's own rules: two occurrences of one table cannot share an alias (`FROM T a JOIN T a` is illegal) and at most one occurrence can be alias-less (`FROM T JOIN T` is an ambiguous reference and is rejected). No alias sorting, occurrence counting, or positional guess is needed. An absent alias is therefore part of the key — one leg of a genuine self-join legitimately carries none (`FROM T JOIN T b`, captured live, also returned 100 rows instead of 10).

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Deep module, one owner | `JoinLegs` in `joins/attribution.rs` | Absorbs the whole attribution decision — alias matching, single-leg name fallback, ambiguity detection — behind four methods. Four call sites stop re-deriving identity and become callers of one answer |
| Identity captured at its source | `collect_join_tree` retains the leaf `alias` | The signal exists exactly once, in the FROM tree. Reading it where the tree is walked removes the need to reconstruct it downstream |
| Reachable only through its owner | `DetectedJoin::legs()` | No caller can build a binding from a different request, and none can invent its own |
| Fail closed | unattributable reference returns the wrapper's existing hard error | Wrong rows are the failure mode being removed; an arbitrary leg choice would reintroduce it silently |
| Redundant guard refused | self-join broadcast ineligibility stays owned by `disjoint_schema_guard` | A same-table guard would duplicate a decision that already has an owner. Pinned by a test instead |

#### Quick Diagnostic

| Question | Answer |
|----------|--------|
| One-sentence responsibility? | "Which join leg does this column reference belong to." |
| Easier to call than to reimplement? | Yes — the alternative is what exists today: four partial reimplementations, three of them wrong |
| Would internals leak outward? | No — callers pass a `column` node or an expression tree and receive a leg index. Alias matching, the single-leg fallback, and ambiguity never appear outside the module |
| Doc comment explains reasoning? | Yes — it states why `tableName` is the wrong currency and why the pair is injective |
| Exactly one owner per decision? | Yes, after this change. That is the change |
| Boundary visible without reading internals? | Yes — `planning`, `rendering`, and `sql_builders` all depend on `attribution`; it depends on neither |
| Tactical shortcut with a follow-up? | None taken |
| Business logic depends only inward? | Yes — `attribution` reads `serde_json` values and leaf records, no Exasol runtime, no catalog, no DataFusion |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Resolve legs from the FROM-tree leaf `alias` | Reconstruct the alias-to-leg mapping by collecting distinct `tableAlias` values from column nodes and binding them to leg positions in a deterministic order | The leaf `alias` was confirmed present live. Reconstruction needs an arbitrary bijection and a rule for alias-count mismatches; reading the leaf makes the mapping exact and removes both |
| `(tableName, alias)` as the leg key, absent alias included | Key on alias alone; key on leg ordinal derived from first appearance | A genuine self-join can leave one occurrence unaliased (captured live), so alias alone is not total. `tableName` alone is what broke. The pair is injective by SQL's rules |
| One `JoinLegs` owner threaded through all four sites | Fix only the render site named in the issue | The other three are the same defect. The side-local filter site silently over-filters a self-join leg — wrong rows with no error, worse than a visible cross product. The FROM-chain site misplaces conditions at N ≥ 3 |
| Single-leg `tableName` resolves without consulting an alias | Always require an alias match | Exasol stamps no `tableAlias` when the user writes none. Requiring one would break every unaliased join — the common, currently-correct case |
| Hard client-facing error for an unattributable reference | Fall back to bare unqualified rendering; fall back to leg 0 | Bare rendering is ambiguous across the wrapper's subqueries; leg 0 is an arbitrary choice that returns wrong rows. The state is unreachable for a well-formed request and is pinned by a unit test |
| No same-table broadcast guard | Add an explicit "reject a repeated table" gate before broadcast | `disjoint_schema_guard` already declines it: a self-join declares an identical column set. A second guard would give one decision two owners |
| Refused-column check stays name-keyed | Re-key it to legs alongside the rest | Over-charging every matching side is the fail-safe direction for a refusal, and the function already charges untagged columns to every side. Narrowing it would admit a query that reads a refused column |
| Iceberg spec check recorded as non-applicable | Quote a normative section anyway | The change reads no manifest, snapshot, field id, or type mapping and behaves identically on Iceberg and Delta. There is no section to quote and no deviation to track — stated explicitly rather than skipped silently |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `vs-adapter/pushdown-planning-join-fallback` | CHANGED | `specs/_plans/fix-join-fallback-self-join-attribution/vs-adapter/pushdown-planning-join-fallback/spec.md` |

## Impact

Self-joins return correct results instead of a cross product. A query of the shape `FROM T a JOIN T b ON a.col = b.col` that previously returned every row combination now returns only matching rows, and a self-join carrying a WHERE conjunct against one alias stops having that conjunct applied to both legs. Row counts and result sets change for these queries — this is the fix, not a regression, but any downstream consumer that recorded the inflated output will see it shrink. Also breaking, and deliberately: a column reference the leg binding cannot attribute now returns a hard client-facing error where it previously rendered a wrong reference; the state is unreachable for a well-formed request. Requests in which no table occurs twice emit byte-identical SQL and are unaffected. No configuration, DDL, or wire-format change; no redeploy beyond the usual `.so` update. Version bump: PATCH (`fix`).

## Requirements

| Requirement | Details |
|-------------|---------|
| Reproduce before fixing | Both issue #361 shapes MUST be reproduced against the Docker Exasol container before any production edit, per the project's verification discipline |
| No new capability advertised | The adapter's capabilities response is unchanged; this fixes rendering of an already-advertised join |
| Byte-identical for unaffected shapes | Every request in which no table occurs twice MUST emit the SQL it emitted before this change |
| Issue linkage | The implementing commit MUST read `Closes #361` |

## Dependencies

Running Docker stack (`exasol`, `minio`, `iceberg-rest`) for tasks 1, 5, and 6. No new crates, no SDK or SLC version change.

## Implementation Tasks

- [ ] 1.1 Reproduce both issue #361 shapes live and close the remaining evidence gaps in the leg-alias signal. Capture the request JSON via `exapump sql -p docker -f csv "EXPLAIN VIRTUAL <query>"` and read element `[2].pushdownRequest` of the `PUSHDOWN_JSON` array (`-f json` emits invalid JSON for these payloads — use CSV). Record, in `decision-log.md`, the observed `alias` / `tableAlias` presence for: a quoted mixed-case alias (`FROM T "myAlias"`), a four-leg join, a right-deep or parenthesized nesting (`A JOIN (B JOIN C)`), a self-join carrying a WHERE filter, and a self-join over a Delta/Unity virtual schema. **Acceptance:** the two #361 repros are confirmed failing with their row counts; each gap above is answered OBSERVED, never inferred. **If any observation contradicts the premise that a FROM-tree leaf carries its occurrence's alias, HALT and escalate — the whole design rests on it.**
- [ ] 2.1 Add `table_alias: Option<String>` to `JoinLeaf` and retain each FROM-tree leaf's `alias` in `collect_join_tree`, leaving every other collection behavior unchanged. **Acceptance:** a unit test asserts the collected leaves of a two-leg self-join, a mixed aliased/unaliased self-join, a three-leg left-deep self-join, and an unaliased two-table join carry exactly the aliases the live captures show. [expert]
- [ ] 2.2 Add `crates/lakehouse-engine/src/adapter/pushdown/joins/attribution.rs` with `JoinLegs` and its sibling `attribution_tests.rs`. Constructors: from the detected join's leaves, and a single-scan form mapping every involved table name onto leg 0 for the N = 1 qualified wrapper. Methods: resolve one `column` node to a leg, resolve the legs an expression tree references (with untagged and no-column flags), the single leg a conjunct is local to, deep-clone an expression tagging each column with its leg's `LHS_T{i}` alias, and the subquery alias of a leg. **Acceptance:** unit tests cover exact-pair match, absent-alias-as-key, single-leg resolution without an alias, a column whose `tableName` matches no leg left unqualified, and an unattributable reference reported as such rather than resolved. Resolution is a pure function of the leaves and the node — no request-global state. [expert]
- [ ] 3.1 Thread `JoinLegs` through every attribution call site in ONE atomic change, deleting the `tableName`-keyed derivations: `build_n_scan_alias_map`, `annotate_columns_with_alias`, `column_tables`, `conjunct_single_side`, and `collect_side_column_names`. Convert `render_expression_qualified`, `render_df_filter_qualified`, `render_self_applied_where`, `n_scan_join_select_items`, `qualified_join_group_by`, `qualified_join_having`, `qualified_join_order_by`, `outer_wrapper_clauses`, `build_n_scan_join_from`, and `referenced_side_columns` to take the binding; replace `side_local_filter(filter, table_name)` with a leg-indexed form; drive `plan_join`'s per-leg resolve loop by leg index so each leg receives only its own leg-local pruning predicate; and build the single-scan binding inside `build_qualified_single_table_fallback_sql`. An unattributable reference returns the wrapper's existing hard client-facing error naming the column and its table. **Acceptance:** `cargo test` green; no call site outside `attribution.rs` reads `tableName` to decide leg identity; the N = 1 wrapper and the refused-column check behave exactly as before. [expert]
- [ ] 4.1 Unit-test all three fixed call sites and the shapes they broke: the rendered `ON` of a two-leg self-join compares two distinct aliases; its select list qualifies each item by its own occurrence; a mixed aliased/unaliased self-join resolves to two legs; a three-leg self-join attaches its two conditions at two different join points with no `ON 1=1` and no duplicated condition; and a WHERE conjunct local to one occurrence reaches only that leg's `ScanSpec.filter` and only that leg's manifest-pruning predicate.
- [ ] 4.2 Correct the tests and doc comments that recorded the collapse as intended. `seam_trailing` in `sql_builders_tests.rs` hardcodes a one-entry alias map and states "A self-join's alias map collapses to ONE entry … so this is exactly what the seam sees" — replace that fixture and comment with a real two-leg binding, and give the self-join capture rows 9-11 the per-occurrence `tableAlias` production sees. **Acceptance:** no test or doc comment in the joins module asserts or explains the collapse as correct behavior.
- [ ] 4.3 Pin the two properties the fix must not lose: every request in which no table occurs twice emits SQL byte-identical to its pre-change output (assert over the existing golden-SQL fixtures), and a two-leg self-join never takes the broadcast path — declined by `disjoint_schema_guard`, with no same-table guard added.
- [ ] 5.1 Add self-join regression tests to `crates/lakehouse-engine/tests/e2e_join_test.rs` reproducing issue #361 permanently: a two-leg self-join on a primitive column, a mixed aliased/unaliased self-join, a three-leg self-join, and a self-join carrying a WHERE conjunct against one alias. **Acceptance:** each asserts the exact expected row multiset against a single-node evaluation of the same query — never merely a row count — and would have failed before task 3.
- [ ] 6.1 Add a self-join test on a nested JSON-rendered column to `crates/lakehouse-engine/tests/e2e_complex_type_test.rs`, matching the issue's second repro (`FROM complex_probe a JOIN complex_probe b ON a.TAGS = b.TAGS`). **Acceptance:** asserts the exact expected pairs, so the NULL-`TAGS` row correctly matches nothing; would have failed before task 3.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B | 2.1, 2.2 |
| Group C | 3.1 |
| Group D | 4.1, 4.2, 4.3 |
| Group E | 5.1, 6.1 |

Sequential dependencies:
- Group A → Group B (the design premise is confirmed before any production edit)
- Group B → Group C (the binding exists before call sites take it)
- Group C → Group D and Group E (signatures settle before tests are written against them)

Within Group B, 2.1 and 2.2 touch different files and run concurrently; 2.2 consumes 2.1's field, so 2.2 must land after 2.1's type change compiles. Within Group D, 4.1, 4.2, and 4.3 all edit files under `crates/lakehouse-engine/src/adapter/pushdown/joins/` and MUST run sequentially in one agent to avoid clobbering each other in the shared tree. Group E's two tasks touch separate test files and run concurrently.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `build_n_scan_alias_map` (`joins/sql_builders.rs`) | The `tableName`-keyed alias map is what collapsed; `JoinLegs` replaces it |
| Function | `annotate_columns_with_alias` (`joins/rendering.rs`) | Absorbed into `JoinLegs`' qualifying clone |
| Function | `column_tables` (`joins/rendering.rs`) | Absorbed into `JoinLegs`' referenced-legs query |
| Function | `conjunct_single_side` (`joins/rendering.rs`) | Absorbed into `JoinLegs`' single-leg query |
| Function | `collect_side_column_names` (`joins/rendering.rs`) | Replaced by the leg-keyed narrowing inside `JoinLegs` |
| Test fixture | `seam_trailing`'s one-entry alias map (`joins/sql_builders_tests.rs`) | Encodes the collapsed map as the seam's real input |
| Doc comment | `seam_trailing`'s "A self-join's alias map collapses to ONE entry" (`joins/sql_builders_tests.rs`) | States the defect as intended behavior |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A table joined to itself renders each occurrence as its own leg | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `self_join_renders_each_occurrence_as_its_own_leg` |
| A table joined to itself renders each occurrence as its own leg | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_self_join_on_primitive_column_matches_single_node` |
| A table joined to itself renders each occurrence as its own leg | Integration | `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` | `e2e_self_join_on_nested_json_column_matches_single_node` |
| One occurrence of a self-joined table carries no alias | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/attribution_tests.rs` | `absent_alias_is_a_distinct_leg_key` |
| One occurrence of a self-joined table carries no alias | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_self_join_with_one_unaliased_occurrence_matches_single_node` |
| A three-leg self-join attaches each condition to its own leg pair | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `three_leg_self_join_attaches_each_condition_at_its_own_join_point` |
| A three-leg self-join attaches each condition to its own leg pair | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_three_leg_self_join_matches_single_node` |
| A WHERE conjunct local to one occurrence is pushed into only that occurrence's leg | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering_tests.rs` | `leg_local_conjunct_reaches_only_its_own_occurrence_leg` |
| A WHERE conjunct local to one occurrence is pushed into only that occurrence's leg | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_self_join_with_one_sided_filter_matches_single_node` |
| A column reference no leg key matches fails loudly | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `unattributable_column_reference_is_a_hard_error_naming_the_column` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `n_scan_wrapper_qualifies_every_clause_by_leg` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_three_table_join_result_correct` (existing, must stay green) |
| Join conditions attach greedily by LEG set and leg-local filters push into each leg | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `conditions_attach_by_leg_set_and_leg_local_filters_partition_exactly` |
| Join conditions attach greedily by LEG set and leg-local filters push into each leg | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `golden_n_scan_join_sql_unchanged` |
| Shared-column-name join uses qualified rendering, not bare-name broadcast rendering | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs` | `self_join_is_never_broadcast_eligible` |
| Shared-column-name join uses qualified rendering, not bare-name broadcast rendering | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_above_threshold_result_matches_broadcast` (existing, must stay green) |

Unit tests here cover pure SQL generation from a request JSON — a total function of its inputs with no I/O. Every one is paired with an integration test asserting the result the generated SQL actually produces, except the unattributable-reference scenario, whose state no live query can reach.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `pushdown-planning-join-fallback` | `exapump sql -p docker "SELECT COUNT(*) FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN MY_LAKEHOUSE.FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY"` | `10`, not the pre-fix `100` |
| `pushdown-planning-join-fallback` | `exapump sql -p docker -f csv "EXPLAIN VIRTUAL SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN MY_LAKEHOUSE.FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY"` | The generated SQL's `ON` reads `("LHS_T0"."O_ORDERKEY" = "LHS_T1"."O_ORDERKEY")` — two distinct aliases, no tautology |
| `pushdown-planning-join-fallback` | `exapump sql -p docker "SELECT COUNT(*) FROM MY_LAKEHOUSE.FACT_ORDERS a JOIN MY_LAKEHOUSE.FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY JOIN MY_LAKEHOUSE.FACT_ORDERS c ON b.O_ORDERKEY = c.O_ORDERKEY"` | `10`, not the pre-fix `1000` |
| `pushdown-planning-join-fallback` | `exapump sql -p docker "SELECT COUNT(*) FROM MY_LAKEHOUSE.FACT_ORDERS o JOIN MY_LAKEHOUSE.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY"` | `10` — the unaffected two-table join is unchanged |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e > /tmp/e2e.log 2>&1; echo "rc=$?"` then read the file | `rc=0`, 0 failures. Do not judge the run from a piped `tail` — capture the exit code and read the log |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
