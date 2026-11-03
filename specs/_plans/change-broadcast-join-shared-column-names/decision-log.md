# Decision Log: change-broadcast-join-shared-column-names

## Interview

Questions and answers are quoted verbatim, not summarized.

- Q: "Issue #573 lifts the shared-column-name guard (`disjoint_schema_guard`) for two-table joins. Should a self-join (the same table on both sides, for example `t a JOIN t b ON a.id = b.parent_id`) also take the broadcast path, or stay on the unified fallback as today?"
  Options offered: "Stay on fallback (Recommended)": "The guard is replaced by a per-table alias keyed on the involved table. A self-join has one involved table, so it needs per-occurrence aliasing. Keeping it on the fallback keeps this plan minimal. The issue says 'two tables'." / "Broadcast self-joins too": "Alias per FROM-tree occurrence, reusing the leg attribution in `pushdown-planning-join-fallback-self-join`. A larger plan that also touches that spec and its fact/dimension tie-break."
  → A (selected option): "Broadcast self-joins too"
- Q: "How should the worker name its output columns when both tables have a same-named column in the projection (e.g. `f.customer_id` and `c.customer_id`)? Exasol maps the worker's EMITS columns to the result by position, so the names are internal."
  Options offered: "Positional names (Recommended)": "Every projected column is emitted as `C0, C1, ...` by position and the EMITS list declares the same. One rule, no collisions possible, nothing to escape." / "Rename only on collision": "Keep the original name unless it appears twice, then suffix it with the side alias. Existing broadcast SQL is unchanged for disjoint joins, but there are two naming rules and a collision check."
  → A (free text): "could we have something more readable like `<alias>_<column>`?"
- Correction from the user, after the first draft applied `<alias>_<column>` to every column inside the scan and the adapter:
  → "this is not what I asked for, I proposed use `<alias>_<column>` as output column names to avoid colisions, but using this everywhere is not what I wanted, when the columns are handled internally they should be tuple with the alias name and the column name, or something like that"
- Q (from the user, after the revision introduced role aliases `F` and `D`): "\"Self-joins: the aliases stay F for the larger table and D for the smaller one\", `stay` means we had alias before there?"
  → Orchestrator's answer: no; on `main` the broadcast path renders bare, unaliased names, the fallback already aliases each leg as `LHS_T{leg}` (`joins/attribution.rs`), and `F`/`D` are new in this plan. Offered reusing `LHS_T{leg}` on the broadcast path.
  → User: "ok. This is not going to evolve well. We plan to support joining more than two tables, so F/D is not fine"
- Q (from the user, on "A column that can't be tied to exactly one table sends the query to the fallback."): "I guess this shall never happen, or?"
  → Orchestrator's answer: correct for any well-formed request. Exasol stamps `tableName` on every join column, and SQL forbids two occurrences with the same alias, so `NoLeg` and `Unattributable` are defensive. The fallback already raises a hard error for an unattributable column (`unattributable_decline`), so "drops to the fallback" really means "fails there". Proposed: the broadcast path raises the same error instead of declining, and said this would go into the fix pass unless the user preferred the fallback wording. The user did not object before the review finished.
- Q (from the user, on the review finding that a self-join is not always a byte tie): "Self-join tie: the spec shouldn't say that, because it depends on filter, the planning my yield differrent set of parquet files to scan per leg, right?"
  → Orchestrator's answer: yes. Each leg is resolved with its own leg-local WHERE conjuncts (`resolve_one_join_side`), so partition and min/max pruning can leave the legs with different byte totals, and either leg can become the dimension. The design already records the dimension leg; only the claim was wrong.
- Q (from the user, on the review advisory about `SELECT a.ID … ORDER BY b.ID`): "1. why should fall to the slow path?"
  → Orchestrator's answer: it is not a new fallback. On `main`, `place_broadcast_window` already declines an ordered window whose sort key is not projected, because the wrapper's ORDER BY binds only to emitted columns. With shared names, `bound_sort_key` compares bare names, so `b.ID` would wrongly match the projected `a.ID`. Verified live with `EXPLAIN VIRTUAL` on `MY_LAKEHOUSE_JOIN`: an unselected sort key, with or without `LIMIT`, takes the N-scan fallback (`LHS_T0`/`LHS_T1`), and Exasol pushes the key without adding it to the select list.
  → User: "the plan should make it clear that any column comparison check should be qualified, to avoid gaps like this, where it can confuse ID in `SELECT a.ID .... ORDER BY b.ID`  or other cases"
- Q (from the user, on the review advisory about self-join E2E coverage): "Elaborate on 3, in concise simple english with clear examples, what will happen in each case", then "what is the default schema and low-threshold schema?"
  → Orchestrator's answer: `VS_NAME` (`MY_LAKEHOUSE_JOIN`, default `JOIN_BROADCAST_MAX_BYTES`) broadcasts; `VS_NAME_LOW` (`MY_LAKEHOUSE_JOIN_LOW`, `JOIN_BROADCAST_MAX_BYTES = '1'`) always takes the fallback. Moving the plain and the unaliased-occurrence self-join tests to `VS_NAME_LOW` only would leave no E2E test of a column without `tableAlias` on the broadcast path. Proposed running all three self-join tests on both schemas.
  → User: "ok, fix it"
- Q (from the user): "now elaborate on 4"
  → Orchestrator's answer: `JoinLegs::qualify` silently passes a `NoLeg` column, so task 2.2 must detect it after `qualify` (any column node without `tableAlias` fails through `unattributable_decline`). The required `JoinSpec` alias fields break the `JoinSpec` literals in four scan test files that task 1.3 did not list (`object_store_tests.rs`, `raw_scan_tests.rs`, `spec_tests.rs`, `storage_ref_tests.rs`). The four adapter-side files the review named hold only Exasol pushdown requests, not join blocks, so they are unaffected.
  → User: "yes"
- Q (from the user): "elaborate on 2"
  → Orchestrator's answer: `EVENTS` and `LABELS` share only `id`, the join key, so query (a)'s `e.ID` and `l.ID` are equal on every row, and the self-join cannot catch a type taken from the wrong relation. No E2E test covers a shared non-key column: mixed-up output, wrong declared type, a filter on the wrong relation, or the unread shared column of the issue. Proposed adding a shared `score` column to `LABELS` with a different type and values, plus queries (d1) to (d3).
  → User: "yes, and in general we should aim at having test that excercise edge cases"

## Design Decisions

### [1] A column is a (relation alias, column) pair inside the worker; `<alias>_<column>` names only its output columns

- **Decision:** The scan's join SQL aliases its two derived relations, and each relation keeps its own column names. The adapter renders every column of the join condition, the filter, and the select list qualified with its relation alias (`"LHS_T1"."CUSTOMER_ID"`), and every check on the broadcast path that compares one column to another, or tests whether a column belongs to a set, compares the pair (relation alias, column), never the bare name: projection membership, sort-key binding, column-type lookup, binary-column detection, the output-name mapping, and the scan's per-relation lookup. Only the worker's output columns, the scan's select-list `AS` names and the `EMITS` list, join the pair into one name, `<alias>_<column>`. The scan spec carries a qualified column by widening `ProjectionItem::Column` and `SortKey.column` to a column reference with an optional relation alias. An unqualified reference keeps its bare-string encoding, so single-table specs serialize unchanged.
- **Rationale:** The user asked for exactly this split: "I proposed use `<alias>_<column>` as output column names to avoid colisions, but using this everywhere is not what I wanted, when the columns are handled internally they should be tuple with the alias name and the column name, or something like that". The user made the comparison rule explicit: "the plan should make it clear that any column comparison check should be qualified, to avoid gaps like this, where it can confuse ID in `SELECT a.ID .... ORDER BY b.ID`  or other cases". It is also the issue's proposal (#573): "Give each table its own alias in the worker SQL, and qualify every column with it", and "Look up column types by table and name, not by name alone". Output names stay inside the pushdown SQL, because Exasol binds the pushdown result to the select list by position: ADR `route-non-emittable-selectlist-to-qualified-wrapper` records Exasol's positional `04000` check, and `emits_ident` already emits `_LH_PROJ_{i}` for expression items. Task 3.1 confirms this against the live Exasol. Widening the two existing wire fields follows AGENTS.md ("Widen an existing field before adding one") and keeps `ScanSpec` format-neutral.
- **Alternatives:** Prefix every column with its alias (`<alias>_CUSTOMER_ID`) in the scan's relations, the condition, the filter, and every lookup. Rejected by the user in the words quoted above. A new `ProjectionItem` variant plus a new `SortKey` field. Rejected: AGENTS.md prefers widening an existing field.
- **Consequences:** The type-rewrite pipeline and `project_columns` read one column-type universe keyed by the pair; single-table and N-scan callers build it without relation aliases and keep today's name lookup. Every broadcast plan's `EMITS` names change to `<alias>_<column>`, so the broadcast golden strings change. An output name is the column name plus 7 characters (`LHS_T0_`) for a two-leg join, so a projected column name longer than 121 characters exceeds Exasol's 128-character identifier limit, and the adapter declines that plan to the fallback.
- **Promotes to ADR:** no

### [2] The broadcast path reuses the fallback's per-occurrence aliases, `LHS_T{leg}`, and the join block carries each relation's alias

- **Decision:** The adapter qualifies every column through `JoinLegs::qualify`, the same binding and the same `LHS_T{leg}` aliases the unified fallback renders. `JoinSides` records which leg `select_broadcast_sides` made the dimension. Each leg's byte total is taken after its own leg-local pruning, so a self-join's two occurrences can differ in size; on an exact byte tie the first leg in FROM-tree order is the dimension. `JoinSpec` gains two required fields, the fact relation's alias and the dimension relation's alias, and the scan aliases its two relations with them. The wire-format module `scan/spec.rs` owns the alias format as one function from leg index to alias, next to the output-name rule, and `JoinLegs::leg_alias` calls it. The scan's join execution reads the two aliases from the join block and formats none itself. A column that resolves to no single leg (`NoLeg` or `Unattributable`) fails the request through `unattributable_decline`, the fallback's own error, rather than declining to the fallback.
- **Rationale:** The user rejected role aliases: "ok. This is not going to evolve well. We plan to support joining more than two tables, so F/D is not fine", after accepting the offer to reuse `LHS_T{leg}` on the broadcast path ("ok"). It also matches the self-join option the user chose: "Alias per FROM-tree occurrence, reusing the leg attribution in `pushdown-planning-join-fallback-self-join`". No existing join-block field can say which relation is the fact side, so the join block carries both aliases as data. These parts carry over unchanged to a join over more legs: `JoinLegs::leg_alias`, the qualified column reference, the (relation alias, column) universe, and the scan's per-relation lookup. A multi-leg broadcast extends `JoinSpec`'s single dimension block (`table_root`, `files`, `logical_schema`, `storage`) to several dimension relations, and the dimension alias field moves with the other dimension fields.
- **Alternatives:** Role aliases, `F` for the fact relation and `D` for the dimension relation, fixed in the scan. Rejected by the user in the words quoted above, because they cannot name a third relation. Carrying the fact relation's alias in a new `CommonScanSpec` field. Rejected: it adds a field to every scan spec for a value only a join reads.
- **Promotes to ADR:** no

## Review Findings

### [1] [user-feedback] Column handling inside the worker

- **Finding:** The user rejected the first draft, which named every column `<alias>_<column>` inside the scan and the adapter: "using this everywhere is not what I wanted, when the columns are handled internally they should be tuple with the alias name and the column name".
- **Direction change:** Design Decision [1] now keeps (relation alias, column) pairs inside the worker and uses `<alias>_<column>` only for output column names. Decision [2] gives each FROM-tree occurrence its own alias.
- **Promotes to ADR:** no

### [2] [user-feedback] Role aliases `F` and `D`

- **Finding:** The user rejected the role aliases of the second draft: "ok. This is not going to evolve well. We plan to support joining more than two tables, so F/D is not fine".
- **Direction change:** Decision [2] now reuses the fallback's `LHS_T{leg}` aliases, and the join block carries the fact relation's and the dimension relation's alias as data.
- **Promotes to ADR:** no

### [3] [user-feedback] A column with no single leg fails the broadcast request

- **Finding:** The user asked whether the unattributable-column decline can happen at all: "I guess this shall never happen, or?" It cannot for a well-formed request, and the fallback already fails such a column with a hard error, so declining to the fallback only moved the failure.
- **Direction change:** The broadcast path now raises the fallback's error for a `NoLeg` or `Unattributable` column, reusing `unattributable_decline`. The NEW scenario step, Decision [2], task 2.2, and the unit test `column_without_a_single_leg_fails_broadcast_with_the_fallback_error` say so. The 128-character output-name decline is unchanged.
- **Promotes to ADR:** no

### [4] [user-feedback] A self-join is not always a byte tie

- **Finding:** The user corrected the claim that every self-join is a byte tie: "it depends on filter, the planning my yield differrent set of parquet files to scan per leg".
- **Direction change:** The NEW scenario step and Decision [2] now choose the dimension from each leg's byte total after its own pruning, with the FROM-tree tie-break only on an exact tie. Task 3.2 keeps the one-sided-filter self-join on the broadcast path and asserts that the pruned second occurrence is the dimension.
- **Promotes to ADR:** no

### [5] [user-feedback] Every column comparison is qualified

- **Finding:** `bound_sort_key` matches a sort key to the projection by bare name, so `SELECT a.ID … ORDER BY b.ID` would bind `b.ID` to the projected `a.ID`. The user: "the plan should make it clear that any column comparison check should be qualified, to avoid gaps like this, where it can confuse ID in `SELECT a.ID .... ORDER BY b.ID`  or other cases".
- **Direction change:** Decision [1] and the NEW scenario state one rule: every column comparison and set-membership test on the broadcast path compares the (relation alias, column) pair. Tasks 1.2, 2.1, and 2.2 inventory the bare-name comparisons reachable from the broadcast path with Serena `find_referencing_symbols` and convert each one, or record why it never sees a broadcast column. Task 2.3 adds one unit test per class of check.
- **Promotes to ADR:** no

### [6] [plan-review] One owner for the relation-alias format

- **Finding:** The output-name rule in `scan/spec.rs` is injective only because every alias has the `LHS_T{leg}` format, which `JoinLegs::leg_alias` in the adapter owned, and nothing enforced the agreement. The scan's empty-projection default listed the dimension relation first, while the adapter's full row lists the legs in leg order, and Exasol binds by position.
- **Direction change:** Decision [2] now puts the alias format in `scan/spec.rs` as one function from leg index to alias, next to the output-name rule, with the injectivity invariant in its doc comment, and `JoinLegs::leg_alias` calls it. The adapter already imports `scan::spec` and `scan/` does not import the adapter, so the dependency direction is unchanged. Task 1.2 drops the scan's empty-projection default for join specs, because `project_columns` always sends at least one projection item for a join.
- **Promotes to ADR:** no
