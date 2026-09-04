# Feature: Pushdown Planning — Single-Group Scalar-Over-Aggregate Select Items

Extends `vs-adapter/pushdown-planning-single-group-agg` so an ungrouped `selectList` item
that is a scalar function wrapping one or more aggregates — `ROUND(SUM(L_QUANTITY), 2)`,
`ROUND(VARIANCE(C_ACCTBAL), 4)` — is decomposed into the same per-shard partial /
outer-merge plan a bare aggregate already gets, and so a nested aggregate the merge cannot
decompose routes to the qualified single-table wrapper instead of being evaluated per
shard. Closes issue
[#194](https://github.com/exasol-labs/lakehouse-engine-rs/issues/194) (unmerged
per-shard partial rows returned as the answer) and issue
[#188](https://github.com/exasol-labs/lakehouse-engine-rs/issues/188) (a scalar-wrapped
`VARIANCE` reaching DataFusion under a name DataFusion does not define). Mirrors the split
`vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate` made against
`vs-adapter/pushdown-planning-grouped-agg`: the base feature keeps capability
advertisement, bare-aggregate detection, the AVG sum/count pair, the empty-`projection`
contract, the `COUNT(DISTINCT)` branch, and the no-`OFFSET` merge; this feature owns only
the scalar-wrapper shape.

## Background

* **The defect is a silent wrong answer, verified live.** `SELECT ROUND(SUM(l_quantity), 2)
  FROM LINEITEM` returns one row (`25304.00`) against a native Exasol schema and FOUR rows
  (`7477.00`, `7033.00`, `8018.00`, `2776.00` — summing to `25304.00`) through the
  lakehouse virtual schema, one per shard, with no error raised. `EXPLAIN VIRTUAL` shows
  why: the broken query carries `"projection":[{"expr":"round(SUM(\"L_QUANTITY\"), 2)"}]`
  with no `"aggregates"` and no merge, while the working bare `SUM(l_quantity)` carries
  `"aggregates":[{"kind":"sum","column":"L_QUANTITY"}]` and a per-shard PARTIAL merged by
  an outer wrapper.
* **The defect does NOT live in aggregate classification.** `detect_aggregates`
  (`adapter/pushdown/single_group_agg.rs`) requires every select-list item to be literally
  `"function_aggregate"`; `ROUND(…)` is `"function_scalar"`, so single-group detection
  ALREADY declines it today and `classify_request_shape` ALREADY yields
  `RequestShape::RowScan`. `parse_agg_item` — which issue #194's own "Decided approach"
  names — is never reached, so declining there would change nothing. The defect lives one
  layer down, in the projection builder: `project_columns`'s `function_scalar`-family arm
  calls `render_expression_safe`, whose `function_aggregate` arm renders a nested aggregate
  verbatim as SQL text (deliberately — that arm is what the grouped merge-substitution
  trick depends on). The item therefore renders SUCCESSFULLY as a `ProjectionItem::Expr`,
  the widening signal stays `false`, and `build_dispatch_sql`'s `RowScan` arm feeds the
  expression into the per-shard `EMITS` clause. Each shard's DataFusion then computes the
  whole aggregate over its own files.
* **`function_aggregate` is excluded from the pushable projection set only at the TOP
  level.** `vs-adapter/pushdown-planning-selectlist-expressions` already requires that "the
  pushable node-type set SHALL be exactly the set the translator renders MINUS
  `function_aggregate` — so an aggregate select item reaches the aggregate planner or the
  wrapper rather than being evaluated per shard as a projection item". A top-level
  aggregate satisfies that rule through the projection builder's unknown-node arm. A
  NESTED one does not, and this feature's companion delta to that feature extends the rule
  to every depth.
* **This feature is layered: a correctness floor plus a performance path.** The floor is
  the depth-insensitive projection guard (delta to
  `vs-adapter/pushdown-planning-selectlist-expressions`): any select-list item whose
  subtree contains a `function_aggregate` widens the derived projection, so it can never be
  evaluated per shard, on EVERY path that consumes that projection — single-table row scan,
  broadcast join, and empty result. The performance path is the decomposition below, which
  keeps the common shape on the partial/merge plan so the floor is reached only by shapes
  the merge genuinely cannot express. The floor holds independently of the decomposition;
  the decomposition is what stops the floor from being the normal outcome.
* **The floor is reached only when decomposition declines, never instead of it.**
  `build_dispatch_sql` reads the widening signal ONLY inside its `RequestShape::RowScan`
  arm, and `classify_request_shape` reaches that arm only after both aggregate tiers
  decline. A decomposable scalar-over-aggregate is therefore classified as
  `RequestShape::SingleGroupAgg` and never consults the widening signal at all.
* **The decomposition mechanism already exists and is already generic.** The grouped planner
  (`adapter/pushdown/grouped_agg.rs`) carries `sentinelize_aggregates` (replace each nested
  `function_aggregate` with a `__LH_AGG_MERGE_{i}__` sentinel `column`, collecting the
  aggregates in encounter order, stopping recursion at an aggregate boundary, flagging a
  bare `column` found OUTSIDE any aggregate as a disqualifying residual),
  `classify_scalar_over_aggregate` (that traversal plus `parse_agg_item` on each collected
  aggregate plus a renderability check on the sentinel tree),
  `render_scalar_over_merge` (re-sentinelize, render the sentinel tree once through the
  Exasol-dialect translator, then string-replace each sentinel token with that aggregate's
  merged `PARTIAL_*` expression), and `fold_aggregate_plan` (dedup by `AggregatePlan`
  equality, with a top-level occurrence's declared type overwriting a nested occurrence's
  default). None of the four carries GROUP BY state: they operate on a `Vec<AggregatePlan>`
  keyed by structural equality. The single-group merge already consumes the same
  `merge_select_items` formulas through `cast_merge_items`, so the merged expressions
  `render_scalar_over_merge` substitutes are the ones the single-group outer merge already
  emits.
* **Deduplication is a correctness requirement here, not an optimization.**
  `render_scalar_over_merge` matches each nested aggregate to its merged expression by
  `plans.iter().position(|p| *p == plan)` — the FIRST structurally-equal slot. Given
  `SELECT COUNT(*), ROUND(SUM(q) / COUNT(*), 2)` an un-deduplicated plan list
  `[Count, Sum, Count]` would resolve the nested `COUNT(*)` to slot 0 rather than slot 2,
  so the ordinal the merge expression names and the ordinal the `EMITS` clause declares
  diverge. Folding through `fold_aggregate_plan` collapses them to one slot and makes the
  position lookup total.
* **The plan list and the select-list item list stop being 1:1, so their declared-type
  sources split.** `aggregate_exasol_types` (`adapter/pushdown/support.rs`) FILTERS
  `selectList` to `function_aggregate` items, so a scalar-over-aggregate item is skipped
  and every later index shifts — it cannot serve either consumer once a scalar item is
  present. `partial_emits_items` needs a PER-PLAN type list (its `aggregate_types.get(i)`
  is the sole type source for an expression-argument aggregate's `SUM`/`MIN`/`MAX` `EMITS`
  column), and `cast_merge_items` needs a PER-SELECT-ITEM type list for the outer CAST.
  This is the same split the grouped planner already made: `plan_types` built by
  `fold_aggregate_plan` for the former, each `GroupedSelectItem`'s own `declared_type` for
  the latter.
* **Issue #188 is the same defect reached through the same node.** `VARIANCE` is Exasol's
  alias for `VAR_SAMP`; DataFusion defines `var`, `var_samp`, and `var_pop` but no
  `variance`. `vs-expression`'s `function_aggregate` arm splices the uppercased name
  verbatim, so a per-shard projection carrying `variance(…)` fails DataFusion planning with
  `Invalid function 'variance'. Did you mean 'radians'?`. Bare `VARIANCE(c_acctbal)` works
  because `parse_agg_item` resolves the name through the `STAT_AGG_KINDS` table
  (`vs-adapter/pushdown-agg-sql-consolidation`) and the aggregate never reaches DataFusion
  by name at all — the scan computes `(cnt, sum, sum_sq)` sufficient statistics.
  `ROUND(VAR_SAMP(…), 4)` works because DataFusion happens to know that name.
  `ROUND(VARIANCE(…), 4)`, `ABS(VARIANCE(…))`, and `VARIANCE(…) + 1` all fail identically.
  Decomposition routes the nested aggregate through `parse_agg_item` and therefore through
  the same two `AggKind` tables, so no aggregate function name crosses into DataFusion —
  the alias mapping is not re-implemented, it is reached.
* **No capability changes.** `SELECTLIST_EXPRESSIONS` and every aggregate capability this
  shape needs are already advertised — which is exactly why the adapter owns the answer:
  once a capability is advertised Exasol delegates it fully and never re-checks or
  re-applies it, so a shape the adapter cannot faithfully push must be rendered as
  equivalent SQL by the adapter itself, never omitted.
* **The new classification adds no `aggregationType` check, matching the existing
  detection.** `detect_aggregates` gates on `groupBy` being absent or empty and on the
  select list's own shape, never on `aggregationType`. A select-list item with no nested
  `function_aggregate` — `ROUND(price, 2)`, `UPPER(name)` — yields no aggregate to
  decompose, declines the classification, and reaches the row-scan projection exactly as
  today.
* **This feature leaves every `dispatch_golden` fixture byte-identical.** None of the
  fixtures under `adapter/pushdown/testdata/dispatch_golden/` carries an aggregate inside a
  row-scan projection, so no fixture encodes the pre-fix behaviour and a diff attributable
  to THIS feature is a regression rather than an expected update. New shapes get new
  fixtures. Separately, `vs-adapter/scan-spec-credential-reference` regenerates the eighteen
  credential-bearing fixtures — this feature's own
  `single_group_scalar_over_aggregate_dedup.sql` and
  `single_group_scalar_over_aggregate_interleaved.sql` among them — for their `storage`
  value alone; a diff outside that value, and any diff in the six `empty_*` fixtures,
  remains a regression.
* **This delta is issue #135. It amends ONE clause of ONE scenario and changes no decomposition, classification, or merge rule.** The four shared primitives, their single owner, the narrowest-visibility rule, the sentinel token format, the classifier's decline rules, and the three-shape merge-rewriting agreement are all UNCHANGED.
* **SUPERSEDES the relocation gate's unconditional full-string golden assertion.** Recorded `:216` requires the grouped planner's rendered SQL to "remain byte-identical after the relocation, asserted by full-string equality against the existing committed `dispatch_golden` fixtures". The RELOCATION still moves no byte — that is the property the gate exists to prove, and it is kept verbatim in intent. What the clause can no longer say is that the fixtures themselves are unchanged, because `vs-adapter/scan-spec-credential-reference` regenerates the eighteen credential-bearing fixtures for their `storage` value. The amended clause pins the full-string equality against the fixtures AS REGENERATED, so the gate stays falsifiable rather than being deleted.
* **This feature's own two goldens are in the regeneration set, which is why the clause could not be left as recorded.** `single_group_scalar_over_aggregate_dedup.sql` and `single_group_scalar_over_aggregate_interleaved.sql` are two of the eighteen; a clause forbidding any diff in them would forbid the shipped fixtures.
* The fixture COUNT the recorded text uses ("None of the eighteen fixtures") is stale independently of this plan — the directory holds twenty-four, eighteen credential-bearing and six `empty_*`. The numeric coincidence with this plan's regeneration set is accidental and is named here so the two are not conflated.
* Iceberg spec compliance: checked, not engaged; quotes retrieved from the published spec,
  not recalled. This feature changes how the adapter classifies an Exasol `selectList` item
  and which SQL it assembles for the outer merge. It reads no manifest, resolves no
  snapshot, touches no field-id projection, applies no delete file, and changes no type
  mapping. The predicate tree handed to Iceberg file resolution is the unmodified `filter`
  node, which this feature does not touch, and Iceberg-level file selection is driven by
  column predicates: "Deriving partition predicates from column predicates on the table
  data is used to separate the logical queries from physical storage: the partitioning can
  change and the correct partition filters are always derived from column predicates"
  (Apache Iceberg table spec, Specification → Partitioning).
* Iceberg spec compliance, the one clause that could have been engaged and is not: this
  feature does NOT answer any aggregate from manifest- or data-file-level statistics. Every
  aggregate it plans is computed by scanning data files, which is what the spec requires
  unconditionally — "Data files that match the query filter must be read by the scan" and
  "Delete files and deletion vector metadata that match the filters must be applied to data
  files at read time" (Specification → Scan Planning). A metadata-only aggregate would be
  the deviation to justify, and this feature does not attempt one: `record_count` is
  `required` in v1/v2/v3 but counts rows BEFORE delete application, and all five per-column
  stat maps (`value_counts`, `null_value_counts`, `nan_value_counts`, `lower_bounds`,
  `upper_bounds`) are `optional` in every format version (Specification → Manifests → Data
  File Fields), so neither is a sound aggregate source. There is no deviation to fix and
  none to track.

## Scenarios

### Scenario: Single-group select item that is a scalar function wrapping aggregates is decomposed into partial columns and one merged row

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO whose data files partition into two or more shards
* *AND* an ungrouped single-table `pushdown` request (no `groupBy`) whose select list contains a select item that is a scalar function wrapping one or more aggregates — for example `ROUND(SUM(L_QUANTITY), 2)`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL classify that item as a scalar-over-aggregate single-group item rather than declining to a row scan, and SHALL decompose every `function_aggregate` nested inside it into the same partial `AggregatePlan` list it builds for a top-level aggregate, so each inner aggregate contributes the `PARTIAL_*` columns the scan UDF emits once per shard
* *AND* the returned scan-driving SQL SHALL carry the aggregates in the scan spec's `aggregates` field and MUST NOT carry any `function_aggregate` inside the per-shard `EMITS` projection, so no shard computes a whole-table aggregate of its own
* *AND* the outer merge SELECT SHALL render the scalar function applied to the MERGED form of each inner aggregate — `ROUND(SUM("PARTIAL_sum_0"), 2)` for `ROUND(SUM(x), 2)` — at that item's original `selectList` ordinal, and MUST NOT reference any source column of the base table
* *AND* the query SHALL return EXACTLY ONE row whose value equals the same expression evaluated over all rows on a single node — `SELECT ROUND(SUM(l_quantity), 2) FROM LINEITEM` returning `25304.00` against the seeded TPC-H fixture, never one partial row per shard (issue #194)
* *AND* an `EXPLAIN VIRTUAL` of that query SHALL show a non-empty `"aggregates"` list and an empty `"projection"` in the emitted `LAKEHOUSE_SCAN` common spec, matching the shape a bare `SUM(l_quantity)` already emits

### Scenario: A scalar-wrapped statistical aggregate resolves through the shared AggKind tables, so no aggregate function name reaches DataFusion

* *GIVEN* an ungrouped `pushdown` request whose select list is a scalar function wrapping a statistical aggregate under an Exasol-only alias — `ROUND(VARIANCE(C_ACCTBAL), 4)`, and likewise `ABS(VARIANCE(C_ACCTBAL))`, `VARIANCE(C_ACCTBAL) + 1`, and `ROUND(STDDEV(C_ACCTBAL), 4)`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve each nested aggregate's function name to its `AggKind` through the SAME two name-mapping tables `vs-adapter/pushdown-agg-sql-consolidation` gives one owner each — `VARIANCE` and `VAR_SAMP` to `VarSamp`, `VAR_POP` to `VarPop`, `STDDEV` and `STDDEV_SAMP` to `StddevSamp`, `STDDEV_POP` to `StddevPop` — and MUST NOT introduce a second alias mapping of its own
* *AND* the per-shard scan SQL SHALL carry only the aggregate's `(cnt, sum, sum_sq)` sufficient-statistic partial columns, so NO aggregate function name is spliced into the DataFusion query text and the query MUST NOT fail with `Error during planning: Invalid function 'variance'` (issue #188)
* *AND* the outer merge SELECT SHALL reconstruct the statistic through the single König–Huygens fragment owner `vs-adapter/pushdown-agg-sql-consolidation` records, so a scalar-wrapped statistical aggregate and a bare one produce the same merge formula
* *AND* `SELECT ROUND(VARIANCE(c_acctbal), 4) FROM CUSTOMER` SHALL return one row equal to the same query evaluated against a native Exasol schema over the same rows
* *AND* the previously-working shapes SHALL remain working: bare `VARIANCE(c_acctbal)` and `ROUND(VAR_SAMP(c_acctbal), 4)` MUST return the same one-row results they return today

### Scenario: Inner aggregates shared across the single-group select list collapse into one deduplicated partial column

* *GIVEN* an ungrouped `pushdown` request whose select list references the same inner aggregate more than once — for example `SELECT COUNT(*), ROUND(SUM(L_QUANTITY) / COUNT(*), 2) FROM LINEITEM`, where `COUNT(*)` appears both as a bare select item and nested inside the scalar wrapper
* *WHEN* the adapter decomposes the select list into partial `AggregatePlan`s
* *THEN* the adapter SHALL collapse aggregates equal by kind and argument into a SINGLE plan slot and a single set of `PARTIAL_*` columns, through the same deduplicating fold the grouped planner uses, rather than emitting one plan per occurrence
* *AND* the deduplication SHALL be treated as a correctness requirement rather than an optimization, because the merge rewrite resolves each nested aggregate to the FIRST structurally-equal plan slot: an un-deduplicated `[Count, Sum, Count]` list would bind the nested `COUNT(*)` to slot 0 while its `EMITS` column was declared at slot 2
* *AND* a plan slot created by a NESTED-only occurrence SHALL take the default declared type, and a top-level occurrence of that same aggregate SHALL overwrite it with the authoritative `selectListDataTypes` entry at its own ordinal, regardless of which occurrence the select list places first
* *AND* both the bare occurrence and the nested occurrence SHALL render to the merged expression over that one shared partial column, and the returned row SHALL equal the same select list evaluated over all rows on a single node

### Scenario: Scalar-over-aggregate and plain aggregate items interleave in select-list order with per-item declared types

* *GIVEN* an ungrouped `pushdown` request whose select list places one or more scalar-over-aggregate items before, after, and between plain aggregates — for example `SELECT MIN(L_QUANTITY), ROUND(SUM(L_QUANTITY), 2), COUNT(*), ROUND(AVG(L_EXTENDEDPRICE), 3) FROM LINEITEM`
* *WHEN* the adapter builds the outer merge SELECT and the fan-out `EMITS` clause
* *THEN* the adapter SHALL emit the outer merge SELECT items in the exact order the corresponding items appear in `selectList`, interleaving merged plain-aggregate expressions and merged scalar-over-aggregate expressions as required, so Exasol's positional pushdown validation passes for every arrangement
* *AND* the adapter SHALL resolve the outer CAST for each item from the `selectListDataTypes` entry at that item's OWN `selectList` ordinal, matched by index rather than by comparing rendered SQL strings, through the single declared-type CAST helper `vs-adapter/pushdown-agg-sql-consolidation` records
* *AND* the adapter SHALL resolve each partial column's `EMITS` type from the PER-PLAN declared-type list the deduplicating fold builds, and MUST NOT read `aggregate_exasol_types` for either purpose once a scalar-over-aggregate item is present, because that function filters `selectList` down to `function_aggregate` items and therefore shifts every index after a skipped scalar item
* *AND* the returned row SHALL carry exactly one column per `selectList` item, with each column equal to that item evaluated over all rows on a single node

### Scenario: A nested aggregate the merge cannot decompose widens the projection instead of being evaluated per shard

* *GIVEN* an ungrouped or joined `pushdown` request whose select list contains a scalar function wrapping an aggregate the single-group merge cannot decompose — an inner `COUNT(DISTINCT …)` or other `DISTINCT` aggregate, a `STDDEV`/`VARIANCE` over a rendered expression rather than a bare column, an aggregate over a non-numeric column that the numeric gate demotes, a bare source `column` sitting outside every nested aggregate, or a residual scalar structure the translator cannot render
* *WHEN* the adapter plans the request
* *THEN* the scalar-over-aggregate classification SHALL decline and the adapter SHALL route the request to the qualified single-table wrapper — the same wrapper the grouped decline (`vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback`) and the multi-`DISTINCT` single-group decline (`vs-adapter/pushdown-planning-single-group-agg`) already use — whose own rendered SQL computes the whole select list as native Exasol SQL over a materialized sharded raw scan narrowed to the referenced columns
* *AND* the adapter MUST NOT emit the item as a per-shard projection expression, and MUST NOT return a bare row scan whose column count or column types differ from `selectListDataTypes` (`sqlCode 04000`), because Exasol never re-aggregates a declined pushdown
* *AND* the decline SHALL hold on the BROADCAST-JOIN path too, where the widened projection is a plain reason the broadcast plan is unavailable — the broadcast planner declining cleanly rather than erroring, so the request falls through to the unaccelerated N-scan fallback that renders the select list natively — because a single-group aggregation over a join is not recognised as carrying an aggregation clause when its only aggregate is nested inside a scalar wrapper
* *AND* the decline SHALL hold on the EMPTY-RESULT path, where every data file is pruned, so the zero-row response carries one column per `selectList` item typed from `selectListDataTypes` rather than the full base row
* *AND* the returned result SHALL equal the same select list evaluated on a single node for every one of those shapes

### Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list

* *GIVEN* an ungrouped `pushdown` request carrying a decomposable scalar-over-aggregate select item — for example `SELECT ROUND(SUM(L_QUANTITY), 2), COUNT(*) FROM LINEITEM WHERE L_QUANTITY < -1`
* *AND* file pruning eliminates every data file, so there is nothing to scan
* *WHEN* the adapter builds the empty-result response
* *THEN* the response SHALL be ONE row — matching the non-empty single-group shape, which always returns exactly one row — with one column per `selectList` item in select-list order, and MUST NOT be the zero-row grouped shape or the full base row
* *AND* the scalar-over-aggregate column SHALL be the zero-row value of that item cast to the item's own declared type through the shared declared-type CAST helper, so it emits a bare uncast value when that declared type is the `VARCHAR(2000000)` default
* *AND* the empty response SHALL reference no scan and no merge UDF, because with zero files there is nothing to scan or merge
* *AND* the empty and non-empty column shapes MUST NOT diverge in count, order, or declared type, since both are validated positionally against the same `selectListDataTypes`

### Scenario: The scalar-over-aggregate decomposition mechanism has ONE owner shared by both aggregate planners

* *GIVEN* the four primitives the grouped planner uses today — the aggregate-sentinelizing tree walk, the scalar-over-aggregate classifier, the merge rewrite that substitutes each sentinel with its merged partial expression, and the deduplicating plan fold — all currently private to `adapter/pushdown/grouped_agg.rs`
* *WHEN* the single-group planner gains the same classification and the same merge rewrite
* *THEN* those four primitives SHALL have exactly ONE owner reachable by BOTH planners, and the single-group planner MUST NOT carry a second copy of the tree walk, the sentinel token format, the classifier's decline rules, or the substitution rewrite
* *AND* the owning module SHALL be a submodule of `adapter::pushdown` exposing the primitives at the narrowest visibility that compiles, never widened to a broader public than they have today, and its tests SHALL live in its own sibling test file per `vs-adapter/pushdown-module-structure`
* *AND* the grouped planner's rendered SQL MUST remain byte-identical after the relocation EXCEPT for the scan-spec `storage` value, which the relocation itself does not touch and which `vs-adapter/scan-spec-credential-reference` re-encodes, asserted by full-string equality against the committed `dispatch_golden` fixtures as regenerated for that value alone — including this feature's own `single_group_scalar_over_aggregate_dedup.sql` and `single_group_scalar_over_aggregate_interleaved.sql` — so the move is a change of owner and not of output
* *AND* a top-level bare aggregate, a grouped scalar-over-aggregate item, and a single-group scalar-over-aggregate item SHALL be rewritten by the SAME merge-rewriting path, so the three produce consistent merged SQL for the same inner aggregate
