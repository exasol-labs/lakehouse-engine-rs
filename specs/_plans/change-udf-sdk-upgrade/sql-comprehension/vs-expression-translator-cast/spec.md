# Feature: VS Expression Translator — CAST

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with CAST target-type rendering, split out of `sql-comprehension/vs-expression-translator-scalar-ops` once TIMESTAMP fractional-seconds precision rendering added a second CAST scenario.

## Background

<!-- DELTA:NEW -->
* This delta removes the only place where the DataFusion dialect APPROXIMATES a requested type
  instead of rendering it or declining it. `snap_timestamp_precision`
  (`crates/vs-expression/src/lib.rs:431`) mapped a `fractionalSecondsPrecision` outside `{0,3,6,9}`
  to the nearest member of that set, so a pushed `CAST(x AS TIMESTAMP(2))` was computed at
  millisecond and a `CAST(x AS TIMESTAMP(5))` at microsecond. That directly contradicts this
  feature's own recorded rule that "the set of CAST target types the translator renders SHALL be
  exactly the set whose DataFusion result matches Exasol's CAST result", which is why the fix is a
  correction rather than a scope change.
* The approximation was paired with an UNVERIFIED claim that Exasol truncates the up-snapped value
  back to the requested precision. Nothing measured it, and it predates issue #405 entirely. The
  decline removes the claim's subject rather than testing it.
* DECLINE has one established meaning in this codebase and it is NOT an Exasol-side re-evaluation of
  a delegated node. CLAUDE.md § "Virtual Schema pushdown delegation" and ADR
  `specs/_decision/045` record that Exasol never re-checks a capability it has delegated, and this
  feature's own Background already replaced the opposite reading once. A decline is the
  DataFusion-dialect renderer returning `Err`/`None` for a node, which routes that node into SQL the
  ADAPTER ITSELF writes in the Exasol dialect and Exasol merely executes.
* Every position a CAST node can occupy already has such a route, and this delta adds none.
  SELECT LIST: `render_expression_safe` returning `None` in `project_columns`'s shared
  scalar-and-predicate arm sets `needs_full_fallback`
  (`crates/lakehouse-engine/src/adapter/pushdown/support.rs:1396-1399`), one of six conditions that
  set it. That signal is piped out as `projection_widened`
  (`adapter/pushdown/mod.rs:187`) and the `RowScan` arm returns
  `qualified_single_table_fallback_pushdown` (`adapter/pushdown/mod.rs:655`). WHERE: the
  DataFusion-renderability probe is `datafusion_renderable`
  (`adapter/pushdown/support.rs:564`) and a declined predicate is self-applied in the wrapper's own
  Exasol-dialect `WHERE` (`vs-adapter/pushdown-declined-filter-self-apply`). GROUP BY: a group-key
  render failure collapses the grouped-aggregate detection to `None`
  (`adapter/pushdown/grouped_agg.rs:189`), which falls through to `RequestShape::GroupByWrapper` and
  the same wrapper. ORDER BY: a non-column sort key is rendered in the EXASOL dialect from the start
  (`parse_declined_sort_key`, `adapter/pushdown/topn.rs:130`, via `render_expression_exasol_safe`),
  so a DataFusion-dialect decline never reaches it.
* The decline is NOT free, and the cost is stated rather than implied. Declining ONE select-list
  item widens the WHOLE select list to the base row and routes the WHOLE request to the wrapper;
  Exasol then computes every select-list item, not only the declined one. What survives is the
  sharded parallel fan-out, the referenced-column projection narrowing
  (`referenced_column_projection`, `adapter/pushdown/joins/sql_builders.rs:920`), and the WHERE
  predicate, which still travels inside the scan spec. What is given up is the per-shard `LIMIT` and
  the bounded top-N: the fan-out spec is built with `limit: None` and `order_by: Vec::new()`
  (`adapter/pushdown/joins/sql_builders.rs:1138-1139`), and
  `build_qualified_single_table_fallback_sql`
  (`adapter/pushdown/joins/sql_builders.rs:1032`) renders the select list, GROUP BY, HAVING, ORDER BY
  and LIMIT in the OUTER wrapper only, so every matching row ships to Exasol.
* The Exasol dialect already renders `TIMESTAMP(p)` verbatim for every `p` in 0-9 and needs no
  change, so the declined expression is computed at the LITERAL precision requested, with no
  DataFusion or Arrow precision ceiling in the way. The decline therefore trades scan-side
  acceleration for exactness, which is this project's recorded correctness-over-availability
  direction.
* The adapter has NO freedom in what it declares for a CAST-projected column. Exasol validates a
  pushdown response positionally against `selectListDataTypes`, so `exasol_type_from_json`
  (`crates/lakehouse-engine/src/types/mapping.rs:660`) must echo the literal
  `fractionalSecondsPrecision` Exasol sent. The declared `TIMESTAMP(p)` and the pushed DataFusion
  expression are therefore two independent surfaces, and only the second one is corrected here.
* A precision outside 0-9 cannot arrive: Exasol's own TIMESTAMP domain is `p` in 0-9 and Exasol
  itself produced the `fractionalSecondsPrecision` value, so the removed clamp of "anything above 9
  to 9" guarded an unreachable input.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect

* *GIVEN* a `function_scalar_cast` node whose `dataType` is `{"type":"TIMESTAMP", ...}` carrying an OPTIONAL `fractionalSecondsPrecision` integer in 0-9 and an OPTIONAL `withLocalTimeZone` flag
* *AND* the precision field name is `fractionalSecondsPrecision` — Exasol's documented data-type field for a TIMESTAMP's fractional-seconds precision, verified against Exasol's virtual-schema data-type API and the reference pushdown fixture `pushdown_request_alltypes.json` (`C_TIMESTAMP_4` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":7}`); it is NOT `precision`, which Exasol uses only for `DECIMAL` (with `scale`) and `INTERVAL` (with `fraction`)
* *WHEN* `render_expression` (DataFusion dialect) or `render_expression_exasol` (Exasol dialect) processes the node
* *THEN* a `withLocalTimeZone: true` dataType SHALL be declined (`Err` in raising mode, `None` in the safe variants) BEFORE any precision handling runs, unchanged by this scenario, because DataFusion's plain TIMESTAMP cannot reproduce its session-timezone / UTC-normalisation semantics
* *AND* when `fractionalSecondsPrecision` is absent the translator SHALL render bare `TIMESTAMP` in BOTH dialects, preserving the pre-change rendering, since bare `TIMESTAMP` equals Exasol's default `TIMESTAMP(3)`
* *AND* when `fractionalSecondsPrecision` is present the Exasol dialect SHALL render `TIMESTAMP(p)` with the declared precision verbatim for every `p` in 0-9, because Exasol's parser accepts all of them
* *AND* when `fractionalSecondsPrecision` is present and `p` is in `{0, 3, 6, 9}` the DataFusion dialect SHALL render `TIMESTAMP(p)` VERBATIM, because DataFusion 54's SQL frontend parses exactly those four (Second/Millisecond/Microsecond/Nanosecond) and the rendered target then matches the requested one exactly
* *AND* when `p` is in `{1, 2, 4, 5, 7, 8}` the DataFusion dialect SHALL DECLINE the node (`Err` in raising mode, `None` in the safe variants), and MUST NOT render an approximated `TIMESTAMP(p')` for any `p'` in `{0, 3, 6, 9}`, REPLACING the recorded nearest-value snap `0→0, 1→0, 2→3, 4→3, 5→6, 7→6, 8→9` and its above-9 clamp
* *AND* the DECLINE MUST NOT be read as Exasol independently re-evaluating the node: it routes the node into SQL the ADAPTER writes in the Exasol dialect, where the literal `p` renders verbatim and Exasol computes the value natively with no DataFusion or Arrow precision ceiling
* *AND* the up-snap claim that the EMITS-declared Exasol column "SHALL truncate back to the requested `p`" SHALL be DELETED rather than reworded, because no measurement ever established it and the decline removes the up-snap that was its subject; likewise the single DOWN-snap `1→0`, recorded as a named precision trade-off, SHALL be deleted because `TIMESTAMP(1)` now declines instead of losing a digit
* *AND* the declared EMITS type for such an item MUST still echo the literal `fractionalSecondsPrecision` Exasol sent, because Exasol validates a pushdown response positionally against `selectListDataTypes`; the decline changes which component COMPUTES the value, never which type is DECLARED
* *AND* the decline SHALL NOT produce a client-facing error for any of `{1, 2, 4, 5, 7, 8}`, unlike the five refused CAST TARGET TYPES of this feature's other scenario, because those are refused in BOTH dialects while this one renders in the Exasol dialect
* *AND* the cost of the decline SHALL be recorded rather than implied: a declined select-list item widens the whole select list and routes the whole request to the qualified single-table wrapper, so Exasol computes EVERY select-list item and the per-shard `LIMIT` and bounded top-N are given up, while the sharded fan-out, the referenced-column projection narrowing, and the scan-side WHERE predicate are retained
<!-- /DELTA:CHANGED -->
