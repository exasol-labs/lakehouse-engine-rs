# Feature: VS Expression Translator — CAST

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with CAST target-type rendering, split out of `sql-comprehension/vs-expression-translator-scalar-ops` once TIMESTAMP fractional-seconds precision rendering added a second CAST scenario.

<!-- DELTA:CHANGED -->
## Background

* This delta corrects what a refused CAST target MEANS for the caller. The refusal itself is
  unchanged — an error in raising mode, `None` in the safe variants, for exactly the targets whose
  DataFusion result would diverge. What is corrected is the claim that the adapter can therefore
  omit the CAST and let Exasol evaluate it: `FN_CAST` IS advertised, so nothing else evaluates it.
  See `pushdown/pushdown-declined-filter-self-apply` and ADR `specs/_decision/045`.
* These five targets are refused in BOTH dialects, so a WHERE predicate carrying one cannot be
  self-applied either. That predicate is the terminal case: a clean client-facing error, never a
  result computed without it.
* Exasol emits CAST as its own top-level node type, `function_scalar_cast` — not nested inside a generic `function_scalar` node — matching the same family pattern as `function_scalar_case` and `function_scalar_extract`. The translator also retains a defensive nested `function_scalar`+`name=CAST` arm for a legacy/alternate encoding, sharing the same rendering logic, but `function_scalar_cast` is the node type Exasol's live engine actually sends.
* `render_expression` renders CAST for the DataFusion dialect (the node-local scan SQL DataFusion 54 parses); `render_expression_exasol` renders it for the Exasol dialect (the wrapper SQL Exasol parses directly). The two parsers have different precision constraints for TIMESTAMP.
* Exasol serialises a TIMESTAMP dataType as `{"type":"TIMESTAMP","withLocalTimeZone":<bool>,"fractionalSecondsPrecision":<0-9>}`; both optional (defaults `false` and `3`). `fractionalSecondsPrecision` — not `precision` — is the fractional-seconds field.
* `render_cast_target`'s two dialect arms also diverge on a `CHAR` target (`specs/_decision/011-fix-count-distinct-shard-cap.md`, follow-up "Exasol-dialect CAST for the qualified wrapper"). The `DataFusion` arm feeds fragments embedded in a `ScanSpec` (`filter`/`projection`/`group_keys`) that datafusion-sql parses inside the scan UDF, and renders a bare, length-less `VARCHAR` — Arrow has only `Utf8` and datafusion-sql rejects a length-qualified character target without `support_varchar_with_length`, which this project does not enable. The `Exasol` arm feeds wrapper SQL text Exasol's own core engine parses, and renders `CHAR({size})`, plus ` ASCII` when the node's `dataType.characterSet` is `ASCII` case-insensitively, matching the width and character set Exasol validates positionally against `selectListDataTypes` — rendering bare `CHAR({size})` for an ASCII-declared target would trade a `VARCHAR(n) ASCII` mismatch for a `CHAR(n) UTF8` one. `CAST(<expr> AS CHAR(n) ASCII)` is valid Exasol CAST syntax, verified live on Exasol 2025.2.1. Three Exasol-parsed wrapper paths reach the Exasol arm: `joins/sql_builders.rs`'s `n_scan_join_select_items` (the N-scan unaccelerated join wrapper) and `build_qualified_single_table_fallback_sql` (the qualified single-table aggregate fallback), both via `render_selectlist_item_qualified` → `render_expression_exasol_safe`; and `grouped_agg.rs`'s `render_scalar_over_merge` (the grouped-merge scalar-over-aggregate wrapper, reached from `build_grouped_aggregate_scan_sql`'s `ScalarOverAggregate` arm), via `render_expression_exasol` directly. The suffix rule mirrors the adapter's `exasol_type_from_json` CHAR rule so the two independent seams cannot disagree on a CHAR target — a claim scoped to CHAR deliberately: on a VARCHAR target the two seams already differ in suffix handling (the adapter's VARCHAR arm appends ` ASCII` for an ASCII `characterSet`; this crate's Exasol-dialect VARCHAR rendering emits `VARCHAR({size})` with no suffix), a pre-existing asymmetry this feature leaves untouched. This crate is shared with a sibling VS-adapter project (`specs/mission.md`), so the CHAR case is a narrowly additive dialect arm that leaves the `Dialect::DataFusion` behavior and the Exasol `VARCHAR` rendering byte-identical.
* The DataFusion dialect never APPROXIMATES a requested type. It renders the type or declines the
  node. Rendering a `fractionalSecondsPrecision` outside `{0,3,6,9}` at the nearest member of that
  set would contradict this feature's own rule that "the set of CAST target types the translator
  renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result".
* DECLINE is NOT an Exasol-side re-evaluation of a delegated node. CLAUDE.md § "Virtual Schema
  pushdown delegation" and ADR `specs/_decision/045` record that Exasol never re-checks a
  capability it has delegated. A decline is the DataFusion-dialect renderer returning `Err`/`None`
  for a node, which routes that node into SQL the ADAPTER ITSELF writes in the Exasol dialect and
  Exasol merely executes.
* Every position a CAST node can occupy has such a route already. SELECT LIST: a `None` from
  `render_expression_safe` sets `needs_full_fallback`, piped out as `projection_widened`, and the
  `RowScan` arm returns `qualified_single_table_fallback_pushdown`. WHERE: `datafusion_renderable`
  probes the node and a declined predicate is self-applied in the wrapper's own Exasol-dialect
  `WHERE` (`pushdown/pushdown-declined-filter-self-apply`). GROUP BY: a group-key render failure
  collapses grouped-aggregate detection to `None`, which falls through to
  `RequestShape::GroupByWrapper` and the same wrapper. ORDER BY: `parse_declined_sort_key` renders
  a non-column sort key in the EXASOL dialect from the start, so a DataFusion-dialect decline never
  reaches it.
* The decline is NOT free. Declining ONE select-list item widens the WHOLE select list to the base
  row and routes the WHOLE request to the wrapper, so Exasol computes every select-list item. The
  sharded parallel fan-out, the referenced-column projection narrowing, and the WHERE predicate
  inside the scan spec survive. The per-shard `LIMIT` and the bounded top-N are given up:
  `build_qualified_single_table_fallback_sql` renders the select list, GROUP BY, HAVING, ORDER BY
  and LIMIT in the OUTER wrapper only, so every matching row ships to Exasol.
* The Exasol dialect renders `TIMESTAMP(p)` verbatim for every `p` in 0-9, so the declined
  expression is computed at the LITERAL precision requested, with no DataFusion or Arrow precision
  ceiling in the way. The decline trades scan-side acceleration for exactness, this project's
  recorded correctness-over-availability direction.
* The adapter has NO freedom in what it declares for a CAST-projected column. Exasol validates a
  pushdown response positionally against `selectListDataTypes`, so `exasol_type_from_json` echoes
  the literal `fractionalSecondsPrecision` Exasol sent. The declared `TIMESTAMP(p)` and the pushed
  DataFusion expression are two independent surfaces.
* A precision outside 0-9 cannot arrive: Exasol's own TIMESTAMP domain is `p` in 0-9 and Exasol
  itself produced the `fractionalSecondsPrecision` value, so no clamp above 9 is needed.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: CAST renders the mapped target type per dialect

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (`function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; both `VARCHAR` and `CHAR` as a bare, length-less `VARCHAR` — a DataFusion-dialect-specific rendering, because datafusion-sql rejects a length-qualified character target without `support_varchar_with_length` and Arrow has only `Utf8`, with no CHAR type for a fixed-width target to map to (the Exasol dialect diverges — see the CHAR scenario below); `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; and `TIMESTAMP` as `TIMESTAMP` or `TIMESTAMP(p)` per the fractional-seconds-precision rule
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects
* *AND* such a refusal in a WHERE predicate SHALL cause the adapter to return a clean client-facing error, because the predicate can be applied neither by DataFusion nor by the adapter's own Exasol-dialect wrapper — REPLACING the recorded "so the adapter omits the CAST and Exasol evaluates it as a correctness backstop", which assumed an Exasol-side re-check of a delegated `FN_CAST` that does not occur
* *AND* the adapter MUST NOT omit a refused CAST from a WHERE predicate and return rows, because the omitted predicate would be evaluated by nobody
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` is never advertised for a target the translator would render divergently
