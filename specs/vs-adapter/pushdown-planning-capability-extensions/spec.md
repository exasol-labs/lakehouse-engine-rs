# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised
capabilities: scalar select-list expression pushdown, HAVING clause pushdown, and
decomposable statistical aggregate pushdown via sufficient statistics. Each extends the
translator or aggregate planner with a shard-associative partial/merge path.

## Background

* Filter, select-list, group-key, and HAVING expressions are all rendered by the shared
  `crates/vs-expression` translator; an untranslatable expression is omitted/falls back
  rather than producing an incorrect result.
* An aggregate is pushed down only when it decomposes into a shard-associative
  partial/merge plan; otherwise the adapter falls back to row scanning.
* Credentials MUST NOT appear in any returned SQL or error message.
* A scalar-function capability is advertised only once a `crates/vs-expression` arm renders it and
  the DataFusion 54 result matches Exasol. `FN_CAST`, `FN_NEG`, and `FN_WEEK` meet this bar;
  `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the regexp scalar functions, the divergent date
  functions, and the bitwise operator functions do not and stay unadvertised.
* This delta corrects one recorded claim, and only that claim: that a HAVING the adapter
  cannot render is omitted from the returned SQL and re-applied by Exasol as a correctness
  backstop. No such behavior exists, and relying on it would return wrong rows.
* No code path omits a HAVING. The adapter has exactly two HAVING renderers, and neither
  omits: `grouped_agg.rs::render_having_over_merge` (the partial/merge path) and
  `joins/sql_builders.rs::qualified_join_having` (the qualified-wrapper and N-scan join path,
  which raises `UdfError::User` on an unrenderable HAVING). The "omitted and retained by
  Exasol" clause therefore described behavior that was never implemented, so no code can rely
  on it regardless of how Exasol behaves.
* The adapter's own code asserts the corrected HAVING rule in six places — `request_shape.rs`
  lines 16, 70, and 85; `grouped_agg.rs` line 3394; `file_resolution.rs` line 1480; and
  `mod.rs` line 363 — all stating that Exasol will not re-apply a HAVING the adapter advertised
  `AGGREGATE_HAVING` for (`capabilities.rs` line 171). The recorded spec was the outlier.
* Exasol's re-apply behavior varies by pushed shape, which is itself a reason no unrenderable
  clause may depend on it. Live precedent under `add-topn-pushdown` B5/B6 (issues #225 / #189):
  an `orderBy` pushed TOGETHER with a `limit` is fully delegated — Exasol re-applies neither, so
  the withheld-limit fallback returned wrong, unsorted, unbounded rows and the adapter now
  renders a self-contained global `ORDER BY … LIMIT` (`topn.rs` lines 444-449, `mod.rs` lines
  690-694). An `orderBy` pushed WITHOUT a `limit` behaves differently: Exasol keeps its own
  top-level `ORDER BY` and re-sorts the returned rows (`tests/e2e_scan_test.rs` lines 1133-1138).
* The LIMIT half of the same comparison is false for the same reason as the HAVING half: no code
  path omits an observable LIMIT, so there is nothing for an Exasol backstop to restore. The
  adapter renders it everywhere it is observable — the grouped path passes `limit` straight
  through (`mod.rs` line 405), and the row-scan declined-ORDER-BY path re-renders it in the outer
  wrapper via `wrap_declined_order_by(…, limit)` (`mod.rs` lines 707-711). The single place
  `effective_limit` drops it (`mod.rs` line 594, when an ORDER BY was pushed that the adapter did
  not render) is structurally unreachable for the one shape it applies to, the single-group
  aggregate: the adapter advertises `ORDER_BY_COLUMN` and NOT `ORDER_BY_EXPRESSION`
  (`capabilities.rs` lines 45-46), so Exasol pushes an `orderBy` only over a bare projected
  column, and a single-group aggregate's output has no bare column to sort on. Exasol therefore
  never pushes an `orderBy` for that shape, so the drop site never executes — for ANY limit value,
  `LIMIT 0` included.
* This delta asserts nothing new about `ORDER_BY_COLUMN`. It deletes the false HAVING and LIMIT
  comparison from the ORDER BY scenario's reliance clause and leaves that clause's ORDER BY
  reliance exactly as recorded, so `vs-adapter/pushdown-planning-topn` — whose "Unsupported
  ordered-query shapes decline the ordered-top-N path" scenario records the same ORDER BY
  reliance for the same trigger set, and which makes no LIMIT-backstop claim of its own — needs
  no amendment and is deliberately left untouched.
* The correct handling for a HAVING the adapter cannot render over the partial/merge
  decomposition is therefore neither omission nor an error: route the request to the qualified
  single-table wrapper, which renders the HAVING as ordinary Exasol SQL over materialized rows.
  See `vs-adapter/pushdown-planning-grouped-agg` (issue #195).
* This delta does NOT adjudicate the WHERE-filter backstop. An untranslatable WHERE predicate
  is genuinely omitted from the scan spec (`vs_expression::render_df_filter_safe` returns
  `None`), a distinct mechanism with its own capability story; the Background statement about
  filter and select-list expressions stands as recorded. Only the HAVING claim is corrected.

## Scenarios

### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, or `CASE WHEN qty > 0 THEN 1 ELSE 0 END`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS`
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node — recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), not only the generic `function_scalar` node — to a DataFusion SQL fragment using the VS expression translator (raising mode), and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result

### Scenario: HAVING predicate is pushed into the grouped scan plan

* *GIVEN* a grouped aggregate `pushdown` request carrying a `having` predicate over the grouped aggregates and group keys
* *AND* the adapter advertises `AGGREGATE_HAVING`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render the HAVING predicate to a DataFusion SQL fragment using the same VS expression translator path used for WHERE predicates
* *AND* the adapter SHALL apply the rendered HAVING predicate only in the OUTER wrapper SQL that merges the per-shard partial-aggregate rows, never inside the per-shard partial scan (a per-shard HAVING would discard groups that only meet the threshold after merge)
* *AND* the adapter MUST NOT omit a HAVING it cannot render from the returned SQL, because Exasol does not re-apply a HAVING whose `AGGREGATE_HAVING` capability the adapter advertises — omission returns wrong rows
* *AND* a HAVING the adapter cannot render over the partial/merge decomposition SHALL instead route the request to the qualified single-table wrapper, which renders the HAVING as ordinary Exasol SQL over materialized rows so the predicate is preserved rather than dropped (see `vs-adapter/pushdown-planning-grouped-agg`, issue #195)

### Scenario: Decomposable statistical aggregate is pushed down via sufficient statistics

* *GIVEN* a query selecting `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP` over a column, optionally with a GROUP BY clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL instruct the scan UDF to emit, per shard (and per group when grouped), the sufficient statistics `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` rather than a per-shard standard deviation or variance
* *AND* the outer wrapper SQL SHALL merge the per-shard sufficient statistics into the final variance as `(SUM(sum_sq) - SUM(sum)*SUM(sum)/SUM(cnt)) / d`, where `d` is `SUM(cnt)` for the population forms and `SUM(cnt) - 1` for the sample forms, and the final standard deviation as the square root of that variance
* *AND* the wrapper SHALL yield NULL (never divide by zero or take the square root of a negative rounding artifact) when the merged count is zero, or one for the sample forms
* *AND* both single-group and grouped aggregate merge expressions SHALL be wrapped in `CAST(<expr> AS <declared_type>)` to match the declared Exasol output column type, satisfying Exasol's strict pushdown output-type validation
* *AND* the merged result SHALL equal the result of the same statistical aggregate evaluated over all rows on a single node within floating-point tolerance

### Scenario: Adapter falls back for non-decomposable aggregates

* *GIVEN* a `pushdown` request whose select list contains an aggregate the adapter does not advertise as decomposable (e.g. `MEDIAN`, `APPROXIMATE_COUNT_DISTINCT`, `LISTAGG`, `GROUP_CONCAT`, or a `COUNT(DISTINCT ...)` that appears inside a GROUP BY request)
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field)
* *AND* Exasol SHALL compute the aggregate on the returned rows using its own engine
* *AND* the adapter MUST NOT emit a partial/merge plan for any aggregate it cannot decompose into a shard-associative partial/merge plan, because doing so would yield an incorrect result
* *AND* a single-group (no GROUP BY) `COUNT(DISTINCT col)` SHALL NOT fall back here — it is decomposed via `vs-adapter/pushdown-planning-count-distinct` — while a `COUNT(DISTINCT ...)` inside a GROUP BY request SHALL still fall back

### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic binary-operator scalar-function capabilities for `+`, `-`, `*`, and `/` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`), verified against the Exasol Virtual Schema capability vocabulary
* *AND* each advertised arithmetic operator SHALL be backed by a `crates/vs-expression` translator arm that renders it (so an advertised operator is never one the translator would decline), keeping the capability set coherent with the translator the same way GROUP BY tuple advertisement is gated on its backing path
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising arithmetic operators MUST NOT introduce any additional join or cross-join capability

### Scenario: An advertised arithmetic expression Exasol cannot see decomposed remains correctness-safe

* *GIVEN* the adapter advertises the arithmetic operator capabilities and Exasol pushes an arithmetic expression tree in a filter, select-list, group-key, or aggregate-argument position
* *WHEN* the `crates/vs-expression` translator cannot render a particular arithmetic node (e.g. an operator or operand shape it does not handle)
* *THEN* the adapter SHALL fall back on the affected clause exactly as for any other untranslatable expression — a filter is omitted and retained by Exasol, a select-list expression falls back to projecting underlying columns, and an aggregate over the unrenderable argument falls back to row scanning
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation

### Scenario: ORDER_BY_COLUMN is advertised so ordered top-N queries can be pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `ORDER_BY_COLUMN` so Exasol pushes column sort keys (with direction and NULL placement) and the accompanying `LIMIT` into the `pushdown` request, enabling the ordered-top-N partial/merge path in `vs-adapter/pushdown-planning-topn`
* *AND* `ORDER_BY_EXPRESSION` SHALL remain absent, so Exasol never pushes an expression sort key the adapter has no bounded-sort path for
* *AND* `LIMIT_WITH_OFFSET` SHALL remain absent, so Exasol never pushes an OFFSET and the ordered-top-N path needs no offset handling
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising `ORDER_BY_COLUMN` MUST NOT introduce any additional join or cross-join capability

### Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe

* *GIVEN* the adapter advertises `ORDER_BY_COLUMN` and Exasol pushes an `order_by` in a `pushdown` request that the adapter cannot serve as an ordered top-N (no accompanying `LIMIT`, a sort key that is not a bare projected column, or a request that also carries aggregates / group keys / a `having`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL fall back to the pre-existing scan plan for that shape without pushing a per-shard row limit ahead of the ordering, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation
* *AND* the adapter SHALL rely on Exasol to apply the `ORDER BY` it retains over the returned rows

### Scenario: Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CAST` and `FN_NEG`, each backed by a `crates/vs-expression` translator arm (the CAST arm over its faithful target-type set and the unary-negation arm), so no advertised capability is one the translator would decline for a shape Exasol expects it to handle
* *AND* a CAST to an unsupported target type SHALL fall back — the adapter omits the CAST and Exasol evaluates it — rather than producing an incorrect result
* *AND* `FN_TO_CHAR`, `FN_TO_NUMBER`, and `FN_DIV` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised, so advertising `FN_CAST` and `FN_NEG` introduces no additional join or cross-join capability

### Scenario: ISO week capability is advertised so WEEK expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_WEEK`, backed by the `crates/vs-expression` `WEEK` arm rendering `date_part('week', …)`, whose ISO-8601 result matches Exasol `WEEK` (see `sql-comprehension/vs-expression-translator-date-fns`)
* *AND* `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_SECONDS`, `FN_ADD_WEEKS`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, `FN_SECONDS_BETWEEN`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so advertising `FN_WEEK` introduces no additional join or cross-join capability

### Scenario: Regexp scalar function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`
* *AND* Exasol SHALL post-process regexp scalar functions rather than pushing them to the node-local scan, because at the pinned DataFusion 54.0.0 and `regex` 1.12.4 the Rust `regex` dialect rejects the backreferences and lookaround Exasol's PCRE dialect accepts, DataFusion has no `regexp_substr`, and its `regexp_replace`/`regexp_instr`/`regexp_count` argument shapes omit Exasol's position, occurrence, and return-option arguments — a compile-time literal-pattern check cannot certify semantic match parity, so no faithful translation exists (see issue #106 and `sql-comprehension/vs-expression-translator-scalar-fns`)
* *AND* the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement SHALL remain unchanged

### Scenario: Bitwise operator function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_BIT_AND`, `FN_BIT_OR`, `FN_BIT_XOR`, `FN_BIT_NOT`, `FN_BIT_LSHIFT`, `FN_BIT_RSHIFT`, `FN_BIT_LROTATE`, `FN_BIT_RROTATE`, `FN_BIT_CHECK`, `FN_BIT_SET`, or `FN_BIT_TO_NUM`
* *AND* Exasol SHALL post-process bitwise operator functions rather than pushing them to the node-local scan, because at pinned DataFusion 54.0.0 no faithful translation exists for any of the eleven over Exasol's bit-function domain (issue #108): Exasol bit functions operate on unsigned 64-bit integers (range `0`–`18446744073709551615`, result `DECIMAL(20,0)`), while DataFusion's `&`/`|`/`#`/`<<`/`>>` act on the operand's signed Arrow integer type — Iceberg sources carry only signed integers (`int` = 32-bit signed, `long` = 64-bit signed; the Iceberg spec defines no unsigned integer primitive), so a bit-63-set result is a large positive value in Exasol but negative under signed `Int64` and the `Int64` → `DECIMAL(20,0)` mapping carries the negative value; `BIT_RSHIFT` diverges unconditionally on any bit-63-set operand because DataFusion's signed `>>` is arithmetic (sign-extending) whereas Exasol's is logical (zero-fill); and DataFusion 54.0.0 provides no operator or scalar function at all for `BIT_NOT` (its SQL planner rejects unary `~`), `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`
* *AND* `FN_BIT_LENGTH` SHALL be treated as out of scope for this decision — it is an Exasol string function (bit count of a string), not a bitwise operator — and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so this decision introduces no additional join, cross-join, or string-function capability change

### Scenario: Projected literal select-list item is pushed into the scan-driving query

* *GIVEN* a row-scan or inner-join `pushdown` request that carries NO aggregate and NO GROUP BY, whose select list contains one or more bare literal/constant items — any of `literal_null`, `literal_bool`, `literal_exactnumeric`, `literal_double`, `literal_string`, `literal_date`, `literal_timestamp` (e.g. `SELECT 1 FROM t`, `SELECT 1, name, 1 FROM t`, the constant-folded `SELECT 2+3` Exasol sends as a single `literal_exactnumeric`, OR the single-element `[{"type":"literal_null"}]` select list Exasol synthesizes for its documented Virtual-Schema-API "selectList is an empty array: select any one column or expression" contract when a LIMIT barrier sits between an outer aggregate and the derived table it wraps — for example the inner derived-table request behind `SELECT COUNT(*) FROM (SELECT c_custkey FROM t LIMIT 5)`, which arrives on the wire as `"selectList":[{"type":"literal_null"}]` with `"selectListDataTypes":[{"type":"BOOLEAN"}]`, a one-element array carrying a `literal_null` item, NOT a JSON `null` and NOT an empty `[]` array — issue #205)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render each literal select-list item through the `crates/vs-expression` translator into a POSITIONAL `Expr` projection item — one projection item per select-list item, typed from the parallel top-level `selectListDataTypes` array — exactly as the `function_scalar` select-list branch already does, and MUST NOT trigger the full-base-row fallback that emits every base column and yields the column-count mismatch Exasol rejects ("Expected number of columns is 1 but pushdown query has N", issues #190 and #205)
* *AND* the emitted scan's column arity SHALL equal the query's select-list arity, so two structurally identical literal items — such as the two `1` items in `SELECT 1, name, 1` — SHALL each occupy their own projected position and MUST NOT be collapsed into one
* *AND* each projected literal SHALL be evaluated once per scanned source row, so `SELECT <literal> FROM t` returns one constant-valued row per source table row, and the synthesized `literal_null` item behind a LIMIT barrier SHALL emit one single-column row per admitted row so the outer `COUNT(*)` counts exactly the rows the inner LIMIT admits (issue #205)
* *AND* a literal the translator cannot render, or one whose declared EMITS type is not a valid Exasol UDF EMITS output type (see the decline scenario below), SHALL fall back to projecting the underlying columns and let Exasol evaluate the select list, the same correctness backstop the scalar select-list path uses

### Scenario: Projected constant whose declared EMITS type Exasol rejects declines to the full base row

* *GIVEN* a row-scan `pushdown` request whose select list contains a rendered literal or scalar item whose declared result type in `selectListDataTypes` is `TIMESTAMP WITH LOCAL TIME ZONE` (e.g. a `literal_timestamp_utc` constant, which the translator renders successfully but whose declared type Exasol rejects as a UDF EMITS output type, sqlCode 22002)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL push a rendered select-list item as a positional `Expr` ONLY when its declared EMITS type is a valid Exasol UDF EMITS output type, so an item declared `TIMESTAMP WITH LOCAL TIME ZONE` SHALL decline to the full-base-row fallback — the same path an untranslatable CAST takes — rather than emit an EMITS clause that fails at scan time
* *AND* projected `TIMESTAMP WITH LOCAL TIME ZONE` constants SHALL remain unsupported — they hit the full-base-row fallback and Exasol post-processes the select list — an accurately-scoped tracked exception, `(#218)`

### Scenario: Projected literal with an ORDER BY on an unprojected column declines to the full base row

* *GIVEN* a row-scan `pushdown` request whose select list projects only literal/constant items and whose `orderBy` sorts on a source column absent from that projection (e.g. `SELECT 1 FROM t ORDER BY name LIMIT 5`), which the adapter cannot serve as a bounded top-N
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL project the full base row for this shape so the declined-ORDER-BY wrapper's outer `ORDER BY` resolves against emitted columns, and MUST NOT emit a narrowed literal-only projection whose declined-ORDER-BY wrapper references a column the scan no longer emits
* *AND* this SHALL preserve the pre-fix behavior for this unsupported shape (a well-formed declined-ORDER-BY wrapper) rather than introduce a distinct scan-time failure mode
