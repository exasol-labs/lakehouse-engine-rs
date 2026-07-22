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
  `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the regexp scalar functions, and the divergent date
  functions do not and stay unadvertised.

## Scenarios

### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`, `CAST(id AS VARCHAR(2000000))`, or `CASE WHEN qty > 0 THEN 1 ELSE 0 END`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS`
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node — recognizing the distinct `function_scalar_cast`, `function_scalar_extract`, and `function_scalar_case` node types Exasol emits for CAST, EXTRACT, and CASE (including CASE-expanded NULLIF/ZEROIFNULL), not only the generic `function_scalar` node — to a DataFusion SQL fragment using the VS expression translator (raising mode), and SHALL carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions rather than triggering the full-base-row fallback that yields a column count Exasol rejects
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type, where result types are read from the parallel top-level `selectListDataTypes` array in the pushdown request
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result

### Scenario: HAVING predicate is pushed into the grouped scan plan

* *GIVEN* a grouped aggregate `pushdown` request carrying a `having` predicate over the grouped aggregates and/or group keys
* *AND* the adapter advertises `AGGREGATE_HAVING`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render the HAVING predicate to a DataFusion SQL fragment using the same VS expression translator path used for WHERE predicates
* *AND* the adapter SHALL apply the rendered HAVING predicate only in the OUTER wrapper SQL that merges the per-shard partial-aggregate rows, never inside the per-shard partial scan (a per-shard HAVING would discard groups that only meet the threshold after merge)
* *AND* a HAVING predicate the adapter cannot translate SHALL be omitted from the wrapper SQL and retained by Exasol as a correctness backstop rather than producing an incorrect result

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
* *AND* the adapter SHALL rely on Exasol to apply the `ORDER BY` it retains over the returned rows, exactly as it already retains a `LIMIT` and a `HAVING` it pushed as a correctness backstop

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
