# Feature: Pushdown Planning, String Function Argument Type Coercion

The adapter type-checks the string-position arguments of every pushed-down Exasol string function. It passes string arguments through, rewraps DATE arguments in an explicit CAST-to-VARCHAR, rewraps DECIMAL arguments in the `decimal_to_varchar_exasol` node, and declines pushdown for every other resolvable type so Exasol evaluates the expression natively.

## Background

* Exasol converts a numeric or DATE argument to VARCHAR implicitly before applying a string function. DataFusion does not, so a pushed-down string function over a non-string column fails the scan (`Function 'upper' requires String, but received Int64`, SQL state 22002).
* The guard is `string_function_arg_type_guard` in the adapter (`pushdown/support.rs`), because `vs-expression` is a syntactic translator with no column-type context. A `column` node carries no `dataType` on the wire. The adapter reads column Exasol types from `involvedTables[0].columns` through `extract_all_column_types`, which uppercases column names.
* The governed functions are `CONCAT`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `LENGTH`, `OCTET_LENGTH`, `UNICODE`, `INSTR`, and `LOCATE`. `CHR` and `UNICODECHR` are excluded because their sole argument is an integer codepoint.
* String-position argument indices are per function, because `SUBSTR(str, start, length)` and `LPAD(str, length, pad)` also take numeric arguments that must not become text.
* The per-function argument table has three outcomes: not governed (leave the node, never decline), coerce (a set of string-position indices), and decline (governed but unrenderable at this arity). `INSTR` and `LOCATE` with more than two arguments are the decline case, because `vs-expression` renders only `args[0]` and `args[1]`.
* The guard returns `Option<Json>`: `Some(tree)` renders the possibly rewritten tree, `None` declines. It recurses through the shared post-order rewrite primitive over the curated child-bearing fields `expressions`, `arguments`, `results`, `expression`, `pattern`, `left`, `right`, `basis`. A filter-side string function sits under a comparison predicate (`UPPER(c) = 'X'` has the function under `left`).
* A WHERE-clause decline routes the request to the qualified single-table wrapper, which renders the original predicate tree as its own `WHERE` (`pushdown/pushdown-declined-filter-self-apply`). A select-list decline sets the full-base-row fallback flag, and Exasol computes the item over the returned columns.
* The select-list path is shared with the broadcast join: `render_broadcast_join` calls `project_columns` against both legs' columns, keyed by (relation alias, column) in leg order, and `joins/mod.rs`'s empty-side path calls it against the union of every involved table's columns. A decline there projects every column of that universe.
* The guard also runs on the broadcast join's combined WHERE filter and the N-scan fallback's per-leg WHERE filter; see `pushdown-joins/pushdown-planning-join-filter-type-coercion`.
* An Exasol integer is `DECIMAL(p,0)` on the wire, so the DECIMAL branch covers integer columns. The trim is a no-op at scale 0.
* The DECIMAL branch reuses the `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper of `pushdown-types/pushdown-planning-decimal-string-format`. The DATE branch reuses the `function_scalar_cast` shape of `guard_like_subject`.
* The DATE CAST matches Exasol only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`), which is also DataFusion's `Date32`-to-`Utf8` form.
* The decline branch is a correctness fix, not only a crash fix. The Iceberg spec assigns no text form to `boolean`, `double`, or `timestamp`. Exasol renders BOOLEAN as `TRUE`/`FALSE` and DataFusion as `true`/`false`, and Exasol's TIMESTAMP text is space-separated where DataFusion's is `T`-separated.
* `pushdown-types/pushdown-planning-string-fn-type-coercion-composition` covers this guard's order relative to `like_subject_type_guard` and `rewrite_decimal_stringifications`.
* Out of scope: a string-position argument that is a literal or computed expression, the grouped-aggregate render path (every `groupBy` element and non-aggregate select item), the aggregate-argument render path, `CHR`/`UNICODECHR` over a non-numeric argument, and rendering of `INSTR`'s optional third and fourth arguments and `LOCATE`'s optional third argument, which the translator drops.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A string-position DATE column argument is wrapped in an explicit CAST to VARCHAR

* *GIVEN* a `pushdown` request whose select list or filter carries a governed string `function_scalar` whose string-position argument is a bare `column` node, for example `LOWER(l_shipdate)`
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the scan spec
* *THEN* the scan spec SHALL render that argument as `CAST(<col> AS VARCHAR)`, the form a DATE LIKE subject takes (`pushdown-types/pushdown-planning-like-type-coercion`)
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR conversion under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 text form both engines render for the Iceberg `date` primitive
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from that default the pushed-down result MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format, an accepted tracked exception, not a silent gap
<!-- /DELTA:CHANGED -->
