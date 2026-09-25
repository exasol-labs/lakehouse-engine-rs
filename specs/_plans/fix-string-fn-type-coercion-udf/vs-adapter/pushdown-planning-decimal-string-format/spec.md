<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — Decimal String Formatting

Makes every pushed-down DECIMAL→string conversion reproduce Exasol's shortest form. Exasol trims
trailing scale zeros when it converts a DECIMAL to text (`2912.00`→`'2912'`,
`-272.60`→`'-272.6'`). DataFusion's `CAST(decimal AS VARCHAR)` and its implicit decimal→utf8
coercion keep the full declared scale, so their text differs from Exasol's (issue #211). The
DataFusion dialect routes every string-converted
argument through `exa_to_varchar`, which trims a DECIMAL by its Arrow type
(`datafusion-scan/scan-execution-exa-to-varchar`). The trim therefore holds on every surface that
renders DataFusion SQL: the select list, the WHERE filter, GROUP BY keys, aggregate arguments, and
both join filters. It holds for a computed DECIMAL argument as well as a bare column (issue #227).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The stringifications this feature covers are the string CAST (`CAST(<x> AS VARCHAR/CHAR)`) and
  the implicit conversions of `CONCAT` (Exasol's `||`) and `LENGTH`, the two string functions that
  convert silently rather than fail. Which arguments are string-converted is owned by
  `sql-comprehension/vs-expression-translator-string-conversion`.
* The trim is the `Decimal128` conversion of `exa_to_varchar`.
* An Exasol integer arrives as `DECIMAL(p,0)`. The trim is a no-op on it.
* Exasol renders `a || b || c` as nested `CONCAT(a, CONCAT(b, c))`. The renderer wraps the
  string-converted arguments of every level, so a DECIMAL reached only through an inner `CONCAT` is
  converted.
* A DECIMAL in a non-stringifying position — arithmetic, a comparison operand, a CAST to a
  non-string target — is not a string-converted argument and renders unwrapped.
* The Iceberg table spec defines `decimal(P,S)` as "Fixed-point decimal; precision P, scale S" with
  "Scale is fixed, precision must be 38 or less" (Primitive Types). The Delta protocol's
  `§ Primitive Types` defines `decimal` as a "signed decimal number with fixed precision (maximum
  number of digits) and scale (number of digits on right side of dot)". A stored value's trailing
  zeros in that scale are an artifact of S, so trimming them changes presentation only, matching
  Exasol's own conversion.
* Deliberate Exasol target-type trade-off: a decimal of precision 37 or 38, which both specs allow
  and Exasol's DECIMAL does not, is declared `VARCHAR(2000000)` and keeps every scale digit
  (`datafusion-scan/scan-execution-exa-to-varchar`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Explicit CAST of a DECIMAL column to VARCHAR renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `function_scalar_cast` item with target `dataType` `VARCHAR` or `CHAR` whose single argument is a bare `DECIMAL(p,s)` column
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the projected SQL SHALL render `CAST(exa_to_varchar(<col>) AS VARCHAR)`, so `10.50` projects as `10.5` and `30.00` as `30`
* *AND* `project_columns` SHALL route the `function_scalar_cast` item through the expression translator, NOT into the full-row fallback
* *AND* the projected EMITS column type SHALL remain the item's declared `selectListDataTypes` text type
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation

* *GIVEN* a `pushdown` request whose select list carries a `CONCAT` over a DECIMAL column, as a direct argument or through an inner `CONCAT` from chained `||` (Exasol renders `id || '-' || c_decimal_a` as `CONCAT("ID", CONCAT('-', "C_DECIMAL_A"))`)
* *WHEN* the adapter builds the scan-spec projection
* *THEN* every `CONCAT` level SHALL render each of its arguments through `exa_to_varchar`, so `id || '-' || c_decimal_a` over `30.00` yields `4-30`, not `4-30.00`
* *AND* a computed DECIMAL argument such as `c_decimal_a * 2` SHALL also yield trimmed text
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Implicit LENGTH over a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose select list carries a `LENGTH` item whose single argument is a bare DECIMAL column
* *WHEN* the adapter builds the scan-spec projection
* *THEN* the projected SQL SHALL render `character_length(exa_to_varchar(<col>))`, so the length is measured over the trimmed text
* *AND* `LENGTH` over `30.00` SHALL yield `2`, not `5`, equal to Exasol's native `LENGTH` over the DECIMAL
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: WHERE-clause stringification of a DECIMAL column renders the trimmed form

* *GIVEN* a `pushdown` request whose filter stringifies a DECIMAL column through a string CAST, `CONCAT`, or `LENGTH` — for example `LENGTH(c_acctbal) > 5`, issue #211's COUNT-divergence repro
* *WHEN* the adapter builds the DataFusion scan-spec filter for the single-table path, the broadcast join's combined filter, or an N-scan fallback side's per-leg filter
* *THEN* the rendered filter SHALL wrap each such argument in `exa_to_varchar`, so the predicate matches the trimmed text and the pushed count equals native Exasol evaluation
* *AND* the raw filter tree forwarded to Iceberg file pruning SHALL stay unchanged
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A DECIMAL column in a non-stringifying filter context is left unchanged

* *GIVEN* a `pushdown` request whose filter references a bare DECIMAL column in a non-stringifying position — for example `c_decimal_a > 5` or `c_decimal_a * 2 = 10`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the renderer SHALL leave the column unwrapped, and the rendered filter SHALL carry no `exa_to_varchar` call, because the column is not a string-converted argument
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged

* *GIVEN* a `pushdown` request whose select list or filter stringifies a bare `column` whose Exasol type in `involvedTables[0].columns` is NOT `DECIMAL` (for example `VARCHAR`, `DATE`, or `DOUBLE`)
* *WHEN* the adapter builds the scan spec
* *THEN* `rewrite_decimal_stringifications` SHALL leave the stringification unchanged, injecting no `decimal_to_varchar_exasol` node, because only DECIMAL stringification diverges through this fix
* *AND* a `function_scalar_cast` to VARCHAR or CHAR over such a column SHALL render exactly as it did before this change end to end, because no other guard governs that node shape
* *AND* a `CONCAT` or `LENGTH` over such a column MAY instead be rewritten or declined at the wired surfaces by `vs-adapter/pushdown-planning-string-fn-type-coercion`, which governs every string function's string-position arguments and runs first — a DATE argument is rewrapped as `CAST(<col> AS VARCHAR)` and a `DOUBLE`/`BOOLEAN`/`TIMESTAMP` argument declines to native Exasol evaluation, so the end-to-end rendering of `CONCAT`/`LENGTH` over a non-DECIMAL column is that feature's contract, NOT this scenario's
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: CAST, CONCAT, or LENGTH over a non-DECIMAL column converts by its type

* *GIVEN* a `pushdown` request whose select list or filter stringifies a bare column whose Exasol type is not DECIMAL — `VARCHAR`, `DATE`, `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP`
* *WHEN* the adapter builds the scan spec
* *THEN* a VARCHAR or CHAR column SHALL convert unchanged and a DATE column SHALL convert to `YYYY-MM-DD` text
* *AND* a DOUBLE, BOOLEAN, or TIMESTAMP column SHALL decline to native Exasol evaluation per `vs-adapter/pushdown-planning-string-fn-type-coercion`, including under a string CAST
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: A stringified computed expression is left unchanged as a tracked exception

* *GIVEN* a `pushdown` request that stringifies a NON-bare-column argument via `CAST(... AS VARCHAR/CHAR)`, `CONCAT`, or `LENGTH` — for example the computed expression `c_acctbal * 2` — in either the select list or the filter
* *WHEN* the adapter builds the scan spec
* *THEN* the rewriter SHALL leave the stringification unchanged, because the argument's Exasol type is not resolvable from `involvedTables[0].columns`
* *AND* a DECIMAL-valued computed argument MAY still render with divergent DataFusion formatting — an accepted, accurately-scoped tracked exception (#223), not a silent gap
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A DECIMAL stringification in a GROUP BY key or an aggregate argument renders the trimmed form

* *GIVEN* issue #227's repros over a `DECIMAL(12,2)` column holding `0.50` and `100.10`: `SELECT CAST(c_acctbal AS VARCHAR(20)) k, COUNT(*) ... GROUP BY CAST(c_acctbal AS VARCHAR(20))` and `SELECT MAX(CAST(c_acctbal AS VARCHAR(20))) ...`
* *WHEN* the adapter pushes the grouped or the single-group aggregate down
* *THEN* the group keys SHALL be `0.5` and `100.1` and the maximum SHALL be `100.1`, equal to native Exasol, NOT `0.50` and `100.10`
* *AND* the select-list key SHALL match its GROUP BY key by rendered text, so the request decomposes into the grouped partial/merge scan
<!-- /DELTA:NEW -->
