<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning, CHAR Type Declaration

The adapter declares a pushed-down result column as Exasol `CHAR(n)` when Exasol declared that ordinal `CHAR`, so Exasol's type checker accepts the pushdown. This feature covers how a `CHAR` type renders: the ASCII suffix, constant projections, wrapper `CAST` targets, and `MIN`/`MAX` partial columns.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* Exasol declares a string-literal expression `CHAR` when every branch has the same length, and `VARCHAR` otherwise. No Iceberg or Arrow source type maps to `CHAR`, so a `CHAR` type reaches the adapter only in `selectListDataTypes`, never as a base-column type.
* Exasol's `CHAR` maximum length is 2,000 characters. `CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`.
* The adapter renders a `CHAR` type, in a scan `EMITS` clause or in a wrapper `CAST` target, as `CHAR(n)`, and appends ` ASCII` when the `dataType`'s `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` adds no suffix.
* Three Exasol-parsed wrapper paths render their `CAST` targets in the Exasol dialect: the N-scan unaccelerated join wrapper, the qualified single-table aggregate fallback, and the grouped-merge scalar-over-aggregate wrapper. In these wrappers Exasol groups and evaluates DISTINCT natively over the padded `CHAR` value.
* An aggregate over an expression argument can carry a `CHAR` declared type, and only SUM and the STDDEV/VARIANCE family require a numeric argument type. `pushdown-types/pushdown-planning-char-type-declaration-padding` covers `CHAR` projection and group-key values.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: A CHAR size above Exasol's maximum is capped at 2,000

* *GIVEN* a `dataType` JSON object of type `CHAR` whose `size` exceeds 2,000
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(2000)`
* *AND* the adapter MUST NOT apply VARCHAR's 2,000,000 cap to a CHAR type, because Exasol rejects any CHAR length above 2,000 with `specified length too long for char type - maximum is 2000`
* *AND* a `CHAR` object with no `size` field SHALL still render a valid CHAR length rather than an out-of-range one
<!-- /DELTA:REMOVED -->

<!-- DELTA:CHANGED -->
### Scenario: A MIN or MAX over a CHAR-typed expression declares a CHAR partial column

* *GIVEN* a `group_by` `pushdown` request whose `selectList` carries `MIN(CAST(<col> AS CHAR(20)))`, an aggregate over an expression argument, not a bare column, with that ordinal's `selectListDataTypes` entry declaring `{"type":"CHAR","size":20}`
* *WHEN* the adapter builds the partial `EMITS` clause and the outer merge SELECT
* *THEN* the partial column SHALL be declared `"PARTIAL_min_0" CHAR(20)`, because an expression-argument aggregate carries its declared type and MIN and MAX accept any argument type
* *AND* the outer merge item SHALL be the uncast `MIN("PARTIAL_min_0")`, because the partial column already carries the declared `CHAR(20)` (`pushdown-aggregates/pushdown-planning-single-group-agg`)
* *AND* the pushdown MUST NOT be declined for this shape, because `CHAR(n)` is both a valid `EMITS` output type and a valid `CAST` target
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A VARCHAR-declared type is unaffected

* *GIVEN* a `dataType` JSON object `{"type":"VARCHAR","size":10}`, the shape Exasol sends for a GROUP BY on a genuine VARCHAR base column
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `VARCHAR(10)`
* *AND* a `BOOLEAN`, `DECIMAL`, `DOUBLE`, or `DATE` `dataType` SHALL render as `BOOLEAN`, `DECIMAL(p,s)`, `DOUBLE PRECISION`, or `DATE`, and a `TIMESTAMP` `dataType` per `pushdown/pushdown-planning`
* *AND* a VARCHAR-declared group key SHALL receive no blank padding, because VARCHAR carries no fixed-width equality semantics
<!-- /DELTA:CHANGED -->
