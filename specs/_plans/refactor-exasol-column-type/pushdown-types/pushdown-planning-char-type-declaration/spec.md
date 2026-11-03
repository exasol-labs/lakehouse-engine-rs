# Feature: Pushdown Planning, CHAR Type Declaration

The adapter declares a pushed-down result column as Exasol `CHAR(n)` when Exasol declared that ordinal `CHAR`, so Exasol's type checker accepts the pushdown. This feature covers how the shared type derivation renders a `CHAR` type: the ASCII suffix, the 2,000-character cap, constant projections, wrapper `CAST` targets, and `MIN`/`MAX` partial columns.

<!-- DELTA:CHANGED -->
## Background

* `exasol_type_from_json` reads an Exasol `dataType` JSON object into the Exasol type the pushdown response renders, both in the query-side `EMITS` clause and in outer-wrapper `CAST` targets.
* No Iceberg or Arrow source type maps to `CHAR`, since Iceberg `string` maps to `VARCHAR`. A `CHAR` type reaches the adapter only as an Exasol-computed expression result in `selectListDataTypes`, never as an `involvedTables[].columns` base-column type.
* Exasol declares a string-literal expression `CHAR` when every branch has the same length, and `VARCHAR` otherwise.
* Exasol's `CHAR` maximum length is 2,000 characters. `CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`.
* The adapter appends ` ASCII` when the `dataType` JSON's `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` adds no suffix.
* Three Exasol-parsed wrapper paths take their select-list column types from `vs-expression`, not from `exasol_type_from_json`: the N-scan unaccelerated join wrapper, the qualified single-table aggregate fallback, and the grouped-merge scalar-over-aggregate wrapper. All three render a cast target through `render_cast_target` in the Exasol dialect. In these wrappers Exasol groups and evaluates DISTINCT natively over the padded `CHAR` value.
* `spec.common.group_keys` is the only DataFusion-side grouping-equality position a `CHAR` type can reach. A bare string literal is classified `Constant`, not a group key, and is already exactly `n` characters wide.
* An aggregate over an expression argument can carry a `CHAR` declared type. `col_type_for` returns the declared type verbatim, and `validate_agg_col_types` gates only SUM and the STDDEV/VARIANCE family on a numeric type.
* `pushdown-types/pushdown-planning-char-type-declaration-padding` covers `CHAR` projection and group-key values.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: A CHAR-declared type renders as CHAR

* *GIVEN* a `dataType` JSON object `{"type":"CHAR","size":20,"characterSet":"UTF8"}`, the shape Exasol sends for an explicit `CAST(<col> AS CHAR(20))` select-list ordinal
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(20)`
* *AND* the adapter MUST NOT render `VARCHAR(20)`
* *AND* the match on the `type` field SHALL be case-insensitive, so both `"CHAR"` and `"char"` resolve, mirroring the existing lowercase-normalized dispatch
