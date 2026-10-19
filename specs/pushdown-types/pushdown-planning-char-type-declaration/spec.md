# Feature: Pushdown Planning, CHAR Type Declaration

The adapter declares a pushed-down result column as Exasol `CHAR(n)` when Exasol declared that ordinal `CHAR`, so Exasol's type checker accepts the pushdown. This feature covers how the shared type derivation renders a `CHAR` type: the ASCII suffix, the 2,000-character cap, constant projections, wrapper `CAST` targets, and `MIN`/`MAX` partial columns.

## Background

* `exasol_type_from_json` maps an Exasol `dataType` JSON object to the type string used in the pushdown response, both in the query-side `EMITS` clause and in outer-wrapper `CAST` targets.
* No Iceberg or Arrow source type maps to `CHAR`, since Iceberg `string` maps to `VARCHAR`. A `CHAR` type reaches the adapter only as an Exasol-computed expression result in `selectListDataTypes`, never as an `involvedTables[].columns` base-column type.
* Exasol declares a string-literal expression `CHAR` when every branch has the same length, and `VARCHAR` otherwise.
* Exasol's `CHAR` maximum length is 2,000 characters. `CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`.
* The adapter appends ` ASCII` when the `dataType` JSON's `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` adds no suffix.
* Three Exasol-parsed wrapper paths take their select-list column types from `vs-expression`, not from `exasol_type_from_json`: the N-scan unaccelerated join wrapper, the qualified single-table aggregate fallback, and the grouped-merge scalar-over-aggregate wrapper. All three render a cast target through `render_cast_target` in the Exasol dialect. In these wrappers Exasol groups and evaluates DISTINCT natively over the padded `CHAR` value.
* `spec.common.group_keys` is the only DataFusion-side grouping-equality position a `CHAR` type can reach. A bare string literal is classified `Constant`, not a group key, and is already exactly `n` characters wide.
* An aggregate over an expression argument can carry a `CHAR` declared type. `col_type_for` returns the declared type verbatim, and `validate_agg_col_types` gates only SUM and the STDDEV/VARIANCE family on a numeric type.
* `pushdown-types/pushdown-planning-char-type-declaration-padding` covers `CHAR` projection and group-key values.

## Scenarios

### Scenario: A CHAR-declared type renders as CHAR

* *GIVEN* a `dataType` JSON object `{"type":"CHAR","size":20,"characterSet":"UTF8"}`, the shape Exasol sends for an explicit `CAST(<col> AS CHAR(20))` select-list ordinal
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(20)`
* *AND* the adapter MUST NOT render `VARCHAR(20)`
* *AND* the match on the `type` field SHALL be case-insensitive, so both `"CHAR"` and `"char"` resolve, mirroring the existing lowercase-normalized dispatch

### Scenario: A CHAR-declared ASCII type carries the ASCII suffix

* *GIVEN* a `dataType` JSON object `{"type":"CHAR","size":3,"characterSet":"ASCII"}`, the shape Exasol sends for an equal-length CASE-of-string-literals ordinal
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(3) ASCII`
* *AND* the `characterSet` comparison SHALL be case-insensitive, mirroring the VARCHAR rule
* *AND* the same object without a `characterSet` field SHALL render bare `CHAR(3)`, which Exasol reads as its UTF8 default

### Scenario: A CHAR size above Exasol's maximum is capped at 2,000

* *GIVEN* a `dataType` JSON object of type `CHAR` whose `size` exceeds 2,000
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `CHAR(2000)`
* *AND* the adapter MUST NOT apply VARCHAR's 2,000,000 cap to a CHAR type, because Exasol rejects any CHAR length above 2,000 with `specified length too long for char type - maximum is 2000`
* *AND* a `CHAR` object with no `size` field SHALL still render a valid CHAR length rather than an out-of-range one

### Scenario: A bare string-literal group-key projection casts to CHAR

* *GIVEN* a grouped-aggregate `pushdown` request whose `selectList` carries a `literal_string` item alongside an aggregate
* *AND* whose `selectListDataTypes` declares that literal's ordinal `{"type":"CHAR","size":1,"characterSet":"ASCII"}`
* *WHEN* the adapter builds that ordinal's constant projection for the outer wrapper
* *THEN* the emitted expression SHALL be `CAST('X' AS CHAR(1) ASCII)`
* *AND* the same declared type SHALL be resolved through the shared type-derivation seam whichever path Exasol's request routes to, so the emitted type matches on the grouped, single-group, and row-scan paths alike
* *AND* the constant SHALL need no padding, because it is classified as a `Constant` projection rather than a group key, has no DataFusion-side counterpart expression, and is already exactly `n` characters wide

### Scenario: A CAST-to-CHAR item inside an Exasol-parsed wrapper declares a CHAR column

* *GIVEN* an aggregate `pushdown` request reaching any of the THREE Exasol-parsed wrapper paths — the qualified single-table fallback (for example two `COUNT(DISTINCT …)` items alongside a `CAST(<col> AS CHAR(20))` select item), the N-scan unaccelerated join wrapper carrying the same select item, or the grouped-merge scalar-over-aggregate wrapper carrying a `CAST(SUM(<col>) AS CHAR(20))` item
* *AND* whose `selectListDataTypes` declares that ordinal `{"type":"CHAR","size":20,...}`
* *WHEN* the wrapper's SELECT list is rendered
* *THEN* that item SHALL render a length-qualified `CHAR` CAST target, carrying the ` ASCII` suffix exactly when the node's own `dataType` declares `characterSet` `ASCII`
* *AND* it MUST NOT render `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20)` and which also strips the value's blank padding, nor a bare length-less `VARCHAR` or `CHAR`, which Exasol's parser rejects outright
* *AND* all three wrapper paths SHALL be corrected by the single shared `render_cast_target` Exasol-dialect case, so no per-wrapper rendering rule is introduced
* *AND* a NESTED CHAR CAST — `CAST(CAST(SUM(<col>) AS CHAR(20) ASCII) AS CHAR(20) ASCII)` on the grouped-merge path — SHALL render `CHAR(20) ASCII` at BOTH levels, because the renderer recurses into itself
* *AND* Exasol SHALL perform the grouping and DISTINCT evaluation natively over the padded `CHAR` value in these wrappers, so no adapter-side padding is required on this path

### Scenario: A MIN or MAX over a CHAR-typed expression declares a CHAR partial column

* *GIVEN* a `group_by` `pushdown` request whose `selectList` carries `MIN(CAST(<col> AS CHAR(20)))` — an aggregate over an expression argument, not a bare column — with that ordinal's `selectListDataTypes` entry declaring `{"type":"CHAR","size":20}`
* *WHEN* the adapter builds the partial `EMITS` clause and the outer merge SELECT
* *THEN* the partial column SHALL be declared `"PARTIAL_min_0" CHAR(20)`, because the declared type is carried through verbatim for an expression-argument aggregate and MIN/MAX are not gated on a numeric type
* *AND* the outer merge item SHALL cast the merged value to `CHAR(20)`
* *AND* the pushdown MUST NOT be declined for this shape, because `CHAR(n)` is both a valid `EMITS` output type and a valid `CAST` target

### Scenario: A VARCHAR-declared type is unaffected

* *GIVEN* a `dataType` JSON object `{"type":"VARCHAR","size":10,"characterSet":"UTF8"}`, the shape Exasol sends for a GROUP BY on a genuine VARCHAR base column
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `VARCHAR(10)`, unchanged by the new CHAR branch
* *AND* the `VARCHAR(2000000)` default SHALL remain the fallback when no declared type is locatable for an ordinal
* *AND* the `boolean`, `decimal`, `double`, `date`, and `timestamp` branches SHALL keep their current renderings
* *AND* a VARCHAR-declared group key SHALL receive no blank padding at all, because VARCHAR carries no fixed-width equality semantics

### Scenario: A CHAR-typed LIKE subject keeps pushing down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` over a bare `column` whose type in `involvedTables[0].columns` resolves to `CHAR(n)`
* *WHEN* the LIKE subject type guard dispatches on that type (see `pushdown-types/pushdown-planning-like-type-coercion`)
* *THEN* the guard SHALL classify `CHAR(n)` as a string subject and leave the predicate unchanged
* *AND* the guard MUST NOT decline the filter, because a CHAR subject needs no coercion
* *AND* this SHALL be a forward-compatibility guard only, because no Iceberg or Arrow source type produces a CHAR base column today
