# Feature: Pushdown Planning, CHAR Type Declaration

The adapter declares a pushed-down result column as Exasol `CHAR(n)` when Exasol declared that ordinal `CHAR`, so Exasol's type checker accepts the pushdown. This feature covers how a `CHAR` type renders: the ASCII suffix, constant projections, wrapper `CAST` targets, and `MIN`/`MAX` partial columns.

## Background

* Exasol declares a string-literal expression `CHAR` when every branch has the same length, and `VARCHAR` otherwise. No Iceberg or Arrow source type maps to `CHAR`, so a `CHAR` type reaches the adapter only in `selectListDataTypes`, never as a base-column type.
* Exasol's `CHAR` maximum length is 2,000 characters (`CAST('a' AS CHAR(2001))` fails with `specified length too long for char type - maximum is 2000`), so a `CHAR` `dataType` above it fails the query (`pushdown/pushdown-planning`).
* The adapter renders a `CHAR` type, in a scan `EMITS` clause or in a wrapper `CAST` target, as `CHAR(n)`, and appends ` ASCII` when the `dataType`'s `characterSet` equals `ASCII` case-insensitively. A `UTF8` or absent `characterSet` adds no suffix.
* Three Exasol-parsed wrapper paths render their `CAST` targets in the Exasol dialect: the N-scan unaccelerated join wrapper, the qualified single-table aggregate fallback, and the grouped-merge scalar-over-aggregate wrapper. In these wrappers Exasol groups and evaluates DISTINCT natively over the padded `CHAR` value.
* An aggregate over an expression argument can carry a `CHAR` declared type, and only SUM and the STDDEV/VARIANCE family require a numeric argument type. `pushdown-types/pushdown-planning-char-type-declaration-padding` covers `CHAR` projection and group-key values.

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

### Scenario: A bare string-literal group-key projection casts to CHAR

* *GIVEN* a grouped-aggregate `pushdown` request whose `selectList` carries a `literal_string` item alongside an aggregate
* *AND* whose `selectListDataTypes` declares that literal's ordinal `{"type":"CHAR","size":1,"characterSet":"ASCII"}`
* *WHEN* the adapter builds that ordinal's constant projection for the outer wrapper
* *THEN* the emitted expression SHALL be `CAST('X' AS CHAR(1) ASCII)`
* *AND* the same declared type SHALL be resolved through the shared type-derivation seam whichever path Exasol's request routes to, so the emitted type matches on the grouped, single-group, and row-scan paths alike
* *AND* the constant SHALL need no padding, because it is classified as a `Constant` projection rather than a group key, has no DataFusion-side counterpart expression, and is already exactly `n` characters wide

### Scenario: A CAST-to-CHAR item inside an Exasol-parsed wrapper declares a CHAR column

* *GIVEN* an aggregate `pushdown` request reaching any of the THREE Exasol-parsed wrapper paths, the qualified single-table fallback (for example two `COUNT(DISTINCT …)` items alongside a `CAST(<col> AS CHAR(20))` select item), the N-scan unaccelerated join wrapper carrying the same select item, or the grouped-merge scalar-over-aggregate wrapper carrying a `CAST(SUM(<col>) AS CHAR(20))` item
* *AND* whose `selectListDataTypes` declares that ordinal `{"type":"CHAR","size":20,...}`
* *WHEN* the wrapper's SELECT list is rendered
* *THEN* that item SHALL render a length-qualified `CHAR` CAST target, carrying the ` ASCII` suffix exactly when the node's own `dataType` declares `characterSet` `ASCII`
* *AND* it MUST NOT render `VARCHAR({size})`, which Exasol rejects as `Data type mismatch ... Expected CHAR(20)` and which also strips the value's blank padding, nor a bare length-less `VARCHAR` or `CHAR`, which Exasol's parser rejects outright
* *AND* all three wrapper paths SHALL be corrected by the single shared `render_cast_target` Exasol-dialect case, so no per-wrapper rendering rule is introduced
* *AND* a NESTED CHAR CAST, `CAST(CAST(SUM(<col>) AS CHAR(20) ASCII) AS CHAR(20) ASCII)` on the grouped-merge path, SHALL render `CHAR(20) ASCII` at BOTH levels, because the renderer recurses into itself
* *AND* Exasol SHALL perform the grouping and DISTINCT evaluation natively over the padded `CHAR` value in these wrappers, so no adapter-side padding is required on this path

### Scenario: A MIN or MAX over a CAST to CHAR runs in the qualified wrapper

* *GIVEN* a `group_by` `pushdown` request whose `selectList` carries `MIN(CAST(<col> AS CHAR(20)))`
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route it to the qualified wrapper, because the scan does not blank-pad (`pushdown-types/pushdown-planning-scan-text-conversion`)
* *AND* the result SHALL equal native Exasol

### Scenario: A VARCHAR dataType renders as VARCHAR(n) and its group key is not padded

* *GIVEN* a `dataType` JSON object `{"type":"VARCHAR","size":10}`, the shape Exasol sends for a GROUP BY on a genuine VARCHAR base column
* *WHEN* the adapter derives that ordinal's Exasol type string
* *THEN* the adapter SHALL render `VARCHAR(10)`
* *AND* a `BOOLEAN`, `DECIMAL`, `DOUBLE`, or `DATE` `dataType` SHALL render as `BOOLEAN`, `DECIMAL(p,s)`, `DOUBLE PRECISION`, or `DATE`, and a `TIMESTAMP` `dataType` per `pushdown/pushdown-planning`
* *AND* a VARCHAR-declared group key SHALL receive no blank padding, because VARCHAR carries no fixed-width equality semantics

### Scenario: A CHAR-typed LIKE subject keeps pushing down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` over a bare `column` whose type in `involvedTables[0].columns` resolves to `CHAR(n)`
* *WHEN* the scan translator renders the filter
* *THEN* it SHALL treat `CHAR(n)` as text and keep the predicate in the scan unchanged
* *AND* this SHALL be a forward-compatibility guard only, because no Iceberg or Arrow source type produces a CHAR base column today
