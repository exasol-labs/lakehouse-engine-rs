# Feature: Pushdown Planning — Literal/Constant Select-List Projection

Extends pushdown planning (`vs-adapter/pushdown-planning`) so a select list made up of bare
literal/constant items — not only column references or scalar expressions — is projected
positionally into the scan-driving query instead of triggering the full-base-row fallback,
and records the shape where a literal-only projection still declines to the full base
row (an EMITS-incompatible declared type). A literal-only select list with an `ORDER BY` on
an unprojected column is now covered by `vs-adapter/pushdown-planning-order-by-capability`'s
hidden-sort-key-column rule instead of the full-base-row fallback.

## Background

* Literal select-list items are rendered through the same `crates/vs-expression` translator
  and the same positional, `selectListDataTypes`-typed `Expr`-projection mechanism as the
  scalar select-list expression path (`vs-adapter/pushdown-planning-selectlist-expressions`);
  this feature covers the literal-specific triggers and decline shapes.
* Iceberg spec compliance: checked, not engaged. This delta changes only how the adapter
  projects literal select-list items and declines to the full base row; it touches no
  manifest, schema-resolution, field-id, or type-mapping surface, so no normative Iceberg
  requirement applies and there is no deviation to fix or track.

## Scenarios

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
