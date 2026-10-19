# Feature: Pushdown Planning, Literal/Constant Select-List Projection

Projects bare literal or constant select-list items positionally into the scan-driving query instead of falling back to the full base row. A literal whose declared EMITS type Exasol rejects routes to the qualified single-table wrapper.

## Background

* The adapter renders literal items through the same `crates/vs-expression` translator and the same positional, `selectListDataTypes`-typed `Expr` projection as the scalar select-list path (`pushdown/pushdown-planning-selectlist-expressions`).
* Exasol validates the pushdown response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000`. A full-base-row response therefore fails the query instead of falling back.
* `pushdown-capabilities/pushdown-planning-capability-extensions` owns the routing of a declined item to the qualified single-table wrapper.

## Scenarios

### Scenario: Projected literal select-list item is pushed into the scan-driving query

* *GIVEN* a row-scan or inner-join `pushdown` request that carries NO aggregate and NO GROUP BY, whose select list contains one or more bare literal/constant items, any of `literal_null`, `literal_bool`, `literal_exactnumeric`, `literal_double`, `literal_string`, `literal_date`, `literal_timestamp` (e.g. `SELECT 1 FROM t`, `SELECT 1, name, 1 FROM t`, the constant-folded `SELECT 2+3` Exasol sends as a single `literal_exactnumeric`, OR the single-element `[{"type":"literal_null"}]` select list Exasol synthesizes for its documented Virtual-Schema-API "selectList is an empty array: select any one column or expression" contract when a LIMIT barrier sits between an outer aggregate and the derived table it wraps, for example the inner derived-table request behind `SELECT COUNT(*) FROM (SELECT c_custkey FROM t LIMIT 5)`, which arrives on the wire as `"selectList":[{"type":"literal_null"}]` with `"selectListDataTypes":[{"type":"BOOLEAN"}]`, a one-element array carrying a `literal_null` item, NOT a JSON `null` and NOT an empty `[]` array, issue #205)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render each literal select-list item through the `crates/vs-expression` translator into a POSITIONAL `Expr` projection item, one projection item per select-list item, typed from the parallel top-level `selectListDataTypes` array, exactly as the `function_scalar` select-list branch already does, and MUST NOT trigger the full-base-row fallback that emits every base column and yields the column-count mismatch Exasol rejects ("Expected number of columns is 1 but pushdown query has N", issues #190 and #205)
* *AND* the emitted scan's column arity SHALL equal the query's select-list arity, so two structurally identical literal items, such as the two `1` items in `SELECT 1, name, 1`, SHALL each occupy their own projected position and MUST NOT be collapsed into one
* *AND* each projected literal SHALL be evaluated once per scanned source row, so `SELECT <literal> FROM t` returns one constant-valued row per source table row, and the synthesized `literal_null` item behind a LIMIT barrier SHALL emit one single-column row per admitted row so the outer `COUNT(*)` counts exactly the rows the inner LIMIT admits (issue #205)
* *AND* a literal the translator cannot render, or one whose declared EMITS type is not a valid Exasol UDF EMITS output type, SHALL route the request to the qualified single-table wrapper (see the decline scenario below) rather than fall back to the full base row, an invalid pushdown response for a delegated select list, not a correctness backstop

### Scenario: Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper

* *GIVEN* a row-scan `pushdown` request whose select list contains a rendered literal or scalar item whose declared result type in `selectListDataTypes` is `TIMESTAMP WITH LOCAL TIME ZONE` (e.g. a `literal_timestamputc`/`literal_timestamp_utc` constant, which the translator renders successfully but whose declared type Exasol rejects as a UDF EMITS output type, sqlCode 22002)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL push a rendered select-list item as a positional `Expr` ONLY when its declared EMITS type is a valid Exasol UDF EMITS output type, so an item declared `TIMESTAMP WITH LOCAL TIME ZONE` SHALL route the whole request to the qualified single-table wrapper instead, the same routing `pushdown-capabilities/pushdown-planning-capability-extensions` specifies in full, which this feature cross-references rather than restates
* *AND* projected `TIMESTAMP WITH LOCAL TIME ZONE` constants SHALL NOT "remain unsupported" as a permanent tracked exception: `(#218)` is a real fix, not a documented gap, the value Exasol computes natively is reproduced via the qualified wrapper's `CAST(CONVERT_TZ(…) AS TIMESTAMP WITH LOCAL TIME ZONE)` rendering, and the full-base-row response this scenario previously described is an INVALID pushdown response Exasol rejects with SQL state `04000`, not a correct-but-unaccelerated fallback
