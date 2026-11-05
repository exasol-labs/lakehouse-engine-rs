# Feature: Pushdown Planning, Literal/Constant Select-List Projection

Projects bare literal or constant select-list items positionally into the scan-driving query instead of falling back to the full base row. A literal whose declared EMITS type Exasol rejects routes to the qualified single-table wrapper.

## Background

* The adapter renders literal items through the same `crates/vs-expression` translator and the same positional, `selectListDataTypes`-typed `Expr` projection as the scalar select-list path (`pushdown/pushdown-planning-selectlist-expressions`).
* Exasol validates the pushdown response positionally against the request's `selectList` and rejects a column-count mismatch with SQL state `04000`. A full-base-row response therefore fails the query instead of falling back.
* `pushdown-capabilities/pushdown-planning-capability-extensions` owns the routing of a declined item to the qualified single-table wrapper.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A NULL select item in an Exasol-rendered wrapper keeps its declared type

* *GIVEN* a request the adapter serves with the qualified single-table wrapper or the N-scan join wrapper
* *AND* whose select list carries a bare NULL literal, the form in which Exasol sends `CAST(NULL AS <type>)`, with `selectListDataTypes` declaring `<type>` at that ordinal, for example `SELECT id, CAST(NULL AS GEOMETRY) FROM vs.t`
* *WHEN* the adapter renders the wrapper's select list
* *THEN* the item SHALL render as `CAST(NULL AS <declared type>)`
* *AND* the query SHALL return NULL in a column of the declared type, as native Exasol does
* *AND* the adapter MUST NOT render a bare `NULL`, whose type Exasol infers on its own and then rejects with `Data type mismatch` against the declared type
<!-- /DELTA:NEW -->
