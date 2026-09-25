# Feature: Pushdown Planning — Expression-Argument Aggregates

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-expression-aggregate/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-expression-aggregate/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An aggregate over a string function of a non-string column is pushed down

* *GIVEN* issue #227's repros over an integer column: `SELECT MAX(UPPER(c_custkey)) FROM customer` and `SELECT COUNT(UPPER(c_custkey)) FROM customer`, and a grouped request that selects `MAX(UPPER(c_custkey))` and carries `HAVING MAX(UPPER(c_custkey)) > '5'`
* *WHEN* the adapter builds the aggregate pushdown
* *THEN* each aggregate SHALL carry `upper(exa_to_varchar("C_CUSTKEY"))` as its rendered argument, and the HAVING reference SHALL match the selected aggregate by that text
* *AND* the requests SHALL decompose into the single-group or the grouped partial/merge scan
* *AND* the returned values SHALL equal native Exasol evaluation
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An aggregate over a string function resolves one shape on the empty-result path

* *GIVEN* the single-group request `SELECT MAX(UPPER(c_custkey)) FROM customer` whose file list is fully pruned
* *WHEN* the adapter builds the empty-result response
* *THEN* the shared classifier SHALL resolve the same `SingleGroupAgg` shape the non-empty path resolves, and the response SHALL be one shape-correct row holding NULL
* *AND* the empty path SHALL match the aggregate against its plan by the same rendered argument text, so no `.expect` on that match panics
<!-- /DELTA:NEW -->
