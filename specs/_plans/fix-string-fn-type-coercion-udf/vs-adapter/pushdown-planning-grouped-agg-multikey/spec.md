# Feature: Pushdown Planning — Multi-Key Grouped Aggregate Queries

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-grouped-agg-multikey/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-grouped-agg-multikey/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A string-function group key over a non-string column pushes down

* *GIVEN* issue #227's repros over an integer column: `SELECT UPPER(c_custkey), COUNT(*) FROM customer GROUP BY UPPER(c_custkey)` and `SELECT COUNT(*) FROM customer GROUP BY UPPER(c_custkey)`
* *WHEN* the adapter builds the grouped pushdown
* *THEN* the grouped scan spec SHALL carry the group key `upper(exa_to_varchar("C_CUSTKEY"))`, the select-list item SHALL match that key by rendered text, and the request SHALL decompose into the grouped partial/merge scan whether or not the key is selected
* *AND* a grouped `ORDER BY UPPER(c_custkey)` SHALL resolve to that key's output ordinal and keep the grouped pushdown
* *AND* the returned groups SHALL equal native Exasol evaluation
<!-- /DELTA:NEW -->
