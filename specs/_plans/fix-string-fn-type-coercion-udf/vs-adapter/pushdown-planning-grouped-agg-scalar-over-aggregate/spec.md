# Feature: Pushdown Planning — Grouped Scalar-Over-Aggregate Select Items

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A scalar over an aggregate of a string function keeps the conversion inside the scan

* *GIVEN* a grouped request over an integer column `c_custkey` that selects `MAX(UPPER(c_custkey))` and `LENGTH(MAX(UPPER(c_custkey)))`, and carries `HAVING LENGTH(MAX(UPPER(c_custkey))) > 1`
* *WHEN* the adapter builds the grouped pushdown
* *THEN* the grouped scan spec SHALL carry the aggregate argument `upper(exa_to_varchar("C_CUSTKEY"))`
* *AND* the outer `LENGTH` and the HAVING SHALL render in the Exasol dialect over the merged partial column, and the merge wrapper SQL MUST NOT contain `exa_to_varchar`
* *AND* the returned values SHALL equal native Exasol evaluation
<!-- /DELTA:NEW -->
