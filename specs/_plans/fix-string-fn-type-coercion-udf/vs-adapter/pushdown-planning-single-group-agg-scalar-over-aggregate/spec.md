# Feature: Pushdown Planning — Single-Group Scalar-Over-Aggregate Select Items

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A single-group scalar over an aggregate of a string function decomposes on both paths

* *GIVEN* the single-group request `SELECT LENGTH(MAX(UPPER(c_custkey))) FROM customer` over an integer column `c_custkey`
* *WHEN* the adapter builds the pushdown, once with files remaining and once with every file pruned
* *THEN* the non-empty path SHALL decompose the item with the inner aggregate argument rendered `upper(exa_to_varchar("C_CUSTKEY"))` and the outer `LENGTH` rendered over the merged value in the Exasol dialect
* *AND* the empty path SHALL re-classify the same node and render one shape-correct row, and `empty_scalar_over_aggregate_literal` SHALL NOT panic on either of its `.expect` calls
<!-- /DELTA:NEW -->
