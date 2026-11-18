# Feature: Pushdown Planning, Ordered Top-N with Constant Sort Keys

A constant `ORDER BY` key orders nothing, so the adapter leaves it out when it decides whether a query takes the per-shard bounded top-N of `pushdown-capabilities/pushdown-planning-topn`.

## Background

* Exasol sends a constant sort key, such as the `2` of `ORDER BY 2` over a constant select item, as a literal `order_by` entry.
* A query whose every sort key is a constant has no ordering, so it runs as a bare `LIMIT`.

## Scenarios

### Scenario: A constant sort key does not decide the bounded top-N path

* *GIVEN* a single-table row-scan query with `LIMIT n` whose `ORDER BY` mixes constant keys with bare projected column keys (`SELECT id, 1 FROM t ORDER BY 2 DESC, 1 LIMIT 3`)
* *WHEN* the adapter checks whether the query is an ordered top-N
* *THEN* the adapter SHALL check the non-constant keys only, so the query above runs as a per-shard bounded top-N on `id`
* *AND* the merged result SHALL equal the same query on a single node, ids 1, 2, 3

### Scenario: An ORDER BY of constants only runs as an unordered LIMIT

* *GIVEN* a single-table row-scan query whose every `ORDER BY` key is a constant, with `LIMIT n` and no offset (`SELECT id, 1 FROM t ORDER BY 2 LIMIT 3`)
* *WHEN* the adapter builds the pushdown SQL
* *THEN* the adapter SHALL plan the query as one without an `ORDER BY`, so each shard carries the row limit `n` and no sort, as for a bare `LIMIT`
* *AND* the query SHALL return `n` distinct rows of the table, because a constant ordering admits any `n` rows
