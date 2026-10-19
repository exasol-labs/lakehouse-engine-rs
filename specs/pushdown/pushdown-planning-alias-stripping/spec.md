# Feature: Pushdown Planning, Alias Stripping

Removes the per-column `tableAlias` that Exasol stamps on a single-table query with an aliased `FROM`, before the adapter renders the request into the DataFusion scan-driving SQL. The scan relation exposes only bare column names, so an alias-qualified reference would not resolve.

## Background

* When a query aliases its table (`FROM CUSTOMER c`), Exasol stamps `tableAlias:"C"` on every `column` node, including columns the user wrote unqualified.
* The adapter strips the alias once, at the single-table entry point, immediately after the join classifier reports the request is not a join. Every downstream single-table render site then renders bare names.
* The `crates/vs-expression` translator always honors a present `tableAlias`, so stripping is the single-table caller's job.
* The unified qualified fallback path (the N = 1 case of `pushdown-joins/pushdown-planning-join-fallback`) re-qualifies every column from its `tableName` and its own subquery alias, never from `tableAlias`.

## Scenarios

### Scenario: Single-table pushdown strips each column's table alias so the scan resolves bare names

* *GIVEN* a single-table (non-join) `pushdown` request whose `from` aliases the table (e.g. `FROM customer c`), so Exasol stamps a `tableAlias` on every `column` node — including nodes the user wrote unqualified (an unqualified `WHERE c_custkey <= 3` under `FROM customer c` still carries `tableAlias`)
* *AND* the request may take any single-table shape the spike proved leaks the alias: row-scan projection, a scalar expression over a column in the select list (e.g. `c.C_CUSTKEY + 1`), single-group aggregate, grouped aggregate, or an ordered top-N
* *WHEN* the adapter plans the single-table push, after the join classifier reports the request is not a join
* *THEN* the adapter SHALL remove every `column` node's `tableAlias` from the pushdown-request subtrees it renders into the DataFusion scan-driving SQL — the filter predicate, the projected select-list expressions, the GROUP BY keys, the HAVING, the ORDER BY keys, and every aggregate argument — before rendering them, so each column renders as a BARE double-quoted name (`"C_CUSTKEY"`) that resolves against the single scan relation
* *AND* the adapter MUST NOT render any such column as an alias-qualified name (`"C"."C_CUSTKEY"`), which does not resolve against the single scan relation
* *AND* the adapter SHALL leave each `column` node's `tableName` intact, so alias removal does not disturb any reader that keys on `tableName`

### Scenario: Single-table alias stripping leaves file pruning, the qualified fallback, and the no-alias path undisturbed

* *GIVEN* the single-table alias stripping applied at the single-table push entry
* *WHEN* the adapter plans a single-table `pushdown` request
* *THEN* the alias-stripped filter SHALL also drive Iceberg file pruning, so pruning reads bare column names; because pruning only narrows which files are opened, the returned result set SHALL be unchanged
* *AND* the unified qualified fallback path (the N = 1 case of `pushdown-joins/pushdown-planning-join-fallback`, taken for a multi-`COUNT(DISTINCT)` or non-numeric grouped shape) SHALL be unaffected by alias stripping, because it re-qualifies every column from its `tableName` and its own subquery alias, never from Exasol's `tableAlias`
* *AND* a single-table request whose `from` does NOT alias the table SHALL produce byte-identical scan-driving SQL to before this change, because no `tableAlias` is present to remove
