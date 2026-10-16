# Feature: Pushdown Planning: Split-Aggregate Column Types

Every column a split aggregate emits and merges carries the exact type native Exasol gives it. The split aggregate is the single-group or grouped partial/merge plan, where each shard emits partial rows and an Exasol wrapper merges them. The adapter takes each type from an exact source. When no exact source exists, the adapter declines the split and routes the request to the qualified wrapper, where Exasol evaluates the expression itself (#476).

## Background

* Exasol sends `selectListDataTypes`, one type per select-list item. A nested node, such as the `MAX(UPPER(s))` inside `LENGTH(MAX(UPPER(s)))`, and an unprojected `groupBy` node carry no type, except a `CAST` node, whose `dataType` is its target type.
* A guessed type breaks the merge. A text, date, or timestamp value fails the emit or the merge cast, a `DOUBLE` partial over an exact expression loses digits, and a group key staged as text splits values that are equal in Exasol, such as `-0.0` and `0.0`.
* The exact type of an expression comes from one resolver, in this order: the node's own `dataType`, the `selectListDataTypes` entry when the node is a select-list item, and the column type from `involvedTables` when the node is a bare column.
* Native Exasol types an aggregate as follows (verified live): `COUNT` is `DECIMAL(18,0)`; `AVG`, `STDDEV`, and `VARIANCE` and their variants are `DOUBLE`; `SUM` over `DECIMAL(p,s)` is `DECIMAL(min(p+11, 36), s)` and over `DOUBLE` is `DOUBLE`; `MIN` and `MAX` take the argument's type.
* The adapter does not infer the type of an arbitrary expression. Exasol's typing rules depend on literal values, the session, and the Exasol version, so an adapter copy of them would drift.
* A constant group key forms one group whatever type carries it, so it is staged as `VARCHAR(2000000)`.
* Iceberg spec compliance: checked, not engaged. This feature changes only the Exasol types the adapter declares for partial and group-key columns. It reads no manifest, resolves no field id, and maps no source type (Apache Iceberg table spec, "Schemas and Data Types").

## Scenarios

### Scenario: A nested aggregate over an expression declines the split

* *GIVEN* a single-group or grouped `pushdown` request whose select list nests `MIN`, `MAX`, or `SUM` over an expression inside a scalar, such as `LENGTH(MAX(UPPER(s)))` or `ROUND(SUM(a * b), 6)`
* *AND* no top-level select-list item is the same aggregate
* *AND* the argument is not a `CAST`
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL route it to the qualified wrapper
* *AND* the result SHALL equal native Exasol over the same rows, including every decimal digit
* *AND* the same request on every path SHALL match native Exasol: every file pruned, files scanned with no matching row, and matching rows

### Scenario: A nested aggregate takes the result type Exasol gives it

* *GIVEN* a split aggregate whose select list nests an aggregate inside a scalar
* *WHEN* the adapter types that aggregate's partial and merged columns
* *THEN* a top-level select-list item that is the same aggregate SHALL supply the type
* *AND* otherwise `COUNT`, `AVG`, `STDDEV`, and `VARIANCE` SHALL take their fixed native types
* *AND* otherwise `MIN`, `MAX`, and `SUM` SHALL take the native type derived from their argument's exact type, for example `TIMESTAMP(3)` for `MIN(CAST(d AS TIMESTAMP))`
* *AND* the request SHALL keep the split path

### Scenario: A merged aggregate under a scalar keeps its native type

* *GIVEN* a split aggregate whose select list, `HAVING`, or `ORDER BY` applies a scalar to an aggregate, such as `SUM(a) / 2` over a `DECIMAL(15,2)` column
* *WHEN* the adapter renders the merge
* *THEN* the merged aggregate SHALL be cast to its native type before the scalar applies, as in `CAST(SUM("PARTIAL_sum_0") AS DECIMAL(26,2)) / 2`
* *AND* the scalar SHALL therefore take the type and value native Exasol computes, although the partial `SUM` column is widened to `DECIMAL(36,s)`

### Scenario: A group key travels with its exact type

* *GIVEN* a grouped split aggregate
* *WHEN* the adapter builds the scan's `EMITS` clause and the merge
* *THEN* each `GK_i` column SHALL be declared with the key's exact type, for example `DECIMAL`, `DOUBLE`, `VARCHAR`, `CHAR(n)`, `DATE`, `TIMESTAMP(p)`, or `BOOLEAN`
* *AND* the scan UDF SHALL emit each key coerced to that type
* *AND* the merge SHALL group on, select, and sort by the typed `GK_i` columns, with no cast
* *AND* values equal in Exasol SHALL form one group across shards, so `-0.0` and `0.0` merge
* *AND* an `ORDER BY` on a key SHALL sort by the key's value, for an unprojected key too

### Scenario: An unprojected bare-column group key takes its column type

* *GIVEN* a grouped request whose `groupBy` holds a bare column absent from the select list, such as `SELECT COUNT(*) FROM t GROUP BY z`
* *WHEN* the adapter types the key
* *THEN* the key SHALL take the column type from `involvedTables`
* *AND* the request SHALL keep the split path

### Scenario: An unprojected group key without an exact type declines the split

* *GIVEN* a grouped request whose `groupBy` holds an expression that is absent from the select list, is not a `CAST`, and is not a constant, such as `SELECT COUNT(*) FROM t GROUP BY UPPER(s)`
* *WHEN* the adapter types the key
* *THEN* the adapter SHALL route the request to the qualified wrapper

### Scenario: An all-pruned grouped request types a text scalar over an aggregate

* *GIVEN* a grouped request whose select list holds a scalar over an aggregate declared `VARCHAR(2000000)`, such as `SELECT k, MAX(s) || '!' FROM t WHERE <prunes every file> GROUP BY k`
* *WHEN* the adapter builds the empty result
* *THEN* that item SHALL be rendered `CAST(NULL AS VARCHAR(2000000))`
* *AND* the query SHALL return zero rows, as native Exasol does
