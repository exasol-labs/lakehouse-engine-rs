# Feature: Pushdown Planning, Expression-Argument Aggregates

The adapter pushes an aggregate whose argument is a scalar expression over table columns, such as `SUM(LENGTH(L_COMMENT))`, into the same shard-associative partial/merge plan as a bare-column aggregate.

## Background

* The `vs-expression` translator (`render_expression`) renders the aggregate argument. An argument it cannot render makes the adapter decline the aggregate pushdown and route the request to the qualified single-table wrapper.
* `COUNT(expr)`, `SUM(expr)`, `MIN(expr)`, `MAX(expr)`, and `AVG(expr)` decompose as their bare-column forms do. `COUNT(*)` has no argument.
* An expression has no single source column, so the partial and merge column types come from the aggregate item's declared type in the top-level `selectListDataTypes` array.

## Scenarios

### Scenario: SUM over a scalar expression argument is pushed down

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a query whose select list is a supported aggregate over a renderable scalar expression, e.g. `SELECT SUM(LENGTH(L_COMMENT)) FROM {vs_table}`
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL render the aggregate's argument expression to a DataFusion SQL fragment via the VS expression translator and carry that fragment (not a bare column identifier) in the aggregate plan
* *AND* the scan UDF SHALL compute the node-local partial aggregate over that rendered expression per shard, and the outer wrapper SHALL merge the per-shard partials into the final result exactly as for the bare-column form
* *AND* the merged result SHALL equal the same aggregate evaluated over all rows on a single node

### Scenario: Expression-argument partial and merge column types come from the declared aggregate type

* *GIVEN* a `pushdown` request carrying `SUM(expr)`, `MIN(expr)`, `MAX(expr)`, `AVG(expr)`, or `COUNT(expr)` over a renderable expression
* *WHEN* the adapter builds the partial EMITS clause and the outer merge SELECT
* *THEN* the adapter SHALL derive each partial column's Exasol type from the aggregate item's declared type in `selectListDataTypes` at that select-list ordinal, rather than from a source column's type
* *AND* a `SUM(expr)` whose declared result type is a DECIMAL SHALL widen its partial column to `DECIMAL(36,s)` to avoid mid-merge overflow, and a `SUM(expr)` over a floating result SHALL use `DOUBLE PRECISION`, matching the bare-column SUM discipline
* *AND* the outer merge expression SHALL be wrapped in `CAST(<expr> AS <declared_type>)` so the pushdown output column type matches Exasol's positional `selectListDataTypes` validation

### Scenario: Aggregate over an untranslatable argument falls back to row scanning

* *GIVEN* a `pushdown` request whose aggregate argument is an expression the VS expression translator cannot render
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL route the request to the qualified single-table wrapper, whose own rendered SQL computes the aggregate over a materialized sharded raw scan, and the scan spec MUST NOT carry an aggregates field
* *AND* the adapter MUST NOT emit a partial/merge plan referencing an argument it could not render soundly

### Scenario: Bare-column aggregates continue to decompose unchanged

* *GIVEN* a `pushdown` request whose aggregates all take bare column arguments (`COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`, and the STDDEV/VARIANCE family)
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL produce the partial EMITS clause, per-shard partial SQL, and outer merge SELECT of the bare-column decomposition, with no rendered argument expression in the aggregate plan
* *AND* the merged result SHALL equal the same aggregate evaluated over all rows on a single node

### Scenario: SUM over a two-column binary-arithmetic argument is pushed down

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a query whose select list is `SUM(col_a OP col_b)` where `OP` is one of `*`, `+`, `-`, `/` and `col_a`, `col_b` are two table columns of compatible numeric types, e.g. `SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue FROM {vs_table} WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01' AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24`
* *WHEN* Exasol sends the corresponding `pushdown` request, carrying the binary-arithmetic expression as the aggregate's argument node
* *THEN* the adapter SHALL render the two-column binary-arithmetic argument to a DataFusion SQL fragment via the VS expression translator and carry that fragment in the aggregate plan, exactly as for a single-column expression argument
* *AND* the scan UDF SHALL compute the node-local partial `SUM` over the rendered two-column arithmetic expression per shard, and the outer wrapper SHALL merge the per-shard partials into the final result
* *AND* the emitted `pushdown` scan spec SHALL contain an `aggregates` field with the rendered argument expression, and MUST NOT fall back to a raw row-scan of the underlying columns
* *AND* the merged result SHALL equal `SUM(col_a OP col_b)` evaluated over all rows on a single node

### Scenario: A DECIMAL two-column product widens its partial column from the declared SUM result type

* *GIVEN* a `pushdown` request carrying `SUM(col_a * col_b)` where both operands are DECIMAL columns (e.g. `DECIMAL(15,2) * DECIMAL(15,2)`)
* *WHEN* the adapter builds the partial EMITS clause and the outer merge SELECT for that aggregate
* *THEN* the adapter SHALL derive the partial column's Exasol type from the SUM item's declared result type in `selectListDataTypes` at that select-list ordinal, never from either source column's type
* *AND* when that declared result type is a DECIMAL the adapter SHALL widen the partial column to `DECIMAL(36,s)` (preserving scale `s`, capping precision at Exasol's maximum 36) so per-shard partial sums of the product do not overflow mid-merge
* *AND* the derived partial and merge types SHALL remain within Exasol's `DECIMAL(p<=36, s<=36)` limits

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
