# Feature: Pushdown Planning — Expression-Argument Aggregates

Extends single-group aggregate pushdown (`vs-adapter/pushdown-planning`) so a supported
aggregate whose argument is a scalar expression over table columns — not just a bare
column reference — is decomposed into the same shard-associative partial/merge plan. This
lets queries like `SUM(LENGTH(L_COMMENT))` push their scan and node-local aggregation into
DataFusion instead of forcing a full raw row-scan fallback that ships every projected
column to Exasol.

## Background

* The aggregate argument is rendered by the shared `crates/vs-expression` translator
  (`render_expression`), the same mechanism `detect_group_by_aggregates` already uses for
  GROUP BY keys; an argument the translator cannot render causes the adapter to decline the
  aggregate pushdown for that query and fall back to row scanning.
* Only the aggregate's argument shape changes: `COUNT(expr)`, `SUM(expr)`, `MIN(expr)`,
  `MAX(expr)`, and `AVG(expr)` decompose exactly as their bare-column forms do; `COUNT(*)`
  has no argument and is unaffected.
* The partial/merge column types for an expression-argument aggregate are read from the
  aggregate item's declared result type in the parallel top-level `selectListDataTypes`
  array, because there is no single source column whose Exasol type could be looked up.
* The merged result MUST equal the same aggregate evaluated over all rows on a single node.
* Credentials MUST NOT appear in any returned SQL or error message.

## Scenarios

### Scenario: SUM over a scalar expression argument is pushed down

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
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
* *THEN* the adapter SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field) so Exasol computes the aggregate on the returned rows using its own engine
* *AND* the adapter MUST NOT emit a partial/merge plan referencing an argument it could not render soundly

### Scenario: Bare-column aggregates continue to decompose unchanged

* *GIVEN* a `pushdown` request whose aggregates all take bare column arguments (`COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`, and the STDDEV/VARIANCE family)
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL produce the identical partial EMITS clause, per-shard partial SQL, and outer merge SELECT it produced before expression-argument support was added
* *AND* the merged result SHALL equal the same aggregate evaluated over all rows on a single node

<!-- DELTA:NEW -->
### Scenario: SUM over a two-column binary-arithmetic argument is pushed down

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list is `SUM(col_a OP col_b)` where `OP` is one of `*`, `+`, `-`, `/` and `col_a`, `col_b` are two table columns of compatible numeric types, e.g. `SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue FROM {vs_table} WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1995-01-01' AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24`
* *WHEN* Exasol sends the corresponding `pushdown` request, carrying the binary-arithmetic expression as the aggregate's argument node
* *THEN* the adapter SHALL render the two-column binary-arithmetic argument to a DataFusion SQL fragment via the VS expression translator and carry that fragment in the aggregate plan, exactly as for a single-column expression argument
* *AND* the scan UDF SHALL compute the node-local partial `SUM` over the rendered two-column arithmetic expression per shard, and the outer wrapper SHALL merge the per-shard partials into the final result
* *AND* the emitted `pushdown` scan spec SHALL contain an `aggregates` field with the rendered argument expression, and MUST NOT fall back to a raw row-scan of the underlying columns
* *AND* the merged result SHALL equal `SUM(col_a OP col_b)` evaluated over all rows on a single node

### Scenario: A DECIMAL two-column product widens its partial column from the declared SUM result type

* *GIVEN* a `pushdown` request carrying `SUM(col_a * col_b)` where both operands are DECIMAL columns (e.g. `DECIMAL(15,2) * DECIMAL(15,2)`)
* *WHEN* the adapter builds the partial EMITS clause and the outer merge SELECT for that aggregate
* *THEN* the adapter SHALL derive the partial column's Exasol type from the SUM item's declared result type in `selectListDataTypes` at that select-list ordinal — never from either source column's type
* *AND* when that declared result type is a DECIMAL the adapter SHALL widen the partial column to `DECIMAL(36,s)` (preserving scale `s`, capping precision at Exasol's maximum 36) so per-shard partial sums of the product do not overflow mid-merge
* *AND* the derived partial and merge types SHALL remain within Exasol's `DECIMAL(p<=36, s<=36)` limits
<!-- /DELTA:NEW -->
