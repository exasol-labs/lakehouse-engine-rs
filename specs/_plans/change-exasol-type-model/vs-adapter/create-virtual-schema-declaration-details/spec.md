# Feature: Create Virtual Schema, Declaration Details

The `createVirtualSchema` declaration flattens multi-level namespaces into table names, uppercases identifiers with the full Unicode fold, resolves the declared timestamp precision from one `database_version()` read per request, and serializes a `TIMESTAMP(p)` column as `fractionalSecondsPrecision` JSON.

## Background

* `str::to_uppercase` is full Unicode case mapping, not a one-to-one fold. It maps `ß` to `SS`. `flatten_table_name` applies the same `to_uppercase` to the table name, so both halves of a declared identifier follow one rule, the one `vs-adapter/create-virtual-schema` declares for column names.
* The `ß`-to-`SS` expansion is a deliberate trade-off. An Iceberg column `straße` is queryable only as `STRASSE`, and `"STRAßE"` resolves against no declared column. Two Iceberg columns that differ only in that expansion would both declare `STRASSE`, and the column path has no collision check. Table names have one: `flatten_table_name`'s output feeds the `__`-collision check.
* `scan-types/type-mapping-timestamp-precision` owns which timestamp declaration each Exasol version gets. This feature owns where the version is read and how the answer travels. The type-mapping module stays free of `UdfContext`, so the read happens at the adapter edge and the answer travels as a plain value.
* `cluster_nodes_from_context` is the precedent. It reads `ctx.node_count()` once at the adapter edge and passes a plain `usize` onward. `ctx.database_version()` follows the same shape, inline in `handle_create_virtual_schema` with no forwarding wrapper.
* `handle_create_virtual_schema` already holds the `UdfContext`. It calls `build_listing_virtual_tables(configured_ns, listing)`, which takes one new plain precision parameter and passes it on to `column_source_type_to_exasol`.
* The scan UDF entry point reads no version. Its `EMITS` types come from the pushdown request's own `dataType` JSON and `ScanSpec::emit_exa_types`.
* The `dataType` field that carries a timestamp's precision is `fractionalSecondsPrecision`. Exasol uses `precision` only for `DECIMAL` and `INTERVAL`.
* `exasol_type_from_json` already reads `fractionalSecondsPrecision`. Exasol echoes it in the pushdown request's `involvedTables` column `dataType`, so the `EMITS` clause and emit-boundary coercion follow without further change.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision

* *GIVEN* a catalog timestamp column that the adapter declares `TIMESTAMP(p)` for a precision `p` in 0-9
* *WHEN* the `createVirtualSchema` response declares the column's `dataType` object
* *THEN* the object SHALL be `{"type": "timestamp", "fractionalSecondsPrecision": p}` when `p` is not 3, and MUST NOT carry a `precision` field, which Exasol uses only for `DECIMAL` and `INTERVAL`
* *AND* a column of precision 3, which is Exasol's bare `TIMESTAMP`, SHALL be declared `{"type": "timestamp"}` with no `fractionalSecondsPrecision` field on every Exasol version
* *AND* the adapter MUST NOT declare a `TIMESTAMP(p)` column as `{"type": "varchar", "size": 2000000}`
* *AND* every other type a catalog column declares SHALL keep its recorded `dataType` object byte-identical: `BOOLEAN`, `DOUBLE PRECISION`, `DATE`, every `DECIMAL(p,s)`, and every `VARCHAR(n)`
<!-- /DELTA:CHANGED -->
