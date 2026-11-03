# Feature: Create Virtual Schema, Declaration Details

The `createVirtualSchema` declaration flattens multi-level namespaces into table names, uppercases identifiers with the full Unicode fold, resolves the declared timestamp precision from one `database_version()` read per request, and serializes a `TIMESTAMP(p)` column as `fractionalSecondsPrecision` JSON.

<!-- DELTA:CHANGED -->
## Background

* `str::to_uppercase` is full Unicode case mapping, not a one-to-one fold. It maps `ß` to `SS`. `flatten_table_name` applies the same `to_uppercase` to the table name, so both halves of a declared identifier follow one rule, the one `vs-adapter/create-virtual-schema` declares for column names.
* The `ß`-to-`SS` expansion is a deliberate trade-off. An Iceberg column `straße` is queryable only as `STRASSE`, and `"STRAßE"` resolves against no declared column. Two Iceberg columns that differ only in that expansion would both declare `STRASSE`, and the column path has no collision check. Table names have one: `flatten_table_name`'s output feeds the `__`-collision check.
* `scan-types/type-mapping-timestamp-precision` owns which timestamp declaration each Exasol version gets. This feature owns where the version is read and how the answer travels. The type-mapping module stays free of `UdfContext`, so the read happens at the adapter edge and the answer travels as a plain value.
* `cluster_nodes_from_context` is the precedent. It reads `ctx.node_count()` once at the adapter edge and passes a plain `usize` onward. `ctx.database_version()` follows the same shape, inline in `handle_create_virtual_schema` with no forwarding wrapper.
* `handle_create_virtual_schema` already holds the `UdfContext`. It calls `build_listing_virtual_tables(configured_ns, listing)`, which takes one new plain precision parameter and passes it on to `column_source_type_to_exasol`.
* The scan UDF entry point reads no version. Its `EMITS` types come from the pushdown request's own `dataType` JSON and `ScanSpec::emit_exa_types`.
* The `dataType` field that carries a timestamp's precision is `fractionalSecondsPrecision`. Exasol uses `precision` only for `DECIMAL` and `INTERVAL`.
* `exasol_type_from_json` already reads `fractionalSecondsPrecision`. Exasol echoes it in the pushdown request's `involvedTables` column `dataType`, so the `EMITS` clause and emit-boundary coercion follow without further change.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision

* *GIVEN* a resolved Exasol type `TIMESTAMP(p)` for an integer precision `p` in 0-9, the shape the version gate now produces for a catalog timestamp column
* *WHEN* `exasol_type_to_json` converts that type into the response's column `dataType` object
* *THEN* it SHALL return `{"type": "timestamp", "fractionalSecondsPrecision": p}`, reading the field name `fractionalSecondsPrecision`, the same name its inverse `exasol_type_from_json` reads, and MUST NOT emit a `precision` field, which Exasol uses only for `DECIMAL` and `INTERVAL`
* *AND* it MUST NOT declare a `TIMESTAMP(p)` column as `{"type": "varchar", "size": 2000000}`, a silently wrong column type rather than a rejected request
* *AND* a bare `TIMESTAMP` type SHALL keep returning `{"type": "timestamp"}` with NO `fractionalSecondsPrecision` field, so the 8.x arm's declaration is byte-identical to the one recorded today
* *AND* `TIMESTAMP WITH LOCAL TIME ZONE` SHALL keep returning `{"type": "timestamp", "withLocalTimeZone": true}` with no precision field, and its inverse `exasol_type_from_json` SHALL read `withLocalTimeZone` before any precision field
* *AND* every other type a catalog column declares SHALL keep its recorded `dataType` object byte-identical: `BOOLEAN`, `DOUBLE PRECISION`, `DATE`, every `DECIMAL(p,s)`, and every `VARCHAR(n)`
* *AND* a `CHAR(n)` type or an `ASCII` character type, which no catalog column declares, SHALL serialize to the `dataType` object that `exasol_type_from_json` reads back as the same type
* *AND* Exasol SHALL declare the resulting virtual column as `TIMESTAMP(6)` in `SYS.EXA_ALL_COLUMNS` and echo `fractionalSecondsPrecision` back on the pushdown request's `involvedTables` column `dataType`, both captured against a live Exasol 2025.x instance rather than asserted from documentation
<!-- /DELTA:CHANGED -->
