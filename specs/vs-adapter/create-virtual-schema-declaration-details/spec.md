# Feature: Create Virtual Schema, Declaration Details

The `createVirtualSchema` declaration flattens multi-level namespaces into table names, uppercases identifiers with the full Unicode fold, resolves the declared timestamp precision from one `database_version()` read per request, and serializes a `TIMESTAMP(p)` column as `fractionalSecondsPrecision` JSON.

## Background

* `str::to_uppercase` is full Unicode case mapping, not a one-to-one fold. It maps `ß` to `SS`. `flatten_table_name` applies the same `to_uppercase` to the table name, so both halves of a declared identifier follow one rule, the one `vs-adapter/create-virtual-schema` declares for column names.
* The `ß`-to-`SS` expansion is a deliberate trade-off. An Iceberg column `straße` is queryable only as `STRASSE`, and `"STRAßE"` resolves against no declared column. Two Iceberg columns that differ only in that expansion would both declare `STRASSE`, and the column path has no collision check. Table names have one: `flatten_table_name`'s output feeds the `__`-collision check.
* `scan-types/type-mapping-timestamp-precision` owns which timestamp declaration each Exasol version gets. This feature owns where the version is read and how the answer travels. The type-mapping module stays free of `UdfContext`, so the read happens at the adapter edge and the answer travels as a plain value.
* `cluster_nodes_from_context` is the precedent. It reads `ctx.node_count()` once at the adapter edge and passes a plain `usize` onward. `ctx.database_version()` follows the same shape, inline in `handle_create_virtual_schema` with no forwarding wrapper.
* `handle_create_virtual_schema` already holds the `UdfContext`. It calls `build_listing_virtual_tables(configured_ns, listing)`, which takes one new plain precision parameter and passes it on to `column_source_type_to_exasol`.
* The scan UDF entry point reads no version. Its `EMITS` types come from the pushdown request's own `dataType` JSON and `ScanSpec::emit_exa_types`.
* `exasol_type_to_json` matches the string `TIMESTAMP` exactly and has no `TIMESTAMP(p)` arm, so a `TIMESTAMP(6)` string would fall through to the catch-all and declare `{"type": "varchar", "size": 2000000}`. The field name is `fractionalSecondsPrecision`. Exasol uses `precision` only for `DECIMAL` and `INTERVAL`.
* `exasol_type_from_json` already reads `fractionalSecondsPrecision`. Exasol echoes it in the pushdown request's `involvedTables` column `dataType`, so the `EMITS` clause and emit-boundary coercion follow without further change.

## Scenarios

### Scenario: Multi-level Iceberg namespaces flatten deterministically into Exasol table names

* *GIVEN* a configured namespace `prod.finance` containing an Iceberg table `orders` and a child namespace `prod.finance.eu` containing a table `orders`
* *WHEN* Exasol sends the `createVirtualSchema` request naming namespace `prod.finance`
* *THEN* the adapter SHALL name the first virtual table `ORDERS` and the second `EU__ORDERS`, flattening only the namespace segments below the configured namespace using `__` and uppercasing the result
* *AND* the adapter SHALL record each virtual table's original-cased Iceberg identifier in the `name` member of that table's identifier note (its table-level `adapterNotes`), so each flattened Exasol name resolves back to the correct table
* *AND* when two distinct Iceberg identifiers that the listing loads flatten to the same Exasol name (a `__` collision) the adapter SHALL return an error naming the colliding Exasol table name rather than silently dropping or overwriting a table

### Scenario: A non-ASCII Iceberg table and column name stay queryable end to end

* *GIVEN* a live Exasol instance, an Iceberg REST catalog, and an Iceberg table whose TABLE name and one of whose COLUMN names are both the non-ASCII identifier `straße`, that column an Iceberg `string` column whose seeded values carry distinguishable prefixes, alongside an `id` column
* *AND* a virtual schema created over that namespace through a real `createVirtualSchema`
* *WHEN* an Exasol user queries that table and that column through the virtual schema
* *THEN* `SYS.EXA_ALL_COLUMNS` and `SYS.EXA_ALL_TABLES` SHALL report both identifiers as `STRASSE`, pinning the full-Unicode `ß`-to-`SS` expansion as observed behavior rather than as documentation
* *AND* an unquoted `SELECT COUNT(*)` over the table SHALL return the seeded row count, so the uppercased table name still resolves through the table's identifier note back to the original-cased Iceberg identifier `straße` and the scan reaches the real table
* *AND* an unquoted projection of the column SHALL return the seeded values in full, so the uppercased column name still maps back to the Iceberg field's own casing at scan time
* *AND* a `LIKE` predicate over that column SHALL return the correct subset of rows
* *AND* the adapter-generated pushdown SQL for that same `LIKE` query SHALL carry the predicate over `"STRASSE"` rather than declining it, so the type-rewrite guards resolved the column's Exasol type from a `col_types` entry whose name came through this fold

### Scenario: createVirtualSchema reads the database version once and threads the resolved precision

* *GIVEN* a `createVirtualSchema` (or `refresh`/`setProperties`) request reaching `handle_create_virtual_schema` with a live `UdfContext`
* *WHEN* the adapter builds the response's per-column `dataType` declarations
* *THEN* the adapter SHALL read `UdfContext::database_version()` EXACTLY ONCE per request and resolve it, through the single owner `scan-types/type-mapping-timestamp-precision` specifies, into a plain `Copy` value naming the timestamp precision
* *AND* that plain value SHALL be threaded as a new parameter into `build_listing_virtual_tables` and onward to `column_source_type_to_exasol`, and the `UdfContext` MUST NOT be threaded into `types/mapping.rs` in its place, so the type-mapping module keeps performing no I/O and reading no ambient state
* *AND* the read SHALL happen inline in `handle_create_virtual_schema` alongside the other per-request resolutions and MUST NOT be wrapped in a helper whose whole body forwards the one call
* *AND* the resolved precision MUST NOT be recorded in `adapterNotes`, persisted, or round-tripped: it is re-derived from the handshake on every request, exactly as the cluster node count is, so a virtual schema created against one engine version and queried after an upgrade declares the upgraded engine's precision on its next `refresh`
* *AND* the scan UDF entry point MUST NOT read `database_version()`: its `EMITS` types come from the pushdown request's own `dataType` JSON, so a second read there would give one decision two owners
* *AND* no other per-column behavior SHALL change, the column-name fold, its single owner, the `__`-flattened table name, each table's identifier note, the 404 skip, and every non-timestamp declared type stay byte-identical

### Scenario: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision

* *GIVEN* a resolved Exasol type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9, the shape the version gate now produces for a catalog timestamp column
* *WHEN* `exasol_type_to_json` converts that string into the response's column `dataType` object
* *THEN* it SHALL return `{"type": "timestamp", "fractionalSecondsPrecision": p}`, reading the field name `fractionalSecondsPrecision`, the same name its inverse `exasol_type_from_json` reads, and MUST NOT emit a `precision` field, which Exasol uses only for `DECIMAL` and `INTERVAL`
* *AND* it MUST NOT fall through to the catch-all VARCHAR arm for a `TIMESTAMP(p)` string, because that arm would declare a timestamp column as `{"type": "varchar", "size": 2000000}`, a silently wrong column type rather than a rejected request
* *AND* a bare `TIMESTAMP` string SHALL keep returning `{"type": "timestamp"}` with NO `fractionalSecondsPrecision` field, so the 8.x arm's declaration is byte-identical to the one recorded today
* *AND* `TIMESTAMP WITH LOCAL TIME ZONE` SHALL keep returning `{"type": "timestamp", "withLocalTimeZone": true}`, matched before any precision logic, so no precision-aware variant of that arm is introduced, its inverse short-circuits the same way
* *AND* every other type string SHALL keep its recorded `dataType` object byte-identical, including `BOOLEAN`, `DOUBLE PRECISION`, `DATE`, every `DECIMAL(p,s)`, and every `VARCHAR(n)`/`CHAR(n)` form
* *AND* Exasol SHALL declare the resulting virtual column as `TIMESTAMP(6)` in `SYS.EXA_ALL_COLUMNS` and echo `fractionalSecondsPrecision` back on the pushdown request's `involvedTables` column `dataType`, both captured against a live Exasol 2025.x instance rather than asserted from documentation
