# Feature: Create Virtual Schema — Declaration Details

Covers the identifier-casing and timestamp-precision details of the `createVirtualSchema` column
and table declaration that `vs-adapter/create-virtual-schema` does not itself carry: multi-level
namespace flattening and its `__`-collision guard, the full-Unicode uppercasing fold end-to-end
over a non-ASCII identifier, the single per-request `database_version()` read that resolves the
declared timestamp precision, and the `fractionalSecondsPrecision` JSON a `TIMESTAMP(p)`
declaration serializes to. Split out of `vs-adapter/create-virtual-schema` once that feature's
scenario count crossed this library's per-spec organization threshold.

## Background

* `str::to_uppercase` is FULL Unicode case mapping, not a one-to-one fold, and the difference is
  observable: it maps `ß` to `SS`, expanding one character into two. Rust's own `str::to_uppercase`
  documentation pins that example. `flatten_table_name` (`adapter/tables.rs:29`, folding at `:42`)
  applies the same `to_uppercase` to the table name, so both halves of a declared identifier go
  through one rule — the same rule `vs-adapter/create-virtual-schema`'s enumeration scenario
  declares for column names.
* The `ß`-to-`SS` expansion is a deliberate trade-off, not a gap. An Iceberg column `straße` is
  queryable ONLY as `STRASSE`; the `ß`-bearing form `"STRAßE"` resolves against no declared column.
  Two Iceberg columns in one table differing only in that expansion (`strasse` and `straße`) would
  both declare `STRASSE`, which the response would carry as a duplicate column name. No table-name
  equivalent exists, because `flatten_table_name`'s output feeds the `__`-collision check the
  multi-level-namespace scenario below already errors on — the column path has no such check.
* Apache Iceberg spec check: NOT implicated as a schema or type question. The spec's Schemas and
  Data Types section defines a `struct` field as carrying a "field name" string and constrains
  field IDs, not the SQL identifier casing a downstream engine uses to expose that name. The spec
  mandates no case-sensitivity rule for consumers, so uppercasing at declaration is an Exasol
  identifier-resolution decision rather than an Iceberg deviation.
* The non-ASCII round trip below is a LIVE E2E scenario, not a unit one, because the property under
  test is a round-trip through a real `createVirtualSchema` and a real query — exactly the class
  CLAUDE.md § Verification discipline requires be checked against a running Exasol instance rather
  than asserted from a capability list or from code inspection. The unit-level fold is already
  covered by `adapter/tables.rs`'s existing `flatten_table_name` casing tests.
* The non-ASCII fixture MUST live in its OWN Iceberg namespace, not in `e2e_lakehouse`. Every
  existing E2E virtual schema is created over `e2e_lakehouse`, so a table added there would appear
  in each of those suites' enumerations and could churn assertions those plans promise to leave
  untouched. A separate namespace makes the fixture invisible to them.
* **This delta is issue #359.** It adds TWO scenarios and AMENDS NO recorded clause. The first records
  the single `ctx.database_version()` read and how the resolved timestamp precision reaches the
  declaration pipeline. The second records the column `dataType` JSON a `TIMESTAMP(p)` declaration
  serializes to.
* **`datafusion-scan/type-mapping-timestamp-precision` owns WHICH declaration each version gets; this
  feature owns WHERE the version is read and HOW the answer travels.** The split matters because the
  type-mapping module must stay free of `UdfContext` — it reads no ambient state and does no I/O —
  so the context read has to happen at the adapter edge and the answer has to travel as a plain
  value.
* **`cluster_nodes_from_context` is the recorded precedent for this shape.** It reads
  `ctx.node_count()` once at the adapter edge, normalizes it, and passes a plain `usize` onward; it
  never threads `ctx` into a helper module. `ctx.database_version()` follows the same shape. There is
  no wrapper function for it, because a wrapper whose whole body forwards one call adds a name without
  adding a decision — the read happens inline in `handle_create_virtual_schema`, where every other
  per-request value is already resolved.
* **`ctx` is already live at the one site that needs it.** `handle_create_virtual_schema(ctx: &mut dyn
  UdfContext, request: &Json)` receives the context and already calls `resolve_connection_config(ctx,
  …)` and `udf_log!(ctx, warn, …)`. `build_listing_virtual_tables(configured_ns, listing)` is the
  declaration pipeline it calls, and it currently takes no precision argument — so exactly one new
  plain parameter reaches it, and from there `column_source_type_to_exasol`.
* **The scan UDF entry point needs NO version read.** `UdfContext::database_version` is available on
  both entry points, but the scan's `EMITS` types arrive in the adapter-generated pushdown SQL and in
  `ScanSpec::emit_exa_types`, both derived from the pushdown request's own `dataType` JSON. Adding a
  second version read there would create a second owner of one decision.
* **`exasol_type_to_json` today has no `TIMESTAMP(p)` arm, which is why this feature needs a
  scenario and not just a threading change.** Its timestamp branch matches the string `TIMESTAMP`
  EXACTLY; a `TIMESTAMP(6)` string falls through every arm to the catch-all and would be declared
  `{"type": "varchar", "size": 2000000}` — a silently wrong column type, not a rejected request. The
  field name is `fractionalSecondsPrecision`, pinned by ADR
  `timestamp-precision-field-is-fractional-seconds-precision`: Exasol uses `precision` only for
  `DECIMAL` and `INTERVAL`, never for a TIMESTAMP.
* **The round trip closes at `exasol_type_from_json`, which already reads the same field.** Once the
  declaration carries `fractionalSecondsPrecision: 6`, Exasol echoes it in the pushdown request's
  `involvedTables` column `dataType`, `exasol_type_from_json` renders `TIMESTAMP(6)`, and the `EMITS`
  clause and emit-boundary coercion follow with no further change — the pair is a matched inverse over
  one grammar, so making the forward direction precision-aware completes a path the inverse already
  handles.
* **Live verification is required before implementation, per CLAUDE.md § Verification discipline.**
  That Exasol accepts `fractionalSecondsPrecision` in a `createVirtualSchema` column `dataType`, that
  it declares the resulting VS column as `TIMESTAMP(6)`, and that it echoes the field back on the
  pushdown request are all claims about live SQL behavior that no documentation or capability registry
  settles. They are captured against the Docker Exasol container before the declaration changes.

## Scenarios

### Scenario: Multi-level Iceberg namespaces flatten deterministically into Exasol table names

* *GIVEN* a configured namespace `prod.finance` containing an Iceberg table `orders` and a child namespace `prod.finance.eu` containing a table `orders`
* *WHEN* Exasol sends the `createVirtualSchema` request naming namespace `prod.finance`
* *THEN* the adapter SHALL name the first virtual table `ORDERS` and the second `EU__ORDERS`, flattening only the namespace segments below the configured namespace using `__` and uppercasing the result
* *AND* the adapter SHALL apply the same flatten function when building the `TABLE_MAP` so the Exasol name maps back to the correct original-cased Iceberg identifier
* *AND* when two distinct Iceberg identifiers flatten to the same Exasol name (a `__` collision) the adapter SHALL return an error naming the colliding Exasol table name rather than silently dropping or overwriting a table

### Scenario: A non-ASCII Iceberg table and column name stay queryable end to end

* *GIVEN* a live Exasol instance, an Iceberg REST catalog, and an Iceberg table whose TABLE name and one of whose COLUMN names are both the non-ASCII identifier `straße` — that column an Iceberg `string` column whose seeded values carry distinguishable prefixes, alongside an `id` column — seeded into its own namespace so no existing E2E virtual schema enumerates it
* *AND* a virtual schema created over that namespace through a real `createVirtualSchema`
* *WHEN* an Exasol user queries that table and that column through the virtual schema
* *THEN* `SYS.EXA_ALL_COLUMNS` and `SYS.EXA_ALL_TABLES` SHALL report both identifiers as `STRASSE`, pinning the full-Unicode `ß`-to-`SS` expansion as observed behavior rather than as documentation
* *AND* an unquoted `SELECT COUNT(*)` over the table SHALL return the seeded row count, so the uppercased table name still resolves through `TABLE_MAP` back to the original-cased Iceberg identifier `straße` and the scan reaches the real table
* *AND* an unquoted projection of the column SHALL return the seeded values in full, so the uppercased column name still maps back to the Iceberg field's own casing at scan time
* *AND* a `LIKE` predicate over that column SHALL return the correct subset of rows
* *AND* the adapter-GENERATED pushdown SQL for that same `LIKE` query SHALL carry the predicate over `"STRASSE"`, so the type-rewrite guards resolved the column's Exasol type from a `col_types` entry whose name came through this fold — the one pushdown path whose `col_types` lookup issue #265 consolidates. A declined filter returns the identical row set, so this generated-SQL assertion, not the row subset, is what discriminates a resolved lookup from a fail-safe decline
* *AND* the scenario SHALL FAIL, not skip, when no live Exasol instance is available, per this repo's E2E contract

### Scenario: createVirtualSchema reads the database version once and threads the resolved precision

* *GIVEN* a `createVirtualSchema` (or `refresh`/`setProperties`) request reaching `handle_create_virtual_schema` with a live `UdfContext`
* *WHEN* the adapter builds the response's per-column `dataType` declarations
* *THEN* the adapter SHALL read `UdfContext::database_version()` EXACTLY ONCE per request and resolve it, through the single owner `datafusion-scan/type-mapping-timestamp-precision` specifies, into a plain `Copy` value naming the timestamp precision
* *AND* that plain value SHALL be threaded as a new parameter into `build_listing_virtual_tables` and onward to `column_source_type_to_exasol`, and the `UdfContext` MUST NOT be threaded into `types/mapping.rs` in its place, so the type-mapping module keeps performing no I/O and reading no ambient state
* *AND* the read SHALL happen inline in `handle_create_virtual_schema` alongside the other per-request resolutions and MUST NOT be wrapped in a helper whose whole body forwards the one call
* *AND* the resolved precision MUST NOT be recorded in `adapterNotes`, persisted, or round-tripped: it is re-derived from the handshake on every request, exactly as the cluster node count is, so a virtual schema created against one engine version and queried after an upgrade declares the upgraded engine's precision on its next `refresh`
* *AND* the scan UDF entry point MUST NOT read `database_version()`: its `EMITS` types come from the pushdown request's own `dataType` JSON, so a second read there would give one decision two owners
* *AND* no other per-column behavior SHALL change — the column-name fold, its single owner, the `__`-flattened table name, `TABLE_MAP`, the 404 skip, and every non-timestamp declared type stay byte-identical

### Scenario: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision

* *GIVEN* a resolved Exasol type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9, the shape the version gate now produces for a catalog timestamp column
* *WHEN* `exasol_type_to_json` converts that string into the response's column `dataType` object
* *THEN* it SHALL return `{"type": "timestamp", "fractionalSecondsPrecision": p}`, reading the field name `fractionalSecondsPrecision` — the same name its inverse `exasol_type_from_json` reads, per ADR `timestamp-precision-field-is-fractional-seconds-precision` — and MUST NOT emit a `precision` field, which Exasol uses only for `DECIMAL` and `INTERVAL`
* *AND* it MUST NOT fall through to the catch-all VARCHAR arm for a `TIMESTAMP(p)` string, because that arm would declare a timestamp column as `{"type": "varchar", "size": 2000000}` — a silently wrong column type rather than a rejected request
* *AND* a bare `TIMESTAMP` string SHALL keep returning `{"type": "timestamp"}` with NO `fractionalSecondsPrecision` field, so the 8.x arm's declaration is byte-identical to the one recorded today
* *AND* `TIMESTAMP WITH LOCAL TIME ZONE` SHALL keep returning `{"type": "timestamp", "withLocalTimeZone": true}`, matched before any precision logic, so no precision-aware variant of that arm is introduced — its inverse short-circuits the same way
* *AND* every other type string SHALL keep its recorded `dataType` object byte-identical, including `BOOLEAN`, `DOUBLE PRECISION`, `DATE`, every `DECIMAL(p,s)`, and every `VARCHAR(n)`/`CHAR(n)` form
* *AND* Exasol SHALL declare the resulting virtual column as `TIMESTAMP(6)` in `SYS.EXA_ALL_COLUMNS` and echo `fractionalSecondsPrecision` back on the pushdown request's `involvedTables` column `dataType`, both captured against a live Exasol 2025.x instance rather than asserted from documentation
