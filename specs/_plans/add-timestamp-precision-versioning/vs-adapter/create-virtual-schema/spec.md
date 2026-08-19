# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table.

## Background

* **This delta is issue #359.** It adds TWO scenarios and AMENDS NO recorded clause. The first records
  the single `ctx.database_version()` read and how the resolved timestamp precision reaches the
  declaration pipeline. The second records the column `dataType` JSON a `TIMESTAMP(p)` declaration
  serializes to. Enumeration, the 404-skip and all-Hive-empty behavior, the non-404 abort, the
  name-fold ownership, `TABLE_MAP`/`adapterNotes` recording, and every other declared column type stay
  as recorded.
* **`datafusion-scan/type-mapping` owns WHICH declaration each version gets; this feature owns WHERE
  the version is read and HOW the answer travels.** The split matters because the type-mapping module
  must stay free of `UdfContext` — it reads no ambient state and does no I/O — so the context read has
  to happen at the adapter edge and the answer has to travel as a plain value.
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

<!-- DELTA:NEW -->
### Scenario: createVirtualSchema reads the database version once and threads the resolved precision

* *GIVEN* a `createVirtualSchema` (or `refresh`/`setProperties`) request reaching `handle_create_virtual_schema` with a live `UdfContext`
* *WHEN* the adapter builds the response's per-column `dataType` declarations
* *THEN* the adapter SHALL read `UdfContext::database_version()` EXACTLY ONCE per request and resolve it, through the single owner `datafusion-scan/type-mapping` specifies, into a plain `Copy` value naming the timestamp precision
* *AND* that plain value SHALL be threaded as a new parameter into `build_listing_virtual_tables` and onward to `column_source_type_to_exasol`, and the `UdfContext` MUST NOT be threaded into `types/mapping.rs` in its place, so the type-mapping module keeps performing no I/O and reading no ambient state
* *AND* the read SHALL happen inline in `handle_create_virtual_schema` alongside the other per-request resolutions and MUST NOT be wrapped in a helper whose whole body forwards the one call
* *AND* the resolved precision MUST NOT be recorded in `adapterNotes`, persisted, or round-tripped: it is re-derived from the handshake on every request, exactly as the cluster node count is, so a virtual schema created against one engine version and queried after an upgrade declares the upgraded engine's precision on its next `refresh`
* *AND* the scan UDF entry point MUST NOT read `database_version()`: its `EMITS` types come from the pushdown request's own `dataType` JSON, so a second read there would give one decision two owners
* *AND* no other per-column behavior SHALL change — the column-name fold, its single owner, the `__`-flattened table name, `TABLE_MAP`, the 404 skip, and every non-timestamp declared type stay byte-identical
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision

* *GIVEN* a resolved Exasol type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9, the shape the version gate now produces for a catalog timestamp column
* *WHEN* `exasol_type_to_json` converts that string into the response's column `dataType` object
* *THEN* it SHALL return `{"type": "timestamp", "fractionalSecondsPrecision": p}`, reading the field name `fractionalSecondsPrecision` — the same name its inverse `exasol_type_from_json` reads, per ADR `timestamp-precision-field-is-fractional-seconds-precision` — and MUST NOT emit a `precision` field, which Exasol uses only for `DECIMAL` and `INTERVAL`
* *AND* it MUST NOT fall through to the catch-all VARCHAR arm for a `TIMESTAMP(p)` string, because that arm would declare a timestamp column as `{"type": "varchar", "size": 2000000}` — a silently wrong column type rather than a rejected request
* *AND* a bare `TIMESTAMP` string SHALL keep returning `{"type": "timestamp"}` with NO `fractionalSecondsPrecision` field, so the 8.x arm's declaration is byte-identical to the one recorded today
* *AND* `TIMESTAMP WITH LOCAL TIME ZONE` SHALL keep returning `{"type": "timestamp", "withLocalTimeZone": true}`, matched before any precision logic, so no precision-aware variant of that arm is introduced — its inverse short-circuits the same way
* *AND* every other type string SHALL keep its recorded `dataType` object byte-identical, including `BOOLEAN`, `DOUBLE PRECISION`, `DATE`, every `DECIMAL(p,s)`, and every `VARCHAR(n)`/`CHAR(n)` form
* *AND* Exasol SHALL declare the resulting virtual column as `TIMESTAMP(6)` in `SYS.EXA_ALL_COLUMNS` and echo `fractionalSecondsPrecision` back on the pushdown request's `involvedTables` column `dataType`, both captured against a live Exasol 2025.x instance rather than asserted from documentation
<!-- /DELTA:NEW -->
</content>
