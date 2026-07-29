# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the cluster's active node count, per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table.

## Background

* **The now-family date/time capabilities are deliberately absent, and this feature only records that they are.** `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` join this feature's "capabilities list MUST NOT include" enumeration so a reader consulting the deliberate-absence list learns they are absent by design and does not re-advertise them. The reason is owned by `vs-adapter/pushdown-planning-capability-extensions` and MUST NOT be restated here: that sibling feature records why the node-local scan cannot evaluate the now-family faithfully. Keeping one owner for the reason and one enumeration for the absence is what stopped the two lists from drifting after issue #210, when a capability change landed in the adapter-side feature and never reached the sibling that owned the same statement.

The catalog endpoint and storage credentials are supplied through the Exasol CONNECTION object named by the `CATALOG_CONNECTION` property; the namespace to expose is supplied by the `ICEBERG_NAMESPACE` property. The adapter holds no state between requests other than the values it returns in `schemaMetadata.adapterNotes`, which Exasol persists.

* Catalog endpoint and storage credentials are supplied through a CONNECTION object
  named by `CATALOG_CONNECTION`. The adapter resolves credentials via `ctx.connection`
  and never persists catalog metadata between requests.
* Credentials MUST NOT appear in any returned response or error message.
* The adapter holds no state between requests; cluster information is resolved per
  request and returned in the `createVirtualSchema` response's `adapterNotes`
  (stringified JSON), which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.
* Cluster configuration and the Exasol-name to Iceberg-identifier map are recorded in
  `adapterNotes` per `vs-adapter/create-virtual-schema-adapter-notes`.
* On the SigV4/Glue path the namespace/table enumeration requests address the
  catalog under the `catalogs/{account-id}` REST prefix derived per
  `vs-adapter/pushdown-planning-cloud-credentials` — the same derivation used by the
  `loadTable` path. This enumeration path is the one that produced the Glue
  `400 "Prefix must follow the 'catalogs/{catalogId}' format."` error (#123).
* A namespace listing can include non-Iceberg tables (e.g. AWS Glue Hive external
  tables). Per-table schema resolution loads each table via `loadTable`; a non-Iceberg
  table's `loadTable` returns HTTP 404 (Iceberg REST OpenAPI `NoSuchTableException`;
  Glue's `NoSuchIcebergTableException`). A 404 table is skipped, not fatal (#138); every
  other per-table load failure aborts the request. A namespace in which *every* listed
  table 404s (an all-Hive namespace) is not an error: `createVirtualSchema` still succeeds,
  yielding a virtual schema with an empty table set and a per-table warning for each skipped
  table.

* This delta ADDS THREE clauses to ONE scenario and adds ONE scenario, both of issue #265's live-capture finding. It AMENDS NO recorded clause. The three added clauses join "Create virtual schema enumerates every table in the configured namespace" and state the field-name fold, that fold's single owner, and the `ß`-expansion trade-off. The added scenario is a live end-to-end round-trip over a non-ASCII Iceberg identifier. No enumeration, skip, abort, credential-redaction, `adapterNotes`, or type-mapping behavior changes — the adapter's casing behavior is unchanged and byte-identical; what changes is that the library now STATES it.
* This delta SUPERSEDES nothing. The recorded declaration clause — "*AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table — whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing — and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the 'One non-Iceberg table in the namespace is skipped' scenario" — is complete about TABLE-name casing and silent about COLUMN-name casing, yet the adapter uppercases both. That clause stands unaltered here. All SEVEN of the recorded scenario's steps are carried into this delta verbatim, and the three added clauses supply the field-name half without changing any existing step.
* The documentation gap this delta closes was surfaced by an issue #265 live capture, not by a code change. `resolve_table_schema` (`adapter/pushdown/file_resolution.rs:610-644`) maps every Iceberg field through `(f.name.to_uppercase(), exasol_ty)` at line 640, and that pair list is the sole input to the per-column `"name"` `build_virtual_tables` declares (`adapter/mod.rs:551`, the `json!` at `:576`). Exactly ONE production site declares a column name, reached from ONE caller (`adapter/mod.rs:255`) whose resolver is `resolve_table_schema`.
* `str::to_uppercase` is FULL Unicode case mapping, not a one-to-one fold, and the difference is observable: it maps `ß` to `SS`, expanding one character into two. Rust's own `str::to_uppercase` documentation pins that example. `flatten_table_name` (`adapter/tables.rs:29`, folding at `:42`) applies the same `to_uppercase` to the table name, so both halves of a declared identifier go through one rule.
* The casing is deliberate and its reason is already in the code at `file_resolution.rs:637-639`: Exasol resolves an unquoted identifier in user SQL by uppercasing it, so declaring `ID` rather than `id` is what makes `SELECT id` resolve. Declaring the Iceberg casing verbatim would force every user query to double-quote every column name.
* The `ß`-to-`SS` expansion is a deliberate trade-off, not a gap, and this delta names it rather than leaving it unstated. An Iceberg column `straße` is queryable ONLY as `STRASSE`; the `ß`-bearing form `"STRAßE"` resolves against no declared column. Two Iceberg columns in one table differing only in that expansion (`strasse` and `straße`) would both declare `STRASSE`, which the response would carry as a duplicate column name. No table-name equivalent exists, because `flatten_table_name`'s output feeds the `__`-collision check that already errors on a collision (`vs-adapter/create-virtual-schema`'s multi-level-namespace scenario) — the column path has no such check. Recording the asymmetry is what keeps it from reading as an oversight.
* The scan side is unaffected and needs no delta: `datafusion-scan/scan-execution-field-id-projection` already maps projection names back to the Parquet field casing case-insensitively, which is why an uppercased declaration still reaches the right Iceberg field.
* Apache Iceberg spec check: NOT implicated as a schema or type question. The spec's Schemas and Data Types section defines a `struct` field as carrying a "field name" string and constrains field IDs, not the SQL identifier casing a downstream engine uses to expose that name. The spec mandates no case-sensitivity rule for consumers, so uppercasing at declaration is an Exasol identifier-resolution decision rather than an Iceberg deviation. The one Exasol-target limitation worth naming is the `ß` expansion above, and it is named as a trade-off rather than left silent.
* The added scenario is a LIVE E2E scenario, not a unit one, because the property under test is a round-trip through a real `createVirtualSchema` and a real query — exactly the class CLAUDE.md § Verification discipline requires be checked against a running Exasol instance rather than asserted from a capability list or from code inspection. The unit-level fold is already covered by `adapter/tables.rs`'s existing `flatten_table_name` casing tests.
* The added scenario's fixture MUST live in its OWN Iceberg namespace, not in `e2e_lakehouse`. Every existing E2E virtual schema is created over `e2e_lakehouse`, so a table added there would appear in each of those suites' enumerations and could churn assertions this plan promises to leave untouched. A separate namespace makes the new fixture invisible to them.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies an `ICEBERG_NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* when SigV4 is enabled, the adapter SHALL address the namespace and table list requests under the `catalogs/{warehouse}` REST prefix derived per `vs-adapter/pushdown-planning-cloud-credentials`, so a bare-account-id `warehouse` produces the Glue-required `catalogs/{account-id}` prefix
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table — whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing — and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the "One non-Iceberg table in the namespace is skipped" scenario
* *AND* the adapter SHALL declare each column's `"name"` as the Iceberg field name uppercased with FULL Unicode case mapping (Rust's `str::to_uppercase`), the same fold the table name receives, because Exasol resolves an unquoted identifier in user SQL by uppercasing it — so declaring the Iceberg casing verbatim would force every user query to double-quote every column name
* *AND* that fold SHALL be owned by exactly ONE site, `resolve_table_schema` (`adapter/pushdown/file_resolution.rs`), which produces the `(name, Exasol type)` pairs the response's column list is built from, and no other code path SHALL declare a differently-cased name
* *AND* the full-Unicode fold's one-to-many expansions SHALL be recorded as a deliberate Exasol-target trade-off rather than left unstated: `ß` becomes `SS`, so an Iceberg column `straße` is queryable ONLY as the ASCII identifier `STRASSE` and the `ß`-bearing form resolves against no declared column, and two Iceberg columns in one table differing only in that expansion declare the same Exasol name with no collision check to reject it
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
