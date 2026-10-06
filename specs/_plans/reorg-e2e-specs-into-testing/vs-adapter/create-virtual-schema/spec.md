<!-- DELTA:CHANGED -->
# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table. The cluster's active node count is NOT among the recorded values: each pushdown reads it from its own UDF handshake (see `vs-adapter/create-virtual-schema-adapter-notes` and `pushdown/pushdown-planning`).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* **The now-family date/time capabilities are deliberately absent, and this feature only records that they are.** `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` join this feature's "capabilities list MUST NOT include" enumeration so a reader consulting the deliberate-absence list learns they are absent by design and does not re-advertise them. The reason is owned by `pushdown-capabilities/pushdown-planning-capability-extensions` and MUST NOT be restated here: that sibling feature records why the node-local scan cannot evaluate the now-family faithfully. Keeping one owner for the reason and one enumeration for the absence is what stopped the two lists from drifting after issue #210, when a capability change landed in the adapter-side feature and never reached the sibling that owned the same statement.

The catalog endpoint and storage credentials are supplied through the Exasol CONNECTION object named by the `CATALOG_CONNECTION` property; the namespace to expose is supplied by the `NAMESPACE` property. The adapter holds no state between requests other than the values it returns in `schemaMetadata.adapterNotes`, which Exasol persists.

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
  `scan-types/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.
* Cluster configuration and the Exasol-name to Iceberg-identifier map are recorded in
  `adapterNotes` per `vs-adapter/create-virtual-schema-adapter-notes`.
* On the SigV4/Glue path the namespace/table enumeration requests address the
  catalog under the `catalogs/{account-id}` REST prefix derived per
  `storage-access/pushdown-planning-cloud-credentials` — the same derivation used by the
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
* The scan side is unaffected and needs no delta: `scan-read-path/scan-execution-field-id-projection` already maps projection names back to the Parquet field casing case-insensitively, which is why an uppercased declaration still reaches the right Iceberg field.
* Apache Iceberg spec check: NOT implicated as a schema or type question. The spec's Schemas and Data Types section defines a `struct` field as carrying a "field name" string and constrains field IDs, not the SQL identifier casing a downstream engine uses to expose that name. The spec mandates no case-sensitivity rule for consumers, so uppercasing at declaration is an Exasol identifier-resolution decision rather than an Iceberg deviation. The one Exasol-target limitation worth naming is the `ß` expansion above, and it is named as a trade-off rather than left silent.
* This delta (plan `add-native-unity-catalog-client`, issue #318) moves the column-name case-fold home from the deleted `resolve_table_schema` to the shared `CatalogClient` listing pipeline. The single-fold-owner invariant holds; only its owning site changes. The enumeration mechanism moves behind the trait, but the enumerated tables, declared names and types, `TABLE_MAP`, warnings, and errors stay byte-identical.
* This delta SUPERSEDES the `*AND* that fold SHALL be owned by exactly ONE site, resolve_table_schema (adapter/pushdown/file_resolution.rs)…` clause of the scenario "Create virtual schema enumerates every table in the configured namespace". The one fold owner becomes the shared `CatalogClient` listing pipeline — the one home that folds every declared name for BOTH the Iceberg REST and native Unity Catalog kinds — replacing the `resolve_table_schema` naming. The supersession keeps "owned by exactly ONE site" and "no other code path SHALL declare a differently-cased name" intact, and preserves byte-identical the full-Unicode `to_uppercase` expansion (`ß`→`SS`, so `straße`→`STRASSE`) and the no-collision-check column trade-off. The fold rule, its Exasol identifier-resolution reason, and its trade-off are unchanged; only the owning site moves.
* Recorded Background prose citing `resolve_table_schema` at `file_resolution.rs:610-644` and `adapter/mod.rs:255/551/576` as the sole fold and column-declaration site is superseded by the same move. The `(name, Exasol type)` production relocates into the shared pipeline, and `IcebergRestCatalogClient::load_table` supplies the ordered original-cased columns the pipeline folds.
* No other scenario of this feature changes under this delta. Enumeration, the 404-skip and all-Hive-empty behavior, the non-404 abort, the unreachable-catalog error, `TABLE_MAP`/`adapterNotes` recording, the multi-level flatten collision, and the non-ASCII end-to-end round-trip all stay as recorded.
* **Split, issue #359: the multi-level/non-ASCII naming scenarios and the timestamp-precision
  version-read/serialization scenarios moved to `vs-adapter/create-virtual-schema-declaration-details`.**
  This feature's scenario count crossed this library's per-spec organization threshold once issue
  #359 landed; that sibling feature now owns the multi-level namespace flatten/collision scenario,
  the non-ASCII round-trip scenario, the single `ctx.database_version()` read and threading, and the
  `TIMESTAMP(p)` `fractionalSecondsPrecision` serialization. This feature keeps capability
  reporting, namespace enumeration, the 404-skip/abort/unreachable-catalog failure contracts, and
  the `TABLE_MAP`/`adapterNotes` recording.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter reports its pushdown capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the adapter SHALL return a JSON response of type `getCapabilities` whose list includes projection (`SELECTLIST_PROJECTION`), scalar select-list expressions (`SELECTLIST_EXPRESSIONS`), filter predicates (`FILTER_EXPRESSIONS`), `LIMIT`, the comparison predicates `FN_PRED_EQUAL`/`FN_PRED_NOTEQUAL`/`FN_PRED_LESS`/`FN_PRED_LESSEQUAL`, the matching predicates `FN_PRED_LIKE`/`FN_PRED_LIKE_ESCAPE`/`FN_PRED_REGEXP_LIKE`, the literal capabilities `LITERAL_BOOL`/`LITERAL_DATE`/`LITERAL_DOUBLE`/`LITERAL_EXACTNUMERIC`/`LITERAL_NULL`/`LITERAL_STRING`/`LITERAL_TIMESTAMP`/`LITERAL_TIMESTAMP_UTC`, the supported math/string/date/conditional scalar-function capabilities enumerated in `pushdown/pushdown-planning`, and `AGGREGATE_HAVING` plus the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`
* *AND* the capabilities list MUST NOT include `FN_PRED_GREATER` or `FN_PRED_GREATEREQUAL` (those names do not exist in the Exasol capability vocabulary — Exasol normalises `a > b` to `b < a` and `a >= b` to `b <= a` before it reaches the adapter — so advertising them is misleading dead capability), nor any of `ORDER_BY_COLUMN`/`ORDER_BY_EXPRESSION`, `JOIN*`, geospatial (`FN_ST_*`), Exasol-only session functions (`FN_CURRENT_USER`/`FN_SYS_GUID`/`FN_CURRENT_SCHEMA`), the now-family date/time functions (`FN_CURRENT_DATE`/`FN_CURRENT_TIMESTAMP`/`FN_SYSDATE`/`FN_SYSTIMESTAMP`), whose withdrawal and reason are owned by `pushdown-capabilities/pushdown-planning-capability-extensions`, `LITERAL_INTERVAL`, `AGGREGATE_GROUP_BY_TUPLE`, any `*_DISTINCT` aggregate, `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, or any `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`
* *AND* every advertised capability name MUST be one the adapter can either translate via the VS expression translator or decompose into a correct partial/merge plan, so the advertised set never claims behaviour the engine cannot execute correctly
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies a `NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* when SigV4 is enabled, the adapter SHALL address the namespace and table list requests under the `catalogs/{warehouse}` REST prefix derived per `storage-access/pushdown-planning-cloud-credentials`, so a bare-account-id `warehouse` produces the Glue-required `catalogs/{account-id}` prefix
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table — whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing — and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the "One non-Iceberg table in the namespace is skipped" scenario
* *AND* the adapter SHALL declare each column's `"name"` as the Iceberg field name uppercased with FULL Unicode case mapping (Rust's `str::to_uppercase`), the same fold the table name receives, because Exasol resolves an unquoted identifier in user SQL by uppercasing it — so declaring the Iceberg casing verbatim would force every user query to double-quote every column name
* *AND* that fold SHALL be owned by exactly ONE site — the shared `CatalogClient` listing pipeline, which folds every declared name for BOTH catalog kinds and produces the `(name, Exasol type)` pairs the response's column list is built from — and no other code path SHALL declare a differently-cased name
* *AND* the full-Unicode fold's one-to-many expansions SHALL be recorded as a deliberate Exasol-target trade-off rather than left unstated: `ß` becomes `SS`, so an Iceberg column `straße` is queryable ONLY as the ASCII identifier `STRASSE` and the `ß`-bearing form resolves against no declared column, and two Iceberg columns in one table differing only in that expansion declare the same Exasol name with no collision check to reject it
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map and the skipped-table list (`SKIPPED_TABLES`, per `vs-adapter/create-virtual-schema-adapter-notes`) recorded in `adapterNotes`
* *AND* a `createVirtualSchema` request that resolves to a CATALOG kind and supplies no `NAMESPACE` property SHALL fail with the required-property error naming `NAMESPACE`, SUPERSEDING the unconditional form of this clause, because the `DIRECT_STORAGE` kind reaches its tables from the CONNECTION address alone and treats an absent `NAMESPACE` as the address itself, per `direct-storage/direct-storage-properties`
<!-- /DELTA:CHANGED -->
