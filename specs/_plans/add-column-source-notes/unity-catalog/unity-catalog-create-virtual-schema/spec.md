<!-- DELTA:CHANGED -->
# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per plannable base table: a listed entry whose Unity Catalog `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA` or `PARQUET`. Every other listed entry (a view, another format, or any other `table_type`) is excluded from the returned virtual tables and warned. Enumeration runs on the SAME kind-agnostic listing pipeline the Iceberg REST kind uses. The admission decision lives inside the Unity Catalog client, so `data_source_format` never crosses the shared trait boundary. Mapping each `catalog.schema.table` identifier to an Exasol table name and each Unity Catalog column to an Exasol column type is sufficient to list the namespace and expose queryable column metadata. Each column's declared source shape is recorded in its note (`vs-adapter/column-source-notes`) from the same catalog metadata: the column's `type_json`, and for a Delta table also the table's `properties`. This path reads only Unity Catalog metadata and no object storage, so it resolves no snapshot, no file list, and no Parquet footer.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

The configured namespace is the `catalog.schema` supplied as the `NAMESPACE` virtual-schema property, dot-split into segments. The adapter obtains the namespace's tables by calling the shared trait's list operation on the constructed Unity Catalog client, and the pipeline that consumes the result is the same code the Iceberg REST kind runs. That pipeline reads neutral table metadata and does not know, or ask, how each client sourced its columns. Inside the Unity Catalog client, every listed table's column metadata and table `properties` come from the single paginated `GET /tables` list sweep, which returns each table's columns inline by default (verified live against `demo_sales_catalog.sales`) and its properties unless the request sets `omit_properties` (Databricks List tables reference: "Whether to omit the properties of the table from the response or not"). That client MUST NOT issue a per-table `GET /tables/{full_name}` to obtain columns or properties, so enumerating a schema costs one paginated sweep rather than an N+1 fan-out. Each enumerated table's Exasol name is the namespace segments below the configured namespace plus the table name, joined with `__` and uppercased through the SAME single case-fold site the Iceberg createVirtualSchema path uses. The shared pipeline applies one column-name fold for both kinds, so no code path declares a differently-cased name. The Exasol-name-to-Unity-Catalog-identifier map is recorded in the response `adapterNotes.TABLE_MAP`. Unity Catalog reports each column's Spark type in `type_json`, the Delta `StructField` JSON. Real Unity clients leave `type_precision` and `type_scale` unset: Databricks reports both as 0, and Unity's Spark connector omits them (#463). The neutral column carries `type_json` verbatim, and the adapter's single type-mapping home parses it. A column whose type has no clean scalar Exasol equivalent, or whose `type_json` is absent or unparseable, is declared `VARCHAR(2000000)` rather than failing the enumeration.

* The Unity Catalog client makes the admission decision (`unity-catalog/unity-catalog-client`) and routes every excluded entry into `CatalogListing.skipped`. The shared `build_listing_virtual_tables` pipeline stays kind-agnostic.
* Excluded entries are warned on the SAME shared skip-warn loop the Iceberg REST kind uses: one `udf_log!(ctx, warn, ...)` line per skipped entry, rendered from the neutral skip reason on the entry rather than from a per-kind branch. The Iceberg path's warning stays byte-identical.
* A Delta table and a Parquet table declare their columns identically, from the same Unity Catalog column types, because this path reads no object storage. A Delta table's column notes also read the column-mapping mode from the table's `properties`, because the Delta protocol keeps the mode in a table property and each column's binding keys in that column's own metadata, which `type_json` carries as the Delta `StructField` JSON (PROTOCOL.md § Column Mapping: "The column mapping is governed by the table property `delta.columnMapping.mode` being one of `none`, `id`, and `name`", and "The physical name is stored as part of the column metadata with the key `delta.columnMapping.physicalName`." and "The column id is stored within the metadata with the key `delta.columnMapping.id`.").
* Shallow clones are admitted by the base-table rule with no shallow-clone-specific handling, because Unity Catalog surfaces a shallow clone as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`. That a real shallow clone's wire shape matches this rule is an assumption not verified live.
* The `DECIMAL` guard's predicate and the timestamp declaration rule each have ONE owner, `scan-types/type-mapping`. This feature consumes them and does not restate them. `vs-adapter/create-virtual-schema` owns the single `ctx.database_version()` read.

* This delta (plan `change-unity-listing-delta-base-filter`, correcting issue #318) scopes the Unity Catalog createVirtualSchema listing to Delta base tables only. It restores the original #318 planning intent — report only Delta-format base tables — which was lost during planning and never recorded, so the shipped code reported every listed entry with no filter and did not deserialize `data_source_format`.
* This delta SUPERSEDES the recorded feature-description and Background clause that the adapter "return one virtual table per listed table", and the recorded Background clause that "A listed entry may be a VIEW, which carries a column list but no storage location; the listing path lists it with its columns". The adapter now returns one virtual table per Delta base table; a view, a non-`DELTA`-format table, or an entry of any other `table_type` is excluded and warned.
* This delta SUPERSEDES the recorded scenario "Create virtual schema lists a Unity Catalog view with its columns and no storage location", which asserted a VIEW is listed with its columns. Its inverse — a VIEW is excluded and warned — is covered by the scenario "Create virtual schema excludes every non-Delta-base entry and warns per exclusion".
* The Delta-base decision is made INSIDE the Unity Catalog client (see `unity-catalog/unity-catalog-client`), which deserializes `data_source_format` and routes every excluded entry into `CatalogListing.skipped`. The shared `build_listing_virtual_tables` pipeline stays kind-agnostic and UNTOUCHED, and `data_source_format` stays a Unity-wire-private field that never appears in a neutral type.
* Excluded entries are warned on the SAME shared skip-warn loop the Iceberg REST kind uses. The loop writes one `udf_log!(ctx, warn, ...)` line per skipped entry, rendered from the neutral skip reason carried on the entry rather than from a per-kind branch. The Iceberg path's warning stays byte-identical, per `vs-adapter/catalog-kind-selection`; the Unity path's warning names the excluded identifier and its disqualifying `table_type` or `data_source_format`.
* The Iceberg REST listing path is unaffected: it still skips only tables the catalog reports as not loadable (HTTP 404), and its skipped-table warning stays byte-identical.
* Shallow clones are INCLUDED by the base-table rule with no shallow-clone-specific handling, because Unity Catalog surfaces a shallow clone as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`. That the wire shape of a real shallow clone matches the base-table rule is an assumption not yet verified live; it is recorded as a tracked assumption rather than a silent claim (see decision log).
* **The guard's predicate, its single-owner requirement, and the Exasol target-type trade-off are owned by `scan-types/type-mapping` and are consumed here, NOT restated.** This feature records only that the Unity arm reads its answer from that one owner, so the two catalog kinds cannot drift.
* **This delta is issue #359.** It AMENDS ONE clause of ONE scenario and adds no scenario. The amended
  clause is the declared Exasol type for the Spark type name `TIMESTAMP`, which becomes version-gated.
  Every other declared type in that scenario, the exhaustive-match requirement, the parameterized-
  descriptor requirement, the case-fold clause, the Delta-base filter, the exclusion warnings, and the
  incompatible-type VARCHAR fallback stay byte-identical.
* **The Delta declaration path IS this Unity path, which is why the amendment lands here.** A Delta
  table reaches `createVirtualSchema` only through the Unity Catalog kind, so
  `spark_primitive_to_exasol` is the one production function that declares a Delta timestamp column's
  Exasol type. Widening issue #359 from its Iceberg-only wording to cover Delta means amending this
  clause, not the Arrow-input resolver its scope text names.
* **This delta closes the timestamp-precision half of this feature's own recorded #322 deferral.** The
  feature description defers *"deeper Delta schema fidelity — reader-feature gating, timestamp
  precision, type widening, and variant types"* to #322. The DECLARATION half of "timestamp precision"
  is settled here; reader-feature gating, type widening, and variant types are unaffected and stay
  where they are recorded.
* **The version rule and both declaration strings have ONE owner outside this feature.**
  `scan-types/type-mapping` owns them, and `vs-adapter/create-virtual-schema` owns the single
  `ctx.database_version()` read. This feature only records which string a Unity `TIMESTAMP` and
  `TIMESTAMP_NTZ` column receives, and MUST NOT restate the rule or either literal.
* **This delta is issue #426, and Unity Catalog metadata stays this path's only input.** Each
  column's note is built from the column's `type_json` and, for a Delta table, the table's
  `properties`, both taken from the same list sweep. This path reads no Delta log, requests no
  storage credential, and reads no object storage, so it needs no access beyond the catalog
  metadata it already reads.
* **A Delta declaration is Unity Catalog's copy of the Delta schema, and the binding keys it records
  stay valid across a rename.** PROTOCOL.md § Writer Requirements for Column Mapping: "The physical
  name of the column is static and can be different than the _display name_ of the column, which is
  changeable", and a writer must "ensure the physical field path of the new column is unique across
  all versions of the table". A binding key recorded at `createVirtualSchema` therefore still names
  the same column after a source-side rename. A copy that disagrees with the Delta log is caught by
  the pushdown Delta reader, which fails the query rather than reading through it
  (`delta/delta-table-planning`).
* **What this path cannot check.** The reader-feature gate and the protocol condition on the mode
  property ("The table property should only be honored if the table's protocol has reader and
  writer versions and/or table features that support the `columnMapping` table feature") need the
  Delta log's protocol, which this path does not read. A Delta table that the gate refuses is
  therefore listed like any other and fails at query time, and the pushdown gate stays the authority
  (`delta/delta-reader-feature-gating`). Excluded entries are still only the non-plannable entries
  this feature already lists.
* **An unusable column-mapping annotation refuses its column, never the table or the statement.** A
  Delta column whose `type_json` lacks the annotation its table's mode requires, or carries it
  malformed, and every column of a table whose mode property holds a value outside `none`, `id`,
  and `name`, is listed with a note whose refusal states the problem. This is the column-scoped
  refusal `delta/delta-type-mapping` already applies to an unmappable type. `createVirtualSchema`
  neither fails nor skips the table, because the cause is a property of one table's metadata and the
  refusal surfaces on every query that reads the column.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Enumeration names tables through the shared fold and stops at catalog metadata

* *GIVEN* the same Unity Catalog namespace enumeration
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL name each returned virtual table by flattening the segments below the configured namespace plus the table name with `__` and uppercasing the result through the shared case-fold site
* *AND* the adapter SHALL source every listed entry's columns, table `properties`, `table_type`, and `data_source_format` from the single paginated `GET /tables` list sweep, issuing no per-table `GET /tables/{full_name}`, reading no Delta transaction log, reading no Parquet footer, and resolving no snapshot, because this path stops at catalog metadata
* *AND* the namespace property SHALL be the SAME `NAMESPACE` property the Iceberg REST kind reads, so neither kind carries a namespace property of its own
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Create virtual schema declares each Delta column from its type_json and the table's column-mapping mode property

* *GIVEN* three Unity Catalog Delta tables, one per column-mapping mode, whose `properties` set `delta.columnMapping.mode` to `name`, to `id`, or not at all, and whose columns' `type_json` carry the `delta.columnMapping.physicalName` and `delta.columnMapping.id` metadata the table's Delta log declares, and a partitioned Delta table whose partition column carries `partition_index` 0
* *WHEN* createVirtualSchema completes
* *THEN* each column's note SHALL carry the binding key the mode in the table's `properties` selects, read from the column's `type_json` metadata, per `delta/delta-table-planning`
* *AND* each column's Exasol `dataType` SHALL still be mapped from its `type_json`, per the type-mapping scenarios of this feature
* *AND* a Delta table's partition columns SHALL be the columns that carry a `partition_index`, in that order, and each one's note SHALL carry its position
* *AND* the adapter SHALL read no Delta log, request no storage credential, and read no object storage for any table
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A Delta column whose column-mapping metadata is unusable is refused in its note and the table stays listed

* *GIVEN* a Unity Catalog Delta table whose `properties` set `delta.columnMapping.mode` to `name` and whose column `value` has a `type_json` without `delta.columnMapping.physicalName`, and a second Delta table whose `properties` set `delta.columnMapping.mode` to `label`
* *WHEN* createVirtualSchema completes and queries run over both tables
* *THEN* createVirtualSchema SHALL succeed, both tables SHALL be listed, and neither SHALL appear in `SKIPPED_TABLES`
* *AND* the note of `value` SHALL carry a refusal naming the column, the `name` mode, and the missing key, so a query that reads `value` fails with that reason and a query that reads only the table's other columns succeeds
* *AND* every column note of the second table SHALL carry a refusal naming the unrecognized mode value, so every query over that table fails with that reason
* *AND* no refusal SHALL contain a credential value
<!-- /DELTA:NEW -->
