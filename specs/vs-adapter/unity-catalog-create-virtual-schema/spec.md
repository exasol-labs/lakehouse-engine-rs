# Feature: Unity Catalog Create Virtual Schema

Enumerates a Unity Catalog namespace during createVirtualSchema and returns one virtual table per Delta base table — a listed entry whose Unity Catalog `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA`. Every other listed entry — a view, a non-`DELTA` format, or any other `table_type` — is excluded from the returned virtual tables and warned. Enumeration runs on the SAME kind-agnostic listing pipeline the Iceberg REST kind uses; the Delta-base decision lives inside the Unity Catalog client, so `data_source_format` never crosses the shared trait boundary. Mapping each `catalog.schema.table` identifier to an Exasol table name and each Unity Catalog column to an Exasol column type is sufficient to list the namespace and expose queryable column metadata; deeper Delta schema fidelity — reader-feature gating, timestamp precision, type widening, and variant types — is deferred to #322. This path reads only Unity Catalog catalog metadata; it does not read the Delta transaction log, so it never resolves a snapshot or a file list.

## Background

The configured namespace is the `catalog.schema` supplied as the existing `ICEBERG_NAMESPACE` virtual-schema property, dot-split into segments; the property keeps its Iceberg-era name under this plan and a catalog-neutral rename is deferred to #324. The adapter obtains the namespace's tables by calling the shared trait's list operation on the constructed Unity Catalog client, and the pipeline that consumes the result is the same code the Iceberg REST kind runs — it reads neutral table metadata and does not know, or ask, how each client sourced its columns. Inside the Unity Catalog client, every listed table's column metadata comes from the single paginated `GET /tables` list sweep, which returns each table's columns inline by default (verified live against `demo_sales_catalog.sales`); that client MUST NOT issue a per-table `GET /tables/{full_name}` to obtain columns, so enumerating a schema costs one paginated sweep rather than an N+1 fan-out. Each enumerated table's Exasol name is the namespace segments below the configured namespace plus the table name, joined with `__` and uppercased through the SAME single case-fold site the Iceberg createVirtualSchema path uses; the shared pipeline applies one column-name fold for both kinds, so no code path declares a differently-cased name. The Exasol-name-to-Unity-Catalog-identifier map is recorded in the response `adapterNotes.TABLE_MAP`. Unity Catalog reports each column's type as a Spark type (for example `LONG`, `STRING`, `INT`, and the parameterized `DECIMAL(p,s)`), which the neutral column carries as a source-tagged type descriptor holding the FULL parameterized type — the type name plus precision and scale from the wire `type_precision`/`type_scale` — so a `DECIMAL` column carries its `p` and `s` rather than a bare `DECIMAL`. The adapter's single type-mapping home maps that descriptor to an Exasol type reusing the Arrow-to-Exasol convention; any type without a clean scalar Exasol equivalent is declared `VARCHAR(2000000)` rather than failing the enumeration.

* This delta (plan `change-unity-listing-delta-base-filter`, correcting issue #318) scopes the Unity Catalog createVirtualSchema listing to Delta base tables only. It restores the original #318 planning intent — report only Delta-format base tables — which was lost during planning and never recorded, so the shipped code reported every listed entry with no filter and did not deserialize `data_source_format`.
* This delta SUPERSEDES the recorded feature-description and Background clause that the adapter "return one virtual table per listed table", and the recorded Background clause that "A listed entry may be a VIEW, which carries a column list but no storage location; the listing path lists it with its columns". The adapter now returns one virtual table per Delta base table; a view, a non-`DELTA`-format table, or an entry of any other `table_type` is excluded and warned.
* This delta SUPERSEDES the recorded scenario "Create virtual schema lists a Unity Catalog view with its columns and no storage location", which asserted a VIEW is listed with its columns. Its inverse — a VIEW is excluded and warned — is covered by the scenario "Create virtual schema excludes every non-Delta-base entry and warns per exclusion".
* The Delta-base decision is made INSIDE the Unity Catalog client (see `vs-adapter/unity-catalog-client`), which deserializes `data_source_format` and routes every excluded entry into `CatalogListing.skipped`. The shared `build_listing_virtual_tables` pipeline stays kind-agnostic and UNTOUCHED, and `data_source_format` stays a Unity-wire-private field that never appears in a neutral type.
* Excluded entries are warned on the SAME shared skip-warn loop the Iceberg REST kind uses. The loop writes one `udf_log!(ctx, warn, ...)` line per skipped entry, rendered from the neutral skip reason carried on the entry rather than from a per-kind branch. The Iceberg path's warning stays byte-identical, per `vs-adapter/catalog-kind-selection`; the Unity path's warning names the excluded identifier and its disqualifying `table_type` or `data_source_format`.
* The Iceberg REST listing path is unaffected: it still skips only tables the catalog reports as not loadable (HTTP 404), and its skipped-table warning stays byte-identical.
* Shallow clones are INCLUDED by the base-table rule with no shallow-clone-specific handling, because Unity Catalog surfaces a shallow clone as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`. That the wire shape of a real shallow clone matches the base-table rule is an assumption not yet verified live; it is recorded as a tracked assumption rather than a silent claim (see decision log).
* **This delta amends TWO scenarios and supersedes ONE Background clause, and is issue #329.** It carries the Unity half of the shared catalog-decimal guard whose contract `datafusion-scan/type-mapping` owns, and deletes a recorded recovery path the code never implemented. No enumeration, Delta-base filtering, skip-warn, `TABLE_MAP`, collision, case-fold, or credential-redaction behavior changes.
* **This delta SUPERSEDES the recorded Background clause naming `type_text` as a source of a Unity column's precision and scale.** The recorded sentence — "which the neutral column carries as a source-tagged type descriptor holding the FULL parameterized type — the type name plus precision and scale from the wire `type_precision`/`type_scale`, or `type_text` — so a `DECIMAL` column carries its `p` and `s` rather than a bare `DECIMAL`" — is replaced by the same sentence with the phrase "or `type_text`" DELETED. The descriptor's precision and scale come from `type_precision`/`type_scale` alone. `ColumnInfo` (`crates/lakehouse-catalog/src/unity/client.rs`) declares no `type_text` field and never deserializes one, so naming it advertised a recovery path that does not exist and would mislead a reader debugging a null-precision column into looking for a fallback the code cannot take.
* **An absent `type_precision` is exactly the `p = 0` case the widened guard now absorbs.** `neutral_column` resolves both `Option<u32>` fields through `.unwrap_or(0)`, so a `DECIMAL` column whose `type_precision` is null on the wire reaches the type mapping as `p = 0` and, before this delta, produced the invalid Exasol type `DECIMAL(0,0)` rather than the VARCHAR fallback. Deleting the phantom `type_text` recovery path and widening the guard are therefore two halves of one fix, not two unrelated edits.
* **The guard's predicate, its single-owner requirement, and the Exasol target-type trade-off are owned by `datafusion-scan/type-mapping` and are consumed here, NOT restated.** This feature records only that the Unity arm reads its answer from that one owner, so the two catalog kinds cannot drift.
* **This delta is issue #359.** It AMENDS ONE clause of ONE scenario and adds no scenario. The amended
  clause is the declared Exasol type for the Spark type name `TIMESTAMP`, which becomes version-gated.
  Every other declared type in that scenario, the exhaustive-match requirement, the parameterized-
  descriptor requirement, the case-fold clause, the Delta-base filter, the exclusion warnings, and the
  incompatible-type VARCHAR fallback stay byte-identical.
* **The Delta declaration path IS this Unity path, which is why the amendment lands here.** A Delta
  table reaches `createVirtualSchema` only through the Unity Catalog kind, so
  `unity_type_name_to_exasol` is the one production function that declares a Delta timestamp column's
  Exasol type. Widening issue #359 from its Iceberg-only wording to cover Delta means amending this
  clause, not the Arrow-input resolver its scope text names.
* **This delta closes the timestamp-precision half of this feature's own recorded #322 deferral.** The
  feature description defers *"deeper Delta schema fidelity — reader-feature gating, timestamp
  precision, type widening, and variant types"* to #322. The DECLARATION half of "timestamp precision"
  is settled here; reader-feature gating, type widening, and variant types are unaffected and stay
  where they are recorded.
* **The version rule and both declaration strings have ONE owner outside this feature.**
  `datafusion-scan/type-mapping` owns them, and `vs-adapter/create-virtual-schema` owns the single
  `ctx.database_version()` read. This feature only records which string a Unity `TIMESTAMP` and
  `TIMESTAMP_NTZ` column receives, and MUST NOT restate the rule or either literal.

## Scenarios

### Scenario: Create virtual schema enumerates every table in the configured Unity Catalog namespace

* *GIVEN* a Unity Catalog reachable through the CONNECTION named by `CATALOG_CONNECTION` and a createVirtualSchema request whose `CATALOG_KIND` is `UNITY_CATALOG` and whose `ICEBERG_NAMESPACE` property names a `catalog.schema`
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL list every table in that schema by calling the shared `CatalogClient` list operation on the constructed Unity Catalog client, and SHALL return one virtual table per listed DELTA BASE table — an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA`
* *AND* the adapter SHALL exclude from the returned virtual tables every other listed entry — a view, a base table whose `data_source_format` is not `DELTA`, or an entry of any other `table_type` — and MUST NOT record an excluded entry in `TABLE_MAP`, so no non-Delta or non-base entry becomes a queryable virtual table
* *AND* the adapter SHALL name each returned virtual table by flattening the segments below the configured namespace plus the table name with `__` and uppercasing the result through the shared case-fold site
* *AND* the adapter SHALL source every listed entry's columns, `table_type`, and `data_source_format` from the single paginated `GET /tables` list sweep — issuing no per-table `GET /tables/{full_name}`, reading no Delta transaction log, and resolving no snapshot — because this path stops at catalog metadata

### Scenario: Create virtual schema includes managed and external Delta base tables, including a shallow clone

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a `MANAGED` table with `data_source_format` `DELTA`, an `EXTERNAL` table with `data_source_format` `DELTA`, and a shallow clone that Unity Catalog reports as a `MANAGED` or `EXTERNAL` table with `data_source_format` `DELTA`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL return a virtual table for each of the three entries, because each satisfies the Delta-base rule
* *AND* the adapter SHALL apply NO shallow-clone-specific handling, because a shallow clone is admitted by the same `table_type` and `data_source_format` rule as any other base table
* *AND* each returned virtual table SHALL carry its columns mapped to Exasol types exactly as any Delta base table, and SHALL appear in `TABLE_MAP`

### Scenario: Both catalog kinds enumerate through one shared listing pipeline

* *GIVEN* one createVirtualSchema request resolving `CatalogKind::UnityCatalogNative` and one resolving `CatalogKind::IcebergRest`, each over a namespace holding the same table names with the same column names
* *WHEN* the adapter handles each request
* *THEN* the adapter SHALL run the SAME listing pipeline for both — one code path that reads neutral table metadata through the shared trait, flattens and case-folds names, maps each column through the single type-mapping home, builds `TABLE_MAP`, and assembles the response
* *AND* that pipeline MUST NOT name or match `CatalogKind` and MUST NOT branch on which client it holds, so the flattening rule, the collision rule, the case-fold, and the `adapterNotes` shape are provably identical across the two kinds rather than identical by coincidence
* *AND* each client SHALL remain free to source its columns its own way — the Unity Catalog client from its single inline sweep, the Iceberg REST client from its per-table load — because that choice is hidden inside the client and never reaches the pipeline

### Scenario: Create virtual schema lists the namespace with no per-table get-table call

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns every table's columns inline across one or more pages
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL obtain every table's columns from the list sweep alone, issuing only the paginated `GET /tables` requests and zero `GET /tables/{full_name}` requests
* *AND* the get-table request count SHALL stay at zero regardless of the number of listed tables, because the list response already carries each table's columns inline

### Scenario: Create virtual schema excludes every non-Delta-base entry and warns per exclusion

* *GIVEN* a Unity Catalog namespace whose `GET /tables` list sweep returns a `VIEW` entry carrying columns, no `storage_location`, and a null `data_source_format`; a `MANAGED` entry whose `data_source_format` is a non-`DELTA` value such as `ICEBERG`, `CSV`, `PARQUET`, or `JSON`; and an entry whose `table_type` is neither `MANAGED`, `EXTERNAL`, nor `VIEW`
* *WHEN* the adapter enumerates that namespace during createVirtualSchema
* *THEN* the adapter SHALL exclude all three entries from the returned virtual tables and from `TABLE_MAP`, and SHALL complete createVirtualSchema successfully with only the Delta base tables in the namespace
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` line per excluded entry naming the excluded `catalog.schema.table` identifier and the disqualifying reason — the entry's `table_type` for a view or other-type entry, or its `data_source_format` for a non-`DELTA` base table
* *AND* none of those warning lines MUST contain the resolved bearer token, any OAuth client secret, or any other credential value
* *AND* a namespace whose every listed entry is excluded SHALL yield a createVirtualSchema response with an empty table list and an empty `TABLE_MAP`, rather than an error

### Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing

* *GIVEN* a Unity Catalog table whose columns declare the Spark type names `BOOLEAN`, `INT`, `LONG`, `DOUBLE`, `STRING`, `DATE`, `TIMESTAMP`, and `DECIMAL(p,s)` whose `p` and `s` fall inside Exasol's `DECIMAL` domain — `1 ≤ p ≤ 36` and `s ≤ p`
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare `BOOLEAN` as `BOOLEAN`, `INT` as `DECIMAL(10,0)`, `LONG` as `DECIMAL(20,0)`, `DOUBLE` as `DOUBLE PRECISION`, `STRING` as `VARCHAR(2000000)`, `DATE` as `DATE`, and `DECIMAL(p,s)` as `DECIMAL(p,s)`, reusing the project's Arrow-to-Exasol convention
* *AND* the adapter SHALL declare `TIMESTAMP` and `TIMESTAMP_NTZ` as `TIMESTAMP(6)` on an Exasol version of 2025.x or later and as the bare string `TIMESTAMP` on 8.x, reading BOTH the version rule and both declaration strings from the single owner `datafusion-scan/type-mapping` specifies — so a Delta timestamp column and an Iceberg timestamp column are declared at the same precision by construction, and this feature carries no copy of either literal
* *AND* the source-tagged type descriptor the mapping reads SHALL carry the FULL parameterized Spark type — the type name plus precision and scale from the wire `type_precision`/`type_scale` — so the `DECIMAL` case resolves to `DECIMAL(p,s)` from the descriptor's own `p` and `s` rather than from a bare `DECIMAL` type name that carries neither; and the descriptor MUST NOT be described as reading `type_text`, because `ColumnInfo` declares no such field and deserializes no such value, so no `type_text` recovery path exists for a column whose `type_precision` or `type_scale` is absent
* *AND* the adapter SHALL reach that mapping through the SAME exhaustive match over the neutral column's source-tagged type descriptor that maps an Iceberg column, so the two catalog kinds have ONE Exasol type-mapping home and a third source type is a build failure there
* *AND* the adapter SHALL declare each column name uppercased through the shared case-fold site, so an unquoted column reference in user SQL resolves against the declared name

### Scenario: An incompatible Unity Catalog column type is declared as VARCHAR rather than failing

* *GIVEN* a Unity Catalog table whose columns include an array, map, struct, binary, interval, or variant type, or a `DECIMAL` whose precision and scale fall outside Exasol's `DECIMAL` domain — a precision above 36, a precision of 0 (which an absent wire `type_precision` produces through `neutral_column`'s `.unwrap_or(0)`), or a scale exceeding its own precision
* *WHEN* the adapter builds the virtual table's column list
* *THEN* the adapter SHALL declare each such column as `VARCHAR(2000000)` rather than aborting the enumeration
* *AND* the adapter SHALL resolve the `DECIMAL` cases through the SINGLE shared guard `datafusion-scan/type-mapping` owns, and MUST NOT carry a Unity-local copy of the precision/scale predicate, so a Unity `DECIMAL(0,0)` and an Iceberg `decimal(0,0)` are declared identically by construction rather than by coincidence
* *AND* the adapter MUST NOT declare an Exasol type Exasol rejects — in particular MUST NOT declare `DECIMAL(0,0)` or a `DECIMAL(p,s)` with `s > p` — because such a declaration fails `createVirtualSchema` outright, which is the failure this VARCHAR fallback exists to prevent
* *AND* the adapter SHALL treat this listing-sufficient mapping as a deliberate boundary, deferring reader-feature gating and full Delta type fidelity to #322, so #318 never produces an untyped or silently-dropped column

### Scenario: Create virtual schema records the Exasol-name to Unity-Catalog-identifier map in adapterNotes

* *GIVEN* a createVirtualSchema request under the Unity Catalog kind that enumerates one or more tables
* *WHEN* the adapter builds the createVirtualSchema response
* *THEN* the adapter SHALL record, inside the response `adapterNotes` `TABLE_MAP`, a mapping from each uppercased flattened Exasol table name to its original-cased fully-qualified `catalog.schema.table` identifier
* *AND* when two distinct Unity Catalog identifiers flatten to the same Exasol name the adapter SHALL return an error naming the colliding Exasol table name rather than dropping or overwriting a table
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `adapterNotes`

### Scenario: Create virtual schema fails clearly when the Unity Catalog is unreachable

* *GIVEN* a createVirtualSchema request under the Unity Catalog kind whose CONNECTION address cannot be reached or whose namespace cannot be listed
* *WHEN* Exasol sends the createVirtualSchema request
* *THEN* the adapter SHALL return an error describing that the Unity Catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain the resolved bearer token, any OAuth client secret, or any other credential value
