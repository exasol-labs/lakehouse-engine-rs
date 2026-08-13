# Feature: Unity Catalog Native REST Client

A thin bespoke client in `crates/lakehouse-catalog` that talks to the native Unity Catalog REST API to enumerate tables and load a table's metadata. Its listing operation filters to Delta base tables: it returns one neutral table per entry whose `table_type` is `MANAGED` or `EXTERNAL` and whose `data_source_format` is `DELTA`, and routes every other listed entry into the skipped set with a neutral reason. It implements the shared `CatalogClient` trait, so the engine reaches it through the same operations it uses for the Iceberg REST catalog and never sees a Unity Catalog wire type. It reuses the crate's existing `reqwest` client and `serde` types and targets the standard API — never the Iceberg-REST compatibility endpoint and never the `delta/v1` Delta Tables API. One client serves both OSS Unity Catalog and Databricks-managed Unity Catalog, because the standard API is identical on both, so there is no Databricks-specific code path.

## Background

* **This delta changes THREE scenarios and adds ONE; it is issue #319.** The neutral table gains two
  fields the Delta scan path needs — a closed table-FORMAT tag and the catalog-assigned key that
  per-table storage credentials are vended against — and the single-table load gains a production
  caller plus a fail-loud refusal for a format the crate cannot name.
* **SUPERSEDES the clause "`data_source_format` SHALL remain a crate-private wire field that MUST NOT
  appear in any neutral type the engine can name, because the Delta-base decision is owned inside the
  client".** Two decisions were conflated under one field. The LISTING-ADMISSION decision — which
  entries are Delta base tables and which are skipped with a reason — stays owned inside the client,
  unchanged and unweakened. The table's FORMAT is not that decision: it is data the engine's format
  dispatch reads to route a scan (`vs-adapter/delta-table-planning`), and withholding it would force
  the engine to assume Unity Catalog implies Delta rather than check it. The raw wire STRING stays
  crate-private; what crosses is a closed neutral enum with one variant per format the engine can
  plan.
* **SUPERSEDES the clause "in this plan it is exercised by the shared trait-contract tests rather
  than by a production caller".** The single-table load is now called by the Delta format reader,
  which is the scan-path caller that clause anticipated.
* **The vending key is OPAQUE to every caller.** It is the Unity Catalog `table_id` the temporary-
  table-credentials POST is scoped against. No caller outside this crate interprets it; a caller hands
  it back to the same client. Naming the neutral field for the DECISION it serves rather than for the
  wire field it holds is what keeps the engine from reading a Unity Catalog concept out of it.
* `table_id` was previously not deserialized at all — the wire type's doc comment lists it among
  "wire fields this client does not consume". That comment is now false and is corrected with the
  field.
* No error, log line, or skip reason gains a credential value. The vending key is a table identity,
  not a secret; it is nonetheless never logged, because it is the scope of a credential request.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The Unity Catalog session is reached only through the shared catalog-client trait

* *GIVEN* a `UnityCatalogSession` and the shared `CatalogClient` trait the Iceberg REST catalog client also implements
* *WHEN* the engine adapter enumerates a namespace or loads one table's metadata under the Unity Catalog kind
* *THEN* `UnityCatalogSession` SHALL implement `CatalogClient`, and the engine SHALL reach both operations THROUGH that trait, so the adapter runs the same pipeline it runs for the Iceberg REST kind
* *AND* both operations SHALL return the catalog-NEUTRAL metadata types the trait declares, converting each deserialized Unity Catalog wire entry into a neutral table whose identifier carries the `catalog`/`schema` namespace segments and the table name, whose storage location is absent when the entry omits it, whose table FORMAT is a closed neutral tag, whose credential-vending key is the catalog-assigned key or absent, and whose ordered columns each carry the column name in its ORIGINAL case plus a source-tagged Unity Catalog type descriptor
* *AND* the Unity Catalog wire types MUST NOT appear in any signature the engine can name, because the engine consuming a Unity-specific shape is exactly the per-kind branch the shared trait exists to remove
* *AND* the client MUST NOT map a column type to an Exasol type, because that mapping belongs to the engine's single type-mapping home and this crate MUST NOT name the Exasol delivery mechanism
* *AND* the neutral format tag SHALL be a closed enum naming exactly the formats the engine can plan, so a format outside it is a deserialization-time refusal rather than a value the engine matches non-exhaustively
* *AND* the neutral credential-vending key SHALL be documented as OPAQUE — a value a caller hands back to the client that produced it and never parses — so no consumer outside this crate can derive a Unity Catalog request from it
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The client lists tables in a configured catalog and schema

* *GIVEN* a `UnityCatalogSession` and a configured Unity Catalog namespace of the form `catalog.schema`
* *WHEN* the adapter enumerates the tables in that namespace through the shared trait
* *THEN* the client SHALL issue `GET /tables?catalog_name={catalog}&schema_name={schema}` on the session and SHALL return one fully-populated neutral table per listed entry that is a DELTA BASE TABLE — an entry whose `table_type` is `MANAGED` or `EXTERNAL` AND whose `data_source_format` is `DELTA` — carrying its identifier, table type, storage location, its neutral format tag, its credential-vending key when the entry carries one, and its ordered columns (each column carrying its name and its FULL parameterized Unity Catalog Spark type — type name plus precision and scale, sufficient to declare `DECIMAL(p,s)`) all from the single list response
* *AND* every neutral table this operation returns SHALL carry the Delta format tag, because the admission filter above has already excluded every other format — so the tag restates the filter's outcome rather than re-deciding it
* *AND* the client SHALL place every other listed entry — a `VIEW`, a base table whose `data_source_format` is not `DELTA`, or an entry of any other `table_type` — into the returned skipped set rather than returning it as a neutral table, so the skipped set is NOT always empty and a non-Delta or non-base entry never reaches the listing pipeline as a table
* *AND* the crate-private wire type SHALL model `data_source_format` as optional (a `VIEW` carries it as null), and the raw `data_source_format` STRING SHALL remain a crate-private wire field that MUST NOT appear in any neutral type the engine can name — SUPERSEDING the recorded clause that extended that prohibition to the table's format itself: the LISTING-ADMISSION decision stays owned inside the client, while a closed neutral format tag crosses the boundary because the engine's format dispatch reads it
* *AND* the client MUST NOT set the `omit_columns` request parameter, SHALL follow `page_token`/`next_page_token` pagination so every page's entries are classified and no listed table is truncated, and SHALL address every request under the `{host}/api/2.1/unity-catalog` base path — never the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The client retrieves a table's metadata including its columns

* *GIVEN* a `UnityCatalogSession` and a fully-qualified table identifier naming a catalog, a schema, and a table
* *WHEN* a caller loads that single table's metadata through the shared trait
* *THEN* the client SHALL issue `GET /tables/{catalog.schema.table}` on the session and return one neutral table carrying its table type, storage location, its neutral format tag, its credential-vending key, and an ordered list of columns, each column carrying its name and its full parameterized Unity Catalog Spark type (type name plus precision and scale, sufficient to declare `DECIMAL(p,s)`)
* *AND* the returned column order SHALL preserve the order the Unity Catalog response declares, so downstream column mapping keeps the table's declared schema order
* *AND* the createVirtualSchema listing path MUST NOT call this single-table load for column metadata — it reads columns inline from the `GET /tables` list sweep — so this load SHALL serve only the scan-path single-table metadata source, whose production caller is now the Delta format reader of `vs-adapter/delta-table-planning`, SUPERSEDING the recorded clause that stated this load has no production caller
* *AND* this load SHALL apply NO listing-admission filter, because it loads the one table its caller named rather than choosing which tables to admit — so the format tag it returns is the caller's ONLY signal that the named table is not a Delta table
* *AND* the returned credential-vending key SHALL be absent rather than empty when the response carries none, so a caller that requires one fails naming the table instead of POSTing an empty scope
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The single-table load refuses a data source format the crate cannot name

* *GIVEN* a `UnityCatalogSession` and a fully-qualified identifier naming a table whose
  `data_source_format` is absent, or is a value that names neither Delta nor Iceberg — such as `CSV`,
  `PARQUET`, or `DELTASHARING`
* *WHEN* a caller loads that single table's metadata through the shared trait
* *THEN* the client SHALL return a `UdfError` naming the table and the unrecognized or absent
  `data_source_format` value, and MUST NOT return a neutral table
* *AND* the client MUST NOT default the format tag to Delta, because the load applies no admission
  filter and a defaulted tag would route a CSV table into the Delta log reader, which would surface a
  missing-transaction-log error rather than a clear format refusal
* *AND* the client SHALL map `DELTA` to the Delta tag and `ICEBERG` to the Iceberg tag, comparing
  against the uppercase Unity Catalog vocabulary the wire emits, so a Unity Catalog UniForm table
  reporting `ICEBERG` is named accurately rather than refused as unrecognized
* *AND* the error SHALL be returned as a `UdfError` value rather than raised as a panic, because a
  panic inside a UDF is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the
  statement part
* *AND* the error message MUST NOT contain the resolved bearer token, any OAuth client secret, or any
  other credential value
<!-- /DELTA:NEW -->
