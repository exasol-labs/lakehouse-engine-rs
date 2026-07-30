# Feature: Refresh And Set Properties

Lets an Exasol user re-read the Iceberg catalog for an existing virtual schema in place — through `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET` — so a namespace's added, dropped, renamed, or type-changed tables and columns become queryable without a `DROP ... CASCADE` + `CREATE`, which loses dependent views and grants.

## Background

* The adapter is stateless per `vs-adapter/create-virtual-schema` — it holds no catalog metadata between requests other than what it returns in `schemaMetadata.adapterNotes`, which Exasol persists and round-trips back. `refresh` and `setProperties` are therefore not cache invalidation; each re-runs the same full namespace enumeration as `createVirtualSchema` and re-emits an updated `schemaMetadata`.
* Enumeration, schema resolution, type mapping, `adapterNotes` construction (including `TABLE_MAP`), and credential redaction reuse the `createVirtualSchema` path verbatim; the only differences are the request `type` recognised, the merge precedence of the incoming properties, the response `type` label, and the `requestedTables` echo.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Refresh rebuilds the table map and preserves other adapter notes

* *GIVEN* a `refresh` request whose `schemaMetadataInfo.adapterNotes` carries the persisted notes from creation (`NR_OF_CORES`, `PARALLELISM_FACTOR`, the DataFusion threading and memory-budget entries, and `TABLE_MAP`), and that MAY additionally carry a `CLUSTER_NODES` entry when the schema was created by an adapter version that still recorded the node count
* *WHEN* the adapter builds the `refresh` response
* *THEN* the adapter SHALL rebuild `TABLE_MAP` from the re-enumerated tables — a full rebuild, never a diff or patch of the prior map
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry when writing the rebuilt `TABLE_MAP`, with an inherited `CLUSTER_NODES` entry as the ONE exception — it is REMOVED, so the stale node count disappears from the persisted notes on the first `refresh` after the upgrade (see `vs-adapter/create-virtual-schema-adapter-notes`)
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `schemaMetadata.adapterNotes`
<!-- /DELTA:CHANGED -->
