# Architecture

## Overview

```
                      Exasol cluster (one .so in the Rust SLC)
 ┌──────────────┐   pushdown JSON   ┌────────────────────────────────────────────┐
 │ Exasol SQL   │──────────────────▶│ vs-adapter                                 │
 │ (user query) │◀──────────────────│  pushdown-planner ─▶ format-readers        │
 └──────┬───────┘   dispatch SQL    │  sharding           lakehouse-catalog ─────┼──▶ Iceberg REST / Unity / Glue / STS
        │                           │  vs-expression                             │
        │ runs dispatch SQL         └────────────────────────────────────────────┘
        ▼
 ┌────────────────────────────┐  GROUP BY shard_key   ┌──────────────────────────────────┐
 │ LAKEHOUSE_DISTRIBUTE_FILES │──────────────────────▶│ LAKEHOUSE_SCAN x G shards        │
 │ (Lua SET passthrough)      │                       │  datafusion-scan ─▶ object store ─┼──▶ S3 / ADLS Gen2 Parquet
 └────────────────────────────┘                       └───────────────┬──────────────────┘
                                                                      │ Arrow C Data or Value rows
                                                                      ▼
                                                      Exasol merge (final agg, ORDER BY, LIMIT) ─▶ result
```

- Layered and stateless with two levels of parallelism: the Exasol cluster spreads G file shards across nodes, and DataFusion executes each shard inside one UDF invocation.
- The VS adapter resolves table metadata and the file list once per query. Each scan UDF invocation receives an explicit file list and discovers no files itself.
- No state survives query completion. The Tokio runtime and the DataFusion session are built per scan invocation and never cached.

## Components

- udf-entry-points (crates/lakehouse-engine/src/lib.rs): exports the three UDF entry points of the single `.so` (VS adapter, `LAKEHOUSE_SCAN`, `LAKEHOUSE_VERSION`) | owns: engine version string | depends on: vs-adapter, datafusion-scan
- vs-adapter (crates/lakehouse-engine/src/adapter/): answers Exasol Virtual Schema requests, reads the CATALOG_CONNECTION, lists catalog tables into virtual tables with one identifier note per table and one source note per column, answers a `REFRESH TABLES` by listing catalog identifiers and loading only the requested tables, and routes pushdown requests | owns: VS properties, schema-level adapterNotes (SKIPPED_TABLES, tuning values) | depends on: lakehouse-catalog, pushdown-planner, table-identity, column-notes, type-mapping, sealed-storage
- pushdown-planner (crates/lakehouse-engine/src/adapter/pushdown/): classifies the pushdown request shape (row scan, top-N, single-group aggregate, grouped aggregate, COUNT DISTINCT, join, fallback wrapper), reads each involved table's catalog identifier from its identifier note and its declared schema from its column notes, and renders the dispatch SQL that carries one scan spec per shard | owns: dispatch SQL, per-shard scan specs | depends on: format-readers, table-identity, column-notes, sharding, vs-expression, scan-spec, sealed-storage, lakehouse-catalog
- format-readers (crates/lakehouse-engine/src/adapter/pushdown/format/): declares each catalog-listed table's columns at createVirtualSchema from the neutral table metadata the catalog client returned, and at pushdown resolves one table's file list, name mapping, and table root for Iceberg, Delta, catalog Parquet, and direct-storage Parquet sources against the declared schema, failing a query whose declaration disagrees with the table's current schema or log | owns: none | depends on: column-notes, lakehouse-catalog, type-mapping
- table-identity (crates/lakehouse-engine/src/adapter/table_identity.rs): encodes each virtual table's identifier note, a table-level adapterNotes JSON object whose `name` member holds the catalog identifier, and reads the identifier back from an involved table at pushdown | owns: table identifier note format | depends on: lakehouse-catalog
- column-notes (crates/lakehouse-engine/src/adapter/column_notes.rs): encodes, size-checks, and parses the per-column declaration notes, and holds the refused-column type and the refusal helpers the declaration builders share | owns: column note format | depends on: scan-spec
- sharding (crates/lakehouse-engine/src/adapter/sharding.rs): computes the shard count G and partitions the file list into G byte-balanced shards | owns: none | depends on: scan-spec
- scan-spec (crates/lakehouse-engine/src/scan/spec.rs): defines the format-neutral JSON wire format between the adapter and the scan UDF (`CommonScanSpec`, `FileEntry`, `LogicalField`, `ScanStorage`) | owns: scan spec wire format | depends on: lakehouse-catalog
- sealed-storage (crates/lakehouse-engine/src/scan/sealed.rs): encrypts storage credentials into the scan spec with AES-GCM under a key derived by HKDF-SHA256 from the CONNECTION password, and decrypts them in the scan UDF | owns: sealed storage envelope format | depends on: none
- datafusion-scan (crates/lakehouse-engine/src/scan/): runs one shard per invocation as a raw scan, a partial aggregate, or a broadcast join scan, applies Iceberg positional deletes and Iceberg and Delta deletion vectors, and emits results batch by batch | owns: per-invocation Tokio runtime, DataFusion session, memory pool | depends on: scan-spec, sealed-storage, scan-object-store, type-mapping, vs-expression
- scan-object-store (crates/lakehouse-engine/src/scan/object_store.rs): builds S3 or ADLS Gen2 object stores with a connection budget and an admission limit, and routes the two join sides by path prefix | owns: per-invocation HTTP connection pool | depends on: lakehouse-catalog
- type-mapping (crates/lakehouse-engine/src/types/): maps Arrow, Iceberg, Delta, and Hive types to Exasol types and applies type widening rules | owns: none | depends on: none
- lakehouse-catalog (crates/lakehouse-catalog/): implements `CatalogClient` for Iceberg REST, Unity Catalog, and AWS Glue (a full listing, an identifier-only listing, a load of listed tables that classifies each table as the full listing does, and the pushdown's single-table load), with OAuth2, bearer, and SigV4 auth, STS role assumption, vended-storage resolution, and credential redaction; its neutral table carries the catalog's table properties verbatim and its neutral column an opaque engine-encoded declaration slot, and it interprets neither | owns: per-request catalog sessions | depends on: none
- vs-expression (crates/vs-expression/): translates VS expression JSON into DataFusion SQL and Exasol SQL fragments | owns: none | depends on: none
- distribute-files (deploy/scripts/install.sh): defines the Lua SET script `LAKEHOUSE_DISTRIBUTE_FILES`, which re-emits per-shard file-list rows so `GROUP BY shard_key` spreads them across nodes | owns: none | depends on: none
- installer (deploy/scripts/install.sh): downloads the engine and SLC release assets, uploads them to BucketFS with exapump, creates the scripts, and runs a version and fingerprint smoke test | owns: none | depends on: distribute-files

## Data Flow

- CREATE or REFRESH VIRTUAL SCHEMA -> vs-adapter -> lakehouse-catalog -> catalog service: table listing returns as virtual table definitions, one identifier note per table holding its catalog identifier, and one column note per column holding the declared source shape
- REFRESH TABLES -> vs-adapter -> lakehouse-catalog -> catalog service: the namespace's catalog identifiers are listed without loading any table, only the identifiers whose Exasol name is requested are loaded and collision-checked, the response carries only the requested tables, and Exasol keeps every other virtual table with its notes
- user query -> Exasol -> vs-adapter: a pushdown request JSON with projection, filter, aggregates, ORDER BY, LIMIT, join tree, and each involved table's identifier note and column notes
- vs-adapter -> format-readers -> lakehouse-catalog and object storage: snapshot and file list resolve once per query against the declared schema the column notes carry; no reader derives a schema
- format-readers -> sharding -> pushdown-planner: the file list becomes G byte-balanced shards, each serialized as `common` and `files` JSON strings in the dispatch SQL
- dispatch SQL -> LAKEHOUSE_DISTRIBUTE_FILES -> GROUP BY shard_key -> LAKEHOUSE_SCAN: Exasol runs one scan VM per node, which scans that node's shard rows one after another
- LAKEHOUSE_SCAN -> datafusion-scan -> scan-object-store -> object storage: Parquet byte ranges stream into one DataFusion plan per invocation
- datafusion-scan -> Exasol: raw and join rows leave through the Arrow C Data Interface via `ctx.emit_batch`, partial aggregates leave as SDK `Value` rows via `ctx.emit`
- Exasol -> result: the dispatch SQL wrapper merges partial aggregates and applies the final ORDER BY and LIMIT

## Interfaces

- VS adapter protocol: Exasol calls the adapter entry point with request JSON of type `getCapabilities`, `createVirtualSchema`, `refresh`, `setProperties`, `dropVirtualSchema`, or `pushdown`; a pushdown answer is `{"type":"pushdown","sql":...}`
- VS properties: `CATALOG_CONNECTION`, `NAMESPACE`, `CATALOG_KIND` (absent means Iceberg REST, else `UNITY_CATALOG`, `GLUE`, `DIRECT_STORAGE`), `ALLOW_HTTP`, `PARALLELISM_FACTOR`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `JOIN_BROADCAST_MAX_BYTES`, `S3_MAX_CONNECTIONS`, and for `DIRECT_STORAGE` also `MERGE_SCHEMA` (fold every file footer, default) and `HIVE_PARTITIONING` (`key=value` directory segments become partition columns, default `TRUE`), both applied at CREATE, REFRESH, and SET only
- CONNECTION object: the address is the catalog URI (empty allowed for `DIRECT_STORAGE`), and the password is a JSON object of catalog and storage credentials
- adapterNotes: Exasol persists `SKIPPED_TABLES` and the tuning values between CREATE and pushdown, up to 2,000,000 bytes
- table adapterNotes: each virtual table's identifier note is a JSON object string `{"name": <catalog identifier>}`, persisted in `EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES` and returned in `involvedTables[].adapterNotes`
- column adapterNotes: each virtual column's note is a JSON object string `{"sourceType": <LogicalField>, "partition": <position>, "refused": <reason>}`, persisted in `EXA_ALL_VIRTUAL_COLUMNS.ADAPTER_NOTES` and returned in `involvedTables[].columns[].adapterNotes`, up to the measured per-column limit
- `LAKEHOUSE_SCAN(common VARCHAR, files VARCHAR)`: scalar UDF that handles one shard row, reads the JSON scan spec halves, and emits the columns that the dispatch SQL declares
- `LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000))`: Lua SET script that emits its input rows unchanged
- `LAKEHOUSE_VERSION()`: returns the `lakehouse-engine` crate version string
- install.sh CLI: `install.sh --account-id <id> --database-id <id> --profile <p>` for SaaS, or `--dsn`, `--host`, `--target`, `--deployment`, `--bfs-*`, `--schema`, `--lakehouse-version`, `--slc-version`, `--arch`, `--skip-slc`
- Release asset: `lakehouse-engine.tar.gz` on GitHub releases, installed to BucketFS at `udf/liblakehouse_engine.so`
- Makefile: `cross-udf-build`, `test`, `test-e2e`, `test-e2e-lakekeeper`, `test-e2e-azure`, `test-e2e-glue`, `test-e2e-unity`, `install-slc`, `bench`

## Constraints

- The toolchain is pinned in `rust-toolchain.toml`, with edition 2024. Every dependency version is pinned in the workspace manifest, not in the specs.
- The release `.so` builds only in the Makefile's `UDF_BUILDER_IMAGE`, whose rustc and glibc match the SLC, because the SLC rejects a `.so` whose SDK fingerprint (`exasol-udf-sdk` version and rustc hash) differs from its own.
- `exasol-udf-sdk` and `exasol-udf-macros` are pinned in the workspace manifest to the SLC's own version.
- `arrow` and `parquet` stay on the Arrow major that `datafusion` and the SDK depend on, not on the major of the `datafusion` or SDK package itself.
- Arrow types never cross the `.so` boundary. Only SDK `Value`s or the Arrow C Data Interface structs from `ctx.emit_batch` cross it.
- `lakehouse-catalog` never depends on `lakehouse-engine`. Its manifest declares no `arrow`, `parquet`, `datafusion`, or `object_store` dependency, and it names no scan-spec type (`FileEntry`, `LogicalField`, `DeleteMechanism`, `CommonScanSpec`). A neutral catalog column carries an Arrow type only as the engine's type-tag string.
- `lakehouse-catalog` declares the shared credential types (`ConnectionCreds`, `StorageProps`, `CatalogProps`, `StorageBackend`) and the credential redaction functions once. The engine re-exports them and declares no copy. Reading and parsing the Exasol CONNECTION object stays in the engine.
- The engine enumerates and declares catalog tables through the `CatalogClient` trait and its neutral table and column types. In `lakehouse-catalog`, auth strategies, HTTP and signing helpers, and vended-resolution steps stay crate-private, and no AWS SDK type crosses the crate boundary.
- Each catalog kind's identifier-only listing returns every identifier its full listing would return or skip, in listing order, without loading table metadata. Its load of listed tables admits or skips each given table exactly as the full listing does and loads no other table.
- The Makefile's `VS_SRCS` lists every crate's sources and manifest, so a change in `lakehouse-catalog` or `vs-expression` rebuilds the `.so` before an E2E run.
- UDFs keep no cross-call state. The scan UDF builds its Tokio runtime per call and never caches it.
- The engine owns no persisted metadata. Exasol stores the schema-level adapterNotes values (`SKIPPED_TABLES`, tuning values), the table identifier notes, and the column notes between CREATE and pushdown.
- A table's declared schema is decided at CREATE, REFRESH, or SET and read from its column notes at pushdown. No format reader derives a logical schema, a partition column, or a refusal at pushdown.
- The adapter resolves metadata once per query. The scan UDF never discovers files.
- Each node scans only its assigned files, with no overlap between shards.
- The shard count is G = node count × `PARALLELISM_FACTOR` (default 1), clamped to at least 1 and at most min(file count, 300).
- The DataFusion memory pool is max((memory limit − 200 MB overhead) × 0.6, 256 MiB), or 1 GiB when the limit is unknown.
- The scan spills to `/tmp` when `/tmp` is a writable non-tmpfs disk. Otherwise it returns a `ResourcesExhausted` error past the pool budget.
- The scan streams one `RecordBatch` at a time and never collects the full result.
- Errors never contain credentials. Storage credentials reach the scan UDF only as a CONNECTION name or a sealed envelope.
- STS AssumeRole and Glue operations time out after 30 seconds.
- Every Iceberg REST, Lakekeeper, Unity Catalog, and OAuth2 token request goes through the one crate-private catalog HTTP client of `lakehouse-catalog`. It opens a connection within 10 seconds, retries only a transient failure, up to 5 attempts, and ends each request within 30 seconds, retries included.
- Exasol's own dispatcher throttles concurrent UDF VMs at 80% of the per-process memory limit.
- DSNs in the Makefile, bench scripts, and installer include `validateservercertificate=0`.
- The engine must be faster than single-node DataFusion, scale with added Exasol nodes, keep duplicate scanning minimal, and keep metadata overhead acceptable.

## External Dependencies

- Iceberg REST catalog (including Lakekeeper and Databricks-managed Iceberg): snapshot discovery and file list resolution for Iceberg tables | failure impact: no Iceberg query can be planned or executed
- Lakekeeper management API (`/management/v1/action/batch-check`): the permission check of a virtual schema that sets `PERMISSION_CHECK = 'LAKEKEEPER'` | failure impact: every query of such a virtual schema is refused
- Unity Catalog REST API (`/api/2.1/unity-catalog`, including Databricks): table listing with each table's columns and properties (the source of its column notes), Delta log location, and temporary table credentials | failure impact: no Delta or Unity Parquet query can be planned or executed
- AWS Glue Data Catalog: table listing and routing, Iceberg metadata location, and Hive Parquet partition lists | failure impact: no `GLUE` query can be planned or executed
- Databricks (as Iceberg REST or Unity Catalog endpoint): access to Databricks-managed tables through either catalog kind | failure impact: Databricks queries fail on both routes, and non-Databricks catalogs are unaffected
- OAuth2 token endpoint: client-credentials grant for REST catalog and Unity Catalog authentication | failure impact: same as the catalog it authenticates being unavailable
- AWS STS: session credentials for a CONNECTION that names an IAM role (`aws_assume_role_arn`) | failure impact: every request through that CONNECTION fails, with no fallback to the static base key pair
- S3-compatible object storage: Parquet data files and table metadata files | failure impact: scans fail or stall, and this is a measured bottleneck risk
- Azure ADLS Gen2 (`abfss://`): Parquet data files and table metadata files for Azure-hosted tables | failure impact: scans of ADLS-hosted tables fail
- Exasol cluster and Rust SLC in BucketFS: UDF execution substrate | failure impact: no execution
- GitHub releases (`exasol-labs/lakehouse-engine-rs`, `exasol-labs/language-container-rs`): engine and SLC assets for install.sh | failure impact: installation fails, and running deployments are unaffected
- exapump: BucketFS upload and SQL execution for install.sh | failure impact: installation fails
