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
                                                                      │ Arrow IPC or Value rows
                                                                      ▼
                                                      Exasol merge (final agg, ORDER BY, LIMIT) ─▶ result
```

- Layered and stateless with two levels of parallelism: the Exasol cluster spreads G file shards across nodes, and DataFusion executes each shard inside one UDF invocation.
- The VS adapter resolves table metadata and the file list once per query. Each scan UDF invocation receives an explicit file list and discovers no files itself.
- No state survives query completion. The Tokio runtime and the DataFusion session are built per scan invocation and never cached.

## Components

- udf-entry-points (crates/lakehouse-engine/src/lib.rs): exports the three UDF entry points of the single `.so` (VS adapter, `LAKEHOUSE_SCAN`, `LAKEHOUSE_VERSION`) | owns: engine version string | depends on: vs-adapter, datafusion-scan
- vs-adapter (crates/lakehouse-engine/src/adapter/): answers Exasol Virtual Schema requests, reads the CATALOG_CONNECTION, lists catalog tables into virtual tables, and routes pushdown requests | owns: VS properties, adapterNotes (TABLE_MAP, SKIPPED_TABLES, tuning values) | depends on: lakehouse-catalog, pushdown-planner, type-mapping, sealed-storage
- pushdown-planner (crates/lakehouse-engine/src/adapter/pushdown/): classifies the pushdown request shape (row scan, top-N, single-group aggregate, grouped aggregate, COUNT DISTINCT, join, fallback wrapper) and renders the dispatch SQL that carries one scan spec per shard | owns: dispatch SQL, per-shard scan specs | depends on: format-readers, sharding, vs-expression, scan-spec, sealed-storage, lakehouse-catalog
- format-readers (crates/lakehouse-engine/src/adapter/pushdown/format/): resolves one table into a format-neutral file list, logical schema, name mapping, and partition columns for Iceberg, Delta, catalog Parquet, and direct-storage Parquet sources | owns: none | depends on: lakehouse-catalog, type-mapping
- sharding (crates/lakehouse-engine/src/adapter/sharding.rs): computes the shard count G and partitions the file list into G byte-balanced shards | owns: none | depends on: scan-spec
- scan-spec (crates/lakehouse-engine/src/scan/spec.rs): defines the format-neutral JSON wire format between the adapter and the scan UDF (`CommonScanSpec`, `FileEntry`, `LogicalField`, `ScanStorage`) | owns: scan spec wire format | depends on: lakehouse-catalog
- sealed-storage (crates/lakehouse-engine/src/scan/sealed.rs): encrypts storage credentials into the scan spec with AES-GCM under a key derived by HKDF-SHA256 from the CONNECTION password, and decrypts them in the scan UDF | owns: sealed storage envelope format | depends on: none
- datafusion-scan (crates/lakehouse-engine/src/scan/): runs one shard per invocation as a raw scan, a partial aggregate, or a broadcast join scan, applies Iceberg positional deletes and Delta deletion vectors, and emits results batch by batch | owns: per-invocation Tokio runtime, DataFusion session, memory pool | depends on: scan-spec, sealed-storage, scan-object-store, type-mapping, vs-expression
- scan-object-store (crates/lakehouse-engine/src/scan/object_store.rs): builds S3 or ADLS Gen2 object stores with a connection budget and an admission limit, and routes the two join sides by path prefix | owns: per-invocation HTTP connection pool | depends on: lakehouse-catalog
- type-mapping (crates/lakehouse-engine/src/types/): maps Arrow, Iceberg, Delta, and Hive types to Exasol types and applies type widening rules | owns: none | depends on: none
- lakehouse-catalog (crates/lakehouse-catalog/): implements `CatalogClient` for Iceberg REST, Unity Catalog, and AWS Glue, with OAuth2, bearer, and SigV4 auth, STS role assumption, vended-storage resolution, and credential redaction | owns: per-request catalog sessions | depends on: none
- vs-expression (crates/vs-expression/): translates VS expression JSON into DataFusion SQL and Exasol SQL fragments | owns: none | depends on: none
- distribute-files (deploy/scripts/install.sh): defines the Lua SET script `LAKEHOUSE_DISTRIBUTE_FILES`, which re-emits per-shard file-list rows so `GROUP BY shard_key` spreads them across nodes | owns: none | depends on: none
- installer (deploy/scripts/install.sh): downloads the engine and SLC release assets, uploads them to BucketFS with exapump, creates the scripts, and runs a version and fingerprint smoke test | owns: none | depends on: distribute-files

## Data Flow

- CREATE or REFRESH VIRTUAL SCHEMA -> vs-adapter -> lakehouse-catalog -> catalog service: table listing returns as virtual table definitions and an adapterNotes TABLE_MAP of Exasol name to catalog identifier
- user query -> Exasol -> vs-adapter: a pushdown request JSON with projection, filter, aggregates, ORDER BY, LIMIT, and join tree
- vs-adapter -> format-readers -> lakehouse-catalog and object storage: snapshot, file list, and logical schema resolve once per query
- format-readers -> sharding -> pushdown-planner: the file list becomes G byte-balanced shards, each serialized as `common` and `files` JSON strings in the dispatch SQL
- dispatch SQL -> LAKEHOUSE_DISTRIBUTE_FILES -> GROUP BY shard_key -> LAKEHOUSE_SCAN: Exasol runs one scan invocation per shard row on its node VM pool
- LAKEHOUSE_SCAN -> datafusion-scan -> scan-object-store -> object storage: Parquet byte ranges stream into one DataFusion plan per invocation
- datafusion-scan -> Exasol: raw and join rows leave as Arrow IPC bytes via `ctx.emit_batch`, partial aggregates leave as SDK `Value` rows via `ctx.emit`
- Exasol -> result: the dispatch SQL wrapper merges partial aggregates and applies the final ORDER BY and LIMIT

## Interfaces

- VS adapter protocol: Exasol calls the adapter entry point with request JSON of type `getCapabilities`, `createVirtualSchema`, `refresh`, `setProperties`, `dropVirtualSchema`, or `pushdown`; a pushdown answer is `{"type":"pushdown","sql":...}`
- VS properties: `CATALOG_CONNECTION`, `NAMESPACE`, `CATALOG_KIND` (absent means Iceberg REST, else `UNITY_CATALOG`, `GLUE`, `DIRECT_STORAGE`), `ALLOW_HTTP`, `PARALLELISM_FACTOR`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `DATAFUSION_THREADING_MODE`, `DATAFUSION_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `JOIN_BROADCAST_MAX_BYTES`, `S3_MAX_CONNECTIONS`, and for `DIRECT_STORAGE` also `MERGE_SCHEMA` (fold every file footer, default) and `HIVE_PARTITIONING` (`key=value` directory segments become partition columns, default `TRUE`)
- CONNECTION object: the address is the catalog URI (empty allowed for `DIRECT_STORAGE`), and the password is a JSON object of catalog and storage credentials
- adapterNotes: Exasol persists `TABLE_MAP`, `SKIPPED_TABLES`, and the tuning values between CREATE and pushdown, up to 2,000,000 bytes
- `LAKEHOUSE_SCAN(common VARCHAR, files VARCHAR)`: scalar UDF that handles one shard row, reads the JSON scan spec halves, and emits the columns that the dispatch SQL declares
- `LAKEHOUSE_DISTRIBUTE_FILES(files VARCHAR(2000000))`: Lua SET script that emits its input rows unchanged
- `LAKEHOUSE_VERSION()`: returns the `lakehouse-engine` crate version string
- install.sh CLI: `install.sh --account-id <id> --database-id <id> --profile <p>` for SaaS, or `--dsn`, `--host`, `--target`, `--deployment`, `--bfs-*`, `--schema`, `--lakehouse-version`, `--slc-version`, `--arch`, `--skip-slc`
- Release asset: `lakehouse-engine.tar.gz` on GitHub releases, installed to BucketFS at `udf/liblakehouse_engine.so`
- Makefile: `cross-udf-build`, `test`, `test-e2e`, `test-e2e-lakekeeper`, `test-e2e-azure`, `test-e2e-glue`, `test-e2e-unity`, `install-slc`, `bench`

## Constraints

- The toolchain is pinned to Rust 1.94 with edition 2024.
- The release `.so` builds only in the `rust:1.94-trixie` image (glibc 2.41), because the SLC rejects a `.so` whose SDK fingerprint (`exasol-udf-sdk` version and rustc hash) differs from its own.
- `exasol-udf-sdk` and `exasol-udf-macros` stay pinned at 0.30.0 in the workspace manifest, and that version is also the SLC version.
- `arrow` and `parquet` stay on major 58 to match `datafusion` 54 and the SDK.
- Arrow types never cross the `.so` boundary. Only SDK `Value`s or Arrow IPC bytes from `ctx.emit_batch` cross it.
- UDFs keep no cross-call state. The scan UDF builds its Tokio runtime per call and never caches it.
- The engine owns no persisted metadata. Exasol stores the adapterNotes values (`TABLE_MAP`, `SKIPPED_TABLES`, tuning values) between CREATE and pushdown.
- The adapter resolves metadata once per query. The scan UDF never discovers files.
- Each node scans only its assigned files, with no overlap between shards.
- The shard count is G = node count × `PARALLELISM_FACTOR` (default 8), clamped to at least 1 and at most min(file count, 300).
- The DataFusion memory pool is max((memory limit − 200 MB overhead) × 0.6, 256 MiB), or 1 GiB when the limit is unknown.
- The scan spills to `/tmp` when `/tmp` is a writable non-tmpfs disk. Otherwise it returns a `ResourcesExhausted` error past the pool budget.
- The scan streams one `RecordBatch` at a time and never collects the full result.
- Errors never contain credentials. Storage credentials reach the scan UDF only as a CONNECTION name or a sealed envelope.
- STS AssumeRole and Glue operations time out after 30 seconds.
- Exasol's own dispatcher throttles concurrent UDF VMs at 80% of the per-process memory limit.
- DSNs in the Makefile, bench scripts, and installer include `validateservercertificate=0`.
- The engine must be faster than single-node DataFusion, scale with added Exasol nodes, keep duplicate scanning minimal, and keep metadata overhead acceptable.

## External Dependencies

- Iceberg REST catalog (including Lakekeeper and Databricks-managed Iceberg): snapshot discovery and file list resolution for Iceberg tables | failure impact: no Iceberg query can be planned or executed
- Lakekeeper management API (`/management/v1/action/batch-check`): the permission check of a virtual schema that sets `PERMISSION_CHECK = 'LAKEKEEPER'` | failure impact: every query of such a virtual schema is refused
- Unity Catalog REST API (`/api/2.1/unity-catalog`, including Databricks): table listing, Delta log location, and temporary table credentials | failure impact: no Delta or Unity Parquet query can be planned or executed
- AWS Glue Data Catalog: table listing and routing, Iceberg metadata location, and Hive Parquet partition lists | failure impact: no `GLUE` query can be planned or executed
- Databricks (as Iceberg REST or Unity Catalog endpoint): access to Databricks-managed tables through either catalog kind | failure impact: Databricks queries fail on both routes, and non-Databricks catalogs are unaffected
- OAuth2 token endpoint: client-credentials grant for REST catalog and Unity Catalog authentication | failure impact: same as the catalog it authenticates being unavailable
- AWS STS: session credentials for a CONNECTION that names an IAM role (`aws_assume_role_arn`) | failure impact: every request through that CONNECTION fails, with no fallback to the static base key pair
- S3-compatible object storage: Parquet data files and table metadata files | failure impact: scans fail or stall, and this is a measured bottleneck risk
- Azure ADLS Gen2 (`abfss://`): Parquet data files and table metadata files for Azure-hosted tables | failure impact: scans of ADLS-hosted tables fail
- Exasol cluster and Rust SLC in BucketFS: UDF execution substrate | failure impact: no execution
- GitHub releases (`exasol-labs/lakehouse-engine-rs`, `exasol-labs/language-container-rs`): engine and SLC assets for install.sh | failure impact: installation fails, and running deployments are unaffected
- exapump: BucketFS upload and SQL execution for install.sh | failure impact: installation fails
