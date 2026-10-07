# Mission: lakehouse-engine, Exasol In-Place Lakehouse Query Engine

> An **in-place query engine**: technically an Exasol Virtual Schema, but rather than only translating and planning it runs the DataFusion engine on the node, in place. It queries Apache Iceberg and Databricks-managed datasets inside Rust UDFs and uses the Exasol cluster as a distributed execution substrate. (Repo: `lakehouse-engine-rs`; the `-rs` is an external "built in Rust" hint. Internally the project is `lakehouse-engine`.)

## Problem Statement

Analytical teams want to query Apache Iceberg and Databricks-managed datasets with the speed and scale of a distributed engine, straight from Exasol SQL. This Virtual Schema pairs Exasol's cluster distribution with DataFusion's vectorized execution into one distributed lakehouse query engine.

Files are sharded across Exasol nodes and scanned in parallel by node-local DataFusion runtimes, then merged in Exasol. Lakehouse scans therefore scale with the cluster instead of bottlenecking on a single node. Projection, filter, and LIMIT pushdown keep each scan lean, and node-local aggregation keeps network transfer small.

The payoff: open lakehouse data (Iceberg, Databricks) becomes first-class and queryable through plain Exasol SQL at cluster scale, with no copy, no caching, and no separate query stack to operate.

## Target Users

| Persona | Goal | Key Workflow |
|---------|------|--------------|
| Exasol engineer / architect | Decide whether to invest in a DataFusion-on-Exasol query path | Run benchmark queries against Iceberg/Databricks through the VS, read the scaling and overhead measurements |
| Analyst (validation proxy) | Query lakehouse tables with plain SQL through Exasol | `SELECT ... FROM <virtual_schema>.<table>` and get correct results |

## Core Capabilities

### Execution model

1. **Stateless Virtual Schema**: translates a user query, analyzes pushdowns, plans parallelization, and maps result schemas. Thin: most execution logic lives in DataFusion.
2. **DataFusion-in-UDF execution**: a disposable Rust UDF creates a DataFusion session, registers Iceberg or Delta tables, applies pushdowns, scans its assigned files, and produces partial results.
3. **File-level cluster parallelism**: the adapter resolves the file list once per query, format-neutral across Iceberg, Delta, and Hive. It partitions the files into G oversubscribed work-unit shards (G = node_count × parallelism_factor, capped at 300), driven via `GROUP BY shard_key`. Exasol distributes the shard groups across nodes and multiplexes them onto each node's core pool. No node scans another node's files.
4. **Bounded, self-throttling execution**: the scan UDF sizes its DataFusion memory pool from the per-instance memory limit reported in UDF metadata, a fraction of it that leaves headroom below the engine's 80% concurrency-stall threshold. When `/tmp` is real disk the UDF spills, so queries complete at any group cardinality. Otherwise a bounded pool returns a clean `ResourcesExhausted` error instead of OOM-crashing. Oversubscribed sharding shrinks each instance's footprint. The scan entry point emits as a SCALAR (not SET) script, so Exasol streams each shard's output instead of materializing the raw-row result in temp-DB RAM, which keeps engine-side scan-output memory constant regardless of scanned data volume.
5. **Tunable, observable scan**: the object-store connection budget, and the DataFusion partitions per UDF are configurable per virtual schema. Opt-in phase timing telemetry is enabled through `LAKEHOUSE_UDF_DEBUG_LEVEL` (`ctx.debug_level()`), not through a virtual schema property.

### Query pushdown

6. **Pushdown**: required are column projection, filter predicates, LIMIT, and ORDER BY + LIMIT (TopN). Shipped:
   - single-group and GROUP BY aggregation with partial/merge decomposition (node-local aggregate, then Exasol final aggregate) to minimize network transfer;
   - COUNT(DISTINCT) via per-shard DISTINCT row-scans counted by an outer Exasol-native COUNT(DISTINCT);
   - partition-equality and min/max range file pruning at plan time;
   - broadcast-eligible inner equi-join pushdown (small-side broadcast fan-out, planned and executed node-locally), with a safe fallback to an unaccelerated wrapper for joins outside the broadcast contract. General and multi-way joins and query rewriting remain out of scope.
7. **SQL expression translation**: scalar functions, date functions, and operators are translated into the pushed-down predicate, projection, and select-list shapes above, so pushdown reaches real-world SQL expressions and not only bare column references.

### Sources

8. **Catalog and storage access**: the `CATALOG_KIND` of the virtual schema selects how tables are found.
   - **Iceberg REST**: Apache Iceberg tables through an Iceberg REST catalog, via `iceberg-rust`. This is also one route to Databricks-managed tables. The adapter authenticates with a static bearer token or OAuth2 client credentials. Lakekeeper is the REST catalog the E2E suite tests against.
   - **Unity Catalog**: Delta and Parquet tables through a Unity Catalog, Databricks-managed or self-hosted OSS, via `delta-kernel-rs`. This is the other route to Databricks-managed tables. The adapter authenticates with a personal access token, OAuth M2M, or no auth. A CONNECTION that supplies both a token and a client ID/secret pair is rejected.
   - **AWS Glue**: `CATALOG_KIND = 'GLUE'` reads the Glue Data Catalog through its native API. An Iceberg table plans from the metadata file Glue points to, and a Hive Parquet table plans from its registered partitions. The adapter signs every Glue request with AWS SigV4 and reads storage with the CONNECTION's static credentials or the session of the AWS IAM role the CONNECTION names, because this kind has no credential vending. Glue's Iceberg REST endpoint (SigV4 with vended credentials) is a second Glue route.
   - **Direct storage**: catalog-free Parquet directories read straight from object storage, with Hive partitioning.
   - **Object storage**: S3-compatible storage or Azure ADLS Gen2.
   - **Credentials**: an Exasol CONNECTION object holds the catalog and storage credentials. The IAM role assumption described under Glue also applies to the Iceberg REST, Unity Catalog, and direct-storage kinds. The scan spec carries the CONNECTION name, and the scan UDF resolves the credentials at execution time. Vended and assumed-role session credentials travel only inside an AES-GCM-sealed envelope. Error messages redact the resolved secret values.
9. **Type mapping and schema evolution**: maps Iceberg, Delta, and Hive types to Exasol types, including type promotion and widening, nested types rendered as JSON `VARCHAR`, and timestamp precision chosen by the Exasol version. Partition columns are filled from catalog metadata, and INT96 timestamps (including far-future sentinel values) are decoded.

### Correctness and delivery

10. **Correct read path**: applies Iceberg positional deletes and Delta deletion vectors (`scan-read-path/scan-execution-delta-deletion-vectors`) at scan time, so results reflect current table state and not raw Parquet file content. Iceberg equality deletes and Puffin deletion vectors are refused with a clean error, not applied. The adapter also refuses Delta tables that need unsupported reader features or protocol versions, and refuses binary columns at plan time.
11. **Packaging and install**: one `.so` for x86 and aarch64, a version query UDF, an architecture-aware install script, and a personal deployment install.
12. **Result parity**: once the adapter advertises a pushdown capability it honors it fully, so results match native Exasol. Guards cover declined-filter self-application, empty results, DECIMAL string format, CHAR type declaration, type coercion, and ORDER BY capability.
13. **End-to-end test infrastructure**: besides the local Exasol Docker suite, E2E suites run against real AWS (Glue and assume-role), Azure, Unity Catalog (Databricks), and Lakekeeper. Shared fixtures cover type promotion, positional deletes, and INT96 timestamps. Scheduled sweeps remove orphaned Azure containers and Glue resources. `specs/testing.md` states how the suites are built, run, and cleaned up.

## Out of Scope

- Caching, result reuse, materialization, query acceleration
- Engine-owned metadata persistence, snapshot tracking, and background refresh. The Exasol `REFRESH` and `SET PROPERTIES` statements are supported: they re-enumerate the source tables, and the engine keeps no state of its own. The only persisted data are the virtual schema's AdapterNotes, which Exasol stores at CREATE time
- Automatic optimization, federated query optimizer
- Background processes, scheduling, lakehouse serving
- General and multi-way joins and complex query rewrites (only broadcast-eligible inner equi-joins are pushed down, see Core Capability 6)
- **Explicit non-goals (not building):** Reyden, Lakehouse RT, a DataFusion cluster scheduler, an Iceberg cache, a Databricks acceleration layer, a materialized query engine

Every query is executed independently, starts from source metadata, and leaves no state behind.

Architecture: see specs/architecture.md.

## Constraints

- **Usable engine**: correctness and safety guards are first-class requirements. The engine is designed to be operated, not just measured. Memory bounding is described in Core Capability 4.

## Domain Glossary

| Term | Definition |
|------|------------|
| Virtual Schema (VS) | Exasol adapter that makes an external data source queryable as a schema; here a thin stateless translation and planning layer |
| Pushdown | Exasol delegating projection, filter, limit, and aggregation to the VS so it executes at the source |
| IPROC / NPROC | `IPROC()` = node number, `NPROC()` = active node count. The shard-count node count is read from `UdfContext::node_count()` per pushdown request, not from `NPROC()`. Sharding does NOT group on `IPROC()`, because that would cap parallelism at the node count |
| Work-unit shard | One of G oversubscribed scan units (G = node_count × parallelism_factor, capped at 300); each is its own `shard_key` group multiplexed onto a node's per-node VM pool (sized to `NR_OF_CORES`) |
| File distributor | The LUA SET script `LAKEHOUSE_DISTRIBUTE_FILES`, installed outside the `.so`, that feeds the file list to the scalar scan UDF |
| DataFusion runtime | A vectorized query engine session that one scan UDF invocation (one shard group) creates for itself; several can run on one node |
| CATALOG_KIND | Virtual schema property that selects the catalog: absent for Iceberg REST, else `UNITY_CATALOG`, `GLUE`, or `DIRECT_STORAGE` |
| CONNECTION | Exasol object that holds the catalog address and the JSON credentials for catalog and storage |
| AdapterNotes | State that Exasol persists for a virtual schema between CREATE and pushdown: table map, skipped tables, and tuning values |
| Vended credentials | Short-lived per-table storage credentials that a catalog issues; they reach the scan UDF only inside a sealed envelope |
| Partial result | Per-shard-group output (raw rows or aggregate) merged by Exasol into the final result |
| Disposable execution container | One scan UDF invocation: it builds its own DataFusion session and keeps no query state after it returns. Exasol may reuse the hosting VM process for later invocations |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Language | Rust (edition 2024) | UDF + VS adapter implementation |
| Query engine | DataFusion + Arrow/Parquet 58 | Node-local vectorized scan and pushdown execution |
| Lakehouse | `iceberg-rust` (Iceberg REST catalog, incl. Databricks-managed Iceberg) + `delta-kernel-rs` 0.26 (Delta tables via native Unity Catalog) + `aws-sdk-glue` (AWS Glue Data Catalog: Iceberg and Hive Parquet tables) | Snapshot discovery, file resolution, table registration |
| UDF runtime | `exasol-udf-sdk` 0.30.0, `exasol-udf-macros`; language-container-rs Rust SLC | Rust UDF ABI, `ctx.emit` |
| Build | `rust:1.94-trixie` (glibc 2.41) in Docker, x86 and aarch64 | Builds `.so` matching the SLC; never built on host |
| Testing | `cargo test`; E2E against a local Exasol Docker container | Unit and cluster behavior validation |

> The Rust SLC and UDF runtime come from `language-container-rs`; this engine follows its UDF programming model and build/E2E workflow. `crates/vs-expression` (expression translation) and `crates/lakehouse-catalog` (Iceberg REST, Unity Catalog, and AWS Glue access) are workspace-internal splits from `crates/lakehouse-engine`; all three build into the one `.so` that carries all three UDF entry points.

## Commands

```bash
# Build (UDF .so, inside the rust:1.94-trixie container, never host `cargo build --release`)
make cross-udf-build

# Test (host unit tests)
cargo test

# Test (E2E against local Exasol Docker container)
make test-e2e

# Lint & Format
cargo clippy --all-targets && cargo fmt
```

## Project Structure

```
lakehouse-engine/
├── specs/                  # mission.md, architecture.md, and spec library (speq)
├── crates/
│   ├── lakehouse-engine/   # Iceberg + Delta file planning, scan-spec wire format, Exasol CONNECTION parsing, VS adapter, DataFusion-in-UDF scan
│   ├── lakehouse-catalog/  # Iceberg REST, Unity Catalog, and AWS Glue access: CatalogSession, auth, namespace enumeration, vended-storage resolution, SigV4 signing, AWS STS role assumption
│   └── vs-expression/      # expression-translation crate, shared with the sibling project
├── Cargo.toml      # workspace manifest
└── Makefile        # cross-udf-build, test-e2e
```

One `.so` carries all three entry points (VS adapter + DataFusion scan UDF + version query UDF). `lakehouse-catalog` and `vs-expression` compile into `lakehouse-engine`'s cdylib as workspace dependencies, so the crate layout does not affect UDF packaging.
