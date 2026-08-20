# Mission: lakehouse-engine — Exasol In-Place Lakehouse Query Engine

> An **in-place query engine**: technically an Exasol Virtual Schema, but rather than only translating and planning it runs the DataFusion engine on the node, in place — querying Apache Iceberg and Databricks-managed datasets inside Rust UDFs, using the Exasol cluster as a distributed execution substrate. (Repo: `lakehouse-engine-rs`; the `-rs` is an external "built in Rust" hint — internally the project is `lakehouse-engine`.)

## Problem Statement

Analytical teams want to query Apache Iceberg and Databricks-managed datasets with the speed and
scale of a distributed engine, straight from Exasol SQL. This Virtual Schema delivers exactly that:
it pairs Exasol's cluster distribution with DataFusion's vectorized execution into one distributed
lakehouse query engine.

**Exasol cluster distribution + DataFusion vectorized execution = parallel lakehouse query
execution.** Files are sharded across Exasol nodes and scanned in parallel by node-local DataFusion
runtimes, then merged in Exasol — so lakehouse scans scale with the cluster instead of bottlenecking
on a single node. Projection, filter, and LIMIT pushdown keep each scan lean; node-local aggregation
keeps network transfer small.

The payoff: open lakehouse data (Iceberg, Databricks) becomes first-class, queryable through plain
Exasol SQL at cluster scale, with no copy, no caching, and no separate query stack to operate.

## Target Users

| Persona | Goal | Key Workflow |
|---------|------|--------------|
| Exasol engineer / architect | Decide whether to invest in a DataFusion-on-Exasol query path | Run benchmark queries against Iceberg/Databricks through the VS, read the scaling and overhead measurements |
| Analyst (validation proxy) | Query lakehouse tables with plain SQL through Exasol | `SELECT ... FROM <virtual_schema>.<table>` and get correct results |

## Core Capabilities

1. **Stateless Virtual Schema** — translates a user query, analyzes pushdowns, plans
   parallelization, and maps result schemas. Thin: most execution logic lives in DataFusion.
2. **DataFusion-in-UDF execution** — a disposable Rust UDF creates a DataFusion session, registers
   Iceberg or Delta tables, applies pushdowns, scans its assigned files, and produces partial
   results.
3. **File-level cluster parallelism** — resolve the file list once per query, format-neutral
   across Iceberg and Delta, and partition
   files into G oversubscribed work-unit shards (G = node_count × parallelism_factor, capped at 300),
   driven via `GROUP BY shard_key` so Exasol distributes shard groups across nodes and multiplexes
   them onto each node's core pool; no node scans another node's files.
4. **Pushdown** — required: column projection, filter predicates, LIMIT, ORDER BY + LIMIT (TopN).
   Shipped: single-group aggregation and GROUP BY aggregation with partial/merge decomposition
   (node-local aggregate → Exasol final aggregate) to minimize network transfer; COUNT(DISTINCT)
   via per-shard DISTINCT row-scans counted by an outer Exasol-native COUNT(DISTINCT); partition-equality and min/max range file pruning
   at plan time; broadcast-eligible inner equi-join pushdown (small-side broadcast fan-out, planned
   and executed node-locally) with a safe fallback to an unaccelerated wrapper for joins outside the
   broadcast contract — general/multi-way joins and query rewriting remain out of scope (see below).
5. **SQL expression translation** — scalar functions, date functions, and operators are translated
   into the pushed-down predicate/projection/select-list shapes above, so pushdown reaches
   real-world SQL expressions rather than only bare column references.
6. **Correct read path** — applies Iceberg positional/row-level deletes and Delta deletion vectors
   (`datafusion-scan/scan-execution-delta-deletion-vectors`) at scan time, so results reflect
   current table state rather than raw Parquet file content.
7. **Iceberg and Unity Catalog access** — query Apache Iceberg tables through an Iceberg REST
   catalog and Delta tables through a Unity Catalog, Databricks-managed or self-hosted OSS, through
   the same engine. A Databricks-managed table is reached by one of two routes, chosen by the
   configured `CATALOG_KIND`: Iceberg REST via `iceberg-rust`, or native Unity Catalog via
   `delta-kernel-rs`.
8. **Bounded, self-throttling execution** — the scan UDF sizes its DataFusion memory pool from the per-instance memory limit reported in UDF metadata (a fraction of it, leaving headroom below the engine's 80% concurrency-stall threshold) and adds a spill backstop: when `/tmp` is real disk it spills (queries complete at any group cardinality); when it is not, a bounded pool returns a clean `ResourcesExhausted` error instead of OOM-crashing. Oversubscribed work-unit sharding (`GROUP BY shard_key`, G = node_count × parallelism_factor capped at 300) shrinks each instance's footprint and lets the engine multiplex shard groups onto each node's core pool. Bounding is not only UDF-side: the scan entry point emits as a SCALAR (not SET) script, so Exasol streams each shard's output rather than materializing the raw-row result into growing temp-DB RAM — keeping engine-side scan-output memory constant regardless of scanned data volume.

## Out of Scope

- Caching, result reuse, materialization, query acceleration
- Metadata persistence, snapshot tracking, refresh mechanisms
- Automatic optimization, federated query optimizer
- Background processes, scheduling, lakehouse serving
- General/multi-way joins and complex query rewrites (only broadcast-eligible inner equi-joins are
  pushed down — see Core Capability 4)
- **Explicit non-goals (not building):** Reyden, Lakehouse RT, a DataFusion cluster scheduler, an
  Iceberg cache, a Databricks acceleration layer, a materialized query engine

Every query is executed independently, starts from source metadata, and leaves no state behind.

## Domain Glossary

| Term | Definition |
|------|------------|
| Virtual Schema (VS) | Exasol adapter that makes an external data source queryable as a schema; here a thin stateless translation + planning layer |
| Pushdown | Exasol delegating projection / filter / limit / aggregation to the VS so it executes at the source |
| IPROC / NPROC | `IPROC()` = node number, `NPROC()` = active node count. The shard-count node count is read from `UdfContext::node_count()` per pushdown request, not from `NPROC()` over connect-back; sharding does NOT group on `IPROC()` (that would cap parallelism at the node count) |
| Work-unit shard | One of G oversubscribed scan units (G = node_count × parallelism_factor, capped 300); each is its own `shard_key` group multiplexed onto a node's per-node VM pool (sized to `NR_OF_CORES`) |
| DataFusion runtime | A node-local vectorized query engine instance created inside a UDF for the lifetime of one query |
| Partial result | Per-node output (raw rows or node-local aggregate) merged by Exasol into the final result |
| Disposable execution container | The UDF: created per query, holds no state, discarded on completion |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Language | Rust (edition 2024) | UDF + VS adapter implementation |
| Query engine | DataFusion + Arrow/Parquet 58 | Node-local vectorized scan & pushdown execution |
| Lakehouse | `iceberg-rust` (Iceberg REST catalog, incl. Databricks-managed Iceberg) + `delta-kernel-rs` 0.26 (Delta tables via native Unity Catalog) | Snapshot discovery, file resolution, table registration |
| UDF runtime | `exasol-udf-sdk` 0.13.1 (connect-back), `exasol-udf-macros`; language-container-rs Rust SLC | Rust UDF ABI, `ctx.emit`, connect-back SQL session |
| Build | `rust:1.94-bookworm` (glibc 2.36) in Docker | Builds `.so` matching the SLC; never built on host |
| Testing | `cargo test`; E2E against a local Exasol Docker container | Unit + cluster behavior validation |

> Sibling projects: the sibling project (VS adapter + UDF conventions) and `language-container-rs` (the Rust
> SLC and UDF runtime). This engine shares their UDF programming model and build/E2E workflow. The
> standalone `crates/vs-expression` expression-translation crate is designed to be shared with
> the sibling project and will migrate to a monorepo layout when the projects converge. `crates/lakehouse-catalog`
> (Iceberg REST + Unity Catalog access) is a workspace-internal split from `crates/lakehouse-engine`, not a
> sibling-shared crate — both still build into the one `.so` that carries both UDF entry points.

## Commands

```bash
# Build (UDF .so — inside the rust:1.94-bookworm container, never host `cargo build --release`)
make cross-musl-udf-build

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
├── specs/                  # mission.md and spec library (speq)
├── crates/
│   ├── lakehouse-engine/   # Iceberg + Delta file planning, scan-spec wire format, Exasol CONNECTION parsing, VS adapter, DataFusion-in-UDF scan
│   ├── lakehouse-catalog/  # Iceberg REST + Unity Catalog access: CatalogSession, auth, namespace enumeration, vended-storage resolution, SigV4 signing
│   └── vs-expression/      # expression-translation crate, shared with the sibling project
├── Cargo.toml      # workspace manifest
└── Makefile        # cross-musl-udf-build, test-e2e
```

One `.so` still carries both entry points (VS adapter + DataFusion scan SET UDF): `lakehouse-catalog`
compiles into `lakehouse-engine`'s cdylib as a workspace dependency, so the crate split changes only
the source layout, not the UDF packaging model.

## Architecture

Layered, stateless, two-level parallelism. Data flow:

```
User Query
  → Virtual Schema (translate, pushdown analysis, parallelization plan, result schema mapping)
  → resolve snapshot + file list ONCE per query (format-neutral: Iceberg or Delta)
  → partition files into G oversubscribed work-unit shards (GROUP BY shard_key, G = node_count × parallelism_factor capped 300)
  → parallel UDF execution: one DataFusion runtime per shard invocation, multiplexed onto each node's core pool
  → Iceberg / Delta Parquet files
  → partial results (raw rows or node-local aggregate)
  → Exasol final processing / merge
  → Result
```

Cluster parallelism (Exasol, across nodes) × local parallelism (DataFusion, within a node), exploited
simultaneously. No state survives query completion.

## Constraints

- **Technical**: UDFs are stateless and disposable — no caching, no metadata persistence, no
  cross-call state. The `.so` is built in glibc 2.36 to match the SLC; only SDK `Value` types cross
  the UDF boundary (never Arrow types). Read DataFusion result batches and `ctx.emit` them
  incrementally; never materialize the whole result set. Metadata must be resolved once per query,
  not once per node. All DSN/connection strings include `validateservercertificate=0`.
- **Usable engine**: correctness and safety guards are first-class requirements. The engine is designed to be operated, not just measured. Execution is bounded: the scan UDF sizes its DataFusion memory pool from the per-instance memory limit and either spills to disk (when `/tmp` is real disk) so high-cardinality grouped queries complete, or returns a clean `ResourcesExhausted` error rather than OOM-crashing — layered on oversubscribed sharding that shrinks per-instance footprint and the engine's own 80% concurrency throttle.
- **Performance**: Must be faster than single-node DataFusion and scale with added Exasol nodes, with
  minimal duplicate scanning and acceptable metadata overhead.

## External Dependencies

| Service | Purpose | Failure Impact |
|---------|---------|----------------|
| Iceberg REST catalog | Snapshot discovery, file list resolution for Iceberg tables | No Iceberg query can be planned or executed |
| Unity Catalog | Table version / log replay, file list resolution for Delta tables | No Delta/Unity query can be planned or executed |
| Databricks (Iceberg REST or Unity Catalog) | Databricks-managed table access via either catalog kind | Databricks queries fail on both catalog-kind routes; the non-Databricks Iceberg REST catalog and Unity Catalog dependencies above are unaffected |
| Object storage (S3-compatible) | Parquet file data | Scans fail / stall; this is a measured bottleneck risk |
| Exasol cluster + Rust SLC (BucketFS) | UDF execution substrate | No execution; the substrate under test |

> Catalog and object-storage access is authenticated (REST-catalog OAuth2/bearer credentials;
> cloud-native credential mechanisms such as vended/STS credentials for object storage) — an auth
> failure has the same failure impact as the underlying dependency being unavailable.