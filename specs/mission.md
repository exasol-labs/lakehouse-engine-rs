# Mission: Exasol DataFusion Virtual Schema PoC

> A stateless Exasol Virtual Schema that queries Apache Iceberg and Databricks-managed datasets by running DataFusion inside Rust UDFs, using the Exasol cluster as a distributed execution substrate.

## Problem Statement

Exasol cannot natively query lakehouse data (Iceberg / Databricks) with its own distributed
execution engine. DataFusion can read these formats with a fast vectorized engine, but only on a
single node. Neither side alone gives distributed lakehouse query execution.

This PoC validates a single hypothesis: **Exasol cluster parallelism + DataFusion vectorized
execution = parallel lakehouse query execution.** Concretely — can Exasol's execution framework be
used as a distributed execution substrate for DataFusion, scaling Iceberg/Databricks scans beyond
single-node DataFusion?

The purpose of this phase is to validate **technical feasibility and performance characteristics
only**. It is not a product.

## Target Users

| Persona | Goal | Key Workflow |
|---------|------|--------------|
| Exasol engineer / architect | Decide whether to invest in a DataFusion-on-Exasol query path | Run benchmark queries against Iceberg/Databricks through the VS, read the scaling and overhead measurements |
| Analyst (validation proxy) | Query lakehouse tables with plain SQL through Exasol | `SELECT ... FROM <virtual_schema>.<table>` and get correct results |

## Core Capabilities

1. **Stateless Virtual Schema** — translates a user query, analyzes pushdowns, plans
   parallelization, and maps result schemas. Thin: most execution logic lives in DataFusion.
2. **DataFusion-in-UDF execution** — a disposable Rust UDF creates a DataFusion session, registers
   Iceberg tables, applies pushdowns, scans its assigned files, and produces partial results.
3. **File-level cluster parallelism** — resolve the Iceberg file list once per query and partition
   files across active Exasol nodes (IPROC-aware) so no node scans another node's files.
4. **Pushdown** — required: column projection, filter predicates, LIMIT. Desired: aggregation and
   partial aggregation (node-local aggregate → Exasol final aggregate) to minimize network transfer.
5. **Iceberg + Databricks access** — query both Apache Iceberg tables and Databricks-managed Iceberg
   through the same path.

## Out of Scope

- Caching, result reuse, materialization, query acceleration
- Metadata persistence, snapshot tracking, refresh mechanisms
- Automatic optimization, federated query optimizer
- Background processes, scheduling, lakehouse serving
- Join pushdown, complex query rewrites
- **Explicit non-goals (not building):** Reyden, Lakehouse RT, a DataFusion cluster scheduler, an
  Iceberg cache, a Databricks acceleration layer, a materialized query engine

Every query is executed independently, starts from source metadata, and leaves no state behind.

## Domain Glossary

| Term | Definition |
|------|------------|
| Virtual Schema (VS) | Exasol adapter that makes an external data source queryable as a schema; here a thin stateless translation + planning layer |
| Pushdown | Exasol delegating projection / filter / limit / aggregation to the VS so it executes at the source |
| IPROC | Exasol's per-node execution process; the unit of cluster parallelism used to shard files across nodes |
| DataFusion runtime | A node-local vectorized query engine instance created inside a UDF for the lifetime of one query |
| Partial result | Per-node output (raw rows or node-local aggregate) merged by Exasol into the final result |
| Disposable execution container | The UDF: created per query, holds no state, discarded on completion |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Language | Rust (edition 2024) | UDF + VS adapter implementation |
| Query engine | DataFusion + Arrow/Parquet 58 | Node-local vectorized scan & pushdown execution |
| Lakehouse | iceberg-rust (Iceberg + Databricks Iceberg catalogs) | Snapshot discovery, file resolution, table registration |
| UDF runtime | `exasol-udf-sdk` 0.13.1 (connect-back), `exasol-udf-macros`; language-container-rs Rust SLC | Rust UDF ABI, `ctx.emit`, connect-back SQL session |
| Build | `rust:1.92-bookworm` (glibc 2.36) in Docker | Builds `.so` matching the SLC; never built on host |
| Testing | `cargo test`; E2E against a local Exasol Docker container | Unit + cluster behavior validation |

> Sibling projects: `strata-rs` (VS adapter + UDF conventions) and `language-container-rs` (the Rust
> SLC and UDF runtime). This PoC reuses their UDF programming model and build/E2E workflow and may
> converge with `strata-rs` (possibly a monorepo) in the long run.

## Commands

```bash
# Build (UDF .so — inside the rust:1.92-bookworm container, never host `cargo build --release`)
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
lakehouse-vs/
├── specs/          # mission.md and spec library (speq)
├── crates/         # VS adapter + DataFusion-in-UDF crate(s)
├── Cargo.toml      # workspace manifest
└── Makefile        # cross-musl-udf-build, test-e2e
```

## Architecture

Layered, stateless, two-level parallelism. Data flow:

```
User Query
  → Virtual Schema (translate, pushdown analysis, parallelization plan, result schema mapping)
  → resolve Iceberg snapshot + file list ONCE per query
  → partition files across active Exasol nodes (IPROC-aware)
  → parallel UDF execution: one DataFusion runtime per node over its file set
  → Iceberg / Databricks Parquet files
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
- **Business**: PoC only — feasibility and measurement, not production hardening.
- **Performance**: Must be faster than single-node DataFusion and scale with added Exasol nodes, with
  minimal duplicate scanning and acceptable metadata overhead.

## External Dependencies

| Service | Purpose | Failure Impact |
|---------|---------|----------------|
| Iceberg catalog | Snapshot discovery, file list resolution | No query can be planned or executed |
| Databricks (Iceberg) | Databricks-managed table access | Databricks queries fail; Iceberg path unaffected |
| Object storage (S3-compatible) | Parquet file data | Scans fail / stall; this is a measured bottleneck risk |
| Exasol cluster + Rust SLC (BucketFS) | UDF execution substrate | No execution; the substrate under test |

## Open Risks to Measure

This PoC exists to measure these, not assume them away:

- **Metadata bottleneck** — metadata cost + scan cost may exceed parallelization benefit.
- **Duplicate metadata fetches** — N nodes must not each load metadata; resolve once per query.
- **UDF startup cost** — DataFusion runtime init latency, memory overhead, scaling behavior.
- **Network bottlenecks** — parallelism may shift the bottleneck to object storage / catalog / network.
- **Aggregation cost** — final aggregation transfer in Exasol must not erase scan-time gains.
