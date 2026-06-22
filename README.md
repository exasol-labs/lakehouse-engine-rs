<div align="center">

# lakehouse-engine-rs

**An in-place lakehouse query engine for Exasol — query Apache Iceberg and Databricks-managed
tables straight from SQL by running DataFusion inside Rust UDFs across the cluster. No caching,
no materialization, no data movement.**

</div>

---

## What this is

`lakehouse-engine-rs` is an Exasol **Virtual Schema** that does more than translate and plan: it
runs the [DataFusion](https://datafusion.apache.org/) engine on the node, in place. The Virtual
Schema stays thin — query translation, pushdown analysis, parallelization planning, and result-schema
mapping — while all execution happens in disposable, node-local DataFusion runtimes inside Rust UDFs.
Exasol's cluster distribution paired with DataFusion's vectorized execution gives you parallel
lakehouse query execution: files are sharded across nodes, scanned in parallel, then merged in Exasol.

Every query is stateless — it starts from source metadata and leaves nothing behind. This is the
opposite of its sibling [`strata-rs`](https://github.com/exasol-labs/strata-rs), which prunes and
**caches** Parquet files; this engine **executes** queries in place with no result reuse.

## Getting started

### Prerequisites

- Docker (the UDF `.so` is built inside `rust:1.92-bookworm` to match the SLC's glibc 2.36)
- Rust toolchain (host unit tests run in debug)
- An Exasol + MinIO + Iceberg REST catalog stack for E2E (a `docker-compose.yml` is included)

### Build and test

```sh
cargo test                   # host unit tests (debug) — no Exasol required
make cross-musl-udf-build    # build liblakehouse_engine.so inside rust:1.92-bookworm
make test-e2e                # E2E against a live Exasol stack — fails (not skips) if unavailable
```

> **Never run `cargo build --release` on the host** for the UDF crate — it writes a host-glibc `.so`
> that fails to load inside Exasol. Build the `.so` only via `make cross-musl-udf-build`.

The E2E suite needs a live stack (`docker compose up -d` brings up Exasol + MinIO + the Iceberg REST
catalog). To deploy manually, `make install-slc` registers the Rust Language Container and
`make bucketfs-upload-so` uploads the compiled `.so` to BucketFS.

## Quick example

Register the adapter + scan scripts (both entry points live in one `.so`), create a Virtual Schema
over an Iceberg table, and query it — projection, filter, and `LIMIT` are pushed down to the scan:

```sql
-- Adapter + scan scripts (the SLC dispatches by script name to the matching entry point)
CREATE OR REPLACE RUST ADAPTER SCRIPT LHVS.LAKEHOUSE_ADAPTER AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

CREATE OR REPLACE RUST SET SCRIPT LHVS.LAKEHOUSE_SCAN(spec VARCHAR(2000000)) EMITS (...) AS
%udf_object buckets/bfsdefault/default/udf/liblakehouse_engine.so
/

-- Virtual Schema over an Iceberg table
CREATE VIRTUAL SCHEMA MY_LAKEHOUSE
USING LHVS.LAKEHOUSE_ADAPTER WITH
  CATALOG_URI = 'http://iceberg-rest:8181'
  WAREHOUSE   = 's3://warehouse/'
  TABLE_NAME  = 'default.events'
  SCAN_SCHEMA = 'LHVS'
  S3_ENDPOINT = 'http://minio:9000'
  S3_REGION   = 'us-east-1'
  ACCESS_KEY  = 'minioadmin'
  SECRET_KEY  = 'minioadmin'
  ALLOW_HTTP  = 'true';

-- Query the lakehouse table (projection + filter + LIMIT pushdown)
SELECT id, name, score FROM MY_LAKEHOUSE.EVENTS WHERE score > 15.0 LIMIT 5;
```

## Capability overview

- **DataFusion-in-UDF execution** — DataFusion + Arrow/Parquet 58 vectorized scans run inside
  disposable Rust UDFs; results stream out batch-by-batch via `ctx.emit`.
- **Table formats** — Apache Iceberg and Databricks-managed Iceberg through the same path.
- **Pushdown** — column projection, filter predicates, and `LIMIT`; plus single-group and
  `GROUP BY` aggregation with partial/merge decomposition (node-local aggregate → Exasol final).
- **Cluster parallelism** — the Iceberg file list is resolved once per query and partitioned into
  `G = node_count × parallelism_factor` oversubscribed work-unit shards (capped at 300), driven via
  `GROUP BY shard_key` so Exasol balances shards across nodes; no node scans another's files.
- **Bounded, self-throttling execution** — the DataFusion memory pool is sized from the per-instance
  memory limit; spills to disk when `/tmp` is real disk, otherwise returns a clean
  `ResourcesExhausted` error instead of OOM-crashing.
- **One `.so`, two entry points** — the VS adapter and the DataFusion scan SET UDF ship in a single
  shared object.

### Crates

| Crate | Purpose |
|-------|---------|
| `crates/lakehouse-engine` | VS adapter + DataFusion scan SET UDF (`cdylib` + `rlib`) |
| `crates/vs-expression` | SQL expression translator (`rlib`; designed to be shared with `strata-rs`) |

## Documentation

Detailed docs live in [`specs/`](specs/) (spec-driven development via the `speq` skill):

- [`specs/mission.md`](specs/mission.md) — purpose, core capabilities, problem statement, tech stack
- [`specs/decision-log.md`](specs/decision-log.md) — architecture decision records
- [`specs/vs-adapter/`](specs/vs-adapter/) — create-virtual-schema and pushdown planning
- [`specs/datafusion-scan/`](specs/datafusion-scan/) — scan execution, grouped aggregation, type mapping
- [`specs/parallelism/`](specs/parallelism/) — work-unit sharding
- [`specs/sql-comprehension/`](specs/sql-comprehension/) — the `vs-expression` translator
- [`specs/packaging/`](specs/packaging/) — single-`.so` two-entry-points layout and E2E harness

## License

License: TBD
