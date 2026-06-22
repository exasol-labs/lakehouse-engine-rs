# Project Rules

**Spec-driven project using speq-skill.**

Project mission in: @specs/mission.md

`lakehouse-engine` (external repo `lakehouse-engine-rs`; `-rs` = built in Rust) — an in-place
lakehouse query engine: technically an Exasol Virtual Schema, but it runs the DataFusion engine on
the node, in place, for querying Iceberg / Databricks from Exasol SQL.
Sibling of `strata-rs` (VS adapter conventions) and `language-container-rs` (the Rust SLC) — mirror
their UDF model, workspace layout, and Makefile/E2E conventions. Likely converges with `strata-rs`
(possibly a monorepo) long-term.

## Exasol / tooling

- Use Exasol Docker images to run Integration and E2E tests; they must **fail**, not skip, if no DB.
- Use `exapump` for all Exasol/BucketFS interaction.
- DSNs must include `validateservercertificate=0` (self-signed Docker cert).

## Architecture boundaries

- **VS stays thin** — query translation, pushdown analysis, parallelization planning, result schema
  mapping. All execution logic lives in DataFusion.
- **UDFs are stateless and disposable** — no caching, no metadata persistence, no cross-call state.
  Every query starts from source metadata.
- **Resolve metadata once per query**, in the VS layer — never once per node. The VS passes each UDF
  an explicit assigned file list (a projection- + predicate-carrying scan spec); the UDF never
  discovers files itself. This seam is what later enables multi-node file sharding.
- **File-level work assignment, no overlap** — a node scans only its assigned files.

## UDF parallelization & memory model (Exasol engine internals)

Verified against `exasol-db` (`script-languages`); cite these when revisiting fan-out or memory work.

- **Groups drive UDF *invocations*, not OS processes.** Actual parallel instances on a node = a
  fixed per-node VM pool sized to `NR_OF_CORES`
  (`Engine/src/exscript/primitives.cpp:267`, `swigengine.cc:1147-1184`). Groups are multiplexed
  onto that pool (`set_function.cpp:240-260`).
- **Avoid `GROUP BY IPROC()` for parallelism.** `IPROC()` = node number, `NPROC()` = node count
  (`misc_primitives.cpp:98-132`). `GROUP BY IPROC()` yields exactly one group per node → caps
  parallelism at the node count and leaves a node's other cores idle. Use it only for the NPROC
  node-count capture, never to shard scan work.
- **Oversubscribe via `GROUP BY shard_key`.** Compute `G = node_count × parallelism_factor` and cap
  G at **300** (Exasol's `max_dynamic_group_count` default). At/below 300 Exasol distributes groups
  **round-robin** (balanced) across nodes; above it Exasol **hash-partitions** them (unbalanced) —
  so keep G ≤ 300 (`globalgroupbyset5.cpp:295-341`). Clamp G to ≥1 and ≤ file_count.
- **Per-instance memory limit comes from UDF metadata** (bytes), enforced by the DB via
  `setrlimit(RLIMIT_RSS)` against the per-process heap (default 4096 MB). Read it via
  `ctx.memory_limit()` (`language-container-rs:add-memory-limit-metadata`; `0` = unbounded/unknown).
- **The engine self-throttles concurrency at 80%.** The dispatcher stalls additional concurrent VMs
  once usage hits 80% of the per-process limit (`swigengine.cc:1574-1595`). Size the DataFusion
  memory pool to a fraction (~0.6) of the per-instance limit so the engine can manage concurrency
  rather than letting an instance OOM.

## Emit buffering

- `ctx.emit` buffers rows and flushes an `MT_EMIT` at a **4,000,000-byte** threshold
  (`EMIT_BUFFER_LIMIT_BYTES`) — 4 *million* bytes, NOT 4 MiB. Do not send a message per call.
- **Always flush at end of `run()`**, even if the threshold was not reached.
- A single row > threshold is still sent as one `MT_EMIT` (only the 2 GB per-value limit remains).

## DataFusion streaming

- Stream the DataFusion result: fetch one Arrow `RecordBatch` at a time, convert it → `Vec<Value>`,
  `ctx.emit` it, then **drop the batch before fetching the next**. Architect rule: "du musst
  resultset in batches lesen und dann gleich emitten".
- Never collect all `RecordBatch`es before converting — that holds two full copies (Arrow + Value)
  in memory at once.
- **Only SDK `Value` types cross the `.so` boundary — never Arrow types.** Arrow `TypeId` is not
  stable across the dynamic-library boundary (the `.so` links its own arrow copy). Convert
  Arrow→`Value` inside the UDF before emitting.

## Connect-back

- Both `SCALAR` and `SET` scripts support connect-back; pick whichever UDF type fits.
- Address is `<container-eth0-ip>:8563` via `ctx.cluster_ip()`; never `127.0.0.1` or the Docker host
  gateway (both → SIGABRT).
- Connect-back is a plain SQL login with CONNECTION-object credentials in its own independent
  transaction. Read-only is always safe.
- `ExaConnection::query` is collect-all (use for small results); use `query_for_each` to stream.

## Data types

Exasol types: BOOLEAN, DECIMAL(p≤36, s≤36), DOUBLE PRECISION, VARCHAR(n≤2,000,000),
CHAR(n≤2,000), DATE, TIMESTAMP(p≤9), TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL YEAR TO
MONTH, INTERVAL DAY TO SECOND, GEOMETRY, HASHTYPE. **No arrays, lists, structs, or maps.**

DataFusion/Arrow → Exasol (apply in both `createVirtualSchema` schema mapping and Arrow→Value
conversion):

| Arrow type | Exasol type |
|---|---|
| Boolean | BOOLEAN |
| Int8/Int16/Int32 | DECIMAL(precision, 0) |
| Int64/UInt32/UInt64 | DECIMAL(20, 0) |
| UInt8/UInt16 | DECIMAL(precision, 0) |
| Float32/Float64 | DOUBLE PRECISION |
| Utf8/LargeUtf8 | VARCHAR(2000000) |
| Date32 | DATE |
| Timestamp(_, None) | TIMESTAMP |
| Timestamp(_, Some(_)) | TIMESTAMP WITH LOCAL TIME ZONE |
| Decimal128(p,s) where p≤36 and s≤36 | DECIMAL(p, s) |
| Decimal128(p,s) where p>36 or s>36 | VARCHAR(2000000) via JSON |

**Incompatible types → `VARCHAR(2000000)` via JSON serialization:** List, LargeList,
FixedSizeList, Struct, Map, Union, Binary, LargeBinary, FixedSizeBinary, Duration, Time32,
Time64, Interval, Decimal256. Serialize the Arrow column to JSON string in the UDF (DataFusion
`CAST(col AS VARCHAR)` / `arrow_cast`) before converting to `Value::String`. Declare these
columns as `VARCHAR(2000000)` in the `createVirtualSchema` schema response. This is what lets
Exasol surface Parquet vectors, lists, and structs — they arrive as queryable JSON strings.

## Build

- Build the UDF `.so` only inside `rust:1.92-bookworm` (glibc 2.36, matches the SLC) via
  `make cross-musl-udf-build`. **Never `cargo build --release` on the host** — it writes a
  host-glibc `.so` that fails to load in Exasol. Host `cargo test` (debug) is fine.
- One crate / one `.so` exports **both** entry points (VS adapter + DataFusion scan SET UDF) —
  `language-container-rs` 0.14.0 supports multiple entry points per `.so`.
