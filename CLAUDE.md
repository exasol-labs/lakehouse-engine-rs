# Project Rules

**Spec-driven project using speq-skill.**

Project mission in: @specs/mission.md

## Feature tracking

- **New features are tracked as GitHub issues** (`gh issue create`) before/at the start of
  work, in addition to speq spec deltas. Reference the issue in the implementing commit
  (`Closes #<n>`) so the work and its tracking stay linked.

## Code navigation & editing

- **Prefer Serena's MCP symbolic tools over `grep`/`Read`/`Edit` for any code file.**
  Use `get_symbols_overview` / `find_symbol` for discovery and `find_referencing_symbols`
  for usages; use `replace_symbol_body`, `insert_after_symbol`, `insert_before_symbol`,
  `rename_symbol`, or `safe_delete_symbol` for edits — never a raw `Edit` on a symbol
  you reached via Serena. `grep`/`Glob` remain fine for discovery only; `Read`/`Edit`
  remain fine for non-code files (docs, specs, config) or a file already fully read
  into context this session.
- If Serena's tools are not yet loaded this session, load them and call
  `initial_instructions` before the first code read/grep/edit — don't default to
  built-in tools out of habit.

## Unit test layout

- **No test code in a production source file.** Unit tests MUST live in a sibling file named after
  the module's own file, declared with the module's other `mod` declarations or as the last item of
  that module:
  ```rust
  #[cfg(test)]
  #[path = "<module>_tests.rs"]
  mod tests;
  ```
  `foo.rs` → `foo_tests.rs`, `lib.rs` → `lib_tests.rs`, `foo/mod.rs` → `foo/foo_tests.rs`
  (`mod_tests.rs` would be meaningless). `#[path]` resolves relative to the declaring file's own
  directory.
- The file name MUST match `[0-9a-zA-Z_-]+[_-]tests.rs`. `cargo llvm-cov` excludes exactly that
  pattern from every report; any other name silently re-inflates the coverage percentage.
- The test module remains a child module of its parent, so `use super::*;` still reaches the
  parent's private items and its imports.
- A test-only helper belongs in the sibling `_tests.rs` file, not in the production module — add it
  there as `impl super::TypeName { ... }` or a plain free fn, **not** gated by `#[cfg(test)]`, since
  the whole file is already test-only. A helper shared across several sibling `_tests.rs` files gets
  its own file, named to match the pattern and declared with `#[path]` so the `mod` identifier keeps
  its honest name: `#[path = "test_support_tests.rs"] mod test_support;`. Compile-time surface
  probes are test-only code and follow the same rule
  (`#[path = "scan_surface_probe_tests.rs"] mod scan_surface_probe;`).
- The only `#[cfg(test)]` that may remain in a production module is a re-export widening visibility
  for tests (`#[cfg(test)] pub use ...`) — it has to sit in the module owning the item. Test *code*
  never stays behind.

## PR title convention

PR titles MUST follow Conventional Commits format: `<type>(<scope>): <description>` (scope is
optional but recommended), using one of: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`,
`perf`. The title MUST describe the change's target/final state once implemented — not its
current lifecycle stage. A plan-only PR for a new feature is still `feat(...)`, not a "planning"
or "spec" prefix, even though only spec deltas are committed so far.

`lakehouse-engine` (repo `lakehouse-engine-rs`; `-rs` = built in Rust) — an in-place
lakehouse query engine: technically an Exasol Virtual Schema, but it runs the DataFusion engine on
the node, in place, for querying Iceberg / Databricks from Exasol SQL.

## Iceberg and Delta Lake specification compliance

Any feature planned via `/speq:plan` that touches scanning, pushdown, or schema/type handling MUST
be checked against the Apache Iceberg table spec (https://iceberg.apache.org/spec/) during
planning — quote the relevant normative section, don't rely on memory. A known deviation from the
spec must either be fixed in the same plan or recorded as an explicit, accurately-scoped tracked
exception — a GitHub issue cited inline in the spec (see the `(#27)` pattern in
`specs/datafusion-scan/scan-execution-field-id-projection/spec.md`); it must never be a silent gap.
A deviation driven by an Exasol target-type limitation (e.g. no struct/list/map types) is not a
gap for either the Iceberg or the Delta spec — but it must still be named as a deliberate
trade-off in the spec, not left unstated.

The same obligation applies to Delta: any feature planned via `/speq:plan` that touches Delta
scanning, pushdown, or schema/type handling MUST be checked against the Delta Lake protocol
(https://github.com/delta-io/delta/blob/master/PROTOCOL.md) during planning — quote the relevant normative section
(e.g. `§ Reader Requirements for Type Widening`), don't rely on memory. A known deviation from the
protocol must either be fixed in the same plan or recorded as an explicit, accurately-scoped
tracked exception — a GitHub issue cited inline in the spec, same convention as the Iceberg rule
above (see `specs/datafusion-scan/type-relaxation/spec.md` and
`specs/vs-adapter/delta-reader-feature-gating/spec.md` for the citation format); it must never be
a silent gap.

## Exasol / tooling

- Use Exasol Docker images to run Integration and E2E tests; they must **fail**, not skip, if no DB.
- Use `exapump` for all Exasol/BucketFS interaction.
- DSNs must include `validateservercertificate=0` (self-signed Docker cert).

## Virtual Schema pushdown delegation

Once the adapter's capabilities response advertises a predicate or function shape, Exasol delegates
it fully to the adapter and never independently re-checks or re-applies it. There is no Exasol-side
fallback once a capability is advertised. The adapter therefore owns generating the equivalent SQL
itself for anything within an advertised capability that it cannot faithfully push into the
DataFusion scan — omitting it returns wrong rows, not a safely-deferred check.

## Verification discipline

- A reported bug MUST be reproduced locally against the Docker Exasol container before it is
  fixed. Do not trust an issue's claimed repro, a capability list, or code inspection alone —
  run the query.
- A claimed SQL capability gap or limitation MUST be verified against a live Exasol system
  (`EXPLAIN VIRTUAL`, an actual pushed query, or an E2E test), not assumed from documentation,
  memory, or a capability registry (`capabilities.rs`) alone.
- No assumptions about SQL capabilities, syntax, or pushdown reachability without checking them
  against a running Exasol instance.

## Bench harness gotchas

- **A stray `bench/.env` silently redirects `make bench`/`bench/run.sh` at a remote target.**
  `bench/.env` is gitignored and target-specific (docker vs remote) — one left over from a prior
  `deploy/scripts/secrets.sh <env>` run sets `BENCH_TARGET=remote` plus a real `EXASOL_HOST`. Exporting
  `BENCH_TARGET=docker` alone is NOT enough to force docker mode cleanly: other vars the `.env` also
  sets (`LH_BUCKETFS_PORT`, `EXASOL_SYS_PASSWORD`, `BUCKETFS_WRITE_PASS`, ...) still leak into the
  docker-mode run unless you override every one of them too. Worse, `wait_exasol`'s TCP check has no
  per-attempt connect timeout, so an unreachable/stale remote host doesn't fail fast — each retry can
  block for a long time, and the whole loop can look like a stuck process rather than a clear error
  for 15-40+ minutes. **Before debugging a "hung" bench run, check for a stray `bench/.env` and either
  move it aside or override every var it sets** — don't just set `BENCH_TARGET`.

## Architecture boundaries

- **VS stays thin** — query translation, pushdown analysis, parallelization planning, result schema
  mapping. All execution logic lives in DataFusion.
- **UDFs are stateless and disposable** — no caching, no metadata persistence, no cross-call state.
  Every query starts from source metadata.
- **Resolve metadata once per query**, in the VS layer — never once per node. The VS passes each UDF
  an explicit assigned file list (a projection- + predicate-carrying scan spec); the UDF never
  discovers files itself. This seam is what later enables multi-node file sharding.
- **File-level work assignment, no overlap** — a node scans only its assigned files.
- **`ScanSpec` is format-neutral.** Every field on `ScanSpec`, `FileEntry`, and `LogicalField` must
  serve any table format — Iceberg, Delta, Hive, or future ones. When a new format or feature needs
  scan-time data, first look for an existing field that already models the same concept and widen it.
  Only add a new field when no existing one covers the concept — and make the new field format-neutral
  too, not a format-specific struct or `Option<FormatXSpec>` block. Format-specific knowledge lives in
  the `FormatReader` at plan time — it populates neutral fields. The scan side dispatches on field
  content (which enum variant, which `Option` is populated), never on format identity.

## UDF parallelization & memory model (Exasol engine behavior)

The instances-vs-groups mental model (with a worked example) is in `specs/udf-context.md` — read it
before changing the shard count or fan-out shape.

- **Groups drive UDF *invocations*, not OS processes.** The actual number of parallel instances on a
  node is a fixed per-node VM pool sized to the node's core count (`NR_OF_CORES`). Groups are
  multiplexed onto that pool.
- **Avoid `GROUP BY IPROC()` for parallelism.** `IPROC()` = node number, `NPROC()` = node count.
  `GROUP BY IPROC()` yields exactly one group per node → caps parallelism at the node count and
  leaves a node's other cores idle. Use it only for the NPROC node-count capture, never to shard
  scan work.
- **Oversubscribe via `GROUP BY shard_key`.** Compute `G = node_count × parallelism_factor` and cap
  G at **300** (Exasol's `max_dynamic_group_count` default). At/below 300 Exasol distributes groups
  **round-robin** (balanced) across nodes; above it Exasol **hash-partitions** them (unbalanced) —
  so keep G ≤ 300. Clamp G to ≥1 and ≤ file_count.
- **Per-instance memory limit comes from UDF metadata** (bytes), enforced by the DB as an RSS limit
  against the per-process heap (default 4096 MB). Read it via `ctx.memory_limit()` (`0` =
  unbounded/unknown).
- **The engine self-throttles concurrency at 80%.** The dispatcher stalls additional concurrent VMs
  once usage hits 80% of the per-process limit. Size the DataFusion memory pool to a fraction
  (~0.6) of the per-instance limit so the engine can manage concurrency rather than letting an
  instance OOM.

## Emit buffering

- `ctx.emit` buffers rows and flushes an `MT_EMIT` at a **4,000,000-byte** threshold
  (`EMIT_BUFFER_LIMIT_BYTES`) — 4 *million* bytes, NOT 4 MiB. Do not send a message per call.
- **Always flush at end of `run()`**, even if the threshold was not reached.
- A single row > threshold is still sent as one `MT_EMIT` (only the 2 GB per-value limit remains).
- **Raw-scan path uses `ctx.emit_batch(&RecordBatch)`** (SDK 0.19.0, `emit-arrow` feature). The SDK
  serializes the batch to Arrow IPC bytes internally; only bytes cross the `.so` boundary, not Arrow
  types. Partial-aggregate single-row emits still use `ctx.emit` with `Value` types.

## VM crashes & failure modes (Exasol engine behavior)

- **A `cleanup VM failed: VM crashed` (SQL state 22002) is an abnormal native VM exit — NOT
  necessarily OOM, NOT a Rust panic.** Before blaming memory, rule it out: check RSS against the
  memory limit, the OS OOM killer / dmesg, core dumps, and the panic log. A clean process exit with
  no core is neither an abort-on-panic nor a Rust panic.
- **Engine SIGKILL fan-out.** When one UDF VM of a statement part dies abnormally, the engine
  SIGKILLs every sibling VM of that part. So a cluster-wide "all VMs crashed on all nodes" symptom
  can originate from a single VM's abnormal exit — look for the earliest death (one VM closes
  slightly ahead, then a tight cluster of SIGKILLs).
- **`MT_EMIT` is synchronous request/reply.** Every emit flush (and the final `MT_FINISHED`) is a
  send-then-wait-for-ack round-trip over the SLC's ZMQ REQ socket; a large emit = hundreds of
  round-trips, each subject to the SLC's socket timeouts. Under load the engine can take >1 s to
  ack. A short, no-retry receive/send timeout treated as fatal breaks the REQ/REP lockstep on a
  slow-but-alive ack → abnormal VM exit → SIGKILL fan-out. **Fixed in lc-rs 0.19.1** by retrying
  transient `EAGAIN` rather than treating the timeout as fatal. Was volume- and load-correlated and
  intermittent.
- **SLC/`.so` fingerprint must match exactly.** The SDK fingerprint (`{exasol-udf-sdk
  version}:{rustc_hash}`) is checked at UDF load; e.g. a 0.19.1 SLC rejects a 0.19.0-SDK `.so` with
  a fingerprint-mismatch error. Keep the SLC and the consumer crate's `exasol-udf-sdk` version in
  lockstep, built with the same rustc (the `rust:1.94-bookworm` SLC builder).

## Live debugging (lc-rs 0.19.0 debug surface)

- **`ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<host>:<port>'`** redirects the UDF VM's fd1/fd2 to a
  listener (`nc -l`), capturing runtime tracing + startup/abort output the Rust SLC otherwise
  discards. (The docs' `SET SESSION SCRIPT OUTPUT ADDRESS` form was REJECTED on this cluster — use
  the `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS` form.) The listener must be reachable FROM the
  cluster nodes (a jumphost/private IP; a NAT'd local client cannot receive the connect-back).
- **`%udf_debug_level debug|info|warn|error`** in the script source (same channel as `%udf_object`)
  sets verbosity; at `debug` the SLC auto-emits per-VM-tagged (`pid`/`node_id`/`session_id`/`vm_id`)
  emit/flush + RSS telemetry with NO UDF code. `udf_log!(ctx, level, …)` + `ctx.debug_level()` emit
  UDF-side lines. Wire it env-gated in the scan-script DDL (`LAKEHOUSE_UDF_DEBUG_LEVEL`, default `info`).
- **CAVEAT: the redirect + per-row debug tracing destabilizes MULTI-LEG JOIN queries** — they can
  crash under debug even when they pass without it. Diagnose with SINGLE-LEG repros (one scan leg +
  engine-side aggregation), which are stable under the redirect.

## DataFusion streaming

- Stream the DataFusion result: fetch one Arrow `RecordBatch` at a time, emit it, then **drop the
  batch before fetching the next**. Architect rule: "du musst resultset in batches lesen und dann
  gleich emitten".
- **Raw scan path**: call `ctx.emit_batch(&batch)` — no `Vec<Value>` intermediate; the SDK
  serializes the batch to Arrow IPC bytes inside the UDF crate.
- **Partial-aggregate path**: convert the single summary row to `Vec<Value>` and call `ctx.emit`.
- Never collect all `RecordBatch`es before emitting — that holds two full copies in memory at once.
- **Arrow *types* must not cross the `.so` boundary — but Arrow IPC bytes (via `emit_batch`) may.**
  Arrow `TypeId` is not stable across the dynamic-library boundary (the `.so` links its own arrow
  copy). `emit_batch` serializes to IPC bytes inside the UDF before anything crosses that boundary,
  so the discipline is preserved. Do not pass Arrow structs or trait objects across the boundary.

## Connect-back

- Both `SCALAR` and `SET` scripts support connect-back; pick whichever UDF type fits.
- Address is `<container-eth0-ip>:8563` via `ctx.cluster_ip()`; never `127.0.0.1` or the Docker host
  gateway (both → SIGABRT).
- Connect-back is a plain SQL login with CONNECTION-object credentials in its own independent
  transaction. Read-only is always safe.
- `ExaConnection::query` is collect-all (use for small results); use `query_for_each` to stream.

## Data types

Exasol types: BOOLEAN, DECIMAL(1≤p≤36, 0≤s≤p), DOUBLE PRECISION, VARCHAR(n≤2,000,000),
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
| Timestamp(_, _) | Arrow→Value/EMITS: bare `TIMESTAMP` at every engine version. Catalog-declared (Iceberg/Delta): `TIMESTAMP(6)` on Exasol 2025.x+ (or an unrecognized version), bare `TIMESTAMP` (millisecond) on 8.x — see `specs/datafusion-scan/type-mapping/spec.md` |
| Decimal128(p,s) where p≤36 and s≤36 | DECIMAL(p, s) |
| Decimal128(p,s) where p>36 or s>36 | VARCHAR(2000000) via JSON |

Exasol's own `DECIMAL` domain is `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`: `DECIMAL(0,0)` is rejected with
*illegal precision value: 0* and `DECIMAL(5,10)` with *illegal scale value: 10* (both SQL state
`42000`, captured live). The two `Decimal128` table rows describe only the Arrow-input direction
and are looser than that domain; a catalog-declared decimal (Iceberg or Unity) is checked against
the full domain and otherwise maps to `VARCHAR(2000000)`.

Iceberg `timestamptz` maps to plain `TIMESTAMP`, not `TIMESTAMP WITH LOCAL TIME ZONE`: Exasol
rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002`).

**Incompatible types → `VARCHAR(2000000)` via JSON serialization:** List, LargeList,
FixedSizeList, Struct, Map, Union, Binary, LargeBinary, FixedSizeBinary, Duration, Time32,
Time64, Interval, Decimal256. Serialize the Arrow column to JSON string in the UDF (DataFusion
`CAST(col AS VARCHAR)` / `arrow_cast`) before converting to `Value::String`. Declare these
columns as `VARCHAR(2000000)` in the `createVirtualSchema` schema response. This is what lets
Exasol surface Parquet vectors, lists, and structs — they arrive as queryable JSON strings.

## Build

- Build the UDF `.so` only inside `rust:1.94-bookworm` (glibc 2.36, matches the SLC) via
  `make cross-musl-udf-build`. **Never `cargo build --release` on the host** — it writes a
  host-glibc `.so` that fails to load in Exasol. Host `cargo test` (debug) is fine.
- Two library crates, one `.so`: `crates/lakehouse-engine` (Iceberg + Delta file planning, scan-spec
  wire format, Exasol CONNECTION parsing, VS adapter, DataFusion-in-UDF scan) depends on
  `crates/lakehouse-catalog` (Iceberg REST + Unity Catalog access — `CatalogSession`, auth, namespace
  enumeration, vended-storage resolution, SigV4 signing). The catalog crate compiles into the
  engine's cdylib, so one `.so` still exports **both** entry points (VS adapter + DataFusion scan
  SET UDF) — `language-container-rs` 0.14.0 supports multiple entry points per `.so`.
- SDK: `exasol-udf-sdk` + `exasol-udf-macros`, pinned **only** in `[workspace.dependencies]` of the
  root `Cargo.toml`. Since 0.18.0, `connect-back` is **always-on** (no longer a feature flag).
  Enable `emit-arrow` to unlock `ctx.emit_batch`.
