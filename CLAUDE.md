# Project Rules

**Spec-driven project using speq-skill.**

Project mission in: @specs/mission.md

## Feature tracking

- **New features are tracked as GitHub issues** (`gh issue create`) before/at the start of
  work, in addition to speq spec deltas. Reference the issue in the implementing commit
  (`Closes #<n>`) so the work and its tracking stay linked.

## PR title convention

PR titles MUST follow Conventional Commits format: `<type>(<scope>): <description>` (scope is
optional but recommended), using one of: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`,
`perf`. The title MUST describe the change's target/final state once implemented — not its
current lifecycle stage. A plan-only PR for a new feature is still `feat(...)`, not a "planning"
or "spec" prefix, even though only spec deltas are committed so far.

`lakehouse-engine` (external repo `lakehouse-engine-rs`; `-rs` = built in Rust) — an in-place
lakehouse query engine: technically an Exasol Virtual Schema, but it runs the DataFusion engine on
the node, in place, for querying Iceberg / Databricks from Exasol SQL.
Sibling of a sibling VS adapter project (VS adapter conventions) and `language-container-rs` (the
Rust SLC) — mirror their UDF model, workspace layout, and Makefile/E2E conventions. Likely
converges with that sibling project (possibly a monorepo) long-term.

## Exasol / tooling

- Use Exasol Docker images to run Integration and E2E tests; they must **fail**, not skip, if no DB.
- Use `exapump` for all Exasol/BucketFS interaction.
- DSNs must include `validateservercertificate=0` (self-signed Docker cert).

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

## UDF parallelization & memory model (Exasol engine internals)

Verified against `[redacted]` (`script-languages`); cite these when revisiting fan-out or memory work.
The instances-vs-groups mental model (from the Exasol engine architect, with a worked example) is in
`specs/udf-context.md` — read it before changing the shard count or fan-out shape.

- **Groups drive UDF *invocations*, not OS processes.** Actual parallel instances on a node = a
  fixed per-node VM pool sized to `NR_OF_CORES`
  (`[redacted]`, `[redacted]`). Groups are multiplexed
  onto that pool (`[redacted]`).
- **Avoid `GROUP BY IPROC()` for parallelism.** `IPROC()` = node number, `NPROC()` = node count
  (`[redacted]`). `GROUP BY IPROC()` yields exactly one group per node → caps
  parallelism at the node count and leaves a node's other cores idle. Use it only for the NPROC
  node-count capture, never to shard scan work.
- **Oversubscribe via `GROUP BY shard_key`.** Compute `G = node_count × parallelism_factor` and cap
  G at **300** (Exasol's `max_dynamic_group_count` default). At/below 300 Exasol distributes groups
  **round-robin** (balanced) across nodes; above it Exasol **hash-partitions** them (unbalanced) —
  so keep G ≤ 300 (`[redacted]`). Clamp G to ≥1 and ≤ file_count.
- **Per-instance memory limit comes from UDF metadata** (bytes), enforced by the DB via
  `setrlimit(RLIMIT_RSS)` against the per-process heap (default 4096 MB). Read it via
  `ctx.memory_limit()` (`language-container-rs:add-memory-limit-metadata`; `0` = unbounded/unknown).
- **The engine self-throttles concurrency at 80%.** The dispatcher stalls additional concurrent VMs
  once usage hits 80% of the per-process limit (`[redacted]`). Size the DataFusion
  memory pool to a fraction (~0.6) of the per-instance limit so the engine can manage concurrency
  rather than letting an instance OOM.

## Emit buffering

- `ctx.emit` buffers rows and flushes an `MT_EMIT` at a **4,000,000-byte** threshold
  (`EMIT_BUFFER_LIMIT_BYTES`) — 4 *million* bytes, NOT 4 MiB. Do not send a message per call.
- **Always flush at end of `run()`**, even if the threshold was not reached.
- A single row > threshold is still sent as one `MT_EMIT` (only the 2 GB per-value limit remains).
- **Raw-scan path uses `ctx.emit_batch(&RecordBatch)`** (SDK 0.19.0, `emit-arrow` feature). The SDK
  serializes the batch to Arrow IPC bytes internally; only bytes cross the `.so` boundary, not Arrow
  types. Partial-aggregate single-row emits still use `ctx.emit` with `Value` types.

## VM crashes & failure modes (Exasol engine internals)

Verified on the live AWS Glue cluster (2026-06-27) while fixing the lineitem "Q3 crash". See
`[[slc-zmq-recv-timeout-bug]]` / `[[live-cluster-memory-budget]]` in auto-memory.

- **`cleanup VM failed: VM crashed` (SQL state 22002, `err_zombie:TRUE/err_except:FALSE`) is an
  abnormal native VM exit — NOT necessarily OOM, NOT a Rust panic.** Before blaming memory, check:
  RSS (was ~120 MB, 33× under the 4096 MB limit), `oom_kill`/dmesg (clean), core dumps
  (`[redacted]`, none), and the `set_hook` panic log (absent). A clean process exit with
  no core is NOT `panic=abort` (which SIGABRTs + cores) and NOT a panic.
- **Engine SIGKILL fan-out.** When ONE UDF VM of a statement part dies abnormally (engine controller
  `[redacted] ... state=15; signaled=FALSE` → `Sending SIGKILL to partID`), the engine **SIGKILLs
  every sibling VM** of that part (`[redacted]: child (exaudfclient) terminated with signal 9`). So a
  cluster-wide "all VMs crashed on all nodes" symptom can originate from a SINGLE VM's abnormal exit
  — find the EARLIEST death (one VM closes slightly ahead, then a tight cluster of SIGKILLs).
- **`MT_EMIT` is synchronous request/reply.** Every emit flush (and the final `MT_FINISHED`) is a
  send-then-wait-for-ack round-trip over the SLC's ZMQ REQ socket; a large emit = hundreds of
  round-trips, each subject to the SLC's socket timeouts. Under load the engine can take >1 s to ack.
  The Rust SLC's ZMQ transport had `RCVTIMEO/SNDTIMEO=1000` (1 s) treated as FATAL with no retry →
  a slow-but-alive ack broke the REQ/REP lockstep → abnormal VM exit → SIGKILL fan-out. **Fixed in
  lc-rs 0.19.1** (`exa-zmq-protocol` retries transient `EAGAIN`; PR #38). Was volume+load correlated,
  intermittent (~67–90%); reproduces WITHOUT a join via a single-leg large raw emit.
- **SLC/`.so` fingerprint must match exactly.** `EXA_SDK_FINGERPRINT = "{exasol-udf-sdk version}:{rustc_hash}"`
  is checked at UDF load; e.g. a 0.19.1 SLC rejects a 0.19.0-SDK `.so` with `F-UDF-CL-RUST-9001:
  Fingerprint mismatch`. Keep the SLC and the consumer crate's `exasol-udf-sdk` version in lockstep,
  built with the same rustc (the `rust:1.94-bookworm` SLC builder).

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

- Build the UDF `.so` only inside `rust:1.94-bookworm` (glibc 2.36, matches the SLC) via
  `make cross-musl-udf-build`. **Never `cargo build --release` on the host** — it writes a
  host-glibc `.so` that fails to load in Exasol. Host `cargo test` (debug) is fine.
- One crate / one `.so` exports **both** entry points (VS adapter + DataFusion scan SET UDF) —
  `language-container-rs` 0.14.0 supports multiple entry points per `.so`.
- SDK: `exasol-udf-sdk` **0.20.3** + `exasol-udf-macros` **0.20.3** (built with rustc 1.94 to match
  the v0.20.3 SLC fingerprint). Since 0.18.0, `connect-back`
  is **always-on** (no longer a feature flag). Enable `emit-arrow` to unlock `ctx.emit_batch`.
