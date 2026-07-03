# Plan: add-scan-connection-concurrency

## Summary

Add an operator-tunable object-store connection-concurrency knob (`S3_MAX_CONNECTIONS`) to the scan UDF so a node's network/IO can be saturated when data-file fetching is the throughput bottleneck, and first bump `exasol-udf-sdk`/`exasol-udf-macros` `0.20.0 → 0.20.1` to fix the upstream `ctx.node_count() == 0` handshake bug so cluster-scale sharding and the benchmark that validates this knob compute the real node count. Closes #47, closes #43.

## Design

### Context

The last recorded benchmark (`specs/_recorded/2026-06-27-change-engine-throughput/`, documented in `docs/performance.md`) tops out at Q4 full-`lineitem` scan ≈ 0.19 GB/s, well under the native `IMPORT FROM PARQUET` ceiling (~0.17 GB/s reported per node; the goal of this plan is to approach the native aggregate). The user's hypothesis is that **serial / under-concurrent data fetching, not the engine, caps cluster network/IO throughput**. Two of the three hypothesis levers already exist as VS properties and need no new code — one shard per node is `PARALLELISM_FACTOR=1` (`G = node_count`, `parallelism/work-unit-sharding`), and one DataFusion instance getting all the node's cores is `DATAFUSION_THREADING_MODE=AUTO` at that shard shape (`auto_threads_per_udf`, `datafusion-scan/scan-execution-threading`). The missing lever is **object-store connection concurrency**: `build_s3_store` (`crates/lakehouse-engine/src/scan/mod.rs:626-662`) builds `AmazonS3Builder` with zero HTTP client tuning, so per-instance fetch concurrency is left entirely to defaults and to DataFusion's `target_partitions` file-group splitting — there is no operator equivalent to the native importer's `MaxConnections` / `MaxConcurrentReads`.

Two prerequisites gate a *meaningful* benchmark. First, `ctx.node_count()` returns `0` on every live cluster at `createVirtualSchema` time because of an upstream `language-container-rs` handshake bug (fixed in v0.20.1 / `exasol-udf-sdk` 0.20.1, PR #42 / issue #41): `SC_FN_VIRTUAL_SCHEMA_ADAPTER_CALL` bound the `exascript_info` handshake to a discarded `_meta`, so `SingleCallContext` never surfaced `node_count`. This exact symptom is already tracked in this repo as **issue #43** ("`CLUSTER_NODES` always 1 on multi-node clusters → fan-out planned as single-node (under-sharding)"), which reproduces the `G = 1 × parallelism_factor` collapse on a 4-node cluster and left the fix undecided pending upstream #41 — #41 is now fixed, resolving #43's open question in favor of option (a) from that issue ("fix consumed via a new SLC release"). Our `resolve_cluster_nodes` (`adapter/mod.rs:693-702`) already correctly maps `0 → 1` (a correct "unknown" fallback) and passes through any positive count verbatim, so **no behaviour in this repo changes** — the fix is a pure dependency-version pin bump. Every prior multi-node benchmark computed `G = node_count × parallelism_factor` with `node_count` pinned to `1`; those runs are not trustworthy for cluster scaling until 0.20.1 is in. Issue #43 also flags that `create_vs_records_cluster_nodes_property` (`crates/lakehouse-engine/tests/e2e_scan_test.rs:770`) asserts only `CLUSTER_NODES >= 1`, which passed despite the bug — Task 1 revisits that assertion.

A larger-scale 2026-07-01 run reinforces both deliverables. Against the full TPC-H `lineitem` table (60 Parquet files, 179,998,372 rows, Glue `eu-west-1`, SLC lc-rs 0.19.1, `NR_OF_CORES=8`, `PARALLELISM_FACTOR=8`; `docs/performance.md` §"Larger-scale validation (180M-row lineitem, 60 files)"), native `IMPORT INTO` (full 180M-row load) averaged **~80.4 s** while the VS `CREATE TABLE AS SELECT *` full-emit averaged **~151 s** — **~1.9× slower**. This *flips* the original recorded benchmark (`specs/_recorded/2026-06-27-change-engine-throughput/`), where the VS *aggregate* path (Q4, partial-aggregated) was competitive with or faster than native IMPORT: the new finding is on a **full raw-row emit** (`SELECT *`), a genuinely different, emit-heavy workload shape, not an aggregate. (Metadata-only `COUNT(*)` still favors the VS by ~20× — ~1.3 s vs. the ~28.8 s native `IMPORT` ceiling — and is unaffected; cited for completeness only.) Crucially, this run recorded `CLUSTER_NODES=1` in `adapterNotes` despite explicit `NR_OF_CORES=8`/`PARALLELISM_FACTOR=8` — the exact pre-0.20.1 `ctx.node_count()==0` handshake bug (issue #43) that **Task 1 already fixes**. It is therefore unknown how much of the 151 s/80 s gap is under-sharding (`G = 1 × 8` instead of `node_count × 8`, which the dep bump would close) versus a genuine emit-path bottleneck (`Int64→Decimal128` coercion, measured 50–200× slower than zero-copy types in a *synthetic* micro-bench per the 2026-06-27 Task 5, but never observed on a real full-emit workload at this scale). This finding strengthens the case for the dep bump (correct node count is a precondition to even reading the gap) and for a tunable fetch-concurrency lever (`S3_MAX_CONNECTIONS`), and it surfaces one open question tracked below as a named re-gate task (Task 10) plus an evidence-gated deferred-work item (§Deferred work) — neither expands this plan's code scope.

- **Goals** — Add a single operator-facing `S3_MAX_CONNECTIONS` knob (explicit value pins it, otherwise AUTO-derived from node capacity) that sizes the object store's HTTP connection concurrency per scan instance; round-trip it through `adapterNotes` → shard-invariant common spec → `ScanSpec` following the exact `PARALLELISM_FACTOR` / threading precedent; fix `node_count()` via the 0.20.1 bump so cluster fan-out and the validating benchmark use the real node count; benchmark toward native-`IMPORT` parity.
- **Non-Goals** — Changing shard-count math, threading derivation, or memory pool sizing (orthogonal, unchanged); a second per-file-vs-per-node dual knob mirroring `MaxConnections`+`MaxConcurrentReads` (collapse to one knob now); any change to `language-container-rs` itself (separate upstream repo, already fixed/released); shipping the benchmark harness as spec scenarios (validation only, per §Verification).

### Decision

Introduce `S3_MAX_CONNECTIONS` as one VS property resolved at `createVirtualSchema` exactly like the existing knobs: `PROP_S3_MAX_CONNECTIONS` (property) → `NOTE_S3_MAX_CONNECTIONS` (`adapterNotes` key) → `s3_max_connections: usize` on both `CommonSpec` and `ScanSpec` (shard-invariant, `#[serde(default)]`) → applied in `build_s3_store`. An explicit positive integer is used verbatim (FIXED-like); absent/empty/zero/invalid triggers an AUTO derivation from `nr_of_cores` and the per-node UDF-instance share, mirroring `auto_threads_per_udf` — with a `0`-cores fallback to the built-in default. The resolved budget is applied to the object store via `AmazonS3Builder::with_client_options(ClientOptions)` — the exact `object_store` 0.13.2 method is confirmed to exist; the concrete pooling call (`ClientOptions::with_pool_max_idle_per_host`, versus also splitting DataFusion file groups / `meta_fetch_concurrency`) is the expert-tagged mechanism decision.

#### Architecture

```
createVirtualSchema(props)
  └─ resolve_s3_max_connections(props, nr_of_cores, udf_instances_per_node)
        explicit positive S3_MAX_CONNECTIONS  → verbatim (FIXED-like)
        else  → AUTO: derive from cores × instance share (0 cores → default)
  └─ build_adapter_notes(... S3_MAX_CONNECTIONS ...)   [serialized ONCE]
        │
pushdown planning: adapter_note(S3_MAX_CONNECTIONS) → CommonSpec.s3_max_connections
        │  (shard-invariant common spec arg, one literal for the whole fan-out)
        ▼
scan UDF: reconstitute ScanSpec → build_s3_store(storage, bucket, s3_max_connections)
             AmazonS3Builder::…::with_client_options(ClientOptions pooled to N)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Property → note → common-spec → apply round-trip | `S3_MAX_CONNECTIONS` end to end | Identical to `PARALLELISM_FACTOR` / threading; the UDF stays resolution-agnostic |
| Explicit-wins-else-AUTO-derive | `resolve_s3_max_connections` | Mirrors threading FIXED/AUTO; single property (no separate MODE) keeps it as light as `PARALLELISM_FACTOR` |
| Shard-invariant field in common spec | `CommonSpec.s3_max_connections` | Same knob for every shard → serialize once, never per shard (`parallelism/work-unit-sharding`) |
| Sentinel `0` cores = unknown → default | AUTO fallback | Matches the existing `nr_of_cores == 0` handling across the adapter |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One `S3_MAX_CONNECTIONS` knob | Dual `MaxConnections`+`MaxConcurrentReads` mirroring native IMPORT | One knob covers the hypothesis ("max file connections per node to saturate"); a second axis is unproven complexity — defer until the benchmark shows one knob is insufficient |
| Explicit-wins-else-AUTO, no MODE property | Full `DATAFUSION_THREADING_MODE`-style AUTO/FIXED mode property | The threading MODE exists because partitions and threads are two coupled fields; connection concurrency is one field, so a mode property is redundant machinery |
| Apply via `object_store` `ClientOptions` on the S3 client | DataFusion `target_partitions` file-group splitting; `datafusion.execution.meta_fetch_concurrency` | `target_partitions` is the CPU/threading axis (already a knob) and `meta_fetch_concurrency` only affects schema/stats reads; the object-store HTTP client pool is the axis that genuinely maps to "concurrent fetches from S3 per instance". Exact call is expert-tagged |
| Dep bump ships no new scenario | Author a `resolve_cluster_nodes` delta | Our observable contract is unchanged — existing scenarios already cover both `node_count()==0→1` and positive passthrough; the fix lives entirely in the upstream SDK handshake |

### Deferred work (evidence-gated, NOT in this plan's scope)

- **Emit-path `Int64→Decimal128` coercion optimization.** The 2026-07-01 180M-row full-emit run (VS `CREATE TABLE AS SELECT *` ~151 s vs. native `IMPORT INTO` ~80.4 s, ~1.9×; `docs/performance.md` §"Larger-scale validation") is the first *real* workload that could be emit-bound rather than fetch-bound. The `BIGINT` (Int64 → Decimal128) Arrow→Value coercion measured 50–200× slower than zero-copy types in a synthetic micro-bench (2026-06-27 benchmark, Task 5), but has never been confirmed as the bottleneck on a full-emit workload at scale. **Do not build emit-path coercion work in this plan.** It is gated on Task 10's re-gate outcome: pursue it only if the ~1.9× gap *persists* after Task 1's dep bump lands and sharding uses the real `node_count` (i.e. under-sharding is ruled out as the cause). This mirrors and concretizes the pre-existing evidence-gated bullet in `docs/performance.md` §"Future engine work (deferred, evidence-gated)" — the 180M-row CTAS is now named there as the first candidate; Task 10 supplies the isolating measurement. Deferring it now is YAGNI: there is no confirmed emit-bound root cause yet, only a confounded gap.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-connection-concurrency | NEW | `specs/_plans/add-scan-connection-concurrency/datafusion-scan/scan-execution-connection-concurrency/spec.md` |

**No spec delta for the dependency bump.** The `0.20.0 → 0.20.1` pin fixes the upstream `ctx.node_count()` handshake bug (PR #42 / issue #41); `resolve_cluster_nodes` (`adapter/mod.rs:693-702`) is unchanged and its observable contract is already fully covered by the existing scenarios `cluster_nodes_passes_through_reported_node_count` (`adapter/mod.rs:883-888`, `:1533-1538`) and the `node_count()==0 → 1` fallback. No new Given/When/Then is warranted for a pure version pin with zero contract change on our side — the version bump is a task, not a scenario.

## Dependencies

- `exasol-udf-sdk` and `exasol-udf-macros` `0.20.0 → 0.20.1` in root `Cargo.toml` (line ~49) and `crates/lakehouse-engine/Cargo.toml` (lines ~21-22); keep the `emit-arrow` feature. Verified published on crates.io (2026-07-02). Fixes upstream `language-container-rs` issue #41 (`node_count()` returned `0` on the single-call VS-adapter path).
- `object_store` 0.13.2 (already pinned) — `AmazonS3Builder::with_client_options` and `ClientOptions` pooling methods confirmed present; no version change.

## Implementation Tasks

1. **Dependency bump.** Bump `exasol-udf-sdk` + `exasol-udf-macros` `0.20.0 → 0.20.1` in root `Cargo.toml` and `crates/lakehouse-engine/Cargo.toml` (keep `emit-arrow`); update the stale SDK version line in `CLAUDE.md`. `cargo build`/`cargo test` (debug, host) to confirm the tree resolves. Do NOT rebuild the `.so` by hand. Closes #43. Revisit `create_vs_records_cluster_nodes_property` (`crates/lakehouse-engine/tests/e2e_scan_test.rs:770`), which per #43 asserts only `CLUSTER_NODES >= 1`: tighten it if the e2e Docker harness's node count is knowable/assertable post-fix, otherwise leave the assertion as-is and note in the PR why (single-node local harness can't distinguish a fixed handshake from a coincidentally-correct fallback).
2. **Add the `s3_max_connections` spec field.** Add `s3_max_connections: usize` with `#[serde(default = "default_s3_max_connections")]` and a `default_s3_max_connections()` fn to `CommonSpec` and `ScanSpec` (`scan/spec.rs`), and thread it through the split (`ScanSpec → CommonSpec`) and merge (`CommonSpec + files → ScanSpec`) impls and the test fixtures (`spec.rs:350-383,~440`). Pure additive, follows `df_threads_per_udf`.
3. **Adapter resolution + AUTO derivation.** Add `PROP_S3_MAX_CONNECTIONS` / `NOTE_S3_MAX_CONNECTIONS` / `DEFAULT_S3_MAX_CONNECTIONS` constants and `resolve_s3_max_connections(props, nr_of_cores, udf_instances_per_node)`: explicit positive integer verbatim, else AUTO-derive a per-instance budget from cores and the per-node instance share (mirroring `auto_threads_per_udf`, `adapter/mod.rs:603-606`), with `0` cores falling back to the default. Decide the exact AUTO formula (how connections scale with cores/instance share to saturate the NIC without unbounded pooling). [expert]
4. **Wire the round-trip.** Add the field to `build_adapter_notes` (`adapter/mod.rs:461-517`) and read it via `adapter_note(...)` into `CommonSpec.s3_max_connections` in pushdown planning (`adapter/mod.rs:~300`, `pushdown.rs`), so it is serialized exactly once in the shard-invariant common spec argument.
5. **Apply the budget to the object store.** In `build_s3_store` (`scan/mod.rs:626-662`) construct a `ClientOptions` sized to the budget and pass it via `AmazonS3Builder::with_client_options`, on both scan paths (`build_session_context`, `scan/mod.rs:597-624`). Choose the concrete `object_store` 0.13.2 mechanism (`ClientOptions::with_pool_max_idle_per_host` and/or companion pooling calls) that maps to "N concurrent fetches from S3 per instance", and expose an assertable seam (e.g. a small `client_options_for(budget)` helper) so scenario 1 can be tested without a live store. Preserve the existing credential-redaction error path. [expert]
6. **Unit tests (resolution, serde default, once-in-common-spec).** In `adapter/mod.rs`: FIXED-wins, AUTO derivation, and `0`-cores→default cases for `resolve_s3_max_connections`. In `scan/spec.rs`: serde default when the field is absent. In `pushdown.rs`: the resolved budget appears exactly once in the serialized common spec and never in a per-shard argument.
7. **Integration test (object store build).** In `crates/lakehouse-engine/tests/scan_two_arg.rs` add a test that builds the session context / object store from a spec with `s3_max_connections = N` and asserts the client-options seam carries N and the store builds successfully (no credential leakage in errors).
8. **Docs.** Update `docs/tuning.md` (new knob, default, AUTO behaviour, relation to `PARALLELISM_FACTOR` + threading) and `docs/performance.md` (native-IMPORT parity goal + that pre-0.20.1 multi-node numbers under-counted nodes).
9. **Benchmark (validation, NOT a spec scenario).** Extend `bench/sweep.sh` with a "few big shards + high `S3_MAX_CONNECTIONS`" row (`PARALLELISM_FACTOR=1`, `DATAFUSION_THREADING_MODE=AUTO`, sweep `S3_MAX_CONNECTIONS`), re-run against the 2026-06-27 `lineitem`/TPC-H setup on the (now correctly node-counted) cluster, and compare Q4 GB/s to the native `IMPORT FROM PARQUET` ceiling. Record findings in the decision log, not the spec.
10. **Re-gate the 180M-row full-emit gap (validation, NOT a spec scenario).** *After Task 1 (dep bump) ships*, re-run the SPECIFIC 60-file / 180M-row comparison from `docs/performance.md` §"Larger-scale validation" — native `IMPORT INTO` (full load) vs. VS `CREATE TABLE AS SELECT *` (full emit) on the same `lineitem` files — and confirm `adapterNotes` now records the real `CLUSTER_NODES` (not `1`). Record whether the ~151 s/~80.4 s (~1.9×) gap narrows once sharding is `node_count × parallelism_factor`. This is distinct from Task 9's general sweep (which targets the smaller 2026-06-27 `lineitem`/TPC-H setup); it is the concrete workload that gates the §Deferred work emit-path item. Record findings in the decision log and update `docs/performance.md`'s caveat — not the spec.
11. **Gate.** `cargo test`, `cargo clippy --all-targets`, `cargo fmt`. `make test-e2e` handles the `.so` rebuild — never build it by hand.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (prereq) | Task 1 (dep bump gates compilation) |
| Group B | Task 2 (spec field), Task 3 (adapter resolution) |
| Group C | Task 4 (round-trip wiring), Task 5 (object store apply) |
| Group D | Task 6 (unit tests), Task 7 (integration test), Task 8 (docs) |
| Group E | Task 9 (benchmark), Task 10 (180M-row re-gate), Task 11 (gate) |

Sequential dependencies:
- Group A → Group B → Group C → Group D → Group E
- Task 4 depends on Tasks 2 and 3; Task 5 depends on Task 2; Task 6 depends on Tasks 3 and 4; Task 7 depends on Task 5.
- Task 10 (180M-row re-gate) depends on Task 1 having shipped (the dep bump must be live on the cluster so `CLUSTER_NODES` is real); it runs in the same late group as Tasks 9 and 11.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | None. This plan is purely additive (one new field, one new property, one new object-store call); it removes no existing code. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Scan configures its object store from the resolved connection budget | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | `scan_applies_s3_max_connections_to_object_store` |
| Scan falls back to a built-in default budget when the field is absent | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `scanspec_defaults_s3_max_connections_when_absent` |
| FIXED value overrides the AUTO derivation at createVirtualSchema | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `resolve_s3_max_connections_fixed_value_wins` |
| AUTO derivation sizes the per-instance budget from node capacity | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `resolve_s3_max_connections_auto_scales_with_cores` |
| AUTO derivation falls back to the default budget when the core count is unknown | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `resolve_s3_max_connections_auto_zero_cores_defaults` |
| Connection budget travels once in the shard-invariant common spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `common_spec_carries_s3_max_connections_exactly_once` |

- Scenarios 2–6 are pure computation (serde defaulting, integer resolution, SQL/JSON serialization) → unit tests. Scenario 1 touches object-store construction → integration test against the assertable client-options seam.
- The `.so` load / real-cluster behaviour is exercised by the existing `make test-e2e` scan suite (unchanged) plus the Task 9 benchmark.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution-connection-concurrency | `CREATE VIRTUAL SCHEMA lh USING ... WITH S3_MAX_CONNECTIONS = '32' ...;` then query `EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES` / adapterNotes | `adapterNotes` carries `S3_MAX_CONNECTIONS=32`; a `SELECT ... FROM lh.<table>` returns correct rows |
| scan-execution-connection-concurrency (AUTO) | `CREATE VIRTUAL SCHEMA lh USING ... ` (no `S3_MAX_CONNECTIONS`); inspect adapterNotes | `adapterNotes` carries an AUTO-derived positive `S3_MAX_CONNECTIONS` on a multi-core node; scan returns correct rows |
| dep-bump (node_count fix) | On a ≥2-node cluster: `CREATE VIRTUAL SCHEMA`, inspect `CLUSTER_NODES` in adapterNotes | `CLUSTER_NODES` equals the real node count (not `1`), confirming the 0.20.1 handshake fix |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (host debug) | `cargo test --no-run` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
| E2E | `make test-e2e` | 0 failures (rebuilds `.so` as needed; never build it by hand) |
