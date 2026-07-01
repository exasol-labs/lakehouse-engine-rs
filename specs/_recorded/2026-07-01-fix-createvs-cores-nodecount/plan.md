# Plan: fix-createvs-cores-nodecount

## Summary

Fix issue #32: `createVirtualSchema` topology discovery collapses `(cluster_nodes, nr_of_cores)` to `(1, 0)` because the bogus `SELECT PARAM_VALUE('NR_OF_CORES')` connect-back query always fails and discards the already-obtained `NPROC()` node count in the same `?`-propagating closure. Replace connect-back topology discovery entirely with in-process sources — `UdfContext::node_count()` (SDK 0.20.0) for the node count and `std::thread::available_parallelism()` for the per-node core count — and drop the `CONNECTION_NAME` VS property and connect-back session for this purpose.

## Design

### Context

`resolve_cluster_nodes` obtains the active node count over a connect-back session (`SELECT NPROC()` — works) then, unless the `NR_OF_CORES` VS property is set, runs `SELECT PARAM_VALUE('NR_OF_CORES')`. `PARAM_VALUE` is not a real Exasol function, so that query always errors; because it runs inside the same closure whose `?` feeds a single `result.unwrap_or((1, 0))`, the failure discards the valid node count too. The result: `(cluster_nodes, nr_of_cores) = (1, 0)` on every real cluster whenever `NR_OF_CORES` is not manually set, which collapses the shard count `G = node_count × parallelism_factor` to `1 × 8` regardless of true cluster size and defeats cluster-scale fan-out and the ADR-023 cores-aware parallelism-factor default.

SDK 0.20.0 exposes `UdfContext::node_count() -> u32` from the live handshake metadata (published on crates.io; API confirmed against `language-container-rs` commit `7d90182`), removing the need for any SQL round-trip to learn the node count. The per-node core count is already read in-process elsewhere in the codebase via `available_parallelism()` (the scan UDF's DataFusion `target_partitions`, ADR-023), so the same source is available here with no session.

- **Goals** — Restore correct `(cluster_nodes, nr_of_cores)` on real clusters; remove the failing `PARAM_VALUE` query and the fragile shared-closure `?` propagation; simplify topology discovery to in-process reads with no SQL session; preserve the `NR_OF_CORES` override contract and the `default 1` / `default 0` fallbacks.
- **Non-Goals** — Live-cluster verification of `available_parallelism()` accuracy (trusted via ADR-023 precedent); touching the `CATALOG_CONNECTION` credential mechanism (separate, unaffected); changing shard-count math, parallelism-factor defaulting, or DataFusion threading derivation.

### Decision

Rewrite `resolve_cluster_nodes` to source topology in-process. Node count: `ctx.node_count()`, mapping `0` (no live handshake — stub/test double/broken handshake) to `1`, any `≥ 1` used verbatim. Core count: the `NR_OF_CORES` property override (via the unchanged `parse_nr_of_cores_override`) when present and `≥ 1`, otherwise `available_parallelism()` mapped to its `usize` (`0` when it errors). Delete the connect-back branch, the `CONNECTION_NAME` property constant, and the now-dead `nproc_value_to_count` / `varchar_value_to_u32` helpers.

#### Architecture

```
handle_create_virtual_schema(ctx, request)
  └─ resolve_cluster_nodes(ctx, &props) -> (u32 cluster_nodes, u32 nr_of_cores)
        cluster_nodes = match ctx.node_count() { 0 => 1, n => n }
        nr_of_cores   = parse_nr_of_cores_override(&props)          // Some(N) → N
                        .unwrap_or_else(available_parallelism_or_0) // None   → available_parallelism()
```

No `ctx.connection()` / `ctx.connect_back()` / `session.query()` calls remain in this path.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| In-process metadata read | `resolve_cluster_nodes` node count | Handshake already carries node count; no SQL round-trip, no session, cannot fail-and-discard |
| Override-then-autodetect | `resolve_cluster_nodes` core count | Preserves the existing `NR_OF_CORES` override precedence; only the autodetect source changes |
| Sentinel `0` = unknown | core count fallback | Downstream `resolve_parallelism_factor` already floors `0` cores to a factor of 8 — unchanged contract |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Remove connect-back entirely for topology | Keep connect-back as defensive fallback for node count | Grep-verified single-purpose usage; `node_count()` is strictly more reliable than `SELECT NPROC()` (no session, no auth, no failure mode). A fallback adds the exact fragile SQL path being removed. Aligns with mission "no separate query stack to operate." |
| `available_parallelism()` in the adapter UDF, no live verification | Add a live-cluster verification task | ADR-023 precedent: the scan UDF already reads full host cores via `available_parallelism()` in a UDF on the target clusters; trust it here. |
| Map `node_count() == 0` to `1` | Treat `0` as an error | `0` only occurs for stub/test double/broken handshake; a live single-node cluster reports `1`. Defaulting to `1` preserves the existing single-shard behaviour contract. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `specs/_plans/fix-createvs-cores-nodecount/vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | CHANGED | `specs/_plans/fix-createvs-cores-nodecount/vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` |

Note: `parallelism/work-unit-sharding` has one Background bullet attributing the node count to `NPROC()`. It carries no scenario behaviour change (a Background-only delta is not a valid delta file), so it is not shipped as a delta; update that Background wording to `UdfContext::node_count()` directly in the permanent spec at record time (see decision-log entry [5]).

## Dependencies

- `exasol-udf-sdk` and `exasol-udf-macros` bump `0.19.1 → 0.20.0` (adds `UdfContext::node_count()`), in both root `Cargo.toml` and `crates/lakehouse-engine/Cargo.toml`. Confirmed published on crates.io.

## Migration

| Current | New |
|---------|-----|
| `CONNECTION_NAME` VS property drove connect-back topology discovery | Property removed; topology from `ctx.node_count()` + `available_parallelism()`. Existing VS instances that set `CONNECTION_NAME` for this purpose ignore it silently (it is not read elsewhere in this crate). |

## Implementation Tasks

1. **Dependency bump.** Bump `exasol-udf-sdk` and `exasol-udf-macros` to `0.20.0` in root `Cargo.toml` (line ~46) and `crates/lakehouse-engine/Cargo.toml` (lines ~20–21); keep the `emit-arrow` feature. Update the stale SDK version line in `CLAUDE.md` (currently reads `0.18.0`) to `0.20.0` while touching versions.
2. **Rewrite `resolve_cluster_nodes`.** Drop the `CONNECTION_NAME` lookup, the connect-back closure, and the `SELECT NPROC()` / `SELECT PARAM_VALUE(...)` queries. Source node count from `ctx.node_count()` (map `0 → 1`); source core count from `parse_nr_of_cores_override(props)` else `std::thread::available_parallelism()` (map `Err`/unavailable → `0`). Update the doc comment to describe the new sources. Preserve the exact `(u32, u32)` signature and the `parse_nr_of_cores_override` helper unchanged. [expert]
3. **Delete dead code and stale comments.** Remove the `PROP_CONNECTION_NAME` constant (line ~39) and the `nproc_value_to_count` (lines ~740–749) and `varchar_value_to_u32` (lines ~753–760) helpers. Also purge every stale *comment* that references connect-back topology, `NPROC()`, `PARAM_VALUE`, or `CONNECTION_NAME` — including the module-level comments at `mod.rs` ~37, ~63, and the `resolve_cluster_nodes` doc comment block (~685–701). **Preserve** the legitimate credential path untouched: `ctx.connection(name)` in `connection.rs:112`, the `use exasol_udf_sdk::connect_back::ConnectionObject;` import in `connection.rs:268` (that is the CONNECTION-object *type* used to read `CATALOG_CONNECTION` credentials — not a connect-back *session*), and the `ctx.connection()` ordering comment at `mod.rs:128`/`:151`. Confirm no remaining references via the zero-trace gate (Task 6).
4. **Update the `NoopCtx` test double.** Give `NoopCtx` (or a small parameterised variant) a configurable `node_count()` override so tests can drive both the `0 → default 1` path and a `> 1` real-cluster path distinctly (trait default is `0`). [expert]
5. **Rewrite affected unit tests.** Rewrite to remove `PROP_CONNECTION_NAME`/connect-back framing and exercise the new sources:
   - `cluster_nodes_defaults_to_one_on_connect_back_failure` (line ~896) → assert `node_count() == 0` maps to `CLUSTER_NODES == 1`.
   - `cluster_nodes_defaults_to_one_when_no_connection_name` (line ~906) → rename/repurpose to the `node_count() == 0` default case; add a companion asserting a stubbed `node_count() == N (>1)` is passed through verbatim.
   - `create_response_carries_cluster_nodes_property` (line ~919) → keep the JSON-assembly assertion; drive it off the stubbed context.
   - `nr_of_cores_defaults_to_zero_when_unavailable` (line ~1184) → assert core count comes from `available_parallelism()` when no override (assert `≥ 1` on a real host; `available_parallelism()` is not injectable, so assert "positive, host-sourced" rather than an exact number) and `0` only in the genuinely-unavailable branch if reachable.
   - `nr_of_cores_property_overrides_connect_back` (line ~1535) → rename to `..._overrides_auto_detect`; keep exact `parse_nr_of_cores_override` assertions and the override-wins path (override `8` → cores `8`, node count from stubbed `node_count()`).
   - `nr_of_cores_property_falls_back_to_auto_detect` (line ~1563) → keep the `parse_nr_of_cores_override` `None` cases; replace the connect-back-fallback tail with the `available_parallelism()` fallback (assert positive) and remove `PROP_CONNECTION_NAME`. [expert]
   - Also update the E2E test `create_vs_records_cluster_nodes_property` in `crates/lakehouse-engine/tests/e2e_scan_test.rs` (~line 765): the assertion is behaviour-preserving (`CLUSTER_NODES >= 1`) but its doc comment (~lines 762–763) attributes the value to `CONNECTION_NAME`/connect-back and must be rewritten to attribute it to `ctx.node_count()`.
6. **Zero-trace gate — no connect-back topology or `NPROC()` remains anywhere.** After Tasks 2–5, assert the removal is total across production code **and** tests. Run, from the repo root, expecting **zero** matches:
   ```
   rg -n 'NPROC|PARAM_VALUE|CONNECTION_NAME|\.connect_back\(|session\.query|nproc_value_to_count|varchar_value_to_u32' crates/
   ```
   Every hit must be gone from `src/**` and `tests/**` — including comments, doc comments, string literals, test names, and assert messages. **Allowlisted survivors (these are NOT connect-back topology and MUST remain):** `ctx.connection(...)` calls and the `use exasol_udf_sdk::connect_back::ConnectionObject;` import in `connection.rs` (CONNECTION-object credential reading for `CATALOG_CONNECTION`); the pattern above is deliberately written to exclude both (`\.connect_back\(` matches the session call, not the `connect_back::ConnectionObject` type path; `CONNECTION_NAME` does not match `CATALOG_CONNECTION`). Then confirm no non-`_recorded` spec still references `CONNECTION_NAME` / `PARAM_VALUE` / connect-back for topology (`rg` over `specs/` excluding `specs/_recorded/`); confirm `packaging/single-so-two-entry-points` "connect-back feature" wording is about the SDK feature flag generally, not this topology path (no change needed).

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 (dependency bump) |
| Group B | Task 2 (rewrite `resolve_cluster_nodes`), Task 3 (delete dead code), Task 4 (`NoopCtx` update) |
| Group C | Task 5 (rewrite unit tests) |
| Group D | Task 6 (cross-check) |

Sequential dependencies:
- Group A → Group B (SDK 0.20.0 must be present for `ctx.node_count()` to compile)
- Group B → Group C (tests exercise the rewritten function and updated test double)
- Group C → Group D

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `PROP_CONNECTION_NAME` in `crates/lakehouse-engine/src/adapter/mod.rs` (~line 39) | Connect-back topology path removed; property no longer read |
| Function | `nproc_value_to_count` in `crates/lakehouse-engine/src/adapter/mod.rs` (~lines 740–749) | `SELECT NPROC()` result parsing no longer performed |
| Function | `varchar_value_to_u32` in `crates/lakehouse-engine/src/adapter/mod.rs` (~lines 753–760) | `SELECT PARAM_VALUE(...)` result parsing no longer performed |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Adapter records the cluster node count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `create_response_carries_cluster_nodes_property` (+ node_count passthrough case) |
| Cluster node count defaults to one when it cannot be determined | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `cluster_nodes_defaults_to_one_when_node_count_zero` |
| Adapter records the per-node core count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_from_available_parallelism_when_unavailable` |
| NR_OF_CORES VS property overrides the auto-detected core count | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_property_overrides_auto_detect` |
| NR_OF_CORES VS property is ignored when absent, empty, or not a positive integer | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `nr_of_cores_property_falls_back_to_auto_detect` |
| Adapter records the DataFusion target partition count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | existing DATAFUSION_TARGET_PARTITIONS tests (unchanged behaviour; re-run) |
| Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | existing DATAFUSION_THREADS_PER_UDF tests (unchanged behaviour; re-run) |
| Recorded node count and parallelism factor drive later work-unit sharding | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | existing parallelism-factor / shard-count tests (unchanged behaviour; re-run) |

Note: these scenarios describe pure per-request computation with no network I/O (topology is now read in-process), so unit tests against a stubbed `UdfContext` are the correct proof form. `available_parallelism()` is not injectable, so the core-count-from-autodetect assertion is "positive, host-sourced" rather than an exact number.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/create-virtual-schema-adapter-notes | `make cross-musl-udf-build && make test-e2e` (createVirtualSchema against the Exasol Docker container, then `SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE ADAPTER_NOTES IS NOT NULL`) | `adapterNotes` JSON shows `CLUSTER_NODES` = the live node count (`1` on single-node Docker, not defaulted-away) and `NR_OF_CORES` = the container's host core count (a positive integer), with no `CONNECTION_NAME` needed |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Zero-trace gate | `rg -n 'NPROC\|PARAM_VALUE\|CONNECTION_NAME\|\.connect_back\(\|session\.query\|nproc_value_to_count\|varchar_value_to_u32' crates/` | **No matches** (0 lines) — no connect-back topology or `NPROC()` trace in production code or tests. `ctx.connection()` / `connect_back::ConnectionObject` credential usage is intentionally not matched and remains. |
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings (no dead-code warnings for removed helpers) |
| Format | `cargo fmt` | No changes |
