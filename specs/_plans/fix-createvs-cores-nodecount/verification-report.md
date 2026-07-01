# Verification Report: fix-createvs-cores-nodecount

**Generated:** 2026-07-01

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Issue #32 fixed: `createVirtualSchema` topology now sourced in-process (`UdfContext::node_count()` + `available_parallelism()`); connect-back `PARAM_VALUE` path removed. All checks green including 35 E2E tests against the live Exasol Docker stack. |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Zero-trace gate | ✓ |
| Scenario Coverage | ✓ |
| Manual / E2E Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit (lakehouse-engine lib) | 312 | 312 | 0 | 0 |
| Unit (vs-expression lib) | 53 | 53 | 0 | 0 |
| Integration (host, non-DB) | 9 | 9 | 0 | 2 (micro_bench) |
| E2E (`e2e_capability_test`) | 7 | 7 | 0 | 0 |
| E2E (`e2e_scan_test`) | 28 | 28 | 0 | 0 |

E2E ran against the live Exasol + MinIO + Iceberg REST Docker stack. The SLC 0.20.0
fingerprint matched the SDK 0.20.0 `.so` (no `F-UDF-CL-RUST-9001: Fingerprint mismatch`),
confirming the SLC-lockstep bump (see Deviations).

## Tool Evidence

### Build

```
docker run --rm ... rust:1.92-bookworm cargo build --release -p lakehouse-engine
  Downloaded exasol-udf-sdk v0.20.0 / exasol-udf-macros v0.20.0
  Finished `release` profile [optimized] target(s) in 1m 24s      # exit 0
```

### Linter

```
cargo clippy --all-targets → Finished dev profile; 0 warnings
```

### Formatter

```
cargo fmt --check → clean (exit 0)
```

### Zero-trace gate

```
rg -n 'NPROC|PARAM_VALUE|CONNECTION_NAME|\.connect_back\(|session\.query|nproc_value_to_count|varchar_value_to_u32' crates/
  → 0 matches (credential path ctx.connection / connect_back::ConnectionObject intentionally preserved in connection.rs)
```

## Scenario Coverage

| Feature | Scenario | Test Location | Test Name | Passes |
|---------|----------|---------------|-----------|--------|
| vs-adapter/create-virtual-schema-adapter-notes | Records the cluster node count in adapterNotes | `src/adapter/mod.rs` | `create_response_carries_cluster_nodes_property` + `cluster_nodes_passes_through_reported_node_count` | Pass |
| vs-adapter/create-virtual-schema-adapter-notes | Node count defaults to one when undeterminable | `src/adapter/mod.rs` | `cluster_nodes_defaults_to_one_when_node_count_zero` | Pass |
| vs-adapter/create-virtual-schema-adapter-notes | Records the per-node core count in adapterNotes | `src/adapter/mod.rs` | `nr_of_cores_from_available_parallelism_when_unavailable` | Pass |
| vs-adapter/create-virtual-schema-adapter-notes | NR_OF_CORES property overrides auto-detected cores | `src/adapter/mod.rs` | `nr_of_cores_property_overrides_auto_detect` | Pass |
| vs-adapter/create-virtual-schema-adapter-notes | NR_OF_CORES ignored when absent/empty/non-positive | `src/adapter/mod.rs` | `nr_of_cores_property_falls_back_to_auto_detect` | Pass |
| vs-adapter/create-virtual-schema-adapter-notes-resources | Node count recorded in adapterNotes end-to-end | `tests/e2e_scan_test.rs` | `create_vs_records_cluster_nodes_property` | Pass |

`available_parallelism()` is not injectable, so the core-count-from-autodetect scenarios assert
"positive, host-sourced" (`>= 1`) rather than an exact number, per the plan's Scenario Coverage note.

## Code Review

0 blocking findings. The reviewer confirmed the failure mode is eliminated (no shared fallible path
remains), precedence and sentinel handling match the spec exactly, both the `0→1` default and `>1`
passthrough are exercised, and no test asserts an impossible `cores == 0` on a real host. One minor
pre-existing observation (`resolve_cluster_nodes` name under-describes its `(u32, u32)` return) —
out of scope, signature preservation was mandated by the plan.

## Deviations from Plan

1. **SLC lockstep bump (0.19.1 → 0.20.0), not in the plan's task list.** The plan bumped the SDK to
   0.20.0 but did not account for the `.so`/SLC fingerprint lockstep (`EXA_SDK_FINGERPRINT =
   {sdk}:{rustc}`) that E2E requires — a 0.19.1 SLC rejects a 0.20.0-SDK `.so`. The matching
   `lc-rust-0.20.0` release exists (latest tag), so `SLC_VERSION` was bumped to 0.20.0 in
   `tests/e2e_scan_test.rs`, `tests/e2e_capability_test.rs`, and the `Makefile`. Verified by the
   clean E2E run (no fingerprint mismatch).
2. **Version bump 0.16.1 → 0.17.0 (minor), user-directed.** Chosen over a patch bump to signal the
   breaking `CONNECTION_NAME` property removal and the mandatory SLC 0.20.0 redeploy.
