# Plan: refactor-nr-of-cores-detection

## Summary

Delete the `NR_OF_CORES` VS property, the `NOTE_NR_OF_CORES` adapterNotes entry, and the `0 = unknown` core-count sentinel. Replace them with one parameterless resolver that reads `std::thread::available_parallelism()` at the single call site the property-aware resolver occupies today and defaults to `1` on failure.

## Design

### Context

The adapter resolves a per-node core count at `createVirtualSchema` time. That count feeds four derivations: the parallelism factor, the DataFusion target-partition count, the DataFusion threads-per-UDF count, and the S3 connection budget. Three mechanisms surround that one value today, and none of them earns its cost.

The `NR_OF_CORES` VS property lets an operator override auto-detection. No recorded need for that override exists. The bench harness is its only user, and the docker bench path already controls the real core count through the container's CPU quota.

The `NOTE_NR_OF_CORES` adapterNotes entry persists the count into the virtual schema. `find_referencing_symbols` on `NOTE_NR_OF_CORES` returns two writes and zero reads. No pushdown ever reads the count back. Only the four derived entries round-trip.

The `0 = unknown` sentinel threads a third state through every derivation. Two derivations already collapse it into their `max(1, ...)` floor, so their `0` branch is dead by construction. One derivation, `resolve_s3_max_connections`, gives `0` a distinct answer of 16 and therefore carries a real branch.

The sentinel is the information leak this plan closes. One encoding decision, that `0` means "not known", is currently reflected in the resolver that produces it, in four derivations that consume it, and in the doc comment of a constant in `scan::spec`. No module owns it. Replacing it with a value every derivation already handles leaves nothing to agree on.

This plan is the direct sequel to #184, which removed the persisted cluster-node count for the same reason. That change left `cluster_nodes_from_context(ctx) -> usize`, called once in `dispatch` and threaded onward as a plain `usize` argument. This plan applies that shape to the core count.

- **Goals**: One source for the per-node core count. One call site. No property, no note, no sentinel. Verified against a real Exasol container before merge.
- **Non-Goals**: Changing how any derivation computes its budget from a given core count. Changing the derivations' parameter shape. Reading the core count from UDF metadata instead of the host.

### Decision

#### Architecture

The core count becomes an ordinary resolved input, produced once and passed down. It stops being configuration and stops being state.

```
BEFORE                                    AFTER

props ──> parse_nr_of_cores_override      available_parallelism()
              │ None                            │ Err
              v                                 v
       available_parallelism_or_0  ──> 0   core_count_or_default ──> 1
              │                                 │
              v                                 v
         nr_of_cores: u32 ──┬──> NOTE_NR_OF_CORES    nr_of_cores: u32 ──┬──> resolve_parallelism_factor
                            ├──> resolve_parallelism_factor             ├──> resolve_df_threading
                            ├──> resolve_df_threading                   └──> resolve_s3_max_connections
                            └──> resolve_s3_max_connections
                                   │ if 0 -> 16
```

The four derivations keep their `nr_of_cores: u32` parameter. Each one stays a pure function of an injected count, so a unit test supplies an arbitrary count without touching the host.

A small pure seam makes the failure branch testable. `resolve_nr_of_cores()` calls `std::thread::available_parallelism()` and hands the `io::Result` to `core_count_or_default`, which maps `Ok(n)` to `n` and `Err` to `1`. A unit test calls `core_count_or_default` with an `Err` directly.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Resolve once at the entry point, thread the value | `handle_create_virtual_schema` reads the count, passes it to four derivations | Matches the `cluster_nodes_from_context` shape #184 established. Keeps every derivation pure and injectable. |
| Pure seam over the fallible call | `core_count_or_default(io::Result<NonZeroUsize>) -> u32` | The `Err` branch is otherwise unreachable in a test, so its behavior would be unverifiable. |
| One floor, applied per derivation | `max(1, ...)` inside each derivation | Removes the shared sentinel. Each derivation already owns its own floor. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Keep `nr_of_cores` as a parameter on each derivation | Have each derivation call `available_parallelism()` itself | An internal call makes each derivation read ambient state, which breaks its unit tests. It also reads the host four times per request instead of once. |
| Uniform fallback of `1`, with no `DEFAULT_S3_MAX_CONNECTIONS` special case | Keep the `0` branch in `resolve_s3_max_connections` only | Keeping one sentinel consumer forces every caller to keep producing the sentinel. The branch fires only when the OS cannot report a core count, which is not a reachable state on Linux, Docker, or the production cluster. |
| Remove the property from the bench harness rather than keep a bench-only lever | Keep `NR_OF_CORES` as an undocumented bench-only property | A property the adapter reads is a supported property, whatever the docs say. Docker bench keeps an equivalent lever in `LH_EXASOL_CPUS`. |
| Leave `DEFAULT_S3_MAX_CONNECTIONS` in `scan::spec` | Move the constant into `adapter` | Deleting the AUTO unknown-core branch removes one of its three consumers. Two survive. `crates/lakehouse-engine/src/adapter/mod.rs:404` falls back to it for an absent or invalid `S3_MAX_CONNECTIONS` adapterNote in `handle_pushdown_request`, and `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs:192` passes it to `build_table_root_store`. Both live in `adapter`, which already depends on `scan::spec`, so moving the constant would create the reverse dependency the current placement avoids. |
| Verify with a Docker E2E assertion plus a manual multi-node check | Trust the Linux cgroup behavior of `available_parallelism()` | CLAUDE.md requires a live check over documentation or memory. No multi-node cluster runs in CI, so that half stays manual. |

### Iceberg and Delta compliance gate

The CLAUDE.md gate does not apply. This change touches per-node core-count detection for CPU, thread, and connection budget sizing, which is a VS-adapter resource-configuration concern. It touches no scanning, no pushdown, and no schema or type handling in either the Iceberg table spec or the Delta protocol sense. No normative citation is required, and the absence of one is not an oversight.

### Terminology left unchanged

Four places name `NR_OF_CORES` as Exasol's own per-node core-count parameter, not as the removed VS property: `CLAUDE.md:150`, `docs/architecture.md:43`, `specs/mission.md:80`, and `specs/udf-context.md:11`. Exasol sizes each node's UDF VM pool from that parameter, and this plan does not change it. Leave all four as they stand.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema-adapter-notes | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/vs-adapter/create-virtual-schema-adapter-notes-resources/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/refresh-and-set-properties | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/vs-adapter/refresh-and-set-properties/spec.md` |
| datafusion-scan/scan-execution-threading | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/datafusion-scan/scan-execution-threading/spec.md` |
| datafusion-scan/scan-execution-connection-concurrency | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/datafusion-scan/scan-execution-connection-concurrency/spec.md` |
| e2e-harness/cloud-e2e-harness | CHANGED | `specs/_plans/refactor-nr-of-cores-detection/e2e-harness/cloud-e2e-harness/spec.md` |

## Impact

Three user-visible changes ship with this plan. All three are breaking.

**The `NR_OF_CORES` virtual-schema property stops working.** A `CREATE VIRTUAL SCHEMA` statement that supplies it still succeeds, because the adapter reads properties by name and ignores names it does not know. The property has no effect. An operator who relied on it to raise or lower the derived thread and connection budgets loses that lever and must constrain the node's CPU quota instead.

**The remote bench target loses its core-count override.** `BENCH_NR_OF_CORES` disappears from `bench/run.sh`, the three sweep scripts, `bench/.env.example`, and `deploy/scripts/secrets.sh`. A remote run now sizes its budgets from the cluster node's real detected core count. Docker bench keeps an equivalent lever, because `LH_EXASOL_CPUS` sets the container's CPU quota and `available_parallelism()` honours it. Remote mode has no equivalent lever. A bench comparison across this change is therefore not directly comparable on the remote target unless `BENCH_PARALLELISM_FACTOR` and the `DATAFUSION_*` properties are pinned.

**The unknown-core S3 connection budget changes from 16 to 4.** This applies only when `std::thread::available_parallelism()` returns an error, which requires an OS that cannot report a core count. Linux, Docker, and the production cluster all report one. The value moves because the uniform derivation now applies at a core count of `1`, giving `1 × S3_CONNECTIONS_PER_THREAD`, which is 4.

**`adapterNotes` loses its `NR_OF_CORES` entry.** No code reads it, so nothing breaks. A virtual schema created under an earlier version keeps its persisted entry, which survives unread. Any external tooling that reads `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` and expects the key must stop doing so.

## Migration

| Current | New |
|---------|-----|
| `CREATE VIRTUAL SCHEMA ... NR_OF_CORES = '8'` | Drop the property. Constrain the node's CPU quota to set the effective count. |
| `BENCH_NR_OF_CORES=8` in `bench/.env` | Drop the variable. Docker bench: set `LH_EXASOL_CPUS`. Remote bench: no equivalent. |
| A schema created before this change, carrying `NR_OF_CORES` in `adapterNotes` | No migration. The entry survives unread and inert. Drop and recreate the schema to remove it. |

## Implementation Tasks

The `vs-adapter/create-virtual-schema` delta carries no task beyond the argument drop of task 2.1. Its changed clause lists `NR_OF_CORES` among the preserved `adapterNotes` entries, and `build_adapter_notes` already merges rather than clobbers, so `table_map_merges_with_existing_notes` needs no further change.

The `vs-adapter/refresh-and-set-properties` delta is different. It adds a normative clause that an `NR_OF_CORES` entry left behind by an earlier adapter version survives the rebuild unread and inert. `refresh_rebuilds_table_map_preserves_notes` seeds only `OTHER_KEY` and `TABLE_MAP` today, so nothing exercises that clause. Task 2.8 covers it.

### 1. Adapter core-count resolution

- [ ] 1.1 Check for an existing GitHub issue covering this refactor. Create one with `gh issue create` when none exists, and reference it in the implementing commit as `Closes #<n>`.
- [ ] 1.2 In `crates/lakehouse-engine/src/adapter/mod.rs`, delete `const PROP_NR_OF_CORES` (line 57), `const NOTE_NR_OF_CORES` (line 46), `fn parse_nr_of_cores_override` (lines 938-948), and `fn available_parallelism_or_0` (lines 974-980). Replace `fn resolve_nr_of_cores(props: &Json) -> u32` (lines 950-960) with a parameterless `fn resolve_nr_of_cores() -> u32` that calls `std::thread::available_parallelism()` and delegates the `io::Result` to a new pure `fn core_count_or_default(detected: std::io::Result<NonZeroUsize>) -> u32` mapping `Err` to `1`. Update the one call site at line 251 of `handle_create_virtual_schema`.
- [ ] 1.3 Delete the `NOTE_NR_OF_CORES` insert (lines 684-687) and the `nr_of_cores: u32` parameter from `fn build_adapter_notes` (lines 659-731). Update its doc comment, which currently lists `NR_OF_CORES` first among the recorded keys. Keep the `#[allow(clippy::too_many_arguments)]` attribute, because the signature still carries 11 arguments.
- [ ] 1.4 Update the `nr_of_cores == 0` framing in the doc comments of `resolve_parallelism_factor` (line 733), `resolve_df_threading` (line 780), `auto_threads_per_udf` (line 808), and `resolve_df_fixed_count` (line 822). Each already floors its own result, so no code changes. Describe the floor as the ordinary formula rather than as an unknown-core branch. Restate two further comments in the same file that name the removed property. Line 48, the `PROP_PARALLELISM_FACTOR` comment reading `Default: max(NR_OF_CORES * 2, 8)`, becomes the hardware-aware default derived from the detected core count. Line 52, the `DEFAULT_PARALLELISM_FACTOR` doc comment reading `Minimum parallelism factor (floor applied when NR_OF_CORES is 0 or very small)`, becomes the floor applied to a small detected core count. Neither restated comment may reference `NR_OF_CORES` or `0`, because the residual-reference sweep in § Checklist fails on both lines otherwise.
- [ ] 1.5 Delete the `if nr_of_cores == 0 { return DEFAULT_S3_MAX_CONNECTIONS; }` early return in `resolve_s3_max_connections` (lines 835-889) and rewrite the doc-comment bullet that documents it. Keep the `nr_of_cores: u32` parameter and the AUTO formula unchanged. Keep the `DEFAULT_S3_MAX_CONNECTIONS` import at `crates/lakehouse-engine/src/adapter/mod.rs:23`. It stays required, because `handle_pushdown_request` still reads the constant at line 404. [expert]
- [ ] 1.6 Delete only the `and by the adapter's AUTO derivation (resolve_s3_max_connections) when nr_of_cores is 0 (unknown)` clause from the `DEFAULT_S3_MAX_CONNECTIONS` doc comment in `crates/lakehouse-engine/src/scan/spec.rs` (lines 1318-1324). Keep both surviving roles and name them: the serde default for a `ScanSpec` or `CommonScanSpec` JSON payload that omits `s3_max_connections`, and the pushdown-side fallback at `crates/lakehouse-engine/src/adapter/mod.rs:404` for an absent or invalid `S3_MAX_CONNECTIONS` adapterNote. Keep the reverse-dependency rationale as it stands, because `adapter` still names the constant at two call sites.
- [ ] 1.7 Reword the doc comment at `crates/lakehouse-engine/src/scan/diagnostics.rs:48`, which reads `do NOT rely on NR_OF_CORES=1`. Name the per-instance thread budget, not the removed property. Reword `crates/lakehouse-engine/src/scan/mod.rs:109`, which reads `the NR_OF_CORES-bound VM pool`, to name the node's core-count-bound VM pool without the property name.

### 2. Adapter unit tests

- [ ] 2.1 In `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, drop the second positional argument from all 17 `build_adapter_notes(...)` call sites.
- [ ] 2.2 Delete `adapter_notes_records_nr_of_cores` (line 543), `nr_of_cores_property_overrides_auto_detect` (line 1043), and `nr_of_cores_property_falls_back_to_auto_detect` (line 1069).
- [ ] 2.3 Rewrite `nr_of_cores_from_available_parallelism_when_unavailable` (line 572) as `core_count_from_available_parallelism_is_not_recorded`, running against the parameterless resolver and asserting both a positive host-sourced count and the absence of an `NR_OF_CORES` key from `build_adapter_notes` output. Add `NR_OF_CORES` absence to the assertions in `adapter_notes_omit_cluster_nodes` (line 361).
- [ ] 2.4 Add `core_count_defaults_to_one_when_detection_fails`, calling `core_count_or_default` with an `Err` and asserting `1`.
- [ ] 2.5 Add `nr_of_cores_property_is_ignored`, building two property objects that differ only by a literal `"NR_OF_CORES": "999"` key and asserting every resolver returns identical values for both.
- [ ] 2.6 Change the `0` core-count argument to `1` in four tests, and rename three of them to name a one-core node instead of an unknown count. The expected results do not change. `df_target_partitions_unknown_cores_defaults_to_1` (line 1139) becomes `df_target_partitions_one_core_defaults_to_1`. `df_threads_per_udf_unknown_cores_defaults_to_1` (line 1173) becomes `df_threads_per_udf_one_core_defaults_to_1`. `auto_mode_falls_back_to_one_when_cores_zero` (line 1290) becomes `auto_mode_yields_one_thread_on_one_core`. `resolve_s3_max_connections_fixed_value_wins` (line 1742) keeps its name and changes only its `0` argument at line 1742.
- [ ] 2.7 Rewrite `resolve_s3_max_connections_auto_zero_cores_defaults` (line 1805) as `resolve_s3_max_connections_auto_one_core_yields_four`, asserting `resolve_s3_max_connections(&absent, 1, 1) == 4` and `resolve_s3_max_connections(&absent, 1, 8) == 4`, which is the deliberate change from `DEFAULT_S3_MAX_CONNECTIONS`. [expert]
- [ ] 2.8 Seed `"NR_OF_CORES": "8"` into the `schemaMetadataInfo.adapterNotes` of the request in `refresh_rebuilds_table_map_preserves_notes` (`crates/lakehouse-engine/src/adapter/adapter_tests.rs:396`), beside the existing `OTHER_KEY` and `TABLE_MAP` entries, and assert the key survives the rebuild carrying its original value. This is the only evidence for the `vs-adapter/refresh-and-set-properties` clause that an inherited `NR_OF_CORES` entry survives unread and inert, and it is the only remaining pin on merge-not-clobber for that key once `build_adapter_notes` stops writing it.

### 3. Docker E2E verification

- [ ] 3.1 In `crates/lakehouse-engine/tests/e2e_scan_test.rs`, flip the `NR_OF_CORES` assertion in `create_vs_omits_cluster_nodes_from_adapter_notes` (lines 1334-1338) from present to absent, mirroring the `CLUSTER_NODES` assertion below it. Update the test's doc comment (lines 1289-1292).
- [ ] 3.2 Add `adapter_detects_container_cpu_quota` to the same file. Create a probe virtual schema through `VsProps::new(...).with_parallelism_factor(1)`, which sets `udf_instances_per_node` to 1 so the AUTO derivation records the detected core count unchanged as `DF_THREADS_PER_UDF`. Read the effective CPU quota from the running Exasol container rather than from the test process environment: invoke `std::process::Command::new("docker")` with `exec`, the name from `exasol_container()`, and a read of `/sys/fs/cgroup/cpu.max`, mirroring the call at `crates/lakehouse-engine/tests/common/stack.rs:136`, then divide the quota field by the period field. Assert the `DF_THREADS_PER_UDF` entry read from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` equals that container-read quota. Assert first that the quota is strictly less than the test host's own `std::thread::available_parallelism()`, and fail with a message naming that unmet precondition when it is not, because at an equal quota and host count the main assertion passes whether the adapter reads the container quota or the unconstrained host. [expert]
- [ ] 3.3 If task 3.2 fails because the adapter VM reports the host core count rather than the container quota, STOP and escalate. Do not weaken the assertion. That outcome means auto-detection cannot replace the property, and the right fix is a different source such as UDF metadata.
- [ ] 3.4 Make the E2E CI job satisfy task 3.2's precondition. `.github/workflows/ci.yml` sets `LH_EXASOL_CPUS: "2"` at lines 537, 647, 750, and 853, under a comment asserting the runner has 2 vCPUs. If that comment is accurate the quota equals the host count and task 3.2 fails on its precondition. Print the runner's `nproc` in the job that runs `make test-e2e`, and when it does not exceed `LH_EXASOL_CPUS`, lower `LH_EXASOL_CPUS` for that job until the inequality holds. Correct the vCPU comment to the measured count.

### 4. Bench harness

- [ ] 4.1 In `bench/run.sh`, drop the `nr_of_cores` parameter and the `NR_OF_CORES` line from `build_vs_extra_props` (lines 38-45). Update both call sites (lines 370 and 404) and the comment at line 367.
- [ ] 4.2 Update the five `build_vs_extra_props` calls in the `bench/run.sh selftest` block and the two `case` patterns at lines 148 and 152 that assert the property string. Run `bash bench/run.sh selftest` and confirm it exits 0.
- [ ] 4.3 Remove the `NR_OF_CORES` property and the `BENCH_NR_OF_CORES` variable from `bench/batch_size_sweep.sh` (lines 27, 48), `bench/emit_s3conn_sweep.sh` (lines 22, 41), and `bench/batch_size_aggcheck.sh` (lines 11, 18).
- [ ] 4.4 Remove `BENCH_NR_OF_CORES` from `bench/.env.example` (lines 16, 19-22) and from the generated file in `deploy/scripts/secrets.sh` (line 49).

### 5. Operator documentation

- [ ] 5.1 Delete the `NR_OF_CORES` row from the property table in `docs/tuning.md` (line 19). Restate the `PARALLELISM_FACTOR`, `DATAFUSION_THREADS_PER_UDF`, and `DATAFUSION_TARGET_PARTITIONS` defaults (lines 20, 22, 23) against the detected core count rather than the property. Rewrite the quick recommendation (line 41), which tells the operator to substitute `<NR_OF_CORES>`. Rewrite the unknown-core bullet (line 53), which states the removed default of 16.
- [ ] 5.2 Rewrite the Parallelism section of `bench/README.md` (line 105). State that the core count is auto-detected, that `LH_EXASOL_CPUS` controls it in docker mode, and that remote mode has no equivalent lever.
- [ ] 5.3 Remove `BENCH_NR_OF_CORES` from the variable list in `docs/benchmark.md` (line 54).

The multi-node confirmation is not an implementation task. It is a pre-merge gate no agent can execute, recorded as the last row of § Verification § Manual Testing.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Adapter core-count resolution and its observations | 1.1-1.7, 2.1-2.8, 3.1-3.4 | — | spec deltas `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, `vs-adapter/create-virtual-schema`, `vs-adapter/refresh-and-set-properties`, `datafusion-scan/scan-execution-threading`, `datafusion-scan/scan-execution-connection-concurrency`; `crates/lakehouse-engine/src/adapter/mod.rs`, `crates/lakehouse-engine/src/adapter/adapter_tests.rs`, `crates/lakehouse-engine/src/scan/spec.rs`, `crates/lakehouse-engine/src/scan/diagnostics.rs`, `crates/lakehouse-engine/src/scan/mod.rs`, `crates/lakehouse-engine/tests/e2e_scan_test.rs`, `crates/lakehouse-engine/tests/common/stack.rs`, `.github/workflows/ci.yml` |
| B: Bench harness and operator documentation | 4.1-4.4, 5.1-5.3 | A (task 5.1 restates the connection-budget behavior group A lands) | spec delta `e2e-harness/cloud-e2e-harness`; `bench/run.sh`, `bench/batch_size_sweep.sh`, `bench/emit_s3conn_sweep.sh`, `bench/batch_size_aggcheck.sh`, `bench/.env.example`, `bench/README.md`, `deploy/scripts/secrets.sh`, `docs/tuning.md`, `docs/benchmark.md` |

Group A is one cluster, not three. Every adapter-side spec delta in this plan is implemented by the same function set in `adapter/mod.rs`, and the unit tests plus the E2E observation assert that same function set. Splitting them by file would make three agents rebuild one mental model.

Group B shares no file and no owned spec delta with group A. It depends on A because task 5.1 documents the connection-budget number group A changes, so B runs after A rather than beside it.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `crates/lakehouse-engine/src/adapter/mod.rs:46` `NOTE_NR_OF_CORES` | The note is no longer written, and no code ever read it |
| Constant | `crates/lakehouse-engine/src/adapter/mod.rs:57` `PROP_NR_OF_CORES` | The property is removed |
| Function | `crates/lakehouse-engine/src/adapter/mod.rs:938-948` `parse_nr_of_cores_override` | Parses the removed property |
| Function | `crates/lakehouse-engine/src/adapter/mod.rs:974-980` `available_parallelism_or_0` | Replaced by `core_count_or_default`, which returns 1 rather than the removed sentinel |
| Parameter | `crates/lakehouse-engine/src/adapter/mod.rs` `build_adapter_notes(nr_of_cores)` | Its only use was the deleted note insert |
| Branch | `crates/lakehouse-engine/src/adapter/mod.rs:885-887` the `nr_of_cores == 0` early return | The sentinel no longer reaches this function |
| Test | `crates/lakehouse-engine/src/adapter/adapter_tests.rs:543` `adapter_notes_records_nr_of_cores` | Asserts a note that is no longer written |
| Test | `crates/lakehouse-engine/src/adapter/adapter_tests.rs:1043` `nr_of_cores_property_overrides_auto_detect` | Tests the removed property |
| Test | `crates/lakehouse-engine/src/adapter/adapter_tests.rs:1069` `nr_of_cores_property_falls_back_to_auto_detect` | Tests the removed property |
| Shell function parameter | `bench/run.sh:41` `build_vs_extra_props nr_of_cores` | Emits the removed property |
| Env variable | `bench/.env.example:16`, `deploy/scripts/secrets.sh:49` `BENCH_NR_OF_CORES` | Feeds the removed property |
| Docs row | `docs/tuning.md:19` `NR_OF_CORES` | Documents the removed property |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| adapter-notes: createVirtualSchema adapterNotes omit the cluster node count | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` |
| adapter-notes: Adapter derives the per-node core count from available_parallelism() on every request | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `core_count_from_available_parallelism_is_not_recorded` |
| adapter-notes: Adapter uses a core count of 1 when available_parallelism() cannot report one | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `core_count_defaults_to_one_when_detection_fails` |
| adapter-notes: An NR_OF_CORES virtual-schema property no longer changes any resolved budget | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `nr_of_cores_property_is_ignored` |
| adapter-notes: The adapter VM detects the Docker container's CPU quota | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `adapter_detects_container_cpu_quota` |
| resources: Adapter records the parallelism factor in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `default_parallelism_factor_floors_at_eight` |
| resources: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `df_target_partitions_one_core_defaults_to_1` |
| resources: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `df_threads_per_udf_one_core_defaults_to_1` |
| resources: Adapter records the memory-pool fraction in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `memory_budget_params_round_trip_through_adapter_notes` |
| resources: Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_instance_overhead_mb_defaults_and_validates` |
| create-virtual-schema: Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `table_map_merges_with_existing_notes` |
| refresh: Refresh rebuilds the table map and preserves other adapter notes | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `refresh_rebuilds_table_map_preserves_notes` |
| threading: AUTO mode derives a per-instance thread budget that does not oversubscribe a node | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `auto_mode_derives_non_oversubscribing_threads` |
| threading: AUTO mode yields a single thread on a one-core node | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `auto_mode_yields_one_thread_on_one_core` |
| threading: FIXED mode uses the operator-supplied thread and partition values verbatim | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `fixed_mode_uses_supplied_values` |
| connection-concurrency: AUTO derivation sizes the per-instance budget from node capacity | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_s3_max_connections_auto_scales_with_cores` |
| connection-concurrency: AUTO derivation yields the single-core budget when the core count cannot be detected | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `resolve_s3_max_connections_auto_one_core_yields_four` |
| cloud-e2e-harness: Remote bench wires PARALLELISM_FACTOR into the virtual schema | Integration | `bench/run.sh` offline self-check | `bash bench/run.sh selftest` |
| cloud-e2e-harness: Remote bench selects its catalog backend from the bench environment | Integration | `bench/run.sh` offline self-check | `bash bench/run.sh selftest` |

Every derivation named above is a pure function of an injected core count, so its scenario maps to a unit test. The two scenarios that assert what the adapter VM observes on a live node map to Docker E2E tests, because no unit test can evidence what `available_parallelism()` returns inside an Exasol UDF VM.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/create-virtual-schema-adapter-notes | `make test-e2e` then `exapump sql -d "$DSN" -q "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'MY_LAKEHOUSE'"` | The JSON carries `PARALLELISM_FACTOR` and `TABLE_MAP`, and carries neither `NR_OF_CORES` nor `CLUSTER_NODES` |
| vs-adapter/create-virtual-schema-adapter-notes (quota detection) | Bring the stack up with `LH_EXASOL_CPUS=2`, create a VS with `PARALLELISM_FACTOR = '1'`, then read `DF_THREADS_PER_UDF` from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` | The value is `2` |
| vs-adapter/create-virtual-schema-adapter-notes-resources | Create a VS supplying `NR_OF_CORES = '99'`, then read `ADAPTER_NOTES` | The statement succeeds, and `PARALLELISM_FACTOR`, `DF_THREADS_PER_UDF`, and `S3_MAX_CONNECTIONS` match the values a VS created without the property carries |
| datafusion-scan/scan-execution-threading | `cargo test -p lakehouse-engine adapter::tests::auto_mode` | 0 failures |
| datafusion-scan/scan-execution-connection-concurrency | `cargo test -p lakehouse-engine resolve_s3_max_connections` | 0 failures |
| e2e-harness/cloud-e2e-harness | `bash bench/run.sh selftest` | Exit 0, no `FAIL:` line |
| e2e-harness/cloud-e2e-harness (docker run) | `BENCH_TARGET=docker LH_EXASOL_CPUS=4 make bench` | The run completes, and the generated `CREATE VIRTUAL SCHEMA` carries `PARALLELISM_FACTOR` but no `NR_OF_CORES` |
| vs-adapter/create-virtual-schema-adapter-notes (multi-node pre-merge gate, MANDATORY) | Against a multi-node Exasol cluster: `CREATE VIRTUAL SCHEMA CORE_PROBE USING ... WITH PARALLELISM_FACTOR = '1'`, then `exapump sql -d "$DSN" -q "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'CORE_PROBE'"`, and read the cluster's own per-node core count from `SELECT PARAM_VALUE('NR_OF_CORES')` | `DF_THREADS_PER_UDF` equals the per-node core count the cluster reports. **MUST NOT ship if the two values differ.** On a mismatch, file a follow-up GitHub issue naming the observed and the expected count, and fix the core-count source, for example by reading it from UDF metadata. Reintroducing the `NR_OF_CORES` VS property is prohibited. CI runs no multi-node cluster, so a human executes this gate and `verification-report.md` records it as not run until one has. |

CAUTION: `make bench` reads `bench/.env` when the file exists. A stale `bench/.env` from a prior `deploy/scripts/secrets.sh` run redirects the run at a remote cluster. Move the file aside before the docker-mode check.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Bench self-check | `bash bench/run.sh selftest` | Exit 0 |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Format | `cargo fmt` | No changes |
| Residual reference sweep | `git grep -n "NR_OF_CORES" -- . ':!specs/_plans' ':!specs/_decision'` | Only the four Exasol-parameter mentions listed under "Terminology left unchanged" |
| Manual multi-node gate | The multi-node pre-merge gate row in § Manual Testing | Executed by a human before the PR leaves draft, with the observed and expected counts recorded in the PR description. A mismatch blocks the merge. |
