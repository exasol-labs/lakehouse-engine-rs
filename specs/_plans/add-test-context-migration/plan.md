# Plan: add-test-context-migration

## Summary

Bumps `exasol-udf-sdk` to 0.24.0 and replaces 19 of the repository's 21 hand-rolled `UdfContext` test doubles with the SDK's `TestContext` and `DefaultsCtx`, plus one shared scan-path wrapper that owns the Arrow IPC decode. Closes issue #383 item 1.

## Design

### Context

Adding one scan test today costs a 40-line `UdfContext` double. Eleven scan integration tests carry a byte-identical `emit_record_batch_ipc` decode loop, and seven adapter tests repeat the same four required trait methods. Issue #383 names this amplification as the cost to remove.

`exasol-udf-sdk` 0.24.0 supplies most of the replacement. It does not supply all of it: `TestContext` leaves `emit_record_batch_ipc` at the trait default, which returns `UdfError::Unimplemented`. The scan raw path emits through that method, so the scan side needs one local wrapper.

- **Goals**: one owner for the Arrow IPC decode. Zero hand-rolled `UdfContext` impls under `tests/` and under `src/adapter/`. No change to any test assertion or expected value.
- **Non-Goals**: no production code change. No comment or doc-discipline pass (issue #383 item 3). No upstream change request against `language-container-rs`. No new spec coverage for the SDK-to-SLC version lockstep, which this plan exercises without altering.

### Decision

#### Architecture

The scan side gains one wrapper. The adapter side gains nothing local and calls the SDK directly.

```
                        exasol-udf-sdk 0.24.0 (dev-dependency, "test-support")
                        ┌──────────────────┬──────────────┐
                        │   TestContext    │ DefaultsCtx  │
                        └────────┬─────────┴──────┬───────┘
                                 │                │
        ┌────────────────────────┴───────┐        │
        │ tests/scan_fixture/            │        │
        │   BatchCapturingCtx            │        │
        │   (delegates; owns IPC decode) │        │
        └────────────────────────┬───────┘        │
                                 │                │
   11 tests/scan_*.rs   src/scan/storage_ref   src/adapter/*_tests.rs
                          _tests.rs (direct)      (7 doubles, direct)
```

`BatchCapturingCtx` holds a `TestContext`, delegates every `UdfContext` method it declares to that inner value, and overrides `emit_record_batch_ipc` alone. `EmitBatch::emit_batch` is a blanket impl over `UdfContext`, so `emit_batch` reaches the override without the wrapper naming it.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Delegating wrapper over one SDK type | `tests/scan_fixture/` | Overrides the single gap in `TestContext` and inherits every other method, so an SDK behavior change reaches the tests |
| Per-call capture grouping | `BatchCapturingCtx` | `scan_telemetry.rs` asserts a payload count, the other ten assert decoded rows, and one payload may carry several batches |
| Policy value over a bespoke double | `EmitPolicy::Reject`, `NextPolicy::Reject` | Both traps the doubles encode today are constructor arguments in 0.24.0 |
| `DefaultsCtx` for a trait-default assertion | `NoopCtx`'s call sites | `TestContext` overrides `node_count`, which is the value that test asserts |

#### Design diagnostic

`BatchCapturingCtx` is the one new interface, checked against `/speq:design-philosophy`'s Quick Diagnostic.

| Question | Answer |
|----------|--------|
| One-sentence responsibility? | Yes. It captures Arrow IPC emissions from a scan and answers everything else as `TestContext` does. |
| Easier to call than to reimplement? | Yes. It replaces a 40-line impl per test with one constructor call. |
| Would an internal change force an edit outside it? | No, provided the capture accessors stay. The decode is the decision it hides, and it is the only owner. |
| Public doc comment states reasoning? | Required by task 2.1: the comment must state why the wrapper exists, which is the `emit_record_batch_ipc` gap in `TestContext`. |
| One owner per design decision? | Yes for the decode, under `tests/`. Two in-crate doubles are retained for reachability and are named in the spec delta. |
| Boundary visible without reading internals? | Yes. It wraps one named SDK type and adds one named method. |
| Tactical shortcut with a follow-up? | The retained in-crate doubles are the one accepted residual, recorded in the spec delta rather than deferred to an untracked follow-up. |
| Business logic depends only inward? | Not applicable. This is test-support code and no production module names it. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One wrapper in `tests/scan_fixture/` | A wrapper reachable from `src/` too, via a `#[path]` include across the `src/` and `tests/` boundary, or a feature-gated `pub mod test_support` on the library | All 11 duplicated decode loops sit under `tests/`. Both cross-boundary options add a permanent structural cost for two in-crate call sites that duplicate nothing. |
| Capture keeps per-call grouping | A flat `Vec<RecordBatch>` | A flat view cannot serve `scan_telemetry.rs`, which counts `emit_record_batch_ipc` calls, because one call may decode to several batches. |
| `DefaultsCtx` replaces `NoopCtx` | `TestContext` for every adapter double | `TestContext` overrides `node_count`, so the `0` to `1` fallback test would assert the double instead of the trait default and would keep passing after the default changed. |
| Retain 2 in-crate doubles | Migrate all 21 | They are unreachable from `tests/scan_fixture/` and duplicate no decode. `SinkCtx` also cannot be `DefaultsCtx`, which has no `emit_record_batch_ipc`. |
| Bump `exasol-udf-macros` with the SDK | Bump the SDK alone | Both crates are pinned together in `[workspace.dependencies]` and share the release train. A split pin risks a fingerprint the SLC rejects. |
| No spec delta for the SDK-to-SLC lockstep | Add a lockstep scenario to `packaging/single-so-two-entry-points` | This plan changes the pinned version value, not the lockstep behavior, which `Makefile` and the E2E harness already derive automatically. Adding coverage here is scope creep beyond issue #383 item 1. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| scan-module-structure | CHANGED | `datafusion-scan/scan-module-structure/spec.md` |
| adapter-module-structure | CHANGED | `vs-adapter/adapter-module-structure/spec.md` |

## Impact

Test-authoring cost drops: a new scan test constructs `BatchCapturingCtx` instead of writing a `UdfContext` impl. No production behavior changes, and no test assertion or expected value changes.

One operator-facing consequence, not breaking but ordered. The `.so` SDK fingerprint is `{exasol-udf-sdk version}:{rustc_hash}`, checked at UDF load. Bumping the pin to 0.24.0 changes it, so an Exasol instance still carrying the 0.23.0 SLC rejects the new `.so` until its SLC is reinstalled. Both automated paths already follow the pin: `Makefile` derives `SLC_VERSION` from the root `Cargo.toml` pin, and `tests/common/e2e_harness.rs` derives its own `SLC_VERSION` const from the SDK fingerprint at compile time. A manually provisioned environment, such as staging, needs an explicit SLC reinstall before its next `.so` upload.

## Dependencies

| Artifact | From | To | Verified |
|----------|------|----|----------|
| `exasol-udf-sdk` | 0.23.0 | 0.24.0 | Published on crates.io |
| `exasol-udf-macros` | 0.23.0 | 0.24.0 | Published on crates.io |
| `cargo-exasol-udf` | 0.23.0 | 0.24.0 | Published on crates.io, installed by `make cross-udf-build` at `=$(SLC_VERSION)` |
| `lc-rust` SLC rootfs | 0.23.0 | 0.24.0 | `lc-rust-0.24.0.tar.gz` and `lc-rust-0.24.0-aarch64.tar.gz` present on the `v0.24.0` release |

`exasol_udf_sdk::context` is unchanged between 0.23.1 and 0.24.0. The only production-surface change is `UdfError` gaining `Clone`, which `EmitPolicy::Reject` and `NextPolicy::Reject` require. The bump adds no required trait method, so the two retained hand-rolled impls keep compiling. The builder image stays `rust:1.94-trixie`, matching the SLC toolchain channel 1.94.

## Migration

| Current | New |
|---------|-----|
| 11 per-file `UdfContext` doubles under `tests/` | 1 `BatchCapturingCtx` in `tests/scan_fixture/` |
| `StubConnections` in `src/scan/storage_ref_tests.rs` | `TestContext` with `with_connection`, `with_script_schema`, `with_script_name`, `EmitPolicy::Reject` |
| `NoopCtx` in `src/adapter/adapter_tests.rs` | `DefaultsCtx` |
| `ConnResolvingCtx`, `ClosedPortConnCtx`, `PasswordCtx`, both `StubCtx`, `UnityConnCtx` | `TestContext` with `with_connection` or `with_node_count` |
| `CapturingCtx` and `SinkCtx` in `src/scan/` | Retained unchanged, reason recorded in the scan spec delta |

## Implementation Tasks

### 1. SDK 0.24.0 dependency bump

- [ ] 1.1 Set `exasol-udf-sdk` and `exasol-udf-macros` to `0.24.0` in the root `Cargo.toml` `[workspace.dependencies]`. Keep `features = ["emit-arrow"]` on `exasol-udf-sdk` and do NOT add `test-support` there.
- [ ] 1.2 Add `exasol-udf-sdk = { workspace = true, features = ["test-support"] }` to `[dev-dependencies]` in `crates/lakehouse-engine/Cargo.toml`. This one entry serves both the `src/**/*_tests.rs` unit tests and the `tests/` integration targets, because cargo unifies dev-dependency features into the crate under test. The SDK gates `test_support` on `#[cfg(any(test, feature = "test-support"))]`, whose `test` arm applies only to the SDK's own build, so the feature is required and not optional for unit tests. Leave `crates/lakehouse-catalog` and `crates/vs-expression` unchanged, because neither declares a `UdfContext` impl.
- [ ] 1.3 Check that `make -n install-slc` reports `lc-rust-0.24.0`, confirming the `Makefile` `SLC_VERSION` sed still matches the edited pin line.
- [ ] 1.4 Run `cargo test -p lakehouse-engine` before any test-double edit, to prove the bump alone leaves all 21 existing doubles compiling and passing.

### 2. Shared scan-path double

- [ ] 2.1 Add `BatchCapturingCtx` to `tests/scan_fixture/`, wrapping `TestContext`, delegating every declared `UdfContext` method to the inner value, and overriding `emit_record_batch_ipc` to decode with `arrow::ipc::reader::StreamReader`. Retain per-call grouping and expose a flattened batch view, an `emit_record_batch_ipc` call count, and a total row count. Write the doc comment to state the `emit_record_batch_ipc` gap that justifies the wrapper. [expert]
- [ ] 2.2 Migrate the 7 uniform raw-scan tests onto `BatchCapturingCtx`: `scan_column_binding.rs`, `scan_deletion_vectors.rs`, `scan_footer_refetch_observable.rs`, `scan_name_mapping.rs`, `scan_no_head_test.rs`, `scan_partition_values.rs`, `scan_positional_deletes.rs`. Each served zero columns and answered every `get_string` with `Ok(None)`, so supply `Value::Null` columns and rely on the trait default mapping. Supply one column per index the test's production path reads, because `TestContext::get` errors past the supplied row where the deleted doubles answered any index. Delete each struct and impl.
- [ ] 2.3 Migrate `scan_join_test.rs`, whose double serves two `Value` columns through `get` rather than `get_string`, including its `with_spec_args` constructor over `to_common_json` and `files_json`. Keep the `join path must use emit_batch` rejection as `EmitPolicy::Reject`.
- [ ] 2.4 Migrate `scan_two_arg.rs`. Keep `read_scan_spec` driven over both arguments and keep the NULL case for each argument independently, mapping a `None` column to `Value::Null`. [expert]
- [ ] 2.5 Migrate `scan_batch_loop.rs`. Its `next` errors today to prove the scalar `run()` path never calls it, so construct through `TestContext::scalar` with `NextPolicy::Reject` carrying the same message. [expert]
- [ ] 2.6 Migrate `scan_telemetry.rs`, including its fully-qualified `impl exasol_udf_sdk::context::UdfContext for FakeCtx`. Map `emitted_batches` to the call count and `emitted_rows` to the row total, and set the configurable level through `with_debug_level`. [expert]
- [ ] 2.7 Migrate `src/scan/storage_ref_tests.rs`'s `StubConnections` to `TestContext`. Register each CONNECTION with `with_connection`, the script identity with `with_script_schema` and `with_script_name`, and the `emit` rejection with `EmitPolicy::Reject`. Check all 8 call sites, including the 3 `none()` cases whose surfaced error text `connection_password` builds. [expert]
- [ ] 2.8 Add a gate scoped to group B's territory. It MUST assert no `impl UdfContext` under `crates/lakehouse-engine/tests/` and exactly 2 under `crates/lakehouse-engine/src/scan/`, at `emit_tests.rs` and `test_support_tests.rs`. Match both the bare and the fully-qualified spelling, because `scan_telemetry.rs` uses the qualified form today. The crate-wide "exactly 2 total" assertion runs post-implementation in the Manual Testing table, after both groups complete.

### 3. Adapter-side SDK doubles

- [ ] 3.1 Replace `NoopCtx` with `DefaultsCtx` at all 7 references in `src/adapter/adapter_tests.rs`. Use `DefaultsCtx`, not `TestContext`, so `cluster_nodes_from_context_defaults_to_one_when_node_count_zero` keeps asserting the trait default `node_count` of 0.
- [ ] 3.2 Replace `adapter_tests.rs`'s `StubCtx` with `TestContext` plus `with_node_count`, keeping the `0` and `4` inputs.
- [ ] 3.3 Replace `ConnResolvingCtx`, `ClosedPortConnCtx`, and `PasswordCtx` with `TestContext` plus `with_connection`, registering each `ConnectionObject` under the CONNECTION name its call sites request, which is `MY_CONN` for the dispatch and `resolve_connection_config` tests and `keep_me` where the persisted-property test names it.
- [ ] 3.4 Replace `connection_tests.rs`'s `StubCtx` across its 51 references. Map `with_conn` to `with_connection` and `no_conn` to a `TestContext` with no registered CONNECTION.
- [ ] 3.5 Replace `unity_schema_tests.rs`'s `UnityConnCtx` with `TestContext` plus `with_connection` under `uc_conn`, keeping the empty-object no-auth password and the personal-access-token case.
- [ ] 3.6 Add a gate proving no `impl UdfContext` remains under `crates/lakehouse-engine/src/adapter/`, matching both forms.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: SDK 0.24.0 pin | 1.1-1.4 | — | `Cargo.toml`, `crates/lakehouse-engine/Cargo.toml`, `Makefile` `SLC_VERSION` |
| B: scan-path doubles | 2.1-2.8 | A (needs the `test-support` dev-dependency) | spec delta `datafusion-scan/scan-module-structure`; `crates/lakehouse-engine/tests/scan_fixture/`, the 11 `tests/scan_*.rs` named in tasks 2.2-2.6, `src/scan/storage_ref_tests.rs` |
| C: adapter doubles | 3.1-3.6 | A (needs the `test-support` dev-dependency) | spec delta `vs-adapter/adapter-module-structure`; `src/adapter/adapter_tests.rs`, `src/adapter/connection_tests.rs`, `src/adapter/unity_schema_tests.rs` |

- Group A is a manifest-only change and gates both migrations, so it runs alone first.
- B and C share no spec delta and no source file, so they run in parallel after A.
- B carries `[expert]` tasks, so the whole group routes to the expert implementer. The wrapper in 2.1 gates every migration in the group, and 4 of the 7 migrations carry a behavior the double encodes rather than a shape it repeats.
- C is untagged. Its one correctness trap, the `DefaultsCtx` choice in 3.1, is stated as a task clause and pinned normatively in the spec delta.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Struct + impl | `tests/scan_batch_loop.rs` `RowCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_column_binding.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_deletion_vectors.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_footer_refetch_observable.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_join_test.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_name_mapping.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_no_head_test.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_partition_values.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_positional_deletes.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_telemetry.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `tests/scan_two_arg.rs` `FakeCtx` | Replaced by `BatchCapturingCtx` |
| Struct + impl | `src/scan/storage_ref_tests.rs` `StubConnections` | Replaced by `TestContext` |
| Struct + impl | `src/adapter/adapter_tests.rs` `ConnResolvingCtx`, `NoopCtx`, `ClosedPortConnCtx`, `StubCtx`, `PasswordCtx` | Replaced by `TestContext` or `DefaultsCtx` |
| Struct + impl | `src/adapter/connection_tests.rs` `StubCtx` | Replaced by `TestContext` |
| Struct + impl | `src/adapter/unity_schema_tests.rs` `UnityConnCtx` | Replaced by `TestContext` |

Issue #383 item 2, a `Default` derive on `ConnectionCreds`, generates no task. Commit f1c1954 already added `#[derive(Clone, Default)]`.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One shared double owns every scan test's Arrow IPC capture | Integration | `crates/lakehouse-engine/tests/scan_two_arg.rs` | `emitted_rows_match_the_single_argument_path` (existing, unedited assertions, run against `BatchCapturingCtx`) |
| One shared double owns every scan test's Arrow IPC capture | Integration | `crates/lakehouse-engine/tests/scan_telemetry.rs` | existing payload-count and row-count tests, proving the per-call grouping survives |
| One shared double owns every scan test's Arrow IPC capture | Integration | `crates/lakehouse-engine/tests/scan_batch_loop.rs` | existing scalar-path test, proving `NextPolicy::Reject` keeps the rejecting `next` |
| One shared double owns every scan test's Arrow IPC capture | Integration | new surface probe under `crates/lakehouse-engine/tests/` | `no_hand_rolled_udf_context_in_integration_tests` (task 2.8) |
| The scan submodule connection double resolves through the SDK context | Unit | `crates/lakehouse-engine/src/scan/storage_ref_tests.rs` | existing missing-connection, script-identity, and redaction tests, unedited |
| Two in-crate doubles are retained for a stated reason | Integration | new surface probe under `crates/lakehouse-engine/tests/` | `no_hand_rolled_udf_context_in_integration_tests` (task 2.8), asserting no impls under `tests/` and exactly 2 under `src/scan/`; the crate-wide "exactly 2 total" check is in Manual Testing |
| Two in-crate doubles are retained for a stated reason | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `emits_batch_by_batch_without_materializing` (unedited, proving `CapturingCtx` still compiles under 0.24.0) |
| SDK-provided doubles replace every hand-rolled adapter context | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `read_connection_parses_uri_and_creds` and the remaining suite, unedited |
| SDK-provided doubles replace every hand-rolled adapter context | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `set_properties_null_unset_required_property_errors_not_panic`, `create_virtual_schema_rejects_old_namespace_alias_without_replacement` |
| SDK-provided doubles replace every hand-rolled adapter context | Unit | new surface probe under `crates/lakehouse-engine/src/adapter/` | `no_hand_rolled_udf_context_in_adapter_tests` (task 3.6) |
| The node-count fallback test keeps asserting the trait default | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `cluster_nodes_from_context_defaults_to_one_when_node_count_zero`, `cluster_nodes_from_context_passes_through_reported_node_count` |

Every scenario is verified by an existing test whose assertions do not change. That is the gate this plan rests on: a migration that altered behavior would fail a test rather than pass silently.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-module-structure | `grep -rn 'UdfContext for' crates/lakehouse-engine/tests/` | No output |
| scan-module-structure | `cargo test -p lakehouse-engine --test scan_telemetry --test scan_two_arg --test scan_batch_loop` | 0 failures |
| adapter-module-structure | `grep -rn 'UdfContext for' crates/lakehouse-engine/src/adapter/` | No output |
| adapter-module-structure | `cargo test -p lakehouse-engine adapter::` | 0 failures |
| Both (residual census) | `grep -rn 'UdfContext for' crates/` | Exactly 2 lines, in `src/scan/emit_tests.rs` and `src/scan/test_support_tests.rs` |
| Both (SLC lockstep) | `make -n install-slc \| grep lc-rust` | Names `lc-rust-0.24.0.tar.gz` |
| Both (production build excludes the feature) | `cargo tree -p lakehouse-engine -e features -i exasol-udf-sdk` | No `test-support` feature on the normal dependency edge |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0, `cargo exasol-udf validate` passes against the 0.24.0 toolchain |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures. Start the docker stack first, because the target does not. The harness installs `lc-rust-0.24.0` in-process from its compile-time SLC const. |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Format | `cargo fmt` | No changes |

The implementing commit closes the tracking issue. Use `Closes #383` in its footer, per the repository feature-tracking rule.
