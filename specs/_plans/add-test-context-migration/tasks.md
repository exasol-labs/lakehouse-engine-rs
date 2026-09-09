# Tasks: add-test-context-migration

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: SDK 0.24.0 pin)
- [x] 1.1 Bump exasol-udf-sdk and exasol-udf-macros to 0.24.0 in root Cargo.toml [workspace.dependencies], keep features = ["emit-arrow"]
- [x] 1.2 Add exasol-udf-sdk dev-dependency with feature "test-support" to crates/lakehouse-engine/Cargo.toml
- [x] 1.3 Check `make -n install-slc` reports lc-rust-0.24.0
- [x] 1.4 Run `cargo test -p lakehouse-engine` before any test-double edit to confirm the bump alone is green

## Phase 2: Implementation (Group B: scan-path doubles)
- [x] 2.1 Add BatchCapturingCtx to tests/scan_fixture/, wrapping TestContext, overriding emit_record_batch_ipc [expert]
- [x] 2.2 Migrate 7 uniform raw-scan tests onto BatchCapturingCtx (scan_column_binding, scan_deletion_vectors, scan_footer_refetch_observable, scan_name_mapping, scan_no_head_test, scan_partition_values, scan_positional_deletes)
- [x] 2.3 Migrate scan_join_test.rs (two-column get, with_spec_args, EmitPolicy::Reject)
- [x] 2.4 Migrate scan_two_arg.rs [expert]
- [x] 2.5 Migrate scan_batch_loop.rs with NextPolicy::Reject [expert]
- [x] 2.6 Migrate scan_telemetry.rs incl. fully-qualified impl [expert]
- [x] 2.7 Migrate src/scan/storage_ref_tests.rs StubConnections to TestContext [expert]
- [x] 2.8 Add surface-probe gate: no impl UdfContext under tests/, exactly 2 under src/scan/

## Phase 2: Implementation (Group C: adapter doubles)
- [x] 3.1 Replace NoopCtx with DefaultsCtx at 7 references in src/adapter/adapter_tests.rs
- [x] 3.2 Replace StubCtx with TestContext + with_node_count in adapter_tests.rs
- [x] 3.3 Replace ConnResolvingCtx, ClosedPortConnCtx, PasswordCtx with TestContext + with_connection
- [x] 3.4 Replace connection_tests.rs StubCtx across 51 references
- [x] 3.5 Replace unity_schema_tests.rs UnityConnCtx with TestContext + with_connection
- [x] 3.6 Add surface-probe gate: no impl UdfContext under src/adapter/

## Phase 3: Verification
- [x] 4.1 Automated checks: build, test, lint, format
- [x] 4.2 Scenario coverage audit
- [x] 4.3 Manual verification (grep census, cargo tree feature check)

## Phase 4: Review Fixes
- [x] 4.4 In crates/lakehouse-engine/src/adapter/adapter_tests.rs, refactor `collect_rust_files` inside `no_hand_rolled_udf_context_in_adapter_tests` to return `Vec<std::path::PathBuf>` directly: move the `Vec::new()` into the function body, extend from recursive calls with `out.extend(collect_rust_files(&path))`, and return `out`. Update the call site to `let files = collect_rust_files(&adapter_dir);`.
