# Tasks: change-bounded-remote-scans

## Phase 2: Implementation (Group 0 — prerequisite: SDK bump)
- [x] 1.1 Bump exasol-udf-sdk→0.18.0 + add emit-arrow feature (root + crate Cargo.toml); bump exasol-udf-macros→0.18.0; cargo update; cargo test --no-run
- [x] 1.2 Confirm EmitBatch trait import path + emit_batch(&RecordBatch) signature against 0.18.0 source; record for task 2

## Phase 2: Implementation (Group A — after Group 0; disjoint files, concurrent)
- [x] 3.1 Classify ResourcesExhausted in redact_storage_error; surface clean memory-exhaustion error, keep credential redaction [expert]
- [x] 3.2 Unit test: ResourcesExhausted surfaces as memory error (not storage), no credentials
- [x] 4.1 Add df_batch_size field to ScanSpec (scan/spec.rs) following df_target_partitions round-trip + default pattern
- [x] 4.2 Set batch_size in session_config_for_spec from spec value (default when absent, clamp ≥1); both raw + partial-agg paths
- [x] 4.3 Unit tests: df_batch_size round-trips + defaults on legacy; session_config applies batch size + clamps sub-1 to 1
- [x] 5.1 bench/run.sh remote: replace VS_EXTRA_PROPS="" with NR_OF_CORES + PARALLELISM_FACTOR block from BENCH_* env; factor shared printf helper

## Phase 2: Implementation (Group B — after Group A; depends on 1.2)
- [x] 2.1 Replace batch_to_rows + row-by-row emit in scan/emit.rs::emit_stream with emit_batch(&batch) per batch; fetch/emit/drop one at a time; remove Vec<Value> intermediate on raw path [expert]
- [x] 2.2 Update emit_stream unit test for IPC batch emit + no-Vec<Value> invariant; repoint/remove dead batch_to_rows on raw path

## Phase 2: Implementation (Group C — tracking + docs, mechanical)
- [x] 6.1 Open GitHub issue (gh issue create); reference in implementing commit (Closes #n)
- [x] 6.2 Update CLAUDE.md SDK-version + emit-buffering notes for emit_batch guidance

## Phase 4: Code Review
- [x] 4.R Review all changed files (code-reviewer)

## Phase 4b: Review Fixes
- [x] R1 [blocker][expert] Persist df_batch_size in adapterNotes: add PROP_DF_BATCH_SIZE + resolve_df_batch_size + write NOTE_DF_BATCH_SIZE in build_adapter_notes/handle_create_virtual_schema (mirror df_target_partitions); round-trip test
- [x] R2 [blocker][expert] Use classify_scan_error at the 5 partial-aggregate error sites in scan/mod.rs; add test that ResourcesExhausted on grouped path surfaces as memory error
- [x] R3 Mark batch_to_rows #[cfg(test)] (no production callers post-emit_batch)
- [x] R5 Comment resources_exhausted_error: only innermost msg surfaced, context dropped (credential risk)
- [x] R6 Remove redundant inline comment at emit.rs drop(batch); .env.example remote cores note

## Phase 4c: E2E-surfaced fix
- [x] R7 [blocker][expert] emit_batch rejects Utf8View (DataFusion 58 Parquet strings). Normalize view types (Utf8View→Utf8, BinaryView→Binary, etc.) before emit_batch so the emit path never VM-crashes on a view type; E2E green
- [x] R8 Bump SLC_VERSION 0.16.0→0.18.0 in both e2e_*_test.rs to match SDK ABI + version bump 0.11.0→0.12.0

## Phase 5: Verification
- [x] 5.A cargo test (host unit) — 287 passed, 0 failures
- [x] 5.B cargo clippy --all-targets — 0 warnings; cargo fmt — no changes
- [x] 5.C make cross-musl-udf-build — exit 0; make test-e2e — 7/7 scan + 27/27 capability pass
