# Code Review Findings: refactor-storage-backend-enum

## Summary
- Files reviewed: 45
- Total findings: 7 (standard: 4, expert: 3)

Verified clean (no findings raised — recorded so the next pass does not re-derive them):

- `StorageBackend::S3(StorageProps)` wraps rather than inlines; `StorageProps`' `Default`, `secret_values`, serde field contract and its own tests are untouched. `storage_props_wire_encoding_unchanged` passes unedited.
- Externally tagged, lowercase key confirmed by `s3_serializes_under_a_lowercase_externally_tagged_variant_key` and, from the decode side, `only_the_lowercase_s3_variant_key_decodes` (rejects bare/untagged, `S3`, and `azure`).
- `catalog_storage_props` is `pub(crate)`; `crates/lakehouse-catalog/tests/catalog_public_surface.rs` references only `secret_values` and `file_io`.
- `build_spec_size_index` is byte-for-byte unedited; its ONE whole-spec result is passed to both `register_side_store` calls (`object_store.rs:59,66,84`). `shared_bucket_join_store_answers_both_sides_sizes_from_the_spec` pins the whole-spec scope. The `!join.files.is_empty()` guard (`object_store.rs:76`) and the `s3_max_connections` budget survive verbatim.
- `extract_bucket`, `build_s3_store`, `build_s3_file_io` are gone from all code; the only surviving occurrences are the intended negative assertion string in `catalog_public_surface.rs:57` and one stale comment (finding 5 below).
- `StorageBackend::S3` is named only at the permitted production sites (`storage.rs`, `vended.rs:37,39`, `object_store.rs:115`, `connection.rs:175`, `scan/spec.rs:693`) plus `#[cfg(test)]` code.
- All 10 `dispatch_golden` fixtures differ from `HEAD` ONLY inside their `storage` value (verified by normalizing the storage blob and diffing the remainder byte-for-byte); same for the three join golden strings.
- `build_s3_file_io` → `catalog_storage_props` collapse is key-for-key and condition-for-condition identical, including the `Some("")` session-token presence gate, which `catalog_storage_props_emits_a_present_but_empty_session_token` pins.
- Registry-key skip is behaviour-equivalent to the old `dim_bucket != bucket` comparison: the context is freshly built, both sides share one backend and one size index, so the only reachable collision is the shared-bucket case.
- Gates re-run this review: `cargo test --workspace` → exit 0, 917 passed, 0 failed across 40 binaries; `cargo clippy --workspace --all-targets -- -D warnings` → clean; `cargo fmt --check` → clean. No `#[allow]`/`#[expect]`, no `#[ignore]`, no TODO/FIXME added by the diff.

Deliberately NOT raised — settled by the adversarially-reviewed spec delta, not open questions:

- `register_side_store`'s `Result<Option<Url>, UdfError>` return is a Command-Query mix whose value no production call site consumes (`object_store.rs:60,78` both discard it). `vs-adapter/storage-backend-enum/spec.md:59` normatively requires that return shape and states the reason. Not re-litigated here.
- `build_s3_store_applies_spec_connection_budget` names a deleted function and sets `s3_max_connections = 16` without any assertion sensitive to it. `vs-adapter/storage-backend-enum/spec.md:63` pins both the name and the weakened assertion, and `review/round-1.md:86` already deliberated exactly this. Not re-litigated here.

## Standard fixes

### crates/lakehouse-engine/src/adapter/connection.rs

#### [MISSING_DESIGN_INTENT] `storage_block`'s doc restates the signature and says nothing about the new parameter
- Location: line 173
- Issue: `storage_block` is `pub` and its whole reason for changing this slice is the added `allow_http` parameter, yet the doc comment reads only "Build a `StorageBackend` from resolved credentials." A caller cannot tell from it why `allow_http` arrives as a parameter rather than out of `ConnectionCreds` like every other field — it comes from the adapter's `PROP_ALLOW_HTTP` property, read in `resolve_connection_config` (`adapter/mod.rs:187-190`), and it is a parameter specifically so no caller has to patch the constructed payload afterwards. That rationale is recorded in `plan.md` and nowhere in the code.
- Fix: In crates/lakehouse-engine/src/adapter/connection.rs, extend `storage_block`'s doc comment (line 173) to state that `allow_http` is a parameter rather than a `ConnectionCreds` field because it originates from the adapter's `PROP_ALLOW_HTTP` property, and that taking it here keeps callers from mutating the constructed `StorageBackend` payload to finish building it. Do not change the signature or the body.

### crates/lakehouse-catalog/tests/catalog_public_surface.rs

#### [VAGUE_TEST_NAME] Test name no longer covers the assertions it makes
- Location: line 51
- Issue: `vended_mechanism_functions_are_not_declared_public` now also asserts the absence of `pub fn build_s3_file_io`, which is not a vended mechanism function at all — it is the deleted `FileIO` builder replaced by `StorageBackend::file_io`. The name states a narrower condition than the loop checks, so a reader hunting the guard against `build_s3_file_io` reappearing has no reason to look inside this test; only the doc comment reveals it.
- Fix: In crates/lakehouse-catalog/tests/catalog_public_surface.rs, rename the test at line 51 from `vended_mechanism_functions_are_not_declared_public` to `demoted_and_deleted_functions_are_not_declared_public` and update the assertion message and doc comment wording to say "demoted or deleted" rather than "vended mechanism" where they name the category. Leave the four-item list and the loop body unchanged.

### crates/lakehouse-engine/tests/common/e2e_harness.rs

#### [OUTDATED_COMMENT] Doc comment names the wrong return type
- Location: line 336
- Issue: the doc reads "`StorageProps` for the host-visible local Docker stack." but `local_stack_storage` (line 337) now returns `StorageBackend`. The comment names a type the function no longer produces.
- Fix: In crates/lakehouse-engine/tests/common/e2e_harness.rs line 336, change the doc comment to read "`StorageBackend` for the host-visible local Docker stack."

### crates/lakehouse-engine/tests/e2e_int96_timestamp_test.rs

#### [OUTDATED_COMMENT] Comment points at a function this change deleted, in a module that never held it
- Location: line 121
- Issue: `fetch_object_bytes`' doc says it reads "through the SAME `object_store` S3 client the scan UDF uses (`build_s3_store` in `scan/mod.rs`)". `build_s3_store` was deleted by this refactor — its body is now the `StorageBackend::S3` arm of `register_side_store` — and it lived in `scan/object_store.rs`, not `scan/mod.rs`, so the reference is wrong on both counts. The file was edited by this change (line 135 gained the `StorageBackend::S3` destructure directly below this comment), so the stale pointer is in the diff's blast radius.
- Fix: In crates/lakehouse-engine/tests/e2e_int96_timestamp_test.rs line 121, change the parenthetical from "(`build_s3_store` in `scan/mod.rs`)" to "(the `StorageBackend::S3` arm of `register_side_store` in `scan/object_store.rs`)".

## Expert fixes

### crates/lakehouse-engine/src/scan/object_store.rs

#### [TOO_MANY_ARGUMENTS] `register_side_store` takes six parameters
- Location: line 106
- Issue: `register_side_store(ctx, backend, files, table_root, s3_max_connections, sizes)` takes six arguments against the ≤3 guardrail — one worse than the five-argument `register_bucket_store` it replaces. The parameters are not six independent things: `files` + `table_root` are one concept the code already names in prose throughout this module ("one side of a scan", "that side's own file list", "the fact side", "the dimension side") but has no type for, and `backend` + `s3_max_connections` + `sizes` are three whole-spec values that both call sites (lines 60-67 and 78-85) pass identically. The flat list is also what makes the `sizes` mis-scoping hazard live: with `files` sitting beside `sizes` in one undifferentiated parameter list, nothing in the signature says one is per-side and the other is whole-spec.
- Fix: In crates/lakehouse-engine/src/scan/object_store.rs, introduce two private borrowed structs above `register_side_store`: `ScanSide<'a> { files: &'a [FileEntry], table_root: &'a str }` and `StoreRegistration<'a> { backend: &'a StorageBackend, connection_budget: usize, sizes: &'a HashMap<ObjectStorePath, u64> }`, each with a doc comment stating that `ScanSide` is per-side and `StoreRegistration`'s `sizes` is the WHOLE spec's index shared by every side. Change the signature to `fn register_side_store(ctx: &SessionContext, registration: &StoreRegistration<'_>, side: ScanSide<'_>) -> Result<Option<Url>, UdfError>` and update the two call sites in `build_session_context` to build one `StoreRegistration` before the fact-side call and reuse it for the dimension-side call. Keep the returned `Result<Option<Url>, UdfError>`, the `get_store` early return, the `!join.files.is_empty()` guard, and the single `build_spec_size_index(spec)?` call feeding BOTH registrations exactly as they are. `build_spec_size_index` must stay unedited and `sizes` must NEVER be derived from `ScanSide::files`. Update the `register_side` test helper (line 433) to the new shape and re-run `cargo test -p lakehouse-engine --lib scan::object_store` — `shared_bucket_join_store_answers_both_sides_sizes_from_the_spec` is the only test that fails if `sizes` is narrowed to the side, so it must pass.

#### [IMPLEMENTATION_IN_NAME] Backend-agnostic registrar takes an S3-named parameter
- Location: line 111
- Issue: `register_side_store` exists specifically so that no caller names the storage backend — its own doc says "deriving a store key is a backend-specific decision, not a shared one" — yet its parameter is `s3_max_connections: usize`, above the `match backend` that is the whole point of the function. When slice C adds an Azure arm, that arm receives a parameter named for S3. The value's own home one level down already avoids this: `client_options_for(budget: usize)` (line 183) calls it `budget`, and its doc calls it "the resolved connection-concurrency budget". The wire field `CommonScanSpec.s3_max_connections` must keep its name (the goldens pin it); only this parameter is the leak.
- Fix: In crates/lakehouse-engine/src/scan/object_store.rs, name the field `connection_budget` (not `s3_max_connections`) when the `[TOO_MANY_ARGUMENTS]` fix moves this parameter onto `StoreRegistration`, and pass `spec.common.s3_max_connections` into it at both construction sites. Apply this in the same edit as that fix — do not touch `CommonScanSpec.s3_max_connections`, `resolve_s3_max_connections`, `NOTE_S3_MAX_CONNECTIONS`, or any golden fixture.

#### [OUTDATED_COMMENT] Module and entry-point docs still describe a single MinIO store
- Location: line 1
- Issue: the module doc (lines 1-3) says the module "builds the S3/MinIO object store (size-indexed HEAD wrapper), registers it on the session runtime", and `build_session_context`'s doc (line 27) says "Build a DataFusion SessionContext with the MinIO object store registered." Both describe the pre-refactor shape: one store, named for one vendor. `build_session_context` is now the backend-agnostic orchestration level — it dispatches through `register_side_store` and registers up to two stores, one per join side, and MinIO is one deployment of the S3 arm rather than the thing being built. The whole point of the slice is that this level names no backend, and its own docs still do.
- Fix: In crates/lakehouse-engine/src/scan/object_store.rs, rewrite the module doc (lines 1-3) to say the module registers the object store each scan side reads through — dispatching on the scan spec's `StorageBackend`, wrapping each store in the spec-sized HEAD decorator — and constructs the memory-pool-sized `SessionContext`. Rewrite `build_session_context`'s first doc line (line 27) to say it builds a DataFusion `SessionContext` with an object store registered per scan side. Keep the existing `memory_limit_bytes` sentinel paragraph (lines 29-31) unchanged, and keep every S3-specific comment inside the `StorageBackend::S3` arm as it is — those correctly name S3.
