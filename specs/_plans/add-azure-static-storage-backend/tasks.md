# Tasks: add-azure-static-storage-backend

## Group A
- [x] 1.1 Add `AdlsCred` and `StorageBackend::Adls { account_name, cred }` in `crates/lakehouse-catalog/src/storage.rs`; add Azure arms of `secret_values`, `catalog_storage_props`, `file_io`
- [x] 1.2 Add `account_name`, `account_key`, `sas_token` to `ConnectionCreds` in `crates/lakehouse-catalog/src/creds.rs`; extend redacting `Debug` impl
- [x] 1.5 Extend `redact_credentials` in `crates/lakehouse-catalog/src/redaction.rs` with Azure labels + unit test

## Group A.2 (needs Group A)
- [x] 1.4 Complete five forced-edit sites for the new `StorageBackend` variant (§ Forced Edits table A)
- [x] 1.7 Complete seven forced-edit sites for the new `ConnectionCreds` fields (§ Forced Edits table B)

## Group C (needs Group A, parallel with A.2) [expert]
- [x] 3.1 Replace `extract_bucket_from_files` with `side_store_url` in `crates/lakehouse-engine/src/scan/object_store.rs` [expert]
- [x] 3.2 Add `validate_sides_share_one_store(spec)` beside `build_spec_size_index`, called once from `build_session_context` [expert]

## Group B (needs Group A.2)
- [x] 1.3 Add compile-forced `Adls` passthrough arm to `resolve_vended_storage` in `crates/lakehouse-catalog/src/vended.rs`
- [x] 2.1 Read `account_name`/`account_key`/`sas_token` in `parse_creds` (`crates/lakehouse-engine/src/adapter/connection.rs`)
- [x] 2.2 Add two Azure rules to `validate_creds` (ambiguity + required-field), update rule-precedence comment [expert]
- [x] 2.3 Add Azure branch to `storage_block`, keeping it TOTAL
- [x] 1.6 Route nine credential-exposed error sites in `file_resolution.rs` through value-based redaction [expert]

## Group D (needs Group B + Group C)
- [x] 3.3 Add Azure arm of `register_side_store` (also added 4 of the 5 named `register_side_store_*` tests — see 4.3 note)

## Group E (needs Group D)
- [x] 4.1 Credential-shape unit tests in `connection.rs` (written by Group B's 2.x agent under TDD: `account_key_creds_select_the_adls_backend`, `sas_token_creds_select_the_adls_backend`, `azure_creds_require_account_name_and_exactly_one_credential`, `mixed_azure_and_s3_credential_fields_are_rejected`, `absent_optional_fields_default_and_still_select_s3`, plus `storage_block_falls_through_to_s3_for_an_unvalidated_azure_shape` — do NOT re-add)
- [x] 4.2 `abfss://` path unit tests in `file_resolution.rs` + size-index test in `object_store.rs`
- [x] 4.3 Registration unit tests in `object_store.rs` (4 already written under TDD by Group C/3.3: `side_store_url_returns_the_same_url_for_s3_as_the_deleted_bucket_derivation`, `side_store_url_preserves_the_s3a_scheme_so_the_key_matches_the_lookup`, `validate_sides_share_one_store_rejects_two_containers_in_one_account`, `validate_sides_share_one_store_accepts_every_s3_spec_shape`, `register_side_store_registers_an_adls_store_under_the_container_qualified_url`, `register_side_store_skips_a_second_side_in_the_same_container`, `register_side_store_registers_both_sides_in_different_accounts`, `register_side_store_surfaces_an_unrecognised_azure_host_redacted` — all present, verified via grep; do NOT re-add. The size-index test `spec_size_index_keys_an_abfss_file_without_its_container` is NOT among these — it belongs to 4.2)
- [x] 4.4 Backend-type unit tests in `storage.rs`

## Group F (needs Group E)
- [x] 4.5 Run full S3 regression gate: `cargo test --workspace`, clippy, fmt, container build, S3 E2E suite

## Phase 4: Review Fixes
- [x] 4.1 [MISSING_BOUNDARY_TEST] Add `AdlsCred` boundary-test coverage to `shared_type_reexports.rs` (engine) and `catalog_public_surface.rs` (catalog) so a narrowed or removed `AdlsCred` re-export fails the build
- [x] 4.2 [INFORMATION_LEAKAGE] Add `#[serde(rename_all = "snake_case")]` to `AdlsCred` in `crates/lakehouse-catalog/src/storage.rs` so its inner variant key matches `StorageBackend`'s lowercase wire convention; update the pinned serialization test literal and add a wrong-case-inner-key rejection payload
- [x] 4.3 [MISSING_DESIGN_INTENT] Document on `StorageBackend::Adls::account_name` (`crates/lakehouse-catalog/src/storage.rs`) that it feeds only the iceberg `FileIO` manifest-read path via `catalog_storage_props`, while the DataFusion scan path derives the account from the file URI host instead
- [x] 4.4 [INFORMATION_LEAKAGE] Give the value-then-label redaction order one owner: add `redact_error_text` (+ order-pinning unit test) to `crates/lakehouse-catalog/src/redaction.rs`, export it from the catalog crate, and replace the private copy in `file_resolution.rs` and both inline compositions in `scan/object_store.rs` with it [expert]
- [x] 4.5 [VAGUE_TEST_NAME] Rename `register_side_store_registers_an_adls_store_under_the_container_qualified_url` to `register_side_store_returns_the_container_qualified_url_but_the_registry_key_drops_the_container` in `crates/lakehouse-engine/src/scan/object_store.rs`; rewrite its docstring to cite `get_url_key` (`datafusion-execution-54.1.0/src/object_store.rs:268-274`) and add a third assertion proving `get_store` also succeeds for a different container of the same account
- [x] 4.6 [OUTDATED_COMMENT] Rewrite the `.filter(|(files, _)| !files.is_empty())` comment in `validate_sides_share_one_store` (`crates/lakehouse-engine/src/scan/object_store.rs`) to state only the dimension side is skipped when empty; the fact side registers unconditionally and fails inside `side_store_url` with "scan spec has no files" when its file list is empty
- [x] 4.7 [BAD_COMMENT] Eliminate the 16 inline (`//`) comments this plan introduced across `redaction.rs`, `vended.rs`, `adapter/connection.rs`, `pushdown/file_resolution.rs`, and `scan/object_store.rs`: delete the ones redundant with an enclosing doc comment or with the code's own naming, delete the task-number/scenario banner dividers, and promote the genuinely non-obvious rationale (effective-vs-static secret resolution in `resolve_file_list`; the dimension-vs-fact empty-side asymmetry in `validate_sides_share_one_store`; the container-collision accepting controls) into the enclosing doc comments [expert]

## Phase 5: Verification
- [x] 5a Automated checks (build, test, lint, format)
- [x] 5b Scenario coverage audit
- [x] 5c Manual testing commands
