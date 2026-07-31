# Verification Report: add-azure-static-storage-backend

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Static Azure (`abfss://`) read path lands end to end: account-key and inline-SAS credentials select an `Adls` storage backend, the DataFusion scan registers the correct per-container object store, every credential-exposed error path is redacted, and the S3 path is byte-identical (unchanged). |
| Code review | 6 findings — 6 fixed. A separate manual pass then found 16 `[INLINE_COMMENT]` guardrail violations the automated review missed (see Notes) — all 16 fixed. |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test --workspace`) | ✓ |
| Lint (`cargo clippy --workspace --all-targets -- -D warnings`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 942 | 942 | 2 |
| E2E (`make test-e2e`, S3 characterization gate) | 190 | 190 | 0 |

Both runs: 0 failed.

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
exit 0, zero warnings
```

### Formatter

```
cargo fmt --check
exit 0, no changes required
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | connection-credentials | Azure account-key credentials select the ADLS storage backend | `crates/lakehouse-engine/src/adapter/connection.rs` | `account_key_creds_select_the_adls_backend` | Pass |
| vs-adapter | connection-credentials | Azure inline-SAS credentials select the ADLS storage backend | `crates/lakehouse-engine/src/adapter/connection.rs` | `sas_token_creds_select_the_adls_backend` | Pass |
| vs-adapter | connection-credentials | Azure CONNECTION without exactly one account name + credential is rejected | `crates/lakehouse-engine/src/adapter/connection.rs` | `azure_creds_require_account_name_and_exactly_one_credential` | Pass |
| vs-adapter | connection-credentials | CONNECTION mixing Azure and static S3 credential fields is rejected | `crates/lakehouse-engine/src/adapter/connection.rs` | `mixed_azure_and_s3_credential_fields_are_rejected` | Pass |
| vs-adapter | connection-credentials | Optional credential fields default sensibly | `crates/lakehouse-engine/src/adapter/connection.rs` | `absent_optional_fields_default_and_still_select_s3` | Pass |
| vs-adapter | storage-backend-enum | Storage backend total-function fallback | `crates/lakehouse-engine/src/adapter/connection.rs` | `storage_block_falls_through_to_s3_for_an_unvalidated_azure_shape` | Pass |
| vs-adapter | storage-backend-enum (type) | Adls `catalog_storage_props`/`file_io`/`secret_values`/`Debug` redaction/wire round trip | `crates/lakehouse-catalog/src/storage.rs` | `adls_catalog_storage_props_emit_the_account_and_one_credential_key`, `adls_file_io_is_configured_from_exactly_the_catalog_storage_props`, `adls_secret_values_are_the_one_credential_and_omit_an_empty_one`, `adls_cred_is_redacted_in_debug_output`, `adls_serializes_under_a_lowercase_externally_tagged_variant_key`, `adls_round_trips_through_its_tagged_encoding`, `only_matching_lowercase_variant_keys_decode` | Pass |
| vs-adapter | storage-backend-enum (scan) | `side_store_url` derivation, cross-side store-collision precondition, registration | `crates/lakehouse-engine/src/scan/object_store.rs` | `side_store_url_returns_the_same_url_for_s3_as_the_deleted_bucket_derivation`, `side_store_url_preserves_the_s3a_scheme_so_the_key_matches_the_lookup`, `register_side_store_returns_the_container_qualified_url_but_the_registry_key_drops_the_container`, `register_side_store_skips_a_second_side_in_the_same_container`, `register_side_store_registers_both_sides_in_different_accounts`, `register_side_store_surfaces_an_unrecognised_azure_host_redacted`, `validate_sides_share_one_store_rejects_two_containers_in_one_account`, `validate_sides_share_one_store_accepts_every_s3_spec_shape`, `spec_size_index_keys_an_abfss_file_without_its_container` | Pass |
| vs-adapter | storage-backend-enum (redaction) | Azure secret labels redacted | `crates/lakehouse-catalog/src/redaction.rs` | `redact_credentials_strips_azure_account_key_and_sas_labels`, `redact_error_text_removes_a_sas_token_whole_unlike_the_inverted_order` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | `resolve_vended_storage` Adls passthrough | `crates/lakehouse-catalog/src/vended.rs` | `resolve_vended_storage_returns_an_adls_backend_unchanged` | Pass |
| vs-adapter | connection-credentials (file resolution) | Manifest-read errors redact literal Azure secrets; `abfss://` path round trip | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` | `manifest_read_errors_redact_the_literal_azure_secret_values`, `abfss_paths_relativize_and_reconstruct_losslessly` | Pass |
| vs-adapter | boundary surface | `AdlsCred` re-export not silently narrowed/removed | `crates/lakehouse-engine/tests/shared_type_reexports.rs`, `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `reexported_paths_resolve_to_the_catalog_crate_types` (extended), catalog surface `use` list | Pass |
| vs-adapter (all three features) | S3 unchanged | Full workspace + S3 E2E characterization gate, no S3 assertion edited | `crates/lakehouse-engine/tests/` (whole suite) + `make test-e2e` | `cargo test --workspace`, S3 E2E suite | Pass |

Every scenario in the plan's Scenario Coverage table now has a corresponding passing test.

## Notes

- **Code review found 6 findings, all fixed**: a wire-casing inconsistency on the nested `AdlsCred` (now `snake_case`, matching `StorageBackend`'s convention), a missing design-intent note on `Adls::account_name` (the catalog and scan paths resolve "which account" from two different sources — documented), a misleadingly-named registration test (renamed to state the registry-key/container asymmetry it actually proves), an outdated comment claiming an empty side never registers a store (only true for the dimension side, not the fact side — corrected), a missing boundary test for the new `AdlsCred` public re-export (added to both boundary-probe test files), and a duplicated redaction-ordering composition (centralized into one `redact_error_text` function in `lakehouse-catalog`, replacing six independent open-coded copies within this diff's scope).
- **Separate finding, caught in manual review after the automated pass**: the code-reviewer subagent's brief listed three specific risk areas to focus on (exhaustive-match discipline, redaction-order consistency, secret-leak paths), and it treated that list as its full scope rather than also sweeping every category of the loaded review taxonomy — it never checked for `[INLINE_COMMENT]` violations (`/speq:code-guardrails`: "No inline comments — code should be self-explanatory"). A manual sweep found 16 such violations this plan introduced across 5 files; all were fixed with judgment (11 deleted as redundant with an existing doc comment or the code's own naming, 2 deleted as pure process-noise banners, 3 promoted into doc comments because they carried genuinely unique design rationale). Re-verified clean afterward (zero inline comments remain in the diff; all tests/clippy/fmt still green).
- **Out-of-scope findings, NOT fixed in this PR** (confirmed real, recorded for follow-up):
  - `crates/lakehouse-catalog/src/auth.rs:91` and `:150` both compose credential redaction in the inverted order (label pass before value pass), the same defect class this plan's `redact_error_text` was built to prevent. Practical blast radius is currently narrow (only catalog-auth secrets, none of which embed their own label, pass through today), but it's the same root cause — no single owner for the composition order — and becomes a one-line fix once `lakehouse_catalog::redact_error_text` is reused there. Related to issue #135.
  - `crates/lakehouse-catalog/src/session.rs:51-53`'s `build_rest_catalog` hardcodes the S3 `OpenDalStorageFactory` instead of deferring to `StorageBackend::file_io` (which this plan extended with the Azure arm) — a second, wrong-for-Azure owner of that decision. Latent: the only caller is namespace enumeration over REST, never a data/manifest read.
  - **Scope note on issue #135**: the Azure account key or SAS token is serialized into the common scan-spec JSON embedded verbatim in generated pushdown SQL (`scan/spec.rs:644` → `pushdown/support.rs:2281`), a known, by-design, pre-existing behavior (the UDF needs the credential) — not introduced by this diff, and unchanged from the S3 case. Worth flagging because an Azure shared account key is a permanent, account-wide, unscoped credential — strictly more damaging on exposure than S3's scoped, expiring STS token that #135 was originally filed against.
- **Environmental gap encountered and resolved, not a code defect**: the first `make test-e2e` run failed 2 of 7 `e2e_int96_timestamp_test` cases with "object not found" — a known, pre-existing gap where the live shared Docker stack never had its `spark-iceberg-fixtures` one-shot job run (see project memory `e2e-spark-fixtures-provisioning`). Fixed by running the fixture job directly against the stack's shared network; the retry passed clean. Unrelated to Azure/storage-backend selection.
- Non-goals (vended Azure SAS credentials #276, real-cloud Azure E2E #277/#278, Azurite emulator support, AAD/workload-identity auth) are unchanged from the plan and remain tracked exceptions, not gaps.
