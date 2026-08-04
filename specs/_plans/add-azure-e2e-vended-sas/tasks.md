# Tasks: add-azure-e2e-vended-sas

## Phase 2: Implementation (Group A) — crates/lakehouse-engine/tests/common/lakekeeper.rs
- [x] 1.1 Add sas_enabled field to AdlsWarehouseProfile, rename new -> static_creds, add vended constructor
- [x] 1.2 No new CONNECTION-password helper; generalize existing doc comment
- [x] 1.3 Extend mod tests: widen adls_warehouse_matches_lakekeeper_profile_shape, extend lakekeeper_connection_password_vended_omits_static_s3

## Phase 2: Implementation (Group B) — crates/lakehouse-engine/tests/common/seed.rs
- [x] 2.1 Correct SeedStorage::Adls arm's comment in build_seed_catalog_with_auth

## Phase 2: Implementation (Group C) — crates/lakehouse-engine/tests/e2e_azure_test.rs
- [x] 3.1 Restructure AzureFixture to carry two arms over one container [expert]
- [x] 3.2 Rewrite the end-to-end test as azure_static_and_vended_creds_end_to_end [expert]
- [x] 3.3 Update binary's module doc comment and surviving test doc comments

## Phase 3: Verification
- [ ] 4.1 Run CI's exact gates (fmt, clippy workspace, clippy azure-e2e, clippy lakekeeper-e2e), cargo test --workspace, cargo test --features azure-e2e --no-run
- [ ] 4.2 Run touched host-side unit coverage (adls_ filter, lakekeeper_connection_password_vended filter)
- [ ] 4.3 Re-run make test-e2e-lakekeeper (regression gate for Group A/B)
- [ ] 4.4 Run make test-e2e-azure against the live account [expert]

## Phase 4: Review Fixes
- [x] 5.1 Bind the vended password to a local in AzureFixture::provision, pass it to create_virtual_schema_with_password, carry it in a new AzureFixture::vended_password field, and assert group 1 against that installed value instead of a re-derived one [expert]
- [x] 5.2 Move the shared-harness provenance block (script-text and adapter-script loops) above the static arm's block in azure_static_and_vended_creds_end_to_end, renumber the groups 1-4 vended / 5 provenance / 6 static / 7 cross-arm, and update the cross-arm back-reference [expert]
- [x] 5.3 Rewrite post_warehouse's doc comment in crates/lakehouse-engine/tests/common/lakekeeper.rs to drop the false "fail loudly unless it exists afterwards" claim and the false "unique key-prefix" overlap inference, stating instead that the overlap-400-means-already-exists mapping is unverified for warehouses sharing one bucket/filesystem and that callers needing certainty must read back via lakekeeper_warehouse_storage_profile
- [x] 5.4 Rewrite create_warehouse_and_confirm's doc comment and assert_eq! message in crates/lakehouse-engine/tests/e2e_azure_test.rs to separate the two failure mechanisms (overlap-rejected create surfaces via lakekeeper_warehouse_storage_profile's own panic; key-prefix assertion covers only a mismatched-prefix registration) and reword the assertion message to match the case it can actually report
