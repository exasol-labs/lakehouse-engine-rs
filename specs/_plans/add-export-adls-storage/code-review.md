# Code Review Findings: add-export-adls-storage

## Summary
- Files reviewed: 23
- Total findings: 6

The targeted unit tests pass (`cargo test --workspace --lib -- adls sas vended location_its_storage azure_connection`, exit 0, log in `target/speq-review-unit.log`).

## Findings

### docs/export.md

#### [UNVERIFIED_CLAIM] Lakekeeper's ADLS SAS expiry is stated before D5 is checked
- Location: § The CONNECTION, the paragraph that starts "With `"use_vended_credentials": true`", sentence "Lakekeeper vends credentials for one hour, states their expiry, and offers `loadCredentials`."
- Issue: D5 and task 1 leave open whether Lakekeeper vends `adls.sas-token-expires-at-ms.<host>`. The `D5-TEMP` diagnostic in `e2e_azure_test.rs` has not reported back. If the key is absent, a Lakekeeper ADLS SAS is never renewed, and the sentence is wrong for ADLS. D5 requires the doc to say so in that case.
- Fix: Run the Azure suite and read the `D5-TEMP keys:` line. If it lists `adls.sas-token-expires-at-ms.<host>`, keep the sentence. Otherwise replace it with: "Lakekeeper vends credentials for one hour and offers `loadCredentials`. It states the expiry of the S3 keys it vends but not of an ADLS SAS, so a writer uses a Lakekeeper SAS as vended and never renews it." Then mark task 1 done.

### crates/lakehouse-engine/src/export/target_tests.rs

#### [SCENARIO_TAG_MISMATCH] A CONNECTION parse test claims the ADLS write scenario
- Location: `an_azure_connection_is_read_with_its_account_key_or_sas_token`, its `/// Scenario: Static and vended credentials write a table on ADLS Gen2` line
- Issue: The test only checks that `read_connection` accepts an Azure CONNECTION. It writes nothing and reads nothing back, so it does not implement the scenario. The plan's Scenario Coverage maps that scenario to the Azure E2E test alone.
- Fix: Delete the `/// Scenario: Static and vended credentials write a table on ADLS Gen2` line above `an_azure_connection_is_read_with_its_account_key_or_sas_token`. Keep the test.

### crates/lakehouse-e2e/tests/e2e_local/export.rs

#### [DEAD_CODE] An unused CONNECTION and a duplicated Azure password
- Location: `export_refuses_unwritable_connections_and_column_names` (the `("EXPORT_E2E_AZURE_CREDS", &azure)` entry of the CONNECTION loop) and `export_through_azure_storage_into_an_s3_location_is_refused_naming_the_scheme` (the `azure` literal)
- Issue: No case of `export_refuses_unwritable_connections_and_column_names` exports through `EXPORT_E2E_AZURE_CREDS` any more, so the test creates a CONNECTION it never uses. The `CatalogConnectionPassword` literal with `warehouse: "s3://warehouse/"`, `account_name: "exporte2e"`, and `account_key: AZURE_ACCOUNT_KEY` is written out in both tests.
- Fix: Add `fn azure_connection_password() -> CatalogConnectionPassword` returning that literal. In `export_refuses_unwritable_connections_and_column_names`, remove the `("EXPORT_E2E_AZURE_CREDS", &azure)` entry, drop the `azure` binding, and build `azure_role` as `CatalogConnectionPassword { aws_assume_role_arn: Some(...), ..azure_connection_password() }`. In `export_through_azure_storage_into_an_s3_location_is_refused_naming_the_scheme`, replace the literal with `azure_connection_password()`.

### crates/lakehouse-catalog/src/vended.rs

#### [SPEC_DEVIATION] The SAS expiry can come from another key spelling than the SAS
- Location: `adls_sas_credentials`, `host_keyed_values(...).next()`; `iceberg_vended_sas`, `.find(|value| !value.is_empty())`
- Issue: D4 reads the expiry "for the host of the selected SAS, with the same case-insensitive host match and tie-break as the SAS key". The SAS skips an empty value and takes the next spelling, but the expiry always takes the first spelling. With an empty exact-host SAS and a non-empty case variant, the SAS comes from the variant and the expiry from the exact key. With a SAS under the exact key and an expiry only under a case variant, the expiry of a different key spelling is used. `host_keyed_values` also yields the exact key twice, once from the direct lookup and once from the sorted variants.
- Fix: In `vended.rs`, replace `host_keyed_values` with `fn host_key_suffixes<'a>(vended: &'a HashMap<String, String>, prefix: &str, host: &str) -> Vec<&'a str>` returning the matching host suffixes, the exact `host` first and then the case variants in ascending key order, without repeating the exact one. In `iceberg_vended_sas`, pick the first suffix whose `<VENDED_SAS_TOKEN_KEY_PREFIX><suffix>` value is non-empty and return that value. Make `adls_sas_credentials` use the same chosen suffix to read `<VENDED_SAS_EXPIRES_AT_MS_KEY_PREFIX><suffix>`, and name that key in the unreadable-expiry error. Add a row to `vended_adls_sas_is_selected_by_anchor_host_with_derived_account_name` or a new test where the exact-host SAS is empty and an upper-case variant carries the SAS and its expiry, and assert that the variant's expiry is returned.

### crates/lakehouse-catalog-write/src/vended_refresh.rs

#### [DUPLICATED_ERROR_TEXT] The ADLS renewal repeats the `loadCredentials` failure prefix
- Location: `AdlsCredentials::renew_vended`, the `format!("loadCredentials of table '{}' failed: the vended {cause}", ...)` closure
- Issue: `renewed` owns the `loadCredentials of table '<t>' failed: ` prefix of a credential set it cannot read. `renew_vended` writes the same prefix again for a SAS that `sas_lease` cannot split, so the error text lives in two places.
- Fix: In `AdlsCredentials::renew_vended`, pass a `read` closure to `renewed` that reads and splits the SAS: `|set, location| set.adls_sas(location).and_then(|seed| sas_lease(seed).map_err(|cause| UdfError::User(format!("the vended {cause}"))))`. Make `renewed` generic over the `T` the closure returns, as it already is, and remove the `.and_then(...)` block with the duplicated prefix from `renew_vended`.

### crates/lakehouse-catalog-write/src/iceberg_write.rs

#### [STALE_COMMENT] `WriteStorage::Static` still says S3 keys only
- Location: `WriteStorage::Static`, doc comment "The CONNECTION's own storage keys, which do not expire."
- Issue: `Static` now also holds an Azure account key or SAS (`StorageBackend::Adls`), which `storage_of` routes to the ADLS storage.
- Fix: Change the doc comment of `WriteStorage::Static` to "The CONNECTION's own storage credentials, S3 keys or an Azure account key or SAS, which do not expire."
