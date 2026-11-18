# Plan: add-export-adls-storage

## Summary

`EXPORT ... INTO SCRIPT LAKEHOUSE_EXPORT` writes Iceberg tables on Azure Data Lake Storage Gen2 through an Iceberg REST catalog, with the same credential options that virtual schemas read with: an account key or a SAS token from the CONNECTION, or a SAS the catalog vends. A writer renews a vended SAS through the catalog's `loadCredentials` endpoint, as it renews vended S3 keys, and uses the current SAS on every storage request (#569).

## Context

- The export writes only onto S3-compatible storage. It refuses a CONNECTION with Azure storage fields and a table location whose scheme is not `s3` or `s3a`.
- Virtual schemas already read ADLS Gen2 tables with an account key, a static SAS, or a vended SAS, and the writer already resolves vended storage through the same function the virtual schema uses.
- The writer's FileIO uses opendal's Azure backend for ADLS. That backend takes one fixed SAS and has no hook to renew it, unlike its S3 backend, which takes the custom credential loader that the export's S3 renewal relies on.
- object_store's Azure client, which the scan already uses for ADLS data files, accepts a credential provider that it asks on each request.
- Iceberg's `FileIO` accepts any `StorageFactory`, so the writer can plug in its own ADLS storage. The new storage is in the architecture delta (`architecture.md`).
- The S3 code already reads an absent `s3.session-token-expires-at-ms` as credentials that never expire. The spec states it ambiguously.
- Depends on #566 and #567, both closed.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| export/export-catalog-storage | CHANGED | `export/export-catalog-storage/spec.md` |
| export/export-iceberg-table | CHANGED | `export/export-iceberg-table/spec.md` |
| export/export-options | CHANGED | `export/export-options/spec.md` |
| storage-access/pushdown-planning-cloud-credentials-vended-storage | CHANGED | `storage-access/pushdown-planning-cloud-credentials-vended-storage/spec.md` |

## Impact

- A CONNECTION with Azure storage fields was refused by the export and now writes, provided it passes the checks `CREATE VIRTUAL SCHEMA` runs. A table location on `abfss://` was refused and now writes.
- The export error for Azure storage fields ("the export writes only to S3-compatible storage") is gone. A CONNECTION with Azure fields and `aws_assume_role_arn` now fails with the virtual schema's error for that CONNECTION.
- `docs/export.md` states ADLS Gen2 support and the SAS renewal.
- No breaking change for S3 exports.

## Dependencies

- `object_store` with its `azure` feature becomes a dependency of `crates/lakehouse-catalog-write`. The workspace already builds it for the scan.
- `typetag` becomes a dependency of `crates/lakehouse-catalog-write`, because iceberg's `Storage` and `StorageFactory` are `#[typetag::serde]` traits at the pinned revision.
- `hmac` and `sha2` become dev-dependencies of `crates/lakehouse-e2e`, to sign the container SAS the Azure suite mints.

## Implementation Notes

- First step: on the Azure stack, read one vended `loadTable` answer of the vended Lakekeeper warehouse and record whether it carries `adls.sas-token-expires-at-ms.<host>` (D5).
- ADLS write storage in `crates/lakehouse-catalog-write`: an `iceberg::io::Storage` and `StorageFactory` over object_store's `MicrosoftAzure` client, built from the table location URL, whose account is the location host's first label (D3), with a `CredentialProvider` that asks the table's ADLS credential source on every request (D1). `writer()` uses object_store's multipart upload, so each part signs with the SAS current at that time. The credential source is skipped on serialization, as iceberg-storage-opendal skips its custom loader, and a deserialized storage refuses every request.
- One write credential source per backend (D7): `StorageCredentials` (`vended_refresh.rs`) stays the S3 source, and a new ADLS source over `ExpiringCredential<AzureCredential>` holds the account key or static SAS, never renewed, or a vended SAS. Both share `keep_at_renewal_point`, `RENEWAL_SKEW`, and the `loadCredentials` renewal and its error text.
- `CatalogSession::load_credentials` in `crates/lakehouse-catalog` returns the selected vended credential set, which `vended_s3_credentials` reads as S3 keys (unchanged) and a new `vended_adls_sas` reads as an ADLS SAS, so the vended config keys stay in `lakehouse-catalog`. `vended_adls_sas` reuses `iceberg_vended_sas` for the SAS and reads `adls.sas-token-expires-at-ms.<host>` for the same host (D4). An absent expiry is `None`, and a present one that is no epoch millisecond is refused, naming the key.
- `WriteTable::open` (`iceberg_write.rs`) replaces its `s3`/`s3a` check with one per storage kind: static S3 or role storage addresses `s3` and `s3a` (`StorageBackend::addresses_scheme`), static Azure addresses `abfss`, and vended storage refuses `abfs` without the export's `ALLOW_HTTP` (D6) before it calls `resolve_vended_storage`, which accepts the rest. `storage_of` returns the FileIO of the backend the location selects: the S3 FileIO with the S3 source, or the ADLS FileIO with the ADLS source.
- `refuse_unwritable` (`export/target.rs`) drops its Azure branch, so `validate_shared_creds` alone judges the Azure fields, as it does for a virtual schema.
- The Azure E2E suite mints a service SAS for its run's container (`sr=c`), signed with the account key it holds, with read, add, create, write, delete, and list permissions and a lifetime of one hour.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Branch | `crates/lakehouse-engine/src/export/target.rs` `refuse_unwritable` | The Azure storage refusal; shared CONNECTION validation judges the Azure fields |
| Test row | `crates/lakehouse-engine/src/export/target_tests.rs` | Any case asserting the Azure storage refusal |
| Test row | `crates/lakehouse-e2e/tests/e2e_local/export.rs` `export_refuses_unwritable_connections_and_column_names` | The `EXPORT_E2E_AZURE_CREDS` expectation of the Azure refusal; the case now expects the location refusal, and an Azure-plus-role case expects the virtual schema's error |
| Test | `crates/lakehouse-catalog-write/src/iceberg_write_tests.rs` `an_abfss_stage_answer_is_refused_naming_the_table_and_the_scheme` | Asserts the refusal of every `abfss` location; replaced by the per-storage-kind case table |
| Function | `crates/lakehouse-catalog-write/src/iceberg_write.rs` `adls_refused` | `storage_of` routes ADLS to the ADLS FileIO, so no caller remains |
| Doc text | `docs/export.md` § The CONNECTION | The S3-only storage requirement |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Static and vended credentials write a table on ADLS Gen2 | E2E (Lakekeeper) | `make lakekeeper-up test-e2e-lakekeeper` | 0 failures; the vended S3 export runs only here |
| E2E (Azure) | `crates/lakehouse-e2e/tests/e2e_azure_test.rs` | `azure_export_writes_and_appends_with_account_key_sas_and_vended_credentials` |
| A writer that outlives its vended credentials refreshes them, or fails naming their expiry (ADLS renewal, every later request uses the renewed SAS) | Fault injection | `crates/lakehouse-catalog-write/src/vended_refresh_tests.rs` | `a_vended_sas_within_five_minutes_of_expiry_is_renewed_and_signs_the_next_request` |
| A writer that outlives its vended credentials refreshes them, or fails naming their expiry (no expiry: never renewed) | Fault injection | `crates/lakehouse-catalog-write/src/vended_refresh_tests.rs` | `credentials_without_an_expiry_are_used_as_vended_and_never_renewed` (S3 and ADLS rows; no `loadCredentials` request reaches the fake catalog) |
| A writer that outlives its vended credentials refreshes them, or fails naming their expiry (unreadable expiry, missing key) | Unit | `crates/lakehouse-catalog/src/vended_tests.rs` | `vended_s3_credentials_without_a_key_or_a_readable_expiry_are_refused_naming_the_key` (existing) and `vended_adls_sas_without_a_sas_or_a_readable_expiry_is_refused_naming_the_key` |
| A writer that outlives its vended credentials refreshes them, or fails naming their expiry (missing key at renewal) | Fault injection | `crates/lakehouse-catalog-write/src/vended_refresh_tests.rs` | `a_renewal_answer_without_a_key_is_refused_naming_the_key` (existing, gains an ADLS row) |
| A CONNECTION the export does not support is refused before any data is read | E2E (local) | `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `export_refuses_unwritable_connections_and_column_names` |
| A table location the CONNECTION's storage cannot address is refused (static Azure CONNECTION on the local `s3://` warehouse) | E2E (local) | `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `export_refuses_unwritable_connections_and_column_names` (its `EXPORT_E2E_AZURE_CREDS` case now expects the location refusal) |
| A table location the CONNECTION's storage cannot address is refused (static S3 on `abfss`, vended on other schemes, vended `abfs` without `ALLOW_HTTP`) | Fault injection | `crates/lakehouse-catalog-write/src/iceberg_write_tests.rs` | `a_location_its_storage_cannot_address_is_refused_naming_the_table_and_the_scheme` |

The existing S3 renewal tests in `vended_refresh_tests.rs` keep their scenario line. The `export/export-iceberg-table` description change, the `export/export-options` Background change, and the removed vended-storage scenario need no new test. The vended `abfs` refusal cannot be produced end to end, because Lakekeeper vends only `abfss` locations.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| export/export-catalog-storage | Against the Azure stack (`make azure-up`), create a CONNECTION with `account_name` and `account_key` to the static Lakekeeper ADLS warehouse, then run `EXPORT (SELECT 1 AS ID) INTO SCRIPT LHVS.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<conn>' TABLE_NAME = 'manual.adls_one'` and `SELECT * FROM <vs>.ADLS_ONE` through a virtual schema on that CONNECTION | The export reports 1 row, and the select returns `ID = 1` |
| export/export-catalog-storage | The same export through a CONNECTION with `"use_vended_credentials": true` to the vended warehouse | The export reports 1 row, and the table reads back |
| export/export-iceberg-table | `SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = '<vs>'` after `ALTER VIRTUAL SCHEMA <vs> REFRESH` | Lists `ADLS_ONE` |
| storage-access/pushdown-planning-cloud-credentials-vended-storage | `SELECT COUNT(*) FROM <vended vs>.<seeded table>` on the Azure stack | The seeded row count, unchanged from before this plan |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E (local) | `make e2e-up test-e2e` | 0 failures |
| E2E (Lakekeeper) | `make lakekeeper-up test-e2e-lakekeeper` | 0 failures; the vended S3 export runs only here |
| E2E (Azure) | `make azure-up test-e2e-azure` | 0 failures; needs the Azure credentials in `test.env`, else reported as not run |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |

## Out of scope

- `docs/export.md` says the export does not renew assumed-role credentials, but the writer renews them by assuming the role again. The doc sentence is stale.
- Exports into AWS Glue (#570) and Unity Catalog (#571), and partitioned ADLS tables beyond what #568 ships.
