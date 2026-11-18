# Tasks: add-export-adls-storage

- [x] 1. D5 check: does Lakekeeper vend `adls.sas-token-expires-at-ms.<host>` with an ADLS SAS (Implementation Notes, first step)
- [x] 2. `lakehouse-catalog`: `load_credentials` returns the selected vended credential set; `vended_adls_sas` reads the SAS and `adls.sas-token-expires-at-ms.<host>` (export-catalog-storage: refresh scenario, unreadable expiry and missing key)
- [x] 3. `lakehouse-catalog-write`: ADLS write credential source beside the S3 one, sharing the renewal timing and `loadCredentials` (export-catalog-storage: refresh scenario, ADLS renewal and no-expiry rows)
- [x] 4. `lakehouse-catalog-write`: ADLS Iceberg storage over object_store's Azure client, taking each request's credential from the ADLS source
- [x] 5. `WriteTable::open`: per-storage-kind location check (D6) and routing to the ADLS FileIO; remove `adls_refused` and the abfss refusal test (export-catalog-storage: location refusal scenario)
- [x] 6. Export target: drop the Azure storage refusal from `refuse_unwritable` (export-catalog-storage: unsupported CONNECTION scenario)
- [x] 7. Local E2E: the Azure case of `export_refuses_unwritable_connections_and_column_names` expects the location refusal; add the Azure-plus-role case
- [x] 8. Azure E2E: `azure_export_writes_and_appends_with_account_key_sas_and_vended_credentials`, with a minted container SAS
- [x] 9. `docs/export.md`: ADLS Gen2 support, the SAS renewal, and the removed S3-only requirement
- [x] 10. Verification: build, unit tests, lint, format, local, Lakekeeper, and Azure E2E suites, manual checks
