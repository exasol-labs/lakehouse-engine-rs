# Verification Report: add-export-adls-storage

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Exports write and append Iceberg tables on ADLS Gen2 with an account key, a container SAS, and a vended SAS; the local, Lakekeeper, and Azure suites pass |
| Code review | 6 findings — 6 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 2178 | 2176 | 2 |
| E2E local (`make e2e-up test-e2e`) | 402 | 402 | 0 |
| E2E Lakekeeper (`make lakekeeper-up test-e2e-lakekeeper`) | 62 | 62 | 0 |
| E2E Azure (`make azure-up test-e2e-azure`) | 29 | 29 | 0 |

The 2 ignored unit tests are the existing `micro_bench::bench_emit_only` and `micro_bench::bench_scan_only` micro-benchmarks.

### Manual Tests

The Azure suite's fixture provisions a per-run container and its Lakekeeper warehouses and deletes them at the end, so the manual steps ran inside `azure_export_writes_and_appends_with_account_key_sas_and_vended_credentials` and `azure_static_and_vended_creds_end_to_end`, with a 500-row source instead of `SELECT 1`.

| Test | Result |
|------|--------|
| export/export-catalog-storage: export through an `account_name` + `account_key` CONNECTION into the static ADLS warehouse, read back through a virtual schema | ✓ 500 rows reported per export; the virtual schema reads 1000 rows, 500 distinct IDs |
| export/export-catalog-storage: the same export through a `"use_vended_credentials": true` CONNECTION into the vended warehouse, `SINGLE_COMMIT = 'true'` | ✓ 500 rows reported per export; the table reads back with every row twice |
| export/export-iceberg-table: `ALTER VIRTUAL SCHEMA ... REFRESH`, then query the exported table | ✓ the exported tables are listed and queryable after REFRESH |
| storage-access/pushdown-planning-cloud-credentials-vended-storage: query the seeded table through the vended virtual schema | ✓ `azure_static_and_vended_creds_end_to_end` passes unchanged |
| D5: a vended `loadTable` answer of the Lakekeeper ADLS warehouse | ✓ the selected credential set carries `adls.sas-token.<host>`, `adls.sas-token-expires-at-ms.<host>`, `adls.refresh-credentials-endpoint`, and `expiration-time`, so Lakekeeper SAS exports renew |

## Tool Evidence

### Linter

```
make lint: exit 0 (cargo clippy --workspace --all-targets -D warnings)
```

### Formatter

```
make fmt-check: exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| export | export-catalog-storage | Static and vended credentials write a table on ADLS Gen2 | `crates/lakehouse-e2e/tests/e2e_azure_test.rs` | `azure_export_writes_and_appends_with_account_key_sas_and_vended_credentials` | Pass |
| export | export-catalog-storage | Static and vended credentials write a table on ADLS Gen2 (vended S3 export) | `make test-e2e-lakekeeper` | Lakekeeper suite, 62 tests | Pass |
| export | export-catalog-storage | A writer that outlives its vended credentials refreshes them, or fails naming their expiry (ADLS renewal) | `crates/lakehouse-catalog-write/src/vended_refresh_tests.rs` | `a_vended_sas_within_five_minutes_of_expiry_is_renewed_and_signs_the_next_request` | Pass |
| export | export-catalog-storage | A writer that outlives its vended credentials refreshes them, or fails naming their expiry (no expiry) | `crates/lakehouse-catalog-write/src/vended_refresh_tests.rs` | `credentials_without_an_expiry_are_used_as_vended_and_never_renewed` | Pass |
| export | export-catalog-storage | A writer that outlives its vended credentials refreshes them, or fails naming their expiry (unreadable expiry, missing key) | `crates/lakehouse-catalog/src/vended_tests.rs` | `vended_s3_credentials_without_a_key_or_a_readable_expiry_are_refused_naming_the_key`, `vended_adls_sas_without_a_sas_or_a_readable_expiry_is_refused_naming_the_key` | Pass |
| export | export-catalog-storage | A writer that outlives its vended credentials refreshes them, or fails naming their expiry (missing key at renewal) | `crates/lakehouse-catalog-write/src/vended_refresh_tests.rs` | `a_renewal_answer_without_a_key_is_refused_naming_the_key`, `a_renewal_sas_answer_without_the_hosts_sas_is_refused_naming_the_key` | Pass |
| export | export-catalog-storage | A CONNECTION the export does not support is refused before any data is read | `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `export_refuses_unwritable_connections_and_column_names` | Pass |
| export | export-catalog-storage | A table location the CONNECTION's storage cannot address is refused (static Azure on `s3://`) | `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `export_through_azure_storage_into_an_s3_location_is_refused_naming_the_scheme` | Pass |
| export | export-catalog-storage | A table location the CONNECTION's storage cannot address is refused (per storage kind) | `crates/lakehouse-catalog-write/src/iceberg_write_tests.rs` | `a_location_its_storage_cannot_address_is_refused_naming_the_table_and_the_scheme` | Pass |

## Notes

- The ADLS missing-key renewal case is its own test, `a_renewal_sas_answer_without_the_hosts_sas_is_refused_naming_the_key`, beside the S3 one, because the two sources hand out different credential types.
- The static-Azure location refusal is its own local E2E test, not a case of `export_refuses_unwritable_connections_and_column_names`: the writer refuses the location after the source is read, so it cannot share that test's failing source.
- Every export error passes the label redaction (`adls.sas-token`, `s3.session-token`, …), which cuts a vended key name after its label, for example `adls.sas-token[REDACTED]`. This predates the plan and applies to the S3 expiry key too.
- The ADLS write storage is tested at the credential-provider and path-mapping level; requests against real storage are covered by the Azure suite only.
- Tasks 2 to 4 were written with production code ahead of some tests, because the crate had to compile to show RED; each test was run and passes.
