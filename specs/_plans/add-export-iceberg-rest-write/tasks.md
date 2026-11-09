# Tasks: add-export-iceberg-rest-write

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: catalog session surface, expiring credentials)
- [x] 1.1 CatalogSession public surface for the write crate
- [x] 1.6 Expiring-credential holder, Unity and Iceberg REST OAuth tokens on it [expert]

## Phase 2: Implementation (Group A2: write crate, commit loop, vended refresh)
- [x] 1.2 Create crate `lakehouse-catalog-write`
- [x] 1.3 `append_files(table, files, tag)` [expert]
- [x] 1.4 `commit_staged(staged, files, tag)`
- [x] 1.5 Record Lakekeeper fixtures under `testdata/`
- [x] 1.7 `vended_refresh.rs`, vended storage source [expert]

## Phase 2: Implementation (Group B: export statement, writer, E2E, docs)
- [x] 2.1 Split `export.rs` into `export/` modules
- [x] 2.2 `exasol_type_from_column_info`, move scan `target_arrow_type` onto it
- [x] 2.3 `export/arrow_schema.rs`
- [x] 2.3a `export/iceberg_schema.rs`
- [x] 2.4 Writer spec extension, export ID, column-name checks
- [x] 2.5 `connection.rs` CONNECTION reading without the adapter
- [x] 2.6 `ALLOW_HTTP` option and plaintext gate
- [x] 2.7 `ExportTarget` and `ExportSink` traits, Iceberg REST target
- [x] 2.8 Writer and Iceberg sink [expert]
- [x] 2.9 `export/batch.rs`
- [x] 3.1 Update local E2E `export.rs`
- [x] 3.2 Lakekeeper E2E `export_write.rs`
- [x] 3.3 Build and run E2E suites, record results
- [x] 4.1 `docs/export.md`, `docs/index.md`
- [x] 4.2 `docs/security.md`
- [x] 4.3 `specs/mission.md`

## Phase 4: Review Fixes
- [x] 4.4 `vended_refresh.rs`: change `refresh_decision` to take `(point: RenewalPoint, endpoint_offered: bool)`, delete its absent-expiry and outside-skew arms and `skew_ms`, rewrite its doc comment, make `decide` pass `point`, rewrite the test case table as `(RenewalPoint, bool, RefreshDecision)` rows without the four dead-arm rows
- [x] 4.5 `iceberg_write.rs` `stage_table`: send through `self.send(&tables_path(..), &body, RepeatSafety::Safe)`, continue on `Sent::Applied`, return `staged_creation_error` for every other `Sent` variant
- [x] 4.6 `iceberg_write.rs`: add doc comments to `CommitTag::export_id`, `CommitTag::writer_id`, `WriteTable::table`, `StagedTable::table`
- [x] 4.7 `session.rs`: add `pub fn namespace_tables_path`, rename `ident_path` to `pub fn table_ident_path`, export both from `lib.rs`; delete `tables_path` and `table_path` from `iceberg_write.rs` and call the exported functions
- [x] 4.8 Delete the three `dotted` copies (write crate `lib.rs`, `session.rs`, `iceberg_sink.rs`), format the identifiers through `Display`, replace `level.join(".")` in `create_namespace_level` with `level`
- [x] 4.9 `snapshot_capture.rs`: measure the 300-writer export with and without `.with_check_duplicate(false)`, record both durations in plan.md § Context, delete the call when they do not differ, else cite the durations in its comment
- [x] 4.10 `retry.rs` `backoff_wait`: replace the `.min(31)` doubling cap with `2_u32.saturating_pow(retry.saturating_sub(1))`
- [x] 4.11 `export/mod.rs`: add `new_writer_id()` next to `new_export_id`; `writer.rs` calls it and drops `random_hex` from its import
- [x] 4.12 `export/target.rs`: split `open_table(schema, single_commit, tag)` into `open_table(schema, tag)` and `stage_table(schema, tag)`, implement both in `iceberg_rest.rs` over a private helper, `write_group` picks by `spec.single_commit`, `FakeTarget` records the two calls separately
- [x] 4.13 `export/target.rs`: delete `is_plain_http`, test each field with `scheme_of(value) == "http"`
- [x] 4.14 Move `spelled_as_measured` from `export/arrow_schema.rs` into vs-expression `exasol_type.rs` as `pub fn is_reported_spelling`, with one case table in `exasol_type_tests.rs`; `arrow_type` calls it
- [x] 4.15 `iceberg_write.rs`: add `existing_table_error` and `pub async fn refuse_existing_table`, `created_by_export` returns `existing_table_error`, delete `TableWriteSession::table_exists`; `iceberg_rest.rs` `prepare` calls `refuse_existing_table` and the `table` field is deleted
- [x] 4.16 Move the exported test helpers of lakehouse-catalog into `exported_test_support_tests.rs` (`pub mod exported_test_support` behind `test-support`), declare `test_support` `#[cfg(test)]` again without per-item attributes, make `header_value` `pub(crate)`, write crate imports from `lakehouse_catalog::exported_test_support`
- [x] 4.17 Move `clock_reading` into `exported_test_support_tests.rs` and delete its copies and `clock_at`; move `STAGED_LOCATION` into the write crate's `test_support_tests.rs`; replace `SILENT_CONFIG` with `PREFIX_CONFIG`
- [x] 4.18 `catalog_http_tests.rs`: delete `STATE_CHANGING_PATHS` and both loops, send each case to `/v1/wh/namespaces/db/tables/t`
- [x] 4.19 `e2e_local/export.rs` `export_single_commit_on_catalog_refusing_stage_create_fails`: delete the export into `accepted_table` and its comment; add `create_table_sends_a_format_2_table_tagged_with_the_export_id` to the scenario row in plan.md § Verification › Scenario Coverage
- [x] 4.20 `e2e_lakekeeper_test.rs` line 9: replace `oauth2_client_credentials_grant` with `ClientCredentialsGrant`
- [x] 4.21 `iceberg_write.rs`: split `Sent::Failed` into `Sent::Refused` and `Sent::Unsent`, retry `Unsent` in `create_namespace_level`, `create_table` and `send_commit`, add a refused-connection fault-injection test asserting `creating table ns.t failed after 5 attempts` [expert]
- [x] 4.22 `session.rs`: make `send` `pub(crate)`, add `pub async fn post`, remove `pub use reqwest::Method`, write crate calls `post` [expert]
- [x] 4.23 `vended.rs`: make `vended_credential_config` `pub(crate)` and guard it in the public-surface test, add `VendedS3Credentials` and `pub fn vended_s3_credentials`, `load_credentials` returns it, `vended_refresh.rs` and `storage_of` build on it, record the scoped exception in a CHANGED delta and plan.md § Features [expert]
- [x] 4.24 Add `export/iceberg_sink_tests.rs`: one fake catalog and S3 server, a case table proving `abort` keeps the data files after a 504 commit and deletes them after a 403 [expert]
