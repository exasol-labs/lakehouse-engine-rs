# Tasks: add-direct-storage-refresh-e2e

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: direct-storage REFRESH E2E)
- [x] 1.1 Add `delete_fixture_prefix` and `delete_fixture_object` helpers to `tests/common/raw_parquet.rs`
- [x] 1.2 Add constants and fixture builders to `e2e_direct_storage_test.rs`
- [x] 1.3 Add staged test `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`
- [x] 1.4 Rename `incompatible_pair_fails_create_and_refresh_naming_column_and_files`
- [x] 1.5 Update `specs/testing.md` § Fixtures
- [x] 1.6 Run fmt, clippy, `make test-e2e`, and the second run of the staged test

## Phase 2: Implementation (Group B: direct-storage and entry-page docs)
- [x] 2.1 B1 `README.md`
- [x] 2.2 B2 `docs/index.md`
- [x] 2.3 B3 `docs/architecture.md`
- [x] 2.4 B4 `docs/catalogs.md` Parquet type table
- [x] 2.5 B5 `docs/catalogs.md` "What a REFRESH changes"
- [x] 2.6 B6 `docs/tuning.md`
- [x] 2.7 Check changed sentences against scenarios and writing guardrails

## Phase 4: Review Fixes
- [x] 4.1 `tests/common/raw_parquet.rs`: add private `block_on_fixture` and use it in `put_fixture_object`, `delete_fixture_prefix`, `delete_fixture_object`
- [x] 4.2 `put_refresh_file`: build the batch with `RecordBatch::try_from_iter_with_nullable`
- [x] 4.3 `put_refresh_file`: replace the infallibility `.expect` with `unwrap_or_else(|e| panic!("build the refresh fixture batch for {key}: {e}"))`
- [x] 4.4 Staged test A7: replace the hand-parsed response with `conn.query_row_count`
- [x] 4.5 Staged test A1, A3, A6: require `"not found"` in the `assert_fails_naming` fragments
- [x] 4.6 Staged test A4, A5: assert the returned error messages do not contain the storage secret
- [x] 4.7 Staged test: bind `secret` from `direct_storage_password().secret_key` and use it in the REFRESH leak check
- [x] 4.8 Delete the doc comments above `assert_reads`, `assert_fails_naming`, `skip_reason`
- [x] 4.9 `docs/architecture.md` line 32: name the `CATALOG_KIND` property instead of the CONNECTION
- [x] 4.10 `docs/catalogs.md` line 615: state the wider integer's Exasol type, `DECIMAL(20,0)` for `INT64`

## Phase 3: Verification
- [x] 3.1 Build, test, E2E, lint, format checklist
- [x] 3.2 Scenario coverage audit and manual testing
