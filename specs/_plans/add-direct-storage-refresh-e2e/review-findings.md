# Code Review Findings: add-direct-storage-refresh-e2e

## Summary
- Files reviewed: 8
- Total findings: 10 (standard: 10, expert: 0)

## Standard fixes

### crates/lakehouse-engine/tests/common/raw_parquet.rs

#### [SHRINKABLE] The current-thread runtime is built three times
- Location: lines 89-92 (`put_fixture_object`), 113-116 (`delete_fixture_prefix`), 124-127 (`delete_fixture_object`)
- Issue: Each of the three public fixture functions repeats the same five-line `tokio::runtime::Builder::new_current_thread().enable_all().build().expect(...)` block. The two new delete helpers add the second and third copy, so the Rule of Three now applies.
- Fix: In crates/lakehouse-engine/tests/common/raw_parquet.rs, add a private `fn block_on_fixture<F: std::future::Future>(future: F) -> F::Output` that builds the current-thread runtime with `.expect("tokio runtime for a fixture object operation")` and returns `runtime.block_on(future)`. Replace the runtime construction and `rt.block_on(...)` in `put_fixture_object`, `delete_fixture_prefix`, and `delete_fixture_object` with a call to `block_on_fixture`, keeping each caller's `unwrap_or_else(|e| panic!(...))` message unchanged.

### crates/lakehouse-engine/tests/e2e_direct_storage_test.rs

#### [SHRINKABLE] `put_refresh_file` re-implements `RecordBatch::try_from_iter_with_nullable`
- Location: lines 1962-1970, `put_refresh_file`
- Issue: The function builds `Field`s from `(name, array)` pairs, unzips them, and calls `RecordBatch::try_new`. Arrow's `RecordBatch::try_from_iter_with_nullable` does exactly this, and the same file already uses it in `all_types_batch` (line 456).
- Fix: In crates/lakehouse-engine/tests/e2e_direct_storage_test.rs, replace the body of `put_refresh_file` up to the batch construction with `let batch = RecordBatch::try_from_iter_with_nullable(columns.into_iter().map(|(name, array)| (name, array, true)))`, followed by the error handling from the [CONTEXTLESS_ERROR] fix below, then keep `write_parquet_fixture(&refresh_key(key), batch);`. Remove any import that becomes unused.

#### [CONTEXTLESS_ERROR] The batch-construction panic claims infallibility and omits the fixture key
- Location: line 1968, `put_refresh_file`
- Issue: `.expect("refresh fixture batch construction is infallible")` is false: columns of unequal length make batch construction fail. The message then names neither the fixture key nor the Arrow error, so a typo in a fixture row list gives no hint which file broke.
- Fix: In `put_refresh_file` in crates/lakehouse-engine/tests/e2e_direct_storage_test.rs, replace the `.expect(...)` on the batch construction with `.unwrap_or_else(|e| panic!("build the refresh fixture batch for {key}: {e}"))`.

#### [SHRINKABLE] The A7 zero-row check hand-parses the Exasol response that `query_row_count` already parses
- Location: lines 2171-2181, `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`
- Issue: The A7 block calls `try_execute`, checks `status`, and reads `responseData.results[0].resultSet.numRows` by hand. `ExaConn::query_row_count` in tests/common/exasol_ws.rs owns that response path and already fails on a non-`ok` status. `zero_matching_files_prune_to_zero_rows_without_error` in the same file uses it for the same "zero rows without an error" claim. The copy puts the WebSocket response shape into a scenario-level test.
- Fix: In `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, replace the `emptied_resp` binding and its two `assert_eq!` calls with `assert_eq!(conn.query_row_count(&format!("SELECT ID FROM {emptied}")), 0, "A7: a table that lost its last data file returns zero rows without an error before REFRESH");`.

#### [UNTESTED_ERROR_PATH] A1, A3, and A6 accept any failure that echoes the identifier
- Location: lines 2116-2122 (A1), 2131-2137 (A3), 2158-2164 (A6)
- Issue: The scenario "Until a refresh, a query reads the current files under the declared columns" requires that a query naming the undeclared table, column, or partition key "SHALL fail in Exasol with an error stating that the object is not found". The test asserts only that the error contains `T_NEW`, `NEW_COL`, or `REGION`. Each SQL statement contains that identifier, so an unrelated failure whose message echoes the statement or the column also passes. Plan § Live Observations records the expected text as `object ... not found`.
- Fix: In `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, change the fragment lists of the three `assert_fails_naming` calls to `&["T_NEW", "not found"]` (A1), `&["NEW_COL", "not found"]` (A3), and `&["REGION", "not found"]` (A6).

#### [UNTESTED_ERROR_PATH] The A4 and A5 query errors are never checked for a credential value
- Location: lines 2147-2152 (A4 `SELECT ID, QTY`), 2277-2282 (A5 `for sql in [...]` loop)
- Issue: The scenario "Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value" ends with "no error message SHALL contain a credential value". It covers the A4 out-of-range error and both A5 query errors. The test discards the message `assert_fails_naming` returns for these three queries and checks only the REFRESH error at line 2290.
- Fix: In `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, bind the `String` returned by the A4 `assert_fails_naming` call for `SELECT ID, QTY` and by each call inside the A5 `for sql in [...]` loop. After each call, assert `!msg.contains(&secret)` with a message starting `A4:` or `A5:` that names the SQL. Use the `secret` binding from the [MAGIC_NUMBER] fix below.

#### [MAGIC_NUMBER] The leak check repeats the storage secret as a bare literal
- Location: line 2290
- Issue: `!msg.contains("lhadminsecret123")` copies the secret that `direct_storage_password()` sets at line 87. Nothing ties the two together. If the fixture secret changes, this assertion keeps passing without checking anything.
- Fix: In `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, add `let secret = direct_storage_password().secret_key;` before the first `assert_fails_naming` call, and replace `"lhadminsecret123"` at line 2290 with `&secret`. Leave the literal in the other tests of the file unchanged.

#### [REDUNDANT_COMMENT] The three new private helpers carry doc comments that restate their signatures
- Location: line 2031 (`assert_reads`), line 2045 (`assert_fails_naming`), line 2063 (`skip_reason`)
- Issue: AGENTS.md § Code style allows a comment only for a non-obvious reason and forbids restating the code. `/// Asserts the result columns of sql as text, NULL as None; a failure names case.`, `/// Runs sql, requires an error naming every fragment, and returns the message.`, and `/// The reason ADAPTER_NOTES records for a skipped table, or None when it is not skipped.` each describe what the private helper's name and signature already state.
- Fix: In crates/lakehouse-engine/tests/e2e_direct_storage_test.rs, delete the `///` doc comment line directly above `assert_reads`, `assert_fails_naming`, and `skip_reason`. Keep the `/// Scenario:` lines above the test function.

### docs/architecture.md

#### [OUTDATED_COMMENT] The CONNECTION does not name the catalog kind
- Location: line 32, second sentence of the paragraph below the flow diagram
- Issue: The new sentence says the format reader picks its source "depending on the catalog kind that the Virtual Schema CONNECTION names". The catalog kind is the virtual schema property `CATALOG_KIND`, as `docs/catalogs.md` and the mission glossary state. The CONNECTION holds the catalog address and the credentials, not the kind.
- Fix: In docs/architecture.md line 32, replace "depending on the catalog kind that the Virtual Schema CONNECTION names (see [Catalogs](catalogs.md))" with "depending on the virtual schema's `CATALOG_KIND` property (see [Catalogs](catalogs.md))".

### docs/catalogs.md

#### [OUTDATED_COMMENT] The wider-integer REFRESH row states `DECIMAL(20,0)` for every wider integer
- Location: line 615, "What a REFRESH changes" table, row "A new file that stores a column as a wider integer than the declaration"
- Issue: The row's first cell describes any wider integer and gives `INT64` over `DECIMAL(10,0)` as one example. The "After `REFRESH`" cell then states "The column is declared `DECIMAL(20,0)`" as the general result. The Parquet type table above maps `INT(16)` to `DECIMAL(5,0)`, so an `INT(16)` file over an `INT(8)` column declares `DECIMAL(5,0)`, not `DECIMAL(20,0)`.
- Fix: In docs/catalogs.md line 615, replace the "After `REFRESH`" cell "The column is declared `DECIMAL(20,0)`, and every value reads back" with "The column is declared at the wider integer's Exasol type, `DECIMAL(20,0)` for `INT64`, and every value reads back".

## Expert fixes
[none]
