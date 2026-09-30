# Code Review Findings: add-glue-catalog-kind

## Summary
- Files reviewed: 66 (the 68 listed paths, less the deleted `unity_parquet_format_reader.rs` and its `_tests.rs`)
- Total findings: 29 (standard: 29, expert: 0)
- `cargo clippy --all-targets --workspace` reports no warning. Every new production symbol has a caller. Every added `/// Scenario:` line quotes its spec title verbatim, one per test. No test that the plan requires to pass without edits was edited.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader.rs

#### [CONTEXTLESS_ERROR] An unparseable or undescribed catalog column's refusal names no column
- Location: lines 312-319 (`catalog_schema`)
- Issue: a `spark_field` error is stored verbatim as `RefusedColumn.reason`, for example `Hive type 'map<int>' is malformed: expected ',' before '>'`. `refused_columns::ensure_no_touched_column_is_refused` joins only the reasons, so a query that reads two columns fails with `pushdown request reads or emits column(s) this engine cannot render: Hive type 'map<int>' ...`, which does not say which column is refused. The classifier's own refusals do name the column (`Glue column 'b' has type 'binary' ...`). The test at `catalog_parquet_format_reader_tests.rs:596-605` shows the gap: `b` and `s` expect `"Glue column 'b'"`, but `u`, `i`, `m`, and `e` expect only the type string.
- Fix: In `catalog_schema` in crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader.rs, change the `Err(reason)` arm to push `RefusedColumn { column_name: column.name.clone(), reason: format!("{label} column '{}': {reason}", column.name) }`. In `binary_unrecognized_and_malformed_glue_types_refuse_only_their_column` (catalog_parquet_format_reader_tests.rs), add `"Glue column 'u'"`, `"Glue column 'i'"`, `"Glue column 'm'"`, and `"Glue column 'e'"` to the fragment lists of rows `u`, `i`, `m`, and `e`.

### crates/lakehouse-engine/src/adapter/mod.rs

#### [SWALLOWED_ERROR] A serialization failure silently zeroes the adapterNotes size base
- Location: line 621 (`build_adapter_notes`)
- Issue: `serde_json::to_string(&notes).map_or(0, |text| text.len())` replaces a serialization error with the length 0. `fit_skipped_tables` would then undercount every byte of the other notes, and the 2,000,000-byte cap would not hold.
- Fix: In `build_adapter_notes` in crates/lakehouse-engine/src/adapter/mod.rs, replace line 621 with `let base_len = Json::Object(notes.clone()).to_string().len();`. `Value`'s `Display` is infallible and is the same serialization as the final `Json::Object(notes).to_string()`.

#### [MIXED_ABSTRACTION_LEVEL] `build_adapter_notes` runs the skipped-list cap algorithm among plain field inserts
- Location: lines 610-629
- Issue: the function inserts nine budget fields and the table map, one `insert` each. It then runs the cap procedure inline: build the entries, insert an empty placeholder, remove the stale omitted count, measure, fit, re-insert, and conditionally insert the omitted count.
- Fix: Apply the SWALLOWED_ERROR fix above first. Then, in crates/lakehouse-engine/src/adapter/mod.rs, move lines 610-629 unchanged into a new `fn insert_skipped_tables(notes: &mut serde_json::Map<String, Json>, skipped: &[SkippedTable])`, placed directly above `fit_skipped_tables`. Replace them in `build_adapter_notes` with `insert_skipped_tables(&mut notes, skipped);`. Keep the signature of `build_adapter_notes`.

### crates/lakehouse-engine/src/adapter/connection.rs

#### [INFORMATION_LEAKAGE] The set of catalog-auth field names is written out twice
- Location: lines 143-149 (`validate_glue_preconditions`) and lines 177-181 (`validate_direct_storage_preconditions`)
- Issue: both validators hold the same five-entry table, `("token", creds.token.is_some())` through `("scope", creds.scope.is_some())`. A new catalog-auth field must be added to both, or one kind silently accepts it. The file already gives each field set a helper (`supplied_s3_fields`, `supplied_azure_fields`).
- Fix: In crates/lakehouse-engine/src/adapter/connection.rs, add `fn supplied_catalog_auth_fields(creds: &ConnectionCreds) -> Vec<&'static str>` beside `supplied_s3_fields`. It returns the five names in the current order, filtered like `supplied_s3_fields`.
  - In `validate_glue_preconditions`, set `let rejected = supplied_catalog_auth_fields(creds);`.
  - In `validate_direct_storage_preconditions`, build `rejected` from `warehouse` (when non-empty), then `supplied_catalog_auth_fields(creds)`, then `use_sigv4` and `use_vended_credentials` (when set). This keeps the current order of the error text.

### crates/lakehouse-catalog/src/glue/client_tests.rs

#### [NONDETERMINISTIC_TEST] The retry test sleeps through real backoff and asserts on wall-clock time
- Location: lines 866-894 (`throttling_and_503_are_retried_within_the_deadline`)
- Issue: the test uses the production `session(&mock)`, so the SDK's standard retry really sleeps between the three attempts. Then `let started = Instant::now(); ... assert!(started.elapsed() < OPERATION_TIMEOUT)` asserts on the real clock. The test takes seconds, and that assertion cannot realistically fail.
- Fix: In crates/lakehouse-catalog/src/glue/client_tests.rs, in this test, replace `session(&mock)` with `fast_retry_session(&mock)`. Delete `let started = Instant::now();` and the `started.elapsed() < OPERATION_TIMEOUT` assertion. Rename the test to `throttling_and_503_are_retried_until_success`. Keep its `/// Scenario:` line and the `requests_for("GetTables").len() == 3` assertion.

#### [UNTESTED_ERROR_PATH] The timeout, code-less, and remaining clock-skew error branches have no test
- Location: client.rs lines 223-227 (`SdkError::TimeoutError`), 412 (the `None` code arm, `HTTP {status}`), and 418-423 (`is_signing_time_rejection`). Only `Signature expired` is exercised.
- Issue: a grep of the tests finds none of `did not complete within`, `HTTP 503`, `RequestTimeTooSkewed`, `RequestExpired`, or `Signature not yet current`.
- Fix: In crates/lakehouse-catalog/src/glue/client_tests.rs, add three tests.
  - (a) `persistent_503_fails_naming_the_http_status`: a `fast_retry_session` over a mock that always answers `MockResponse::status(503)`. Assert that the `list_tables` error message contains `GetTables` and `HTTP 503`.
  - (b) `every_signing_time_rejection_names_the_clock`: assert that `is_signing_time_rejection` returns true for `("RequestTimeTooSkewed", "")`, `("RequestExpired", "")`, and `("InvalidSignatureException", "Signature not yet current: x")`. Assert that it returns false for `("InvalidSignatureException", "The request signature we calculated does not match")`.
  - (c) `an_operation_outliving_its_timeout_fails_naming_the_deadline`:
    - In crates/lakehouse-catalog/src/glue/mock_glue_tests.rs, add `MockResponse::hang()`. `serve` answers it by holding the connection open without writing.
    - Build the session with `GlueCatalogSession::from_config(glue_config(&mock.address, &creds, REGION.to_string()).timeout_config(TimeoutConfig::builder().operation_timeout(Duration::from_millis(200)).build()).build(), storage(&mock), &creds)`.
    - Assert that the `list_tables` error message contains `GetTables` and `did not complete within`.

#### [DUPLICATE_TEST] Two tests assert the same dropped-table planning load
- Location: lines 849-865 (`a_missing_table_names_the_table_and_states_it_does_not_exist`) and lines 1011-1042 (`the_planning_load_fails_for_a_dropped_or_no_longer_plannable_table`)
- Issue: both call `load_table_for_planning(&ident("orders"))` against a `catalog_of` without `orders`, and both assert `sales.orders` and `does not exist`.
- Fix: In crates/lakehouse-catalog/src/glue/client_tests.rs, delete `a_missing_table_names_the_table_and_states_it_does_not_exist`. In `the_planning_load_fails_for_a_dropped_or_no_longer_plannable_table`, add `assert!(dropped.contains("GetTable"), "{dropped}");` and `assert!(dropped.contains("EntityNotFoundException"), "{dropped}");`.

#### [VAGUE_TEST_NAME] The test name claims concurrency that the test never observes
- Location: line 439 (`iceberg_metadata_files_are_read_concurrently_in_listing_order`)
- Issue: the test asserts only the result order and one read per table. The mock answers instantly, so no concurrent read is ever observed.
- Fix: In crates/lakehouse-catalog/src/glue/client_tests.rs, rename the test to `iceberg_metadata_files_are_each_read_once_in_listing_order`.

### crates/lakehouse-catalog/src/glue/mock_glue_tests.rs

#### [SENTINEL_ERROR_VALUE] An empty string stands in for "no Glue operation"
- Location: lines 25-30 (`RecordedRequest::operation`)
- Issue: `operation()` returns `""` for a storage request, and callers rely on that value:
  - `client_tests.rs` calls `requests_for("")` at lines 322, 343, 351, 413, and 461, and matches `"" if request.method == "GET"` at line 197;
  - `read_glue_request` in `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs:399` does the same with `.unwrap_or_default()`.
- Fix:
  - In crates/lakehouse-catalog/src/glue/mock_glue_tests.rs, make `operation()` return `Option<&str>` by dropping `.unwrap_or("")`, and compare `Option`s in `serve`.
  - Add `MockGlue::storage_requests()`, which returns the recorded requests that carry no `X-Amz-Target`.
  - In crates/lakehouse-catalog/src/glue/client_tests.rs, replace each `requests_for("")` with `storage_requests()`. Change the line-197 arm to `None if request.method == "GET"`, and wrap the other arms of that match in `Some(..)`.
  - In crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs, make `read_glue_request` return `Option<(Option<String>, Json)>`, and compare against `Some(operation)` in `bodies_of`.

#### [SWALLOWED_ERROR] An unparseable request body becomes JSON null
- Location: lines 32-34 (`RecordedRequest::json`), and `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs:402`
- Issue: `serde_json::from_str(&self.body).unwrap_or(Value::Null)` and `from_slice(&body).unwrap_or(Value::Null)` turn a malformed body into `Null`. An absence assertion then passes on it, for example `body.get("CatalogId").is_none()` at `client_tests.rs:764`.
- Fix: In crates/lakehouse-catalog/src/glue/mock_glue_tests.rs and crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs, replace each `unwrap_or(serde_json::Value::Null)` / `unwrap_or(Value::Null)` on a request-body parse with `.expect("a Glue request body is JSON")`.

### crates/lakehouse-catalog/src/glue/routing_tests.rs

#### [MISSING_BOUNDARY_TEST] View matching is case-insensitive but only upper case is tested
- Location: lines 102-118 (`route_skips_a_view_naming_its_table_type_before_any_parameter`)
- Issue: `route` matches `VIRTUAL_VIEW` with `eq_ignore_ascii_case` (routing.rs:29), but every test passes `"VIRTUAL_VIEW"`.
- Fix: In crates/lakehouse-catalog/src/glue/routing_tests.rs, in this test, loop over `["VIRTUAL_VIEW", "virtual_view"]`, and for each `t` assert `route(Some(t), None, None) == skip(&format!("TableType={t}"))`.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [IMPLEMENTATION_COUPLED_TEST] The oversized-list test recomputes the production size formula
- Location: lines 1700-1709 (`an_oversized_skipped_list_is_capped_to_the_longest_prefix_that_fits_the_exasol_limit`)
- Issue: `with_one_more = text.len() + 1 + next_entry_len - omitted_entry_len(total - kept) + omitted_entry_len(total - kept - 1)` calls the private production helper `omitted_entry_len` and repeats the arithmetic of `fit_skipped_tables`. A bug in that formula therefore passes the test.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs, in this test, delete `next_entry_len` and the `with_one_more` arithmetic. Build `with_one_more` by serializing the notes with one more entry:
  - clone the parsed notes object;
  - push `json!({"table": catalog_identifier_string(&skipped[kept].ident), "reason": skip_reason(&skipped[kept])})` onto its `SKIPPED_TABLES` array;
  - set `SKIPPED_TABLES_OMITTED` to `json!((total - kept - 1).to_string())`, or remove that key when `total - kept - 1 == 0`;
  - take `.to_string().len()`, and keep the `> ADAPTER_NOTES_MAX_BYTES` assertion.

### crates/lakehouse-engine/src/adapter/connection_tests.rs

#### [VAGUE_TEST_NAME] The SigV4-fields test also asserts an unnamed second concept
- Location: lines 1545-1559 (`glue_connection_requires_the_sigv4_fields`)
- Issue: its second half asserts that a CONNECTION without `region` is accepted for the standard Glue address. That is a separate behavior the name does not state.
- Fix: In crates/lakehouse-engine/src/adapter/connection_tests.rs, move the second `password` / `ctx` / `read_connection(..).expect("the standard Glue address supplies the signing region")` block into a new test, `a_glue_connection_without_a_region_signs_for_the_standard_address_region`, with the same `/// Scenario:` line.

### crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs

#### [ASSERTION_FREE_TEST] The Glue reader-selection test passes whichever reader is selected
- Location: lines 193-219 (`format_reader_selects_readers_for_a_glue_table_by_format_without_contacting_the_catalog`)
- Issue: the Glue Iceberg and Parquet arms of `format_reader` both return `Ok(Box::new(..))`, so `assert!(selected.is_ok())` still passes if the two arms are swapped.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs:
  - Give `glue_table` the column `CatalogColumn { name: "id".into(), source_type: ColumnSourceType::Glue { hive_type: "int".into() } }`.
  - Make the test `#[tokio::test] async`, and build `storage` as `object_endpoint("bucket", vec![("sales/orders/part-0".to_string(), "rows".to_string())]).await`, imported from `test_support`.
  - Call `.resolve_scan(None).await` on each selected reader.
  - For `TableFormat::Parquet`, assert that the resolved file paths equal `["part-0"]`.
  - For `TableFormat::Iceberg`, assert that the result is an `Err` whose message contains `s3://bucket/sales/orders/metadata/v1.json`.
  - Keep the offline Glue session, so either arm fails if it contacts the catalog.

### crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs

#### [VAGUE_TEST_NAME] One test covers four behaviors under a name stating one
- Location: lines 919-1010 (`a_glue_iceberg_table_is_planned_from_its_metadata_file`)
- Issue: besides REST/metadata-file equivalence, the test asserts the date-promotion refusal, an unreadable metadata file, and an unset `metadata_location`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs, keep the equivalence, `table_root`, and schema-authority assertions in `a_glue_iceberg_table_is_planned_from_its_metadata_file`.
  - Move the `promoted` block into a new test, `the_date_promotion_refusal_applies_to_a_metadata_file_table`.
  - Move the `missing` and `unset` blocks into a new test, `an_unreadable_or_unset_metadata_location_fails_naming_the_table`.
  - Neither new test carries a `/// Scenario:` line.

#### [SHRINKABLE] This change adds another copy of the `user_message` test helper
- Location: line 910 (`fn user_message`)
- Issue: the helper is identical to `delta_schema_tests.rs:18` and to at least three other copies in the engine's tests.
- Fix: Add `pub(super) fn user_message(error: UdfError) -> String` (same body) to crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs. Delete the local copies in crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs and crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs, and import `crate::adapter::pushdown::test_support::user_message` in both. Leave the copies in unchanged files alone.

### crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs

#### [SHRINKABLE] The Iceberg loadTable document is written out by hand twice
- Location: lines 2063-2092 (`binary_iceberg_table_body`)
- Issue: the body is the same document that `load_table_body_with_columns(fields, last_column_id)` (iceberg_tests.rs:1114, added in this change) builds. Only the table UUID differs.
- Fix: Move `load_table_body_with_columns` from crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs to crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs as `pub(super)`, and import it in iceberg_tests.rs. In crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs, make `binary_iceberg_table_body()` return `load_table_body_with_columns(<its current fields array as a json! value>, 6)`.

### crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs

#### [MISSING_BOUNDARY_TEST] The first-dot split is never tested with a dotted table name
- Location: line 97 (`a_recorded_glue_identifier_splits_at_the_first_dot_and_refuses_an_empty_part`)
- Issue: only `"sales.orders"` is tested, so the first-dot rule of `glue_table_ident` (scan_resolution.rs:229-244) is never exercised.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs, in this test, assert that `glue_table_ident("sales.orders.v2")` returns `namespace == vec!["sales".to_string()]` and `name == "orders.v2"`.

### crates/lakehouse-engine/src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs

#### [REDUNDANT_COMMENT] Doc comments on private test helpers restate what the code does
- Location:
  - catalog_parquet_format_reader_tests.rs: line 431 (`delta_classification`), 643 (`partitions_page`), 686 (`scanned_key`), 888 (`ListingGauge`);
  - `crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs:306-307` (`GlueEndpoint`);
  - `crates/lakehouse-catalog/src/glue/client_tests.rs:181-182` (`catalog_of`).
- Issue: each comment describes what the helper does, not a non-obvious why (CLAUDE.md § Code comment style), for example `/// The object key the scan reads for \`entry\`.`
- Fix: Delete those six doc comments, at the lines listed in each of the three files.

### crates/lakehouse-engine/tests/e2e_glue_test.rs

#### [DUPLICATE_TEST] The absent-variable half repeats `missing_glue_variable_fails_loud`
- Location: lines 792-800 (in `glue_suite_fails_when_stack_unavailable`)
- Issue: the `GlueEnv::from_lookup(|_| None)` block repeats `missing_glue_variable_fails_loud` (`common/glue.rs:1468`). That test runs in the same binary and covers all four variables, three absent forms each, and the no-value-echo check.
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs, delete lines 792-800. Keep the `ACCESS_KEY_ID_VAR` import, which line 812 still uses.

#### [SWALLOWED_ERROR] A GetDatabase or S3-list error turns into a misleading assertion failure
- Location: lines 241-252
- Issue: `.unwrap_or(false)` on `env.database_exists(..)` and `.unwrap_or_default()` on `env.object_keys(..)` drop the AWS error. The test then fails with "must exist" or "must have written an object".
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs, replace `.unwrap_or(false)` with `.unwrap_or_else(|e| panic!("{}", env.redact(&format!("check {}: {e:#}", run.database()))))`. Replace `.unwrap_or_default()` with `.unwrap_or_else(|e| panic!("{}", env.redact(&format!("list {}: {e:#}", run.object_prefix()))))`.

#### [CONTEXTLESS_ERROR] An AWS SDK error is printed with `Display`, which drops its code and message
- Location: line 300 (`GetTables`) and line 412 (`GetPartitions`)
- Issue: `format!("GetTables {database}: {e}")` and `format!("GetPartitions: {e}")` print only "service error". The harness's own `GlueEnv::glue_failure` (`common/glue.rs:175`) uses `DisplayErrorContext` for this reason.
- Fix: In crates/lakehouse-engine/tests/common/glue.rs, make `GlueEnv::glue_failure` `pub`. In crates/lakehouse-engine/tests/e2e_glue_test.rs:
  - make line 300's closure `panic!("{}", env.glue_failure(&format!("GetTables {database}"), &e))`;
  - make line 412's closure `panic!("{}", env.glue_failure(&format!("GetPartitions {database}.{PARTITIONED}"), &e))`.

#### [SWALLOWED_ERROR] The unpushed oracle turns an unparsable amount into 0.0
- Location: lines 602-605
- Issue: `value_to_string(&full[2][i]).parse::<f64>().unwrap_or(0.0) > 20.0` silently zeroes a bad value. It also derives the oracle from a pushed query result rather than from the fixture.
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs, replace lines 602-605 with `let unpushed_filtered: Vec<i64> = ORDERS.iter().filter(|o| o.amount_hundredths > 2000).map(|o| o.order_id).collect();`.

#### [SWALLOWED_ERROR] Cleanup and setup calls discard their failure
- Location: e2e_glue_test.rs lines 122, 849, and 858; `crates/lakehouse-engine/tests/common/type_matrix.rs:1482`
- Issue: `let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS ...")`, `let _ = conn.try_execute("DROP CONNECTION IF EXISTS ...")`, and `let _ = catalog.create_namespace(...)` discard errors that should never occur. The drops use `IF EXISTS`, and the namespace call runs only after `namespace_exists` returned false.
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs, replace `let _ = conn.try_execute(` with `conn.execute(` at lines 122, 849, and 858. `ExaConn::execute` asserts `status == "ok"`. In crates/lakehouse-engine/tests/common/type_matrix.rs line 1482, replace the statement with `catalog.create_namespace(&namespace, HashMap::new()).await.context("create the type-matrix namespace")?;`.

#### [SHRINKABLE] `GlueFixture` holds the environment twice
- Location: lines 59-62
- Issue: `GlueFixture { env: GlueEnv, run: GlueRun }` duplicates `env`. `GlueRun::create(&env)` already stores a clone, which `GlueRun::env()` (`common/glue.rs:276`) exposes.
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs, remove the `env` field and return `Self { run }` from `register`. Replace `fixture.env` with `fixture.run.env()` at lines 89 and 727. At line 294, write `let (env, run) = (fixture.run.env(), &fixture.run);`.

### crates/lakehouse-engine/tests/common/glue.rs

#### [INFORMATION_LEAKAGE] Each Hive column's data lives in a name-keyed match apart from the row it belongs to
- Location: lines 1066-1174 (`hive_type_column_data`), against `HIVE_TYPE_COLUMNS` at lines 549-636
- Issue: a column's data is found by matching its name string in a separate function. The fallback `_ => return Ok(None)` writes no data for an unknown name, so a new `Outcome::Values` row, or a typo in its name, reads NULLs and fails with a misleading values mismatch.
- Fix: In crates/lakehouse-engine/tests/common/glue.rs:
  - Add `data: Option<fn() -> Result<(DataType, ArrayRef)>>` to `HiveTypeColumn`.
  - Move each arm of `hive_type_column_data` into a named `fn h_<name>_data() -> Result<(DataType, ArrayRef)>`, and set it as that row's `data`. Set `data: None` on the five `Outcome::Refused` rows.
  - Delete `hive_type_column_data`. In `register_all_types`, use `if let Some(data) = hive_type_column.data { let (data_type, values) = data()?; ... }`.

#### [TOO_MANY_ARGUMENTS] Positional fixture constructors take four or five arguments
- Location: common/glue.rs line 511 (`order`, 4 arguments), 532 (`hive`, 4), and 656 (`partitioned_row`, 5); `crates/lakehouse-engine/tests/common/type_matrix.rs:144` (`written_in`, 4)
- Issue: each function only repeats a struct literal positionally. `PARTITIONS` in the same file already uses struct literals.
- Fix: In crates/lakehouse-engine/tests/common/glue.rs, delete `order`, `hive`, and `partitioned_row`, and write `ORDERS`, `HIVE_TYPE_COLUMNS`, and `PARTITIONED_ROWS` as struct literals. For example: `Order { order_id: 1, customer: Some("ada"), amount_hundredths: 1225, order_date: "2024-01-01" }` and `PartitionedRow { id: 1, v: "a", p_int: 1, p_date: "2024-01-01", p_str: Some("alpha") }`. In crates/lakehouse-engine/tests/common/type_matrix.rs, delete `written_in`, and replace its seven `MATRIX` call sites with `Cell::Written { table, source_type, exasol_type, outcome }` literals.

### crates/lakehouse-engine/tests/common/type_matrix.rs

#### [MAGIC_NUMBER] Iceberg field ids use bare offsets
- Location: lines 1447-1452 (`iceberg_schema`)
- Issue: field ids are computed as `2 + index` and `100 + 10 * index`.
- Fix: In crates/lakehouse-engine/tests/common/type_matrix.rs, add the module-scope constants `const FIRST_COLUMN_FIELD_ID: i32 = 2;`, `const FIRST_NESTED_FIELD_ID: i32 = 100;`, and `const NESTED_FIELD_IDS_PER_COLUMN: i32 = 10;`. Use `FIRST_COLUMN_FIELD_ID + index` and `FIRST_NESTED_FIELD_ID + NESTED_FIELD_IDS_PER_COLUMN * index`.

### .github/workflows/glue-orphan-sweep.yml

#### [OUTDATED_COMMENT] The capture comment misstates how a failed aws call is caught
- Location: line 62
- Issue: the comment `# Captured to a variable (not a pipe) so a failed aws call trips set -e.` is wrong. Each capture ends with `|| { echo ... >&2; exit 1; }`, which suspends `set -e` for that command, so the explicit handler catches the failure.
- Fix: In .github/workflows/glue-orphan-sweep.yml, replace the line-62 comment with `# Captured to a variable (not a pipe) so the aws exit status reaches the explicit || handler.`

## Expert fixes
[none]
