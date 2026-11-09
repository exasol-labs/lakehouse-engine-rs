# Code Review Findings: add-export-iceberg-rest-write

## Summary
- Files reviewed: 75
- Total findings: 21 (standard: 17, expert: 4)

## Standard fixes

### crates/lakehouse-catalog-write/src/vended_refresh.rs

#### [UNREACHABLE_CODE] refresh_decision repeats the holder's skew test and handles an absent expiry that never reaches it
- Location: lines 44 to 71 (`refresh_decision`, `skew_ms`), line 147 (`decide`)
- Issue: The only production caller is `decide`, which `ExpiringCredential::get_or_keep` calls only for a lease that has an expiry and is within `RENEWAL_SKEW` of it. `decide` always passes `Some(point.expires_at_ms)`, so the `None => Keep` arm never runs. The `expires_at.saturating_sub(now) > skew_ms()` arm repeats the holder's `is_due` test with the same skew, so it never returns `Keep` either. The function also takes 4 arguments, 3 of which are the fields of the `RenewalPoint` that `decide` already holds. Only the test table reaches the two dead arms.
- Fix: In crates/lakehouse-catalog-write/src/vended_refresh.rs change `refresh_decision` to `pub(crate) fn refresh_decision(point: RenewalPoint, endpoint_offered: bool) -> RefreshDecision`. Delete its `let Some(expires_at) = expires_at else` arm, its `expires_at.saturating_sub(now) > skew_ms()` arm, and the function `skew_ms`. Rewrite its doc comment to state that it runs only at the holder's renewal point. Make `decide` pass `point` and `source.endpoint_offered`. In crates/lakehouse-catalog-write/src/vended_refresh_tests.rs rewrite the case table of `the_decision_keeps_renews_or_fails_by_expiry_endpoint_and_last_renewal` as `(RenewalPoint, bool, RefreshDecision)` rows, and delete the two rows with an expiry of `None` and the two rows at `expires_at - 5 * MINUTE_MS - 1`.

### crates/lakehouse-catalog-write/src/iceberg_write.rs

#### [SHRINKABLE] stage_table repeats the status classification and cause text of send
- Location: lines 283 to 312 (`stage_table`), lines 509 to 538 (`send`, `classify`)
- Issue: `stage_table` calls `self.session.send(Method::POST, ...)` directly, checks `(200..300)` itself, and formats `"the catalog {} answered HTTP {}: {}"` itself. `send` and `classify` already do both, so the cause text of a catalog answer is written twice in this file.
- Fix: In crates/lakehouse-catalog-write/src/iceberg_write.rs `stage_table`, replace the direct `self.session.send(...)` call and its status check with `self.send(&tables_path(&table.namespace), &body, RepeatSafety::Safe).await`. Continue with the body of `Sent::Applied(answer)`, and return `staged_creation_error(table, cause)` for every other `Sent` variant.

#### [MISSING_DOC_COMMENT] Public accessors and fields of the write crate carry no doc comment
- Location: line 59 and line 60 (`CommitTag::export_id`, `CommitTag::writer_id`), line 97 (`WriteTable::table`), line 112 (`StagedTable::table`)
- Issue: These public items of the crate's API have no doc comment.
- Fix: In crates/lakehouse-catalog-write/src/iceberg_write.rs add a one-line doc comment to `CommitTag::export_id` ("The export statement's ID, the `lakehouse.export-id` of every snapshot it commits."), to `CommitTag::writer_id` ("The writer's own ID, the `lakehouse.export-writer` of its snapshot."), to `WriteTable::table` ("The iceberg table, whose FileIO signs with the table's storage credentials."), and to `StagedTable::table` ("The staged table, which the catalog does not list until `commit_staged` creates it.").

#### [INFORMATION_LEAKAGE] The Iceberg REST table routes are built in both catalog crates
- Location: lines 688 to 694 (`tables_path`, `table_path`); crates/lakehouse-catalog/src/session.rs lines 32 to 38 (`table_path`, `ident_path`)
- Issue: The route layout `namespaces/{namespace}/tables` and `namespaces/{namespace}/tables/{table}` is written in the write crate and again in the session of the catalog crate. A change of the layout, such as a different namespace encoding, needs an edit in both crates.
- Fix: In crates/lakehouse-catalog/src/session.rs add `pub fn namespace_tables_path(namespace: &NamespaceIdent) -> String` and rename `ident_path` to `pub fn table_ident_path(table: &TableIdent) -> String`, each with a doc comment that names its Iceberg REST route, and build both on the private `table_path`. Export both from crates/lakehouse-catalog/src/lib.rs. In crates/lakehouse-catalog-write/src/iceberg_write.rs delete `tables_path` and `table_path` and call the two exported functions instead.

### crates/lakehouse-catalog-write/src/lib.rs

#### [SHRINKABLE] Three copies of dotted reimplement the Display of TableIdent
- Location: crates/lakehouse-catalog-write/src/lib.rs line 23; crates/lakehouse-catalog/src/session.rs line 40; crates/lakehouse-engine/src/export/iceberg_sink.rs line 213
- Issue: Each `dotted` formats `{namespace joined by "."}.{name}`. iceberg-rust's `impl Display for TableIdent` (crates/iceberg/src/catalog/mod.rs at rev `da5cc19`) writes exactly that string, and `impl Display for NamespaceIdent` writes the joined namespace.
- Fix: Delete `dotted` from crates/lakehouse-catalog-write/src/lib.rs, crates/lakehouse-catalog/src/session.rs, and crates/lakehouse-engine/src/export/iceberg_sink.rs. Remove `use crate::dotted;` from crates/lakehouse-catalog-write/src/iceberg_write.rs and crates/lakehouse-catalog-write/src/vended_refresh.rs. Format the identifier itself at every former call site: `dotted(table)` becomes `table`, and in iceberg_sink.rs `dotted(table)` becomes `table.identifier()`. In `create_namespace_level` of iceberg_write.rs replace each `level.join(".")` with `level`.

### crates/lakehouse-catalog-write/src/snapshot_capture.rs

#### [UNMEASURED_OPTIMIZATION] The duplicate check is switched off for a cost nobody measured
- Location: lines 37 to 41 (`.with_check_duplicate(false)` and its comment)
- Issue: The comment justifies switching off iceberg-rust's duplicate data-file check by the manifests it would re-read on each attempt. No measurement backs that cost. plan.md § Context records the 300-writer measurement of task 3.3 for the reload order only.
- Fix: Run the 300-writer export of task 3.3 on the Docker Exasol once with `.with_check_duplicate(false)` and once with that call removed, and record both statement durations in specs/_plans/add-export-iceberg-rest-write/plan.md § Context. When the two durations do not differ, delete the call and its comment from crates/lakehouse-catalog-write/src/snapshot_capture.rs. Otherwise keep the call and add the measured durations to its comment.

### crates/lakehouse-catalog-write/src/retry.rs

#### [MAGIC_NUMBER] backoff_wait caps its doublings with a bare 31
- Location: line 28
- Issue: `.min(31)` keeps `1 << doublings` from overflowing `u32`. The literal states no name and no reason.
- Fix: In crates/lakehouse-catalog-write/src/retry.rs `backoff_wait`, replace `let doublings = retry.saturating_sub(1).min(31);` and `MIN_WAIT.saturating_mul(1 << doublings)` with `let factor = 2_u32.saturating_pow(retry.saturating_sub(1));` and `MIN_WAIT.saturating_mul(factor)`. Keep `the_wait_doubles_from_100_ms_and_stops_at_60_s` passing unchanged.

### crates/lakehouse-engine/src/export/writer.rs

#### [MAGIC_NUMBER] The writer ID length is a bare 8
- Location: line 31 (`writer_id: random_hex::<8>()`)
- Issue: The export ID has a named function, `new_export_id`, with a doc comment that states its 16 bytes. The writer ID inlines its byte count.
- Fix: In crates/lakehouse-engine/src/export/mod.rs add `fn new_writer_id() -> String` next to `new_export_id`, with the doc comment "8 random bytes in hex, drawn once per writer invocation.", returning `random_hex::<8>()`. In crates/lakehouse-engine/src/export/writer.rs call `new_writer_id()` and drop `random_hex` from the `use super::{...}` import.

### crates/lakehouse-engine/src/export/target.rs

#### [BOOLEAN_FLAG_PARAMETER] open_table selects creation or staging by a bool
- Location: lines 33 to 38 (`ExportTarget::open_table`); crates/lakehouse-engine/src/export/iceberg_rest.rs lines 68 to 94
- Issue: `single_commit: bool` picks one of two branches inside `IcebergRestTarget::open_table`: stage the table or create it.
- Fix: In crates/lakehouse-engine/src/export/target.rs replace `open_table(&self, schema, single_commit, tag)` with `open_table(&self, schema: &Schema, tag: &WriterTag)`, which creates the table or takes the one another writer of the export created, and `stage_table(&self, schema: &Schema, tag: &WriterTag)`, which prepares a table that appears only with the sink's data, each with its own doc comment. Implement both in crates/lakehouse-engine/src/export/iceberg_rest.rs and share the session resolution and the `IcebergSink::open` call through a private helper. In crates/lakehouse-engine/src/export/writer.rs `write_group`, call `stage_table` when `spec.single_commit` is true and `open_table` otherwise. In crates/lakehouse-engine/src/export/writer_tests.rs make `FakeTarget` record `stage_table` and `open_table` as separate calls and update the expected call lists.

#### [SHRINKABLE] is_plain_http duplicates scheme_of
- Location: line 147 (`is_plain_http`), line 137 (its caller)
- Issue: `is_plain_http` splits at `://` and compares the scheme case-insensitively. `lakehouse_catalog::scheme_of` already returns the lowercased scheme, and crates/lakehouse-catalog-write/src/vended_refresh.rs uses it for the same test.
- Fix: In crates/lakehouse-engine/src/export/target.rs delete `is_plain_http` and test each field with `scheme_of(value) == "http"`, importing `scheme_of` from `lakehouse_catalog`.

### crates/lakehouse-engine/src/export/arrow_schema.rs

#### [INFORMATION_LEAKAGE] The export reads Exasol's type_name spelling outside the one column-metadata reader
- Location: lines 50 to 71 (`spelled_as_measured`), line 24 (`arrow_type`)
- Issue: Decision [8] makes `exasol_type_from_column_info` in vs-expression the one reader of `ColumnInfo`. `spelled_as_measured` holds a second copy of Exasol's `type_name` spelling: `DECIMAL(p,s)`, `VARCHAR(n) UTF8`, ` ASCII`, and the bare `TIMESTAMP` of precision 3 that the reader also decodes. A change of the reported spelling needs an edit in both crates.
- Fix: Move `spelled_as_measured` from crates/lakehouse-engine/src/export/arrow_schema.rs into crates/vs-expression/src/exasol_type.rs as `pub fn is_reported_spelling(exasol_type: ExasolType, type_name: &str) -> bool`, with a doc comment stating that it tells whether `type_name` is the spelling Exasol reports for `exasol_type`. Add its cases to crates/vs-expression/src/exasol_type_tests.rs as one case table: each measured spelling yields true, and `TIMESTAMP(3) WITH LOCAL TIME ZONE` against `ExasolType::timestamp(3)` yields false. Make `arrow_type` call `is_reported_spelling`.

### crates/lakehouse-engine/src/export/iceberg_rest.rs

#### [INFORMATION_LEAKAGE] The refusal of an existing table is written in two crates
- Location: lines 57 to 66 (`prepare`), line 30 (`table` field); crates/lakehouse-catalog-write/src/iceberg_write.rs lines 226 to 228 (`table_exists`) and lines 483 to 486 (`created_by_export`)
- Issue: `prepare` and `created_by_export` each format "table {} already exists; the export only creates new tables". `TableWriteSession::table_exists` is a pass-through of `CatalogSession::table_exists` that only `prepare` calls, and the `table` field of `IcebergRestTarget` exists only for this message.
- Fix: In crates/lakehouse-catalog-write/src/iceberg_write.rs add a private `fn existing_table_error(table: &TableIdent) -> UdfError` with that message, and `pub async fn refuse_existing_table(&self, table: &TableIdent) -> Result<(), UdfError>` with a doc comment, which returns `existing_table_error(table)` when `self.session.table_exists(table)` is true. Make `created_by_export` return `existing_table_error(table)`. Delete `TableWriteSession::table_exists`. In crates/lakehouse-engine/src/export/iceberg_rest.rs make `prepare` call `session.refuse_existing_table(&self.ident).await?` before `ensure_namespace`, and delete the `table` field and its initialization in `target`.

### crates/lakehouse-catalog/src/test_support_tests.rs

#### [SHRINKABLE] 31 per-item cfg(test) attributes keep crate-only helpers out of the feature build
- Location: lines 4 to 13, every `#[cfg(test)]` up to line 282, lines 244 to 247 (`Layer` carries the attribute twice), line 199 (`read_request`), line 227 (`header_value`)
- Issue: The module turned `pub` behind the `test-support` feature, so every helper that the write crate does not use got its own `#[cfg(test)]`. AGENTS.md places test helpers in the `_tests.rs` file without `#[cfg(test)]`. `Layer` carries the attribute twice. `read_request` and `header_value` became `pub`, although no other crate uses them. vs-expression shows the intended shape: a separate file per exported support module (`udf_column_test_support_tests.rs`).
- Fix: Move `spawn_server`, `spawn_server_with_headers`, `request_body`, and `read_request` into a new file crates/lakehouse-catalog/src/exported_test_support_tests.rs that opens with the doc comment "The test helpers that dependent crates enable through the `test-support` feature: a fake HTTP server and a movable epoch clock.". Declare it in crates/lakehouse-catalog/src/lib.rs as `#[cfg(any(test, feature = "test-support"))] #[path = "exported_test_support_tests.rs"] pub mod exported_test_support;`, keep `read_request` `pub(crate)`, and add a doc comment to every `pub` item. Declare `test_support` as `#[cfg(test)] #[path = "test_support_tests.rs"] mod test_support;` again, delete every `#[cfg(test)]` inside test_support_tests.rs, make `header_value` `pub(crate)`, and add `pub(crate) use crate::exported_test_support::{read_request, request_body, spawn_server, spawn_server_with_headers};` so the existing imports keep working. In crates/lakehouse-catalog-write/src/test_support_tests.rs and crates/lakehouse-catalog-write/src/iceberg_write_tests.rs import from `lakehouse_catalog::exported_test_support`.

### crates/lakehouse-catalog-write/src/vended_refresh_tests.rs

#### [SHRINKABLE] Test helpers and fixtures are copied between test files
- Location: crates/lakehouse-catalog-write/src/vended_refresh_tests.rs line 9 (`STAGED_LOCATION`), line 22 (`SILENT_CONFIG`), line 64 (`clock_at`); crates/lakehouse-catalog-write/src/iceberg_write_tests.rs line 17 (`STAGED_LOCATION`); crates/lakehouse-catalog/src/expiring_tests.rs line 8 (`clock_reading`); crates/lakehouse-catalog/src/session_tests.rs line 1035 (`clock_reading`)
- Issue: `specs/testing.md` § Test code requires the change that writes a second copy of a helper to extract it. `STAGED_LOCATION` is defined twice in the write crate. `SILENT_CONFIG` is the same string as `PREFIX_CONFIG` of the write crate's test_support_tests.rs. `clock_reading` is defined twice in the catalog crate, and `clock_at` in the write crate is a third copy of it.
- Fix: Move `clock_reading` into crates/lakehouse-catalog/src/exported_test_support_tests.rs (created by the fix of test_support_tests.rs above) as `pub fn clock_reading(time: &Arc<AtomicU64>) -> EpochClock` with a doc comment, and delete the copies in expiring_tests.rs and session_tests.rs and `clock_at` in vended_refresh_tests.rs. Move `STAGED_LOCATION` into crates/lakehouse-catalog-write/src/test_support_tests.rs and delete both copies. Delete `SILENT_CONFIG` and use `PREFIX_CONFIG` in its place.

### crates/lakehouse-catalog/src/catalog_http_tests.rs

#### [DUPLICATE_TEST] The state-changing path loop runs one test three times
- Location: line 838 (`STATE_CHANGING_PATHS`), line 859 and line 888 (the two loops)
- Issue: `CatalogHttp` never reads the path, so each of the three iterations exercises the same code with the same outcome. The three request kinds are distinguished by the callers that mark them `RepeatSafety::Unsafe`, and iceberg_write_tests.rs covers those callers.
- Fix: In crates/lakehouse-catalog/src/catalog_http_tests.rs delete `STATE_CHANGING_PATHS` and both `for path in STATE_CHANGING_PATHS` loops. Send each case of `only_a_request_safe_to_repeat_is_sent_again_after_a_504` and `a_request_unsafe_to_repeat_is_sent_once_after_a_timeout` to the one path `/v1/wh/namespaces/db/tables/t`.

### crates/lakehouse-e2e/tests/e2e_local/export.rs

#### [TACTICAL_SHORTCUT] The second export of the stage-create test proves nothing about a catalog that refuses staged creation
- Location: lines 623 to 631 (`accepted_table` and its export)
- Issue: The scenario step "the same export without SINGLE_COMMIT SHALL succeed" concerns a catalog that refuses staged creation. The test exports into another namespace that the proxy filter does not cover, so the catalog refuses nothing there, and the assertion passes for any working export.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/export.rs `export_single_commit_on_catalog_refusing_stage_create_fails`, delete the comment at line 623 and the export into `accepted_table`. In specs/_plans/add-export-iceberg-rest-write/plan.md § Verification › Scenario Coverage, add to the row "SINGLE_COMMIT on a catalog without staged creation fails and creates no table" the unit test `create_table_sends_a_format_2_table_tagged_with_the_export_id` in crates/lakehouse-catalog-write/src/iceberg_write_tests.rs, which proves that an export without SINGLE_COMMIT never asks for staged creation.

### crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs

#### [OUTDATED_COMMENT] The module doc names a function this change removed
- Location: line 9
- Issue: The doc names "the adapter's own `oauth2_client_credentials_grant`". This change replaced that function with `ClientCredentialsGrant` in crates/lakehouse-catalog/src/auth.rs.
- Fix: In crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs line 9 replace `oauth2_client_credentials_grant` with `ClientCredentialsGrant`.

## Expert fixes

### crates/lakehouse-catalog-write/src/iceberg_write.rs

#### [BROAD_CATCH] Sent::Failed treats a request that never reached the catalog as a permanent refusal
- Location: lines 176 to 177 (`Sent::Failed`), line 517 (`send`), line 272 (`create_table`), line 449 (`create_namespace_level`), line 429 (`send_commit`)
- Issue: `send` maps `CatalogSendError::Unsent`, a refused or failed connection, to `Sent::Failed`, the same arm as a 4xx answer, and every operation fails at once on it. `CatalogHttp` sends an unsafe request once, so nobody retries it. The catalog-http-resilience Background classes a refused connection as a transient failure, and the catalog provably never saw the request, so a repeat is the safest case. A refused connection during a 300-writer commit burst or a proxy reload therefore fails the writer, and the export, without a retry. The doc comment "a repeat fails alike" is false for this case.
- Fix: In crates/lakehouse-catalog-write/src/iceberg_write.rs split `Sent::Failed` into `Sent::Refused(String)`, for an HTTP answer outside 2xx, 409, 429, 500, 502, 503, and 504, and `Sent::Unsent(String)`, for `CatalogSendError::Unsent`. Treat `Sent::Unsent` as a retryable failure whose outcome is known: in `create_namespace_level` and `create_table` give it the arm of `Sent::Busy`, and in `send_commit` map it to `retry(cause, false)`. Rewrite the doc comment of each variant to match. Add a fault-injection test to crates/lakehouse-catalog-write/src/iceberg_write_tests.rs whose fake catalog answers `/v1/config`, closes that connection, and then closes its listener, so every later request meets a refused connection. Assert that `create_table` fails with `creating table ns.t failed after 5 attempts`.

### crates/lakehouse-catalog/src/session.rs

#### [LEAKED_BOUNDARY_TYPE] The public request method takes reqwest's Method and also serves reads
- Location: line 227 (`CatalogSession::send`); crates/lakehouse-catalog/src/lib.rs line 44 (`pub use reqwest::Method;`)
- Issue: `send` accepts any `reqwest::Method`, so the catalog crate re-exports a reqwest type and the write crate names it. Decision [10] makes the public method the way to send a state-changing request, and keeps reads in the catalog crate so that the method is no back door for reads. The method accepts `GET` and `HEAD` all the same, and the crate itself uses it for `table_exists` and `load_credentials`. The write crate sends only `POST`.
- Fix: In crates/lakehouse-catalog/src/session.rs make `send(method, path, body, safety)` `pub(crate)`, and add `pub async fn post(&self, path: &str, body: &serde_json::Value, safety: RepeatSafety) -> Result<CatalogResponse, CatalogSendError>`, which calls `self.send(Method::POST, path, Some(body), safety)`. Give `post` the doc comment of today's `send`, narrowed to the state-changing POST of decision [10]. Remove `pub use reqwest::Method;` from crates/lakehouse-catalog/src/lib.rs. In crates/lakehouse-catalog-write/src/iceberg_write.rs replace every `self.session.send(Method::POST, ...)` with `self.session.post(...)` and drop `Method` from the import. In crates/lakehouse-catalog/src/session_tests.rs call `post` in the tests of the public method.

### crates/lakehouse-catalog/src/vended.rs

#### [BOUNDARY_VIOLATION] The demoted credential-source selection is public again under a new name
- Location: line 51 (`pub fn vended_credential_config`); crates/lakehouse-catalog/src/lib.rs line 49; crates/lakehouse-catalog-write/src/iceberg_write.rs line 592; crates/lakehouse-catalog-write/src/vended_refresh.rs lines 27 to 31 and line 228 (`vended_lease`)
- Issue: The recorded scenario "One concept-level call resolves the effective scan storage from a loadTable response" (specs/storage-access/pushdown-planning-cloud-credentials-vended-storage/spec.md) requires that `resolve_vended_storage` stay the only public Iceberg-path vended entry point and that every mechanism step stay crate-private. This change renamed the private `select_credential_source` to `vended_credential_config` and made it `pub`. The guard test `demoted_and_deleted_functions_are_not_declared_public` (crates/lakehouse-catalog/tests/it/catalog_public_surface.rs) lists only the old name, so it still passes. No spec delta of this plan records an exception. The write crate then parses the returned map with its own copies of the keys `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, and `s3.endpoint`, which `iceberg_vended_s3` in vended.rs also reads, so the vended config format is known in two crates. `storage_of` also selects the credential source twice, once directly and once inside `resolve_vended_storage`.
- Fix: In crates/lakehouse-catalog/src/vended.rs make `vended_credential_config` `pub(crate)` and remove it from the `pub use vended::{...}` line of crates/lakehouse-catalog/src/lib.rs. Add `"pub fn vended_credential_config"` to the list in `demoted_and_deleted_functions_are_not_declared_public`. In vended.rs add `pub struct VendedS3Credentials { pub access_key_id: String, pub secret_access_key: String, pub session_token: Option<String>, pub endpoint: Option<String>, pub expires_at_ms: Option<u64> }` with a doc comment and a `Debug` impl that prints no field value. Build it in one crate-private parser that reuses the key reads of `iceberg_vended_s3` and `required_vended_value` and adds the parse of `s3.session-token-expires-at-ms`, whose error names the key and no value. Expose it as `pub fn vended_s3_credentials(answer: &LoadTableResult, location: &str) -> Result<VendedS3Credentials, UdfError>`, and make `CatalogSession::load_credentials` return `Result<VendedS3Credentials, UdfError>` through the same parser. In crates/lakehouse-catalog-write/src/vended_refresh.rs delete the constants `ACCESS_KEY_ID` to `ENDPOINT` and `non_empty`, build the `Lease<AwsCredential>` of `vended_lease` from `VendedS3Credentials`, keep `refuse_plaintext_endpoint` on its `endpoint` field, and update the message assertions in vended_refresh_tests.rs and session_tests.rs. In crates/lakehouse-catalog-write/src/iceberg_write.rs `storage_of`, seed the source from `vended_s3_credentials`. Record the two public vended reads as a scoped exception in a new CHANGED delta specs/_plans/add-export-iceberg-rest-write/storage-access/pushdown-planning-cloud-credentials-vended-storage/spec.md that amends the last step of that scenario, and add its row to plan.md § Features.

### crates/lakehouse-engine/src/export/iceberg_sink.rs

#### [UNTESTED_ERROR_PATH] No test proves that abort keeps the data files after a commit with an unknown outcome
- Location: line 147 (`self.files_may_be_committed = error.outcome_unknown();`), lines 173 to 183 (`abort`)
- Issue: Decision [2] requires that a writer whose last commit outcome is unknown keeps its data files, because the table may hold them. Deleting them would leave a committed snapshot that points at missing files. The sink has no test file, the writer tests use a fake sink, and decision [5] states that no E2E stack can produce an unknown commit outcome. A regression in this flag would pass every existing test.
- Fix: Add crates/lakehouse-engine/src/export/iceberg_sink_tests.rs, declared in iceberg_sink.rs as `#[cfg(test)] #[path = "iceberg_sink_tests.rs"] mod tests;`, and add `lakehouse-catalog = { path = "../lakehouse-catalog", features = ["test-support"] }` to the `[dev-dependencies]` of crates/lakehouse-engine/Cargo.toml. Serve the catalog and the S3 storage from one `spawn_server` fake: `/v1/config`, a table creation answered with a load answer at location `s3://warehouse/ns/t` whose `commit.retry.num-retries` is `"0"`, the reload, a 200 answer to every PUT, and the case's answer to the commit. Open the sink through `iceberg_rest::target` with a static-key CONNECTION whose `endpoint` is the fake's address, `path_style` true, and `allow_http` true, write one batch, call `finish`, then call `abort`. Write one case table: a commit answered 504 makes `finish` fail with an unknown outcome and `abort` send no DELETE, and a commit answered 403 makes `abort` send one DELETE per recorded data file location.
