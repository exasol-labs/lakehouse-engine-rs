# Tasks: add-direct-storage-catalog-kind

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Kind, CONNECTION, properties)
- [x] 1.1 Add `CatalogKind::DirectStorage` and the `DIRECT_STORAGE` spelling, extend `resolve_catalog_kind`, and rewrite the unrecognized-value error to name all three accepted spellings and state that absence selects Iceberg REST.
- [x] 1.2 Add a scheme-agreement method on `StorageBackend` with no catch-all arm, accepting `s3`, `s3a`, and `abfss`, and rejecting `abfs` with an error naming `abfss`.
- [x] 1.3 Widen `validate_creds` and `validate_kind_preconditions` to receive the CONNECTION address, and add the direct-storage arm: require a non-empty storage base path, reject `warehouse`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `use_sigv4`, and `use_vended_credentials` while accepting an explicit `false`, and check scheme agreement.
- [x] 1.4 Add `adapter/direct_storage_properties.rs`: parse and validate `NAMESPACE`, `MERGE_SCHEMA` (default TRUE), and `HIVE_PARTITIONING` (parsed and validated, not acted on), and compose the storage base path from the CONNECTION address and `NAMESPACE` with a single `/` join. Reject an unparseable value rather than defaulting.
- [x] 1.5 Add the admission-limited store builder to `scan/object_store.rs`: one `LimitStore` cap of 16 and a connection-retention budget derived from that same constant.
- [x] 1.6 Unit tests in the sibling `_tests.rs` files for 1.1 to 1.5, including the credential-safety assertion that no rejection message carries a credential value.

## Phase 2: Implementation (Group B: Directory seam and widening owner)
- [x] 2.1 Add `types/widening.rs` owning the 13 recorded relaxation pairs as production code, with one function returning the wider of two Arrow types or no answer. Leave `scan/type_relaxation_tests.rs`'s concrete 17-entry `supported_relaxation_pairs` list intact as the pin, and assert it AGAINST that owner rather than generating it from the owner. [expert]
- [x] 2.2 Add `adapter/parquet_directory.rs` listing: recursive, `*.parquet` only, every path segment beginning `_` or `.` excluded, sizes carried from the listing response, deterministic order, and `key=value` segments split into a per-file map left unread.
- [x] 2.3 Add the footer fold: bounded concurrent footer reads through the caller's store, pairwise widening in listing order, union of column sets in first-appearance order, every column NULLABLE, uppercase-fold name collisions rejected, and an unfoldable pair failing with the column, both types, and both file paths. [expert]
- [x] 2.4 Add the merge-mode argument selecting every footer or exactly the first file's footer, returning the same file list either way.
- [x] 2.5 Return the parsed per-file Parquet metadata PAIRED with the files whose footers the mode read and ABSENT for every other listed file.
- [x] 2.6 Unit tests in `adapter/parquet_directory_tests.rs` and `types/widening_tests.rs` over fixture Parquet files written in the test.

## Phase 2: Implementation (Group C: Neutral catalog types, type mapping, tag vocabulary)
- [x] 3.1 Add `TableFormat::Parquet`, the no-data-file `SkipReason` variant, and `ColumnSourceType::Parquet` carrying an Arrow tag string to `crates/lakehouse-catalog`.
- [x] 3.2 Extend `crates/lakehouse-catalog/tests/catalog_public_surface.rs` to construct and observe all three added variants from its external vantage, adding no source-text assertion.
- [x] 3.3 Add the third arm to `column_source_type_to_exasol`: read the tag back through the existing tag parser and return `arrow_to_exasol_type`'s answer, resolving an unparseable tag to `VARCHAR(2000000)`.
- [x] 3.4 Unit tests in `types/mapping_tests.rs` pinning the third arm, the declared-type and Arrow-tag lockstep, and every unchanged Iceberg and Unity answer.
- [x] 3.5 Widen the scan-spec tag vocabulary in `types/mapping.rs` to cover every Arrow type `compatible_exasol_type` admits (`Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, `Timestamp` at every `TimeUnit` in naive and tz-aware form), updating the doc comment, keeping every recorded tag spelling and parse answer byte-identical, and adding the round-trip test. Leave the Delta reader's `byte`/`short` mapping on the `int32` tag UNCHANGED. Keep `every_natively_representable_delta_type_maps_to_its_own_arrow_tag` passing with no edit to any expected tag.

## Phase 2: Implementation (Group D: Discovery client and adapter wiring)
- [x] 4.1 Add `adapter/direct_storage.rs` declaring `DirectStorageCatalogClient` in `lakehouse-engine` and implementing `CatalogClient`, with a doc comment stating why the implementor is not in the catalog crate.
- [x] 4.2 Implement enumeration: first-level directories are tables, a loose file under the base path is ignored, a nested directory contributes files, and a directory with no data file is reported as skipped with the new neutral reason. Implement `load_table` as a clear error naming the direct-storage kind, never a panic and never an empty or synthesized table.
- [x] 4.3 Implement column resolution through the seam, applying the string substitution for a nested or unrepresentable column before rendering each Arrow tag.
- [x] 4.4 Make `construct_catalog_client` fallible, pass it the raw `NAMESPACE` property, and add the direct-storage arm that builds the one admission-limited store and the client.
- [x] 4.5 Record `TABLE_MAP` with the bare original-cased directory name, reuse the shared flatten and collision helpers, and reject a recovered pushdown identifier that is empty or carries a path separator.
- [x] 4.6 Scope the required-`NAMESPACE` rule to the catalog kinds, leaving it optional under direct storage.
- [x] 4.7 Update the compile-time signature pin in `adapter/catalog_client_tests.rs` for the fallible construction site.
- [x] 4.8 Unit tests in `adapter/direct_storage_tests.rs` and `adapter/adapter_tests.rs`.

## Phase 2: Implementation (Group E: Planning, scan source, join concurrency)
- [x] 5.1 Add the third `ScanSource` variant carrying the store, the table root, and the merge mode, and the third `RequestSession` variant, both matched exhaustively. [expert]
- [x] 5.2 Add `adapter/pushdown/format/parquet_format_reader.rs` resolving files and schema through the seam: empty deletes, empty partition values, identity binding with no synthesized field-id, empty partition columns and name-mapping, static storage, the composed table root, and no refused column.
- [x] 5.3 Add the third arm at the one format-reader selection site, leaving the Iceberg and Delta arms unchanged.
- [x] 5.4 Replace the sequential join-leg loop in `adapter/pushdown/joins/mod.rs` with concurrent resolution over the request's one shared session, preserving leg-index order and byte-identical generated SQL and scan specs. [expert]
- [x] 5.5 Size each broadcast-join side from the seam's listing rather than from any Parquet read.
- [x] 5.6 Unit tests in `adapter/pushdown/format/parquet_format_reader_tests.rs`, `adapter/pushdown/format/format_tests.rs`, `adapter/pushdown/scan_resolution_tests.rs`, `adapter/pushdown/joins/joins_tests.rs`, and `scan/field_id_projection_tests.rs`.

## Phase 2: Implementation (Group F: E2E suites and documentation)
- [x] 6.1 Add `tests/common/raw_parquet.rs`, declared once in `tests/common/mod.rs`, writing one `RecordBatch` to one object key through the shared `local_stack_storage()` backend, creating no catalog table and attaching no Iceberg field-id metadata.
- [x] 6.2 Extend the shared harness so the per-binary virtual-schema parameters include `CATALOG_KIND`, the namespace property, and `MERGE_SCHEMA`, omitting any parameter a binary does not supply.
- [x] 6.3 Add `tests/e2e_direct_storage_test.rs` covering the fixture-shape assertion, the mixed-type directory, widening, the missing column, the incompatible pair, `MERGE_SCHEMA=FALSE` on both paths, `stale_declaration_decides_the_emitted_width`, and the Delta-directory caveat. Write the incompatible-pair fixture under the ISOLATED base path `s3://warehouse/direct_incompatible/incompatible/`. Keep every other fixture under `s3://warehouse/direct/`.
- [x] 6.4 Add the discovery, `NAMESPACE`, CONNECTION-rejection, and pushdown-parity tests to the same binary (three pushdown-parity tests). Add the join's second fixture directory `s3://warehouse/direct/event_labels/`. Assert the join test against the equivalent unpushed join and on `EXPLAIN VIRTUAL` naming both tables in ONE pushdown request.
- [x] 6.5 Add `--test e2e_direct_storage_test` to the `test-e2e` recipe and extend the existing build-convention guard to assert the recipe names it.
- [x] 6.6 Add the raw-Parquet-directory scenario to `tests/e2e_azure_test.rs` under the existing per-run container guard.
- [x] 6.7 Add the direct-storage recipe to `docs/catalogs.md` beside the Iceberg REST and Unity Catalog recipes, stating the CONNECTION shape, the three properties, the Iceberg or Delta directory caveat, and the plan-time footer cost. Name the mixed timestamp-unit case as a limitation.

## Phase 3: Verification
- [x] 7.1 Run test suite (`cargo test`) — `cargo test --workspace`: 54/54 binaries ok, 0 failed
- [x] 7.2 Run E2E suite (`make test-e2e`) — 16/16 binaries ok, 0 failed, against Docker Exasol 2025.1.16 + MinIO + Iceberg REST (fresh `exa-data` volume; the pre-existing one was pinned to a stale 8.29.13 image)
- [x] 7.3 Run Azure E2E suite (`make test-e2e-azure`) — 25/25 tests ok, 0 failed, against the Lakekeeper+Keycloak overlay stack + real Azure Blob Storage (`test.env`)
- [x] 7.4 Run linter (`cargo clippy --all-targets`) — clean, 0 warnings
- [x] 7.5 Run formatter check (`cargo fmt`) — `cargo fmt --check`: no diff
- [x] 7.6 Open the untracked-gap follow-up issue (file-count/file-size safety limit and unmeasured admission cap of 16), per plan.md § Impact — done as #419 during 4.7

## Phase 4: Review Fixes
- [x] 4.1 Restore `#[test]` and the scenario doc comment on `unrecognized_catalog_kind_is_rejected` in `adapter/catalog_kind_tests.rs`, which lost both when the direct-storage test was inserted above it.
- [x] 4.2 Build the client in `adapter/direct_storage_tests.rs` through the real constructor rather than a struct literal, dropping the separate `prefix` parameter, and add a prefix-derivation test and a merge-mode footer-read test.
- [x] 4.3 Add `merge_schema_resolves_the_same_mode_on_both_paths` to `adapter/direct_storage_properties_tests.rs`, pinning that the enumeration path and the pushdown path derive ONE `MergeMode` from one `MERGE_SCHEMA` value.
- [x] 4.4 Replace `#[allow(clippy::type_complexity)]` in `adapter/catalog_client_tests.rs` with a file-scope `CatalogClientConstruction` function-pointer alias.
- [x] 4.5 Delete `DIRECT_STORAGE_REJECTED_FIELDS` and its string-matching loop (including the `unreachable!` arm) from `adapter/connection.rs`, replacing them with a straight sequence of guarded pushes covering all eight rejected fields.
- [x] 4.6 Introduce `StoreBounds { connection_budget, admission_limit }` in `scan/object_store.rs` and reduce `build_undecorated_store` back to four parameters, updating its three callers.
- [x] 4.7 Open the follow-up issue covering the absent file-count/file-size safety limit and the unmeasured admission cap of 16 (plan.md § Impact), then cite its number in `DIRECT_STORAGE_ADMISSION_LIMIT`'s doc comment in place of the claim that the plan tracks it.
- [x] 4.8 Extract the raw-Parquet ADLS block from `azure_static_and_vended_creds_end_to_end` into a named `direct_storage_over_adls_returns_correct_rows` helper called as that test's last statement.
- [x] 4.9 Move the `--test e2e_direct_storage_test` assertion out of the type-relaxation wiring test in `tests/build_convention.rs` into its own `make_test_e2e_runs_the_direct_storage_binary` test.
- [x] 4.10 Make `DirectStorageCatalogClient::new` take a `MergeMode` and derive its listing prefix through `parquet_directory::store_prefix`, deleting the inlined merge-mode mapping and the `ListingTableUrl` derivation, and map the mode at the one call site in `adapter/mod.rs`. [expert]
- [x] 4.11 Replace `resolve_columns`' lossless-round-trip refusal in `adapter/direct_storage.rs` with the tag's own normalization, so a non-UTC timezone column is declared exactly as the plan path renders it, and make `resolve_columns` infallible. [expert]
- [x] 4.12 Rename `compose_storage_base_path` to `join_storage_path`, make it the ONE owner of the direct-storage base-path/segment join (stripping every trailing `/` before appending one), and route `adapter/pushdown/scan_resolution.rs`'s table-root composition through it. [expert]
- [x] 4.13 Delete the duplicate empty/path-separator guard from `resolve_pushdown_identifier` in `adapter/mod.rs`, reverting its doc comment, and drop the adapter-side refusal test that `direct_storage_directory` already owns. [expert]

## Phase 5: Verification Fixes
- [x] 5.1 Fix `create_virtual_schema_with_password` in `tests/common/e2e_harness.rs` to omit the `NAMESPACE` clause entirely when `props.namespace` is empty (matching the existing optional-clause pattern used for `PARALLELISM_FACTOR`/`JOIN_BROADCAST_MAX_BYTES`/`CATALOG_KIND`/`MERGE_SCHEMA`) instead of sending `NAMESPACE = ''`, which Exasol rejects with sqlCode 42000; live-verified against the Docker Exasol 2025.1.16 container. Also fixed two incidental test-only defects uncovered once `e2e_direct_storage_test` could run past `CREATE VIRTUAL SCHEMA`: a `VARCHAR(2000000)` exact-match assertion not tolerant of Exasol's ` UTF8` charset suffix on `COLUMN_TYPE` (2 call sites), and an unquoted `VALUE` column reference colliding with the Exasol reserved keyword. Two further live E2E failures remain OUT OF SCOPE of this fix and need dedicated follow-up: `discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree` (first-level directory discovery over MinIO for `CATALOG_KIND='DIRECT_STORAGE'` returns zero tables where the `direct-storage-table-discovery` spec requires two; the test harness's `served_tables` helper also panics rather than returning an empty list on a genuinely zero-row `SYS.EXA_ALL_TABLES` result) and `events_directory_declares_and_returns_mixed_types_across_both_files` (a raw-Parquet `TIMESTAMP` column now declares `TIMESTAMP(3)` against a test expectation of bare `TIMESTAMP`, an open question of intended direct-storage timestamp-precision behavior, not a test typo).
- [x] 5.2 Fix `events_directory_declares_and_returns_mixed_types_across_both_files` in `tests/e2e_direct_storage_test.rs`: the failure was a test-tolerance defect, not a production one. Live `SYS.EXA_ALL_COLUMNS` on `DIRECT_LAKEHOUSE.EVENTS` (Docker Exasol 2025.1.16) reports `EVENT_TS` as `TIMESTAMP(3)` and `SCORE` as `DOUBLE`, while the test demanded exactly `TIMESTAMP` and `DOUBLEPRECISION`. Both are Exasol's own `COLUMN_TYPE` rendering of what the engine declares: direct storage maps `ColumnSourceType::Parquet` through `arrow_to_exasol_type`, whose `Timestamp(_, _)` arm emits bare `TIMESTAMP` (Exasol substitutes its default precision, the same quirk documented in `e2e_timestamp_precision_test.rs`) and whose `Float64` arm emits `DOUBLE PRECISION` (Exasol reports the `DOUBLE` alias). Added an `assert_declared_type` helper mirroring the established `assert_col_type` idiom in `e2e_unity_test.rs` (space-stripped prefix match) and moved all seven EVENTS declared-type assertions onto it, which also folds in the ad-hoc `VARCHAR(2000000)` charset-suffix tolerance from 5.1 and removes its duplicate catalog query. No production code changed.
- [x] 5.3 Fix `served_tables`' zero-row crash by making `ExaConn::fetch_result_columns_with_num_bytes` in `tests/common/exasol_ws.rs` derive the column count from `numColumns`/`columns` when the response carries no `data` key. Live probe against the container established ground truth and corrected 5.1's diagnosis: `SYS.EXA_ALL_TABLES` for `DIRECT_DISCOVERY_VS` returns three rows (`DEEP`, `ORDERS`, `SUB`), so first-level directory discovery for `CATALOG_KIND='DIRECT_STORAGE'` works as the `direct-storage-table-discovery` spec requires and needed no fix. The panic came from the third `served_tables` call, over `DIRECT_DISCOVERY_EMPTY_NS_VS`, which correctly serves no table: Exasol omits `data` entirely from a zero-row inline result set (`{"columns":[...],"numColumns":1,"numRows":0,"numRowsInMessage":0}`), so the helper returned zero columns instead of one empty column and every caller indexing `cols[0]` panicked. This was a latent fragility for any genuinely empty result in any E2E binary, not specific to this plan; it also made `e2e_capability_test`'s `cols.iter().all(|c| c.is_empty())` assertion pass vacuously, and that test still passes now that it is non-vacuous. Also converted the `namespace_clause` added in 5.1 to an `if`/`else` to clear the `clippy::obfuscated_if_else` warning it raised in every E2E test binary.
