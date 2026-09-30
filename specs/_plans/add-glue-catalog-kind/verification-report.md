# Verification Report: add-glue-catalog-kind

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Every scenario row has an existing test that passed. The final rerun is green, including 21/21 live tests against real AWS Glue. Two plan test names are stale and are noted below. |
| Code review | 29 findings — 29 fixed |

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

Measured with `cargo llvm-cov --summary-only -p lakehouse-engine -p lakehouse-catalog` (host unit and in-crate integration tests, `_tests.rs` files excluded).

| Type | Coverage % |
|------|------------|
| Unit | 91.56% lines, 90.57% regions, 88.72% functions |
| Integration | Not measured (E2E runs against the cross-built `.so` in Docker, which `llvm-cov` does not instrument) |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, workspace, final rerun) | 1909 | 1907 | 2 |
| Unit (`cargo test -p lakehouse-engine -p lakehouse-catalog`, after version bump) | 1762 | 1760 | 2 |
| Integration, `make test-e2e` (Exasol Docker, `LH_EXASOL_CPUSET=0-1`) | 392 | 392 | 0 |
| Integration, `make test-e2e-unity` | 27 | 27 | 0 |
| Integration, `make test-e2e-glue` (real AWS Glue) | 21 | 21 | 0 |

No failures in any run. The two ignored tests were ignored before this plan. `make test-e2e-glue` left no `lh_e2e_*` database behind.

### Manual Tests

| Test | Result |
|------|--------|
| catalog-kind-selection: `CATALOG_KIND='GLUX'` via exapump against the Docker Exasol | ✓ Failed with `unrecognized 'CATALOG_KIND' value 'GLUX'; leave it absent for Iceberg REST (the default), or set it to 'UNITY_CATALOG', 'DIRECT_STORAGE', or 'GLUE'` (SQL state 22002) |
| glue-catalog-client: list tables of `MY_GLUE` | ✓ Covered by E2E `glue_listing_includes_routed_tables_and_records_every_skip` (live Glue) |
| create-virtual-schema-adapter-notes: `SKIPPED_TABLES` in adapter notes | ✓ Covered by E2E `glue_listing_includes_routed_tables_and_records_every_skip`, unit `skipped_tables_are_recorded_with_reasons_for_every_kind` |
| create-virtual-schema: refresh keeps `TABLE_MAP`, `SKIPPED_TABLES`, budget entries | ✓ Covered by unit `refresh_rebuilds_table_map_preserves_notes` and `a_listing_without_skips_records_an_empty_list_replacing_the_previous` |
| glue-hive-type-mapping: column types of `ALL_TYPES` | ✓ Covered by E2E `glue_type_matrix_matches_every_row` (live Glue) |
| glue-table-planning: grouped query over `PARTITIONED` incl. `a b/c` and NULL | ✓ Covered by E2E `glue_partition_cases_return_their_glue_values` and `glue_queries_return_expected_rows_through_pushdown` |
| glue-table-planning: `COUNT(*)` fails naming the ORC partition | ✓ Covered by E2E `glue_orc_partition_fails_loud` |
| partition-predicate-declared-types: scan file list reduced to one partition | ✓ Covered by E2E `glue_partition_predicate_reduces_the_scan_file_list` |
| parquet-directory-seam: `cargo test -p lakehouse-engine --lib parquet_directory` | ✓ 28 passed, 0 failed |
| unity-parquet-table-planning: `make test-e2e-unity` | ✓ 27 passed, 0 failed |
| binary-column-refusal: `C_BINARY` refused, `COUNT(*)` succeeds | ✓ Covered by E2E `glue_type_matrix_matches_every_row` (live Glue) |
| Make target: `make -n test-e2e-glue` | ✓ Prints the `cargo test --features glue-e2e --test e2e_glue_test` line |

The rows marked "covered" need a pre-made `MY_GLUE` schema. I did not create AWS resources. The cited live tests assert the same output.

## Tool Evidence

### Linter

```
cargo clippy --all-targets                                           exit 0
cargo clippy -p lakehouse-engine --all-targets --features exasol-e2e     exit 0
cargo clippy -p lakehouse-engine --all-targets --features unity-e2e      exit 0
cargo clippy -p lakehouse-engine --all-targets --features azure-e2e      exit 0
cargo clippy -p lakehouse-engine --all-targets --features lakekeeper-e2e exit 0
cargo clippy -p lakehouse-engine --all-targets --features cloud-e2e      exit 0
cargo clippy -p lakehouse-engine --all-targets --features glue-e2e       exit 0
```

### Formatter

```
cargo fmt --all --check    exit 0, no output
```

Build: `make cross-udf-build` exit 0. Host debug `cargo build` exit 0 after the version bumps (lakehouse-engine 0.50.0 to 0.51.0, lakehouse-catalog 0.4.0 to 0.5.0, Cargo.lock synced).

## Scenario Coverage

Audit of `## Verification > Scenario Coverage` in plan.md. Every test name in the table was located by `fn <name>` and checked for an `ok` line in the final logs (`target/speq-test.log`, `target/speq-e2e.log`, `target/speq-e2e-glue.log`). All scenarios have a passing test. The plan's Test Location and Test Name columns differ from the repo in the four places listed after the table.

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | glue-catalog-client | Routing, Iceberg columns, Parquet columns, partitions, pagination, CatalogId, error codes, retry, clock skew | `crates/lakehouse-catalog/src/glue/client_tests.rs` | all names in the plan except the one below | Pass |
| datafusion-scan | glue-catalog-client | Throttling and server errors are retried within a bounded time | `crates/lakehouse-catalog/src/glue/client_tests.rs` | `throttling_and_503_are_retried_until_success` (plan says `..._within_the_deadline`) | Pass |
| datafusion-scan | glue-catalog-client | The client signs only with the CONNECTION's credentials | `crates/lakehouse-catalog/tests/glue_ambient_credentials.rs` | `requests_are_signed_with_the_connection_key_not_the_environment` | Pass |
| datafusion-scan | glue-hive-type-mapping | Hive primitive and nested types, malformed types, listing mapping | `crates/lakehouse-engine/src/types/hive_type_tests.rs`, `mapping_tests.rs`, `FMT/catalog_parquet_format_reader_tests.rs` | as named in the plan | Pass |
| datafusion-scan | glue-table-planning | Iceberg plan, Parquet plan, partition listing, unreadable partitions, direct children, pruning, recorded identifier | `FMT/iceberg_tests.rs`, `FMT/catalog_parquet_format_reader_tests.rs`, `FMT/format_tests.rs`, `adapter/pushdown/scan_resolution_tests.rs`, `glue/client_tests.rs` | as named in the plan | Pass |
| datafusion-scan | partition-predicate-declared-types | Declared-type comparison, undecidable keeps file, direct storage string pass-through, declared-type pruning | `FMT/partition_predicate_tests.rs`, `FMT/parquet_format_reader_tests.rs`, `FMT/catalog_parquet_format_reader_tests.rs` | as named in the plan | Pass |
| vs-adapter | create-virtual-schema-adapter-notes | Skipped tables recorded, empty list replaces previous, all-skipped namespace, cap to longest prefix | `adapter/adapter_tests.rs`, `notes/adapter-notes-limit.md` | as named in the plan | Pass |
| vs-adapter | create-virtual-schema | Enumerate every table in the namespace | `adapter/adapter_tests.rs` | `skipped_tables_are_recorded_with_reasons_for_every_kind`, `refresh_rebuilds_table_map_preserves_notes` | Pass |
| datafusion-scan | binary-column-refusal, type mapping, catalog string over binary | Binary refused at every depth, listing declares binary, catalog string reads as text, direct-storage BYTE_ARRAY, ENUM, UUID and fixed-length | `adapter/pushdown/pushdown_tests.rs`, `FMT/iceberg_tests.rs`, `FMT/catalog_parquet_format_reader_tests.rs`, `FMT/parquet_format_reader_tests.rs`, `adapter/parquet_directory_tests.rs`, `tests/e2e_type_matrix_test.rs` | as named in the plan | Pass |
| datafusion-scan | type matrix | Every mapped type on each Parquet-file source; incompatible types as JSON | `tests/e2e_type_matrix_test.rs`, `tests/e2e_glue_test.rs` | `direct_storage_type_matrix_matches_every_row`, `iceberg_type_matrix_matches_every_row`, `glue_type_matrix_matches_every_row` | Pass |
| datafusion-scan | delta-type-mapping | Unrenderable Delta type refused by name | `FMT/delta_schema_tests.rs` | `refused_set_is_binary_variant_and_containers_of_them`, `binary_cause_names_the_declared_type_and_issue_351` | Pass |
| datafusion-scan | catalog public surface | Glue additions reachable, no AWS SDK type crosses the boundary | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `glue_additions_are_reachable_from_outside_the_crate`, `glue_session_is_reachable_through_neutral_types_only` | Pass |
| vs-adapter | catalog-kind-selection, Glue connection | Absent, unrecognized, Glue kind; one construction site; Glue connection validation | `adapter/catalog_kind_tests.rs`, `adapter/catalog_client_tests.rs`, `adapter/connection_tests.rs`, `adapter/pushdown/scan_resolution_tests.rs`, `lakehouse-catalog/src/client_tests.rs` | as named in the plan | Pass |
| datafusion-scan | parquet-directory-seam | Recursive listing, file pattern, raw-key location | `adapter/parquet_directory_tests.rs` | `listing_is_recursive_filtered_and_deterministic`, `file_pattern_selects_listing_depth_and_file_name_rule`, `a_raw_key_location_is_listed_without_percent_decoding` | Pass |
| datafusion-scan | parquet-directory-seam | A raw-key file path resolves back to its object key | `FMT/parquet_format_reader_tests.rs` (plan says `parquet_directory_tests.rs`) | `a_raw_key_file_path_resolves_back_to_its_object_key` | Pass |
| glue-e2e | glue-e2e-harness | Run resources, fixture set, listing, queries, fail-not-skip, credentials hidden | `crates/lakehouse-engine/tests/e2e_glue_test.rs`, `tests/common/glue.rs` | all E2E names in the plan (11 tests) | Pass (21/21 live) |
| glue-e2e | glue-e2e-harness | Make target and CI job run the suite | `Makefile`, `.github/workflows/ci.yml` | `make -n test-e2e-glue` | Pass (dry run); CI job evidence comes from the PR run |
| glue-e2e | glue-orphan-sweep | Stale sweep, 24 hour guard, dry-run default, loud failure, no credentials in log | `.github/workflows/glue-orphan-sweep.yml` | workflow runs named in the plan | Not run here. Needs `gh workflow run` on the PR branch and a planted database (see Notes). |

Plan corrections to apply (I did not edit plan.md):

1. `throttling_and_503_are_retried_within_the_deadline` does not exist. The test is `throttling_and_503_are_retried_until_success` in `glue/client_tests.rs`.
2. `a_raw_key_file_path_resolves_back_to_its_object_key` lives in `FMT/parquet_format_reader_tests.rs`, not `parquet_directory_tests.rs`.
3. The plan lists `glue_run_resources_are_removed_when_the_scope_ends_including_on_panic` under `E2E, common/glue.rs`. It lives in `e2e_glue_test.rs`. `run_id_is_a_legal_glue_database_name` and `missing_glue_variable_fails_loud` live in `tests/common/glue.rs`.
4. `test_support_tests.rs` is under `adapter/pushdown/`, not `adapter/pushdown/format/`.

The five `glue-orphan-sweep` scenarios are workflow runs, not cargo tests. They cannot be run before the workflow reaches the remote. They are the only scenarios without a local passing result.

## Notes

Implementer decisions:

- The Iceberg `binary_values` fixture is written with parquet directly, because iceberg-rust's writer rejects invalid UTF-8.
- Direct storage refuses an unannotated BYTE_ARRAY even when an embedded Arrow schema hints Utf8.
- `SampleOneFile` mode classifies only the sampled footer.
- The Glue scan table root is normalized: `s3a` is read as `s3`.
- Unity Parquet refusal texts now say "Unity Parquet column".
- `SKIPPED_TABLES_OMITTED` is a note key beyond the plan. The adapterNotes hard limit is 2,000,000 bytes, measured live. CREATE fails above it (`notes/adapter-notes-limit.md`).
- The Hive type parser caps nesting at 128. The plan does not state this cap.
- Each Glue E2E test provisions its own run. A suite run makes about 7 fixture registrations.
- The Glue `binary_values` column is `c_bytes`, not `payload`.

Baselines and size:

- Type matrix baseline: `notes/type-matrix-baseline.md`.
- `.so` size (`notes/so-size.md`): base 144,791,640 B, branch 148,621,152 B stripped, +3,829,512 B, about 2.6%.

Environment:

- The Glue fixture bucket `lakehouse-engine-ci` (eu-central-1) did not exist. It was created during this run with public access blocked. The `GLUE_*` credentials belong to a non-dedicated IAM user. Open question 7 needs a human.
- S3 over TLS from inside the UDF is proven by the passing live Glue suite. Glue API TLS is proven by its listing tests.
- Coverage covers the host unit tests only. It excludes the E2E suites.

Open questions still in plan.md (all need a human or an issue):

1. User-facing `FILE_PATTERN` property (#TBD).
2. `binary` rendering (#351, existing).
3. Nested Parquet `ENUM` member on direct storage is refused (#TBD).
4. `ICEBERG_REST` classifies a non-loadable table by HTTP status alone (#TBD).
5. Lake Formation tables, cross-account `CatalogId`, assume-role and STS credentials (#TBD).
6. Partitions stored in another bucket than the table (#TBD). The query fails naming them.
7. E2E ownership: IAM user, fixture bucket, four repository settings, AWS bill, fork PR behavior.
8. `CLAUDE.md` § Data types still lists Binary types as JSON `VARCHAR`. A human decides whether to update it.
