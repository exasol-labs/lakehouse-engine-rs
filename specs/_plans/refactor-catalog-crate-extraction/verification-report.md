# Verification Report: refactor-catalog-crate-extraction

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 6 automated gates green, all 17 scenario-coverage rows have a passing named test, all 5 manual-testing rows verified with captured evidence. Note: the Requirements table's `lakehouse-engine` `0.30.12` -> `0.30.13` version bump has not landed yet — this is expected to happen at the version-bump step of the outer `/speq:implement-pr` flow, after this `/speq:implement` verification gate, and is called out below so it is not silently missed. |
| Code review | 15 findings — 15 fixed (9 standard, 6 expert) |

| Check | Status |
|-------|--------|
| Build | PASS — `make cross-musl-udf-build` exit 0, one `.so` emitted from `-p lakehouse-engine` |
| Tests | PASS — `cargo test --workspace` 887 passed / 0 failed / 2 ignored; `make test-e2e` 184 passed / 0 failed; `make test-e2e-lakekeeper` 17 passed / 0 failed |
| Lint | PASS — `cargo clippy --workspace --all-targets -- -D warnings` 0 warnings/errors |
| Format | PASS — `cargo fmt --all -- --check` no diff |
| Scenario Coverage | PASS — 17/17 rows have a confirmed passing test |
| Manual Tests | PASS — 5/5 rows executed, output matches plan's expected output |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Not machine-measured — see Notes |
| Integration | Not machine-measured — see Notes |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`, lib targets: `lakehouse-catalog`, `lakehouse-engine`, `vs-expression`) | 821 | 821 | 0 |
| Integration (`cargo test --workspace`, `tests/*.rs` targets — most E2E suites are feature-gated and report 0 tests here) | 68 | 66 | 2 |
| Integration/E2E (`make test-e2e`) | 184 | 184 | 0 |
| Integration/E2E (`make test-e2e-lakekeeper`) | 17 | 17 | 0 |
| **Grand total** | **1090** | **1088** | **2** |

`cargo test -p lakehouse-catalog` alone (Manual Testing row 2): 74 passed, 0 failed (72 in `src/lib.rs` unit tests + 1 in `catalog_crate_boundary.rs` + 1 in `catalog_public_surface.rs`).

### Manual Tests

| Test | Result |
|------|--------|
| `cargo tree -p lakehouse-catalog --depth 1` | PASS — lists `aws-credential-types`, `aws-sigv4`, `aws-smithy-runtime-api`, `exasol-udf-sdk`, `iceberg`, `iceberg-catalog-rest`, `iceberg-storage-opendal`, `reqwest`, `serde`, `serde_json`, `url`, dev-dep `tokio`. No `arrow`, `parquet`, `datafusion`, `object_store`, `roaring`, or `lakehouse-engine` present. |
| `cargo test -p lakehouse-catalog` | PASS — 74 passed, 0 failed (72 unit + `catalog_manifest_declares_no_execution_engine_dependency` + `vended_mechanism_functions_are_not_declared_public`) |
| `make test-e2e` — `e2e_range_filter_prunes_by_file_bounds` | PASS — resolves 3 files unfiltered, 1 file under the partition filter (`region = 'north'`), 1 file under the range filter (`id <= 5`); all three calls share one harness-built `CatalogSession`. Every E2E suite in the run passed (184/184). |
| `make test-e2e-lakekeeper` — `lakekeeper_vended_creds_projection_filter` | PASS — vended STS keys, `s3.endpoint`, and `s3.path-style-access` reach MinIO through `resolve_vended_storage`; full suite 17/17 passed. |
| `cargo test -p lakehouse-engine --test pushdown_public_surface` | PASS — compiles and passes (0 runtime tests; the file is a compile-time `use`-list probe). The 12-item external `use` list matches the plan's redrawn baseline exactly, verified against the in-crate 22-item probe (`pushdown_surface_probe.rs`) by direct inspection. |

## Tool Evidence

### Linter

```
$ cargo clippy --workspace --all-targets -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.02s
```
Exit 0. No warnings, no errors, across all workspace members and all targets (lib, tests, examples).

### Formatter

```
$ cargo fmt --all -- --check
```
Exit 0. Empty output — no file requires reformatting.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | catalog-crate-structure | The catalog access layer lives in a standalone crate the engine depends on one way | `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` | `catalog_manifest_declares_no_execution_engine_dependency` | Yes |
| vs-adapter | catalog-crate-structure | One crate declares each shared credential type, re-exported at its pre-move engine path | `crates/lakehouse-engine/tests/shared_type_reexports.rs` | `reexported_paths_resolve_to_the_catalog_crate_types`, `storage_props_wire_encoding_unchanged` | Yes |
| vs-adapter | catalog-crate-structure | The crate exposes the concept-level API and hides every mechanism step | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | compile-time `use` list, `vended_mechanism_functions_are_not_declared_public` | Yes |
| vs-adapter | catalog-crate-structure | Every moved module keeps its own tests | `crates/lakehouse-catalog/src/{sigv4,session,auth,iceberg_io,vended,namespace,redaction}.rs` | e.g. `sigv4::tests::signed_request_carries_sigv4_header`, `session::tests::build_load_table_url_inserts_prefix_verbatim_without_encoding`, `namespace::tests::parse_table_ident_handles_multilevel_namespace` | Yes |
| vs-adapter | pushdown-catalog-session | Behavior is unchanged across the extraction | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `grouped_aggregate_matches_golden`, `group_by_fallback_matches_golden`, `lone_count_distinct_matches_golden`, `multi_count_distinct_decline_matches_golden`, `single_group_row_scan_matches_golden`, `empty_grouped_matches_golden` | Yes |
| vs-adapter | pushdown-catalog-session | Behavior is unchanged across the extraction | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_partition_filter_prunes_and_returns_correct_rows`, `e2e_range_filter_prunes_by_file_bounds` | Yes |
| vs-adapter | pushdown-catalog-session | CatalogSession is public and every file-resolution entry point takes one | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `file_resolution_entry_points_take_a_shared_session` | Yes |
| vs-adapter | pushdown-catalog-session | CatalogSession is public and every file-resolution entry point takes one | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `malformed_table_ident_fails_before_any_catalog_contact` | Yes |
| vs-adapter | pushdown-catalog-session | CatalogSession is public and every file-resolution entry point takes one | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_range_filter_prunes_by_file_bounds` (harness-built session resolves the same file lists) | Yes |
| vs-adapter | pushdown-catalog-session | createVirtualSchema resolves every table's schema on one shared session | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `schema_resolution_entry_point_takes_a_shared_session` | Yes |
| vs-adapter | pushdown-catalog-session | createVirtualSchema resolves every table's schema on one shared session | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_create_virtual_schema_lists_tables_over_oidc` | Yes |
| vs-adapter | pushdown-planning-cloud-credentials | One concept-level call resolves the effective scan storage from a loadTable response | `crates/lakehouse-catalog/src/vended.rs` | `resolve_vended_storage_{empty_access_key_preserves_static, empty_secret_key_preserves_static, absent_session_token_preserves_static, unparseable_path_style_preserves_static, matched_entry_missing_key_does_not_fall_back_to_config, allow_http_always_from_base, selects_credential_source_once_for_all_six_values}`, plus `vended_storage_{prefers_storage_credentials_over_flat_config, longest_matching_prefix_wins, falls_back_to_flat_config, uses_flat_config_when_no_storage_credentials, anchor_is_the_s3_table_location, adopts_endpoint_and_path_style_from_flat_config, adopts_endpoint_from_storage_credentials, keeps_static_endpoint_and_path_style_when_absent, adopts_region_from_flat_config, session_token_overrides_static}` | Yes |
| vs-adapter | pushdown-planning-cloud-credentials | One concept-level call resolves the effective scan storage from a loadTable response | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_creds_projection_filter` | Yes |
| vs-adapter | pushdown-module-structure | The pushdown façade releases exactly the three items the catalog extraction relocates | `crates/lakehouse-engine/tests/pushdown_public_surface.rs` and `crates/lakehouse-engine/src/adapter/pushdown_surface_probe.rs` | compile-time `use` lists (12 external items, 22 in-crate items) | Yes |

Note: the plan's table lists 17 scenario rows; two are duplicates of the same underlying test cited against two different scenario statements (`e2e_range_filter_prunes_by_file_bounds` appears against both "Behavior is unchanged" and "CatalogSession is public..."). All 17 rows as listed in `plan.md` are accounted for above and independently confirmed passing (13 distinct test-location rows covering all 17 scenario statements).

## Notes

- **Version bump not yet applied.** `crates/lakehouse-engine/Cargo.toml` still reads `version = "0.30.12"`; the plan's Requirements table calls for `0.30.13`. No task in the Implementation Tasks list performs this bump — it is the responsibility of the outer `/speq:implement-pr` orchestration step (version bump happens between `/speq:implement` and `/speq:record`). Flagging here so it is not silently dropped before that step runs.
- **Coverage percentage is not machine-measured.** `cargo-tarpaulin` and `cargo-llvm-cov` are installed on this host but neither is wired into this project's `Makefile` or CI, and the plan's Verification Checklist does not call for a coverage run. Rather than estimate a percentage, this report states pass/fail/ignored counts only, per the actual `cargo test` / `make test-e2e*` output.
- **Docker stack reused, not started.** `docker ps` showed the Exasol/MinIO/Iceberg-REST stack and the Lakekeeper/Keycloak overlay already up and healthy before this run (from the prior 7.3 run); both E2E suites ran against that existing stack rather than a freshly started one.
- **Duplicate scenario-coverage rows.** The plan's own Verification > Scenario Coverage table lists `e2e_range_filter_prunes_by_file_bounds` against two different scenario descriptions and `lakekeeper_vended_creds_projection_filter`/`lakekeeper_create_virtual_schema_lists_tables_over_oidc` each once — this is a pre-existing structure of the plan (rows share tests across related scenario statements), not a gap introduced during verification.
- **Speq plan validation** (`speq plan validate refactor-catalog-crate-extraction`) passes with only non-blocking style warnings (several scenarios have more than 3 AND-steps) — no errors.
- All 15 code-review findings (9 standard, 6 expert) were spot-checked against the current source in this session in addition to the two implementer agents' own reports: crate doc tense/citations, `test_support.rs` doc and helper relocation (`AUTH_PROP_KEYS` moved to `auth.rs`, vended fixtures moved to `vended.rs`), `vended.rs` comment rewording, `sigv4.rs` module doc conversion, `scan/emit.rs` re-export narrowed to `pub(crate)`, inline-comment removal in `adapter/mod.rs`/`pushdown/mod.rs`, work-tracking comment rewrites, sentinel-assertion deletion, the lazy empty-namespace session build plus its new test, `redact_catalog_error` deletion and re-pointing, the `resolve_vended_storage`-level test rewrite plus inlining of the four mechanism helpers, `CatalogProps.uri` field deletion, the unneeded `reqwest` engine dependency removal, and the four `aws-*`/`reqwest` pins moving into `[workspace.dependencies]` — all confirmed present and green under the full gate re-run.

Ready for: `/speq:record refactor-catalog-crate-extraction`
