# Tasks: fix-vs-property-validation

## Phase 2: Implementation (Group A: property validation)
- [x] 1.1 Reproduce #534 against the local Docker Exasol (three statements)
- [x] 1.2 Probe request shapes with a Python stub adapter
- [x] 2.1 Add E2E `property_validation.rs` and declare it in `main.rs`
- [x] 2.2 Remove `SCAN_SCHEMA` from `e2e_added_columns_initial_default_fill_all_types`, tag scenario
- [x] 2.3 Extend `malformed_connections_are_rejected_at_create_virtual_schema`
- [x] 2.4 Update `create_vs_omits_cluster_nodes_from_adapter_notes`, tag `pushdown_shards_from_handshake_node_count_without_note`
- [x] 3.1 Write `properties_tests.rs` unit tests first
- [x] 3.2 Implement `adapter/properties.rs`
- [x] 3.3 Move every property name constant into `properties.rs`
- [x] 4.1 Write adapter unit tests first and update lenient tests
- [x] 4.2 Wire the check into `handle_create_virtual_schema`, typed create-time resolution
- [x] 4.3 `resolve_connection_config` parses `ALLOW_HTTP` with the catalog rule

## Phase 2: Implementation (Group B: documentation and retired names)
- [x] 5.1 Update `docs/tuning.md` and `docs/architecture.md`
- [x] 5.2 Remove retired names and the old debug switch outside `docs/`, add the no-trace rule

## Phase 3: Verification
- [x] 6.1 Run `make fmt-check lint` and `cargo test`
- [x] 6.2 Run `make test-e2e`, `make test-e2e-lakekeeper`, `make test-e2e-unity` (glue and azure when credentials are available) (local suite only, per user)
- [x] 6.3 Re-run task 1.1 statements and § Manual Testing, compare output

## Phase 4: Review Fixes
- [x] 4.4 Add private `check_tuning` in `properties.rs` so `check_properties` no longer reads `TuningChecker::invalid`
- [x] 4.5 Move `impl ThreadingMode { fn as_note }` from `properties.rs` into `adapter/mod.rs` next to `build_adapter_notes`
- [x] 4.6 Rename `parse_allow_http` to `pub(super) parse_boolean`, reuse it in `parse_bool_property`, fix the `resolve_allow_http` doc and test name
- [x] 4.7 Add unit test `an_empty_or_null_value_counts_as_absent` in `properties_tests.rs`
- [x] 4.8 Assert `PROPERTY_NAMES` and `ALL_NAMES` hold the same names in `every_known_name_passes`
- [x] 4.9 Delete the five duplicate tuning tests from `adapter_tests.rs`
- [x] 4.10 Replace the five `error_text(&conn.try_execute(..))` call sites in `property_validation.rs` with `query_error`
- [x] 4.11 Add harness `schema_notes` and read schema-level adapterNotes through it in `skipped_entries`, `property_validation.rs`, and `scan.rs`
- [x] 4.12 Assert the `MERGE_SCHEMA` error content and add an unparseable `HIVE_PARTITIONING` CREATE in `direct_storage.rs`
- [x] 4.13 Correct the empty or `null` value sentence on docs/tuning.md line 11
- [x] 4.14 Add the `DIRECT_STORAGE` form to the `NAMESPACE` row of docs/tuning.md
- [x] 4.15 Fold the eight `resolve_*` tuning functions into `resolve_tuning_notes` and rewrite their tests [expert]
