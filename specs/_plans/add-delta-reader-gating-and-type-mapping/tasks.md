# Tasks: add-delta-reader-gating-and-type-mapping

## Phase 2: Implementation (Group A)
- [x] 1.1 Add `format/delta_protocol.rs` with a failing test that a `typeWidening-preview` reader feature is refused by name, then implement `ensure_readable(min_reader_version, reader_features)` — default-deny allow-list of `columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck`, refusing the `_` remainder and `TableFeature::Unknown(_)`.
- [x] 2.1 Add failing unit tests asserting `can_cast_types(physical, Utf8)` for `Binary`, `List(Int32)`, `Interval(YearMonth)`, `Interval(DayTime)`, out-of-domain `Decimal128`, a POPULATED `Struct`, a `Map`, and a `List(Struct)`, pinning the three sets' membership to arrow's own answer.

## Phase 2: Implementation (Group B)
- [x] 1.2 Extend the gate with the version-range check against `MIN_VALID_RW_VERSION` and `MAX_VALID_READER_VERSION`, ordered before the per-feature check, and the legacy-protocol (`reader_features == None`) pass.
- [x] 1.3 Make the refusal name every refused feature in one error, sorted, and cite issue #349 for `typeWidening` and `typeWidening-preview`.
- [x] 2.2 Extend `delta_type_to_arrow_tag` to map the native set — adding `byte` and `short` to the existing `int32` tag — and the text-rendered set: out-of-domain `decimal`, `void`, `interval year to month`, `interval day to second`. Read the decimal domain from the shared `exasol_representable_catalog_decimal` predicate.
- [x] 2.3 Implement the RECURSIVE `array<E>` rule: `utf8` when `E` is itself in the native or text-rendered set, refused when `E` is in the refused set, applied at any nesting depth. [expert]
- [x] 2.4 Replace `unmapped_delta_type_error` with per-type refusal reasons for `binary`, `struct`, `map`, and `variant` — each naming the actual cause, citing issue #350 for the first three, and citing no closed issue.

## Phase 2: Implementation (Group C)
- [x] 1.4 Call the gate from inside `DeltaSnapshot::open`, extracting the two values through `table_configuration().protocol()` — the `internal-api` reach — and add the integration test over a synthetic log proving the refusal precedes any schema read, partition-column read, or file replay. [expert]
- [x] 3.1 Add `RefusedColumn` to `format/mod.rs`, add `ResolvedScan::refused_columns`, re-export the type at the `pushdown` façade, update both probe `use` lists and their stated counts to 26 and 16, and return an empty list from the Iceberg reader.

## Phase 2: Implementation (Group D)
- [x] 1.5 Add integration tests over the vendored fixtures: `type-widening` and `unshredded-variant` refused; `table-with-dv-small`, `multi-part-stats`, `stats-all-types`, `basic_partitioned`, `cdf-column-mapping-id-mode`, and `cdf-column-mapping-name-mode` all still resolve.
- [x] 2.5 Assert `void` reads as all-NULL end to end through the scan's missing-physical-column path, over a synthetic Delta log declaring a `void` column, and under `name` column mapping so the physical name that no data file carries is exercised too. [expert]
- [x] 2.6 Verify the one unverified link in the text-rendered set: that the scan's OWN field-id and physical-name expression adapter — not only DataFusion's default one — performs the physical-to-logical cast for a column whose physical Arrow type is `List(Int32)` and whose logical tag is `utf8`. Cover it with a scan-level integration test over a Parquet file carrying a list column, so a missing cast in that adapter fails here rather than in the E2E suite. [expert]
- [x] 3.2 Change `build_delta_table_schema` to emit no `LogicalField` for a refused column and return the refused list alongside the schema, classifying the TYPE before reading the column-mapping binding key; thread the list through `read_delta_log` and `resolve_scan`.
- [x] 3.3 Refuse the whole table when no column is mappable, with the `raw_scan` empty-logical-schema justification in the reader's own doc comment.

## Phase 2: Implementation (Group E)
- [x] 3.4 Implement the gate: one total recursive walk over the pushdown request JSON collecting every `column` node's uppercased name, unioned with the final projection, intersected with the refused list. [expert]

## Phase 2: Implementation (Group F)
- [x] 3.5 Call the gate at both resolve sites — `handle_pushdown` BEFORE the zero-active-files early return, and `joins::planning` per resolved side — and carry the list on `ResolvedJoinSide`. [expert]
- [x] 3.6 Add integration tests: a mappable-only projection plans; a projection naming a refused column refuses; a WHERE clause on a refused column refuses while its select list names only mappable columns; `SELECT *` refuses; an empty-file-list table naming a refused column refuses rather than returning an empty result; a join leg reaching a refused column refuses.

## Phase 2: Implementation (Group G)
- [x] 4.1 Replace `unity_delta_unmappable_table_fails_the_query_loud` with the reader-feature refusal test over `TYPE_WIDENING` and `UNSHREDDED_VARIANT`, asserting the feature name, the #349 citation, no column-typed error, session survival, and no credential leak.
- [x] 4.2 Add the varied-types test over `STATS_ALL_TYPES`: the 13-column projection, `SELECT COUNT(*) = 4`, the declared Exasol type per column, non-NULL `byte_col`/`short_col`, a bracketed `array_col` rendering, and a captured pushdown SQL asserting the scan UDF drives the query.
- [x] 4.3 Add the per-column refusal test over `STATS_ALL_TYPES`: `binary_col`, `map_col`, `nested_struct`, `SELECT *`, and a refused column in a WHERE clause all refuse with the #350 citation, while the 13-column projection still succeeds in the same run.
- [x] 5.1 Update `scripts/unity/fixtures/PROVENANCE.md`'s `stats-all-types` row and its `#322 gating note` to state what actually shipped: `timestampNtz` is mapped rather than gated, and `array`/`map`/`struct`/`binary` split between the text-rendered and refused sets rather than all reaching JSON `VARCHAR`. Update `scripts/unity/README.md`'s two `#322` rows to match.

## Phase 4: Review Fixes
- [x] 4.1 Delete `ensure_no_side_refuses_a_referenced_column` and the `use super::rendering::extract_join_projection;` import from `joins/planning.rs`, closing the planning ↔ rendering cycle; re-home the gate at its call site in `joins/mod.rs`, keeping the all-sides-clean short-circuit ahead of any `extract_join_projection` call. [expert]
- [x] 4.2 Attribute join column references to their own side before intersecting: build a per-side touched set from the request (untagged `column` nodes charged to every side, fail-safe) and restrict each side's projection union to the columns that side declares; add `a_refused_column_on_one_join_side_does_not_refuse_a_same_named_mappable_column_on_the_other`. [expert]
- [x] 4.3 Replace `ensure_no_refused_column_referenced`'s `projection` + `projection_widened` pair with one `emitted_projection: Option<&[ProjectionItem]>` argument, making the widened-projection combination unrepresentable, and update both call sites and `refused_columns_tests.rs`. [expert]
- [x] 4.4 Add the refusal-side tests for the widened-projection exclusion: `a_widened_projection_is_still_refused_when_the_request_itself_names_a_refused_column` in `refused_columns_tests.rs`, plus `an_aggregate_over_a_refused_column_is_refused` and `a_count_star_filtered_on_a_refused_column_is_refused` in `pushdown_tests.rs`. [expert]
- [x] 4.5 Delete the obsolete `COUNT(*)` over-refusal workaround in `e2e_unity_test.rs`, restoring a bare `SELECT COUNT(*) FROM {table}` in `unity_delta_varied_types_return_their_expected_exasol_types_and_values`. [expert]
- [x] 4.6 Replace `delta_type_to_arrow_tag`'s `Result` with a `ClassifiedDeltaColumn { Tag, Refused }` classification returned directly, so a refused column is data rather than an error round-tripped through `UdfError`, leaving `binding_key` as `build_delta_table_schema`'s only error path. [expert]
- [x] 4.7 In `delta_protocol_tests.rs`, add four tests: `a_legacy_protocol_table_with_no_reader_feature_list_passes_the_gate` (`ensure_readable(1, None)` and `ensure_readable(2, None)` are `Ok`); `the_readable_version_range_is_inclusive_at_both_ends` (`0` refused, `1` and `3` `Ok`); `every_allow_listed_reader_feature_together_passes_the_gate`; `an_unrecognized_reader_feature_is_refused_by_its_raw_protocol_name` using `TableFeature::Unknown("someFutureFeature".to_string())` asserting the message contains `someFutureFeature`. In `delta_replay_tests.rs`, add `a_legacy_reader_version_table_passes_the_gate_and_keeps_its_column_mapping_mode` over the `cdf-column-mapping-id-mode` fixture (minReaderVersion 2, no `readerFeatures`), asserting the snapshot opens and `column_mapping_mode()` is `ColumnMappingMode::Id`.
- [x] 4.8 In `pushdown_tests.rs`, add `a_unity_catalog_pushdown_gates_the_delta_protocol_and_refuses_per_column`: reuse `unity_delta_catalog`, `delta_commit_zero_key`, `delta_object_endpoint`; serve a commit-zero body whose `protocol` action declares `{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["typeWidening-preview"],"writerFeatures":["typeWidening-preview"]}` alongside a one-column `metaData`, asserting `delta_pushdown` returns `UdfError::User` naming `typeWidening-preview` and citing `#349`. Add a `protocol`-parameterised sibling of `fileless_delta_commit` in `test_support_tests.rs` rather than duplicating its body.
- [x] 4.9 In `types/mapping_tests.rs`'s `arrow_castability_to_utf8_pins_the_three_delta_type_sets`, split assertions into three labelled groups: move `DataType::Binary` under its own comment stating binary IS castable to Utf8 but is refused because the cast replaces non-UTF-8 bytes with NULL; keep `List(Int32)`, both `Interval` units, and `Decimal128(38,10)` under the text-rendered group; keep struct/map/list-of-struct under the refused-for-non-castability group.
- [x] 4.10 In `adapter/pushdown/test_support_tests.rs`, delete `percent_decoded` and replace the `param` closure in `list_bucket_result` with a lookup over `url::form_urlencoded::parse(query.as_bytes())`, returning the decoded value for `prefix` and `start-after` and defaulting to empty when absent.
- [x] 4.11 In `scan/raw_scan_tests.rs`, add a `let _ = std::fs::remove_dir_all(&dir);` pre-clean before `create_dir_all` (matching what `delta_replay_tests.rs`'s void-column test already does). In both `delta_replay_tests.rs` and `raw_scan_tests.rs`, rename the fixture directory from its `std::process::id()` suffix to the test function's own name (`lh_delta_void_column` / `lh_list_utf8_cast`).
- [x] 4.12 In `scripts/unity/fixtures/PROVENANCE.md` and `scripts/unity/README.md`, restate the gate as a default-deny allow-list: name the five supported reader features (`columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck`) and state every other reader feature is refused, with `variantType`/`typeWidening` called out as the fixtures' concrete cases.
- [x] 4.13 In `adapter/pushdown/format/delta_replay_tests.rs`'s `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves`, delete the bare `snapshot.schema();` and `snapshot.partition_columns();` statements, leaving the `active_files()` assertion as the test's act-and-assert.

## Phase 3: Verification
- [x] V.1 Run test suite (`cargo test`)
- [x] V.2 Run linter (`cargo clippy --all-targets`)
- [x] V.3 Check format (`cargo fmt --check`)
- [x] V.4 Build UDF (`make cross-musl-udf-build`)
- [x] V.5 E2E (Unity/Delta) (`make test-e2e-unity`)
- [x] V.6 E2E (Iceberg regression) (`make test-e2e`)
