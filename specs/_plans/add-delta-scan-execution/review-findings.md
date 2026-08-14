# Code Review Findings: add-delta-scan-execution

## Summary
- Files reviewed: 47 (39 modified, 8 new)
- Total findings: 24 (standard: 22, expert: 2)
- Evidence run before review: `cargo clippy --all-targets` exit 0 (no warnings); `cargo test -p lakehouse-engine --lib` 900 passed / 0 failed; `--test scan_deletion_vectors` 9 passed, `--test scan_partition_values` 8 passed, `--test scan_join_test` 6 passed.
- No `#[allow(dead_code)]`, `#[ignore]`, `todo!`, `unimplemented!`, TODO/FIXME, or commented-out code was introduced. `ScanSpec`/`FileEntry`/`LogicalField` stayed format-neutral; the scan side dispatches on `DeleteMechanism` variant content, never on format identity.

## Standard fixes

### crates/lakehouse-engine/src/scan/spec.rs

#### [MISSING_DOC_COMMENT] `JoinSpec`'s entire struct doc was deleted while adding `partition_columns`
- Location: line 472 (struct), line 527 (dangling reference)
- Issue: the 20-line doc comment above `pub struct JoinSpec` — shard-invariance of the dimension side, the verbatim-splice contract of `condition`, and the rationale that the dimension side is read through its OWN `StorageBackend` because a vended credential is scoped to the table it was resolved for — was removed by this change (`git diff HEAD -- crates/lakehouse-engine/src/scan/spec.rs` shows the block removed with no replacement). The struct is now the only undocumented public struct in the module, and its `storage` field doc at line 527 still reads "Required — see the struct doc", pointing at a doc that no longer exists.
- Fix: In crates/lakehouse-engine/src/scan/spec.rs, restore the `JoinSpec` struct-level doc comment verbatim from `git show HEAD:crates/lakehouse-engine/src/scan/spec.rs` (the block immediately above `#[derive(...)] pub struct JoinSpec`), placing it above the derive attribute so the `storage` field's "see the struct doc" reference resolves again.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [MISSING_DOC_COMMENT] `build_broadcast_join_sql`'s interface doc was deleted while adding one field
- Location: line 681
- Issue: the 24-line doc comment on `pub(in super::super) fn build_broadcast_join_sql` was removed by this change; the only edit to the function was adding `partition_columns: dimension.partition_columns.clone()` to the `JoinSpec` literal. The lost text is the sole statement of the broadcast fan-out contract: the `Ok(None)` fall-through to the N-scan wrapper, the fact/dimension sharding split, the per-side `StorageBackend` rule, and where the row window lands relative to the node-local join. The function is now the only undocumented item in the broadcast-join builder chain.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, restore the doc comment above `build_broadcast_join_sql` verbatim from `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, and append one sentence stating that each side's `partition_columns` ride in that side's own spec block.

### crates/lakehouse-engine/src/scan/object_store_tests.rs

#### [MISSING_DOC_COMMENT] Five test/helper doc comments were deleted as collateral of a one-line field addition
- Location: object_store_tests.rs:44 and :355; spec_tests.rs:1311 and :1410; scan_join_test.rs:238 and :803; e2e_unity_test.rs:119
- Issue: each of these items received exactly one added line (`partition_columns: Vec::new(),` in a `JoinSpec` literal, or a CONNECTION password change) and lost its doc comment in the same edit — `spec_with_join`, `adls_spec_with_join`, `join_block_round_trips_through_split_and_merge`, `join_spec_omitting_post_join_limit_deserializes_to_none`, `join_spec`, `a_dimension_side_read_failure_redacts_the_dimension_sides_credential` (a 19-line comment stating exactly what makes that redaction test falsifiable), and `create_unity_virtual_schema`. None of the removed text is made false by this change; the deletions are unrelated to it.
- Fix: Restore each of those seven doc comments verbatim from `git show HEAD:<path>` for crates/lakehouse-engine/src/scan/object_store_tests.rs, crates/lakehouse-engine/src/scan/spec_tests.rs, crates/lakehouse-engine/tests/scan_join_test.rs, and crates/lakehouse-engine/tests/e2e_unity_test.rs; on `create_unity_virtual_schema` amend the restored CONNECTION sentence to state that the password now also carries the MinIO endpoint and static storage credentials.

### crates/lakehouse-engine/src/adapter/mod.rs

#### [TOO_MANY_ARGUMENTS] `handle_pushdown_request` grew to 8 positional arguments behind a new lint suppression
- Location: line 346 (`#[allow(clippy::too_many_arguments)]`), line 347 (`handle_pushdown_request`)
- Issue: threading `catalog_kind` made this an 8-argument function, and the change silenced the resulting clippy warning with a newly added `#[allow(clippy::too_many_arguments)]` rather than grouping. Five of the eight (`catalog_uri`, `storage`, `creds`, `allow_http`, `catalog_kind`) are exactly the tuple `resolve_connection_config` returns at line 158 and are spread only to be re-collected — `catalog_uri`/`catalog_kind` into `TableScanResolver::for_request` and `storage`/`creds`/`allow_http` into `ConnectionStorage`.
- Fix: In crates/lakehouse-engine/src/adapter/mod.rs, introduce a `struct ResolvedConnectionConfig { catalog_uri: String, storage: StorageBackend, creds: ConnectionCreds, allow_http: bool, catalog_kind: CatalogKind }`, return it from `resolve_connection_config`, pass it as one argument to `handle_pushdown_request` and on to `handle_pushdown`, and delete the `#[allow(clippy::too_many_arguments)]` at line 346.

### crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs

#### [TOO_MANY_ARGUMENTS] `ResolvedJoinSide::new` grew to 8 arguments that are a `ResolvedScan` plus two strings
- Location: line 220 (`#[allow(clippy::too_many_arguments)]`), line 221 (`fn new`)
- Issue: the change added `partition_columns` as an 8th positional argument and silenced the lint with a newly added `#[allow]`. Six of the eight arguments (`table_root`, `files`, `logical_schema`, `name_mapping`, `effective_storage`, `partition_columns`) are the fields of the `ResolvedScan` that `resolve_one_join_side` destructures at line 340 and immediately re-passes one by one — the constructor's only production caller.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs, replace `ResolvedJoinSide::new`'s 8 parameters with `(table_name: String, table_identifier: String, resolved: ResolvedScan)`, move the field destructuring inside it, update `resolve_one_join_side` to pass the `ResolvedScan` whole, update the call in crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs, and delete the `#[allow(clippy::too_many_arguments)]` at line 220.

#### [IMPLEMENTATION_IN_NAME] `iceberg_ident` now carries Unity Catalog identifiers
- Location: planning.rs:35 (`JoinLeaf::iceberg_ident`), :192 (`ResolvedJoinSide::iceberg_ident`), :334 (`resolve_one_join_side` parameter), :320 (doc), joins/mod.rs:151, pushdown/mod.rs:170
- Issue: after this change these fields hold the recorded catalog identifier for BOTH catalog kinds — a Delta/Unity `catalog.schema.table` flows through them on every Unity join leg. The change even edited the field docs to drop the word Iceberg ("The original-cased catalog identifier this side was resolved from"), leaving the name contradicting its own doc and naming a vendor/format instead of the abstraction, in the module the plan made format-neutral.
- Fix: Rename the field `iceberg_ident` to `table_identifier` on both `JoinLeaf` and `ResolvedJoinSide`, and the `resolve_one_join_side` parameter likewise, using Serena's `rename_symbol`; update the ~25 references across crates/lakehouse-engine/src/adapter/pushdown/ and crates/lakehouse-engine/tests/.

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [SHALLOW_MODULE] `file_resolution` no longer resolves files
- Location: crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs (whole module), declared at pushdown/mod.rs:36
- Issue: the plan moved `resolve_file_list` and `parse_name_mapping` into `format/iceberg.rs`, leaving `file_resolution.rs` holding three unrelated helpers — `relativize_path_to_root`/`relativize_shards_to_root` (shard path rewriting), `encode_initial_default` (Iceberg default encoding), and `empty_result_sql` (empty-result SQL rendering) — under a module name that describes none of them. No decision-log entry records keeping the name, unlike the deliberate `positional_deletes.rs` keep.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/, move `encode_initial_default` next to its only consumer in `format/iceberg.rs`, move `empty_result_sql` into its own `empty_result.rs` submodule with a sibling `empty_result_tests.rs`, rename the remainder to `shard_paths.rs` with sibling `shard_paths_tests.rs`, and update the `mod`/`use` lines in pushdown/mod.rs and joins/mod.rs; split file_resolution_tests.rs to match the per-submodule test rule recorded in vs-adapter/pushdown-module-structure.

### crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs

#### [SUPPRESSED_WARNING] Dead lint suppression on a 5-argument helper
- Location: line 2074
- Issue: `#[allow(clippy::too_many_arguments)]` sits on `seam_handle_pushdown`, which takes 5 arguments; clippy's `too_many_arguments` fires only above 7, so the suppression silences nothing and misleads the next reader into thinking the helper is over the limit.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs, delete the `#[allow(clippy::too_many_arguments)]` at line 2074.

#### [MISSING_BOUNDARY_TEST] No test drives the JOIN request shape through the production resolver
- Location: line 2087 (`every_request_shape_resolves_through_the_format_reader_seam`)
- Issue: the test's name and doc claim every request shape, but it drives only the single-table shape (`nq4_request()`) for two catalog kinds. The plan's scenario "One catalog session per request serves every table the request resolves" was mapped to `a_two_leg_join_builds_exactly_one_catalog_session`, which does not exist; the closest test, `scan_resolution_tests.rs:187 one_catalog_session_serves_every_table_the_resolver_resolves`, calls `TableScanResolver::resolve` twice directly and never enters `plan_join`. Nothing therefore asserts that the production join path builds ONE resolver for two legs — the exact regression the collapse of per-leg `resolve_file_list` is meant to prevent.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs, add `#[tokio::test] async fn a_two_leg_join_resolves_both_legs_on_one_catalog_session()` that drives `handle_pushdown` (via the existing `seam_handle_pushdown` helper) with a two-table join request against a `RecordingCatalog`, and assert `catalog.targets()` is exactly `[ICEBERG_CONFIG_TARGET, <leg 1 loadTable>, <leg 2 loadTable>]` — one `/v1/config` for the whole request. Add the `UnityCatalogNative` twin in crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs: resolve two identifiers on one Unity resolver and assert exactly two `unity-catalog/tables/...` targets and no repeated auth target, since `one_catalog_session_serves_every_table_the_resolver_resolves` covers only the Iceberg arm of the reuse claim the resolver doc makes for both.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [VAGUE_TEST_NAME] `unity_kind_pushdown_routes_to_the_delta_format_reader` never reaches the Delta reader
- Location: line 260 (name), line 283 (assertion message)
- Issue: the test asserts on `"Unity Catalog load table request failed"`, which `UnityCatalogSession::load_table` raises inside `TableScanResolver::resolve` (scan_resolution.rs:109-111) — three lines BEFORE `format_reader(ScanSource::UnityDelta { .. })` is called at :112-118. No `DeltaFormatReader` is ever constructed on this path, so both the test name and its assertion message ("expected the request to reach the Delta reader's Unity load-table call") name a component the test cannot observe. What it does prove — that `dispatch` routes the `UNITY_CATALOG` kind to the Unity session loader with no early refusal and no `/v1/config` — is valuable and unasserted elsewhere. The Delta reader IS genuinely reached by `pushdown_tests.rs`'s `every_request_shape_resolves_through_the_format_reader_seam`, whose Unity half serves a 200 body and surfaces the reader's own plan-time error.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs, rename the test to `unity_kind_pushdown_routes_to_the_unity_catalog_loader`, reword the line-283 assertion message to "must reach the Unity Catalog load-table call", and add `assert!(message.contains("unity-catalog/tables"))` so the endpoint claim the doc makes is actually asserted; point the plan's "Planning: The Delta reader is reached from production pushdown" scenario row at `every_request_shape_resolves_through_the_format_reader_seam` instead of the non-existent `delta_format_reader_tests.rs::delta_reader_is_selected_from_the_production_pushdown_path`.

### crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs

#### [ASSERTION_FREE_TEST] Refusal assertion is vacuous for the empty-string input
- Location: lines 98-107
- Issue: the loop over `["cat.sch.", "", "   "]` asserts `err.to_string().contains(unresolvable)`. For the `""` element that is true of every string, so that iteration proves only that `resolve` returned some error — not that the refusal names the identifier it could not resolve, which is the stated claim.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs, change the assertion at lines 104-107 to match the quoted rendering — `err.to_string().contains(&format!("'{unresolvable}'"))` — which is non-vacuous for all three inputs.

#### [DUPLICATE_TEST] Resolver test re-asserts the Iceberg reader's whole payload
- Location: line 124 (`an_iceberg_identifier_resolves_through_the_iceberg_reader_with_no_partition_columns`)
- Issue: it asserts the same six `ResolvedScan` fields against a snapshotless loopback fixture that `format/iceberg_tests.rs:104 iceberg_reader_owns_resolution_and_keeps_its_encoding` already asserts (`table_root`, empty `files`, `effective_storage`, field-id-bound logical schema, `name_mapping`, empty `partition_columns`); only the fixture width differs. Its one otherwise-unique assertion — the request targets — is a strict subset of the target list asserted at lines 206-214 of the same file.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs, reduce `an_iceberg_identifier_resolves_through_the_iceberg_reader_with_no_partition_columns` to what the resolver alone owns — that the `IcebergRest` kind reaches the Iceberg reader and that `partition_columns` comes back empty — and delete the `table_root`, `files`, `effective_storage`, `logical_schema`, and `name_mapping` assertions, which belong to the reader's own test.

### crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs

#### [OUTDATED_COMMENT] Doc still frames the test as a two-path comparison
- Location: lines 95-102
- Issue: the doc reads "Scenario: Iceberg planning is byte-identical through the new seam", a claim that made sense while the test compared the reader against `resolve_file_list`. That comparison target is deleted; the test now asserts the reader's resolved values against the fixture, and byte-identity is actually carried by the dispatch goldens plus `skip_serializing_if = "Vec::is_empty"` on `partition_columns` (scan/spec.rs:1074).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs, reword the doc at lines 95-102 to state what the test asserts — the reader resolves the fixture's files, table root, effective storage, field-id-bound logical schema, and name mapping, and adds no partition columns — and name `dispatch_golden_tests.rs` as where encoding byte-identity is pinned.

#### [DUPLICATE_TEST] Second loopback-catalog fixture with a colliding helper name
- Location: iceberg_tests.rs:39 (`snapshotless_load_table_body()`) and :75 (`loopback_catalog`), against test_support_tests.rs:18 (`RecordingCatalog`), :110 (`snapshotless_load_table_body(location: &str)`), :152 (`iceberg_catalog`)
- Issue: the shared `RecordingCatalog` test support added by this change and the pre-existing `loopback_catalog` in `iceberg_tests.rs` are two single-shot loopback catalog servers with the same purpose in sibling modules, and each module defines a DIFFERENT `snapshotless_load_table_body` — same name, different arity. One name now denotes two concepts inside one crate.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs, delete `loopback_catalog` and route its tests through `super::super::test_support::RecordingCatalog`; rename the local body builder at line 39 to `name_mapped_load_table_body` so it no longer collides with `test_support_tests.rs`'s `snapshotless_load_table_body`.

### crates/lakehouse-engine/src/scan/positional_deletes.rs

#### [REDUNDANT_COMMENT] Inline comment restates the doc comment ten lines above it
- Location: line 844
- Issue: `partitioned_files`' doc comment already states "Partition values are converted for the whole shard before any read: they are the one thing here that can fail on the spec's own content rather than on storage, and doing them first keeps that failure ahead of every fetch, exactly as `applicable_delete_mechanism` keeps an unapplicable mechanism ahead of Phase A's." The body then repeats it as a 4-line inline comment ("Converted for the whole shard BEFORE any object-store read, so a value the declared type cannot represent fails the scan on the same terms an unapplicable delete mechanism does…"), so the same rationale must now be kept in sync in two places.
- Fix: In crates/lakehouse-engine/src/scan/positional_deletes.rs, delete the inline comment at lines 844-847 inside `partitioned_files`; the doc comment above the method already carries the rationale.

### crates/lakehouse-engine/src/scan/positional_deletes_tests.rs

#### [ASSERTION_FREE_TEST] Vacuous assertion attached to the wrong error
- Location: lines 350-353
- Issue: `only_iceberg_equality_and_puffin_delete_mechanisms_are_refused` ends with `assert!(!err.contains("#320"), "the refusal no longer cites the issue that implemented Delta deletion vectors")`, but `err` at that point is the PUFFIN refusal, which never cited #320. The assertion can never fail and proves nothing about the Delta arm — whose refusal was removed entirely, so no error exists to check.
- Fix: In crates/lakehouse-engine/src/scan/positional_deletes_tests.rs, delete the `assert!(!err.contains("#320"), ...)` block at lines 350-353; the removal of the Delta refusal is already covered by this test's `ApplicableDelete::DeletionVector` arm.

### crates/lakehouse-engine/tests/scan_deletion_vectors.rs

#### [IMPLEMENTATION_COUPLED_TEST] Integration test asserts `delta_kernel`'s internal error wording
- Location: lines 748-826 (`malformed_deletion_vector_containers_fail_the_scan_without_panicking`)
- Issue: the test pins five substrings of the third-party decoder's messages — `"Invalid version"`, `"size mismatch"`, `"Invalid magic"`, `"CRC32"`, `"not enough bytes"` — so a `delta_kernel` upgrade that rewords any of them fails the suite without any behavior changing. The same five corruptions are already asserted at unit level in src/scan/deletion_vectors_tests.rs:205, and the value this test adds over that one is end-to-end propagation, not per-corruption cause text.
- Fix: In crates/lakehouse-engine/tests/scan_deletion_vectors.rs, drop the `expected_cause` element from the `corruptions` tuples and its `assert!(msg.contains(expected_cause), …)`; keep the loop and assert instead that the message names the affected data file and that no batch was emitted (`ctx.emitted` empty) for each corruption.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [DUPLICATE_TEST] `unity_virtual_schema_connection_carries_minio_storage_credentials` asserts nothing the suite does not already assert
- Location: lines 546-569
- Issue: its enumeration loop (lines 551-558) is the same loop over `EXPECTED_TABLES` that `unity_create_virtual_schema_lists_fixture_tables_and_columns` runs at lines 205-212, and its `COUNT(*) FROM MULTI_PART_STATS == 5` (lines 560-568) is the identical assertion `unity_delta_delete_free_table_returns_its_rows` makes at lines 578-587. Both halves are real Exasol round trips in a suite that runs single-threaded, and the test's own claim — that the CONNECTION carries storage credentials — is proven by any successful scan.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, delete `unity_virtual_schema_connection_carries_minio_storage_credentials` and move its doc comment's credential rationale onto `unity_delta_delete_free_table_returns_its_rows`, whose `COUNT(*)` over `MULTI_PART_STATS` is the credential proof; update the plan's Scenario Coverage row for "The suite's virtual schema carries the storage credentials a UDF-side scan needs" to name that test.

#### [MISSING_BOUNDARY_TEST] Join ground truth is computed as a semi-join
- Location: lines 894-910
- Issue: `expected_join` is built by filtering `basic_partitioned` rows with `cm_ids.contains(number)`, which yields at most one expected row per base row. A real `JOIN … ON p.NUMBER = c.ID` yields one row per matching PAIR, so the oracle equals the join only while `cm_id_mode.ID` is duplicate-free — a fixture property the test deliberately never pins (its doc at lines 786-789 refuses to hard-code fixture values). A future fixture with a duplicated ID makes the assertion fail against correct engine output.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, immediately after collecting `cm_ids`, assert its values are distinct (`assert_eq!(cm_ids.iter().collect::<std::collections::HashSet<_>>().len(), cm_ids.len(), "cm_id_mode.ID must be distinct for the semi-join ground truth to equal a join")`).

#### [IMPLEMENTATION_COUPLED_TEST] Broadcast-side assertion splits a flattened EXPLAIN VIRTUAL string by offset
- Location: lines 879-891
- Issue: the test locates `"join":{` in `pushed` and asserts the fact table's name appears before it and the dimension table's after it. `explain_virtual_sql` (tests/common/e2e_harness.rs:285) flattens the EXPLAIN VIRTUAL result COLUMN-major across every column and joins with a space, so the concatenation carries Exasol's echoed pushdown request alongside the generated SQL in an order that is not source order. Both table names can land ahead of the first `"join":{`, in which case both assertions pass regardless of which side was broadcast.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, replace the `split_at(join_idx)` assertions at lines 879-891 with assertions over the parsed spec: extract the `"join":{…}` object from `pushed` and assert its `files` list carries a `basic_partitioned` path while the surrounding common spec's `files` carry a `cdf-column-mapping-id-mode` path.

#### [OUTDATED_COMMENT] Module doc still describes the pre-change CONNECTION
- Location: lines 20-23
- Issue: the module doc states the CONNECTION's "password supplies no auth field, because the OSS server's authorization is disabled", but task 4.1 changed that password to `local_stack_connection_password()` (line 124), which now carries the MinIO endpoint, static S3 access/secret keys, and path-style addressing — the whole point of the new round-trip scenarios. The file-level description of the password is now wrong by omission.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, extend the sentence at lines 20-23 to state that the password supplies no CATALOG-auth field but does carry the MinIO endpoint and static storage credentials the UDF-side scan reads through.

### crates/lakehouse-engine/tests/catalog_session_signatures.rs

#### [OUTDATED_COMMENT] Scenario-coverage claim does not match what the file proves
- Location: lines 22-26
- Issue: the module doc maps the scenario "The Iceberg file resolver is collapsed into its reader and leaves the façade" to `iceberg_scan_source_carries_a_shared_session`. That probe proves only that `ScanSource::Iceberg` holds a `&CatalogSession`; the façade departure is proven elsewhere, by the item lists in tests/pushdown_public_surface.rs (15 items) and src/adapter/pushdown_surface_probe_tests.rs (25 items).
- Fix: In crates/lakehouse-engine/tests/catalog_session_signatures.rs, retitle the covered-scenario line at lines 22-26 to the fact this file proves ("the Iceberg scan source carries a shared catalog session") and add a sentence pointing the façade-departure claim at the two surface-probe files.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [BOUNDARY_VIOLATION] Iceberg's identifier parser validates Unity Catalog identifiers on the shared pushdown path
- Location: pushdown/mod.rs:170 and :225; scan_resolution.rs:135-150; scan_resolution_tests.rs:70
- Issue: `parse_table_ident` — `lakehouse_catalog`'s Iceberg rule, which rejects anything without a `.`, reports "table property must be 'namespace.table'", and builds an iceberg-rs `NamespaceIdent` — now runs unconditionally for BOTH catalog kinds ahead of `TableScanResolver::for_request`, on the single-table path (line 225) and once per join leg (line 170). Two consequences: a Unity Catalog user whose recorded identifier is malformed gets an Iceberg-worded refusal from outside the format seam; and the resolver's own Unity rule in `unity_table_ident` is only ever reached for identifiers Iceberg's rule already accepted, so its `None => (Vec::new(), table_identifier)` arm is unreachable in production while scan_resolution_tests.rs:70 asserts it as if it were live. The identifier SHAPE decision now lives in two modules that can disagree, which is the leak the format-reader seam exists to prevent (CLAUDE.md: format-specific knowledge lives behind the reader; the plan: "one exhaustive `CatalogKind` match").
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs, add `pub(super) fn validate_table_identifier(kind: CatalogKind, identifier: &str) -> Result<(), UdfError>` that matches the kind exhaustively and delegates to `parse_table_ident` for `IcebergRest` and to `unity_table_ident` for `UnityCatalogNative`; replace both `parse_table_ident(...)` calls in crates/lakehouse-engine/src/adapter/pushdown/mod.rs (lines 170 and 225) with it, keeping them BEFORE `TableScanResolver::for_request` so the parse-before-config guarantee holds; decide the Unity shape in `unity_table_ident` alone (either accept the bare-name form and add a pushdown-level test proving it resolves with zero catalog HTTP, or reject it there with a Unity-worded error and change scan_resolution_tests.rs:70 to assert that refusal).

### crates/lakehouse-engine/src/scan/partition_values.rs

#### [TACTICAL_SHORTCUT] Partition split panics on an assumption nothing validates, inside UDF code
- Location: lines 90-110 (`file_count` derivation, `.expect("every partition column was located in the declared schema")` at line 109)
- Issue: `split` computes the file half's width as `declared.fields().len() - partition_columns.len()`, fills `partition_fields` by matching declared field NAMES, and then unwraps each slot with `.expect(...)`. All of that is sound only if the declared schema's field names are unique, which nothing checks: `Schema::index_of` (line 75) resolves to the FIRST field of a given name, so a declared schema carrying two fields named like a partition column passes validation, both declared columns receive the SAME `scan_index_by_declared` entry, and `file_fields` ends one field short of `file_count` — after which `remap_projection` hands `FileScanConfigBuilder::with_projection_indices` an index past the end of `file_schema ++ table_partition_cols`. The residual `.expect` is also a panic on the UDF's own scan path, which this plan's own spec forbids for this exact code ("that error MUST be returned as an error value, never raised as a panic") and which CLAUDE.md records as an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part.
- Fix: In crates/lakehouse-engine/src/scan/partition_values.rs `PartitionedScanSchema::split`, drop the `file_count` subtraction and the `Option<FieldRef>`/`expect` pair: collect `file_fields` and the partition slots in one pass, return `Err(format!("partition column '{name}' matches more than one field in the table's logical schema"))` when a partition column's name matches a second declared field, compute each partition column's scan index from `file_fields.len()` after the pass, and assert in the same function that every declared column maps to a distinct scan index. Add unit tests in crates/lakehouse-engine/src/scan/partition_values_tests.rs for (a) a declared schema with two fields sharing a partition column's name — refused, not panicked — and (b) a declared schema with two same-named NON-partition fields, proving the split and remap stay consistent.
