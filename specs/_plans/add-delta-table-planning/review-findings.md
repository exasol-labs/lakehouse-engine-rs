# Code Review Findings: add-delta-table-planning

## Summary
- Files reviewed: 36
- Total findings: 18 (standard: 14, expert: 4)

Verified clean, no findings raised: `cargo clippy -p lakehouse-engine -p lakehouse-catalog --all-targets`
exits 0 with zero warnings; the 32 new `adapter::pushdown::format` tests pass; `Cargo.lock` resolves
exactly one `arrow` (58.3.0) and one `object_store` (0.13.2); all ten `arrow_type` tag strings
`delta_schema.rs` emits are accepted verbatim by the single consumer
`types::mapping::arrow_type_from_tag` and are spelled identically to the Iceberg producer's
(`arrow_type_to_tag`), so no Delta tag falls into that parser's silent `DataType::Utf8` branch; no
production path populates `FileEntry::deletes` on a Delta entry (`FileEntry::with_delta` hardcodes it
empty and `replay_leaves_the_iceberg_delete_list_empty_on_every_entry` covers it); the blocking
`read_delta_log` call inside an async future stalls no sibling work, because no adapter call site
resolves legs concurrently (no `join!`/`try_join!`/`spawn` anywhere in `src/adapter/`); and
`delta_replay.rs`'s short-selection-vector assumption matches `delta_kernel` 0.26's documented
contract verbatim (`engine_data.rs:19-20`). The live Unity E2E test contains no skip of any kind — no
`#[ignore]`, no env-var guard, no early return; every failure path panics — and no credential value can
reach its output on any failure path (every `panic!`/`{:?}` site was traced; `ResolvedScan`,
`StorageBackend`, and `ConnectionCreds` are never formatted). The listing filter's admission decision in
`unity/client.rs` is byte-identical to `main`, and `vended_credential_key` is genuinely opaque —
never split, parsed, or branched on inside `lakehouse-catalog`.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs

#### [INFORMATION_LEAKAGE] Dotted table-identifier rendering gets a second home
- Location: lines 136-140 (`dotted_identifier`)
- Issue: `dotted_identifier` is semantically byte-for-byte identical to the already-shipped
  `crate::adapter::tables::catalog_identifier_string` (`crates/lakehouse-engine/src/adapter/tables.rs:51-56`),
  which is `pub`, lives in this module's own parent, and is documented as *the* canonical dot-joined
  fully-qualified identifier — "the value stored in `TABLE_MAP` and later parsed back by
  `parse_table_ident`". Both collect `ident.namespace` into a `Vec<&str>`, push `ident.name`, and
  `join(".")`. How a catalog table is rendered as a dotted string is now one decision with two
  owners: if it ever changes (quoting a segment that itself contains a dot, for instance), the Delta
  refusal messages silently disagree with the `TABLE_MAP` keys. It also breaks the one-word-per-concept
  rule — the codebase already names this concept `catalog_identifier_string`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs, delete the private
  `dotted_identifier` function, add `use crate::adapter::tables::catalog_identifier_string;`, and
  replace the call in `format_reader`'s non-Delta refusal with `catalog_identifier_string(&table.ident)`.
  In crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs, change the
  `use super::{FormatReader, ResolvedScan, dotted_identifier};` import to drop `dotted_identifier`, add
  `use crate::adapter::tables::catalog_identifier_string;`, and have `DeltaFormatReader::table_name`
  return `catalog_identifier_string(&self.table.ident)`.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs

#### [OUTDATED_COMMENT] Doc names the raw property; the caller passes the protocol-gated mode
- Location: lines 19-20
- Issue: `build_delta_table_schema`'s doc states "`column_mapping_mode` is the table's raw
  `delta.columnMapping.mode` property (`None` when the table sets no mode)". The only caller,
  `delta_format_reader.rs:182`, passes `Some(snapshot.column_mapping_mode())`, and that accessor's own
  doc (`delta_replay.rs:88-93`) states the opposite in terms: "The column-mapping mode IN FORCE, which
  is **not** simply the `delta.columnMapping.mode` property: the Delta protocol requires that property
  to be ignored unless the protocol supports the `columnMapping` reader feature ... Reading the ungated
  property instead would have this engine expect physical column names a table never wrote." The two
  modules therefore document contradictory contracts across the seam they share, and this file's
  version is the dangerous one — it invites a future caller to supply exactly the ungated raw property
  the sibling module warns against.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, rewrite the
  `column_mapping_mode` sentence in `build_delta_table_schema`'s doc comment to state that the argument
  is the column-mapping mode already IN FORCE — the protocol-gated mode from
  `DeltaSnapshot::column_mapping_mode`, not the raw `delta.columnMapping.mode` property — and that
  passing the ungated property would make this engine expect physical column names the table never
  wrote.

#### [DEAD_FLEXIBILITY] `Option<ColumnMappingMode>` wraps a type that already has a `None` variant
- Location: line 28 (parameter), lines 62-68 (`wire_column_mapping_mode`)
- Issue: `ColumnMappingMode` already carries a `None` variant, so the `Option` wrapper adds a second
  encoding of the same absence — `wire_column_mapping_mode` collapses `None` and
  `Some(ColumnMappingMode::None)` onto one output in a single match arm. The only production caller
  always passes `Some(...)`; the wrapper is varied by tests alone, and it spawns two tests that assert
  the same outcome through the two redundant encodings
  (`absent_column_mapping_mode_defaults_to_none_with_physical_name_equal_to_logical_name` and
  `explicit_column_mapping_mode_property_none_also_yields_wire_mode_none`, delta_schema_tests.rs:70 and
  :88). This is a configuration parameter the module declined to eliminate.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, change
  `build_delta_table_schema`'s second parameter to `column_mapping_mode: ColumnMappingMode` and
  `wire_column_mapping_mode`'s parameter to `mode: ColumnMappingMode`, dropping the `None` arm.
  Update the call in crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs:182 to
  pass `snapshot.column_mapping_mode()` without `Some`. In
  crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs, delete
  `explicit_column_mapping_mode_property_none_also_yields_wire_mode_none` and change every remaining
  `Some(ColumnMappingMode::X)` / `None` argument to the bare `ColumnMappingMode::X` /
  `ColumnMappingMode::None`.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs

#### [DUPLICATE_TEST] Second unmapped-type test asserts strictly less than the first, on identical input
- Location: lines 147-157 (`unmapped_delta_type_does_not_emit_a_logical_field_for_any_column`)
- Issue: This test builds the same `STATS_ALL_TYPES_SCHEMA` and asserts only `result.is_err()`, which
  `unmapped_delta_type_is_refused_naming_the_column_and_issue_322` (line 134) already establishes on the
  same input via `expect_err` plus three message assertions. Its name promises a claim about logical
  fields that it never checks — and cannot, since the `Err` variant carries none — so it neither adds
  coverage nor states the condition and behavior it is named for.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs, delete the test
  `unmapped_delta_type_does_not_emit_a_logical_field_for_any_column` in its entirety.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs

#### [OUTDATED_COMMENT] `read_delta_log` claims it redacts every error, but one call is unwrapped
- Location: lines 168-170 (doc) against line 176 (`build_table_root_store` call)
- Issue: `read_delta_log`'s doc asserts "Every error is redacted HERE rather than where it was raised".
  Three of its four fallible calls are wrapped in `.map_err(|error| redacted(error, secrets))`, but
  `build_table_root_store(...)` on line 176 uses a bare `?`. The path is in fact safe — the S3 and Azure
  `.build()` arms of `scan::object_store::build_undecorated_store` call `redact_error_text(&e.to_string(),
  all_secrets)` internally, and the remaining errors (`store_root_url`, "file URI has no bucket/host")
  carry no secret — but the invariant is then enforced in two places by two different mechanisms while
  the doc claims one. A reviewer auditing this function cannot tell whether the missing `.map_err` is a
  delegation or a leak, which is the whole risk the stated invariant exists to remove.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs, wrap the
  `build_table_root_store(...)` call in `read_delta_log` with
  `.map_err(|error| redacted(error, secrets))` so all four fallible calls are redacted uniformly at this
  layer, and leave the doc's "every error is redacted here" claim as written.

#### [INLINE_COMMENT] Name-mapping rationale sits inline inside a struct literal
- Location: lines 149-152
- Issue: A four-line rationale for leaving `name_mapping` empty is an inline comment inside the
  `ResolvedScan { .. }` literal, where the guardrails place no comments. `resolve_scan` already carries a
  doc comment two lines above, which is the compliant home for exactly this design intent.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs, delete the inline
  comment on lines 149-152 and append its content — that the Delta table block already carries every
  column's physical name and id, so an Iceberg-shaped name mapping here would be a second home for one
  decision free to drift from it, and that the field-id binding consulting either is #320's — as a new
  paragraph in the doc comment on `impl FormatReader for DeltaFormatReader::resolve_scan`.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay.rs

#### [INLINE_COMMENT] Six inline blocks carry the delta_kernel contract the module doc should own
- Location: lines 107-108, 110-111, 159, 163, 167-168, 190-192, 254-255, 272-273
- Issue: Eight inline comments sit inside `active_files`, `append_active_files`, `deletion_vector_at`,
  and `partition_values_at`. Each states genuine, non-obvious `delta_kernel` contract knowledge (the
  short-selection-vector rule, newest-first replay order, NULL `path` on non-`add` rows, the incomplete
  nested null mask read out of checkpoint parquet) — so deleting them would destroy information — but the
  guardrails admit no inline comments and the enclosing functions are private, so they have no local
  compliant home. The module already opens with a `//!` block whose stated purpose is precisely this
  third-party-contract knowledge.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay.rs, move the content of all
  eight inline comments into the module-level `//!` doc block as a "delta_kernel scan-row contract"
  paragraph list — the short-selection-vector rule, newest-first replay giving the first row per path,
  NULL `path` on rows carrying no `add`, keying deletion-vector presence on `storageType` rather than the
  nested null mask, reading partition-value offsets as a total function because a panic aborts the UDF VM,
  a logged NULL staying an explicit absent value, and `StatsOptions::none()` / `without_row_transforms()`
  being deliberate — then delete the inline comments from the function bodies.

### crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs, delta_format_reader.rs, crates/lakehouse-engine/src/scan/object_store.rs

#### [TOO_MANY_ARGUMENTS] Four new functions exceed three arguments, threading the same triple
- Location: `format_reader` (format/mod.rs:103-108, 4 args), `DeltaFormatReader::new`
  (delta_format_reader.rs:47-53, 5 args), `build_table_root_store` (scan/object_store.rs:174-179, 4 args),
  `build_undecorated_store` (scan/object_store.rs:193-198, 4 args)
- Issue: `(storage: &StorageBackend, creds: &ConnectionCreds, allow_http: bool)` is threaded as three
  separate parameters through `format_reader`, `DeltaFormatReader::new`, and both reader structs' fields —
  the same triple, always travelling together, never varied independently. That is one cohesive concept
  (the CONNECTION's static storage decision) spelled out five times, and it is what pushes two of these
  functions past the three-argument guardrail.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs, add a
  `pub struct ConnectionStorage<'a> { pub storage: &'a StorageBackend, pub creds: &'a ConnectionCreds,
  pub allow_http: bool }` with a doc comment stating it is the CONNECTION's static storage backend, its
  resolved credentials, and the resolved `ALLOW_HTTP` consent gate. Change `format_reader` to
  `format_reader<'a>(source: ScanSource<'a>, connection: &ConnectionStorage<'a>)` and
  `DeltaFormatReader::new` to `new(session, table, connection: &ConnectionStorage<'a>)`, storing the
  three fields from it; give `IcebergFormatReader` a single `connection: ConnectionStorage<'a>` field and
  destructure it at the `resolve_file_list` call so that function's signature stays byte-identical. Do
  NOT change `resolve_file_list` itself. This also removes the unexplained bare `true` positional
  argument at the `format_reader(…, &storage, &creds, true)` call sites. Update
  crates/lakehouse-engine/src/adapter/pushdown/format/format_tests.rs,
  delta_format_reader_tests.rs, iceberg_tests.rs, and
  crates/lakehouse-engine/tests/e2e_unity_test.rs (its `format_reader` call) to construct
  `ConnectionStorage`, and add
  `ConnectionStorage` to the `use` lists in
  crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs (25 → 26) and
  crates/lakehouse-engine/tests/pushdown_public_surface.rs (15 → 16), updating both files' stated counts
  in their module docs. Leave `build_table_root_store` and `build_undecorated_store` as they are — their
  four arguments are the shipped `build_side_store` shape the plan required be moved verbatim.

### crates/lakehouse-catalog/src/unity/client.rs

#### [OUTDATED_COMMENT] The corrected `TableInfo` doc introduces two new inaccurate claims
- Location: lines 405-409 (`TableInfo` doc), lines 313-315 (`DELTA_DATA_SOURCE_FORMAT` doc)
- Issue: Task 2 required correcting `TableInfo`'s stale claim that `table_id` is not consumed. The stale
  claim is gone, but the replacement asserts "`full_name` is the one wire field this client does not
  consume", which is false — the struct also omits `catalog_name`, `schema_name`, and
  `columns[].position`, all three of which the crate's own test fixtures send
  (`unity/client_tests.rs:18,31,43` and `:32`). The same doc justifies the absent-tolerant modelling as
  "because a VIEW carries none of them", which does not hold for `table_id`: Unity assigns a `table_id`
  to views as well, so the `#[serde(default)]` there is defensive tolerance, not a VIEW consequence.
  Separately, `DELTA_DATA_SOURCE_FORMAT` is still documented as "The only `data_source_format` the
  listing admits" although the const now also drives the load path through `neutral_table_format`, while
  its own sibling const at lines 317-321 correctly names both paths — so the pair reads asymmetrically.
- Fix: In crates/lakehouse-catalog/src/unity/client.rs, rewrite `TableInfo`'s doc comment to drop the
  "`full_name` is the one wire field this client does not consume" exclusivity claim (state instead that
  the struct models only the fields this client consumes) and to justify the absent-tolerant
  `storage_location` / `data_source_format` modelling by the VIEW case while giving `table_id` its own
  reason — defensive tolerance of a catalog response that omits it. Then extend
  `DELTA_DATA_SOURCE_FORMAT`'s doc to name both the listing-admission and the single-table-load use, in
  the same both-paths style as the sibling const at lines 317-321.

#### [MISSING_BOUNDARY_TEST] An empty-string `data_source_format` yields a value-less refusal
- Location: lines 369-374 (`neutral_table_format`), lines 281-285 (`neutral_table`), table at
  `unity/client_tests.rs:542-548`
- Issue: `"data_source_format": ""` falls into the catch-all arm and produces a refusal with an empty
  span where the offending value belongs: `reports data_source_format=, which names no table format this
  engine can plan`. The requirement is an error naming the value, and this names nothing. The parameterised
  table at `client_tests.rs:542-548` covers `CSV`, `PARQUET`, `DELTASHARING`, lowercase `delta`, JSON
  `null`, and a missing key — but not `""`. The inconsistency is inside one function:
  `neutral_table` already normalises empty-to-absent for both of its sibling projections
  (`.filter(|location| !location.is_empty())` and `info.table_id.filter(|key| !key.is_empty())`), while
  the format value gets no such normalisation.
- Fix: In crates/lakehouse-catalog/src/unity/client.rs, normalise the format value in
  `neutral_table_format` by treating an empty or whitespace-only `data_source_format` the same as an
  absent one, so it renders through `ABSENT_DATA_SOURCE_FORMAT` rather than as an empty span. Add an
  `""` row to the parameterised table in
  crates/lakehouse-catalog/src/unity/client_tests.rs:542-548 asserting the refusal renders the
  absent-format stand-in word and never an empty span.

#### [INFORMATION_LEAKAGE] "Empty vending key" is defined twice, with two different rules
- Location: line 285 (`info.table_id.filter(|key| !key.is_empty())`), doc at lines 274-276 and
  `client.rs:81-89`, against
  `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs:99-100`
- Issue: `lakehouse-catalog` documents that "An empty vending key projects to an ABSENT one" and
  implements it as `!key.is_empty()`. The sole consumer re-implements the same rule with a different
  predicate — `.filter(|key| !key.trim().is_empty())` — because the catalog's guarantee is not strong
  enough: a whitespace-only `table_id` satisfies `!key.is_empty()` and so reaches the engine as a
  `Some(...)` vending key that the catalog's own doc implies cannot occur. One decision ("what counts as
  no vending key") therefore has two owners across a crate boundary, disagreeing on the whitespace case,
  and every future consumer must know to re-check.
- Fix: In crates/lakehouse-catalog/src/unity/client.rs, change the `vended_credential_key` projection in
  `neutral_table` to `info.table_id.filter(|key| !key.trim().is_empty())` so the crate's own rule matches
  the guarantee it publishes, and update the doc at lines 274-276 and the `vended_credential_key` field
  doc at client.rs:81-89 to state that an empty OR whitespace-only key projects to an absent one. Add a
  test to crates/lakehouse-catalog/src/unity/client_tests.rs named
  `a_whitespace_only_table_id_projects_to_an_absent_vending_key` driving a `"table_id": "   "` fixture
  through `load_table` and asserting `vended_credential_key` is `None`. Leave
  delta_format_reader.rs's `.trim().is_empty()` guard in place as defence — its own doc already explains
  why it re-checks.

### crates/lakehouse-catalog/src/unity/client_tests.rs

#### [DUPLICATE_TEST] Two private-function tests re-assert what the public-path tests already cover
- Location: line 600 (`neutral_table_carries_the_format_tag_and_the_opaque_vending_key`), line 634
  (`neutral_table_format_maps_the_uppercase_unity_vocabulary`)
- Issue: The test at line 600 drives the same `single_table_body()` fixture and asserts the same two
  outcomes (`format == TableFormat::Delta`, `vended_credential_key == Some("uuid-1")`) as
  `load_table_returns_format_tag_vending_key_and_ordered_columns` at line 498, differing only in calling
  the private `neutral_table` instead of the public path — no input and no outcome differs. The test at
  line 634 asserts `DELTA` → `Delta` and `ICEBERG` → `Iceberg`, already established at lines 498 and 524
  respectively, again against the private function with no added condition. It is also the only new test
  in the file with no doc comment, and its name states neither its inputs nor its expected tags.
- Fix: In crates/lakehouse-catalog/src/unity/client_tests.rs, delete both
  `neutral_table_carries_the_format_tag_and_the_opaque_vending_key` and
  `neutral_table_format_maps_the_uppercase_unity_vocabulary` in their entirety. Do NOT delete
  `load_table_returns_format_tag_vending_key_and_ordered_columns` or
  `load_table_refuses_an_absent_or_unrecognized_data_source_format` — they are the plan's named scenario
  coverage for this behavior.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [OUTDATED_COMMENT] Doc still describes the static-key vending this change replaced
- Location: line 404, plus `docker-compose.unity.yml` line 5
- Issue: Line 404 reads "Catalog temporary-table-credentials request, static-key on the OSS server)".
  The change under review replaced exactly that: the harness now mints a real MinIO STS session via
  SigV4 AssumeRole and injects it, because MinIO rejects a non-STS token. The comment documents the
  behavior the same commit removed, in the file whose test exercises the replacement.
  `docker-compose.unity.yml` line 5 has the matching gap — its header usage line still reads
  `./scripts/unity/seed.sh   # upload Delta fixtures + register them in UC`, never updated for the new
  mint-and-inject step, although the "What this stack exposes" block below it at lines 22-23 was.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, rewrite the parenthetical on line 404 to say
  the OSS server vends a real MinIO STS session minted by the fixture harness, not a static key. In
  docker-compose.unity.yml, update the header usage line 5 to list the seed script's three steps
  (upload Delta fixtures, mint and inject the MinIO STS session, register the tables in UC) so it matches
  seed.sh's own header. In scripts/unity/README.md, change the "→ vended keys →" wording on line 81 to
  name a vended STS session triple. Then update the two remaining copies of the same stale
  "static creds"/"static keys" claim that this change invalidated: `Makefile` line 232 and
  crates/lakehouse-catalog/src/unity/vended.rs lines 43-44.

#### [IMPLEMENTATION_COUPLED_TEST] The live test hand-builds the storage backend production derives
- Location: lines 343-353 (`delta_static_storage`)
- Issue: `delta_static_storage` constructs `StorageBackend::S3(StorageProps { .. })` by hand, restating
  the endpoint, region, and keys that `delta_creds()` already holds. Production has exactly one home for
  that derivation — `lakehouse_engine::adapter::connection::storage_block(creds, allow_http)`
  (`crates/lakehouse-engine/src/adapter/connection.rs:336`, documented as "the ONE site that selects a
  storage backend from input") — and it is publicly reachable from an integration test. The whole
  function is `storage_block(&delta_creds(false), true)`. As written, the static-credential half of the
  vended-versus-static comparison runs against a backend the adapter would never build, so a change to
  `storage_block` (session-token propagation, `path_style` derivation) leaves this E2E green while
  silently diverging from production.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, replace `delta_static_storage`'s hand-built
  `StorageBackend::S3(StorageProps { .. })` body with
  `lakehouse_engine::adapter::connection::storage_block(&delta_creds(false), true)`, add the
  corresponding `use`, and delete any `StorageProps` import the change orphans. Keep the function's name
  and signature so its call sites are untouched.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs

#### [SWALLOWED_ERROR] Absent or oversized column-mapping annotations degrade to colliding ids and unwritten names
- Location: lines 83-88 (`field_id`), lines 73-81 (`physical_name`); locked in by
  `delta_schema_tests.rs:99-119`
- Issue: Under `Id` or `Name` column-mapping mode the Delta protocol requires every field to carry both
  `delta.columnMapping.id` and `delta.columnMapping.physicalName`. This code substitutes a plausible
  wrong value for either rather than refusing. `field_id` does
  `.and_then(|id| i32::try_from(id).ok()).unwrap_or_else(|| (ordinal as i32) + 1)` — so a field with NO
  assigned id, **and equally a field whose assigned id simply does not fit `i32`**, silently becomes its
  1-based ordinal. Those ordinals share one namespace with the real ids, so they can collide: an
  unannotated first column yields id 1, and a second column annotated `delta.columnMapping.id: 1`
  also yields 1. That directly contradicts the promise `DeltaColumnMapping::physical_id` publishes in
  `crates/lakehouse-engine/src/scan/spec.rs` — "so ids stay unique and stable per column in every mode" —
  and #320 will bind columns by exactly that id. `physical_name` likewise falls back to the logical name
  under `Id`/`Name` mode, making #320 read a physical Parquet column the writer never wrote.
  `explicit_column_mapping_id_wins_over_ordinal_position_when_present` asserts the fallback
  (`assert_eq!(logical_fields[0].field_id, 1); assert_eq!(table_spec.columns[0].physical_name, "a");`
  for an unannotated field under `ColumnMappingMode::Id`), so the defect ships behind a passing test.
  This is also the exact inverse of the module's own stated policy, which refuses rather than "emitting a
  misdescribed tag" for an unmapped type.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs, make `field_id` and
  `physical_name` fallible and mode-aware. Under `DeltaColumnMappingMode::None`, keep today's behavior
  exactly (1-based ordinal id, physical name equal to the logical name). Under
  `DeltaColumnMappingMode::Id` or `::Name`, return a `UdfError::User` naming the column, the table's
  column-mapping mode, and the missing or unrepresentable annotation when `field.column_mapping_id()` is
  `None`, when its value does not fit `i32`, or when
  `field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)` is absent or not a
  `MetadataValue::String` — never an ordinal or logical-name substitute. Propagate both through
  `build_delta_table_schema`'s existing `Result`. Then in
  crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs, rewrite
  `explicit_column_mapping_id_wins_over_ordinal_position_when_present` so its unannotated field is
  covered only under `ColumnMappingMode::None`, and add two tests:
  `id_mode_column_without_a_column_mapping_id_is_refused_naming_the_column` and
  `id_mode_column_without_a_physical_name_is_refused_naming_the_column`. Verify
  `column_mapping_mode_is_reported_from_the_tables_metadata`'s three fixtures still resolve through
  `build_delta_table_schema` afterwards — `cdf-column-mapping-name-mode` and `cdf-column-mapping-id-mode`
  annotate every column, so they must keep passing.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader.rs

#### [UNTESTED_ERROR_PATH] Secret redaction is never reached by any test
- Location: lines 171-202 (`read_delta_log`, `redacted`)
- Issue: `redacted` is the single guard keeping an object-store or `delta_kernel` error from echoing a
  vended or static credential out of plan time, and no test executes it. Both tests in
  `delta_format_reader_tests.rs` fail strictly earlier — `empty_storage_location_...` in
  `checked_table_root`, `vending_without_a_vending_key_...` in `effective_storage` — so control never
  enters `read_delta_log`. Confirmed by running the suite: 32 tests pass and neither reaches line 176.
  `delta_replay.rs` and `delta_schema.rs` are tested only through directly-injected stores, which never
  build a `StorageBackend` and so never produce a secret to mask. The existing
  `!message.contains(STATIC_SECRET)` assertion proves only that a message built before the credential
  split omits the secret; it exercises no redaction. The one behavior the brief calls highest-risk is
  therefore asserted nowhere.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs add two tests.
  First, `redacted_masks_every_effective_storage_secret_in_a_raised_error`: call the module-private
  `redacted(UdfError::User("... <secret> ...".into()), &secrets)` with a `secrets` slice holding two
  distinct sentinel values, and assert the returned message contains neither sentinel, is a
  `UdfError::User`, and retains the surrounding non-secret text. Second,
  `a_failed_log_read_reports_no_static_credential_value`: build a `DeltaFormatReader` over
  `creds(false)` and a `sample_storage()`-shaped `StorageBackend::S3` whose `access_key`/`secret_key` are
  distinctive sentinels and whose `endpoint` is `http://127.0.0.1:1`, with
  `storage_location: Some("s3://bucket/cat/sch/orders")`; assert `resolve_scan(None)` returns
  `Err(UdfError::User(_))`, that the message names the table root, and that it contains neither sentinel —
  which exercises `read_delta_log` end to end against a closed port, deterministically and without
  network egress.

### crates/lakehouse-engine/src/scan/spec.rs

#### [TACTICAL_SHORTCUT] The "Delta entries carry no Iceberg deletes" invariant is documented, not enforced
- Location: `FileEntry::with_delta` doc (the `with_delta` constructor), `FileEntry::deletes` doc, and
  `FileEntryWire::WithDelta` / `files_from_json`
- Issue: `FileEntry::with_delta`'s doc claims leaving `deletes` empty is "what keeps the invariant
  structural", and `FileEntry::deletes`' doc states it "Stays EMPTY on every Delta entry ... so the
  Iceberg positional-delete reader is never handed a reference it would misread". Neither is structural.
  Every `FileEntry` field is `pub`, so a struct literal can set both; and `FileEntryWire::WithDelta`
  carries `deletes` alongside `delta`, so `ScanSpec::files_from_json` silently accepts an entry holding a
  Delta deletion vector AND a non-empty Iceberg positional-delete list. `every_file_entry_combination()`
  in `spec_tests.rs` deliberately includes that pair and round-trips it, so the wire's permissiveness is
  asserted while the invariant it violates is only asserted for the replay path. Once #320 wires the scan
  side in, that combination is the shape that applies two independent delete mechanisms to one file and
  returns wrong rows — the exact failure this design is organized to prevent. The authors already accept
  refusing at reconstitution for a closed-set violation
  (`deletion_vector_storage_kind_outside_the_closed_set_is_refused`), so the precedent for enforcing here
  is their own.
- Fix: In crates/lakehouse-engine/src/scan/spec.rs, make `ScanSpec::files_from_json` reject an entry that
  carries a `delta` block and a non-empty `deletes` list, returning the existing input-free error string
  extended to state that a Delta entry may not carry Iceberg positional-delete refs and naming the
  offending entry's index (never its path or the raw input). Keep `FileEntry`'s `From`/`Into`
  `FileEntryWire` conversions total and lossless so the struct-level round trip is unchanged. Then in
  crates/lakehouse-engine/src/scan/spec_tests.rs, remove the Delta-block-plus-non-empty-deletes entries
  from `every_file_entry_combination()` (leaving the struct-level round-trip set to the shapes production
  can build), adjust the doc comment on that helper that currently justifies including them, and add
  `a_file_entry_carrying_both_a_delta_block_and_iceberg_deletes_is_refused` asserting `files_from_json`
  returns `Err`, that the message names the entry index, and that it echoes neither the path nor the raw
  input. Re-run `cargo test -p lakehouse-engine scan::spec` and confirm
  `common_blob_wire_is_byte_stable` and
  `delta_blocks_round_trip_losslessly_and_leave_iceberg_encodings_byte_identical` still pass unedited.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [MISSING_BOUNDARY_TEST] The live agreement assertion is satisfied by two empty partition maps
- Location: lines 436-440, in `unity_delta_planning_agrees_under_vended_and_static_credentials`
- Issue: `assert_eq!(vended.files, static_creds.files, "vended and static credential runs must agree on
  the file list and per-file partition values")` compares two reads of the SAME immutable transaction
  log, so structural equality is satisfied just as well by two identically-EMPTY `partition_values` maps,
  or by two entries whose `delta` block is `None` outright. Nothing in the live test asserts that any
  partition value is populated at all — `!vended.files.is_empty()` (line 429) bounds only the file count.
  The assertion message therefore claims more than the assertion proves. That gap lands squarely on the
  encoding the design calls load-bearing: `basic_partitioned` was chosen as the fixture precisely because
  it carries `letter=__HIVE_DEFAULT_PARTITION__/` alongside `letter=a|b|c|e`, and
  `crates/lakehouse-engine/src/scan/spec.rs`'s `DeltaFileSpec::partition_values` doc makes the
  present-key-with-no-value versus missing-key distinction the difference between a NULL the scan
  materialises and "a planning defect the scan can detect". The `table_with_dv` half of this same test
  models the right shape — it asserts presence (`deletion_vector.is_some()`, line 453) rather than mere
  agreement — so the partition half is the outlier. A regression that dropped partition values entirely
  on the live S3 path, or that resolved `delta: None` there, would keep this test green while the offline
  unit test at `delta_replay_tests.rs:195` (which does pin the six values including the explicit NULL)
  passes over a local filesystem store and could not detect it.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, strengthen
  `unity_delta_planning_agrees_under_vended_and_static_credentials`: after the existing
  `assert_eq!(vended.files, static_creds.files, …)`, assert that every entry in `vended.files` carries a
  `Some` `delta` block whose `partition_values` holds exactly one entry keyed `"letter"`, and assert the
  collected `letter` values across the six files — sorted by path — equal
  `[None, Some("a"), Some("a"), Some("b"), Some("c"), Some("e")]`, mirroring the offline pin in
  `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs:195-228`. Add a separate
  assertion that no carried value is the literal `"__HIVE_DEFAULT_PARTITION__"`, so the live stack proves
  the Hive default-partition directory resolves to an explicit NULL rather than to the directory literal.
  Narrow the existing agreement assertion's message so it claims only agreement, leaving the new
  assertions to carry the population claim. Verify with
  `make unity-up && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test --
  --test-threads=1` and confirm no credential value appears in the output.
