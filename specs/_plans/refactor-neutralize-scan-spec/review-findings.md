# Code Review Findings: refactor-neutralize-scan-spec

## Summary
- Files reviewed: 39 (38 modified, 1 new)
- Total findings: 8 (standard: 7, expert: 1)

Verified clean, no findings raised: the `DeleteMechanismWire` byte-identity approach (private
untagged enum, `{path, size, content_type}` field order, no `#[serde(tag = …)]` anywhere); exhaustive
dispatch with no wildcard arm at every site that matches `DeleteMechanism` or `ColumnMappingMode`
(`spec.rs::is_delete_file_reference`, `object_store_path`, `object_store_path_mut`, both
`DeleteMechanismWire` conversions, `positional_deletes.rs::applicable_positional_delete`,
`delta_schema.rs::binding_key`, `file_resolution.rs::iceberg_delete_mechanism`); test-layout
compliance (every unit test lives in a sibling `_tests.rs` declared via `#[path]`, no test code in a
production file); comment discipline (no added TODO/FIXME/`task N.N`/"this refactor" comment
anywhere in the diff); `bind_columns`' resolution order is behaviour-preserving for Iceberg
(`declared_physical_names` is empty for every Iceberg table, and step 3 still refuses to consult
`name_mapping` for a physical field carrying an embedded id); and the Delta refusal message text is
unchanged (`{mode:?}` on `delta_kernel`'s `ColumnMappingMode` still renders `Id`/`Name`, and
`column_mapping_id` is still read before `column_mapping_physical_name` in both mapped modes, so the
same annotation loses first as before).

## Standard fixes

### crates/lakehouse-engine/src/scan/spec.rs

#### [OUTDATED_COMMENT] `object_store_path`'s stated reason is false for the `AbsolutePath` storage kind
- Location: lines 638-646 (`DeleteMechanism::object_store_path` doc), mirrored at
  `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` lines 55-57 and
  `crates/lakehouse-engine/src/scan/spec_tests.rs` lines 1974-1978 and 2007
- Issue: all three doc comments justify returning `None` for `DeleteMechanism::DeltaDeletionVector`
  with "its `path_or_inline_dv` is a UUID token or an inline vector payload". That is true for only
  two of the three storage kinds the payload admits: `DeltaDeletionVectorStorage::AbsolutePath`
  (spec.rs:554-555, Delta `p`) is documented in this same file as "an absolute path". The
  `file_resolution_tests.rs` author knew this — the new
  `relativization_leaves_a_deletion_vectors_path_or_inline_dv_untouched` calls the `p` kind "the
  discriminating case: its `path_or_inline_dv` looks exactly like an under-root object-store path" —
  so the accessor's own rationale contradicts the test that exercises it. The behaviour is right
  (a deletion vector's bytes are resolved at file registration, never fetched, relativized or routed
  from the delete list); only the reason given is wrong, and #320 will read these comments when it
  wires deletion-vector reads.
- Fix: In `crates/lakehouse-engine/src/scan/spec.rs`, rewrite the second paragraph of
  `DeleteMechanism::object_store_path`'s doc so the reason for `None` is that a deletion vector's
  `path_or_inline_dv` is resolved into a path at file registration and never addressed from the
  delete list — naming that it may be a UUID token, an inline payload, OR an absolute path
  (`DeltaDeletionVectorStorage::AbsolutePath`), so the value is never any of fetched, relativized,
  or claimed for a store here. Apply the same correction to the "A mechanism naming no path" sentence
  in `adapter/pushdown/file_resolution.rs` (lines 55-57) and to the doc comment and final assertion
  message of `only_a_delete_file_mechanism_exposes_an_object_store_path` in `scan/spec_tests.rs`
  (lines 1974-1978, 2007).

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay.rs

#### [TACTICAL_SHORTCUT] `append_active_files` constructs a `FileEntry` and then overwrites the field the constructor just set
- Location: lines 183-187 (`append_active_files`)
- Issue: `FileEntry::with_partition_values` promises "A data-file entry with its partition values and
  no delete mechanism" (`scan/spec.rs:958`), and its only production caller immediately assigns
  `entry.deletes` on the result. The three-statement construct-then-mutate sequence makes a
  `FileEntry` briefly exist in a shape no caller wants, and direct field assignment also bypasses the
  "must not mix a deletion vector with an Iceberg delete-file reference" contract that
  `FileEntry::with_deletes` documents — the one construction-time surface that states the invariant.
  `FileEntry`'s four fields are all public, so one struct literal expresses the intended value
  directly. The local name `carried` states no responsibility either.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay.rs`, replace the
  `let carried = …; let deletion_vector = …; let mut entry = FileEntry::with_partition_values(…);
  entry.deletes = …;` sequence in `append_active_files` with a single `active.push(FileEntry { path,
  size, deletes: deletion_vector_at(deletion_vectors, row, &path)?.into_iter().collect(),
  partition_values: partition_values_at(partition_values, row, &path)?, })` expression that sets all
  four fields at once, preserving the current evaluation order (partition values read before the
  deletion vector) so error precedence is unchanged. Leave `FileEntry::with_partition_values` and its
  doc comment as they are.

### crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs

#### [MISSING_BOUNDARY_TEST] The Delta `INTEGER` → `int32` type mapping lost its only assertion
- Location: line 114 (`none_mode_ignores_a_residual_column_mapping_annotation`, which replaced the
  deleted `absent_column_mapping_mode_defaults_to_none_with_physical_name_equal_to_logical_name`)
- Issue: the deleted test carried `assert_eq!(logical_fields[0].arrow_type, "int32")` for a
  `DataType::INTEGER` column, and its replacement asserts only `binding_keys(&logical_fields)`. That
  assertion was the only one in the repo covering the `Integer => "int32"` arm of
  `delta_type_to_arrow_tag` (`delta_schema.rs:172`): `grep -rn '"int32"' crates/` now matches that
  production line and nothing else. The surviving type assertions in this file cover only
  `int64`/`utf8`/`float64` (line 103-106) and `decimal128(10,2)` (line 283), so a regression mapping
  Delta `INTEGER` to the wrong Arrow tag would ship silently.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs`, add
  `assert_eq!(logical_fields[0].arrow_type, "int32", "a Delta INTEGER column maps to the int32 Arrow
  tag");` to `none_mode_ignores_a_residual_column_mapping_annotation` (its first field is already a
  `DataType::INTEGER` column), and extend that test's doc comment to state that it also pins the
  `INTEGER` → `int32` tag.

### crates/lakehouse-engine/src/scan/store_router.rs

#### [MISSING_BOUNDARY_TEST] The new "mechanism names no object-store path" branch in the owned-path set has no test
- Location: lines 91-97 (`RoutedSide::from_side`'s `if let Some(path) = delete.object_store_path()`)
- Issue: the delete loop gained a guard so a `DeleteMechanism::DeltaDeletionVector` contributes no
  owned path, and nothing exercises it: `grep -n "DeltaDeletionVector\|deletion"
  crates/lakehouse-engine/src/scan/store_router_tests.rs` returns nothing, and that file's only edit
  was retyping the `entry_with_delete` helper. Without a test, a later change that drops the guard —
  handing a UUID token such as `vBn[lx{q8@P<9BNH/isA` to `store_path` — would either add a bogus
  owned path or fail routing, and the suite would stay green.
- Fix: In `crates/lakehouse-engine/src/scan/store_router_tests.rs`, add a test named
  `a_deletion_vector_contributes_no_owned_path` that builds a side whose single `FileEntry` carries
  one `DeleteMechanism::DeltaDeletionVector` (use `DeltaDeletionVectorStorage::UuidRelative` with a
  non-path `path_or_inline_dv` token), constructs the `RoutedSide` the same way the existing tests do,
  and asserts the owned-path set contains exactly the data file's path and nothing derived from
  `path_or_inline_dv`.

### crates/lakehouse-engine/src/scan/object_store.rs

#### [UNTESTED_ERROR_PATH] The uniform-object-store check now skips path-free mechanisms with no test covering the skip
- Location: lines 556-562 (`validate_uniform_object_store_files`' delete loop)
- Issue: the "delete file" root check is now conditional on `delete.object_store_path()`, so a
  deletion vector is never checked against the side's root. `validate_uniform_object_store_files` has
  no direct test at all (`grep -rn validate_uniform_object_store_files crates/` matches only its
  definition and the single `raw_scan.rs:199` call site), so neither the pre-existing delete-file
  rejection nor the new skip is pinned. The skip is the branch that keeps a Delta entry from being
  rejected for a token that is not a URI, and it is exactly the branch #320 will lean on.
- Fix: In `crates/lakehouse-engine/src/scan/object_store_tests.rs`, add two tests against
  `validate_uniform_object_store_files`: one named
  `a_delete_file_under_a_different_root_is_rejected` passing a `FileEntry::with_deletes` whose
  `DeleteMechanism::IcebergPositionalDelete` path names a different scheme/host than `first_abs` and
  asserting the error names `"delete file"`; and one named
  `a_deletion_vector_is_not_checked_against_the_object_store_root` passing an entry whose only delete
  is a `DeleteMechanism::DeltaDeletionVector` with a non-URI `path_or_inline_dv` and asserting the
  call returns `Ok`.

### crates/lakehouse-engine/src/scan/field_id_projection_tests.rs

#### [OUTDATED_COMMENT] `present_field_binds_real_value_not_default`'s doc still keys defaults by field-id
- Location: line 711
- Issue: the doc ends "even when a default exists for its field-id", but this change re-keyed
  `FieldIdResolution::defaults` to the logical column NAME (`field_id_projection.rs:1486-1490`), and
  the test body now builds `resolution_with_defaults(&[("rating", …)])` — a logical name, not an id.
  The comment describes a lookup key the code no longer has.
- Fix: In `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, change line 711 to read
  "even when a default exists for its logical column name."

### crates/lakehouse-engine/src/scan/spec_tests.rs

#### [SHRINKABLE] The closed-set storage-kind fixture carries an unrelated `partition_values` member
- Location: line 1841 (`deletion_vector_storage_kind_outside_the_closed_set_is_refused`)
- Issue: the fixture was rewritten as the object wire form and had `"partition_values":{"region":"eu"}`
  added purely so `FileEntryWire::WithPartitionValues` matches — the variant requires that key. The
  partition value has nothing to do with the storage kind under test, so a reader must reconstruct why
  it is there, and the fixture now carries two independent reasons it could fail. The sibling test
  added in the same change, `an_iceberg_delete_content_type_outside_the_closed_set_is_refused`
  (line 2013 onward), already uses the minimal 3-tuple form for the identical assertion shape.
- Fix: In `crates/lakehouse-engine/src/scan/spec_tests.rs`, rewrite
  `deletion_vector_storage_kind_outside_the_closed_set_is_refused`'s fixture as the 3-tuple form
  `[["f.parquet",1,[{"storage":"puffin","path_or_inline_dv":"x","size_in_bytes":1,"cardinality":1}]]]`,
  dropping the `partition_values` member, so the unknown storage kind is the fixture's only defect —
  matching `an_iceberg_delete_content_type_outside_the_closed_set_is_refused`.

## Expert fixes

### crates/lakehouse-engine/src/scan/field_id_projection_tests.rs

#### [MISSING_BOUNDARY_TEST] The two fill-seam behaviour fixes are pinned only at the intermediate binding set, never at the emitted expression
- Location: `field_id_projection.rs` lines 1522-1544 (the `absent_default_by_index` filter) versus
  `field_id_projection_tests.rs` `no_name_mapping_falls_back_to_physical_name` (line ~2167) and
  `present_field_binds_real_value_not_default` (line 713)
- Issue: replacing `resolved_logical_field_ids` with `ColumnBinding::bound_logical_names` silently
  fixed two latent defects in the old pair. Under the old code a logical field was considered
  "resolved" only when some physical field matched it by embedded `PARQUET:field_id` or through
  `name_mapping`; a column bound by the step-4 physical-name fallback, and a column whose physical
  counterpart carried an embedded id absent from the logical schema, both counted as ABSENT. If such a
  field carried an `initial_default`, `FieldIdExprAdapter::rewrite` emitted `Literal(<default>)`
  instead of reading the real column — returning the default in place of stored data. The new code
  derives boundness from the renamed physical schema, so both cases now bind real values.
  What is asserted is only the intermediate: `no_name_mapping_falls_back_to_physical_name` checks
  `binding.bound_logical_names == {"rating"}`, and `declared_physical_name_claims_a_field_whose_embedded_id_is_unknown`
  checks the renamed name. No test drives either case through `rewrite` with a default present, which
  is the observable behaviour that changed — `present_field_binds_real_value_not_default` covers only
  the embedded-field-id and `name_mapping` paths. So the fix rests on an implementation-coupled
  assertion, and a future refactor that re-broke it would keep every test green. This matters
  additionally because the spec delta
  `specs/_plans/refactor-neutralize-scan-spec/datafusion-scan/scan-execution-field-id-projection/spec.md:35`
  asserts "`name_mapping` and `initial_default` are unchanged", so no recorded scenario covers the
  corrected behaviour either.
- Fix: In `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`, extend
  `present_field_binds_real_value_not_default` with two further cases driven through `rewrite_with`,
  each using `resolution_with_defaults` keyed by the logical column's name so a default IS available
  for the column under test, and each asserting the rewritten expression resolves to a real `Column`
  (bare or `CastExpr`-wrapped, as the existing name-mapping case already unwraps) at the physical
  index and is NOT a `Literal`: (1) a logical field carrying `field_id: Some(2)` and a default, whose
  physical counterpart is `field_no_id` with the SAME name as the logical field and no covering
  `name_mapping` entry — the step-4 physical-name fallback; (2) a logical field carrying
  `field_id: Some(2)` and a default, whose physical counterpart carries an embedded field-id absent
  from the logical schema (for example 99) but the same NAME as the logical field — the
  unknown-embedded-id case. Give each case an assertion message stating that a column the file
  supplies must bind its real value and must never be default-filled. Extend
  `present_field_binds_real_value_not_default`'s doc comment to name all four resolution paths it now
  covers.
