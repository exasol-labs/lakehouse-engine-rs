# Code Review Findings: fix-iceberg-initial-default-rendered-types

## Summary
- Files reviewed: 10
- Total findings: 4

## Findings

### crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs

#### [SCENARIO_TAG_MISMATCH] Unit test carries the E2E scenario's title instead of the encoding scenario it implements
- Location: line 420, `declaration_encodes_every_primitive_initial_default`
- Issue: The plan's coverage table maps this test to "The VS encodes each field's Iceberg initial-default once per query into the scan spec", and maps "An initial-default of a type without a native Arrow tag reads as a stored value of its column reads" to the E2E test `e2e_added_columns_initial_default_fill_all_types`. The unit test asserts only the encoded note (tag and encoded text). It never compares a pre-add row with a stored row, so it does not implement the E2E scenario. No test anywhere carries the changed scenario 1 title (`grep "Scenario: The VS encodes each field"` finds nothing).
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` line 420, replace `/// Scenario: An initial-default of a type without a native Arrow tag reads as a stored value of its column reads` with `/// Scenario: The VS encodes each field's Iceberg initial-default once per query into the scan spec`.

### crates/lakehouse-engine/src/scan/field_id_projection_tests.rs

#### [MISSING_SCENARIO_TAG] Round-trip test lacks its scenario line
- Location: line 1008, `initial_default_round_trips_across_full_type_vocabulary`
- Issue: The plan maps the changed scenario "Every supported primitive initial-default survives the scan-spec serialization round-trip" to this test, and the diff extends the test with the scenario's new `binary`, `fixed_size_binary(L)`, and wrong-length clauses. AGENTS.md requires one `/// Scenario: <title>` line per implemented scenario. The test has none.
- Fix: Insert `/// Scenario: Every supported primitive initial-default survives the scan-spec serialization round-trip` directly above `#[test]` at line 1008 of `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs`.

### crates/lakehouse-e2e/tests/e2e_local/scan.rs

#### [TAUTOLOGICAL_ASSERTION] Pre-add-equals-stored loop cannot fail once the literal assertions pass
- Location: lines 749-760, and the `rendered` field at `crates/lakehouse-e2e/tests/common/seed.rs` line 1671 with its eleven `rendered: false` / `rendered: true` initializers
- Issue: Every `rendered` column is built by `rendered_initdef_column`, which sets `default` and `real` to the same `ExpectedValue::Text(text)`. `ExpectedValue::Text::matches` is exact string equality (`seed.rs` line 1657). The per-row loop above (lines 725-746) already asserts that every pre-add row and every post-add row equals that same text. So every row equals the stored row before the new loop runs, and the new loop adds no coverage. The `rendered` flag exists only to drive the loop, and its value is derivable from `default == real`.
- Fix: Delete the loop at lines 749-760 of `crates/lakehouse-e2e/tests/e2e_local/scan.rs`. In `crates/lakehouse-e2e/tests/common/seed.rs`, delete the `rendered` field and its doc comment from `InitDefColumn`, delete every `rendered: false,` line in `initdef_columns`, and delete `rendered: true,` in `rendered_initdef_column`. Keep the `rendered_initdef_column` helper, whose identical `default` and `real` text already proves that a pre-add row reads as a stored value.

### crates/lakehouse-e2e/tests/common/seed.rs

#### [REINVENTED_STDLIB] Hand-rolled hex decoding of the uuid fixture
- Location: `make_initdef_full_batch`, the `let uuid_bytes = INITDEF_UUID ... .collect::<Vec<u8>>();` block
- Issue: The block decodes the uuid string two characters at a time with `chunks(2)`, `from_utf8`, and `u8::from_str_radix`. The standard library parses the whole value in one call, which is the same big-endian form the adapter's `binary_hex` uses (`uuid.to_be_bytes()`).
- Fix: Replace the `uuid_bytes` block with `let uuid_bytes = u128::from_str_radix(&INITDEF_UUID.replace('-', ""), 16).expect("initdef uuid is hex").to_be_bytes();`, and pass `vec![uuid_bytes; n]` to `FixedSizeBinaryArray::try_from_iter` unchanged.
