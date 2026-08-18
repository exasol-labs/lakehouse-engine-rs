# Tasks: add-type-relaxation

## Phase 2: Implementation (Group A)
- [x] 1.1 File two GitHub issues (Iceberg date-promotion bounds-width gap = #355; Iceberg `unknown` type support = #356) and substitute their numbers into `vs-adapter/iceberg-type-promotion/spec.md` and the refusal text
- [x] 1.2 Add `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` (declared from `scan/mod.rs`) pinning `arrow::compute::can_cast_types` for all thirteen supported physical-to-logical pairs and asserting `long` → `double` is absent
- [x] 3.1 Add a `delta.typeChanges` parser reading `fromType`, `toType`, optional `fieldPath`, ignoring unrecognized keys (notably `tableVersion`)
- [x] 4.4 Pin the exhaustiveness tripwire in `types/mapping_tests.rs`: `iceberg_primitive_to_arrow` and `iceberg_primitive_to_exasol` answer every `PrimitiveType` variant
- [x] 6.1 Add `scripts/spark-fixtures/create_iceberg_type_promotion_fixture.sql`: format-version-2 `iceberg_type_promotion` — done, wired into `run_fixtures.sh`. No `iceberg_date_promotion` table — Iceberg Java's `TypeUtil.isPromotionAllowed` implements no `date` promotion at any version this stack can run; resolved via decision [14] (pre-authorized fallback: unit-test-only coverage for this pair, task 4.3) [expert]

## Phase 2: Implementation (Group B)
- [x] 1.3 Extend `type_relaxation_tests.rs` with the read assertion per pair (four pairs no fixture reaches, plus a two-file case) [expert]
- [x] 2.1 Add `TableFeature::TypeWidening` and `TypeWideningPreview` to `is_allow_listed` (`format/delta_protocol.rs`)
- [x] 3.2 Implement the supported-pair predicate per the protocol's list, decimal targets as `k1 >= k2 >= 0`, `Long` → `Double` absent [expert]
- [x] 4.1 Implement `refuse_date_promotion` over `TableMetadata::schemas_iter` [expert]
- [x] 5.1 Extend `scripts/unity/seed.sh`'s `type_widening` entry from 3 columns to all 13, each at its widened Delta type
- [x] 6.2 Add `crates/lakehouse-engine/tests/common/type_promotion_fixtures.rs` (ground truth for `iceberg_type_promotion` only, declared in `tests/common/mod.rs` under `exasol-e2e`); add `crates/lakehouse-engine/tests/e2e_type_relaxation_test.rs`; add to `make test-e2e`'s `--test` list

## Phase 2: Implementation (Group C)
- [x] 2.2 Delete `describe_refused_feature` and its issue-#349 arm, inlining `to_string()`; update `ensure_readable`
- [x] 2.3 Update `delta_protocol_tests.rs`: `typeWidening` refusal/#349 tests → PASS assertions; seven-feature allow-list test; refusal tests switch to `variantType` carrier
- [x] 3.3 Wire the `delta.typeChanges` validation into `build_delta_table_schema` so an unsupported recorded change adds a `RefusedColumn`
- [x] 4.2 Call `refuse_date_promotion` from `resolve_scan` BEFORE `ensure_supported_delete_mechanisms`
- [x] 6.3 Add the fixture-shape test asserting the pre-promotion data file's physical Parquet types (`INT32`, `FLOAT`, `INT64` carrying the `DECIMAL(10,2)` logical annotation)

## Phase 2: Implementation (Group D)
- [x] 2.4 Move `type-widening` from refused-fixture to allow-listed replay list in `delta_replay_tests.rs`; switch `refused_protocol_table_storage` in `pushdown_tests.rs` to a `variantType` carrier
- [x] 3.4 Add unit tests: every supported pair accepted; `Long`→`Double` refused; decimal cases; `tableVersion` ignored; a `fieldPath` entry validated without path parsing; a table with no annotation unchanged
- [x] 4.3 Add unit tests over synthetic `TableMetadata`: `date`→`timestamp` refused, `date`→`timestamp_ns` refused, unpromoted `date` planning normally, `int`→`long`/`float`→`double`/decimal-precision histories planning normally
- [x] 5.2 Replace `TYPE_WIDENING` case in `unity_delta_unsupported_reader_feature_fails_the_query_loud` with `UNSHREDDED_VARIANT` alone, dropping `cites_349`
- [x] 5.3 Add the widened-read E2E test over `TYPE_WIDENING` (11-column projection for protocol-supported columns, pre/post-widening values, pushdown SQL capture, plus a per-column refusal assertion for `byte_decimal`/`short_decimal` — decision [15])
- [x] 6.4 Add the E2E test: `iceberg_type_promotion` (pre/post rows). No `iceberg_date_promotion` E2E test — refusal coverage is unit-only (task 4.3, decision [14])
- [x] 7.1 Update `scripts/unity/fixtures/PROVENANCE.md`'s `type-widening` row and `scripts/unity/README.md` matching rows

## Phase 4: Review Fixes
- [x] R.1 delta_schema.rs: fix two OUTDATED_COMMENT doc claims (build_delta_table_schema, ClassifiedDeltaColumn); remove dead field_path member + DEAD_FLEXIBILITY pub(super) widening; extract unsupported_type_change helper (MIXED_ABSTRACTION_LEVEL) [standard]
- [x] R.2 type_promotion_fixtures.rs + create_iceberg_type_promotion_fixture.sql: delete six unused source/target-type constants; replace pi/e float_double literals to resolve SUPPRESSED_WARNING, in lockstep with the SQL fixture and its ground-truth header [standard]
- [x] R.3 emit_tests.rs: add a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch (MISSING_BOUNDARY_TEST) [standard]
- [x] R.4 build_convention.rs: add the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e (MISSING_BOUNDARY_TEST) [standard]
- [x] R.5 mapping_tests.rs: rewrite the exhaustiveness tripwire test to be a real compile-time exhaustive-match assertion (ASSERTION_FREE_TEST) [expert]
- [x] R.6 delta_replay_tests.rs: add an_unsupported_recorded_type_change_refuses_only_its_own_column against the real vendored fixture (MISSING_BOUNDARY_TEST) [expert]
- [x] R.7 e2e_unity_test.rs: add the missing Scenario doc comment AND extend unity_delta_type_widening_returns_the_widened_types_across_both_files with the 5 missing column value assertions, verifying decimal rendering live against the Unity stack (MISSING_DOC_COMMENT + MISSING_BOUNDARY_TEST, folded together — same test function) [expert]
- [x] R.8 iceberg_tests.rs: add a_readable_iceberg_promotion_plans_normally_and_carries_the_current_type (MISSING_BOUNDARY_TEST) [expert]

## Phase 3: Verification
- [x] V.1 Build: `make cross-musl-udf-build` — exit 0
- [x] V.2 Test: `cargo test` — 1058 passed, 0 failed (lib), all other binaries green
- [x] V.3 Lint: `cargo clippy --all-targets --all-features` — clean
- [x] V.4 Format: `cargo fmt --check` — clean
- [x] V.5 E2E (Iceberg): `make test-e2e` (justified — gates merge in CI) — 264 passed, 0 failed after fixing a decimal ground-truth mismatch (Exasol trims the trailing zero on `12345678.90` → `12345678.9`; corrected `type_promotion_fixtures.rs`'s four `decimal_decimal` constants to match live rendering, consistent with the project's known `e2e_decimal_cast_trims_trailing_zeros` behavior)
- [x] V.6 E2E (Unity/Delta): `make test-e2e-unity` (justified — gates merge in CI) — 24 passed, 0 failed
