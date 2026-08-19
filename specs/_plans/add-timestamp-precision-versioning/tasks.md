# Tasks: add-timestamp-precision-versioning

## Phase 2: Implementation (Group A)
- [x] 1. Live-verify the precision surface against the Docker stack (2025.2.1 and 8.29.13), record captures in decision-log.md [expert]

## Phase 2: Implementation (Group B)
- [x] 2. Add `TimestampPrecision` enum + `from_database_version` to `types/mapping.rs`, unit-tested over the full matrix [expert]
- [x] 6. Add `seed_timestamp_precision_probe` to `tests/common/seed.rs`
- [x] 10. Matrix the core `e2e` job in `.github/workflows/ci.yml` over 2025.2.1 and 8.29.13

## Phase 2: Implementation (Group C)
- [x] 3. Thread resolved precision through the declaration pipeline per the exact call-site census [expert]
- [x] 4. Give `exasol_type_to_json` a `TIMESTAMP(p)` arm
- [x] 5. Add the shared E2E precision oracle to `tests/common/`
- [x] 12. Update the `Timestamp(_, _)` row of CLAUDE.md's Data types table

## Phase 2: Implementation (Group D)
- [x] 7. Add `tests/e2e_timestamp_precision_test.rs` [expert]
- [x] 8. Repair `e2e_upper_timestamp_declines_to_native_oracle` in `tests/e2e_capability_test.rs`
- [x] 9. Add version-aware Delta declared-type assertions to `tests/e2e_unity_test.rs`

## Phase 2: Implementation (Group E)
- [x] 11. Run the full local E2E suite against both images, confirm green, reconcile any precision-sensitive assertion

## Phase 3: Verification
- [x] V1. Automated checks (build, cargo test, clippy, fmt)
- [x] V2. Scenario coverage audit
- [x] V3. Manual verification steps

## Phase 4: Review Fixes
- [x] 4.1 Split `unity_type_name_to_exasol`'s `precision`/`scale` params into a `CatalogDecimal { precision: u32, scale: u32 }` struct so adding `timestamp_precision` keeps it at 3 params (mapping.rs)
- [x] 4.2 Rewrite the `TimestampPrecision` doc (mapping.rs:275-277) to say both engine lines accept `TIMESTAMP(6)` but only the calendar-versioned line honors it — 8.x clamps to `TIMESTAMP(3)` and strips `fractionalSecondsPrecision` from the pushdown echo — citing decision-log.md [C1]/[C3]
- [x] 4.3 Replace the `from_database_version` rationale paragraph (mapping.rs:293-298) to drop the retracted "fails loudly" claim per decision-log.md [C1]/[C6]
- [x] 4.4 Collapse the two-line rationale block above `DataType::Timestamp(_, _)` (mapping.rs:68-70) into a single WHY-line
- [x] 4.5 Extend `exasol_type_to_json_renders_timestamp_fractional_seconds_precision` with malformed-`TIMESTAMP(p)` boundary cases (`TIMESTAMP()`, `TIMESTAMP(abc)`, `TIMESTAMP(-1)`) plus a doc note (mapping_tests.rs)
- [x] 4.6 Add `retained_fractional_digits: u32` to `ExpectedTimestampPrecision`, drop the string-decoding `declared_precision` helper in `e2e_timestamp_precision_test.rs` (tests/common/timestamp_precision.rs, tests/e2e_timestamp_precision_test.rs)
- [x] 4.7 Extend `declared_column_type`'s doc comment to record both contracts (exact `COLUMN_TYPE` string and legal CAST target restricted to `p in {3, 6}`), citing decision-log.md [C3] (tests/common/timestamp_precision.rs)
- [x] 4.8 Strip whitespace from the `column_types` value before each `assert_eq!` in `unity_delta_timestamp_columns_declare_the_exact_gated_precision`, matching `e2e_timestamp_precision_test::declared_type` (tests/e2e_unity_test.rs)
- [x] 4.9 Rewrite the "only by coincidence" comment (e2e_capability_test.rs:2731-2737) to state the `.100` fixture renders identically under both CAST targets on both engines per decision-log.md [C4]
- [x] 4.10 Rewrite the `Timestamp(_, _)` row of CLAUDE.md's Data types table to separate the Arrow→Value (always bare `TIMESTAMP`) and catalog-declared (version-gated) directions, and fix the spec link to `specs/datafusion-scan/type-mapping/spec.md`
- [x] 4.11 File a GitHub issue for adding `E2E (8.29.x)` to main's required-checks ruleset, then cite it inline in the `.github/workflows/ci.yml` comment above the `e2e` job (lines 458-460)
