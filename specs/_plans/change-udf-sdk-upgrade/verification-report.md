# Verification Report: change-udf-sdk-upgrade

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | SDK pin moved to 0.28.1, timestamp precision split into source width and engine ceiling, both emit boundaries follow the declared precision, CAST precision declines outside `{0,3,6,9}`, all measured live on 2025.1.16 and 8.29.13 |
| Code review | 9 findings — 9 fixed |

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

| Type | Coverage % |
|------|------------|
| Unit | Not measured (no coverage tool run this pass) |
| Integration | All scenarios in the plan's Scenario Coverage table have a passing test on both engine legs |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (host, `cargo test --workspace`) | 53 binaries | 1667 | 2 |
| Integration (E2E, Exasol 2025.1.16) | 15 binaries | 341 | 0 |
| Integration (E2E, Exasol 8.29.13) | 15 binaries | 341 | 0 |

One transient BucketFS-upload network flake occurred mid-run on the first 2025.1.16 pass
(`cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs`); isolated retry passed,
and a full clean rerun (`target/speq-e2e-2025-final.log`) confirms 0 failures. Not a code defect.

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets
(clean, 0 warnings)
```

### Formatter

```
cargo fmt --all -- --check
(clean, no diff)
```

### Compile gate

```
cargo check --workspace --all-targets --features exasol-e2e,unity-e2e,lakekeeper-e2e,azure-e2e,cloud-e2e
EXIT=0
```

### make print-slc-version

```
0.28.1
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | type-mapping-timestamp-precision | Catalog timestamp column declared TIMESTAMP(6) on Exasol 2025.x+ | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `timestamp_declaration_is_version_gated_for_both_catalog_kinds`, `database_version_leading_component_selects_the_declared_timestamp_precision` | Pass |
| datafusion-scan | type-mapping-timestamp-precision | Nanosecond catalog timestamp column declared TIMESTAMP(9) on 2025.x+ | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `every_iceberg_timestamp_variant_declares_its_own_source_width` | Pass |
| datafusion-scan | type-mapping-timestamp-precision | Nanosecond catalog timestamp column declared TIMESTAMP(9), live | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `iceberg_nanosecond_source_column_is_declared_and_retained_per_engine_arm` | Pass (both legs, arm-guarded) |
| datafusion-scan | type-mapping-timestamp-precision | Empty/unparseable database version declares microsecond, unclamped | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unreadable_database_version_declares_the_source_width_unclamped` | Pass |
| datafusion-scan | type-mapping-timestamp-precision | Iceberg timestamptz maps to plain Exasol TIMESTAMP | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `every_iceberg_timestamp_variant_declares_its_own_source_width` | Pass |
| datafusion-scan | type-mapping-timestamp-precision | Declared TIMESTAMP(p) EMITS column maps to the matching Arrow unit | `crates/lakehouse-engine/src/scan/emit_tests.rs`, `crates/lakehouse-engine/src/types/mapping_tests.rs` | per-precision `target_arrow_type`/`exasol_type_to_arrow` tests | Pass |
| datafusion-scan | type-mapping-timestamp-precision | Declared TIMESTAMP(p) EMITS column round-trips live, both engine legs | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_value`, `cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs`, `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` | Pass (both legs) |
| datafusion-scan | scan-execution-value-conversion | Output columns coerced to the declared EMITS Arrow type before emit_batch | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `coerce_batch_to_exa_types_casts_every_declared_variant`, `emit_stream_fails_on_numeric_with_out_of_range_payload` | Pass |
| datafusion-scan | scan-execution-partial-agg | Every emitted partial-aggregate cell matches its declared output column | `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` | `partial_agg_fails_on_numeric_with_out_of_range_payload`, nanosecond MIN/MAX conversion test | Pass |
| sql-comprehension | vs-expression-translator-cast | CAST to TIMESTAMP renders declared fractional-seconds precision per dialect | `crates/vs-expression/src/lib_tests.rs` | `renders_cast_timestamp_precision_per_dialect` | Pass |
| sql-comprehension | vs-expression-translator-cast | CAST to TIMESTAMP renders declared fractional-seconds precision per dialect (routing) | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | task 4.3's wrapper-shape regression test | Pass |
| sql-comprehension | vs-expression-translator-cast | CAST to TIMESTAMP renders declared fractional-seconds precision per dialect (value, live) | `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs` | `declined_cast_to_timestamp2_is_computed_natively_by_exasol_in_the_wrapper` | Pass |

## Notes

- Live-engine measurement (task 5.2/5.4) reached the STOP-free, engine-limit-free case on both
  legs: 2025.1.16 accepts and round-trips all three widths (millisecond, microsecond, nanosecond);
  8.29.13 clamps to millisecond exactly as `[C1]`/`[C3]` predicted, with no rejection. Decision-log
  entry `[1]` and the `type-mapping-timestamp-precision` spec's live-engine clauses were corrected
  to state exactly what was measured (task 5.5).
- 9 standard code-review findings were fixed: inverted doc-comment wording, a silent
  epoch-substitution on an out-of-range timestamp instant replaced with a named error,
  doc-comment/ordering cleanups in the E2E harness, a duplicated version-boundary check
  consolidated into one shared helper, a strengthened clamped-arm EMITS precondition, a stale
  `TimestampPrecision::from_database_version` reference corrected to `EngineTimestampSupport`, a
  fully-subsumed test deleted, and the declining-CAST-precision test extended to cover precisions
  above 9. 0 expert findings.
- One transient BucketFS network flake during the first 2025.1.16 E2E pass, not reproducible on
  retry or full rerun; not attributable to this change.
- Both engine legs finish with identical per-binary test counts (341/341), which also rules out any
  test being silently skipped or `#[ignore]`d on one leg.
