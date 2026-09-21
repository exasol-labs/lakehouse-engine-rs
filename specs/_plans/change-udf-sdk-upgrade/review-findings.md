# Code Review Findings: change-udf-sdk-upgrade

## Summary
- Files reviewed: 23
- Total findings: 9 (standard: 9, expert: 0)

Scope note: the SDK pin move, the `TimestampPrecision`/`EngineTimestampSupport` split, both emit
boundaries, the CAST decline and the two-engine measurement all land as planned. Every finding below
is a local defect in the delivered code or its documentation; none of them challenges the design.

## Standard fixes

### crates/lakehouse-engine/src/types/mapping.rs

#### [OUTDATED_COMMENT] `from_declared_digits` doc states the inverse of the rule it implements
- Location: line 334
- Issue: the doc comment says the function returns "the finest of the three that is NOT COARSER than
  `p`". The finest of `{Millisecond, Microsecond, Nanosecond}` is `Nanosecond`, and `Nanosecond` is
  never coarser than any `p`, so the sentence as written describes a function that returns
  `Nanosecond` for every input. The body returns the *coarsest* unit that is not coarser than `p`
  (`0..=3 => Millisecond`, `4..=6 => Microsecond`, `_ => Nanosecond`). This is the doc comment on the
  single owner of the precision table, so the inverted wording is the most load-bearing comment in
  the change. The same inverted phrasing was copied into two test doc comments
  (`crates/lakehouse-engine/src/types/mapping_tests.rs:1394` "the finest of the three source widths
  that is NOT COARSER than it" and `crates/lakehouse-engine/src/scan/emit_tests.rs:364` "resolves to
  the finest unit not coarser than it") and into the spec delta
  (`specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md:197`
  "the SMALLEST of those three units that is NOT COARSER than the declaration", where "smallest unit"
  also reads as nanosecond).
- Fix: In `crates/lakehouse-engine/src/types/mapping.rs`, change `TimestampPrecision::from_declared_digits`'s
  doc comment (line 334) from "the finest of the three that is NOT COARSER than `p`" to "the COARSEST
  of the three that is not coarser than `p` — the widest unit that still holds every digit `p`
  declares, floored at millisecond". Apply the same correction to the doc comment of
  `declared_digits_resolve_to_the_arrow_unit_of_their_source_width` in
  `crates/lakehouse-engine/src/types/mapping_tests.rs` (line 1394), to the doc comment of
  `exa_type_timestamp_maps_to_the_arrow_unit_of_its_declared_precision` in
  `crates/lakehouse-engine/src/scan/emit_tests.rs` (line 364), and to the `*AND*` clause at line 197
  of `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`,
  replacing "the SMALLEST of those three units that is NOT COARSER than" with "the COARSEST of those
  three units that is NOT COARSER than". Change no code.

### crates/lakehouse-engine/src/scan/convert.rs

#### [SWALLOWED_ERROR] An out-of-range instant is silently replaced with the UNIX epoch
- Location: line 207
- Issue: `timestamp_to_naive_datetime` ends with
  `DateTime::<Utc>::from_timestamp(seconds, subsecond_nanos).map(|dt| dt.naive_utc()).unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap().naive_utc())`.
  When `from_timestamp` returns `None` — reachable from a `TimestampSecondArray` or
  `TimestampMillisecondArray` whose value exceeds `chrono`'s representable range — the function
  substitutes `1970-01-01T00:00:00` and the scan emits that as the row's real value. The caller
  `arrow_value_at` already returns `Result<Value, UdfError>`, so the error mechanism is available and
  unused. This is the same silent-loss defect class the plan exists to remove: the plan's own
  rationale for this rewrite is that "nothing records that loss", and the rewritten function still
  records nothing for this branch.
- Fix: In `crates/lakehouse-engine/src/scan/convert.rs`, change `timestamp_to_naive_datetime`'s return
  type to `Result<NaiveDateTime, UdfError>`, drop the `unwrap_or_else` epoch fallback, and return
  `Err(UdfError::User(...))` on `None` with a message naming the Arrow unit, the raw array value, the
  derived seconds and sub-second nanoseconds, and that the instant is outside the range
  `chrono::NaiveDateTime` represents. Propagate with `?` at the single call site in `arrow_value_at`
  (`Value::Timestamp(timestamp_to_naive_datetime(col, row, unit)?)`). Add a test to
  `crates/lakehouse-engine/src/scan/convert_tests.rs` named
  `timestamp_outside_the_representable_range_fails_the_conversion` that builds a
  `TimestampSecondArray` holding `i64::MAX`, calls the batch-to-rows helper the sibling timestamp
  tests already use, and asserts the call returns an `Err` whose message names the column's unit —
  not a `Value::Timestamp` at the epoch.

### crates/lakehouse-engine/tests/common/seed.rs

#### [OUTDATED_COMMENT] The tsprecision namespace doc block now documents the format-version constant
- Location: lines 3527-3540
- Issue: the two new constants `ICEBERG_FORMAT_VERSION_PROPERTY` and `ICEBERG_FORMAT_VERSION_3` were
  inserted between the pre-existing doc block at lines 3527-3534 ("Namespace and table for the
  timestamp-precision E2E probe … so tasks 7/9 can rely on this fixture deterministically.") and the
  item it documents, `E2E_TSPRECISION_NAMESPACE` at line 3540. Rustdoc now attaches that whole block
  plus the new two-line description to `ICEBERG_FORMAT_VERSION_PROPERTY`, producing one doc comment
  that describes two unrelated things, and leaves `E2E_TSPRECISION_NAMESPACE` undocumented.
- Fix: In `crates/lakehouse-engine/tests/common/seed.rs`, move the two constants
  `ICEBERG_FORMAT_VERSION_PROPERTY` and `ICEBERG_FORMAT_VERSION_3` (lines 3537-3538), together with
  their own two-line doc comment ("The Iceberg table property that fixes a table's format version…"),
  to sit ABOVE the doc block that begins at line 3527, so that block is again immediately followed by
  `pub const E2E_TSPRECISION_NAMESPACE` and documents it.

### crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs

#### [INFORMATION_LEAKAGE] The 2025 engine boundary is re-implemented a second time on the test side
- Location: line 146
- Issue: `engine_is_2025_or_later` parses the leading dot-separated version component and gates on
  `>= 2025` with an unparseable-version fallthrough. `crates/lakehouse-engine/tests/common/timestamp_precision.rs`
  already owns exactly that parse for this test suite, in `expected_timestamp_precision_for` (line 75),
  and that module's own header states it exists to be the suite's single independent oracle for the
  version rule. The boundary now lives in two test-side places and would have to be edited in both;
  a future divergence makes `cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_value` and
  `declined_cast_to_timestamp2_is_computed_natively_by_exasol_in_the_wrapper` skip on a leg where
  `iceberg_nanosecond_source_column_is_declared_and_retained_per_engine_arm` still asserts the
  unclamped arm, with no test failing.
- Fix: Move `engine_is_2025_or_later` out of `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs`
  into `crates/lakehouse-engine/tests/common/timestamp_precision.rs` as
  `pub fn engine_honors_declared_precision(conn: &mut ExaConn) -> bool`, keeping its doc comment.
  Extract the leading-component parse into one private helper in that module and have BOTH
  `expected_timestamp_precision_for` and the new function call it, so the `>= 2025` boundary and the
  unparseable-version fallthrough appear exactly once. Import the new function in
  `e2e_timestamp_precision_test.rs` and replace all three `engine_is_2025_or_later(&mut conn)` call
  sites (inside `accepts_every_cast_precision`, `cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs`,
  and `iceberg_nanosecond_source_column_is_declared_and_retained_per_engine_arm`) with it.

#### [ASSERTION_FREE_TEST] The 8.x EMITS precondition cannot fail for any timestamp declaration
- Location: lines 381-386
- Issue: `cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs` selects
  `echoed_declaration = "TIMESTAMP"` on the clamped leg and hands it to `assert_emits_declares`,
  whose body is `assert!(emits.contains(expected), ...)` (line 201). On the 8.x leg the check is
  therefore `emits.contains("TIMESTAMP")`, which is satisfied by `TIMESTAMP`, `TIMESTAMP(3)`,
  `TIMESTAMP(6)` and `TIMESTAMP(9)` alike. The plan makes this precondition the central guard: it is
  asserted before any value so that a stripped `fractionalSecondsPrecision` echo reports as its own
  named failure. On the clamped leg it reports nothing — a regression that declared `TIMESTAMP(6)`
  there (microseconds emitted into a bare declaration, narrowed by the engine) passes this assertion
  and the `COUNT(DISTINCT) == 2` assertion both.
- Fix: In `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs`, add a second parameter to
  `assert_emits_declares` distinguishing an exact bare-`TIMESTAMP` expectation from a parameterized
  one, or add a sibling assertion in
  `cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs` for the clamped arm that
  requires the EMITS clause to contain `"TIMESTAMP"` and NOT to contain `"TIMESTAMP("`, so a
  parameterized declaration on that leg fails. Keep the existing two-cause failure message.

### crates/lakehouse-engine/tests/common/timestamp_precision.rs

#### [OUTDATED_COMMENT] Module header names a production symbol that no longer exists
- Location: line 5
- Issue: the module header reads "Deliberately duplicates the version-to-precision mapping rather than
  calling `TimestampPrecision::from_database_version`". This change moved that function to
  `EngineTimestampSupport::from_database_version`; `TimestampPrecision` no longer carries any version
  rule at all. The one sentence stating why this oracle exists now points at a symbol a reader cannot
  find.
- Fix: In `crates/lakehouse-engine/tests/common/timestamp_precision.rs`, change the module header at
  line 5 to name `EngineTimestampSupport::from_database_version`
  (`crates/lakehouse-engine/src/types/mapping.rs`) instead of `TimestampPrecision::from_database_version`.

### crates/lakehouse-engine/src/types/mapping_tests.rs

#### [DUPLICATE_TEST] `iceberg_timestamptz_declares_timestamp_at_the_gated_precision` is fully subsumed
- Location: line 1448
- Issue: the updated test asserts, for `Timestamptz` and `TimestamptzNs`,
  `DeclaredPrecision -> "TIMESTAMP(6)"/"TIMESTAMP(9)"` and `MillisecondOnly -> "TIMESTAMP"`. The new
  `every_iceberg_timestamp_variant_declares_its_own_source_width` (line 1372) asserts exactly those
  four calls with exactly those four expectations, as a strict subset of its four-variant sweep. Two
  tests now fail or pass together on every change to `iceberg_primitive_to_exasol`'s timestamp arms,
  and the older one adds no case. The zone-flattening scenario it was named for
  (timestamptz -> plain `TIMESTAMP`, never `TIMESTAMP WITH LOCAL TIME ZONE`) is also covered by
  `iceberg_types_map_to_exasol_type`, the test the current plan's Verification table names for it.
- Fix: In `crates/lakehouse-engine/src/types/mapping_tests.rs`, delete the test
  `iceberg_timestamptz_declares_timestamp_at_the_gated_precision` (line 1448) together with its doc
  comment, and append its zone-flattening rationale as one sentence to the doc comment of
  `every_iceberg_timestamp_variant_declares_its_own_source_width` (line 1372): that a zoned variant
  collapses to the plain Exasol `TIMESTAMP` family rather than `TIMESTAMP WITH LOCAL TIME ZONE`,
  which Exasol rejects as a UDF EMITS output type, and that the zone flattening and the
  fractional-second width are independent.

### crates/vs-expression/src/lib_tests.rs

#### [MISSING_BOUNDARY_TEST] No case for a CAST precision above the maximum
- Location: line 875
- Issue: `renders_cast_timestamp_precision_per_dialect` exercises the DataFusion-dialect decline for
  `p` in `{1, 2, 4, 5, 7, 8}` only. `render_cast_target`'s new arm is `0 | 3 | 6 | 9 => Ok(..), _ =>
  Err(..)`, so every `p` above 9 also declines, and plan task 4.1 names that case explicitly
  ("declines every other value including above 9"). The deleted `snap_timestamp_precision` clamped
  anything above 9 to 9 and rendered it, so this is the one input whose behaviour the change reverses
  without a test pinning it.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, extend the declining-precision loop at line 875
  from `[1u64, 2, 4, 5, 7, 8]` to `[1u64, 2, 4, 5, 7, 8, 10, 99]`, keeping both existing assertions
  (`render_expression(&expr).is_err()` and `render_expression_safe(&expr) == None`) and their
  messages unchanged.

### specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-partial-agg/spec.md

#### [OUTDATED_COMMENT] A normative clause names the function this plan deleted
- Location: line 45
- Issue: the requirement reads "the `TimeUnit::Nanosecond` branch of `timestamp_to_micros` MUST NOT
  divide the value by 1,000". Task 3.3 replaced `timestamp_to_micros` with
  `timestamp_to_naive_datetime` (`crates/lakehouse-engine/src/scan/convert.rs:172`), which has no
  division and no micros intermediate, so the clause constrains a symbol that no longer exists and
  will enter the permanent spec library that way at `/speq:record`. The Background reference at line
  17 is historical and correctly describes the pre-change state; only the `*AND*` clause at line 45
  is normative and wrong.
- Fix: In `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-partial-agg/spec.md`,
  rewrite the `*AND*` clause at line 45 so it constrains the current function: the `TimeUnit::Nanosecond`
  branch of `timestamp_to_naive_datetime` (`crates/lakehouse-engine/src/scan/convert.rs`) SHALL NOT
  divide the array value by 1,000 and SHALL NOT route the instant through an `i64` count of
  nanoseconds, because `Value::Timestamp` carries a nanosecond-resolved `chrono::NaiveDateTime` and
  an `i64` nanosecond count represents only 1677-2262. Leave the Background reference at line 17
  unchanged.

## Expert fixes
[none]
