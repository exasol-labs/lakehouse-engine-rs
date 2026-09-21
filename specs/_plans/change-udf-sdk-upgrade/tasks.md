# Tasks: change-udf-sdk-upgrade

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 1.1 Set exasol-udf-sdk and exasol-udf-macros to 0.28.1 in root Cargo.toml [workspace.dependencies], update Cargo.lock. Confirm `make print-slc-version` prints 0.28.1
- [x] 1.2 Run `cargo check --workspace --all-targets` and `cargo check --workspace --all-targets --features exasol-e2e,unity-e2e,lakekeeper-e2e,azure-e2e,cloud-e2e`; record full error list; any file the compiler names not already listed in tasks 2-3 MUST be added to this plan
- [x] 2.1 types/mapping.rs: replace two-state TimestampPrecision with three-value source width (Millisecond/Microsecond/Nanosecond) + declaration()/arrow_unit()/from_declared_digits(); add EngineTimestampSupport with from_database_version + clamp() [expert]
- [x] 2.2 types/mapping.rs producers: iceberg_primitive_to_exasol splits Timestamp|Timestamptz vs TimestampNs|TimestamptzNs arms via engine.clamp(source).declaration(); unity_type_name_to_exasol names Microsecond with protocol comment; rename threaded parameter through iceberg_type_to_exasol and column_source_type_to_exasol
- [x] 2.3 types/mapping.rs exasol_type_to_arrow: TIMESTAMP/TIMESTAMP(p) arm resolves TimeUnit via TimestampPrecision::from_declared_digits, bare TIMESTAMP treated as 3
- [x] 2.4 adapter/mod.rs: handle_create_virtual_schema calls EngineTimestampSupport::from_database_version; build_listing_virtual_tables takes renamed parameter type
- [x] 2.5 Update tests naming the old enum in types/mapping_tests.rs, adapter/adapter_tests.rs, adapter/catalog_client_tests.rs; rewrite exasol_type_to_arrow_parses_timestamp_precision into per-precision assertions; add table-driven test for all four Iceberg timestamp variants on both engine arms plus from_declared_digits(p).arrow_unit() for p in 0-9
- [x] 3.1 scan/emit.rs target_arrow_type: delete TimestampTz/Geometry/HashType/IntervalYearToMonth/IntervalDayToSecond arms; match ExaType::Timestamp{precision} via TimestampPrecision::from_declared_digits(*precision).arrow_unit(); keep match exhaustive, no wildcard [expert]
- [x] 3.2 scan/emit.rs decimal_target: take precision: u32, scale: u32; delete Option zip and absent-payload error branch
- [x] 3.3 scan/convert.rs: replace timestamp_to_micros with a conversion producing chrono::NaiveDateTime from the array's own unit with no digit loss, no i64-nanosecond-count intermediate; update call site at :133 [expert]
- [x] 3.4 scan/declared_columns_test_support_tests.rs: drop Some(...) from varchar()/numeric(), wrap declared_size/declared_precision/declared_scale returns in Some(...)
- [x] 3.5 tests/scan_fixture/mod.rs: same helper changes for varchar()/decimal() plus declared_type_name rendering ExaType::Timestamp{precision} as TIMESTAMP({precision})
- [x] 3.6 scan/emit_tests.rs: update variant sweep for ten-variant enum; replace exa_type_timestamp_maps_to_microsecond_target with per-precision (0-9) assertions; add coerce_column nanosecond-passthrough test; trim absent-payload cases from the two named numeric tests and rename; update Char{size} sites
- [x] 3.7 scan/partial_agg_tests.rs: delete absent-payload case, rename test, update ExaType::String{size} site; add MIN/MAX nanosecond conversion test
- [x] 3.8 tests/micro_bench.rs: update numeric(), the two ExaType::String{size} sites, and the two ExaType::Timestamp sites

## Phase 2: Implementation (Group B)
- [x] 4.1 crates/vs-expression/src/lib.rs: delete snap_timestamp_precision; render_cast_target's Dialect::DataFusion TIMESTAMP arm renders p in {0,3,6,9} verbatim, declines (Err) every other value including above 9
- [x] 4.2 crates/vs-expression/src/lib_tests.rs renders_cast_timestamp_precision_per_dialect: replace snap assertion with decline assertions for {1,2,4,5,7,8}, keep rendering assertions for {0,3,6,9}; assert safe variant returns None; Exasol-dialect half stays green for all p in 0-9

## Phase 2: Implementation (Group C)
- [x] 4.3 adapter/pushdown/pushdown_tests.rs: regression test that a pushdown request with a function_scalar_cast to TIMESTAMP p=2 returns the qualified single-table wrapper shape, not a plain row-scan projection
- [x] 5.1 docker compose down -v, up -d, make test-e2e on default 2025.1.16 image; confirm no SLC fingerprint mismatch; e2e_timestamp_precision_test passes unchanged for microsecond columns [expert]
- [x] 5.2 Extend e2e_timestamp_precision_test.rs with three emit widths over e2e_tsprecision fixture; assert EMITS clause per width (precondition), then value round-trip for (a) TIMESTAMP(9), (b) TIMESTAMP(3), (c) declined CAST(2) via wrapper; classify outcome into STOP/engine-limit/engine-build-difference cases [expert]
- [x] 5.4 (runs between 5.2 and 5.3) Extend seed_timestamp_precision_probe with a ts_ns Iceberg timestamp_ns column (format-version=3 property); assert TIMESTAMP(9) declaration + distinct values on >=2025 arm, clamp behavior on 8.x arm; record outcome or seeding-failure narrowing [expert]
- [x] 5.3 docker compose down -v, EXASOL_IMAGE=exasol/docker-db:8.29.13, up -d, make test-e2e; confirm clamped bare TIMESTAMP + millisecond emit; observe SLC-reported precision via ALTER SESSION SCRIPT_OUTPUT_ADDRESS debug and record it [expert]
- [x] 5.5 Record measured behavior in decision-log.md entry [1]; correct type-mapping-timestamp-precision spec.md live-engine clauses to match measurement exactly [expert]
- [x] 5.6 Reset stack, re-run 2025.1.16 leg; both engine legs finish 0 failures; guarded assertions run only on their selected arm, unguarded assertions green on both [expert]

## Phase 4: Review Fixes
- [x] 4.4 types/mapping.rs: fix `TimestampPrecision::from_declared_digits` doc comment (line 334) from "the finest of the three that is NOT COARSER than `p`" to "the COARSEST of the three that is not coarser than `p` — the widest unit that still holds every digit `p` declares, floored at millisecond"; apply the same correction to `declared_digits_resolve_to_the_arrow_unit_of_their_source_width` in mapping_tests.rs (line 1394), to `exa_type_timestamp_maps_to_the_arrow_unit_of_its_declared_precision` in scan/emit_tests.rs (line 364), and to the `*AND*` clause at line 197 of specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md (replace "SMALLEST ... NOT COARSER" with "COARSEST ... NOT COARSER")
- [x] 4.5 scan/convert.rs: change `timestamp_to_naive_datetime`'s return type to `Result<NaiveDateTime, UdfError>`, drop the `unwrap_or_else` epoch fallback, return `Err(UdfError::User(...))` on `None` naming the Arrow unit, raw array value, derived seconds/sub-second nanos, and that the instant is outside `chrono::NaiveDateTime`'s range; propagate with `?` at the call site in `arrow_value_at`; add `timestamp_outside_the_representable_range_fails_the_conversion` to scan/convert_tests.rs building a `TimestampSecondArray` holding `i64::MAX` and asserting `Err` naming the column's unit
- [x] 4.6 tests/common/seed.rs: move `ICEBERG_FORMAT_VERSION_PROPERTY` and `ICEBERG_FORMAT_VERSION_3` (with their own doc comment) above the doc block at line 3527 so that block again immediately precedes and documents `E2E_TSPRECISION_NAMESPACE`
- [x] 4.7 tests/e2e_timestamp_precision_test.rs + tests/common/timestamp_precision.rs: move `engine_is_2025_or_later` into timestamp_precision.rs as `pub fn engine_honors_declared_precision(conn: &mut ExaConn) -> bool`, extract the leading-component version parse into one private helper shared with `expected_timestamp_precision_for`, and replace all three `engine_is_2025_or_later(&mut conn)` call sites in e2e_timestamp_precision_test.rs with the new function
- [x] 4.8 tests/e2e_timestamp_precision_test.rs: in `cast_to_timestamp3_emits_milliseconds_and_collapses_the_seeded_pairs`, strengthen the clamped-leg EMITS precondition (extend `assert_emits_declares` or add a sibling assertion) so it requires the EMITS clause to contain `"TIMESTAMP"` and NOT contain `"TIMESTAMP("`, catching a regression that declares a parameterized precision on the clamped arm
- [x] 4.9 tests/common/timestamp_precision.rs: fix the module header at line 5 to name `EngineTimestampSupport::from_database_version` (crates/lakehouse-engine/src/types/mapping.rs) instead of the no-longer-existing `TimestampPrecision::from_database_version`
- [x] 4.10 types/mapping_tests.rs: delete `iceberg_timestamptz_declares_timestamp_at_the_gated_precision` (line 1448) together with its doc comment, fully subsumed by `every_iceberg_timestamp_variant_declares_its_own_source_width`; append its zone-flattening rationale (zoned variant collapses to plain Exasol TIMESTAMP, never TIMESTAMP WITH LOCAL TIME ZONE, independent of fractional-second width) as one sentence to that test's doc comment (line 1372)
- [x] 4.11 vs-expression/src/lib_tests.rs: extend `renders_cast_timestamp_precision_per_dialect`'s declining-precision loop (line 875) from `[1u64, 2, 4, 5, 7, 8]` to `[1u64, 2, 4, 5, 7, 8, 10, 99]`, keeping both existing assertions and messages unchanged
- [x] 4.12 specs/_plans/change-udf-sdk-upgrade/datafusion-scan/scan-execution-partial-agg/spec.md: rewrite the `*AND*` clause at line 45 to constrain `timestamp_to_naive_datetime` (crates/lakehouse-engine/src/scan/convert.rs) instead of the deleted `timestamp_to_micros`: the `TimeUnit::Nanosecond` branch SHALL NOT divide the array value by 1,000 and SHALL NOT route the instant through an `i64` count of nanoseconds, because `Value::Timestamp` carries a nanosecond-resolved `chrono::NaiveDateTime` and an `i64` nanosecond count represents only 1677-2262; leave the Background reference at line 17 unchanged

## Phase 3: Verification
- [x] V.1 Run automated checklist: make cross-udf-build, cargo test, E2E (2025.x and 8.x), compile gate, clippy, fmt
- [x] V.2 Scenario coverage audit against plan's Verification > Scenario Coverage table
- [x] V.3 Manual verification per plan's Verification > Manual Testing table
