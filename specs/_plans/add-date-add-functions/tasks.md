# Tasks: add-date-add-functions

## Phase 2: Implementation (Group A: Typed pass and rendering)
- [x] 1.1 Add `TypeClass::Date` and `TypeClass::Timestamp { precision: u8 }`; `TypeClass::of`, `node_class`, `to_text`/`cast_to_text` declines, `union`, doc comments
- [x] 1.2 Add date-addition `ResultRule` variant and the four `VerbatimCall` entries in `SCALAR_FUNCTIONS`
- [x] 1.3 Route the new rule in `scalar_function_class` to one gate function (arg acceptance, declines, CAST wrapping, result class)
- [x] 1.4 Export `vs_add_days`, `vs_add_weeks`, `vs_add_hours`, `vs_add_minutes` constants; DataFusion-dialect per-name arm; delete comment at `lib.rs` 888-889
- [x] 1.5 Unit tests in `scan_typing_tests.rs`
- [x] 1.6 Unit tests in `lib_tests.rs`; update `unsupported_date_functions_decline_in_the_datafusion_dialect`

## Phase 2: Implementation (Group B: Scan functions and the scan rewrite)
- [x] 2.1 Add `scan/datetime_add.rs`: `ScalarUDFImpl` per unit, registered under the four names in the scan-session constructor [expert]
- [x] 2.2 Unit tests in `scan/datetime_add_tests.rs`
- [x] 2.3 Extend `timestamp_literal_floor` in `adapter/pushdown/support.rs` for the four calls; add `overprecise_timestamp_literal_against_a_date_addition_is_floored`

## Phase 2: Implementation (Group C: Capabilities, docs, live verification)
- [x] 3.1 Advertise `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_WEEKS`; update `capabilities_tests.rs`
- [x] 3.2 Update `docs/capabilities.md` and `docs/capability-matrix.md`
- [x] 3.3 Add sections to `docs/semantic-differences.md`
- [x] 3.4 Add E2E module `e2e_local/date_add.rs`, register in `main.rs`; delete comment at `capability.rs` 1354-1355
- [x] 3.5 Add `date_addition_over_a_nanosecond_timestamp_runs_in_exasol` to `timestamp_precision.rs`
- [x] 3.6 `make cross-udf-build` and `make e2e-up test-e2e`; resolve parity differences per plan

## Phase 3: Verification
- [x] 4.1 `make cross-udf-build`
- [x] 4.2 `cargo test --workspace --lib`
- [x] 4.3 `make e2e-up test-e2e`
- [x] 4.4 `make fmt-check lint`
- [x] 4.5 Scenario coverage audit
- [x] 4.6 Manual verification

## Phase 4: Review Fixes
- [x] 4.7 In `scan/datetime_add.rs`, delete the doc comment above `NANOS_PER_MICRO` and give `Unit::nanosecond_count_limit` the 128-bit nanosecond overflow rationale
- [x] 4.8 Add `pub const DATE_ADDITION_FUNCTIONS` next to `ADD_DAYS_FN` in `vs-expression/src/lib.rs` and use it in `scan_fractional_digits` in `adapter/pushdown/support.rs` instead of the four-name `matches!`
- [x] 4.9 In `vs-expression/src/lib_tests.rs`, move the one-argument assertion out of `date_addition_renders_verbatim_in_the_exasol_dialect` into a new test `date_addition_with_one_argument_fails_in_the_datafusion_dialect`
- [x] 4.10 In `vs-expression/src/scan_typing_tests.rs`, rename `a_boolean_date_addition_count_adds_one_or_zero` to `a_boolean_date_addition_count_renders_as_one_or_zero` and add its `/// Scenario: The scan rounds a fractional count as Exasol does` line
- [x] 4.11 In `vs-expression/src/scan_typing.rs` `date_addition_class`, decline a numeric literal count whose magnitude exceeds `MAX_SCAN_CONSTANT_COUNT` (1e10); add unit tests, rewrite the E2E `error_cases` in `e2e_local/date_add.rs`, and update the spec delta, `docs/semantic-differences.md`, and `docs/capabilities.md` [expert]
- [x] 4.12 Replace the pre-1582 paragraph and table in `docs/semantic-differences.md` with the `TO_CHAR`-measured column-argument examples, and correct the proleptic-calendar example in the spec delta and plan.md Context bullet 9 [expert]
- [x] 4.13 Document the empty-result constant-count difference in docs/semantic-differences.md
- [x] 4.14 Add the missing Scenario line to reports_audited_capability_set
