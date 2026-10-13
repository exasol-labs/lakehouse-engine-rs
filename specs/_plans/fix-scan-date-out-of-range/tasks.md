# Tasks: fix-scan-date-out-of-range

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 1: Reproduce (Group A)
- [x] 1.1 Add `date_range` fixture to `write_all_fixtures` in `e2e_direct_storage_test.rs`
- [x] 1.2 Write E2E test `out_of_range_date_fails_the_query_on_every_emit_path`
- [x] 1.3 Build, run against unchanged code, confirm failures, record SLC raw-path result for PR description

## Phase 2: Guard (Group A)
- [x] 2.1 Shared DATE-range guard in `scan/convert.rs`; replace both `.unwrap_or(epoch)` in `arrow_value_at`
- [x] 2.2 Table-driven unit test in `scan/convert_tests.rs`
- [x] 2.3 Raw-row path guard in `coerce_batch_to_exa_types` (`scan/emit.rs`), both branches
- [x] 2.4 Aggregate path guard in `coerce_partial_agg_columns` (`scan/partial_agg.rs`)
- [x] 2.5 Rebuild and run E2E until it passes

## Phase 3: Documentation (Group A)
- [x] 3.1 "Dates outside Exasol's DATE range" section in `docs/semantic-differences.md`

## Phase 4: Verification and delivery (Group A)
- [x] 4.1 `cargo fmt --all`, `cargo clippy --all-targets`, `cargo test`
- [x] 4.2 `make test-e2e`
- [x] 4.3 Changed-line coverage via `cargo llvm-cov --workspace --lcov`
- [ ] 4.4 Implementing commit carries `Closes #470`

## Phase 4: Review Fixes
- [x] 4.5 Replace the three doc lines above `check_date32_in_exasol_range` in `scan/convert.rs` with the two-line contract doc
- [x] 4.6 In `scan/convert_tests.rs`, make `date32_guard_accepts_the_range_bounds_and_rejects_one_day_beyond` assert the converted `Value::Date` for each case via `Option<NaiveDate>` expectations
- [x] 4.7 In `scan/convert_tests.rs`, move the first-offender, nulls-only, and non-Date32 assertions into `date32_guard_names_the_first_offender_and_passes_nulls_and_non_date_columns`
- [x] 4.8 In `scan/emit_tests.rs`, add `emit_stream_fails_on_an_out_of_range_date_and_emits_nothing` covering the fully matching branch of `coerce_batch_to_exa_types`
- [x] 4.9 Replace the three doc lines above `check_emitted_dates_in_range` in `scan/emit.rs` with the two-line doc
- [x] 4.10 Delete the redundant DATE-range sentence from the doc of `coerce_partial_agg_columns` in `scan/partial_agg.rs`
- [x] 4.11 Verify `COUNT(DISTINCT STORED_DATE)` fails live, then reword "returns" to "emits" in `docs/semantic-differences.md`
