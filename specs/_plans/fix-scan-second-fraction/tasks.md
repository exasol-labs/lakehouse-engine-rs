# Tasks: fix-scan-second-fraction

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Typed SECOND fraction)
Full task text: plan.md § Implementation Tasks.
- [x] 1.1 Classify the SECOND/EXTRACT(SECOND) argument from exact_expression_type (scan_typing.rs)
- [x] 1.2 Rewrite a classified node into a synthetic DataFusion-only node in type_scan_tree [expert]
- [x] 1.3 Render the synthetic node in the DataFusion dialect (lib.rs)
- [x] 1.4 exact_expression_type returns DECIMAL(2+p, p) for SECOND over TIMESTAMP(p)
- [x] 1.5 Unit tests in scan_typing_tests.rs
- [x] 1.6 Update lib_tests.rs assertions
- [x] 1.7 Declared-surface sweep via render_scan_expression
- [x] 2.1 E2E parity test over qualified_probe() in timestamp_precision.rs
- [x] 2.2 E2E over ALL_TYPES / TIMESTAMP(9) via lakehouse_vs()
- [x] 2.3 Build .so, make e2e-up, run the E2E tests

## Phase 4: Review Fixes
- [x] 4.1 In crates/vs-expression/src/lib.rs, change the `UNTYPED_SECOND` string to "SECOND renders only as a one-argument call through the typed scan pass, which knows the argument's precision" and its doc comment to "Only the typed scan pass knows the fractional digits a one-argument SECOND keeps."
- [x] 4.2 In crates/vs-expression/src/scan_typing_tests.rs, add TS1 (TIMESTAMP(1)) and TS7 (TIMESTAMP(7)) to `columns()` and two rows to `second_renders_the_fraction_of_its_argument_precision` pinning the microsecond-to-nanosecond boundary
- [x] 4.3 In crates/vs-expression/src/scan_typing_tests.rs, add TSLTZ (TIMESTAMP WITH LOCAL TIME ZONE, precision 6) to `columns()` and `second(col("TSLTZ"))` to `second_over_any_other_argument_declines`
- [x] 4.4 In crates/vs-expression/src/lib_tests.rs, delete the doc comment above `fn microsecond_ts()`
