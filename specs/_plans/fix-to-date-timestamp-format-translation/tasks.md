# Tasks: fix-to-date-timestamp-format-translation

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 1.1 Create `crates/vs-expression/src/datetime_format_tests.rs` with two table-driven unit tests of `chrono_for
- [x] 1.2 Add `crates/vs-expression/src/datetime_format.rs`, declared in `lib.rs` as a private `mod datetime_format;
- [x] 1.3 In `crates/vs-expression/src/lib_tests.rs`, change the format rows of `renders_to_date_to_timestamp` to Ex
- [x] 1.4 In `crates/vs-expression/src/lib.rs`, replace the `"TO_DATE"` and `"TO_TIMESTAMP"` arms of `render_express
- [x] 2.1 In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, add `e2e_to_date_to_timestamp_translated_format
- [x] 2.2 In the same file, add `e2e_to_date_translated_format_fails_on_rejected_input` with the line `/// Scenario:
- [x] 2.3 In the same file, add `e2e_to_date_declined_format_matches_native` with the line `/// Scenario: A declined
- [x] 2.5 In `docs/semantic-differences.md`, add a short section `## TO_DATE and TO_TIMESTAMP with a format`: the su
- [x] 2.6 Run `cargo fmt --all`, `cargo clippy --all-targets` (zero warnings), and `cargo test --workspace`. Run `ma

## Phase 3: Verification
- [x] 3.1 Run checklist commands and scenario audit

## Phase 4: Review Fixes
- [x] 4.1 In `crates/vs-expression/src/datetime_format.rs`, make `Element::Fraction` a unit variant, store the strftime form in `Token::Element(Element, &'static str)`, move a 12-row `ELEMENTS` table to module scope, make `next_token` return `Option<(Token, &str)>`, count fractions with `count_of(Element::Fraction)`, delete `count_elements`, and make `strftime` return the stored pattern or separator
- [x] 4.2 In `crates/vs-expression/src/datetime_format.rs`, add the doc comment `/// The conversion that parses with the format. Only \`Timestamp\` admits time elements.` above `FormatTarget`
- [x] 4.3 Rename `chrono_format` to `strftime_pattern` in `datetime_format.rs`, `lib.rs`, and `datetime_format_tests.rs`
- [x] 4.4 In `datetime_format_tests.rs` `declines_unsupported_formats`, add the cases bare `FF` after `.`, bare `FF` without `.`, and `YYYY年MM月DD日`, and raise the array length to 17
- [x] 4.5 In `crates/vs-expression/src/lib.rs`, remove the `function` parameter of `render_to_datetime`, derive `(node, function)` from `target`, use `node` in errors and `render_conversion_source`, and collapse the call sites
- [x] 4.6 In `render_to_datetime`, change the format decline message to name the `{node}` and `{format}` and the exact-strftime constraint
- [x] 4.7 In `render_to_datetime`, render the pattern through `sql_escape`
- [x] 4.8 In `e2e_to_date_translated_format_fails_on_rejected_input`, replace the three `query_error` calls on `vs_table()` queries with `assert_query_fails` and fragments verified against the Docker Exasol
