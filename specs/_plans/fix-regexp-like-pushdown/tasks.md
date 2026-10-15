# Tasks: fix-regexp-like-pushdown

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [!] tested-green — red: e2e (logs: target/speq-e2e.log)
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 1.1 Translator tests: decline REGEXP_LIKE (add decline tests, scenario lines)
- [x] 1.2 Remove REGEXP_LIKE rendering from vs-expression lib.rs
- [x] 1.3 Delete obsolete vs-expression tests, sweep rows, forbidden token

## Phase 2: Implementation (Group B)
- [x] 2.1 support.rs: like_subject_type_guard and project_columns drop predicate_like_regexp
- [x] 2.2 support_tests.rs: delete regexp tests and rows
- [x] 2.3 Add missing `/// Scenario:` lines to existing LIKE tests

## Phase 2: Implementation (Group C)
- [x] 3.1 capabilities_tests.rs: docs-vs-CAPABILITIES drift test
- [x] 3.2 reports_audited_capability_set: move FN_PRED_REGEXP_LIKE, add absent names
- [x] 3.3 capabilities.rs: remove FN_PRED_REGEXP_LIKE
- [x] 3.4 docs/capabilities.md corrections
- [x] 3.5 e2e_regexp_like_returns_native_rows
- [x] 3.6 e2e_regexp_like_conjunct_stays_in_exasol_while_sibling_pushes
- [x] 3.7 Keep e2e_count_distinct_regexp_like_matches_native_oracle unchanged

## Phase 3: Verification
- [x] 4.1 fmt, clippy, cargo test --workspace
- [x] 4.2 make test-e2e
- [x] 4.3 Changed-line coverage >= 90%
- [x] 4.4 Residual regexp_like grep

## Phase 4: Review Fixes
- [x] 4.5 vs-expression lib.rs: change "76 names" to "75 names" and "Ten names" to "Nine names" in the `Dialect` doc; rewrap the `ExasolForm::Shaped` doc to at most 90 columns
- [x] 4.6 vs-expression lib_tests.rs: change "ten Shaped names"/"five" to "nine"/"four" in the three sweep-test comments
- [x] 4.7 vs-expression lib_tests.rs: replace the "This plan branches ..." dialect-invariant section comment
- [x] 4.8 support.rs: fix `like_subject_type_guard` doc ("acts on `predicate_like` nodes") and rewrap the `like_subject_type_guard` and `guard_like_subject` docs to 90 columns
- [x] 4.9 support_tests.rs: replace "(`support.rs:546`)" with "(`classify_exa_type`)" in `like_guard_char_subject_unchanged` and rewrap to 90 columns
- [x] 4.10 e2e_capability_test.rs: drop "REGEXP_LIKE, " from the module doc list and append the non-advertised REGEXP_LIKE sentence
- [x] 4.11 e2e_capability_test.rs: replace the first two paragraphs of the `e2e_count_distinct_regexp_like_matches_native_oracle` doc with the single paragraph from the finding
