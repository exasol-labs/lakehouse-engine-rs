# Code Review Findings: fix-broadcast-join-limit-suppression

## Summary
- Files reviewed: 19
- Total findings: 3 (standard: 3, expert: 0)

Core logic is correct and well-tested. `carries_aggregation_clause` preserves the deleted
boolean's four aggregation conditions byte-for-byte (verified against the function body); the
`build_broadcast_join_sql` four-arm dispatch, the `Ordered` projection-membership downgrade, and
the `debug_assert_ne!`+`None` release fall-back are sound; `common.limit` is read on no join path
(only the `scan/mod.rs:285` diagnostic, which short-circuits on `join.is_none()`, and the
unreachable `raw_scan.rs:411`); the serde field is additive/defaulted and unordered broadcast SQL
stays byte-identical (golden tests plus the explicit diff assertion in
`broadcast_bare_limit_caps_each_shard_and_the_merge`); and the unit + integration tests assert
observable behavior and genuinely distinguish a post-join cap from a pre-join cap
(`join_limit_bounds_joined_output_not_scanned_input`).

All three findings are the same class of defect: task 7's `cargo fmt` / `cargo clippy` gate is not
clean. `cargo clippy -p lakehouse-engine --all-targets` emits 2 warnings and `cargo fmt --check`
reports 3 diffs — the plan's Verification checklist requires "No changes" from fmt and
"0 errors/warnings" from clippy, and CI runs clippy with `-D warnings`, so the branch as-is fails
the gate.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs

#### [UNUSED_IMPORT] `detected_join` imported but never used
- Location: line 2
- Issue: `cargo clippy -p lakehouse-engine --all-targets` reports `warning: unused import: detected_join` at `planning_tests.rs:2`. The symbol was added to the `use super::super::tests::{ detected_join, equi_condition, … }` list but no test in the file references it (its only occurrence in the file is the import itself). Separately, a stray trailing blank line at line 345 (end of file, after the closing `}` of `equal_size_tie_breaks_to_first_argument`) makes `cargo fmt --check` report a diff at `planning_tests.rs:345`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs, remove `detected_join` from the `use super::super::tests::{…}` import list on line 2, and delete the trailing blank line at the end of the file so it ends with a single newline after the final `}`. Then run `cargo fmt` and confirm `cargo clippy -p lakehouse-engine --all-targets` no longer warns about this file.

### crates/lakehouse-engine/tests/scan_join_test.rs

#### [REDUNDANT_COMMENT] Blank line orphans the `join_spec` doc comment (clippy `empty_line_after_doc_comments`)
- Location: line 241 (between the doc comment ending at line 240 and `fn join_spec` at line 242)
- Issue: `cargo clippy -p lakehouse-engine --all-targets` reports `warning: empty line after doc comment` at `scan_join_test.rs:240`. A spurious blank line was inserted at line 241, separating the `/// A join ScanSpec: …` doc comment (lines 238-240) from the `fn join_spec` it documents. `cargo fmt` does NOT remove this blank line, so it must be deleted by hand.
- Fix: In crates/lakehouse-engine/tests/scan_join_test.rs, delete the blank line at line 241 so the `fn join_spec` declaration immediately follows its `///` doc comment, resolving clippy's `empty_line_after_doc_comments` warning.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs

#### [SHRINKABLE] Two rustfmt diffs left unformatted (import block + double blank line)
- Location: line 1 (the `use super::super::planning::{ … }` import) and line 1675 (double blank line between `join_window_classification_covers_every_forcing_and_served_shape` and the following doc comment)
- Issue: `cargo fmt --check` reports two diffs in this file: (1) at line 1 the `use super::super::planning::{ IneligibleJoinReason, JoinShape, classify_join_window, detect_join };` import is spread across three lines but rustfmt collapses it to a single line; (2) at line 1675 there is a double blank line that rustfmt collapses to a single blank line. Neither is a logic defect, but both fail the format gate the plan's Verification checklist requires.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs, run `cargo fmt` to collapse the multi-line `planning` import at line 1 onto one line and to reduce the double blank line near line 1675 to a single blank line. After fixing all three files, re-run `cargo fmt --check` (expect no diff) and `cargo clippy -p lakehouse-engine --all-targets` (expect zero warnings) to confirm the gate is clean.

## Expert fixes
[none]
