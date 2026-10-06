# Code Review Findings: reorg-e2e-specs-into-testing

## Summary
- Files reviewed: 20
- Total findings: 2 (standard: 2, expert: 0)

The diff matches plan.md § Reference Edits line for line: 16 path-reference lines, 24 `/// Scenario:` or `/// specs/testing.md §` lines, and 4 pointer edits in `AGENTS.md` and `specs/mission.md`. Every changed `.rs` and `.yml` line is a comment, and `cargo fmt --all -- --check` exits 0. Every cited feature path resolves to one of the 134 features in the resulting library (permanent specs minus the 124 REMOVED features, plus the NEW and CHANGED deltas). Every quoted scenario title in the edited lines exists verbatim in its destination delta. The four `specs/testing.md` sections cited (§ Ops › Orphaned fixture sweeps, § Per-run cloud resources, § Failure contract, § Credentials in tests) exist and state the rules the annotated tests exercise. The changed files contain no other reference to a removed domain. The remaining `glue-e2e`, `unity-e2e`, and `cloud-e2e` tokens are cargo feature names and string constants. One edited doc line cites a scenario that the test does not implement. One unedited doc line in a changed file quotes a moved scenario's title with the wrong case.

## Standard fixes

### crates/lakehouse-engine/tests/e2e_glue_test.rs

#### [OUTDATED_COMMENT] ORC refusal test cites the partition-listing scenario instead of the refusal scenario
- Location: line 432
- Issue: `check_orc_partition_fails_loud` now carries `/// Scenario: Each kept partition's location is listed and its files carry the partition's Glue values`. The test asserts that a query over the table with a kept ORC partition fails, and that the error names `p_int=9`, the ORC input format, `is not Parquet`, and the partition location, without the secret key. That is the THEN clause of `glue/glue-table-planning` § "A kept partition the reader cannot read faithfully fails the query naming it" ("the query SHALL fail with an error naming the partition's values, its location, and the cause: its input format, or its bucket"). The cited scenario is about listing kept partitions and carrying their Glue values. `check_partition_cases_return_their_glue_values` at line 395 already implements that scenario and cites it. Plan.md row 120 was applied mechanically here, because the old line 432 already quoted the unrelated removed title "Queries through pushdown return the expected rows".
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs line 432, replace `/// Scenario: Each kept partition's location is listed and its files carry the partition's Glue values` with `/// Scenario: A kept partition the reader cannot read faithfully fails the query naming it`. In specs/_plans/reorg-e2e-specs-into-testing/plan.md § Reference Edits › Scenario doc lines, change the New line cell of the `crates/lakehouse-engine/tests/e2e_glue_test.rs:432` row to `` `/// Scenario: A kept partition the reader cannot read faithfully fails the query naming it` `` so the plan matches the code. Change no test name, body, or attribute.

### crates/lakehouse-engine/src/scan/type_relaxation_tests.rs

#### [OUTDATED_COMMENT] Scenario doc line does not quote the moved scenario's title verbatim
- Location: line 378
- Issue: `/// Scenario: a narrow physical column binds to the current wider logical type and is cast per file.` starts with a lowercase letter and ends with a period. The scenario this plan moves to `scan-types/type-relaxation` is titled "A narrow physical column binds to the current wider logical type and is cast per file". AGENTS.md § Code style requires one `/// Scenario: <title>` line that quotes the title verbatim. The plan normalized the same case defect in nine other lines it rewrote. This line was not edited in this diff, but the file is in the changed set.
- Fix: In crates/lakehouse-engine/src/scan/type_relaxation_tests.rs line 378, replace `/// Scenario: a narrow physical column binds to the current wider logical type and is cast per file.` with `/// Scenario: A narrow physical column binds to the current wider logical type and is cast per file`. Change no test name, body, or attribute.

## Expert fixes
[none]
