# Tasks: fix-join-fallback-self-join-attribution

## Phase 2: Implementation (Group A)
- [x] 1.1 Reproduce both issue #361 shapes live and close the remaining evidence gaps in the leg-alias signal (Docker Exasol container required). Record observations in decision-log.md. HALT and escalate if any observation contradicts the FROM-tree leaf alias premise.

## Phase 2: Implementation (Group B)
- [x] 2.1 Add `table_alias: Option<String>` to `JoinLeaf` and retain each FROM-tree leaf's `alias` in `collect_join_tree`. [expert]
- [x] 2.2 Add `crates/lakehouse-engine/src/adapter/pushdown/joins/attribution.rs` with `JoinLegs` and `attribution_tests.rs`. [expert]

## Phase 2: Implementation (Group C)
- [x] 3.1 Thread `JoinLegs` through every attribution call site in ONE atomic change, deleting the tableName-keyed derivations. [expert]

## Phase 2: Implementation (Group D)
- [x] 4.1 Unit-test all three fixed call sites and the shapes they broke.
- [x] 4.2 Correct tests/doc comments (`seam_trailing` in sql_builders_tests.rs) that recorded the collapse as intended.
- [x] 4.3 Pin the two properties the fix must not lose (byte-identical SQL for no-table-twice requests; self-join never broadcast-eligible).

## Phase 2: Implementation (Group E)
- [x] 5.1 Add self-join regression tests to `crates/lakehouse-engine/tests/e2e_join_test.rs` (Docker Exasol container required).
- [x] 6.1 Add a self-join test on a nested JSON-rendered column to `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` (Docker Exasol container required).

## Phase 3: Verification
- [x] 7.1 Automated checks: build, cargo test, clippy, fmt
- [x] 7.2 Scenario coverage audit
- [x] 7.3 Manual verification commands (subsumed by e2e run — see verification-report.md Notes)
