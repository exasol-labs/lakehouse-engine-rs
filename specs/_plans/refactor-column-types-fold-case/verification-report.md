# Verification Report: refactor-column-types-fold-case

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Removed `fold_case` from `column_types`, folded with `str::to_uppercase`, deleted the divergence test and its dead imports/comments, reworded five stale doc comments. Zero behavior change: full workspace build, unit suite (703 tests), and E2E suite (190 tests, live Docker Exasol) all pass; `git diff` shows no changed assertion or expected-SQL value outside the one deleted test. |
| Code review | 4 findings — standard: 3, expert: 1 — all fixed |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| Tests (`make test-e2e`) | ✓ |
| Lint (`cargo clippy --workspace --all-targets -- -D warnings`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Pre-existing suite, unchanged — one characterization test deleted, no new test added (behavior-preserving refactor per decision-log.md) |
| Integration/E2E | Pre-existing suite, unchanged |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, lib) | 703 | 703 | 0 |
| Unit (`cargo test`, full workspace incl. doctests/other crates) | 883 | 883 | 2 |
| Integration/E2E (`make test-e2e`, 8 files) | 190 | 190 | 0 |

E2E breakdown: `e2e_capability_test` 60, `e2e_count_distinct_test` 16, `e2e_int96_timestamp_test` 7, `e2e_join_test` 15, `e2e_non_ascii_identifier_test` 6, `e2e_positional_deletes_test` 16, `e2e_refresh_test` 11, `e2e_scan_test` 59 — all `test result: ok`, 0 failed.

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine adapter::pushdown` — no `fold_case` symbol remains | ✓ |
| `cargo test --features exasol-e2e --test e2e_non_ascii_identifier_test -- --test-threads=1 --nocapture` — `non_ascii_table_and_column_stay_queryable` passes, `straße` served as `STRASSE` | ✓ |
| `git diff -- '*/src/adapter/pushdown/*' \| grep -E '^[-+].*assert'` — only lines from the deleted test | ✓ |
| `grep -rn 'fold_case' crates/` — no matches | ✓ |
| `speq plan validate refactor-column-types-fold-case` — both delta specs listed, validation passes | ✓ (validated via `/speq:implement`'s Phase 1 plan load and Phase 4 delta review; no structural errors surfaced) |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
exit 0, 0 warnings
```

### Formatter

```
cargo fmt --check
exit 0, no diff
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-col-types-consolidation | One builder produces the column-type list for both the first-table and the named-table selection | `crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs` | `non_ascii_table_and_column_stay_queryable` | Pass |
| vs-adapter | pushdown-col-types-consolidation | One builder produces the column-type list for both the first-table and the named-table selection | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` | Pass |
| vs-adapter | pushdown-col-types-consolidation | One builder produces the column-type list for both the first-table and the named-table selection | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | full `dispatch_golden` fixture set, unchanged | Pass |
| vs-adapter | pushdown-col-types-consolidation | One builder produces the column-type list for both the first-table and the named-table selection | `crates/lakehouse-engine/src/adapter/pushdown/joins/{sql_builders,rendering}.rs` | full golden-SQL assertion set, unchanged | Pass |
| datafusion-scan | type-mapping-module-structure | No code-change scenario — delta is a Background bullet + cross-reference clause edit | n/a | delta validated during plan load / recorder review | Pass |

## Notes

- The plan's one work unit (Task 1: remove `fold_case`, fix both call sites, delete the divergence test) and Task 2 (reword two stale doc comments) were implemented together as a single compiling change, per the plan's Parallelization note — no intermediate state compiles.
- Code review surfaced 4 defects, all comment/design-seam issues, zero behavior regressions: two stale/self-contradicting doc-comment claims in `support.rs` (`column_exa_type`'s "two lookups" phrasing, `walk_column_nodes`' false claim that no non-ASCII collect-walk test exists — it does: `column_collectors_keep_divergent_case_folding`), one re-leaked fold reference in `involved_table_columns`' doc comment (`joins/planning.rs`), and one expert-tier finding: the refactor turns a by-construction fold agreement between `involved_table_columns`' output and `collect_side_column_names`' ASCII-folded set (compared in `referenced_side_columns`) into a premise-dependent one. All four were fixed as documentation-only changes — no fold, signature, or assertion touched — with the expert fix recording the cross-fold seam and its `resolve_table_schema`/E2E-guarded premise directly in both functions' doc comments, per code-guardrails' rule that a design seam gets documented rather than silently left implicit.
- The removed `assert_eq!` pair (the only assertion lines touched anywhere in the diff) both come from the deleted `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` characterization test — confirmed by direct diff inspection, not by trusting a grep summary alone.
- Closes GitHub issue #270.
