# Verification Report: refactor-col-types-guard-dedup

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Issue #265's mechanical de-dup (one `col_types` lookup helper, one shared builder, one type-family classifier) landed byte-identical; a live-capture gate (task 3) refuted the plan's original fold-divergence premise mid-implementation, the plan was revised and re-reviewed (round 8/9, 0 blockers after fix), and a new permanent E2E scenario proving a `straße`-named table/column round-trips correctly (including LIKE-pushdown) was added per explicit user request. |
| Code review | 5 findings — standard: 4, expert: 1 — all 5 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | N/A — pure-computation scenarios covered by new + pre-existing unit tests, no line-coverage tool run |
| Integration | N/A — full existing suite unedited, one new E2E binary added |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`, lib targets) | 819 | 819 | 0 |
| Integration (`cargo test --workspace`, non-E2E `tests/*.rs` binaries) | 60 | 60 | 0 |
| E2E (`make test-e2e`, live Docker Exasol container, `--features exasol-e2e`) | 190 | 190 | 0 |

`cargo test --workspace` total: 879 passed, 0 failed. `make test-e2e` total (8 binaries — `e2e_scan_test` 60, `e2e_capability_test` 16, `e2e_count_distinct_test` 7, `e2e_join_test` 15, `e2e_positional_deletes_test` 6, `e2e_int96_timestamp_test` 16, `e2e_refresh_test` 11, `e2e_non_ascii_identifier_test` 59): 190 passed, 0 failed, run twice (once before code-review fixes at task 6, once after, both green).

### Manual Tests

| Test | Result |
|------|--------|
| `grep -cF 'find(\|(n, _)\| *n == name)' support.rs` = 1 | ✓ |
| `grep -rEn 'starts_with\("VARCHAR"\)\|starts_with\("CHAR"\)\|starts_with\("DECIMAL"\)\|Some\("DATE"\)' adapter/` = 0 hits | ✓ |
| Live capture: `SYS.EXA_ALL_COLUMNS` for an adapter-declared `straße` column | ✓ RAN — returned `STRASSE`, refuting the plan's original "ß survives" inference (see Notes) |
| `grep -rn 'exasol_type_from_json' joins/planning.rs` = 0 hits | ✓ |
| `grep -rn 'to_ascii_uppercase' joins/planning.rs` = 1 production hit (+2 in task 4's test doc comment, expected) | ✓ |
| `grep -nE 'not yet wired\|intended first consumer' types/mapping.rs` = 0 hits | ✓ |
| `grep -rn 'support.rs:411' crates/lakehouse-engine/src` = 0 hits | ✓ |
| `grep -rc 'column_exa_type' support.rs` ≥ 5 (actual: 8) | ✓ |
| `cargo test --features exasol-e2e --test e2e_non_ascii_identifier_test -- --test-threads=1` = 0 failures | ✓ 6 passed |
| `SYS.EXA_ALL_TABLES`/`SYS.EXA_ALL_COLUMNS` for `STRASSE_VS.STRASSE` = `STRASSE` | ✓ (asserted inside the E2E test) |
| `EXPLAIN VIRTUAL` non-empty `"filter":"` naming the LIKE over `STRASSE` | ✓ (asserted inside the E2E test; the one discriminating pushdown check, per the expert code-review fix) |
| `grep -c 'e2e_non_ascii_identifier_test' Makefile` = 1 | ✓ |
| `make test-e2e` (all 8 binaries) = 0 failures | ✓ 190 passed |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
exit 0 (clean, both before and after code-review fixes)
```

### Formatter

```
cargo fmt --all -- --check
exit 0 (no diff, both before and after code-review fixes)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-module-structure | One helper resolves a bare column node's Exasol type for every type-rewrite guard | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` + 8 pre-existing pass-through/decline tests, unedited | Pass |
| vs-adapter | pushdown-module-structure | One builder produces the column-type list for both first-table and named-table selection (unit) | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs` | `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` (task 4) | Pass |
| vs-adapter | pushdown-module-structure | One builder produces the column-type list for both selections (integration/golden SQL) | `joins/sql_builders.rs`, `joins/rendering.rs` | 4 golden-SQL full-string assertions + 5 join guard tests, unedited | Pass |
| vs-adapter | pushdown-module-structure | The three type-rewrite guards read their type family from the shared classifier | `support.rs` | 10 per-family gate tests, unedited | Pass |
| datafusion-scan | type-mapping-module-structure | Classifier names the type-string families the guards branch on (amended clauses) | `types/mapping.rs` | `classify_exa_type_matches_pushdown_guard_predicates`, unedited | Pass |
| datafusion-scan | type-mapping-module-structure | Exasol/JSON type conversions live in type-mapping module (amended clause) | `types/mapping.rs`, `joins/sql_builders.rs`, `joins/rendering.rs` | existing relocation tests + golden SQL, unedited | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | LIKE on an unresolvable bare column declines (amended clause — owner renamed) | `support.rs` | `like_guard_unresolvable_column_declines`, `string_fn_guard_resolves_case_mismatched_column_name`, task 1's test | Pass |
| vs-adapter | pushdown-planning-string-fn-type-coercion | String-position arg with unresolved column declines fail-safe (amended clause) | `support.rs` | `string_fn_guard_declines_unresolved_column_name`, `string_fn_guard_resolves_case_mismatched_column_name` | Pass |
| vs-adapter | create-virtual-schema | createVirtualSchema declares column casing correctly (added clauses) | `crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs` | `non_ascii_table_and_column_stay_queryable` (task 7) | Pass |
| vs-adapter | create-virtual-schema | A non-ASCII Iceberg table and column stay queryable end to end | `crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs` | `non_ascii_table_and_column_stay_queryable` (task 7) | Pass |
| all | all four original + create-virtual-schema | Behavior unchanged across the refactor | `crates/lakehouse-engine/tests/` (unedited) + `Makefile` (+1 line) | `cargo test --workspace`, `make test-e2e` | Pass |

## Notes

- **Mid-implementation plan correction.** Task 3's live-capture gate (against the local Docker Exasol
  container) refuted the plan's original inference that Exasol's own engine preserves "ß" in an
  adapter-declared column name. The served name came back `STRASSE`, not the expected `STRAßE`. Root-cause
  investigation traced this to this crate's own `resolve_table_schema`
  (`file_resolution.rs:640`, `f.name.to_uppercase()`), not Exasol engine behavior — every column name is
  already Unicode-uppercased before Exasol ever sees it. Consequence: the "fold divergence between
  `extract_all_column_types` and `involved_table_columns`" this plan set out to characterize is
  unreachable for ANY column name through the real adapter, not merely mis-scoped to the wrong literal.
  The plan (§ Impact, § Patterns, § Non-Goals, § Consequences, tasks 1/3/4/6, § Parallelization,
  § Verification, § Manual Testing), the `pushdown-module-structure` delta's captured-evidence bullets,
  both sibling guard deltas, and decision-log decisions [3]/[4]/[6]/[12]/[13]/[14] were corrected in place
  via `/speq:plan`, then adversarially re-reviewed (round 8: 2 BLOCKERs found and fixed; round 9: 0
  BLOCKERs, 5 ADVISORY). Task 4's characterization test was kept (not dropped or inverted) — its literal is
  now framed as constructed to exercise fold-sensitivity as a unit, not as a claim about production
  reachability — and its tracked follow-up (issue #270) was rescoped from a correctness fix to a
  low-priority `fold_case`-parameter-removal simplification.
- **New scope, added mid-implementation at explicit user request.** Task 7 (permanent E2E coverage for a
  `straße`-named table/column, including a genuinely discriminating LIKE-pushdown assertion) was not part
  of original issue #265 — it was added via the `vs-adapter/create-virtual-schema` spec delta (new for this
  plan) once the fold-divergence investigation surfaced that non-ASCII identifier handling had no test
  coverage anywhere in the spec library.
- **Code review findings, all fixed.** 4 standard findings were stale doc comments left behind by the
  mid-implementation correction (claims about fold reachability / mapping equivalence that were no longer
  accurate); 1 expert finding was a vacuous E2E assertion (`pushed_sql.contains("STRASSE")` — the substring
  appears in the `EXPLAIN VIRTUAL` blob regardless of whether the LIKE was pushed or declined, since it's
  echoed in the VS/table name and Exasol's own request). The genuinely discriminating assertion
  (`"filter":"` field presence) was kept and is the one that proves the LIKE was actually pushed down.
- **5 ADVISORY findings from plan review round 9** were left as report-only per this project's review
  protocol (never gate, never looped on) — not investigated further as part of this implementation.
- No pre-existing test assertion or expected value was edited anywhere in this plan's diff, confirmed via
  `git diff` review at task 6 and again after code-review fixes.
