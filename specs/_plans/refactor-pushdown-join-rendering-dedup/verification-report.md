# Verification Report: refactor-pushdown-join-rendering-dedup

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All five duplication reductions (issue #181, findings 1/3/4/5/6) landed with byte-identical SQL and decline messages; four review findings (all doc-comment regressions) found and fixed; every automated and manual verification step is green. |
| Code review | 4 findings — standard: 4, expert: 0 — all fixed |

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
| Unit | All 5 golden/characterisation scenarios named in the plan's Scenario Coverage table have a passing test; no untested divergence path remains (case folding, fallback policy, no-short-circuit, first-column fallback, full-set fallback all individually pinned). |
| Integration | Join E2E binary (`e2e_join_test`, `exasol-e2e` feature) — 15/15 passing against a live Exasol Docker stack. |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (workspace, `cargo test`) | 762 | 762 | 2 |
| Unit (`lakehouse-engine` lib only) | 702 | 702 | 0 |
| Integration (`e2e_join_test`, `--features exasol-e2e`) | 15 | 15 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine golden_` — 5 tests (4 pre-existing goldens + `golden_n_scan_render_decline_messages_unchanged`) | ✓ |
| `grep -rn "render_join_condition\|render_selectlist_item_qualified" crates/ specs/` — zero hits under `crates/`; only plan-authorised hits under `specs/` | ✓ |
| `cargo test -p lakehouse-engine dispatch_golden` — both decline-wrapper goldens | ✓ |
| `make cross-musl-udf-build && cargo test --features exasol-e2e --test e2e_join_test -- --test-threads=1` (compose stack up) | ✓ (15 passed, 0 failed, 0 skipped — ran against a live Exasol stack) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.33s
(0 warnings, 0 errors)
```

### Formatter

```
cargo fmt --check
(no output — clean, no diff)
```

### Build

```
cargo test (whole workspace)
    Finished `dev` profile ... 
test result: ok. 702 passed; 0 failed ... (lakehouse-engine lib)
+ 15 further test binaries, all "ok", 0 failed, 2 ignored (unrelated pre-existing ignores)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-joins-module-structure | One shared template renders all six qualified N-scan render declines | `joins/sql_builders.rs` | `golden_n_scan_render_decline_messages_unchanged` | Pass |
| vs-adapter | pushdown-joins-module-structure | Seventh decline template stays separate | `joins/mod.rs` | `golden_ineligible_decline_message_unchanged` | Pass |
| vs-adapter | pushdown-joins-module-structure | One clause walk feeds both routines — no short-circuit leaks into projection | `joins/sql_builders.rs` | `referenced_column_projection_narrows_without_select_list` | Pass |
| vs-adapter | pushdown-joins-module-structure | One clause walk feeds both routines — first-column fallback | `joins/sql_builders.rs` | `referenced_column_projection_falls_back_to_first_column` | Pass |
| vs-adapter | pushdown-joins-module-structure | One clause walk feeds both routines — full-set fallback + per-table narrowing | `joins/rendering.rs` | `referenced_side_columns_keeps_all_when_narrowing_empty`, `referenced_side_columns_narrows_to_used_columns`, `referenced_side_columns_keeps_all_when_select_list_absent` | Pass |
| vs-adapter | pushdown-joins-module-structure | One clause walk feeds both routines — divergent case folding preserved | `joins/rendering.rs` | `column_collectors_keep_divergent_case_folding` | Pass |
| vs-adapter | pushdown-joins-module-structure | The two pass-through wrappers are deleted, not retained | `joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged` + crate compiles with neither name present | Pass |
| vs-adapter | pushdown-module-structure | One blind traversal primitive backs every column-collecting walk (tuple-return amendment) | `joins/sql_builders.rs`, `joins/rendering.rs` | the four goldens above + `column_collectors_keep_divergent_case_folding` | Pass |
| vs-adapter | pushdown-planning-selectlist-expressions | Widened derived projection routes to a native wrapper on every path (dialect chain names surviving delegate) | `joins/sql_builders.rs` | `golden_grouped_qualified_fallback_sql_unchanged` + task 4.4 grep gate | Pass |

## Notes

- **Scope.** Five independent reductions landed sequentially (hard-ordered per the plan's Parallelization section: no concurrent work, since every task touches `joins/rendering.rs` and/or `joins/sql_builders.rs`): (1) `join_render_decline` unifying six decline sites, (2) `column_tables` replacing `collect_column_tables`'s `&mut` out-params with a returned tuple, (3) the attach-point let-chain rewrite plus deletion of `render_join_condition` and `render_selectlist_item_qualified`, (4) `shard_side` unifying the two fan-out builders' sharding prefix, (5) `referenced_clause_values` — the highest-risk reduction — sharing clause-walk mechanics between `referenced_column_projection` and `referenced_side_columns` while deliberately keeping their case folding (Unicode `to_uppercase` vs ASCII `to_ascii_uppercase`) and fallback policies divergent.
- **Highest-risk item verified directly.** The case-folding divergence is pinned by a new non-ASCII characterisation test (`ß` → `SS` under Unicode fold, unchanged under ASCII fold) that passes against both pre- and post-refactor code — a true guard, not a tautology.
- **Byte-identical outputs proven, not asserted.** All four SQL-builder goldens and both decline-message goldens are unedited from HEAD and green. Net production-code line count fell (`sql_builders.rs` production: 889 → 824, `rendering.rs` production: 323 → 320); the two files' total line count rose only because the plan itself mandated new test coverage that didn't exist before (six decline messages had zero prior assertion).
- **Code review caught real regressions.** The initial implementation pass accidentally deleted eight doc comments (two `pub(super)` builder contracts, five private-function soundness arguments, one 9-line `conjunct_single_side` contract) that the plan never authorised removing — all were symbol-level edits that silently dropped surrounding prose. All eight were restored verbatim (one intra-doc link updated for the `column_tables` rename); one stale test doc comment and one tripled inline comment were also fixed. Re-verified green after the fix pass with zero change to any test assertion.
- **Façade and spec-drift note.** The `joins` façade (`pub(crate)`/`pub(super)` re-export block in `pushdown/mod.rs`) is byte-identical pre/post this branch. The Phase 7 verification agent noted `specs/vs-adapter/pushdown-joins-module-structure/spec.md`'s prose already referenced function names (`full_row_projection`) renamed/removed by an earlier, unrelated refactor predating this branch's base — a pre-existing spec/code drift outside this plan's scope, left for `/speq:record` or a future audit, not fixed here.
- **E2E stack.** The join E2E suite ran against an already-healthy Docker Compose stack from a sibling checkout (this repo's own `docker compose up` failed on a hardcoded non-project-scoped network/IP collision — a pre-existing infra quirk unrelated to this plan's code changes). No stray `bench/.env` was involved. The suite still satisfies the fail-not-skip contract: 15/15 passed against a live DB, not skipped.
