# Plan: refactor-column-types-fold-case

## Summary

Remove `support::column_types`' `fold_case` parameter and collapse both wrappers onto the Unicode `str::to_uppercase`, closing issue #270. The divergence the parameter preserved is unobservable through the adapter, so this is a simplification with no behavior change.

## Design

### Context

Issue #265 merged two byte-identical `col_types` walks into one builder, `support::column_types`, and kept `fold_case: impl Fn(&str) -> String` so each wrapper could retain its historical fold: `extract_all_column_types` passes `str::to_uppercase`, `involved_table_columns` passes `str::to_ascii_uppercase`. That plan deliberately preserved the divergence to keep its own diff minimal, and its spec states that unifying the folds "SHALL NOT happen under this scenario", citing issue #270 for the later removal.

No reachable input distinguishes the two folds. `resolve_table_schema` (`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:640`) maps every Iceberg field through `f.name.to_uppercase()` before `build_virtual_tables` declares it to Exasol, so a name carrying a lowercase non-ASCII letter never reaches either wrapper. `tests/e2e_non_ascii_identifier_test.rs::non_ascii_table_and_column_stay_queryable` pins that against a Docker Exasol container: an Iceberg `straße` column is served as `STRASSE`. A name already produced by `to_uppercase` is a fixed point of both folds.

- **Goals** — delete the `fold_case` parameter, one surviving fold in the builder body, delete the characterization test that has nothing left to distinguish, prove byte-identical SQL.
- **Non-Goals** — no call-site signature change, no wrapper deletion, no new test, no touch to the collect-walk folds (`column_tables`, `collect_side_column_names`), no reshaping of the surviving selection parameter.

### Decision

Keep `column_types` and both wrappers; drop one parameter.

```
extract_all_column_types(request)          involved_table_columns(request, name)
        │ tables.first()                              │ find(name == …)
        └──────────────────┬───────────────────────────┘
                  column_types(request, select_table)
                      └── str::to_uppercase   ← was the fold_case argument
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Parameter elimination | `support::column_types` | A configuration parameter is a decision the module declined to make; with one reachable outcome there is nothing to decide, so the builder makes it |
| Partial application retained | both wrappers | Each still supplies a table selection the builder does not choose, so neither is a pass-through |
| Compile-enforced deletion proof | `#[cfg(test)]` import in `joins/planning.rs` | The `-D warnings` clippy gate fails on the orphaned import, so the compiler proves the test went away rather than being silently kept alive |

One `/speq:design-philosophy` diagnostic row applies — *Would changing how a module works internally force an edit anywhere outside it?* No. `column_types` is private to `adapter::pushdown`, and the fold it now hard-codes is the one its in-module consumer `support::column_exa_type` already applies, so both wrappers, all six call sites, and every declared signature stay untouched. The reverse direction is not leakage either: if `resolve_table_schema` stopped Unicode-uppercasing declared names, `column_types` would still fold with `to_uppercase` — only the byte-identity claim would weaken, and `non_ascii_table_and_column_stay_queryable` fails first. That premise is guarded by a test, not encoded in this body. This reverses the recorded feature's refusal to unify; see `decision-log.md` § [6].

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| The surviving fold is Unicode `str::to_uppercase` | `str::to_ascii_uppercase` | `to_uppercase` is `resolve_table_schema`'s own fold and the fold the consuming `column_exa_type` lookup applies, so the join side converges on the adapter's existing normalization instead of the reverse |
| Delete `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal`, add nothing | Rewrite it as a unified-fold agreement test | With one fold the test's assertion is false, not weakened; an agreement test over two wrappers of one builder restates `str::to_uppercase` and guards nothing |
| Keep the selection closure parameter | Reshape to `Option<&str>` table name | Out of scope for #270, and it would change both call sites for no observable gain |
| Leave `column_tables` and `collect_side_column_names` ASCII | Unify every pushdown fold in one pass | `vs-adapter/pushdown-module-structure`'s blind-traversal scenario forbids unifying the collect walks' folds |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-col-types-consolidation | CHANGED | `specs/_plans/refactor-column-types-fold-case/vs-adapter/pushdown-col-types-consolidation/spec.md` |
| datafusion-scan/type-mapping-module-structure | CHANGED | `specs/_plans/refactor-column-types-fold-case/datafusion-scan/type-mapping-module-structure/spec.md` |

## Impact

None. No generated SQL changes, no wrapper signature changes, no call site changes. The removed parameter is private to `adapter::pushdown`, so no public surface moves. Closes issue #270.

## Dependencies

Issue #265 / `vs-adapter/pushdown-col-types-consolidation` is recorded and merged — `support::column_types` exists as commit f59a4a9 left it.

## Implementation Tasks

1. Remove the parameter and everything that breaks with it — ONE compiling change across `support.rs` and `joins/planning.rs`:
   - Remove `fold_case` from `column_types` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs:450`) and fold with `str::to_uppercase` in the body.
   - Delete `column_types`' doc-comment paragraph justifying two parameters together with its `(#270)` tracking note.
   - Drop the fold argument at both call sites: `extract_all_column_types` (`support.rs:475`) and `involved_table_columns` (`joins/planning.rs:362`).
   - Reword `involved_table_columns`' doc comment so it names the shared Unicode fold instead of "the ASCII-only `to_ascii_uppercase` fold", and drop "and the ASCII-only fold this side has always applied" from its closing partial-application paragraph (`joins/planning.rs:359-360`) while keeping the find-by-name selection clause.
   - Delete `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` (`joins/planning.rs:795`), its `// Case-fold characterization` section banner, and the `#[cfg(test)]`-scoped `use …support::extract_all_column_types` inside it.
2. Reword the two remaining stale doc comments, both in `support.rs`:
   - `column_exa_type`'s agreement sentence (`support.rs:671-673`) so it names the SINGLE fold both builders now apply rather than "`involved_table_columns`' ASCII-folded keys", keeping the `resolve_table_schema` premise and the `type`-tag paragraph intact.
   - the doc comment of `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` so its ASCII-folded `col_types` literal reads as a constructed list no builder produces, keeping the falsifiability rationale intact.
3. Run the behavior-preservation gate: full `cargo test`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, and confirm `git diff` shows no changed test assertion and no changed expected SQL value anywhere.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Sequential chain, no parallelism | Task 1 → Task 2 → Task 3 |

Nothing here runs concurrently. Dependencies:
- **Task 1 is one work unit, not three.** Removing `fold_case` breaks both call sites and the divergence test at once, so no intermediate state compiles and no sub-step can run its own build or test gate. Splitting it would produce two tasks that cannot be verified.
- **Every task before the gate edits `crates/lakehouse-engine/src/adapter/pushdown/support.rs`**, so they MUST run one at a time. Task 1 edits it at lines 442-475 and task 2 at lines 671-673 and in its test module; the file is ~6,100 lines and two concurrent writers are the merge hazard the parent plan (`specs/_recorded/004-refactor-col-types-guard-dedup/plan.md` § Parallelization) called out by name.
- **Task 3 runs last**, because it is the gate on both preceding tasks' output.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Parameter | `column_types`' `fold_case`, `support.rs:453` | Dead flexibility — both arguments produce identical output on every reachable name (#270) |
| Test | `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal`, `joins/planning.rs:795` | Asserts a divergence that no longer exists |
| Import | `use crate::adapter::pushdown::support::extract_all_column_types`, `joins/planning.rs:796` | Sole consumer is the deleted test; `-D warnings` rejects it |
| Comment | `// Case-fold characterization` banner, `joins/planning.rs:774-776` | Labels the deleted test section |
| Comment | `column_types`' two-parameters-by-design paragraph and `(#270)` note, `support.rs:442-449` | Justifies a parameter that no longer exists |
| Comment | `involved_table_columns`' closing clause "and the ASCII-only fold this side has always applied", `joins/planning.rs:359-360` | This side supplies no fold after the change; the find-by-name selection clause stays |
| Comment | `column_exa_type`'s "`involved_table_columns`' ASCII-folded keys agree …" sentence, `support.rs:671-673` | Names an ASCII-folded key set no builder produces; reworded to name the single shared fold |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One builder produces the column-type list for both the first-table and the named-table selection | Integration | `crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs` | `non_ascii_table_and_column_stay_queryable` |
| One builder produces the column-type list for both the first-table and the named-table selection | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` |
| One builder produces the column-type list for both the first-table and the named-table selection | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | the full `dispatch_golden` fixture set, unchanged |
| One builder produces the column-type list for both the first-table and the named-table selection | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/{sql_builders,rendering}.rs` | the full golden-SQL assertion set, unchanged |

`datafusion-scan/type-mapping-module-structure` has no row above by design: its delta changes one Background bullet and one cross-reference clause with no code change, so its gate is delta validation rather than a test.

The scenario is behavior-preserving, so its coverage is the pre-existing suite passing with zero assertion edits — no new test is added, per the interview answer recorded in `decision-log.md`. The E2E entry is the standing guard on the premise: it asserts an Iceberg `straße` column is served as `STRASSE`, so a `resolve_table_schema` change that stopped Unicode-uppercasing declared names fails a test before any fold could matter.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-col-types-consolidation | `cargo test -p lakehouse-engine adapter::pushdown` | All pushdown unit tests pass; no `fold_case` symbol remains |
| vs-adapter/pushdown-col-types-consolidation | `cargo test --features exasol-e2e --test e2e_non_ascii_identifier_test -- --test-threads=1 --nocapture` | `non_ascii_table_and_column_stay_queryable` passes; the `straße` column is served as `STRASSE` |
| vs-adapter/pushdown-col-types-consolidation | `git diff -- '*/src/adapter/pushdown/*' \| grep -E '^[-+].*assert' ` | Only lines from the one deleted test; no assertion or expected-SQL value on any surviving line changes |
| vs-adapter/pushdown-col-types-consolidation | `grep -rn 'fold_case' crates/` | No matches |
| datafusion-scan/type-mapping-module-structure | `speq plan validate refactor-column-types-fold-case` | validation passes; both delta specs listed |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
