# Verification Report: fix-select-list-like-type-coercion

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `like_subject_type_guard` now runs first in the select-list type-rewrite pipeline; the two pipeline functions collapsed into one `apply_type_rewrites`; all suites green |
| Code review | 2 findings — 2 fixed |

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
| Unit | Not measured (no coverage tool wired into this project) |
| Integration | Not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test`, host) | `cargo test` | 687 (lakehouse-engine lib) + smaller crates, 0 failed | 2 |
| Integration/E2E (`make test-e2e`, Exasol Docker stack) | `make test-e2e` | 59 passed, 0 failed | 0 |

## Manual Tests

| Test | Result |
|------|--------|
| `git grep -n "219" -- crates/lakehouse-engine/src/adapter/pushdown/` — no hit describing an open gap | ✓ (2 hits, both name the closed fix, not a gap) |
| `git grep -cn "apply_type_rewrites" -- crates/lakehouse-engine/src/adapter/pushdown/` names one function everywhere | ✓ (1 + 10 + 7 hits across `joins/rendering.rs`, `mod.rs`, `support.rs`) |
| `git grep -n "apply_filter_type_rewrites\|apply_select_item_type_rewrites" -- crates/` returns nothing | ✓ (exit 1, zero hits) |
| `cargo test --features exasol-e2e --no-run` compiles | ✓ |
| `make test-e2e` against the running Exasol Docker stack | ✓ (59 passed, 0 failed) |
| Deployed-VS manual SQL checks (`SELECT O_ORDERDATE LIKE …`, `L_QUANTITY LIKE …`, `C_NAME LIKE …`, nested `CASE`) | Not run — no staging/deployed VS target available in this session; behavior is covered by the equivalent unit tests (`selectlist_like_over_date_projects_cast_expr`, `selectlist_like_over_non_string_subject_falls_back_to_full_row`, `selectlist_like_inside_case_over_decimal_falls_back_to_full_row`) and the e2e suite |

## Tool Evidence

### Linter

```
cargo clippy --all-targets → exit 0, 0 warnings
```

### Formatter

```
cargo fmt --check → exit 0, no diff
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-like-type-coercion | Select-list LIKE over DATE projects CAST-to-VARCHAR form | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_like_over_date_projects_cast_expr` | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | Select-list LIKE over VARCHAR/CHAR unchanged | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_predicate_node_projects_as_expr` (existing, unedited) | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | Select-list LIKE over non-string subject widens to full base row | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_like_over_non_string_subject_falls_back_to_full_row` | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | Nested reach: LIKE inside `function_scalar_case` | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_like_inside_case_over_decimal_falls_back_to_full_row` | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | Broadcast-join SELECT list reaches same dispatch, widens over union of both tables | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `join_projection_like_guard_reaches_join_select_list` | Pass |
| vs-adapter | pushdown-module-structure | Single pass list serves both render surfaces, no omission | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `type_rewrite_pipeline_runs_like_guard` | Pass |
| vs-adapter | pushdown-module-structure | One function serves both callers, `pub(super)`, decline meaning stated caller-agnostically | `crates/lakehouse-engine/src/adapter/pushdown/{mod.rs,support.rs}` | Six filter-chain tests in `mod.rs` + `selectlist_*` tests in `support.rs`, both now calling `apply_type_rewrites` | Pass |
| vs-adapter | pushdown-module-structure | Every other clause (pass order, fallibility bridge, byte-identical SQL) unchanged | `crates/lakehouse-engine/src/adapter/pushdown/` | Existing corpus, unedited | Pass |

## Notes

- Code review (`code-reviewer`) found 2 standard findings, 0 expert: (1) `apply_type_rewrites`'
  parameter and shadowed bindings still named `filter` post-collapse, violating the
  `pushdown-module-structure` caller-agnostic naming rule — fixed by renaming to `expr`
  (signature/call sites unchanged); (2) the new join select-list test discarded the `widened` flag
  that is the sole signal `render_broadcast_join` uses to decline the broadcast fan-out
  (`joins/sql_builders.rs:85`) — fixed by asserting `!widened`/`widened` in both halves plus a
  `ProjectionItem::Column(_)` shape check on the decline half. Both fixes verified green.
- `git diff` confirms exactly one flipped assertion beyond the new test additions
  (`select_list_pipeline_omits_like_pass_pending_219`'s second assertion, `Some(filter.clone())` →
  `None`, later folded to a single assertion under `type_rewrite_pipeline_runs_like_guard` in task
  5); `selectlist_predicate_node_projects_as_expr`'s `regexp_like("NAME", '^a.*')` expectation is
  untouched.
- `apply_filter_type_rewrites` and `apply_select_item_type_rewrites` no longer exist anywhere in the
  repo; `apply_type_rewrites` is the sole survivor, `pub(super)`, signature-preserving.
- Deployed-VS manual SQL checks from the plan's Manual Testing table were not executed — no
  staging/deployed VS endpoint was available in this session. The behavior they'd exercise is
  covered by unit tests at the pipeline/projection level and by the full `make test-e2e` run against
  the local Exasol Docker stack, which passed.
- Workspace version bumped: `lakehouse-engine` 0.30.11 → 0.30.12 (patch — this plan is scoped as a
  `fix`, no `workspace/version` spec delta specified an alternative).
