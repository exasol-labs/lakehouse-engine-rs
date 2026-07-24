# Plan: fix-207-like-non-string-column

## Summary

Make pushed-down LIKE and REGEXP_LIKE type-aware in the single-table pushdown path so a LIKE over a non-string column no longer hard-fails the DataFusion scan. The adapter casts a DATE subject to VARCHAR, declines pushdown of the whole filter for any other non-string subject, and leaves string subjects unchanged.

## Design

### Context

`predicate_like` / `predicate_like_regexp` render the subject and pattern verbatim (`crates/vs-expression/src/lib.rs:358-393`). Exasol implicitly casts a non-string LIKE subject to VARCHAR before matching; DataFusion has no such coercion, so a pushed-down LIKE over a DATE, DECIMAL, or integer column hard-fails at scan time with `There isn't a common type to coerce <Type> and Utf8 in LIKE expression` (issue #207, reproduced this session against the live Exasol+MinIO+Iceberg-REST stack for `c_date`, `c_decimal_a`, and `id`).

A LIKE predicate's `column` node never carries a `dataType` on the wire; column types live only in `involvedTables[0].columns`. `crates/vs-expression` is a pure syntactic JSON-to-SQL translator with zero external state, reused by a sibling VS-adapter project, so it cannot decide type-safety itself. The type-aware decision belongs one level up, in the adapter, where `extract_all_column_types(request)` already exposes the column-type map.

- **Goals** — a pushed-down LIKE/REGEXP_LIKE over a non-string column produces correct results (via CAST for DATE) or falls back to native Exasol evaluation (decline for other non-string types), never a scan-time hard error; string subjects keep their current pushdown.
- **Non-Goals** — decimal/integer LIKE pushdown with Exasol-faithful formatting (tracked in issue #211); LIKE type-awareness on the broadcast-join per-leg filter path (`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, tracked in a follow-up issue — see Dependencies); any change to `crates/vs-expression`'s own rendering logic or to advertised capabilities.

### Decision

Preprocess the raw filter JSON in the adapter, before `render_df_filter_safe`, using the existing column-type map. A recursive guard walks the filter tree through `predicate_and` / `predicate_or` / `predicate_not`; at each `predicate_like` / `predicate_like_regexp` whose `expression` is a bare `column` node it dispatches on the column's Exasol type.

#### Architecture

```
handle_pushdown (mod.rs, single-table path)
  col_types = extract_all_column_types(request)      # moved above the filter block
  filter = filter_json_raw
      .and_then(|f| like_subject_type_guard(f, &col_types))   # None => decline whole filter
      .and_then(|f| render_df_filter_safe(&f))                # existing renderer, unchanged
```

`like_subject_type_guard` (new, in `pushdown/support.rs`) returns `Option<Json>`:
- `Some(tree)` — render this tree (unchanged, or with DATE subjects rewrapped in a `function_scalar_cast` to `{"type":"VARCHAR"}`).
- `None` — decline the whole top-level filter (a non-string, non-DATE LIKE subject was found anywhere in the tree). Composes with `render_df_filter_safe`'s existing `None`-means-omit contract.

The rewrite feeds only the DataFusion filter renderer. `filter_json_raw` forwarded to `resolve_file_list` for Iceberg file pruning is left untouched; the pruning translator already drops LIKE nodes it cannot translate soundly.

#### Type dispatch table (subject = bare `column`, type from `involvedTables[0].columns`)

| Exasol type | Action |
|-------------|--------|
| `VARCHAR(n)`, `CHAR(n)` | leave unchanged |
| `DATE` | rewrap subject as `CAST(<col> AS VARCHAR)` (`function_scalar_cast`) |
| `DECIMAL(p,s)` (incl. integer `DECIMAL(p,0)`) | decline whole filter |
| `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, all other non-string | decline whole filter |
| bare `column` whose name is not found in `involvedTables[0].columns` (lookup miss) | decline whole filter (fail-safe) |
| subject not a bare `column` | leave unchanged (out of scope) |

The subject-name lookup is case-normalized (uppercased) before matching the type map, mirroring `extract_all_column_types`'s existing uppercasing of column names (`support.rs:411`), so a case-mismatched name resolves rather than spuriously declining.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| All-or-nothing filter decline | `like_subject_type_guard` returns `None` | Mirrors the documented untranslatable-predicate backstop (`mod.rs:14-15`); Exasol evaluates the predicate natively |
| Reuse existing CAST node shape | injected `function_scalar_cast` node | `render_cast` / `render_cast_target` already render this shape; zero `vs-expression` changes |
| Type decision in adapter, not translator | `support.rs` | `vs-expression` is stateless and shared; column type is external context |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| CAST DATE, decline other non-string | CAST every non-string type | DataFusion decimal/double/timestamp-to-string formatting diverges from Exasol's, silently changing matches — worse than a native-eval fallback. The DATE CAST is Exasol-faithful only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`); an altered session format is an accepted tracked exception (#216) — see decision-log entry [8] |
| Guard in adapter (`support.rs`) | Fix inside `vs-expression` | `vs-expression` has no column-type context and is shared with a sibling project |
| Decline whole top-level filter | Decline only the offending conjunct | Matches the existing all-or-nothing backstop; partial-filter surgery risks changing result semantics |
| Single-table path only | Also fix the join per-leg path now | Join path needs per-side type threading; scoped to a tracked follow-up to keep this fix minimal |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |

`sql-comprehension/vs-expression-translator` is intentionally NOT changed — see decision-log entry [4]. `vs-adapter/pushdown-planning-capability-extensions` is intentionally NOT changed — see decision-log entry [5].

## Dependencies

- Issue #215 tracks the join per-leg LIKE type-awareness gap (`joins/sql_builders.rs` `render_df_filter_safe` calls at ~line 71 and ~506) — the real, already-filed follow-up issue; cite its number in the join spec when the join path is addressed.
- Stacks on PR #213 (issue #206); PR base branch is `fix/206-not-in-null-const-list`.

## Implementation Tasks

1. Add `like_subject_type_guard(filter: &Json, col_types: &[(String, String)]) -> Option<Json>` to `crates/lakehouse-engine/src/adapter/pushdown/support.rs`: recursively walk `predicate_and`/`predicate_or`/`predicate_not`; at each `predicate_like`/`predicate_like_regexp` with a bare-`column` subject, uppercase the subject name before looking it up in `col_types` (matching `extract_all_column_types`'s uppercasing at `support.rs:411`), then dispatch per the type table (leave / CAST-rewrap / decline); a bare-column subject whose name is not found in the type map declines the whole filter (fail-safe); propagate a decline as `None` up the whole tree. [expert]
2. Verify the injected `function_scalar_cast` `{"type":"VARCHAR"}` node renders to `CAST(<col> AS VARCHAR)` via the existing `render_cast_target` DataFusion arm, and that DataFusion's `Date32`→`Utf8` cast yields the `YYYY-MM-DD` form; no `vs-expression` change required. Contingency: if DataFusion's `Date32`→`Utf8` cast format is ever confirmed NOT to be `YYYY-MM-DD` (it is in current DataFusion/arrow — low risk), DATE MUST fall back to the DECLINE branch (same as DECIMAL), never ship a wrong-format cast. [expert]
3. Wire `like_subject_type_guard` into `handle_pushdown` (`mod.rs`): move `extract_all_column_types` above the filter block and thread the guard between `filter_json_raw` and `render_df_filter_safe`; leave the raw tree forwarded to `resolve_file_list` untouched.
4. Add unit regression tests in `support.rs` covering: VARCHAR passthrough, DATE→CAST, DECIMAL decline, integer `DECIMAL(20,0)` decline, non-column subject untouched, an unresolvable bare-column subject (name not in the type map) declining the whole filter, and a nested non-string LIKE declining the enclosing filter. Each DATE/DECIMAL test MUST fail on current code and pass after the fix.
5. Commit the three untracked capture-harness deliverables already present in the worktree — `crates/lakehouse-engine/tests/e2e_capture_pushdown.rs`, `scripts/capture-pushdown-payload.sh`, `docs/debugging-pushdown.md` — as part of this PR; they are the pushdown-payload capture tooling used to gather issue #207's ground truth, not unrelated clutter.
6. Run `cargo clippy --all-targets && cargo fmt` and host `cargo test`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 2 |
| Group B | Task 4 |

Sequential dependencies:
- Group A → Task 3 (wiring depends on the guard existing)
- Task 3 → Group B (tests exercise the wired path)
- Task 5 is independent (commit-only); Task 6 runs last.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | None. This change adds one helper and rewires one call site; no code is obsoleted. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Filter predicate is pushed into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `pushdown_translates_or_omits_predicate` (existing, still passes) |
| LIKE on a VARCHAR or CHAR column pushes down unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_varchar_subject_unchanged` |
| LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_date_subject_wraps_cast` |
| LIKE on a DECIMAL column declines the whole filter | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_decimal_subject_declines` |
| LIKE on an integer column declines the whole filter | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_integer_subject_declines` |
| LIKE on a non-column subject is left untouched | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_non_column_subject_untouched` |
| LIKE on a bare column whose type cannot be resolved declines the whole filter | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_unresolvable_column_declines` |
| A nested non-string LIKE declines the entire enclosing filter | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_nested_decimal_declines_whole_filter` |

Unit tests are appropriate here: the guard is pure computation over JSON with no I/O. The DATE and DECIMAL tests are the required regressions — they fail on current (unfixed) code and pass after the fix.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning | `cargo test -p lakehouse-engine like_guard` | All `like_guard_*` tests pass; DATE test shows a rendered `CAST(... AS VARCHAR)`, DECIMAL/integer tests show `None` (declined) |

Full local E2E is out of scope for this plan's verification — reserved for CI. The one-off `scripts/capture-pushdown-payload.sh` captures were already completed this session and are sufficient ground truth.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |

Build note: `make cross-musl-udf-build` (the `.so` build) is a CI/release concern, not a unit-test gate; do not run host `cargo build --release`.
