# Plan: fix-in-constlist-null-handling

## Summary

Strip NULL entries from a rendered `predicate_in_constlist` list so a pushed-down `IN`/`NOT IN` matches Exasol semantics instead of silently returning an empty result set. Closes GitHub issue #206.

## Context

`predicate_in_constlist` (`crates/vs-expression/src/lib.rs:308-327`) joins every rendered argument verbatim into the `IN (...)` list, including NULL-valued literals. DataFusion applies three-valued logic: `x NOT IN (v, NULL)` evaluates to UNKNOWN for every non-matching row, so all such rows are filtered and the query returns an empty result. Exasol instead ignores NULL entries in an IN list, so `x IN (1, NULL)` behaves as `x IN (1)` and `x NOT IN (1, NULL)` behaves as `x NOT IN (1)`. The pushdown therefore produces silently wrong results whenever a const list carries a NULL.

A NULL entry does not always render as the bare token `NULL`. A `literal_null` node does (`lib.rs:197`), but a null-valued `literal_date` renders as `DATE NULL` (`lib.rs:216`, via `quote_literal` at `lib.rs:67-73`) and a null-valued `literal_timestamp` renders as `arrow_cast(NULL, 'Timestamp(Microsecond, None)')` (`lib.rs:222-225`). A post-render match on the string `"NULL"` misses the typed-null shapes, leaving the bug alive for DATE/TIMESTAMP const-list entries. The fix therefore keys on the argument node before rendering: an argument is stripped if it is a `literal_null` node or any `literal_*` node whose `value` is JSON `null` (or absent). This is a SQL-dialect translation bug in predicate rendering, not a scan, pushdown-planning, or type-handling change — so no Iceberg-spec compliance check applies (this repo's CLAUDE.md requires that check only for scanning/pushdown/schema-type work).

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator | CHANGED | `sql-comprehension/vs-expression-translator/spec.md` |

## Implementation Tasks

1. In `predicate_in_constlist` (`crates/vs-expression/src/lib.rs:314-326`), skip each argument node that is a NULL-valued literal *before* calling `render_expression_inner` on it, so it never enters the `rendered` Vec. The check is on the argument's JSON node, not its rendered string: skip when `arg["type"] == "literal_null"`, or when `arg["type"]` starts with `literal_` AND `arg["value"]` is JSON `null` or absent. This uniformly strips bare NULL, `DATE NULL`, and `arrow_cast(NULL, ...)` typed-null shapes. An all-NULL list then arrives empty at the existing emptiness check and falls through to the `FALSE` rendering; a mixed list joins only the surviving non-NULL arguments. (A small `is_null_literal(arg: &Json) -> bool` helper keeps the loop readable.)
2. Add unit tests in `crates/vs-expression/src/lib.rs` `mod tests` alongside `renders_in_constlist`: (a) a mixed list of a real literal plus a `literal_null` plus a null-valued `literal_date` renders the `IN (...)` list with both NULL shapes stripped and only the real value surviving; (b) an all-NULL list (mixing `literal_null` and a null-valued typed literal) renders `FALSE`.
3. Add a third unit test that wraps `predicate_in_constlist` in a `predicate_not` node, over a mixed real-plus-`literal_null`-plus-null-valued-`literal_date` list, asserting the emitted SQL is `(NOT (<target> IN (<real value only>)))` with no bare `NULL`, `DATE NULL`, or `arrow_cast(NULL, ...)` entry surviving. This is the unit-level proxy for the reported `NOT IN` bug (issue #206: `NOT IN` silently returning 0 rows).

No task needs deep reasoning: a node-level filter over the arguments loop plus three table-driven unit tests copied from the existing `renders_in_constlist` pattern. No `[expert]` tag — a standard implementer suffices.

The implementing commit MUST reference `Closes #206` (applied by `/speq:implement-pr`, recorded here so the issue link is not lost).

## Parallelization

Single sequential path: Tasks 2 and 3 test the behavior Task 1 introduces. No parallel groups.

## Dead Code Removal

None. The existing empty-list `FALSE` branch is retained unchanged and is reached by the all-NULL case after stripping.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| IN constant list translates to SQL IN expression | Unit | `crates/vs-expression/src/lib.rs` | `renders_in_constlist` (unchanged, real-literal list), `renders_in_constlist_strips_null` (mixed real + `literal_null` + null-valued `literal_date`), `renders_all_null_in_as_false` (mixed null shapes), `renders_not_in_constlist_strips_null` (`predicate_not` wrapper) |

Unit tests are correct here: `render_expression` is pure computation over serde_json input with no I/O. No E2E test is planned locally — CI runs the E2E suite once the PR is pushed.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| sql-comprehension/vs-expression-translator | `cargo test -p vs-expression in_constlist` | `renders_in_constlist_strips_null`, `renders_all_null_in_as_false`, and `renders_not_in_constlist_strips_null` pass; all three fail against pre-fix code |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test | `cargo test -p vs-expression` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
