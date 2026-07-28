# Plan: refactor-pushdown-expr-rewrite-primitive

`Closes #257`

## Summary

Extract one post-order rewrite primitive plus two canonical child-field consts in
`crates/lakehouse-engine/src/adapter/pushdown/support.rs`, reducing all three type-aware
expression-tree guards to a per-node closure. Commit 1 is a pure refactor with byte-identical
rendered SQL; commit 2 points `like_subject_type_guard` at the wider curated traversal, which
closes its documented `function_scalar_case` blind spot.

## Design

### Context

Three type-aware rewriters over the untyped `serde_json` pushdown expression grammar each
hand-roll the same post-order recursion:

| Guard | Traversal | Fallibility |
|---|---|---|
| `like_subject_type_guard` (~`support.rs:536`) | narrow — `predicate_and`/`predicate_or` (`expressions`), `predicate_not` (`expression`) only | fallible (`None` declines the whole filter) |
| `rewrite_decimal_stringifications` (~`support.rs:661`) | wide — curated child-field set | infallible (`&Json -> Json`) |
| `string_function_arg_type_guard` (~`support.rs:887`) | wide — the SAME set, copied verbatim | fallible |

The curated field list is duplicated verbatim in the two wide walkers and kept in sync by comment:

```
["expressions", "arguments", "results"]                 // array-valued children
["expression", "pattern", "left", "right", "basis"]     // single-child fields
```

Each of the three type-blind fixes (#207, #210, #211) copied the traversal again. A fourth would
copy it a third time. The list is curated on purpose — a rewrite must not descend into `dataType`
or `name` sub-objects and rebuild them.

- **Goals** — one traversal, one field-list declaration, three per-node closures; byte-identical
  rendered SQL in commit 1; the LIKE guard's `function_scalar_case` blind spot closed in commit 2.
- **Non-Goals** — no `Visitor` trait per node type and no typed AST (the IR is deliberately untyped
  `serde_json`; a typed AST contradicts `vs-expression`'s no-SQL-parser property); no pass-ordering
  pipeline for the `like` → `string` → `decimal` chain; no merge with issue #177's BLIND
  collect-style `walk_json` (it recurses over every map value, which is exactly what the curated
  list must not do); no new call site for any guard, so #215 and #219 stay open.

### Decision

One free function plus one closure per guard — the honest size for this IR.

#### Architecture

```
                        ┌───────────────────────────────┐
                        │ EXPR_ARRAY_FIELDS  (3 fields) │
                        │ EXPR_SINGLE_FIELDS (5 fields) │
                        └───────────────┬───────────────┘
                                        │ read by
                        ┌───────────────▼───────────────┐
                        │ rewrite_expr_tree(node, &f)   │
                        │ post-order: children, then f  │
                        │ None from f  ⇒ whole tree None│
                        └───┬───────────┬───────────┬───┘
       per-node closure     │           │           │
  ┌─────────────────────────▼┐  ┌───────▼────────┐  ┌▼──────────────────────────┐
  │ like_subject_type_guard  │  │ string_function│  │ rewrite_decimal_          │
  │  → guard_like_subject    │  │ _arg_type_guard│  │ stringifications          │
  │  (fallible)              │  │  (fallible)    │  │  (always Some, no panic)  │
  └──────────────────────────┘  └────────────────┘  └───────────────────────────┘
```

Key interface:

```rust
/// Post-order: rewrite every curated child first, then apply `f` to this node.
/// `f` returning `None` declines the whole tree (propagates via `?`).
fn rewrite_expr_tree(node: &Json, f: &impl Fn(&Json) -> Option<Json>) -> Option<Json>
```

Child handling reproduces today's conditions exactly: an array field is recursed only when it is a
`Json::Array`; a single-child field only when the child `is_object()`.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Free function + per-node closure | `rewrite_expr_tree` | Absorbs traversal, post-order discipline, and decline propagation while exposing one parameter; a visitor trait would add a type per node kind for no gain over an untyped IR |
| Single-source const | `EXPR_ARRAY_FIELDS` / `EXPR_SINGLE_FIELDS` | The curated list is a design decision; one owner replaces two comment-synced copies, and extending it is a one-line change all three guards inherit |
| `Option` as the decline channel | primitive return type | The fallible guards already use `None` = decline-whole-tree; the infallible rewriter composes as the never-declining case via `.unwrap_or_else` rather than gaining a decline path |
| Private `fn` until a caller needs more | `rewrite_expr_tree` | Matches #257's own snippet and `pushdown-module-structure`'s recorded rule — a cross-submodule helper widens to the narrowest visibility that compiles. Nothing outside `support.rs` calls it in this plan, so private is that narrowest visibility; #177 widens it to `pub(super)` when it adds the first cross-submodule caller (`joins/rendering.rs`), which is a one-word change |

Design-philosophy diagnostic: the primitive is deep — one sentence describes it, calling it is far
cheaper than re-deriving post-order-plus-curated-fields-plus-decline-propagation, and it removes a
leaked decision (the field list) that two modules previously asserted independently. No
configuration parameter is added; `f` is the only knob.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Leaves are passed to `f` too | Keep each guard's `!node.is_object()` early return inside the primitive | Provably behavior-preserving and simpler: today both wide walkers early-return on a non-object and skip step 2, but step 2 is a no-op on a non-object anyway — `get("type")` yields `None`, so the decimal walker falls to its `_ => out` arm and the string walker's `!= Some("function_scalar")` returns `Some(out)` unchanged. Pinned by a test per guard |
| `rewrite_decimal_stringifications` keeps `-> Json`, composing via `.unwrap_or_else(\|\| node.clone())` | Change its signature to `-> Option<Json>`; or keep `-> Json` via `.expect` | Its two call sites compose with `.map`, not `.and_then`, so widening the signature churns callers for a decline that cannot happen. `.unwrap_or_else` honors the same contract without adding a panic site to the query-planning path, where the pre-refactor function had none |
| The LIKE guard's widened reach may turn a former pushdown into a decline | Keep the narrow traversal and leave #207's blind spot open | A decline is always correct (Exasol evaluates natively). Where the subject type resolves to a non-string type the pre-change render hard-failed the scan, so the decline fixes a crash; where the name does not resolve it may cost a working pushdown — slower, never wrong (§ Impact, decision-log [7]) |
| Commit 2 is a separate commit with its own regression test | One combined commit | Commit 1's value is provable byte-identity; mixing a behavior change into it destroys that proof |
| `like_subject_type_guard` stays filter-only | Wire it into `project_columns` and the join per-leg filter path | A select-list decline must set the widen-projection flag, not drop a filter — different fallback semantics, so #215/#219 stay their own fixes. This refactor only makes closing them cheaper |
| Issue #177's blind `walk_json` stays separate | One universal JSON walker | The blind walker recurses over `map.values()`; the curated walker must NOT touch `dataType`/`name`. Merging them would silently widen the rewrite surface |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-module-structure | CHANGED | `vs-adapter/pushdown-module-structure/spec.md` |
| vs-adapter/pushdown-planning-like-type-coercion | CHANGED | `vs-adapter/pushdown-planning-like-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-string-fn-type-coercion | CHANGED | `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-decimal-string-format | CHANGED | `vs-adapter/pushdown-planning-decimal-string-format/spec.md` |

## Impact

Commit 1: none — rendered SQL is byte-identical.

Commit 2: one query-behavior change, at a LIKE position the junction-only traversal did not reach
(inside a `function_scalar_case`, under a comparison operand, or inside a scalar function's
`arguments`), in two sub-cases. Where the subject type RESOLVES to a non-string type it fixes a
crash: the predicate previously hard-failed the DataFusion scan and now declines to native Exasol
evaluation, or is rewritten to `CAST(<col> AS VARCHAR)` for a DATE subject. Where the subject name
does NOT resolve it may COST a working pushdown — slower, never wrong (see the like-coercion delta's
Background for the `extract_all_column_types` mechanism).

No breaking change: no signature, DDL, deployment, or configuration surface changes, and the raw
filter tree forwarded to Iceberg file pruning is untouched, so no file-pruning decision changes.

## Requirements

| Requirement | Details |
|-------------|---------|
| Byte-identical commit 1 | Two distinct evidence classes, both MUST pass with no assertion edit. (a) JSON-shape equality: the `support.rs` guard corpus asserts rewritten JSON trees, not SQL. (b) Rendered SQL: only the five chain-replicating tests in `mod.rs`'s `mod tests` assert SQL strings — `where_filter_decimal_stringification_rewritten_to_trim`, `filter_decimal_comparison_not_rewritten`, `where_filter_string_fn_under_comparison_predicate_coerced`, `where_filter_string_fn_over_double_declines`, `where_filter_upper_decimal_inside_like_subject_coerced` |
| Call-site census | Production has exactly ONE chain site: `mod.rs:210-214` (the single-table chokepoint), plus `support.rs`'s `project_columns` select-list chain (~`:1104-1125`, string + decimal guards only; the LIKE guard is NOT wired there). Issue #257's "six `mod.rs` sites" counts that one production site plus the five test replications — verified against `mod tests` starting at `mod.rs:789` |
| Feature-gated test crates | Per the project census rule, `cargo test --features exasol-e2e --no-run` MUST compile — host `cargo test` skips those crates. `tests/e2e_capability_test.rs` (~`:2085`, `:2139`, `:2311`) and `joins/rendering.rs:529` reference the guards in COMMENTS only, no calls |
| Iceberg-spec compliance | Determination: NOT implicated — this changes traversal shape over the Exasol pushdown JSON IR only. `filter_json_raw` stays unmodified for `resolve_file_list` pruning and no Iceberg-boundary type mapping is touched; see decision-log [11] |
| Pushdown-semantics invariant | The guard chain feeds ONLY the DataFusion-bound scan filter. `filter_json_raw` MUST remain unmodified for the later `resolve_file_list` call |
| Stale-documentation sweep | Commit 2 invalidates every code-documentation claim about `like_subject_type_guard`'s reach; each MUST be corrected in the same commit. Before closing commit 2 the implementer MUST run `grep -rn "junction" crates/`, which is necessary but NOT sufficient — two sites assert the contrast without the word — so the site list in tasks 9-10 governs |

## Dependencies

None. No new crate, no version bump, no SLC or `.so` rebuild — the change is confined to the
adapter planning layer.

## Implementation Tasks

### Commit 1 — pure refactor, byte-identical rendered SQL

1. Add the leaf-equivalence characterization test BEFORE any migration, and prove it against the
   UNMIGRATED code: one test asserting `rewrite_decimal_stringifications` returns a non-object node
   unchanged (mirroring the existing `string_fn_guard_passes_through_non_object_node`, which already
   characterizes the string guard's leaf behavior). It MUST be added and PASS against today's
   `rewrite_decimal_stringifications` with its `!node.is_object()` early return still in place — that
   is what makes it a proof of equivalence rather than a description of the new code — and MUST then
   re-run unchanged after task 4. If it can only be made to pass after the migration, the
   "leaves are passed to `f` too" simplification is NOT behavior-preserving and task 4 must stop.
2. Add `EXPR_ARRAY_FIELDS`, `EXPR_SINGLE_FIELDS`, and a PRIVATE `fn rewrite_expr_tree` to
   `support.rs` — private is the narrowest visibility that compiles, since no caller outside
   `support.rs` exists in this plan. The doc comment MUST state the post-order contract, the
   decline-propagation contract, the always-`Some` composition for the infallible walker, and WHY
   the field list is curated (never descend into `dataType`/`name`). Child conditions MUST reproduce
   today's exactly: array field only when `Json::Array`, single field only when
   `child.is_object()`. [expert]
3. Migrate `string_function_arg_type_guard` onto `rewrite_expr_tree`, deleting its hand-rolled
   step-1 loops and its `!node.is_object()` early return; its step 2 becomes the per-node closure.
4. Migrate `rewrite_decimal_stringifications` onto `rewrite_expr_tree` with an always-`Some`
   closure, keeping its `-> Json` signature via `.unwrap_or_else(|| node.clone())` — NOT `.expect`.
   The closure is statically always-`Some`, so the fallback is unreachable; a panicking form would
   add a failure mode to the query-planning path for no benefit. State the always-`Some` invariant
   in the doc comment, not in a panic message.
5. Update both migrated guards' doc comments: the string guard's "POST-ORDER over the same
   child-bearing fields as `rewrite_decimal_stringifications`" and the decimal walker's inline
   field enumeration now name the shared primitive as the single owner of the traversal. This is the
   implementing task for the `vs-adapter/pushdown-planning-decimal-string-format` and
   `vs-adapter/pushdown-planning-string-fn-type-coercion` deltas, both of which only reattribute
   traversal ownership — neither changes a rendered byte.
6. Run the commit-1 gate: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, and
   `cargo test --features exasol-e2e --no-run`. No test assertion may be edited.

### Commit 2 — LIKE guard onto the primitive, behavior change

7. Add the failing regression tests: `support.rs` unit tests for a DECIMAL-column LIKE inside a
   `function_scalar_case`'s `arguments` (declines) and a DATE-column LIKE at the same position
   (rewritten in place, enclosing CASE structure preserved); plus one wired-chain test in
   `mod.rs`'s `mod tests` replicating the production chain over
   `predicate_equal(function_scalar_case(... predicate_like(<decimal col>) ...), 1)` and asserting
   the whole filter is omitted.
8. Migrate `like_subject_type_guard` onto `rewrite_expr_tree`, reducing it to a per-node closure
   that dispatches `predicate_like`/`predicate_like_regexp` to `guard_like_subject` and returns
   every other node unchanged. Two equivalences MUST be verified and stated in the code
   documentation, not assumed: (a) the primitive now visits a LIKE node's own `expression` and
   `pattern` children BEFORE the LIKE dispatch, which is inert because the closure acts only on the
   two LIKE node types, so a bare `column` subject reaches `guard_like_subject` unchanged; (b) the
   old `predicate_not` arm recursed into a non-object `expression` child while the primitive skips
   it — equivalent, because recursing a non-object returned `Some(clone)` and reassigned the same
   value. [expert]
9. Rewrite `like_subject_type_guard`'s ENTIRE traversal paragraph (`support.rs:500-507`), not only
   its caveat sentence. Every one of its three claims is now false: the enumeration "Walks the filter
   tree through the only node types that can nest a `predicate_like` … `predicate_and` /
   `predicate_or` … and `predicate_not`"; the caveat "A `LIKE` reachable only through some other node
   (e.g. buried in a `function_scalar_case`) is out of scope … it renders as before and is the
   pre-existing risk"; and the closing sentence "Any node that is neither a junction nor a `LIKE` is
   returned unchanged". Replace them with the shared-primitive reach, the closed blind spot, and the
   decline trade in BOTH its sub-cases (resolved non-string type — a fixed hard scan failure;
   unresolved name — a possibly-lost working pushdown traded for correct native evaluation).
10. Sweep the remaining stale reach claims. `grep -rn "junction" crates/` returns exactly four live
    claims about `like_subject_type_guard`'s reach; task 9 covers the first, this task covers the
    other three:
    - `support.rs:875` — `string_function_arg_type_guard`'s doc, "a node `like_subject_type_guard`'s
      junction-only recursion never descends into". Keep WHY the wide field list is load-bearing
      (a filter-side string function sits under a comparison predicate); drop the contrast.
    - `support.rs:5361-5363` — the doc comment on the test
      `string_fn_guard_reaches_function_under_comparison_predicate` (`:5365`), "the reach
      `like_subject_type_guard`'s junction-only recursion does not have". The test body and its
      assertions stay unedited; only the rationale changes.
    - `mod.rs:949` — the doc comment on the test
      `where_filter_string_fn_under_comparison_predicate_coerced` (`:956`), "a node
      `like_subject_type_guard`'s junction-only recursion … never descends into". Body unedited.
    Plus TWO sites the grep does NOT catch, because they assert the claim without the word — both
    MUST be corrected:
    - `mod.rs:188-209`, the chain comment, whose parenthetical "over the whole tree (not just LIKE
      subjects — it reaches a string function nested under any comparison predicate too)" reads only
      as a contrast with a narrower LIKE guard.
    - `support.rs:563-564`, the inline comment on `like_subject_type_guard`'s `_` match arm: "Any
      other node (predicate_equal, column, literals, …) is not a LIKE and cannot nest one in this
      grammar — returned unchanged". Commit 2 falsifies the second half precisely — `predicate_equal`
      is exactly the node the widened traversal descends through to reach a LIKE under `left`, this
      plan's headline repro. DELETE the "cannot nest one in this grammar" claim or restate it as "is
      not itself a LIKE". It survives task 8's rewrite otherwise, because it annotates the `_` arm
      that becomes the closure's catch-all.
    `mod.rs:1013-1018` (`where_filter_upper_decimal_inside_like_subject_coerced`, `:1020`) and
    `joins/rendering.rs:529` were checked and assert no reach claim — leave both alone.
11. Run the commit-2 gate: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, and
    `cargo test --features exasol-e2e --no-run`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1 |
| Group B | 2 |
| Group C | 3, 4 |
| Group D | 5 |
| Group E | 6 |
| Group F | 7 |
| Group G | 8 |
| Group H | 9, 10 |
| Group I | 11 |

Sequential dependencies:
- Group A → Group B (the leaf-equivalence test must pass against the UNMIGRATED walker first, so it
  proves equivalence instead of describing the migrated shape)
- Group B → Group C (both migrations need the primitive)
- Group C → Group D (the doc rewrites describe the migrated shape)
- Group D → Group E (commit-1 gate)
- Group E → Group F → Group G (failing regression tests before the behavior change)
- Group G → Group H → Group I

Tasks 3 and 4 touch disjoint functions in one file; serialize the edits if the implementer cannot
guarantee non-overlapping regions.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Code block | `rewrite_decimal_stringifications` step-1 loops + `!node.is_object()` early return (~`support.rs:665-691`) | Replaced by `rewrite_expr_tree` |
| Code block | `string_function_arg_type_guard` step-1 loops + `!node.is_object()` early return (~`support.rs:891-917`) | Replaced by `rewrite_expr_tree` |
| Code block | `like_subject_type_guard`'s `predicate_and`/`predicate_or`/`predicate_not` match arms and their self-recursive calls, THROUGH the `_` arm's inline comment (~`support.rs:544-564`) | Replaced by `rewrite_expr_tree` in commit 2; the range deliberately extends past `:561` to cover the "cannot nest one in this grammar" comment at `:563-564` that commit 2 falsifies (task 10) |
| Comment | The duplicated child-field enumerations and their sync comments in both wide walkers | The consts are now the single source |

`guard_like_subject`, `is_bare_decimal_column`, `wrap_decimal_to_varchar`, `wrap_cast_to_varchar`,
`string_position_args`, and `coerce_string_position_arg` all stay — they become closure bodies or
their helpers, not dead code.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| pushdown-module-structure: The three type-rewrite guards walk the expression tree through one shared post-order primitive | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `decimal_rewrite_passes_through_non_object_node` (new) + the unedited existing corpus: `rewrite_reaches_decimal_inside_case_then_branch`, `rewrite_nested_concat_wraps_only_inner_decimal`, `string_fn_guard_passes_through_non_object_node`, `string_fn_guard_reaches_function_under_comparison_predicate`, `string_fn_guard_nested_decline_propagates_to_root`, `string_fn_guard_coerces_inner_nested_string_function` |
| pushdown-module-structure: byte-identity clause, rendered-SQL half (the `support.rs` corpus above covers the JSON-shape half — those tests assert JSON trees, not SQL) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | The five chain-replicating tests, all unedited: `where_filter_decimal_stringification_rewritten_to_trim`, `filter_decimal_comparison_not_rewritten`, `where_filter_string_fn_under_comparison_predicate_coerced`, `where_filter_string_fn_over_double_declines`, `where_filter_upper_decimal_inside_like_subject_coerced` |
| like-type-coercion: A nested non-string LIKE declines the entire enclosing filter | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_nested_decimal_declines_whole_filter`, `like_guard_not_wrapped_decimal_declines` (existing, unedited) + `like_guard_decimal_inside_case_declines` (new) |
| like-type-coercion: A LIKE nested inside a CASE expression is type-guarded | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `like_guard_decimal_inside_case_declines`, `like_guard_date_inside_case_wraps_cast` (new) |
| like-type-coercion: A LIKE nested inside a CASE expression is type-guarded (wired chain) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `where_filter_like_decimal_inside_case_declines_whole_filter` (new) |
| string-fn-type-coercion: The guard composes with the LIKE type guard and the decimal-stringification rewriter without double coercion | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `where_filter_decimal_stringification_rewritten_to_trim`, `where_filter_upper_decimal_inside_like_subject_coerced` (existing, unedited — the clause now states only the verifiable half, which these two already pin) |
| decimal-string-format: Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `rewrite_nested_concat_wraps_only_inner_decimal`, `selectlist_nested_concat_decimal_arg_rewritten` (both existing, unedited — the delta reattributes the descent to the shared traversal and changes no rendered byte) |

Unit tests are the correct level here: all three guards are pure `&Json -> Json`/`Option<Json>`
computations with no I/O, and the chain-replicating tests in `mod.rs` cover the composed shape.
The end-to-end behavior of commit 2 is additionally exercised by `make test-e2e`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-module-structure | `cargo test -p lakehouse-engine adapter::pushdown` | 0 failures; `git diff` shows no edited test assertion or expected value |
| pushdown-module-structure | `git diff --stat crates/lakehouse-engine/src/adapter/pushdown/` after commit 1 | `support.rs` loses more lines than it gains; no `mod.rs` test body changed |
| like-type-coercion | Start the Exasol Docker stack, then `make test-e2e` (the target does NOT start the stack; without it every DB-backed test FAILS, not skips) | 0 failures |
| like-type-coercion | Against the deployed VS: `SELECT COUNT(*) FROM <vs_schema>.LINEITEM WHERE CASE WHEN L_QUANTITY LIKE '1%' THEN 1 ELSE 0 END = 1` | Returns a count equal to the same query over a native Exasol copy of the table. Pre-change this shape aborted with `There isn't a common type to coerce Decimal128(…) and Utf8 in LIKE expression` (SQL state 22002) |
| like-type-coercion | Against the deployed VS: `SELECT COUNT(*) FROM <vs_schema>.LINEITEM WHERE CASE WHEN L_SHIPDATE LIKE '1994%' THEN 1 ELSE 0 END = 1` | Returns the correct count with the DATE subject pushed down as `CAST("L_SHIPDATE" AS VARCHAR)`, no scan failure |
| string-fn-type-coercion | Against the deployed VS: `SELECT COUNT(*) FROM <vs_schema>.LINEITEM WHERE UPPER(L_QUANTITY) = '17'` | Unchanged from pre-refactor: pushes down through the trimmed `decimal_to_varchar_exasol` form and returns the same count |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test | `cargo test` | 0 failures |
| Census (feature-gated crates) | `cargo test --features exasol-e2e --no-run` | Compiles, exit 0 |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| E2E | Exasol Docker stack up, then `make test-e2e` | 0 failures |
