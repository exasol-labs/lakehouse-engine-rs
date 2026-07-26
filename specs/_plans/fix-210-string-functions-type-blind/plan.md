# Plan: fix-210-string-functions-type-blind

## Summary

Add an adapter-level recursive guard, `string_function_arg_type_guard`, that resolves each Exasol string function's string-position argument indices and dispatches every such bare-column argument on its Exasol type before rendering, so `UPPER`/`LOWER`/`TRIM`/`INSTR`/`LOCATE` and the rest of the family stop hard-failing the DataFusion scan on a numeric or DATE column (issue #210). String arguments pass through, DATE arguments cast to VARCHAR, DECIMAL arguments reuse #211's trimmed `decimal_to_varchar_exasol` node, and every other resolvable type declines to native Exasol evaluation.

## Context

`crates/vs-expression/src/lib.rs:716-773` renders the whole string-function family — the `CONCAT | LOWER | UPPER | … | UNICODECHR` arm plus the separate `INSTR` and `LOCATE` arms — by handing each argument straight to `render_args` with zero type inspection. `crates/lakehouse-engine/src/adapter/capabilities.rs:85-108` advertises `FN_UPPER`, `FN_LOWER`, `FN_TRIM`, `FN_INSTR`, `FN_LOCATE`, and the rest unconditionally, so Exasol pushes these nodes for any argument type. Exasol implicitly converts a numeric or DATE argument to VARCHAR first; DataFusion refuses, and the scan dies at plan time with `F-UDF-CL-RUST-9001 … DataFusion SQL error: Error during planning` (SQL state 22002) — a hard error, not a native fallback. Issue #210 lists five live repros, all select-list shaped: `UPPER(c_custkey)`, `TRIM(c_custkey)`, `LOWER(l_shipdate)`, `LTRIM(c_acctbal)`, `INSTR(c_custkey, '1')`.

The fix cannot live in `vs-expression`: that crate is a pure syntactic JSON-to-SQL translator with no column-type context, and it is shared with a sibling VS-adapter project. This is the third instance of the same bug class in the same place, and the two prior fixes set the pattern this plan follows:

* **#207** — `like_subject_type_guard` in `crates/lakehouse-engine/src/adapter/pushdown/support.rs:468`, an `Option<Json>`-returning guard where `None` declines the whole filter. Spec: `specs/vs-adapter/pushdown-planning-like-type-coercion/spec.md`.
* **#211** — `rewrite_decimal_stringifications` in the same file at line 600, a post-order recursive rewriter over every child-bearing field, plus the `decimal_to_varchar_exasol` node and `wrap_decimal_to_varchar` helper. Spec: `specs/vs-adapter/pushdown-planning-decimal-string-format/spec.md`, whose own scope explicitly named "every other string function that hard-fails on a DECIMAL argument (issue #210)" as its follow-up. This plan is that follow-up.

The new guard needs both halves: #207's `Option` decline contract and #211's broad recursion. #207's junction-only recursion is insufficient here — a filter-side string function sits under a comparison predicate (`UPPER(c) = 'X'` is `predicate_equal` with the function under `left`), which `like_subject_type_guard` never descends into.

- **Goals** — at the two wired render surfaces, every governed string function over any Exasol column type either pushes down with Exasol-faithful semantics or declines to native Exasol evaluation: no hard scan error, no silently wrong result. The two surfaces are the single-table WHERE-clause filter chain in `handle_pushdown`, and the select-list/projection path `project_columns` — which is shared with the broadcast-join SELECT list, so that surface is covered too. Every other render surface stays unguarded as a named tracked exception (#227).
- **Non-Goals** — the broadcast-join PER-LEG WHERE-clause filter path (#223), the ENTIRE grouped-aggregate render path — group keys AND select items, whether or not a key is also selected — plus the aggregate-argument path (both #227), a non-bare-column string-position argument (#223), `CHR`/`UNICODECHR` argument typing, a faithful rendering of `INSTR`/`LOCATE`'s optional third and fourth arguments (#228 — this plan declines those calls instead of rendering them), and any change to the advertised `FN_*` capability set.

## Design

### Context

Three forces decide the shape. First, the guard must reach a string function anywhere in either tree, which rules out #207's narrow recursion and points at #211's generic child-field walk. Second, it must be able to decline, which rules out #211's infallible `Json`-returning signature and points at #207's `Option<Json>`. Third, per-argument type dispatch is not per-function-uniform: `SUBSTR(str, start, length)` and `LPAD(str, length, pad)` each mix string-position and genuinely-numeric arguments, so coercing all arguments would corrupt the numeric ones.

### Decision

Cross the two existing patterns: #211's post-order recursion over every child-bearing field, with #207's `Option<Json>` decline propagating through `?`. Split the per-function knowledge into a pure argument table so the recursion stays readable and the table is unit-testable in isolation. The table carries three outcomes, because one governed function family — `INSTR`/`LOCATE` beyond two arguments — must decline on arity alone, independent of any argument's type.

#### Architecture

```
                       WHERE-clause chain (mod.rs handle_pushdown)
  filter_json_raw ─▶ like_subject_type_guard ─▶ string_function_arg_type_guard
                                                          │
                                                          ▼
                          rewrite_decimal_stringifications ─▶ render_df_filter_safe
                                    (idempotent no-op on already-wrapped args)

                       select-list chain (support.rs project_columns, per item)
       (single-table via extract_projection; broadcast join via extract_join_projection,
        whose col_types are the union of both joined tables' columns)
  select_list[i] ─▶ string_function_arg_type_guard ─┬─ Some ─▶ rewrite_decimal_stringifications ─▶ dispatch
                                                    └─ None ─▶ needs_full_fallback = true
                                                               (full base row; for a join, the
                                                                union of both sides' columns)

  string_function_arg_type_guard
    ├─ string_position_args(fn_name, arg_count) -> StringPositionArgs          [pure table]
    │    NotGoverned ─▶ leave node unchanged, never decline
    │    Coerce(idx) ─▶ coerce each listed argument
    │    Decline     ─▶ None  (INSTR/LOCATE with arg_count > 2, #228)
    └─ coerce_string_position_arg(arg, col_types)      -> Option<Json>         [type dispatch]
         VARCHAR/CHAR ─▶ unchanged
         DATE         ─▶ wrap_cast_to_varchar(arg)          [shared with guard_like_subject]
         DECIMAL…     ─▶ wrap_decimal_to_varchar(arg)       [reused from #211]
         other / unresolved ─▶ None
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| `Option<Json>`, `None` = decline whole tree | `string_function_arg_type_guard` | Mirrors `like_subject_type_guard`; composes with `render_df_filter_safe`'s `None`-means-omit contract and with `needs_full_fallback` |
| Post-order recursion over `expressions`/`arguments`/`results` + `expression`/`pattern`/`left`/`right`/`basis` | `string_function_arg_type_guard` | Copied from `rewrite_decimal_stringifications`; the only recursion in this file that reaches a function under a comparison predicate |
| Pure per-function table with three outcomes — `NotGoverned` / `Coerce(indices)` / `Decline` | `string_position_args` | Keeps mixed string/numeric arity knowledge out of the walk and directly unit-testable; `Decline` is what lets an arity the translator renders incompletely stay a native-fallback rather than a silent wrong answer |
| Decline `INSTR`/`LOCATE` beyond two arguments, for every argument type | `string_position_args` | `vs-expression` reads only `args[0]`/`args[1]` and drops the rest (#228). Coercing index 0 would let a truncated rendering plan successfully — today's loud DataFusion error becomes a silently wrong position, the failure the decline branch exists to prevent |
| Reuse `wrap_decimal_to_varchar` verbatim | DECIMAL branch | Decimal formatting has one owner (#211); a second `CAST(… AS VARCHAR)` would reintroduce the trailing-zero divergence #211 fixed |
| Extract `wrap_cast_to_varchar`, call from both guards | DATE branch + `guard_like_subject` | Removes a duplicated `json!` literal; keeps the two DATE branches provably identical |
| Guard runs before `rewrite_decimal_stringifications` | both wired surfaces | The wrap makes the argument a non-bare-column node, so #211's rewriter no-ops instead of double-wrapping |
| Decline, never cast, for BOOLEAN/DOUBLE/TIMESTAMP | `coerce_string_position_arg` | Both engines' text forms differ (`TRUE`/`true`, space/`T` separator); a cast converts a crash into a wrong answer |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| New feature `pushdown-planning-string-fn-type-coercion` + one CHANGED delta on `pushdown-planning-decimal-string-format` | Extend #207's or #211's feature in place | The concern is argument typing across a whole function family, not LIKE subjects or decimal formatting; #211's "non-DECIMAL left unchanged" scenario still needs reconciling, hence the delta |
| Fix in the adapter, not `vs-expression` | Add type context to `vs-expression` | `vs-expression` is stateless, syntactic, and sibling-shared; #207 and #211 both already settled this |
| Guard before `rewrite_decimal_stringifications` | After it | After it, both rewriters would see a bare DECIMAL column and double-wrap; before it, the wrap makes #211's rewriter a no-op — locked by a composition test, not assumed |
| Keep #211's `CONCAT`/`LENGTH` arms | Delete them as newly dead | They stay the only handler for `function_scalar_cast`-to-string over DECIMAL, and remain a correct idempotent backstop for the #223 surfaces that do not yet run the new guard |
| `LPAD`/`RPAD` coerce index 2 only when present | Always coerce indices 0 and 2 | A 2-argument `LPAD` has no index 2; the table is arity-aware to avoid indexing past the end |
| Accept that the guard reaches the broadcast-join SELECT list through the shared `project_columns`, and add one join-projection test | Rest on #211's precedent of adding no join test; or fork a single-table-only copy of the projection path | The reach is real and correct — a decline sets `needs_full_fallback` over the union of both joined tables' columns, the join path's established fallback. #211's no-test argument does not transfer: its rewriter never declines, so sharing the function could not change join control flow, while this guard can. One test is cheaper than an undisclosed behavior change; forking the path would duplicate the dispatch |
| Exclude `CHR`/`UNICODECHR` | Include them for symmetry | Their sole argument is a genuine integer codepoint; coercing it to text would break correct pushdown |
| No `FN_*` capability change | Un-advertise the family, or advertise conditionally | The family is already advertised and now handled for every type; un-advertising would lose the common VARCHAR pushdown, and Exasol's `getCapabilities` has no per-argument-type conditioning |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-string-fn-type-coercion | NEW | `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-decimal-string-format | CHANGED | `vs-adapter/pushdown-planning-decimal-string-format/spec.md` |

## Dependencies

Stacked on the #211 and #212 fixes already committed to this branch's history. Requires `wrap_decimal_to_varchar`, `decimal_to_varchar_exasol`, and `format_decimal_exasol_style`, all shipped by #211. No new crate dependency.

## Implementation Tasks

1. **Pure string-position argument table**
   1. Add `enum StringPositionArgs { NotGoverned, Coerce(Vec<usize>), Decline }` and `string_position_args(fn_name: &str, arg_count: usize) -> StringPositionArgs` to `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, beside `is_bare_decimal_column`. Uppercase `fn_name` before matching. Three outcomes, one per variant:
      - `NotGoverned` — not a governed string function; the caller leaves the node unchanged and never declines on it. Covers `CHR`, `UNICODECHR`, and every non-string function.
      - `Coerce(indices)` — the string-position argument indices: all `0..arg_count` for `CONCAT`/`TRIM`/`LTRIM`/`RTRIM`/`REPLACE`/`TRANSLATE`; `[0]` for `LOWER`/`UPPER`/`ASCII`/`INITCAP`/`REVERSE`/`LENGTH`/`OCTET_LENGTH`/`UNICODE`/`SUBSTR`/`REPEAT`/`LEFT`/`RIGHT`; `[0, 2]` for `LPAD`/`RPAD` when `arg_count > 2` else `[0]`; `[0, 1]` for `INSTR`/`LOCATE` when `arg_count == 2`. Clamp every returned index to `< arg_count`.
      - `Decline` — `INSTR` or `LOCATE` with `arg_count > 2`, unconditionally on argument type. `crates/vs-expression/src/lib.rs:741-772` reads only `args[0]`/`args[1]` and drops the rest (#228), so coercing index 0 would let a truncated rendering plan successfully and return a position computed from offset 1 — a silent wrong answer where today there is a loud DataFusion error. The doc comment MUST state this reason, so the branch is not "simplified" away later.
      Also document that `LOCATE`'s render-time argument reorder does not change which indices are string-position.
   2. Unit-test the table: every governed name returns its documented `Coerce` indices; `CHR`/`UNICODECHR`/`ABS`/`CASE` return `NotGoverned`; `LPAD` with 2 args returns `Coerce([0])` and with 3 returns `Coerce([0, 2])`; a lowercase `fn_name` resolves; no returned index is `>= arg_count`.
   3. Unit-test the arity decline specifically: `INSTR` with 3 and with 4 arguments and `LOCATE` with 3 arguments each return `Decline`, while both with exactly 2 return `Coerce([0, 1])`.

2. **Argument type dispatch and the recursive guard**
   1. Extract the DATE `CAST`-to-VARCHAR `json!` literal out of `guard_like_subject` into a private `wrap_cast_to_varchar(node: &Json) -> Json` helper beside `wrap_decimal_to_varchar`, and call it from `guard_like_subject`. Pure refactor: no behavior change, `like_subject_type_guard`'s existing tests must pass untouched.
   2. Add `coerce_string_position_arg(arg: &Json, col_types: &[(String, String)]) -> Option<Json>`. A non-`column` node returns `Some(arg.clone())` unchanged. A `column` node's `name` is uppercased and looked up in `col_types`, mirroring `guard_like_subject`: `VARCHAR…`/`CHAR…` → unchanged; `DATE` → `wrap_cast_to_varchar`; `starts_with("DECIMAL")` → `wrap_decimal_to_varchar`; any other resolvable type, a lookup miss, or a missing `name` → `None`.
   3. Add `pub(super) fn string_function_arg_type_guard(node: &Json, col_types: &[(String, String)]) -> Option<Json>`. Non-object → `Some(clone)`. Post-order: recurse every child of `expressions`/`arguments`/`results` and every object-valued `expression`/`pattern`/`left`/`right`/`basis`, propagating a child `None` with `?`. Then, if the node is a `function_scalar`, match `string_position_args(name, arg_count)`: `NotGoverned` → return the node unchanged; `Coerce(indices)` → replace each listed argument with `coerce_string_position_arg(...)?`; `Decline` → return `None`. Return the node. Document the decline-propagation and post-order rationale the way `rewrite_decimal_stringifications` does. [expert]
   4. Extend the existing `decimal_rewrite_col_types()` test fixture — currently only `C_DECIMAL_A`/`ID`/`NAME`/`D` — with a `DOUBLE PRECISION`, a `BOOLEAN`, and a `TIMESTAMP` column, or add a sibling fixture, so the decline branch is testable. Adding entries must not change any existing assertion, since every existing test references only the four current names.
   5. Unit-test the guard directly, one test per spec scenario:
      - Coerce branches — VARCHAR passthrough; DECIMAL wrap for `UPPER`/`TRIM`/`LTRIM`, including an integer `DECIMAL(p,0)`; DATE `CAST` wrap for `LOWER`.
      - Decline branches — BOOLEAN, DOUBLE, and TIMESTAMP; an unresolved column name; a nameless `column` node; a nested decline propagating to the root.
      - Index-table behavior through the guard — `SUBSTR`/`REPEAT`/`LEFT`/`RIGHT`/`LPAD` leave numeric-position arguments untouched; `INSTR` and `LOCATE` with 2 arguments coerce indices 0 and 1; `INSTR` with 3 or 4 arguments and `LOCATE` with 3 arguments return `None` even when every argument is VARCHAR; `CHR`/`UNICODECHR` untouched but still recursed.
      - Non-bare-column and nesting — a case-mismatched column name resolves; a literal and a computed `c_decimal_a * 2` argument are left unchanged without declining; `UPPER(TRIM(c_decimal_a))` coerces the inner `TRIM`.

3. **Wire into the WHERE-clause filter chain**
   1. In `crates/lakehouse-engine/src/adapter/pushdown/mod.rs`, insert `.and_then(|f| string_function_arg_type_guard(&f, &col_types))` into `handle_pushdown`'s filter chain between `like_subject_type_guard` and `rewrite_decimal_stringifications`, add the import, and extend the chain comment to state why the new guard precedes the decimal rewriter. The chain still feeds only the DataFusion-bound filter; `filter_json_raw` stays untouched for Iceberg pruning.
   2. Update the two existing chain-reproducing tests in `mod.rs` — `where_filter_decimal_stringification_rewritten_to_trim` and `filter_decimal_comparison_not_rewritten` — to reproduce the new four-stage chain. Both assertions must still hold: the first proves exactly one trim wrapper survives the composition, the second proves `c_decimal_a > 5` still renders `("C_DECIMAL_A" > 5)`.
   3. Add `mod.rs` tests: `UPPER(c_decimal_a) = 'X'` (a `predicate_equal`, unreachable by `like_subject_type_guard`'s recursion) renders the trimmed form through the full chain; `UPPER(c_double) = 'X'` yields `None` so no filter is pushed; `UPPER(c_decimal_a) LIKE '1%'` is coerced even though `like_subject_type_guard` leaves the non-bare-column subject alone.

4. **Wire into the select-list projection (single-table AND broadcast join)**
   1. In `project_columns` (`support.rs`), call `string_function_arg_type_guard` on each select-list item before the existing `rewrite_decimal_stringifications` call. On `None`, set `needs_full_fallback = true` and `continue` to the next item; on `Some`, feed the rewritten node into the existing decimal rewriter and dispatch unchanged. Extend the existing per-item comment, and state in it that `project_columns` has three callers — `extract_projection` (single table), `extract_join_projection` (`joins/rendering.rs`, union of both joined tables' columns), and `joins/mod.rs`'s empty-side path — so the guard and its decline reach the broadcast-join SELECT list too.
   2. Confirm the four existing wired `project_columns` tests still render byte-identical SQL: `stringify_nondecimal_column_unchanged` and `stringify_computed_decimal_arg_untouched` pass through `function_scalar_cast`/`MULT`, neither of which is a governed string function, and the nested-`CONCAT` and `LENGTH` wired tests reference only DECIMAL columns, which the new guard wraps with the same node the decimal rewriter used to add. Confirm the two existing `extract_join_projection` tests in `joins/rendering.rs` still pass unchanged for the same reason.
   3. Add `project_columns` tests: `UPPER(c_decimal_a)` projects to a single `ProjectionItem::Expr` carrying the trim, at the item's declared `selectListDataTypes` type, and NOT the full base row; `LOWER(c_date)` projects a single `Expr` containing `CAST("C_DATE" AS VARCHAR)`; `UPPER(c_double)` degrades to the full base row with no error; `INSTR(c_decimal_a, '.')` projects a single `Expr` whose first `strpos` argument is trimmed and whose second is the untouched literal; `INSTR(c_varchar, 'b', 3)` degrades to the full base row rather than projecting a truncated `strpos` (#228).
   4. Add one `extract_join_projection` test in `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs`, beside the two existing ones: a two-table join whose SELECT list carries `UPPER(<decimal column of one side>)` projects a single `ProjectionItem::Expr` carrying the trim, and a SELECT list carrying `UPPER(<double column>)` degrades to the full projection over the UNION of both tables' columns, with no error. This is the disclosure the CHANGED-behavior surface needs; #211's rewriter could not decline, so its no-join-test precedent does not carry over.

5. **E2E repro coverage**
   1. Add tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` over `vs_typed_table()` (`typed_distinct_probe`: `c_varchar`, `c_decimal_a` `DECIMAL(9,2)`, `c_date`, `c_double`, `c_ts`, `c_bool`), mirroring issue #210's repro rows: `UPPER(c_varchar)` still pushes and returns the uppercased string; `UPPER(id)` and `LTRIM(c_decimal_a)` return the Exasol-trimmed text, reusing the existing `exasol_trim_decimal_string` oracle; `LOWER(c_date)` returns `YYYY-MM-DD`; `INSTR(c_decimal_a, '.')` returns the position within the trimmed text. Each must have hard-failed with `F-UDF-CL-RUST-9001` before this change.
   2. Add the type-decline E2E: `UPPER(c_double)` over the virtual table must succeed and return the SAME text as the identical expression over a plain Exasol literal of that row's value evaluated in the same session (`SELECT UPPER(CAST(<value> AS DOUBLE))`, no virtual schema) — an in-session native oracle that is not a tautology, since a broken decline either hard-fails or returns DataFusion's divergent text. Probe `c_ts` and `c_bool` the same way; if the live container rejects Exasol's own implicit conversion for either, drop that case and record why rather than asserting blind.
   3. Add the arity-decline E2E, both surfaces, against the same in-session native oracle: `SELECT INSTR(c_varchar, 'b', 3) FROM <vs>.typed_distinct_probe WHERE id = <row>` must equal `SELECT INSTR('<that row's c_varchar>', 'b', 3)` with no virtual schema, and `SELECT c_varchar FROM <vs>.typed_distinct_probe WHERE INSTR(c_varchar, 'b', 3) = <expected>` must return that row. Choose the search substring and start position from the existing fixture rows' `c_varchar` values so that the faithful and the truncated renderings return DIFFERENT numbers — otherwise the test passes on a regressed decline. If no existing row admits such a pair, add one row to the fixture. This case returned a wrong answer before this change rather than hard-failing (#228).

6. **Verification** — run the checklist below and capture the five repro rows end to end.

Both tracked exceptions this plan names already have filed issues, so no issue-filing task remains:

| Issue | Tracked exception | Cited in |
|-------|-------------------|----------|
| [#227](https://github.com/exasol-labs/lakehouse-engine-rs/issues/227) | Grouped-aggregate render path (group keys AND select items) and the aggregate-argument path are unguarded | `pushdown-planning-string-fn-type-coercion/spec.md` out-of-scope list |
| [#228](https://github.com/exasol-labs/lakehouse-engine-rs/issues/228) | `INSTR`'s optional 3rd/4th and `LOCATE`'s optional 3rd argument are dropped by `vs-expression`; this plan declines those calls rather than rendering them | same, plus the INSTR/LOCATE scenario |

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 |
| Group B | 2.1, 2.2, 2.3, 2.4, 2.5 |
| Group C | 3.1, 3.2, 3.3 |
| Group D | 4.1, 4.2, 4.3, 4.4 |
| Group E | 5.1, 5.2, 5.3 |
| Group F | 6 |

Sequential dependencies: A → B (the guard calls the argument table) → C and D, which run concurrently against two different call sites in two different files. E depends on C and D, since only both wired surfaces together make the repro rows pass. F runs last.

## Dead Code Removal

No code is removed. Two adjustments to existing code:

| Type | Location | Reason |
|------|----------|--------|
| Inline literal | `guard_like_subject`, `support.rs` | The DATE `CAST`-to-VARCHAR `json!` literal moves into the shared `wrap_cast_to_varchar` helper (task 2.1); behavior unchanged |
| Test | `where_filter_decimal_stringification_rewritten_to_trim`, `filter_decimal_comparison_not_rewritten`, `mod.rs` | Both reproduce `handle_pushdown`'s filter chain inline and must gain the new stage (task 3.2); both assertions stay as they are |

`rewrite_decimal_stringifications`'s `CONCAT`/`LENGTH` arms are deliberately kept, not removed — see the Consequences table.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A string-position VARCHAR or CHAR column argument pushes down unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_leaves_varchar_argument_unchanged` |
| A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_wraps_decimal_argument_in_trim` |
| A string-position DATE column argument is wrapped in an explicit CAST to VARCHAR | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_casts_date_argument_to_varchar` |
| A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `where_filter_string_fn_over_double_declines` |
| A non-coercible resolvable column type in a select-list string function falls back to the full base row | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_string_fn_over_double_falls_back_to_full_row` |
| … and its broadcast-join AND clause: the decline reached through `extract_join_projection` falls back over the union of both joined tables' columns | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `join_projection_string_fn_coerces_decimal_and_declines_unrenderable_arity` |
| A string-position argument whose column name does not resolve declines fail-safe | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_declines_unresolved_column_name` |
| Only string-position argument indices are coerced | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_position_args_excludes_numeric_arguments` |
| INSTR and LOCATE coerce their first two arguments and decline beyond two | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_coerces_both_instr_and_locate_arguments` (2-argument coercion) and `string_position_args_declines_instr_locate_beyond_two_args` (arity decline, task 1.3) |
| … and its select-list AND clause: a >2-argument INSTR over an all-VARCHAR argument list falls back instead of projecting a truncated `strpos` | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `selectlist_instr_with_start_position_falls_back_to_full_row` |
| CHR and UNICODECHR are excluded from the guard | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_excludes_chr_and_unicodechr` |
| A non-bare-column string-position argument is left unchanged as a tracked exception | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `string_fn_guard_leaves_computed_argument_unchanged` |
| The guard composes with the LIKE type guard and the decimal-stringification rewriter without double coercion | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `where_filter_decimal_stringification_rewritten_to_trim` |
| CAST, CONCAT, or LENGTH over a non-DECIMAL column is left unchanged (CHANGED, #211) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `rewrite_non_decimal_argument_unchanged` and `stringify_nondecimal_column_unchanged` (both existing; assert `rewrite_decimal_stringifications` in isolation, so both stay green) |

Every scenario is pure JSON-tree-to-JSON-tree or JSON-to-SQL-string computation with no I/O, so unit tests against the guard and its call sites — the filter chain, the single-table projection, and the join projection — are the correct proof form. The end-to-end proof that the hard scan error is gone is the E2E set below, which runs against a live Exasol container and a live Iceberg catalog.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-string-fn-type-coercion | `cargo test -p lakehouse-engine --lib adapter::pushdown` | 0 failures; the new `string_fn_guard_*` and `string_position_args_*` tests pass |
| vs-adapter/pushdown-planning-string-fn-type-coercion | `make test-e2e` then `exapump sql "SELECT UPPER(id), LTRIM(c_decimal_a), LOWER(c_date), INSTR(c_decimal_a,'.') FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE id IN (1,4,6) ORDER BY id"` | Rows return; no `F-UDF-CL-RUST-9001 … requires String, but received …`; `LTRIM(c_decimal_a)` shows `10.5`/`30`/`40.99`, not `10.50`/`30.00` |
| vs-adapter/pushdown-planning-string-fn-type-coercion | `exapump sql "SELECT UPPER(c_double) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE id = 1"` | Row returns (native fallback, filter/projection declined); text matches `SELECT UPPER(CAST(<row 1 c_double> AS DOUBLE))` run without the virtual schema |
| vs-adapter/pushdown-planning-string-fn-type-coercion | `exapump sql "SELECT INSTR(c_varchar, 'b', 3) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE id = 1"` | Matches `SELECT INSTR('<row 1 c_varchar>', 'b', 3)` run without the virtual schema — the >2-argument form declines instead of pushing a truncated `strpos` (#228) |
| vs-adapter/pushdown-planning-decimal-string-format | `cargo test -p lakehouse-engine --lib adapter::pushdown::support::tests::rewrite` | 0 failures; #211's existing rewriter tests pass unchanged |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Spec validate | `speq plan validate fix-210-string-functions-type-blind` | Pass |
