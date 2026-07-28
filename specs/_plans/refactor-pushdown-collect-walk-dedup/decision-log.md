# Decision Log: refactor-pushdown-collect-walk-dedup

## Interview

**Q:** Scope — include the `resolve_df_*` sliver the issue blesses, or keep the plan to the two duplications named in the issue title?
**A:** Include it. The plan covers all three work items: the blind collect walker, the `nonempty_str` hoist, and folding `resolve_df_target_partitions` + `resolve_df_threads_per_udf` into one parameterized helper. Closes #177 fully as written.

**Q:** Shape of the extracted collect primitive — the issue's literal `walk_json(expr, &mut impl FnMut(&Map))` over every Object node, a widest-form `FnMut(&Json)`, or something narrower?
**A:** `walk_column_nodes` — visits `column` nodes only. The primitive owns BOTH the traversal AND the `type == "column"` check, because all three current collectors act only on `column` nodes. Each collector reduces to a 2–3 line closure over the column node's field map. Approved signature:

```rust
fn walk_column_nodes(expr: &Json, f: &mut impl FnMut(&Map<String, Json>))

// e.g. collect_all_column_names becomes:
walk_column_nodes(value, &mut |m| {
    if let Some(n) = m.get("name").and_then(|n| n.as_str()) {
        names.insert(n.to_uppercase());
    }
});
```

This narrows the reuse surface relative to the issue's suggestion. The trade was made deliberately and is the smaller total diff.

**Q:** How do `str_prop` and `str_field` collapse — the issue's "both call it", or one function at both call sites?
**A:** One function, both call sites use it directly. A single `nonempty_str` lives private in `adapter/mod.rs`; child modules see a parent module's private items, so `adapter/connection.rs` reaches it as `super::nonempty_str` with no visibility widening. Delete `str_field` entirely and delete the `str_prop` name too. All ~26 call sites (15 in `mod.rs`, 11 in `connection.rs`) call `nonempty_str`. The issue's literal wording would leave two one-line pass-through wrappers — the shape `/speq:design-philosophy` flags as a red flag.

## Design Decisions

### [1] `walk_column_nodes` visits `column` nodes only, narrowing the issue's suggested `walk_json`

- **Decision:** Extract `pub(super) fn walk_column_nodes(expr: &Json, f: &mut impl FnMut(&Map<String, Json>))` in `adapter/pushdown/support.rs`. The primitive owns the recursion AND the `type == "column"` test; the callback receives the `column` node's field map.
- **Alternatives:** (a) Issue #177's literal `walk_json(expr, &mut impl FnMut(&Map))`, invoked for every Object node — rejected because it pushes the `type == "column"` test back into all three closures, replacing one duplication with a smaller one three times over. (b) A widest-form `FnMut(&Json)` seeing arrays and scalars too — rejected because no current caller needs a non-column node, so every closure would immediately re-narrow.
- **Rationale:** All three current collectors act only on `column` nodes, so the node-type test belongs in the primitive. `walk_column_nodes` is the deeper module by the `/speq:design-philosophy` test: one function plus a one-argument closure hides both the recursion and the node-type test, and every caller's remaining body is 2–4 lines. This is an intentional departure from the issue's suggested name and shape.
- **Promotes to ADR:** yes

### [2] Fold by deleting the wrapper, not by leaving a pass-through

- **Decision:** `str_prop`, `str_field`, `resolve_df_target_partitions`, and `resolve_df_threads_per_udf` are all deleted. Their ~40 call sites — production and test — call `nonempty_str` and `resolve_df_fixed_count` directly. `nonempty_str` stays private to `adapter`; `connection.rs` names it as `super::nonempty_str`.
- **Alternatives:** Issue #177's literal "`str_prop` / `str_field` both call it", and the same shape for the resolver pair. Rejected: a function whose entire body is a call to another function with the same arguments is the pass-through red flag from `/speq:design-philosophy`, so that shape would trade two duplications for two shallow layers.
- **Rationale:** The call-site edits are mechanical and buy the deletion of four names. No visibility widens: a child module can name a private item of its parent, so hoisting the accessor to `adapter/mod.rs` keeps it private to `adapter`. For the resolver pair, every test function name and every asserted expected value stays unchanged — only the callee spelling and the added key argument change across all ten affected test functions. Two of the ten round-trip through `build_adapter_notes` and so characterize `vs-adapter/create-virtual-schema-adapter-notes-resources`: `df_target_partitions_uses_supplied_value` (`mod.rs:1829`) and `df_threads_per_udf_uses_supplied_value` (`:1935`).
- **Promotes to ADR:** yes

### [3] The collect primitive stays separate from issue #257's rewrite primitive

- **Decision:** This plan introduces no traversal shared with issue #257's curated post-order `rewrite_expr_tree`. It changes none of the three `support` type-rewrite guards and neither descoped transform walk (`annotate_columns_with_alias`, `strip_table_alias`), so it neither pre-empts nor blocks #257.
- **Alternatives:** One walker serving both collect and rewrite callers. Rejected.
- **Rationale:** The separation is substantive, not stylistic. A rewrite MUST NOT descend into and rebuild `dataType` or `name` sub-objects, which is why #257 enumerates its child fields; a collect is read-only, so blind traversal over every field is both correct and what makes a column buried in a `CASE` or function call reachable. Both issues state the separation in writing.
- **Promotes to ADR:** yes

### [4] New feature `vs-adapter/adapter-module-structure` owns the two adapter-root folds

- **Decision:** Author a new structural feature for the adapter root holding the `nonempty_str` hoist and the resolver fold. The collect walker goes to the existing `vs-adapter/pushdown-module-structure` as a delta.
- **Alternatives:** (a) Put both folds in `vs-adapter/create-virtual-schema-adapter-notes-resources` — rejected: that feature is behavioral and owns neither the credential-field reader nor the property readers of `refresh-and-set-properties`. (b) Split the accessor requirement across the four behavioral features that consume it — rejected: it would put one structural decision in four places, the exact failure `/speq:design-philosophy` names.
- **Rationale:** The accessor duplication spans `vs-adapter/connection-credentials`, `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-adapter-notes-resources`, and `vs-adapter/refresh-and-set-properties`, so no single behavioral feature can own it without leaking across a boundary. `*-module-structure` is the established home for structural contracts in this library, with two in-tree precedents (`vs-adapter/pushdown-module-structure`, `datafusion-scan/scan-module-structure`), so this reuses a pattern rather than inventing one. The adapter root previously had no structural home at all.
- **Promotes to ADR:** no

### [5] No delta to `vs-adapter/pushdown-joins-module-structure`

- **Decision:** Add no scenario to the joins module-structure feature; cite its two existing binding clauses instead, from the `pushdown-module-structure` delta.
- **Alternatives:** A mirror joins-side scenario naming the two migrated collectors, their golden gate, and the visibility ceiling.
- **Rationale:** Both requirements already bind. Its "Generated join SQL is byte-identical across the split" scenario captures a golden-SQL baseline over "any duplication extraction" across the exact four paths this change touches — broadcast, N-scan fallback, grouped-qualified fallback, ineligible decline — and the in-code gate carries the instruction to re-run after every dedup extraction. Its "joins becomes a nested directory module organized by concern" scenario already caps a cross-submodule helper at `pub(super)`. A mirror scenario would restate two live requirements and create a second place to maintain them.
- **Promotes to ADR:** no

### [6] The `to_uppercase` / `to_ascii_uppercase` divergence is preserved verbatim

- **Decision:** Each closure keeps its predecessor's case-folding call exactly. `collect_all_column_names` keeps the full Unicode `to_uppercase`; both joins walks keep the ASCII-only `to_ascii_uppercase`. Unifying them is out of scope and named as such in both spec deltas.
- **Alternatives:** Unify on one form while the code is open.
- **Rationale:** The two disagree for non-ASCII column and table names, so unifying them is a behavior change, not a cleanup, and issue #177 is a pure refactor. The spec scenario states the prohibition explicitly.
- **Promotes to ADR:** no

### [7] `resolve_s3_max_connections` is not folded; the descoped framework stays descoped

- **Decision:** Fold only the one byte-identical resolver pair. `resolve_s3_max_connections` keeps its own body, gaining only the `nonempty_str` rename in its doc comment. No `prop_parsed<T>` / `note_parsed<T>` framework and no config table over the eleven `resolve_*` readers.
- **Alternatives:** Fold all three resolvers; or build the generic framework issue #177 originally proposed.
- **Rationale:** `resolve_s3_max_connections` derives its fallback from `auto_threads_per_udf` rather than returning `max(nr_of_cores, 1)`, so its body differs; folding it would need a second parameter that re-splits the function at every call. The framework was rejected in issue #177 on 2026-07-28 as DRY over code that was never the pain — individually documented one-liners with differing defaults and validation.
- **Promotes to ADR:** no

### [8] Only the new primitive gets a new test; every other scenario is characterized by existing tests

- **Decision:** Add exactly one test, for `walk_column_nodes`. Every other requirement is verified by existing suites that must pass with no edit to any assertion or expected value, plus two grep checks in Manual Testing that confirm the deletions.
- **Alternatives:** Add direct tests for `nonempty_str` and `resolve_df_fixed_count`.
- **Rationale:** The primitive is new non-trivial logic (blind recursion reaching a nested column), so it leaves one runnable check behind. The two folded helpers are moved one-liners whose contracts are already asserted — the empty-string-to-default path by four existing property tests and the `connection-credentials` suite, the resolver rule by ten existing tests — so new tests would duplicate coverage. The grep checks matter because "exactly one implementation" is a structural claim no `cargo test` run can fail on; without them the requirement would be unfalsifiable.
- **Promotes to ADR:** no

### [9] The Iceberg-spec compliance gate is explicitly evaluated and does not apply

- **Decision:** State in `plan.md` that CLAUDE.md's Iceberg-spec planning gate does not bite here, with the reasoning, rather than omitting the check.
- **Rationale:** The change touches VS-layer pushdown-*planning* JSON traversal and adapter property reading only — no Iceberg metadata read, no file resolution or pruning, no pushdown semantics, no type mapping.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] The `Json::Array` structural gate expected 0 where a correct implementation leaves 2

- **Finding:** `plan.md`'s Manual Testing row and task 5.3 demanded zero `Json::Array` occurrences in `joins/rendering.rs`. The file holds four today: the two collector array arms this plan deletes (`:147`, `:265`), plus `annotate_columns_with_alias`'s rebuild arm (`:79`) and `referenced_side_columns`'s `selectList` match (`:293`), which both MUST survive. A correct implementation therefore leaves 2. As written the gate either failed on correct work or drove the implementer into `annotate_columns_with_alias` — the walk issue #177 descopes, issue #257 owns, and task 4.6 forbids editing. Verified against the file: four occurrences at exactly those lines.
- **Direction change:** The Manual Testing row now expects `2` and names both surviving arms, and states that `0` means out-of-scope code was edited. A second row scopes the real check to the two migrated functions (`grep -A 12 'fn collect_column_tables\|fn collect_side_column_names' … | grep -c 'Json::'` → `0`). Task 5.3 restates the check per function and marks the two surviving occurrences as MUST-survive. The spec delta gained a Background scope bullet recording that two `Json::Array` arms remain after the extraction.
- **Promotes to ADR:** no

### [2] [plan-review] Two named golden anchors for `collect_all_column_names` cannot fail if the primitive is wrong

- **Finding:** `plan.md`'s Scenario Coverage named `empty_group_by_wrapper_matches_golden` as a falsifier for the wrapper-projection path, and the spec delta's final AND named the `dispatch_golden` "grouped-aggregate" assertion. Neither reaches the collector: the first routes through `empty_sql` → `empty_result_sql`, which never calls `referenced_column_projection`, and `grouped_aggregate_matches_golden` takes the partial/merge decomposition path. Both would have produced a verification report claiming coverage the suite does not have. Verified: `empty_sql` at `dispatch_golden.rs:227` calls `empty_result_sql` only.
- **Direction change:** The Scenario Coverage row now names only the two decline wrappers — `group_by_fallback_matches_golden` and `multi_count_distinct_decline_matches_golden`, whose committed goldens carry inner-scan projections narrowed from the four-column fixture universe to `["REGION","NAME"]` and `["NAME","ID"]` — and states why the empty-result and partial/merge goldens are excluded. The spec delta's final AND now names the two decline-wrapper assertions and their narrowed projections instead of the grouped-aggregate assertion.
- **Promotes to ADR:** no
