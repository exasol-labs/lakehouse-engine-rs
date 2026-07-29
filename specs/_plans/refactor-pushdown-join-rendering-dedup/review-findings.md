# Code Review Findings: refactor-pushdown-join-rendering-dedup

## Summary
- Files reviewed: 4
- Total findings: 4 (standard: 4, expert: 0)

### Verified clean (no findings)

Evidence gathered before writing findings — the plan's four Requirements and the brief's two
highest-risk claims all hold:

- **Byte-identical decline messages — mechanically proven, not asserted.** All six
  `"join pushdown declined: …"` string literals present at `HEAD` in
  `joins/sql_builders.rs` appear verbatim in the working tree (as
  `golden_n_scan_render_decline_messages_unchanged`'s expected strings), plus exactly one new
  literal, the `join_render_decline` template. Continuation-resolved set comparison: 6 shared,
  0 lost, 1 added.
- **Byte-identical SQL — golden bodies unedited.** `golden_broadcast_join_sql_unchanged`,
  `golden_n_scan_join_sql_unchanged`, and `golden_grouped_qualified_fallback_sql_unchanged`
  are byte-for-byte identical to their `HEAD` bodies (brace-matched extraction + string
  compare) and all pass. Same for `golden_ineligible_decline_message_unchanged`.
- **Divergent case folding preserved.** `collect_all_column_names` still folds Unicode
  `to_uppercase` (`support.rs`); `collect_side_column_names` is byte-identical to `HEAD` and
  still folds `to_ascii_uppercase`; `column_tables` folds `to_ascii_uppercase`. Pinned by the
  new `column_collectors_keep_divergent_case_folding` (`ß` → `SS` vs `ß`), which passes.
- **Divergent fallback policies preserved.** `referenced_side_columns` keeps its
  absent/empty-`selectList` short-circuit *before* the shared walk and its full-set
  empty-narrowing fallback; `referenced_column_projection` has neither and keeps its
  first-column fallback. Pinned by `referenced_column_projection_narrows_without_select_list`,
  `referenced_column_projection_falls_back_to_first_column`, and
  `referenced_side_columns_keeps_all_when_narrowing_empty` — all pass.
- **Behavioural equivalences hold.** `walk_column_nodes` descends `Json::Array`, so passing the
  whole `selectList` array is equivalent to the old per-item loop; every collector inserts into
  a `HashSet`, so collecting `condition` before rather than after `selectList` is order-immaterial.
  The let-chain rewrite of the attach point is truth-table identical to the old
  `resolvable.then(…).flatten()` match.
- **Gates.** `cargo test -p lakehouse-engine`: 702 passed, 0 failed. `cargo clippy -p
  lakehouse-engine --all-targets` (forced rebuild): 0 warnings. `cargo fmt --all --check`: clean.
  `cargo doc --document-private-items`: no new unresolved intra-doc link in any changed file
  (the `plan_join` / `side_local_filter` warnings all originate in `joins/planning.rs`, which
  this plan does not touch).
- Zero `render_join_condition` / `render_selectlist_item_qualified` / `collect_column_tables`
  hits under `crates/`; the surviving `specs/` hits are exactly the two the plan authorises,
  and both have authored deltas in this plan directory.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [MISSING_DOC_COMMENT] Two `pub(super)` builders lost their entire doc comments
- Location: line 500 (`build_side_fan_out_sql`), line 545 (`build_broadcast_join_sql`)
- Issue: both functions are now undocumented. `build_side_fan_out_sql` lost 12 lines
  explaining the outer-ungrouped-scalar vs from-less-scalar shape, why `columns` must expose
  every column any outer clause references (the outer Exasol query still applies the full
  `WHERE`), and why `side_filter` is rendered bare-name so DataFusion row-group-prunes before
  emitting. `build_broadcast_join_sql` lost 15 lines explaining that the dimension side rides
  once in the shard-invariant common blob so every shard re-scans it node-locally with no
  cross-shard exchange, plus the one-`StorageProps`-serves-both-tables caveat under
  per-prefix vended STS credentials. Neither deletion is authorised: the plan's
  §Dead Code Removal table lists exactly seven items and neither doc comment is among them,
  §Impact says "None", and no Requirement or spec delta touches these two doc comments. The
  refactor's line-count reduction is partly this deletion — the two changed files grew
  +177 lines net while ~66 lines of doc comment silently disappeared.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, restore the
  doc comments on `build_side_fan_out_sql` and `build_broadcast_join_sql` verbatim from
  `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`
  (the 12-line block above `pub(super) fn build_side_fan_out_sql` and the 15-line block above
  `pub(super) fn build_broadcast_join_sql` at HEAD). Change no word of either block — every
  intra-doc link they contain (`[build_n_scan_join_sql]`, `[referenced_side_columns]`,
  `[side_local_filter]`, `[JoinSpec]`, `[build_scan_driving_sql]`) still resolves. Then run
  `cargo doc -p lakehouse-engine --no-deps --document-private-items` and confirm no new
  unresolved-link warning appears in this file.

#### [MISSING_DESIGN_INTENT] Five private builders lost non-obvious correctness rationale
- Location: line 112 (`build_n_scan_join_from`), line 220 (`n_scan_join_select_items`),
  line 589 (`qualified_join_group_by`), line 612 (`qualified_join_having`),
  line 626 (`qualified_join_order_by`)
- Issue: five more doc comments were deleted with no authorisation from the plan, and four of
  them carried reasoning that is not recoverable from the code. `build_n_scan_join_from` lost
  the whole safety argument for its residual path — that for an inner join a condition in the
  outer `WHERE` is result-equivalent to the same condition in an `ON` clause, that attachment
  is GREEDY to the join point bringing its highest-indexed leg in, that scope is resolved by
  the SET of `tableName`s and NEVER by column name so two legs sharing a column name cannot
  fool it, and that a join point with no attached condition renders `ON 1=1`. Nothing in the
  body states any of that, and it is the file's only record of why the residual backstop is
  sound. `qualified_join_having` lost "dropping it would return wrong rows"; and
  `qualified_join_order_by` lost "dropping it would return an unordered result Exasol
  delegated and no longer re-sorts" — both are the *reason* those sites hard-error instead of
  degrading, which is exactly what a future reader would otherwise be tempted to relax.
  `n_scan_join_select_items` and `qualified_join_group_by` lost their `None`/absent-clause
  contracts. Read against `/speq:code-guardrails`' "private methods: no comments", these are
  private — but the plan is a pure refactor that authorised exactly two doc-comment
  relocations and zero deletions, and `/speq:design-philosophy` treats a non-obvious
  soundness argument as design documentation to preserve, not incidental prose.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, restore the
  deleted doc comments on `build_n_scan_join_from`, `n_scan_join_select_items`,
  `qualified_join_group_by`, `qualified_join_having`, and `qualified_join_order_by` verbatim
  from `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`.
  Make exactly one edit while restoring: in `build_n_scan_join_from`'s block, change the
  intra-doc link `[`collect_column_tables`]` to `[`column_tables`]`, since the renamed
  function is what it now refers to. Then run
  `cargo doc -p lakehouse-engine --no-deps --document-private-items` and confirm no new
  unresolved-link warning appears in this file.

#### [OUTDATED_COMMENT] Golden decline test still describes the pre-refactor code
- Location: line 1955
- Issue: `golden_n_scan_render_decline_messages_unchanged`'s doc comment was written at task 1.3
  against unmodified HEAD and never updated after task 2 landed. It states the six messages are
  "today written out as six separate `UdfError::User` string literals" — there is now exactly
  one, in `join_render_decline` — and frames the test as "the coverage gap the dedup refactor
  (issue #181, finding 3) closes first", describing work that is already done. A reader now
  gets a false picture of the code the test guards.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, rewrite
  `golden_n_scan_render_decline_messages_unchanged`'s doc comment to describe the post-refactor
  state: it pins the full text of all six qualified N-scan render-decline messages now produced
  through the shared `join_render_decline` template, so a future reword of the template or of
  any caller's clause fragment fails here. Keep the existing final sentence about triggering
  each case directly against the producing function with an unrecognised node `type` rather
  than through the full `build_n_scan_join_sql` pipeline. Drop the "today written out as six
  separate `UdfError::User` string literals" clause and the "closes first" framing.

### crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs

#### [MISSING_DESIGN_INTENT] `conjunct_single_side` lost its contract and soundness argument
- Location: line 143
- Issue: the 9-line doc comment above `conjunct_single_side` was deleted. It stated the
  function's whole contract — `Some(UPPERCASE table name)` iff every `column` node is tagged
  with that ONE `tableName`; `None` when the conjunct spans two tables, carries an untagged
  column, or references no column at all (a bare literal), in which case the conjunct is
  withheld from BOTH sides' pruning and only the outer wrapper's `WHERE` applies it — plus the
  soundness argument that a conjunct over one side alone is a necessary condition for that
  side's rows to survive an inner equi-join, so pruning with it can never drop a row the join
  would have kept. The body is three lines of flag checks that state none of this. The plan
  authorised replacing the `collect_column_tables` call inside this function (task 3.2); it did
  not authorise removing its documentation, and the closest surviving statement of the argument
  is in a different module (`joins/planning.rs`) attached to a different function.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs, restore the 9-line
  doc comment above `fn conjunct_single_side` verbatim from
  `git show HEAD:crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` (the block
  beginning "The single side a conjunct is local to" and ending "can never drop a row the join
  would have kept."). Change no word of it — it names no renamed or deleted symbol.

#### [INLINE_COMMENT] Guard comment triples a rationale two doc comments already carry
- Location: lines 298-300
- Issue: the pre-existing one-line inline comment on `referenced_side_columns`' early return
  (`// Absent/empty select list ⇒ the wrapper projects every column (SELECT *).`) grew to three
  lines, the two added lines asserting that the guard "runs BEFORE the shared clause walk and
  deliberately stays out of it: it is this routine's own fallback policy, not part of the clause
  set". `referenced_clause_values`' doc comment (line 260) already devotes a full paragraph to
  precisely this — that `referenced_side_columns` keeps its own short-circuit before the walk,
  that `selectList` is named twice by design, and that the guard MUST NOT be folded in — and
  `referenced_side_columns`' own doc comment already describes both total-safety fallbacks. The
  same statement now lives in three places inside one file, which is the maintenance hazard the
  plan set out to remove, applied to prose. `/speq:code-guardrails` bans inline comments
  outright; the pre-existing single line is grandfathered by the plan's "keep the early return",
  the two new lines are not.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs, trim the inline
  comment above the `if !matches!(pushdown_req.get("selectList"), …)` guard in
  `referenced_side_columns` back to its single pre-existing line,
  `// Absent/empty select list ⇒ the wrapper projects every column (SELECT *).`, deleting the
  two sentences about the guard running before the shared clause walk. Leave
  `referenced_clause_values`' doc comment, which already owns that statement, untouched.

## Expert fixes

[none]
