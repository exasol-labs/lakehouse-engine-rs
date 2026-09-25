# Feature: Pushdown Module Dedup Consolidation

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/vs-adapter/pushdown-module-dedup-consolidation/spec.md`.

<!-- DELTA:CHANGED -->
## Background

* This delta amends one clause set of the shared-classifier scenario: the classifier now
  resolves the grouped HAVING's merge-rendering as part of the routing decision, and returns
  the rendered fragment instead of the raw `having` node. Every other module-structure
  scenario is unchanged.
* The reason the rendering moves into the classifier is a routing reason, not a rendering
  one: whether the HAVING can be rewritten over the `PARTIAL_*` merge columns decides WHICH
  shape is available (partial/merge grouped, or the qualified single-table wrapper), so the
  decision cannot be deferred to a path that has already committed to one shape. See
  `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` for the fallback behavior this enables
  (issue #195).
* Each path still renders its own SQL: the non-empty dispatch path splices the classifier's
  rendered HAVING fragment into the outer merge wrapper without re-rendering it, and the
  empty path ignores it because a zero-row result satisfies any HAVING.
* One rendering-level decline remains in the dispatcher — a grouped ORDER BY whose sort key
  resolves to no grouped output column — because it does not change the reachable shape set.
* This delta also removes the classifier's LAST grouped-tier hard error, the non-numeric
  aggregate column type carrying a HAVING. It rested on the same disproven premise: the
  qualified single-table wrapper renders the HAVING natively, so nothing is dropped. Both
  grouped declines — gate failure and unmergeable HAVING — now share the one fall-through exit
  to the wrapper, so the grouped tier returns `Ok` for every input.
* This delta adds ONE scenario, the blind column-collecting traversal primitive (issue #177). Every existing scenario of this feature is unchanged, and no scenario of any `vs-adapter/pushdown-planning*` feature changes, because the extraction moves no decision and alters no generated SQL.
* `pub(super)` on an item of `adapter::pushdown::support` makes it visible in `adapter::pushdown` AND every descendant of `adapter::pushdown`, including `adapter::pushdown::joins::rendering`. The primitive therefore reaches both joins-side walks at the visibility ceiling the `vs-adapter/pushdown-joins-module-structure` feature already imposes, so no `use` path and no visibility widens on the joins side.
* The joins-side verification asset already exists and is already scoped to this change: `vs-adapter/pushdown-joins-module-structure`'s "Generated join SQL is byte-identical across the split" scenario captures a golden-SQL baseline over "any duplication extraction" across the broadcast, N-scan-fallback, grouped-qualified-fallback, and ineligible-decline paths, and the in-code gate carries the instruction to re-run after every dedup extraction. This delta consumes that baseline rather than restating it, so `vs-adapter/pushdown-joins-module-structure` needs no delta.
* The two case-folding calls this codebase uses are NOT interchangeable. `str::to_uppercase` applies full Unicode case mapping; `str::to_ascii_uppercase` leaves every non-ASCII byte alone. The three walks disagree today (`collect_all_column_names` uses the Unicode form, both joins walks use the ASCII form), and reconciling that disagreement is a behavior change outside this feature's scope.
* A SECOND traversal primitive, `rewrite_expr_tree` in `pushdown/support.rs`, walks a curated field set post-order for the type-aware walks (issue #257). The two primitives stay separate because a rewrite MUST NOT descend into and rebuild `dataType` or `name` sub-objects, whereas a collect is read-only and so must traverse every field.
* The blind collect primitive backs column-collecting walks only. The rewrite-shaped walks `annotate_columns_with_alias` and `strip_table_alias` have their own recursion, and the `support` type-aware walks run on `rewrite_expr_tree`. None of them is a column-collecting traversal.
* The type-aware walks are `like_subject_type_guard` (`vs-adapter/pushdown-planning-like-type-coercion`), which rewraps a DATE `LIKE` subject or declines, and the string-conversion decline check `string_conversion_declined` (`vs-adapter/pushdown-planning-string-fn-type-coercion`), which only declines. The decline check uses the primitive for its reach and its decline propagation, and discards the tree the primitive returns.
* The child-field list is curated on purpose: the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis`. A walk descends into expression children only and never into a node's `dataType` or `name` sub-objects.
* The primitive is a free function taking a per-node closure, not a visitor trait or a typed AST. The pushdown IR is deliberately untyped `serde_json`, and a typed AST would contradict `vs-expression`'s no-SQL-parser property.
* `apply_type_rewrites` owns the pass order for every DataFusion-bound expression tree: `like_subject_type_guard`, then the decline check. No pass wraps a string-converted argument, because the renderer owns the `exa_to_varchar` conversion (`sql-comprehension/vs-expression-translator-string-conversion`), so the order carries no double-conversion hazard. `vs-adapter/pushdown-planning-string-fn-type-coercion-composition` verifies the composition.
* One pipeline serves callers whose decline meanings differ, because it names none of them: the single-table WHERE caller self-applies the filter, `project_columns` widens the projection to the full base row, and the join callers forfeit the broadcast plan or make a conjunct residual.
* Both passes are private to `support`, so `apply_type_rewrites` is their only entry point outside it and the pass order is compiler-enforced rather than conventional. The pipeline is `pub(super)`, the narrowest visibility that compiles.
* Issue #181's duplication-reduction pass changes the signature of ONE item this feature pins normatively. The joins-side column-tables walk stops writing three `&mut` accumulator out-parameters and returns the same three values instead: `collect_column_tables(expr, &mut tables, &mut has_untagged, &mut any_column)` becomes `column_tables(expr) -> (HashSet<String>, bool, bool)`. Only the scenario clause that pinned that signature changes. The walk still runs on `walk_column_nodes`, still attributes by `tableName`, still folds ASCII-only, and still returns the same three values for the same input.
* The case-folding clause of the "One blind traversal primitive backs every column-collecting walk" scenario is UNCHANGED and still binding, and so is the §Background bullet recording that `str::to_uppercase` and `str::to_ascii_uppercase` are NOT interchangeable. Issue #181 is gated on PRESERVING that divergence, not on relaxing it: it adds the non-ASCII characterisation test this feature's scenario left unwritten, so the forbidden reconciliation would now fail a test rather than pass the whole suite silently.
* No visibility changes on either side. The renamed walk keeps `pub(super)` in `joins/rendering.rs` and `collect_side_column_names` keeps its private visibility, so no item's visibility widens. The restated clause's "no join-module `use` path changes" consequence scopes to declaring the primitive in `support`; issue #181 does edit the `use super::rendering::{…}` list inside `joins` (tasks 3.2, 4.4, 6.4), which widens no path and adds no cross-module reach.
* The generated-SQL gate of the "One blind traversal primitive backs every column-collecting walk" scenario stays the proof for this delta, narrowed to permit the `storage` value alone: the four join golden-SQL full-string assertions, the two `dispatch_golden` decline-wrapper assertions, and the declined-`ORDER BY` hidden-column assertions MUST all pass with no edit to any assertion or expected value outside that value, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant. The collected table set drives side-local versus cross-side conjunct partitioning and is therefore visible in every other byte of the generated SQL.
* This delta carves the scan spec's `storage` value out of FOUR behavior-preservation gates of this feature: the generated-SQL gate bullet above, and one clause in each of these scenarios: "One blind traversal primitive backs every column-collecting walk", "The dispatcher builds each fan-out spec from one shared shard-invariant base", and "Both qualified single-table fallback guards call one shared helper". It supersedes no other Background bullet and changes no structural rule.
* `vs-adapter/storage-backend-enum` (issue #274) wraps the scan spec's `storage` value in an externally-tagged backend variant. That value is embedded in the scan-driving SQL, so the two `dispatch_golden` decline-wrapper goldens (`group_by_fallback.sql`, `multi_count_distinct_decline.sql`) and three of the four join golden-SQL full-string assertions change by exactly one substring each. Those two goldens are the very output the "Both qualified single-table fallback guards call one shared helper" scenario gates, and `storage` is one of the shard-invariant fields the "one shared shard-invariant base" scenario's GIVEN enumerates, which is why both need the carve-out.
* The carve-out is narrow and directional: it permits an edit to the `storage` value ALONE. Every other byte of every golden, and every non-golden assertion, stays unedited — that unchanged remainder is what keeps this feature's gate falsifiable rather than retiring it.
* No column-collecting walk, no type-rewrite pass, no pipeline order, and no visibility rule changes. `vs-adapter/storage-backend-enum` edits no file under `pushdown/` except the golden fixtures and the golden-string literals.
* **This delta is issue #135. It amends THREE scenarios — the `storage` clause and the byte-identity clause of each — and changes no consolidation rule.** The shared shard-invariant base, the two fallback guards, the request-shape classifier, the blind traversal primitive, the type-aware walks, and the ordered pipeline are all UNCHANGED.
* **SUPERSEDES the recorded shared-base clause for `storage` and the byte-identity clause that followed it.** The shared base still carries the storage value and no construction site re-derives it; what it carries is now the tagged wrapper of `vs-adapter/scan-spec-credential-reference` rather than a bare backend, and the wrapper's reference variant carries no credential payload at all.
* **WIDENS this feature's other two `storage` carve-outs from the VARIANT TAG to the whole `storage` VALUE, for the same reason.** The shared-fallback-guard scenario's golden clause names `group_by_fallback.sql` and `multi_count_distinct_decline.sql` — both in this plan's eighteen-fixture regeneration set — and the classifier scenario's clause carves out the scan-driving SQL's tag alone. A tag-shaped exception was exact under `vs-adapter/storage-backend-enum` (issue #274), which changed only what enclosed the backend's encoding; it is too narrow once the value itself is a reference or a sealed envelope. Everything OUTSIDE that value stays byte-identical in both scenarios — including each golden's narrowed inner-scan `projection`, which is the remainder that distinguishes an accepted wire re-encoding from a lost shared-helper call.
* **The classifier scenario's empty-result half is NOT widened, deliberately.** An empty-result response carries no scan spec at all, so it stays byte-identical with NO exception — the same reason the six `empty_*` goldens are asserted unchanged rather than regenerated. Keeping that half unexcepted is what keeps the two-path agreement falsifiable.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The three type-rewrite guards walk the expression tree through one shared post-order primitive

* *GIVEN* the three type-aware expression rewriters in `pushdown/support.rs` — the LIKE subject guard, the string-function argument guard, and the decimal-stringification rewriter — each previously hand-rolling its own post-order recursion, two of them over a child-field list duplicated verbatim between them
* *WHEN* the adapter rewrites a filter tree or a select-list expression tree through any of the three
* *THEN* all three SHALL recurse through ONE shared post-order primitive that rewrites every curated child FIRST and only then applies that guard's own per-node decision, so each guard contributes a per-node closure and no traversal code of its own
* *AND* the curated child-bearing field set — the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis` — SHALL be declared in exactly one place, so extending it for a future node type is a one-line change that reaches all three guards at once, and the primitive SHALL NOT descend into a node's non-expression sub-objects (`dataType`, `name`), because the set is curated to rewrite expression children only and never to rebuild type or identifier metadata
* *AND* the primitive SHALL propagate a per-node decline from any depth to its top-level caller — so a fallible guard keeps its all-or-nothing decline contract and an infallible guard composes as the never-declining case without gaining a decline path — and SHALL apply a guard's per-node decision to a non-object leaf node, which is behavior-preserving because every guard's per-node decision returns a node carrying no `type` it governs unchanged, the property the leaf pass-through test of each guard that previously early-returned on a non-object pins, the LIKE guard having always applied its dispatch to leaves
* *AND* the scan-driving SQL generated for every request whose per-node decisions the extraction itself leaves unchanged SHALL be byte-identical to its pre-extraction output — proven by the existing JSON-shape corpus for the two migrated walkers and the replicated wired-chain rendered-SQL tests passing with no assertion edit — with the ONE deliberate exception being the widened-reach scenarios of `vs-adapter/pushdown-planning-like-type-coercion`, which arrive in a separate commit that changes a per-node reach rather than the traversal, and are covered by that feature's own scenarios: byte-identity here scopes to the extraction, NOT to this plan's end state
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The type-aware tree walks share one post-order primitive

* *GIVEN* the type-aware expression-tree walks in `pushdown/support.rs`: the LIKE subject guard, which rewrites a DATE subject or declines, and the string-conversion decline check, which only declines
* *WHEN* the adapter screens a filter tree or a select-list expression tree
* *THEN* both walks SHALL recurse through ONE shared post-order primitive that visits every curated child first and then applies the walk's per-node decision, so neither walk carries traversal code of its own
* *AND* the curated child-bearing field set SHALL be declared in exactly one place, and the primitive SHALL NOT descend into a node's `dataType` or `name` sub-objects
* *AND* the primitive SHALL propagate a per-node decline from any depth to its top-level caller, which is how both walks decline the whole tree
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: One ordered pipeline function owns the type-rewrite pass order

* *GIVEN* the type-rewrite passes every DataFusion-bound expression tree runs before rendering: the LIKE-subject guard and the string-conversion decline check
* *WHEN* the adapter screens a single-table or join WHERE-clause filter tree, or a select-list item in `project_columns`, including the item reached through the broadcast join
* *THEN* exactly ONE function in `support`, `apply_type_rewrites`, SHALL own the pass sequence — the LIKE-subject guard, then the decline check — and every caller SHALL call it instead of sequencing the passes itself
* *AND* its doc comment SHALL state that order and that no pass wraps a string-converted argument, because the renderer owns that conversion
* *AND* it SHALL take the expression tree and the column-type list and return `Option<Json>`, `None` meaning a pass declined, and it SHALL NOT name what a caller does with a decline nor absorb the caller's "is there a tree at all" question
* *AND* both passes SHALL be private to `support`, so the pipeline is their only entry point outside it
<!-- /DELTA:CHANGED -->
