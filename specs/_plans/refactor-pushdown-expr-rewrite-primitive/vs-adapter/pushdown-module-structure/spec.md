# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

* This delta adds ONE scenario: the three type-aware expression-tree rewriters in `pushdown/support.rs` collapse onto one shared post-order rewrite primitive. Every other module-structure scenario is unchanged.
* The three rewriters are `like_subject_type_guard` (`vs-adapter/pushdown-planning-like-type-coercion`), `string_function_arg_type_guard` (`vs-adapter/pushdown-planning-string-fn-type-coercion`), and `rewrite_decimal_stringifications` (`vs-adapter/pushdown-planning-decimal-string-format`). Each hand-rolled its own post-order recursion over the untyped `serde_json` pushdown expression grammar, and the two wide walkers duplicated the curated child-field list verbatim, kept in sync only by comment. A fourth type-blind fix would have copied it a third time (issue #257).
* The child-field list is curated on purpose: a rewrite descends into expression children only and must never descend into a node's `dataType` or `name` sub-objects and rebuild them.
* The primitive is a free function taking a per-node closure, not a visitor trait or a typed AST. The pushdown IR is deliberately untyped `serde_json`, and a typed AST would contradict `vs-expression`'s no-SQL-parser property.
* The blind, collect-style JSON walk that issue #177 also dedups stays a SEPARATE primitive — it recurses over every map value, which is exactly what the curated list must not do.
* Ordering the three guards into a pass pipeline is NOT part of this scenario. The one production chain site that sequences them (`like` → `string` → `decimal`) keeps its explicit composition and its load-bearing order comment.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The three type-rewrite guards walk the expression tree through one shared post-order primitive

* *GIVEN* the three type-aware expression rewriters in `pushdown/support.rs` — the LIKE subject guard, the string-function argument guard, and the decimal-stringification rewriter — each previously hand-rolling its own post-order recursion, two of them over a child-field list duplicated verbatim between them
* *WHEN* the adapter rewrites a filter tree or a select-list expression tree through any of the three
* *THEN* all three SHALL recurse through ONE shared post-order primitive that rewrites every curated child FIRST and only then applies that guard's own per-node decision, so each guard contributes a per-node closure and no traversal code of its own
* *AND* the curated child-bearing field set — the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis` — SHALL be declared in exactly one place, so extending it for a future node type is a one-line change that reaches all three guards at once, and the primitive SHALL NOT descend into a node's non-expression sub-objects (`dataType`, `name`), because the set is curated to rewrite expression children only and never to rebuild type or identifier metadata
* *AND* the primitive SHALL propagate a per-node decline from any depth to its top-level caller — so a fallible guard keeps its all-or-nothing decline contract and an infallible guard composes as the never-declining case without gaining a decline path — and SHALL apply a guard's per-node decision to a non-object leaf node, which is behavior-preserving because every guard's per-node decision returns a node carrying no `type` it governs unchanged, the property the leaf pass-through test of each guard that previously early-returned on a non-object pins, the LIKE guard having always applied its dispatch to leaves
* *AND* the scan-driving SQL generated for every request whose per-node decisions the extraction itself leaves unchanged SHALL be byte-identical to its pre-extraction output — proven by the existing JSON-shape corpus for the two migrated walkers and the replicated wired-chain rendered-SQL tests passing with no assertion edit — with the ONE deliberate exception being the widened-reach scenarios of `vs-adapter/pushdown-planning-like-type-coercion`, which arrive in a separate commit that changes a per-node reach rather than the traversal, and are covered by that feature's own scenarios: byte-identity here scopes to the extraction, NOT to this plan's end state
<!-- /DELTA:NEW -->
