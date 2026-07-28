# Tasks: refactor-pushdown-collect-walk-dedup

## Phase 2: Implementation (Group A)
- [x] 2.1 Fold the two non-empty-string JSON accessors into `nonempty_str` (plan Task 1)
- [x] 2.2 Extract `walk_column_nodes` and migrate `collect_all_column_names` (plan Task 3)

## Phase 2: Implementation (Group B)
- [x] 2.3 Fold the two DataFusion FIXED-mode count resolvers (plan Task 2)
- [x] 2.4 Migrate the two joins collect walks onto `walk_column_nodes` (plan Task 4) [expert]

## Phase 2: Implementation (Group C)
- [x] 2.5 Run the gates and confirm the structural claims (plan Task 5)

## Phase 3: Verification
- [x] 3.1 Run automated checks (fmt, clippy, test, build)
- [x] 3.2 Scenario coverage audit
- [x] 3.3 Manual verification commands

## Phase 4: Review Fixes
- [x] 4.1 Document `walk_column_nodes`' deliberate case-folding divergence in its doc comment (review-findings `## Expert fixes`, `[MISSING_DESIGN_INTENT]`) [expert]
- [x] 4.2 Rename `nonempty_str`'s first parameter from `props` to `obj` in signature and body (review-findings `## Standard fixes`, `[IMPLEMENTATION_IN_NAME]`)
- [x] 4.3 Add `walk_column_nodes_never_invokes_callback_for_a_non_container_root` test covering non-container roots (review-findings `## Standard fixes`, `[MISSING_BOUNDARY_TEST]`)
