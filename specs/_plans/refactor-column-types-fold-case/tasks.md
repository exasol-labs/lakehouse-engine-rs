# Tasks: refactor-column-types-fold-case

## Phase 2: Implementation (Sequential, no parallelism)
- [x] 2.1 Remove `fold_case` parameter from `column_types` and fix both call sites + delete the divergence test (support.rs, joins/planning.rs)
- [x] 2.2 Reword the two remaining stale doc comments in support.rs

## Phase 4: Review Fixes
- [x] 4.1 [expert] Document the cross-fold comparison seam between involved_table_columns and referenced_side_columns
- [x] 4.2 Fix column_exa_type doc's "two lookups" / agreement-premise non-sequitur
- [x] 4.3 Fix walk_column_nodes doc's false claim that no non-ASCII collect-walk test exists
- [x] 4.4 Drop the fold from involved_table_columns' doc comment (fold has one documented home in column_types)

## Phase 3: Verification
- [x] 3.1 Run behavior-preservation gate: cargo test, cargo clippy --workspace --all-targets -- -D warnings, cargo fmt --check
- [x] 3.2 Confirm git diff shows no changed test assertion / expected SQL value anywhere
