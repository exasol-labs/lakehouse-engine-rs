# Tasks: fix-absent-table-location-error-consistency

## Phase 2: Implementation (Group A — sequential)
- [x] 2.1 Host unit test for the absent-location rejection, both paths [expert]
- [x] 2.2 Hoist the check above the vended/static split, rewrite doc comment

## Phase 2: Implementation (Group B — parallel with Group A)
- [x] 2.3 Correct two cloud_e2e_test.rs env-var doc strings
- [x] 2.4 Tighten the docs/catalogs.md field-table row

## Phase 2: Implementation (Group C — after A and B)
- [x] 2.5 Verification sweep (regex greps)

## Phase 3: Verification
- [ ] 3.1 Run test suite (cargo test)
- [ ] 3.2 Run linter (cargo clippy --all-targets)
- [ ] 3.3 Run format check (cargo fmt)
- [ ] 3.4 Build (make cross-musl-udf-build)
- [ ] 3.5 E2E (make test-e2e)

## Phase 4: Review Fixes
- [x] 4.1 Name the table and correct the empty-vs-absent shape in the absent-location `UdfError::User` message in file_resolution.rs; add a table-name assertion to the two-path test
- [x] 4.2 Fix the outdated `table_root` comment parenthetical to reflect the hoisted guard's non-empty postcondition
- [x] 4.3 Add a `resolve_file_list` doc-comment paragraph describing the empty-location rejection above the vended/static split
- [x] 4.4 Bind and await the loopback fake's `JoinHandle` in `resolve_file_list_against_locationless_catalog` so its panics surface
- [x] 4.5 Drop the plan-directory parenthetical from the Task 2.1 test-section banner
- [x] 4.6 Rewrite the docs/catalogs.md `warehouse` field-table row to state its meaning instead of a self-contradicting lexical-shape claim
