# Tasks: fix-join-decline-hard-fail

## Phase 2: Implementation (Group A — detection)
- [x] 1.1 Walk the nested-join `from` tree collecting every base-table leaf and every join node's condition; assert all-inner; introduce `JoinShape::MultiTable(MultiTableJoin)`. [expert]
- [x] 1.2 Classify boundaries: N==2 stays Eligible/Ineligible; N≥3 all-inner → MultiTable; non-inner node → Ineligible(NotInnerJoinType); malformed leaf → Ineligible(UnsupportedShape); leaf missing from TABLE_MAP → hard Err.

## Phase 2: Implementation (Group B — routing + resolution)
- [x] 2.1 Add `JoinShape::MultiTable(m) => plan_multi_table_join(...)` arm in `handle_pushdown`.
- [x] 3.1 Resolve each of the N sides once via `resolve_one_join_side`, forwarding side-local WHERE conjuncts.
- [x] 3.2 Empty-file-list side → shape-correct empty result over combined N-table column universe. [expert]

## Phase 2: Implementation (Group C — SQL builder)
- [x] 4.1 Build N-entry alias map (`LHS_T0..LHS_T{N-1}`); render every join condition + residual WHERE table-qualified. [expert]
- [x] 4.2 Build N-scan wrapper SQL: per-side fan-out subqueries, qualified select/GROUP BY/HAVING/ORDER BY/LIMIT, cross-join + conjunctive WHERE. [expert]

## Phase 2: Implementation (Group D — host tests + E2E seed)
- [x] 5.1 Unit tests: `detect_join` over 3-table/4-table all-inner trees → MultiTable; non-inner node → Ineligible; missing TABLE_MAP leaf → Err.
- [x] 5.2 Unit tests: `build_n_scan_join_sql` for Q1/Q2/NQ3 shapes → N-scan wrapper, not Err; shared-column-name pair across three tables renders qualified.
- [x] 5.3 Update `join_outside_contract_declined_safely`: TooManyTables no longer asserted as a decline; retire dead `TooManyTables` decline path if `detect_join` stops producing it.
- [x] 6.1 Seed a third/fourth small Iceberg table in the join E2E namespace fixtures.

## Phase 2: Implementation (Group E — E2E behavior)
- [ ] 6.2 Add `e2e_three_table_join_result_correct` and `e2e_four_table_join_result_correct`: assert success (no F-UDF-CL-RUST-9001), N-scan wrapper SQL shape, correct results.

## Phase 3: Verification
- [ ] 7.1 cargo test (host)
- [ ] 7.2 cargo clippy --all-targets
- [ ] 7.3 cargo fmt
- [ ] 7.4 make cross-musl-udf-build
- [ ] 7.5 make test-e2e
- [ ] 7.6 Scenario coverage audit
- [ ] 7.7 Manual testing (EXPLAIN VIRTUAL / live query shapes)
