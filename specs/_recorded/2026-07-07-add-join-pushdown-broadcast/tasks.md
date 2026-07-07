# Tasks: add-join-pushdown-broadcast

## Phase 2: Implementation (Group A)
- [x] 1.1 Add JOIN/JOIN_TYPE_INNER/JOIN_CONDITION_EQUI to CAPABILITIES; update capability unit tests
- [x] 2.1 Extend CommonScanSpec/ScanSpec with optional join block; wire serde + reconstitution merge [expert]

## Phase 2: Implementation (Group B)
- [x] 3.1 Detect join `from` clause, recover both Iceberg idents via TABLE_MAP, reject non-inner/non-equi/>2-table
- [x] 3.6 Read JOIN_BROADCAST_MAX_BYTES adapter note (with default), thread through handle_pushdown

## Phase 2: Implementation (Group C)
- [x] 3.2 Resolve both sides' file lists/schemas/byte sizes once; select smaller side vs threshold [expert]
- [x] 3.3 Render join condition/cross-table projection/EMITS/filter via vs-expression; disjoint-column guard [expert]

## Phase 2: Implementation (Group D)
- [x] 3.4 Build broadcast fan-out scan-driving SQL (fact sharded, dimension full file list) [expert]
- [x] 3.5 Build unaccelerated two-scan join fallback SQL; error only as last resort
- [x] 3.5a Fix: two-scan fallback renders TABLE-QUALIFIED (`"LHS_FACT"`/`"LHS_DIM"`) condition/WHERE/select-list independent of the disjoint-column guard, and executes an aggregate/GROUP BY/HAVING/ORDER BY/LIMIT over the join via Exasol; broadcast used only for a plain projection+filter disjoint join within threshold; hard `Err` only when the qualified two-scan cannot be built [expert]
- [x] 4.1 UDF: register fact(shard)+dimension(full) as two tables in one session, aliased sub-SELECTs
- [x] 4.2 Execute inner equi-join with projection/filter/limit; stream via emit_batch; dimension = build side [expert]
- [x] 4.3 Route unreadable-file/deserialization errors through classify_scan_error

## Phase 2: Implementation (Group E)
- [x] 5.1 Capability advertisement test (unit + e2e_capability_test.rs)
- [x] 5.2 Join detection/side-selection/threshold/SQL-shape unit tests (scan_plan_shape.rs) — landed incidentally in Groups B-D
- [x] 5.3 Host DataFusion join-execution tests over local Parquet (scan_join_test.rs) — landed in Group D
- [x] 5.4 E2E broadcast join correctness against local Exasol Docker (e2e_join_test.rs)
- [x] 5.5 E2E above-threshold unaccelerated-fallback correctness (same file)
- [x] 5.6 Fix regression coverage: e2e `SELECT a.id, b.label FROM EVENTS a JOIN LABELS b ON a.id=b.id` (shared `id`, e2e_scan_test.rs `e2e_pushdown_resolves_files_once_multi_table`) now succeeds via qualified two-scan; new e2e aggregate-over-join tests (`e2e_aggregate_over_join_uses_two_scan_wrapper`, `e2e_aggregate_over_join_result_correct`) in e2e_join_test.rs; new host unit tests for qualified two-scan + aggregate routing in pushdown.rs; vs-expression `tableAlias` qualified-column tests

## Phase 3: Verification
- [x] V.1 Build: make cross-musl-udf-build (rebuilt .so in rust:1.94-bookworm, exit 0)
- [x] V.2 Test: cargo test (host: lakehouse-engine 441 passed / 2 ignored; vs-expression 58 passed)
- [x] V.3 E2E: e2e_scan_test + e2e_capability_test + e2e_count_distinct_test + e2e_join_test — 63 passed, 0 failed (incl. e2e_pushdown_resolves_files_once_multi_table shared-`id` regression + new aggregate-over-join tests)
- [x] V.4 Lint: cargo clippy --all-targets — no issues
- [x] V.5 Format: cargo fmt --check — clean
