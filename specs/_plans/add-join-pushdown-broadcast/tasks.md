# Tasks: add-join-pushdown-broadcast

## Phase 2: Implementation (Group A)
- [x] 1.1 Add JOIN/JOIN_TYPE_INNER/JOIN_CONDITION_EQUI to CAPABILITIES; update capability unit tests
- [x] 2.1 Extend CommonScanSpec/ScanSpec with optional join block; wire serde + reconstitution merge [expert]

## Phase 2: Implementation (Group B)
- [x] 3.1 Detect join `from` clause, recover both Iceberg idents via TABLE_MAP, reject non-inner/non-equi/>2-table
- [x] 3.6 Read JOIN_BROADCAST_MAX_BYTES adapter note (with default), thread through handle_pushdown

## Phase 2: Implementation (Group C)
- [ ] 3.2 Resolve both sides' file lists/schemas/byte sizes once; select smaller side vs threshold [expert]
- [ ] 3.3 Render join condition/cross-table projection/EMITS/filter via vs-expression; disjoint-column guard [expert]

## Phase 2: Implementation (Group D)
- [ ] 3.4 Build broadcast fan-out scan-driving SQL (fact sharded, dimension full file list) [expert]
- [ ] 3.5 Build unaccelerated two-scan join fallback SQL; error only as last resort
- [ ] 4.1 UDF: register fact(shard)+dimension(full) as two tables in one session, aliased sub-SELECTs
- [ ] 4.2 Execute inner equi-join with projection/filter/limit; stream via emit_batch; dimension = build side [expert]
- [ ] 4.3 Route unreadable-file/deserialization errors through classify_scan_error

## Phase 2: Implementation (Group E)
- [ ] 5.1 Capability advertisement test (unit + e2e_capability_test.rs)
- [ ] 5.2 Join detection/side-selection/threshold/SQL-shape unit tests (scan_plan_shape.rs)
- [ ] 5.3 Host DataFusion join-execution tests over local Parquet (scan_join_test.rs)
- [ ] 5.4 E2E broadcast join correctness against local Exasol Docker (e2e_join_test.rs)
- [ ] 5.5 E2E above-threshold unaccelerated-fallback correctness (same file)

## Phase 3: Verification
- [ ] V.1 Build: make cross-musl-udf-build
- [ ] V.2 Test: cargo test
- [ ] V.3 E2E: make test-e2e
- [ ] V.4 Lint: cargo clippy --all-targets
- [ ] V.5 Format: cargo fmt
