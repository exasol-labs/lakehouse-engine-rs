# Tasks: change-shard-parallelism

## Phase 2: Implementation (Group A — sharding path)
- [x] 2.1 T1 — Thread file sizes through file resolution (`plan_files_from_table`, `resolve_file_list`, `handle_pushdown`)
- [x] 2.2 T2 — Byte-balanced partitioner `partition_files_by_bytes` in `sharding.rs` [expert]

## Phase 2: Implementation (Group B — adapter notes / factor)
- [x] 2.3 T3 — Capture `NR_OF_CORES` in create-VS connect-back (`resolve_cluster_nodes`)
- [x] 2.4 T4 — Cores-aware default factor `resolve_parallelism_factor`

## Phase 2: Implementation (Group C — DataFusion threading)
- [x] 2.5 T6 — VS adapter: DataFusion thread properties (notes + ScanSpec fields)
- [x] 2.6 T7 — Scan UDF: consume thread config from ScanSpec [expert]

## Phase 2: Implementation (Group D — cleanup / tests)
- [x] 2.7 T5 — Update sharding/notes tests, remove dead `partition_files`
- [x] 2.8 T8 — Threading tests and ScanSpec round-trip

## Phase 3: Verification
- [x] 3.1 Code review of changed files (1 must-fix applied: `t.length`→`t.file_size_in_bytes`; +3 nice-to-fixes)
- [x] 3.2 Build (`make cross-musl-udf-build`) — release `.so` built in rust:1.92-bookworm, exit 0
- [x] 3.3 Host tests (`cargo test`) — 223 passed, 0 failed
- [x] 3.4 Lint (`cargo clippy --all-targets`) + format (`cargo fmt`) — clean
- [x] 3.5 E2E (`make test-e2e`) — 29 passed (7 capability + 22 scan), 0 failed
