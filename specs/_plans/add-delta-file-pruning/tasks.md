# Tasks: add-delta-file-pruning

## Phase 2: Implementation (Group A)
- [x] 1.1 Add temporary probe test in `delta_replay_tests.rs` (multi-part-stats + basic_partitioned, hand-built predicates, assert pruned counts 2/2 vs unpruned 5/6; halt+report if unpruned) [expert]
- [x] 1.2 In the same probe, assert counts 5/6 when `StatsOptions::none()` is restored; keep both assertions permanently, drop only scaffolding

## Phase 2: Implementation (Group B)
- [x] 2.1 Create `format/delta_predicate.rs` + `delta_predicate_tests.rs` sibling, declare privately in `format/mod.rs`, failing test for integer equality, implement `to_delta_predicate` for that node kind with `None`-means-no-constraint doc comment

## Phase 2: Implementation (Group C)
- [x] 2.2 Implement `resolve_column` (case-insensitive match, exact field name + `PrimitiveType`, `None` for unknown/non-primitive)
- [x] 2.3 Implement `literal_to_scalar(lit, prim)` over all literal kinds incl. decimal (new vs `literal_to_datum`), routing date/timestamp through `PrimitiveType::parse_scalar` [expert]

## Phase 2: Implementation (Group D)
- [x] 2.4 Implement five comparison node kinds with column-on-either-side flip; `predicate_notequal` returns `None`
- [x] 2.5 Implement `predicate_is_null`/`predicate_is_not_null`/`predicate_not` (via `Predicate::not` free fn)
- [x] 2.6 Implement `fold_and`/`fold_or` (drop-under-AND, forfeit-under-OR semantics); tests assert no literal-false predicate and no junction ctor on empty set [expert]
- [x] 2.7 Implement `predicate_in_constlist` as OR-chain of equalities; empty element set returns `None` before `or_from` [expert]
- [x] 2.8 Implement `predicate_between` as lower+upper bound AND, keeping one bound when the other fails

## Phase 2: Implementation (Group E)
- [x] 2.9 Complete `delta_predicate_tests.rs` full sweep (per node kind, per literal type, per fold rule, plus the named edge-case tests)
- [x] 3.1 Change `DeltaSnapshot::active_files` to take `prune: Option<PredicateRef>`, delete `.with_stats(StatsOptions::none())` + unused import, update 7 existing call sites to `None`, add `replay_fixture_pruned` helper

## Phase 2: Implementation (Group F)
- [x] 3.2 Change `read_delta_log` to take `filter_json: Option<&Json>`, build predicate from it + `snapshot.schema()`, pass to `active_files`
- [x] 3.3 Change `DeltaFormatReader::resolve_scan` to forward `filter_json`; replace its now-false doc comment

## Phase 2: Implementation (Group G)
- [x] 3.4 Add integration tests over vendored fixtures via `replay_fixture_pruned` asserting exact surviving file sets for the 8 listed predicates
- [x] 3.5 Add integration tests for the fail-open contract (statless column, mixed usable/unusable conjunct, boolean-column equality) — each MUST assert success
- [x] 3.6 Add `delta_format_reader_tests.rs` test: pruned `ResolvedScan` has fewer files than unpruned; other fields unchanged
- [x] 3.7 Add byte-identical serialization test for a non-pruning Delta request (no statistic field)

## Phase 2: Implementation (Group H)
- [x] 4.1 Verify empirically whether the kernel maps logical→physical stat path under column mapping (`cdf-column-mapping-name-mode`/`-id-mode`); record observed answer in spec, file+cite follow-up if it degrades [expert]
- [x] 4.2 Verify pruning-to-empty-file-list reaches the adapter's empty-result route (pushdown-level test, `LETTER = 'z'`-shaped filter) [expert]

## Phase 2: Implementation (Group I)
- [x] 5.1 Add plan-time E2E test in `e2e_unity_test.rs` extending `resolve_delta_scan` to accept a filter; assert pruned counts for `basic_partitioned`/`LETTER='a'` and `multi_part_stats`/`ID<=2` against real MinIO-backed storage
- [x] 5.2 Add query E2E test asserting unchanged rows under pruning (partition, range, equality, zero-match, prunable+unprunable mix; double-quote `VALUE`)
- [x] 5.3 Extend one E2E test to capture pushdown SQL via `explain_virtual_sql`, assert it drives the scan UDF and embeds fewer file paths than the active file count

## Phase 3: Code Review
- [x] R.1 Code review of all changed files (guardrail violations, dead code, test quality, bad comments, YAGNI, error handling, design depth) — 12 findings, 12 fixed (2 correctness-critical)

## Phase 4: Verification
- [x] V.1 Build: `make cross-musl-udf-build`
- [x] V.2 Test: `cargo test` (1023 unit/integration passed, 0 failed)
- [x] V.3 E2E (Unity/Delta): `make test-e2e-unity` (23/23 passed)
- [x] V.4 E2E (Iceberg regression): `make test-e2e` (254 passed, 0 failed)
- [x] V.5 Lint: `cargo clippy --all-targets` (0 warnings)
- [x] V.6 Format: `cargo fmt --check` (clean)
- [x] V.7 Scenario coverage audit against plan's Scenario Coverage table (36/36 resolved; 1 real gap found+closed: `each_delta_join_leg_prunes_by_its_own_side_local_predicate`)
- [x] V.8 Manual testing per plan's Manual Testing table (5/5, run live against Docker stack)
- [x] V.9 Generate verification-report.md
