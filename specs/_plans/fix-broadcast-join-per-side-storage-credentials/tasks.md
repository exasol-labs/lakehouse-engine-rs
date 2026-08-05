# Tasks: fix-broadcast-join-per-side-storage-credentials

## Phase 1: Investigation and reproduction (Group A — GATING, strictly sequential)
- [x] 1.1 Split `seed_star_schema` into auth-taking core + defaulting wrapper (tests/common/seed.rs)
- [x] 1.2 Empirically establish whether vended credentials differ in SCOPE (cross-table read denied?) [expert]
- [x] 1.3 GATE — proceed only if 1.2 shows DENIED; else halt B-F and escalate — PASSED: cross-table read DENIED (403), gate is open
- [x] 1.4 Reproduce the defect against live Docker Exasol — sub-step (a) DONE. Sub-step (b) as originally written is SUPERSEDED by tasks 1.5-1.9 below (see decision-log.md "Task 1.4 follow-up investigation" and "Round 2" [16]): it unexpectedly passed because two compounding, independent defects (harness `resultSetMaxRows` forcing the fallback, and a second alias-qualification defect) kept it from ever reaching the credential defect it targets.

## Phase 1 (Round 2): Alias-qualification fix and gate closing

### Group G (1.5 → 1.6 → 1.7, strictly sequential)
- [x] 1.5 File a GitHub issue for the alias-qualification defect (problem-focused); record the number in place of `#TRACKED-ALIAS-QUALIFICATION` — filed as #303
- [x] 1.6 Fix `render_broadcast_join`: strip `tableAlias` from condition, filter, and projection before rendering; rewrite its doc
- [x] 1.7 Replace `render_broadcast_join_preserves_native_table_alias_unchanged`; add condition + projection alias-stripping tests

### Group A′ (gate-closing) — 1.8 may start in parallel with Group G; 1.9 needs both complete
- [x] 1.8 Add scoped `ExaConn::unbounded_result_sets()` opt-out (tests/common/exasol_ws.rs); rewire `lakekeeper_vended_broadcast_join_result_correct` to use it
- [x] 1.9 GATE re-run of `lakekeeper_vended_broadcast_join_result_correct` — must now fail with a CREDENTIAL error, not an alias/schema error. Only this PASS (a genuine credential-error failure) opens Groups B-F — PASSED: failed with literal `403 Forbidden` (see decision-log.md "Task 1.9 — GATE re-run: PASSED"); Groups B-F unblocked

### Repair (non-gating) — needs Group G + 1.8; independent of 1.9 and of Groups B-F
- [x] 1.10 Repair the 4 vacuous `e2e_join_test.rs` tests (apply `.unbounded_result_sets()`)

## Phase 2: Wire format (Group B) — UNBLOCKED pending 1.9's PASS
- [x] 2.1 Add required `storage: StorageBackend` field to `JoinSpec` (scan/spec.rs)
- [x] 2.2 Supply the field at the 6 remaining construction sites
- [x] 2.3 Set `join.storage = dimension.effective_storage.clone()` in `build_broadcast_join_sql`; rewrite docs

## Phase 3: Routing object store (Group C — new module, sequential) — UNBLOCKED pending 1.9's PASS
- [x] 3.1 Implement `PrefixRoutingObjectStore` / `RoutedSide` structure (scan/store_router.rs) [expert]
- [x] 3.2 Implement `route()` + every `ObjectStore` trait method [expert]
- [x] 3.3 Write store_router.rs `#[cfg(test)] mod tests`

## Phase 4: Wire router into session construction (Group D) — UNBLOCKED pending 1.9's PASS
- [x] 4.1 Rework `build_session_context` (object_store.rs) — per-side grouping, union secrets [expert]
- [x] 4.2 Replace `register_side_store` with `build_side_store` + `side_size_index` [expert]
- [x] 4.3 Pass dimension's own storage to its table registration (join_scan.rs)
- [x] 4.4 Rewrite `validate_sides_share_one_store` doc (no logic change)

## Phase 5: Unit and integration tests for the fix (Group E) — UNBLOCKED pending 1.9's PASS
- [x] 5.1 object_store.rs tests: per-bucket store counts, per-side size index, routing error
- [x] 5.6 Provenance test through `build_session_context` (loopback fakes) [expert]
- [x] 5.7 `dim_storage()` helper + rename/redaction test in scan_join_test.rs
- [x] 5.8 Rename + extend `join_executes_inner_equi` → `join_registers_each_side_against_its_own_backend`
- [x] 5.2 Router-level failing-stub test in scan_join_test.rs
- [x] 5.3 Extend scan_positional_deletes.rs join case (dimension-side delete file)
- [x] 5.4 Apply Test Disposition RESTATE/REPLACE rows to sql_builders.rs/planning.rs/spec.rs tests
- [x] 5.5 Rewrite `validate_sides_share_one_backend` doc (no logic change)

## Phase 6: Tracking (Group F) — UNBLOCKED pending 1.9's PASS
- [x] 6.1 File GitHub issue for pre-existing multi-bucket-per-side refusal; cite in spec delta
- [ ] 6.2 (record-time only) remove superseded Background bullet from pushdown-planning-cloud-credentials spec

## Phase 7: Verification
- [x] 7.1 1.9's reproduction test (`lakekeeper_vended_broadcast_join_result_correct`) re-run — must now PASS — PASSED (see decision-log.md "Task 7.1 — Reproduction re-run: PASSES with the fix in place")
- [x] 7.2 Full checklist: build, cargo test, test-e2e (including the 4 repaired tests from 1.10), test-e2e-lakekeeper, clippy, fmt — ALL GREEN: build exit 0; workspace `cargo test` all green; `make test-e2e` 230 passed/0 failed across 8 binaries (e2e_join_test 25/25); `make test-e2e-lakekeeper` 23/23; `cargo clippy --all-targets` clean; `cargo fmt --check` clean
