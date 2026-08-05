# Tasks: refactor-positional-delete-footer-fetch

## Phase 2: Implementation (Group A) — scan_no_head_test.rs, sequential
- [x] 1.1 Retrofit `scan_reads_footer_via_range_get_once` with a non-empty `common.logical_schema` via new `raw_spec_with_logical_schema` helper; rewrite its doc comment
- [x] 1.2 Add `scan_access_plan_footer_fetch_is_one_range_get` asserting exactly 1 hinted range GET during plan construction

## Phase 2: Implementation (Group B) — scan_positional_deletes.rs, sequential
- [x] 1.3 Add `scan_spec_with_logical_schema` + `logical_fields` helpers; add `scan_footer_fetches_bounded_by_connection_budget` and `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`; rename `ConcurrencyProbe::delete_needles`→`needles`, `is_delete_read`→`is_probed_read`
- [x] 1.4 Add `scan_footer_fetches_bounded_across_join_sides` via `build_join_physical_plan`, plan-construction-only, peak == 3 [expert]

## Phase 2: Implementation (Group C) — same file, sequential, depends on A+B
- [x] 1.5 Rename `delete_read_limiter` → `delete_path_read_limiter` (field, ctor fn, param, unit test) via Serena `rename_symbol`; update 4 doc comments
- [x] 1.6 Rewrite `PositionalDeleteScanTable::partitioned_files` to fan out Phase B via `try_join_all` + shared limiter, hinted size, `PageIndexPolicy::Skip` [expert]

## Phase 2: Implementation (Group D) — depends on C
- [x] 1.7 Add `scan_footer_reuse_holds_at_shard_scale`: calibrated wide-fixture shard-scale reuse test; record measured values in decision-log.md [6] [expert]
- [x] 1.7b Ship eviction-observability guard: record cached footer paths in `positional_deletes.rs`, oracle+counter in `scan::diagnostics`, one `udf_log!` debug line in `scan/mod.rs`, reset at `run_scan_dispatch`, new test file `scan_footer_refetch_observable.rs`

## Phase 2: Implementation (Group E) — depends on D
- [x] 1.8 Update module-level and struct-level doc comments in `positional_deletes.rs` describing the two-phase pipeline

## Phase 3: Verification (Group F) — depends on E
- [ ] 1.9 Run verification checklist: `cargo test`, `make lint`, `make fmt`, `make test-e2e`

## Phase 4: Review Fixes
- [x] 4.1 Split the two causes `footer_refetch_count` conflates: keep an absent recorded path an unconditional re-fetch, gate the `hits == 0` clause on a new opener-coverage parameter computed at the `emit_footer_refetch_diagnostic` call site from `spec.common.limit.is_none() && spec.common.join.is_none()`; document why a pushed LIMIT or a join makes `hits == 0` ambiguous; add the limit-pushdown regression assertions inside `scan_footer_refetch_observable.rs`'s existing test [expert]
- [x] 4.2 Add a second `#[test]` to `scan_footer_refetch_observable.rs` driving two sequential `run_scan_one` calls over different data files through `run_scan_dispatch`, asserting invocation 2 reports zero re-fetches; verify it fails with `reset_access_plan_cached_footers()` commented out; correct the existing test's false reset claim and the module doc's test count [expert]
- [x] 4.3 In `crates/lakehouse-engine/src/scan/mod.rs`, guard `emit_footer_refetch_diagnostic` with an early return unless `diagnostics::telemetry_enabled(ctx.debug_level())`, placed before the `session_ctx.runtime_env()` chain; update its doc comment to name the level gate (via `telemetry_enabled`, shared with `emit_phase_telemetry`) as what makes it inert at the production default. Do not change `record_access_plan_cached_footer`'s call site in `positional_deletes.rs`.
- [x] 4.4 In `crates/lakehouse-engine/src/scan/positional_deletes.rs`, change Phase B's `acquire_owned()` failure message (near the delete-carrying-branch permit acquisition, currently a verbatim copy of Phase A's "delete-read limiter unavailable: {e}") to name the operation (data-file footer fetch via `delete_path_read_limiter`) and include the file path `abs` passed through the existing `redact(..., secrets)` helper. Leave Phase A's message unchanged.
- [x] 4.5 In `crates/lakehouse-engine/src/scan/positional_deletes.rs`, add `use crate::scan::diagnostics;` to the `use` block so the `partitioned_files` doc comment's intra-doc link `[`diagnostics::record_access_plan_cached_footer`]` resolves, and shorten the call site to `diagnostics::record_access_plan_cached_footer(&meta.location);`. Verify with `cargo doc -p lakehouse-engine --no-deps --document-private-items` that the unresolved-link warning is gone.
- [x] 4.6 In `crates/lakehouse-engine/tests/scan_positional_deletes.rs` and `crates/lakehouse-engine/tests/scan_no_head_test.rs`, delete the RED-phase "EXPECTED TO FAIL" paragraphs from `scan_footer_fetches_bounded_by_connection_budget`, `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`, `scan_footer_fetches_bounded_across_join_sides`, `scan_reads_footer_via_range_get_once`, and `scan_access_plan_footer_fetch_is_one_range_get`. Restate any information that outlives the RED phase in present tense as what a failure of the assertion would mean. Comment-only; do not weaken any assertion.
- [x] 4.7 In `crates/lakehouse-engine/tests/scan_positional_deletes.rs`, rename `tracking_store_with_probe`'s parameter `delete_needles`→`needles` (struct-literal shorthand at its field-init call site) and rewrite its doc comment to say it instruments the given bare filenames (delete files for `scan_delete_reads_*`, data files for `scan_footer_fetches_*`); rewrite `ConcurrencyProbe::in_flight`'s doc to "Probed reads currently inside a delayed `get_opts`." Leave `DELETE_READ_DELAY`/`DELETE_READ_TIMEOUT` constant names unchanged.
