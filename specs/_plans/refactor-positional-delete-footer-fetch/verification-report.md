# Verification Report: refactor-positional-delete-footer-fetch

## Verdict

| Check | Result |
|-------|--------|
| Build (`make cross-musl-udf-build`) | PASS — real `.so` build inside `rust:1.94-bookworm`, release profile, 1m28s |
| Test (`cargo test --workspace`) | PASS — 0 failures across all unit + integration binaries |
| E2E (`make test-e2e`) | PASS — 228 tests, 0 failures across 8 binaries, against the live Exasol + MinIO + Iceberg REST stack |
| Lint (`make lint` / `cargo clippy --all-targets`) | PASS — 0 warnings |
| Format (`make fmt`) | PASS — no diff |
| Code review | 7 findings — standard: 5, expert: 2 — all fixed and re-verified |

**Overall: PASS.** All Implementation Tasks (1.1–1.8) landed, both review passes fixed, all four checklist commands green, and the plan's full Scenario Coverage table is backed by passing tests.

## Scenario Coverage

| Scenario | Test | Result |
|----------|------|--------|
| connection-concurrency: budget also bounds the positional-delete path's reads | `scan_footer_fetches_bounded_across_join_sides` | PASS (peak == 3, both single-provider and cross-join-side) |
| positional-deletes: concurrent footer fetches stay within the connection budget | `scan_footer_fetches_bounded_by_connection_budget` | PASS |
| positional-deletes: delete-free file costs no footer fetch of its own | `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files` | PASS |
| positional-deletes: delete-application safety invariants preserved | 8 pre-existing tests, unmodified | PASS (all 8, e.g. `scan_applies_file_granularity_positional_deletes`, `scan_reads_shared_delete_file_once_per_shard`) |
| file-metadata: footer read via one range GET, not HEAD, not twice | `scan_access_plan_footer_fetch_is_one_range_get`, `scan_reads_footer_via_range_get_once` | PASS |
| memory-and-credentials: shared metadata reader avoids duplicate footer parse | `scan_footer_reuse_holds_at_shard_scale` | PASS — calibrated fixture at 78.28% of the 50 MiB `DEFAULT_METADATA_CACHE_LIMIT` (64×64×4 fixture, K=22); eviction cliff independently verified at K=29 |
| memory-and-credentials: eviction that re-fetches a footer is observable | `scan_footer_refetch_is_observable_when_the_cache_evicts`, `scan_dispatch_resets_the_footer_record_between_invocations` | PASS — includes review-driven regression coverage for LIMIT/join false positives and the invocation-start reset |
| E2E regression proof | `e2e_positional_deletes_test` (18/18, live stack) | PASS |

## Tool Evidence

- `cargo test --workspace`: 42 `test result: ok` blocks, 0 `FAILED`.
- `cargo test -p lakehouse-engine` (post-review-fixes): 826 passed, 0 failed.
- `make test-e2e`: 8 binaries, `test result: ok` for each (75, 18, 9, 25, 8, 18, 13, 62 = 228 total), 0 failed. Includes the real `cross-musl-udf-build` dependency (`docker run rust:1.94-bookworm cargo build --release -p lakehouse-engine`, `Finished release profile [optimized]`).
- `cargo clippy -p lakehouse-engine --all-targets -- -D warnings`: clean.
- `cargo fmt --check`: clean.
- `cargo doc -p lakehouse-engine --no-deps --document-private-items`: the plan-introduced unresolved intra-doc link (`diagnostics::record_access_plan_cached_footer`) is resolved; remaining warnings are pre-existing and unrelated to this plan.

## Code Review

7 findings (standard: 5, expert: 2), written to `review-findings.md`. All fixed:
- Standard: level-gate the re-fetch diagnostic (`emit_footer_refetch_diagnostic`) behind `telemetry_enabled`; correct Phase B's misleading permit-failure error message; resolve a broken intra-doc link; delete five stale TDD RED-phase doc-comment paragraphs; finish the `ConcurrencyProbe` rename in two doc comments and one parameter.
- Expert: split `footer_refetch_count`'s `hits == 0` clause so a pushed LIMIT or an empty-build-side join no longer produces false-positive re-fetch counts (verified fail→pass with a new in-process regression run); add a second test proving the invocation-start reset in `run_scan_dispatch` actually matters (verified fail→pass by commenting out the reset call).

Re-verified after fixes: 826 passed, 0 failed; clippy clean; fmt clean.

## Manual Testing (plan § Verification § Manual Testing)

Not executed in this headless run — each requires an interactively reachable environment this run does not have:

| Feature | Command | Status |
|---------|---------|--------|
| connection-concurrency | `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS` + `LAKEHOUSE_UDF_DEBUG_LEVEL=debug` against a delete-carrying single-leg query | NOT RUN — needs a listener reachable from the cluster nodes (jumphost/private IP), not available in this sandbox |
| memory-and-credentials | `make bench` docker-target before/after comparison | NOT RUN — the plan itself notes wall-clock is out of scope for the acceptance criteria ("The speedup itself is not measured here"); left for a follow-up bench sweep as the plan specifies |

The automated Verification Checklist (build/test/e2e/lint/fmt) is the acceptance gate per plan § Verification § Checklist and is fully green; Manual Testing is exploratory/operator-facing per the plan's own framing, not a merge blocker.

## Decision Log

`decision-log.md` [6] updated with measured calibration values from task 1.7 (per-entry size 1,865,550 bytes; fixture 64×64×4; K=22; aggregate 41,042,100 bytes; 78.28% of the 50 MiB limit).

## Notes

- No new dependencies introduced (per plan § Dependencies).
- No wire-format, VS property, or query-result change (per plan § Impact) — verified by the unmodified pass of all 8 pre-existing delete-application tests.
- Two open questions from the plan's review rounds were already resolved before implementation began (commits `d37fa81`, `d0eb1a3` on this branch, prior to this session).
