# Decisions: refactor-positional-delete-footer-fetch

## ADR: Deadlock freedom rests on no-hold-and-wait, not on phase ordering

**ID:** footer-fetch-no-hold-and-wait
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Phase A (delete-file body reads) completes and drops its permits before Phase B's fan-out
(data-file footer fetches) is constructed within one `partitioned_files` call. That ordering holds
only WITHIN one provider. A broadcast join runs two providers concurrently, so provider A's Phase A
permits and provider B's Phase B permits genuinely coexist on the one shared semaphore.

### Decision

Every fan-out task in both phases acquires exactly one permit, holds it across exactly one
object-store read, and releases it on completion. No task holds a permit while awaiting another
permit, and no task awaits another task.

### Options Considered

| Option | Verdict |
|--------|---------|
| No-hold-and-wait per task, checked independently of phase ordering | ✓ Chosen — the property that makes cross-provider, cross-phase contention queue rather than deadlock |
| Rely on phase ordering (Phase A drops its permits before Phase B starts) | ✗ Rejected — true only within one provider; a broadcast join's two concurrently-planned providers make the phases coexist |

### Consequences

The implementation and its review must check the no-hold-and-wait property directly, not the
phase-ordering coincidence. This generalizes past this plan: any future fan-out sharing this
semaphore must preserve the same one-permit, one-read, no-nesting shape.

## ADR: A performance invariant that fails silently needs a runtime observable, not only a test

**ID:** footer-cache-eviction-needs-runtime-observable
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Round-1 plan review found the original cache-reuse test fixture (K=64, two columns, 64 row groups)
cached well under 1 MB against the 50 MiB `DEFAULT_METADATA_CACHE_LIMIT` — structurally unable to
fail for eviction, the one cause it was offered as a guard for. The supporting claim that 50 MiB
"holds several hundred footers" was unquantified and false in general: a cached entry is a parsed
`ParquetMetaData` holding one `ColumnChunkMetaData` per `columns × row_groups`, so a wide Iceberg
data file's entry is megabytes and no fixed file count is safe across tables.

### Decision

Ship both halves of the underlying requirement. Task 1.7 measures reuse with a fixture CALIBRATED
so K cached footers occupy 70-90% of `DEFAULT_METADATA_CACHE_LIMIT` (a
`write_wide_local_parquet(columns, row_groups, rows_per_row_group)` helper plus a calibration loop
reading `FileMetadataCache::list_entries()`'s `size_bytes`). Task 1.7b ships a runtime guard: a
per-invocation counter of footers re-fetched after eviction, surfaced as one level-gated `udf_log!`
debug line at scan completion.

### Options Considered

| Option | Verdict |
|--------|---------|
| Calibrated reuse fixture (70-90% of the limit) AND a shipped eviction-observable guard | ✓ Chosen — measurement that can fail, plus a production signal for the cases that go over the limit anyway |
| Size the cache limit from the per-instance memory budget in `build_runtime_env` | ✗ Rejected — blind fix; adds RSS the memory pool does not account for, next to an engine that stalls concurrency at 80% |
| Raise the limit to a larger fixed constant | ✗ Rejected — same blind-fix problem; no measured basis |
| Assume the 50 MiB default suffices and add no check | ✗ Rejected — unquantified and false in general per the entry-size analysis above |
| Reuse measurement only, guard deferred to "if the measurement fails" (the plan's first draft) | ✗ Rejected — the guard became contingent on a measurement that structurally could not fail, so it never shipped |

### Consequences

A shard whose footers evict now produces an observable signal instead of a silent double-fetch.
Raising the cache limit remains available later, derived internally, and would then be driven by
the 1.7b counter's real telemetry rather than by a guess.

Move detection off the object store entirely.

Delete the `debug_checkpoint` call; the single level-gated `udf_log!` line at the report site is the whole output surface.

