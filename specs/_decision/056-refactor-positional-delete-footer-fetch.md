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

## ADR: Source the metadata size hint from the `ParquetFormat` the opener uses, not a new constant

**ID:** footer-fetch-size-hint-from-parquet-format
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

A footer read with no size hint costs two object-store round-trips (a suffix probe, then the
metadata range). `datafusion.execution.parquet.metadata_size_hint` defaults to `Some(512 * 1024)`
and is already held on the `ParquetFormat` this provider owns and hands to
`create_physical_plan`, which the opener reads.

### Decision

Pass `self.format.metadata_size_hint()` to `DFParquetMetadata::with_metadata_size_hint`, rather
than introducing a new constant, scan-spec field, or VS property.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read the hint back off the existing `ParquetFormat` | ✓ Chosen — the two footer-reading sites (access-plan construction, opener) share one value that cannot drift; no new knob |
| A new module constant (e.g. `FOOTER_SIZE_HINT_BYTES`) | ✗ Rejected — invents a second number when one already exists on a value this code already holds |
| A new `ScanSpec` field or `S3_METADATA_SIZE_HINT` VS property | ✗ Rejected — a decision no operator has asked to tune |

### Consequences

Access-plan construction and the Parquet opener use the identical hint value by construction, so
the two sites cannot silently diverge, and the wire format and VS surface gain no new knob.

## ADR: Skip the Parquet page index during access-plan construction

**ID:** footer-fetch-skip-page-index
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

An unset page-index policy resolves to `Optional` whenever a metadata cache is set, and Phase B
sets one — so Phase B was paying a third round-trip for a page index `build_access_plan` never
reads, and caching a correspondingly larger entry. The opener itself passes `Skip` explicitly.

### Decision

Pass `with_page_index_policy(Some(PageIndexPolicy::Skip))` in Phase B.

### Options Considered

| Option | Verdict |
|--------|---------|
| Explicit `Skip` in Phase B | ✓ Chosen — matches what the opener already requests, removes an unread round-trip, shrinks the cached entry |
| Leave the policy unset (pre-existing code) | ✗ Rejected — not neutral; resolves to `Optional` and pays for and caches page-index data access-plan construction never reads |

### Consequences

Phase B's request shape becomes symmetric with the delete-free baseline rather than regressing
against it; the opener still loads the page index on demand when pruning needs it, unaffected.

## ADR: Every new request-count assertion carries a non-empty logical schema

**ID:** footer-fetch-tests-require-logical-schema
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

An empty logical schema sends `register_file_list` down the legacy `ParquetFormat::infer_schema`
fallback, which fetches and caches the footer BEFORE Phase B runs — making Phase B a pure cache hit
whose request shape is invisible to any assertion. `scan_reads_footer_via_range_get_once` passed
today even though Phase B issued an unhinted, page-index-loading fetch, because inference had
already populated the cache entry. Production always supplies a logical schema, so Phase B is the
first reader of the footer there.

### Decision

New and strengthened request-count tests build their `ScanSpec` with a populated
`common.logical_schema`, and task 1.1 retrofits the existing `scan_reads_footer_via_range_get_once`
the same way.

### Options Considered

| Option | Verdict |
|--------|---------|
| Populate `logical_schema` in every request-count test | ✓ Chosen — exercises the branch production actually takes; a test that cannot fail is worse than no test |
| Keep using the existing `raw_spec` helper (empty `logical_schema`) | ✗ Rejected — verified locally to pass even with an unhinted, page-index-loading Phase B fetch, so it reads as coverage without being coverage |

### Consequences

Every footer-request-count assertion this plan adds or strengthens is load-bearing against the
production configuration, not against the inference-fallback shortcut.

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

## ADR: A runtime observable must be detectable from the site the tests actually traverse, not only from production wiring

**ID:** footer-refetch-detection-must-be-test-reachable
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Round-2 plan review found the round-1 design sited re-fetch detection inside
`SpecSizedObjectStore::get_opts`, a private struct constructed only by the `StorageBackend::S3`
and `StorageBackend::Adls` arms of `register_side_store`, which short-circuit once a test has
registered its own store — which every host integration test does. So the planned test's two
assertions disagreed by construction: the request log would show footer-shaped GETs while
`footer_refetch_count()` stayed at 0. The design also gave the object-store decorator a second,
unrelated responsibility — knowing what a Parquet footer read looks like — which
`positional_deletes.rs` already owns.

### Decision

Move detection off the object store entirely. `PositionalDeleteScanTable::partitioned_files`
records each data file's `meta.location` immediately after the `fetch_metadata()` call that cached
it. The report site reads `state.runtime_env().cache_manager.get_file_metadata_cache().list_entries()`
and counts as a re-fetch every recorded path absent from the entry map or present with
`hits == 0`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Record at the fetch site; report by reading the session `FileMetadataCache`'s entries once at completion | ✓ Chosen — reachable from any host test regardless of which object-store decorator it registers; verified against DataFusion 54.1.0's `get`/`put` hit-counting semantics |
| Detect at `SpecSizedObjectStore::get_opts` (a non-HEAD read ending at the file's spec size) | ✗ Rejected — unreachable under every host integration test, which registers its own store and bypasses that decorator; also mixes an unrelated responsibility into the object-store layer |

### Consequences

The failed design was reachable in production and unreachable under test — the shape that ships
an untested guard. This generalizes past this plan: a production observable's detection site must
be one the test suite's own substitution points still traverse.

## ADR: Level-gate the re-fetch observable and reset its counter at every invocation start

**ID:** footer-refetch-observable-level-gated-and-reset
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Round-2 plan review found two conflicts in the original task 1.7b design. First, it called
`debug_checkpoint` once per detected re-fetch — a function that takes no level, checks none,
writes to stderr, and fsyncs a log file per line — so a shard losing every footer would write K
stderr lines and K fsyncs at the production default `info` level, on the exact workload this plan
exists to speed up. Second, the counter's reset was scoped to "test isolation" only, but per
CLAUDE.md's UDF parallelization model one VM serves many scan invocations in sequence off a fixed
per-node pool, so an unreset process-global would make invocation 2 report invocation 1's count.

### Decision

Delete the `debug_checkpoint` call; the single level-gated `udf_log!` line at the report site is
the whole output surface. Reset the counter at the START of every scan invocation, at
`run_scan_dispatch` (`crates/lakehouse-engine/src/scan/mod.rs:252`), the one site all three
dispatch paths pass through — not framed as a test-only concern.

### Options Considered

| Option | Verdict |
|--------|---------|
| One level-gated `udf_log!` line at report time; reset at `run_scan_dispatch` for every invocation | ✓ Chosen — nothing emitted and nothing written at the production default level; correct per-invocation counts in a VM that serves many invocations |
| `debug_checkpoint` per re-fetch (task 1.7b's original design) | ✗ Rejected — unconditional stderr + fsync per re-fetch at the production default level, on the workload the plan speeds up |
| Reset scoped only to test isolation | ✗ Rejected — leaves a shared VM's second invocation reporting the first invocation's count in production |

### Consequences

A scan whose footers all stay cached reports zero in production, not just in a freshly-started
test process. The control run in task 1.7b's own test doubles as the reset's regression test: the
second scan inherits the first run's process-global record and must still report zero.
