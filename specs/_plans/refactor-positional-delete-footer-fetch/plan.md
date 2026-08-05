# Plan: refactor-positional-delete-footer-fetch

> **Status:** blocked — see open-questions.md

## Summary

Replace the serial `for` loop that fetches each delete-carrying data file's Parquet footer in `PositionalDeleteScanTable::partitioned_files` with a bounded-concurrent fan-out under the SAME instance-level limiter Phase A already uses, and collapse each footer read to a single object-store request by passing the metadata size hint the Parquet opener already uses. Behavior-preserving: identical post-delete row sets, identical file order, no wire-format change, no new operator knob ([#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165)).

## Context

PR #162 fixed the quadratic half of positional-delete cost: Phase A now reads each unique delete file once, concurrently, with `file_path` row-group pruning. The remaining cost sits in Phase B, `PositionalDeleteScanTable::partitioned_files` (`crates/lakehouse-engine/src/scan/positional_deletes.rs:661-689`), where the loop over delete-carrying data files awaits each footer fetch before starting the next:

```rust
for entry in &self.files {
    if let Some(deletes) = delete_positions.get(abs.as_str()) … {
        let parquet_metadata = DFParquetMetadata::new(store.as_ref(), &meta)
            .with_file_metadata_cache(Some(Arc::clone(&metadata_cache)))
            .with_metadata_size_hint(None)   // two round-trips per file
            .fetch_metadata()
            .await …;                        // serialized across all delete-carrying files
    }
}
```

For a shard with K delete-carrying data files that is K serialized footer fetches, each costing two object-store round-trips because no size hint is supplied: DataFusion's push decoder first requests the last 8 bytes to learn the metadata length, then requests the metadata range. A delete-free scan of the same K files never pays this shape. DataFusion's opener fetches footers concurrently and supplies a 512 KiB size hint by default, so the delete path serializes and doubles exactly the metadata phase the base scan parallelizes and collapses. The dominant real-world case is file-granularity deletes: many data files, one small delete file each, so Phase B dominates Phase A.

Three findings from reading the DataFusion 54.1.0 source shaped this plan beyond the issue's proposal.

**The size hint already exists and has an owner.** `datafusion.execution.parquet.metadata_size_hint` defaults to `Some(512 * 1024)` (`datafusion-common-54.1.0/src/config.rs:803`). `ParquetFormat::create_physical_plan` copies that value from `self.options.global` onto the `ParquetSource` the opener uses (`datafusion-datasource-parquet-54.1.0/src/file_format.rs:481-505`). The provider already holds that same `ParquetFormat` in `self.format` and already hands it to `create_physical_plan`. Reading the hint back off it makes one value govern both the access-plan fetch and the opener, with no new constant and no new knob.

**Setting a metadata cache silently opts the fetch into loading the page index.** `DFParquetMetadata::effective_page_index_policy` returns `PageIndexPolicy::Optional` whenever the caller sets a cache and leaves the policy unset (`datafusion-datasource-parquet-54.1.0/src/metadata.rs:197-205`). Phase B does exactly that, so today it pays a third round-trip for a page index that `build_access_plan` never reads, and stores a much larger cache entry. The opener passes `PageIndexPolicy::Skip` explicitly (`datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:750`). Matching the opener makes the hinted fetch a single request deterministically, rather than one that depends on where the page index happens to sit in the file.

**The existing footer-once guard does not cover the production configuration.** `scan_reads_footer_via_range_get_once` (`crates/lakehouse-engine/tests/scan_no_head_test.rs:690`) builds its spec through `raw_spec`, which leaves `logical_schema` empty. `register_file_list` then takes the legacy `ParquetFormat::infer_schema` fallback, which fetches and caches the footer BEFORE Phase B runs. Phase B is therefore a pure cache hit in that test and its cost is invisible to the assertion. Verified locally: the test passes today (`cargo test -p lakehouse-engine --test scan_no_head_test scan_reads_footer_via_range_get_once`, exit 0) even though Phase B issues no hinted request and no page-index-skipping request. Production never takes that branch, because the adapter supplies a logical schema, so Phase B is the FIRST reader of the footer and its request shape is fully load-bearing. Every new request-count assertion in this plan therefore carries a non-empty `logical_schema`.

Iceberg spec compliance: checked, not engaged. This plan changes how many object-store requests the scan issues for a Parquet footer and how many run concurrently. It changes no delete-file association, no sequence-number applicability rule, no `file_path`/`pos` semantics, and no row-selection arithmetic. The Apache Iceberg table spec (https://iceberg.apache.org/spec/, Position Delete Files) is normative on the delete file's sort order and on the permission to use column metrics to skip work; both clauses are already quoted and exploited in `datafusion-scan/scan-execution-positional-deletes` and neither is touched here. No deviation to fix or track.

- **Goals** — Phase B footer fetches run concurrently within the existing instance-level `s3_max_connections` bound; each fetch is one object-store request in the common case; the footer is fetched once per scan for both access-plan construction and the opener, proven at shard scale and in the production configuration; post-delete row sets and file order unchanged.
- **Non-Goals** — cluster-wide re-reads of a shared partition-granularity delete file across shards (a separate concern, no issue filed); the `RowSelection` decode cost, where a page holding any surviving row is still fully decompressed (fundamental to the access-plan approach); any change to Phase A, to the delete-file row-group pruning, to the wire format, or to the adapter; raising the DataFusion metadata-cache limit (see Consequences).

## Design

### Architecture

```
PositionalDeleteScanTable::partitioned_files(state)          [one call per registered provider]
  │
  ├─ Phase A  collect_delete_positions(store)                 UNCHANGED
  │     ensure_positional_delete(..) for every delete ref     (backstop before any I/O)
  │     try_join_all over UNIQUE delete files
  │       each task: acquire ONE permit ──┐
  │                  read delete body      │
  │                  drop permit ──────────┤
  │     merge → HashMap<data_path, RoaringTreemap>
  │                                        │
  │   (all Phase A permits released here)  │   ONE Arc<Semaphore>, size s3_max_connections
  │                                        │   delete_path_read_limiter
  └─ Phase B  try_join_all over ALL assigned files   CHANGED   │
        entry WITHOUT deletes → PartitionedFile, no permit, no I/O
        entry WITH deletes    → acquire ONE permit ────────────┘
                                DFParquetMetadata
                                  .with_file_metadata_cache(session cache)
                                  .with_metadata_size_hint(self.format.metadata_size_hint())
                                  .with_page_index_policy(Some(Skip))
                                  .fetch_metadata()                    ONE range GET
                                drop permit
                                build_access_plan(row_groups, deletes)
        try_join_all preserves input order → PartitionedFile list in spec order
                                        │
                                        ▼
        ParquetFormat::create_physical_plan  installs CachedParquetFileReaderFactory
        over the SAME session FileMetadataCache → opener reads Phase B's entry, 0 GETs
```

The same `Arc<Semaphore>` is held by every provider registered for one scan invocation, including both sides of a broadcast join, exactly as `datafusion-scan/scan-execution-connection-concurrency` already requires for Phase A.

### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Bounded fan-out via `try_join_all` + owned semaphore permit | Phase B in `partitioned_files` | Mirrors Phase A's existing shape exactly, so the module has one concurrency idiom rather than two; `try_join_all` also preserves input order, which keeps the `FileGroup` identical to the serial build |
| Permit acquired inside the delete-carrying branch only | Phase B task body | A delete-free file issues no I/O; taking a permit for it would charge the connection budget for work that never touches the network |
| Read a shared setting back from its single owner | `self.format.metadata_size_hint()` | The `ParquetFormat` already decides the hint for the opener; reading it back removes the possibility of two sites disagreeing, and adds no constant and no knob |
| Request exactly the metadata the consumer reads | `with_page_index_policy(Some(Skip))` | `build_access_plan` reads only per-row-group row counts; the default-when-cached `Optional` policy fetches a page index it discards, costing a round-trip and inflating the cache entry |
| No-hold-and-wait | Every fan-out task | One permit, one read, release. No task holds a permit while awaiting another, so contention between phases and between join sides queues instead of deadlocking |
| Detect the failure at the store, report it at the entry point | `SpecSizedObjectStore::get_opts` counts footer-shaped reads per path; `scan::diagnostics` holds the counter; `scan/mod.rs` emits the debug line | The reuse property fails silently by construction — a cache eviction produces a correct result, just twice the requests. The store decorator is the only site that sees every read AND holds each file's authoritative size, so it is where a second footer read of one path is recognizable; the entry point is the only site holding a `UdfContext`, so it is where a `udf_log!` line can be written. Splitting detection from reporting keeps the decorator free of UDF-runtime types |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Reuse the ONE existing `Arc<Semaphore>` for Phase B | A second size-N semaphore dedicated to footer fetches | `datafusion-scan/scan-execution-connection-concurrency` already forbids two size-N limiters for exactly this reason: a provider could then run N delete reads and N footer fetches at once, doubling the instance's in-flight bound to 2N. A second limiter would also reintroduce the join-side bug PR #162 fixed |
| `try_join_all` + semaphore | `futures::stream::buffer_unordered(n)`, DataFusion's own idiom for concurrent footer fetching (`file_format.rs:369`); `tokio::task::JoinSet` | `buffer_unordered` would impose a SECOND bound alongside the semaphore and lose input order, changing the `FileGroup`'s file order for no benefit. `JoinSet` forces `'static` bounds on borrows of `&self`. `try_join_all` matches Phase A, preserves order, and needs the semaphore anyway for the cross-provider bound |
| Rename `delete_read_limiter` to `delete_path_read_limiter` | Leave the name; document the widened role in the doc comment only | The field would otherwise be named for one of the two things it bounds, and the spec text it implements has to be reworded anyway. A name that actively misdescribes the guarantee is the defect class the design rules call out; the rename is mechanical across three modules |
| Hint sourced from `self.format.metadata_size_hint()` | A new `const FOOTER_SIZE_HINT_BYTES: usize = 512 * 1024` in `positional_deletes.rs`; a new scan-spec field or VS property | A constant duplicates a decision `ParquetFormat` already owns and lets the two sites drift. A spec field or VS property is a decision the module declined to make, for a value no operator has asked to tune. Both are rejected by the "one owner per decision" and "prefer a default over a parameter" rules |
| `PageIndexPolicy::Skip` in Phase B | Leave the policy unset (today's behavior, which resolves to `Optional` because a cache is set) | Access-plan construction never reads the page index. `Skip` makes the single-request property deterministic instead of layout-dependent, shrinks the cache entry, and matches the policy the opener itself passes, so the cached entry is exactly the shape the opener asks for. The opener still loads the page index on demand when pruning needs it, exactly as it already does on the delete-free path, so this is symmetric with the baseline rather than a regression against it |
| Do NOT raise `datafusion.runtime.metadata_cache_limit`; measure reuse AND make eviction observable | Size it from the per-instance memory budget in `build_runtime_env`; raise it to a larger fixed constant; keep only the reuse measurement and defer the guard | Raising the 50 MiB default (`DEFAULT_METADATA_CACHE_LIMIT`) adds RSS that the memory pool does not account for, next to an engine that stalls concurrency at 80% of the per-instance limit, so it is a risk rather than a safety measure. The claim that the default "holds several hundred footers" is not something this plan can assert: a cached entry is a parsed `ParquetMetaData` holding one `ColumnChunkMetaData` per `columns × row_groups`, so a wide Iceberg data file's entry is megabytes, and an entry larger than the whole limit is silently never cached at all. So the plan does two things instead of assuming one. Task 1.7 calibrates its fixture until the K entries occupy 70-90% of the limit and records the MEASURED per-entry size, making the reuse assertion able to fail for eviction. Task 1.7b ships the observable issue #165 asks for — a footer re-fetched after access-plan construction cached it increments a `scan::diagnostics` counter and surfaces as one debug `udf_log!` line — so a shard that does exceed the limit in production is visible rather than silent. Raising the limit remains available as a later fix, derived internally with no wire or VS change, and would then be driven by that signal rather than by a guess |
| New request-count assertions carry a non-empty `logical_schema` | Keep using `raw_spec`'s empty schema | An empty logical schema routes registration through `ParquetFormat::infer_schema`, which pre-populates the metadata cache and makes every Phase B request-count assertion vacuous. The production path always supplies a logical schema |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-connection-concurrency | CHANGED | `datafusion-scan/scan-execution-connection-concurrency/spec.md` |
| datafusion-scan/scan-execution-positional-deletes | CHANGED | `datafusion-scan/scan-execution-positional-deletes/spec.md` |
| datafusion-scan/scan-execution-file-metadata | CHANGED | `datafusion-scan/scan-execution-file-metadata/spec.md` |
| datafusion-scan/scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |

No new feature area. The change adds no capability: it alters the request count and the concurrency of reads the scan already performs, which the four features above already govern.

## Impact

No change to query results, generated SQL, the scan-spec wire format, or any VS property. This is a behavior-preserving performance refactor with no migration and no operator action.

Four observable consequences.

**Planning time for delete-carrying scans drops.** For a shard with K delete-carrying data files, footer fetching goes from K serialized fetches of two or three object-store requests each, to a concurrent fan-out of K single-request fetches bounded by the existing `s3_max_connections` budget. File-granularity deletes benefit most, because there K is large and each delete file is small.

**Peak in-flight requests during plan construction rise from 1 to at most N.** N is `s3_max_connections`, the same budget that already caps Phase A delete-file reads and configures the object store's HTTP client pool, so the instance ceiling is unchanged. What changes is that the delete path can now reach that ceiling during footer fetching as well. Operators who pinned a low `S3_MAX_CONNECTIONS` keep exactly the bound they set.

**A metadata-cache eviction stops being silent.** Task 1.7b adds one debug-level signal: when a data file's footer is fetched a second time in one scan invocation, a counter in `scan::diagnostics` advances and the scan entry point writes one `udf_log!` debug line naming the count. At the production default level (`info`) nothing is emitted and nothing changes; at `debug`, an operator capturing `SCRIPT_OUTPUT_ADDRESS` sees the double-fetch that today would only show up as unexplained S3 request volume. No VS property, no wire-format field, and no new error path.

**The speedup itself is not measured here.** The acceptance criteria are request counts and concurrency bounds, both asserted by integration tests. Wall-clock improvement is left to a bench sweep, listed under Manual Testing, and no number is claimed by this plan.

## Dependencies

None new. `futures::future::try_join_all` and `tokio::sync::Semaphore` are already used by Phase A in the same module. `parquet::arrow::arrow_reader::PageIndexPolicy` comes from the `parquet` 58 dependency the crate already declares.

## Implementation Tasks

- [ ] 1.1 Make `scan_reads_footer_via_range_get_once` (`crates/lakehouse-engine/tests/scan_no_head_test.rs:690`) exercise the production configuration. Give both the baseline and the delta spec a non-empty `common.logical_schema` matching the fixture (`id` Int64, `name` Utf8, with Iceberg field-ids), so `register_file_list` installs the field-id adapter instead of taking the `ParquetFormat::infer_schema` fallback that pre-populates the metadata cache. Add a `raw_spec_with_logical_schema` helper rather than changing `raw_spec`, whose other three callers assert unrelated no-HEAD properties. Rewrite the test's doc comment to state why the logical schema is load-bearing: without it the access-plan fetch is a cache hit and the range-equality assertion holds regardless of how many round-trips that fetch would cost. The fixture Parquet carries no field-id metadata, so the installed adapter binds by name fallback; that is expected and changes no read. Expect this test to FAIL once the logical schema is added and PASS after task 1.6: pre-change the delta scan's data-file ranges are the unhinted probe plus metadata plus page index, post-change they are the single hinted suffix range the baseline opener already issues.
- [ ] 1.2 Add `scan_access_plan_footer_fetch_is_one_range_get` to `crates/lakehouse-engine/tests/scan_no_head_test.rs`. Register one delete-carrying data file with a non-empty `logical_schema` through the production `register_files` seam, then call `build_raw_scan_physical_plan` and STOP: plan construction runs Phase A and Phase B and nothing else, so the request log at that point contains only preparation reads. Assert the number of non-HEAD `get_opts` calls against the DATA file's location is exactly 1, and that its range is a bounded suffix range rather than the 8-byte footer probe. Fails today with 2 or 3.
- [ ] 1.3 Add `scan_footer_fetches_bounded_by_connection_budget` to `crates/lakehouse-engine/tests/scan_positional_deletes.rs`. Build a shard of K delete-carrying data files with K strictly greater than `s3_max_connections = N` (use K=6, N=3 to match `scan_delete_reads_bounded_by_connection_budget`), reuse `tracking_store_with_probe` with the DATA-file names as needles, and drive PLAN CONSTRUCTION ONLY: call `build_raw_scan_physical_plan`, read `peak` immediately after it returns, and NEVER execute the returned plan. Executing it would re-introduce the opener's execute-time column reads of those same four-plus data files, which match the same needles, hold no semaphore permit, and latch permanently into the monotonic `fetch_max` peak — turning the core assertion into an order-dependent flake. Assert the observed peak is EXACTLY N: a lower peak means the fan-out never ran, a higher peak means the bound leaked. Assert file order from the plan, not from execution: walk the returned plan to its single leaf (the `collect_leaf_scans` helper in `crates/lakehouse-engine/tests/scan_agg_projection_pruning.rs:142-155` is the existing walk, though it collects labels and schemas rather than the node, so this test needs the same recursion returning the leaf `&dyn ExecutionPlan`), downcast that leaf to `DataSourceExec`, call `downcast_to_file_source::<ParquetSource>()` to reach the `FileScanConfig`, and assert its single `file_groups` entry lists the files in the per-shard spec's order. Do NOT assert a post-delete row set here — behavior preservation is already covered by the existing `scan_applies_file_granularity_positional_deletes`, which runs a full scan against a plain `TrackingStore` with no probe and no artificial delay, and is listed under § Scenario Coverage as an unmodified behavior-preservation test. Rename `ConcurrencyProbe::delete_needles` to `needles` and `is_delete_read` to `is_probed_read`, since the probe now instruments data-file reads too; update the two existing call sites, and update the `TrackingStore::get_opts` comment (`scan_positional_deletes.rs:741-748`) that today asserts a needle match is always a permit-holding delete read — with data-file needles that only holds for a plan-construction-only driver. In the same task add `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`, also plan-construction-only: a shard mixing delete-carrying and delete-free data files, asserting after `build_raw_scan_physical_plan` that the non-HEAD `get_opts` count against each DELETE-FREE file's location is zero, that the delete-carrying files were each fetched once, and that every file appears in the leaf `FileScanConfig`'s file group in spec order with an access plan attached only to the delete-carrying ones.
- [ ] 1.4 Add `scan_footer_fetches_bounded_across_join_sides` to `crates/lakehouse-engine/tests/scan_positional_deletes.rs`. Drive PLAN CONSTRUCTION ONLY: build the session as `scan_delete_reads_bounded_across_join_sides` does (`scan_positional_deletes.rs:1155`) — `planning_concurrency = 2` so both leaves plan concurrently, the same fixed per-read delay, and `DELETE_READ_TIMEOUT` so a mis-wired limiter fails instead of hanging — but call `build_join_physical_plan` (`crates/lakehouse-engine/src/scan/join_scan.rs:248`, re-exported at `crates/lakehouse-engine/src/scan/mod.rs:39`) instead of `run_join_scan_with_session`, and read `peak` immediately after it returns. `build_join_physical_plan` registers both sides and returns the physical plan without executing it, which is the whole reason it exists as a public seam. Do NOT reuse `run_join_scan_with_session` here: unlike the existing test, whose needles match only DELETE files the opener never reads (see its `TrackingStore::get_opts` comment, `scan_positional_deletes.rs:741-748`), this test's needles are DATA files. Executing the join would count and delay the opener's execute-time column reads of those same files, which hold no semaphore permit, against the same monotonic `fetch_max` peak — so any execute-time overlap above 3 would latch permanently and `assert_eq!(peak, BUDGET)` would fail or flake. Two delete-carrying data files per side, `s3_max_connections = 3`, needles on the four DATA files only so Phase A reads stay undelayed and the probe measures the Phase B windows. Assert the peak is EXACTLY 3. This is the only test that fails against a per-provider or Phase-B-private semaphore, which would peak at 4; it is the regression guard task 1.3 cannot provide. The join's post-delete row-set equality is NOT asserted here; `scan_delete_reads_bounded_across_join_sides` already covers it, executing against its own store with no data-file needles. [expert]
- [ ] 1.5 Rename the limiter to match what it now bounds, using Serena's `rename_symbol` so every reference moves together: the `PositionalDeleteScanTable` field and constructor parameter, the `raw_scan::delete_read_limiter` function and its local bindings in `raw_scan::register_files` and `join_scan`, the `register_file_list` parameter, and the unit test `delete_read_limiter_clamps_zero_connections_to_one`. New name: `delete_path_read_limiter` (function `delete_path_read_limiter`, test `delete_path_read_limiter_clamps_zero_connections_to_one`). Update the four doc comments that describe it as bounding delete-file reads (`positional_deletes.rs:508-514`, `positional_deletes.rs:530-533`, `raw_scan.rs:59-66`, `raw_scan.rs:157-162`) to state that it bounds every object-store read the delete path issues while preparing a scan: Phase A delete-file bodies and Phase B data-file footers, one permit per read, shared across every provider of one invocation.
- [ ] 1.6 Rewrite `PositionalDeleteScanTable::partitioned_files` (`crates/lakehouse-engine/src/scan/positional_deletes.rs:644-690`) to make tasks 1.1 through 1.4 pass (task 1.5 has already renamed the limiter, so use the new name). Replace the serial `for` loop with a `try_join_all` fan-out over ALL assigned entries that preserves input order; inside each task, build the `ObjectMeta` and `PartitionedFile` as today, and take the delete-carrying branch only when `delete_positions` holds a non-empty set for that path. In that branch, acquire ONE permit from `self.delete_path_read_limiter` via `acquire_owned` (same error mapping as Phase A), then call `DFParquetMetadata::new(store.as_ref(), &meta)` with `.with_file_metadata_cache(Some(Arc::clone(&metadata_cache)))`, `.with_metadata_size_hint(self.format.metadata_size_hint())`, and `.with_page_index_policy(Some(PageIndexPolicy::Skip))`. Keep the existing `redact(...)` error mapping unchanged so no credential can reach an error message, and keep `build_access_plan` and the `with_extension` attachment byte-identical. Do NOT acquire a permit on the delete-free branch. Verify by inspection that the `ObjectMeta` passed to `DFParquetMetadata` is the same value cloned into `PartitionedFile::from(meta.clone())`, since the metadata cache admits an entry only when the requesting meta's size and `last_modified` both match the stored one. [expert]
- [ ] 1.7 Add `scan_footer_reuse_holds_at_shard_scale` to `crates/lakehouse-engine/tests/scan_no_head_test.rs`, the shard-scale check issue #165 asks for in place of assuming the metadata cache never evicts. The fixture MUST be scale-representative, not merely numerous: a two-column, 64-row-group footer is a few KB, so K=64 of them cache under 1 MB against the 50 MiB `DEFAULT_METADATA_CACHE_LIMIT` and the reuse assertion is structurally unable to fail for eviction — the one cause it is offered as the guard for. Add a `write_wide_local_parquet(dir, relative, columns, row_groups, rows_per_row_group)` helper (the cached `ParquetMetaData` is dominated by one `ColumnChunkMetaData` per `columns × row_groups`, so those two knobs, not the row count, set the entry size) and a matching `raw_spec_with_wide_logical_schema` so the spec's `logical_schema` names all `columns`; keep `common.projection` at two columns so the opener's execute-time reads stay cheap while the footer stays wide. CALIBRATE the shape by measurement rather than by assumption: after the first run, read the session cache's per-entry size from `state.runtime_env().cache_manager.get_file_metadata_cache().list_entries()`, whose `FileMetadataCacheEntry.size_bytes` is exactly the `memory_size()` the cache charges against its limit (`datafusion-execution-54.1.0/src/cache/cache_manager.rs:315-323`, `file_metadata_cache.rs:219-236`), and scale `columns × row_groups × K` until the K entries together occupy 70-90% of `DEFAULT_METADATA_CACHE_LIMIT` — close enough to the eviction cliff that the assertion is a real test, still under it so the positive case is expected to pass. As an order-of-magnitude starting point only, K=8 files of 128 columns × 128 row groups × 4 rows per row group is the first shape to measure; replace it with whatever the measurement says. Then run two scans over those K data files (non-empty wide `logical_schema`, `RequestLoggingStore`): one delete-free, one where every file carries a one-position delete file. Assert the total count of non-HEAD `get_opts` against the K DATA-file locations is EQUAL between the two runs, proving no footer cached during access-plan construction is evicted before the opener reads it. Assert as well that the measured aggregate entry size is within the calibrated 70-90% band, so a future DataFusion change to `memory_size()` fails this test loudly instead of silently re-vacuating it. RECORD the measured numbers in `decision-log.md` [6]: per-entry `CachedFileMetadataEntry` size in bytes, the fixture shape that produced it, K, the aggregate, and its percentage of the 50 MiB limit. Document in the test's doc comment what the assertion is sensitive to: `DEFAULT_METADATA_CACHE_LIMIT`, LRU eviction on `put`, and the fact that an entry larger than the whole limit is silently never cached. Note the expected runtime cost (writing and parsing K wide footers twice) in the doc comment so a future reader does not mistake it for an accident. [expert]
- [ ] 1.7b Ship the runtime eviction guard issue #165's proposed-change item 3 asks for ("guard against silent double-fetch if it evicts"), so an eviction in production is observable rather than silent. Task 1.7 proves reuse holds at a calibrated shard scale; this task proves an eviction is not silent when a real shard exceeds it. THREE parts. (a) DETECTION, in `SpecSizedObjectStore::get_opts` (`crates/lakehouse-engine/src/scan/object_store.rs:345-368`) — the one decorator every scan read already passes through, and the only site that holds each file's authoritative size, since answering HEAD from that index is its existing job. A non-HEAD `get_opts` whose requested range ENDS at that location's spec size is a footer read; a column-chunk read never ends at EOF because the footer follows it. Count footer reads per path for the invocation; the SECOND and each later footer read of the same path is a re-fetch of a footer access-plan construction already cached. (b) COUNTER, in `scan::diagnostics`: a process-global `AtomicU64` beside the existing `DEBUG_ROWS`, with `record_footer_refetch(location)`, `footer_refetch_count()`, and `reset_footer_refetch_count()` (the reset exists for test isolation). Each detected re-fetch increments it and writes one `debug_checkpoint` line. That line carries only the object-store `Path`'s final segment: an object-store `Path` is a bucket-relative key and holds no credential, unlike the `s3://user:secret@…` URI forms the spec can carry, so logging a segment of it introduces no new redaction obligation — and logging only the segment keeps that true even if a future caller passes a URI-shaped location. Reconstruct no URI at this site. (c) REPORT, in `crates/lakehouse-engine/src/scan/mod.rs` beside `emit_phase_telemetry` (line 276): when `footer_refetch_count()` is non-zero, emit ONE `udf_log!(ctx, debug, ...)` line naming the count and stating that the metadata cache evicted a footer between access-plan construction and the opener, so the eviction surfaces in the same debug stream operators already capture through `SCRIPT_OUTPUT_ADDRESS`. Test it with `scan_footer_refetch_is_observable_when_the_cache_evicts` in a NEW file `crates/lakehouse-engine/tests/scan_footer_refetch_observable.rs` holding that ONE test function, so no sibling test in the same binary can race the process-global counter (the workspace has no `serial_test` dev-dependency and this task adds none). The test forces eviction deterministically instead of with a 50 MiB fixture: build the session with `SessionContext::new_with_config_rt(config, RuntimeEnvBuilder::new().with_metadata_cache_limit(<a few KB>).build_arc())` (`datafusion-execution-54.1.0/src/runtime_env.rs:439`) so the Phase B entry is evicted or never admitted, run one delete-carrying scan to completion, and assert the `RequestLoggingStore` log holds TWO footer-shaped GETs against the data file and that `footer_refetch_count()` advanced by at least 1. Then, in the same function so the two runs cannot interleave, reset the counter and re-run the identical scan with the DEFAULT cache limit, asserting exactly ONE footer-shaped GET and a counter that stays at 0 — the control that proves the observable reports eviction rather than firing on every scan.
- [ ] 1.8 Update the module-level and struct-level doc comments in `crates/lakehouse-engine/src/scan/positional_deletes.rs` that describe the two-phase pipeline. The `partitioned_files` doc comment (lines 631-643) currently says Phase B "is delete-file-I/O-free" and describes a per-file loop; restate it as a bounded-concurrent, order-preserving fan-out that performs no DELETE-file I/O but does fetch each delete-carrying data file's own footer under the shared limiter, in one hinted range GET, with the page index skipped because `build_access_plan` never reads it. The struct doc comment (lines 479-497) claims the footer "parses once (task 2.5)"; add that this now holds on the production path only because access-plan construction is the first reader, and name the size hint's single owner.
- [ ] 1.9 Run the verification checklist below: `cargo test`, `make lint`, `make fmt`, and `make test-e2e`.

Tasks 1.4, 1.6, and 1.7 are tagged `[expert]`. 1.6 introduces concurrency over shared state (one semaphore, one metadata cache, two providers) where the failure mode is a silently exceeded connection bound rather than a compile error, and its cache-reuse correctness turns on an `ObjectMeta` equality the type system does not enforce. 1.4 must produce a deterministic peak-concurrency assertion across two concurrently planned join leaves, the case a single-provider test cannot reach. 1.7 calibrates a fixture against a memory measurement whose target band and oracle both need judgement. 1.7b is deliberately NOT tagged: its detection rule is one range comparison, its counter follows the existing `DEBUG_ROWS` atomic pattern in the same module, its report site is one line beside `emit_phase_telemetry`, and its test forces eviction with an explicit cache limit rather than by timing. Every other task is a mechanical edit, a rename the compiler enumerates, or a doc-comment rewrite.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 (both in `scan_no_head_test.rs`, sequential within the group) |
| Group B | 1.3, 1.4 (both in `scan_positional_deletes.rs`, sequential within the group) |
| Group C | 1.5, 1.6 (same source file, sequential within the group: the rename lands before the rewrite) |
| Group D | 1.7, 1.7b (sequential within the group: 1.7 calibrates and measures the reuse property, 1.7b ships the guard for the case that measurement cannot reach) |
| Group E | 1.8 |
| Group F | 1.9 |

Sequential dependencies:
- Group A ∥ Group B (different test files, no shared symbols; both must land before C)
- Group A, Group B → Group C (1.6 is the change that turns the new failing tests green)
- Group C → Group D (1.7 measures 1.6's behavior; 1.7b's detection sits in `object_store.rs`, which 1.6 does not touch, but its control run asserts the single-footer-GET shape 1.6 produces)
- Group D → Group E → Group F

## Record Notes

Edits the recorder must apply outside a `DELTA:*` marker, listed by file and anchor:

| Spec file | Edit |
|-----------|------|
| `datafusion-scan/scan-execution-connection-concurrency/spec.md` | RENAME the scenario heading `### Scenario: The connection budget also bounds positional-delete file reads` to `### Scenario: The connection budget also bounds the positional-delete path's object-store reads`, then replace that scenario's body with the `DELTA:CHANGED` block. The heading rename is why the delta cannot be matched by heading alone |
| `datafusion-scan/scan-execution-positional-deletes/spec.md` | The `DELTA:CHANGED` block replaces the existing `### Scenario: The refactor preserves the delete-application safety invariants` in place; heading unchanged |
| `datafusion-scan/scan-execution-file-metadata/spec.md` | The `DELTA:CHANGED` block replaces `### Scenario: Data-file Parquet footer is read via a range GET, not a HEAD, and not twice` in place; heading unchanged |
| `datafusion-scan/scan-execution-memory-and-credentials/spec.md` | The `DELTA:CHANGED` block replaces `### Scenario: A shared Parquet metadata reader avoids a duplicate footer parse` in place; heading unchanged. The `DELTA:NEW` scenario `### Scenario: A metadata-cache eviction that re-fetches a footer is observable` is appended after it as a new scenario |

Each delta file's `## Background` carries a condensed subset of the permanent spec's existing bullets for context, plus a `DELTA:NEW` block. Merge only the `DELTA:NEW` bullets into the permanent Background; the condensed context bullets are already there and MUST NOT replace the fuller originals. No Feature-description line changes in any of the four features.

`specs/mission.md` needs no change: the scan's connection-concurrency and delete-application capabilities are described at a level this refactor does not alter.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Statement | `crates/lakehouse-engine/src/scan/positional_deletes.rs:672` | `.with_metadata_size_hint(None)` is replaced by the hint read from `self.format`; the explicit `None` becomes a wrong value, not merely a redundant one |
| Identifier | `raw_scan::delete_read_limiter` and the struct field of the same name | Renamed to `delete_path_read_limiter` by task 1.5; the old name has no remaining caller |
| Test | `raw_scan.rs::delete_read_limiter_clamps_zero_connections_to_one` | Renamed, not deleted: the clamp it guards is unchanged and still needed |

No function, module, or test becomes obsolete. The change replaces a loop with a fan-out inside one method.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| connection-concurrency: The connection budget also bounds the positional-delete path's object-store reads | Integration | `crates/lakehouse-engine/tests/scan_positional_deletes.rs` | `scan_footer_fetches_bounded_across_join_sides` |
| positional-deletes: Concurrent data-file footer fetches stay within the connection budget | Integration | `crates/lakehouse-engine/tests/scan_positional_deletes.rs` | `scan_footer_fetches_bounded_by_connection_budget` |
| positional-deletes: A delete-free data file still costs no footer fetch of its own | Integration | `crates/lakehouse-engine/tests/scan_positional_deletes.rs` | `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files` |
| positional-deletes: The refactor preserves the delete-application safety invariants | Integration | `crates/lakehouse-engine/tests/scan_positional_deletes.rs` | existing suite, unchanged and unmodified: `scan_applies_file_granularity_positional_deletes`, `scan_filters_partition_delete_by_file_path`, `scan_unions_multiple_delete_files`, `scan_fully_deleted_file_yields_no_rows`, `scan_deletes_compose_with_pushdown_and_pruning`, `scan_rejects_unapplicable_delete_file`, `scan_reads_shared_delete_file_once_per_shard`, `scan_prunes_delete_row_groups_by_file_path` |
| file-metadata: Data-file Parquet footer is read via a range GET, not a HEAD, and not twice | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | `scan_access_plan_footer_fetch_is_one_range_get`, plus `scan_reads_footer_via_range_get_once` made non-vacuous by task 1.1 |
| memory-and-credentials: A shared Parquet metadata reader avoids a duplicate footer parse | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | `scan_footer_reuse_holds_at_shard_scale` |
| memory-and-credentials: A metadata-cache eviction that re-fetches a footer is observable | Integration | `crates/lakehouse-engine/tests/scan_footer_refetch_observable.rs` | `scan_footer_refetch_is_observable_when_the_cache_evicts` |

The existing delete-application suite is listed to make the behavior-preservation claim checkable. Those tests MUST pass unmodified; a plan that had to edit one of their assertions would not be behavior-preserving.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| positional-deletes, file-metadata | `make test-e2e` | Exit 0. `e2e_positional_deletes_test` passes against the live Exasol + MinIO + Iceberg REST stack, returning the same post-delete row counts as before the change |
| connection-concurrency | `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<reachable-host>:<port>'` with `LAKEHOUSE_UDF_DEBUG_LEVEL=debug`, then a `SELECT` over a delete-carrying virtual-schema table with `S3_MAX_CONNECTIONS=2` | Query returns the same rows as with the default budget; no VM crash and no `ResourcesExhausted`. Use a SINGLE-LEG query: per CLAUDE.md the output redirect destabilizes multi-leg joins under debug |
| memory-and-credentials | `make bench` against the docker target, comparing a delete-carrying TPC-H run before and after the branch | Scan phase wall-clock does not regress. Check for a stray `bench/.env` first: a leftover file silently retargets the run at a remote cluster and can appear to hang for 15+ minutes |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt` | No changes |
