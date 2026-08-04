# Decision Log: refactor-positional-delete-footer-fetch

## Interview

Headless run. No live interview took place; GitHub issue [#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165) ("perf(scan): parallelize positional-delete Phase B footer fetches + add metadata size hint", label `refactoring`) stands in for it. The exchanges below reproduce the issue's sections as the question they answer.

**Q:** What problem is being solved, and where exactly?
**A:** PR #162 removed the quadratic re-download of positional-delete files. Phase A now reads each unique delete file once, concurrently, with `file_path` row-group pruning. The remaining bottleneck is Phase B: in `PositionalDeleteScanTable::partitioned_files` (`crates/lakehouse-engine/src/scan/positional_deletes.rs:661-689`) the loop over delete-carrying data files awaits each metadata fetch in sequence, with `with_metadata_size_hint(None)` so each is roughly two round-trips. For a shard with N delete-carrying data files that is N sequential footer round-trips. A plain, delete-free scan never pays this: DataFusion's Parquet opener fetches footers concurrently (`meta_fetch_concurrency`, default 32). The delete path therefore serializes exactly the metadata phase the base scan parallelizes, and file-granularity deletes (many data files, one small delete file each) are the dominant case.

**Q:** What change is proposed?
**A:** Option A, three parts. (1) Replace the serial `for` loop with a bounded-concurrent fan-out (`try_join_all` or `buffer_unordered`) over the delete-carrying data files, bounded by the SAME instance-level `delete_read_limiter` semaphore sized `s3_max_connections`, so the per-instance connection budget still holds, including both sides of a broadcast join. (2) Pass `with_metadata_size_hint(Some(N))`, roughly 256 to 512 KB, to collapse each footer read to one request. (3) Verify that the session `FileMetadataCache` populated in Phase B actually survives to the opener's `CachedParquetFileReaderFactory`, in capacity and eviction terms; if it evicts, every footer is fetched twice. Add a check or guard if needed.

**Q:** What is explicitly out of scope?
**A:** Cluster-wide re-reads of a shared partition-granularity delete file across shards, a separate concern. And the inherent `RowSelection` decode cost, where pages with any surviving row are still fully decompressed, which is fundamental to the access-plan approach.

**Q:** What must be true for the change to be accepted?
**A:** Phase B footer fetches run concurrently within the `s3_max_connections` bound, with no more than N in flight across the instance. Each footer read is a single object-store request in the common case. Correctness is unchanged: identical post-delete row sets, and the existing `scan_positional_deletes` tests stay green.

**Q:** Which spec features does this target, and may a new feature area be invented?
**A:** The existing `datafusion-scan/scan-execution-connection-concurrency`, `scan-execution-file-metadata`, and `scan-execution-memory-and-credentials` features, and possibly `scan-execution-positional-deletes`. Extend or amend their scenarios via DELTA markers rather than inventing a new feature area, unless research shows a new one is genuinely warranted. Research showed it is not: the change alters the request count and concurrency of reads those four features already govern, and adds no capability.

**Q:** Is there a constraint on how the concurrency bound is shared?
**A:** Yes, and it is already recorded. `scan-execution-connection-concurrency` requires exactly ONE size-N limiter shared across all registered scan tables in one scan invocation, covering both sides of a broadcast join, and explicitly forbids two independent size-N limiters. Phase B must reuse that same limiter.

## Design Decisions

### [1] Reuse the one existing fan-out limiter for Phase B rather than adding a second

- **Decision:** Phase B's footer fetches acquire permits from the SAME `Arc<Semaphore>` Phase A uses, the one constructed per scan invocation in `raw_scan::delete_read_limiter` and cloned into every registered provider.
- **Alternatives:** A second size-N semaphore dedicated to footer fetches; a larger single semaphore sized `2 × s3_max_connections`.
- **Rationale:** `datafusion-scan/scan-execution-connection-concurrency` already forbids two size-N limiters, because concurrently planned join-side leaves would then allow up to 2N in-flight reads. A Phase-B-private semaphore reintroduces exactly the bug PR #162 fixed, one layer down. Enlarging the single semaphore would silently change the operator-facing meaning of `S3_MAX_CONNECTIONS`.
- **Promotes to ADR:** no

### [2] Deadlock freedom rests on no-hold-and-wait, not on phase ordering

- **Decision:** Every fan-out task in both phases acquires exactly one permit, holds it across exactly one object-store read, and releases it on completion. No task holds a permit while awaiting another permit, and no task awaits another task.
- **Alternatives:** Rely on the fact that Phase A completes and drops its permits before Phase B's fan-out is constructed within one `partitioned_files` call.
- **Rationale:** The phase ordering is real but is not the safety argument, because it only holds WITHIN one provider. A broadcast join runs two providers concurrently, so provider A's Phase A permits and provider B's Phase B permits genuinely coexist. The no-hold-and-wait property is what makes that contention queue rather than deadlock, and it is the property the implementation and the review must check.
- **Promotes to ADR:** yes

### [3] Fan out with `try_join_all` plus the semaphore, not `buffer_unordered`

- **Decision:** `futures::future::try_join_all` over all assigned entries, with the semaphore providing the bound.
- **Alternatives:** `futures::stream::StreamExt::buffer_unordered(n)`, which is DataFusion's own idiom for concurrent footer fetching (`datafusion-datasource-parquet-54.1.0/src/file_format.rs:369`, driven by `meta_fetch_concurrency`); `tokio::task::JoinSet`.
- **Rationale:** `buffer_unordered` would impose a second bound alongside the semaphore, leaving two numbers governing one budget, and it loses input order, which would reorder the shard's single `FileGroup` for no benefit. `JoinSet` forces `'static` bounds on what are currently borrows of `&self`. `try_join_all` matches the shape Phase A already uses in the same module, preserves order for free, and needs the semaphore regardless because the bound must hold ACROSS providers, which no per-fan-out combinator can express.
- **Promotes to ADR:** no

### [4] Source the metadata size hint from the `ParquetFormat` the opener uses, not a new constant

- **Decision:** Pass `self.format.metadata_size_hint()` to `DFParquetMetadata::with_metadata_size_hint`.
- **Alternatives:** A new module constant, for example `const FOOTER_SIZE_HINT_BYTES: usize = 512 * 1024`, as the issue's "e.g. 256-512 KB" suggests; a new scan-spec field; a new `S3_METADATA_SIZE_HINT` VS property.
- **Rationale:** `ParquetFormat` already owns this decision. `datafusion.execution.parquet.metadata_size_hint` defaults to `Some(512 * 1024)` (`datafusion-common-54.1.0/src/config.rs:803`), and `ParquetFormat::create_physical_plan` copies that same value onto the `ParquetSource` the opener uses (`datafusion-datasource-parquet-54.1.0/src/file_format.rs:481-505`). The provider already holds that `ParquetFormat` and already hands it to `create_physical_plan`. Reading the hint back off it gives the two sites one value that cannot drift, lands inside the issue's suggested range without inventing a number, and adds neither a constant nor a knob. A spec field or VS property would be a decision the module declined to make, for a value no operator has asked to tune.
- **Promotes to ADR:** yes

### [5] Skip the Parquet page index during access-plan construction

- **Decision:** Pass `with_page_index_policy(Some(PageIndexPolicy::Skip))` in Phase B.
- **Alternatives:** Leave the policy unset, which is today's code.
- **Rationale:** Leaving it unset is not neutral. `DFParquetMetadata::effective_page_index_policy` resolves an unset policy to `Optional` whenever a metadata cache is set (`datafusion-datasource-parquet-54.1.0/src/metadata.rs:197-205`), and Phase B sets one. So Phase B currently pays a third round-trip for a page index `build_access_plan` never reads, and stores a correspondingly larger cache entry. `Skip` is also the policy the opener passes explicitly (`datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:750`), so the cached entry becomes exactly the shape the opener asks for, and the single-request property stops depending on where the page index happens to sit in the file. The opener still loads the page index on demand when pruning needs it, exactly as it already does on the delete-free path, so the delete path becomes symmetric with the baseline rather than regressing against it.
- **Promotes to ADR:** yes

### [6] Do not raise the DataFusion metadata-cache limit; measure the reuse property instead

- **Decision:** Leave `datafusion.runtime.metadata_cache_limit` at DataFusion's 50 MiB default. Satisfy the issue's "verify metadata-cache reuse, add a check or guard if needed" with a shard-scale test (task 1.7) that asserts K delete-carrying data files issue the same total data-file request count as K delete-free ones.
- **Alternatives:** Size the limit from the per-instance memory budget in `build_runtime_env`; raise it to a larger fixed constant; assume the default suffices and add no check.
- **Rationale:** The cache is real and does evict. `DEFAULT_METADATA_CACHE_LIMIT` is 50 MiB, eviction is LRU on `put`, and an entry larger than the whole limit is silently never cached at all (`datafusion-execution-54.1.0/src/cache/file_metadata_cache.rs:63-102`, `cache_manager.rs:451`). But 50 MiB holds several hundred row-group-only footers, above a realistic shard's delete-carrying file count, and decision [5] is what keeps entries that small. Raising the limit would add RSS the memory pool does not account for, next to an engine that stalls concurrency at 80% of the per-instance limit, so it is a risk rather than a safety measure. Measuring the property beats reading a capacity constant, because the test fails for any reason the footer is refetched, not only for eviction. If the measurement fails at K=64, raising the limit becomes the fix, derived internally with no wire-format or VS change, and the outcome is recorded here either way.
- **Promotes to ADR:** no

### [7] Rename `delete_read_limiter` to `delete_path_read_limiter`

- **Decision:** Rename the field, the constructor function, the `register_file_list` parameter, and the clamp unit test.
- **Alternatives:** Keep the name and widen only the doc comment.
- **Rationale:** The limiter no longer bounds only reads OF delete files; it bounds every object-store read the delete path issues while preparing a scan, delete-file bodies and data-file footers alike. A name that describes one of two things it guards is the kind of misnaming that makes the next reader assume the footer fetches are unbounded. The spec text it implements has to be reworded for the same reason, so the vocabulary shift is happening regardless. Serena's `rename_symbol` moves every reference together across the three modules.
- **Promotes to ADR:** no

### [8] Every new request-count assertion carries a non-empty logical schema

- **Decision:** New and strengthened request-count tests build their `ScanSpec` with a populated `common.logical_schema`, and task 1.1 retrofits the existing `scan_reads_footer_via_range_get_once` the same way.
- **Alternatives:** Keep using the existing `raw_spec` helper, whose `logical_schema` is empty.
- **Rationale:** An empty logical schema sends `register_file_list` down the legacy `ParquetFormat::infer_schema` fallback, which fetches and caches the footer BEFORE Phase B runs. Phase B is then a pure cache hit and its request shape is invisible to any assertion. This is not hypothetical: `scan_reads_footer_via_range_get_once` passes today (verified locally, `cargo test -p lakehouse-engine --test scan_no_head_test scan_reads_footer_via_range_get_once`, exit 0) even though Phase B issues an unhinted, page-index-loading fetch, because inference already populated the entry. Production always supplies a logical schema, so Phase B is the first reader of the footer there and its request shape is fully load-bearing. A test that cannot fail is worse than no test, because it reads as coverage.
- **Promotes to ADR:** yes

### [9] No new feature area

- **Decision:** Amend four existing features via DELTA markers rather than create a new one.
- **Alternatives:** A new `datafusion-scan/scan-execution-delete-prep-concurrency` feature collecting the Phase A and Phase B fan-out rules in one place.
- **Rationale:** The change adds no capability. It alters the request count and the concurrency of reads the four existing features already own, and each amended clause sits naturally under the feature that already owns that axis: the budget under connection-concurrency, the two-phase pipeline under positional-deletes, the footer round-trip count under file-metadata, and the cache reuse under memory-and-credentials. A new feature would split one axis across two homes, which is the leakage the spec library's organization exists to avoid.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by plan-reviewer blockers and, later, by code review. -->
