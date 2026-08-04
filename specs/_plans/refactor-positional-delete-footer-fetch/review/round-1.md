# Plan Review Findings: refactor-positional-delete-footer-fetch (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 15 (Blockers: 3, Advisory: 12)
- Intent Fidelity blockers: 1

## Premortem

Three failure stories, each routed into the taxonomy below.

**"The green build lied."** Ops reports S3 throttling during planning on a wide Iceberg table. Phase B's footer entries for a 300-column, 400-row-group file are megabytes each; a dozen of them exceed the 50 MiB `DEFAULT_METADATA_CACHE_LIMIT`, entries evict between Phase B and the opener, and every footer is fetched twice. `scan_footer_reuse_holds_at_shard_scale` at K=64 two-column fixtures never approached the limit and passed on every CI run. Nothing logs the refetch. → Intent `[SCOPE_REDUCTION]`.

**"The regression guard that never guarded."** `scan_footer_fetches_bounded_across_join_sides` is quarantined as flaky weeks after merge, because its peak counter also instruments the opener's execute-time reads of the same data files. Someone relaxes `assert_eq!(peak, 3)` to `assert!(peak <= 4)`. A later refactor gives each provider its own semaphore and nothing fails. → Feasibility `[UNSTATED_ASSUMPTION]`.

**"One value, two owners, after all."** A later change sets `datafusion.execution.parquet.metadata_size_hint` in `SessionConfig` to tune footer reads. Nothing happens: `int96_coerced_parquet_format()` builds its own `TableParquetOptions::default()`, and `ParquetFormat::create_physical_plan` copies THAT onto the `ParquetSource`. The spec text says the hint is the DataFusion config option; it is not. → Feasibility `[UNSTATED_ASSUMPTION]`.

## Verified claims (spot-check of the three DataFusion-source findings)

All three hold. Recorded here so round 2 need not re-derive them.

1. `metadata_size_hint` default `Some(512 * 1024)` — `datafusion-common-54.1.0/src/config.rs:802`. `ParquetFormat::create_physical_plan` copies `self.options.global.metadata_size_hint` onto the `ParquetSource` — `datafusion-datasource-parquet-54.1.0/src/file_format.rs:481-505`. `ParquetFormat::metadata_size_hint()` is public — `file_format.rs:177`. `PositionalDeleteScanTable::scan` does hand `self.format` to `create_physical_plan` (`positional_deletes.rs:735`). Signatures match: `DFParquetMetadata::with_metadata_size_hint(Option<usize>)` (`metadata.rs:96`), `with_page_index_policy(Option<PageIndexPolicy>)` (`metadata.rs:120`).
2. `effective_page_index_policy` resolves an unset policy to `Optional` when a cache is set — `metadata.rs:197-205`. The opener passes `PageIndexPolicy::Skip` — `opener/mod.rs:750` — and loads the page index on demand at `opener/mod.rs:958-966`. So `Skip` in Phase B matches the opener and stays symmetric with the delete-free baseline.
3. `scan_reads_footer_via_range_get_once` passes today: `cargo test -p lakehouse-engine --test scan_no_head_test scan_reads_footer_via_range_get_once` → `ok`, re-run during this review. The vacuity mechanism checks out: `raw_spec` leaves `logical_schema` empty, `register_file_list` takes the `infer_schema` branch (`raw_scan.rs:208-217`), `ParquetFormat::infer_schema` populates the session `FileMetadataCache` (`file_format.rs:349-363`), and the listing `ObjectMeta` matches `object_meta_for`'s epoch `last_modified` only because `RequestLoggingStore` answers HEAD with `Utc.timestamp_nanos(0)` — so `CachedFileMetadataEntry::is_valid_for` (`cache_manager.rs:266-269`) passes and Phase B is a cache hit. Also confirmed: `RuntimeEnvBuilder::with_metadata_cache_limit` exists (`datafusion-execution-54.1.0/src/runtime_env.rs:439`), and `try_join_all` switches to `FuturesOrdered` above 30 futures (`futures-util-0.3.32/src/future/try_join_all.rs:139-142`), so the fan-out has no quadratic-polling cost at shard scale.

## Intent Fidelity

#### [SCOPE_REDUCTION] BLOCKER
- Location: plan.md § Consequences (row "Do NOT raise `datafusion.runtime.metadata_cache_limit`") and § Implementation Tasks task 1.7; decision-log.md § Design Decisions [6]
- Issue: issue #165's proposed-change item 3 is "Verify metadata-cache reuse between Phase B and the opener's `CachedParquetFileReaderFactory`; **guard against silent double-fetch if it evicts**." The plan delivers neither half. Task 1.7 measures reuse over `K=64` fixtures written by `write_local_parquet` (two columns, 64-row row groups — footers of a few KB), so the whole run caches well under 1 MB against a 50 MiB limit: the assertion is structurally unable to fail for eviction, the cause it is offered as the guard for. Decision [6] then makes the guard contingent on that unfailable measurement ("if the measurement fails at K=64, raising the limit becomes the fix"), so no guard, no metric, and no log ships. The plan's supporting claim — "50 MiB holds several hundred row-group-only footers, above a realistic shard's delete-carrying file count" — is unquantified; the cached object is a parsed `ParquetMetaData` holding `columns × row_groups` `ColumnChunkMetaData` structs, so a wide Iceberg data file's entry is megabytes, and the plan itself notes an entry larger than the limit "is silently never cached at all".
- Fix: In plan.md task 1.7, make the fixture scale-representative — write the K files with enough columns and row groups that the K parsed footers approach `DEFAULT_METADATA_CACHE_LIMIT`, and record the measured per-entry `CachedFileMetadataEntry` memory size in decision-log.md [6]. Add a new task 1.7b that ships the guard the issue asks for: an observable that fires when a footer cached during access-plan construction is re-fetched by the opener (a `udf_log!` debug line plus a counter surfaced through `scan::diagnostics`), so a production eviction is not silent, and add a `datafusion-scan/scan-execution-memory-and-credentials` DELTA scenario covering it. Update decision-log.md [6] to state that reuse is measured AND eviction is observable, rather than deferring the guard to an unfailable measurement.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks task 1.4
- Issue: the task moves the `ConcurrencyProbe` needles onto "the four DATA files only" while telling the implementer to model the test on `scan_delete_reads_bounded_across_join_sides` (`crates/lakehouse-engine/tests/scan_positional_deletes.rs:1155`), which runs the whole join through `run_join_scan_with_session`. That existing test is deterministic precisely because its needles match only DELETE files, which the opener never reads — its own comment says so (`scan_positional_deletes.rs:1032-1038`: "the count observed here is exactly the number of delete reads holding a semaphore permit"). Once the needles are data files and the scan executes, the opener's execute-time column reads of those same four files are counted and delayed by the probe, and they hold no semaphore permit. `peak` is a monotonic `fetch_max`, so any execute-time overlap above 3 latches permanently and `assert_eq!(peak, BUDGET)` fails or flakes. The task never says "plan construction only", although task 1.3 says exactly that for the single-provider case.
- Fix: In plan.md task 1.4, replace "modelled on `scan_delete_reads_bounded_across_join_sides`" with an explicit instruction to drive plan construction only via `build_join_physical_plan` (`crates/lakehouse-engine/src/scan/join_scan.rs:248`, re-exported at `crates/lakehouse-engine/src/scan/mod.rs:41`), which registers both sides and returns the physical plan without executing it, and to read `peak` immediately after it returns. Keep `planning_concurrency = 2`, the fixed per-read delay, and `DELETE_READ_TIMEOUT`. State that the join's post-delete row-set assertion, if wanted, runs as a separate scan against an un-probed store.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks task 1.3
- Issue: the task contradicts itself. It requires "drive plan construction only (`build_raw_scan_physical_plan`) so the probe observes Phase B without the opener's execute-time reads" and, in the same sentence chain, "assert the returned scan emits the same post-delete row set as a serial run". A physical plan emits nothing; executing it re-introduces exactly the opener reads the first clause excludes, against the same monotonic `peak` counter and the same 50 ms per-read delay, so the `assert_eq!(peak, N)` the task calls its core assertion becomes order-dependent and unreliable. The task also asks to assert that "the `PartitionedFile` order matches the per-shard spec's file order" without saying where that order is observable from a physical plan.
- Fix: In plan.md task 1.3, split the assertions. Keep the peak assertion in a plan-construction-only test that reads `peak` immediately after `build_raw_scan_physical_plan` returns and never executes the plan. Move the post-delete row-set equality to a separate test (or reuse the existing `scan_applies_file_granularity_positional_deletes`) run against a plain `TrackingStore` with no probe. State that the file-order assertion downcasts the returned plan to `DataSourceExec` and reads its `FileScanConfig` file group, the same technique the existing plan-shape tests use.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Context (paragraph "The size hint already exists and has an owner"); decision-log.md [4]; `datafusion-scan/scan-execution-file-metadata/spec.md` § Background, DELTA:NEW bullet 2
- Issue: the spec delta records "DataFusion already carries one: `datafusion.execution.parquet.metadata_size_hint`, default `Some(512 * 1024)`, held on the `ParquetFormat` this provider owns." The provider's `ParquetFormat` is built by `int96_coerced_parquet_format()` (`crates/lakehouse-engine/src/scan/raw_scan.rs:264-269`) from `TableParquetOptions::default()` — it never reads `SessionConfig`. The 512 KiB is the struct default, not the session option, and `create_physical_plan` overwrites the source's options with the format's (`file_format.rs:487`). Setting the session key changes neither site. The repo already relies on this being untrue elsewhere: `crates/lakehouse-engine/src/scan/mod.rs:136` sets `datafusion.execution.parquet.pushdown_filters = true` on the session, which this provider's `ParquetSource` never sees. The plan's mechanism still works, but the recorded provenance is wrong and will mislead the next reader into tuning a knob that does nothing.
- Fix: In the `scan-execution-file-metadata` delta Background bullet 2, plan.md § Context, and decision-log.md [4], replace the claim that the hint is `datafusion.execution.parquet.metadata_size_hint` with: the hint is `TableParquetOptions::default()`'s built-in 512 KiB carried on the provider-owned `ParquetFormat` from `int96_coerced_parquet_format()`, which `ParquetFormat::create_physical_plan` copies onto the opener's `ParquetSource`; the session-level `datafusion.execution.parquet.*` options do not reach this provider.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Impact
- Issue: the hint makes each Phase B fetch request `size.saturating_sub(512 KiB)..size` (`datafusion-datasource-parquet-54.1.0/src/metadata.rs:240-243`), so N concurrent fetches can hold up to N × 512 KiB of prefetch bytes in memory at plan time, N being `s3_max_connections`. Today Phase B is serial and unhinted, holding one small footer at a time. The plan quantifies the request-count and concurrency change but says nothing about the transient RSS, next to a mission constraint that the engine self-throttles at 80% of the per-instance limit and a memory pool that does not account for Parquet fetch buffers. `specs/mission.md` § Constraints makes bounded per-instance memory a first-class requirement.
- Fix: Add a fourth paragraph to plan.md § Impact stating the transient plan-time buffer ceiling as `s3_max_connections × metadata_size_hint` (≈16 MiB at N=32), that it is outside the DataFusion memory pool, and that it is the same ceiling the delete-free opener path already reaches.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md § Dependencies
- Issue: "`parquet::arrow::arrow_reader::PageIndexPolicy` comes from the `parquet` 58 dependency" names a path that does not exist. In `parquet-58.3.0`, `arrow/arrow_reader/mod.rs:43-46` imports `PageIndexPolicy` privately (`use`, not `pub use`); the public path is `parquet::file::metadata::PageIndexPolicy` (`parquet-58.3.0/src/file/metadata/mod.rs:133`), which is what DataFusion itself imports at `opener/mod.rs:77`.
- Fix: In plan.md § Dependencies and task 1.6, change the import path to `parquet::file::metadata::PageIndexPolicy`.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: `datafusion-scan/scan-execution-memory-and-credentials/spec.md` § Scenarios, DELTA:CHANGED "A shared Parquet metadata reader avoids a duplicate footer parse"
- Issue: the scenario now asserts "a shard of K delete-carrying data files SHALL issue the same total number of object-store requests against its data files as a shard of the same K data files with no deletes attached" and, two clauses later, retains "if no shared reader is installed, the UDF MAY accept one additional footer range GET per delete-carrying data file". The MAY permits precisely what the SHALL forbids. Its antecedent is also dead: `ParquetFormat::create_physical_plan` installs `CachedParquetFileReaderFactory` unconditionally (`file_format.rs:494-501`), so "no shared reader is installed" cannot occur on this path. A retained escape clause beside an absolute SHALL is the ambiguity the requirements guardrails ban.
- Fix: Delete the "if no shared reader is installed, the UDF MAY accept one additional footer range GET per delete-carrying data file" clause from the DELTA:CHANGED block, keeping only its "MUST NOT issue a HEAD request in either case" obligation, restated unconditionally.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `datafusion-scan/scan-execution-positional-deletes/spec.md` § Scenarios, DELTA:NEW "Concurrent data-file footer fetches stay within the connection budget", last clause; plan.md § Verification § Scenario Coverage
- Issue: the new clause "a footer fetch that fails SHALL surface as a credential-redacted user error naming the failure, with the remaining fetches abandoned, no partial access plan attached, and no row emitted for the shard" is a new normative error-path obligation with no implementing task and no test. § Scenario Coverage maps the whole scenario to `scan_footer_fetches_bounded_by_connection_budget`, which only asserts peak concurrency and row counts on the success path. The existing suite's error test, `scan_rejects_unapplicable_delete_file`, covers the Phase A backstop, not a Phase B footer failure.
- Fix: Add a task 1.3b to plan.md § Implementation Tasks: add `scan_footer_fetch_failure_redacts_and_abandons_remaining` to `crates/lakehouse-engine/tests/scan_positional_deletes.rs`, using an `ObjectStore` decorator that errors on the second delete-carrying data file's footer GET with a message containing a secret value from `dummy_storage()`, and assert the scan fails with a `UdfError::User` that names the failure, contains no secret substring, and emits no rows. Add the test to § Scenario Coverage under that scenario.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `datafusion-scan/scan-execution-connection-concurrency/spec.md` DELTA:CHANGED clause 4; `datafusion-scan/scan-execution-positional-deletes/spec.md` DELTA:NEW "A delete-free data file still costs no footer fetch of its own", clause 2
- Issue: both deltas state "a data file carrying NO deletes SHALL NOT acquire a permit". That is a white-box claim about an internal semaphore, not observable behavior, and no test can fail for it: the mapped test `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files` asserts zero GETs against delete-free files, which a permit-taking implementation would also satisfy. As written the clause is not verifiable.
- Fix: Restate both clauses in observable terms — a mixed shard of M delete-free files and one delete-carrying file with `s3_max_connections = 1` SHALL complete plan construction within a single footer-fetch window, and SHALL issue no object-store read against any delete-free file — and add that N=1 mixed-shard case to task 1.3's `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks task 1.3
- Issue: one checkbox carries three separable units of work: renaming `ConcurrencyProbe::delete_needles`/`is_delete_read` plus updating two call sites, writing `scan_footer_fetches_bounded_by_connection_budget`, and writing `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`. The first is a mechanical rename that must land before the other two compile; the last is a different scenario in a different feature. A single checkbox cannot be verified as one unit, and the mid-task rename collides conceptually with task 1.5, which the plan presents as "the rename task".
- Fix: Split task 1.3 into 1.3a (rename `ConcurrencyProbe::delete_needles` → `needles` and `is_delete_read` → `is_probed_read`, update the two existing call sites), 1.3b (`scan_footer_fetches_bounded_by_connection_budget`), and 1.3c (`scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`). Update § Parallelization Group B accordingly.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 1.9 vs § Verification § Checklist; § Verification § Scenario Coverage row 1
- Issue: two mismatches. Task 1.9 names `cargo test`, `make lint`, `make fmt`, `make test-e2e` but omits `make cross-musl-udf-build`, which § Checklist lists as the Build step — so following the task list leaves one checklist row unrun. Separately, the connection-concurrency scenario is mapped to `scan_footer_fetches_bounded_across_join_sides` alone, although that scenario's single-table AT-MOST-N clause is covered by `scan_footer_fetches_bounded_by_connection_budget` and its no-permit-for-delete-free clause by `scan_mixed_shard_fetches_footers_only_for_delete_carrying_files`, both filed under other scenarios.
- Fix: Add `make cross-musl-udf-build` to task 1.9's command list, and list all three tests against the connection-concurrency row of § Scenario Coverage.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Impact; § Verification § Manual Testing
- Issue: the partial-aggregate scan path registers through the same seam — `crates/lakehouse-engine/src/scan/partial_agg.rs:36` calls `register_files`, which builds the limiter and the `PositionalDeleteScanTable` (`raw_scan.rs:115-133`) — so Phase B's fan-out changes behavior there too. `scan-execution-connection-concurrency` § Background already records that the budget applies "on both the raw-row scan path and the partial-aggregate path". Neither § Impact nor § Manual Testing mentions the partial-aggregate path, so a reviewer cannot tell whether it was considered or missed.
- Fix: Add one sentence to plan.md § Impact naming the partial-aggregate path as also affected (same provider, same limiter, no behavior change), and add a partial-aggregate row to § Manual Testing — a `GROUP BY` query over a delete-carrying virtual-schema table returning the same aggregates as before the branch.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Design § Patterns (row `with_page_index_policy(Some(Skip))`); decision-log.md [5]
- Issue: Phase B must now mirror three of the Parquet opener's internal read decisions for the cache entry to be reused — the size hint, the page-index policy, and the `ObjectMeta` identity. Two have a single owner the plan reads from (`ParquetFormat`, and the `meta` value cloned into the `PartitionedFile`). The third does not: `PageIndexPolicy::Skip` is hard-coded to match `datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:750`, a DataFusion internal with no public accessor and no compile-time link. If a future DataFusion release changes the opener's policy, Phase B's `Skip`-shaped cache entry silently forces a `load_page_index` round-trip per file that the delete-free path does not pay. The plan does not name what detects that.
- Fix: In decision-log.md [5] and the task 1.8 doc-comment instruction, state that `Skip` mirrors a DataFusion-internal constant (`opener/mod.rs:750` in 54.1.0) that this crate cannot read programmatically, and name `scan_footer_reuse_holds_at_shard_scale` as the test that fails if a DataFusion upgrade changes it — so the coupling is documented at the site that would break.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Impact, second observable consequence
- Issue: "Peak in-flight requests during plan construction rise from 1 to at most N" is factually wrong. Phase A's `collect_delete_positions` already fans out to N during plan construction today (`crates/lakehouse-engine/src/scan/positional_deletes.rs:598-616`), and the dominant file-granularity case — one delete file per data file — reaches that bound. The plan-construction peak does not rise; what rises is the number of phases that reach it. As written the sentence overstates the change and will read as a new risk in the PR.
- Fix: Rewrite the sentence as: "Plan-construction peak in-flight requests stay bounded by N. Phase A already reaches that bound; Phase B now can too, drawing from the same permit pool rather than adding to it."

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary
- Issue: the first sentence runs 55 words against the 25-word cap, and packs four independent claims (replace the loop, reuse the limiter, add the hint, no behavior change) into one clause chain, so no single claim is front-loaded.
- Fix: Split § Summary's first sentence into two: one stating the change ("Fan out Phase B's per-data-file footer fetches concurrently under the limiter Phase A already uses, and hint each fetch down to one object-store request."), one stating the invariant ("Behavior-preserving: identical post-delete row sets, identical file order, no wire-format change, no new operator knob (#165).").
