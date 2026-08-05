# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from
`ctx.memory_limit()` and size the DataFusion memory pool from a *net* budget — the
per-instance limit minus a configurable container/binary overhead — scaled by a
configurable fraction, to bound the per-batch Parquet decode working set via a
configured `batch_size`, to enable Parquet row-group and page pruning so the scan
reads only the byte ranges its predicate needs, and to consume storage credentials
carried in the scan spec (including vended STS tokens) without re-authenticating to
the catalog. The credentials and tuning knobs travel in the shard-invariant common
spec argument, serialized once for the whole fan-out.

## Background

* Delete-carrying data files need their Parquet footer both for access-plan construction
  and by the opener; a shared reader factory / cached metadata reader avoids parsing the
  footer twice.
* The DataFusion memory pool bounds aggregation, sort, and join — but NOT the
  Parquet→Arrow decode and scan buffers.
* See `datafusion-scan/scan-execution` for the base two-argument scan execution scenarios.

<!-- DELTA:NEW -->
* Cache reuse hinges on an `ObjectMeta` identity that is currently implicit. DataFusion's
  `FileMetadataCache` keys entries by object-store PATH, but admits a stored entry only when the
  requesting `ObjectMeta`'s byte size AND its `last_modified` timestamp both equal the stored
  entry's. Access-plan construction and the opener agree today only because the access plan
  builds one `ObjectMeta` (from the spec-supplied size and a fixed epoch timestamp) and clones
  that SAME value onto the file's `PartitionedFile`. A re-derived or re-timestamped `ObjectMeta`
  on either side would miss on every lookup, silently restore the duplicate footer fetch, and —
  because the miss path overwrites the entry under the same path key — make the two sides thrash
  the cache entry rather than share it. Issue
  [#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165).
* The cache is bounded and evicts, so reuse is a shard-scale property rather than a
  single-file one. DataFusion's default `FileMetadataCache` holds 50 MiB
  (`DEFAULT_METADATA_CACHE_LIMIT`) and evicts least-recently-used entries once a `put` exceeds
  that; an entry larger than the whole limit is silently never cached at all. If a shard's
  delete-carrying footers exceed the limit, entries populated during access-plan construction are
  evicted before the opener reads them and every footer is fetched twice. Restricting the
  access-plan fetch to the row-group metadata — no page index — is what keeps each entry small
  enough for that not to bite at realistic shard sizes; the verifiable property, not the
  mechanism, is what this feature requires. Because the entry size scales with a data file's
  `columns × row_groups`, no fixed shard-file count is safe for every table, so the feature
  requires BOTH halves of issue #165's item 3: reuse measured at a shard scale whose cached
  footers approach the limit, and an eviction that does happen anyway made observable rather
  than silent.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A shared Parquet metadata reader avoids a duplicate footer parse

* *GIVEN* a data file that carries positional deletes, whose Parquet footer is needed both to build the base `ParquetAccessPlan` and by the Parquet opener
* *WHEN* the scan UDF configures the `ParquetSource` for its assigned files
* *THEN* the UDF SHOULD install a `ParquetFileReaderFactory` (or an equivalent cached metadata reader) so the data file's footer metadata parsed for access-plan construction is reused by the opener rather than parsed a second time
* *AND* the `ObjectMeta` the UDF fetches the footer with SHALL be the SAME value it attaches to that file's `PartitionedFile`, because the metadata cache admits a stored entry only when the requesting `ObjectMeta`'s byte size and last-modified timestamp both match the stored entry's
* *AND* the reuse SHALL hold at SHARD SCALE, not only for one file: a shard of K delete-carrying data files SHALL issue the same total number of object-store requests against its data files as a shard of the same K data files with no deletes attached, so no footer cached during access-plan construction is evicted before the opener reads it
* *AND* if no shared reader is installed, the UDF MAY accept one additional footer range GET per delete-carrying data file, but MUST NOT issue a HEAD request in either case
* *AND* the configured batch size and Parquet row-group / page pruning SHALL apply unchanged whether or not a shared reader is installed
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A metadata-cache eviction that re-fetches a footer is observable

* *GIVEN* a scan whose delete-carrying data files' parsed footers do not all fit in the session metadata cache, so an entry populated during access-plan construction is evicted before the Parquet opener reads it
* *WHEN* the opener fetches that data file's footer a second time within the same scan invocation
* *THEN* the UDF SHALL count that second footer fetch as a metadata-cache re-fetch, per scan invocation
* *AND* the UDF SHALL surface the accumulated re-fetch count on its debug diagnostic channel, so an operator running the scan at debug level observes the double-fetch directly instead of inferring it from object-store request volume
* *AND* a scan whose footers all stay cached SHALL report a re-fetch count of ZERO, so the signal distinguishes eviction from normal operation rather than firing on every delete-carrying scan
* *AND* the observable MUST stay inert otherwise: at the production default debug level it MUST NOT emit output, it MUST NOT alter the scan's result rows, it MUST NOT fail the scan (an eviction is a cost signal, not an error), and its record MUST NOT contain a storage credential
<!-- /DELTA:NEW -->
