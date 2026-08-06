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

* The per-instance memory limit is read from `ctx.memory_limit()` (bytes; `0` =
  unknown sentinel). For a positive limit the pool is sized to
  `fraction × (limit − overhead_bytes)`, floored at a minimum non-zero budget,
  leaving headroom below the Exasol engine's 80% concurrency-stall threshold.
* The memory-pool fraction (default `0.6`) and the per-instance container-overhead
  megabytes (default `200`) are VS properties carried into the scan spec; a scan
  spec lacking them deserializes to those defaults.
* When the limit is the `0` sentinel, a conservative default budget is used and the
  fraction and overhead are ignored.
* The DataFusion memory pool bounds aggregation, sort, and join — but NOT the
  Parquet→Arrow decode and scan buffers. The configured `batch_size` is the lever
  that bounds that out-of-pool working set.
* The `batch_size` is carried in the scan spec; a spec lacking it deserializes to a
  conservative built-in default, and a sub-1 value is clamped to 1.
* Scan efficiency depends on the Parquet reader skipping data the predicate cannot
  match: row-group pruning (per-row-group statistics), page-index pruning (page-level
  statistics), and pushed-down filters into the `ParquetExec`. These are
  configuration flags on the DataFusion session / Parquet scan options, distinct from
  Iceberg file-level pruning (`vs-adapter/pushdown-file-pruning`), which prunes whole
  files before the reader opens them. The two compose: Iceberg drops files, the
  Parquet reader then drops row groups and pages within the surviving files.
* Storage credentials (including vended S3 keys) reach the UDF only inside the
  shard-invariant common spec argument, serialized once for the whole fan-out rather
  than repeated per shard; the UDF never contacts the catalog or re-requests credentials.
  This single S3 object store built from those credentials is reused for both data files
  and their associated positional-delete files.
* Credentials MUST NOT appear in any error message, including one raised while reading a
  delete file.
* Delete-carrying data files need their Parquet footer both for access-plan construction
  and by the opener; a shared reader factory / cached metadata reader avoids parsing the
  footer twice.
* See `datafusion-scan/scan-execution` for the base two-argument scan execution scenarios.
* This delta amends ONE clause each in the two credential-passthrough scenarios and nothing else. `vs-adapter/storage-backend-enum` (issue #274) makes the common blob's storage block a backend value rather than a bare S3 props object, so the clauses that say the UDF configures "its S3 object store" from those credentials are restated as the UDF registering the object store the carried backend names. Every other scenario of this feature is unchanged, and no Background bullet is superseded.
* Nothing about the memory pool, the batch size, the Parquet pruning flags, or the shared metadata reader is affected: the backend value replaces the storage value the object-store construction already read, and the S3 store it builds is byte-identical.
* The one-store-per-side rule is unchanged and still owned here: data files and their positional-delete files are read through the SAME registered store. `vs-adapter/storage-backend-enum` owns how that store is derived and registered without the scan path naming a backend.
* **A join spec now carries TWO storage backends, one per side, and the "single object store built from those credentials" rule is restated per side.** This feature's recorded Background says "This single S3 object store built from those credentials is reused for both data files and their associated positional-delete files". That sentence was written for a spec with one storage value. Under issue #294 it stays true PER SIDE: one store per side serves that side's data files and that side's delete files, built from that side's own backend. Nothing about the memory pool, the batch size, the Parquet pruning flags, or the shared metadata reader changes.
* **The whole-spec size index becomes a per-side size index, and that is the point rather than a tidy-up.** The index was deliberately whole-spec because one registered store had to answer BOTH sides' `head` calls — the collapse this delta removes. With one inner store per side, each store's index holds only its own side's files, so a `head` for one side's path can no longer be satisfied by the other side's store. `datafusion-scan/scan-execution-join` owns the routing decorator that makes this possible.
* **Layering is routing OUTSIDE, spec-sized `head` INSIDE.** The routing decorator wraps one spec-sized store per side, not the other way round, so every operation is routed BEFORE the sized-`head` shortcut can answer it. Sizing outside the router would answer an unroutable `head` from the index and defer the routing failure to the later range read, where it would surface as a credential-shaped access denial instead of the plan defect it is.
* **The single-table (non-join) path is untouched.** A spec with no join block registers exactly one spec-sized store over exactly one backend, with an index over exactly its own files — the same shape as before, now narrowed from "the whole spec" to "the only side there is".
* **Redaction is the union of the sides in scope.** An error raised while building or using a routed store can be produced while either side's credential is in scope, so the redaction set for such a message is every side's `secret_values()`, not the fact side's alone.
* **The per-side store split leaves footer-cache reuse intact, because the cache is keyed by path, not by store.** The session's `FileMetadataCache` is shared across both registered sides; the store split changes only WHICH store issues a given side's footer fetch, not which cache entry that fetch populates or reads. Two sides of a join therefore contend for one bounded cache, so a join's effective per-side footer budget is smaller than a single-table scan's — the eviction observable below is what makes that visible rather than silent.
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

## Scenarios

### Scenario: Scan sizes its memory pool from the reported per-instance limit

* *GIVEN* a scan UDF invocation whose UDF context reports a positive per-instance memory limit via `ctx.memory_limit()`
* *AND* a scan spec carrying a memory-pool fraction and a per-instance container-overhead byte count
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL subtract the container-overhead bytes from the per-instance limit and size the DataFusion memory pool to the configured fraction of that net budget
* *AND* the resulting pool budget MUST stay below the Exasol engine's 80% concurrency-stall threshold for the reported limit
* *AND* the UDF MUST NOT hardcode the pool budget to the unknown-limit default when a positive limit is reported

### Scenario: Scan falls back to the default budget when no memory limit is reported

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` returns `0` (the unknown / unavailable sentinel)
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL size the DataFusion memory pool to the conservative default budget, ignoring the configured fraction and overhead
* *AND* the scan SHALL otherwise execute identically to the positive-limit path

### Scenario: Scan clamps the memory pool to a minimum floor when overhead exceeds the limit

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` reports a positive per-instance limit
* *AND* a scan spec whose container-overhead bytes are greater than or equal to that limit
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL clamp the DataFusion memory pool budget to a minimum non-zero floor rather than producing a zero or negative budget
* *AND* the scan SHALL still build a usable session context that can execute a scan

### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries a storage backend holding vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL register the object store the carried storage backend names, configured from the credentials that backend holds
* *AND* the UDF MUST NOT decide the storage backend itself, derive it from a file URI scheme, or read the backend's payload outside the single backend-dispatching registration function specified by `vs-adapter/storage-backend-enum`
* *AND* when the spec also carries a join block, the UDF SHALL build a SECOND store from the join block's OWN storage backend and read the dimension side's files through it, so the whole-spec backend serves only the side whose files the whole-spec `table_root` and per-shard `files` describe
* *AND* the store the UDF builds for a side SHALL answer that side's per-file metadata lookups from a size index over THAT side's files only, so one side's `head` can never be satisfied by the other side's store
* *AND* the storage credentials SHALL travel in the shard-invariant common spec argument (serialized once for the whole fan-out), NOT be repeated per shard — the dimension side's backend included, since the join block is itself shard-invariant
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value from ANY carried backend MUST NOT appear in any error message the UDF returns

### Scenario: Scan bounds the Parquet decode working set via a configured batch size

* *GIVEN* a scan UDF building its DataFusion session configuration for a scan spec
* *WHEN* the UDF builds the `SessionConfig` (`session_config_for_spec`)
* *THEN* the UDF SHALL set the DataFusion `batch_size` so the per-batch Parquet decode and scan working set stays bounded, rather than leaving it at the DataFusion default
* *AND* the configured `batch_size` SHALL be sourced from the scan spec when present and otherwise from a conservative built-in default, clamped to at least 1
* *AND* the bound SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode source Parquet files

### Scenario: Scan enables Parquet row-group and page pruning so the reader skips non-matching data

* *GIVEN* a scan UDF building its DataFusion session configuration for a scan spec carrying a filter predicate
* *WHEN* the UDF builds the session config and the `ParquetExec` for its assigned files
* *THEN* the UDF SHALL enable Parquet predicate pushdown, row-group statistics pruning, and page-index pruning on the Parquet scan options rather than relying on the DataFusion defaults
* *AND* a row group whose column statistics provably exclude the predicate SHALL NOT be decoded
* *AND* this Parquet-level pruning SHALL compose with the Iceberg file-level pruning of `vs-adapter/pushdown-file-pruning` — files dropped by Iceberg are never opened, and within the surviving files non-matching row groups and pages are skipped
* *AND* the emitted rows SHALL be identical to a scan with pruning disabled (pruning narrows what is read, never the result set)

### Scenario: Positional-delete files are read with the same vended credentials

* *GIVEN* a scan invocation whose shard-invariant common spec carries a storage backend holding vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF reads a data file's associated positional-delete files from object storage
* *THEN* the UDF SHALL read the delete files through the SAME registered object store used for the data files OF THAT SIDE, configured from that side's backend credentials
* *AND* on a join spec, the dimension side's delete files SHALL be read with the DIMENSION side's credentials and the fact side's with the FACT side's, never one side's delete files with the other side's credentials
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials to read a delete file
* *AND* a credential value MUST NOT appear in any error message the UDF returns while reading a delete file, for whichever side's delete file failed

### Scenario: A shared Parquet metadata reader avoids a duplicate footer parse

* *GIVEN* a data file that carries positional deletes, whose Parquet footer is needed both to build the base `ParquetAccessPlan` and by the Parquet opener
* *WHEN* the scan UDF configures the `ParquetSource` for its assigned files
* *THEN* the UDF SHOULD install a `ParquetFileReaderFactory` (or an equivalent cached metadata reader) so the data file's footer metadata parsed for access-plan construction is reused by the opener rather than parsed a second time
* *AND* the `ObjectMeta` the UDF fetches the footer with SHALL be the SAME value it attaches to that file's `PartitionedFile`, because the metadata cache admits a stored entry only when the requesting `ObjectMeta`'s byte size and last-modified timestamp both match the stored entry's
* *AND* the reuse SHALL hold at SHARD SCALE, not only for one file: a shard of K delete-carrying data files SHALL issue the same total number of object-store requests against its data files as a shard of the same K data files with no deletes attached, so no footer cached during access-plan construction is evicted before the opener reads it
* *AND* if no shared reader is installed, the UDF MAY accept one additional footer range GET per delete-carrying data file, but MUST NOT issue a HEAD request in either case
* *AND* the configured batch size and Parquet row-group / page pruning SHALL apply unchanged whether or not a shared reader is installed

### Scenario: A metadata-cache eviction that re-fetches a footer is observable

* *GIVEN* a scan whose delete-carrying data files' parsed footers do not all fit in the session metadata cache, so a footer that access-plan construction fetched is not available from that cache when the Parquet opener reads it — because the entry was evicted, or because it exceeded the cache limit and was never admitted
* *WHEN* the opener fetches that data file's footer a second time within the same scan invocation
* *THEN* the UDF SHALL count that second footer fetch as a metadata-cache re-fetch, per scan invocation
* *AND* the UDF SHALL surface the accumulated re-fetch count on its debug diagnostic channel, so an operator running the scan at debug level observes the double-fetch directly instead of inferring it from object-store request volume
* *AND* a scan whose footers all stay cached SHALL report a re-fetch count of ZERO, so the signal distinguishes eviction from normal operation rather than firing on every delete-carrying scan
* *AND* the observable MUST stay inert otherwise: at the production default debug level it MUST NOT emit output, it MUST NOT alter the scan's result rows, it MUST NOT fail the scan (an eviction is a cost signal, not an error), and its record MUST NOT contain a storage credential
