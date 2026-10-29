# Feature: DataFusion Scan Execution, Memory Budgeting and Credential Passthrough

The scan UDF sizes its DataFusion memory pool from the per-instance memory limit, bounds the Parquet decode working set with a `batch_size`, enables Parquet row-group and page pruning, and obtains storage credentials from the scan spec without contacting the catalog. The common spec carries the credentials or their reference and the tuning knobs, serialized once for the whole fan-out.

## Background

* `ctx.memory_limit()` returns the per-instance limit in bytes, with `0` meaning unknown. For a positive limit the pool is `fraction × (limit − overhead_bytes)`, floored at a minimum non-zero budget, which leaves headroom below the Exasol engine's 80% concurrency-stall threshold. For `0`, the UDF uses a conservative default budget and ignores the fraction and overhead.
* The memory-pool fraction (default `0.6`) and the per-instance container overhead in megabytes (default `200`) are VS properties carried in the scan spec. A spec without them deserializes to those defaults.
* The DataFusion memory pool bounds aggregation, sort, and join, but not the Parquet-to-Arrow decode and scan buffers. The scan spec's `batch_size` bounds that working set. A spec without it deserializes to a built-in default, and a value below 1 is clamped to 1.
* The row-position delete state (`scan-read-path/scan-execution-row-position-deletes`) is built before the first row is emitted. The deleted-position sets live until the access plans are built, and the access plans live until the scan finishes. Their size is a non-spillable reservation in the memory pool, so a large delete set shrinks the pool left for aggregation, sort, and join, and a delete set larger than the pool fails the scan instead of exhausting the instance.
* The deleted-position sets are reserved as they grow. Each decoded batch of a positional-delete file joins its set before the next batch decodes, so no unreserved copy of the positions outlives one batch. The reservation equals the sets' serialized size. A decoded deletion vector is reserved once it is decoded.
* The Arrow decode batch stays outside the pool, like any decode buffer. So do a delete file's footer bytes, at most the metadata size hint: the row groups inside them decode in the footer read's own task, and the bytes drop before that task ends.
* Row-group pruning, page-index pruning, and filter pushdown into the `ParquetExec` are DataFusion session and Parquet scan options. They differ from Iceberg file-level pruning (`file-planning/pushdown-file-pruning`), which drops whole files before the reader opens them.
* A delete-carrying data file needs its Parquet footer for access-plan construction and again in the opener. A shared reader factory or cached metadata reader avoids parsing it twice.
* The common spec carries, per side, a reference to the Exasol CONNECTION that supplies the storage credentials, or a sealed envelope with the backend the planning layer vended. `storage-access/scan-spec-credential-reference` owns the wire contract, the sealed envelope, the required grant, and the rotation consequence. `storage-access/storage-backend-enum` owns how the registered store derives from the backend.
* The UDF resolves the spec's storage blocks once at the top of the invocation, not at each store-construction site. A join spec has a block per side, so the result is a pair. `ctx.connection()` is engine-local, so it does not contact the table catalog.
* Each side has one registered object store. It serves that side's data files and every delete payload they reference: positional-delete files, Puffin deletion-vector files, and Delta deletion-vector sidecars. The single-table path registers one store over one backend.
* Each side's store answers per-file metadata lookups from a size index over that side's files only. A routing decorator wraps one spec-sized store per side, so every operation is routed before the sized-`head` shortcut can answer it (`scan-aggregation/scan-execution-join`).
* Every scan-path site that builds a value-based redaction secret set takes it from the resolved storage backends: on the raw-scan path, the partial-aggregate path, the join path, and in the scan's Parquet table provider. A spec that carries only a connection name has no secret to yield. The wire wrapper exposes no secret accessor, so a site that still reads the unresolved value fails to compile. The redaction set for a routed store is the union of every side's `secret_values()`.
* DataFusion's `FileMetadataCache` keys entries by object-store path. It admits a stored entry only when the requesting `ObjectMeta` has the same byte size and the same `last_modified`. Access-plan construction and the opener agree because the access plan builds one `ObjectMeta` (spec-supplied size, fixed epoch timestamp) and clones it onto the file's `PartitionedFile`. A re-derived `ObjectMeta` would miss on every lookup and restore the duplicate footer fetch.
* The per-side store split does not change cache reuse, because the cache is keyed by path. Both join sides share one bounded cache.
* The default `FileMetadataCache` holds 50 MiB (`DEFAULT_METADATA_CACHE_LIMIT`) and evicts least-recently-used entries. An entry larger than the limit is never cached. Entry size scales with a file's `columns × row_groups`, so no fixed shard-file count is safe for every table. The access-plan fetch requests row-group metadata only, with no page index, which keeps entries small.

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

### Scenario: Scan reads data files with credentials referenced or carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries, per side, EITHER a reference to the Exasol CONNECTION that supplies that side's storage credentials OR a sealed envelope carrying the storage backend the planning layer vended, resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL resolve every reference to a storage backend EXACTLY ONCE per invocation, before it builds any object store, under `storage-access/scan-spec-credential-reference`
* *AND* the UDF SHALL register the object store the RESOLVED storage backend names, configured from the credentials that backend holds
* *AND* the UDF MUST NOT decide the storage backend itself, derive it from a file URI scheme, or read the backend's payload outside the single backend-dispatching registration function specified by `storage-access/storage-backend-enum`
* *AND* when the spec also carries a join block, the UDF SHALL build a SECOND store from the join block's OWN resolved storage backend and read the dimension side's files through it, so the whole-spec backend serves only the side whose files the whole-spec `table_root` and per-shard `files` describe
* *AND* the store the UDF builds for a side SHALL answer that side's per-file metadata lookups from a size index over THAT side's files only, so one side's `head` can never be satisfied by the other side's store
* *AND* the credentials or their reference SHALL travel in the shard-invariant common spec argument, serialized once for the whole fan-out, NOT be repeated per shard, the dimension side's included, since the join block is itself shard-invariant
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials; resolving a CONNECTION by name through `ctx.connection()` is NOT such a request, because it reaches the database's own catalog rather than the table catalog and discovers no file, mints no token, and reads no snapshot
* *AND* a credential value from ANY resolved backend MUST NOT appear in any error message the UDF returns

### Scenario: Every redaction secret set in the scan path is built from the resolved backends

* *GIVEN* every scan-path site that builds a value-based redaction secret set, on the raw-scan path, the partial-aggregate path, the join path, and in the scan's Parquet table provider
* *WHEN* the scan spec's storage block carries a connection reference rather than a credential
* *THEN* EVERY such site SHALL take its secret set from the RESOLVED storage backend or backends, and NONE SHALL read it from the scan spec's own storage block
* *AND* the wire wrapper MUST NOT expose a secret-value accessor, so a site left reading the unresolved value fails to COMPILE rather than returning an empty set and silently disarming redaction
* *AND* an error raised on the RAW-SCAN path and an error raised on the PARTIAL-AGGREGATE path SHALL each be asserted to carry no resolved credential value, because those two paths read the fact-side set directly
* *AND* a spec whose storage block carries a reference MUST NOT yield an empty secret set once resolution has run, and a test SHALL assert the set is NON-empty for a resolved reference

### Scenario: Every scan error is redacted at the scan boundary

* *GIVEN* a scan whose session build or run path fails, including at a site that formats a raw DataFusion or table-registration error
* *WHEN* the error leaves the scan invocation
* *THEN* its message SHALL be redacted by value, then by credential label, against the union of every side's resolved secrets
* *AND* an inner site MAY still redact the text it surfaces, because the redaction is idempotent

### Scenario: Row-position delete state is reserved in the memory pool

* *GIVEN* a shard whose data files carry row-position deletes
* *WHEN* the scan UDF builds the deleted-position sets and the base access plans
* *THEN* the UDF SHALL reserve the deleted-position sets in the DataFusion memory pool until the access plans are built, and the access plans until the scan finishes
* *AND* the UDF SHALL reserve each deleted-position set as it grows, batch by batch, so a delete set larger than the pool fails while it is read rather than after it is fully built
* *AND* a shard whose delete state does not fit the pool SHALL fail with an error naming the memory pool before any row is emitted

### Scenario: Scan bounds the Parquet decode working set via a configured batch size

* *GIVEN* a scan UDF building its DataFusion session configuration for a scan spec
* *WHEN* the UDF builds the DataFusion `SessionConfig`
* *THEN* the UDF SHALL set the DataFusion `batch_size` so the per-batch Parquet decode and scan working set stays bounded, rather than leaving it at the DataFusion default
* *AND* the configured `batch_size` SHALL be sourced from the scan spec when present and otherwise from a conservative built-in default, clamped to at least 1
* *AND* the bound SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode source Parquet files

### Scenario: Scan enables Parquet row-group and page pruning so the reader skips non-matching data

* *GIVEN* a scan UDF building its DataFusion session configuration for a scan spec carrying a filter predicate
* *WHEN* the UDF builds the session config and the `ParquetExec` for its assigned files
* *THEN* the UDF SHALL enable Parquet predicate pushdown, row-group statistics pruning, and page-index pruning on the Parquet scan options rather than relying on the DataFusion defaults
* *AND* a row group whose column statistics provably exclude the predicate SHALL NOT be decoded
* *AND* this Parquet-level pruning SHALL compose with the Iceberg file-level pruning of `file-planning/pushdown-file-pruning`, files dropped by Iceberg are never opened, and within the surviving files non-matching row groups and pages are skipped
* *AND* the emitted rows SHALL be identical to a scan with pruning disabled (pruning narrows what is read, never the result set)

### Scenario: Delete payloads are read with the same resolved credentials

* *GIVEN* a scan invocation whose shard-invariant common spec references or carries, per side, the storage credentials for that side
* *WHEN* the scan UDF reads a data file's associated positional-delete files, Puffin deletion-vector files, or Delta deletion-vector sidecars from object storage
* *THEN* the UDF SHALL read those delete payloads through the SAME registered object store used for the data files OF THAT SIDE, configured from that side's RESOLVED backend credentials
* *AND* on a join spec, the dimension side's delete files SHALL be read with the DIMENSION side's credentials and the fact side's with the FACT side's, never one side's delete files with the other side's credentials
* *AND* the UDF MUST NOT re-authenticate to the catalog, re-request vended credentials, or resolve the referenced CONNECTION a second time to read a delete file, because the one per-invocation resolution already supplied it
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

* *GIVEN* a scan whose delete-carrying data files' parsed footers do not all fit in the session metadata cache, so a footer that access-plan construction fetched is not available from that cache when the Parquet opener reads it, because the entry was evicted, or because it exceeded the cache limit and was never admitted
* *WHEN* the opener fetches that data file's footer a second time within the same scan invocation
* *THEN* the UDF SHALL count that second footer fetch as a metadata-cache re-fetch, per scan invocation
* *AND* the UDF SHALL surface the accumulated re-fetch count on its debug diagnostic channel, so an operator running the scan at debug level observes the double-fetch directly instead of inferring it from object-store request volume
* *AND* a scan whose footers all stay cached SHALL report a re-fetch count of ZERO, so the signal distinguishes eviction from normal operation rather than firing on every delete-carrying scan
* *AND* the observable MUST stay inert otherwise: at the production default debug level it MUST NOT emit output, it MUST NOT alter the scan's result rows, it MUST NOT fail the scan (an eviction is a cost signal, not an error), and its record MUST NOT contain a storage credential
