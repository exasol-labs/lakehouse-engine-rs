# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from
`ctx.memory_limit()` and size the DataFusion memory pool from a *net* budget — the
per-instance limit minus a configurable container/binary overhead — scaled by a
configurable fraction, to bound the per-batch Parquet decode working set via a
configured `batch_size`, to enable Parquet row-group and page pruning so the scan
reads only the byte ranges its predicate needs, and to obtain storage credentials
from the scan spec — resolved from the referenced Exasol CONNECTION, or unsealed
from the AES-GCM envelope in which the planning layer carried the credentials it
vended, keyed from that same CONNECTION — without re-authenticating to the
catalog. The credentials or their reference and the tuning knobs travel in the
shard-invariant common spec argument, serialized once for the whole fan-out.

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
* **This delta is issue #135. It amends the two credential-passthrough scenarios, adds one, and changes nothing else.** The memory pool, the batch size, the Parquet pruning flags, the shared metadata reader, and the metadata-cache observable are all UNCHANGED. What changes is only WHERE a credential comes from before the object store is built.
* **The prohibition this feature owns is UNCHANGED.** "The UDF MUST NOT re-authenticate to the catalog or re-request vended credentials" still binds. `ctx.connection()` contacts neither the catalog nor object storage: it is one engine-local metadata request over the script-language-container protocol, answered by the database from its own catalog. No file is discovered, no snapshot is read, and no token is minted, so `specs/mission.md`'s "resolve metadata once per query, in the VS layer" is untouched — the file list, the snapshot, and any vended credential are still resolved exactly once, by the adapter.
* **The resolution is ONE step at the top of the invocation, not a lookup at each store-construction site.** A join spec carries a storage block per side, so the resolved value is a PAIR. Resolving lazily per store would read the same CONNECTION twice in one invocation and would leave the redaction secret set undefined for the window between the two reads.
* **The redaction secret set moves off the spec and onto the resolved pair, and this is the delta's one correctness trap.** SEVEN sites under `crates/lakehouse-engine/src/scan/` build such a set. Two read the union off the spec: `object_store.rs:66` and `join_scan.rs:48`, both `spec.common.all_secret_values()`. Three read the fact side off the spec directly: `partial_agg.rs:70`, `partial_agg.rs:125`, and `raw_scan.rs:54`, each `spec.common.storage.secret_values()`. Two already take a `&StorageBackend` parameter and are fed by their callers: `raw_scan.rs:224` in `register_file_list`, and `positional_deletes.rs:629` in `PositionalDeleteScanTable::new`. A spec carrying a connection NAME has no secret to yield, so leaving the set on the spec would silently disarm value-based redaction at the five spec-reading sites — a fix that reduced protection on the error path while fixing the SQL path. The set is therefore computed from the resolved backends, and the wire wrapper exposes no secret accessor so a missed site fails to compile.
* **The raw-scan and partial-aggregate paths are where a disarmed set would go unnoticed**, because they read the fact-side set directly and no recorded scenario asserts redaction on either. This delta adds that assertion.
* **`vs-adapter/scan-spec-credential-reference` owns the wire contract, the storage-only projection the UDF deserializes, the sealed vended envelope, the required grant, and the mid-query rotation consequence.** This feature CITES it and restates none of it, so the two do not drift.
* **Nothing about the store the UDF builds changes.** The resolved value is a `StorageBackend`, the same type the spec carried inline before, so the backend-dispatching registration function of `vs-adapter/storage-backend-enum`, the per-side size index, the routing decorator, and the one-store-per-side rule are all reached with a field-for-field identical input.

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
* *THEN* the UDF SHALL resolve every reference to a storage backend EXACTLY ONCE per invocation, before it builds any object store, under `vs-adapter/scan-spec-credential-reference`
* *AND* the UDF SHALL register the object store the RESOLVED storage backend names, configured from the credentials that backend holds
* *AND* the UDF MUST NOT decide the storage backend itself, derive it from a file URI scheme, or read the backend's payload outside the single backend-dispatching registration function specified by `vs-adapter/storage-backend-enum`
* *AND* when the spec also carries a join block, the UDF SHALL build a SECOND store from the join block's OWN resolved storage backend and read the dimension side's files through it, so the whole-spec backend serves only the side whose files the whole-spec `table_root` and per-shard `files` describe
* *AND* the store the UDF builds for a side SHALL answer that side's per-file metadata lookups from a size index over THAT side's files only, so one side's `head` can never be satisfied by the other side's store
* *AND* the credentials or their reference SHALL travel in the shard-invariant common spec argument, serialized once for the whole fan-out, NOT be repeated per shard — the dimension side's included, since the join block is itself shard-invariant
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials; resolving a CONNECTION by name through `ctx.connection()` is NOT such a request, because it reaches the database's own catalog rather than the table catalog and discovers no file, mints no token, and reads no snapshot
* *AND* a credential value from ANY resolved backend MUST NOT appear in any error message the UDF returns

### Scenario: Every redaction secret set in the scan path is built from the resolved backends

* *GIVEN* the sites under the scan path that build a value-based redaction secret set — the two that read the whole-spec union, the three that read the fact side off the spec, and the two that already receive a storage backend as a parameter
* *WHEN* the scan spec's storage block carries a connection reference rather than a credential
* *THEN* EVERY one of those sites SHALL take its secret set from the RESOLVED storage backend or backends, and NONE SHALL read it from the scan spec's own storage block
* *AND* the wire wrapper MUST NOT expose a secret-value accessor, so a site left reading the unresolved value fails to COMPILE rather than returning an empty set and silently disarming redaction
* *AND* an error raised on the RAW-SCAN path and an error raised on the PARTIAL-AGGREGATE path SHALL each be asserted to carry no resolved credential value, because those two paths read the fact-side set directly and no recorded scenario covered either
* *AND* a spec whose storage block carries a reference MUST NOT yield an empty secret set once resolution has run, and a test SHALL assert the set is NON-empty for a resolved reference

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

### Scenario: Positional-delete files are read with the same resolved credentials

* *GIVEN* a scan invocation whose shard-invariant common spec references or carries, per side, the storage credentials for that side
* *WHEN* the scan UDF reads a data file's associated positional-delete files from object storage
* *THEN* the UDF SHALL read the delete files through the SAME registered object store used for the data files OF THAT SIDE, configured from that side's RESOLVED backend credentials
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

* *GIVEN* a scan whose delete-carrying data files' parsed footers do not all fit in the session metadata cache, so a footer that access-plan construction fetched is not available from that cache when the Parquet opener reads it — because the entry was evicted, or because it exceeded the cache limit and was never admitted
* *WHEN* the opener fetches that data file's footer a second time within the same scan invocation
* *THEN* the UDF SHALL count that second footer fetch as a metadata-cache re-fetch, per scan invocation
* *AND* the UDF SHALL surface the accumulated re-fetch count on its debug diagnostic channel, so an operator running the scan at debug level observes the double-fetch directly instead of inferring it from object-store request volume
* *AND* a scan whose footers all stay cached SHALL report a re-fetch count of ZERO, so the signal distinguishes eviction from normal operation rather than firing on every delete-carrying scan
* *AND* the observable MUST stay inert otherwise: at the production default debug level it MUST NOT emit output, it MUST NOT alter the scan's result rows, it MUST NOT fail the scan (an eviction is a cost signal, not an error), and its record MUST NOT contain a storage credential
