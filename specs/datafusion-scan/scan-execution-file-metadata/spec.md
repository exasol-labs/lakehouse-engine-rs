# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard `(path,
size)` file entry into a registered, absolute, sized file — without issuing a per-file
object-store `HEAD` request the adapter's already-resolved size makes redundant — and extends
the same no-HEAD guarantee to the associated positional-delete files.

## Background

* Each per-shard file entry carries the file's byte size; the UDF constructs each assigned
  file's object metadata from that size and MUST NOT issue a per-file object-store metadata
  (`HEAD`) request to re-discover a size the adapter already resolved. Every data-file and
  delete-file byte size is authoritative from the per-shard spec entry, so the UDF never
  issues a HEAD to discover a size for either.
* A per-shard file path is resolved to an absolute URI before registration: an entry that is
  already absolute (contains a `://` scheme) passes through unchanged; a relative entry is
  joined onto the common spec's table root (normalizing the trailing `/`). Delete-file paths
  follow the same rule as data-file paths.
* When the common spec carries an empty table root, every entry is treated as absolute and
  none are joined.
* Field-id-based column projection (`datafusion-scan/scan-execution-field-id-projection`) is
  preserved regardless of how per-file metadata is supplied.
* Building a data file's base `ParquetAccessPlan` needs its per-row-group row counts, obtained
  by reading the Parquet footer via a range GET (not a HEAD), ideally parsed once and reused.
* See `datafusion-scan/scan-execution` for the overall scan invocation and registration flow.
* **The empty-table-root clauses are retained as a wire-format totality property, not as a
  reachable path.** `vs-adapter/pushdown-planning-file-resolution` now rejects a `loadTable` response carrying
  an empty table metadata `location` before the vended/static storage split, so the adapter can
  no longer emit a common spec whose table root is empty. This feature's three empty-table-root
  clauses — two normative `SHALL` clauses and one descriptive Background bullet — therefore
  describe an input the current adapter cannot produce. Those three clauses are the recorded
  Background bullet beginning "When the common spec carries an empty table root" and the final
  clause of each of the two scenarios reproduced below. They are retained deliberately and their
  text is UNCHANGED: they make the path-resolution rule a total function over the wire format, so
  a scan spec reaching the UDF with an empty root still resolves deterministically instead of
  joining paths onto nothing. They MUST NOT be deleted or converted into an error — an empty root
  is unreachable from a `loadTable` response, which makes the branch unreachable rather than dead,
  and the UDF is not the component that validates a catalog response. The scan-side rejoin this
  property governs is `reconstruct_abs_uri`
  (`crates/lakehouse-engine/src/scan/object_store.rs:250`).
* A footer read without a size hint costs TWO object-store round-trips, not one. DataFusion's
  `DFParquetMetadata` drives a push decoder that first requests the last 8 bytes to learn the
  metadata length, then requests the metadata range itself. A `metadata_size_hint` of N
  speculatively fetches the file's last N bytes up front, collapsing both rounds into ONE request
  whenever the footer and metadata fit inside N. Access-plan construction passed no hint and so
  paid both rounds per delete-carrying data file. Issue
  [#165](https://github.com/exasol-labs/lakehouse-engine-rs/issues/165).
* The hint is NOT a new constant and NOT a new operator knob. DataFusion already carries one:
  `datafusion.execution.parquet.metadata_size_hint`, default `Some(512 * 1024)`, held on the
  `ParquetFormat` this provider owns. `ParquetFormat::create_physical_plan` copies that same value
  onto the `ParquetSource` the opener uses, so reading the hint back off the SAME `ParquetFormat`
  for access-plan construction gives both sites one value that cannot drift.
* An undersized hint is not a correctness problem and not a new risk class. When the footer
  exceeds the hint the decoder re-requests the full metadata range, costing the same number of
  round-trips as no hint plus the wasted prefetch bytes — and the delete-free scan path already
  accepts exactly that trade at exactly this value for every file it opens.
* Access-plan construction reads only the per-row-group ROW COUNTS. It never reads the Parquet
  page index. Requesting the page index alongside the footer would add a round-trip and inflate
  the cached entry for metadata the access plan discards, so the request must be scoped to what
  the access plan consumes.
* **The pre-existing footer-once guard was vacuous on the production path, and this delta makes
  it real.** `scan_reads_footer_via_range_get_once`
  (`crates/lakehouse-engine/tests/scan_no_head_test.rs`) builds its spec with an EMPTY
  `logical_schema`, which sends `register_file_list` down the legacy `ParquetFormat::infer_schema`
  fallback. Inference fetches and caches the footer BEFORE access-plan construction runs, so the
  access-plan fetch the test means to measure is a pure cache hit and the assertion holds no
  matter how many round-trips that fetch would otherwise cost. Production never takes that branch:
  the adapter supplies a logical schema, no inference runs, and access-plan construction is the
  FIRST reader of the footer. The test must therefore carry a non-empty `logical_schema` to
  exercise the configuration the scan actually runs in.

## Scenarios

### Scenario: Scan builds file metadata from the spec and issues no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries every assigned file's byte size alongside its path
* *WHEN* the scan UDF registers its assigned files and builds the scan
* *THEN* the UDF SHALL construct each assigned file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (`HEAD`) request to discover a file's size before scanning, because the size is authoritative from the spec
* *AND* the rows the UDF emits SHALL be identical to those produced when the size is instead discovered from object storage, so supplying the size changes only the pre-scan metadata round-trips, never the result

### Scenario: Relative paths resolve against the table root and absolute paths pass through

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative entries (paths under that root) with at least one absolute entry (a path not under the root, carrying its own `://` scheme)
* *WHEN* the scan UDF resolves its assigned files for registration
* *THEN* the UDF SHALL join each relative entry onto the table root (normalizing the boundary `/`) to form the absolute URI, and SHALL pass each already-absolute entry through unchanged
* *AND* the set of registered absolute file URIs SHALL equal the original resolved data-file URIs the adapter partitioned into this shard
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every entry as absolute and join none of them
* *AND* this rule SHALL apply to a spec produced by EITHER format reader, because the table root is a neutral field both populate

### Scenario: Delete files also carry their size and incur no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries, for each associated positional-delete file, its byte size alongside its path
* *WHEN* the scan UDF reads a data file's associated delete files
* *THEN* the UDF SHALL construct each delete file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (HEAD) request for a delete file to discover its size, because the size is authoritative from the spec
* *AND* the emitted rows SHALL be identical to those produced when a delete file's size is instead discovered from object storage

### Scenario: Data-file Parquet footer is read via a range GET, not a HEAD, and not twice

* *GIVEN* a data file that carries positional deletes, whose per-row-group row counts are needed to build the base `ParquetAccessPlan`
* *WHEN* the scan UDF constructs the access plan for that data file
* *THEN* the UDF SHALL obtain the per-row-group row counts by reading the Parquet footer via an object-store range GET (the file size is already known from the spec), and MUST NOT issue a HEAD request
* *AND* the UDF SHALL supply that read with a metadata size hint, so a data file whose Parquet footer and metadata fit within the hint costs EXACTLY ONE object-store range GET rather than a suffix probe followed by a second GET, and that hint SHALL be the SAME value the Parquet opener uses for its own footer reads — read back from the one `ParquetFormat` the provider both builds access plans against and hands to the physical-plan builder, so the two sites cannot drift and no separate constant and no operator-facing knob is introduced
* *AND* the UDF SHALL request only the metadata the access plan consumes — the per-row-group row counts — and MUST NOT additionally request the Parquet page index, which access-plan construction never reads
* *AND* the UDF SHOULD parse each data file's footer at most once per scan, reusing the parsed metadata for both access-plan construction and the Parquet opener (via a shared reader factory / cached metadata) rather than reading the footer twice
* *AND* attaching positional deletes to a data file MUST NOT increase the number of object-store requests issued against THAT data file, compared with the same scan of the same file with no deletes attached, measured with a non-empty `logical_schema` — the production configuration in which no `ParquetFormat::infer_schema` pass pre-populates the metadata cache, since measuring it under the legacy inference fallback proves nothing about the access-plan fetch
* *AND* the emitted rows SHALL be identical regardless of how many times the footer is physically fetched

### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
