# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard `(path,
size)` file entry into a registered, absolute, sized file — without issuing a per-file
object-store `HEAD` request the adapter's already-resolved size makes redundant — and extends
the same no-HEAD guarantee to the associated positional-delete files.

## Background

* Each per-shard file entry carries the file's byte size; the UDF constructs each assigned
  file's object metadata from that size and MUST NOT issue a per-file object-store metadata
  (`HEAD`) request to re-discover a size the adapter already resolved.
* Building a data file's base `ParquetAccessPlan` needs its per-row-group row counts, obtained
  by reading the Parquet footer via a range GET (not a HEAD), ideally parsed once and reused.
* See `datafusion-scan/scan-execution` for the overall scan invocation and registration flow.

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Data-file Parquet footer is read via a range GET, not a HEAD, and not twice

* *GIVEN* a data file that carries positional deletes, whose per-row-group row counts are needed to build the base `ParquetAccessPlan`
* *WHEN* the scan UDF constructs the access plan for that data file
* *THEN* the UDF SHALL obtain the per-row-group row counts by reading the Parquet footer via an object-store range GET (the file size is already known from the spec), and MUST NOT issue a HEAD request
* *AND* the UDF SHALL supply that read with a metadata size hint, so a data file whose Parquet footer and metadata fit within the hint costs EXACTLY ONE object-store range GET rather than a suffix probe followed by a second GET, and that hint SHALL be the SAME value the Parquet opener uses for its own footer reads — read back from the one `ParquetFormat` the provider both builds access plans against and hands to the physical-plan builder, so the two sites cannot drift and no separate constant and no operator-facing knob is introduced
* *AND* the UDF SHALL request only the metadata the access plan consumes — the per-row-group row counts — and MUST NOT additionally request the Parquet page index, which access-plan construction never reads
* *AND* the UDF SHOULD parse each data file's footer at most once per scan, reusing the parsed metadata for both access-plan construction and the Parquet opener (via a shared reader factory / cached metadata) rather than reading the footer twice
* *AND* attaching positional deletes to a data file MUST NOT increase the number of object-store requests issued against THAT data file, compared with the same scan of the same file with no deletes attached, measured with a non-empty `logical_schema` — the production configuration in which no `ParquetFormat::infer_schema` pass pre-populates the metadata cache, since measuring it under the legacy inference fallback proves nothing about the access-plan fetch
* *AND* the emitted rows SHALL be identical regardless of how many times the footer is physically fetched
<!-- /DELTA:CHANGED -->
