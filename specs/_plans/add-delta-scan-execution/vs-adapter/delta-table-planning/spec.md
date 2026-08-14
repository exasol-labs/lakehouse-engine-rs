# Feature: Delta Table Planning

Resolves a Delta Lake table into the engine's existing `ScanSpec` shape at plan time, so file-level
sharding, the pushdown wire format, streaming emit, and the memory model are reused for the Delta
path exactly as they already are for Iceberg.

## Background

* **This delta is issue #320.** The Delta reader's own resolution behavior — log replay, partition
  values, deletion-vector descriptors, column-mapping binding keys, credential vending, and type
  refusal — is unchanged. What changes is that production pushdown now reaches it.
* The Iceberg reader stops delegating and owns its resolution logic, closing the collapse this
  feature's recorded contract scheduled for #320.
* Two recorded deferrals stay deferred and are restated here as scoped exceptions: filter-based file
  pruning is issue #321, and Delta reader-feature gating with broad type mapping is issue #322.
* **Percent-decoding of `add.path` is VERIFIED, not assumed (task 5.1).** `delta_kernel` 0.26 leaves
  `add.path` percent-encoded on the `scan_row` `path` column this reader reads
  (`DeltaSnapshot::active_files` in `delta_replay.rs`); its own reference `DefaultEngine` only decodes
  it later, at the URL-to-object-store-path boundary (`Path::from_url_path`,
  `delta_kernel_default_engine::parquet` — e.g. `src/parquet.rs:433`). This reader's own path,
  `reconstruct_abs_uri` joined through `ListingTableUrl::parse` (`store_path` in
  `crates/lakehouse-engine/src/scan/store_router.rs`, and the identical construction in
  `index_file_sizes`/`object_meta_for`), reaches that exact same `Path::from_url_path` decode inside
  `datafusion-datasource`'s `ListingTableUrl::try_new`, so every object-store request this reader
  issues already carries the DECODED path. Covered by
  `store_path_decodes_a_percent_encoded_entry_path` in `store_router_tests.rs`. No gap; no tracked
  issue needed.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Iceberg planning is byte-identical through the new seam

* *GIVEN* the shipped Iceberg file-resolution logic and its former callers — the single-table pushdown
  path, every join leg, and the external test callers
* *WHEN* the Iceberg format reader resolves a table's scan through the trait
* *THEN* the reader SHALL OWN that resolution logic outright: the separately published
  `resolve_file_list` entry point SHALL be deleted and its body SHALL live in the reader, SUPERSEDING
  the recorded rule that its name, `pub` visibility, signature, and call sites stay unchanged, because
  `vs-adapter/pushdown-format-neutral-resolution` routes every former caller through this seam
* *AND* the reader SHALL construct each associated positional-delete reference as the Iceberg
  positional-delete variant of the format-neutral delete mechanism, and its SERIALIZED per-shard file
  list SHALL stay byte-identical to the pre-#342 encoding, including the delete-carrying 3-tuple form
  and its `{"path":…,"size":…,"content_type":"position_deletes"}` member encoding
* *AND* the returned scan SHALL carry EMPTY partition columns, and each of its file entries SHALL carry
  EMPTY partition values, so the serialized shard-invariant common blob and per-shard file list for
  every Iceberg request stay byte-identical to their pre-#342 encoding
* *AND* every logical field the Iceberg reader emits SHALL carry its Iceberg field-id and NO physical
  name, so an Iceberg column is still bound by field-id and the physical-name and identity binding
  strategies are unreachable from the Iceberg path
* *AND* the existing Iceberg unit, integration, and E2E suites MUST pass with no change to any test
  assertion or expected value
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Delta planning adds no production pushdown path in this plan

* *GIVEN* the recorded rule that a Unity Catalog pushdown is refused before any catalog client, credential, or file resolution, and that the Delta path is reachable from its own tests alone
* *WHEN* a pushdown request arrives whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`
* *THEN* this scenario SHALL be REMOVED, because every one of its clauses asserts the absence of the production pushdown path that issue #320 exists to add
* *AND* it SHALL be REPLACED by "The Delta reader is reached from production pushdown under the Unity Catalog kind" below, which restates the #321 and #322 deferrals and the credential-redaction requirement under the new routing rule
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind

* *GIVEN* a virtual schema created with `CATALOG_KIND` set to `UNITY_CATALOG`, and a query against one
  of its Delta tables
* *WHEN* the adapter handles the resulting pushdown request
* *THEN* the adapter SHALL select the Delta format reader through the scan-source seam and SHALL plan
  the query from the `ResolvedScan` that reader returns, SUPERSEDING the recorded rule that the Delta
  path is reachable from its own tests alone
* *AND* the reader's resolved partition columns SHALL reach the shard-invariant common spec and its
  per-file partition values SHALL reach the per-shard file entries, so the deferred scan-side partition
  reconstruction this reader's contract names is satisfied by
  `datafusion-scan/scan-execution-partition-values` rather than left open
* *AND* the reader SHALL still apply NO filter-based file pruning, because per-file statistics and
  partition pruning remain issue #321, so a filter narrows the rows the scan emits without narrowing
  the files it reads
* *AND* the reader SHALL still perform NO Delta reader-feature gating, because gating remains issue
  #322; a table whose reader features this engine does not implement is therefore query-reachable and
  its correctness is bounded by #322 rather than by a refusal, which this feature records as a known,
  scoped exception rather than leaving unstated
* *AND* every error the reader surfaces on this path MUST be returned as an error value, never raised
  as a panic, and MUST NOT contain any vended or static credential value
<!-- /DELTA:NEW -->
