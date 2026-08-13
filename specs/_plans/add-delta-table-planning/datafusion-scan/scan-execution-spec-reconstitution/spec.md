# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* **This delta adds ONE scenario and is issue #319.** It records the wire shape the Delta table
  format adds to both arguments: a table-level Delta block on the shard-invariant common spec, and a
  per-file Delta block on each file-list entry.
* **No recorded clause is superseded.** The Delta blocks are OPTIONAL and absent from JSON when
  absent in the value, so the recorded byte-identity guarantees hold unedited: the common blob for a
  non-join Iceberg spec stays byte-identical to its pre-consolidation encoding, and the per-shard
  files list stays byte-identical for both the legacy 2-tuple and the delete-carrying 3-tuple forms.
* **The recorded no-catalog-identifier rule governs the new blocks and is satisfied by
  construction.** Everything the Delta blocks carry is scan-time DATA — a path, a serialized
  partition value, a deletion-vector byte range, a physical column name — never a catalog handle,
  because the scan UDF never contacts the catalog. The table's catalog-assigned vending key stays in
  the planning layer and MUST NOT reach the scan spec.
* **There is no cross-version wire-compatibility requirement**, as this feature already records: one
  `.so` produces and consumes the spec within one deploy. The Delta wire shape is chosen for
  Iceberg-side byte identity, not for reading a spec written by an older build.
* Producing these blocks is `vs-adapter/delta-table-planning`; consuming them — applying the deletion
  vector, injecting partition values, and resolving column mapping — is issue #320.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Reconstitution carries the Delta table block and per-file Delta blocks

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying a Delta table
  block — the column-mapping mode, the ordered per-column logical name, physical name, and physical
  id, and the ordered partition-column names — and whose second argument is a JSON array of per-shard
  file entries, each carrying a data-file path, its byte size, its partition values, and at most one
  deletion-vector reference
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize both arguments and MERGE them into one scan spec whose per-shard
  files carry their own Delta block from the second argument and whose Delta TABLE block and every
  other shard-invariant field come from the first
* *AND* the merge SHALL store each data-file path and each deletion-vector `pathOrInlineDv` verbatim
  without resolving either, so path reconstruction stays deferred to file registration
* *AND* a file entry's partition values SHALL distinguish a partition column whose value is NULL from
  one that is absent from the map, because a NULL partition value is a value the scan materializes and
  an absent column is a planning defect
* *AND* the Delta TABLE block and the per-file Delta block SHALL each be OPTIONAL and absent from
  JSON when absent in the value, so an Iceberg common blob and an Iceberg file-list entry serialize
  byte-identically to their pre-Delta encoding and every committed golden fixture passes unedited
* *AND* a file-list entry carrying a Delta block SHALL be a self-describing JSON OBJECT rather than a
  fourth tuple slot, so the recorded 2-tuple legacy form and 3-tuple delete-carrying form keep their
  exact encodings and their deserialization precedence
* *AND* the round trip SHALL be LOSSLESS in both directions for every combination the type admits, so
  no field is silently dropped by the shortest-form serialization rule
* *AND* the reconstituted scan spec MUST NOT carry the table's catalog-assigned credential-vending
  key or any other catalog identifier field, because the scan UDF never contacts the catalog
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or session token
<!-- /DELTA:NEW -->
