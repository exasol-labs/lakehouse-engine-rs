# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* The scan UDF's first argument is the shard-invariant common spec (projection, filter,
  limit, aggregates, group keys, logical schema, EMITS types, storage credentials, the
  Iceberg table root, and tuning knobs), serialized once per fan-out; the second argument is
  this shard's file list. See `datafusion-scan/scan-execution` for the scan behavior once the
  spec is merged.
* The per-shard file list is a JSON array of compact `[path, size]` 2-tuples, where `path` is
  either relative to the common spec's table root or an absolute URI, and `size` is the file's
  byte size resolved from the Iceberg manifest by the adapter.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* A parse failure on either argument MUST surface an error identifying scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or
  session token.
* Per-file positional-delete references travel with their data-file entry in the per-shard
  argument.
* This delta amends ONE clause of the two-argument-wire scenario and nothing else. `vs-adapter/storage-backend-enum` (issue #274) wraps the common blob's `storage` value in an externally-tagged backend variant, so the clause requiring the common blob to be byte-identical to the pre-consolidation encoding needs the `storage` value carved out. Every other scenario of this feature is unchanged, and no Background bullet is superseded.
* The carve-out is safe on this feature's own recorded terms: the legacy-file-list scenario already states that "the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)". The tag is therefore a self-consistent intra-deploy encoding change, not a compatibility break, and that bullet's reasoning is unchanged.
* The per-shard file-list argument (arg 1) is untouched by the tag: `storage` is shard-invariant and lives only in the common blob.
* **This delta widens ONE carve-out in the two-argument-wire scenario and nothing else.** Issue #294 adds a REQUIRED `storage` field to the common blob's join block, so the clause requiring the common blob to be byte-identical to the pre-consolidation encoding needs that field carved out alongside the whole-spec `storage` value already carved out for `vs-adapter/storage-backend-enum`. Every other scenario of this feature is unchanged and no Background bullet is superseded.
* **The carve-out is safe on this feature's own recorded terms.** The legacy-file-list scenario already states that "the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)", so adding a required field inside the join block is a self-consistent intra-deploy encoding change, not a compatibility break.
* **The field is REQUIRED rather than defaulted, deliberately.** A `#[serde(default)]` on the join block's storage would let a join block that names no dimension backend deserialize into one that silently reuses the whole-spec (fact-side) backend — reinstating exactly the collapse issue #294 removes. Making it required turns "every join block names its own backend" into a property of the type rather than a rule an auditor has to verify at each of the seven `JoinSpec` construction sites.
* **The per-shard files-list argument (arg 1) is untouched.** The join block, and therefore its storage backend, is shard-invariant and lives only in the common blob.
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

### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the Iceberg table root) and whose second argument is a JSON array of `[path, size]` 2-tuples
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard file-list JSON and MERGE them into one `ScanSpec` value whose `files` are the `(path, size)` entries from the second argument and whose every other field — including the table root — comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each file entry's path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Reconstitution carries per-file positional-delete references

* *GIVEN* a scan invocation whose second argument is a JSON array of per-shard file entries, each carrying a data-file path, its byte size, and zero or more associated positional-delete file references (each with a path, byte size, and delete content type)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each file entry together with its associated delete-file references and MERGE them into one scan spec whose per-shard files (with deletes) come from the second argument and whose every other field comes from the first
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value and, when a join block is present, that block's own `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the `storage` value SHALL be the externally-tagged storage-backend encoding specified by `vs-adapter/storage-backend-enum`, whose tagged payload is itself byte-identical to the pre-consolidation `storage` object, so the variant tag is the ONLY difference from the pre-consolidation common blob
* *AND* the join block's `storage` value SHALL use that SAME externally-tagged encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* a common blob carrying NO join block SHALL be byte-identical to its pre-change encoding, so every committed golden common-blob fixture for a non-join spec passes unedited
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` is shard-invariant and appears only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy

### Scenario: A file-list argument that predates the delete encoding still reconstitutes

* *GIVEN* a scan invocation whose second argument holds legacy file entries that carry a path and byte size but NO delete-file references (a spec that predates positional-delete support)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each legacy entry with its associated delete list defaulting to empty, so the entry reconstitutes as a delete-free data file
* *AND* a missing table root SHALL still be treated as "all paths are absolute" so no path is joined onto a root
* *AND* the resulting scan spec SHALL be usable by the shared scan path unchanged, because the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)

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
