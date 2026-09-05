# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* The scan UDF's first argument is the shard-invariant common spec (projection, filter,
  limit, aggregates, group keys, logical schema, EMITS types, a storage reference the
  UDF resolves itself, the table root, and tuning knobs), serialized once per fan-out;
  the second argument is this shard's file list. See `datafusion-scan/scan-execution` for the scan behavior once the
  spec is merged.
* The per-shard file list is a JSON array of compact `[path, size]` 2-tuples, where `path` is
  either relative to the common spec's table root or an absolute URI, and `size` is the file's
  byte size resolved from the table's own metadata by its format reader — an Iceberg manifest's
  `file_size_in_bytes` for an Iceberg table, a Delta `add` action's `size` for a Delta one.
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
* **This delta is issue #342.** It replaces the pair of Delta-named blocks the wire gained in #319 —
  a table block on the common spec and a per-file block on each file entry — with format-neutral
  fields both table formats populate: a per-file `partition_values` map, a per-file `deletes` list of
  self-describing delete MECHANISMS, a shard-invariant `partition_columns` list, and one binding key
  per logical field. No scan behavior changes; the wire carries the same values in neutral fields.
* **No recorded byte-identity clause needs a carve-out.** The common blob's new `partition_columns`
  is absent from JSON when empty, and the Iceberg reader leaves it empty, so a non-join Iceberg common
  blob stays byte-identical to its pre-consolidation encoding and the committed golden common-blob
  fixture passes unedited. The per-shard files list likewise stays byte-identical: the 2-tuple legacy
  form and the 3-tuple delete-carrying form keep their exact encodings, INCLUDING each Iceberg
  positional-delete member's `{"path":…,"size":…,"content_type":"position_deletes"}` object with its
  key ORDER unchanged.
* **Key order is why the delete mechanism keeps a private wire form.** A directly tagged enum would
  emit its discriminant key FIRST and reorder every Iceberg delete member, breaking the pinned
  encoding above for no behavioral gain. The public `DeleteMechanism` therefore routes
  (de)serialization through a private wire enum, exactly as `FileEntry` already routes through
  `FileEntryWire`, so the neutral Rust-level type and the frozen JSON encoding are independent
  decisions with one owner each.
* **The object file-entry form is now selected by partition values, not by format.** An entry
  serializes as the compact 2-tuple when it carries neither deletes nor partition values, as the
  3-tuple when it carries deletes and no partition values, and as a self-describing JSON OBJECT
  whenever it carries partition values. A Delta entry whose only extra content is a deletion vector
  therefore rides in the 3-tuple form — correctly, because the delete member itself names its
  mechanism.
* **The mutual-exclusion gate narrows to the real hazard.** #319 refused any entry carrying a Delta
  block AND a non-empty Iceberg delete list. The neutral gate refuses an entry whose ONE delete list
  MIXES a deletion vector with an Iceberg delete-file reference. Partition values are no longer part
  of the test, because an Iceberg table with identity-transform partition values and positional
  deletes (issue #99) is a legitimate future shape rather than a defect.
* **The recorded no-catalog-identifier rule governs the neutral fields and is satisfied by
  construction**, unchanged: a partition value, a partition-column name, a physical column name, a
  path, and a deletion-vector byte range are all scan-time DATA, never a catalog handle.
* **There is still no cross-version wire-compatibility requirement** — one `.so` produces and consumes
  the spec within one deploy. The neutral wire shape is chosen for Iceberg-side byte identity, not for
  reading a spec written by an older build.
* **This delta is issue #135. It amends ONE scenario and changes no reconstitution rule.** The two-argument contract, the per-shard `[path, size]` encoding, the positional-delete 3-tuple encoding, the legacy-entry defaulting, the neutral partition values, and the no-catalog-block rule are all UNCHANGED. What changes is what the `storage` value holds.
* **The `storage` value now carries a further enclosing wrapper whose reference variant holds no backend at all**, specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES.
* **A common blob carrying no join block is NO LONGER byte-identical to its pre-change encoding: its `storage` value gains the wrapper.** Every committed golden common-blob fixture for a non-join spec that carries a `storage` value is regenerated; the six `empty_*` fixtures carry no `storage` value at all and stay byte-identical.
* **The per-shard files-list argument is still byte-identical**, because `storage` is shard-invariant and appears only in the common blob.

## Scenarios

### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the table root) and whose second argument is a JSON array of `[path, size]` 2-tuples
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard file-list JSON and MERGE them into one `ScanSpec` value whose `files` are the `(path, size)` entries from the second argument and whose every other field — including the table root — comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each file entry's path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
* *AND* the merge SHALL follow these same rules for arguments produced by EITHER format reader, because the table root and each entry's byte size are neutral values both populate

### Scenario: Reconstitution carries per-file positional-delete references

* *GIVEN* a scan invocation whose second argument is a JSON array of per-shard file entries, each carrying a data-file path, its byte size, and zero or more associated delete mechanisms (an Iceberg positional-delete reference carrying a path, byte size, and delete content type)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each file entry together with its associated delete mechanisms and MERGE them into one scan spec whose per-shard files (with deletes) come from the second argument and whose every other field comes from the first
* *AND* each Iceberg positional-delete member SHALL deserialize from, and re-serialize to, its pre-#342 `{"path":…,"size":…,"content_type":"position_deletes"}` encoding with its key order unchanged, so every committed 3-tuple golden encoding passes unedited
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value and, when a join block is present, that block's own `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the `storage` value SHALL be the externally-tagged scan-spec storage WRAPPER specified by `vs-adapter/scan-spec-credential-reference` — a `connection` reference variant carrying a name and `allow_http` and no credential; a `sealed` variant carrying a connection name and the base64 nonce-plus-AES-GCM-ciphertext of the externally-tagged storage-backend encoding of `vs-adapter/storage-backend-enum`, which is byte-identical to the pre-consolidation `storage` object once unsealed; or an `inline` variant whose payload is that same backend encoding in plaintext, emitted by no adapter path and accepted for host-test spec construction
* *AND* the join block's `storage` value SHALL use that SAME wrapper encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* a common blob carrying NO join block SHALL be byte-identical to its pre-change encoding EXCEPT for the `storage` value's wrapper, so a committed golden common-blob fixture for a non-join spec passes unedited only when it carries no `storage` value and is REGENERATED when it does
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` is shard-invariant and appears only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy

### Scenario: A file-list argument that predates the delete encoding still reconstitutes

* *GIVEN* a scan invocation whose second argument holds legacy file entries that carry a path and byte size but NO delete-file references (a spec that predates positional-delete support)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each legacy entry with its associated delete list defaulting to empty, so the entry reconstitutes as a delete-free data file
* *AND* a missing table root SHALL still be treated as "all paths are absolute" so no path is joined onto a root
* *AND* the resulting scan spec SHALL be usable by the shared scan path unchanged, because the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)

### Scenario: Reconstitution carries neutral partition values and a neutral delete mechanism list

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying the table's
  ordered partition-column names and a logical schema whose fields carry a field-id, a physical name,
  or neither, and whose second argument is a JSON array of per-shard file entries, each carrying a
  data-file path, its byte size, its partition values, and a delete list holding either Iceberg
  positional-delete references or one deletion-vector descriptor
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize both arguments and MERGE them into one scan spec whose per-shard
  partition values and delete mechanisms come from the second argument and whose partition-column
  names, logical schema, and every other shard-invariant field come from the first
* *AND* the merge SHALL store each data-file path and each deletion-vector `pathOrInlineDv` verbatim
  without resolving either, so path reconstruction stays deferred to file registration
* *AND* a file entry's partition values SHALL distinguish a partition column whose value is NULL from
  one that is absent from the map, because a NULL partition value is a value the scan materializes and
  an absent column is a planning defect
* *AND* each delete mechanism SHALL be SELF-DESCRIBING, naming its own mechanism on the wire, so the
  scan side reads one delete list and dispatches on its content without ever asking which table format
  produced the spec
* *AND* the neutral fields SHALL each be absent from JSON when empty, so an Iceberg common blob and an
  Iceberg file-list entry serialize byte-identically to their pre-#342 encoding and every committed
  golden fixture passes unedited
* *AND* a file-list entry carrying partition values SHALL be a self-describing JSON OBJECT rather than
  a fourth tuple slot, so the 2-tuple legacy form and the 3-tuple delete-carrying form keep their
  exact encodings and their deserialization precedence
* *AND* the round trip SHALL be LOSSLESS in both directions for every combination the types admit, so
  no field is silently dropped by the shortest-form serialization rule
* *AND* an entry whose delete list MIXES a deletion vector with an Iceberg delete-file reference
  SHALL be REFUSED with an error naming the entry by index, because the two are independent delete
  mechanisms and applying both to one data file returns wrong rows; the error MUST NOT echo the raw
  input
* *AND* the reconstituted scan spec MUST NOT carry the table's catalog-assigned credential-vending key
  or any other catalog identifier field, because the scan UDF never contacts the catalog
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or session token
