# Feature: Delta Reader-Feature Gating

Refuses a Delta table whose reader protocol version or reader-feature set this engine does not
implement end to end, at plan time and before any log replay, so an unsupported table returns a named
error rather than silently wrong rows.

## Background

* **This delta is issue #349.** It moves `typeWidening` and `typeWidening-preview` from the refused
  remainder onto the allow-list, growing it from five reader features to seven, and removes the
  bespoke refusal text that cited #349 as a tracked gap. Nothing else about the gate changes: the
  protocol-version range, the legacy-protocol pass, the malformed-version-3 refusal, the
  one-error-names-every-refusal rule, and the gate's position inside `DeltaSnapshot` construction all
  stay exactly as recorded.
* **The recorded justification for refusing `typeWidening` was WRONG, and this delta supersedes it.**
  This feature records that *"`DeltaSnapshot::active_files` builds its kernel scan with
  `.without_row_transforms()`, so no per-file cast transform is applied. A widened column is then
  read with each older data file's OLD physical Parquet type against the table's NEW logical type —
  wrong values, no error."* Both halves are false. `delta_kernel` 0.26's own documentation scopes
  `without_row_transforms()` to *"partition column injection, column-mapping renames, and generated
  row ids"*, and `delta_kernel` 0.26 implements NO type-widening cast anywhere — its
  `TableFeature::TypeWidening` handling is a capability declaration and a schema-comparison
  validator, never a cast. There was no cast transform for that call to discard. The widening cast is
  performed by this engine's OWN format-neutral column-binding adapter chain, which registers the
  DataFusion table schema from the scan spec's logical schema and lets DataFusion's
  `DefaultPhysicalExprAdapter` insert the physical-to-logical cast per file.
  `datafusion-scan/type-relaxation` owns that mechanism and its verification.
* **The refusal was nevertheless the right call at the time, and lifting it is a verification result
  rather than a reversal.** Issue #322 refused `typeWidening` because the widening pairs were
  UNVERIFIED, not because a defect had been observed. This plan verifies every pair in the Delta
  protocol's supported list against a real narrow-physical file read under a widened logical schema,
  which is what converts "unverified" into "allow-listed".
* **The Delta Lake protocol specification states the reader obligation the allow-list now accepts.**
  From `delta-io/delta`, `PROTOCOL.md`, `master`, § Reader Requirements for Type Widening: *"Readers
  must allow reading data files written before the table underwent any supported type change, and
  must convert such values to the current, wider type."* and *"Readers must validate that they
  support all type changes in the `delta.typeChanges` field in the table schema for the table version
  they are reading and fail when finding any unsupported type change."* The FIRST obligation is met
  by `datafusion-scan/type-relaxation`; the SECOND is a per-column validation met by
  `vs-adapter/delta-type-mapping`, which owns the refused-column mechanism it reuses.
* **Both feature names carry the same protocol floors and the same behavior.** `typeWidening` and
  `typeWidening-preview` are each reader-and-writer features at Reader Version 3 and Writer Version
  7. The preview name identifies the client that enabled the feature (Delta Lake 3.2), not a
  different read contract — which is why the allow-list admits both and the per-change validation
  reads the actual `fromType`/`toType` pairs rather than branching on the feature name.
* **The allow-list stays the ONLY enumeration in production code.** Adding two variants to the
  matched set leaves the default-deny `_` remainder untouched, so a `delta_kernel` upgrade that adds
  a `TableFeature` variant still refuses it rather than admitting it unreviewed.
* **`describe_refused_feature`'s `typeWidening` arm becomes unreachable and is removed rather than
  left in place.** Once both variants are allow-listed they can never reach the refusal formatter, so
  the arm would be dead code whose only effect is to make a reader believe the refusal still exists.
* **Apache Iceberg spec check — this delta changes no Iceberg behavior.** It moves two entries of a
  Delta protocol allow-list and adds no code on the Iceberg resolution path. The Iceberg table spec's
  Column Projection rules and the recorded deviation on its ordered-resolution rule (1) in
  `datafusion-scan/scan-execution-field-id-projection` are untouched. Iceberg's own type promotions
  are `vs-adapter/iceberg-type-promotion`'s scope.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A reader feature outside the allow-list refuses the table before any log replay

* *GIVEN* a Delta table whose `protocol` action declares `minReaderVersion` 3 and a `readerFeatures`
  list holding at least one feature outside this engine's allow-list — one of exactly
  `variantType`, `variantType-preview`, `variantShredding`, `variantShredding-preview`,
  `catalogManaged`, `catalogOwned-preview`, `adaptiveMetadata-preview`, or a name `delta_kernel` 0.26
  does not recognize and surfaces as `TableFeature::Unknown`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return a `UdfError` naming the table root and EVERY refused feature by its
  Delta protocol name — the `readerFeatures` spelling, so `variantType-preview` is reported as
  `variantType-preview` and an unrecognized name is reported verbatim
* *AND* the reader SHALL name every refused feature in ONE error rather than the first alone, so a
  table carrying two unsupported features is diagnosed in one query instead of two
* *AND* the refusal formatter SHALL carry NO per-feature special case, because the only one it ever
  held was the `typeWidening` issue-#349 citation and both type-widening variants are now
  allow-listed — a formatter arm for an allow-listed feature is unreachable code that reads as a
  refusal still in force
* *AND* the reader MUST NOT replay one Delta log commit, read one checkpoint, list one object-store
  prefix, or build the table's logical schema after the refusal, because the refusal exists to stop
  an unsupported table before it costs object-storage access
* *AND* the reader MUST NOT rely on `delta_kernel` raising the refusal, because
  `delta_kernel`'s own `ensure_operation_supported(Operation::Scan)` reports support for every reader
  feature its enum parses and reads both the `type-widening` and `unshredded-variant` fixtures
  without error
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Every allow-listed reader feature keeps its table queryable

* *GIVEN* a Delta table whose `readerFeatures` list holds only features drawn from this engine's
  allow-list, which is exactly `columnMapping`, `deletionVectors`, `timestampNtz`, `typeWidening`,
  `typeWidening-preview`, `v2Checkpoint`, and `vacuumProtocolCheck`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL pass the gate and resolve the table's schema, partition columns, and active
  file list unchanged
* *AND* `typeWidening` and `typeWidening-preview` SHALL both be admitted, because they name the same
  read contract at the same Reader Version 3 floor and differ only in which client enabled the
  feature — admitting one and refusing the other would refuse a table this engine reads correctly
* *AND* admitting the feature SHALL NOT by itself admit every type change a table records: a table
  whose `delta.typeChanges` names a change this engine cannot perform is still refused, per column,
  by `vs-adapter/delta-type-mapping` — the protocol gate answers whether the FEATURE is implemented
  and the schema gate answers whether each recorded CHANGE is
* *AND* the gate MUST NOT refuse a table because its `writerFeatures` list holds a feature outside the
  allow-list, because the protocol obliges a READER to respect `readerFeatures` alone and a
  write-only feature changes nothing this engine reads
* *AND* the allow-list SHALL be the ONLY enumeration in the production code: the gate SHALL match the
  seven allow-listed variants and refuse the `_` remainder, so a `delta_kernel` upgrade that adds a
  `TableFeature` variant refuses it rather than admitting it unreviewed
* *AND* every shipped Delta fixture whose reader features are wholly allow-listed SHALL keep passing
  every scenario already recorded for it in
  `datafusion-scan/scan-execution-delta-deletion-vectors`,
  `datafusion-scan/scan-execution-partition-values`, and
  `e2e-harness/unity-catalog-e2e-harness-delta-queries` — namely `table_with_dv` and
  `multi_part_stats` (`deletionVectors`) and `stats_all_types` (`timestampNtz` + `columnMapping`) —
  and the `type_widening` fixture SHALL join that set rather than staying refused
<!-- /DELTA:CHANGED -->
