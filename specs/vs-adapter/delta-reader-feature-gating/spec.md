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

The Delta Lake protocol specification (`delta-io/delta`, `PROTOCOL.md`, `master`) states the
normative obligations this feature enforces:

* Obligation — *"Readers and writers must not ignore table features when they are present"*, and
  *"to read a table, readers must implement and respect all features listed in `readerFeatures`"*.
* Where the list lives — `minReaderVersion` is *"The minimum version of the Delta read protocol that
  a client must implement in order to correctly \*read\* this table"* and is `required`;
  `readerFeatures` is *"A collection of features that a client must implement in order to correctly
  read this table (exist only when `minReaderVersion` is set to `3`)"* and is `optional`.
* Table-features gate — *"When the table's Reader Version is 3, `readerFeatures` must exist in the
  `protocol` action"*, and *"A feature is supported by a table when its name is in the `protocol`
  action's `readerFeatures` and/or `writerFeatures`."*
* Legacy column mapping — *"The table property should only be honored if the table's protocol has
  reader and writer versions and/or table features that support the `columnMapping` table feature.
  For readers this is Reader Version 2, or Reader Version 3 with the `columnMapping` table feature
  listed as supported."*
* Reader-versus-writer breadth — *"to write a table, writers must implement and respect all features
  listed in `writerFeatures`. Because writers have to read the table (or only the Delta log) before
  write, they must implement and respect all reader features as well."* The converse does NOT hold: a
  reader respects `readerFeatures` alone, which is why this read-only engine gates no writer feature.
* Per-feature reader-version floors this feature relies on — deletion vectors: *"To support Deletion
  Vectors, a table must have Reader Version 3 and Writer Version 7. A feature name `deletionVectors`
  must exist in the table's `readerFeatures` and `writerFeatures`."*; `timestampNtz`: *"To have a
  column of TimestampNtz type in a table, the table must have Reader Version 3 and Writer Version 7.
  A feature name `timestampNtz` must exist in the table's `readerFeatures` and `writerFeatures`."*;
  V2 checkpoints: *"To add V2 Checkpoints support to a table, the table must have Reader Version 3 and
  Writer Version 7. A feature name `v2Checkpoint` must exist in the table's `readerFeatures` and
  `writerFeatures`."*; `vacuumProtocolCheck`: *"The feature `vacuumProtocolCheck` must exist in the
  table `protocol`'s `writerFeatures` and `readerFeatures`."*

* **This is issue #322's gating half.** `vs-adapter/delta-table-planning` records today that the
  Delta reader "SHALL perform NO Delta reader-feature gating" and that "a table whose reader features
  this engine does not implement is therefore query-reachable and its correctness is bounded by
  #322 rather than by a refusal". This feature closes that gap; both recorded clauses are superseded
  in the same plan.
* **`delta_kernel`'s own support check is not a substitute for this gate.** `ScanBuilder::build()`
  calls `TableConfiguration::ensure_operation_supported(Operation::Scan)`, whose per-feature
  classification reports support for essentially every reader feature its enum can parse — the
  kernel's own doc comment on `TableFeature` states *"The kernel currently supports all reader
  features"*. That answers whether the KERNEL can read the LOG, not whether THIS ENGINE can interpret
  the data the log describes. Spike #325 verified the kernel reads both the `type-widening` and the
  `unshredded-variant` fixture without error, which is why the engine must inspect
  `protocol.readerFeatures` itself.
* **The gate is default-deny.** It matches an explicit allow-list and refuses every other feature, so
  a `delta_kernel` upgrade that adds a `TableFeature` variant refuses that feature by construction
  rather than admitting it silently. The refusal list in the scenario below is the enumeration as of
  `delta_kernel` 0.26, recorded so a reader can see what is refused today — the CODE enumerates the
  allow-list only.
* **`TableFeature::Unknown(_)` is `delta_kernel`'s forward-compatibility catch-all** for a
  `readerFeatures` entry whose name its enum does not recognize. Default-deny refuses it, and its
  inner string names the feature in the error.
* **Writer-only features are structurally absent from `readerFeatures` and are therefore NOT on the
  allow-list.** `delta_kernel` 0.26 classifies `appendOnly`, `invariants`, `checkConstraints`,
  `changeDataFeed`, `generatedColumns`, `identityColumns`, `inCommitTimestamp`, `rowTracking`,
  `domainMetadata`, `icebergCompatV1`, `icebergCompatV2`, `icebergCompatV3`, `clustering`,
  `materializePartitionColumns`, and `allowColumnDefaults` in its enum's writer-only block, so they
  can never appear in a reader-feature list. `domainMetadata` and `inCommitTimestamp` are called out
  because they read as read-affecting and are not: their absence from the allow-list is a
  classification fact, not an omission.
* **Protocol-version bounds are read from `delta_kernel`'s own public constants**,
  `table_features::MIN_VALID_RW_VERSION` (`1`) and `table_features::MAX_VALID_READER_VERSION` (`3`),
  rather than from literals, so a kernel upgrade that widens the readable range is a one-line change
  at one site instead of a silent divergence. Both constants are plain `pub` and need no cargo
  feature. `table_features::TABLE_FEATURES_MIN_READER_VERSION` (`3`) — the version at which the
  protocol makes `readerFeatures` mandatory — is likewise plain `pub`, so the gate names that version
  by the kernel's own constant rather than by the literal `3` or by reusing
  `MAX_VALID_READER_VERSION`, which happens to hold the same value today for an unrelated reason.
* **The malformed version-3-without-a-feature-list refusal is defense in depth, not a reachable path
  today.** `delta_kernel` 0.26 constructs every `Protocol` through `Protocol::try_new`, which already
  rejects reader version 3 with no `readerFeatures` (and reader version ≠ 3 WITH one) while parsing
  the log, so a table of that shape fails before this gate sees it. The gate still checks it, because
  the gate's contract is default-deny on its own inputs and the kernel's validation is not this
  engine's to rely on — the same reason `ensure_operation_supported` is not a substitute above.
* **The reader-feature and reader-version accessors need the `internal-api` cargo feature**, already
  enabled on `delta_kernel` for `TableConfiguration::partition_columns` and `::column_mapping_mode`.
  `Snapshot::table_configuration`, `TableConfiguration::protocol`, `Protocol::reader_features`,
  `Protocol::min_reader_version`, and the `TableFeature` enum are all `#[internal_api]` and become
  `pub` under it. `delta_kernel`'s own `extract_enabled_reader_features` and
  `check_reader_version_range` free functions are `pub(crate)` WITHOUT `#[internal_api]` and are
  therefore unreachable — the engine implements the equivalent logic against the accessors above.
* Every error this feature surfaces is a `UdfError`, never a panic, because a panic inside a UDF is an
  abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part. No error text
  carries a vended or static credential value.

## Scenarios

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

### Scenario: A reader protocol version outside the readable range is refused

* *GIVEN* a Delta table whose `protocol` action declares a `minReaderVersion` below
  `delta_kernel::table_features::MIN_VALID_RW_VERSION` or above
  `delta_kernel::table_features::MAX_VALID_READER_VERSION`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return a `UdfError` naming the table root and the declared
  `minReaderVersion`, and stating the range this engine reads
* *AND* the bounds SHALL be read from those two `delta_kernel` constants rather than from the
  literals `1` and `3`, so the readable range has ONE owner and a kernel upgrade that widens it does
  not leave a stale copy behind
* *AND* the version check SHALL run BEFORE the per-feature check, so a table declaring an unreadable
  reader version is refused on the version rather than on a feature list the protocol forbids it to
  carry

### Scenario: A legacy-protocol table carries no explicit reader-feature list

* *GIVEN* a Delta table whose `protocol` action declares a `minReaderVersion` of 1 or 2 and therefore,
  per the protocol, carries no `readerFeatures` array — the shape of the `basic_partitioned`,
  `cm_id_mode`, and `cm_name_mode` fixtures
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL pass the gate on the version check alone, because the only reader feature a
  legacy protocol can imply is `columnMapping` at Reader Version 2 and that feature is allow-listed
* *AND* the reader MUST NOT treat an absent `readerFeatures` list as an empty list of a
  version-3 table, and MUST NOT refuse the table for carrying no list, because the protocol makes the
  array exist *"only when `minReaderVersion` is set to `3`"*
* *AND* conversely, a table declaring `minReaderVersion` equal to
  `delta_kernel::table_features::TABLE_FEATURES_MIN_READER_VERSION` (`3`) while carrying NO
  `readerFeatures` array SHALL be refused with a `UdfError` naming the missing list, because the
  protocol makes the array mandatory at that version — a default-deny gate fails loud on a malformed
  `protocol` action rather than reading it as feature-free and admitting every unimplemented reader
  feature the table may actually use
* *AND* a version-3 table carrying an EMPTY `readerFeatures` array SHALL pass the gate, because an
  empty list is a well-formed declaration of no reader features rather than a missing one
* *AND* the `cm_id_mode` and `cm_name_mode` fixtures — Reader Version 2 with
  `delta.columnMapping.mode` set — SHALL keep resolving their column-mapping binding keys exactly as
  `vs-adapter/delta-table-planning` records, so the gate regresses neither column-mapping mode

### Scenario: The gate runs inside snapshot construction, so no resolution path can bypass it

* *GIVEN* the Delta plan-time resolution path, whose steps read the snapshot's schema, its
  partition-column list, its column-mapping mode, and its active file list
* *WHEN* a `DeltaSnapshot` is constructed for a table root
* *THEN* the gate SHALL run inside that construction, so construction either returns a gated snapshot
  or returns the refusal — no code path SHALL be able to obtain a `DeltaSnapshot` whose protocol was
  never checked
* *AND* the gate SHALL therefore run BEFORE the logical schema is built, BEFORE the partition columns
  are read, BEFORE the column-mapping mode is resolved, and BEFORE the active file list is replayed,
  so an unsupported table costs exactly the object-store reads that resolving the current version
  already needed and no more
* *AND* the check MUST NOT be added at the format reader's own entry point instead, because that
  leaves the constructor reachable from a second caller — including a test — that would then exercise
  an ungated snapshot and record the ungated behavior as correct
* *AND* the refusal SHALL be returned as a `UdfError` value, never raised as a panic, and MUST NOT
  contain any vended or static credential value
