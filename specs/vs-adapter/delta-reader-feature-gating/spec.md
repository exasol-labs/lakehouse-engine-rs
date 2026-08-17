# Feature: Delta Reader-Feature Gating

Refuses a Delta table whose reader protocol version or reader-feature set this engine does not
implement end to end, at plan time and before any log replay, so an unsupported table returns a named
error rather than silently wrong rows.

## Background

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
* **The concrete correctness hole `typeWidening` opens.** `DeltaSnapshot::active_files` builds its
  kernel scan with `.without_row_transforms()`, so no per-file cast transform is applied. A widened
  column is then read with each older data file's OLD physical Parquet type against the table's NEW
  logical type — wrong values, no error. Type-widening support is issue #349, filed and out of scope
  here.
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
  feature.
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
* **Apache Iceberg spec check — this feature changes no Iceberg behavior.** It gates the Delta
  protocol alone and adds no code on the Iceberg resolution path, so the Iceberg table spec's Column
  Projection rules and the one recorded deviation
  (`datafusion-scan/scan-execution-field-id-projection`'s partition-metadata rule (1)) are
  untouched.

## Scenarios

### Scenario: A reader feature outside the allow-list refuses the table before any log replay

* *GIVEN* a Delta table whose `protocol` action declares `minReaderVersion` 3 and a `readerFeatures`
  list holding at least one feature outside this engine's allow-list — one of exactly
  `typeWidening`, `typeWidening-preview`, `variantType`, `variantType-preview`, `variantShredding`,
  `variantShredding-preview`, `catalogManaged`, `catalogOwned-preview`, `adaptiveMetadata-preview`, or
  a name `delta_kernel` 0.26 does not recognize and surfaces as `TableFeature::Unknown`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return a `UdfError` naming the table root and EVERY refused feature by its
  Delta protocol name — the `readerFeatures` spelling, so `typeWidening-preview` is reported as
  `typeWidening-preview` and an unrecognized name is reported verbatim
* *AND* the reader SHALL name every refused feature in ONE error rather than the first alone, so a
  table carrying two unsupported features is diagnosed in one query instead of two
* *AND* the error for `typeWidening` and `typeWidening-preview` SHALL cite issue #349, so the refusal
  reads as tracked, scoped work rather than a permanent limitation
* *AND* the reader MUST NOT replay one Delta log commit, read one checkpoint, list one object-store
  prefix, or build the table's logical schema after the refusal, because the refusal exists to stop
  an unsupported table before it costs object-storage access
* *AND* the reader MUST NOT rely on `delta_kernel` raising the refusal, because
  `delta_kernel`'s own `ensure_operation_supported(Operation::Scan)` reports support for every reader
  feature its enum parses and reads both the `type-widening` and `unshredded-variant` fixtures
  without error

### Scenario: Every allow-listed reader feature keeps its table queryable

* *GIVEN* a Delta table whose `readerFeatures` list holds only features drawn from this engine's
  allow-list, which is exactly `columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`, and
  `vacuumProtocolCheck`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL pass the gate and resolve the table's schema, partition columns, and active
  file list unchanged
* *AND* the gate MUST NOT refuse a table because its `writerFeatures` list holds a feature outside the
  allow-list, because the protocol obliges a READER to respect `readerFeatures` alone and a
  write-only feature changes nothing this engine reads
* *AND* the allow-list SHALL be the ONLY enumeration in the production code: the gate SHALL match the
  five allow-listed variants and refuse the `_` remainder, so a `delta_kernel` upgrade that adds a
  `TableFeature` variant refuses it rather than admitting it unreviewed
* *AND* every shipped Delta fixture whose reader features are wholly allow-listed SHALL keep passing
  every scenario already recorded for it in
  `datafusion-scan/scan-execution-delta-deletion-vectors`,
  `datafusion-scan/scan-execution-partition-values`, and
  `e2e-harness/unity-catalog-e2e-harness-delta-queries` — namely `table_with_dv` and
  `multi_part_stats` (`deletionVectors`) and `stats_all_types` (`timestampNtz` + `columnMapping`)

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
