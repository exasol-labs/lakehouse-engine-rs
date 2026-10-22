# Feature: Delta Reader-Feature Gating

The adapter refuses a Delta table whose reader protocol version or reader-feature set the engine does not implement, at plan time and before any log replay. An unsupported table returns a named error instead of wrong rows.

## Background

* The Delta protocol requires a reader to implement and respect every feature in `readerFeatures`. `readerFeatures` exists only when `minReaderVersion` is 3, and a table at Reader Version 3 must carry it. A reader does not gate `writerFeatures`, so a write-only feature never refuses a table.
* The gate is default-deny. It matches an allow-list of seven reader features (`columnMapping`, `deletionVectors`, `timestampNtz`, `typeWidening`, `typeWidening-preview`, `v2Checkpoint`, `vacuumProtocolCheck`) and refuses every other feature. A `TableFeature` variant that a newer `delta_kernel` adds is refused until reviewed. The allow-list is the only enumeration in production code.
* `delta_kernel`'s own support check is not a substitute. The kernel checks reader features at snapshot load (`Operation::SnapshotLoad`) and at scan build (`Operation::Scan`). It classifies `variantType`, `variantType-preview`, `variantShredding`, and `variantShredding-preview` as supported because it can read their log, not because this engine can interpret their data, so for those four features this gate is the only refusal.
* The kernel rejects `geospatial`, `adaptiveMetadata-preview`, and every unrecognized name at snapshot load, and fails a `catalogManaged` or `catalogOwned-preview` table at load because the engine supplies no maximum catalog version. `DeltaSnapshot::open` loads the snapshot before this gate runs, so such a table fails with the kernel's error wrapped with the table root, naming only the first feature the kernel rejects. This is a scoped exception to the one-error-names-every-refusal rule. The allow-list still omits these features, so a kernel that starts loading them leaves them refused by this gate.
* `TableFeature::Unknown(_)` is the kernel's catch-all for an unrecognized `readerFeatures` name. The gate refuses it and reports the name verbatim.
* `typeWidening` and `typeWidening-preview` name the same read contract at Reader Version 3 and Writer Version 7. The preview name identifies the client that enabled the feature. The engine's format-neutral column-binding adapter chain performs the widening cast per file (`scan-types/type-relaxation`). `delta_kernel` performs no cast. `delta/delta-type-mapping` refuses a column whose recorded `delta.typeChanges` the engine cannot perform.
* Writer-only features never appear in `readerFeatures`, so they are not on the allow-list. This includes `domainMetadata` and `inCommitTimestamp`.
* The protocol-version bounds come from `delta_kernel::table_features::MIN_VALID_RW_VERSION` (1) and `MAX_VALID_READER_VERSION` (3). The version at which `readerFeatures` becomes mandatory is `TABLE_FEATURES_MIN_READER_VERSION` (3). The gate uses those constants, not literals. The kernel's own `check_reader_version_range` already rejects an out-of-range version at snapshot load, so the gate's version check is defense in depth.
* `delta_kernel` rejects reader version 3 without `readerFeatures` while it parses the log. The gate checks that shape too, so it does not depend on the kernel's validation.
* The reader-feature accessors (`Snapshot::table_configuration`, `TableConfiguration::protocol`, `Protocol::reader_features`, `Protocol::min_reader_version`, `TableFeature`) need the `internal-api` cargo feature on `delta_kernel`. The kernel's `extract_enabled_reader_features` and `check_reader_version_range` are unreachable, so the engine implements the equivalent logic.
* Every error is a `UdfError`, never a panic, and no error text carries a credential value.

## Scenarios

### Scenario: A reader feature outside the allow-list refuses the table before any log replay

* *GIVEN* a Delta table whose `protocol` action declares `minReaderVersion` 3 and a `readerFeatures`
  list holding at least one feature outside this engine's allow-list, one of exactly
  `variantType`, `variantType-preview`, `variantShredding`, `variantShredding-preview`,
  `catalogManaged`, `catalogOwned-preview`, `adaptiveMetadata-preview`, `geospatial`, or a name
  `delta_kernel` does not recognize and surfaces as `TableFeature::Unknown`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return a `UdfError` naming the table root and EVERY refused feature by its
  Delta protocol name, the `readerFeatures` spelling, so `variantType-preview` is reported as
  `variantType-preview` and an unrecognized name is reported verbatim
* *AND* the reader SHALL name every refused feature in ONE error rather than the first alone, so a
  table carrying two unsupported features is diagnosed in one query instead of two
* *AND* where `delta_kernel` itself rejects a feature at snapshot load, the reader SHALL instead
  return the kernel's error wrapped with the table root, which names the first feature the kernel
  rejects, as the Background records
* *AND* the refusal formatter SHALL carry NO per-feature special case, because a formatter arm for an allow-listed feature is unreachable code that reads as a refusal still in force
* *AND* the reader MUST NOT replay one Delta log commit, read one checkpoint, list one object-store
  prefix, or build the table's logical schema after the refusal, because the refusal exists to stop
  an unsupported table before it costs object-storage access
* *AND* the reader MUST NOT rely on `delta_kernel` raising the refusal, because
  `delta_kernel` loads and scans a table carrying any of the four variant features without error

### Scenario: Every allow-listed reader feature keeps its table queryable

* *GIVEN* a Delta table whose `readerFeatures` list holds only features drawn from this engine's
  allow-list, which is exactly `columnMapping`, `deletionVectors`, `timestampNtz`, `typeWidening`,
  `typeWidening-preview`, `v2Checkpoint`, and `vacuumProtocolCheck`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL pass the gate and resolve the table's schema, partition columns, and active
  file list unchanged
* *AND* `typeWidening` and `typeWidening-preview` SHALL both be admitted, because they name the same
  read contract at the same Reader Version 3 floor and differ only in which client enabled the
  feature, admitting one and refusing the other would refuse a table this engine reads correctly
* *AND* admitting the feature SHALL NOT by itself admit every type change a table records: a table
  whose `delta.typeChanges` names a change this engine cannot perform is still refused, per column,
  by `delta/delta-type-mapping`, the protocol gate answers whether the FEATURE is implemented
  and the schema gate answers whether each recorded CHANGE is
* *AND* the gate MUST NOT refuse a table because its `writerFeatures` list holds a feature outside the
  allow-list, because the protocol obliges a READER to respect `readerFeatures` alone and a
  write-only feature changes nothing this engine reads
* *AND* the allow-list SHALL be the ONLY enumeration in the production code: the gate SHALL match the
  seven allow-listed variants and refuse the `_` remainder, so a `delta_kernel` upgrade that adds a
  `TableFeature` variant refuses it rather than admitting it unreviewed
* *AND* a table that declares `deletionVectors`, `timestampNtz` with `columnMapping`, or `typeWidening` SHALL stay queryable under `scan-read-path/scan-execution-delta-deletion-vectors`, `scan-read-path/scan-execution-partition-values`, `delta/delta-table-planning`, and `delta/delta-type-mapping`

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
* *AND* `delta_kernel` rejects such a version at snapshot load before this check runs, so the table
  fails with the kernel's error wrapped with the table root, and this check stays as defense in depth
* *AND* the version check SHALL run BEFORE the per-feature check, so a table declaring an unreadable
  reader version is refused on the version rather than on a feature list the protocol forbids it to
  carry

### Scenario: A legacy-protocol table carries no explicit reader-feature list

* *GIVEN* a Delta table whose `protocol` action declares a `minReaderVersion` of 1 or 2 and therefore,
  per the protocol, carries no `readerFeatures` array, the shape of the `basic_partitioned`,
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
  protocol makes the array mandatory at that version, and a default-deny gate fails loud on a malformed
  `protocol` action rather than reading it as feature-free
* *AND* a version-3 table carrying an EMPTY `readerFeatures` array SHALL pass the gate, because an
  empty list is a well-formed declaration of no reader features rather than a missing one
* *AND* the `cm_id_mode` and `cm_name_mode` fixtures, Reader Version 2 with
  `delta.columnMapping.mode` set, SHALL keep resolving their column-mapping binding keys exactly as
  `delta/delta-table-planning` specifies, so the gate regresses neither column-mapping mode

### Scenario: The gate runs inside snapshot construction, so no resolution path can bypass it

* *GIVEN* the Delta plan-time resolution path, whose steps read the snapshot's schema, its
  partition-column list, its column-mapping mode, and its active file list
* *WHEN* a `DeltaSnapshot` is constructed for a table root
* *THEN* the gate SHALL run inside that construction, so construction either returns a gated snapshot
  or returns the refusal, no code path SHALL be able to obtain a `DeltaSnapshot` whose protocol was
  never checked
* *AND* the gate SHALL therefore run BEFORE the logical schema is built, BEFORE the partition columns
  are read, BEFORE the column-mapping mode is resolved, and BEFORE the active file list is replayed,
  so an unsupported table costs exactly the object-store reads that resolving the current version
  already needed and no more
* *AND* the check MUST NOT be added at the format reader's own entry point instead, because that
  leaves the constructor reachable from a second caller, including a test, that would then exercise
  an ungated snapshot and record the ungated behavior as correct
* *AND* the refusal SHALL be returned as a `UdfError` value, never raised as a panic, and MUST NOT
  contain any vended or static credential value

### Scenario: A refused table fails the query with a SQL error and leaves the session usable

* *GIVEN* a Delta table that declares a reader feature outside the allow-list, queried through a virtual schema
* *WHEN* a `SELECT` over it runs
* *THEN* the query SHALL fail with a SQL error that names the refused feature, and SHALL NOT return a row
* *AND* a following `SELECT 1 FROM DUAL` on the same connection SHALL succeed, because a crashed UDF VM would end the session instead of returning a clean SQL error
* *AND* the error MUST NOT contain a credential value
