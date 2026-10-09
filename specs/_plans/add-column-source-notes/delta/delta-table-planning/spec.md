# Feature: Delta Table Planning

Resolves a Delta Lake table into the engine's existing `ScanSpec` shape at plan time, so file-level
sharding, the pushdown wire format, streaming emit, and the memory model are reused for the Delta
path exactly as they already are for Iceberg.

<!-- DELTA:CHANGED -->
## Background

* **This is issue #319, the milestone's second table format.** The engine resolves Iceberg tables
  through `iceberg`'s `plan_files()` (`file-planning/pushdown-planning-file-resolution`). This feature
  adds the parallel Delta path — `delta-kernel-rs` 0.26 log replay — behind one shared abstraction,
  and both paths emit the same `ScanSpec`.
* **The seam is a `FormatReader` trait in `lakehouse-engine`, not in `lakehouse-catalog`.** It is the
  per-table-format counterpart of the per-catalog-kind `CatalogClient` trait
  (`catalog/catalog-crate-structure`). It lives in the engine because the Iceberg file-planning
  code and the `ScanSpec` wire format live there, and because `lakehouse-catalog` MUST NOT name
  `iceberg`, `datafusion`, `arrow`, `parquet`, or `object_store` — a rule that extends to
  `delta_kernel` for the same reason. `CatalogClient` stays metadata-only.
* **Each `FormatReader` implementation owns its WHOLE resolution** — catalog request, storage
  credential, and file discovery. A shared caller cannot pre-fetch the metadata each format needs to
  reach its file list: the Iceberg path needs the catalog's own `TableMetadata` to build an
  `iceberg::table::Table`, while the Delta path needs only a table-root URL plus a credentialed
  object store. Splitting resolution across the boundary would reintroduce the per-format fork the
  trait exists to remove.
* **Format dispatch is one exhaustive match over `ScanSource`, whose variant pairs a live catalog
  session with the table it reads.** `ScanSource` is NOT `CatalogKind`: the kind is a parsed
  virtual-schema property, whose permitted match sites `vs-adapter/catalog-kind-selection` lists;
  `ScanSource` carries a resolved session and a loaded table. Matching `ScanSource` is what removes
  the need for a second `CatalogKind` match site, so that list gains no entry.
* **Production pushdown now wires into the Delta path, issue #320.** The recorded refusal
  `vs-adapter/catalog-kind-selection` § "A pushdown request under the Unity Catalog kind is refused
  as not yet executable" is SUPERSEDED by "A pushdown request under the Unity Catalog kind is
  planned as a Delta scan": deletion vectors, partition values, and column mapping are now applied
  at scan time (`scan-read-path/scan-execution-delta-deletion-vectors`,
  `scan-read-path/scan-execution-partition-values`), so wiring `handle_pushdown` through the
  scan-source seam no longer returns silently wrong rows.
* **Apache Iceberg spec check — this feature changes no Iceberg behavior, and the one overlapping
  rule is already a recorded trade-off.** The Iceberg table spec's "Column Projection" ordered
  resolution defines rule (1) as "Return the value from partition metadata if an Identity Transform
  exists for the field and the partition value is present in the `partition` struct on `data_file`
  object in the manifest". `scan-read-path/scan-execution-field-id-projection` records rule (1) as
  unimplemented and as a deliberate, accurately-scoped trade-off. Delta has no analogous escape:
  Delta NEVER writes a partition column's value into the data file, so carrying partition values in
  the scan spec is mandatory rather than an edge case. This feature therefore carries them for the
  Delta path only and leaves the Iceberg path's recorded trade-off untouched.
* **Per-file min/max statistics are OUT of scope.** Delta log replay exposes per-file stats, and
  stats-based file pruning is issue #321, whose `multi-part-stats` fixture is already vendored for
  it. This plan designs no stats wire shape before its consumer exists.
* **Delta reader-feature gating and broad Delta type mapping are OUT of scope** — issue #322. This
  plan maps only the Delta primitive types that already have an Arrow type tag and refuses the rest
  at plan time, so an unmapped type is an error rather than a wrong value.
* **The log-replay step takes an INJECTED object store**, so it is exercised offline against the
  vendored fixtures over `file://` as well as over SeaweedFS through the live stack. Building the store
  is the reader's, not the replay step's.
* Every error surfaced by this feature is a `UdfError`, never a panic, because a panic inside a UDF
  is an abnormal VM exit that makes the engine SIGKILL every sibling VM of the statement part. No
  error text carries a bearer token, an OAuth client secret, a vended storage key, or any other
  credential value.
* **This delta is issue #342 and changes NO planning behavior.** It re-homes what Delta planning
  already resolves — partition values, the deletion-vector reference, and each column's physical name
  and id — from Delta-named `ScanSpec` blocks onto format-neutral `ScanSpec` fields both table
  formats populate. Every value the reader resolves, every refusal it raises, and every credential
  decision it makes is unchanged; only the field each value lands in changes.
* **The asymmetry this removes.** #319 put Iceberg concepts directly on the shared types
  (`FileEntry::deletes`, `LogicalField::field_id`, `name_mapping`) and bolted Delta concepts on behind
  `Option`-gated Delta blocks (`CommonScanSpec::delta`, `FileEntry::delta`). The scan side then had to
  ask which FORMAT produced a spec before it could read it. After this delta both formats populate the
  same neutral fields and the scan side dispatches on CONTENT.
* **Neutral homes, one per concept.** A data file's deletions ride in ONE `FileEntry::deletes` list of
  `DeleteMechanism` values, whose variant names the mechanism and carries that mechanism's payload. A
  data file's partition values ride in `FileEntry::partition_values`. The table's ordered
  partition-column names ride in `CommonScanSpec::partition_columns`. Each column's binding key rides
  on its own `LogicalField`.
* **The column-mapping MODE is no longer carried at all.** It survives as a `createVirtualSchema` input,
  read from the catalog's copy of the table property, that decides WHICH binding key each column's
  note records, and as the pushdown reader's check of that declaration against the log, not as a
  value on the wire or in the note — see the scenarios below. `DeltaTableSpec`, `DeltaFileSpec`, `DeltaColumnMapping`, and `DeltaColumnMappingMode` cease to
  exist; `DeltaDeletionVectorStorage` survives only as the payload enum of the deletion-vector delete
  mechanism, keeping the closed 3-kind set this feature already requires.
* **The pushdown refusal is gone, issue #320.** Both halves of its former justification are now
  closed: the Delta deletion vector is modelled AND applied at read time
  (`scan-read-path/scan-execution-positional-deletes`), and the scan now materializes a partition
  column absent from the physical Parquet file
  (`scan-read-path/scan-execution-partition-values`). Production pushdown reaches the Delta reader
  through the scan-source seam — see the scenario below.
* **`partition_columns` and `partition_values` are now consumed at scan time**, by
  `scan-read-path/scan-execution-partition-values` — issue #320 closed the deferral this feature
  recorded.
* **Apache Iceberg spec check — the Iceberg path's behavior and its one recorded deviation are both
  unchanged.** The table spec's Column Projection section states that "Columns in Iceberg data files
  are selected by field id" and that "projection must be done using field ids". The Iceberg format
  reader populates `LogicalField::field_id` for EVERY logical field and populates no physical name, so
  every Iceberg column is still selected by field id and the new binding strategies are unreachable
  from the Iceberg path. The spec's ordered resolution rule (1) — "Return the value from partition
  metadata if an Identity Transform exists for the field and the partition value is present in the
  `partition` struct on `data_file` object in the manifest" — stays unimplemented and stays the
  deliberate, accurately-scoped trade-off `scan-read-path/scan-execution-field-id-projection`
  records. This delta neither closes nor widens it: it gives that rule a neutral wire shape to land in
  later (issue #99), while the Iceberg reader leaves both new fields empty today.
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
* **This delta is issue #322.** The two deferrals this feature has recorded since #319 — Delta
  reader-feature gating and broad Delta type mapping — are both closed. Neither the log replay, the
  partition values, the deletion-vector descriptors, the column-mapping binding keys, nor the
  credential vending changes; what changes is that an unsupported table is now refused and a wider
  type surface is now mapped.
* **Gating moves to `delta/delta-reader-feature-gating`** and type mapping to
  `delta/delta-type-mapping`, rather than growing this feature further. This feature already
  carries nine scenarios spanning log replay, partition values, deletion vectors, column mapping,
  credentials, format dispatch, and Iceberg parity; a protocol gate and a full type-surface mapping are
  each a distinct reason to change and each carries its own normative protocol citations.
* **Only two recorded statements are affected.** The scenario "A Delta type this plan does not map is
  refused at plan time" is REMOVED, because every clause of it either restates a mapping
  `delta/delta-type-mapping` now owns or asserts the absence of the gate
  `delta/delta-reader-feature-gating` now adds. The scenario "The Delta reader is reached from
  production pushdown under the Unity Catalog kind" is CHANGED, because its "SHALL still perform NO
  Delta reader-feature gating" clause and its scoped-exception clause are the exception this plan
  closes.
* **Filter-based file pruning stays deferred, unchanged.** Per-file statistics and partition pruning
  remain issue #321, so a filter still narrows the rows the scan emits without narrowing the files it
  reads. This plan touches neither.
* **Apache Iceberg spec check — this delta changes no Iceberg behavior.** It adds a Delta protocol
  gate and widens the Delta type mapping; no code on the Iceberg resolution path changes. The Iceberg
  table spec's Column Projection requirement that "projection must be done using field ids" still
  holds for every Iceberg column, and its ordered resolution rule (1) — the partition-metadata rule —
  remains the deliberate, accurately-scoped trade-off
  `scan-read-path/scan-execution-field-id-projection` records, neither closed nor widened here.
* **This delta is issue #321, and it closes the LAST deferral this feature has carried since #319.**
  Filter-based file pruning is now implemented and owned by `delta/delta-file-pruning`. Three
  recorded statements are superseded, all of them assertions that pruning does NOT happen: the
  "**Per-file min/max statistics are OUT of scope**" bullet, the "filter-based file pruning is issue
  #321" half of the two-deferrals bullet, and the "**Filter-based file pruning stays deferred,
  unchanged**" bullet. This feature therefore records NO remaining pruning exception. Nothing else
  about Delta planning changes: log replay, partition values, deletion-vector descriptors,
  column-mapping binding keys, credential vending, the protocol gate, and type refusal are all
  untouched.
* **The no-stats-on-the-wire rule SURVIVES this delta, with a corrected justification.** The scan still
  carries no per-file minimum or maximum, because `delta_kernel` evaluates the bounds internally during
  log replay and hands back a selection vector — pruning completes before a file entry exists, so the
  stats wire shape this feature deferred never acquired a consumer and is not designed now either. The
  scenario clause below is CHANGED only in its reason, never in its obligation, so `ScanSpec` stays
  format-neutral per CLAUDE.md and every golden scan-spec encoding is unmoved.
* **This delta is issue #426.** A Delta table's logical schema, partition columns, and refused
  columns are decided once, at `createVirtualSchema`, from Unity Catalog's copy of the table's
  schema: each column's `type_json` (the Delta `StructField` JSON, column metadata included), the
  `delta.columnMapping.mode` entry of the table's `properties`, and the columns' `partition_index`.
  They are recorded in each column's note (`vs-adapter/column-source-notes`), and
  `createVirtualSchema` reads no Delta log. The pushdown reader still opens the gated snapshot for
  the reader-feature gate, the active files, the deletion vectors, and the partition values, checks
  the declaration against that snapshot, and derives no logical field from it. Log replay, partition
  values, deletion-vector descriptors, credential vending, and the protocol gate are unchanged.
* **Delta protocol check for a declaration older than the log's current version.** PROTOCOL.md
  § Writer Requirements for Column Mapping: "The physical name of the column is static and can be
  different than the _display name_ of the column, which is changeable", and a writer must "ensure
  the physical field path of the new column is unique across all versions of the table". § Reader
  Requirements for Column Mapping: "In `name` mode, readers must resolve columns in the data files
  by their physical names", "Partition values and column level statistics will also be resolved by
  their physical names", and "In `id ` mode, readers must resolve columns by using the `field_id` in the parquet
  metadata for each file". A recorded binding key therefore names the same column after a
  source-side rename, so the scan and the pruning translation resolve each declared column through
  its recorded key. Under the `none` mode the protocol offers no rename or drop without column
  mapping ("Delta can use column mapping to avoid any column naming restrictions, and to support the
  renaming and dropping of columns without having to rewrite all the data"), so a declared name
  stays the column's identity.
* **Delta protocol check for a declaration taken from the catalog.** PROTOCOL.md § Column Mapping
  keeps the mode in a table property and the binding keys in column metadata: "The column mapping is
  governed by the table property `delta.columnMapping.mode` being one of `none`, `id`, and `name`",
  and "The physical name is stored as part of the column metadata with the key
  `delta.columnMapping.physicalName`." Unity Catalog's `properties` and `type_json` are copies of
  those two places. The same section conditions the property on the protocol: "The table property
  should only be honored if the table's protocol has reader and writer versions and/or table
  features that support the `columnMapping` table feature." `createVirtualSchema` cannot apply that
  condition, because it reads no protocol, so the pushdown reader compares the declaration with the
  mode the gated snapshot honors and fails a query on any disagreement (scenario below). A copy that
  omits the mode or a partition column therefore fails loudly instead of binding the scan to the
  wrong file columns.
* **Scoped exception: a Delta type change the declaration does not record.** PROTOCOL.md § Type
  Widening: type changes are "stored in the `metadata` of their nearest ancestor" struct field
  "using the key `delta.typeChanges`", and § Reader Requirements for Type Widening: readers "must validate that
  they support all type changes in the `delta.typeChanges` field in the table schema for the table
  version they are reading and fail when finding any unsupported type change". `createVirtualSchema`
  validates the type changes the catalog's `type_json` metadata records and records an unsupported
  one as its column's refusal. A change the catalog's copy omits, or one recorded after the last
  `REFRESH`, is not validated by the declaration. Until then, a data file written under an
  unsupported change fails the query that reads it at scan time, because the scan admits only the
  widening pairs `scan-types/type-relaxation` proves and admits, and a query that reads only files
  written before the change returns their declared values.
* **Scoped exception: a column dropped at the source after the last REFRESH, under `name` or `id`
  mode.** The declared binding key still names the dropped column, so a query reads the values the
  older data files store under that key and NULL from files written without it, until `REFRESH`
  removes the column. A column the catalog declares and the log never carried reads NULL the same
  way. A dropped column declared non-nullable instead fails a query that reads a file written
  without it, with the required-absent error
  `scan-read-path/scan-execution-field-id-projection-absent-fields` records ("Added required column
  absent from a file with no initial-default errors cleanly"), until `REFRESH`. This mirrors the
  Iceberg exception `file-planning/pushdown-planning-file-resolution` records.
* **A declared nullability older than the log.** Each note fixes its column's `nullable` flag at
  `createVirtualSchema`, from the catalog's `type_json` copy. A writer can relax a column to nullable
  later (`ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL`), and an OSS registrant's copy can disagree
  with the log. DataFusion rewrites `c IS NULL` to `false` and `c IS NOT NULL` to `true` for a
  non-nullable field, so a stale `nullable: false` would drop matching rows without an error. The
  pushdown reader therefore fails a query whose declared non-nullable column the snapshot marks
  nullable (scenario below). A declared nullable column that the log marks non-nullable plans
  normally, because no row is lost.
* **The reader-feature gate stays at pushdown and stays the authority.** It needs the log's
  protocol, which `createVirtualSchema` does not read, so a table the gate refuses is listed at
  `createVirtualSchema` and fails at query time, unchanged (`delta/delta-reader-feature-gating`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Each logical field carries the binding key its column-mapping mode selects

* *GIVEN* three Delta tables registered in Unity Catalog, one per column-mapping mode: one whose
  `properties` set `delta.columnMapping.mode` to `name` and whose columns' `type_json` metadata each
  carry the `delta.columnMapping.physicalName` and `delta.columnMapping.id` the Delta log declares,
  one setting it to `id` with the same annotations, and one setting no mode at all
* *WHEN* `createVirtualSchema` declares each table's columns from that catalog metadata and the Delta
  format reader then resolves each table's scan
* *THEN* `createVirtualSchema` SHALL record each column's binding key in that column's OWN note, and
  the returned scan SHALL carry it on that column's OWN logical field, and neither SHALL carry a
  table-level column-mapping mode, a per-table column list, or any other Delta-named block, because
  the mode's only consumer is the choice of binding key and encoding the choice leaves nothing for a
  second home to drift from
* *AND* the Delta format reader SHALL take each logical field from the column notes and MUST NOT
  build one from the snapshot's schema at pushdown
* *AND* under `id` mode each logical field SHALL carry its `delta.columnMapping.id` as its field-id
  and SHALL carry NO physical name, because Delta writes Parquet field-ids in `id` mode and only there
* *AND* under `name` mode each logical field SHALL carry its `delta.columnMapping.physicalName` as its
  physical name and SHALL carry NO field-id, because the Delta protocol requires a `name`-mode reader
  to match on the physical name and a carried field-id would offer a second, unauthorized key
* *AND* under `none` mode each logical field SHALL carry NEITHER a field-id NOR a physical name, so
  the scan binds it by its own logical name, superseding the removed scenario's 1-based-ordinal
  field-id: an ordinal is a value the writer never wrote into any file, and carrying it invites a
  false match against a file that happens to carry field-ids
* *AND* a column under `id` or `name` mode whose `type_json` lacks the annotation its mode requires,
  or carries it malformed, SHALL be declared with a note whose refusal names the column, the mode,
  and the problem, and `createVirtualSchema` SHALL neither fail nor skip the table, because its
  ordinal position and its logical name are values the writer never used and the refusal is scoped
  to the column as `delta/delta-type-mapping` scopes a type refusal
* *AND* the reader SHALL still carry an EMPTY `schema.name-mapping.default` list, because a
  name-mapping entry is a table-level fallback for files lacking field-ids and the per-column physical
  name is the authoritative declaration that replaces it for Delta
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A declaration that disagrees with the Delta log fails the query

* *GIVEN* the `name`-mode table `cdf-column-mapping-name-mode` registered in Unity Catalog without
  `delta.columnMapping.mode` in its `properties`, so its notes carry no binding key, and the
  partitioned table `basic_partitioned` registered with no `partition_index` on its partition column
  `letter`
* *WHEN* a `SELECT` runs over each table
* *THEN* the Delta format reader SHALL compare the declaration with the gated snapshot it already
  opens, and each query SHALL fail before any file is planned with an error naming the table, stating
  which part disagrees (the log's column-mapping mode against the declared binding keys, or the log's
  partition columns against the declared ones), and stating that the fix is to give the catalog
  entry the Delta schema's column metadata and properties, then run `ALTER VIRTUAL SCHEMA ...
  REFRESH`
* *AND* the reader MUST NOT re-derive a logical field, a binding key, or a partition column from the
  log, because the check refuses a disagreeing declaration and never repairs it
* *AND* a declaration in which every non-refused field carries exactly the key the snapshot's mode
  selects, and whose partition columns are the log's partition columns in the log's order, identified
  by binding key under `name` or `id` mode and by name under `none`, SHALL plan normally
* *AND* no error SHALL contain a credential value
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A declared non-nullable column that the Delta log marks nullable fails the query

* *GIVEN* the `name`-mode table `cdf-column-mapping-name-mode` registered in Unity Catalog with
  `"nullable": false` in the `type_json` of `id`, whose Delta log marks `id` nullable, so the note of
  `id` declares it non-nullable
* *WHEN* `SELECT id FROM <table> WHERE id IS NULL` runs, and separately a query that reads only
  `name`
* *THEN* each query SHALL fail before any file is planned with an error naming the table and the
  column `id`, stating that the declaration marks it non-nullable while the Delta log allows NULL,
  and naming `ALTER VIRTUAL SCHEMA ... REFRESH` as the fix, because a non-nullable declaration lets
  the scan drop rows whose value is NULL
* *AND* the reader SHALL identify each declared column in the snapshot by its binding key under
  `name` or `id` mode and by its name under `none`, as the declaration check does
* *AND* a declared nullable column whose snapshot field is non-nullable SHALL plan normally
* *AND* no error SHALL contain a credential value
<!-- /DELTA:NEW -->
