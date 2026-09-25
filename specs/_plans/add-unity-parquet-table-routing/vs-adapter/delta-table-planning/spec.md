# Feature: Delta Table Planning

Resolves a Delta Lake table into the engine's existing `ScanSpec` shape at plan time, so file-level
sharding, the pushdown wire format, streaming emit, and the memory model are reused for the Delta
path exactly as they already are for Iceberg.

## Background

* **This is issue #319, the milestone's second table format.** The engine resolves Iceberg tables
  through `iceberg`'s `plan_files()` (`vs-adapter/pushdown-planning-file-resolution`). This feature
  adds the parallel Delta path — `delta-kernel-rs` 0.26 log replay — behind one shared abstraction,
  and both paths emit the same `ScanSpec`.
* **The seam is a `FormatReader` trait in `lakehouse-engine`, not in `lakehouse-catalog`.** It is the
  per-table-format counterpart of the per-catalog-kind `CatalogClient` trait
  (`vs-adapter/catalog-crate-structure`). It lives in the engine because the Iceberg file-planning
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
  virtual-schema property, whose match sites `vs-adapter/catalog-kind-selection` freezes by a
  source-level probe; `ScanSource` carries a resolved session and a loaded table. Matching
  `ScanSource` is what removes the need for a second `CatalogKind` match site, so that probe stays
  intact and unweakened.
* **Production pushdown now wires into the Delta path, issue #320.** The recorded refusal
  `vs-adapter/catalog-kind-selection` § "A pushdown request under the Unity Catalog kind is refused
  as not yet executable" is SUPERSEDED by "A pushdown request under the Unity Catalog kind is
  planned as a Delta scan": deletion vectors, partition values, and column mapping are now applied
  at scan time (`datafusion-scan/scan-execution-delta-deletion-vectors`,
  `datafusion-scan/scan-execution-partition-values`), so wiring `handle_pushdown` through the
  scan-source seam no longer returns silently wrong rows.
* **Apache Iceberg spec check — this feature changes no Iceberg behavior, and the one overlapping
  rule is already a recorded trade-off.** The Iceberg table spec's "Column Projection" ordered
  resolution defines rule (1) as "Return the value from partition metadata if an Identity Transform
  exists for the field and the partition value is present in the `partition` struct on `data_file`
  object in the manifest". `datafusion-scan/scan-execution-field-id-projection` records rule (1) as
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
  vendored fixtures over `file://` as well as over MinIO through the live stack. Building the store
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
* **The column-mapping MODE is no longer carried at all.** It survives as a plan-time input that
  decides WHICH binding key each `LogicalField` gets, not as a value on the wire — see the scenario
  below. `DeltaTableSpec`, `DeltaFileSpec`, `DeltaColumnMapping`, and `DeltaColumnMappingMode` cease to
  exist; `DeltaDeletionVectorStorage` survives only as the payload enum of the deletion-vector delete
  mechanism, keeping the closed 3-kind set this feature already requires.
* **The pushdown refusal is gone, issue #320.** Both halves of its former justification are now
  closed: the Delta deletion vector is modelled AND applied at read time
  (`datafusion-scan/scan-execution-positional-deletes`), and the scan now materializes a partition
  column absent from the physical Parquet file
  (`datafusion-scan/scan-execution-partition-values`). Production pushdown reaches the Delta reader
  through the scan-source seam — see the scenario below.
* **`partition_columns` and `partition_values` are now consumed at scan time**, by
  `datafusion-scan/scan-execution-partition-values` — issue #320 closed the deferral this feature
  recorded.
* **Apache Iceberg spec check — the Iceberg path's behavior and its one recorded deviation are both
  unchanged.** The table spec's Column Projection section states that "Columns in Iceberg data files
  are selected by field id" and that "projection must be done using field ids". The Iceberg format
  reader populates `LogicalField::field_id` for EVERY logical field and populates no physical name, so
  every Iceberg column is still selected by field id and the new binding strategies are unreachable
  from the Iceberg path. The spec's ordered resolution rule (1) — "Return the value from partition
  metadata if an Identity Transform exists for the field and the partition value is present in the
  `partition` struct on `data_file` object in the manifest" — stays unimplemented and stays the
  deliberate, accurately-scoped trade-off `datafusion-scan/scan-execution-field-id-projection`
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
* **Gating moves to `vs-adapter/delta-reader-feature-gating`** and type mapping to
  `vs-adapter/delta-type-mapping`, rather than growing this feature further. This feature already
  carries nine scenarios spanning log replay, partition values, deletion vectors, column mapping,
  credentials, format dispatch, and Iceberg parity; a protocol gate and a full type-surface mapping are
  each a distinct reason to change and each carries its own normative protocol citations.
* **Only two recorded statements are affected.** The scenario "A Delta type this plan does not map is
  refused at plan time" is REMOVED, because every clause of it either restates a mapping
  `vs-adapter/delta-type-mapping` now owns or asserts the absence of the gate
  `vs-adapter/delta-reader-feature-gating` now adds. The scenario "The Delta reader is reached from
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
  `datafusion-scan/scan-execution-field-id-projection` records, neither closed nor widened here.
* **This delta is issue #321, and it closes the LAST deferral this feature has carried since #319.**
  Filter-based file pruning is now implemented and owned by `vs-adapter/delta-file-pruning`. Three
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

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The format reader is selected at one site and refuses a mismatched pairing

* *GIVEN* a `ScanSource` whose variant pairs one resolved session with the table it reads
* *WHEN* the adapter selects the format reader for that source
* *THEN* the adapter SHALL match `ScanSource` EXHAUSTIVELY at exactly ONE site, which returns a boxed `FormatReader`, so adding a FOURTH table format or a FOURTH catalog kind is a compile error at that site rather than a silent fall-through
* *AND* the Unity Catalog variant SHALL match the loaded table's FORMAT tag exhaustively: the Delta tag selects the Delta reader, the Parquet tag selects the Unity Parquet reader of `vs-adapter/unity-parquet-table-planning`, and the Iceberg tag returns a `UdfError` naming the table and the reported format, because a table routed into another format's reader surfaces a missing log or a wrong schema instead of a clear format refusal
* *AND* that match MUST NOT be replaced by an assumption that the Unity Catalog listing filter already excluded a format, because the single-table load applies no listing filter
* *AND* the selection site MUST NOT match `CatalogKind`, so the permitted-site set of `vs-adapter/catalog-kind-selection` (the enum with its resolver, the catalog-client construction site, credential validation, and the pushdown scan-source construction site) stays intact and gains no file
<!-- /DELTA:CHANGED -->
