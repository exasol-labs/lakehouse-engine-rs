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

## Scenarios

### Scenario: A Delta table resolves its current version's active data files

* *GIVEN* a Delta table whose transaction log holds more than one JSON commit, reachable through a
  credentialed object store at a table-root URL
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL resolve the log's CURRENT version and replay every JSON commit and any
  checkpoint up to it, and SHALL return exactly the data files active at that version — each entry
  carrying the `add` action's `path` verbatim and its `size` — so a file added at one version and
  removed at a later one is absent and a file added at a later version is present
* *AND* the reader SHALL return one entry per active path even when the same path is removed and
  re-added within one commit, because a Delta `DELETE` that writes a deletion vector emits a
  `remove` and an `add` for the identical path and a per-`add` collection would return that file
  twice
* *AND* the reader SHALL store each path verbatim, resolving it against no table root, because path
  reconstruction belongs to file registration (see
  `datafusion-scan/scan-execution-spec-reconstitution`)
* *AND* the returned scan SHALL carry the table root taken from the table's own catalog-reported
  storage location, so the shard-invariant common spec carries it once
* *AND* the returned scan MUST NOT carry any per-file minimum or maximum statistic, because
  stats-based file pruning is issue #321 and its wire shape is designed with its consumer
* *AND* the reader MUST NOT construct its own object store: it SHALL read the log through the store
  it is given, so the replay is exercised over a local filesystem store as well as over S3

### Scenario: Partition values are carried per data file, including a NULL partition value

* *GIVEN* a Delta table partitioned by one column, whose active data files include one written to the
  Hive default partition directory because that row's partition value is NULL
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each returned file entry SHALL carry that file's Delta `partitionValues` in the
  format-neutral per-file `partition_values` map — one entry per partition column, holding the
  serialized value or an explicit absent value for NULL — because Delta stores a partition column's
  value ONLY in the transaction log and never inside the data file, so a scan that reads the Parquet
  file alone cannot recover it
* *AND* that map SHALL be the SAME field an Iceberg identity-transform partition value (issue #99) and
  a future Hive-style partition value would populate, and MUST NOT be reachable only through a
  format-named block, because resolving a per-file partition value at plan time is a property of
  partitioned tables rather than of Delta
* *AND* the file whose logged partition value is NULL SHALL carry an explicit absent value, and MUST NOT
  carry the literal partition-directory text `__HIVE_DEFAULT_PARTITION__`, because that text is a
  directory-naming artifact and not the column's value
* *AND* the returned scan SHALL carry the table's ordered partition-column names ONCE in the
  shard-invariant common spec's format-neutral `partition_columns` field, so a scan of a table with
  zero active files still knows which schema columns have no physical counterpart
* *AND* the per-file partition values SHALL serialize in a deterministic key order, so a golden
  encoding of one scan spec is byte-stable across runs

### Scenario: A data file's deletion vector reference is carried verbatim exactly once

* *GIVEN* a Delta table whose latest commit removes and re-adds one data file, attaching a deletion
  vector to the re-added entry
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return exactly ONE entry for that path, carrying the deletion vector from
  the re-added `add` action and NOT the earlier delete-free `add`
* *AND* that entry SHALL carry the Delta `deletionVector` descriptor verbatim — its storage kind, its
  `pathOrInlineDv`, its `offset`, its `sizeInBytes`, and its `cardinality` — as ONE deletion-vector
  delete mechanism in the entry's single format-neutral `deletes` list, resolved into no path and
  applied to no row at plan time, because applying it belongs to #320
* *AND* the storage kind SHALL be modelled as a closed set of the Delta protocol's three kinds
  (UUID-relative, inline, absolute path), so a descriptor naming a kind outside that set fails at
  plan time rather than reaching the scan as an unread string
* *AND* the deletion vector SHALL be a DISTINCT variant of the delete mechanism, never an Iceberg
  positional-delete reference, because the two mechanisms are unrelated: an Iceberg delete reference
  names a whole delete FILE, while a Delta deletion vector names a byte range inside a shared `.bin`
  file
* *AND* a Delta entry's `deletes` list SHALL hold ONLY its deletion vector and MUST NOT hold any
  Iceberg delete-file reference, so the Iceberg positional-delete reader is never handed a reference
  it would misread — the same guarantee the pre-#342 "`FileEntry::deletes` stays EMPTY on every Delta
  entry" rule gave, restated for the unified list
* *AND* the plan-time relativization of file paths against the table root SHALL apply to Iceberg
  delete-file references ONLY, and MUST leave a deletion vector's `pathOrInlineDv` untouched, because
  that member is a UUID token or an inline payload rather than an object-store path

### Scenario: Each logical field carries the binding key its column-mapping mode selects

* *GIVEN* three Delta tables, one per column-mapping mode: one setting `delta.columnMapping.mode` to
  `name` with each schema field carrying a `delta.columnMapping.physicalName` and a
  `delta.columnMapping.id`, one setting it to `id` with the same annotations, and one setting no mode
  at all
* *WHEN* the Delta format reader resolves each table's scan
* *THEN* the reader SHALL carry each column's binding key on that column's OWN logical field, and the
  returned scan MUST NOT carry a table-level column-mapping mode, a per-table column list, or any
  other Delta-named block, because the mode's only consumer is the choice of binding key and encoding
  the choice leaves nothing for a second home to drift from
* *AND* under `id` mode each logical field SHALL carry its `delta.columnMapping.id` as its field-id
  and SHALL carry NO physical name, because Delta writes Parquet field-ids in `id` mode and only there
* *AND* under `name` mode each logical field SHALL carry its `delta.columnMapping.physicalName` as its
  physical name and SHALL carry NO field-id, because the Delta protocol requires a `name`-mode reader
  to match on the physical name and a carried field-id would offer a second, unauthorized key
* *AND* under `none` mode each logical field SHALL carry NEITHER a field-id NOR a physical name, so
  the scan binds it by its own logical name, superseding the removed scenario's 1-based-ordinal
  field-id: an ordinal is a value the writer never wrote into any file, and carrying it invites a
  false match against a file that happens to carry field-ids
* *AND* a column under `id` or `name` mode missing the annotation its mode requires SHALL still be
  refused with a `UdfError` naming the column, unchanged, because its ordinal position and its logical
  name are values the writer never used
* *AND* the reader SHALL still carry an EMPTY `schema.name-mapping.default` list, because a
  name-mapping entry is a table-level fallback for files lacking field-ids and the per-column physical
  name is the authoritative declaration that replaces it for Delta

### Scenario: Delta planning resolves its storage credential through the table's own catalog

* *GIVEN* a Delta table registered in a Unity Catalog, and a CONNECTION that either enables
  `use_vended_credentials` or supplies static storage credentials
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* under vending the reader SHALL request per-table, short-lived, scoped credentials from that
  Unity Catalog against the table's own catalog-assigned vending key, and SHALL terminate the
  response in a `StorageBackend` through the ONE shared vended-storage policy
  (`vs-adapter/unity-catalog-vended-credentials`), so the `abfs://` plaintext-consent gate and the S3
  address rule apply identically to the Iceberg and the Delta path
* *AND* under vending the reader SHALL fail with an error naming the table when its catalog reported
  no vending key, and MUST NOT fall back to the CONNECTION's static credential, because a silent
  fallback would read object storage with a credential the operator did not select for this table
* *AND* with vending disabled the reader SHALL use the CONNECTION's static storage backend unchanged
* *AND* the reader SHALL return the EFFECTIVE storage backend alongside the file list, so the
  shard-invariant common spec carries the same backend the log was read through
* *AND* every error the reader surfaces from this point on SHALL be redacted against the effective
  storage's secret values, and MUST NOT contain any vended or static credential value

### Scenario: An empty table storage location is rejected before any object-store access

* *GIVEN* a Delta table whose catalog metadata carries an absent or empty storage location
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return a `UdfError` naming the table and its empty storage location, from
  ONE check that runs BEFORE the vended/static storage split, so both values of
  `use_vended_credentials` report the IDENTICAL error text
* *AND* the reader MUST NOT substitute the catalog URI, the CONNECTION's endpoint, or any other
  CONNECTION-derived value for the empty location, because none of them denotes the table's object
  store — matching the rule `vs-adapter/pushdown-planning-file-resolution` already applies to an
  empty Iceberg table `location`
* *AND* the reader MUST NOT request vended credentials, build an object store, or read one log file,
  so a malformed catalog response costs zero object-storage access

### Scenario: The format reader is selected at one site and refuses a mismatched pairing

* *GIVEN* a `ScanSource` whose variant pairs one live catalog session with the table it reads
* *WHEN* the adapter selects the format reader for that source
* *THEN* the adapter SHALL match `ScanSource` EXHAUSTIVELY at exactly ONE site, which returns a boxed
  `FormatReader`, so adding a third table format or a third catalog kind is a compile error at that
  site rather than a silent fall-through
* *AND* the Unity Catalog variant SHALL check the loaded table's FORMAT tag and SHALL return a
  `UdfError` naming the table and the reported format when it is not Delta, because Unity Catalog can
  report a non-Delta format and misrouting one into the Delta reader would surface a log-not-found
  error instead of a clear format refusal
* *AND* that check MUST NOT be replaced by an assumption that the Unity Catalog listing filter
  already excluded non-Delta tables, because the single-table load applies no listing filter
* *AND* the selection site MUST NOT match `CatalogKind`, so the source-level probe of
  `vs-adapter/catalog-kind-selection` — which asserts `CatalogKind`'s variant names appear in no
  production module beyond the enum, its resolver, the catalog-client construction site, credential
  validation, and the pushdown scan-source construction site — stays intact and unweakened

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

### Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind

* *GIVEN* a virtual schema created with `CATALOG_KIND` set to `UNITY_CATALOG`, and a query against one
  of its Delta tables
* *WHEN* the adapter handles the resulting pushdown request
* *THEN* the adapter SHALL select the Delta format reader through the scan-source seam and SHALL plan
  the query from the `ResolvedScan` that reader returns
* *AND* the reader's resolved partition columns SHALL reach the shard-invariant common spec and its
  per-file partition values SHALL reach the per-shard file entries, so the deferred scan-side partition
  reconstruction this reader's contract names is satisfied by
  `datafusion-scan/scan-execution-partition-values` rather than left open
* *AND* the reader SHALL still apply NO filter-based file pruning, because per-file statistics and
  partition pruning remain issue #321, so a filter narrows the rows the scan emits without narrowing
  the files it reads
* *AND* the reader SHALL now GATE the Delta reader protocol and reader-feature set
  (`vs-adapter/delta-reader-feature-gating`), SUPERSEDING the recorded rule that it performs no such
  gating: a table whose reader features this engine does not implement is no longer query-reachable,
  so this feature records NO remaining reader-feature exception
* *AND* the reader SHALL refuse a request that reads or emits a column whose Delta type this engine
  cannot render faithfully, per column rather than per table
  (`vs-adapter/delta-type-mapping`), so a table carrying one struct column stays queryable on its
  other columns
* *AND* every error the reader surfaces on this path MUST be returned as an error value, never raised
  as a panic, and MUST NOT contain any vended or static credential value
