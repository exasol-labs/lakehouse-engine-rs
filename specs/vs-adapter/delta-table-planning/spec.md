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
* **This plan wires NOTHING into the production pushdown path.** The recorded refusal
  `vs-adapter/catalog-kind-selection` § "A pushdown request under the Unity Catalog kind is refused
  as not yet executable" stays in force, verbatim, and is NOT superseded here. Removing it belongs to
  #320, which applies deletion vectors, partition values, and column mapping at scan time. Wiring
  Delta planning into `handle_pushdown` before #320 exists would return silently wrong rows rather
  than a clean failure, for two verified reasons: `FileEntry::deletes` and its consumer
  `crates/lakehouse-engine/src/scan/positional_deletes.rs` model Iceberg positional-delete FILES
  only, so a Delta deletion vector left unmodelled triggers no rejection and the scan reads the
  delete-free path with deleted rows restored; and `register_file_list`
  (`crates/lakehouse-engine/src/scan/raw_scan.rs`) has no mechanism to inject a column value absent
  from the physical Parquet file, so Delta partition columns would read NULL. No interim guard is
  built either — a guard would be a slice of #320's work delivered early.
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
* **One recorded justification for the pushdown refusal is now half-true, and the refusal stands on
  its other half.** This feature records that Delta planning is not wired into `handle_pushdown`
  partly because "a Delta deletion vector left unmodelled triggers no rejection and the scan reads the
  delete-free path with deleted rows restored". After this delta the deletion vector IS modelled and
  IS refused at read time (`datafusion-scan/scan-execution-positional-deletes`), so that half no
  longer holds. The other half is untouched: the scan still has no mechanism to materialize a
  partition column absent from the physical Parquet file, so a Delta pushdown would still read those
  columns as NULL. The recorded refusal therefore stays in force, unedited, and its own scenario needs
  no change.
* **Nothing this delta adds is consumed at scan time.** `partition_columns` and `partition_values` are
  carried and round-tripped and read by nothing, exactly as their Delta-block predecessors were.
  Consuming them is issue #320.
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

### Scenario: A Delta type this plan does not map is refused at plan time

* *GIVEN* a Delta table whose schema declares a field whose type has no Arrow type tag in the
  engine's tag vocabulary — for example `byte`, `short`, `binary`, `array`, `map`, `struct`, or
  `variant`
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL return a `UdfError` naming the column and its Delta type, and MUST NOT emit
  a logical field whose Arrow tag widens, narrows, or otherwise misdescribes the column, because a
  misdescribed tag returns wrong values rather than an error
* *AND* the error SHALL state that broad Delta type mapping — including the incompatible-type
  `VARCHAR(2000000)`-via-JSON convention — is issue #322, so the refusal reads as a scoped gap rather
  than an unsupported table
* *AND* the reader SHALL map the Delta primitive types that DO have a tag — `boolean`, `integer`,
  `long`, `float`, `double`, `string`, `date`, `timestamp`, `timestamp_ntz`, and `decimal(p,s)` — and
  SHALL carry each field's nullability from the Delta schema
* *AND* the reader SHALL perform NO Delta reader-feature gating, because gating is issue #322 and a
  gate added here would refuse the deletion-vector and column-mapping fixtures this plan resolves

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
  production module beyond the enum, its resolver, the client construction site, credential
  validation, and the pushdown refusal — stays intact and unweakened

### Scenario: Iceberg planning is byte-identical through the new seam

* *GIVEN* the shipped Iceberg file-resolution entry point `resolve_file_list` and its callers — the
  single-table pushdown path, every join leg, and the external test callers
* *WHEN* the Iceberg format reader resolves a table's scan through the trait
* *THEN* the reader SHALL delegate to `resolve_file_list`, whose name, `pub` visibility, signature,
  and every one of its call sites stay unchanged, so the shipped Iceberg planning path keeps its shape
  and its callers
* *AND* `resolve_file_list` SHALL construct each associated positional-delete reference as the Iceberg
  positional-delete variant of the format-neutral delete mechanism — the ONE body change issue #342
  makes to that function — and its SERIALIZED per-shard file list SHALL stay byte-identical to the
  pre-#342 encoding, including the delete-carrying 3-tuple form and its
  `{"path":…,"size":…,"content_type":"position_deletes"}` member encoding
* *AND* the resolved scan SHALL carry EMPTY partition columns, and each of its file entries SHALL carry
  EMPTY partition values, so the serialized shard-invariant common blob and per-shard file list
  for every Iceberg request stay byte-identical to their pre-#342 encoding
* *AND* every logical field the Iceberg reader emits SHALL carry its Iceberg field-id and NO physical
  name, so an Iceberg column is still bound by field-id and the physical-name and identity binding
  strategies are unreachable from the Iceberg path
* *AND* the existing Iceberg unit, integration, and E2E suites MUST pass with no change to any test
  assertion or expected value
* *AND* collapsing `resolve_file_list` into the Iceberg reader SHALL still be deferred to #320, which
  removes its direct callers when it routes production pushdown through this seam

### Scenario: Delta planning adds no production pushdown path in this plan

* *GIVEN* the recorded refusal that a pushdown request under the Unity Catalog kind is not yet
  executable, and its existing test asserting the refusal message
* *WHEN* a pushdown request arrives whose virtual schema was created with `CATALOG_KIND` set to
  `UNITY_CATALOG`
* *THEN* the adapter SHALL still return that refusal, before any catalog client, credential, or file
  resolution, and its existing test MUST pass UNEDITED
* *AND* `handle_pushdown` MUST NOT select a format reader, resolve a Delta scan, or reach any code
  this plan adds, so a Unity Catalog pushdown still issues no catalog request at all
* *AND* the recorded refusal scenario SHALL NOT be superseded, narrowed, or re-scoped by this plan,
  because its removal is #320's and depends on deletion-vector, partition-value, and column-mapping
  application existing
* *AND* the Delta path this plan adds SHALL be reachable from its own tests alone, so the plan's
  value is verified without exposing a query path that returns wrong rows
