# Feature: Delta Table Planning

The adapter resolves a Delta Lake table into the engine's `ScanSpec` at plan time, so file-level sharding, the pushdown wire format, streaming emit, and the memory model are shared with the Iceberg path.

## Background

* Delta resolution uses `delta-kernel-rs` log replay behind the `FormatReader` trait in `lakehouse-engine`. The Iceberg path uses `iceberg`'s `plan_files()` (`file-planning/pushdown-planning-file-resolution`). Both paths emit the same `ScanSpec`.
* `FormatReader` lives in the engine, not in `lakehouse-catalog`, because Iceberg file planning and the `ScanSpec` wire format live there and the catalog crate must not name `iceberg`, `datafusion`, `arrow`, `parquet`, `object_store`, or `delta_kernel`. `CatalogClient` stays metadata-only.
* Each `FormatReader` owns its whole resolution: catalog request, storage credential, and file discovery. The Iceberg path needs the catalog's `TableMetadata`. The Delta path needs a table-root URL and a credentialed object store.
* Format dispatch is one exhaustive match over `ScanSource`, which pairs a live catalog session with the table it reads. `ScanSource` is not `CatalogKind`, so the permitted `CatalogKind` match sites in `vs-adapter/catalog-kind-selection` gain no entry.
* The log-replay step takes an injected object store, so tests run it over `file://` and over SeaweedFS. Building the store is the reader's job.
* Delta never writes a partition column's value into the data file, so the scan spec must carry partition values. The Iceberg identity-transform rule (1) of the table spec's "Column Projection" stays an unimplemented, recorded trade-off in `scan-read-path/scan-execution-field-id-projection`. The Iceberg reader leaves `partition_columns` and `partition_values` empty.
* Neutral `ScanSpec` homes, one per concept: a data file's deletions in `FileEntry::deletes` as `DeleteMechanism` values, a data file's partition values in `FileEntry::partition_values`, the ordered partition-column names in `CommonScanSpec::partition_columns`, and each column's binding key on its `LogicalField`. The scan side dispatches on content, not on format. The column-mapping mode is not carried on the wire. It decides which binding key each column's note records at `createVirtualSchema`, and the pushdown reader checks the declaration against the log. `DeltaDeletionVectorStorage` is only the payload enum of the deletion-vector delete mechanism.
* Pushdown under the Unity Catalog kind plans a Delta scan (`vs-adapter/catalog-kind-selection`). Deletion vectors, partition values, and column mapping apply at scan time (`scan-read-path/scan-execution-delta-deletion-vectors`, `scan-read-path/scan-execution-partition-values`).
* The scan carries no per-file minimum or maximum. `delta_kernel` evaluates bounds during log replay and returns a selection vector, so pruning (`delta/delta-file-pruning`) needs no wire shape for statistics.
* Reader-feature gating is `delta/delta-reader-feature-gating`. Type mapping is `delta/delta-type-mapping`.
* A Delta table's logical schema, partition columns, and refused columns are decided once at `createVirtualSchema` from Unity Catalog's copy: each column's `type_json` (the Delta `StructField` JSON), the `delta.columnMapping.mode` table property, and each column's `partition_index`. They are recorded in each column's note (`vs-adapter/column-source-notes`), and `createVirtualSchema` reads no Delta log. The pushdown reader opens the gated snapshot for the feature gate, active files, deletion vectors, and partition values, checks the declaration against it (`delta/delta-table-planning-declaration-checks`), and derives no logical field from it.
* Delta protocol "Column Mapping": the physical name is static and the display name can change. In `name` mode readers resolve columns, partition values, and statistics by physical name, and in `id` mode by `field_id`. A recorded binding key therefore names the same column after a source-side rename. Under `none` mode a declared name is the column's identity.
* The protocol honors `delta.columnMapping.mode` only when the table's protocol supports the `columnMapping` feature. `createVirtualSchema` reads no protocol, so the pushdown reader compares the declaration with the mode the gated snapshot honors and fails the query on disagreement.
* DataFusion rewrites `c IS NULL` to `false` for a non-nullable field. A stale `nullable: false` would drop matching rows, so the pushdown reader fails a query whose declared non-nullable column the snapshot marks nullable. A declared nullable column that the log marks non-nullable plans normally.
* Scoped exception: a type change the declaration does not record, or one recorded after the last `REFRESH`, is not validated by the declaration. A data file written under an unsupported change fails the query at scan time, because the scan admits only the widening pairs `scan-types/type-relaxation` proves.
* Scoped exception: a column dropped at the source after the last `REFRESH`, under `name` or `id` mode, reads NULL from files written without it until `REFRESH`. A dropped non-nullable column fails a query that reads such a file (`scan-read-path/scan-execution-field-id-projection-absent-fields`). This mirrors the Iceberg exception in `file-planning/pushdown-planning-file-resolution`.
* The reader-feature gate stays at pushdown. A table the gate refuses is listed at `createVirtualSchema` and fails at query time.
* `delta_kernel` leaves `add.path` percent-encoded, and the engine's `ListingTableUrl::parse` path decodes it before any object-store request.
* Every error is a `UdfError`, never a panic, and no error text carries a credential value.


## Scenarios

### Scenario: A Delta table resolves its current version's active data files

* *GIVEN* a Delta table whose transaction log holds more than one JSON commit, reachable through a
  credentialed object store at a table-root URL
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL resolve the log's CURRENT version and replay every JSON commit and any
  checkpoint up to it, and SHALL return exactly the data files active at that version, each entry
  carrying the `add` action's `path` verbatim and its `size`, so a file added at one version and
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
  `delta_kernel` compares those bounds itself during log replay and returns a selection vector rather
  than values, so plan-time pruning (`delta/delta-file-pruning`) needs no wire shape for them
* *AND* the reader MUST NOT construct its own object store: it SHALL read the log through the store
  it is given, so the replay is exercised over a local filesystem store as well as over S3

### Scenario: Partition values are carried per data file, including a NULL partition value

* *GIVEN* a Delta table partitioned by one column, whose active data files include one written to the
  Hive default partition directory because that row's partition value is NULL
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each returned file entry SHALL carry that file's Delta `partitionValues` in the
  format-neutral per-file `partition_values` map, one entry per partition column, holding the
  serialized value or an explicit absent value for NULL, because Delta stores a partition column's
  value ONLY in the transaction log and never inside the data file, so a scan that reads the Parquet
  file alone cannot recover it
* *AND* that map SHALL be the SAME field a Hive-style partition value populates, and MUST NOT be reachable only through a
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
* *AND* that entry SHALL carry the Delta `deletionVector` descriptor verbatim, its storage kind, its
  `pathOrInlineDv`, its `offset`, its `sizeInBytes`, and its `cardinality`, as ONE deletion-vector
  delete mechanism in the entry's single format-neutral `deletes` list, resolved into no path and
  applied to no row at plan time, because applying it belongs to scan time (`scan-read-path/scan-execution-delta-deletion-vectors`)
* *AND* the storage kind SHALL be modelled as a closed set of the Delta protocol's three kinds
  (UUID-relative, inline, absolute path), so a descriptor naming a kind outside that set fails at
  plan time rather than reaching the scan as an unread string
* *AND* the deletion vector SHALL be a DISTINCT variant of the delete mechanism, never an Iceberg
  positional-delete reference, because the two mechanisms are unrelated: an Iceberg delete reference
  names a whole delete FILE, while a Delta deletion vector names a byte range inside a shared `.bin`
  file
* *AND* a Delta entry's `deletes` list SHALL hold ONLY its deletion vector and MUST NOT hold any
  Iceberg delete-file reference, so the Iceberg positional-delete reader is never handed a reference
  it would misread
* *AND* the plan-time relativization of file paths against the table root SHALL apply to Iceberg
  delete-file references ONLY, and MUST leave a deletion vector's `pathOrInlineDv` untouched, because
  that member is a UUID token or an inline payload rather than an object-store path

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
  the scan binds it by its own logical name, because an ordinal is a value the writer never wrote into any file, and carrying it invites a
  false match against a file that happens to carry field-ids
* *AND* a column under `id` or `name` mode whose `type_json` lacks the annotation its mode requires,
  or carries it malformed, SHALL be declared with a note whose refusal names the column, the mode,
  and the problem, and `createVirtualSchema` SHALL neither fail nor skip the table, because its
  ordinal position and its logical name are values the writer never used and the refusal is scoped
  to the column as `delta/delta-type-mapping` scopes a type refusal
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
  (`unity-catalog/unity-catalog-vended-credentials`), so the `abfs://` plaintext-consent gate and the S3
  address rule apply identically to the Iceberg and the Delta path
* *AND* under vending the reader SHALL fail with an error naming the table when its catalog reported
  no vending key, and MUST NOT fall back to the CONNECTION's static credential, because a silent
  fallback would read object storage with a credential the operator did not select for this table
* *AND* with vending disabled the reader SHALL use the CONNECTION's static storage backend unchanged
* *AND* the reader SHALL return the EFFECTIVE storage backend alongside the file list, and the
  shard-invariant common spec SHALL carry that backend SEALED (the AES-GCM envelope of `storage-access/scan-spec-credential-reference`) under vending or when the CONNECTION names `aws_assume_role_arn`, and a REFERENCE to the CONNECTION that supplied it otherwise, both of which the scan UDF resolves to a field-for-field equal backend under that same feature
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
  store, matching the rule `file-planning/pushdown-planning-file-resolution` applies to an
  empty Iceberg table `location`
* *AND* the reader MUST NOT request vended credentials, build an object store, or read one log file,
  so a malformed catalog response costs zero object-storage access

### Scenario: The format reader is selected at one site and refuses a mismatched pairing

* *GIVEN* a `ScanSource` whose variant pairs one resolved session with the table it reads
* *WHEN* the adapter selects the format reader for that source
* *THEN* the adapter SHALL match `ScanSource` EXHAUSTIVELY at exactly ONE site, which returns a boxed `FormatReader`, so adding a table format or a catalog source is a compile error at that site rather than a silent fall-through
* *AND* the Unity Catalog variant SHALL match the loaded table's FORMAT tag exhaustively: the Delta tag selects the Delta reader, the Parquet tag selects the Unity Parquet reader of `unity-catalog/unity-parquet-table-planning`, and the Iceberg tag returns a `UdfError` naming the table and the reported format, because a table routed into another format's reader surfaces a missing log or a wrong schema instead of a clear format refusal
* *AND* that match MUST NOT be replaced by an assumption that the Unity Catalog listing filter already excluded a format, because the single-table load applies no listing filter
* *AND* the selection site MUST NOT match `CatalogKind`, so the permitted-site set of `vs-adapter/catalog-kind-selection` (the enum with its resolver, the catalog-client construction site, credential validation, and the pushdown scan-source construction site) stays intact and gains no file

### Scenario: The Iceberg format reader owns file resolution and binds columns by field-id

* *GIVEN* the Iceberg file-resolution logic, reached by the single-table pushdown path and every join leg
* *WHEN* the Iceberg format reader resolves a table's scan through the trait
* *THEN* the reader SHALL OWN that resolution logic outright, with no separately published file-resolution entry point, because `file-planning/pushdown-format-neutral-resolution` routes every caller through this seam
* *AND* the reader SHALL construct each associated positional-delete reference as the Iceberg positional-delete variant of the format-neutral delete mechanism, serialized as `{"path":…,"size":…,"content_type":"position_deletes"}` inside the per-shard file list
* *AND* the returned scan SHALL carry EMPTY partition columns, and each of its file entries SHALL carry EMPTY partition values
* *AND* every logical field the Iceberg reader emits SHALL carry its Iceberg field-id and NO physical name, so an Iceberg column is still bound by field-id and the physical-name and identity binding strategies are unreachable from the Iceberg path

### Scenario: The Delta reader is reached from production pushdown under the Unity Catalog kind

* *GIVEN* a virtual schema created with `CATALOG_KIND` set to `UNITY_CATALOG`, and a query against one
  of its Delta tables
* *WHEN* the adapter handles the resulting pushdown request
* *THEN* the adapter SHALL select the Delta format reader through the scan-source seam and SHALL plan
  the query from the `ResolvedScan` that reader returns
* *AND* the reader's resolved partition columns SHALL reach the shard-invariant common spec and its
  per-file partition values SHALL reach the per-shard file entries, so the scan reconstructs partition columns as
  `scan-read-path/scan-execution-partition-values` specifies
* *AND* the reader SHALL PRUNE the file list by the request's filter, on partition values and on per-file statistics alike (`delta/delta-file-pruning`), so a filter narrows the files the scan reads as well as the rows it emits
* *AND* the reader SHALL GATE the Delta reader protocol and reader-feature set (`delta/delta-reader-feature-gating`), so a table whose reader features this engine does not implement is not query-reachable
* *AND* the reader SHALL refuse a request that reads or emits a column whose Delta type this engine
  cannot render faithfully, per column rather than per table
  (`delta/delta-type-mapping`), so a table carrying one struct column stays queryable on its
  other columns
* *AND* every error the reader surfaces on this path MUST be returned as an error value, never raised
  as a panic, and MUST NOT contain any vended or static credential value
