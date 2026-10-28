# Feature: Delta Table Planning

The adapter resolves a Delta Lake table into the engine's `ScanSpec` at plan time, so file-level sharding, the pushdown wire format, streaming emit, and the memory model are shared with the Iceberg path.

<!-- DELTA:CHANGED -->
## Background

* Delta resolution uses `delta-kernel-rs` log replay behind the `FormatReader` trait in `lakehouse-engine`. The Iceberg path uses `iceberg`'s `plan_files()` (`file-planning/pushdown-planning-file-resolution`). Both paths emit the same `ScanSpec`.
* `FormatReader` lives in the engine, not in `lakehouse-catalog`, because Iceberg file planning and the `ScanSpec` wire format live there and the catalog crate must not name `iceberg`, `datafusion`, `arrow`, `parquet`, `object_store`, or `delta_kernel`. `CatalogClient` stays metadata-only.
* Each `FormatReader` owns its whole resolution: catalog request, storage credential, and file discovery. The Iceberg path needs the catalog's `TableMetadata`. The Delta path needs a table-root URL and a credentialed object store.
* Format dispatch is one exhaustive match over `ScanSource`, which pairs a live catalog session with the table it reads.
* The log-replay step takes an injected object store, so tests run it over `file://` and over SeaweedFS. Building the store is the reader's job.
* Delta never writes a partition column's value into the data file, so the scan spec must carry partition values. The Iceberg reader leaves `partition_columns` empty and fills `partition_values` only with the `identity` partition values of each file's own spec: see `scan-read-path/scan-execution-field-id-projection-absent-fields`.
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
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The Iceberg format reader owns file resolution and binds columns by field-id

* *GIVEN* the Iceberg file-resolution logic, reached by the single-table pushdown path and every join leg
* *WHEN* the Iceberg format reader resolves a table's scan through the trait
* *THEN* the reader SHALL OWN that resolution logic outright, with no separately published file-resolution entry point, because `file-planning/pushdown-format-neutral-resolution` routes every caller through this seam
* *AND* the reader SHALL construct each associated positional-delete reference as the Iceberg positional-delete variant of the format-neutral delete mechanism, serialized as `{"path":…,"size":…,"content_type":"position_deletes"}` inside the per-shard file list, and each Puffin deletion vector as the Iceberg deletion-vector variant
* *AND* the returned scan SHALL carry EMPTY partition columns, and each of its file entries SHALL carry only the `identity` partition values of that file's own partition spec, per `scan-read-path/scan-execution-field-id-projection-absent-fields`
* *AND* every logical field the Iceberg reader emits SHALL carry its Iceberg field-id and NO physical name, so an Iceberg column is still bound by field-id and the physical-name and identity binding strategies are unreachable from the Iceberg path
<!-- /DELTA:CHANGED -->
