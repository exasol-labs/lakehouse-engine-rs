# Feature: DataFusion Scan Execution, Broadcast Join

The scan UDF executes a node-local broadcast inner equi-join. It registers the fact side's per-shard files and the dimension side's full file list in one DataFusion session, applies the pushed projection, filter, and post-join ordering and cap, and streams the joined rows as Arrow batches through the Arrow C Data Interface.

## Background

* Only SDK `Value` types and Arrow C Data Interface structs cross the `.so` boundary.
* Both sides register from the file lists in the scan spec, the fact side from the per-shard argument and the dimension side from the common-spec join block, each against its own logical schema. The UDF holds no state and discovers no files.
* The memory pool is sized from the per-instance memory limit as on the raw-scan path, and the bounded dimension side is the hash-join build side.
* The hash join collects the whole dimension side into one hash table. A build split by hash partition hands the fact scan one key filter per partition, and Parquet statistics cannot evaluate that filter (`specs/_decision/073-change-broadcast-join-collect-left.md`).
* Statistics pruning ignores an IN-list longer than the table's Parquet `max_in_list_size`, which DataFusion defaults to 20. Every scan table sets it to the hash join's IN-list pushdown limit, so the whole pushed key set prunes.
* Storage keys and secrets never appear in an error message.
* Each side reads its files through its own storage credential, because a catalog can downscope a vended credential to the table it loaded. The fact side's storage value is the whole-spec `storage`. The dimension side's is in the join block. Each is a reference or a sealed envelope (`storage-access/scan-spec-credential-reference`), and both sides share one virtual schema and one CONNECTION.
* DataFusion keys its object-store registry on `scheme://host[:port]` and cannot see a path, so one bucket holds one registered store. A routing `ObjectStore` decorator is registered once per bucket and holds one inner store per side. It selects the side that owns a requested `object_store::Path`, and it is used on every spec with a join block, including when both credentials are identical.
* Routing matches a side's own data-file and delete-file paths first and its table root by longest prefix second, because the Iceberg spec allows a table's files outside its `location` (Appendix E, Version 4: "Absolute paths must be used for files that do not share a common prefix with the table location"). The Iceberg REST `StorageCredential.prefix` follows the same longest-prefix rule.
* Every path the scan requests for a side is one that side's spec names, or a prefix under its table root. That covers range reads of Parquet footers, positional-delete files, and Iceberg deletion-vector blobs inside their Puffin files, and one `head` plus a prefixed `list` on the schema-inference branch. A Delta deletion-vector sidecar's path is not in the spec, so only the side's table root routes it, and a sidecar stored by absolute path outside that root matches neither rule.
* A side whose files span more than one bucket is refused with a clear error. The Iceberg spec permits such a layout, so this is a scoped exception (#304).
* An ADLS store is container-scoped and its `Path` is container-relative, so path routing cannot separate two containers of one storage account. The scan-time check that both sides share one store is the only guard for that case (`storage-access/pushdown-planning-cloud-credentials-vended-storage`).
* The post-join bounds live in the join block as `post_join_limit` and `post_join_order_by`. The shard-invariant `limit` and `order_by` mean a pre-join bound on single-table legs, and a spec without a join block carries no post-join field. Both join-block fields are additive and defaulted.
* The limit applies after the join and WHERE in the rendered SQL, and DataFusion's `push_down_limit` does not push a fetch into the inputs of a non-cross inner join. A pre-join cap would discard fact rows that match the dimension side, so the scan carries no input-side limit.
* A per-shard top-N must rank each key by the value the shard emits, which is what the Exasol wrapper ranks. A column emitted through the JSON rendering or the `CAST(... AS VARCHAR)` fallback arrives as text. An empty string arrives as NULL, because Exasol's VARCHAR has no empty string. A `NaN` in a `Float32` or `Float64` column arrives as NULL. String comparison is assumed to agree between DataFusion and Exasol, as on the single-table top-N path.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers both tables and executes the inner equi-join

* *GIVEN* a reconstituted join scan spec
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL register the fact side's assigned files and the dimension side's full file list as two separate tables in ONE DataFusion session, each with its declared logical schema
* *AND* the join block SHALL carry the fact relation's alias and the dimension relation's alias as REQUIRED fields with no deserialization default, and the join SQL SHALL alias each side's derived relation with its own alias from the join block, never with an alias the UDF chooses itself
* *AND* each relation SHALL keep its columns under their own uppercased names, so the pushed condition, filter, projection, and post-join ordering reference a column as a pair qualified by its relation alias, `"<alias>"."<COLUMN>"`
* *AND* a column name that both sides carry SHALL stay two distinct columns, one per relation
* *AND* the UDF SHALL resolve each projected column and each post-join sort key by its (relation alias, column) pair, MUST NOT match a column by its bare name, and SHALL choose the column's rendering and its ranking from the type it has on that relation
* *AND* the UDF SHALL emit each projected column that carries a relation alias under the output name `<alias>_<column>`, the only name in the join SQL that joins the alias and the column into one identifier, so two same-named projected columns stay distinct in the output
* *AND* the UDF SHALL register each side's table against that side's OWN RESOLVED storage backend, and the redaction set guarding a read error SHALL be the UNION of both sides' resolved secret values, because the one per-invocation resolution defines the set before either store exists
* *AND* the UDF SHALL execute an inner equi-join of the two registered tables on the rendered join condition
* *AND* the UDF MUST NOT resolve or discover any file beyond the two file lists carried in the spec
* *AND* the UDF SHALL register a side whose spec a Delta reader produced by the SAME path it registers an Iceberg-produced one, reading only the neutral logical schema and file list carried in the spec
<!-- /DELTA:CHANGED -->
