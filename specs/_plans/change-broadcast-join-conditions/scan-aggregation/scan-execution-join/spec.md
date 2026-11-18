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
### Scenario: The fact scan skips row groups that no dimension join key matches

* *GIVEN* a join scan spec whose fact file holds row groups that each cover a narrow join-key range, and whose dimension side keeps a few join keys after its filter
* *WHEN* the scan UDF runs
* *THEN* the fact scan SHALL receive the dimension side's join-key bounds as a filter once the hash table is built, plus its key set when the dimension side holds at most 150 distinct join keys (DataFusion's IN-list pushdown limit)
* *AND* the fact scan SHALL skip, on Parquet statistics, every row group whose key range lies outside those bounds
* *AND* for a join on one plain-column key, when the key set reaches the fact scan, the fact scan SHALL also skip every row group whose key range holds none of those keys, at any key count up to that limit
* *AND* for a join on several keys, the fact scan SHALL skip every row group that lies outside the bounds of any one key column, and MAY read a row group whose key ranges each hold some dimension key although no dimension key tuple matches, because DataFusion delivers that key set as one `struct(...) IN (...)` filter, which Parquet statistics cannot evaluate
* *AND* the joined rows SHALL be exactly the inner join's rows, including when the filtered dimension side is empty, no key matches, a join key is NULL, every fact key matches, the filter constrains the join key, the join has several keys, or a key compares an expression
<!-- /DELTA:CHANGED -->
