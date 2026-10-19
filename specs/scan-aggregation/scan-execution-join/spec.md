# Feature: DataFusion Scan Execution, Broadcast Join

The scan UDF executes a node-local broadcast inner equi-join. It registers the fact side's per-shard files and the dimension side's full file list in one DataFusion session, applies the pushed projection, filter, and post-join ordering and cap, and streams the joined rows as Arrow IPC batches.

## Background

* Only SDK `Value` types and Arrow IPC byte buffers cross the `.so` boundary.
* Both sides register from the file lists in the scan spec, the fact side from the per-shard argument and the dimension side from the common-spec join block, each against its own logical schema. The UDF holds no state and discovers no files.
* The memory pool is sized from the per-instance memory limit as on the raw-scan path, and the bounded dimension side is the hash-join build side.
* Storage keys and secrets never appear in an error message.
* Each side reads its files through its own storage credential, because a catalog can downscope a vended credential to the table it loaded. The fact side's storage value is the whole-spec `storage`. The dimension side's is in the join block. Each is a reference or a sealed envelope (`storage-access/scan-spec-credential-reference`), and both sides share one virtual schema and one CONNECTION.
* DataFusion keys its object-store registry on `scheme://host[:port]` and cannot see a path, so one bucket holds one registered store. A routing `ObjectStore` decorator is registered once per bucket and holds one inner store per side. It selects the side that owns a requested `object_store::Path`, and it is used on every spec with a join block, including when both credentials are identical.
* Routing matches a side's own data-file and delete-file paths first and its table root by longest prefix second, because the Iceberg spec allows a table's files outside its `location` (Appendix E, Version 4: "Absolute paths must be used for files that do not share a common prefix with the table location"). The Iceberg REST `StorageCredential.prefix` follows the same longest-prefix rule.
* Every path the scan requests for a side is one that side's spec names, or a prefix under its table root. That covers range reads of Parquet footers and positional-delete files, and one `head` plus a prefixed `list` on the schema-inference branch.
* A side whose files span more than one bucket is refused with a clear error (`validate_uniform_object_store_files`). The Iceberg spec permits such a layout, so this is a scoped exception.
* An ADLS store is container-scoped and its `Path` is container-relative, so path routing cannot separate two containers of one storage account. The scan-time `validate_sides_share_one_store` check is the only guard for that case (`storage-access/pushdown-planning-cloud-credentials-vended-storage`).
* The post-join bounds live in the join block as `post_join_limit` and `post_join_order_by`. The shard-invariant `limit` and `order_by` mean a pre-join bound on single-table legs, and a spec without a join block carries no post-join field. Both join-block fields are additive and defaulted.
* The limit applies after the join and WHERE in the rendered SQL, and DataFusion's `push_down_limit` does not push a fetch into the inputs of a non-cross inner join. A pre-join cap would discard fact rows that match the dimension side, so the scan carries no input-side limit.
* A per-shard top-N must rank each key by the value the shard emits, which is what the Exasol wrapper ranks. A column emitted through the JSON rendering or the `CAST(... AS VARCHAR)` fallback arrives as text. An empty string arrives as NULL, because Exasol's VARCHAR has no empty string. A `NaN` in a `Float32` or `Float64` column arrives as NULL. String comparison is assumed to agree between DataFusion and Exasol, as on the single-table top-N path.

## Scenarios

### Scenario: Scan reconstitutes a join scan spec carrying two file lists

* *GIVEN* a scan invocation whose common-spec argument carries a join block (the dimension side's table root, full file list, logical schema, name mapping, its OWN storage value, the rendered join condition, and the join type) and whose per-shard argument carries the fact side's `(path, size)` file subset
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL reconstitute one join `ScanSpec` whose fact files come from the per-shard argument and whose dimension side and every other field come from the common spec
* *AND* the join block's storage value SHALL be a REQUIRED field with no deserialization default, so a join block that carries none fails to deserialize rather than silently reusing the whole-spec storage value
* *AND* the reconstituted spec SHALL carry TWO storage values — the fact side's as the whole-spec `storage` value and the dimension side's inside the join block, each either a connection REFERENCE or a SEALED envelope under `storage-access/scan-spec-credential-reference` — and the UDF MUST NOT read either side's files through the other's resolved backend
* *AND* a parse failure on either argument SHALL surface an error identifying scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token from EITHER side's backend
* *AND* the reconstituted spec MUST NOT carry any catalog identifier, because the scan UDF never contacts the catalog

### Scenario: Scan registers both tables and executes the inner equi-join

* *GIVEN* a reconstituted join scan spec
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL register the fact side's assigned files and the dimension side's full file list as two separate tables in ONE DataFusion session, each with its declared logical schema and each exposing its columns under the Exasol-facing (uppercased) names the pushed condition and projection reference
* *AND* the UDF SHALL register each side's table against that side's OWN RESOLVED storage backend, and the redaction set guarding a read error SHALL be the UNION of both sides' resolved secret values, because the one per-invocation resolution defines the set before either store exists — SUPERSEDING the recorded per-side set
* *AND* the UDF SHALL execute an inner equi-join of the two registered tables on the rendered join condition
* *AND* the UDF MUST NOT resolve or discover any file beyond the two file lists carried in the spec
* *AND* the UDF SHALL register a side whose spec a Delta reader produced by the SAME path it registers an Iceberg-produced one, reading only the neutral logical schema and file list carried in the spec

### Scenario: Each join side reads its files through its own storage credential

* *GIVEN* a join scan spec whose fact-side and dimension-side storage backends hold DIFFERENT credentials, and whose two sides' files resolve to the SAME object-store bucket
* *WHEN* the scan UDF builds its DataFusion session context and reads both sides' files
* *THEN* the UDF SHALL register, under that bucket's single store key, ONE routing object store holding one inner store per side, each inner store configured from that side's OWN storage backend
* *AND* the UDF SHALL route every object-store operation carrying a path — including each `get`, ranged read, `head`, and listing call — to the inner store of the side that owns the requested path, so no request for one side's file is ever issued with the other side's credential
* *AND* the UDF SHALL select the owning side by matching the requested path against that side's OWN data-file and positional-delete-file paths FIRST, and only then against the sides' table roots by longest-prefix match, so a file that the Iceberg spec permits to sit outside its table root is still routed to its own side
* *AND* a listing operation carrying a path prefix SHALL route by that SAME two-step rule rather than by a rule of its own, because the schema-inference branch issues a prefixed listing whose prefix is either the first data file's own exact path or a directory under that side's table root
* *AND* when the two sides' file paths and roots leave more than one side eligible for a path, the UDF SHALL resolve the tie deterministically in favour of the fact side, so one spec never routes the same path two ways across invocations
* *AND* the UDF SHALL apply this routing on EVERY spec carrying a non-empty join block, including one whose two backends are byte-identical, so the common same-warehouse case takes the identical code path
* *AND* the joined rows SHALL be identical to those the same query produced when both sides shared one credential that could read both prefixes, so this routing narrows which credential is used and never which rows are returned
* *AND* no credential value from EITHER side SHALL appear in any error the UDF surfaces while building or using the routing store

### Scenario: Two join sides in different buckets register two separate stores

* *GIVEN* a join scan spec whose fact side's files resolve to one bucket and whose dimension side's files resolve to a DIFFERENT bucket of the same object-storage backend
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL register one store per bucket, each configured from the storage backend of the side whose files resolve to that bucket
* *AND* neither registration SHALL overwrite the other, because the two buckets yield different DataFusion registry keys
* *AND* each side's reads SHALL be issued with that side's own credential exactly as in the shared-bucket case, so the different-bucket case needs no separate code path in the caller
* *AND* the same SHALL hold when the two sides resolve to different BACKENDS — an `s3://` fact side and an `abfss://` dimension side — because store construction dispatches on each side's OWN backend and the two schemes yield different registry keys; the UDF MUST NOT address one side's files through the other side's backend arm, and no plan-time check refuses such a join (`storage-access/pushdown-planning-cloud-credentials-vended-storage`)

### Scenario: A requested path owned by no join side is a clear error

* *GIVEN* a join scan spec whose two sides' file lists and table roots are known
* *WHEN* the routing object store receives an operation for a path that matches no side's data-file or delete-file path and lies under no side's table root
* *THEN* the routing store SHALL return an error rather than choose a side, because choosing one would issue the request with a credential of unknown scope for that path
* *AND* the error SHALL name the unroutable path and the table roots that were tried, so the defect is attributable to the plan that produced the spec
* *AND* the error MUST NOT contain any storage access key, secret key, session token, or SAS token from any side's backend
* *AND* a bucket-wide listing operation carrying NO path prefix SHALL be treated as unroutable by the same rule, because it cannot be attributed to one side

### Scenario: A post-join ordering and cap bound each join shard to its own top-N over the joined output

* *GIVEN* a join scan spec whose join block carries a post-join ordering (one or more sort keys, each with a column, a direction, and a NULL placement) and a post-join cap `n`
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit exactly `n` joined rows that satisfy both the join condition and the filter, or every such row when the shard yields fewer than `n`, and no omitted joined row SHALL outrank an emitted row under the keys in order, each with its direction and NULL placement
* *AND* the ordering and the cap SHALL apply AFTER the join and its WHERE, and the executed plan MUST NOT carry a sort or a fetch below the join node on either input
* *AND* the bounded sort SHALL be a top-N that retains only `n` rows rather than a full sort of the shard's joined rows
* *AND* the UDF SHALL read the ordering from the join block and MUST NOT consult the spec's shard-invariant order-by field on this path

### Scenario: A join shard ranks each sort key by the value it emits

* *GIVEN* a join scan spec whose join block carries a post-join ordering and a post-join cap
* *WHEN* the scan UDF ranks the joined rows for its local top-N
* *THEN* the UDF SHALL rank the rows exactly as the Exasol-side wrapper ranks the emitted rows, so the shard's local top-N keeps every row of that shard that the wrapper's global top-N selects
* *AND* a key column the scan emits through the JSON rendering or the `CAST(... AS VARCHAR)` fallback SHALL rank by that emitted text, not by its native value
* *AND* an emitted empty string SHALL rank as NULL, placed by the key's NULL placement
* *AND* a `NaN` in a `Float32` or `Float64` key SHALL rank as NULL, placed by the key's NULL placement (#246)

### Scenario: Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC

* *GIVEN* a join scan spec carrying a projection spanning both sides, an optional filter, and an optional post-join row cap `n` and post-join ordering carried in its JOIN BLOCK
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit only the projected join-output columns, in spec order, for rows satisfying both the join condition and the filter
* *AND* the UDF SHALL emit no more rows than the cap when one is carried, reading it from the join block and MUST NOT consult the spec's shard-invariant row-limit field on this path (issue #307)
* *AND* a join spec whose serialized join block omits the cap, the ordering, or both SHALL deserialize with no cap and no ordering and behave exactly as an unlimited, unordered join scan, so a spec produced before this delta is unaffected
* *AND* a join block whose post-join ordering is empty SHALL serialize without the ordering key, so a broadcast plan that carries no per-shard ordering keeps its encoding
* *AND* the cap SHALL bound JOINED OUTPUT rows and MUST NOT bound either side's scanned input rows, so for a fixture whose first `n` fact rows match no dimension row the UDF SHALL still emit `n` joined rows rather than zero (issue #307)
* *AND* the UDF MUST NOT register either side with an input-side fetch, and the executed plan MUST NOT carry a limit below the join node on either input
* *AND* the UDF SHALL emit each result batch via the SDK Arrow-batch emit path (`emit_batch`), fetching one batch, emitting it, and dropping it before the next, never materializing the entire joined result set
* *AND* no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer

### Scenario: The bounded dimension side is the hash-join build side

* *GIVEN* a join scan spec whose dimension side is below the broadcast threshold and whose fact side is a large sharded subset
* *WHEN* the scan UDF plans the join
* *THEN* the join SHALL build its hash table on the bounded dimension side and probe with the fact side, so per-instance memory is bounded by the dimension side rather than the fact shard
* *AND* the DataFusion memory pool SHALL be sized from the per-instance memory limit exactly as on the raw-scan path

### Scenario: Scan reports a clear error when an assigned join file is unreadable

* *GIVEN* a join scan spec referencing a fact-side or dimension-side file that cannot be read from object storage
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL return an error identifying that the assigned data could not be read
* *AND* the error message MUST NOT contain a storage access key, secret key, session token, or SAS token from EITHER side's storage backend, because one message can be produced while either side's credential is in scope
* *AND* a dimension-side read that the dimension side's own credential is not authorised to perform SHALL surface as that same read error rather than as a wrong-rows result
