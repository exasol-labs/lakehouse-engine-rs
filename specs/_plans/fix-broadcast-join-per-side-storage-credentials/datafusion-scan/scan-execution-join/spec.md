# Feature: DataFusion Scan Execution — Broadcast Join

Extends `datafusion-scan/scan-execution` with node-local broadcast inner equi-join execution. A join scan invocation receives, in addition to its per-shard fact-file subset, the FULL dimension-side file list carried once in the shard-invariant common spec. The UDF registers both sides as Iceberg tables in ONE DataFusion session, executes the inner equi-join with the pushed projection, filter, and LIMIT, and streams the joined rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* Only SDK `Value` types and Arrow IPC byte buffers cross the `.so` boundary; no typed Arrow value does.
* Both sides register from the file lists carried in the scan spec — the fact side from the per-shard argument, the dimension side from the common-spec join block — each declared against its own logical Iceberg schema.
* The DataFusion memory pool is sized from the per-instance memory limit exactly as the raw-scan path does; the bounded dimension side is the hash-join build side.
* Storage access keys and secret keys MUST NOT appear in any error message.

<!-- DELTA:NEW -->
* **This delta fixes issue #294: a pushed-down broadcast join read BOTH tables through the FACT side's storage credential.** `join_fan_out_scan_spec` set the whole spec's single `storage` value from the fact side and `JoinSpec` carried no storage at all, so the dimension side's own credential — already resolved per side, per table location, by `resolve_vended_storage` — was discarded. Against a catalog that downscopes a vended credential to the table it loaded, the fact side's credential is DENIED on the dimension side's prefix, so the join fails to read. `JoinSpec`'s recorded claim that "credentials never appear here" and the object-store layer's "same credentials, same size index" comment are both superseded by this delta.
* **DataFusion selects an object store by BUCKET ONLY, so one bucket is served by exactly one registered store.** `get_url_key` keys the registry on `scheme://host[:port]` (`datafusion-execution-54.1.0/src/object_store.rs:266-274`), `ObjectStoreUrl::parse` rejects any URL carrying a path (`:58-72`), and the scan hands `get_store` that path-less URL (`datafusion-datasource-54.1.0/src/file_scan_config/mod.rs:640`). Two credentials therefore cannot be attached to one bucket through the registry. Databricks makes one shared metastore bucket the NORMAL case for two tables of one catalog, so refusing a credential-divergent join, or falling back to the unaccelerated N-scan path, would forfeit broadcast exactly where it is wanted.
* **The fix is a routing `ObjectStore` decorator registered once per bucket, holding one inner store per side.** Object-store trait methods receive the full `object_store::Path`, so the decorator can select the side that owns a requested path even though the registry cannot see the path. The decorator is used on EVERY spec carrying a join block, including when the two sides' credentials are byte-identical: one code path, no credential comparison, no branch that can be wrong.
* **Routing matches the side's OWN file paths first and its table root only as a fallback, because the Iceberg spec does not guarantee a table's files live under its `location`.** The NORMATIVE support is the Appendix E → Version 4 rule "Absolute paths must be used for files that do not share a common prefix with the table location" — format version 4 legislates FOR out-of-tree files rather than forbidding them. Two further normative definitions agree: `location` is "The table's base location. This is used by writers to determine where to store data files, manifest files, and table metadata files" (Table Metadata Fields) — a writer target, not a reader constraint — and `data_file.file_path` (field 100) is "Full URI for the file with FS scheme" (Data File Fields), with no containment clause. `write.data.path` ("If `write.data.path` is an absolute path, it is used directly as the base for new data files", Appendix F, Path Construction) is cited as CORROBORATION only, NOT as normative support: Appendix F is titled *Implementation Notes* and opens "This section covers topics not required by the specification but recommendations for systems implementing the Iceberg specification", and Path Construction adds "the specific construction logic is not strictly required by the spec". Routing on the table root ALONE would therefore misroute a spec-legal table. The scan discovers no files, so each side's spec already names every path that side will request; matching those paths exactly is both spec-safe and unambiguous.
* **Every path the scan requests for a side is a path that side's spec names.** The VERIFIED access set, per side, is: **zero** `head` calls per data file on the production path — every `PartitionedFile` is built from an `ObjectMeta` synthesized from the spec-carried size (`scan/positional_deletes.rs:663-665`, via `object_meta_for` at `:215-225`, whose own doc records "built without any object-store HEAD (the size is supplied by the caller)"); **exactly one** `head`, on the FIRST file only, when the spec carries no logical schema and the scan falls back to Arrow schema inference (guarded by `!logical_schema.is_empty()`, `scan/raw_scan.rs:204-217`, reaching `store.head` at `datafusion-datasource-54.1.0/src/url.rs:276`); range reads of each data file's Parquet footer, page index, and bloom filters; range reads of each associated positional-delete file (its size is supplied, so no `head`); and — on that schema-inference branch ONLY — a `list(Some(prefix))` fallback, issued either when that single `head` returns `NotFound` (`url.rs:282-291`) or when the entry path ends with `/` so `is_collection()` skips the `head` altogether (`url.rs:204-206`, `:266-274`), both reaching `store.list(Some(&full_prefix))` (`url.rs:397-398`). No other path is requested, because this feature already forbids resolving or discovering any file beyond the two carried lists. Every one of these accesses is routable: each carries either a path the side's spec enumerates exactly, or a prefix under that side's table root.
* **Routing on the two sides' own path sets is the Iceberg REST protocol's own mechanism, not an invention.** `StorageCredential.prefix` is required in the REST catalog schema and documented as "Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available" — the response is an array precisely because one credential does not cover every prefix. `select_credential_source` already implements that longest-prefix rule per side; this delta stops the scan from throwing away its per-side result.
* **PRE-EXISTING, LOUD, and NOT introduced or fixed here: a side whose own files span more than one bucket is refused.** `validate_uniform_object_store_files` fails with a clear error when a data file or delete file of one side resolves to a different scheme+host than that side's first file. The Iceberg spec permits exactly that (the v4 absolute-path rule quoted above), so this is a real deviation from the table spec — but a loud refusal, never a wrong-credential read or a wrong row. It predates this plan on both the single-table and the join path and is unchanged by it. Tracked as issue `#304` (filed by this plan; see `plan.md` § Implementation Tasks).
* **The ADLS different-container case is NOT subsumed by path routing and keeps its own plan-time and scan-time guards.** An Azure store built from a container-qualified URL is container-scoped, and the `object_store::Path` its trait methods receive is container-RELATIVE — so two tables in two containers of one storage account can produce IDENTICAL `Path` values that no path-based routing can distinguish. DataFusion's registry key drops the userinfo that carries the container (its own test asserts `s3://username:password@host:123` keys as `s3://host:123`, `datafusion-execution-54.1.0/src/object_store.rs:330-332`), so the two sides would also collapse onto one registry key. Both existing guards therefore stay.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reconstitutes a join scan spec carrying two file lists

* *GIVEN* a scan invocation whose common-spec argument carries a join block (the dimension side's table root, full file list, logical schema, name mapping, its OWN storage backend, the rendered join condition, and the join type) and whose per-shard argument carries the fact side's `(path, size)` file subset
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL reconstitute one join `ScanSpec` whose fact files come from the per-shard argument and whose dimension side and every other field come from the common spec
* *AND* the join block's storage backend SHALL be a REQUIRED field with no deserialization default, so a join block that carries none fails to deserialize rather than silently reusing the whole-spec storage value
* *AND* the reconstituted spec SHALL carry TWO storage backends — the fact side's as the whole-spec `storage` value and the dimension side's inside the join block — and the UDF MUST NOT read either side's files through the other's backend
* *AND* a parse failure on either argument SHALL surface an error identifying scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token from EITHER side's backend
* *AND* the reconstituted spec MUST NOT carry any catalog identifier, because the scan UDF never contacts the catalog
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Scan registers both tables and executes the inner equi-join

* *GIVEN* a reconstituted join scan spec
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL register the fact side's assigned files and the dimension side's full file list as two separate tables in ONE DataFusion session, each with its declared logical Iceberg schema and each exposing its columns under the Exasol-facing (uppercased) names the pushed condition and projection reference
* *AND* the UDF SHALL register each side's table against that side's OWN storage backend, so the redaction set guarding that side's read errors holds that side's credential values rather than the other side's
* *AND* the UDF SHALL execute an inner equi-join of the two registered tables on the rendered join condition
* *AND* the UDF MUST NOT resolve or discover any file beyond the two file lists carried in the spec
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
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

### Scenario: A requested path owned by no join side is a clear error

* *GIVEN* a join scan spec whose two sides' file lists and table roots are known
* *WHEN* the routing object store receives an operation for a path that matches no side's data-file or delete-file path and lies under no side's table root
* *THEN* the routing store SHALL return an error rather than choose a side, because choosing one would issue the request with a credential of unknown scope for that path
* *AND* the error SHALL name the unroutable path and the table roots that were tried, so the defect is attributable to the plan that produced the spec
* *AND* the error MUST NOT contain any storage access key, secret key, session token, or SAS token from any side's backend
* *AND* a bucket-wide listing operation carrying NO path prefix SHALL be treated as unroutable by the same rule, because it cannot be attributed to one side
<!-- /DELTA:NEW -->

### Scenario: Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC

* *GIVEN* a join scan spec carrying a projection spanning both sides, an optional filter, and an optional row limit
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit only the projected join-output columns, in spec order, for rows satisfying both the join condition and the filter
* *AND* the UDF SHALL emit no more rows than the limit when one is carried
* *AND* the UDF SHALL emit each result batch via the SDK Arrow-batch emit path (`emit_batch`), fetching one batch, emitting it, and dropping it before the next, never materializing the entire joined result set
* *AND* no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer

### Scenario: The bounded dimension side is the hash-join build side

* *GIVEN* a join scan spec whose dimension side is below the broadcast threshold and whose fact side is a large sharded subset
* *WHEN* the scan UDF plans the join
* *THEN* the join SHALL build its hash table on the bounded dimension side and probe with the fact side, so per-instance memory is bounded by the dimension side rather than the fact shard
* *AND* the DataFusion memory pool SHALL be sized from the per-instance memory limit exactly as on the raw-scan path

<!-- DELTA:CHANGED -->
### Scenario: Scan reports a clear error when an assigned join file is unreadable

* *GIVEN* a join scan spec referencing a fact-side or dimension-side file that cannot be read from object storage
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL return an error identifying that the assigned data could not be read
* *AND* the error message MUST NOT contain a storage access key, secret key, session token, or SAS token from EITHER side's storage backend, because one message can be produced while either side's credential is in scope
* *AND* a dimension-side read that the dimension side's own credential is not authorised to perform SHALL surface as that same read error rather than as a wrong-rows result
<!-- /DELTA:CHANGED -->
