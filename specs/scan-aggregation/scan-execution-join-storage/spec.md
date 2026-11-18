# Feature: DataFusion Scan Execution, Broadcast Join Storage

Routes each object-store request of a broadcast join scan (`scan-aggregation/scan-execution-join`) to the side that owns the requested path, so each side reads its files through its own storage credential.

## Background

* Each side reads its files through its own storage credential, as `scan-aggregation/scan-execution-join` states, and storage keys and secrets never appear in an error message.
* DataFusion keys its object-store registry on `scheme://host[:port]` and cannot see a path, so one bucket holds one registered store. A routing `ObjectStore` decorator is registered once per bucket and holds one inner store per side. It selects the side that owns a requested `object_store::Path`, and it is used on every spec with a join block, including when both credentials are identical.
* Routing matches a side's own data-file and delete-file paths first and its table root by longest prefix second, because the Iceberg spec allows a table's files outside its `location` (Appendix E, Version 4: "Absolute paths must be used for files that do not share a common prefix with the table location"). The Iceberg REST `StorageCredential.prefix` follows the same longest-prefix rule.
* Every path the scan requests for a side is one that side's spec names, or a prefix under its table root. That covers range reads of Parquet footers, positional-delete files, and Iceberg deletion-vector blobs inside their Puffin files, and one `head` plus a prefixed `list` on the schema-inference branch. A Delta deletion-vector sidecar's path is not in the spec, so only the side's table root routes it, and a sidecar stored by absolute path outside that root matches neither rule.
* A side whose files span more than one bucket is refused with a clear error. The Iceberg spec permits such a layout, so this is a scoped exception (#304).
* An ADLS store is container-scoped and its `Path` is container-relative, so path routing cannot separate two containers of one storage account. The scan-time check that both sides share one store is the only guard for that case (`storage-access/pushdown-planning-cloud-credentials-vended-storage`).

## Scenarios

### Scenario: Each join side reads its files through its own storage credential

* *GIVEN* a join scan spec whose fact-side and dimension-side storage backends hold DIFFERENT credentials, and whose two sides' files resolve to the SAME object-store bucket
* *WHEN* the scan UDF builds its DataFusion session context and reads both sides' files
* *THEN* the UDF SHALL register, under that bucket's single store key, ONE routing object store holding one inner store per side, each inner store configured from that side's OWN storage backend
* *AND* the UDF SHALL route every object-store operation carrying a path, including each `get`, ranged read, `head`, and listing call, to the inner store of the side that owns the requested path, so no request for one side's file is ever issued with the other side's credential
* *AND* the UDF SHALL select the owning side by matching the requested path against that side's OWN data-file, positional-delete-file, and Puffin deletion-vector-file paths FIRST, and only then against the sides' table roots by longest-prefix match, so a file that the Iceberg spec permits to sit outside its table root is still routed to its own side
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
* *AND* the same SHALL hold when the two sides resolve to different BACKENDS, an `s3://` fact side and an `abfss://` dimension side, because store construction dispatches on each side's OWN backend and the two schemes yield different registry keys; the UDF MUST NOT address one side's files through the other side's backend arm, and no plan-time check refuses such a join (`storage-access/pushdown-planning-cloud-credentials-vended-storage`)

### Scenario: A requested path owned by no join side is a clear error

* *GIVEN* a join scan spec whose two sides' file lists and table roots are known
* *WHEN* the routing object store receives an operation for a path that matches no side's data-file or delete-file path and lies under no side's table root
* *THEN* the routing store SHALL return an error rather than choose a side, because choosing one would issue the request with a credential of unknown scope for that path
* *AND* the error SHALL name the unroutable path and the table roots that were tried, so the defect is attributable to the plan that produced the spec
* *AND* the error MUST NOT contain any storage access key, secret key, session token, or SAS token from any side's backend
* *AND* a bucket-wide listing operation carrying NO path prefix SHALL be treated as unroutable by the same rule, because it cannot be attributed to one side
