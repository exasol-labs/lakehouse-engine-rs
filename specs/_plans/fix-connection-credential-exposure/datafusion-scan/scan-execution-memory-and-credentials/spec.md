# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from
`ctx.memory_limit()` and size the DataFusion memory pool from a *net* budget — the
per-instance limit minus a configurable container/binary overhead — scaled by a
configurable fraction, to bound the per-batch Parquet decode working set via a
configured `batch_size`, to enable Parquet row-group and page pruning so the scan
reads only the byte ranges its predicate needs, and to obtain storage credentials
from the scan spec — resolved from the referenced Exasol CONNECTION, or carried
inline when the planning layer vended them — without re-authenticating to the
catalog. The credentials or their reference and the tuning knobs travel in the
shard-invariant common spec argument, serialized once for the whole fan-out.

## Background

* **This delta is issue #135. It amends the two credential-passthrough scenarios, adds one, and changes nothing else.** The memory pool, the batch size, the Parquet pruning flags, the shared metadata reader, and the metadata-cache observable are all UNCHANGED. What changes is only WHERE a credential comes from before the object store is built.
* **The prohibition this feature owns is UNCHANGED.** "The UDF MUST NOT re-authenticate to the catalog or re-request vended credentials" still binds. `ctx.connection()` contacts neither the catalog nor object storage: it is one engine-local metadata request over the script-language-container protocol, answered by the database from its own catalog. No file is discovered, no snapshot is read, and no token is minted, so `specs/mission.md`'s "resolve metadata once per query, in the VS layer" is untouched — the file list, the snapshot, and any vended credential are still resolved exactly once, by the adapter.
* **The resolution is ONE step at the top of the invocation, not a lookup at each store-construction site.** A join spec carries a storage block per side, so the resolved value is a PAIR. Resolving lazily per store would read the same CONNECTION twice in one invocation and would leave the redaction secret set undefined for the window between the two reads.
* **The redaction secret set moves off the spec and onto the resolved pair, and this is the delta's one correctness trap.** SEVEN sites under `crates/lakehouse-engine/src/scan/` build such a set. Two read the union off the spec: `object_store.rs:66` and `join_scan.rs:48`, both `spec.common.all_secret_values()`. Three read the fact side off the spec directly: `partial_agg.rs:70`, `partial_agg.rs:125`, and `raw_scan.rs:54`, each `spec.common.storage.secret_values()`. Two already take a `&StorageBackend` parameter and are fed by their callers: `raw_scan.rs:224` in `register_file_list`, and `positional_deletes.rs:629` in `PositionalDeleteScanTable::new`. A spec carrying a connection NAME has no secret to yield, so leaving the set on the spec would silently disarm value-based redaction at the five spec-reading sites — a fix that reduced protection on the error path while fixing the SQL path. The set is therefore computed from the resolved backends, and the wire wrapper exposes no secret accessor so a missed site fails to compile.
* **The raw-scan and partial-aggregate paths are where a disarmed set would go unnoticed**, because they read the fact-side set directly and no recorded scenario asserts redaction on either. This delta adds that assertion.
* **`vs-adapter/scan-spec-credential-reference` owns the wire contract, the storage-only projection the UDF deserializes, the required grant, and the mid-query rotation consequence.** This feature CITES it and restates none of it, so the two do not drift.
* **Nothing about the store the UDF builds changes.** The resolved value is a `StorageBackend`, the same type the spec carried inline before, so the backend-dispatching registration function of `vs-adapter/storage-backend-enum`, the per-side size index, the routing decorator, and the one-store-per-side rule are all reached with a field-for-field identical input.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reads data files with credentials referenced or carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries, per side, EITHER a reference to the Exasol CONNECTION that supplies that side's storage credentials OR a storage backend holding vended credentials resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL resolve every reference to a storage backend EXACTLY ONCE per invocation, before it builds any object store, under `vs-adapter/scan-spec-credential-reference`
* *AND* the UDF SHALL register the object store the RESOLVED storage backend names, configured from the credentials that backend holds
* *AND* the UDF MUST NOT decide the storage backend itself, derive it from a file URI scheme, or read the backend's payload outside the single backend-dispatching registration function specified by `vs-adapter/storage-backend-enum`
* *AND* when the spec also carries a join block, the UDF SHALL build a SECOND store from the join block's OWN resolved storage backend and read the dimension side's files through it, so the whole-spec backend serves only the side whose files the whole-spec `table_root` and per-shard `files` describe
* *AND* the store the UDF builds for a side SHALL answer that side's per-file metadata lookups from a size index over THAT side's files only, so one side's `head` can never be satisfied by the other side's store
* *AND* the credentials or their reference SHALL travel in the shard-invariant common spec argument, serialized once for the whole fan-out, NOT be repeated per shard — the dimension side's included, since the join block is itself shard-invariant
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials; resolving a CONNECTION by name through `ctx.connection()` is NOT such a request, because it reaches the database's own catalog rather than the table catalog and discovers no file, mints no token, and reads no snapshot
* *AND* a credential value from ANY resolved backend MUST NOT appear in any error message the UDF returns
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Every redaction secret set in the scan path is built from the resolved backends

* *GIVEN* the sites under the scan path that build a value-based redaction secret set — the two that read the whole-spec union, the three that read the fact side off the spec, and the two that already receive a storage backend as a parameter
* *WHEN* the scan spec's storage block carries a connection reference rather than a credential
* *THEN* EVERY one of those sites SHALL take its secret set from the RESOLVED storage backend or backends, and NONE SHALL read it from the scan spec's own storage block
* *AND* the wire wrapper MUST NOT expose a secret-value accessor, so a site left reading the unresolved value fails to COMPILE rather than returning an empty set and silently disarming redaction
* *AND* an error raised on the RAW-SCAN path and an error raised on the PARTIAL-AGGREGATE path SHALL each be asserted to carry no resolved credential value, because those two paths read the fact-side set directly and no recorded scenario covered either
* *AND* a spec whose storage block carries a reference MUST NOT yield an empty secret set once resolution has run, and a test SHALL assert the set is NON-empty for a resolved reference
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Positional-delete files are read with the same resolved credentials

* *GIVEN* a scan invocation whose shard-invariant common spec references or carries, per side, the storage credentials for that side
* *WHEN* the scan UDF reads a data file's associated positional-delete files from object storage
* *THEN* the UDF SHALL read the delete files through the SAME registered object store used for the data files OF THAT SIDE, configured from that side's RESOLVED backend credentials
* *AND* on a join spec, the dimension side's delete files SHALL be read with the DIMENSION side's credentials and the fact side's with the FACT side's, never one side's delete files with the other side's credentials
* *AND* the UDF MUST NOT re-authenticate to the catalog, re-request vended credentials, or resolve the referenced CONNECTION a second time to read a delete file, because the one per-invocation resolution already supplied it
* *AND* a credential value MUST NOT appear in any error message the UDF returns while reading a delete file, for whichever side's delete file failed
<!-- /DELTA:CHANGED -->
