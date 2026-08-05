# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from `ctx.memory_limit()` and size the DataFusion memory pool from a *net* budget — the per-instance limit minus a configurable container/binary overhead — scaled by a configurable fraction, to bound the per-batch Parquet decode working set via a configured `batch_size`, to enable Parquet row-group and page pruning so the scan reads only the byte ranges its predicate needs, and to consume storage credentials carried in the scan spec (including vended STS tokens) without re-authenticating to the catalog. The credentials and tuning knobs travel in the shard-invariant common spec argument, serialized once for the whole fan-out.

## Background

<!-- DELTA:NEW -->
* **A join spec now carries TWO storage backends, one per side, and the "single object store built from those credentials" rule is restated per side.** This feature's recorded Background says "This single S3 object store built from those credentials is reused for both data files and their associated positional-delete files". That sentence was written for a spec with one storage value. Under issue #294 it stays true PER SIDE: one store per side serves that side's data files and that side's delete files, built from that side's own backend. Nothing about the memory pool, the batch size, the Parquet pruning flags, or the shared metadata reader changes.
* **The whole-spec size index becomes a per-side size index, and that is the point rather than a tidy-up.** The index was deliberately whole-spec because one registered store had to answer BOTH sides' `head` calls — the collapse this delta removes. With one inner store per side, each store's index holds only its own side's files, so a `head` for one side's path can no longer be satisfied by the other side's store. `datafusion-scan/scan-execution-join` owns the routing decorator that makes this possible.
* **Layering is routing OUTSIDE, spec-sized `head` INSIDE.** The routing decorator wraps one spec-sized store per side, not the other way round, so every operation is routed BEFORE the sized-`head` shortcut can answer it. Sizing outside the router would answer an unroutable `head` from the index and defer the routing failure to the later range read, where it would surface as a credential-shaped access denial instead of the plan defect it is.
* **The single-table (non-join) path is untouched.** A spec with no join block registers exactly one spec-sized store over exactly one backend, with an index over exactly its own files — the same shape as before, now narrowed from "the whole spec" to "the only side there is".
* **Redaction is the union of the sides in scope.** An error raised while building or using a routed store can be produced while either side's credential is in scope, so the redaction set for such a message is every side's `secret_values()`, not the fact side's alone.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries a storage backend holding vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL register the object store the carried storage backend names, configured from the credentials that backend holds
* *AND* the UDF MUST NOT decide the storage backend itself, derive it from a file URI scheme, or read the backend's payload outside the single backend-dispatching registration function specified by `vs-adapter/storage-backend-enum`
* *AND* when the spec also carries a join block, the UDF SHALL build a SECOND store from the join block's OWN storage backend and read the dimension side's files through it, so the whole-spec backend serves only the side whose files the whole-spec `table_root` and per-shard `files` describe
* *AND* the store the UDF builds for a side SHALL answer that side's per-file metadata lookups from a size index over THAT side's files only, so one side's `head` can never be satisfied by the other side's store
* *AND* the storage credentials SHALL travel in the shard-invariant common spec argument (serialized once for the whole fan-out), NOT be repeated per shard — the dimension side's backend included, since the join block is itself shard-invariant
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value from ANY carried backend MUST NOT appear in any error message the UDF returns
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Positional-delete files are read with the same vended credentials

* *GIVEN* a scan invocation whose shard-invariant common spec carries a storage backend holding vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF reads a data file's associated positional-delete files from object storage
* *THEN* the UDF SHALL read the delete files through the SAME registered object store used for the data files OF THAT SIDE, configured from that side's backend credentials
* *AND* on a join spec, the dimension side's delete files SHALL be read with the DIMENSION side's credentials and the fact side's with the FACT side's, never one side's delete files with the other side's credentials
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials to read a delete file
* *AND* a credential value MUST NOT appear in any error message the UDF returns while reading a delete file, for whichever side's delete file failed
<!-- /DELTA:CHANGED -->
