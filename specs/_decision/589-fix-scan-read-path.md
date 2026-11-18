# Decisions: fix-scan-read-path

## ADR: Row-position deletes load when a data file is opened

**ID:** deletes-load-at-file-open
**Plan:** fix-scan-read-path
**Status:** Accepted
**Supersedes:** footer-fetch-no-hold-and-wait

### Context

Reading every delete payload and every delete-carrying file's footer at planning made the first row wait for the slowest delete read, and a LIMIT paid for files it never read (#516). DataFusion 55 applies a base `ParquetAccessPlan` only from the `PartitionedFile` it is handed, and Parquet has no `FileOpener` layer to attach one later.

### Decision

A wrapper around the Parquet file source plans each delete-carrying file through the morsel API: one pending planner loads the file's footer and its delete payloads together, attaches the access plan, and hands the file to DataFusion's own planner. The streams that await a load drive it, so no task outlives the plan. A delete read holds one permit across one request and waits for no other load while it holds it. The reader factory refuses a delete-carrying file that arrives without an access plan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the delete reads at planning and move only the footer read | Rejected: the first row still waits for every delete payload of the shard |
| Prune row groups ourselves, then load only the surviving deletes | Rejected: DataFusion rewrites the predicate per file, so a second pruning pass can drop live rows |
| Load deletes in spawned background tasks | Rejected: a task can outlive the invocation, and Exasol reuses the VM process |

### Consequences

- The morsel API is experimental. A DataFusion upgrade rechecks the wrapper, which re-wraps every source a pushdown rewrite returns.
- A lost wrapper fails the query instead of returning deleted rows.
- A delete read or a reservation can fail after other files emitted rows. Exasol aborts the statement, so the result stays correct.
- A selective filter still loads the deletes of row groups it later prunes.

## ADR: The scan builds its file groups and shares each file's reads

**ID:** scan-built-file-groups-and-shared-file-reads
**Plan:** fix-scan-read-path
**Status:** Accepted

### Context

DataFusion cut one file group into `target_partitions` byte ranges after `scan()`, at any offset. Every range fetched its file's footer and page index again, a small file was fetched once per range and again for its columns, and a shard under 1 MiB was read by one stream (#589).

### Decision

The scan table builds its own file groups and turns DataFusion's file-scan repartitioning off. A file of at most 8 MiB stays whole, and is fetched in one GET when the scan projects at least half of the table's columns. A larger file is cut into byte ranges whose readers share one footer load, the bytes fetched with it, and one page-index load. Held bytes are reserved in the memory pool, and bytes that do not fit are not held.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep DataFusion's split and only share the footer | Rejected: a cut small file still costs a request per range, and a tiny shard stays serial |
| Rely on the session `FileMetadataCache` | Rejected: concurrent ranges all miss it before one fills it, and it holds neither bytes nor the page index |

### Consequences

- Storage requests no longer grow with `DATAFUSION_TARGET_PARTITIONS`.
- Below half of the columns a small file is read by ranges, because a whole-file fetch was twice as slow on 8 MiB files of 40 columns read for two. At or above half it moves its unprojected columns too, in exchange for one request.
- Shared state is released once a file's finished ranges cover it. A file reopened after that is fetched again, which changes no row.
