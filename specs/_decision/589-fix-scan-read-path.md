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

## ADR: Row-group reads wait on a byte budget

**ID:** row-group-read-byte-budget
**Plan:** fix-scan-read-path
**Status:** Accepted

### Context

DataFusion's Parquet reader holds the fetched bytes of each stream's current row group and reserves nothing for them. Scan memory was the stream count times the projected row group, outside the memory pool: 2.7 GB at 192 streams with 16 MB row groups.

### Decision

The shared reader admits every fetch of a row group against one byte budget per invocation, a quarter of the memory pool. The bytes stay reserved in the pool until the reader fetches its next row group or drops. A fetch that does not fit waits in arrival order. A first fetch is admitted when nothing else is held. A later fetch of the same row group keeps its bytes while it waits and goes before any first fetch. When every reader that holds bytes waits, the first of them goes beyond the budget. That reader fetches the rest of its row group without waiting, and no other reader goes beyond the budget until it releases the row group.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cap the stream count | Rejected: it bounds memory for one row-group size only, and it lowers small-file concurrency |
| Fail a read that does not fit | Rejected: a query that ran before would fail, where waiting only slows it |
| Let a later fetch of an admitted row group skip the budget | Rejected: a filter fetches its columns first, so every stream was admitted for a small fetch and then held a whole row group. 16 streams reserved 95 MB against a 16 MB budget, and the next operator's reservation failed |
| Let a reader beyond the budget wait again for its next fetch | Rejected: it then counted as waiting, so the next reader went beyond the budget too. A filter of two conjuncts fetches a row group in three requests, and 16 streams reserved 109 MB against a 64 MB pool |
| Admit the whole row group at its first fetch | Rejected: the reader does not know the scan's Parquet columns, so it would count unread columns and stall wide tables |
| Count held file bytes against the same budget | Rejected: a file's footer bytes stay held until all its ranges finish, so they could block the reads that release them |

### Consequences

- Scan read memory follows the budget, not the stream count: 263 MB from 32 to 192 streams with a 1 GiB pool, at the same run time.
- Row-group bytes now count in the pool, so operators get that much less of it. The reservation never fails, so a row group larger than the free pool can make an operator's reservation fail.
- The budget counts fetched bytes. Decoded batches are bounded per stream by the batch size.
- Held bytes exceed the budget by at most one row group.
- A reader that waits for a first fetch holds no row-group bytes, and a reader that waits for a later fetch is passed when no other reader runs. This is deadlock-free only while no plan makes one scan stream wait for another, which a host test checks for every pushed plan shape.
- Streams that hold filter columns while they wait take budget from the streams that decode. With a small pool and many streams, few row groups decode at once.
