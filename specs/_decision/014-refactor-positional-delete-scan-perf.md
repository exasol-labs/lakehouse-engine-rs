# Decisions: refactor-positional-delete-scan-perf

## ADR: Two-phase read-once restructure for delete application

**ID:** positional-delete-two-phase-read-once-restructure
**Plan:** `refactor-positional-delete-scan-perf`
**Status:** Accepted

### Context

`PositionalDeleteScanTable::partitioned_files` iterated data files one at a time and, for
each, iterated that entry's delete files serially, calling `union_delete_positions` per
pair. A partition-granularity delete file referenced by K data files in a shard was
downloaded and fully re-scanned K times, serializing all delete-file I/O.

### Decision

Replace the serial `partitioned_files` → `access_plan_for_data_file` →
`union_delete_positions` loops with two phases: Phase A reads each unique delete file
exactly once, concurrently, into a merged `HashMap<data_file_path, RoaringTreemap>`; Phase
B performs a per-data-file in-memory lookup against that map, with no delete-file I/O. The
union across delete files is commutative, so concurrent reads cannot change the result.

### Options Considered

| Option | Verdict |
|--------|---------|
| Two-phase read-once restructure (Phase A I/O, Phase B lookup) | ✓ Chosen — the intended target architecture; Phase A is the only I/O, Phase B is CPU-only |
| Lighter cache-check patch keeping the per-(data_file, delete_file) call shape | ✗ Rejected — does not eliminate the redundant re-scan pattern; only masks it with a cache check |

### Consequences

A shared delete file is read at most once per shard regardless of how many data files
reference it. Concurrency, `file_path` row-group pruning, and bulk `RoaringTreemap`
construction all live inside Phase A's single per-delete-file read path.

## ADR: One shared instance-level semaphore bounds delete-file read concurrency

**ID:** positional-delete-shared-instance-semaphore-bound
**Plan:** `refactor-positional-delete-scan-perf`
**Status:** Accepted

### Context

Delete-file reads needed a concurrency bound bounded by the existing `s3_max_connections`
budget. An initial draft gave each `PositionalDeleteScanTable` its own size-N semaphore. In
a broadcast join, `register_join_tables` registers two delete-carrying providers, and
DataFusion 54 plans the two scan leaves concurrently (`physical_planner.rs`), so two
independent size-N semaphores would allow up to 2N concurrent delete-file reads on one
instance — breaking the mission's bounded-execution contract. A temporal-disjointness
argument (Phase A finishing before the data-file scan) addressed the wrong contention axis
(delete-reads-vs-data-reads, not delete-reads-vs-delete-reads) and did not establish the
"at most N in flight" bound.

### Decision

Construct exactly one `Arc<tokio::sync::Semaphore>` sized `spec.s3_max_connections` per
scan invocation, and thread the SAME handle into every registered provider: one in
`register_files` for the single-table path, and one shared clone passed to both the
fact-side and dimension-side `register_file_list` calls in `register_join_tables`. Every
in-flight delete-file read holds a permit from that one semaphore, so "at most N
delete-file reads in flight" holds across the whole instance by construction.

### Options Considered

| Option | Verdict |
|--------|---------|
| One shared instance-level semaphore threaded into every provider | ✓ Chosen — the only option that bounds delete-reads-vs-delete-reads across all providers on the instance, including both sides of a concurrently-planned join |
| One size-N semaphore per provider | ✗ Rejected — allows up to 2N concurrent delete reads when a broadcast join plans two delete-carrying scan leaves concurrently |
| A fraction of `s3_max_connections` per semaphore, for headroom | ✗ Rejected — the interview decision reuses the full budget; a fraction under-utilizes it without fixing the per-provider bound problem |

### Consequences

The instance-wide delete-read concurrency bound holds regardless of table shape (single
scan or broadcast join). The semaphore is rebuilt every query invocation, never cached at
process scope, consistent with the stateless/disposable-UDF invariant. This shared handle
is the pattern future planners should follow when adding any new object-store fan-out that
must stay within a per-instance budget.
