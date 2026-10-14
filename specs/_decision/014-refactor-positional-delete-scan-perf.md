# Decisions: refactor-positional-delete-scan-perf

## ADR: Two-phase read-once restructure for delete application

**ID:** positional-delete-two-phase-read-once-restructure
**Plan:** `refactor-positional-delete-scan-perf`
**Status:** Accepted

### Context

The scan read delete files serially per data file, so a delete file referenced by K data files was downloaded and scanned K times.

### Decision

The scan reads each unique delete file once, concurrently, into a merged map of deleted positions per data file. It then looks up each data file in memory with no further delete-file I/O. The union across delete files is commutative, so concurrent reads do not change the result.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cache check on the per-(data file, delete file) call shape | Rejected: masks the redundant re-scan instead of removing it |

### Consequences

A shared delete file is read at most once per shard.

## ADR: One shared instance-level semaphore bounds delete-file read concurrency

**ID:** positional-delete-shared-instance-semaphore-bound
**Plan:** `refactor-positional-delete-scan-perf`
**Status:** Accepted

### Context

Delete-file reads must stay within the `s3_max_connections` budget. A broadcast join plans two delete-carrying scans concurrently, so a semaphore per scan would allow twice the budget.

### Decision

The instance creates one semaphore sized to `s3_max_connections` per scan invocation and shares it with every registered provider, including both sides of a join. Every delete-file read holds a permit from it.

### Options Considered

| Option | Verdict |
|--------|---------|
| One semaphore per provider | Rejected: allows up to 2N concurrent delete reads in a broadcast join |
| A fraction of the budget per semaphore | Rejected: under-uses the budget and leaves the per-provider bound problem |

### Consequences

The semaphore is rebuilt per query, never cached at process scope, which matches the stateless UDF invariant. New object-store fan-out that must respect a per-instance budget follows this pattern.
