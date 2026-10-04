# Decisions: refactor-positional-delete-footer-fetch

## ADR: Deadlock freedom rests on no-hold-and-wait, not on phase ordering

**ID:** footer-fetch-no-hold-and-wait
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

Within one provider, Phase A (delete-file reads) drops its permits before Phase B (data-file footer fetches) starts. A broadcast join runs two providers concurrently, so one provider's Phase A permits and the other's Phase B permits coexist on the one shared semaphore.

### Decision

Every fan-out task in both phases acquires one permit, holds it across one object-store read, and releases it. No task holds a permit while awaiting another permit, and no task awaits another task.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on phase ordering | Rejected: true only within one provider, and a broadcast join makes the phases coexist |

### Consequences

Implementation and review check the no-hold-and-wait property directly. Any future fan-out sharing the semaphore keeps the one-permit, one-read, no-nesting shape.

## ADR: A performance invariant that fails silently needs a runtime observable, not only a test

**ID:** footer-cache-eviction-needs-runtime-observable
**Plan:** refactor-positional-delete-footer-fetch
**Status:** Accepted

### Context

A cached footer is a parsed `ParquetMetaData` with one `ColumnChunkMetaData` per column and row group, so a wide Iceberg file's entry is megabytes and no fixed file count fits safely in the default metadata cache limit. A reuse-test fixture far below that limit cannot fail for eviction, the cause it guards against.

### Decision

The reuse test uses a fixture calibrated so K cached footers fill 70-90% of the default cache limit. The scan also counts footers re-fetched after eviction and logs the count in one level-gated debug line at scan completion. Detection stays off the object store, and the single log line is the whole output surface.

### Options Considered

| Option | Verdict |
|--------|---------|
| Size the cache limit from the per-instance memory budget | Rejected: a blind fix that adds RSS the memory pool does not account for |
| Raise the limit to a larger constant | Rejected: no measured basis |
| Assume the default suffices and add no check | Rejected: unquantified and false in general |
| Reuse measurement only, with the guard added if the measurement fails | Rejected: the measurement structurally could not fail, so the guard would never ship |

### Consequences

A shard whose footers evict produces an observable signal instead of a silent double-fetch. A later cache-limit increase can be driven by the counter's telemetry.
