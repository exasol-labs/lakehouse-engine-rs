# Decisions: add-export-iceberg-rest-write

## ADR: The export commit loop re-checks the writer's tag before every re-commit

**ID:** export-commit-loop-rechecks-writer-tag
**Plan:** add-export-iceberg-rest-write
**Status:** Accepted

### Context

A commit whose answer was lost returns HTTP 409 on a repeat, and a blind re-apply adds the writer's rows twice. The Iceberg REST spec answers 500, 502, or 504 when "the commit state is unknown".

### Decision

The catalog write crate sends each writer's commit and, after a conflict or an uncertain outcome, reloads the table and commits again only when no snapshot carries the writer's tag. A writer whose last outcome is unknown fails and keeps its data files.

### Options Considered

| Option | Verdict |
|--------|---------|
| `RestCatalog` with `Transaction::commit` and a raised retry count | Rejected: duplicate rows, and it bypasses the one catalog HTTP client |

### Consequences

- The commit loop is the only retry layer of a commit, and later write features reuse it.

## ADR: The export has no final commit, so each writer commits on its own

**ID:** export-has-no-final-commit-per-writer-commits
**Plan:** add-export-iceberg-rest-write
**Status:** Accepted

### Context

Exasol's UDF model has no step after the last group of a SET script, and the engine has no coordinator or state across statements.

### Decision

Each writer commits its own data files. `SINGLE_COMMIT` runs one writer, whose one commit makes the table appear only with its data.

### Options Considered

| Option | Verdict |
|--------|---------|
| A coordinator writer that waits for all others | Rejected: writers have no channel to each other and run in any order |

### Consequences

- A failed parallel export can leave the commits of some writers in the table.
- Exactly-once per writer is the only duplicate protection, and atomic exports are single-writer.
- Every later catalog implements per-writer commits plus its own way of making a single-writer table appear only with its data.

## ADR: Catalog requests that change state are retried only by the operation that checks its outcome

**ID:** state-changing-catalog-requests-retried-by-their-operation
**Plan:** add-export-iceberg-rest-write
**Status:** Accepted

### Context

The catalog HTTP client retried every request after a timeout or HTTP 502 or 504, which is wrong for a request that changes state and whose answer was lost. This conforms to `catalog-http-times-out-on-silence`, which sets the attempt timeout and retry window but not which requests repeat.

### Decision

Every request through the catalog HTTP client states whether it is safe to repeat, and the client retries a transient failure only for a safe request. Create namespace, create table, and commit are not safe, and each operation owns its retries because it can check its own outcome.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the client's retries for every request | Rejected: two retry layers with different rules and a hidden retry |
| Retry unsafe requests with an idempotency key | Rejected: Iceberg REST commits carry none |

### Consequences

- Every later write feature marks its requests the same way and owns its retries.
