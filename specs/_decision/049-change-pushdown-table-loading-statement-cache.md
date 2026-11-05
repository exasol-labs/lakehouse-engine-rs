# Decisions: change-pushdown-table-loading-statement-cache

## ADR: Pushdown loads each table through the catalog client of the one catalog-kind match

**ID:** pushdown-loads-tables-through-the-catalog-client
**Plan:** change-pushdown-table-loading-statement-cache
**Status:** Accepted
**Supersedes:** one-per-request-scan-source-resolver

### Context

Pushdown loads tables through a second per-kind session stack beside the catalog clients. The earlier reason for avoiding the catalog client, that an abstract client hides the concrete session a load needs, no longer holds, because each client can run its load on its own concrete session.

### Decision

Every request type builds its catalog client at the one catalog-kind match. For pushdown, that client loads a table through one engine-side scan load, which calls the table format's loader directly, so only the loaded table's format tag is matched after the kind. `CatalogClient` has no single-table load.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the resolver's per-kind session enum and the scan-source match | Rejected: two more kind matches beside the catalog-client construction |
| Keep `CatalogClient::load_table` beside the scan load | Rejected: every client has two table loads, and the direct-storage one always fails |
| Widen `CatalogClient::load_table` to return scan state | Rejected: file-planning state moves into the catalog crate |

### Consequences

- A new catalog kind adds one client and one arm in the catalog-kind match, plus its CONNECTION rules, and no load path names the kind.
- Each catalog kind and each table format hides its load behind one engine-side interface.

## ADR: A pushdown table loads once and prunes once per reference

**ID:** pushdown-table-loads-once-and-prunes-per-reference
**Plan:** change-pushdown-table-loading-statement-cache
**Status:** Accepted
**Supersedes:** format-reader-engine-owns-whole-resolution

### Context

A statement can reference one table several times with different filters. A format that resolves its whole scan in one call loads the table once per reference.

### Decision

Each table format resolves a pushdown table in two steps. The load depends on neither the filter nor the declaration and runs once per table. The prune runs once per table reference, with that reference's filter and declared schema.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse the whole resolved scan per identifier and filter | Rejected: a self-join with a different filter per leg loads the table twice |
| Keep each table's unpruned file list and prune it in memory | Rejected: the Delta kernel skips files on their statistics, and the scan's file entries keep none |

### Consequences

- A Delta reference still reads the commit and checkpoint files in its own prune.

## ADR: The adapter keeps a virtual schema's pushdown state for one statement, keyed by the virtual schema name

**ID:** adapter-keeps-pushdown-state-for-one-statement
**Plan:** change-pushdown-table-loading-statement-cache
**Status:** Accepted

### Context

Exasol runs one adapter VM per statement and virtual schema, so state kept in the VM lives for one statement. Several references to one table in a statement can then share their loads.

### Decision

The adapter keeps a virtual schema's catalog client, its loaded tables, and its permission-checked tables in process-static state. The state is keyed by the virtual schema name alone and lives as long as the adapter VM.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep no state, so each pushdown request loads its own tables | Rejected: a UNION ALL branch, a CTE reference, and a subquery each arrive as a request of their own, so one table loads once per request |
| Keep one state per VM, with no key | Rejected: nothing guarantees that a VM never serves two virtual schemas of one statement, and each must use its own CONNECTION and properties |
| Key the state by session id, statement id, or a TTL | Rejected: the VM never outlives its statement, so the name alone suffices |

### Consequences

- An Exasol version that reuses one adapter VM across statements would serve stale metadata, and the name key does not protect against that.
