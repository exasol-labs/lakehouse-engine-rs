# Decisions: fix-create-vs-http-perf

## ADR: Namespace enumeration runs on the CatalogSession, not on iceberg-rust's RestCatalog

**ID:** enumeration-on-catalog-session
**Plan:** `fix-create-vs-http-perf`
**Status:** Accepted

### Context

`createVirtualSchema` listed the namespace through `iceberg-rust`'s `RestCatalog` (its own OAuth
grant, `/v1/config` lookup, and connection) and then built a second `CatalogSession` for the
per-table loads. The SigV4 branch had its own request code: a fresh `reqwest::Client` per request,
no pagination (tables past the first page were silently lost), and a best-effort child-namespace
listing that turned any error into "no children".

### Decision

The session issues the paginated `listTables`/`listNamespaces` GETs itself, through the same
authenticated GET as `loadTable`, and a SigV4 (Glue) catalog is never asked for child namespaces
because Glue documents single-level namespaces only. A failed `/v1/config` lookup is an error: with
a guessed empty prefix every `loadTable` 404s and the schema silently comes back empty.

### Options Considered

| Option | Verdict |
|--------|---------|
| Self-issued list GETs on the shared session | ✓ Chosen — one grant, one config lookup, one auth path, one pagination loop |
| Keep `RestCatalog` for listing, reuse its token for loads | ✗ Rejected — `RestCatalog` exposes neither its token nor its prefix |

### Consequences

`build_rest_catalog` and the `RestCatalog` auth-prop injection are gone; `iceberg-catalog-rest` is
retained only for its response types. `IcebergRestCatalogClient` no longer takes a
`StorageBackend`.

## ADR: Per-table loads and sibling namespaces run concurrently under a fixed bound

**ID:** bounded-enumeration-concurrency
**Plan:** `fix-create-vs-http-perf`
**Status:** Accepted

### Context

Every `loadTable` and every namespace listing awaited the previous one, so wall time was catalog
latency times table count.

### Decision

Up to 8 loads (and 8 sibling namespaces) in flight at once, results kept in listing order. A
constant, not a property: the adapter runs on a single-threaded runtime and the bound protects the
catalog, not the node.

### Consequences

A 500-table namespace at 100 ms catalog latency enumerates in seconds rather than a minute; the
same bound serves the scoped refresh and the Unity per-table loads.

## ADR: Enumeration loads request no storage credentials and only referenced snapshots

**ID:** enumeration-load-shape
**Plan:** `fix-create-vs-http-perf`
**Status:** Accepted

### Context

The enumeration `loadTable` reused the pushdown load verbatim: with `use_vended_credentials` the
catalog minted storage credentials per table that create never used, and the response carried the
full snapshot history although only the current schema is read.

### Decision

The enumeration load omits `X-Iceberg-Access-Delegation` and sends `snapshots=refs` (Iceberg REST
spec `loadTable`: "`refs` would load all snapshots referenced by branches or tags"). The pushdown
load is unchanged. A catalog that ignores the parameter (Unity Catalog does, tracked upstream)
still answers correctly; none of the supported catalogs rejects it.

## ADR: A REFRESH TABLES naming known tables loads only those tables

**ID:** scoped-refresh-via-table-map
**Plan:** `fix-create-vs-http-perf`
**Status:** Accepted

### Context

`refresh` ignored `requestedTables` because the engine's handling of a scoped response was
unknown. Probed live on Exasol 2025.1 with a throwaway adapter: a response holding only the
requested tables leaves the other tables untouched, a requested table the response omits is
dropped, and a requested name unknown to the schema still reaches the adapter.

### Decision

When every requested name is in the persisted `TABLE_MAP`, the adapter loads only those
identifiers through the new `CatalogClient::load_tables`, responds with only them, and merges the
result into the persisted map (requested names replaced or removed, the rest kept). Any unknown
name falls back to the full enumeration. The map lives in Exasol-persisted `adapterNotes`, so the
adapter still holds no state of its own.

### Options Considered

| Option | Verdict |
|--------|---------|
| Scoped load, `TABLE_MAP` merge | ✓ Chosen — one table's refresh costs one load, and a dropped table still leaves the schema |
| Keep full re-enumeration | ✗ Rejected — the only reason was the unverified engine behavior |

## ADR: Timeouts and retries are fixed constants on one shared client shape

**ID:** catalog-http-timeouts-and-retry
**Plan:** `fix-create-vs-http-perf`
**Status:** Accepted

### Context

Every catalog client was a bare `reqwest::Client` with no timeout, and one 429 or 503 failed the
whole request.

### Decision

One builder for all catalog kinds: 10 s connect, 120 s request, and up to 3 attempts on a connect
failure or a 429/502/503/504, with doubling backoff or a capped `Retry-After`. Not exposed as
properties: no measured need, and each property would be a new CONNECTION/VS surface to document.

## ADR: A duplicate folded column name is an error; a non-404 load failure stays fatal but names the table

**ID:** column-collision-and-named-load-failure
**Plan:** `fix-create-vs-http-perf`
**Status:** Accepted

### Context

Exasol accepts a `createVirtualSchema` response declaring the same column twice without complaint
(verified live), leaving an unqueryable table. A non-404 `loadTable` failure aborted the request
without saying which table failed.

### Decision

Two columns folding to one name fail the request naming the table and both columns, matching the
existing table-name collision rule and the direct-storage column rule. A non-404 load failure still
aborts (the recorded `namespace-skip-non-iceberg-on-404-only` reasoning holds) but names the table.
Skipping a 403 table was rejected: it would hide a misconfiguration behind a partial schema.
