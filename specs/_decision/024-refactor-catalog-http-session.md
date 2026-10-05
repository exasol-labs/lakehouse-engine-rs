# Decisions: refactor-catalog-http-session

## ADR: CatalogSession Bundles Client, Auth, and Prefix, Resolved Once Per Query

**ID:** catalog-session-bundles-client-auth-prefix-once
**Plan:** `refactor-catalog-http-session`
**Status:** Accepted

### Context

Each table resolved the catalog auth, the `/v1/config` prefix, and a new HTTP client independently. These are catalog-scoped, not table-scoped, per the Apache Iceberg REST Catalog OpenAPI spec, where `GET /v1/config` configures "the catalog and its HTTP client" and is keyed by `warehouse`. An N-table join ran N OAuth grants and N config lookups.

### Decision

A `CatalogSession` bundles the client, catalog URI, auth, and prefix. The adapter builds it once per query and passes it by reference to the file-resolution code, which issues only the per-table `loadTable` request.

### Options Considered

| Option | Verdict |
|--------|---------|
| Memoize only the client | Rejected: the grant and config lookup still run per table |
| Pass raw client, auth, and prefix tuples | Rejected: loses the one-catalog-per-session binding |

### Consequences

The OAuth grant and config lookup run at most once per query, and one pooled client serves all catalog requests. The per-table `loadTable` stays because only its response carries per-table vended storage credentials.

The adapter builds the session inside the join arm and builds a single-use one in the single-table path, never before join detection, so an ineligible join declines with no session and fast-failing validation makes no network call.
