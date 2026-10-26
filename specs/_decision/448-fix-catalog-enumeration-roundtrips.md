# Decisions: fix-catalog-enumeration-roundtrips

## ADR: Every Iceberg REST catalog request runs on the request's CatalogSession

**ID:** iceberg-rest-requests-run-on-the-catalog-session
**Plan:** fix-catalog-enumeration-roundtrips
**Status:** Accepted

### Context

An adapter request that lists a namespace and loads its tables needs one authentication handshake, one `/v1/config` lookup, and control over how many requests run at once.

### Decision

The adapter issues every Iceberg REST catalog request itself, on the one `CatalogSession` of the request: the namespace and table listings, every `loadTable`, and the permission check. It does not use the `iceberg-catalog-rest` client, only its response types.

### Options Considered

| Option | Verdict |
|--------|---------|
| `iceberg-catalog-rest`'s `RestCatalog` for the listings | Rejected: it runs its own OAuth2 grant, `/v1/config` lookup, and connection pool, and its `load_table` drops the vended credentials |

### Consequences

- Paging, the `parent` descent, and the request cap are the adapter's own code.
- A catalog-auth mode is implemented once, in the session.
