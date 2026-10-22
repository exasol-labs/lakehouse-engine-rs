# Feature: Pushdown Catalog HTTP Session

The adapter builds the catalog HTTP state once per pushdown request and reuses it for every table's `loadTable` GET. The state is one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool.

<!-- DELTA:CHANGED -->
## Background

* The catalog-auth token and the `/v1/config` prefix are catalog-scoped, so the session computes them once per query. The `loadTable` GET stays per-table because each response carries that table's own vended storage credentials.
* A `CatalogSession` is valid for one `(catalog_uri, warehouse)` tuple. Join legs override only `CatalogProps.table`.
* `CatalogSession` is a `pub` type of the `lakehouse-catalog` crate. The scan-resolution seam in the engine takes the session by shared reference for an Iceberg scan source, so every file resolution of a request runs on one session. `architecture.md` states the crate boundary.
* The session holds the live OAuth2 bearer token in its auth strategy. Redaction at the `loadTable` error site strips both the static catalog-auth secrets and that token.
* The caller validates every involved-table identifier before it builds the session. A malformed identifier issues no catalog HTTP request.
* The scan spec's `storage` value is the only part of the scan-driving SQL that depends on how the credential is delivered. `storage-access/scan-spec-credential-reference` encodes it as a connection reference or as a sealed envelope over the backend's encoding. Each table's own vended keys still land in that table's storage block, field for field, before sealing. `storage-access/pushdown-planning-cloud-credentials` owns that gate.
* The namespace enumeration for `createVirtualSchema` is done by `IcebergRestCatalogClient::list_tables` in `lakehouse-catalog`, behind the shared `CatalogClient` trait. It builds one `CatalogSession` for the whole enumeration.
* The enumeration also builds its own `RestCatalog` through `list_namespace_tables`, independent of any `CatalogSession`. With OAuth2 client credentials that `RestCatalog` runs its own grant and `/v1/config` handshake, so a request performs two grants. With a static token or no auth it runs no grant and one extra `/v1/config` lookup. With SigV4 it builds no `RestCatalog`.
* Related features: `catalog/rest-catalog-oauth-auth` (the three auth modes), `connection/connection-credentials-catalog-auth`, and `pushdown/pushdown-planning` (file list resolved once).
* The Iceberg REST `listTables` response carries only table identifiers and a next-page token (`ListTablesResponse` in `iceberg-catalog-rest`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A scoped refresh lists Iceberg identifiers and loads the requested tables on one session

* *GIVEN* an Iceberg REST namespace with descendant namespaces, and a partial refresh per `vs-adapter/refresh-and-set-properties`
* *WHEN* the Iceberg REST client lists the namespace's identifiers and then loads the tables the adapter selects
* *THEN* the identifier listing SHALL call only the namespace and table listing endpoints, besides the listing handshake this Background records, and SHALL issue no `loadTable` GET
* *AND* the load SHALL build exactly ONE `CatalogSession`, issue one `loadTable` GET per given identifier through the load helper the full listing uses, and build NO session for an empty batch
* *AND* a table the catalog reports as not a loadable Iceberg table SHALL be skipped with the `NotLoadableIcebergTable` reason the full listing gives it, and any other load failure SHALL fail the load
<!-- /DELTA:NEW -->
