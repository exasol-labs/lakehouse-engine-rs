# Feature: Pushdown Catalog HTTP Session

The adapter authenticates to an Iceberg REST catalog and looks up its `/v1/config` prefix once per statement and virtual schema, and every `loadTable` GET of that statement's pushdown requests carries that token and that prefix over one connection pool. A statement over N tables therefore runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool. `file-planning/pushdown-statement-state` states the statement scope.

<!-- DELTA:CHANGED -->
## Background

* The catalog-auth token and the `/v1/config` prefix are catalog-scoped, so the session computes them once per statement and virtual schema. The `loadTable` GET stays per-table because each response carries that table's own vended storage credentials.
* A `CatalogSession` is valid for one `(catalog_uri, warehouse)` tuple. Join legs override only `CatalogProps.table`.
* The session holds the live OAuth2 bearer token in its auth strategy, and renews it before it expires (`catalog/rest-catalog-oauth-auth`). Redaction at the `loadTable` error site strips both the static catalog-auth secrets and every token the session has held.
* The adapter validates every involved-table identifier before its first catalog request. A malformed identifier issues no catalog HTTP request.
* The scan spec's `storage` value is the only part of the scan-driving SQL that depends on how the credential is delivered. `storage-access/scan-spec-credential-reference` encodes it as a connection reference or as a sealed envelope over the backend's encoding. Each table's own vended keys still land in that table's storage block, field for field, before sealing. `storage-access/pushdown-planning-cloud-credentials` owns that gate.
* `IcebergRestCatalogClient` in `lakehouse-catalog`, behind the shared `CatalogClient` trait, runs the namespace enumeration for `createVirtualSchema` and the pushdown loads. It resolves its one `CatalogSession` at its first catalog request. That session serves a `createVirtualSchema` request's namespace listing and every `loadTable` GET of a statement's pushdowns, so each runs one OAuth2 grant and one `/v1/config` lookup. `architecture.md` states the crate boundary.
* Related features: `catalog/rest-catalog-oauth-auth` (the three auth modes), `connection/connection-credentials-catalog-auth`, and `file-planning/pushdown-statement-state` (what a statement keeps).
* The Iceberg REST `listTables` response carries only table identifiers and a next-page token (`ListTablesResponse` in `iceberg-catalog-rest`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* the first pushdown request of a statement on a virtual schema over an Iceberg REST catalog, with one involved table
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the statement, before the table's `loadTable` GET, except that one more grant runs for each token that reaches its renewal point during the statement
* *AND* the table's `loadTable` GET SHALL carry that grant's token and that prefix, and the success-path catalog request URLs, the `/v1/config` prefix lookup and the `loadTable` GET, MUST be the URLs that the same table yields as the only table of its statement
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST NOT depend on how many requests of the statement share the grant and the prefix, EXCEPT for the `storage` VALUE, which `storage-access/scan-spec-credential-reference` encodes as a connection reference carrying no credential or as a sealed envelope over the backend's encoding
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Catalog secrets and the live session token never leak on the loadTable error path

* *GIVEN* a `CatalogSession` whose resolved auth is an OAuth2-obtained bearer token
* *WHEN* a `loadTable` GET issued on that session returns a transport, HTTP-status, or parse error
* *THEN* the returned error message MUST NOT contain any static catalog-auth secret from the CONNECTION
* *AND* the returned error message MUST NOT contain any bearer token the session's auth strategy has held
* *AND* neither the static secrets nor any obtained token SHALL appear in any returned SQL string
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The Lakekeeper permission check reuses the statement's one catalog session

* *GIVEN* a single-table or an N-table join pushdown request under OAuth2 client-credentials catalog auth, on a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'` whose Lakekeeper answer allows every table
* *WHEN* the adapter checks permissions and resolves every table's file list
* *THEN* the OAuth2 grant and the `/v1/config` lookup SHALL each run at most once for the statement, as they do with the check off
* *AND* the batch-check SHALL carry the bearer token that the session holds at that moment
* *AND* a request that reads a table that no earlier pushdown of the statement checked SHALL add exactly one request, the batch-check, after the `/v1/config` lookup and before the first `loadTable` GET of those tables, and SHALL leave every other request of "Single-table pushdown builds one catalog session and reuses it" and "N-table join reuses one session across all legs" unchanged
* *AND* a request whose tables earlier pushdowns of the statement all checked SHALL add no request
<!-- /DELTA:CHANGED -->
