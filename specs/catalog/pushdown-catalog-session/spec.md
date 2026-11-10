# Feature: Pushdown Catalog HTTP Session

The adapter authenticates to an Iceberg REST catalog once per statement and virtual schema and takes the catalog prefix from the schema's `CATALOG_PREFIX` note, and every `loadTable` GET of that statement's pushdown requests carries that token and that prefix over one connection pool. A statement over N tables therefore runs one OAuth2 grant, no `/v1/config` lookup, and one connection pool. `file-planning/pushdown-statement-state` states the statement scope.

## Background

* The catalog-auth token and the `/v1/config` prefix are catalog-scoped, so the session computes them once per statement and virtual schema. The `loadTable` GET stays per-table because each response carries that table's own vended storage credentials.
* A `CatalogSession` is valid for one `(catalog_uri, warehouse)` tuple. Join legs override only `CatalogProps.table`.
* The session holds the live OAuth2 bearer token in its auth strategy, and renews it before it expires (`catalog/rest-catalog-oauth-auth`). Redaction at the `loadTable` error site strips both the static catalog-auth secrets and every token the session has held.
* The adapter validates every involved-table identifier before its first catalog request. A malformed identifier issues no catalog HTTP request.
* The scan spec's `storage` value is the only part of the scan-driving SQL that depends on how the credential is delivered. `storage-access/scan-spec-credential-reference` encodes it as a connection reference or as a sealed envelope over the backend's encoding. Each table's own vended keys still land in that table's storage block, field for field, before sealing. `storage-access/pushdown-planning-cloud-credentials` owns that gate.
* `IcebergRestCatalogClient` in `lakehouse-catalog`, behind the shared `CatalogClient` trait, runs the namespace enumeration for `createVirtualSchema` and the pushdown loads. It resolves its one `CatalogSession` at its first catalog request. That session serves a `createVirtualSchema` request's namespace listing and every `loadTable` GET of a statement's pushdowns, so each runs one OAuth2 grant and one `/v1/config` lookup. `architecture.md` states the crate boundary.
* Related features: `catalog/rest-catalog-oauth-auth` (the three auth modes), `connection/connection-credentials-catalog-auth`, and `file-planning/pushdown-statement-state` (what a statement keeps).
* The Iceberg REST `listTables` response carries only table identifiers and a next-page token (`ListTablesResponse` in `iceberg-catalog-rest`).

## Scenarios

### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* the first pushdown request of a statement on a virtual schema over an Iceberg REST catalog, with one involved table
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the statement, before the table's `loadTable` GET, except that one more grant runs for each token that reaches its renewal point during the statement
* *AND* the table's `loadTable` GET SHALL carry that grant's token and that prefix, and the success-path catalog request URLs, the `/v1/config` prefix lookup and the `loadTable` GET, MUST be the URLs that the same table yields as the only table of its statement
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST NOT depend on how many requests of the statement share the grant and the prefix, EXCEPT for the `storage` VALUE, which `storage-access/scan-spec-credential-reference` encodes as a connection reference carrying no credential or as a sealed envelope over the backend's encoding

### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N legs on a virtual schema over an Iceberg REST catalog, where two legs may name the same table, as a self-join does
* *WHEN* the adapter resolves every leg's file list
* *THEN* the OAuth2 client-credentials grant SHALL run at most once for the whole request, not once per leg, and the adapter SHALL send no `/v1/config` request
* *AND* the adapter SHALL issue at most one `loadTable` GET per distinct table of the request, so a two-leg self-join issues one, and SHALL issue none for a table that an earlier pushdown of the statement loaded

### Scenario: A pushdown takes the catalog prefix from the schema notes

* *GIVEN* a pushdown request on an Iceberg REST virtual schema whose schema-level adapterNotes hold `CATALOG_PREFIX`, the empty string included
* *WHEN* the adapter resolves the statement's catalog session
* *THEN* the adapter MUST NOT send a `/v1/config` request
* *AND* every `loadTable` GET SHALL carry the recorded prefix, so its URL equals the one the `/v1/config` prefix yields

### Scenario: Per-table loadTable GET is preserved so per-table vended credentials are returned

* *GIVEN* a query resolving one or more tables through a session, with vended credentials enabled
* *WHEN* the adapter loads each table's metadata
* *THEN* the adapter SHALL issue a distinct `loadTable` GET per table on the shared session, carrying `X-Iceberg-Access-Delegation: vended-credentials` exactly as before
* *AND* the adapter SHALL extract each table's own vended storage credentials from that table's `loadTable` response and place them into that table's per-shard scan-spec storage block, which `storage-access/scan-spec-credential-reference` requires to be the SEALED envelope of that table's own backend rather than a plaintext backend, so no vended value appears there in plaintext
* *AND* the backend each table produces MUST carry that table's vended values FIELD FOR FIELD, wrapped only by the externally-tagged backend wrapper `storage-access/storage-backend-enum` and the scan-spec storage wrapper `storage-access/scan-spec-credential-reference`, whose sealed variant encrypts exactly that encoding

### Scenario: A pushdown's loadTable asks only for the referenced snapshots

* *GIVEN* a pushdown request that resolves an Iceberg REST table
* *WHEN* the adapter issues the table's `loadTable` GET
* *THEN* the GET SHALL request `snapshots=refs`, because the scan reads only the current snapshot
* *AND* the answer SHALL still hold the current snapshot, because the Iceberg table spec states: "There is always a `main` branch reference pointing to the `current-snapshot-id` even if the `refs` map is null"

### Scenario: The per-table loader cannot re-derive auth or prefix

* *GIVEN* the per-table `loadTable` loader
* *WHEN* it issues a table's `loadTable` GET
* *THEN* it SHALL take the catalog-auth strategy, the prefix, and the HTTP client from the `CatalogSession` passed by shared reference
* *AND* it MUST NOT run the OAuth2 grant, MUST NOT perform the `/v1/config` lookup, and MUST NOT construct a new HTTP client
* *AND* this constraint SHALL be enforced structurally, the loader receives the session by shared reference and holds no means to re-derive auth or prefix

### Scenario: createVirtualSchema resolves every table's schema on one shared session

* *GIVEN* a createVirtualSchema request over a namespace holding N Iceberg tables, whose schema resolution runs inside the Iceberg REST catalog client's `list_tables` in `lakehouse-catalog`
* *WHEN* the Iceberg REST catalog client enumerates the namespace and resolves every enumerated table's columns
* *THEN* the client SHALL build exactly ONE `CatalogSession` and use it for the namespace listing and every table's load, so a request runs one OAuth2 grant and one `/v1/config` lookup whatever the table count
* *AND* an empty namespace SHALL still cost that one session, because only a catalog request can show that the namespace is empty: ONE grant under OAuth2 client-credentials and ZERO under the no-auth and static-token modes
* *AND* a table the catalog reports as not a loadable Iceberg table SHALL still be skipped with the same warning and SHALL NOT abort the enumeration, routed into `CatalogListing.skipped`, so the non-Iceberg-table skip behavior is unchanged
* *AND* an OAuth2 grant failure SHALL surface once, before the namespace listing
* *AND* the enumerated tables, each table's resolved column list, and its Exasol type mapping MUST NOT depend on how many sessions the enumeration builds

### Scenario: Catalog secrets and the live session token never leak on the loadTable error path

* *GIVEN* a `CatalogSession` whose resolved auth is an OAuth2-obtained bearer token
* *WHEN* a `loadTable` GET issued on that session returns a transport, HTTP-status, or parse error
* *THEN* the returned error message MUST NOT contain any static catalog-auth secret from the CONNECTION
* *AND* the returned error message MUST NOT contain any bearer token the session's auth strategy has held
* *AND* neither the static secrets nor any obtained token SHALL appear in any returned SQL string

### Scenario: The Lakekeeper permission check reuses the statement's one catalog session

* *GIVEN* a single-table or an N-table join pushdown request under OAuth2 client-credentials catalog auth, on a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'` whose Lakekeeper answer allows every table
* *WHEN* the adapter checks permissions and resolves every table's file list
* *THEN* the OAuth2 grant and the `/v1/config` lookup SHALL each run at most once for the statement, as they do with the check off
* *AND* the batch-check SHALL carry the bearer token that the session holds at that moment
* *AND* a request that reads a table that no earlier pushdown of the statement checked SHALL add exactly one request, the batch-check, after the `/v1/config` lookup and before the first `loadTable` GET of those tables, and SHALL leave every other request of "Single-table pushdown builds one catalog session and reuses it" and "N-table join reuses one session across all legs" unchanged
* *AND* a request whose tables earlier pushdowns of the statement all checked SHALL add no request

### Scenario: A scoped refresh lists Iceberg identifiers and loads the requested tables on one session

* *GIVEN* an Iceberg REST namespace with descendant namespaces, and a partial refresh per `vs-adapter/refresh-and-set-properties`
* *WHEN* the Iceberg REST client lists the namespace's identifiers and then loads the tables the adapter selects
* *THEN* the identifier listing SHALL call only the namespace and table listing endpoints, besides the session's grant and `/v1/config` lookup, and SHALL issue no `loadTable` GET
* *AND* the load SHALL reuse the listing's `CatalogSession` and issue one `loadTable` GET per given identifier through the load helper the full listing uses
* *AND* a table the catalog reports as not a loadable Iceberg table SHALL be skipped with the reason the full listing gives it, and any other load failure SHALL fail the load

### Scenario: A failed config lookup fails the request

* *GIVEN* a CONNECTION without `use_sigv4` whose `GET {uri}/v1/config?warehouse={warehouse}` fails or answers a non-success status, for example for an unknown warehouse name
* *WHEN* the adapter resolves the catalog session of a `createVirtualSchema`, `REFRESH`, or `SET PROPERTIES` request
* *THEN* the request SHALL fail with an error that names the warehouse and carries the catalog's answer
* *AND* the adapter MUST NOT fall back to an empty prefix, because an unprefixed request reaches the catalog's default warehouse or answers every table with HTTP 404

### Scenario: A failed loadTable names the table it loads

* *GIVEN* a `loadTable` GET on a `CatalogSession` for the table `orders` in the namespace `sales.eu`, issued either by the enumeration of a `createVirtualSchema`, `REFRESH`, or `SET PROPERTIES` request or by a pushdown's table resolution
* *WHEN* the GET fails with an HTTP status other than 404, with a transport error, a timeout of `catalog/catalog-http-resilience` included, or with an answer that does not parse as a `loadTable` result
* *THEN* the returned error SHALL name the table by its dotted catalog identifier `sales.eu.orders`, the form its identifier note records
* *AND* the error SHALL carry the cause: the HTTP status and the redacted answer body, every cause the HTTP client reports, or the parse failure
* *AND* a pushdown's `loadTable` that answers HTTP 404 SHALL fail with an error that names the table in the same way
* *AND* naming the table MUST NOT change which enumeration loads are skipped, so an enumeration `loadTable` that answers HTTP 404 or 400 SHALL still skip the table per `vs-adapter/create-virtual-schema` instead of failing
