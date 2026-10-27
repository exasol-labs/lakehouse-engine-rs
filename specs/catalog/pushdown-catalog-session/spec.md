# Feature: Pushdown Catalog HTTP Session

The adapter builds the catalog HTTP state once per pushdown request and reuses it for every table's `loadTable` GET. The state is one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool.

## Background

* The catalog-auth token and the `/v1/config` prefix are catalog-scoped, so the session computes them once per query. The `loadTable` GET stays per-table because each response carries that table's own vended storage credentials.
* A `CatalogSession` is valid for one `(catalog_uri, warehouse)` tuple. Join legs override only `CatalogProps.table`.
* `CatalogSession` is a `pub` type of the `lakehouse-catalog` crate. The scan-resolution seam in the engine takes the session by shared reference for an Iceberg scan source, so every file resolution of a request runs on one session. `architecture.md` states the crate boundary.
* The session holds the live OAuth2 bearer token in its auth strategy. Redaction at the `loadTable` error site strips both the static catalog-auth secrets and that token.
* The caller validates every involved-table identifier before it builds the session. A malformed identifier issues no catalog HTTP request.
* The scan spec's `storage` value is the only part of the scan-driving SQL that depends on how the credential is delivered. `storage-access/scan-spec-credential-reference` encodes it as a connection reference or as a sealed envelope over the backend's encoding. Each table's own vended keys still land in that table's storage block, field for field, before sealing. `storage-access/pushdown-planning-cloud-credentials` owns that gate.
* The namespace enumeration for `createVirtualSchema` is done by `IcebergRestCatalogClient` in `lakehouse-catalog`, behind the shared `CatalogClient` trait. One `CatalogSession`, resolved at the client's first catalog request, serves the namespace listing and every `loadTable` GET, so a request runs one OAuth2 grant and one `/v1/config` lookup.
* Related features: `catalog/rest-catalog-oauth-auth` (the three auth modes), `connection/connection-credentials-catalog-auth`, and `pushdown/pushdown-planning` (file list resolved once).
* The Iceberg REST `listTables` response carries only table identifiers and a next-page token (`ListTablesResponse` in `iceberg-catalog-rest`).

## Scenarios

### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* a single-table pushdown request against an Iceberg REST catalog
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the adapter SHALL build exactly one `CatalogSession`, one HTTP client, one resolved catalog-auth strategy, and one resolved `/v1/config` prefix, before issuing the table's `loadTable` GET
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the request
* *AND* the table's `loadTable` GET SHALL be issued on that session's client using the session's auth and prefix, and the success-path catalog request URLs, the `/v1/config` prefix lookup and the `loadTable` GET, MUST be the same as the URLs a per-table session would issue for the same query
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST NOT depend on the session, EXCEPT for the `storage` VALUE, which `storage-access/scan-spec-credential-reference` encodes as a connection reference carrying no credential or as a sealed envelope over the backend's encoding

### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N tables whose legs share `catalog_uri` and `warehouse` and differ only by `CatalogProps.table`
* *WHEN* the join planner resolves every leg's file list
* *THEN* the planner SHALL build the `CatalogSession` once and pass the same session by shared reference to every per-leg resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL run once for the whole request, not once per leg
* *AND* the adapter SHALL issue exactly one `loadTable` GET per leg on the shared session
* *AND* the generated broadcast or N-scan join SQL MUST NOT depend on the session, EXCEPT for each leg's `storage` VALUE, which `storage-access/scan-spec-credential-reference` encodes per side

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

### Scenario: CatalogSession is public and every file-resolution entry point takes one

* *GIVEN* `CatalogSession` declared `pub` in the `lakehouse-catalog` crate
* *WHEN* any caller, whether the single-table pushdown path, a join leg, or an external integration test, resolves a file list
* *THEN* file resolution SHALL go through the engine's scan-resolution seam, which takes the request's `&CatalogSession` for an Iceberg scan source, and no entry point SHALL take a catalog URI in place of the session, because the session already carries the catalog URI and a second copy could disagree with it
* *AND* the caller SHALL validate every involved-table identifier BEFORE it builds the session, so a malformed identifier issues ZERO catalog HTTP requests and returns the identifier parse error
* *AND* `CatalogSession`'s fields MUST stay private and `CatalogAuth` MUST stay crate-private to `lakehouse-catalog`, so making the type public exposes no auth internals
* *AND* a caller outside the `lakehouse-catalog` crate SHALL be able to construct a `CatalogSession` itself and pass it into file resolution, and the file lists it resolves MUST be unchanged

### Scenario: Catalog secrets and the live session token never leak on the loadTable error path

* *GIVEN* a `CatalogSession` whose resolved auth is an OAuth2-obtained bearer token
* *WHEN* a `loadTable` GET issued on that session returns a transport, HTTP-status, or parse error
* *THEN* the returned error message MUST NOT contain any static catalog-auth secret from the CONNECTION
* *AND* the returned error message MUST NOT contain the live bearer token held in the session's auth strategy
* *AND* neither the static secrets nor the obtained token SHALL appear in any returned SQL string

### Scenario: The Lakekeeper permission check reuses the request's one catalog session

* *GIVEN* a single-table or an N-table join pushdown request under OAuth2 client-credentials catalog auth, on a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'` whose Lakekeeper answer allows every table
* *WHEN* the adapter checks permissions and resolves every table's file list
* *THEN* the OAuth2 grant and the `/v1/config` lookup SHALL each run exactly once for the request, as they do with the check off
* *AND* the batch-check SHALL carry the bearer token of that one grant
* *AND* the check SHALL add exactly one request, the batch-check, between the `/v1/config` lookup and the first `loadTable` GET, and SHALL leave every other request of "Single-table pushdown builds one catalog session and reuses it" and "N-table join reuses one session across all legs" unchanged

### Scenario: A scoped refresh lists Iceberg identifiers and loads the requested tables on one session

* *GIVEN* an Iceberg REST namespace with descendant namespaces, and a partial refresh per `vs-adapter/refresh-and-set-properties`
* *WHEN* the Iceberg REST client lists the namespace's identifiers and then loads the tables the adapter selects
* *THEN* the identifier listing SHALL call only the namespace and table listing endpoints, besides the session's grant and `/v1/config` lookup, and SHALL issue no `loadTable` GET
* *AND* the load SHALL reuse the listing's `CatalogSession` and issue one `loadTable` GET per given identifier through the load helper the full listing uses
* *AND* a table the catalog reports as not a loadable Iceberg table SHALL be skipped with the `NotLoadableIcebergTable` reason the full listing gives it, and any other load failure SHALL fail the load

### Scenario: A failed config lookup fails the request

* *GIVEN* a CONNECTION without `use_sigv4` whose `GET {uri}/v1/config?warehouse={warehouse}` fails or answers a non-success status, for example for an unknown warehouse name
* *WHEN* the adapter resolves the catalog session of a pushdown, `createVirtualSchema`, `REFRESH`, or `SET PROPERTIES` request
* *THEN* the request SHALL fail with an error that names the warehouse and carries the catalog's answer
* *AND* the adapter MUST NOT fall back to an empty prefix, because an unprefixed request reaches the catalog's default warehouse or answers every table with HTTP 404

### Scenario: A failed loadTable names the table it loads

* *GIVEN* a `loadTable` GET on a `CatalogSession` for the table `orders` in the namespace `sales.eu`, issued either by the enumeration of a `createVirtualSchema`, `REFRESH`, or `SET PROPERTIES` request or by a pushdown's table resolution
* *WHEN* the GET fails with an HTTP status other than 404, with a transport error, a timeout of `catalog/catalog-http-resilience` included, or with an answer that does not parse as a `loadTable` result
* *THEN* the returned error SHALL name the table by its dotted catalog identifier `sales.eu.orders`, the form its identifier note records
* *AND* the error SHALL carry the cause: the HTTP status and the redacted answer body, every cause the HTTP client reports, or the parse failure
* *AND* a pushdown's `loadTable` that answers HTTP 404 SHALL fail with an error that names the table in the same way
* *AND* naming the table MUST NOT change which enumeration loads are skipped, so an enumeration `loadTable` that answers HTTP 404 SHALL still skip the table with the `NotLoadableIcebergTable` reason
