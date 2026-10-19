# Feature: Pushdown Catalog HTTP Session

The adapter builds the catalog HTTP state once per pushdown request and reuses it for every table's `loadTable` GET. The state is one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool.

## Background

* The catalog-auth token and the `/v1/config` prefix are catalog-scoped, so the session computes them once per query. The `loadTable` GET stays per-table because each response carries that table's own vended storage credentials.
* A `CatalogSession` is valid for one `(catalog_uri, warehouse)` tuple. Join legs override only `CatalogProps.table`.
* `CatalogSession` is a `pub` type of the `lakehouse-catalog` crate. `resolve_file_list` takes `&CatalogSession` as its first parameter and is the only file-resolution entry point that takes one. `catalog/catalog-crate-structure` owns the crate boundary.
* The session holds the live OAuth2 bearer token in its auth strategy. Redaction at the `loadTable` error site strips both the static catalog-auth secrets and that token.
* The caller validates every involved-table identifier before it builds the session. A malformed identifier issues no catalog HTTP request.
* The scan spec's `storage` value is the only part of the scan-driving SQL that differs from the pre-session output. `storage-access/scan-spec-credential-reference` encodes it as a connection reference or as a sealed envelope over the backend's encoding. Each table's own vended keys still land in that table's storage block, field for field, before sealing. `storage-access/pushdown-planning-cloud-credentials` owns that gate.
* The namespace enumeration for `createVirtualSchema` is done by `IcebergRestCatalogClient::list_tables` in `lakehouse-catalog`, behind the shared `CatalogClient` trait. It builds one `CatalogSession` for the whole enumeration.
* The enumeration also builds its own `RestCatalog` through `list_namespace_tables`, independent of any `CatalogSession`. With OAuth2 client credentials that `RestCatalog` runs its own grant and `/v1/config` handshake, so a request performs two grants. With a static token or no auth it runs no grant and one extra `/v1/config` lookup. With SigV4 it builds no `RestCatalog`.
* Related features: `catalog/rest-catalog-oauth-auth` (the three auth modes), `connection/connection-credentials-catalog-auth`, and `pushdown/pushdown-planning` (file list resolved once).


## Scenarios

### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* a single-table pushdown request against an Iceberg REST catalog
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the adapter SHALL build exactly one `CatalogSession` — one HTTP client, one resolved catalog-auth strategy, and one resolved `/v1/config` prefix — before issuing the table's `loadTable` GET
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the request
* *AND* the table's `loadTable` GET SHALL be issued on that session's client using the session's auth and prefix, and the success-path catalog request URLs — the `/v1/config` prefix lookup and the `loadTable` GET — MUST be identical to the pre-refactor path for the same query
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` VALUE, which `storage-access/scan-spec-credential-reference` re-encodes as a connection reference carrying no credential or as a sealed envelope over the backend's byte-identical (pre-encryption) encoding — a WIDENING of the recorded variant-tag exception, because the value itself now changes rather than only what encloses it

### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N tables whose legs share `catalog_uri` and `warehouse` and differ only by `CatalogProps.table`
* *WHEN* the join planner resolves every leg's file list
* *THEN* the planner SHALL build the `CatalogSession` once and pass the same session by shared reference to every per-leg resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL run once for the whole request, not once per leg
* *AND* the adapter SHALL issue exactly one `loadTable` GET per leg on the shared session
* *AND* the generated broadcast or N-scan join SQL MUST be byte-identical to the pre-refactor output EXCEPT for each leg's `storage` VALUE, re-encoded per side by `storage-access/scan-spec-credential-reference` — a WIDENING of the recorded variant-tag exception, and the ONLY widening: every other byte of the join SQL stays byte-identical

### Scenario: Per-table loadTable GET is preserved so per-table vended credentials are returned

* *GIVEN* a query resolving one or more tables through a session, with vended credentials enabled
* *WHEN* the adapter loads each table's metadata
* *THEN* the adapter SHALL issue a distinct `loadTable` GET per table on the shared session, carrying `X-Iceberg-Access-Delegation: vended-credentials` exactly as before
* *AND* the adapter SHALL extract each table's own vended storage credentials from that table's `loadTable` response and place them into that table's per-shard scan-spec storage block, which `storage-access/scan-spec-credential-reference` requires to be the SEALED envelope of that table's own backend rather than a plaintext backend, so no vended value appears there in plaintext (issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378))
* *AND* the backend each table produces MUST be identical to the pre-refactor output for that table FIELD FOR FIELD — the per-table CONTENT guarantee this clause exists to protect, unchanged — while the permitted ENCODING differences are exactly two and no more: the externally-tagged backend wrapper `storage-access/storage-backend-enum` adds around it, and the scan-spec storage wrapper `storage-access/scan-spec-credential-reference` adds around that, whose sealed variant encrypts precisely that byte-identical encoding

### Scenario: The per-table loader cannot re-derive auth or prefix

* *GIVEN* the refactored per-table `loadTable` loader
* *WHEN* it issues a table's `loadTable` GET
* *THEN* it SHALL take the catalog-auth strategy, the prefix, and the HTTP client from the `CatalogSession` passed by shared reference
* *AND* it MUST NOT run the OAuth2 grant, MUST NOT perform the `/v1/config` lookup, and MUST NOT construct a new HTTP client
* *AND* this constraint SHALL be enforced structurally — the loader receives the session by shared reference and holds no means to re-derive auth or prefix

### Scenario: createVirtualSchema resolves every table's schema on one shared session

* *GIVEN* a createVirtualSchema request over a namespace holding N Iceberg tables, whose schema resolution moves from an adapter-side loop calling `resolve_table_schema` per table into `IcebergRestCatalogClient::list_tables` in `lakehouse-catalog`
* *WHEN* the Iceberg REST catalog client enumerates the namespace and resolves every enumerated table's columns
* *THEN* the client SHALL build exactly ONE `CatalogSession` for the whole enumeration and reuse it across every table via a private session-taking load helper distinct from the trait `load_table`, so an N-table namespace costs one schema-loop grant instead of N
* *AND* an empty identifier batch (a namespace the catalog reports as holding no table) SHALL build NO resolution `CatalogSession` and perform NO resolution-phase OAuth2 grant — the resolution half of the guarantee, migrating from the removed engine test to `crates/lakehouse-catalog/src/client_tests.rs::empty_namespace_builds_no_session_and_no_grant` (which drives the private `resolve_listing(&[])` directly against an unreachable URI under OAuth2 creds) and exercised end-to-end through the public `list_tables` by `::list_tables_over_empty_namespace_lists_nothing`; the enumeration-phase grant of the next clause is UNAFFECTED, so an empty namespace still costs ONE grant under OAuth2 client-credentials and ZERO under the no-auth and static-token modes
* *AND* on the OAuth2 client-credentials mode — both `client_id` and `client_secret` set — the namespace-enumeration `RestCatalog` SHALL retain its own independent grant and `/v1/config` handshake, so such a request still performs TWO grants in total rather than one; on the static-token and no-auth modes it performs NO grant and one extra `/v1/config` lookup; on the SigV4 mode `list_namespace_tables` builds no `RestCatalog` at all
* *AND* a table the catalog reports as not a loadable Iceberg table SHALL still be skipped with the same warning and SHALL NOT abort the enumeration, routed into `CatalogListing.skipped`, so the non-Iceberg-table skip behavior is unchanged
* *AND* an OAuth2 grant failure SHALL surface once, before the per-table loop, instead of once at the first table, because the session is built ahead of the enumeration
* *AND* the enumerated tables, each table's resolved column list, and its Exasol type mapping MUST stay byte-identical to the pre-refactor per-session output, verified by `crates/lakehouse-catalog/src/client_tests.rs::enumeration_builds_exactly_one_session`

### Scenario: CatalogSession is public and every file-resolution entry point takes one

* *GIVEN* `CatalogSession` declared `pub` in the `lakehouse-catalog` crate, and the `refactor-catalog-http-session` wrapper pair `resolve_file_list(catalog_uri: &str, …)` plus `resolve_file_list_with_session(&CatalogSession, …)`
* *WHEN* any caller — the single-table pushdown path, a join leg, or an external integration test — resolves a file list
* *THEN* exactly ONE file-resolution function SHALL remain, named `resolve_file_list`, `pub`, taking `&CatalogSession` as its first parameter, and `resolve_file_list_with_session` SHALL be DELETED rather than kept as an alias
* *AND* `resolve_table_schema` SHALL be DELETED from the file-resolution entry-point set — its only production caller replaced by the shared `CatalogClient` listing pipeline — so after deletion `resolve_file_list` is the SOLE `&CatalogSession`-taking file-resolution entry point and no `resolve_table_schema` entry point remains to take a session
* *AND* `resolve_file_list` SHALL NOT retain a `catalog_uri: &str` parameter, because the session already carries the catalog URI and a second copy could disagree with it
* *AND* the caller SHALL validate every involved-table identifier BEFORE it builds the session, so a malformed identifier still issues ZERO catalog HTTP requests and returns the same parse error as before — the guarantee moves from inside `resolve_file_list` to its callers, and is not dropped
* *AND* `CatalogSession`'s fields MUST stay private and `CatalogAuth` MUST stay crate-private to `lakehouse-catalog`, so making the type public exposes no auth internals
* *AND* a caller outside the `lakehouse-catalog` crate SHALL be able to construct a `CatalogSession` itself and pass it into file resolution, which is the capability the crate extraction exists to grant, and the file lists it resolves MUST be unchanged

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
