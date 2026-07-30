# Feature: Pushdown Catalog HTTP Session

Builds the catalog HTTP state — one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix — once per pushdown request and reuses it across every table's `loadTable` GET, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool instead of N of each. The per-table `loadTable` GET stays per-table because each response carries that table's own vended storage credentials. This is pure connection and session reuse: the URLs, catalog auth, resolved file lists, and generated SQL are identical to the pre-refactor path.

## Background

* The catalog-auth token and the `/v1/config` prefix are CATALOG-scoped, not table-scoped, so both are correctly computed once per query. The Apache Iceberg REST Catalog spec confirms this: `GET /v1/config` is the route "All REST clients should first call ... to get catalog configuration properties from the server to configure the catalog and its HTTP client", keyed by the `warehouse` parameter; the OAuth2 client-credentials grant returns a catalog bearer token; and only the `loadTable` response carries "credentials that should be used for subsequent requests for the table" — i.e. per-table vended `storage_credentials`.
* A `CatalogSession` is valid for exactly one `(catalog_uri, warehouse)` tuple. Across a single pushdown request — single-table OR N-table join — `catalog_uri` and `warehouse` are constant; join legs override only `CatalogProps.table`. The per-table variation is namespace and table only, entering solely at `build_load_table_url` inside the per-table `loadTable` GET.
* Catalog-auth secrets are consumed only in the planning layer and never cross the UDF boundary. The session holds the live OAuth2-obtained bearer token in its auth strategy, so redaction at the `loadTable`-GET error site MUST still strip both the static catalog-auth secrets and that live token.
* Table-identifier validation runs before the session's `/v1/config` lookup. A malformed involved-table identifier is rejected at the pushdown seam before `CatalogSession::resolve`, so a malformed-table request issues no catalog HTTP request and returns the same parse error as before. This preserves the issue's identical-URLs intent: the catalog request set on the success path is byte-identical, and the error path is never larger than before.
* `CatalogSession` is a `pub` type of the `lakehouse-catalog` crate (issue #204), reachable as `lakehouse_catalog::CatalogSession`. It is NOT re-exported on the `crate::adapter::pushdown::<name>` public façade: external callers name it on the crate that declares it, so a second path on the façade would be a redundant alias. `vs-adapter/catalog-crate-structure` owns the crate boundary; `vs-adapter/pushdown-module-structure` owns the redrawn façade.
* `resolve_file_list` takes `&CatalogSession` directly. The `refactor-catalog-http-session` wrapper pair — a `catalog_uri`-taking public entry point delegating to a `pub(crate)` `resolve_file_list_with_session` core — existed only because `CatalogSession` was `pub(crate)`. With the type genuinely `pub`, one function serves the single-table path, every join leg, and the external E2E callers, and `resolve_file_list_with_session` is deleted rather than kept as an alias.
* Retiring the wrapper MOVES the parse-before-config guarantee rather than dropping it. `resolve_file_list` no longer builds the session, so it can no longer be the place that validates the identifier first. Both single-table and join callers now validate every involved-table identifier at the `handle_pushdown` seam before `CatalogSession::resolve`, which is where the join path already did it.
* The createVirtualSchema table loop is the one remaining per-table session build, and the crate extraction is what makes fixing it a signature change rather than a new public type. Its per-table cost was accepted in `refactor-catalog-http-session` (#185) only because `resolve_table_schema` could not name a `pub(crate)` type in its public signature.
* The schema loop is NOT the whole request, and the residual cost differs per auth mode. `adapter/mod.rs:246` calls `list_namespace_tables` before the loop. When `client_id` and `client_secret` are both set, that builds a `RestCatalog` whose `iceberg-catalog-rest` 0.10.0 client runs its own `client_credentials` exchange (`client.rs:123`) and its own `/v1/config` handshake (`catalog.rs:430`), driven by the `credential`, `oauth2-server-uri`, and `scope` props `inject_catalog_auth_props` sets (`credentials.rs:101-123`); such a request goes from N+1 grants to 2, never to 1. On the static-token and no-auth modes `inject_catalog_auth_props` sets a bearer `token` or nothing, so `iceberg-catalog-rest` runs no grant and the residual cost is one extra `/v1/config` lookup. On the SigV4 mode `list_namespace_tables` (`namespace.rs:36-38`) takes the `list_in_namespace_signed` branch and builds no `RestCatalog`, so neither a grant nor a config lookup exists to count. That handshake, where it exists, is independent of any `CatalogSession` and is unchanged by this feature, and any scenario clause about grant counts binds only the schema loop.
* Related features stay accurate and unedited: `vs-adapter/pushdown-planning-cloud-credentials` (SigV4 signing, vended-credential extraction, Glue prefix derivation), `vs-adapter/rest-catalog-oauth-auth` (the three catalog-auth modes), `vs-adapter/connection-credentials-catalog-auth` (secrets never leak), and `vs-adapter/pushdown-planning` (file list resolved once). This feature adds the once-per-query session-reuse invariant that spans the single-table and join paths; it does not restate their behavior.
* Out of scope: S3 manifest reads (`plan_files`, the delete-mechanism gate — a separate object-store pool), the createVirtualSchema listing `RestCatalog` optimization, and the namespace-listing self-issued HTTP path (`list_namespace_tables`). Only the pushdown catalog-HTTP path is touched.
* The out-of-scope bullet above still holds after issue #204 relocates `list_namespace_tables` into the `lakehouse-catalog` crate: relocation is not session adoption. Namespace enumeration keeps its own `RestCatalog`/SigV4 branch and still builds no `CatalogSession`. Folding it onto the session is a separate change with its own auth-path merge.
* This delta carves the scan spec's `storage` value out of THREE behavior-preservation clauses of this feature, one per scenario: the byte-identical clause of "Single-table pushdown builds one catalog session and reuses it", the byte-identical join-SQL clause of "N-table join reuses one session across all legs", and the per-table storage-block clause of "Per-table loadTable GET is preserved so per-table vended credentials are returned". It supersedes no Background bullet and changes no session, auth, URL, or reuse rule.
* `vs-adapter/storage-backend-enum` (issue #274) wraps the scan spec's `storage` value in an externally-tagged backend variant whose payload is byte-identical to today's `StorageProps` encoding. That value is embedded in the scan-driving SQL, so this feature's "the per-shard scan-spec storage … MUST be byte-identical" clause names exactly the one value that changes.
* The carve-out permits an edit to the `storage` value ALONE. The catalog request URLs, the header set, the grant body, the grant and `/v1/config` COUNTS, the resolved file lists, and every other byte of the generated SQL stay unedited — that unchanged remainder is what keeps this feature's session-reuse gate falsifiable rather than retiring it.
* The vended-credential CONTENT guarantee is untouched: each table's own vended keys still land in that table's storage block, field for field. Only the block's wrapping layer changes, and `vs-adapter/pushdown-planning-cloud-credentials` owns the field-for-field gate.

## Scenarios

### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* a single-table pushdown request against an Iceberg REST catalog
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the adapter SHALL build exactly one `CatalogSession` — one HTTP client, one resolved catalog-auth strategy, and one resolved `/v1/config` prefix — before issuing the table's `loadTable` GET
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the request
* *AND* the table's `loadTable` GET SHALL be issued on that session's client using the session's auth and prefix, and the success-path catalog request URLs — the `/v1/config` prefix lookup and the `loadTable` GET — MUST be identical to the pre-refactor path for the same query
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value's variant tag, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant over a byte-identical payload

### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N tables whose legs share `catalog_uri` and `warehouse` and differ only by `CatalogProps.table`
* *WHEN* the join planner resolves every leg's file list
* *THEN* the planner SHALL build the `CatalogSession` once and pass the same session by shared reference to every per-leg resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL run once for the whole request, not once per leg
* *AND* the adapter SHALL issue exactly one `loadTable` GET per leg on the shared session
* *AND* the generated broadcast or N-scan join SQL MUST be byte-identical to the pre-refactor output EXCEPT for each leg's `storage` value's variant tag, and no assertion SHALL be weakened, disabled, or deleted to accommodate that tag

### Scenario: Per-table loadTable GET is preserved so per-table vended credentials are returned

* *GIVEN* a query resolving one or more tables through a session, with vended credentials enabled
* *WHEN* the adapter loads each table's metadata
* *THEN* the adapter SHALL issue a distinct `loadTable` GET per table on the shared session, carrying `X-Iceberg-Access-Delegation: vended-credentials` exactly as before
* *AND* the adapter SHALL extract each table's own vended storage credentials from that table's `loadTable` response and place them into that table's per-shard scan-spec storage block
* *AND* the storage block each table produces MUST be identical to the pre-refactor output for that table field for field, the ONLY permitted difference being the externally-tagged backend wrapper `vs-adapter/storage-backend-enum` adds around it

### Scenario: The per-table loader cannot re-derive auth or prefix

* *GIVEN* the refactored per-table `loadTable` loader
* *WHEN* it issues a table's `loadTable` GET
* *THEN* it SHALL take the catalog-auth strategy, the prefix, and the HTTP client from the `CatalogSession` passed by shared reference
* *AND* it MUST NOT run the OAuth2 grant, MUST NOT perform the `/v1/config` lookup, and MUST NOT construct a new HTTP client
* *AND* this constraint SHALL be enforced structurally — the loader receives the session by shared reference and holds no means to re-derive auth or prefix

### Scenario: createVirtualSchema resolves every table's schema on one shared session

* *GIVEN* a createVirtualSchema request over a namespace holding N Iceberg tables, whose schema loop previously called a `catalog_uri`-taking `resolve_table_schema` once per table and so built N sessions
* *WHEN* the adapter resolves the schema of every enumerated table
* *THEN* the adapter SHALL build exactly ONE `CatalogSession` for the whole enumeration and pass it by shared reference into every per-table schema resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup performed BY THE SCHEMA LOOP SHALL each run at most ONCE for the whole enumeration, not once per table, so an N-table namespace costs one schema-loop grant instead of N
* *AND* on the OAuth2 client-credentials mode — both `client_id` and `client_secret` set — the namespace-enumeration `RestCatalog` SHALL retain its own independent grant and `/v1/config` handshake, so such a request still performs TWO grants in total rather than one; on the static-token and no-auth modes it performs NO grant and one extra `/v1/config` lookup, and on the SigV4 mode `list_namespace_tables` builds no `RestCatalog` at all; folding namespace enumeration onto the session is out of scope
* *AND* the adapter SHALL still issue exactly one `loadTable` GET per table, and each table's resolved column list and Exasol type mapping MUST be identical to the per-session output for that table
* *AND* a table the catalog reports as not loadable SHALL still be skipped with the same warning and SHALL NOT abort the enumeration, so the non-Iceberg-table skip behavior is unchanged
* *AND* an OAuth2 grant failure SHALL surface once, before the loop, instead of once at the first table, because the session is now built ahead of the enumeration

### Scenario: CatalogSession is public and every file-resolution entry point takes one

* *GIVEN* `CatalogSession` declared `pub` in the `lakehouse-catalog` crate, and the `refactor-catalog-http-session` wrapper pair `resolve_file_list(catalog_uri: &str, …)` plus `resolve_file_list_with_session(&CatalogSession, …)`
* *WHEN* any caller — the single-table pushdown path, a join leg, the createVirtualSchema schema loop, or an external integration test — resolves a file list or a table schema
* *THEN* exactly ONE file-resolution function SHALL remain, named `resolve_file_list`, `pub`, taking `&CatalogSession` as its first parameter, and `resolve_file_list_with_session` SHALL be DELETED rather than kept as an alias
* *AND* `resolve_table_schema` SHALL likewise take `&CatalogSession` as its first parameter and SHALL NOT construct a session of its own
* *AND* NEITHER function SHALL retain a `catalog_uri: &str` parameter, because the session already carries the catalog URI and a second copy could disagree with it
* *AND* the caller SHALL validate every involved-table identifier BEFORE it builds the session, so a malformed identifier still issues ZERO catalog HTTP requests and returns the same parse error as before — the guarantee moves from inside `resolve_file_list` to its callers, and is not dropped
* *AND* `CatalogSession`'s fields MUST stay private and `CatalogAuth` MUST stay crate-private to `lakehouse-catalog`, so making the type public exposes no auth internals
* *AND* the external E2E callers (`tests/common/e2e_harness.rs`, `tests/e2e_scan_test.rs`) SHALL construct a `CatalogSession` themselves and pass it, which is the capability the crate extraction exists to grant; the file lists they resolve MUST be unchanged

### Scenario: Catalog secrets and the live session token never leak on the loadTable error path

* *GIVEN* a `CatalogSession` whose resolved auth is an OAuth2-obtained bearer token
* *WHEN* a `loadTable` GET issued on that session returns a transport, HTTP-status, or parse error
* *THEN* the returned error message MUST NOT contain any static catalog-auth secret from the CONNECTION
* *AND* the returned error message MUST NOT contain the live bearer token held in the session's auth strategy
* *AND* neither the static secrets nor the obtained token SHALL appear in any returned SQL string
</content>
