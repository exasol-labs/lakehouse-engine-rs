# Feature: Pushdown Catalog HTTP Session

Builds the catalog HTTP state — one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix — once per pushdown request and reuses it across every table's `loadTable` GET, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool instead of N of each. The per-table `loadTable` GET stays per-table because each response carries that table's own vended storage credentials. This is pure connection and session reuse: the URLs, catalog auth, resolved file lists, and generated SQL are identical to the pre-refactor path.

## Background

* The catalog-auth token and the `/v1/config` prefix are CATALOG-scoped, not table-scoped, so both are correctly computed once per query. The Apache Iceberg REST Catalog spec confirms this: `GET /v1/config` is the route "All REST clients should first call ... to get catalog configuration properties from the server to configure the catalog and its HTTP client", keyed by the `warehouse` parameter; the OAuth2 client-credentials grant returns a catalog bearer token; and only the `loadTable` response carries "credentials that should be used for subsequent requests for the table" — i.e. per-table vended `storage_credentials`.
* A `CatalogSession` is valid for exactly one `(catalog_uri, warehouse)` tuple. Across a single pushdown request — single-table OR N-table join — `catalog_uri` and `warehouse` are constant; join legs override only `CatalogProps.table`. The per-table variation is namespace and table only, entering solely at `build_load_table_url` inside the per-table `loadTable` GET.
* Catalog-auth secrets are consumed only in the planning layer and never cross the UDF boundary. The session holds the live OAuth2-obtained bearer token in its auth strategy, so redaction at the `loadTable`-GET error site MUST still strip both the static catalog-auth secrets and that live token.
* Table-identifier validation runs before the session's `/v1/config` lookup. A malformed involved-table identifier is rejected at the pushdown seam before `CatalogSession::resolve`, so a malformed-table request issues no catalog HTTP request and returns the same parse error as before. This preserves the issue's identical-URLs intent: the catalog request set on the success path is byte-identical, and the error path is never larger than before.
* `CatalogSession` is an internal planning-layer type. It is NOT re-exported on the `crate::adapter::pushdown::<name>` public façade frozen by `vs-adapter/pushdown-module-structure`.
* The public `resolve_file_list` entry point keeps its pre-refactor signature (`catalog_uri: &str`) because external integration-test crates call it and cannot construct the `pub(crate)` `CatalogSession`. It builds a single-use session internally and delegates to a `pub(crate)` session-taking core, `resolve_file_list_with_session`, which the join legs call with one shared session. Session reuse spans the join legs through that core, not through the public entry point.
* Related features stay accurate and unedited: `vs-adapter/pushdown-planning-cloud-credentials` (SigV4 signing, vended-credential extraction, Glue prefix derivation), `vs-adapter/rest-catalog-oauth-auth` (the three catalog-auth modes), `vs-adapter/connection-credentials-catalog-auth` (secrets never leak), and `vs-adapter/pushdown-planning` (file list resolved once). This feature adds the once-per-query session-reuse invariant that spans the single-table and join paths; it does not restate their behavior.
* Out of scope: S3 manifest reads (`plan_files`, the delete-mechanism gate — a separate object-store pool), the createVirtualSchema listing `RestCatalog` optimization, and the namespace-listing self-issued HTTP path (`list_namespace_tables`). Only the pushdown catalog-HTTP path is touched.

## Scenarios

### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* a single-table pushdown request against an Iceberg REST catalog
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the adapter SHALL build exactly one `CatalogSession` — one HTTP client, one resolved catalog-auth strategy, and one resolved `/v1/config` prefix — before issuing the table's `loadTable` GET
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the request
* *AND* the table's `loadTable` GET SHALL be issued on that session's client using the session's auth and prefix, and the success-path catalog request URLs — the `/v1/config` prefix lookup and the `loadTable` GET — MUST be identical to the pre-refactor path for the same query
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST be byte-identical to the pre-refactor output

### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N tables whose legs share `catalog_uri` and `warehouse` and differ only by `CatalogProps.table`
* *WHEN* the join planner resolves every leg's file list
* *THEN* the planner SHALL build the `CatalogSession` once and pass the same session by shared reference to every per-leg resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL run once for the whole request, not once per leg
* *AND* the adapter SHALL issue exactly one `loadTable` GET per leg on the shared session
* *AND* the generated broadcast or N-scan join SQL MUST be byte-identical to the pre-refactor output

### Scenario: Per-table loadTable GET is preserved so per-table vended credentials are returned

* *GIVEN* a query resolving one or more tables through a session, with vended credentials enabled
* *WHEN* the adapter loads each table's metadata
* *THEN* the adapter SHALL issue a distinct `loadTable` GET per table on the shared session, carrying `X-Iceberg-Access-Delegation: vended-credentials` exactly as before
* *AND* the adapter SHALL extract each table's own vended storage credentials from that table's `loadTable` response and place them into that table's per-shard scan-spec storage block
* *AND* the storage block each table produces MUST be identical to the pre-refactor output for that table

### Scenario: The per-table loader cannot re-derive auth or prefix

* *GIVEN* the refactored per-table `loadTable` loader
* *WHEN* it issues a table's `loadTable` GET
* *THEN* it SHALL take the catalog-auth strategy, the prefix, and the HTTP client from the `CatalogSession` passed by shared reference
* *AND* it MUST NOT run the OAuth2 grant, MUST NOT perform the `/v1/config` lookup, and MUST NOT construct a new HTTP client
* *AND* this constraint SHALL be enforced structurally — the loader receives the session by shared reference and holds no means to re-derive auth or prefix

### Scenario: createVirtualSchema builds a single-use session inline

* *GIVEN* a createVirtualSchema schema resolution for one table
* *WHEN* the adapter resolves that table's schema
* *THEN* the adapter SHALL build a single-use `CatalogSession` inline and load the table's metadata on it — one OAuth2 grant, one `/v1/config` lookup, and one `loadTable` GET, matching the pre-refactor cost with no regression
* *AND* the public signature of the schema-resolution entry point MUST be unchanged, so its external createVirtualSchema caller compiles without edits

### Scenario: Catalog secrets and the live session token never leak on the loadTable error path

* *GIVEN* a `CatalogSession` whose resolved auth is an OAuth2-obtained bearer token
* *WHEN* a `loadTable` GET issued on that session returns a transport, HTTP-status, or parse error
* *THEN* the returned error message MUST NOT contain any static catalog-auth secret from the CONNECTION
* *AND* the returned error message MUST NOT contain the live bearer token held in the session's auth strategy
* *AND* neither the static secrets nor the obtained token SHALL appear in any returned SQL string

### Scenario: CatalogSession stays off the frozen public pushdown façade

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` public-surface baseline captured by the reachability probe
* *WHEN* `CatalogSession` is introduced and threaded through the `pub(crate)` file-resolution core and the join-planning functions
* *THEN* `CatalogSession` MUST NOT be re-exported at `crate::adapter::pushdown::CatalogSession`
* *AND* the reachability probe MUST compile unchanged, so the re-extracted `name → visibility` set diffs empty against the baseline — no item added, removed, narrowed, or widened
* *AND* the public file-resolution entry points (`resolve_file_list`, `resolve_table_schema`) MUST keep the same names, external visibility, AND signatures they held before, so their external callers compile unedited
* *AND* the shared session MUST be carried by a new internal `pub(crate)` core (`resolve_file_list_with_session`) and threaded through the internal join-planning functions, whose parameter lists change
