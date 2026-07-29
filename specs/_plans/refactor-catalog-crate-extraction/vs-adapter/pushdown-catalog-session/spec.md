# Feature: Pushdown Catalog HTTP Session

Builds the catalog HTTP state — one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix — once per pushdown request and reuses it across every table's `loadTable` GET, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool instead of N of each. The per-table `loadTable` GET stays per-table because each response carries that table's own vended storage credentials. This is pure connection and session reuse: the URLs, catalog auth, resolved file lists, and generated SQL are identical to the pre-refactor path.

## Background

* The catalog-auth token and the `/v1/config` prefix are CATALOG-scoped, not table-scoped, so both are correctly computed once per query. The Apache Iceberg REST Catalog spec confirms this: `GET /v1/config` is the route "All REST clients should first call ... to get catalog configuration properties from the server to configure the catalog and its HTTP client", keyed by the `warehouse` parameter; the OAuth2 client-credentials grant returns a catalog bearer token; and only the `loadTable` response carries "credentials that should be used for subsequent requests for the table" — i.e. per-table vended `storage_credentials`.
* A `CatalogSession` is valid for exactly one `(catalog_uri, warehouse)` tuple. Across a single pushdown request — single-table OR N-table join — `catalog_uri` and `warehouse` are constant; join legs override only `CatalogProps.table`. The per-table variation is namespace and table only, entering solely at `build_load_table_url` inside the per-table `loadTable` GET.
* Catalog-auth secrets are consumed only in the planning layer and never cross the UDF boundary. The session holds the live OAuth2-obtained bearer token in its auth strategy, so redaction at the `loadTable`-GET error site MUST still strip both the static catalog-auth secrets and that live token.
* Table-identifier validation runs before the session's `/v1/config` lookup. A malformed involved-table identifier is rejected at the pushdown seam before `CatalogSession::resolve`, so a malformed-table request issues no catalog HTTP request and returns the same parse error as before. This preserves the issue's identical-URLs intent: the catalog request set on the success path is byte-identical, and the error path is never larger than before.
* Related features stay accurate and unedited: `vs-adapter/pushdown-planning-cloud-credentials` (SigV4 signing, vended-credential extraction, Glue prefix derivation), `vs-adapter/rest-catalog-oauth-auth` (the three catalog-auth modes), `vs-adapter/connection-credentials-catalog-auth` (secrets never leak), and `vs-adapter/pushdown-planning` (file list resolved once). This feature adds the once-per-query session-reuse invariant that spans the single-table and join paths; it does not restate their behavior.
* Out of scope: S3 manifest reads (`plan_files`, the delete-mechanism gate — a separate object-store pool), the createVirtualSchema listing `RestCatalog` optimization, and the namespace-listing self-issued HTTP path (`list_namespace_tables`). Only the pushdown catalog-HTTP path is touched.
* The out-of-scope bullet above still holds after issue #204 relocates `list_namespace_tables` into the `lakehouse-catalog` crate: relocation is not session adoption. Namespace enumeration keeps its own `RestCatalog`/SigV4 branch and still builds no `CatalogSession`. Folding it onto the session is a separate change with its own auth-path merge.
* This delta SUPERSEDES the preceding Background bullet "`CatalogSession` is an internal planning-layer type. It is NOT re-exported on the `crate::adapter::pushdown::<name>` public façade frozen by `vs-adapter/pushdown-module-structure`." `CatalogSession` is now a `pub` type of the `lakehouse-catalog` crate (issue #204), reachable as `lakehouse_catalog::CatalogSession`. It is still NOT re-exported on the pushdown façade, but for a different reason: external callers name it on the crate that declares it, so a second path would be a redundant alias. `vs-adapter/catalog-crate-structure` owns the crate boundary; `vs-adapter/pushdown-module-structure` owns the redrawn façade.
* This delta SUPERSEDES the preceding Background bullet "The public `resolve_file_list` entry point keeps its pre-refactor signature (`catalog_uri: &str`) because external integration-test crates call it and cannot construct the `pub(crate)` `CatalogSession`. It builds a single-use session internally and delegates to a `pub(crate)` session-taking core, `resolve_file_list_with_session`, which the join legs call with one shared session. Session reuse spans the join legs through that core, not through the public entry point." The wrapper existed ONLY because `CatalogSession` was `pub(crate)`. With the type genuinely `pub`, `resolve_file_list` takes `&CatalogSession` directly and `resolve_file_list_with_session` is deleted; one function serves the single-table path, every join leg, and the external E2E callers.
* Retiring the wrapper MOVES the parse-before-config guarantee rather than dropping it. `resolve_file_list` no longer builds the session, so it can no longer be the place that validates the identifier first. Both single-table and join callers now validate every involved-table identifier at the `handle_pushdown` seam before `CatalogSession::resolve`, which is where the join path already did it.
* The createVirtualSchema table loop is the one remaining per-table session build, and the crate extraction is what makes fixing it a signature change rather than a new public type. Its per-table cost was accepted in `refactor-catalog-http-session` (#185) only because `resolve_table_schema` could not name a `pub(crate)` type in its public signature.
* The schema loop is NOT the whole request. `adapter/mod.rs:246` calls `list_namespace_tables` before the loop, and on the non-SigV4 path that builds a `RestCatalog` whose `iceberg-catalog-rest` 0.10.0 client runs its own `client_credentials` exchange (`client.rs:123`) and its own `/v1/config` handshake (`catalog.rs:430`), driven by the `credential`, `oauth2-server-uri`, and `scope` props `inject_catalog_auth_props` sets. That handshake is independent of any `CatalogSession` and is unchanged by this delta. A createVirtualSchema request therefore goes from N+1 grants to 2, never to 1, and any scenario clause about grant counts binds only the schema loop.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: createVirtualSchema builds a single-use session inline

* *GIVEN* a createVirtualSchema schema resolution for one table
* *WHEN* the adapter resolves that table's schema
* *THEN* the adapter SHALL build a single-use `CatalogSession` inline and load the table's metadata on it — one OAuth2 grant, one `/v1/config` lookup, and one `loadTable` GET, matching the pre-refactor cost with no regression
* *AND* the public signature of the schema-resolution entry point MUST be unchanged, so its external createVirtualSchema caller compiles without edits
<!-- /DELTA:REMOVED -->

The scenario above is reproduced verbatim from the recorded library so the removal target is unambiguous. It is REMOVED rather than amended because both of its clauses are now false: the schema-resolution entry point's signature DOES change, and the session is no longer built per table. The scenario "createVirtualSchema resolves every table's schema on one shared session" below replaces it.

<!-- DELTA:REMOVED -->
### Scenario: CatalogSession stays off the frozen public pushdown façade

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` public-surface baseline captured by the reachability probe
* *WHEN* `CatalogSession` is introduced and threaded through the `pub(crate)` file-resolution core and the join-planning functions
* *THEN* `CatalogSession` MUST NOT be re-exported at `crate::adapter::pushdown::CatalogSession`
* *AND* the reachability probe MUST compile unchanged, so the re-extracted `name → visibility` set diffs empty against the baseline — no item added, removed, narrowed, or widened
* *AND* the public file-resolution entry points (`resolve_file_list`, `resolve_table_schema`) MUST keep the same names, external visibility, AND signatures they held before, so their external callers compile unedited
* *AND* the shared session MUST be carried by a new internal `pub(crate)` core (`resolve_file_list_with_session`) and threaded through the internal join-planning functions, whose parameter lists change
<!-- /DELTA:REMOVED -->

The scenario above is reproduced verbatim from the recorded library so the removal target is unambiguous. Its "probe compiles unchanged" and "signatures unchanged" clauses are the exact constraints issue #204 exists to lift, so it is REMOVED rather than amended. The scenario "CatalogSession is public and every file-resolution entry point takes one" below replaces it; `vs-adapter/pushdown-module-structure` owns the redrawn façade baseline.

<!-- DELTA:NEW -->
### Scenario: CatalogSession is public and every file-resolution entry point takes one

* *GIVEN* `CatalogSession` declared `pub` in the `lakehouse-catalog` crate, and the `refactor-catalog-http-session` wrapper pair `resolve_file_list(catalog_uri: &str, …)` plus `resolve_file_list_with_session(&CatalogSession, …)`
* *WHEN* any caller — the single-table pushdown path, a join leg, the createVirtualSchema schema loop, or an external integration test — resolves a file list or a table schema
* *THEN* exactly ONE file-resolution function SHALL remain, named `resolve_file_list`, `pub`, taking `&CatalogSession` as its first parameter, and `resolve_file_list_with_session` SHALL be DELETED rather than kept as an alias
* *AND* `resolve_table_schema` SHALL likewise take `&CatalogSession` as its first parameter and SHALL NOT construct a session of its own
* *AND* NEITHER function SHALL retain a `catalog_uri: &str` parameter, because the session already carries the catalog URI and a second copy could disagree with it
* *AND* the caller SHALL validate every involved-table identifier BEFORE it builds the session, so a malformed identifier still issues ZERO catalog HTTP requests and returns the same parse error as before — the guarantee moves from inside `resolve_file_list` to its callers, and is not dropped
* *AND* `CatalogSession`'s fields MUST stay private and `CatalogAuth` MUST stay crate-private to `lakehouse-catalog`, so making the type public exposes no auth internals
* *AND* the external E2E callers (`tests/common/e2e_harness.rs`, `tests/e2e_scan_test.rs`) SHALL construct a `CatalogSession` themselves and pass it, which is the capability the crate extraction exists to grant; the file lists they resolve MUST be unchanged
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: createVirtualSchema resolves every table's schema on one shared session

* *GIVEN* a createVirtualSchema request over a namespace holding N Iceberg tables, whose schema loop previously called a `catalog_uri`-taking `resolve_table_schema` once per table and so built N sessions
* *WHEN* the adapter resolves the schema of every enumerated table
* *THEN* the adapter SHALL build exactly ONE `CatalogSession` for the whole enumeration and pass it by shared reference into every per-table schema resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup performed BY THE SCHEMA LOOP SHALL each run at most ONCE for the whole enumeration, not once per table, so an N-table namespace costs one schema-loop grant instead of N
* *AND* the namespace-enumeration `RestCatalog` SHALL retain its own independent catalog-auth handshake, so a createVirtualSchema request still performs TWO grants and TWO `/v1/config` lookups in total rather than one of each; folding namespace enumeration onto the session is out of scope
* *AND* the adapter SHALL still issue exactly one `loadTable` GET per table, and each table's resolved column list and Exasol type mapping MUST be identical to the per-session output for that table
* *AND* a table the catalog reports as not loadable SHALL still be skipped with the same warning and SHALL NOT abort the enumeration, so the non-Iceberg-table skip behavior is unchanged
* *AND* an OAuth2 grant failure SHALL surface once, before the loop, instead of once at the first table, because the session is now built ahead of the enumeration
<!-- /DELTA:NEW -->
