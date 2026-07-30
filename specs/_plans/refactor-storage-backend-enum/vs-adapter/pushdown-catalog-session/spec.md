# Feature: Pushdown Catalog HTTP Session

Builds the catalog HTTP state — one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix — once per pushdown request and reuses it across every table's `loadTable` GET, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool instead of N of each.

## Background

<!-- DELTA:NEW -->
* This delta carves the scan spec's `storage` value out of THREE behavior-preservation clauses of this feature, one per scenario: the byte-identical clause of "Single-table pushdown builds one catalog session and reuses it", the byte-identical join-SQL clause of "N-table join reuses one session across all legs", and the per-table storage-block clause of "Per-table loadTable GET is preserved so per-table vended credentials are returned". It supersedes no Background bullet and changes no session, auth, URL, or reuse rule.
* `vs-adapter/storage-backend-enum` (issue #274) wraps the scan spec's `storage` value in an externally-tagged backend variant whose payload is byte-identical to today's `StorageProps` encoding. That value is embedded in the scan-driving SQL, so this feature's "the per-shard scan-spec storage … MUST be byte-identical" clause names exactly the one value that changes.
* The carve-out permits an edit to the `storage` value ALONE. The catalog request URLs, the header set, the grant body, the grant and `/v1/config` COUNTS, the resolved file lists, and every other byte of the generated SQL stay unedited — that unchanged remainder is what keeps this feature's session-reuse gate falsifiable rather than retiring it.
* The vended-credential CONTENT guarantee is untouched: each table's own vended keys still land in that table's storage block, field for field. Only the block's wrapping layer changes, and `vs-adapter/pushdown-planning-cloud-credentials` owns the field-for-field gate.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* a single-table pushdown request against an Iceberg REST catalog
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the adapter SHALL build exactly one `CatalogSession` — one HTTP client, one resolved catalog-auth strategy, and one resolved `/v1/config` prefix — before issuing the table's `loadTable` GET
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the request
* *AND* the table's `loadTable` GET SHALL be issued on that session's client using the session's auth and prefix, and the success-path catalog request URLs — the `/v1/config` prefix lookup and the `loadTable` GET — MUST be identical to the pre-refactor path for the same query
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value's variant tag, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant over a byte-identical payload
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N tables whose legs share `catalog_uri` and `warehouse` and differ only by `CatalogProps.table`
* *WHEN* the join planner resolves every leg's file list
* *THEN* the planner SHALL build the `CatalogSession` once and pass the same session by shared reference to every per-leg resolution
* *AND* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL run once for the whole request, not once per leg
* *AND* the adapter SHALL issue exactly one `loadTable` GET per leg on the shared session
* *AND* the generated broadcast or N-scan join SQL MUST be byte-identical to the pre-refactor output EXCEPT for each leg's `storage` value's variant tag, and no assertion SHALL be weakened, disabled, or deleted to accommodate that tag
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Per-table loadTable GET is preserved so per-table vended credentials are returned

* *GIVEN* a query resolving one or more tables through a session, with vended credentials enabled
* *WHEN* the adapter loads each table's metadata
* *THEN* the adapter SHALL issue a distinct `loadTable` GET per table on the shared session, carrying `X-Iceberg-Access-Delegation: vended-credentials` exactly as before
* *AND* the adapter SHALL extract each table's own vended storage credentials from that table's `loadTable` response and place them into that table's per-shard scan-spec storage block
* *AND* the storage block each table produces MUST be identical to the pre-refactor output for that table field for field, the ONLY permitted difference being the externally-tagged backend wrapper `vs-adapter/storage-backend-enum` adds around it
<!-- /DELTA:CHANGED -->
