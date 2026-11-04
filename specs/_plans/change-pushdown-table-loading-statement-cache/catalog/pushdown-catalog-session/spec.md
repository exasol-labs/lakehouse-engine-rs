<!-- DELTA:CHANGED -->
# Feature: Pushdown Catalog HTTP Session

The adapter authenticates to an Iceberg REST catalog and looks up its `/v1/config` prefix once per statement and virtual schema, and every `loadTable` GET of that statement's pushdown requests carries that token and that prefix over one connection pool. A statement over N tables therefore runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool. `file-planning/pushdown-format-neutral-resolution` states the statement scope.
<!-- /DELTA:CHANGED -->

## Background

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Single-table pushdown builds one catalog session and reuses it

* *GIVEN* the first pushdown request of a statement on a virtual schema over an Iceberg REST catalog, with one involved table
* *WHEN* the adapter resolves the file list for the involved table
* *THEN* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL each run at most once for the statement, before the table's `loadTable` GET
* *AND* the table's `loadTable` GET SHALL carry that grant's token and that prefix, and the success-path catalog request URLs, the `/v1/config` prefix lookup and the `loadTable` GET, MUST be the URLs that the same table yields as the only table of its statement
* *AND* the resolved file list, the per-shard scan-spec storage, and the scan-driving SQL MUST NOT depend on how many requests of the statement share the grant and the prefix, EXCEPT for the `storage` VALUE, which `storage-access/scan-spec-credential-reference` encodes as a connection reference carrying no credential or as a sealed envelope over the backend's encoding
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: N-table join reuses one session across all legs

* *GIVEN* an inner-join pushdown over N legs on a virtual schema over an Iceberg REST catalog, where two legs may name the same table, as a self-join does
* *WHEN* the adapter resolves every leg's file list
* *THEN* the OAuth2 client-credentials grant and the `/v1/config` prefix lookup SHALL run at most once for the whole request, not once per leg
* *AND* the adapter SHALL issue at most one `loadTable` GET per distinct table of the request, so a two-leg self-join issues one, and SHALL issue none for a table that an earlier pushdown of the statement loaded
* *AND* the generated broadcast or N-scan join SQL MUST NOT depend on how many legs share a table, EXCEPT for each leg's `storage` VALUE, which `storage-access/scan-spec-credential-reference` encodes per side
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The Lakekeeper permission check reuses the request's one catalog session

* *GIVEN* a single-table or an N-table join pushdown request under OAuth2 client-credentials catalog auth, on a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'` whose Lakekeeper answer allows every table
* *WHEN* the adapter checks permissions and resolves every table's file list
* *THEN* the OAuth2 grant and the `/v1/config` lookup SHALL each run at most once for the statement, as they do with the check off
* *AND* the batch-check SHALL carry the bearer token of that one grant
* *AND* a request that reads a table that no earlier pushdown of the statement checked SHALL add exactly one request, the batch-check, after the `/v1/config` lookup and before the first `loadTable` GET of those tables, and SHALL leave every other request of "Single-table pushdown builds one catalog session and reuses it" and "N-table join reuses one session across all legs" unchanged
* *AND* a request whose tables earlier pushdowns of the statement all checked SHALL add no request
<!-- /DELTA:CHANGED -->
