<!-- DELTA:CHANGED -->
# Feature: Pushdown Catalog HTTP Session

Builds the catalog HTTP state — one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix — once per pushdown request and reuses it across every table's `loadTable` GET, so an N-table join runs one OAuth2 grant, one `/v1/config` lookup, and one connection pool instead of N of each. The per-table `loadTable` GET stays per-table because each response carries that table's own vended storage credentials. This is pure connection and session reuse: the URLs, catalog auth, resolved file lists, and generated SQL are identical to the pre-refactor path. When a virtual schema sets `PERMISSION_CHECK = 'LAKEKEEPER'`, the same session also carries the request's one Lakekeeper permission check (`vs-adapter/lakekeeper-permission-check`), which is the only request the check adds.
<!-- /DELTA:CHANGED -->

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-catalog-session/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The Lakekeeper permission check reuses the request's one catalog session

* *GIVEN* a single-table or an N-table join pushdown request under OAuth2 client-credentials catalog auth, on a virtual schema with `PERMISSION_CHECK = 'LAKEKEEPER'` whose Lakekeeper answer allows every table
* *WHEN* the adapter checks permissions and resolves every table's file list
* *THEN* the OAuth2 grant and the `/v1/config` lookup SHALL each run exactly once for the request, as they do with the check off
* *AND* the batch-check SHALL go out on the session's HTTP client with the session's bearer token, and the adapter MUST NOT build a second HTTP client, run a second grant, or perform a second `/v1/config` lookup
* *AND* the check SHALL add exactly one request, the batch-check, between the `/v1/config` lookup and the first `loadTable` GET, and SHALL leave every other request of "Single-table pushdown builds one catalog session and reuses it" and "N-table join reuses one session across all legs" unchanged
<!-- /DELTA:NEW -->
