# Feature: REST Catalog Token & OAuth2 Authentication

Lets the Virtual Schema authenticate to an Iceberg REST Catalog that requires a bearer token or an OAuth2 client-credentials exchange, in addition to the static-S3 credential model. Catalog authentication and S3 storage credentials are orthogonal. Catalog auth secrets are consumed only in the planning layer and never cross the UDF boundary — and after the `catalog` field is dropped, `ScanSpec` carries no catalog block at all.

## Background

* Catalog auth props (`token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`) are consumed ONLY in the planning layer. They are NOT carried in `ScanSpec` and MUST NOT cross the UDF boundary; the scan UDF works from pre-resolved file paths and never calls the catalog.
* `ScanSpec` carries no catalog identifier block (`uri`/`warehouse`/`table`) — those fields were dropped as dead weight the scan UDF never reads.
* Token, `client_secret`, and obtained OAuth2 bearer token values MUST NEVER appear in any returned SQL string, error message, or log line.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Catalog auth props are never placed in any scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials, with `use_vended_credentials` either enabled or disabled
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* the `ScanSpec` SHALL carry no catalog identifier block at all — the scan UDF never contacts the catalog, so `ScanSpec` MUST NOT include catalog `uri`, `warehouse`, or `table` fields
* *AND* each `ScanSpec` storage block SHALL carry only the S3 storage credentials — the vended STS credentials when `use_vended_credentials` is enabled and they were resolved, otherwise the static credentials — exactly as in `vs-adapter/pushdown-planning-cloud-credentials`
<!-- /DELTA:CHANGED -->
