# Feature: REST Catalog Token & OAuth2 Authentication

Lets the Virtual Schema authenticate to an Iceberg REST Catalog that requires a bearer
token or an OAuth2 client-credentials exchange, in addition to the existing static-S3
credential model. Three catalog-auth modes are supported: (1) no auth (the default,
current behaviour); (2) a static bearer `token`, attached directly as the catalog's
bearer credential; and (3) OAuth2 client credentials (`client_id` + `client_secret`,
with optional `oauth2_server_uri` and `scope`), where the catalog performs the
client-credentials grant itself to obtain and refresh a token. Catalog authentication and
S3 storage credentials are orthogonal — any combination is valid. This auth path is
separate from, and mutually exclusive with, AWS SigV4 request signing. Catalog auth
secrets are consumed only in the planning layer and never cross the UDF boundary — and
after the `catalog` field is dropped, `ScanSpec` carries no catalog block at all.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/rest-catalog-oauth-auth/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Catalog auth props are never placed in any scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials, with `use_vended_credentials` either enabled or disabled
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* the `ScanSpec` SHALL carry no catalog identifier block at all — the scan UDF never contacts the catalog, so `ScanSpec` MUST NOT include catalog `uri`, `warehouse`, or `table` fields
* *AND* each `ScanSpec` storage block SHALL carry only storage material — inside the SEALED envelope, the vended STS credentials when the storage credential source is vending and they were resolved, or an assumed role's session credentials when it is an assumed role (`vs-adapter/connection-credentials-assume-role`); otherwise a REFERENCE to the CONNECTION that supplies the static credentials — exactly as in `vs-adapter/pushdown-planning-cloud-credentials` and `vs-adapter/scan-spec-credential-reference`
<!-- /DELTA:CHANGED -->
