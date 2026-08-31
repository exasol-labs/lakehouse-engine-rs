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

* **This delta is issue #135. It amends ONE scenario and changes no authentication rule.** The static bearer token path, the OAuth2 client-credentials grant, the no-auth path, the multi-warehouse base-path resolution, and the prohibition on catalog-auth props in any scan spec are all UNCHANGED.
* **SUPERSEDES the recorded clause "each `ScanSpec` storage block SHALL carry only the S3 storage credentials".** A storage block now carries EITHER a reference to the CONNECTION that supplies the static credentials, or the vended credentials inline — specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES.
* **The catalog-auth guarantee this feature owns is STRENGTHENED rather than weakened.** `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` still reach no `ScanSpec` field, and the scan UDF now reads the same CONNECTION password on the vending-disabled path yet cannot construct any of them, because it deserializes a storage-only nine-field projection — see `vs-adapter/connection-credentials-catalog-auth`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Catalog auth props are never placed in any scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials, with `use_vended_credentials` either enabled or disabled
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* the `ScanSpec` SHALL carry no catalog identifier block at all — the scan UDF never contacts the catalog, so `ScanSpec` MUST NOT include catalog `uri`, `warehouse`, or `table` fields
* *AND* each `ScanSpec` storage block SHALL carry only storage material — the vended STS credentials INLINE when `use_vended_credentials` is enabled and they were resolved, otherwise a REFERENCE to the CONNECTION that supplies the static credentials — exactly as in `vs-adapter/pushdown-planning-cloud-credentials` and `vs-adapter/scan-spec-credential-reference`, SUPERSEDING the recorded clause that required the static credentials themselves
<!-- /DELTA:CHANGED -->
