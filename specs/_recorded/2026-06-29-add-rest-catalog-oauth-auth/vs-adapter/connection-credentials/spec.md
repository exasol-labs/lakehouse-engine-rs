# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and S3 credentials from a named
Exasol CONNECTION object instead of plain VS properties, so credentials are managed
and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`),
and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue) and
its backing object storage. The credential set carried by the CONNECTION also selects
whether requests to the catalog are AWS SigV4-signed, whether the engine requests
short-lived vended S3 credentials at table-load time, and whether the catalog is
reached with a static bearer token or an OAuth2 client-credentials exchange.

## Background

The connection name is supplied as the VS property `CATALOG_CONNECTION`. The adapter
resolves it with `ctx.connection(name)`. The resolved `ConnectionObject.address` is the
catalog URI; the resolved `ConnectionObject.password` is a JSON object string carrying
the credential fields. The resolved password value MUST NEVER appear in any error
message, returned SQL, or log line. Both adapter entry points
(`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through
this same path. `warehouse` is the only unconditionally-required field. Catalog
authentication (none / static `token` / OAuth2 `client_id`+`client_secret`) and S3 storage
credentials are fully orthogonal: any combination is valid, including an unauthenticated
catalog that vends S3 credentials and an OAuth-authenticated catalog used with static S3
credentials. The one conditional requirement is the AWS Glue SigV4 path: when `use_sigv4`
is true the static `access_key`, `secret_key`, and `region` are required (they sign the
catalog `load_table` request, ahead of any credential vending); `endpoint` stays optional.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Connection password missing required credential fields is rejected listing only the field names

* *GIVEN* a CONNECTION whose JSON password omits `warehouse`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `warehouse` as the missing required field
* *AND* the error message MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT report any of `endpoint`, `region`, `access_key`, or `secret_key` as missing, because those fields are optional
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Static S3 credentials are optional regardless of catalog auth mode

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, does not enable `use_sigv4`, but omits `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL accept the password without reporting any of `endpoint`, `region`, `access_key`, or `secret_key` as missing
* *AND* the adapter SHALL treat each omitted S3 field as absent, independently of whether any catalog-auth field or `use_vended_credentials` is set
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: When SigV4 is enabled, access_key, secret_key, and region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse` but omits one or more of `access_key`, `secret_key`, and `region`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming the missing field(s) and stating they are required when SigV4 signing is enabled
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the static `access_key`, `secret_key`, and `region` sign the catalog `load_table` request before any vended credentials are used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Static bearer token is exposed on the resolved credentials

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse` and a non-empty `token`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `token` on the credentials
* *AND* the adapter SHALL treat `oauth2_server_uri` and `scope` as not applicable to the token mode
* *AND* the resolved `token` value MUST NOT appear in any error message
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: OAuth2 client credentials are exposed on the resolved credentials

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse`, a non-empty `client_id`, and a non-empty `client_secret`, and optionally `oauth2_server_uri` and `scope`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `client_id`, `client_secret`, and the optional `oauth2_server_uri` and `scope` on the credentials
* *AND* the adapter SHALL treat `oauth2_server_uri` and `scope` as optional, leaving them absent when not supplied
* *AND* the resolved `client_secret` value MUST NOT appear in any error message
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Incomplete OAuth2 client credentials are rejected naming only the missing field

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse` and `client_id` but omits `client_secret` (or supplies `client_secret` but omits `client_id`)
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that OAuth2 client credentials require both `client_id` and `client_secret` and naming the missing one
* *AND* the error message MUST NOT contain the supplied `client_id` or `client_secret` value
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Catalog token/OAuth auth and SigV4 are mutually exclusive

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true AND also supplies a catalog-auth field (`token`, or `client_id`/`client_secret`)
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that SigV4 signing and catalog token/OAuth authentication cannot both be enabled
* *AND* the error message MUST NOT contain any supplied credential value
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 MinIO/REST stacks behave exactly as before
* *AND* the adapter SHALL apply the supplied `path_style` value (defaulting to a value that preserves existing MinIO behaviour)
<!-- /DELTA:CHANGED -->
