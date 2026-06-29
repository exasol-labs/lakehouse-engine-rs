# Feature: Connection-Object Credential Source

Lets the Virtual Schema read its catalog endpoint and S3 credentials from a named
Exasol CONNECTION object instead of plain VS properties, so credentials are managed
and access-controlled by Exasol (never typed inline into `CREATE VIRTUAL SCHEMA`),
and so the engine can authenticate to a cloud Iceberg REST catalog (AWS Glue) and
its backing object storage. The credential set carried by the CONNECTION also selects
whether requests to the catalog are AWS SigV4-signed and whether the engine requests
short-lived vended S3 credentials at table-load time. The REST-catalog authentication
mode (none / static bearer token / OAuth2 client-credentials) is carried on the same
CONNECTION and specified by the sibling feature `connection-credentials-catalog-auth`.

## Background

The connection name is supplied as the VS property `CATALOG_CONNECTION`. The adapter
resolves it with `ctx.connection(name)`. The resolved `ConnectionObject.address` is the
catalog URI; the resolved `ConnectionObject.password` is a JSON object string carrying
the credential fields. The resolved password value MUST NEVER appear in any error
message, returned SQL, or log line. Both adapter entry points
(`createVirtualSchema`/`refreshVirtualSchema` and `pushdown`) resolve credentials through
this same path. `warehouse` is the only unconditionally-required field. Catalog
authentication and S3 storage credentials are fully orthogonal: any combination is valid,
including an unauthenticated catalog that vends S3 credentials and an OAuth-authenticated
catalog used with static S3 credentials (the catalog-auth modes themselves are specified in
`connection-credentials-catalog-auth`). The one conditional requirement is the AWS Glue SigV4
path: when `use_sigv4`
is true the static `access_key`, `secret_key`, and `region` are required (they sign the
catalog `load_table` request, ahead of any credential vending); `endpoint` stays optional.

## Scenarios

### Scenario: Adapter reads catalog and storage credentials from a CONNECTION object

* *GIVEN* a `CREATE VIRTUAL SCHEMA` that supplies `CATALOG_CONNECTION = '<conn_name>'`
* *AND* an Exasol CONNECTION named `<conn_name>` whose address is the Iceberg REST catalog URI and whose password is a JSON object holding `warehouse`, `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter handles a `createVirtualSchema` or `pushdown` request
* *THEN* the adapter SHALL call `ctx.connection('<conn_name>')` to obtain the credentials
* *AND* the adapter SHALL use the resolved address as the catalog URI and the parsed JSON password to build the catalog and storage configuration
* *AND* the adapter MUST NOT read `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN`, `CATALOG_URI`, `S3_ENDPOINT`, or `S3_REGION` from plain VS properties when `CATALOG_CONNECTION` is present

### Scenario: Missing connection name is rejected with a clear, credential-safe error

* *GIVEN* a VS request whose properties do not include a non-empty `CATALOG_CONNECTION`
* *WHEN* the adapter handles the request
* *THEN* the adapter SHALL return an error stating that `CATALOG_CONNECTION` is required
* *AND* the error message MUST NOT contain any credential value

### Scenario: Malformed connection password is rejected without leaking the password

* *GIVEN* a CONNECTION whose password is not a parseable JSON object
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating the CONNECTION password is not a valid JSON object
* *AND* the error message MUST NOT contain the password text

### Scenario: Connection password missing required credential fields is rejected listing only the field names

* *GIVEN* a CONNECTION whose JSON password omits `warehouse`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming `warehouse` as the missing required field
* *AND* the error message MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT report any of `endpoint`, `region`, `access_key`, or `secret_key` as missing, because those fields are optional

### Scenario: Static S3 credentials are optional regardless of catalog auth mode

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, does not enable `use_sigv4`, but omits `endpoint`, `region`, `access_key`, and `secret_key`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL accept the password without reporting any of `endpoint`, `region`, `access_key`, or `secret_key` as missing
* *AND* the adapter SHALL treat each omitted S3 field as absent, independently of whether any catalog-auth field or `use_vended_credentials` is set

### Scenario: When SigV4 is enabled, access_key, secret_key, and region are required

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true and supplies `warehouse` but omits one or more of `access_key`, `secret_key`, and `region`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error naming the missing field(s) and stating they are required when SigV4 signing is enabled
* *AND* the adapter SHALL apply this guard even when `use_vended_credentials` is true, because the static `access_key`, `secret_key`, and `region` sign the catalog `load_table` request before any vended credentials are used
* *AND* `endpoint` SHALL remain optional even when `use_sigv4` is true
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Optional credential fields default sensibly

* *GIVEN* a CONNECTION password that supplies `warehouse` but omits the optional `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `use_sigv4`, `use_vended_credentials`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` fields
* *WHEN* the adapter builds the storage and catalog configuration
* *THEN* the adapter SHALL treat `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` as absent
* *AND* the adapter SHALL default `use_sigv4` and `use_vended_credentials` to false so existing static-S3 MinIO/REST stacks behave exactly as before
* *AND* the adapter SHALL apply the supplied `path_style` value (defaulting to a value that preserves existing MinIO behaviour)
