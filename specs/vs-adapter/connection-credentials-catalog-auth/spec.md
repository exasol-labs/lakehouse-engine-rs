# Feature: Catalog Authentication Credentials

Carries the REST-catalog authentication credentials on the resolved CONNECTION, beyond the
static-S3 storage credentials covered by `connection-credentials`. The Virtual Schema can reach
an Iceberg REST catalog in one of three mutually exclusive modes: no catalog authentication, a
static bearer `token`, or an OAuth2 client-credentials exchange (`client_id` + `client_secret`,
with optional `oauth2_server_uri` and `scope`). Catalog authentication is fully orthogonal to S3
storage credentials and to credential vending — an unauthenticated catalog may still vend S3
credentials, and an OAuth-authenticated catalog may be used with static S3 credentials.

## Background

The catalog-auth fields live on the same JSON CONNECTION password parsed by
`connection-credentials` and are exposed on the resolved credentials for the planning layer to
consume; they never cross the UDF boundary. Catalog authentication and AWS SigV4 request signing
are mutually exclusive strategies: SigV4 signs the `load_table` request with static AWS
credentials, whereas catalog token/OAuth authenticates to the REST catalog itself, so enabling
both is a configuration error. Every authentication value (`token`, `client_secret`) MUST NEVER
appear in any error message, returned SQL, or log line.

## Scenarios

### Scenario: Static bearer token is exposed on the resolved credentials

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse` and a non-empty `token`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `token` on the credentials
* *AND* the adapter SHALL treat `oauth2_server_uri` and `scope` as not applicable to the token mode
* *AND* the resolved `token` value MUST NOT appear in any error message

### Scenario: OAuth2 client credentials are exposed on the resolved credentials

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse`, a non-empty `client_id`, and a non-empty `client_secret`, and optionally `oauth2_server_uri` and `scope`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `client_id`, `client_secret`, and the optional `oauth2_server_uri` and `scope` on the credentials
* *AND* the adapter SHALL treat `oauth2_server_uri` and `scope` as optional, leaving them absent when not supplied
* *AND* the resolved `client_secret` value MUST NOT appear in any error message

### Scenario: Incomplete OAuth2 client credentials are rejected naming only the missing field

* *GIVEN* a CONNECTION whose JSON password supplies `warehouse` and `client_id` but omits `client_secret` (or supplies `client_secret` but omits `client_id`)
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that OAuth2 client credentials require both `client_id` and `client_secret` and naming the missing one
* *AND* the error message MUST NOT contain the supplied `client_id` or `client_secret` value

### Scenario: Catalog token/OAuth auth and SigV4 are mutually exclusive

* *GIVEN* a CONNECTION whose JSON password sets `use_sigv4` to true AND also supplies a catalog-auth field (`token`, or `client_id`/`client_secret`)
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that SigV4 signing and catalog token/OAuth authentication cannot both be enabled
* *AND* the error message MUST NOT contain any supplied credential value
