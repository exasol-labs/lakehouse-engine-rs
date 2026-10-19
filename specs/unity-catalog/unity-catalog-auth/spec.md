# Feature: Unity Catalog Authentication

Resolves the authentication strategy a `UnityCatalogSession` applies to every Unity Catalog REST request. The modes are a static bearer token (a Databricks personal access token), Databricks OAuth machine-to-machine with a cached and refreshed token, and no authentication for an OSS Unity Catalog whose authentication is disabled. Every mode ends in an `Authorization: Bearer` header or no header.

## Background

* The mode is selected from the resolved CONNECTION credentials by the one shared classifier of `connection/connection-credentials-catalog-auth`. A non-empty `token` selects the personal-access-token mode, a `client_id` plus `client_secret` selects the OAuth machine-to-machine mode, and neither selects the unauthenticated mode. Credential validation rejects a CONNECTION that supplies both.
* The OAuth mode posts `grant_type=client_credentials&scope=all-apis` with HTTP Basic `client_id:client_secret` to the token endpoint: `oauth2_server_uri` when supplied, otherwise `{host}/oidc/v1/token` derived from the CONNECTION address. It reads `access_token` and `expires_in` (3600 seconds on Databricks, no refresh token).

## Scenarios

### Scenario: A personal access token is applied as the bearer verbatim

* *GIVEN* resolved Unity Catalog CONNECTION credentials that supply a non-empty `token` and no OAuth client credentials — the only shape credential validation admits for the personal-access-token mode
* *WHEN* the session resolves its authentication strategy and issues a Unity Catalog request
* *THEN* the strategy SHALL set the request's `Authorization` header to `Bearer` followed by the supplied `token` verbatim
* *AND* the strategy MUST NOT perform any token exchange, because a personal access token is already a bearer credential
* *AND* the session SHALL select this mode through the ONE shared catalog-auth mode classifier `connection/connection-credentials-catalog-auth` specifies, and MUST NOT test the `token` ahead of the `client_id`/`client_secret` pair, because that ordering answered an ambiguous CONNECTION the opposite way from the Iceberg REST path
* *AND* the supplied `token` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: OAuth machine-to-machine mints a bearer token via the client-credentials grant

* *GIVEN* resolved Unity Catalog CONNECTION credentials that supply `client_id` and `client_secret` and no static `token` — the only shape credential validation admits for the OAuth machine-to-machine mode
* *WHEN* the session resolves its authentication strategy for the first request
* *THEN* the strategy SHALL POST `grant_type=client_credentials&scope=all-apis` with HTTP Basic authentication carrying `client_id` and `client_secret` to the token endpoint, defaulting the endpoint to `{host}/oidc/v1/token` when `oauth2_server_uri` is absent and the scope to `all-apis` when `scope` is absent
* *AND* the strategy SHALL read the returned `access_token` and apply it as the request's bearer token
* *AND* the session SHALL select this mode through that same shared classifier, and the classifier SHALL carry only the three mutually exclusive auth fields — the `oauth2_server_uri` and `scope` defaults SHALL stay owned by this feature, because the Unity endpoint default derives from the CONNECTION address while the Iceberg REST default leaves the property unset for the catalog to fill
* *AND* the `client_secret`, the Basic authentication header, and the minted `access_token` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: A minted OAuth token is cached and refreshed before expiry rather than re-minted per request

* *GIVEN* an OAuth machine-to-machine strategy that has minted an `access_token` with a positive `expires_in`
* *WHEN* the session issues further Unity Catalog requests before the token's expiry
* *THEN* the strategy SHALL reuse the cached `access_token` and MUST NOT perform a second client-credentials grant while the cached token is still valid
* *AND* the strategy SHALL mint a fresh token once the cached token has reached or passed its refresh point, because the grant returns no refresh token and an expired bearer would fail every subsequent request

### Scenario: The unauthenticated mode sends no Authorization header

* *GIVEN* resolved Unity Catalog CONNECTION credentials that supply neither a `token` nor OAuth client credentials
* *WHEN* the session resolves its authentication strategy and issues a Unity Catalog request
* *THEN* the strategy SHALL issue the request with no `Authorization` header, so an OSS Unity Catalog whose authentication is disabled accepts it
* *AND* the strategy MUST NOT invent a placeholder credential, because a placeholder bearer against an auth-disabled server is unnecessary and against an auth-enabled server is misleading

### Scenario: A failed OAuth grant surfaces a clear, credential-safe error

* *GIVEN* an OAuth machine-to-machine strategy whose client-credentials grant returns a non-success status or an unparseable body
* *WHEN* the session attempts to mint a token
* *THEN* the strategy SHALL return an error stating that the Unity Catalog OAuth client-credentials grant failed
* *AND* the error message MUST NOT contain the `client_secret`, the Basic authentication header, or any partial token material
* *AND* the strategy SHALL return the failure as an error value rather than panicking
