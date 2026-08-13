# Feature: Unity Catalog Authentication

Resolves the authentication strategy a `UnityCatalogSession` applies to every Unity Catalog REST request. Three modes are supported: a static bearer token (a Databricks personal access token) passed straight through; Databricks OAuth machine-to-machine, where the client performs an OIDC client-credentials grant to mint a short-lived bearer token and refreshes it before expiry; and an unauthenticated mode for OSS Unity Catalog whose local default has authentication disabled. Every mode terminates in an `Authorization: Bearer` header or no header, so token-versus-OAuth is not a request-construction difference — only the token's origin and lifecycle differ.

## Background

* **This delta (issue #331) removes the personal-access-token-wins PRECEDENCE this feature carried, and adds no behaviour for any CONNECTION the adapter accepts.** `resolve_unity_auth` (`crates/lakehouse-catalog/src/unity/auth.rs`) tested a non-empty `token` BEFORE the `client_id`/`client_secret` pair, so a CONNECTION supplying both silently took the personal-access-token mode — while the Iceberg REST path took the OAuth2 mode, the opposite answer for the same input. Credential validation now rejects that CONNECTION outright (`vs-adapter/connection-credentials-catalog-auth`), and `resolve_unity_auth` now reads the ONE shared mode classifier that feature specifies rather than deciding again.
* **This feature's Background sentence naming the mode selection stays true and gains no exception clause.** "A non-empty `token` selects the personal-access-token mode; a `client_id` plus `client_secret` selects the OAuth machine-to-machine mode; neither selects the unauthenticated mode" left the both-present case unaddressed. It is now unaddressed because it cannot occur, not because it was overlooked.
* **The grant mechanics, the token endpoint default, the cache-and-refresh behaviour, and the credential-safe error text are untouched.** What changes is only how the mode is chosen, so both scenarios below keep their THEN clauses and gain one clause each naming the shared classifier.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A personal access token is applied as the bearer verbatim

* *GIVEN* resolved Unity Catalog CONNECTION credentials that supply a non-empty `token` and no OAuth client credentials — the only shape credential validation admits for the personal-access-token mode
* *WHEN* the session resolves its authentication strategy and issues a Unity Catalog request
* *THEN* the strategy SHALL set the request's `Authorization` header to `Bearer` followed by the supplied `token` verbatim
* *AND* the strategy MUST NOT perform any token exchange, because a personal access token is already a bearer credential
* *AND* the session SHALL select this mode through the ONE shared catalog-auth mode classifier `vs-adapter/connection-credentials-catalog-auth` specifies, and MUST NOT test the `token` ahead of the `client_id`/`client_secret` pair, because that ordering answered an ambiguous CONNECTION the opposite way from the Iceberg REST path
* *AND* the supplied `token` MUST NOT appear in any error message, returned SQL, or log line
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: OAuth machine-to-machine mints a bearer token via the client-credentials grant

* *GIVEN* resolved Unity Catalog CONNECTION credentials that supply `client_id` and `client_secret` and no static `token` — the only shape credential validation admits for the OAuth machine-to-machine mode
* *WHEN* the session resolves its authentication strategy for the first request
* *THEN* the strategy SHALL POST `grant_type=client_credentials&scope=all-apis` with HTTP Basic authentication carrying `client_id` and `client_secret` to the token endpoint, defaulting the endpoint to `{host}/oidc/v1/token` when `oauth2_server_uri` is absent and the scope to `all-apis` when `scope` is absent
* *AND* the strategy SHALL read the returned `access_token` and apply it as the request's bearer token
* *AND* the session SHALL select this mode through that same shared classifier, and the classifier SHALL carry only the three mutually exclusive auth fields — the `oauth2_server_uri` and `scope` defaults SHALL stay owned by this feature, because the Unity endpoint default derives from the CONNECTION address while the Iceberg REST default leaves the property unset for the catalog to fill
* *AND* the `client_secret`, the Basic authentication header, and the minted `access_token` MUST NOT appear in any error message, returned SQL, or log line
<!-- /DELTA:CHANGED -->
