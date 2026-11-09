# Feature: REST Catalog Token & OAuth2 Authentication

The Virtual Schema authenticates to an Iceberg REST Catalog in one of three modes: no auth, a static bearer `token`, or OAuth2 client credentials (`client_id` and `client_secret`, with optional `oauth2_server_uri` and `scope`). Catalog authentication is separate from AWS SigV4 signing and from storage credentials.

<!-- DELTA:CHANGED -->
## Background

* Catalog auth props (`token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`) are consumed only in the planning layer. `ScanSpec` does not carry them and carries no catalog identifier block (`uri`, `warehouse`, `table`), because the scan UDF works from pre-resolved file paths and never calls the catalog.
* Catalog auth and credential vending are orthogonal. Either auth mode combines with `use_vended_credentials` on or off. Vended-credential extraction is specified in `storage-access/pushdown-planning-cloud-credentials`.
* Catalog token/OAuth auth and SigV4 are mutually exclusive. Credential validation rejects the combination (see `connection/connection-credentials`). The adapter tests `use_sigv4` before it classifies the auth mode.
* The adapter chooses the auth mode with the one shared classifier specified in `connection/connection-credentials-catalog-auth`. A CONNECTION that supplies both a `token` and a `client_id`/`client_secret` pair is rejected, so the mode is never ambiguous.
* The adapter sends the `loadTable` GET itself in every mode. In OAuth2 mode it performs the client-credentials grant: a form-encoded POST (`grant_type=client_credentials`, `client_id`, `client_secret`, optional `scope`) to `oauth2_server_uri` or the catalog default token endpoint. The returned `access_token` is the bearer until it reaches its renewal point, when the session runs one new grant.
* A multi-warehouse catalog served under a base-path prefix (the Lakekeeper shape) uses a catalog-assigned warehouse name in the `warehouse` field. The adapter reads the per-warehouse `prefix` from the merged catalog config, where `overrides` wins over `defaults` per the Iceberg REST spec. Databricks-style catalogs serve `prefix` in `overrides`, Lakekeeper serves it in `defaults`, so the adapter prefers `overrides.prefix` and falls back to `defaults.prefix`.
* Token, `client_secret`, and every bearer token the session has held never appear in any returned SQL string, error message, or log line.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: OAuth2 client credentials drive the catalog client-credentials grant

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, supply no `token`, the only shape credential validation admits for the OAuth2 mode, and do not enable `use_sigv4`
* *WHEN* the adapter resolves the catalog session of a request
* *THEN* the adapter SHALL POST a form-encoded client-credentials grant carrying `grant_type=client_credentials`, `client_id`, and `client_secret` to `oauth2_server_uri` when a non-empty one was supplied, otherwise to `{uri}/v1/oauth/tokens`
* *AND* the grant SHALL carry `scope` only when a non-empty `scope` was supplied
* *AND* the adapter SHALL send the returned `access_token` as the bearer on every catalog request of that session until the token reaches its renewal point, as the next scenario states
* *AND* the adapter SHALL select the OAuth2 mode through that same shared classifier, so the mode it applies and the mode the Unity Catalog path applies for the same credential set are the same mode by construction
* *AND* the `client_secret` value and every bearer token the session has held MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: An OAuth2 token that reaches its renewal point is renewed once

* *GIVEN* a catalog session in OAuth2 mode whose grant answered with `expires_in`
* *WHEN* the session sends a request after the token reached its renewal point, which is `expires_in` minus the renewal skew, and a second request follows
* *THEN* the session SHALL run one new client-credentials grant before the first of them, and send both requests with the new token
* *AND* concurrent requests at the renewal point SHALL share that one grant
* *AND* a grant answer without `expires_in` SHALL keep its token for the life of the session, as today
* *AND* no token the session has held MUST appear in any returned SQL string or error message
<!-- /DELTA:NEW -->
