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

* **This delta (issue #331) removes the token-versus-OAuth PRECEDENCE this feature's Iceberg path carried, and adds no behaviour for any CONNECTION the adapter accepts.** Both `resolve_catalog_auth` and `inject_catalog_auth_props` (`crates/lakehouse-catalog/src/auth.rs`) tested a complete `client_id`/`client_secret` pair BEFORE a non-empty `token`, so a CONNECTION supplying both silently took the OAuth2 mode — while the Unity Catalog path took the token, the opposite answer for the same input. Credential validation now rejects that CONNECTION outright (`vs-adapter/connection-credentials-catalog-auth`), which makes the ordering unreachable, and both functions now read the ONE shared mode classifier that feature specifies instead of each deciding again.
* **Each mode's resulting props, requests, and error redaction are byte-identical.** What changes is only HOW the mode is chosen. The three scenarios below each already state their GIVEN as one mode's fields, so their THEN clauses stand unedited apart from the two clauses this delta adds.
* **`use_sigv4` continues to be tested ahead of the mode classification, and that is not a precedence.** SigV4 and catalog token/OAuth are mutually exclusive strategies rejected in combination at credential-resolution time, so the two branches cannot both apply; the dispatch picks between two upstream-exclusive strategies. The recorded resolver test that asserted SigV4 wins "regardless of any token also being set" documented a combination validation forbids and is corrected rather than kept.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Static bearer token is attached to unsigned catalog requests

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, supply neither `client_id` nor `client_secret` — the only shape credential validation admits for the token mode — and do not enable `use_sigv4`
* *AND* a query that requires resolving the Iceberg snapshot and file list from a REST catalog endpoint
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL set the catalog `token` property from the resolved credentials when building the REST catalog
* *AND* the adapter SHALL NOT set the catalog `credential`, `oauth2-server-uri`, or `scope` properties, since the token mode never consults them
* *AND* the adapter SHALL select the token mode through the ONE shared catalog-auth mode classifier `vs-adapter/connection-credentials-catalog-auth` specifies, and MUST NOT test the `client_id`/`client_secret` pair ahead of the `token`, because that ordering answered an ambiguous CONNECTION the opposite way from the Unity Catalog path
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: OAuth2 client credentials drive the catalog client-credentials grant

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, supply no `token` — the only shape credential validation admits for the OAuth2 mode — and do not enable `use_sigv4`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL set the catalog `credential` property to the string formed by joining `client_id` and `client_secret` with a single colon
* *AND* the adapter SHALL set the catalog `oauth2-server-uri` property only when a non-empty `oauth2_server_uri` was supplied, otherwise leaving it unset so the catalog defaults to `{uri}/v1/oauth/tokens`
* *AND* the adapter SHALL set the catalog `scope` property only when a non-empty `scope` was supplied, otherwise leaving it unset so the catalog applies its default
* *AND* the adapter SHALL select the OAuth2 mode through that same shared classifier, so the mode it applies and the mode the Unity Catalog path applies for the same credential set are the same mode by construction
* *AND* the adapter SHALL NOT set the catalog `token` property, and the `client_secret` value MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:CHANGED -->
