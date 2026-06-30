# Feature: REST Catalog Token & OAuth2 Authentication

Lets the Virtual Schema authenticate to an Iceberg REST Catalog that requires a bearer
token or an OAuth2 client-credentials exchange, in addition to the existing static-S3
credential model. Three catalog-auth modes are supported: (1) no auth (the default,
current behaviour); (2) a static bearer `token`, attached directly as the catalog's
bearer credential; and (3) OAuth2 client credentials (`client_id` + `client_secret`,
with optional `oauth2_server_uri` and `scope`), where the catalog performs the
client-credentials grant itself to obtain and refresh a token. Catalog authentication and
S3 storage credentials are orthogonal — any combination is valid. This auth path is
separate from, and mutually exclusive with, AWS SigV4 request signing.

## Background

* The exact REST-catalog property keys are fixed by `iceberg-catalog-rest` 0.9.1: `token`
  (static bearer), `credential` (the string `"client_id:client_secret"` for the OAuth2
  client-credentials grant), `oauth2-server-uri` (optional token endpoint; the crate
  defaults it to `{uri}/v1/oauth/tokens`), and `scope` (optional; the crate defaults it to
  `catalog`). These keys flow through `RestCatalogBuilder::load` in the same props map that
  carries `uri`, `warehouse`, and the S3 keys — the builder copies every prop except
  `uri`/`warehouse` into the catalog config.
* The token and client-credentials modes are distinct in the crate's `authenticate()`
  (`client.rs:211`): when a `token` is present it is used directly as the bearer header and
  `oauth2-server-uri`/`scope` are never consulted; `oauth2-server-uri` is read ONLY inside
  the credential-exchange path (`exchange_credential_for_token`, `client.rs:112`). When both
  `credential` and `token` are missing, authentication is skipped (no-auth mode).
* Catalog auth props are consumed ONLY in the planning layer's unsigned catalog-build path
  (`build_rest_catalog`). They are NOT carried in `ScanSpec` and MUST NOT cross the UDF
  boundary; the scan UDF works from pre-resolved file paths and never calls the catalog.
* Catalog auth and SigV4 are mutually exclusive auth strategies: SigV4 self-issues signed
  HTTP requests and bypasses `RestCatalogBuilder` entirely, so token/OAuth props would be
  silently ignored on that path. The combination is rejected at credential-resolution time
  (see `vs-adapter/connection-credentials`).
* Token and `client_secret` values MUST NEVER appear in any returned SQL string, error
  message, or log line.
* See `vs-adapter/connection-credentials` for credential parsing and the orthogonal,
  always-optional static-S3 fields, and `vs-adapter/pushdown-planning-cloud-credentials` for
  the SigV4 and vended-credential flows.

## Scenarios

### Scenario: Static bearer token is attached to unsigned catalog requests

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token` and do not enable `use_sigv4`
* *AND* a query that requires resolving the Iceberg snapshot and file list from a REST catalog endpoint
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL set the catalog `token` property from the resolved credentials when building the REST catalog
* *AND* the adapter SHALL NOT set the catalog `credential`, `oauth2-server-uri`, or `scope` properties, since the token mode never consults them
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message

### Scenario: OAuth2 client credentials drive the catalog client-credentials grant

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret` and do not enable `use_sigv4`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL set the catalog `credential` property to the string formed by joining `client_id` and `client_secret` with a single colon
* *AND* the adapter SHALL set the catalog `oauth2-server-uri` property only when a non-empty `oauth2_server_uri` was supplied, otherwise leaving it unset so the catalog defaults to `{uri}/v1/oauth/tokens`
* *AND* the adapter SHALL set the catalog `scope` property only when a non-empty `scope` was supplied, otherwise leaving it unset so the catalog applies its default
* *AND* the adapter SHALL NOT set the catalog `token` property, and the `client_secret` value MUST NOT appear in any returned SQL string or error message

### Scenario: No catalog auth props are set when neither token nor OAuth credentials are supplied

* *GIVEN* a virtual schema whose CONNECTION credentials supply no `token`, `client_id`, or `client_secret`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL NOT set the catalog `token`, `credential`, `oauth2-server-uri`, or `scope` properties
* *AND* the catalog build SHALL be identical in shape to the pre-feature behaviour

### Scenario: Catalog auth props are never placed in any scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* each `ScanSpec` storage block SHALL carry only the S3 storage credentials (vended or static) exactly as in the established credential flows
