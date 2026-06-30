# Feature: REST Catalog Token & OAuth2 Authentication

Lets the Virtual Schema authenticate to an Iceberg REST Catalog requiring a static bearer token or an OAuth2 client-credentials exchange, in addition to no-auth, with catalog authentication kept orthogonal to S3 storage credentials and credential vending.

<!--
DELTA against specs/vs-adapter/rest-catalog-oauth-auth/spec.md.
Reconciles the scan-spec scenario with the now-orthogonal vended-credential flow:
the established "only S3 storage credentials (vended or static)" wording is made
true on this path by the orthogonality work. Adds one Background clause. Unmarked
scenarios are unchanged and shown for context only.
-->

## Background

<!-- DELTA:CHANGED -->
* Catalog authentication and credential vending are orthogonal. The catalog-auth
  mode (no-auth / static bearer `token` / OAuth2 client-credentials) selects how
  the table-load request is authenticated; `use_vended_credentials` independently
  selects whether short-lived S3 STS credentials are extracted from the `loadTable`
  response and carried in the scan specs. Either mode may combine with vending on
  or off. The vended-credential extraction mechanics live in
  `vs-adapter/pushdown-planning-cloud-credentials`; this feature only guarantees the
  catalog-auth secrets themselves never leak into any scan spec.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Static bearer token is attached to unsigned catalog requests

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token` and do not enable `use_sigv4`
* *AND* a query that requires resolving the Iceberg snapshot and file list from a REST catalog endpoint
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL authenticate the catalog `loadTable` request with the resolved `token` as the bearer credential
* *AND* the adapter SHALL NOT consult `credential`, `oauth2-server-uri`, or `scope`, since the token mode never uses them
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message

### Scenario: OAuth2 client credentials drive the catalog client-credentials grant

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret` and do not enable `use_sigv4`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL obtain a bearer token via the OAuth2 client-credentials grant formed from `client_id` and `client_secret`
* *AND* the adapter SHALL direct the grant at the supplied `oauth2_server_uri` when non-empty, otherwise at the catalog default token endpoint
* *AND* the adapter SHALL include the supplied `scope` in the grant when non-empty, otherwise apply the catalog default
* *AND* the `client_secret` value and the obtained bearer token MUST NOT appear in any returned SQL string or error message

### Scenario: No catalog auth props are set when neither token nor OAuth credentials are supplied

* *GIVEN* a virtual schema whose CONNECTION credentials supply no `token`, `client_id`, or `client_secret`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL NOT attach any bearer token or perform any OAuth2 grant
* *AND* the catalog `loadTable` request SHALL be issued without an `Authorization` header

<!-- DELTA:CHANGED -->
### Scenario: Catalog auth props are never placed in any scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials, with `use_vended_credentials` either enabled or disabled
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* each `ScanSpec` storage block SHALL carry only the S3 storage credentials — the vended STS credentials when `use_vended_credentials` is enabled and they were resolved, otherwise the static credentials — exactly as in `vs-adapter/pushdown-planning-cloud-credentials`
<!-- /DELTA:CHANGED -->
