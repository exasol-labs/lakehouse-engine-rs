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

* Catalog authentication and credential vending are orthogonal. The catalog-auth
  mode (no-auth / static bearer `token` / OAuth2 client-credentials) selects how
  the table-load request is authenticated; `use_vended_credentials` independently
  selects whether short-lived S3 STS credentials are extracted from the `loadTable`
  response and carried in the scan specs. Either mode may combine with vending on
  or off. The vended-credential extraction mechanics live in
  `vs-adapter/pushdown-planning-cloud-credentials`; this feature only guarantees the
  catalog-auth secrets themselves never leak into any scan spec.
* The adapter self-issues the `loadTable` GET on every catalog-auth mode (no-auth,
  bearer token, OAuth2-grant-derived bearer), authenticating the request accordingly.
  The adapter performs the OAuth2 client-credentials grant itself: a form-encoded
  POST (`grant_type=client_credentials`, `client_id`, `client_secret`, optional
  `scope`) to `oauth2_server_uri` or the catalog default token endpoint, returning the
  `access_token` used as the bearer.
* Catalog auth props (`token`, `client_id`, `client_secret`, `oauth2_server_uri`,
  `scope`) are consumed ONLY in the planning layer. They are NOT carried in `ScanSpec`
  and MUST NOT cross the UDF boundary; the scan UDF works from pre-resolved file paths
  and never calls the catalog.
* `ScanSpec` carries no catalog identifier block (`uri`/`warehouse`/`table`) — those
  fields were dropped as dead weight the scan UDF never reads.
* Catalog auth and SigV4 are mutually exclusive auth strategies: SigV4 self-issues
  signed HTTP requests using the SigV4 signing path; token/OAuth and SigV4 may not
  be combined. The combination is rejected at credential-resolution time (see
  `vs-adapter/connection-credentials`).
* Token, `client_secret`, and obtained OAuth2 bearer token values MUST NEVER appear
  in any returned SQL string, error message, or log line.
* See `vs-adapter/connection-credentials` for credential parsing and the orthogonal,
  always-optional static-S3 fields, and `vs-adapter/pushdown-planning-cloud-credentials`
  for the SigV4, vended-credential, and orthogonality flows.
* The OAuth2 client-credentials mode extends to a multi-warehouse catalog served
  under a base-path prefix (the Lakekeeper shape), where the `warehouse` field is a
  catalog-assigned name and the adapter resolves a per-warehouse `overrides.prefix`
  from the catalog config endpoint; no new CONNECTION field is introduced.
* The `prefix` config property is read from the MERGED config per the Iceberg REST
  spec (a client merges the server's `defaults` base and `overrides`; `overrides`
  wins). It may be served in either map: Databricks-style catalogs place it in
  `overrides`, while Lakekeeper 0.13.1 serves the per-warehouse UUID prefix in
  `defaults`. Reading only `overrides.prefix` yielded an empty prefix against
  Lakekeeper and a malformed `loadTable` URL (missing the required warehouse
  segment → HTTP 404); the adapter therefore prefers `overrides.prefix` and falls
  back to `defaults.prefix`.

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

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials, with `use_vended_credentials` either enabled or disabled
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* the `ScanSpec` SHALL carry no catalog identifier block at all — the scan UDF never contacts the catalog, so `ScanSpec` MUST NOT include catalog `uri`, `warehouse`, or `table` fields
* *AND* each `ScanSpec` storage block SHALL carry only the S3 storage credentials — the vended STS credentials when `use_vended_credentials` is enabled and they were resolved, otherwise the static credentials — exactly as in `vs-adapter/pushdown-planning-cloud-credentials`

### Scenario: OAuth2 client-credentials path resolves tables from a multi-warehouse catalog served under a base path

* *GIVEN* a virtual schema whose CONNECTION address is a REST catalog URI that includes a base path segment (for example `http://host:8181/catalog`), whose `warehouse` is a catalog-assigned warehouse NAME rather than an S3 URI, and whose credentials supply `client_id` and `client_secret` without enabling `use_sigv4`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL fetch `GET {uri}/v1/config?warehouse={name}` authenticated by the OAuth2-derived bearer token and read the per-warehouse `prefix` from the merged config, preferring `overrides.prefix` and falling back to `defaults.prefix` (Lakekeeper serves the per-warehouse prefix in `defaults`)
* *AND* the adapter SHALL address the subsequent `loadTable` request under `{uri}/v1/{prefix}/namespaces/{ns}/tables/{table}`, preserving the base path segment from the configured URI and accepting the warehouse-name as the warehouse identifier
* *AND* the `client_secret` and the obtained bearer token MUST NOT appear in any returned SQL string or error message
