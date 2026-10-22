# Feature: REST Catalog Token & OAuth2 Authentication

The Virtual Schema authenticates to an Iceberg REST Catalog in one of three modes: no auth, a static bearer `token`, or OAuth2 client credentials (`client_id` and `client_secret`, with optional `oauth2_server_uri` and `scope`). Catalog authentication is separate from AWS SigV4 signing and from storage credentials.

## Background

* Catalog auth props (`token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`) are consumed only in the planning layer. `ScanSpec` does not carry them and carries no catalog identifier block (`uri`, `warehouse`, `table`), because the scan UDF works from pre-resolved file paths and never calls the catalog.
* Catalog auth and credential vending are orthogonal. Either auth mode combines with `use_vended_credentials` on or off. Vended-credential extraction is specified in `storage-access/pushdown-planning-cloud-credentials`.
* Catalog token/OAuth auth and SigV4 are mutually exclusive. Credential validation rejects the combination (see `connection/connection-credentials`). The adapter tests `use_sigv4` before it classifies the auth mode.
* The adapter chooses the auth mode with the one shared classifier specified in `connection/connection-credentials-catalog-auth`. A CONNECTION that supplies both a `token` and a `client_id`/`client_secret` pair is rejected, so the mode is never ambiguous.
* The adapter sends the `loadTable` GET itself in every mode. In OAuth2 mode it performs the client-credentials grant: a form-encoded POST (`grant_type=client_credentials`, `client_id`, `client_secret`, optional `scope`) to `oauth2_server_uri` or the catalog default token endpoint. The returned `access_token` is the bearer.
* A multi-warehouse catalog served under a base-path prefix (the Lakekeeper shape) uses a catalog-assigned warehouse name in the `warehouse` field. The adapter reads the per-warehouse `prefix` from the merged catalog config, where `overrides` wins over `defaults` per the Iceberg REST spec. Databricks-style catalogs serve `prefix` in `overrides`, Lakekeeper serves it in `defaults`, so the adapter prefers `overrides.prefix` and falls back to `defaults.prefix`.
* Token, `client_secret`, and the obtained bearer token never appear in any returned SQL string, error message, or log line.


## Scenarios

### Scenario: Static bearer token is attached to unsigned catalog requests

* *GIVEN* a virtual schema whose CONNECTION credentials supply a non-empty `token`, supply neither `client_id` nor `client_secret`, the only shape credential validation admits for the token mode, and do not enable `use_sigv4`
* *AND* a query that requires resolving the Iceberg snapshot and file list from a REST catalog endpoint
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL set the catalog `token` property from the resolved credentials when building the REST catalog
* *AND* the adapter SHALL NOT set the catalog `credential`, `oauth2-server-uri`, or `scope` properties, since the token mode never consults them
* *AND* the adapter SHALL select the token mode through the ONE shared catalog-auth mode classifier `connection/connection-credentials-catalog-auth` specifies, and MUST NOT test the `client_id`/`client_secret` pair ahead of the `token`, because that ordering answered an ambiguous CONNECTION the opposite way from the Unity Catalog path
* *AND* the `token` value MUST NOT appear in any returned SQL string or error message

### Scenario: OAuth2 client credentials drive the catalog client-credentials grant

* *GIVEN* a virtual schema whose CONNECTION credentials supply `client_id` and `client_secret`, supply no `token`, the only shape credential validation admits for the OAuth2 mode, and do not enable `use_sigv4`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL set the catalog `credential` property to the string formed by joining `client_id` and `client_secret` with a single colon
* *AND* the adapter SHALL set the catalog `oauth2-server-uri` property only when a non-empty `oauth2_server_uri` was supplied, otherwise leaving it unset so the catalog defaults to `{uri}/v1/oauth/tokens`
* *AND* the adapter SHALL set the catalog `scope` property only when a non-empty `scope` was supplied, otherwise leaving it unset so the catalog applies its default
* *AND* the adapter SHALL select the OAuth2 mode through that same shared classifier, so the mode it applies and the mode the Unity Catalog path applies for the same credential set are the same mode by construction
* *AND* the adapter SHALL NOT set the catalog `token` property, and the `client_secret` value MUST NOT appear in any returned SQL string or error message

### Scenario: No catalog auth props are set when neither token nor OAuth credentials are supplied

* *GIVEN* a virtual schema whose CONNECTION credentials supply no `token`, `client_id`, or `client_secret`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL NOT set the catalog `token`, `credential`, `oauth2-server-uri`, or `scope` properties
* *AND* the catalog build SHALL carry no auth property, so the catalog is reached unauthenticated

### Scenario: Catalog auth props are never placed in any scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials supply a `token` or OAuth2 client credentials, with `use_vended_credentials` either enabled or disabled
* *WHEN* the adapter builds the per-shard scan specs after resolving the file list
* *THEN* the adapter MUST NOT place `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope` into any `ScanSpec` field
* *AND* the `ScanSpec` SHALL carry no catalog identifier block at all, the scan UDF never contacts the catalog, so `ScanSpec` MUST NOT include catalog `uri`, `warehouse`, or `table` fields
* *AND* each `ScanSpec` storage block SHALL carry only storage material, in the wire variant that `storage-access/scan-spec-credential-reference` § "Own credentials travel by name and every other credential travels sealed" selects

### Scenario: OAuth2 client-credentials path resolves tables from a multi-warehouse catalog served under a base path

* *GIVEN* a virtual schema whose CONNECTION address is a REST catalog URI that includes a base path segment (for example `http://host:8181/catalog`), whose `warehouse` is a catalog-assigned warehouse NAME rather than an S3 URI, and whose credentials supply `client_id` and `client_secret` without enabling `use_sigv4`
* *WHEN* the adapter calls the catalog through the unsigned catalog path, either to enumerate the configured namespace during `createVirtualSchema` or to resolve the file list during a pushdown
* *THEN* the adapter SHALL fetch `GET {uri}/v1/config?warehouse={name}` authenticated by the OAuth2-derived bearer token and read the per-warehouse `prefix` from the merged config, preferring `overrides.prefix` and falling back to `defaults.prefix` (Lakekeeper serves the per-warehouse prefix in `defaults`)
* *AND* the adapter SHALL address every subsequent catalog request under `{uri}/v1/{prefix}/`: the namespace and table listing requests of the enumeration under `{uri}/v1/{prefix}/namespaces`, and the `loadTable` request under `{uri}/v1/{prefix}/namespaces/{ns}/tables/{table}`, preserving the base path segment from the configured URI and accepting the warehouse-name as the warehouse identifier
* *AND* the `client_secret` and the obtained bearer token MUST NOT appear in any returned SQL string or error message
