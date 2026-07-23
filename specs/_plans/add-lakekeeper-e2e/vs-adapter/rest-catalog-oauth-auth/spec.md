# Feature: REST Catalog Token & OAuth2 Authentication

Lets the Virtual Schema authenticate to an Iceberg REST Catalog that requires a bearer
token or an OAuth2 client-credentials exchange. This delta extends the OAuth2
client-credentials mode to a multi-warehouse catalog served under a base-path prefix
(the Lakekeeper shape), where the `warehouse` field is a catalog-assigned name and the
adapter resolves a per-warehouse `overrides.prefix` from the catalog config endpoint.

## Background

* This delta adds one scenario to the existing feature; the permanent feature's
  Background (catalog-auth modes, SigV4 mutual exclusivity, secret redaction) is
  unchanged and governs the new scenario.
* The base-path + per-warehouse-prefix resolution uses the adapter's existing
  `GET {uri}/v1/config?warehouse=<name>` lookup and `overrides.prefix` handling; no
  new CONNECTION field is introduced.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: OAuth2 client-credentials path resolves tables from a multi-warehouse catalog served under a base path

* *GIVEN* a virtual schema whose CONNECTION address is a REST catalog URI that includes a base path segment (for example `http://host:8181/catalog`), whose `warehouse` is a catalog-assigned warehouse NAME rather than an S3 URI, and whose credentials supply `client_id` and `client_secret` without enabling `use_sigv4`
* *WHEN* the adapter resolves the file list through the unsigned catalog path
* *THEN* the adapter SHALL fetch `GET {uri}/v1/config?warehouse={name}` authenticated by the OAuth2-derived bearer token and read `overrides.prefix` from the response
* *AND* the adapter SHALL address the subsequent `loadTable` request under `{uri}/v1/{prefix}/namespaces/{ns}/tables/{table}`, preserving the base path segment from the configured URI and accepting the warehouse-name as the warehouse identifier
* *AND* the `client_secret` and the obtained bearer token MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:NEW -->
