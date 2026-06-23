# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Extends the pushdown planning layer to sign catalog HTTP requests with AWS SigV4 when
the CONNECTION enables it, and to extract short-lived vended S3 credentials from the
Glue `load_table` response and embed them into every per-shard scan spec — resolving
credentials once in the planning layer, never per shard or node.

## Background

* SigV4 signing and credential vending are opt-in per CONNECTION (`use_sigv4`,
  `use_vended_credentials`); both default to false so existing MinIO/REST stacks
  behave exactly as before.
* Credentials (signing keys, vended STS tokens) MUST NEVER appear in any returned
  SQL string or error message.
* The adapter issues the signed `load_table` GET itself (see ADR for rationale);
  `iceberg-catalog-rest` 0.9.1 has no per-request SigV4 hook and drops
  `storage_credentials`.
* See `vs-adapter/pushdown-planning` for the base pushdown planning scenarios.

## Scenarios

### Scenario: Catalog REST requests to Glue are SigV4-signed when enabled

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and supply `region`, `access_key`, and `secret_key`
* *AND* a query that requires resolving the Iceberg snapshot and file list from an AWS Glue Iceberg REST catalog endpoint
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL sign every outbound catalog HTTP request with an AWS SigV4 signature computed from the credentials, the configured `region`, and the `glue` signing service name
* *AND* the adapter SHALL resolve the data-file list through the signed catalog requests
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message

### Scenario: Unsigned catalog path is unchanged when SigV4 is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_sigv4` or set it to false (the existing MinIO / local REST case)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve the file list with unsigned catalog requests exactly as before
* *AND* the generated scan-driving SQL SHALL be identical in shape to the pre-SigV4 behaviour

### Scenario: Vended S3 credentials from load_table override static credentials in the scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *AND* a Glue Iceberg REST `load_table` response that carries short-lived vended S3 credentials (access key, secret key, and session token) in its storage-credentials or config block
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL extract the vended S3 access key, secret key, and session token from the `load_table` response, resolving them exactly once per query in the planning layer (never per shard or node)
* *AND* the adapter SHALL place the vended credentials (not the static ones) into the storage block of every per-shard scan spec, preserving the static `endpoint`, `region`, and `path_style`
* *AND* the vended credentials MUST NOT appear in any error message

### Scenario: Static credentials are used for data files when vending is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL place the static `access_key`, `secret_key`, and optional `session_token` from the CONNECTION into each scan spec storage block
* *AND* the adapter MUST NOT attempt to read vended credentials from the `load_table` response
