# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves
the Iceberg data-file list once (signing catalog requests with AWS SigV4 and applying
vended S3 credentials when the CONNECTION enables them), captures the requested projection,
filter, LIMIT, and any supported single-group or grouped aggregate, and emits the SQL that
drives the DataFusion scan SET UDF — fanned out across G oversubscribed work-unit shards
via `GROUP BY shard_key` — over exactly those files.

## Background

* The file list is resolved exactly once in the planning layer; the scan UDF discovers nothing.
* Catalog and storage credentials are resolved from the CONNECTION object, not plain properties.
* SigV4 signing and credential vending are opt-in per CONNECTION; disabled by default.
* Credentials MUST NEVER appear in any returned SQL string or error message.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Catalog REST requests to Glue are SigV4-signed when enabled

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_sigv4` to true and supply `region`, `access_key`, and `secret_key`
* *AND* a query that requires resolving the Iceberg snapshot and file list from an AWS Glue Iceberg REST catalog endpoint
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL sign every outbound catalog HTTP request with an AWS SigV4 signature computed from the credentials, the configured `region`, and the `glue` signing service name
* *AND* the adapter SHALL resolve the data-file list through the signed catalog requests
* *AND* the SigV4 signing keys MUST NOT appear in any returned SQL string or error message

<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Unsigned catalog path is unchanged when SigV4 is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_sigv4` or set it to false (the existing MinIO / local REST case)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve the file list with unsigned catalog requests exactly as before
* *AND* the generated scan-driving SQL SHALL be identical in shape to the pre-SigV4 behaviour

<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Vended S3 credentials from load_table override static credentials in the scan spec

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *AND* a Glue Iceberg REST `load_table` response that carries short-lived vended S3 credentials (access key, secret key, and session token) in its storage-credentials or config block
* *WHEN* Exasol sends the `pushdown` request and the adapter loads the table once to resolve files
* *THEN* the adapter SHALL extract the vended S3 access key, secret key, and session token from the `load_table` response, resolving them exactly once per query in the planning layer (never per shard or node)
* *AND* the adapter SHALL place the vended credentials (not the static ones) into the storage block of every per-shard scan spec, preserving the static `endpoint`, `region`, and `path_style`
* *AND* the vended credentials MUST NOT appear in any error message

<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Static credentials are used for data files when vending is disabled

* *GIVEN* a virtual schema whose CONNECTION credentials omit `use_vended_credentials` or set it to false
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL place the static `access_key`, `secret_key`, and optional `session_token` from the CONNECTION into each scan spec storage block
* *AND* the adapter MUST NOT attempt to read vended credentials from the `load_table` response

<!-- /DELTA:NEW -->
