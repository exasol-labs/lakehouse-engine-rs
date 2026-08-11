# Feature: Unity Catalog Vended Credentials

Requests per-table, short-lived, scoped storage credentials from the Unity Catalog Temporary Table Credentials API and terminates them in a `StorageBackend` value. The client posts `{table_id, operation}` to `POST /temporary-table-credentials`; a `resolve_uc_vended_storage` selector reads the vended response and the table's storage location and returns the storage backend the vended credentials describe. This is a third backend-selection site beside the two the Storage Backend Enum already defines, reading a disjoint input — the Unity Catalog temporary-credentials response shape, distinct from the Iceberg REST `loadTable` response. The vending path is unit-tested here and is not exercised end to end until Delta scan execution lands (#319/#320).

## Background

The vended response carries exactly one credential family keyed by the storage backend: `aws_temp_credentials` (access key id, secret access key, session token) for S3, `azure_user_delegation_sas` (SAS token) for ADLS, or `gcp_oauth_token` for Google Cloud Storage, plus an `expiration_time` and the table's storage location. `resolve_uc_vended_storage` selects the storage-backend variant from the storage location's URI scheme alone — `s3`/`s3a` select S3, `abfs`/`abfss` select ADLS — reusing the single scheme-to-variant decision the Iceberg vended selector uses, and MUST NOT read any CONNECTION-derived value. Every vended secret (the access key, secret key, session token, SAS token, and GCP token) MUST NEVER appear in any error message, returned SQL, or log line. The OSS fixture vends static keys with no endpoint; the local-fixture object-store endpoint injection is a Delta scan concern deferred to #319/#320.

## Scenarios

### Scenario: An S3 vended response terminates in an S3 storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `aws_temp_credentials` with an access key id, a secret access key, and a session token, and a storage location whose scheme is `s3`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the S3 variant of `StorageBackend` carrying the vended access key, secret key, and session token
* *AND* the selector SHALL leave the S3 endpoint empty when the response carries none and MUST NOT read the static `endpoint`, `region`, or any other credential field from the CONNECTION
* *AND* the vended access key, secret key, and session token MUST NOT appear in any error message, returned SQL, or log line

### Scenario: An ADLS vended response terminates in an ADLS storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `azure_user_delegation_sas` with a SAS token, and a storage location whose scheme is `abfss`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the ADLS variant of `StorageBackend` carrying the SAS credential and the account name recovered from the storage location's host
* *AND* the selector MUST NOT read `account_name`, `account_key`, or `sas_token` from the CONNECTION
* *AND* the vended SAS token MUST NOT appear in any error message, returned SQL, or log line

### Scenario: The storage-backend variant is selected from the location scheme alone

* *GIVEN* a Unity Catalog temporary-credentials response and a table storage location
* *WHEN* `resolve_uc_vended_storage` selects the storage-backend variant
* *THEN* the selector SHALL map the location scheme `s3` and `s3a` to the S3 variant and `abfs` and `abfss` to the ADLS variant through the single scheme-to-variant decision the Iceberg vended selector already uses
* *AND* the selector MUST NOT consult the CONNECTION credential shape, the vended credential family present in the response, or any virtual-schema property to make this selection

### Scenario: A location scheme with no supported backend is a clear error

* *GIVEN* a Unity Catalog temporary-credentials response whose storage location carries a scheme that names no supported backend — a `gs` Google Cloud Storage location, or any other unsupported or absent scheme
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend
* *THEN* the selector SHALL return an error naming the unsupported scheme and MUST NOT fall back to a default backend, because Google Cloud Storage is not a supported storage backend and a silent default would read data through the wrong store
* *AND* the error message MUST NOT contain any credential value or vended secret

### Scenario: A vended response missing the credential the location's backend needs is a clear error

* *GIVEN* a Unity Catalog temporary-credentials response whose credential family does not match the storage location's backend — an S3 location with no `aws_temp_credentials`, or an ADLS location with no `azure_user_delegation_sas`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend
* *THEN* the selector SHALL return an error stating that the Unity Catalog returned no usable credential for that location and naming the location's scheme or host
* *AND* the selector MUST NOT fall back to any static CONNECTION credential
* *AND* the error message MUST NOT contain any credential value or vended secret

### Scenario: A vended plaintext endpoint is honored only with operator consent

* *GIVEN* an S3 Unity Catalog temporary-credentials response whose storage location resolves to a plaintext `http` endpoint
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend with the resolved `ALLOW_HTTP` consent value
* *THEN* the selector SHALL honor the plaintext endpoint only when `ALLOW_HTTP` is true and otherwise SHALL return an error naming the plaintext scheme and the `ALLOW_HTTP` property, matching the plaintext-transport consent gate the Iceberg vended selector applies
* *AND* the error message MUST NOT contain any credential value or vended secret
