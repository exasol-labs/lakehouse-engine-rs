# Feature: Unity Catalog Vended Credentials

Requests per-table, short-lived, scoped storage credentials from the Unity Catalog Temporary Table Credentials API (`POST /temporary-table-credentials` with `{table_id, operation}`). `resolve_uc_vended_storage` reads the response and the table's storage location and returns the `StorageBackend` the vended credentials describe.

## Background

* The vended response carries one credential family: `aws_temp_credentials` (access key id, secret access key, session token) for S3, `azure_user_delegation_sas` (SAS token) for ADLS, or `gcp_oauth_token` for Google Cloud Storage. It also carries an `expiration_time` and the table's storage location.
* `resolve_uc_vended_storage` selects the backend variant from the storage location's URI scheme alone: `s3` and `s3a` select S3, `abfs` and `abfss` select ADLS. It uses the same scheme-to-variant decision as the Iceberg vended selector.
* A vended secret never appears in an error message or log line, and never appears in plaintext in a returned SQL string. It reaches the scan UDF only inside the AES-GCM-sealed envelope of `storage-access/scan-spec-credential-reference`.
* The selector reads a CONNECTION-configured store `endpoint` and `region` for addressing and never reads a CONNECTION credential. For `endpoint` and `region` independently, a non-empty CONNECTION value wins over the vended value. An empty address is legal and resolves to the AWS default chain, because Databricks AWS vends no endpoint and no region.
* `abfs` names plaintext transport on the location, and an `http://` endpoint names it on the store. One shared gate, reached by both vended selectors, refuses either one unless the operator sets `ALLOW_HTTP`. Without the gate, an `abfs://` location would silently be read over HTTPS.

## Scenarios

### Scenario: An S3 vended response terminates in an S3 storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `aws_temp_credentials` with an access key id, a secret access key, and a session token, and a storage location whose scheme is `s3`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the S3 variant of `StorageBackend` carrying the vended access key, secret key, and session token
* *AND* the selector MUST NOT read an access key, a secret key, or a session token from the CONNECTION, so a credential the response does not carry is an error and never a static fallback
* *AND* the selector SHALL resolve the store `endpoint` and `region` through the ONE shared store-address rule both vended selectors call, taking each independently from the CONNECTION when the CONNECTION's value is non-empty and from the vended response otherwise
* *AND* the selector SHALL leave a field empty when NEITHER source states it, and an S3 backend whose `endpoint` and `region` are BOTH empty SHALL be returned successfully rather than refused, because Databricks AWS vends no endpoint and no region and the AWS default chain places that store
* *AND* the vended access key, secret key, and session token MUST NOT appear in any error message or log line, and MUST NOT appear in PLAINTEXT in the returned SQL string — they appear there only as the sealed envelope's ciphertext under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause whose returned-SQL half was FALSE before this plan

### Scenario: An ADLS vended response terminates in an ADLS storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `azure_user_delegation_sas` with a SAS token, and a storage location whose scheme is `abfss`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the ADLS variant of `StorageBackend` carrying the SAS credential and the account name recovered from the storage location's host
* *AND* the selector MUST NOT read `account_name`, `account_key`, or `sas_token` from the CONNECTION
* *AND* the vended SAS token MUST NOT appear in any error message or log line, and MUST NOT appear in PLAINTEXT in the returned SQL string — it appears there only as the sealed envelope's ciphertext under issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378), closed by this plan — SUPERSEDING the recorded clause whose returned-SQL half was FALSE before this plan

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

* *GIVEN* an S3 Unity Catalog temporary-credentials response and a resolved store `endpoint` whose scheme is plaintext `http`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend with the resolved `ALLOW_HTTP` consent value
* *THEN* the selector SHALL honor that endpoint only when `ALLOW_HTTP` is true and otherwise SHALL return an error naming the plaintext endpoint, the table location, and the `ALLOW_HTTP` property
* *AND* the gate SHALL apply to the RESOLVED endpoint whichever source supplied it — the CONNECTION's when set, the vended one otherwise — because the gate is on the transport the store will actually use and not on where the value came from
* *AND* the gate SHALL be the ONE shared gate `resolve_vended_storage` also reaches, with one error text naming no catalog kind, so the two selectors cannot disagree about what plaintext consent means
* *AND* the error message MUST NOT contain any credential value or vended secret

### Scenario: A plaintext abfs:// location is honored only with operator consent

* *GIVEN* a Unity Catalog temporary-credentials response carrying a usable `azure_user_delegation_sas`, and a storage location whose scheme is the plaintext `abfs` rather than the TLS `abfss`
* *WHEN* either vended selector resolves the storage backend with the resolved `ALLOW_HTTP` consent value
* *THEN* the selector SHALL honor that location only when `ALLOW_HTTP` is true, and otherwise SHALL return an error naming the `abfs://` scheme, the table location, and the `ALLOW_HTTP` property
* *AND* the refusal SHALL state WHY a silent acceptance is wrong: `abfs` names plaintext transport, this engine has no plaintext Azure path, so accepting it would read the location over HTTPS instead — a silent scheme upgrade the operator never authorised
* *AND* that gate SHALL live in the ONE shared ADLS construction function both vended selectors call, so NEITHER selector can construct an ADLS backend without it having run, and a per-kind copy of the gate SHALL NOT be added
* *AND* this scenario SHALL REPLACE the Iceberg-only coverage of the same rule rather than duplicate it, so one scenario governs both catalog kinds and a future third kind inherits it by calling the shared function
* *AND* an `abfss` location SHALL stay ungated, because it names TLS transport and needs no consent
* *AND* the error message MUST NOT contain any credential value or vended secret

### Scenario: A catalog that vends no storage endpoint or credential is read through the CONNECTION's storage fields

* *GIVEN* a Unity Catalog virtual schema over a catalog server that vends no object-storage endpoint and no storage credential for its tables, whose CONNECTION supplies the storage `endpoint` and static storage credentials
* *WHEN* a query over one of its Delta tables runs
* *THEN* the scan UDF SHALL reach the object store at the CONNECTION's `endpoint` with the static credentials it resolves from the CONNECTION
* *AND* the storage fields MUST NOT change the tables and columns `createVirtualSchema` lists, because enumeration reads catalog metadata only and no object storage

### Scenario: Vended and static credentials resolve the same scan

* *GIVEN* a Unity Catalog Delta table and a Unity Catalog Parquet table, each resolved once with `use_vended_credentials` and once with static storage credentials from the CONNECTION
* *WHEN* the reader resolves each table's scan under both credential arms
* *THEN* both arms SHALL resolve the identical table root and the identical file list, entry for entry
* *AND* the Delta table's partition values SHALL include the Hive default-partition file's explicit NULL, and SHALL never carry the `__HIVE_DEFAULT_PARTITION__` directory literal as a value
