# Feature: Unity Catalog Vended Credentials

Requests per-table, short-lived, scoped storage credentials from the Unity Catalog Temporary Table Credentials API and terminates them in a `StorageBackend` value. The client posts `{table_id, operation}` to `POST /temporary-table-credentials`; a `resolve_uc_vended_storage` selector reads the vended response and the table's storage location and returns the storage backend the vended credentials describe. This is a third backend-selection site beside the two the Storage Backend Enum already defines, reading a disjoint input — the Unity Catalog temporary-credentials response shape, distinct from the Iceberg REST `loadTable` response. The vending path is unit-tested here and is not exercised end to end until Delta scan execution lands (#319/#320).

## Background

* **This delta is issue #135. It amends TWO scenarios and changes no vended selection rule.** The credential-family-per-backend mapping, the scheme-to-variant decision, the shared store-address rule, the missing-credential error, the unsupported-scheme error, and both plaintext-consent gates are all UNCHANGED.
* **SUPERSEDES the recorded claim that every vended secret "MUST NEVER appear in any error message, returned SQL, or log line."** The error-message and log-line halves hold. The returned-SQL half is FALSE and stays false: a vended credential travels INLINE in the scan-spec storage block, because no CONNECTION name identifies a credential the catalog vended for one table. That residual is the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378).
* **The residual is narrower than what issue #135 closes.** A vended credential expires and is scoped to the prefix the catalog vended it for; a CONNECTION `secret_key` is long-lived and account-wide. `vs-adapter/scan-spec-credential-reference` owns the reference contract this feature's path does NOT take, and this feature CITES it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: An S3 vended response terminates in an S3 storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `aws_temp_credentials` with an access key id, a secret access key, and a session token, and a storage location whose scheme is `s3`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the S3 variant of `StorageBackend` carrying the vended access key, secret key, and session token
* *AND* the selector MUST NOT read an access key, a secret key, or a session token from the CONNECTION, so a credential the response does not carry is an error and never a static fallback
* *AND* the selector SHALL resolve the store `endpoint` and `region` through the ONE shared store-address rule both vended selectors call, taking each independently from the CONNECTION when the CONNECTION's value is non-empty and from the vended response otherwise
* *AND* the selector SHALL leave a field empty when NEITHER source states it, and an S3 backend whose `endpoint` and `region` are BOTH empty SHALL be returned successfully rather than refused, because Databricks AWS vends no endpoint and no region and the AWS default chain places that store
* *AND* the vended access key, secret key, and session token MUST NOT appear in any error message or log line, and DO appear in the returned SQL string under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade both
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An ADLS vended response terminates in an ADLS storage backend

* *GIVEN* a Unity Catalog temporary-credentials response carrying `azure_user_delegation_sas` with a SAS token, and a storage location whose scheme is `abfss`
* *WHEN* `resolve_uc_vended_storage` resolves the storage backend from that response and location
* *THEN* the selector SHALL return the ADLS variant of `StorageBackend` carrying the SAS credential and the account name recovered from the storage location's host
* *AND* the selector MUST NOT read `account_name`, `account_key`, or `sas_token` from the CONNECTION
* *AND* the vended SAS token MUST NOT appear in any error message or log line, and DOES appear in the returned SQL string under the tracked exception issue [#378](https://github.com/exasol-labs/lakehouse-engine-rs/issues/378) — SUPERSEDING the recorded clause that forbade both
<!-- /DELTA:CHANGED -->
