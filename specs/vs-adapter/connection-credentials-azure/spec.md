# Feature: Connection-Object Credential Source — Azure Storage Credentials

Extends `vs-adapter/connection-credentials` with Azure Data Lake Storage Gen2: optional `account_name`, `account_key`, `sas_token` on the CONNECTION password JSON. Vending-disabled path only.

## Background

* Three optional fields; empty = absent. Any Azure field present selects the ADLS backend (no `backend` field). Exactly one of account-key or SAS required (`AdlsCred`). Mixing Azure and S3 fields is rejected. Errors name field names only, never values.

## Scenarios

### Scenario: Azure credentials select the ADLS backend

* Account-key: `account_name` + `account_key` → ADLS with account-key credential; `account_key` never in errors or SQL
* SAS: `account_name` + `sas_token` → ADLS with SAS credential; `sas_token` treated as secret; never in errors or SQL

### Scenario: Malformed or mixed Azure credentials are rejected

* `account_name` absent with a credential present, both `account_key` and `sas_token`, or `account_name` alone → error naming the required shape; no fallback to S3
* Any Azure field AND any S3 field → error naming both field sets; no precedence rule
* No credential values in any error
