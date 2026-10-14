# Feature: Connection-Object Credential Source — Azure Storage Credentials

Extends `connection/connection-credentials` with Azure Data Lake Storage Gen2: optional `account_name`, `account_key`, `sas_token` on the CONNECTION password JSON. Vending-disabled path only.

## Background

* Three optional fields; empty = absent. Any Azure field present selects the ADLS backend (no `backend` field). Exactly one of account-key or SAS required (`AdlsCred`). Mixing Azure and S3 fields is rejected. Errors name field names only, never values.

## Scenarios

### Scenario: Azure credentials select the ADLS backend

* *GIVEN* a CONNECTION carrying `account_name` plus either `account_key` or `sas_token`
* *WHEN* the adapter resolves the storage backend
* *THEN* it SHALL select ADLS with an account-key credential for `account_key` and with a SAS credential for `sas_token`
* *AND* `account_key` and `sas_token` MUST be treated as secrets and MUST NOT appear in errors or SQL

### Scenario: Malformed or mixed Azure credentials are rejected

* *GIVEN* a CONNECTION with `account_name` absent but a credential present, with both `account_key` and `sas_token`, with `account_name` alone, or with both an Azure field and an S3 field
* *WHEN* the adapter validates the credentials
* *THEN* it SHALL return an error naming the required shape, or naming both field sets for the Azure-plus-S3 case, and SHALL NOT fall back to S3 or apply a precedence rule
* *AND* no error MUST carry a credential value
