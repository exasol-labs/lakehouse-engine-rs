# Feature: Connection-Object Credential Source — Azure Storage Credentials

Extends `vs-adapter/connection-credentials` with the Azure Data Lake Storage Gen2 credential shape:
optional `account_name`, `account_key`, and `sas_token` on the CONNECTION password JSON.
Vending-disabled path only.

## Background

* Three optional password fields: `account_name`, `account_key`, `sas_token`. Empty = absent.
* The credential SHAPE selects the backend — any Azure field present makes it Azure. No `backend` field.
* Exactly one credential required: `AdlsCred` has account-key and SAS states, not both.
* Mixing Azure and S3 fields is rejected. Every error names field names only, no values.

## Scenarios

### Scenario: Azure account-key credentials select the ADLS backend

* *GIVEN* a CONNECTION with `warehouse`, `account_name`, `account_key`, and no S3 or `sas_token` fields
* *THEN* the resolved backend is ADLS with the supplied `account_name` and account-key credential; no S3 backend; `account_key` never in errors or SQL

### Scenario: Azure SAS credentials select the ADLS backend

* *GIVEN* a CONNECTION with `warehouse`, `account_name`, `sas_token`, and no S3 or `account_key` fields
* *THEN* the resolved backend is ADLS with a SAS credential; `sas_token` treated as secret on every path; never in errors or SQL

### Scenario: A malformed Azure credential set is rejected

* *GIVEN* `account_name` absent with a credential present, or both `account_key` and `sas_token` present, or `account_name` alone
* *THEN* an error naming the required shape; no fallback to S3; no credential values in the error

### Scenario: Mixing Azure and S3 fields is rejected

* *GIVEN* any Azure field AND any S3 field present
* *THEN* an error naming both field sets; no precedence rule; no credential values in the error
