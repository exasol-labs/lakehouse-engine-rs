# Feature: Connection-Object Credential Source — Azure Storage Credentials

Extends `vs-adapter/connection-credentials` with the Azure Data Lake Storage Gen2
(`abfss://`) credential shape carried on the same CONNECTION password JSON: the
optional `account_name`, `account_key`, and `sas_token` fields, the rule that selects
the ADLS storage backend from their presence, and the rules that reject a malformed or
ambiguous Azure credential set. The vending-disabled path only; how a supplied Azure
credential is treated once `use_vended_credentials` is true is specified in the base
feature's "Static storage credentials are ignored, not rejected, when vending is
requested" scenario.

## Background

* Three new optional password fields: `account_name` (the Azure storage account), `account_key` (shared-key secret), and `sas_token` (SAS secret). Empty string = absent.
* The credential SHAPE selects the backend — any Azure field present makes it an Azure CONNECTION. No `backend` field.
* Exactly one credential required: `AdlsCred` has account-key and SAS states, not both.
* Mixing Azure and S3 fields is rejected. `allow_http` does not reach the Azure backend in this slice.
* Every error names field names only — no credential values.

## Scenarios

### Scenario: Azure account-key credentials select the ADLS storage backend

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, a non-empty `account_name`, and a non-empty `account_key`, and omits `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, and `sas_token`
* *WHEN* the adapter resolves the connection and builds the storage configuration
* *THEN* the adapter SHALL accept the password without reporting any missing field
* *AND* the resolved storage backend SHALL be the ADLS variant carrying the supplied `account_name` and an account-key credential holding the supplied `account_key`
* *AND* the adapter MUST NOT produce an S3 backend for this CONNECTION
* *AND* the supplied `account_key` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: Azure inline-SAS credentials select the ADLS storage backend

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, a non-empty `account_name`, and a non-empty `sas_token`, and omits `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, and `account_key`
* *WHEN* the adapter resolves the connection and builds the storage configuration
* *THEN* the adapter SHALL accept the password without reporting any missing field
* *AND* the resolved storage backend SHALL be the ADLS variant carrying the supplied `account_name` and a SAS credential holding the supplied `sas_token`
* *AND* the adapter SHALL treat the `sas_token` value as a secret on every path that treats `account_key` as one, because a SAS supplied inline in the CONNECTION grants the same access
* *AND* the supplied `sas_token` MUST NOT appear in any error message, returned SQL, or log line

### Scenario: An Azure CONNECTION without exactly one account name and one credential is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse` and at least one of `account_name`, `account_key`, and `sas_token`, in one of three malformed shapes: `account_name` absent while a credential is present; `account_name` present while BOTH `account_key` and `sas_token` are present; or `account_name` present while NEITHER is present
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that an Azure CONNECTION requires `account_name` and exactly one of `account_key` and `sas_token`
* *AND* the error SHALL name the offending field names and MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT fall back to the S3 backend for any of the three shapes, because a malformed Azure credential set is an error and not an absent one

### Scenario: A CONNECTION mixing Azure and static S3 credential fields is rejected

* *GIVEN* a CONNECTION whose JSON password supplies a non-empty `warehouse`, at least one of `account_name`, `account_key`, and `sas_token`, AND at least one of `endpoint`, `region`, `access_key`, `secret_key`, and `session_token`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL return an error stating that Azure and S3 storage credentials cannot both be supplied on one CONNECTION
* *AND* the error SHALL name the supplied Azure field names and the supplied S3 field names and MUST NOT contain any supplied credential value
* *AND* the adapter MUST NOT apply a precedence rule between the two credential sets, because an undeclared precedence resolves an ambiguous credentials input silently
