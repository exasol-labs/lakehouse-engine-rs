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

* **This delta adds the Azure credential shape and nothing else.** It implements issue #275, slice C of six (A-F) for Azure Data Lake Storage Gen2 (`abfss://`) support. Every existing scenario keeps its behaviour: a CONNECTION that supplies no Azure field is parsed, validated, and projected exactly as before, byte for byte.
* **Three new optional password fields, all read by the same `nonempty_str` rule as every existing field:** `account_name` (the Azure storage account, not a secret — it appears in every `abfss://` URI), `account_key` (a shared-key secret), and `sas_token` (a shared-access-signature secret; a SAS supplied inline in the CONNECTION is a secret exactly as an account key is). An empty-string value is "absent", the convention every other field already uses.
* **The credential SHAPE selects the backend; there is no `backend` field.** Adding one would be a second source of truth free to disagree with the credentials actually supplied. `vs-adapter/storage-backend-enum` already records that `storage_block` is the ONLY site that selects a backend from input when vending is disabled; this delta is the first input that makes that selection observable, and it adds no second decision point.
* **The presence of ANY Azure field — not just `account_name` — makes the CONNECTION an Azure CONNECTION.** Keying selection on `account_name` alone would let a CONNECTION that supplies `account_key` and forgets `account_name` fall silently back to S3 with the key ignored. Keying on any-of-three turns that same input into a named-field error.
* **Exactly one credential, never two.** `AdlsCred` has an account-key state and a SAS state and no "both" state, so a CONNECTION supplying both describes a backend the type cannot represent. Rejecting is the only reading that does not silently pick one.
* **Mixing Azure and static S3 fields is rejected rather than resolved.** A credentials path MUST NOT resolve an ambiguous input silently. This is the one rule the issue text does not name; it is here because the alternative — an undeclared precedence between two credential sets — is exactly the silent misconfiguration the rest of this feature exists to prevent.
* **`use_sigv4` together with Azure fields is NOT given its own guard.** SigV4 already requires `access_key`, `secret_key`, and `region`, so such a CONNECTION is rejected either by the mixed-fields rule (when those are supplied) or by the existing SigV4 rule (when they are not). A third guard would add a second error for an input that already fails loud.
* **`allow_http` does not reach the Azure backend in this slice.** It arrives from the `ALLOW_HTTP` VS property and is consumed only by the S3 payload. Azurite-emulator support (plain-HTTP Azure endpoints) is out of scope here and is not silently half-wired: the Azure backend carries no HTTP-scheme knob at all.
* Every error added by this delta names FIELD NAMES only. No `account_key`, `sas_token`, or any other supplied value appears in any error message, returned SQL, or log line.

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
