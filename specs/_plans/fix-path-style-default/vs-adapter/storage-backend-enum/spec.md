# Feature: Storage Backend Enum

Gives the storage-backend decision exactly one home: a `StorageBackend` enum whose variant IS the backend, so every other module asks the enum for what it needs instead of deciding for itself which backend it is talking to.

## Background

<!-- DELTA:NEW -->
* **This delta widens the store-address type by one addressing field (#130).** `StaticStoreAddress` gains `path_style: Option<bool>` beside `endpoint` and `region`. No `StorageBackend` variant changes, no `StorageProps` field changes, no wire encoding changes.
* **The credential-disjointness probe is unchanged.** `path_style` names none of the forbidden credential spellings. The one-construction and field-privacy rules cover the added field: it is non-`pub`, read through an accessor, and the probe reads the declaration rather than a hardcoded list.
<!-- /DELTA:NEW -->

## Scenarios

### Scenario: The vended selectors take a store address that cannot carry a credential

* *GIVEN* the recorded clause that `resolve_vended_storage` "MUST NOT take a `StorageBackend`, a `ConnectionCreds`, or any other CONNECTION-derived value as a parameter, so the two selectors' input disjointness is enforced by its signature rather than by inspection of its body", and the matching clause requiring `resolve_uc_vended_storage` to take no `warehouse`, `region`, or existing `StorageBackend`
* *AND* the defect those clauses cause: real Databricks AWS vends short-lived credentials with NO endpoint and its credential type carries no region field at all, so a legal Databricks `s3://` table lands in exactly the state `resolve_vended_storage`'s "store address undetermined" error rejects
* *WHEN* credentials are split from addressing
<!-- DELTA:CHANGED -->
* *THEN* both clauses SHALL be SUPERSEDED for ADDRESSING ALONE: each vended selector SHALL take one additional parameter carrying the CONNECTION's configured store `endpoint`, `region`, and `path_style`, and carrying NOTHING else — SUPERSEDING the recorded clause naming `endpoint` and `region` alone (#130)
<!-- /DELTA:CHANGED -->
* *AND* the credential half of the disjointness rule SHALL be UNCHANGED: an access key, a secret key, a session token, an Azure account key, and a SAS token SHALL come from the vended response ALONE on every vended path, and MUST NEVER be read from the CONNECTION under vending
<!-- DELTA:CHANGED -->
* *AND* because the signature no longer enforces that half, it SHALL be enforced MECHANICALLY rather than by prose: the new parameter's type SHALL declare EXACTLY the three addressing fields, and a source-level probe SHALL assert that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password` — SUPERSEDING the recorded clause naming EXACTLY two addressing fields
* *AND* the added field SHALL be an OPTION of boolean, because the resolution chain branches on whether the CONNECTION stated a value
<!-- /DELTA:CHANGED -->
* *AND* the type SHALL have EXACTLY ONE construction from the CONNECTION, declared beside it, so which CONNECTION fields are permitted to cross into vended resolution is one decision in one place rather than a struct literal repeated per call site
<!-- DELTA:CHANGED -->
* *AND* that ONE-construction rule SHALL be enforced BY THE TYPE rather than by prose or by review: EVERY addressing field SHALL be declared NON-`pub`, reads SHALL go through accessor methods, and a source-level probe SHALL assert that the declaration keeps EVERY field non-`pub`
* *AND* that probe SHALL read the field list out of the type's own declaration rather than holding a hardcoded list, so a field added later is covered without editing the probe
<!-- /DELTA:CHANGED -->
* *AND* passing a `ConnectionCreds`, a `StorageBackend`, or any other type carrying a credential field to either vended selector SHALL remain FORBIDDEN, so the superseded rule is narrowed to addressing and not dropped
* *AND* the disjointness rule's PURPOSE SHALL be restated in the spec rather than left to be inferred: it exists to stop a vended credential silently falling back to a static one, and it was never about addressing
