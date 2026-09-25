# Feature: Connection-Object Credential Source (Direct-Storage Kind Parameterization)

Extends `vs-adapter/connection-credentials` with the `CatalogKind::DirectStorage` validation branch.
The CONNECTION address is an object-storage base path rather than a catalog URI. The password
carries storage credentials only. Every catalog-authentication field is refused at
`CREATE VIRTUAL SCHEMA` rather than accepted and ignored.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/connection-credentials-direct-storage/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Credential vending is unreachable under the direct-storage kind

* *GIVEN* a virtual schema resolving `CatalogKind::DirectStorage` and a query over one of its tables
* *WHEN* the adapter resolves the effective scan storage for that table
* *THEN* the effective storage SHALL be the CONNECTION's static backend, and the adapter MUST NOT call either vended-storage selector, request temporary credentials, or read a vending key
* *AND* the shard-invariant common spec SHALL carry a REFERENCE to the CONNECTION when the CONNECTION names no `aws_assume_role_arn`, per `vs-adapter/scan-spec-credential-reference`, so the scan UDF resolves the credential itself and the generated SQL carries none
* *AND* the shard-invariant common spec SHALL carry the backend inside the sealed envelope of `vs-adapter/scan-spec-credential-reference` when the CONNECTION names `aws_assume_role_arn` (`vs-adapter/connection-credentials-assume-role`), and MUST NOT carry a bare CONNECTION reference, so the scan UDF reads the session credentials without an STS request
* *AND* the permanent unreachability of the vended path for this kind SHALL be recorded as a property of the kind rather than as a per-CONNECTION choice, because the other two kinds select vending per CONNECTION and a reader would otherwise expect the same switch here
<!-- /DELTA:CHANGED -->
