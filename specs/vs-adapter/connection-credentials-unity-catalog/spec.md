# Feature: Connection-Object Credential Source — Unity Catalog Kind Parameterization

Extends `vs-adapter/connection-credentials` so credential validation takes the resolved
`CatalogKind` as input: `warehouse`-required applies under `IcebergRest` only; Unity Catalog
carries no warehouse. Reuses the existing auth fields with no new credential field.

## Background

* Validation parameterized by `CatalogKind`: `warehouse` required only under `IcebergRest`.
* Token-vs-OAuth exclusion applies under BOTH kinds (rule owned by `connection-credentials-catalog-auth`).

## Scenarios

### Scenario: Credential validation is parameterized by catalog kind

* *GIVEN* the mode-aware credential contract
* *THEN* `warehouse` is required under `IcebergRest` only; all other rules (Azure/S3 exclusion, SigV4 rules, OAuth completeness) apply with BEHAVIOR UNCHANGED under `IcebergRest`; the `CatalogKind` arrives as an explicit input, not from the password JSON; no credential values in errors

### Scenario: Unity Catalog reuses existing auth fields

* *GIVEN* a CONNECTION under `UnityCatalogNative` with at most one of `token` and `client_id`/`client_secret`
* *THEN* the adapter exposes the resolved auth fields through the SAME parsing; accepts no-auth CONNECTIONs (OSS Unity runs unauthenticated); enforces the token-vs-OAuth exclusion; `token` and `client_secret` never in errors or SQL
