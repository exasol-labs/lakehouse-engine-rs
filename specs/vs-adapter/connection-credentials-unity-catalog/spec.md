# Feature: Connection-Object Credential Source — Unity Catalog Kind Parameterization

Extends `vs-adapter/connection-credentials` so credential validation takes the resolved `CatalogKind` as input: `warehouse`-required applies under `IcebergRest` only; Unity Catalog carries no warehouse.

## Background

* Validation parameterized by `CatalogKind`. Token-vs-OAuth exclusion applies under BOTH kinds (rule owned by `connection-credentials-catalog-auth`).

## Scenarios

### Scenario: Credential validation is parameterized by catalog kind

* `warehouse` required under `IcebergRest` only; all other rules apply unchanged; `CatalogKind` arrives as explicit input, not from the password JSON; no credential values in errors

### Scenario: Unity Catalog reuses existing auth fields

* Accepts no-auth CONNECTIONs (OSS Unity runs unauthenticated); enforces token-vs-OAuth exclusion; `token` and `client_secret` never in errors or SQL
