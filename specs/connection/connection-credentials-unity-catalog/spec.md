# Feature: Connection-Object Credential Source — Unity Catalog Kind Parameterization

Extends `connection/connection-credentials` so credential validation takes the resolved `CatalogKind` as input: `warehouse`-required applies under `IcebergRest` only; Unity Catalog carries no warehouse.

## Background

* Validation parameterized by `CatalogKind`. Token-vs-OAuth exclusion applies under BOTH kinds (rule owned by `connection-credentials-catalog-auth`).

## Scenarios

### Scenario: Credential validation is parameterized by catalog kind

* *GIVEN* a CONNECTION and an explicit `CatalogKind` input, not read from the password JSON
* *WHEN* the adapter validates the credentials
* *THEN* `warehouse` SHALL be required under `IcebergRest` only, and all other rules SHALL apply unchanged
* *AND* no error MUST carry a credential value

### Scenario: Unity Catalog reuses existing auth fields

* *GIVEN* a Unity Catalog CONNECTION, possibly with no auth fields because OSS Unity runs unauthenticated
* *WHEN* the adapter validates the credentials
* *THEN* it SHALL accept a no-auth CONNECTION and SHALL enforce the token-versus-OAuth exclusion
* *AND* `token` and `client_secret` MUST NOT appear in errors or SQL
