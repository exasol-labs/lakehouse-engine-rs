# Feature: Connection-Object Credential Source — Unity Catalog Kind Parameterization

Extends `vs-adapter/connection-credentials` so credential validation takes the
resolved `CatalogKind` as an explicit input, applies the `warehouse`-required rule
only under `CatalogKind::IcebergRest`, and lets a Unity Catalog CONNECTION reuse the
existing catalog-auth fields (`token`, `client_id`/`client_secret`,
`oauth2_server_uri`, `scope`) with no new credential field.

## Background

* Credential validation is parameterized by the resolved `CatalogKind`: `warehouse`-required applies under `IcebergRest` only; Unity Catalog carries no warehouse.
* A CONNECTION supplying a `token` together with a `client_id`/`client_secret` pair is rejected (rule owned by `vs-adapter/connection-credentials-catalog-auth`, cited here).

## Scenarios

### Scenario: Credential validation is parameterized by the resolved catalog kind

* *GIVEN* the mode-aware credential contract whose rule 1 makes `warehouse` the only unconditionally-required field
* *WHEN* the adapter resolves a CONNECTION under a resolved `CatalogKind` — `IcebergRest` by default, `UnityCatalogNative` when `CATALOG_KIND` selects it
* *THEN* the credential validation SHALL take the resolved `CatalogKind` as an input, and the `warehouse`-required rule SHALL apply under `CatalogKind::IcebergRest` ONLY, because a native Unity Catalog is addressed by `catalog.schema.table` and carries no Iceberg warehouse identifier
* *AND* under `CatalogKind::IcebergRest` every rule of the base feature that predates the token-versus-OAuth exclusion — the `warehouse` requirement, the Azure/S3 mutual exclusion, the Azure-shape rules, the SigV4-versus-catalog-auth exclusion, the SigV4 required-fields rule, and the OAuth2 completeness rule — SHALL apply with BEHAVIOR UNCHANGED, so a connection resolved under the default kind produces byte-identical acceptance and byte-identical errors to before the `CatalogKind` parameter was introduced, even though the validation entry point itself gains that parameter
* *AND* the token-versus-OAuth exclusion SHALL apply under BOTH kinds and is the ONE rule of this feature that is not behaviour-unchanged, because it rejects a CONNECTION both kinds previously accepted; it is specified by `vs-adapter/connection-credentials-catalog-auth` and CITED here, so the kind-parameterized entry point carries no per-kind copy of it
* *AND* the `CatalogKind` SHALL arrive as an explicit validation input rather than being read from the CONNECTION password JSON, because the catalog kind is a virtual-schema property and not a credential field
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line under either kind

### Scenario: A Unity Catalog CONNECTION reuses the existing auth fields without a new credential field

* *GIVEN* a CONNECTION resolved under `CatalogKind::UnityCatalogNative` whose JSON password supplies at most one of a non-empty `token` and a `client_id`/`client_secret` pair, and may supply `oauth2_server_uri` and `scope`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` on the credentials through the SAME parsing the base feature already applies, adding no new CONNECTION password field for Unity Catalog authentication
* *AND* the adapter SHALL accept a Unity Catalog CONNECTION that supplies none of those auth fields, because OSS Unity Catalog runs with authentication disabled
* *AND* the "at most one" precondition of this scenario SHALL be ENFORCED rather than assumed: a Unity Catalog CONNECTION supplying a `token` together with a complete `client_id`/`client_secret` pair SHALL be rejected by the same kind-independent rule that rejects it under `CatalogKind::IcebergRest`, specified by `vs-adapter/connection-credentials-catalog-auth`
* *AND* the resolved `token` and `client_secret` values MUST NOT appear in any error message, returned SQL, or log line
