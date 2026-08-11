# Feature: Connection-Object Credential Source

The connection name is supplied as the `CATALOG_CONNECTION` virtual-schema property; the adapter resolves it, using the resolved address as the catalog URI and the JSON password as the credential source. The resolved password value MUST NEVER appear in any error message, returned SQL, or log line.

## Background

This delta parameterizes credential validation by the resolved `CatalogKind`, so the `warehouse`-required rule applies under the Iceberg REST kind only. The catalog kind arrives as an explicit input rather than from the CONNECTION password JSON. Every guarantee below is BEHAVIORAL, not code-level: validation is refactored to take the kind as a parameter, and the Iceberg REST listing path it feeds is refactored behind the shared `CatalogClient` trait, so the promise is that a connection resolved under the default kind is accepted or rejected identically and produces byte-identical error text — not that the code producing it is untouched.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Credential validation is parameterized by the resolved catalog kind

* *GIVEN* the mode-aware credential contract whose rule 1 makes `warehouse` the only unconditionally-required field
* *WHEN* the adapter resolves a CONNECTION under a resolved `CatalogKind` — `IcebergRest` by default, `UnityCatalogNative` when `CATALOG_KIND` selects it
* *THEN* the credential validation SHALL take the resolved `CatalogKind` as an input, and the `warehouse`-required rule SHALL apply under `CatalogKind::IcebergRest` ONLY, because a native Unity Catalog is addressed by `catalog.schema.table` and carries no Iceberg warehouse identifier
* *AND* under `CatalogKind::IcebergRest` every rule of this feature — the `warehouse` requirement, the Azure/S3 mutual exclusion, the Azure-shape rules, the SigV4-versus-catalog-auth exclusion, the SigV4 required-fields rule, and the OAuth2 completeness rule — SHALL apply with BEHAVIOR UNCHANGED, so a connection resolved under the default kind produces byte-identical acceptance and byte-identical errors to before this delta, even though the validation entry point itself gains the `CatalogKind` parameter
* *AND* the `CatalogKind` SHALL arrive as an explicit validation input rather than being read from the CONNECTION password JSON, because the catalog kind is a virtual-schema property and not a credential field
* *AND* no supplied credential value SHALL appear in any error message, returned SQL, or log line under either kind

### Scenario: A Unity Catalog CONNECTION reuses the existing auth fields without a new credential field

* *GIVEN* a CONNECTION resolved under `CatalogKind::UnityCatalogNative` whose JSON password supplies at most one of a non-empty `token` and a `client_id`/`client_secret` pair, and may supply `oauth2_server_uri` and `scope`
* *WHEN* the adapter resolves the connection
* *THEN* the adapter SHALL expose the resolved `token`, `client_id`, `client_secret`, `oauth2_server_uri`, and `scope` on the credentials through the SAME parsing this feature already applies, adding no new CONNECTION password field for Unity Catalog authentication
* *AND* the adapter SHALL accept a Unity Catalog CONNECTION that supplies none of those auth fields, because OSS Unity Catalog runs with authentication disabled
* *AND* the resolved `token` and `client_secret` values MUST NOT appear in any error message, returned SQL, or log line
<!-- /DELTA:NEW -->
