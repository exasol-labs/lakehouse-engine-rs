# Feature: Connection-Object Credential Source — Unity Catalog Kind Parameterization

Extends `vs-adapter/connection-credentials` so credential validation takes the
resolved `CatalogKind` as an explicit input, applies the `warehouse`-required rule
only under `CatalogKind::IcebergRest`, and lets a Unity Catalog CONNECTION reuse the
existing catalog-auth fields (`token`, `client_id`/`client_secret`,
`oauth2_server_uri`, `scope`) with no new credential field.

## Background

* This delta (plan `add-native-unity-catalog-client`, issue #318) parameterizes credential validation by the resolved `CatalogKind`, so the `warehouse`-required rule applies under the Iceberg REST kind only. The catalog kind arrives as an explicit input rather than from the CONNECTION password JSON. Every guarantee below is BEHAVIORAL, not code-level: validation is refactored to take the kind as a parameter, and the Iceberg REST listing path it feeds is refactored behind the shared `CatalogClient` trait, so the promise is that a connection resolved under the default kind is accepted or rejected identically and produces byte-identical error text — not that the code producing it is untouched.
* **This delta (issue #331) adds ONE rule to this feature's rule list and changes no parsing rule, no other guard, and no existing error text.** The new rule rejects a CONNECTION supplying a `token` together with a complete `client_id`/`client_secret` pair. Its normative home is the sibling feature `vs-adapter/connection-credentials-catalog-auth` § "A CONNECTION supplying both a static token and OAuth2 client credentials is rejected", which owns the catalog-auth modes and their mutual exclusion; this feature CITES it rather than restating it. `ConnectionCreds` gains no field and loses none.
* **The new rule sits AFTER the SigV4 rules and is disjoint from the OAuth2-completeness rule.** Placing it after the SigV4 rules keeps every SigV4 error byte-identical: a CONNECTION enabling `use_sigv4` alongside any catalog-auth field is already rejected by the SigV4-versus-catalog-auth exclusion and never reaches the new rule. Placing it before the OAuth2-completeness rule is a readability choice with no behavioural consequence, because the new rule requires all three fields while the completeness rule requires exactly one of the pair — no input satisfies both.
* **SUPERSEDES the rule enumeration in scenario "Credential validation is parameterized by the resolved catalog kind."** That clause listed six rules as applying under `CatalogKind::IcebergRest` with BEHAVIOR UNCHANGED. The list now also carries the token-versus-OAuth exclusion, which is NEW rather than behaviour-unchanged and which applies under BOTH kinds. Leaving the enumeration alone would let it read as exhaustive while omitting the one rule this delta adds.

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
