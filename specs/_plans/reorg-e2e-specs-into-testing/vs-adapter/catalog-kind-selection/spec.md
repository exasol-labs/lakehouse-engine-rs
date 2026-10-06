# Feature: Catalog Kind Selection

Selects which catalog kind a virtual schema resolves against from the `CATALOG_KIND` virtual-schema property, and CONSTRUCTS the matching catalog client. The four kinds are the Iceberg REST catalog, a native Unity Catalog, direct storage with no catalog service, and the native AWS Glue Data Catalog. The kind decides only which client is built. Every createVirtualSchema listing operation then runs through the shared `CatalogClient` trait on one pipeline. The property is a createVirtualSchema adapter property read from the request's plain VS properties, not a field inside the CONNECTION password JSON. When the property is absent the adapter resolves Iceberg REST, so every pre-existing virtual schema keeps its current behavior with no configuration change.

<!-- DELTA:CHANGED -->
## Background

The catalog kind is a `CatalogKind` enum with exactly four variants: `IcebergRest`, `UnityCatalogNative`, `DirectStorage`, and `Glue`. The variant IS the catalog kind. The `CATALOG_KIND` property value is compared case-insensitively. `CatalogKind` is an adapter-layer type in `crates/lakehouse-engine`, read from an Exasol virtual-schema property. The `lakehouse-catalog` crate must not name that delivery mechanism. Credential validation takes the kind as an explicit parameter rather than re-deriving it.

* The accepted non-absent values are exactly `UNITY_CATALOG`, `DIRECT_STORAGE`, and `GLUE`. The literal `ICEBERG_REST` stays an UNRECOGNIZED value, because Iceberg REST is selected by leaving `CATALOG_KIND` absent.
* The production files permitted to name a `CatalogKind` variant are FOUR: the enum and its resolver (`adapter/catalog_kind.rs`), the catalog-client construction site (`adapter/mod.rs`), credential validation (`adapter/connection.rs`), and the pushdown scan-source construction site (`adapter/pushdown/scan_resolution.rs`).
* Exhaustive matching and review hold that set. Every match on the kind is exhaustive, so a new variant is a build failure at each site. No source-level probe exists, because this project does not accept a probe that reads production source text. A fifth site added later is visible in review as a new `CatalogKind` import.
* The direct-storage client implements `CatalogClient` but is declared in `lakehouse-engine`, not in `lakehouse-catalog`. `direct-storage/direct-storage-table-discovery` owns that placement. The Glue client is declared in `lakehouse-catalog` (`glue/glue-catalog-client`).
* `RequestSession` is not a second `CatalogKind` match. The pushdown resolver carries an already-resolved session in a private enum with one variant per kind, matched exhaustively when a table resolves. That enum names no `CatalogKind` variant.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties do not include `CATALOG_KIND`
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::IcebergRest` and construct the Iceberg REST catalog client, and MUST NOT read `CATALOG_KIND` from the CONNECTION password JSON
* *AND* enumerating a namespace that contains no table SHALL build NO resolution-phase `CatalogSession` and perform NO resolution-phase OAuth2 grant. The namespace-enumeration `RestCatalog` keeps its OWN grant, so an empty namespace costs exactly ONE grant under the OAuth2 client-credentials mode and ZERO under the no-auth and static-token modes (see `catalog/pushdown-catalog-session`). The enumeration grant is unavoidable, because only a catalog call can show that the namespace is empty. A virtual schema over an empty namespace whose credentials would fail a grant therefore succeeds ONLY in the no-auth and static-token modes, and under OAuth2 it can fail on the enumeration grant. Enumerating a namespace with at least one table SHALL build EXACTLY ONE resolution `CatalogSession` for the whole enumeration, so the resolution phase runs at most one OAuth2 grant and one `/v1/config` lookup
* *AND* the Iceberg REST scan and pushdown path SHALL resolve files through the request's catalog session and the Iceberg-native table metadata, and SHALL NOT go through the `CatalogClient` trait
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Glue validation implies SigV4 and rejects catalog-auth and vending fields

* *GIVEN* CONNECTIONs under the Glue kind: one supplying only `region`, `access_key`, and `secret_key`, one that also supplies `warehouse`, one setting `use_sigv4` to false, one setting `use_vended_credentials` to true, one supplying `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, and one omitting `secret_key`
* *WHEN* the adapter validates each connection
* *THEN* the adapter SHALL accept the first two, treating SigV4 signing as enabled and `warehouse` as the optional Glue `CatalogId`
* *AND* the adapter SHALL reject `use_sigv4` false with an error stating that the Glue kind always signs with AWS SigV4
* *AND* the adapter SHALL reject `use_vended_credentials` true with an error stating that the Glue kind has no native credential vending
* *AND* the adapter SHALL reject every supplied token or OAuth2 field with one error naming each supplied field
* *AND* the SigV4 required-fields rule of `connection/connection-credentials-sigv4` SHALL apply, so the error names the missing `secret_key`, and a standard `https://glue.<region>.amazonaws.com` address supplies the signing region
* *AND* `region`, `access_key`, `secret_key`, `session_token`, `endpoint`, and `path_style` SHALL keep their meaning, so the same keys sign the Glue and the S3 requests, unless the CONNECTION names a role, in which case the role's session replaces those keys for both, per `connection/connection-credentials-assume-role-session-use`
* *AND* no error message SHALL contain a credential value
<!-- /DELTA:CHANGED -->
