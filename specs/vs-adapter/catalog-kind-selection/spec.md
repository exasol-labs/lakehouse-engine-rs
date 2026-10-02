# Feature: Catalog Kind Selection

Selects which catalog kind a virtual schema resolves against from the `CATALOG_KIND` virtual-schema property, and CONSTRUCTS the matching catalog client. The four kinds are the Iceberg REST catalog, a native Unity Catalog, direct storage with no catalog service, and the native AWS Glue Data Catalog. The kind decides only which client is built. Every createVirtualSchema listing operation then runs through the shared `CatalogClient` trait on one pipeline. The property is a createVirtualSchema adapter property read from the request's plain VS properties, not a field inside the CONNECTION password JSON. When the property is absent the adapter resolves Iceberg REST, so every pre-existing virtual schema keeps its current behavior with no configuration change.

## Background

The catalog kind is a `CatalogKind` enum with exactly four variants: `IcebergRest`, `UnityCatalogNative`, `DirectStorage`, and `Glue`. The variant IS the catalog kind. The `CATALOG_KIND` property value is compared case-insensitively. `CatalogKind` is an adapter-layer type in `crates/lakehouse-engine`, read from an Exasol virtual-schema property. The `lakehouse-catalog` crate must not name that delivery mechanism. Credential validation takes the kind as an explicit parameter rather than re-deriving it.

* The accepted non-absent values are exactly `UNITY_CATALOG`, `DIRECT_STORAGE`, and `GLUE`. The literal `ICEBERG_REST` stays an UNRECOGNIZED value, because Iceberg REST is selected by leaving `CATALOG_KIND` absent.
* The production files permitted to name a `CatalogKind` variant are FOUR: the enum and its resolver (`adapter/catalog_kind.rs`), the catalog-client construction site (`adapter/mod.rs`), credential validation (`adapter/connection.rs`), and the pushdown scan-source construction site (`adapter/pushdown/scan_resolution.rs`).
* Exhaustive matching and review hold that set. Every match on the kind is exhaustive, so a new variant is a build failure at each site. No source-level probe exists, because this project does not accept a probe that reads production source text. A fifth site added later is visible in review as a new `CatalogKind` import.
* The direct-storage client implements `CatalogClient` but is declared in `lakehouse-engine`, not in `lakehouse-catalog`. `vs-adapter/direct-storage-table-discovery` owns that placement. The Glue client is declared in `lakehouse-catalog` (`vs-adapter/glue-catalog-client`).
* `RequestSession` is not a second `CatalogKind` match. The pushdown resolver carries an already-resolved session in a private enum with one variant per kind, matched exhaustively when a table resolves. That enum names no `CatalogKind` variant.

## Scenarios

### Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties do not include `CATALOG_KIND`
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::IcebergRest` and construct the Iceberg REST catalog client, and MUST NOT read `CATALOG_KIND` from the CONNECTION password JSON
* *AND* enumerating a namespace that contains no table SHALL build NO resolution-phase `CatalogSession` and perform NO resolution-phase OAuth2 grant. The namespace-enumeration `RestCatalog` keeps its OWN grant, so an empty namespace costs exactly ONE grant under the OAuth2 client-credentials mode and ZERO under the no-auth and static-token modes (see `vs-adapter/pushdown-catalog-session`). The enumeration grant is unavoidable, because only a catalog call can show that the namespace is empty. A virtual schema over an empty namespace whose credentials would fail a grant therefore succeeds ONLY in the no-auth and static-token modes, and under OAuth2 it can fail on the enumeration grant. Enumerating a namespace with at least one table SHALL build EXACTLY ONE resolution `CatalogSession` for the whole enumeration, so the resolution phase runs at most one OAuth2 grant and one `/v1/config` lookup
* *AND* the Iceberg REST scan and pushdown path SHALL resolve files through the request's catalog session and the Iceberg-native table metadata, and SHALL NOT go through the `CatalogClient` trait

### Scenario: CATALOG_KIND naming Unity Catalog resolves the native Unity Catalog kind

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to `UNITY_CATALOG` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::UnityCatalogNative`
* *AND* the adapter SHALL construct the native Unity Catalog client rather than the Iceberg REST client, and SHALL then run the SAME listing pipeline it runs for the Iceberg REST kind
* *AND* the adapter SHALL compare the property value case-insensitively, so `unity_catalog`, `Unity_Catalog`, and `UNITY_CATALOG` all resolve the same kind

### Scenario: The catalog kind is matched at one construction site and nowhere else

* *GIVEN* the resolved `CatalogKind` and the shared `CatalogClient` trait every catalog kind implements
* *WHEN* the adapter handles a createVirtualSchema request under any kind
* *THEN* the adapter SHALL match `CatalogKind` EXHAUSTIVELY at exactly ONE construction site, which returns a `Result` holding a boxed `CatalogClient`, so a new catalog kind is a compile error at that site
* *AND* every later createVirtualSchema step (enumerating the namespace, flattening and case-folding names, mapping column types, building `TABLE_MAP`, recording skipped tables, and assembling the response) SHALL run ONE pipeline that reads the boxed client through the trait and MUST NOT match `CatalogKind`
* *AND* the only other production sites permitted to take `CatalogKind` as an input SHALL be credential validation, which takes it as an explicit parameter, and the pushdown scan-source construction site, which matches it EXHAUSTIVELY and yields the per-request resolver every request shape resolves through
* *AND* the pushdown resolver's private already-resolved-session enum SHALL carry one variant per catalog kind, matched exhaustively wherever it is read, and MUST NOT name a `CatalogKind` variant
* *AND* the direct-storage client and the Glue client SHALL each be usable as a boxed `CatalogClient` from that one construction site
* *AND* the compile-time signature probe pinning the construction site SHALL stay in step with its signature through explicit reviewed edits

### Scenario: Unity Catalog validation does not require a warehouse and rejects SigV4

* *GIVEN* a createVirtualSchema request resolving `CatalogKind::UnityCatalogNative` whose CONNECTION JSON password omits `warehouse`
* *WHEN* the adapter resolves and validates the connection under the Unity Catalog kind
* *THEN* the adapter SHALL accept the connection without reporting `warehouse` as a missing field, because a Unity Catalog is addressed by `catalog.schema.table` rather than by an Iceberg warehouse identifier
* *AND* the adapter SHALL reject a Unity Catalog CONNECTION that sets `use_sigv4` to true with an error stating that AWS SigV4 signing is not a Unity Catalog authentication mode, because the native Unity Catalog API authenticates with a bearer token or Databricks OAuth rather than a signed AWS request
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Iceberg REST validation is unchanged under the default catalog kind

* *GIVEN* a createVirtualSchema request resolving `CatalogKind::IcebergRest` through an absent `CATALOG_KIND`, whose CONNECTION JSON password omits `warehouse`
* *WHEN* the adapter resolves and validates the connection under the Iceberg REST kind
* *THEN* the adapter SHALL return the same missing-`warehouse` error it returned before this feature, so the Iceberg REST credential contract is unchanged
* *AND* every Iceberg REST validation rule — the Azure/S3 mutual exclusion, the Azure-shape rules, the SigV4-versus-catalog-auth exclusion, the SigV4 required-fields rule, and the OAuth2 completeness rule — SHALL apply exactly as before

### Scenario: An unrecognized CATALOG_KIND value is rejected with a clear error

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to a value that names none of the Unity Catalog kind, the direct-storage kind, and the Glue kind
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL return an error naming the unrecognized value and every accepted choice: absent for Iceberg REST, `UNITY_CATALOG`, `DIRECT_STORAGE`, and `GLUE`
* *AND* the error SHALL state that the Iceberg REST kind is selected by leaving `CATALOG_KIND` absent, so an operator who reads the error learns why the literal `ICEBERG_REST` is rejected rather than retrying it
* *AND* the adapter SHALL resolve the kind BEFORE it reads the CONNECTION, on createVirtualSchema and pushdown requests alike, so an unrecognized value fails without a connect-back round trip
* *AND* the adapter MUST NOT fall back to a default catalog kind, because silently defaulting an unrecognized kind would resolve a misconfigured virtual schema against the wrong catalog
* *AND* the error message MUST NOT contain any credential value

### Scenario: A pushdown request under the Unity Catalog kind is planned as a Delta scan

* *GIVEN* a pushdown request whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`, over a seeded Delta table
* *WHEN* the adapter handles the pushdown request
* *THEN* the adapter SHALL resolve the request through the Unity Catalog scan source and the Delta format reader, and SHALL return a scan-driving SQL response, SUPERSEDING the recorded refusal that Unity Catalog scan execution is not yet supported
* *AND* the adapter MUST NOT resolve the request through the Iceberg REST file-resolution path, because a Unity Catalog table is a Delta table the Iceberg path cannot read
* *AND* a pushdown request whose Delta table cannot be planned SHALL fail with the reader's own plan-time error rather than a kind-level refusal, so an unreadable table and an unsupported catalog kind are distinguishable
* *AND* no error message on any of these paths SHALL contain a credential value

### Scenario: CATALOG_KIND naming direct storage resolves the direct-storage kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties include `CATALOG_KIND` set to `DIRECT_STORAGE` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::DirectStorage`
* *AND* the adapter SHALL construct the direct-storage catalog client rather than the Iceberg REST or the native Unity Catalog client, and SHALL then run the SAME listing pipeline it runs for the other kinds
* *AND* the adapter SHALL compare the property value case-insensitively, so `direct_storage`, `Direct_Storage`, and `DIRECT_STORAGE` all resolve the same kind
* *AND* the resolution SHALL contact no catalog service, because this kind has none: the client reads the object store named by the CONNECTION address directly

### Scenario: CATALOG_KIND naming Glue resolves the native Glue kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties include `CATALOG_KIND` set to `GLUE` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::Glue`
* *AND* the adapter SHALL construct the Glue catalog client and SHALL run the SAME listing pipeline it runs for the other kinds

### Scenario: Glue validation implies SigV4 and rejects catalog-auth and vending fields

* *GIVEN* CONNECTIONs under the Glue kind: one supplying only `region`, `access_key`, and `secret_key`, one that also supplies `warehouse`, one setting `use_sigv4` to false, one setting `use_vended_credentials` to true, one supplying `token`, `client_id`, `client_secret`, `oauth2_server_uri`, or `scope`, and one omitting `secret_key`
* *WHEN* the adapter validates each connection
* *THEN* the adapter SHALL accept the first two, treating SigV4 signing as enabled and `warehouse` as the optional Glue `CatalogId`
* *AND* the adapter SHALL reject `use_sigv4` false with an error stating that the Glue kind always signs with AWS SigV4
* *AND* the adapter SHALL reject `use_vended_credentials` true with an error stating that the Glue kind has no native credential vending
* *AND* the adapter SHALL reject every supplied token or OAuth2 field with one error naming each supplied field
* *AND* the SigV4 required-fields rule of `vs-adapter/connection-credentials-sigv4` SHALL apply, so the error names the missing `secret_key`, and a standard `https://glue.<region>.amazonaws.com` address supplies the signing region
* *AND* `region`, `access_key`, `secret_key`, `session_token`, `endpoint`, and `path_style` SHALL keep their meaning, so the same keys sign the Glue and the S3 requests, unless the CONNECTION names a role, in which case the role's session replaces those keys for both, per `vs-adapter/connection-credentials-assume-role`
* *AND* no error message SHALL contain a credential value
