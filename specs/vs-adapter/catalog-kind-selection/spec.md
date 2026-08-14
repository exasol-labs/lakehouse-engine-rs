# Feature: Catalog Kind Selection

Selects which catalog kind a virtual schema resolves against — the existing Iceberg REST catalog or a native Unity Catalog — from the `CATALOG_KIND` virtual-schema property, and CONSTRUCTS the matching catalog client. The kind decides only which client is built; every createVirtualSchema listing operation then runs through the shared `CatalogClient` trait on one pipeline. The property is a createVirtualSchema adapter property read from the request's plain VS properties, not a field inside the CONNECTION password JSON. When the property is absent the adapter resolves Iceberg REST, so every pre-existing virtual schema keeps its current behavior with no configuration change.

## Background

The catalog kind is a `CatalogKind` enum with exactly two variants, `IcebergRest` and `UnityCatalogNative`; the variant IS the catalog kind. The kind is matched EXHAUSTIVELY at a small, enumerated set of construction sites, so adding a third kind is a build failure there rather than a silent fall-through, and no listing or pushdown operation re-matches it per request shape. The `CATALOG_KIND` property value is compared case-insensitively. `CatalogKind` is an adapter-layer type in `crates/lakehouse-engine`: it is read from an Exasol virtual-schema property, and the `lakehouse-catalog` crate must not name that delivery mechanism. Credential validation is one further place the kind is an input, and it takes the kind as an explicit parameter rather than re-deriving it.

* **This delta is issue #320.** It replaces the pushdown-time refusal with pushdown-time resolution.
  The kind's role does not widen: the refusal site becomes a construction site, so the count of
  production sites permitted to name a variant is unchanged.
* Every createVirtualSchema rule — kind resolution, case-insensitive comparison, credential
  validation, and the unrecognized-value rejection — is unchanged by this delta.

## Scenarios

### Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties do not include `CATALOG_KIND`
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::IcebergRest` and construct the Iceberg REST catalog client, and MUST NOT read `CATALOG_KIND` from the CONNECTION password JSON
* *AND* the adapter SHALL produce output BEHAVIOR-IDENTICAL to the pre-feature output for the same request: the resolved catalog URI, resolved credentials, enumerated tables, declared column names and Exasol types, `TABLE_MAP`, skipped-table warnings, per-shard scan specs, generated SQL, and error messages are all byte-identical. The listing CODE PATH is refactored behind the shared `CatalogClient` trait rather than left untouched, so the guarantee is behavioral, not code-level
* *AND* enumerating a namespace that contains no table SHALL still build NO resolution-phase `CatalogSession` and perform NO resolution-phase OAuth2 grant; the namespace-enumeration `RestCatalog` retains its OWN grant, so under the OAuth2 client-credentials mode an empty namespace still costs exactly ONE grant (the enumeration grant, unavoidable because the catalog must be contacted to discover the namespace is empty) and under the no-auth and static-token modes ZERO — byte-identical to before this feature (see `vs-adapter/pushdown-catalog-session`). A virtual schema over an empty namespace whose credentials would fail a grant therefore keeps succeeding exactly as it does today ONLY in the no-auth and static-token modes; under OAuth2 it still performs — and can still fail on — the enumeration grant, exactly as it did before this feature. Enumerating a namespace with at least one table SHALL build EXACTLY ONE resolution `CatalogSession` for the whole enumeration, so no request performs more OAuth2 grants or `/v1/config` lookups than before this feature
* *AND* the Iceberg REST scan and pushdown path SHALL be untouched: it continues to resolve files through the existing catalog session and Iceberg-native table metadata, and does NOT go through the `CatalogClient` trait in this plan

### Scenario: CATALOG_KIND naming Unity Catalog resolves the native Unity Catalog kind

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to `UNITY_CATALOG` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::UnityCatalogNative`
* *AND* the adapter SHALL construct the native Unity Catalog client rather than the Iceberg REST client, and SHALL then run the SAME listing pipeline it runs for the Iceberg REST kind
* *AND* the adapter SHALL compare the property value case-insensitively, so `unity_catalog`, `Unity_Catalog`, and `UNITY_CATALOG` all resolve the same kind

### Scenario: The catalog kind is matched at one construction site and nowhere else

* *GIVEN* the resolved `CatalogKind` and the shared `CatalogClient` trait both catalog kinds implement
* *WHEN* the adapter handles a createVirtualSchema request under either kind
* *THEN* the adapter SHALL match `CatalogKind` EXHAUSTIVELY at exactly ONE construction site, which returns a boxed `CatalogClient`, so a third catalog kind is a compile error at that site
* *AND* every subsequent createVirtualSchema step — enumerating the namespace, flattening and case-folding names, mapping column types, building `TABLE_MAP`, and assembling the response — SHALL run ONE pipeline that reads the boxed client through the trait and MUST NOT name or match `CatalogKind`, so a listing change lands once rather than once per kind
* *AND* the ONLY other production sites permitted to take `CatalogKind` as an input SHALL be credential validation, which takes it as an explicit parameter (see the Connection-Object Credential Source feature), and the pushdown path's per-request scan-source construction site, which SUPERSEDES the pushdown refusal in this list because `vs-adapter/pushdown-format-neutral-resolution` replaces that refusal with a resolution seam; no other production module SHALL match on the enum
* *AND* the pushdown scan-source construction site SHALL match the kind EXHAUSTIVELY and SHALL yield the per-request resolver every request shape resolves through, so the site count is unchanged and pushdown gains no per-shape fork
* *AND* a source-level probe SHALL assert that `CatalogKind`'s variant names appear in no production module other than the enum's own declaration, `resolve_catalog_kind`, the catalog-client construction site, credential validation, and the pushdown scan-source construction site, so a per-operation fork cannot be reintroduced silently

### Scenario: Unity Catalog validation does not require a warehouse and rejects SigV4

* *GIVEN* a createVirtualSchema request resolving `CatalogKind::UnityCatalogNative` whose CONNECTION JSON password omits `warehouse`
* *WHEN* the adapter resolves and validates the connection under the Unity Catalog kind
* *THEN* the adapter SHALL accept the connection without reporting `warehouse` as a missing field, because a Unity Catalog is addressed by `catalog.schema.table` rather than by an Iceberg warehouse identifier
* *AND* the adapter SHALL reject a Unity Catalog CONNECTION that sets `use_sigv4` to true with an error stating that AWS SigV4 signing is not a Unity Catalog authentication mode, because the native Unity Catalog API authenticates with a bearer token or Databricks OAuth rather than a signed AWS request
* *AND* the error message MUST NOT contain any supplied credential value

### Scenario: Iceberg REST validation is unchanged under the default catalog kind

* *GIVEN* a createVirtualSchema request resolving `CatalogKind::IcebergRest` — whether by an absent `CATALOG_KIND` or an explicit Iceberg-REST value — whose CONNECTION JSON password omits `warehouse`
* *WHEN* the adapter resolves and validates the connection under the Iceberg REST kind
* *THEN* the adapter SHALL return the same missing-`warehouse` error it returned before this feature, so the Iceberg REST credential contract is unchanged
* *AND* every Iceberg REST validation rule — the Azure/S3 mutual exclusion, the Azure-shape rules, the SigV4-versus-catalog-auth exclusion, the SigV4 required-fields rule, and the OAuth2 completeness rule — SHALL apply exactly as before

### Scenario: An unrecognized CATALOG_KIND value is rejected with a clear error

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to a value that names neither the Iceberg REST kind nor the Unity Catalog kind
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL return an error naming the unrecognized value and the accepted catalog-kind values
* *AND* the adapter MUST NOT fall back to a default catalog kind, because silently defaulting an unrecognized kind would resolve a misconfigured virtual schema against the wrong catalog
* *AND* the error message MUST NOT contain any credential value

### Scenario: A pushdown request under the Unity Catalog kind is planned as a Delta scan

* *GIVEN* a pushdown request whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`, over a seeded Delta table
* *WHEN* the adapter handles the pushdown request
* *THEN* the adapter SHALL resolve the request through the Unity Catalog scan source and the Delta format reader, and SHALL return a scan-driving SQL response, SUPERSEDING the recorded refusal that Unity Catalog scan execution is not yet supported
* *AND* the adapter MUST NOT resolve the request through the Iceberg REST file-resolution path, because a Unity Catalog table is a Delta table the Iceberg path cannot read
* *AND* the adapter SHALL keep resolving the catalog kind BEFORE it reads the CONNECTION, so an unrecognized `CATALOG_KIND` still fails without a connect-back round-trip
* *AND* a pushdown request whose Delta table cannot be planned SHALL fail with the reader's own plan-time error rather than a kind-level refusal, so an unreadable table and an unsupported catalog kind are distinguishable
* *AND* no error message on any of these paths SHALL contain a credential value
