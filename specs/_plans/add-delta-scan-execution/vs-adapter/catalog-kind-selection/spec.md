# Feature: Catalog Kind Selection

The catalog kind is a `CatalogKind` enum with exactly two variants, `IcebergRest` and
`UnityCatalogNative`; the variant IS the catalog kind. The kind is matched EXHAUSTIVELY at a small,
enumerated set of construction sites, so adding a third kind is a build failure there rather than a
silent fall-through, and no listing or pushdown operation re-matches it per request shape.

## Background

* **This delta is issue #320.** It replaces the pushdown-time refusal with pushdown-time resolution.
  The kind's role does not widen: the refusal site becomes a construction site, so the count of
  production sites permitted to name a variant is unchanged.
* Every createVirtualSchema rule — kind resolution, case-insensitive comparison, credential
  validation, and the unrecognized-value rejection — is unchanged by this delta.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The catalog kind is matched at one construction site and nowhere else

* *GIVEN* the resolved `CatalogKind` and the shared `CatalogClient` trait both catalog kinds implement
* *WHEN* the adapter handles a createVirtualSchema request under either kind
* *THEN* the adapter SHALL match `CatalogKind` EXHAUSTIVELY at exactly ONE construction site, which returns a boxed `CatalogClient`, so a third catalog kind is a compile error at that site
* *AND* every subsequent createVirtualSchema step — enumerating the namespace, flattening and case-folding names, mapping column types, building `TABLE_MAP`, and assembling the response — SHALL run ONE pipeline that reads the boxed client through the trait and MUST NOT name or match `CatalogKind`, so a listing change lands once rather than once per kind
* *AND* the ONLY other production sites permitted to take `CatalogKind` as an input SHALL be credential validation, which takes it as an explicit parameter (see the Connection-Object Credential Source feature), and the pushdown path's per-request scan-source construction site, which SUPERSEDES the pushdown refusal in this list because `vs-adapter/pushdown-format-neutral-resolution` replaces that refusal with a resolution seam; no other production module SHALL match on the enum
* *AND* the pushdown scan-source construction site SHALL match the kind EXHAUSTIVELY and SHALL yield the per-request resolver every request shape resolves through, so the site count is unchanged and pushdown gains no per-shape fork
* *AND* a source-level probe SHALL assert that `CatalogKind`'s variant names appear in no production module other than the enum's own declaration, `resolve_catalog_kind`, the catalog-client construction site, credential validation, and the pushdown scan-source construction site, so a per-operation fork cannot be reintroduced silently
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: A pushdown request under the Unity Catalog kind is refused as not yet executable

* *GIVEN* a pushdown request whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`
* *WHEN* the adapter handles the pushdown request
* *THEN* this scenario SHALL be REMOVED, because its title and every one of its clauses assert a refusal that issue #320 replaces with resolution through the Delta format reader
* *AND* it SHALL be REPLACED by "A pushdown request under the Unity Catalog kind is planned as a Delta scan" below, which restates the no-Iceberg-fallback requirement and the credential-redaction requirement under the new routing rule
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A pushdown request under the Unity Catalog kind is planned as a Delta scan

* *GIVEN* a pushdown request whose virtual schema was created with `CATALOG_KIND` set to `UNITY_CATALOG`, over a seeded Delta table
* *WHEN* the adapter handles the pushdown request
* *THEN* the adapter SHALL resolve the request through the Unity Catalog scan source and the Delta format reader, and SHALL return a scan-driving SQL response, SUPERSEDING the recorded refusal that Unity Catalog scan execution is not yet supported
* *AND* the adapter MUST NOT resolve the request through the Iceberg REST file-resolution path, because a Unity Catalog table is a Delta table the Iceberg path cannot read
* *AND* the adapter SHALL keep resolving the catalog kind BEFORE it reads the CONNECTION, so an unrecognized `CATALOG_KIND` still fails without a connect-back round-trip
* *AND* a pushdown request whose Delta table cannot be planned SHALL fail with the reader's own plan-time error rather than a kind-level refusal, so an unreadable table and an unsupported catalog kind are distinguishable
* *AND* no error message on any of these paths SHALL contain a credential value
<!-- /DELTA:NEW -->
