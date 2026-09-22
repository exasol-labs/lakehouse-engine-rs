<!-- DELTA:CHANGED -->
# Feature: Catalog Kind Selection

Selects which catalog kind a virtual schema resolves against from the `CATALOG_KIND` virtual-schema property, and CONSTRUCTS the matching catalog client. The three kinds are the existing Iceberg REST catalog, a native Unity Catalog, and direct storage with no catalog service. The kind decides only which client is built. Every createVirtualSchema listing operation then runs through the shared `CatalogClient` trait on one pipeline. The property is a createVirtualSchema adapter property read from the request's plain VS properties, not a field inside the CONNECTION password JSON. When the property is absent the adapter resolves Iceberg REST, so every pre-existing virtual schema keeps its current behavior with no configuration change.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

The catalog kind is a `CatalogKind` enum with exactly three variants: `IcebergRest`, `UnityCatalogNative`, and `DirectStorage`. The variant IS the catalog kind. The kind is matched EXHAUSTIVELY at a small, enumerated set of construction sites, so adding a fourth kind is a build failure there rather than a silent fall-through. No listing or pushdown operation re-matches it per request shape. The `CATALOG_KIND` property value is compared case-insensitively. `CatalogKind` is an adapter-layer type in `crates/lakehouse-engine`, read from an Exasol virtual-schema property. The `lakehouse-catalog` crate must not name that delivery mechanism. Credential validation is one further place the kind is an input. It takes the kind as an explicit parameter rather than re-deriving it.

* **This delta is issue #320.** It replaces the pushdown-time refusal with pushdown-time resolution.
  The kind's role does not widen. The refusal site becomes a construction site, so the count of
  production sites permitted to name a variant is unchanged.
* Every createVirtualSchema rule is unchanged by this delta: kind resolution, case-insensitive
  comparison, credential validation, and the unrecognized-value rejection.
* **This delta is issue #407 and adds the THIRD variant, `DirectStorage`.** It supersedes the
  recorded two-variant enumeration above and the recorded accepted-value list of the
  unrecognized-value scenario. It adds NO construction site and REMOVES none: the third variant is
  matched at exactly the four files the recorded set already enumerates.
* The direct-storage client implements `CatalogClient` but is declared in `lakehouse-engine`, not in
  `lakehouse-catalog`. `vs-adapter/direct-storage-table-discovery` owns that placement and its
  reason. Nothing in this feature changes: the catalog-client construction site still matches the
  kind exhaustively and still returns a boxed `CatalogClient`.
* The literal `ICEBERG_REST` stays an UNRECOGNIZED value. Iceberg REST is selected by leaving
  `CATALOG_KIND` absent, so the accepted non-absent values are exactly `UNITY_CATALOG` and
  `DIRECT_STORAGE`.
* **The recorded source-level probe was never built, and this delta does NOT build it.** The clause
  below has required since issue #318 that a probe assert `CatalogKind`'s variant names appear in no
  production module outside an enumerated set. What exists today is a COMPILE-TIME SIGNATURE probe
  (`catalog_client_tests.rs`). Its own doc comment states that it "does not (and cannot) prove the
  kind is matched nowhere else". No allowlist constant exists anywhere in the workspace. The probe
  the clause describes can only be a test that reads production source text and matches variant
  names in it. This project does not accept a structural invariant enforced that way. The clause is
  therefore superseded by a statement of what holds the invariant instead. Recording a probe that
  nobody will build is worse than recording that the invariant rests on exhaustive matches and
  review.
* **The enumerated sites are FOUR files**: the enum and its resolver (`adapter/catalog_kind.rs`),
  the catalog-client construction site (`adapter/mod.rs`), credential validation
  (`adapter/connection.rs`), and the pushdown scan-source construction site
  (`adapter/pushdown/scan_resolution.rs`). The recorded clause's set is unchanged in extent. Only
  its granularity is stated.
* **What DOES hold the invariant is compile-time.** Each of the three matching sites is exhaustive,
  so a fourth variant is a build failure at every one of them. Nothing silently falls through. A
  fifth site added later is visible in review as a new `CatalogKind` import. That is a weaker
  guarantee than the recorded clause promised. This delta states it as such.
* **`RequestSession` is not a second `CatalogKind` match.** The pushdown path's resolver carries an
  already-resolved session in a private enum that mirrors the kind, matched exhaustively when a
  table resolves. A third kind adds a third variant there. That addition is a compile error rather
  than a fall-through. The enum names no `CatalogKind` variant, so the permitted-site list is
  unaffected.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: CATALOG_KIND naming direct storage resolves the direct-storage kind

* *GIVEN* a createVirtualSchema or pushdown request whose plain VS properties include `CATALOG_KIND` set to `DIRECT_STORAGE` in any letter case
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL resolve `CatalogKind::DirectStorage`
* *AND* the adapter SHALL construct the direct-storage catalog client rather than the Iceberg REST or the native Unity Catalog client, and SHALL then run the SAME listing pipeline it runs for the other two kinds
* *AND* the adapter SHALL compare the property value case-insensitively, so `direct_storage`, `Direct_Storage`, and `DIRECT_STORAGE` all resolve the same kind
* *AND* the adapter SHALL resolve the kind BEFORE it reads the CONNECTION, unchanged, so an unrecognized `CATALOG_KIND` still fails without a connect-back round-trip
* *AND* the resolution SHALL contact no catalog service, because this kind has none: the client reads the object store named by the CONNECTION address directly
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: The catalog kind is matched at one construction site and nowhere else

* *GIVEN* the resolved `CatalogKind` and the shared `CatalogClient` trait all three catalog kinds implement
* *WHEN* the adapter handles a createVirtualSchema request under any kind
* *THEN* the adapter SHALL match `CatalogKind` EXHAUSTIVELY at exactly ONE construction site, which returns a boxed `CatalogClient`, so a fourth catalog kind is a compile error at that site
* *AND* every subsequent createVirtualSchema step — enumerating the namespace, flattening and case-folding names, mapping column types, building `TABLE_MAP`, and assembling the response — SHALL run ONE pipeline that reads the boxed client through the trait and MUST NOT name or match `CatalogKind`, so a listing change lands once rather than once per kind
* *AND* the ONLY other production sites permitted to take `CatalogKind` as an input SHALL be credential validation, which takes it as an explicit parameter (see the Connection-Object Credential Source feature), and the pushdown path's per-request scan-source construction site, which SUPERSEDES the pushdown refusal in this list because `vs-adapter/pushdown-format-neutral-resolution` replaces that refusal with a resolution seam; no other production module SHALL match on the enum
* *AND* the pushdown scan-source construction site SHALL match the kind EXHAUSTIVELY and SHALL yield the per-request resolver every request shape resolves through, so the site count is unchanged and pushdown gains no per-shape fork
* *AND* the permitted set SHALL be exactly four production files of `lakehouse-engine` — the enum's own declaration with `resolve_catalog_kind` (`adapter/catalog_kind.rs`), the catalog-client construction site (`adapter/mod.rs`), credential validation (`adapter/connection.rs`), and the pushdown scan-source construction site (`adapter/pushdown/scan_resolution.rs`) — and no other production module SHALL name a `CatalogKind` variant
* *AND* that set SHALL be held by EXHAUSTIVE matching plus review rather than by a source-level probe, SUPERSEDING the recorded clause that a source-level probe SHALL assert it: no such probe exists, the only probe that could assert it reads production source text to match variant names, and this project does not accept a structural invariant enforced that way
* *AND* each of the three matching sites SHALL be EXHAUSTIVE, so a fourth catalog kind is a build failure at every one of them rather than a silent fall-through, which is the guarantee this feature actually carries
* *AND* this weakening SHALL be recorded rather than left implicit, because a reader who finds no probe would otherwise read its absence as an oversight and rebuild the one this clause declines
* *AND* the pushdown resolver's private already-resolved-session enum SHALL gain a third variant matched exhaustively wherever it is read, so a third kind is a compile error there too, and that enum MUST NOT name a `CatalogKind` variant, so it adds no allowlisted file
* *AND* the direct-storage client SHALL be usable as a boxed `CatalogClient` from that one construction site even though it is declared in `lakehouse-engine` rather than in `lakehouse-catalog`, so the construction site's shape is unchanged apart from becoming fallible
* *AND* the construction site SHALL return a `Result`, because building the direct-storage client opens an object store over the CONNECTION address and that can fail, and the compile-time signature probe pinning that site SHALL be updated to the new signature as an explicit reviewed edit
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: An unrecognized CATALOG_KIND value is rejected with a clear error

* *GIVEN* a createVirtualSchema request whose plain VS properties include `CATALOG_KIND` set to a value that names none of the Iceberg REST kind, the Unity Catalog kind, and the direct-storage kind
* *WHEN* the adapter resolves the catalog kind
* *THEN* the adapter SHALL return an error naming the unrecognized value and ALL THREE accepted catalog-kind spellings, SUPERSEDING the recorded two-value list
* *AND* the error SHALL state that the Iceberg REST kind is selected by leaving `CATALOG_KIND` absent, so an operator who reads the error learns why the literal `ICEBERG_REST` is rejected rather than retrying it
* *AND* the adapter MUST NOT fall back to a default catalog kind, because silently defaulting an unrecognized kind would resolve a misconfigured virtual schema against the wrong catalog
* *AND* the error message MUST NOT contain any credential value
<!-- /DELTA:CHANGED -->
