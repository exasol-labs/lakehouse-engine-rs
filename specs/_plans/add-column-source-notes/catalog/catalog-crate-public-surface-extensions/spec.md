# Feature: Catalog Crate Public Surface Extensions

Tracks each explicit, reviewed extension of `lakehouse-catalog`'s enumerated public surface after
the crate boundary itself was drawn — the shared `CatalogClient` trait, a demotion once a caller
moved in-crate, and the narrow additions that the engine-side format, credential, and catalog-signing
work required.

This is the sibling of `catalog/catalog-crate-structure`, split out once the base feature's
scenario count crossed this library's per-spec organization threshold. `catalog/catalog-crate-structure`
owns the crate's existence, its behavior-preservation guarantee, and its concept-level API shape;
this feature owns the running history of what gets ADDED to that `pub` set and why, each entry an
explicit reviewed edit to the crate's reachability probe at
`crates/lakehouse-catalog/tests/catalog_public_surface.rs`.

The Unity Parquet table-planning addition (`add-unity-parquet-table-routing`, issue #409) is
recorded in `unity-catalog/catalog-crate-public-surface-extensions-unity-parquet`, split out once this
feature's own scenario count crossed the same threshold.

## Background

* This delta (plan `add-native-unity-catalog-client`, issue #318) adds a shared `CatalogClient` trait and its catalog-neutral metadata types to the crate surface, so the engine holds ONE operation surface for every catalog kind. It SUPERSEDES the recorded "exactly these items SHALL be `pub`" enumeration in two directions: it ADMITS the trait, the neutral types, and the two client types the engine constructs, and it DEMOTES `list_namespace_tables` to crate-private now that the Iceberg client is its only caller. The Unity Catalog wire types stay crate-private, because the engine consumes only the neutral shape. The reachability probe is edited to name every added item and to assert both demotions, and the one-way dependency stays intact.
* This delta (plan `change-unity-listing-delta-base-filter`, correcting issue #318) extends the crate's enumerated public surface with two neutral types — `SkipReason` and `SkippedTable` — and reshapes the neutral `CatalogListing.skipped` field so every skipped entry carries the reason it was not admitted. It touches only the crate's structural surface; the listing behavior itself is owned by `unity-catalog/unity-catalog-client` and `unity-catalog/unity-catalog-create-virtual-schema`.
* This delta SUPERSEDES the clause of scenario "One shared catalog-client trait and its neutral types become the crate's operation surface" that described the listing type as "carrying the resolved tables plus the identifiers the catalog reported as not loadable". The listing type now carries the resolved tables plus a skipped set whose each element pairs a not-admitted identifier with a neutral skip reason. The Iceberg not-loadable case is preserved — it is now the `NotLoadableIcebergTable` reason rather than a bare identifier.
* `SkipReason` is `NotLoadableIcebergTable | NotDeltaBaseTable`, where `NotDeltaBaseTable` carries the disqualifying `table_type` or `data_source_format` as neutral detail. It is a neutral value shared by both catalog clients: the Iceberg REST client sets `NotLoadableIcebergTable`, the Unity Catalog client sets `NotDeltaBaseTable`, and the shared listing pipeline renders it without branching on catalog kind, per `unity-catalog/unity-catalog-create-virtual-schema`.
* The Unity-wire `data_source_format` field stays crate-private and MUST NOT appear on `SkipReason` or any other neutral type, per `unity-catalog/unity-catalog-client`; only its rendered value travels inside the `NotDeltaBaseTable` detail, not the field itself.
* The reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs` is edited — an explicit reviewed change — to name `SkipReason` and `SkippedTable` and to construct `CatalogListing.skipped` with a `SkippedTable` entry, so narrowing either below `pub` is a build failure rather than a silent gap. This mirrors how the two prior surface extensions (`One shared catalog-client trait ...` and `The native Unity Catalog client extends ...`) each edited the probe.
* **This delta adds ONE type and ONE conversion to the crate's public surface and is issue #330.** The credentials/addressing split gives both vended selectors a parameter carrying the CONNECTION's configured store `endpoint` and `region`; a parameter of a `pub fn` must itself be `pub`, so the enumerated public surface is superseded to admit it.
* **The addition is narrow by design, and its narrowness is the point.** The type carries exactly two addressing fields and no credential field. That absence is what preserves "a vended credential never falls back to a static one" now that the vended selectors' signatures admit a CONNECTION-derived value at all, so the type's field list IS the whitelist of CONNECTION fields permitted to cross into vended resolution.
* **Every shared policy and construction step stays crate-private.** The neutral vended S3 value shape, the scheme and storage-host derivations, the ADLS account-name derivation, both plaintext consent gates, and the two per-variant construction functions are mechanism steps of the two published vended entry points; publishing any of them would widen the surface this feature exists to narrow.
* The one-way dependency holds unchanged: no `lakehouse-catalog` source names `lakehouse-engine`, and the new type names no Exasol CONNECTION or virtual-schema-property delivery mechanism — it carries two plain strings the engine fills from a `ConnectionCreds` the crate already declares.
* **This delta adds ONE scenario and is issue #319.** It records the fifth explicit, reviewed
  extension of the crate's enumerated public surface, in the same shape as the four before it: the
  shared trait, the Delta-base skip reason, the Unity Catalog client, and the vended store-address
  type.
* **The one-way dependency is unchanged and is what forces this shape.** The Delta reader lives in
  `lakehouse-engine`, not here, because this crate MUST NOT name `iceberg`, `datafusion`, `arrow`,
  `parquet`, or `object_store` — and `delta_kernel` falls under the same rule for the same reason: it
  is an execution-layer reader, not catalog access. So the format TAG crosses the boundary while the
  format READER never does.
* **`CatalogClient` gains NO method.** The recorded clause that the trait "SHALL carry NO
  file-planning, scan, or data-file method in this plan, and its two listing operations SHALL be
  shaped so that adding one later is an ADDITIVE change that reshapes neither of them" holds
  unedited: the Delta path reaches its files through the engine-side `FormatReader` seam
  (`delta/delta-table-planning`) and reaches this crate only through the already-declared
  `load_table`, the already-public temporary-credentials request, and the already-public vended
  selector.
* **`resolve_uc_vended_storage` gains its first production caller** and stops being latent. Its
  signature, its shared policy home, and its crate-private construction steps are unchanged.
* **This delta widens `StaticStoreAddress` by one field and one accessor (#130).** The type gains `path_style` beside `endpoint` and `region`. The enumerated public surface grows by one method on an existing item, not by an item. The no-credential probe is unchanged: `path_style` names none of the forbidden spellings.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The neutral column's declaration slot extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog`, its recorded neutral column type carrying a name and a source-tagged type descriptor, and the external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`
* *WHEN* an engine-side catalog client, such as the direct-storage client, must hand the listing pipeline a column declaration it resolved from data files
* *THEN* the neutral column type SHALL gain ONE optional string field holding that engine-encoded declaration, documented as OPAQUE: this crate never reads, parses, or builds it, and every client this crate declares SHALL leave it absent
* *AND* the field SHALL be named for what it holds, a declaration, and MUST NOT be named for the Exasol `adapterNotes` channel or carry any other Exasol, CONNECTION, or virtual-schema-property concept, because the engine decides how a declaration reaches Exasol
* *AND* the field MUST NOT be a new public type, because a newtype would add a public item while buying no invariant this crate can check
* *AND* the reachability probe SHALL be edited, an explicit reviewed change to the probe file, to construct the neutral column with the field present and absent
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse_engine`, and the crate's manifest SHALL gain no dependency
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The Iceberg column source carries the whole Iceberg field through an explicit reviewed edit

* *GIVEN* the recorded neutral column source type, whose Iceberg variant carries the column's Iceberg type alone
* *WHEN* the engine declares each Iceberg column's logical field at `createVirtualSchema`, which needs the column's field id, its required flag, and its `initial-default` beside its type
* *THEN* the Iceberg variant SHALL carry the column's whole Iceberg field, as the Iceberg library's shared field reference, in place of its type alone, and the shared Iceberg-metadata projection every catalog kind uses SHALL fill it from the current schema's top-level field
* *AND* the variant count of the source type SHALL be unchanged, so the engine's exhaustive matches gain no arm
* *AND* the reachability probe SHALL be edited to construct the Iceberg variant from a field rather than a type
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The neutral table's catalog properties extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the recorded neutral table type and the external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`
* *WHEN* the engine declares a Unity Catalog Delta table's columns at `createVirtualSchema`, which needs the table-level `delta.columnMapping.mode` property Unity Catalog reports beside the table's columns
* *THEN* the neutral table type SHALL gain ONE field holding the table-level properties the catalog reported, as a string-to-string map copied verbatim, empty when the catalog reports none
* *AND* this crate SHALL interpret no key of that map, so no Delta property name appears in this crate's source, and the engine reads the one key it needs
* *AND* the Unity Catalog client SHALL fill the map from the same wire entry that supplies the table's columns, in the list sweep and in the single-table load, and every other client this crate declares SHALL leave it empty, because no engine path reads another catalog's properties
* *AND* the field MUST NOT be a new public type, because a newtype would add a public item while buying no invariant this crate can check
* *AND* the reachability probe SHALL be edited, an explicit reviewed change to the probe file, to construct the neutral table with a non-empty map
<!-- /DELTA:NEW -->
