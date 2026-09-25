# Feature: Catalog Crate Public Surface Extensions

Tracks each explicit, reviewed extension of `lakehouse-catalog`'s enumerated public surface after
the crate boundary itself was drawn — the shared `CatalogClient` trait, a demotion once a caller
moved in-crate, and four narrow additions the engine-side format and credential work required.

This is the sibling of `vs-adapter/catalog-crate-structure`, split out once the base feature's
scenario count crossed this library's per-spec organization threshold. `vs-adapter/catalog-crate-structure`
owns the crate's existence, its behavior-preservation guarantee, and its concept-level API shape;
this feature owns the running history of what gets ADDED to that `pub` set and why, each entry an
explicit reviewed edit to the crate's reachability probe at
`crates/lakehouse-catalog/tests/catalog_public_surface.rs`.

## Background

* This delta (plan `add-native-unity-catalog-client`, issue #318) adds a shared `CatalogClient` trait and its catalog-neutral metadata types to the crate surface, so the engine holds ONE operation surface for every catalog kind. It SUPERSEDES the recorded "exactly these items SHALL be `pub`" enumeration in two directions: it ADMITS the trait, the neutral types, and the two client types the engine constructs, and it DEMOTES `list_namespace_tables` to crate-private now that the Iceberg client is its only caller. The Unity Catalog wire types stay crate-private, because the engine consumes only the neutral shape. The reachability probe is edited to name every added item and to assert both demotions, and the one-way dependency stays intact.
* This delta (plan `change-unity-listing-delta-base-filter`, correcting issue #318) extends the crate's enumerated public surface with two neutral types — `SkipReason` and `SkippedTable` — and reshapes the neutral `CatalogListing.skipped` field so every skipped entry carries the reason it was not admitted. It touches only the crate's structural surface; the listing behavior itself is owned by `vs-adapter/unity-catalog-client` and `vs-adapter/unity-catalog-create-virtual-schema`.
* This delta SUPERSEDES the clause of scenario "One shared catalog-client trait and its neutral types become the crate's operation surface" that described the listing type as "carrying the resolved tables plus the identifiers the catalog reported as not loadable". The listing type now carries the resolved tables plus a skipped set whose each element pairs a not-admitted identifier with a neutral skip reason. The Iceberg not-loadable case is preserved — it is now the `NotLoadableIcebergTable` reason rather than a bare identifier.
* `SkipReason` is `NotLoadableIcebergTable | NotDeltaBaseTable`, where `NotDeltaBaseTable` carries the disqualifying `table_type` or `data_source_format` as neutral detail. It is a neutral value shared by both catalog clients: the Iceberg REST client sets `NotLoadableIcebergTable`, the Unity Catalog client sets `NotDeltaBaseTable`, and the shared listing pipeline renders it without branching on catalog kind, per `vs-adapter/unity-catalog-create-virtual-schema`.
* The Unity-wire `data_source_format` field stays crate-private and MUST NOT appear on `SkipReason` or any other neutral type, per `vs-adapter/unity-catalog-client`; only its rendered value travels inside the `NotDeltaBaseTable` detail, not the field itself.
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
  (`vs-adapter/delta-table-planning`) and reaches this crate only through the already-declared
  `load_table`, the already-public temporary-credentials request, and the already-public vended
  selector.
* **`resolve_uc_vended_storage` gains its first production caller** and stops being latent. Its
  signature, its shared policy home, and its crate-private construction steps are unchanged.
* **This delta widens `StaticStoreAddress` by one field and one accessor (#130).** The type gains `path_style` beside `endpoint` and `region`. The enumerated public surface grows by one method on an existing item, not by an item. The no-credential probe is unchanged: `path_style` names none of the forbidden spellings.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The neutral table's partition columns and the Unity column's type descriptor extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog`, its recorded neutral table and neutral column source type, and the external-vantage reachability probe that fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the engine gains a reader that plans a Unity Catalog Parquet table from the columns and partition columns its catalog declares
* *THEN* the neutral table SHALL gain its ordered partition-column names, SUPERSEDING the recorded enumeration of that type's fields, and the Unity Catalog variant of the neutral column source type SHALL gain the column's optional Spark `StructField` JSON as a plain string
* *AND* the Iceberg REST client and the direct-storage client SHALL set an EMPTY partition-column list on every neutral table they return, because neither catalog declares partition columns by name: the Iceberg reader reads its partition spec from the table metadata, and the direct-storage reader discovers its keys from the paths
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The addition stays narrow, and its reviewed probe supersedes the format-guard clause it replaces

* *GIVEN* the same two neutral-type additions and the same external-vantage reachability probe
* *WHEN* the engine consumes the new fields to plan a Unity Catalog Parquet table
* *THEN* neither addition SHALL be a new public type, the raw Unity Catalog `partition_index` wire field SHALL stay crate-private, and the `type_json` string SHALL cross the boundary verbatim and be parsed only in `lakehouse-engine`, because this crate MUST NOT declare `delta_kernel`
* *AND* the reachability probe SHALL be edited, as an explicit reviewed change to the probe file, to construct and observe both additions from its external vantage, and that edit MUST NOT add a source-text assertion
* *AND* the recorded clause that names the Delta reader-selection arm's equality check as the guard against misrouting a Parquet-tagged table SHALL be SUPERSEDED, because the Unity scan source now selects its reader by an exhaustive match on the format tag (`vs-adapter/delta-table-planning`)
<!-- /DELTA:NEW -->
