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

## Scenarios

### Scenario: One shared catalog-client trait and its neutral types become the crate's operation surface

* *GIVEN* the recorded clause enumerating "exactly these items SHALL be `pub`" on `lakehouse-catalog`, and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the shared catalog-client abstraction lands, so the engine adapter runs one listing pipeline for every catalog kind
* *THEN* the crate SHALL declare a trait-object-usable `CatalogClient` trait as the ONLY operation surface the engine's createVirtualSchema path uses to enumerate a namespace or load one table's metadata, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit that trait, the two client types the engine constructs, and the catalog-neutral metadata types the trait returns — a table identifier carrying its namespace as SEGMENTS (never a pre-joined dotted string, because the engine's flattening and `TABLE_MAP` construction consume segments and re-splitting would introduce a separator ambiguity neither catalog guarantees against) plus its name; a table-metadata type carrying that identifier, its table type, its optional storage location, and its ordered columns; a column type carrying its name and a type descriptor; and a listing type carrying the resolved tables plus a skipped set whose each element pairs an identifier the catalog did not admit as a listable table with a neutral skip reason
* *AND* the trait SHALL carry NO file-planning, scan, or data-file method in this plan, and its two listing operations SHALL be shaped so that adding one later is an ADDITIVE change that reshapes neither of them
* *AND* the neutral column's type descriptor SHALL be SOURCE-TAGGED — carrying the Iceberg type for an Iceberg REST table and the Unity Catalog type name for a Unity Catalog table — and MUST NOT be pre-mapped to an Exasol type inside this crate, because `lakehouse-catalog` MUST NOT name the Exasol delivery mechanism or the engine's type-mapping home, and because discarding the source type here would destroy the fidelity the deferred Delta type work (#322) reads
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the trait and every neutral type, and to assert that each client type is usable as a boxed `CatalogClient`, so narrowing one below `pub` or breaking trait-object compatibility is a build failure rather than a silent gap

### Scenario: Namespace enumeration is demoted once the Iceberg client is its only caller

* *GIVEN* the recorded clause naming `list_namespace_tables` among the items that SHALL be `pub` on `lakehouse-catalog`, and its recorded rationale that it reached crate-private-adjacent visibility only to serve one engine-side caller
* *WHEN* the Iceberg REST catalog client implements `CatalogClient` and calls `list_namespace_tables` from inside the crate, leaving the engine with no direct call site
* *THEN* `list_namespace_tables` SHALL be DEMOTED to crate-private and removed from the crate's `pub` re-exports, and the recorded `pub` enumeration SHALL be SUPERSEDED accordingly, because a `pub` function whose only caller is in-crate widens the surface this feature exists to narrow
* *AND* the reachability probe SHALL drop it from the enumerated `pub` set and SHALL assert the crate declares no `pub fn list_namespace_tables`, joining the existing demotion assertions for the vended-mechanism functions, which SHALL remain intact and unweakened
* *AND* the demotion SHALL change no behavior: the same namespaces are enumerated, the same identifiers are returned in the same order, and the same errors are produced under both the signed and unsigned paths

### Scenario: The native Unity Catalog client extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the native Unity Catalog client and its vended-credentials selector land in the crate
* *THEN* the crate SHALL add to its public surface exactly the concept-level Unity Catalog items the engine adapter consumes — the `UnityCatalogSession` type and its constructor, the temporary-credentials type `resolve_uc_vended_storage` consumes, and the `resolve_uc_vended_storage` function — each re-exported at the crate root, and it SHALL likewise expose the Iceberg REST catalog client type and its constructor, which are the two types the engine's single construction site builds; both client types SHALL implement `CatalogClient` and the engine adapter SHALL reach every enumeration and table-load operation THROUGH that trait, so neither type needs an inherent enumeration or table-load method on the public surface
* *AND* the Unity Catalog WIRE types (the deserialized `GET /tables` table and column shapes), the authentication strategy, the session's catalog and schema enumeration, and every request-construction step SHALL stay crate-private — so making the session reachable exposes neither auth internals nor a Unity-specific shape, mirroring how `CatalogSession`'s fields stay private; publishing the wire shape would let a consumer branch on which catalog it is talking to, which is the branch this trait exists to remove
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name every added Unity Catalog public item, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions SHALL remain intact and unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse-engine`, and neither the `CatalogClient` trait, its neutral types, nor the Unity Catalog client SHALL name the Exasol CONNECTION or virtual-schema-property delivery mechanism, which stays in the engine adapter

### Scenario: The Delta-base skip reason extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the Delta-base listing filter's neutral skip-reason model lands, so the shared listing pipeline warns per excluded entry without branching on catalog kind
* *THEN* the crate SHALL add to its public surface exactly two items — the `SkipReason` enum (`NotLoadableIcebergTable`; `NotDeltaBaseTable` carrying the disqualifying `table_type` or `data_source_format` as neutral detail) and the `SkippedTable` type pairing a `CatalogTableIdent` with a `SkipReason` — each re-exported at the crate root, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit them
* *AND* the neutral `CatalogListing.skipped` field SHALL change from `Vec<CatalogTableIdent>` to `Vec<SkippedTable>`, so every skipped entry carries the reason it was not admitted; the Iceberg REST client SHALL set `NotLoadableIcebergTable` and the Unity Catalog client SHALL set `NotDeltaBaseTable`, and the shared listing pipeline SHALL NOT branch on catalog kind
* *AND* `SkipReason` SHALL be a neutral value that names NO `CatalogKind`, NO Exasol CONNECTION or virtual-schema-property delivery mechanism, and NO `lakehouse-engine` symbol, and the Unity-wire `data_source_format` field SHALL stay crate-private and MUST NOT appear on `SkipReason` or any other neutral type — only its rendered value travels inside the `NotDeltaBaseTable` detail, so the one-way dependency holds
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name `SkipReason` and `SkippedTable` and to construct `CatalogListing.skipped` with a `SkippedTable` entry, so narrowing either below `pub` is a build failure rather than a silent gap

### Scenario: The vended store-address type extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the credentials/addressing split gives both vended selectors a CONNECTION-configured store-address parameter
* *THEN* the crate SHALL add to its public surface exactly ONE type — a store-address value declaring EXACTLY the CONNECTION's `endpoint` and `region` — plus its `Default` and exactly ONE conversion from `ConnectionCreds`, re-exported at the crate root, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit them
* *AND* the type SHALL declare NO credential field, and the reachability probe SHALL assert from that type's own source that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password` — so widening it into a second credential path is a test failure rather than a silent regression of the vended-only credential guarantee
* *AND* the conversion from `ConnectionCreds` SHALL be the ONE place that decides which CONNECTION fields are permitted to cross into vended resolution, so no call site builds that value field-by-field and the decision cannot be re-litigated per caller — enforced by the type's own field privacy rather than by prose, per `vs-adapter/storage-backend-enum` § "The vended selectors take a store address that cannot carry a credential": the added type exposes its two fields through accessors only, so outside the crate the `Default` and that conversion are the only constructions reachable at all
* *AND* every shared vended policy and construction step SHALL stay crate-private and MUST NOT be re-exported: the neutral vended S3 value shape, the URI-scheme and storage-host derivations, the ADLS account-name derivation, the two plaintext-transport consent gates, and the per-variant construction functions
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the added items, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions SHALL remain intact and unweakened

### Scenario: The neutral table's format tag and vending key extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability
  probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any
  enumerated item is narrowed below `pub`
* *WHEN* the neutral table gains the table-FORMAT tag and the credential-vending key that the
  engine-side Delta format reader consumes
* *THEN* the crate SHALL add to its public surface exactly ONE type — a closed table-format enum with
  one variant per format the engine can plan (Iceberg, Delta) — re-exported at the crate root, and the
  recorded `pub` enumeration SHALL be SUPERSEDED to admit it
* *AND* the recorded clause enumerating the neutral table-metadata type as "carrying that identifier,
  its table type, its optional storage location, and its ordered columns" SHALL be SUPERSEDED to add
  its table format and its OPTIONAL credential-vending key, because that enumeration is the contract
  a consumer reads and an incomplete one is indistinguishable from a forbidden field
* *AND* the vending key SHALL be a plain optional string documented as OPAQUE, and SHALL NOT be a new
  public type, because a newtype would put a Unity Catalog concept on the crate's enumerated surface
  while buying no invariant the neutral table's own privacy does not already give
* *AND* the Iceberg REST catalog client SHALL set the Iceberg format tag and an ABSENT vending key on
  every neutral table it returns, because it vends storage credentials inline with the table's own
  metadata and needs no per-table scope — so neither field forks the listing pipeline
* *AND* the raw Unity Catalog `data_source_format` and `table_id` wire fields SHALL stay crate-private
  and MUST NOT appear in any neutral type, so only their neutral projections cross the boundary
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to
  name the added format enum and to construct the neutral table with both added fields, so narrowing
  the enum below `pub` or dropping a field is a build failure rather than a silent gap
* *AND* the existing demotion assertions of that probe — that the crate declares no `pub fn` for the
  demoted vended-mechanism functions and no `pub fn list_namespace_tables` — SHALL remain intact and
  unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name
  `lakehouse_engine`, the crate's manifest MUST NOT declare `delta_kernel` or
  `delta_kernel_default_engine`, and neither added field SHALL name the Exasol CONNECTION or
  virtual-schema-property delivery mechanism
