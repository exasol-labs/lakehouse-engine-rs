# Feature: Catalog Crate Structure

Moves the Iceberg REST catalog access layer — catalog authentication, the per-query HTTP session, the `loadTable` GET, namespace enumeration, vended-storage resolution, and the credential types and redaction those need — out of `lakehouse-engine` into the standalone `lakehouse-catalog` crate, so a crate boundary rather than a module-private visibility rule decides what the planning layer may reach.

This is the catalog layer's structural feature, the sibling of `vs-adapter/adapter-module-structure`, `vs-adapter/pushdown-module-structure`, and `datafusion-scan/scan-module-structure`. It exists because the boundary it draws spans three features at once — `vs-adapter/pushdown-catalog-session`, `vs-adapter/rest-catalog-oauth-auth`, and `vs-adapter/pushdown-planning-cloud-credentials` — so no single behavioral feature can own it without leaking a structural decision across its edge.

## Background

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

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
