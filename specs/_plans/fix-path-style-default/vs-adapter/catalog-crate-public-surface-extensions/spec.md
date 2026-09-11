# Feature: Catalog Crate Public Surface Extensions

Tracks each explicit, reviewed extension of `lakehouse-catalog`'s enumerated public surface after
the crate boundary itself was drawn — the shared `CatalogClient` trait, a demotion once a caller
moved in-crate, and four narrow additions the engine-side format and credential work required.

## Background

<!-- DELTA:NEW -->
* **This delta widens `StaticStoreAddress` by one field and one accessor (#130).** The type gains `path_style` beside `endpoint` and `region`. The enumerated public surface grows by one method on an existing item, not by an item. The no-credential probe is unchanged: `path_style` names none of the forbidden spellings.
<!-- /DELTA:NEW -->

## Scenarios

### Scenario: The vended store-address type extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the credentials/addressing split gives both vended selectors a CONNECTION-configured store-address parameter
<!-- DELTA:CHANGED -->
* *THEN* the crate SHALL add to its public surface exactly ONE type — a store-address value declaring EXACTLY the CONNECTION's `endpoint`, `region`, and `path_style` — plus its `Default`, one reading accessor per field, and exactly ONE conversion from `ConnectionCreds`, re-exported at the crate root — SUPERSEDING the recorded clause naming EXACTLY `endpoint` and `region` (#130)
<!-- /DELTA:CHANGED -->
* *AND* the type SHALL declare NO credential field, and the reachability probe SHALL assert from that type's own source that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password`
<!-- DELTA:CHANGED -->
* *AND* the conversion from `ConnectionCreds` SHALL be the ONE place that decides which CONNECTION fields are permitted to cross into vended resolution — enforced by the type's own field privacy per `vs-adapter/storage-backend-enum`
<!-- /DELTA:CHANGED -->
* *AND* every shared vended policy and construction step SHALL stay crate-private and MUST NOT be re-exported
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the added items, and its existing demotion assertions SHALL remain intact and unweakened
