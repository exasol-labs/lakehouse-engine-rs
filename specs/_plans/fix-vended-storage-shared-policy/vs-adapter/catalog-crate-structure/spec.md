# Feature: Catalog Crate Structure

Splits Iceberg REST catalog access into the workspace-internal `lakehouse-catalog` crate with a concept-level public surface and every mechanism step crate-private.

## Background

* **This delta adds ONE type and ONE conversion to the crate's public surface and is issue #330.** The credentials/addressing split gives both vended selectors a parameter carrying the CONNECTION's configured store `endpoint` and `region`; a parameter of a `pub fn` must itself be `pub`, so the enumerated public surface is superseded to admit it.
* **The addition is narrow by design, and its narrowness is the point.** The type carries exactly two addressing fields and no credential field. That absence is what preserves "a vended credential never falls back to a static one" now that the vended selectors' signatures admit a CONNECTION-derived value at all, so the type's field list IS the whitelist of CONNECTION fields permitted to cross into vended resolution.
* **Every shared policy and construction step stays crate-private.** The neutral vended S3 value shape, the scheme and storage-host derivations, the ADLS account-name derivation, both plaintext consent gates, and the two per-variant construction functions are mechanism steps of the two published vended entry points; publishing any of them would widen the surface this feature exists to narrow.
* The one-way dependency holds unchanged: no `lakehouse-catalog` source names `lakehouse-engine`, and the new type names no Exasol CONNECTION or virtual-schema-property delivery mechanism — it carries two plain strings the engine fills from a `ConnectionCreds` the crate already declares.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The vended store-address type extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the credentials/addressing split gives both vended selectors a CONNECTION-configured store-address parameter
* *THEN* the crate SHALL add to its public surface exactly ONE type — a store-address value declaring EXACTLY the CONNECTION's `endpoint` and `region` — plus its `Default` and exactly ONE conversion from `ConnectionCreds`, re-exported at the crate root, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit them
* *AND* the type SHALL declare NO credential field, and the reachability probe SHALL assert from that type's own source that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password` — so widening it into a second credential path is a test failure rather than a silent regression of the vended-only credential guarantee
* *AND* the conversion from `ConnectionCreds` SHALL be the ONE place that decides which CONNECTION fields are permitted to cross into vended resolution, so no call site builds that value field-by-field and the decision cannot be re-litigated per caller — enforced by the type's own field privacy rather than by prose, per `vs-adapter/storage-backend-enum` § "The vended selectors take a store address that cannot carry a credential": the added type exposes its two fields through accessors only, so outside the crate the `Default` and that conversion are the only constructions reachable at all
* *AND* every shared vended policy and construction step SHALL stay crate-private and MUST NOT be re-exported: the neutral vended S3 value shape, the URI-scheme and storage-host derivations, the ADLS account-name derivation, the two plaintext-transport consent gates, and the per-variant construction functions
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the added items, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions SHALL remain intact and unweakened
<!-- /DELTA:NEW -->
