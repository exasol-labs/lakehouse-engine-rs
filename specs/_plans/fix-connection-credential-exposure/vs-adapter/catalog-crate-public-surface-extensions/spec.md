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

* **This delta adds ONE type and its two operations to the crate's enumerated public surface, and is issue #135.** It records the sixth explicit, reviewed extension, in the same shape as the five before it: the shared trait, the Delta-base skip reason, the Unity Catalog client, the vended store-address type, and the neutral table's format tag.
* **The reason is a second reader, not a new capability.** `vs-adapter/scan-spec-credential-reference` gives the scan UDF a CONNECTION password to turn into a storage backend. The selection rule already exists, adapter-side, inside `storage_block`. Publishing a storage-credential PROJECTION type plus that ONE selector is what lets both readers reach one decision without the scan path calling into the adapter module.
* **`vs-adapter/catalog-crate-structure`'s "SHALL stay in `lakehouse_engine::adapter::connection`" clause is SATISFIED, not superseded.** `read_connection`, `validate_creds`, `parse_creds`, `storage_block`, `catalog_block`, and `REQUIRED_KEY` all stay where that clause pins them. The crate gains a credential TYPE and a selector over it, and names no Exasol CONNECTION or virtual-schema-property delivery mechanism — exactly the boundary the vended store-address type already sits on.
* **The addition's narrowness is the point.** The type declares exactly the nine storage fields and no catalog-authentication field, and that absence is what preserves `vs-adapter/connection-credentials-catalog-auth`'s guarantee once the scan UDF reads a CONNECTION password at all. The type's field list IS the whitelist of password fields permitted to cross into the UDF.
* **The `pub` enumeration at `catalog-crate-structure` is exhaustive and the probe names every member**, so admitting a type without editing both is a silent widening. This delta does both.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The storage-credential projection extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the scan UDF becomes a second reader of a CONNECTION password under `vs-adapter/scan-spec-credential-reference`
* *THEN* the crate SHALL add to its public surface exactly ONE type — a storage-credential projection declaring EXACTLY `endpoint`, `region`, `access_key`, `secret_key`, `session_token`, `path_style`, `account_name`, `account_key`, and `sas_token` — plus its deserialization from a JSON credential document and its ONE backend selector, re-exported at the crate root, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit them
* *AND* the type SHALL declare NO catalog-authentication field, and the probe SHALL assert from that type's own source that its declaration names no field spelled `token`, `client_id`, `client_secret`, `oauth2_server_uri`, `scope`, `warehouse`, `use_sigv4`, or `use_vended_credentials` — the same source-level form the probe already applies to the vended store-address type
* *AND* exactly ONE conversion from `ConnectionCreds` SHALL be declared beside the type, so the adapter reaches the selector through the projection and no call site builds the projection field-by-field
* *AND* the backend selector SHALL be the ONLY declaration of the CONNECTION-to-backend selection rule in either crate, so `storage_block` retains no selection logic of its own while keeping its adapter-module home
* *AND* the probe SHALL be edited — an explicit reviewed change to the probe file — to name the added items, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions and no `pub fn list_namespace_tables` SHALL remain intact and unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse_engine`, and the added type SHALL name no Exasol CONNECTION or virtual-schema-property delivery mechanism
<!-- /DELTA:NEW -->
