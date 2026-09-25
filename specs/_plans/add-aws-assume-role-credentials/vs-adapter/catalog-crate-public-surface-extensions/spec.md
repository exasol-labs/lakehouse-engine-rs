# Feature: Catalog Crate Public Surface Extensions

Tracks each explicit, reviewed extension of `lakehouse-catalog`'s enumerated public surface after
the crate boundary itself was drawn — the shared `CatalogClient` trait, a demotion once a caller
moved in-crate, and the narrow additions that the engine-side format, credential, and catalog-signing
work required.

This is the sibling of `vs-adapter/catalog-crate-structure`, split out once the base feature's
scenario count crossed this library's per-spec organization threshold. `vs-adapter/catalog-crate-structure`
owns the crate's existence, its behavior-preservation guarantee, and its concept-level API shape;
this feature owns the running history of what gets ADDED to that `pub` set and why, each entry an
explicit reviewed edit to the crate's reachability probe at
`crates/lakehouse-catalog/tests/catalog_public_surface.rs`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/catalog-crate-public-surface-extensions/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The AWS identity resolver extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the adapter gains AWS IAM role assumption under `vs-adapter/connection-credentials-assume-role`
* *THEN* the crate SHALL add to its public surface exactly ONE async function that resolves the AWS identity a request acts as, re-exported at the crate root, and `ConnectionCreds` SHALL gain the `aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint` fields beside its existing `pub` fields
* *AND* the STS endpoint and region resolution, the request construction, the `sts` signing, and the response parsing SHALL stay crate-private, and the probe SHALL be edited, as an explicit reviewed change, to name the added items while its existing demotion assertions stay intact and unweakened
* *AND* the crate SHALL gain exactly ONE manifest dependency, `quick-xml` at the version `object_store` already compiles into the workspace, so `Cargo.lock` gains no new package
* *AND* no `lakehouse-catalog` source file SHALL name `lakehouse_engine`, and the added items SHALL name no Exasol CONNECTION or virtual-schema-property delivery mechanism
<!-- /DELTA:NEW -->
