# Feature: Catalog Crate Public Surface Extensions — Glue Catalog Kind

Records the reviewed extension of `lakehouse-catalog`'s public surface that lets the engine list and plan AWS Glue Data Catalog tables. It is the sibling of `vs-adapter/catalog-crate-public-surface-extensions-unity-parquet` and owns every addition made for the Glue catalog kind.

## Background

* Each addition is an explicit reviewed edit to the reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`.
* The Hive type string crosses the boundary verbatim, and only `lakehouse-engine` parses it.

## Scenarios

### Scenario: The Glue client and its neutral additions extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and the external-vantage reachability probe that fails to compile if an enumerated item is narrowed below `pub`
* *WHEN* the probe reaches each Glue addition from outside the crate
* *THEN* the crate SHALL export the Glue catalog session, which implements the shared `CatalogClient` trait and returns a table's partitions
* *AND* the crate SHALL export a neutral partition type carrying the partition's values keyed by partition column, its location, and a neutral partition format that is Parquet or names the unsupported input format, together with the Hive default-partition literal that reads NULL
* *AND* the crate SHALL export the Glue table-identifier parser, the dotted catalog identifier, and the redacting Iceberg metadata-file reader, so the engine reuses them instead of keeping its own copies
* *AND* the neutral column source type SHALL gain a Glue variant carrying the Hive type string verbatim, and the skip reason SHALL gain a Glue variant carrying the Glue value that decided the skip
* *AND* the neutral table SHALL gain an optional metadata location, which the Iceberg REST, Unity Catalog, and direct-storage clients SHALL leave absent
* *AND* the reachability probe SHALL be edited to construct and observe each addition, and that edit MUST NOT add a source-text assertion

### Scenario: No AWS SDK type crosses the crate boundary

* *GIVEN* the same additions
* *WHEN* the engine calls the Glue catalog session
* *THEN* no public signature of `lakehouse-catalog` SHALL name an `aws-sdk-glue` or `aws-smithy` type, so the engine depends on the neutral types alone
