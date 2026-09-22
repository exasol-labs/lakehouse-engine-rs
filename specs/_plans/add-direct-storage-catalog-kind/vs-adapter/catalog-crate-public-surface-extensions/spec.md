# Feature: Catalog Crate Public Surface Extensions

Enumerates the public surface of the lakehouse-catalog crate and pins it from an external vantage. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta is issue #407. It adds three neutral variants to the crate's enumerated public surface. They are a Parquet table format, a Parquet column source carrying an Arrow tag string, and a skip reason naming the absence of a data file.
* The crate gains no source file, no manifest dependency, and no further public item. The client that produces these values is declared in `lakehouse-engine`. It reaches this crate only through the already-declared `CatalogClient` trait.
* The column source carries a TAG STRING rather than an Arrow type because `vs-adapter/catalog-crate-structure` forbids this crate's manifest from declaring `arrow`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The Parquet table format, the Parquet column source, and the no-data-file skip reason extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog`, its recorded closed table-format enum carrying one variant per format the engine can plan, its recorded neutral column source type, its recorded neutral skip reason, and the external-vantage reachability probe that fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the engine gains a reader that plans a plain directory of Parquet files and an enumeration that reports a directory holding no data file
* *THEN* the table-format enum SHALL gain a THIRD variant naming the Parquet format, SUPERSEDING the recorded two-variant enumeration, and the neutral skip reason SHALL gain ONE variant naming the absence of a data file, SUPERSEDING the recorded two-variant enumeration of that enum
* *AND* the neutral column source type SHALL gain a THIRD variant carrying the column's logical Arrow type as a TAG STRING of the engine's scan-spec tag vocabulary, and it MUST NOT carry an Arrow type value, because the manifest prohibition `vs-adapter/catalog-crate-structure` records forbids this crate from declaring `arrow`
* *AND* the added skip reason SHALL carry NO detail payload, because the identifier it is paired with already names the directory and no further catalog-specific string exists for a directory that simply holds no data file
* *AND* the engine-side tag RENDERER and the engine-side tag PARSER SHALL be the ONLY producer and the ONLY consumer of that variant's string, so no third site spells or interprets the format, because this crate cannot check a string whose vocabulary lives in `lakehouse-engine` and two independent spellings would drift with nothing enforcing agreement
* *AND* a test SHALL assert that EVERY tag the direct-storage client emits parses back to the Arrow type it was rendered from, so the unenforceable cross-crate agreement is held by a round-trip assertion rather than by convention
* *AND* neither addition SHALL name a `CatalogKind`, an Exasol CONNECTION or virtual-schema-property delivery mechanism, or any `lakehouse-engine` symbol, so both stay neutral values of this crate
* *AND* the crate SHALL gain NO source file, NO manifest dependency, and NO further public item for the direct-storage kind, because the client that produces these values is declared in `lakehouse-engine` and reaches this crate only through the already-declared `CatalogClient` trait and its neutral types
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to CONSTRUCT and OBSERVE the three added variants from its external vantage, exactly as it already constructs the recorded ones, so narrowing any of the three enums or removing a variant fails to COMPILE rather than passing silently
* *AND* that edit MUST NOT add a source-TEXT assertion over any enum's variant list, because a structural invariant enforced by matching production source text is not one this project accepts; the probe's value here is that it compiles against the crate from outside it
* *AND* the probe's existing demotion assertions — that the crate declares no `pub fn` for the demoted vended-mechanism functions and no `pub fn list_namespace_tables` — SHALL remain intact and unweakened
* *AND* the table-format enum SHALL NOT gain an exhaustive production match as part of this change, and the ONE existing guard — the Delta reader-selection arm's equality check, which refuses a table whose reported format is not Delta — SHALL be recorded as the guard that a Parquet-tagged table is not misrouted, because a reader expecting a compile error would otherwise read its absence as an oversight
<!-- /DELTA:NEW -->
