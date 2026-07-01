# Feature: DataFusion-to-Exasol Type Mapping

Defines the single authoritative mapping from DataFusion/Arrow column types to Exasol SQL types, and the companion Iceberg-to-Arrow mapping used to build the logical schema the scan registers, so that every column an Iceberg table exposes is queryable through Exasol. Types Exasol supports natively map directly; types Exasol cannot represent (vectors, lists, structs, maps, binary, and out-of-range decimals) are serialized to JSON strings and surfaced as `VARCHAR`. The same mapping governs the `createVirtualSchema` schema declaration, the Arrow-to-Value conversion in the scan, and the logical schema carried into the scan spec, keeping declared and emitted types in agreement.

## Background

* The mapping is authoritative and shared: the `createVirtualSchema` declaration, the scan-time Arrow-to-Value conversion, and the logical schema carried into the scan spec all use it.
* Complex Arrow/Iceberg types (list, struct, map, binary) and out-of-range decimals map to a string-family type surfaced as JSON `VARCHAR`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Iceberg logical schema maps to Arrow types for scan registration

* *GIVEN* an Iceberg table's current schema whose fields include primitive types (int, long, double, string, boolean, date, timestamp) and complex/out-of-range types (list, struct, map, out-of-range decimal)
* *WHEN* the adapter derives the logical schema it carries into the scan spec
* *THEN* each Iceberg field SHALL map to the Arrow `DataType` the scan UDF registers for that column, consistent with the existing Iceberg-to-Exasol mapping (primitive types to their direct Arrow equivalents; complex and out-of-range types to a string-family Arrow type that surfaces as JSON `VARCHAR`)
* *AND* each mapped field SHALL preserve the source Iceberg field-id and its required/optional nullability
* *AND* the mapping used for the logical schema SHALL agree with the `createVirtualSchema` schema declaration so the declared Exasol column type and the registered Arrow type stay in agreement
<!-- /DELTA:NEW -->
