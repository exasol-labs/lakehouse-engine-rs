# Feature: Unity Catalog Native REST Client: Table Properties

Extends `unity-catalog/unity-catalog-client` with the carriage of each table's properties from the list sweep and the single-table load.

This is a sibling of `unity-catalog/unity-catalog-client`, split out once that feature's scenario count crossed this library's per-spec organization threshold. The parent feature keeps its original scope.

## Background

* The scenarios here share the parent feature's Background.

## Scenarios

### Scenario: The client carries each table's properties verbatim from the list sweep and the single-table load

* *GIVEN* a `UnityCatalogSession` over a Unity Catalog namespace holding a Delta base table registered with `properties` holding `delta.columnMapping.mode` set to `name` and one more key, and a Parquet base table registered with no properties
* *WHEN* the adapter enumerates the namespace through the shared trait, and a caller then loads the Delta table through the single-table load
* *THEN* the neutral Delta table SHALL carry both properties verbatim, keys and values unchanged, and the neutral Parquet table SHALL carry an empty map
* *AND* the single-table load SHALL carry the same map for the same entry, because both operations convert the wire entry through one conversion
* *AND* the client SHALL issue no request beyond the list sweep to obtain properties, and SHALL interpret no property key
