# Feature: Catalog Crate Public Surface Extensions: Declaration and Table Properties

Extends `catalog/catalog-crate-public-surface-extensions` with the reviewed public-surface additions that carry a table's declaration through the crate: the neutral column's declaration slot, the whole Iceberg field, and the neutral table's catalog properties.

This is a sibling of `catalog/catalog-crate-public-surface-extensions`, split out once that feature's scenario count crossed this library's per-spec organization threshold. The parent feature keeps its original scope.

## Background

* The scenarios here share the parent feature's Background.

## Scenarios

### Scenario: The neutral column's declaration slot extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog`, its recorded neutral column type carrying a name and a source-tagged type descriptor, and the external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`
* *WHEN* an engine-side catalog client, such as the direct-storage client, must hand the listing pipeline a column declaration it resolved from data files
* *THEN* the neutral column type SHALL gain ONE optional string field holding that engine-encoded declaration, documented as OPAQUE: this crate never reads, parses, or builds it, and every client this crate declares SHALL leave it absent
* *AND* the field SHALL be named for what it holds, a declaration, and MUST NOT be named for the Exasol `adapterNotes` channel or carry any other Exasol, CONNECTION, or virtual-schema-property concept, because the engine decides how a declaration reaches Exasol
* *AND* the field MUST NOT be a new public type, because a newtype would add a public item while buying no invariant this crate can check
* *AND* the reachability probe SHALL be edited, an explicit reviewed change to the probe file, to construct the neutral column with the field present and absent
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse_engine`, and the crate's manifest SHALL gain no dependency

### Scenario: The Iceberg column source carries the whole Iceberg field through an explicit reviewed edit

* *GIVEN* the recorded neutral column source type, whose Iceberg variant carries the column's Iceberg type alone
* *WHEN* the engine declares each Iceberg column's logical field at `createVirtualSchema`, which needs the column's field id, its required flag, and its `initial-default` beside its type
* *THEN* the Iceberg variant SHALL carry the column's whole Iceberg field, as the Iceberg library's shared field reference, in place of its type alone, and the shared Iceberg-metadata projection every catalog kind uses SHALL fill it from the current schema's top-level field
* *AND* the variant count of the source type SHALL be unchanged, so the engine's exhaustive matches gain no arm
* *AND* the reachability probe SHALL be edited to construct the Iceberg variant from a field rather than a type

### Scenario: The neutral table's catalog properties extend the crate's public surface through an explicit reviewed edit

* *GIVEN* the recorded neutral table type and the external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`
* *WHEN* the engine declares a Unity Catalog Delta table's columns at `createVirtualSchema`, which needs the table-level `delta.columnMapping.mode` property Unity Catalog reports beside the table's columns
* *THEN* the neutral table type SHALL gain ONE field holding the table-level properties the catalog reported, as a string-to-string map copied verbatim, empty when the catalog reports none
* *AND* this crate SHALL interpret no key of that map, so no Delta property name appears in this crate's source, and the engine reads the one key it needs
* *AND* the Unity Catalog client SHALL fill the map from the same wire entry that supplies the table's columns, in the list sweep and in the single-table load, and every other client this crate declares SHALL leave it empty, because no engine path reads another catalog's properties
* *AND* the field MUST NOT be a new public type, because a newtype would add a public item while buying no invariant this crate can check
* *AND* the reachability probe SHALL be edited, an explicit reviewed change to the probe file, to construct the neutral table with a non-empty map
