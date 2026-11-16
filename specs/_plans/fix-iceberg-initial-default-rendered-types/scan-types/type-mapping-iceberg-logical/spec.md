# Feature: Iceberg Logical Schema and Catalog Decimal Mapping

Defines the Iceberg-to-Arrow logical schema the scan registers and the catalog-declared decimal guard that keeps the Exasol declaration and the logical Arrow type in lockstep. The general DataFusion-to-Exasol mapping lives in `scan-types/type-mapping`.

## Background

* A catalog-declared decimal (an Iceberg `PrimitiveType::Decimal` or a Unity Catalog `DECIMAL`) maps by the decimal domain `scan-types/type-mapping` states for every decimal.
* `iceberg_type_to_arrow` is not recursive. A column's logical Arrow type stays `Utf8` for every list, struct, and map, so the JSON string is the column's type everywhere it is read: the registered DataFusion schema, the `ScanSpec::logical_schema` tag vocabulary, the pushdown planner's `needs_json_fallback` decisions, and the `VARCHAR(2000000)` declaration. A nested Arrow type would make the column a real nested type in DataFusion, which has no comparison, ordering, hashing, or aggregation operator for `Struct` or `Map`. The adapter would then have to decline every WHERE predicate, GROUP BY key, aggregate argument, and join condition on such a column at five decision sites, and resolve the logical schema before the filter decision.
* The nested field tree reaches the scan on a separate field. A rendering keyed by a file's physical nested names would emit a column-mapped Delta table's `col-…` identifiers as JSON keys, so `LogicalField` carries an optional, format-neutral nested descriptor. It names each nested field's logical name and the one binding key the format's column mapping selects (`field_id`, `physical_name`, or identity), recursively. It is not a type, and the column's type remains `Utf8`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR

* *GIVEN* a column whose catalog-declared type is a decimal carrying an unsigned precision `p` and an unsigned scale `s`, an Iceberg `PrimitiveType::Decimal { precision, scale }` or a Unity Catalog `decimal(p,s)` parsed from the column's `type_json`
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `DECIMAL(p,s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `VARCHAR(2000000)` otherwise, so `p = 0` yields `VARCHAR(2000000)` rather than the invalid `DECIMAL(0,0)` and `s > p` yields `VARCHAR(2000000)` rather than an invalid shape such as `DECIMAL(5,10)`
* *AND* the domain check SHALL have one owner, the Exasol type's DECIMAL constructor, which both catalog kinds, the logical Arrow mapping, and the `initial-default` encoding, which picks the raw mantissa or the rendered text (`scan-read-path/scan-execution-field-id-projection-absent-fields`), read, so no two of them can classify a pair differently
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on either bad pair, the `VARCHAR(2000000)` fallback absorbs them exactly as it absorbs `p > 36`, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* `(18,4)`, `(10,2)`, `(1,0)`, and the boundary pair `(36,36)` SHALL map to `DECIMAL(18,4)`, `DECIMAL(10,2)`, `DECIMAL(1,0)`, and `DECIMAL(36,36)`, and `(38,10)` and `(18,37)` SHALL map to `VARCHAR(2000000)`

<!-- /DELTA:CHANGED -->
