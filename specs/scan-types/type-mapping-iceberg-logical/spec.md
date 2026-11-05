# Feature: Iceberg Logical Schema and Catalog Decimal Mapping

Defines the Iceberg-to-Arrow logical schema the scan registers and the catalog-declared decimal guard that keeps the Exasol declaration and the logical Arrow type in lockstep. The general DataFusion-to-Exasol mapping lives in `scan-types/type-mapping`.

## Background

* A catalog-declared decimal (an Iceberg `PrimitiveType::Decimal` or a Unity Catalog `DECIMAL`) maps by the decimal domain `scan-types/type-mapping` states for every decimal.
* `iceberg_type_to_arrow` is not recursive. A column's logical Arrow type stays `Utf8` for every list, struct, and map, so the JSON string is the column's type everywhere it is read: the registered DataFusion schema, the `ScanSpec::logical_schema` tag vocabulary, the pushdown planner's `needs_json_fallback` decisions, and the `VARCHAR(2000000)` declaration. A nested Arrow type would make the column a real nested type in DataFusion, which has no comparison, ordering, hashing, or aggregation operator for `Struct` or `Map`. The adapter would then have to decline every WHERE predicate, GROUP BY key, aggregate argument, and join condition on such a column at five decision sites, and resolve the logical schema before the filter decision.
* The nested field tree reaches the scan on a separate field. A rendering keyed by a file's physical nested names would emit a column-mapped Delta table's `col-…` identifiers as JSON keys, so `LogicalField` carries an optional, format-neutral nested descriptor. It names each nested field's logical name and the one binding key the format's column mapping selects (`field_id`, `physical_name`, or identity), recursively. It is not a type, and the column's type remains `Utf8`.

## Scenarios

### Scenario: Iceberg logical schema maps to Arrow types for scan registration

* *GIVEN* an Iceberg table's current schema whose fields include primitive types (int, long, double, string, boolean, date, timestamp) and complex/out-of-range types (list, struct, map, out-of-range decimal)
* *WHEN* the adapter derives the logical schema it carries into the scan spec
* *THEN* each Iceberg field SHALL map to the Arrow `DataType` the scan UDF registers for that column, consistent with the existing Iceberg-to-Exasol mapping (primitive types to their direct Arrow equivalents; complex and out-of-range types to a string-family Arrow type that surfaces as JSON `VARCHAR`)
* *AND* `iceberg_type_to_arrow` SHALL keep returning `DataType::Utf8` for `list`, `struct`, and `map`, and MUST NOT recurse into element, field, key, or value types to build a nested Arrow type, because the column's logical type IS the rendered JSON string, see this delta's Background bullet for the five pushdown decision sites a nested logical type would oblige this plan to change
* *AND* each mapped field SHALL preserve the source Iceberg field-id and its required/optional nullability
* *AND* a `list`, `struct`, or `map` field SHALL ADDITIONALLY carry the format-neutral nested descriptor `scan-types/nested-json-rendering` consumes, every nested field's LOGICAL name plus the ONE binding key the format's column-mapping selects, recursively, and a primitive field SHALL carry NONE, so a spec authored before the descriptor existed deserializes unchanged
* *AND* the mapping used for the logical schema SHALL agree with the `createVirtualSchema` schema declaration so the declared Exasol column type and the registered Arrow type stay in agreement

### Scenario: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR

* *GIVEN* a column whose catalog-declared type is a decimal carrying an unsigned precision `p` and an unsigned scale `s`, an Iceberg `PrimitiveType::Decimal { precision, scale }` or a Unity Catalog `decimal(p,s)` parsed from the column's `type_json`
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `DECIMAL(p,s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `VARCHAR(2000000)` otherwise, so `p = 0` yields `VARCHAR(2000000)` rather than the invalid `DECIMAL(0,0)` and `s > p` yields `VARCHAR(2000000)` rather than an invalid shape such as `DECIMAL(5,10)`
* *AND* the domain check SHALL have one owner, the Exasol type's DECIMAL constructor, which both catalog kinds, the logical Arrow mapping, and the `initial-default` encoding gate (`scan-read-path/scan-execution-field-id-projection-absent-fields`) read, so no two of them can classify a pair differently
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on either bad pair, the `VARCHAR(2000000)` fallback absorbs them exactly as it absorbs `p > 36`, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* `(18,4)`, `(10,2)`, `(1,0)`, and the boundary pair `(36,36)` SHALL map to `DECIMAL(18,4)`, `DECIMAL(10,2)`, `DECIMAL(1,0)`, and `DECIMAL(36,36)`, and `(38,10)` and `(18,37)` SHALL map to `VARCHAR(2000000)`

### Scenario: The Iceberg-to-Arrow logical mapping reads the same DECIMAL domain guard

* *GIVEN* an Iceberg `PrimitiveType::Decimal { precision, scale }` carrying an unsigned precision `p` and an unsigned scale `s`
* *WHEN* the VS resolves that column's LOGICAL ARROW type for the scan spec's logical schema, rather than its Exasol declaration
* *THEN* the resolver SHALL return `Decimal128(p, s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `Utf8` otherwise, reading that predicate from the SAME single owner the Exasol-type resolver reads and MUST NOT carry its own copy of it
* *AND* the two directions SHALL therefore be in lockstep BY CONSTRUCTION rather than by convention: for every catalog-declared decimal, `Decimal128(p,s)` accompanies the `DECIMAL(p,s)` declaration and `Utf8` accompanies the `VARCHAR(2000000)` declaration, with no pair producing one of each
* *AND* the resolver MUST NOT return a `Decimal128` tag for a column `createVirtualSchema` declares `VARCHAR(2000000)`, because such a tag pairs an Arrow decimal with a `VARCHAR` declaration, and arrow-rs rejects `precision == 0` and `scale > precision` when a `Decimal128Array` is built, so the tag would name an Arrow type the scan cannot instantiate at all
* *AND* `(18,4)`, `(36,36)`, and `(36,0)` SHALL map to `Decimal128`, and `(0,0)`, `(5,10)`, `(38,10)`, and `(18,37)` SHALL map to `Utf8`
* *AND* the mapping SHALL remain the LOGICAL Iceberg-to-Arrow mapping, unaffected by physical Parquet decode coercion

> The version-gated timestamp declaration precision, its default on an unreadable version, and the
> `timestamptz`/`TIMESTAMP(p)` scenarios live in `scan-types/type-mapping-timestamp-precision`.
