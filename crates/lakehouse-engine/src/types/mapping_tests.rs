use super::*;
use arrow::datatypes::DataType;
use iceberg::spec::{PrimitiveType, Type};
use lakehouse_catalog::ColumnSourceType;

/// Scenario: Compatible Arrow types map to their Exasol type
#[test]
fn compatible_types_map_to_exasol_type() {
    assert_eq!(arrow_to_exasol_type(&DataType::Boolean), "BOOLEAN");
    // numeric family
    assert_eq!(arrow_to_exasol_type(&DataType::Int8), "DECIMAL(3,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Int16), "DECIMAL(5,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Int32), "DECIMAL(10,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Int64), "DECIMAL(20,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt8), "DECIMAL(3,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt16), "DECIMAL(5,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt32), "DECIMAL(20,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt64), "DECIMAL(20,0)");
    // float family
    assert_eq!(arrow_to_exasol_type(&DataType::Float32), "DOUBLE PRECISION");
    assert_eq!(arrow_to_exasol_type(&DataType::Float64), "DOUBLE PRECISION");
    // string family
    assert_eq!(arrow_to_exasol_type(&DataType::Utf8), "VARCHAR(2000000)");
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeUtf8),
        "VARCHAR(2000000)"
    );
    // date/time family
    assert_eq!(arrow_to_exasol_type(&DataType::Date32), "DATE");
    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
        "TIMESTAMP"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        )),
        "TIMESTAMP"
    );
}

/// Scenario: In-range Decimal128 maps to a precise Exasol DECIMAL
#[test]
fn decimal128_in_range_maps_to_decimal() {
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(18, 6)),
        "DECIMAL(18,6)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(36, 36)),
        "DECIMAL(36,36)"
    );
    // boundary: p=36 s=0 is in-range
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(36, 0)),
        "DECIMAL(36,0)"
    );
}

/// Scenario: Out-of-range Decimal128 falls back to VARCHAR via JSON
#[test]
fn decimal128_out_of_range_maps_to_varchar_json() {
    // precision > 36
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(38, 10)),
        "VARCHAR(2000000)"
    );
    // scale > 36
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(18, 37)),
        "VARCHAR(2000000)"
    );
    // both out of range
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(38, 38)),
        "VARCHAR(2000000)"
    );
    // out-of-range also needs JSON fallback
    assert!(needs_json_fallback(&DataType::Decimal128(38, 6)));
}

/// Scenario: Incompatible Arrow types are serialized to JSON VARCHAR
#[test]
fn incompatible_types_map_to_varchar_json() {
    // list family
    assert_eq!(
        arrow_to_exasol_type(&DataType::List(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", DataType::Int32, true)
        ))),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeList(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", DataType::Int32, true)
        ))),
        "VARCHAR(2000000)"
    );
    // struct/map/binary families
    assert_eq!(
        arrow_to_exasol_type(&DataType::Struct(arrow::datatypes::Fields::from(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, true)
        ]))),
        "VARCHAR(2000000)"
    );
    assert_eq!(arrow_to_exasol_type(&DataType::Binary), "VARCHAR(2000000)");
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeBinary),
        "VARCHAR(2000000)"
    );
    // all incompatible types need JSON fallback
    assert!(needs_json_fallback(&DataType::Binary));
    assert!(needs_json_fallback(&DataType::List(std::sync::Arc::new(
        arrow::datatypes::Field::new("item", DataType::Int32, true)
    ))));
    assert!(!needs_json_fallback(&DataType::Boolean));
    assert!(!needs_json_fallback(&DataType::Decimal128(36, 6)));
}

/// Scenario: Incompatible Arrow types are serialized to JSON VARCHAR
///
/// The nested half (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`) is owned by
/// `needs_nested_json_rendering`; the non-nested half (`Binary` and an out-of-range
/// `Decimal128` among others) keeps the `CAST(col AS VARCHAR)` path `needs_json_fallback`
/// already governs, unchanged by the new predicate.
#[test]
fn nested_and_non_nested_incompatible_halves_are_owned_by_one_predicate_each() {
    let list_of_int = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Int32,
        true,
    )));
    let large_list_of_int = DataType::LargeList(std::sync::Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Int32,
        true,
    )));
    let fixed_size_list_of_int = DataType::FixedSizeList(
        std::sync::Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, true)),
        3,
    );
    let struct_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
        arrow::datatypes::Field::new("a", DataType::Int32, true),
    ]));
    let map_type = DataType::Map(
        std::sync::Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                arrow::datatypes::Field::new("value", DataType::Utf8, true),
            ])),
            false,
        )),
        false,
    );
    let out_of_range_decimal = DataType::Decimal128(38, 6);

    for nested in [
        &list_of_int,
        &large_list_of_int,
        &fixed_size_list_of_int,
        &struct_type,
        &map_type,
    ] {
        assert!(
            needs_nested_json_rendering(nested),
            "{nested:?} is one of the five nested variants the JSON encoder renders"
        );
        assert!(
            needs_json_fallback(nested),
            "{nested:?} still needs JSON fallback, unchanged by the new predicate"
        );
    }

    for non_nested in [&DataType::Binary, &out_of_range_decimal] {
        assert!(
            !needs_nested_json_rendering(non_nested),
            "{non_nested:?} keeps the CAST(col AS VARCHAR) path, not the JSON encoder"
        );
        assert!(
            needs_json_fallback(non_nested),
            "{non_nested:?} must stay in needs_json_fallback's CAST path"
        );
    }

    assert!(!needs_nested_json_rendering(&DataType::Boolean));
    assert!(!needs_json_fallback(&DataType::Boolean));
}

/// Scenario (delta-type-mapping): The castability claims behind the Delta type
/// mapping are asserted against `arrow-cast` directly, not assumed. Pins the
/// native/text-rendered/refused set membership so an `arrow-cast` upgrade that
/// changes one of these answers fails this test instead of silently
/// re-partitioning the sets.
#[test]
fn arrow_castability_to_utf8_pins_the_three_delta_type_sets() {
    use arrow::compute::can_cast_types;
    use arrow::datatypes::{Fields, IntervalUnit};

    let populated_struct = DataType::Struct(Fields::from(vec![arrow::datatypes::Field::new(
        "a",
        DataType::Int32,
        true,
    )]));
    let map = DataType::Map(
        std::sync::Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                arrow::datatypes::Field::new("keys", DataType::Utf8, false),
                arrow::datatypes::Field::new("values", DataType::Int32, true),
            ])),
            false,
        )),
        false,
    );
    let list_of_struct = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
        "item",
        populated_struct.clone(),
        true,
    )));

    // Text-rendered set: castable to Utf8, mapped by rendering the value as text.
    assert!(can_cast_types(
        &DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int32,
            true
        ))),
        &DataType::Utf8
    ));
    assert!(can_cast_types(
        &DataType::Interval(IntervalUnit::YearMonth),
        &DataType::Utf8
    ));
    assert!(can_cast_types(
        &DataType::Interval(IntervalUnit::DayTime),
        &DataType::Utf8
    ));
    assert!(can_cast_types(
        &DataType::Decimal128(38, 10),
        &DataType::Utf8
    ));

    // Binary IS castable to Utf8, but is refused anyway: the cast replaces any
    // non-UTF-8 byte sequence with NULL rather than erroring, which would silently
    // corrupt data this engine has no way to detect.
    assert!(can_cast_types(&DataType::Binary, &DataType::Utf8));

    // Refused set: NOT castable to Utf8 (a POPULATED struct, not a zero-field one).
    assert!(!can_cast_types(&populated_struct, &DataType::Utf8));
    assert!(!can_cast_types(&map, &DataType::Utf8));
    assert!(!can_cast_types(&list_of_struct, &DataType::Utf8));
}

/// Scenario (nested-json-rendering): the `List(Utf8) → Utf8` kernel `arrow-cast`
/// makes available is a raw display-text renderer, not a JSON encoder. Pins that
/// using it directly (unrelated to `render_nested_column_as_json`) produces
/// unquoted Arrow display text that does NOT parse as JSON.
#[test]

fn list_to_utf8_cast_kernel_renders_display_text_not_json() {
    use arrow::array::{ListBuilder, StringArray, StringBuilder};
    use arrow::compute::cast;

    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_value("hello");
    builder.values().append_value("world");
    builder.append(true);
    let list = builder.finish();

    let casted =
        cast(&list, &DataType::Utf8).expect("List(Utf8) -> Utf8 is a real arrow-cast kernel");
    let rendered = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast target is Utf8")
        .value(0);

    assert_eq!(
        rendered, "[hello, world]",
        "the raw cast kernel renders unquoted Arrow display text"
    );
    assert!(
        serde_json::from_str::<serde_json::Value>(rendered).is_err(),
        "unquoted bare words are not valid JSON tokens, unlike the JSON encoder's \
         output: {rendered}"
    );
}

/// Scenario: One arm list decides both the Exasol type string and the
/// JSON-fallback flag — and the string alone cannot decide it. `Utf8` and
/// `LargeUtf8` declare `VARCHAR(2000000)` and cross the boundary unchanged,
/// while an out-of-range `Decimal128` declares the SAME string but must be
/// JSON-serialized first. Deriving the flag from the returned type string
/// would therefore JSON-wrap every string column.
#[test]
fn varchar_type_string_alone_does_not_decide_the_json_fallback() {
    let out_of_range_decimal = DataType::Decimal128(38, 10);

    for string_type in [DataType::Utf8, DataType::LargeUtf8] {
        assert_eq!(arrow_to_exasol_type(&string_type), "VARCHAR(2000000)");
        assert_eq!(
            arrow_to_exasol_type(&string_type),
            arrow_to_exasol_type(&out_of_range_decimal),
            "{string_type:?} and an out-of-range Decimal128 must declare the same Exasol type"
        );
        assert!(
            !needs_json_fallback(&string_type),
            "{string_type:?} crosses the boundary unchanged, with no JSON serialization"
        );
    }

    assert!(
        needs_json_fallback(&out_of_range_decimal),
        "an out-of-range Decimal128 must be JSON-serialized despite the identical type string"
    );
}

/// Scenario (D.4): Iceberg-field → Exasol-type schema mapping.
/// Each Iceberg primitive → correct Exasol type; complex types → VARCHAR(2000000).
#[test]
fn iceberg_types_map_to_exasol_type() {
    let ts_precision = TimestampPrecision::Millisecond;
    // primitives
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Boolean), ts_precision),
        "BOOLEAN"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Int), ts_precision),
        "DECIMAL(10,0)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Long), ts_precision),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Float), ts_precision),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Double), ts_precision),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::String), ts_precision),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Date), ts_precision),
        "DATE"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Timestamp), ts_precision),
        "TIMESTAMP"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Timestamptz), ts_precision),
        "TIMESTAMP"
    );
    // in-range decimal
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 18,
                scale: 4,
            }),
            ts_precision
        ),
        "DECIMAL(18,4)"
    );
    // out-of-range decimal → VARCHAR
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: 10,
            }),
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
    // precision = 0 → VARCHAR
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 0,
                scale: 0,
            }),
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
    // scale > precision → VARCHAR
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 5,
                scale: 10,
            }),
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
    // incompatible primitive → VARCHAR
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Binary), ts_precision),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Time), ts_precision),
        "VARCHAR(2000000)"
    );
}

/// Scenario: `exasol_type_to_arrow` reproduces Exasol's EXACT DECIMAL→ExaType
/// precision binning, plus the non-DECIMAL types.
///
/// The target is the Arrow type the engine's `emit_batch` feed accepts for the
/// declared EMITS type — NOT a round-trip of `arrow_to_exasol_type` (which is
/// not an identity, because the engine bins DECIMAL precision into Int32 /
/// Int64 / Numeric). Bins asserted here:
///   scale 0, p ≤ 9   → Arrow Int32   (ExaType Int32)
///   scale 0, 10 ≤ p ≤ 18 → Arrow Int64   (ExaType Int64)
///   scale > 0, OR 19 ≤ p ≤ 36 → Arrow Decimal128(p,s) (ExaType Numeric)
#[test]
fn exasol_type_to_arrow_reproduces_decimal_precision_binning() {
    let cases: &[(&str, DataType)] = &[
        ("BOOLEAN", DataType::Boolean),
        ("DOUBLE PRECISION", DataType::Float64),
        ("DATE", DataType::Date32),
        (
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            "TIMESTAMP WITH LOCAL TIME ZONE",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        // --- Int32 bin: scale 0, precision 1..=9 ---
        ("DECIMAL(1,0)", DataType::Int32),
        ("DECIMAL(3,0)", DataType::Int32),
        ("DECIMAL(9,0)", DataType::Int32), // boundary: 9 is the last Int32
        // --- Int64 bin: scale 0, precision 10..=18 ---
        ("DECIMAL(10,0)", DataType::Int64), // boundary: COUNT(*) live case
        ("DECIMAL(18,0)", DataType::Int64), // boundary: 18 is the last Int64
        ("DECIMAL(20,0)", DataType::Decimal128(20, 0)), // p>18 → Numeric/Decimal128
        // --- Numeric/Decimal128 bin: scale > 0, OR precision 19..=36 ---
        ("DECIMAL(19,0)", DataType::Decimal128(19, 0)), // boundary: first >Int64
        ("DECIMAL(36,0)", DataType::Decimal128(36, 0)),
        ("DECIMAL(9,2)", DataType::Decimal128(9, 2)), // scale>0 → Decimal128 even at p≤9
        ("DECIMAL(18,4)", DataType::Decimal128(18, 4)),
        ("DECIMAL(36,36)", DataType::Decimal128(36, 36)),
    ];
    for (declared, expected_arrow) in cases {
        let arrow = exasol_type_to_arrow(declared)
            .unwrap_or_else(|| panic!("{declared} must map to a concrete Arrow type"));
        assert_eq!(&arrow, expected_arrow, "wrong Arrow target for {declared}");
    }
}

/// Scenario: a `TIMESTAMP(p)` EMITS string (produced once the CAST renderer
/// and EMITS-type derivation stop collapsing precision to bare `TIMESTAMP`)
/// maps back to the same microsecond Arrow timestamp as bare `TIMESTAMP`,
/// regardless of the declared precision `p`. The project already collapses
/// every TIMESTAMP precision to one Arrow representation on the way in, so
/// the emit-boundary coercion mirrors that on the way out (issue #212).
#[test]
fn exasol_type_to_arrow_parses_timestamp_precision() {
    let expected = Some(DataType::Timestamp(TimeUnit::Microsecond, None));
    assert_eq!(exasol_type_to_arrow("TIMESTAMP(0)"), expected);
    assert_eq!(exasol_type_to_arrow("TIMESTAMP(6)"), expected);
    assert_eq!(exasol_type_to_arrow("TIMESTAMP(9)"), expected);
}

/// Scenario: the live bench failures map to the correct integer Arrow target.
///
/// Both live "cannot feed declared ExaType Int64" errors were DECIMAL columns
/// in the Int64 bin (p 10..=18, scale 0). The target must be Arrow `Int64` —
/// NOT `Decimal128`, which the engine rejects for an Int64 column.
#[test]
fn exasol_type_to_arrow_count_star_decimal_is_int64() {
    // COUNT(*) is declared DECIMAL(10,0) by Exasol → ExaType Int64.
    assert_eq!(exasol_type_to_arrow("DECIMAL(10,0)"), Some(DataType::Int64));
    // An Iceberg `int` column declared DECIMAL(10,0) → also Int64 (p≤18).
    // (The first live error: an Arrow Int32 source must be cast Int32→Int64.)
    assert_eq!(exasol_type_to_arrow("DECIMAL(18,0)"), Some(DataType::Int64));
    // Small scale-0 DECIMALs are Int32, not Decimal128.
    assert_eq!(exasol_type_to_arrow("DECIMAL(9,0)"), Some(DataType::Int32));
}

/// Scenario: String-family declared types (VARCHAR/CHAR) and unknown strings
/// return `None` — the caller routes them through the Utf8/JSON string path
/// rather than a fixed Arrow target.
#[test]
fn exasol_type_to_arrow_returns_none_for_string_family() {
    assert_eq!(exasol_type_to_arrow("VARCHAR(2000000)"), None);
    assert_eq!(exasol_type_to_arrow("VARCHAR(100)"), None);
    assert_eq!(exasol_type_to_arrow("CHAR(10)"), None);
    // Unknown / unsupported declarations also route to the string path.
    assert_eq!(exasol_type_to_arrow("GEOMETRY"), None);
    assert_eq!(exasol_type_to_arrow("HASHTYPE"), None);
}

/// Scenario: parsing is case-insensitive and whitespace-tolerant.
#[test]
fn exasol_type_to_arrow_is_case_and_whitespace_insensitive() {
    assert_eq!(
        exasol_type_to_arrow("  decimal(20,0) "),
        Some(DataType::Decimal128(20, 0))
    );
    assert_eq!(
        exasol_type_to_arrow("double precision"),
        Some(DataType::Float64)
    );
    // DECIMAL(p) with no scale defaults to scale 0 → Int32 bin (p=9 ≤ 9).
    assert_eq!(exasol_type_to_arrow("DECIMAL(9)"), Some(DataType::Int32));
}

/// Task 1.2: `iceberg_type_to_arrow` maps all families of Iceberg types to their
/// Arrow equivalents. Primitives → direct Arrow types; complex / out-of-range
/// types → `DataType::Utf8` (surfaced as JSON VARCHAR).
#[test]
fn iceberg_type_to_arrow_maps_all_families() {
    use arrow::datatypes::TimeUnit;

    // Boolean
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Boolean)),
        DataType::Boolean
    );

    // Integer primitives
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Int)),
        DataType::Int32
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Long)),
        DataType::Int64
    );

    // Float primitives
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Float)),
        DataType::Float32
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Double)),
        DataType::Float64
    );

    // String / UUID → Utf8
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::String)),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Uuid)),
        DataType::Utf8
    );

    // Date
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Date)),
        DataType::Date32
    );

    // Timestamp (no tz) — micros
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Timestamp)),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    // TimestampNs (no tz) — nanos
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::TimestampNs)),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    // Timestamptz — micros, UTC
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Timestamptz)),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    // TimestamptzNs — nanos, UTC
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::TimestamptzNs)),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );

    // In-range Decimal128 (p ≤ 36 and s ≤ 36)
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 18,
            scale: 4,
        })),
        DataType::Decimal128(18, 4)
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 36,
            scale: 36,
        })),
        DataType::Decimal128(36, 36)
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 36,
            scale: 0,
        })),
        DataType::Decimal128(36, 0)
    );

    // Out-of-range Decimal → Utf8 (JSON fallback)
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 38,
            scale: 10,
        })),
        DataType::Utf8
    );
    // scale > 36
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 18,
            scale: 37,
        })),
        DataType::Utf8
    );
    // precision 0 — outside Exasol's catalog-decimal domain, and an Arrow
    // precision arrow-rs refuses to build an array with
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 0,
            scale: 0,
        })),
        DataType::Utf8
    );
    // scale > precision — likewise rejected by arrow-rs
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 5,
            scale: 10,
        })),
        DataType::Utf8
    );

    // Time → Utf8 (no Exasol/Arrow equivalent)
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Time)),
        DataType::Utf8
    );

    // Binary / Fixed → Utf8
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Binary)),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Fixed(16))),
        DataType::Utf8
    );

    // Complex types (List, Struct, Map) → Utf8
    assert_eq!(
        iceberg_type_to_arrow(&Type::List(iceberg::spec::ListType {
            element_field: std::sync::Arc::new(iceberg::spec::NestedField::required(
                1,
                "element",
                iceberg::spec::Type::Primitive(PrimitiveType::Int)
            )),
        })),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Map(iceberg::spec::MapType {
            key_field: std::sync::Arc::new(iceberg::spec::NestedField::required(
                1,
                "key",
                iceberg::spec::Type::Primitive(PrimitiveType::String)
            )),
            value_field: std::sync::Arc::new(iceberg::spec::NestedField::optional(
                2,
                "value",
                iceberg::spec::Type::Primitive(PrimitiveType::Int)
            )),
        })),
        DataType::Utf8
    );
}

/// D.5 — one test per mapping category asserting BOTH the declared Exasol type
/// AND that the `needs_json_fallback` flag agrees.
#[test]
fn numeric_family_types_and_fallback_flags() {
    let cases: &[(DataType, &str, bool)] = &[
        (DataType::Int8, "DECIMAL(3,0)", false),
        (DataType::Int16, "DECIMAL(5,0)", false),
        (DataType::Int32, "DECIMAL(10,0)", false),
        (DataType::Int64, "DECIMAL(20,0)", false),
        (DataType::UInt8, "DECIMAL(3,0)", false),
        (DataType::UInt16, "DECIMAL(5,0)", false),
        (DataType::UInt32, "DECIMAL(20,0)", false),
        (DataType::UInt64, "DECIMAL(20,0)", false),
    ];
    for (dt, expected_type, expected_json) in cases {
        assert_eq!(
            arrow_to_exasol_type(dt),
            *expected_type,
            "type mismatch for {dt:?}"
        );
        assert_eq!(
            needs_json_fallback(dt),
            *expected_json,
            "fallback flag mismatch for {dt:?}"
        );
    }
}

#[test]
fn float_family_types_and_fallback_flags() {
    for dt in [DataType::Float32, DataType::Float64] {
        assert_eq!(arrow_to_exasol_type(&dt), "DOUBLE PRECISION");
        assert!(!needs_json_fallback(&dt));
    }
}

#[test]
fn string_family_types_and_fallback_flags() {
    for dt in [DataType::Utf8, DataType::LargeUtf8] {
        assert_eq!(arrow_to_exasol_type(&dt), "VARCHAR(2000000)");
        assert!(!needs_json_fallback(&dt));
    }
}

#[test]
fn date_time_family_types_and_fallback_flags() {
    assert_eq!(arrow_to_exasol_type(&DataType::Date32), "DATE");
    assert!(!needs_json_fallback(&DataType::Date32));

    let ts_no_tz = DataType::Timestamp(TimeUnit::Microsecond, None);
    assert_eq!(arrow_to_exasol_type(&ts_no_tz), "TIMESTAMP");
    assert!(!needs_json_fallback(&ts_no_tz));

    let ts_tz = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    assert_eq!(arrow_to_exasol_type(&ts_tz), "TIMESTAMP");
    assert!(!needs_json_fallback(&ts_tz));
}

#[test]
fn exasol_type_to_json_roundtrip() {
    let cases = [
        ("BOOLEAN", "boolean"),
        ("DOUBLE PRECISION", "double"),
        ("DATE", "date"),
        ("TIMESTAMP", "timestamp"),
    ];
    for (ty, expected_type) in cases {
        let j = exasol_type_to_json(ty);
        assert_eq!(
            j["type"].as_str().unwrap().to_lowercase(),
            expected_type,
            "type mismatch for {ty}"
        );
    }
    let dec = exasol_type_to_json("DECIMAL(18,4)");
    assert_eq!(dec["precision"].as_u64().unwrap(), 18);
    assert_eq!(dec["scale"].as_u64().unwrap(), 4);
}

/// Divergence class 1 of routing `exasol_type_to_json` through
/// `parse_decimal_args`: an absent scale used to leave the DECIMAL branch
/// entirely (the hand-rolled parser required exactly two arguments) and
/// surfaced as a VARCHAR object. `parse_decimal_args` defaults an absent
/// scale to `0`, so it is now a decimal object of scale 0.
#[test]
fn exasol_type_to_json_absent_decimal_scale_becomes_scale_zero_decimal() {
    assert_eq!(
        exasol_type_to_json("DECIMAL(10)"),
        json!({"type": "decimal", "precision": 10, "scale": 0})
    );
}

/// Divergence class 2: a precision or scale outside `parse_decimal_args`'
/// `u8`/`i8` range used to be accepted as a `u64` and echoed into a decimal
/// object; it now fails the parse and falls through to the VARCHAR default.
/// Unreachable from every producer in this repo — each guards `p,s <= 36`.
#[test]
fn exasol_type_to_json_out_of_range_decimal_args_become_varchar() {
    assert_eq!(
        exasol_type_to_json("DECIMAL(300,2)"),
        json!({"type": "varchar", "size": 2000000})
    );
    assert_eq!(
        exasol_type_to_json("DECIMAL(10,200)"),
        json!({"type": "varchar", "size": 2000000})
    );
}

/// Divergence class 3: a negative scale used to fail the `u64` parse and
/// surface as a VARCHAR object; it now parses as `i8` and is serialized as a
/// SIGNED JSON number, so it can never wrap into a large unsigned value.
#[test]
fn exasol_type_to_json_negative_decimal_scale_stays_signed() {
    assert_eq!(
        exasol_type_to_json("DECIMAL(10,-2)"),
        json!({"type": "decimal", "precision": 10, "scale": -2})
    );
}

/// The two inputs the spec names as NON-divergences: a three-argument list
/// and an empty one already fell through to VARCHAR before consolidation and
/// still do, so the divergence set stays the closed three classes above.
#[test]
fn exasol_type_to_json_malformed_decimal_arg_lists_stay_varchar() {
    for malformed in ["DECIMAL(10,2,3)", "DECIMAL()"] {
        assert_eq!(
            exasol_type_to_json(malformed),
            json!({"type": "varchar", "size": 2000000}),
            "{malformed} must stay a VARCHAR object"
        );
    }
}

#[test]
fn exasol_type_to_json_timestamp_with_local_time_zone() {
    let tstz = exasol_type_to_json("TIMESTAMP WITH LOCAL TIME ZONE");
    assert_eq!(
        tstz,
        serde_json::json!({"type": "timestamp", "withLocalTimeZone": true})
    );

    let ts = exasol_type_to_json("TIMESTAMP");
    assert_eq!(ts, serde_json::json!({"type": "timestamp"}));
}

/// `exasol_type_from_json` must read the `withLocalTimeZone` flag back off a
/// `{"type":"timestamp", ...}` dataType JSON (the shape Exasol echoes back in
/// `involvedTables[].columns[].dataType` for a VS column declared via
/// `exasol_type_to_json`), not just the bare `"type"` string — otherwise a
/// TIMESTAMP WITH LOCAL TIME ZONE column round-trips back into the pushdown
/// path as plain TIMESTAMP and Exasol rejects the EMITS type mismatch.
#[test]
fn exasol_type_from_json_reads_with_local_time_zone_flag() {
    let tstz = serde_json::json!({"type": "timestamp", "withLocalTimeZone": true});
    assert_eq!(
        exasol_type_from_json(&tstz),
        "TIMESTAMP WITH LOCAL TIME ZONE"
    );

    let ts = serde_json::json!({"type": "timestamp"});
    assert_eq!(exasol_type_from_json(&ts), "TIMESTAMP");
}

/// `exasol_type_from_json` must read `fractionalSecondsPrecision` back off a
/// `{"type":"timestamp", ...}` dataType JSON and render it as `TIMESTAMP(p)` — the
/// field is `fractionalSecondsPrecision`, not `precision` (that key is
/// DECIMAL/INTERVAL-only in Exasol's data-type API). Absent precision still falls
/// back to bare `TIMESTAMP`, and `withLocalTimeZone: true` still takes precedence
/// over precision (no `(p)` suffix on WLTZ), matching issue #212's collapse-point-1
/// fix.
#[test]
fn exasol_type_from_json_reads_timestamp_fractional_seconds_precision() {
    let ts0 = serde_json::json!({"type": "timestamp", "fractionalSecondsPrecision": 0});
    assert_eq!(exasol_type_from_json(&ts0), "TIMESTAMP(0)");

    let ts6 = serde_json::json!({"type": "timestamp", "fractionalSecondsPrecision": 6});
    assert_eq!(exasol_type_from_json(&ts6), "TIMESTAMP(6)");

    let ts9 = serde_json::json!({"type": "timestamp", "fractionalSecondsPrecision": 9});
    assert_eq!(exasol_type_from_json(&ts9), "TIMESTAMP(9)");

    let ts_absent = serde_json::json!({"type": "timestamp"});
    assert_eq!(exasol_type_from_json(&ts_absent), "TIMESTAMP");

    let tstz_with_precision = serde_json::json!({
        "type": "timestamp",
        "withLocalTimeZone": true,
        "fractionalSecondsPrecision": 7
    });
    assert_eq!(
        exasol_type_from_json(&tstz_with_precision),
        "TIMESTAMP WITH LOCAL TIME ZONE"
    );
}

/// `exasol_type_from_json` must read the `characterSet` field back off a
/// `{"type":"varchar", ...}` dataType JSON (Exasol's wire format for CHAR/VARCHAR
/// select-list items, e.g. `{"type":"CHAR","size":3,"characterSet":"ASCII"}` as
/// confirmed by `vs-expression`'s `renders_cast_char_as_datafusion_varchar` test) and append
/// `" ASCII"` when it is `"ASCII"` (case-insensitively) — otherwise a CASE/literal
/// expression Exasol declares as `VARCHAR(n) ASCII` round-trips back through our
/// EMITS clause as bare `VARCHAR(n)`, which Exasol's type checker treats as
/// `VARCHAR(n) UTF8` by default, causing a "Data type mismatch" pushdown error
/// (issue #136 follow-up).
#[test]
fn exasol_type_from_json_propagates_ascii_character_set() {
    let ascii = serde_json::json!({"type": "VARCHAR", "size": 4, "characterSet": "ASCII"});
    assert_eq!(exasol_type_from_json(&ascii), "VARCHAR(4) ASCII");

    let no_charset = serde_json::json!({"type": "VARCHAR", "size": 4});
    assert_eq!(exasol_type_from_json(&no_charset), "VARCHAR(4)");
}

/// `exasol_type_from_json` must render a genuine `{"type":"CHAR", ...}` dataType
/// JSON as `CHAR(n)` — not fall through to the catch-all's `VARCHAR(n)` the way
/// pre-#192 code did. An equal-length CASE expression (e.g. `CASE WHEN ... THEN
/// 'NEG' ELSE 'POS' END`) round-trips back through this function as `CHAR(3)
/// ASCII`; rendering it `VARCHAR(3) ASCII` instead causes Exasol's type checker
/// to reject the pushdown with "Data type mismatch" (issue #192).
#[test]
fn exasol_type_from_json_renders_char_type() {
    let ascii = serde_json::json!({"type": "CHAR", "size": 3, "characterSet": "ASCII"});
    assert_eq!(exasol_type_from_json(&ascii), "CHAR(3) ASCII");
}

/// The CHAR arm must mirror VARCHAR's `characterSet` handling exactly: append
/// `" ASCII"` only when `characterSet` is `"ASCII"` (case-insensitively), and
/// render a bare `CHAR(n)` (no suffix) for `"UTF8"` or when `characterSet` is
/// absent — e.g. `CAST(c_phone AS CHAR(20))`, which Exasol declares `CHAR(20)
/// UTF8` (live-verified), must round-trip as bare `CHAR(20)`.
#[test]
fn exasol_type_from_json_propagates_char_ascii_character_set() {
    let utf8 = serde_json::json!({"type": "CHAR", "size": 20, "characterSet": "UTF8"});
    assert_eq!(exasol_type_from_json(&utf8), "CHAR(20)");

    let no_charset = serde_json::json!({"type": "CHAR", "size": 20});
    assert_eq!(exasol_type_from_json(&no_charset), "CHAR(20)");
}

/// Exasol rejects a CHAR declaration above 2,000 characters
/// (`CAST('a' AS CHAR(2001))` fails live with "specified length too long for
/// char type - maximum is 2000"), so the CHAR arm must cap `size` at 2,000 —
/// unlike VARCHAR's 2,000,000 cap.
#[test]
fn exasol_type_from_json_caps_char_size_at_exasol_maximum() {
    let oversized = serde_json::json!({"type": "CHAR", "size": 9999});
    assert_eq!(exasol_type_from_json(&oversized), "CHAR(2000)");
}

/// An absent `size` on a CHAR `dataType` is unreachable from a real Exasol
/// request, but if it occurred the CHAR arm must not silently default to
/// the *maximum* width (`CHAR(2000)`, which blank-pads every value to
/// 2,000 characters) — it must fall back to the project's "unknown width"
/// convention, matching `vs-expression`'s `render_cast_target` Exasol CHAR
/// arm.
#[test]
fn exasol_type_from_json_char_without_size_falls_back_to_unknown_width() {
    let no_size = serde_json::json!({"type": "CHAR"});
    assert_eq!(exasol_type_from_json(&no_size), "VARCHAR(2000000)");
}

/// Scenario: One classifier names the Exasol type-string families the pushdown
/// guards branch on. Pins the exact predicates of `guard_like_subject`,
/// `is_bare_decimal_column`, and `coerce_string_position_arg`
/// (`adapter/pushdown/support.rs`): a bare `DECIMAL` (no arguments) must classify
/// as `Decimal`, the case that distinguishes the correct `starts_with("DECIMAL")`
/// predicate from the wrong `starts_with("DECIMAL(")` one.
#[test]
fn classify_exa_type_matches_pushdown_guard_predicates() {
    assert_eq!(
        classify_exa_type("VARCHAR(4) ASCII"),
        ExaTypeClass::Character
    );
    assert_eq!(classify_exa_type("CHAR(2)"), ExaTypeClass::Character);

    assert_eq!(classify_exa_type("DECIMAL(20,0)"), ExaTypeClass::Decimal);
    assert_eq!(classify_exa_type("DECIMAL"), ExaTypeClass::Decimal);

    assert_eq!(classify_exa_type("DATE"), ExaTypeClass::Date);

    assert_eq!(classify_exa_type("TIMESTAMP"), ExaTypeClass::Other);
    assert_eq!(classify_exa_type("DOUBLE PRECISION"), ExaTypeClass::Other);
}

/// Scenario: Both an Iceberg-sourced and a Unity-sourced column map through the
/// single `ColumnSourceType` match
#[test]
fn column_source_type_maps_to_exasol_in_one_home() {
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long)),
            TimestampPrecision::Millisecond,
        ),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "LONG".to_string(),
                precision: 0,
                scale: 0,
            },
            TimestampPrecision::Millisecond,
        ),
        "DECIMAL(20,0)"
    );
}

/// Scenario: Unity Catalog Spark column types map to Exasol types
#[test]
fn unity_spark_types_map_to_exasol() {
    let cases = [
        ("BOOLEAN", 0, 0, "BOOLEAN"),
        ("BYTE", 0, 0, "DECIMAL(3,0)"),
        ("SHORT", 0, 0, "DECIMAL(5,0)"),
        ("INT", 0, 0, "DECIMAL(10,0)"),
        ("LONG", 0, 0, "DECIMAL(20,0)"),
        ("FLOAT", 0, 0, "DOUBLE PRECISION"),
        ("DOUBLE", 0, 0, "DOUBLE PRECISION"),
        ("STRING", 0, 0, "VARCHAR(2000000)"),
        ("DATE", 0, 0, "DATE"),
        ("TIMESTAMP", 0, 0, "TIMESTAMP"),
        ("TIMESTAMP_NTZ", 0, 0, "TIMESTAMP"),
        ("DECIMAL", 10, 2, "DECIMAL(10,2)"),
        ("DECIMAL", 36, 36, "DECIMAL(36,36)"),
    ];
    for (type_name, precision, scale, expected) in cases {
        let source = ColumnSourceType::Unity {
            type_name: type_name.to_string(),
            precision,
            scale,
        };
        assert_eq!(
            column_source_type_to_exasol(&source, TimestampPrecision::Millisecond),
            expected,
            "type_name={type_name} precision={precision} scale={scale}"
        );
    }
}

/// Scenario: An incompatible Unity Catalog column type and an out-of-range
/// DECIMAL both fall back to VARCHAR
#[test]
fn incompatible_unity_types_declared_varchar() {
    let ts_precision = TimestampPrecision::Millisecond;
    for type_name in ["ARRAY", "MAP", "STRUCT", "BINARY", "INTERVAL", "VARIANT"] {
        let source = ColumnSourceType::Unity {
            type_name: type_name.to_string(),
            precision: 0,
            scale: 0,
        };
        assert_eq!(
            column_source_type_to_exasol(&source, ts_precision),
            "VARCHAR(2000000)",
            "type_name={type_name}"
        );
    }

    // precision > 36
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "DECIMAL".to_string(),
                precision: 38,
                scale: 10,
            },
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
    // scale > 36
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "DECIMAL".to_string(),
                precision: 18,
                scale: 37,
            },
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
    // precision = 0
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "DECIMAL".to_string(),
                precision: 0,
                scale: 0,
            },
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
    // scale > precision
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "DECIMAL".to_string(),
                precision: 5,
                scale: 10,
            },
            ts_precision
        ),
        "VARCHAR(2000000)"
    );
}

/// Scenario (datafusion-scan/type-mapping): A catalog-declared DECIMAL outside
/// Exasol's DECIMAL domain falls back to VARCHAR — identically for both
/// catalog kinds, since both read the same shared guard.
#[test]
fn catalog_decimal_guard_is_shared_by_both_source_kinds() {
    let cases = [
        (0, 0, "VARCHAR(2000000)"),
        (0, 5, "VARCHAR(2000000)"),
        (5, 10, "VARCHAR(2000000)"),
        (5, 6, "VARCHAR(2000000)"),
        (1, 0, "DECIMAL(1,0)"),
        (18, 4, "DECIMAL(18,4)"),
        (36, 36, "DECIMAL(36,36)"),
        (37, 0, "VARCHAR(2000000)"),
        (38, 10, "VARCHAR(2000000)"),
        (18, 37, "VARCHAR(2000000)"),
    ];
    for (precision, scale, expected) in cases {
        let iceberg_result = column_source_type_to_exasol(
            &ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Decimal {
                precision,
                scale,
            })),
            TimestampPrecision::Millisecond,
        );
        let unity_result = column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "DECIMAL".to_string(),
                precision,
                scale,
            },
            TimestampPrecision::Millisecond,
        );
        assert_eq!(
            iceberg_result, expected,
            "iceberg precision={precision} scale={scale}"
        );
        assert_eq!(
            unity_result, expected,
            "unity precision={precision} scale={scale}"
        );
        assert_eq!(
            iceberg_result, unity_result,
            "kinds diverged for precision={precision} scale={scale}"
        );
    }
}

// Scenario Coverage (iceberg-type-promotion): The unknown primitive type is unrepresentable, and
// the mapping is the tripwire
//
// `iceberg_primitive_to_exasol` and `iceberg_primitive_to_arrow` are each an EXHAUSTIVE match over
// `iceberg::spec::PrimitiveType` with no catch-all arm, so an `iceberg` upgrade that adds a variant
// fails the BUILD with a compile error — that is a build event, not something a running test could
// observe. `expected_mapping` is a third such match, so it fails that same build alongside them;
// what it adds on top is an answer written independently of production for every variant, so each
// variant that does compile has both its Exasol type string and its Arrow `DataType` asserted.
#[test]
fn iceberg_primitive_mappings_are_exhaustive_so_a_new_variant_breaks_the_build() {
    let every_variant = [
        PrimitiveType::Boolean,
        PrimitiveType::Int,
        PrimitiveType::Long,
        PrimitiveType::Float,
        PrimitiveType::Double,
        PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        },
        PrimitiveType::Date,
        PrimitiveType::Time,
        PrimitiveType::Timestamp,
        PrimitiveType::Timestamptz,
        PrimitiveType::TimestampNs,
        PrimitiveType::TimestamptzNs,
        PrimitiveType::String,
        PrimitiveType::Uuid,
        PrimitiveType::Fixed(16),
        PrimitiveType::Binary,
    ];

    for variant in &every_variant {
        let (expected_exasol, expected_arrow) = expected_mapping(variant);
        assert_eq!(
            iceberg_primitive_to_exasol(variant, TimestampPrecision::Millisecond),
            expected_exasol,
            "iceberg_primitive_to_exasol mapped {variant:?} to an unexpected Exasol type"
        );
        assert_eq!(
            iceberg_primitive_to_arrow(variant),
            expected_arrow,
            "iceberg_primitive_to_arrow mapped {variant:?} to an unexpected Arrow type"
        );
    }
}

fn expected_mapping(pt: &PrimitiveType) -> (&'static str, DataType) {
    match pt {
        PrimitiveType::Boolean => ("BOOLEAN", DataType::Boolean),
        PrimitiveType::Int => ("DECIMAL(10,0)", DataType::Int32),
        PrimitiveType::Long => ("DECIMAL(20,0)", DataType::Int64),
        PrimitiveType::Float => ("DOUBLE PRECISION", DataType::Float32),
        PrimitiveType::Double => ("DOUBLE PRECISION", DataType::Float64),
        PrimitiveType::Decimal { precision, scale } => {
            assert_eq!(
                (*precision, *scale),
                (10, 2),
                "expected_mapping pins only the decimal shape every_variant drives"
            );
            ("DECIMAL(10,2)", DataType::Decimal128(10, 2))
        }
        PrimitiveType::Date => ("DATE", DataType::Date32),
        PrimitiveType::Time => ("VARCHAR(2000000)", DataType::Utf8),
        PrimitiveType::Timestamp => (
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        PrimitiveType::TimestampNs => {
            ("TIMESTAMP", DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        PrimitiveType::Timestamptz => (
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        PrimitiveType::TimestamptzNs => (
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ),
        PrimitiveType::String => ("VARCHAR(2000000)", DataType::Utf8),
        PrimitiveType::Uuid => ("VARCHAR(2000000)", DataType::Utf8),
        PrimitiveType::Fixed(_) => ("VARCHAR(2000000)", DataType::Utf8),
        PrimitiveType::Binary => ("VARCHAR(2000000)", DataType::Utf8),
    }
}

/// Scenario (datafusion-scan/type-mapping): A catalog timestamp column is declared
/// TIMESTAMP(6) on Exasol 2025.x and later — the version rule and both declaration
/// strings at their single owner. `8.29.13` and `2025.2.1` are the real Docker image
/// tags `ctx.database_version()` reports on the two engine lines.
#[test]
fn database_version_leading_component_selects_the_declared_timestamp_precision() {
    let cases = [
        ("2025.2.1", TimestampPrecision::Microsecond, "TIMESTAMP(6)"),
        ("2026.1.0", TimestampPrecision::Microsecond, "TIMESTAMP(6)"),
        ("2025", TimestampPrecision::Microsecond, "TIMESTAMP(6)"),
        ("2024.12.31", TimestampPrecision::Millisecond, "TIMESTAMP"),
        ("8.29.13", TimestampPrecision::Millisecond, "TIMESTAMP"),
        ("7.1.20", TimestampPrecision::Millisecond, "TIMESTAMP"),
    ];
    for (version, expected, expected_declaration) in cases {
        let resolved = TimestampPrecision::from_database_version(version);
        assert_eq!(resolved, expected, "version={version}");
        assert_eq!(
            resolved.declaration(),
            expected_declaration,
            "version={version}"
        );
    }
}

/// Scenario (datafusion-scan/type-mapping): An empty or unparseable database version
/// declares the microsecond precision — the SAME arm a recognised 2025.x version takes,
/// deliberately not the bare-TIMESTAMP default.
#[test]
fn unreadable_database_version_declares_microsecond_precision() {
    for version in ["", "v2025.2.1", "unknown", ".2.1", "8x.1.0", " "] {
        let resolved = TimestampPrecision::from_database_version(version);
        assert_eq!(
            resolved,
            TimestampPrecision::Microsecond,
            "version={version:?}"
        );
        assert_eq!(
            resolved.declaration(),
            "TIMESTAMP(6)",
            "version={version:?}"
        );
    }
}

/// Scenario (datafusion-scan/type-mapping): An Iceberg `timestamp` and a Delta
/// `TIMESTAMP` are declared at the SAME resolved precision — the two catalog
/// declaration producers read one owner, so neither line can drift from the other.
#[test]
fn timestamp_declaration_is_version_gated_for_both_catalog_kinds() {
    let cases = [
        (TimestampPrecision::Microsecond, "TIMESTAMP(6)"),
        (TimestampPrecision::Millisecond, "TIMESTAMP"),
    ];
    for (precision, expected) in cases {
        assert_eq!(
            iceberg_primitive_to_exasol(&PrimitiveType::Timestamp, precision),
            expected,
            "iceberg timestamp at {precision:?}"
        );
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Unity {
                    type_name: "TIMESTAMP".to_string(),
                    precision: 0,
                    scale: 0,
                },
                precision,
            ),
            expected,
            "delta TIMESTAMP at {precision:?}"
        );
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Unity {
                    type_name: "TIMESTAMP_NTZ".to_string(),
                    precision: 0,
                    scale: 0,
                },
                precision,
            ),
            expected,
            "delta TIMESTAMP_NTZ at {precision:?}"
        );
    }
}

/// Scenario (datafusion-scan/type-mapping): The Arrow-input resolver stays outside
/// the version gate — it resolves the UDF's declared EMITS type, not a catalog
/// declaration, so it takes no `TimestampPrecision` at all. The function-pointer
/// binding is the assertion: a threaded precision parameter would not compile.
#[test]
fn arrow_input_resolver_stays_outside_the_timestamp_version_gate() {
    let _ungated: fn(&DataType) -> String = arrow_to_exasol_type;

    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
        "TIMESTAMP"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        )),
        "TIMESTAMP"
    );
}

/// Scenario (datafusion-scan/type-mapping): `timestamptz` keeps collapsing to the
/// plain (now precision-gated) Exasol TIMESTAMP declaration rather than TIMESTAMP
/// WITH LOCAL TIME ZONE, which Exasol rejects as a UDF EMITS output type.
#[test]
fn iceberg_timestamptz_declares_timestamp_at_the_gated_precision() {
    let zoned = [PrimitiveType::Timestamptz, PrimitiveType::TimestamptzNs];
    for variant in &zoned {
        assert_eq!(
            iceberg_primitive_to_exasol(variant, TimestampPrecision::Microsecond),
            "TIMESTAMP(6)",
            "{variant:?}"
        );
        assert_eq!(
            iceberg_primitive_to_exasol(variant, TimestampPrecision::Millisecond),
            "TIMESTAMP",
            "{variant:?}"
        );
    }
}

/// Scenario (datafusion-scan/type-mapping): A parameterized `TIMESTAMP(p)` renders
/// as a timestamp dataType carrying `fractionalSecondsPrecision` — completing the
/// pair `exasol_type_from_json` already reads — instead of silently falling through
/// to the VARCHAR catch-all. The two unparameterized timestamp spellings keep their
/// recorded objects. A malformed `p` (empty, non-numeric, negative) is the recorded
/// exception: it still falls through to the VARCHAR catch-all, because
/// `TimestampPrecision::declaration()` is the only producer of a `TIMESTAMP(p)`
/// string and emits only `TIMESTAMP` and `TIMESTAMP(6)`.
#[test]
fn exasol_type_to_json_renders_timestamp_fractional_seconds_precision() {
    assert_eq!(
        exasol_type_to_json("TIMESTAMP(6)"),
        json!({"type": "timestamp", "fractionalSecondsPrecision": 6})
    );
    assert_eq!(
        exasol_type_to_json("TIMESTAMP(9)"),
        json!({"type": "timestamp", "fractionalSecondsPrecision": 9})
    );
    assert_eq!(
        exasol_type_to_json("TIMESTAMP(0)"),
        json!({"type": "timestamp", "fractionalSecondsPrecision": 0})
    );

    assert_eq!(
        exasol_type_to_json("TIMESTAMP"),
        json!({"type": "timestamp"})
    );
    assert_eq!(
        exasol_type_to_json("TIMESTAMP WITH LOCAL TIME ZONE"),
        json!({"type": "timestamp", "withLocalTimeZone": true})
    );

    for malformed in ["TIMESTAMP()", "TIMESTAMP(abc)", "TIMESTAMP(-1)"] {
        assert_eq!(
            exasol_type_to_json(malformed),
            json!({"type": "varchar", "size": 2_000_000}),
            "malformed={malformed}"
        );
    }

    for declared in [
        "TIMESTAMP",
        "TIMESTAMP(6)",
        "TIMESTAMP WITH LOCAL TIME ZONE",
    ] {
        assert_eq!(
            exasol_type_from_json(&exasol_type_to_json(declared)),
            declared,
            "declared={declared}"
        );
    }
}
