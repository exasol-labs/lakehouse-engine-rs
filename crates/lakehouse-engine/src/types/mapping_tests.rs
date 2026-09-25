use super::*;
use arrow::datatypes::DataType;
use iceberg::spec::{PrimitiveType, Type};
use lakehouse_catalog::ColumnSourceType;

/// Scenario: Compatible Arrow types map to their Exasol type
#[test]
fn compatible_types_map_to_exasol_type() {
    assert_eq!(arrow_to_exasol_type(&DataType::Boolean), "BOOLEAN");
    assert_eq!(arrow_to_exasol_type(&DataType::Int8), "DECIMAL(3,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Int16), "DECIMAL(5,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Int32), "DECIMAL(10,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Int64), "DECIMAL(20,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt8), "DECIMAL(3,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt16), "DECIMAL(5,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt32), "DECIMAL(20,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::UInt64), "DECIMAL(20,0)");
    assert_eq!(arrow_to_exasol_type(&DataType::Float32), "DOUBLE PRECISION");
    assert_eq!(arrow_to_exasol_type(&DataType::Float64), "DOUBLE PRECISION");
    assert_eq!(arrow_to_exasol_type(&DataType::Utf8), "VARCHAR(2000000)");
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeUtf8),
        "VARCHAR(2000000)"
    );
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
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(36, 0)),
        "DECIMAL(36,0)"
    );
}

/// Scenario: Out-of-range Decimal128 falls back to VARCHAR via JSON
#[test]
fn decimal128_out_of_range_maps_to_varchar_json() {
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(38, 10)),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(18, 37)),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(38, 38)),
        "VARCHAR(2000000)"
    );
    assert!(needs_json_fallback(&DataType::Decimal128(38, 6)));
}

/// Scenario: Incompatible Arrow types are serialized to JSON VARCHAR
#[test]
fn incompatible_types_map_to_varchar_json() {
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
    assert!(needs_json_fallback(&DataType::Binary));
    assert!(needs_json_fallback(&DataType::List(std::sync::Arc::new(
        arrow::datatypes::Field::new("item", DataType::Int32, true)
    ))));
    assert!(!needs_json_fallback(&DataType::Boolean));
    assert!(!needs_json_fallback(&DataType::Decimal128(36, 6)));
}

/// Scenario: Incompatible Arrow types are serialized to JSON VARCHAR; only nested types take JSON document rendering
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

/// Scenario: The Delta type mapping's castability claims hold against `arrow-cast` directly
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

    // Text-rendered set: castable to Utf8.
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

    // Binary casts to Utf8 but silently NULLs non-UTF-8 bytes, so it is refused anyway.
    assert!(can_cast_types(&DataType::Binary, &DataType::Utf8));

    // Refused set: a populated struct (a zero-field one does cast).
    assert!(!can_cast_types(&populated_struct, &DataType::Utf8));
    assert!(!can_cast_types(&map, &DataType::Utf8));
    assert!(!can_cast_types(&list_of_struct, &DataType::Utf8));
}

/// Scenario: `arrow-cast`'s `List(Utf8) → Utf8` kernel renders display text, not JSON
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

/// Scenario: One arm list decides both the Exasol type string and the JSON-fallback flag
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

/// Scenario: Each Iceberg primitive maps to its Exasol type; complex types map to VARCHAR(2000000)
#[test]
fn iceberg_types_map_to_exasol_type() {
    let ts_precision = EngineTimestampSupport::MillisecondOnly;
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
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Binary), ts_precision),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Time), ts_precision),
        "VARCHAR(2000000)"
    );
}

/// Scenario: `exasol_type_to_arrow` reproduces Exasol's DECIMAL→ExaType precision binning
#[test]
fn exasol_type_to_arrow_reproduces_decimal_precision_binning() {
    let cases: &[(&str, DataType)] = &[
        ("BOOLEAN", DataType::Boolean),
        ("DOUBLE PRECISION", DataType::Float64),
        ("DATE", DataType::Date32),
        // Exasol's bare TIMESTAMP is TIMESTAMP(3).
        (
            "TIMESTAMP",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        (
            "TIMESTAMP WITH LOCAL TIME ZONE",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        ("DECIMAL(1,0)", DataType::Int32),
        ("DECIMAL(3,0)", DataType::Int32),
        ("DECIMAL(9,0)", DataType::Int32),
        ("DECIMAL(10,0)", DataType::Int64),
        ("DECIMAL(18,0)", DataType::Int64),
        ("DECIMAL(20,0)", DataType::Decimal128(20, 0)),
        ("DECIMAL(19,0)", DataType::Decimal128(19, 0)),
        ("DECIMAL(36,0)", DataType::Decimal128(36, 0)),
        ("DECIMAL(9,2)", DataType::Decimal128(9, 2)),
        ("DECIMAL(18,4)", DataType::Decimal128(18, 4)),
        ("DECIMAL(36,36)", DataType::Decimal128(36, 36)),
    ];
    for (declared, expected_arrow) in cases {
        let arrow = exasol_type_to_arrow(declared)
            .unwrap_or_else(|| panic!("{declared} must map to a concrete Arrow type"));
        assert_eq!(&arrow, expected_arrow, "wrong Arrow target for {declared}");
    }
}

/// Scenario: A `TIMESTAMP(p)` EMITS string maps back to the Arrow unit of that precision
#[test]
fn exasol_type_to_arrow_parses_timestamp_precision() {
    let cases = [
        ("TIMESTAMP(0)", TimeUnit::Millisecond),
        ("TIMESTAMP(6)", TimeUnit::Microsecond),
        ("TIMESTAMP(9)", TimeUnit::Nanosecond),
        ("TIMESTAMP", TimeUnit::Millisecond),
    ];
    for (declared, expected_unit) in cases {
        assert_eq!(
            exasol_type_to_arrow(declared),
            Some(DataType::Timestamp(expected_unit, None)),
            "declared={declared}"
        );
    }
}

/// Scenario: A scale-0 DECIMAL with precision 10..=18 maps to Arrow `Int64`, not `Decimal128`
#[test]
fn exasol_type_to_arrow_count_star_decimal_is_int64() {
    assert_eq!(exasol_type_to_arrow("DECIMAL(10,0)"), Some(DataType::Int64));
    assert_eq!(exasol_type_to_arrow("DECIMAL(18,0)"), Some(DataType::Int64));
    assert_eq!(exasol_type_to_arrow("DECIMAL(9,0)"), Some(DataType::Int32));
}

/// Scenario: String-family and unknown declared types return `None`
#[test]
fn exasol_type_to_arrow_returns_none_for_string_family() {
    assert_eq!(exasol_type_to_arrow("VARCHAR(2000000)"), None);
    assert_eq!(exasol_type_to_arrow("VARCHAR(100)"), None);
    assert_eq!(exasol_type_to_arrow("CHAR(10)"), None);
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
    assert_eq!(exasol_type_to_arrow("DECIMAL(9)"), Some(DataType::Int32));
}

/// Scenario: `iceberg_type_to_arrow` maps every Iceberg type family to its Arrow equivalent
#[test]
fn iceberg_type_to_arrow_maps_all_families() {
    use arrow::datatypes::TimeUnit;

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Boolean)),
        DataType::Boolean
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Int)),
        DataType::Int32
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Long)),
        DataType::Int64
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Float)),
        DataType::Float32
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Double)),
        DataType::Float64
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::String)),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Uuid)),
        DataType::Utf8
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Date)),
        DataType::Date32
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Timestamp)),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::TimestampNs)),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Timestamptz)),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::TimestamptzNs)),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );

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

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 38,
            scale: 10,
        })),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 18,
            scale: 37,
        })),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 0,
            scale: 0,
        })),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Decimal {
            precision: 5,
            scale: 10,
        })),
        DataType::Utf8
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Time)),
        DataType::Utf8
    );

    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Binary)),
        DataType::Utf8
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Fixed(16))),
        DataType::Utf8
    );

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

/// Scenario: Each mapping category's declared Exasol type agrees with its `needs_json_fallback` flag
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

/// Scenario: `exasol_type_to_json` renders a scale-less `DECIMAL(p)` as a decimal object of scale 0
#[test]
fn exasol_type_to_json_absent_decimal_scale_becomes_scale_zero_decimal() {
    assert_eq!(
        exasol_type_to_json("DECIMAL(10)"),
        json!({"type": "decimal", "precision": 10, "scale": 0})
    );
}

/// Scenario: A DECIMAL argument outside the `u8`/`i8` range falls through to the VARCHAR object
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

/// Scenario: A negative DECIMAL scale serializes as a signed JSON number
#[test]
fn exasol_type_to_json_negative_decimal_scale_stays_signed() {
    assert_eq!(
        exasol_type_to_json("DECIMAL(10,-2)"),
        json!({"type": "decimal", "precision": 10, "scale": -2})
    );
}

/// Scenario: Three-argument and empty DECIMAL argument lists fall through to VARCHAR
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

/// Scenario: `exasol_type_from_json` reads `withLocalTimeZone` back as TIMESTAMP WITH LOCAL TIME ZONE
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

/// Scenario: `exasol_type_from_json` renders `fractionalSecondsPrecision` as `TIMESTAMP(p)`, with `withLocalTimeZone` taking precedence
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

/// Scenario: `exasol_type_from_json` appends ` ASCII` for an ASCII `characterSet`
#[test]
fn exasol_type_from_json_propagates_ascii_character_set() {
    let ascii = serde_json::json!({"type": "VARCHAR", "size": 4, "characterSet": "ASCII"});
    assert_eq!(exasol_type_from_json(&ascii), "VARCHAR(4) ASCII");

    let no_charset = serde_json::json!({"type": "VARCHAR", "size": 4});
    assert_eq!(exasol_type_from_json(&no_charset), "VARCHAR(4)");
}

/// Scenario: A CHAR dataType renders as `CHAR(n)`, not `VARCHAR(n)` (#192)
#[test]
fn exasol_type_from_json_renders_char_type() {
    let ascii = serde_json::json!({"type": "CHAR", "size": 3, "characterSet": "ASCII"});
    assert_eq!(exasol_type_from_json(&ascii), "CHAR(3) ASCII");
}

/// Scenario: The CHAR arm appends ` ASCII` only for an ASCII `characterSet`
#[test]
fn exasol_type_from_json_propagates_char_ascii_character_set() {
    let utf8 = serde_json::json!({"type": "CHAR", "size": 20, "characterSet": "UTF8"});
    assert_eq!(exasol_type_from_json(&utf8), "CHAR(20)");

    let no_charset = serde_json::json!({"type": "CHAR", "size": 20});
    assert_eq!(exasol_type_from_json(&no_charset), "CHAR(20)");
}

/// Scenario: The CHAR arm caps `size` at Exasol's 2,000-character maximum
#[test]
fn exasol_type_from_json_caps_char_size_at_exasol_maximum() {
    let oversized = serde_json::json!({"type": "CHAR", "size": 9999});
    assert_eq!(exasol_type_from_json(&oversized), "CHAR(2000)");
}

/// Scenario: A CHAR dataType without `size` falls back to VARCHAR(2000000), not CHAR(2000)
#[test]
fn exasol_type_from_json_char_without_size_falls_back_to_unknown_width() {
    let no_size = serde_json::json!({"type": "CHAR"});
    assert_eq!(exasol_type_from_json(&no_size), "VARCHAR(2000000)");
}

/// Scenario: One classifier names the Exasol type-string families the pushdown guards branch on, including a bare `DECIMAL`
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

/// Scenario: Both an Iceberg-sourced and a Unity-sourced column map through the single `ColumnSourceType` match
#[test]
fn column_source_type_maps_to_exasol_in_one_home() {
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long)),
            EngineTimestampSupport::MillisecondOnly,
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
            EngineTimestampSupport::MillisecondOnly,
        ),
        "DECIMAL(20,0)"
    );
}

/// Scenario: A Parquet-sourced column's declared type and its Arrow tag never drift apart — both routes share the one parser.
#[test]
fn parquet_column_source_type_reads_its_tag_in_lockstep_with_arrow_to_exasol_type() {
    for (tag, expected) in [
        ("bool", "BOOLEAN"),
        ("int32", "DECIMAL(10,0)"),
        ("int64", "DECIMAL(20,0)"),
        ("float32", "DOUBLE PRECISION"),
        ("float64", "DOUBLE PRECISION"),
        ("utf8", "VARCHAR(2000000)"),
        ("date32", "DATE"),
        ("timestamp_us", "TIMESTAMP"),
        ("timestamptz_ns", "TIMESTAMP"),
        ("decimal128(10,2)", "DECIMAL(10,2)"),
    ] {
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Parquet(tag.to_string()),
                EngineTimestampSupport::MillisecondOnly,
            ),
            expected,
            "tag `{tag}` must declare `{expected}`"
        );
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Parquet(tag.to_string()),
                EngineTimestampSupport::MillisecondOnly,
            ),
            arrow_to_exasol_type(&arrow_type_from_tag(tag)),
            "tag `{tag}` must resolve through the same parser both routes share"
        );
    }
}

/// Scenario: An unparseable Arrow tag still resolves — falls through to the JSON VARCHAR fallback.
#[test]
fn parquet_column_source_type_with_an_unparseable_tag_resolves_to_varchar_json() {
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Parquet("not-a-real-tag".to_string()),
            EngineTimestampSupport::MillisecondOnly,
        ),
        "VARCHAR(2000000)"
    );
}

/// Scenario: Every admitted Arrow type round-trips through its tag; a tz-aware timestamp comes back labelled `"UTC"`
#[test]
fn every_admitted_arrow_type_round_trips_through_its_tag() {
    use arrow::datatypes::TimeUnit;

    let mut admitted = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Date32,
        DataType::Decimal128(10, 2),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        admitted.push(DataType::Timestamp(unit, None));
        admitted.push(DataType::Timestamp(unit, Some("UTC".into())));
    }

    for dt in admitted {
        let tag = arrow_type_to_tag(&dt);
        assert_eq!(
            arrow_type_from_tag(&tag),
            dt,
            "tag `{tag}` rendered from {dt:?} must parse back to it"
        );
    }
}

/// Scenario: A refused Arrow type still renders/parses via the `"utf8"` fallback tag.
#[test]
fn refused_arrow_types_fall_back_to_the_utf8_tag() {
    for dt in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Decimal256(10, 2),
    ] {
        let tag = arrow_type_to_tag(&dt);
        assert_eq!(tag, "utf8");
        assert_eq!(arrow_type_from_tag(&tag), DataType::Utf8);
    }
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
            column_source_type_to_exasol(&source, EngineTimestampSupport::MillisecondOnly),
            expected,
            "type_name={type_name} precision={precision} scale={scale}"
        );
    }
}

/// Scenario: An incompatible Unity Catalog column type and an out-of-range DECIMAL both fall back to VARCHAR
#[test]
fn incompatible_unity_types_declared_varchar() {
    let ts_precision = EngineTimestampSupport::MillisecondOnly;
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

/// Scenario: A catalog-declared DECIMAL outside Exasol's domain falls back to VARCHAR identically for both catalog kinds
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
            EngineTimestampSupport::MillisecondOnly,
        );
        let unity_result = column_source_type_to_exasol(
            &ColumnSourceType::Unity {
                type_name: "DECIMAL".to_string(),
                precision,
                scale,
            },
            EngineTimestampSupport::MillisecondOnly,
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

/// Scenario: The Iceberg primitive mappings are exhaustive matches, so a new `PrimitiveType` variant breaks the build
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
            iceberg_primitive_to_exasol(variant, EngineTimestampSupport::MillisecondOnly),
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

/// Scenario: The engine version picks the timestamp declaration for a microsecond source
#[test]
fn database_version_leading_component_selects_the_declared_timestamp_precision() {
    use EngineTimestampSupport::{DeclaredPrecision, MillisecondOnly};
    let cases = [
        ("2025.2.1", DeclaredPrecision, "TIMESTAMP(6)"),
        ("2026.1.0", DeclaredPrecision, "TIMESTAMP(6)"),
        ("2025", DeclaredPrecision, "TIMESTAMP(6)"),
        ("2024.12.31", MillisecondOnly, "TIMESTAMP"),
        ("8.29.13", MillisecondOnly, "TIMESTAMP"),
        ("7.1.20", MillisecondOnly, "TIMESTAMP"),
    ];
    for (version, expected, expected_declaration) in cases {
        let resolved = EngineTimestampSupport::from_database_version(version);
        assert_eq!(resolved, expected, "version={version}");
        assert_eq!(
            resolved
                .clamp(TimestampPrecision::Microsecond)
                .declaration(),
            expected_declaration,
            "version={version}"
        );
    }
}

/// Scenario: An empty or unparseable version takes the 2025.x arm
#[test]
fn unreadable_database_version_declares_the_source_width_unclamped() {
    for version in ["", "v2025.2.1", "unknown", ".2.1", "8x.1.0", " "] {
        let resolved = EngineTimestampSupport::from_database_version(version);
        assert_eq!(
            resolved,
            EngineTimestampSupport::DeclaredPrecision,
            "version={version:?}"
        );
        assert_eq!(
            resolved
                .clamp(TimestampPrecision::Microsecond)
                .declaration(),
            "TIMESTAMP(6)",
            "version={version:?}"
        );
        assert_eq!(
            resolved.clamp(TimestampPrecision::Nanosecond).declaration(),
            "TIMESTAMP(9)",
            "version={version:?}"
        );
    }
}

/// Scenario: An Iceberg `timestamp` and a Delta `TIMESTAMP` are declared at the same resolved precision
#[test]
fn timestamp_declaration_is_version_gated_for_both_catalog_kinds() {
    let cases = [
        (EngineTimestampSupport::DeclaredPrecision, "TIMESTAMP(6)"),
        (EngineTimestampSupport::MillisecondOnly, "TIMESTAMP"),
    ];
    for (engine, expected) in cases {
        assert_eq!(
            iceberg_primitive_to_exasol(&PrimitiveType::Timestamp, engine),
            expected,
            "iceberg timestamp on {engine:?}"
        );
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Unity {
                    type_name: "TIMESTAMP".to_string(),
                    precision: 0,
                    scale: 0,
                },
                engine,
            ),
            expected,
            "delta TIMESTAMP on {engine:?}"
        );
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Unity {
                    type_name: "TIMESTAMP_NTZ".to_string(),
                    precision: 0,
                    scale: 0,
                },
                engine,
            ),
            expected,
            "delta TIMESTAMP_NTZ on {engine:?}"
        );
    }
}

/// Scenario: Each Iceberg timestamp variant is declared at its own width, and all take bare `TIMESTAMP` when clamped
#[test]
fn every_iceberg_timestamp_variant_declares_its_own_source_width() {
    let cases = [
        (PrimitiveType::Timestamp, "TIMESTAMP(6)"),
        (PrimitiveType::Timestamptz, "TIMESTAMP(6)"),
        (PrimitiveType::TimestampNs, "TIMESTAMP(9)"),
        (PrimitiveType::TimestamptzNs, "TIMESTAMP(9)"),
    ];
    for (variant, unclamped_declaration) in &cases {
        assert_eq!(
            iceberg_primitive_to_exasol(variant, EngineTimestampSupport::DeclaredPrecision),
            *unclamped_declaration,
            "{variant:?} on an engine that honors the declared precision"
        );
        assert_eq!(
            iceberg_primitive_to_exasol(variant, EngineTimestampSupport::MillisecondOnly),
            "TIMESTAMP",
            "{variant:?} on an engine that emits milliseconds only"
        );
    }
}

/// Scenario: A declared precision resolves to the coarsest width not coarser than it, floored at millisecond
#[test]
fn declared_digits_resolve_to_the_arrow_unit_of_their_source_width() {
    let cases = [
        (0, TimeUnit::Millisecond),
        (1, TimeUnit::Millisecond),
        (2, TimeUnit::Millisecond),
        (3, TimeUnit::Millisecond),
        (4, TimeUnit::Microsecond),
        (5, TimeUnit::Microsecond),
        (6, TimeUnit::Microsecond),
        (7, TimeUnit::Nanosecond),
        (8, TimeUnit::Nanosecond),
        (9, TimeUnit::Nanosecond),
    ];
    for (digits, expected_unit) in cases {
        assert_eq!(
            TimestampPrecision::from_declared_digits(digits).arrow_unit(),
            expected_unit,
            "declared precision={digits}"
        );
    }
}

/// Scenario: `exasol_type_to_arrow` takes no `TimestampPrecision`; the function-pointer binding is the assertion
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

/// Scenario: `TIMESTAMP(p)` renders as a timestamp dataType with `fractionalSecondsPrecision`; a malformed `p` falls through to VARCHAR
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
