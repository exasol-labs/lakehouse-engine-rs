use super::*;
use crate::adapter::pushdown::test_support::iceberg_source;
use crate::tests::hive_type_cases::{
    JSON_TEXT, UNRECOGNIZED_HIVE_TYPES, binary_hive_types, nested_hive_types, primitive_hive_types,
};
use arrow::datatypes::DataType;
use iceberg::spec::{PrimitiveType, Type};
use lakehouse_catalog::ColumnSourceType;
use serde_json::json;
use vs_expression::timestamp::TimestampUnit;

/// Scenario: Compatible Arrow types map to their Exasol type
#[test]
fn compatible_types_map_to_exasol_type() {
    assert_eq!(
        arrow_to_exasol_type(&DataType::Boolean).to_string(),
        "BOOLEAN"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Int8).to_string(),
        "DECIMAL(3,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Int16).to_string(),
        "DECIMAL(5,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Int32).to_string(),
        "DECIMAL(10,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Int64).to_string(),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::UInt8).to_string(),
        "DECIMAL(3,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::UInt16).to_string(),
        "DECIMAL(5,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::UInt32).to_string(),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::UInt64).to_string(),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Float32).to_string(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Float64).to_string(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Utf8).to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeUtf8).to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(arrow_to_exasol_type(&DataType::Date32).to_string(), "DATE");
    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).to_string(),
        "TIMESTAMP"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        ))
        .to_string(),
        "TIMESTAMP"
    );
}

#[test]
fn decimal128_in_range_maps_to_decimal() {
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(18, 6)).to_string(),
        "DECIMAL(18,6)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(36, 36)).to_string(),
        "DECIMAL(36,36)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Decimal128(36, 0)).to_string(),
        "DECIMAL(36,0)"
    );
}

#[test]
fn decimal128_out_of_range_maps_to_varchar_json() {
    for (precision, scale) in [(38, 10), (18, 37), (38, 38), (38, 6), (10, -2), (5, 10)] {
        let decimal = DataType::Decimal128(precision, scale);
        assert_eq!(
            arrow_to_exasol_type(&decimal),
            ExasolType::WIDEST_VARCHAR,
            "{decimal}"
        );
        assert!(needs_json_fallback(&decimal), "{decimal}");
    }
}

/// Scenario: Incompatible Arrow types are serialized to JSON VARCHAR
#[test]
fn incompatible_types_map_to_varchar_json() {
    assert_eq!(
        arrow_to_exasol_type(&DataType::List(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", DataType::Int32, true)
        )))
        .to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeList(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", DataType::Int32, true)
        )))
        .to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Struct(arrow::datatypes::Fields::from(vec![
            arrow::datatypes::Field::new("a", DataType::Int32, true)
        ])))
        .to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Binary).to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::LargeBinary).to_string(),
        "VARCHAR(2000000)"
    );
    assert!(needs_json_fallback(&DataType::Binary));
    assert!(needs_json_fallback(&DataType::List(std::sync::Arc::new(
        arrow::datatypes::Field::new("item", DataType::Int32, true)
    ))));
    assert!(!needs_json_fallback(&DataType::Boolean));
    assert!(!needs_json_fallback(&DataType::Decimal128(36, 6)));
}

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

#[test]
fn varchar_type_string_alone_does_not_decide_the_json_fallback() {
    let out_of_range_decimal = DataType::Decimal128(38, 10);

    for string_type in [DataType::Utf8, DataType::LargeUtf8] {
        assert_eq!(
            arrow_to_exasol_type(&string_type).to_string(),
            "VARCHAR(2000000)"
        );
        assert_eq!(
            arrow_to_exasol_type(&string_type).to_string(),
            arrow_to_exasol_type(&out_of_range_decimal).to_string(),
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

#[test]
fn iceberg_types_map_to_exasol_type() {
    let ts_precision = EngineTimestampSupport::MillisecondOnly;
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Boolean), ts_precision).to_string(),
        "BOOLEAN"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Int), ts_precision).to_string(),
        "DECIMAL(10,0)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Long), ts_precision).to_string(),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Float), ts_precision).to_string(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Double), ts_precision).to_string(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::String), ts_precision).to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Date), ts_precision).to_string(),
        "DATE"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Timestamp), ts_precision)
            .to_string(),
        "TIMESTAMP"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Timestamptz), ts_precision)
            .to_string(),
        "TIMESTAMP"
    );
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 18,
                scale: 4,
            }),
            ts_precision
        )
        .to_string(),
        "DECIMAL(18,4)"
    );
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: 10,
            }),
            ts_precision
        )
        .to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 0,
                scale: 0,
            }),
            ts_precision
        )
        .to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 5,
                scale: 10,
            }),
            ts_precision
        )
        .to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Binary), ts_precision).to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Time), ts_precision).to_string(),
        "VARCHAR(2000000)"
    );
}

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
        DataType::FixedSizeBinary(16)
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
        DataType::Binary
    );
    assert_eq!(
        iceberg_type_to_arrow(&Type::Primitive(PrimitiveType::Fixed(16))),
        DataType::FixedSizeBinary(16)
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
            arrow_to_exasol_type(dt).to_string(),
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
        assert_eq!(arrow_to_exasol_type(&dt).to_string(), "DOUBLE PRECISION");
        assert!(!needs_json_fallback(&dt));
    }
}

#[test]
fn string_family_types_and_fallback_flags() {
    for dt in [DataType::Utf8, DataType::LargeUtf8] {
        assert_eq!(arrow_to_exasol_type(&dt).to_string(), "VARCHAR(2000000)");
        assert!(!needs_json_fallback(&dt));
    }
}

#[test]
fn date_time_family_types_and_fallback_flags() {
    assert_eq!(arrow_to_exasol_type(&DataType::Date32).to_string(), "DATE");
    assert!(!needs_json_fallback(&DataType::Date32));

    let ts_no_tz = DataType::Timestamp(TimeUnit::Microsecond, None);
    assert_eq!(arrow_to_exasol_type(&ts_no_tz).to_string(), "TIMESTAMP");
    assert!(!needs_json_fallback(&ts_no_tz));

    let ts_tz = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    assert_eq!(arrow_to_exasol_type(&ts_tz).to_string(), "TIMESTAMP");
    assert!(!needs_json_fallback(&ts_tz));
}

#[test]
fn column_source_type_maps_to_exasol_in_one_home() {
    assert_eq!(
        column_source_type_to_exasol(
            &iceberg_source(PrimitiveType::Long),
            EngineTimestampSupport::MillisecondOnly,
        )
        .to_string(),
        "DECIMAL(20,0)"
    );
    assert_eq!(
        column_source_type_to_exasol(&unity("long"), EngineTimestampSupport::MillisecondOnly,)
            .to_string(),
        "DECIMAL(20,0)"
    );
}

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
        ("decimal128(10,2)", "DECIMAL(10,2)"),
    ] {
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Parquet(tag.to_string()),
                EngineTimestampSupport::MillisecondOnly,
            )
            .to_string(),
            expected,
            "tag `{tag}` must declare `{expected}`"
        );
        assert_eq!(
            column_source_type_to_exasol(
                &ColumnSourceType::Parquet(tag.to_string()),
                EngineTimestampSupport::MillisecondOnly,
            )
            .to_string(),
            arrow_to_exasol_type(&arrow_type_from_tag(tag)).to_string(),
            "tag `{tag}` must resolve through the same parser both routes share"
        );
    }
}

#[test]
fn parquet_timestamp_tag_declares_its_unit_through_the_engine_clamp() {
    for (tag, declared) in [
        ("timestamp_s", "TIMESTAMP"),
        ("timestamp_ms", "TIMESTAMP"),
        ("timestamp_us", "TIMESTAMP(6)"),
        ("timestamp_ns", "TIMESTAMP(9)"),
        ("timestamptz_s", "TIMESTAMP"),
        ("timestamptz_ms", "TIMESTAMP"),
        ("timestamptz_us", "TIMESTAMP(6)"),
        ("timestamptz_ns", "TIMESTAMP(9)"),
    ] {
        let source = ColumnSourceType::Parquet(tag.to_string());
        assert_eq!(
            column_source_type_to_exasol(&source, EngineTimestampSupport::DeclaredPrecision)
                .to_string(),
            declared,
            "tag `{tag}` on an engine honoring the declared precision"
        );
        assert_eq!(
            column_source_type_to_exasol(&source, EngineTimestampSupport::MillisecondOnly)
                .to_string(),
            "TIMESTAMP",
            "tag `{tag}` on a millisecond-only engine"
        );
    }
}

#[test]
fn parquet_column_source_type_with_an_unparseable_tag_resolves_to_varchar_json() {
    assert_eq!(
        column_source_type_to_exasol(
            &ColumnSourceType::Parquet("not-a-real-tag".to_string()),
            EngineTimestampSupport::MillisecondOnly,
        )
        .to_string(),
        "VARCHAR(2000000)"
    );
}

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

#[test]
fn refused_arrow_types_fall_back_to_the_utf8_tag() {
    let tag = arrow_type_to_tag(&DataType::Decimal256(10, 2));
    assert_eq!(tag, "utf8");
    assert_eq!(arrow_type_from_tag(&tag), DataType::Utf8);
}

#[test]
fn every_variable_binary_encoding_tags_as_binary_and_a_fixed_one_keeps_its_width() {
    for dt in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        assert_eq!(arrow_type_to_tag(&dt), "binary");
    }
    assert_eq!(arrow_type_from_tag("binary"), DataType::Binary);
    assert_eq!(
        arrow_type_to_tag(&DataType::FixedSizeBinary(20)),
        "fixed_size_binary(20)"
    );
    assert_eq!(
        arrow_type_from_tag("fixed_size_binary(20)"),
        DataType::FixedSizeBinary(20)
    );
}

#[test]
fn a_binary_column_declares_two_hex_characters_per_byte() {
    assert_eq!(
        arrow_to_exasol_type(&DataType::Binary).to_string(),
        "VARCHAR(2000000)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::FixedSizeBinary(16)).to_string(),
        "VARCHAR(32)"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::FixedSizeBinary(1_000_001)).to_string(),
        "VARCHAR(2000000)",
        "a width whose hex exceeds VARCHAR(2000000) declares the widest VARCHAR"
    );
}

fn unity(spark_type: &str) -> ColumnSourceType {
    ColumnSourceType::Unity {
        type_json: Some(
            json!({"name": "c", "type": spark_type, "nullable": true, "metadata": {}}).to_string(),
        ),
    }
}

#[test]
fn unity_columns_declare_their_type_json() {
    let cases = [
        ("boolean", "BOOLEAN"),
        ("byte", "DECIMAL(3,0)"),
        ("short", "DECIMAL(5,0)"),
        ("integer", "DECIMAL(10,0)"),
        ("long", "DECIMAL(20,0)"),
        ("float", "DOUBLE PRECISION"),
        ("double", "DOUBLE PRECISION"),
        ("string", "VARCHAR(2000000)"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP"),
        ("timestamp_ntz", "TIMESTAMP"),
        ("decimal(10,2)", "DECIMAL(10,2)"),
        ("decimal(18,0)", "DECIMAL(18,0)"),
        ("decimal(36,36)", "DECIMAL(36,36)"),
    ];
    for (spark_type, expected) in cases {
        assert_eq!(
            column_source_type_to_exasol(
                &unity(spark_type),
                EngineTimestampSupport::MillisecondOnly
            )
            .to_string(),
            expected,
            "type_json {spark_type}"
        );
    }
}

#[test]
fn unity_types_exasol_cannot_hold_or_unreadable_type_json_declare_varchar() {
    let unreadable = [
        ColumnSourceType::Unity { type_json: None },
        ColumnSourceType::Unity {
            type_json: Some("{}".to_string()),
        },
    ];
    let spark_types = [
        "binary",
        "variant",
        "interval day to second",
        "geometry(4326)",
        "decimal(38,10)",
        "decimal(18,37)",
        "decimal(0,0)",
        "decimal(5,10)",
    ];
    for source in unreadable.into_iter().chain(spark_types.map(unity)) {
        assert_eq!(
            column_source_type_to_exasol(&source, EngineTimestampSupport::MillisecondOnly)
                .to_string(),
            "VARCHAR(2000000)",
            "{source:?}"
        );
    }
    let array = ColumnSourceType::Unity {
        type_json: Some(
            json!({"name": "c", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": true, "metadata": {}})
                .to_string(),
        ),
    };
    assert_eq!(
        column_source_type_to_exasol(&array, EngineTimestampSupport::MillisecondOnly).to_string(),
        "VARCHAR(2000000)"
    );
}

/// Scenario: The listing declares each Glue column through the Spark listing mapping
#[test]
fn glue_columns_declare_the_spark_listing_type() {
    let primitives = primitive_hive_types().map(|(hive_type, _, declared)| (hive_type, declared));
    let json_text = nested_hive_types()
        .map(|(hive_type, _)| hive_type)
        .into_iter()
        .chain(binary_hive_types().map(|(hive_type, _)| hive_type))
        .chain(UNRECOGNIZED_HIVE_TYPES)
        .map(|hive_type| (hive_type, JSON_TEXT));
    let glue = |hive_type: &str| ColumnSourceType::Glue {
        hive_type: hive_type.to_string(),
    };

    for (hive_type, expected) in primitives.into_iter().chain(json_text) {
        let declared = column_source_type_to_exasol(
            &glue(hive_type),
            EngineTimestampSupport::DeclaredPrecision,
        )
        .to_string();
        assert_eq!(declared, expected, "hive type {hive_type:?}");
    }
    let unity_timestamp = unity("timestamp_ntz");
    for engine in [
        EngineTimestampSupport::MillisecondOnly,
        EngineTimestampSupport::DeclaredPrecision,
    ] {
        assert_eq!(
            column_source_type_to_exasol(&glue("timestamp"), engine).to_string(),
            column_source_type_to_exasol(&unity_timestamp, engine).to_string(),
            "a Glue timestamp declares the engine's timestamp type as Unity does: {engine:?}"
        );
    }
}

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
            &iceberg_source(PrimitiveType::Decimal { precision, scale }),
            EngineTimestampSupport::MillisecondOnly,
        )
        .to_string();
        let unity_result = column_source_type_to_exasol(
            &unity(&format!("decimal({precision},{scale})")),
            EngineTimestampSupport::MillisecondOnly,
        )
        .to_string();
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
        PrimitiveType::Geometry(iceberg::spec::GeometryType::new(None).unwrap()),
        PrimitiveType::Geography(iceberg::spec::GeographyType::default()),
        PrimitiveType::Unknown,
    ];

    for variant in &every_variant {
        let (expected_exasol, expected_arrow) = expected_mapping(variant);
        assert_eq!(
            iceberg_primitive_to_exasol(variant, EngineTimestampSupport::MillisecondOnly)
                .to_string(),
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
        PrimitiveType::Uuid => ("VARCHAR(32)", DataType::FixedSizeBinary(16)),
        PrimitiveType::Fixed(width) => {
            assert_eq!(
                *width, 16,
                "expected_mapping pins only the fixed width every_variant drives"
            );
            ("VARCHAR(32)", DataType::FixedSizeBinary(16))
        }
        PrimitiveType::Binary => ("VARCHAR(2000000)", DataType::Binary),
        PrimitiveType::Geometry(_) => ("VARCHAR(2000000)", DataType::Utf8),
        PrimitiveType::Geography(_) => ("VARCHAR(2000000)", DataType::Utf8),
        PrimitiveType::Unknown => ("VARCHAR(2000000)", DataType::Utf8),
    }
}

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
            resolved.declaration(TimestampUnit::Microsecond).to_string(),
            expected_declaration,
            "version={version}"
        );
    }
}

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
            resolved.declaration(TimestampUnit::Microsecond).to_string(),
            "TIMESTAMP(6)",
            "version={version:?}"
        );
        assert_eq!(
            resolved.declaration(TimestampUnit::Nanosecond).to_string(),
            "TIMESTAMP(9)",
            "version={version:?}"
        );
    }
}

#[test]
fn timestamp_declaration_is_version_gated_for_both_catalog_kinds() {
    let cases = [
        (EngineTimestampSupport::DeclaredPrecision, "TIMESTAMP(6)"),
        (EngineTimestampSupport::MillisecondOnly, "TIMESTAMP"),
    ];
    for (engine, expected) in cases {
        assert_eq!(
            iceberg_primitive_to_exasol(&PrimitiveType::Timestamp, engine).to_string(),
            expected,
            "iceberg timestamp on {engine:?}"
        );
        assert_eq!(
            column_source_type_to_exasol(&unity("timestamp"), engine,).to_string(),
            expected,
            "delta TIMESTAMP on {engine:?}"
        );
        assert_eq!(
            column_source_type_to_exasol(&unity("timestamp_ntz"), engine,).to_string(),
            expected,
            "delta TIMESTAMP_NTZ on {engine:?}"
        );
    }
}

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
            iceberg_primitive_to_exasol(variant, EngineTimestampSupport::DeclaredPrecision)
                .to_string(),
            *unclamped_declaration,
            "{variant:?} on an engine that honors the declared precision"
        );
        assert_eq!(
            iceberg_primitive_to_exasol(variant, EngineTimestampSupport::MillisecondOnly)
                .to_string(),
            "TIMESTAMP",
            "{variant:?} on an engine that emits milliseconds only"
        );
    }
}

#[test]
fn arrow_input_resolver_stays_outside_the_timestamp_version_gate() {
    let _ungated: fn(&DataType) -> ExasolType = arrow_to_exasol_type;

    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).to_string(),
        "TIMESTAMP"
    );
    assert_eq!(
        arrow_to_exasol_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        ))
        .to_string(),
        "TIMESTAMP"
    );
}
