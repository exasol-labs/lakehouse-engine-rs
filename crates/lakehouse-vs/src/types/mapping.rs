/// Arrow-to-Exasol type mapping — authoritative table shared by createVirtualSchema schema
/// declaration and Arrow→Value conversion in the scan.
///
/// The mapping is pure: no I/O, no external state.
use arrow::datatypes::DataType;

/// The Exasol SQL type string for a given Arrow data type.
///
/// Returns `"VARCHAR(2000000)"` for every incompatible Arrow type rather than
/// erroring — incompatible values are serialized to JSON strings in the scan.
pub fn arrow_to_exasol_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),

        // Signed integers
        DataType::Int8 => "DECIMAL(3,0)".to_string(),
        DataType::Int16 => "DECIMAL(5,0)".to_string(),
        DataType::Int32 => "DECIMAL(10,0)".to_string(),
        // Int64/UInt32/UInt64 → DECIMAL(20,0)
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "DECIMAL(20,0)".to_string(),
        // UInt8/UInt16 → DECIMAL(precision,0)
        DataType::UInt8 => "DECIMAL(3,0)".to_string(),
        DataType::UInt16 => "DECIMAL(5,0)".to_string(),

        DataType::Float32 | DataType::Float64 => "DOUBLE PRECISION".to_string(),

        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR(2000000)".to_string(),

        DataType::Date32 => "DATE".to_string(),

        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH LOCAL TIME ZONE".to_string(),

        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => {
            format!("DECIMAL({p},{s})")
        }

        // Out-of-range Decimal128 or any incompatible type: VARCHAR via JSON fallback.
        _ => "VARCHAR(2000000)".to_string(),
    }
}

/// Whether an Arrow DataType needs JSON serialization before crossing the boundary.
/// True for out-of-range Decimal128 and all incompatible types.
pub fn needs_json_fallback(dt: &DataType) -> bool {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Date32 => false,
        DataType::Timestamp(_, _) => false,
        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => false,
        // Everything else needs the JSON fallback (out-of-range Decimal128, all
        // incompatible types: List, LargeList, Struct, Map, Binary, etc.)
        _ => true,
    }
}

/// Map an Iceberg `PrimitiveType` to an Exasol type string, used by
/// `createVirtualSchema`.
pub fn iceberg_primitive_to_exasol(pt: &iceberg::spec::PrimitiveType) -> String {
    use iceberg::spec::PrimitiveType::*;
    match pt {
        Boolean => "BOOLEAN".to_string(),
        Int => "DECIMAL(10,0)".to_string(),
        Long => "DECIMAL(20,0)".to_string(),
        Float => "DOUBLE PRECISION".to_string(),
        Double => "DOUBLE PRECISION".to_string(),
        Decimal { precision, scale } if *precision <= 36 && *scale <= 36 => {
            format!("DECIMAL({precision},{scale})")
        }
        // Out-of-range Decimal
        Decimal { .. } => "VARCHAR(2000000)".to_string(),
        Date => "DATE".to_string(),
        // Time has no Exasol equivalent → VARCHAR via JSON
        Time => "VARCHAR(2000000)".to_string(),
        Timestamp | TimestampNs => "TIMESTAMP".to_string(),
        Timestamptz | TimestamptzNs => "TIMESTAMP WITH LOCAL TIME ZONE".to_string(),
        String | Uuid => "VARCHAR(2000000)".to_string(),
        // Fixed-width binary and arbitrary binary → VARCHAR via JSON
        Fixed(_) | Binary => "VARCHAR(2000000)".to_string(),
    }
}

/// Map an Iceberg `Type` to an Exasol type string.
/// Non-primitive types (List, Struct, Map) → VARCHAR(2000000) via JSON.
pub fn iceberg_type_to_exasol(ty: &iceberg::spec::Type) -> String {
    use iceberg::spec::Type;
    match ty {
        Type::Primitive(pt) => iceberg_primitive_to_exasol(pt),
        // List, Struct, Map → JSON string fallback
        _ => "VARCHAR(2000000)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use iceberg::spec::{PrimitiveType, Type};

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
            "TIMESTAMP WITH LOCAL TIME ZONE"
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
            arrow_to_exasol_type(&DataType::Struct(arrow::datatypes::Fields::empty())),
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

    /// Scenario (D.4): Iceberg-field → Exasol-type schema mapping.
    /// Each Iceberg primitive → correct Exasol type; complex types → VARCHAR(2000000).
    #[test]
    fn iceberg_types_map_to_exasol_type() {
        // primitives
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Boolean)),
            "BOOLEAN"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Int)),
            "DECIMAL(10,0)"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Long)),
            "DECIMAL(20,0)"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Float)),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Double)),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::String)),
            "VARCHAR(2000000)"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Date)),
            "DATE"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Timestamp)),
            "TIMESTAMP"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Timestamptz)),
            "TIMESTAMP WITH LOCAL TIME ZONE"
        );
        // in-range decimal
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Decimal {
                precision: 18,
                scale: 4,
            })),
            "DECIMAL(18,4)"
        );
        // out-of-range decimal → VARCHAR
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: 10,
            })),
            "VARCHAR(2000000)"
        );
        // incompatible primitive → VARCHAR
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Binary)),
            "VARCHAR(2000000)"
        );
        assert_eq!(
            iceberg_type_to_exasol(&Type::Primitive(PrimitiveType::Time)),
            "VARCHAR(2000000)"
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
        assert_eq!(
            arrow_to_exasol_type(&ts_tz),
            "TIMESTAMP WITH LOCAL TIME ZONE"
        );
        assert!(!needs_json_fallback(&ts_tz));
    }
}
