/// Arrow-to-Exasol type mapping — authoritative table shared by createVirtualSchema schema
/// declaration and Arrow→Value conversion in the scan.
///
/// The mapping is pure: no I/O, no external state.
use arrow::datatypes::{DataType, TimeUnit};

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

/// The canonical Arrow `DataType` that the engine's `emit_batch` IPC feed accepts
/// for a column declared as the given Exasol EMITS type string.
///
/// This is the inverse of [`arrow_to_exasol_type`] and is the single source of
/// truth for the type an emitted Arrow column must have so the strict
/// Arrow→ExaType validation in `emit_batch` accepts it. The scan coerces every
/// output column to this target before emitting.
///
/// The input is a declared Exasol type string exactly as it appears in an EMITS
/// clause (e.g. `"DECIMAL(20,0)"`, `"DOUBLE PRECISION"`, `"VARCHAR(2000000)"`).
/// Parsing is case-insensitive and tolerant of surrounding whitespace.
///
/// Returns `None` for a type string that maps to `VARCHAR` (string family) —
/// the caller routes those through the JSON/Utf8 string path, which already
/// handles arbitrary Arrow source types (including the incompatible set). A
/// `VARCHAR` target is intentionally not represented as a single fixed Arrow
/// type because the correct source coercion (display/JSON for incompatible
/// types vs a plain Utf8 cast) depends on the source column, not the target.
///
/// ## DECIMAL precision binning (CRITICAL)
///
/// Exasol does NOT represent every `DECIMAL(p,s)` as the same internal type.
/// The engine bins a `DECIMAL(p,s)` into an ExaType **by precision when the
/// scale is 0**, and `emit_batch` requires the fed Arrow column to match that
/// ExaType's Arrow representation:
///
/// - scale 0, precision ≤ [`DECIMAL_INT32_MAX_PRECISION`] (9)  → ExaType Int32 → Arrow `Int32`
/// - scale 0, precision ≤ [`DECIMAL_INT64_MAX_PRECISION`] (18) → ExaType Int64 → Arrow `Int64`
/// - scale > 0, OR precision 19..=36                            → ExaType Numeric → Arrow `Decimal128(p,s)`
///
/// These 9 / 18 thresholds are the standard Exasol DECIMAL internal
/// representation (precision ≤ 9 fits a 32-bit int, ≤ 18 fits a 64-bit int,
/// ≤ 36 needs 128-bit). Confirmed by:
/// - Exasol "Sizing for Data Types" docs (DECIMAL with total precision ≤ 18
///   fits 64-bit);
/// - the SLC emit block layout (`exa-udf-runtime` `rowset.rs`): `ExaType::Int32`
///   → int32 block, `ExaType::Int64` → int64 block, `ExaType::Numeric` → string
///   (decimal) block;
/// - the two live bench failures: `DECIMAL(10,0)` (COUNT(*)) binned to ExaType
///   Int64 (rejecting a `Decimal128(10,0)` feed), and an Iceberg `int`
///   (`DECIMAL(10,0)`, p≤18) binned to Int64 (rejecting an `Int32` feed).
///
/// A previous version mapped every `DECIMAL(p,s)` → `Decimal128(p,s)`, which is
/// wrong for the integer-binned cases (the engine rejects `Decimal128(10,0)` for
/// an Int64 column).
pub fn exasol_type_to_arrow(exasol_type: &str) -> Option<DataType> {
    let upper = exasol_type.trim().to_uppercase();

    if upper == "BOOLEAN" {
        return Some(DataType::Boolean);
    }
    if upper == "DOUBLE PRECISION" || upper == "DOUBLE" {
        return Some(DataType::Float64);
    }
    if upper == "DATE" {
        return Some(DataType::Date32);
    }
    if upper == "TIMESTAMP" {
        return Some(DataType::Timestamp(TimeUnit::Microsecond, None));
    }
    if upper == "TIMESTAMP WITH LOCAL TIME ZONE" {
        return Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
    }
    if let Some((p, s)) = parse_decimal_args(&upper) {
        // Replicate Exasol's DECIMAL→ExaType precision binning (see doc comment).
        if s == 0 && p <= DECIMAL_INT32_MAX_PRECISION {
            return Some(DataType::Int32);
        }
        if s == 0 && p <= DECIMAL_INT64_MAX_PRECISION {
            return Some(DataType::Int64);
        }
        return Some(DataType::Decimal128(p, s));
    }

    // VARCHAR / CHAR / unknown → string path (handled by the caller, not a fixed
    // Arrow target). Returning None signals "route through the Utf8/JSON path".
    None
}

/// Max precision a scale-0 DECIMAL fits into a 32-bit int (Exasol ExaType Int32).
pub const DECIMAL_INT32_MAX_PRECISION: u8 = 9;

/// Max precision a scale-0 DECIMAL fits into a 64-bit int (Exasol ExaType Int64).
pub const DECIMAL_INT64_MAX_PRECISION: u8 = 18;

/// Parse the `(p,s)` arguments of a `DECIMAL(p,s)` Exasol type string.
///
/// Accepts `DECIMAL(p,s)` and `DECIMAL(p)` (scale defaults to 0). The input is
/// expected to be already upper-cased and trimmed. Returns `None` for any string
/// that is not a well-formed DECIMAL declaration.
fn parse_decimal_args(upper: &str) -> Option<(u8, i8)> {
    let inner = upper.strip_prefix("DECIMAL(")?.strip_suffix(')')?;
    let mut parts = inner.split(',');
    let p: u8 = parts.next()?.trim().parse().ok()?;
    let s: i8 = match parts.next() {
        Some(s_str) => s_str.trim().parse().ok()?,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((p, s))
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
    use arrow::datatypes::DataType;
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
