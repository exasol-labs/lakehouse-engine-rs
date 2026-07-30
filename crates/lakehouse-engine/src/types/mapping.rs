//! Arrow-to-Exasol type mapping — authoritative table shared by createVirtualSchema schema
//! declaration and Arrow→Value conversion in the scan.
//!
//! This module owns the conversions between the three representations a type takes
//! as it crosses the VS boundary: the Arrow `DataType` (the scan's in-process
//! representation), the Exasol SQL type string (e.g. `"DECIMAL(20,0)"`, as it appears
//! in an EMITS clause or a pushed-down column declaration), and the VS `dataType`
//! JSON object (the Exasol Virtual Schema protocol's own wire shape for a column's
//! type, e.g. `{"type": "decimal", "precision": 20, "scale": 0}`, sent and received
//! in `createVirtualSchema` / pushdown request-response payloads).
//!
//! The mapping is pure: no I/O, no external state — the `serde_json` import is for
//! the JSON object's in-process `Value` representation (one of the three
//! representations above), not for performing I/O. The module owns a wire
//! representation, never a wire operation; reading/writing the request or response
//! itself is the adapter layer's job.
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::{Value as Json, json};

/// The Exasol SQL type string for a given Arrow data type.
///
/// Returns `"VARCHAR(2000000)"` for every incompatible Arrow type rather than
/// erroring — incompatible values are serialized to JSON strings in the scan.
pub fn arrow_to_exasol_type(dt: &DataType) -> String {
    match compatible_exasol_type(dt) {
        Some(CompatibleExaType::Fixed(s)) => s.to_string(),
        Some(CompatibleExaType::Decimal(p, s)) => format!("DECIMAL({p},{s})"),
        None => "VARCHAR(2000000)".to_string(),
    }
}

/// A compatible Exasol type, deferring string formatting until the caller needs
/// it — `needs_json_fallback` only needs the `Option`'s discriminant, not the
/// rendered string.
enum CompatibleExaType {
    Fixed(&'static str),
    Decimal(u8, i8),
}

/// The Exasol type for an Arrow type Exasol represents directly, or `None` for
/// one that has to cross the boundary as a JSON string.
///
/// `None` IS the JSON-fallback flag: `Utf8` and an out-of-range `Decimal128` both
/// surface as `VARCHAR(2000000)`, so the rendered string cannot separate the type
/// that crosses unchanged from the one that must be serialized first.
fn compatible_exasol_type(dt: &DataType) -> Option<CompatibleExaType> {
    let exasol_type = match dt {
        DataType::Boolean => "BOOLEAN",

        // Integers, signed and unsigned, by the precision that holds their range.
        DataType::Int8 | DataType::UInt8 => "DECIMAL(3,0)",
        DataType::Int16 | DataType::UInt16 => "DECIMAL(5,0)",
        DataType::Int32 => "DECIMAL(10,0)",
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "DECIMAL(20,0)",

        DataType::Float32 | DataType::Float64 => "DOUBLE PRECISION",

        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR(2000000)",

        DataType::Date32 => "DATE",

        // Both timezone-naive and timezone-aware timestamps map to plain Exasol
        // TIMESTAMP: Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS
        // output type, and an Iceberg timestamptz is a UTC instant, so the emitted
        // value is unchanged. The internal tz-aware Arrow label is preserved
        // elsewhere (arrow_type_to_tag / exasol_type_to_arrow).
        DataType::Timestamp(_, _) => "TIMESTAMP",

        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => {
            return Some(CompatibleExaType::Decimal(*p, *s));
        }

        // Every incompatible type (List, Struct, Map, Binary, ...): VARCHAR via
        // the JSON fallback.
        _ => return None,
    };
    Some(CompatibleExaType::Fixed(exasol_type))
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
    if upper == "TIMESTAMP" || (upper.starts_with("TIMESTAMP(") && upper.ends_with(')')) {
        // Every declared TIMESTAMP(p) precision collapses to the same Arrow
        // microsecond representation; `p` is Exasol's own type-check concern,
        // never the internal Arrow representation (see decision-log #212).
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
/// Accepts `DECIMAL(p,s)` and `DECIMAL(p)` (scale defaults to 0). The input must
/// be upper-cased, and the `DECIMAL(` prefix must start at offset 0 — leading or
/// trailing whitespace around the whole string yields `None`. Whitespace around
/// each individual argument (`p`, `s`), by contrast, IS trimmed before parsing.
/// Returns `None` for any string that is not a well-formed DECIMAL declaration.
///
/// This is the only implementation of the Exasol `DECIMAL` argument grammar; it
/// is `pub(crate)` so every consumer of an Exasol type string reads precision and
/// scale the same way, rather than re-deriving the parse and silently disagreeing
/// on the edges (an absent scale, a negative scale, an out-of-range argument).
pub(crate) fn parse_decimal_args(upper: &str) -> Option<(u8, i8)> {
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
    compatible_exasol_type(dt).is_none()
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
        // timestamptz collapses to plain Exasol TIMESTAMP alongside timestamp:
        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS output type,
        // and an Iceberg timestamptz is a UTC instant, so the emitted value is
        // unchanged. The internal tz-aware Arrow representation is kept in
        // iceberg_primitive_to_arrow.
        Timestamp | TimestampNs | Timestamptz | TimestamptzNs => "TIMESTAMP".to_string(),
        String | Uuid => "VARCHAR(2000000)".to_string(),
        // Fixed-width binary and arbitrary binary → VARCHAR via JSON
        Fixed(_) | Binary => "VARCHAR(2000000)".to_string(),
    }
}

/// Map an Iceberg `PrimitiveType` to an Arrow `DataType`, used for building the
/// logical Arrow schema carried in `ScanSpec::logical_schema`.
///
/// Matches the Exasol mapping in [`iceberg_primitive_to_exasol`]: in-range
/// Decimal128 maps to `Decimal128(p, s)`, out-of-range Decimal and types with no
/// Arrow equivalent (Time, Fixed, Binary) map to `Utf8` (surfaced as JSON VARCHAR).
///
/// This is the logical Iceberg→Arrow mapping and is unaffected by physical
/// Parquet INT96 decode coercion, which is a scan-layer concern (see
/// `crates/lakehouse-engine/src/scan/`) — not this file.
pub fn iceberg_primitive_to_arrow(pt: &iceberg::spec::PrimitiveType) -> DataType {
    use arrow::datatypes::TimeUnit;
    use iceberg::spec::PrimitiveType::*;
    match pt {
        Boolean => DataType::Boolean,
        Int => DataType::Int32,
        Long => DataType::Int64,
        Float => DataType::Float32,
        Double => DataType::Float64,
        Decimal { precision, scale } if *precision <= 36 && *scale <= 36 => {
            DataType::Decimal128(*precision as u8, *scale as i8)
        }
        Decimal { .. } => DataType::Utf8,
        Date => DataType::Date32,
        Time => DataType::Utf8,
        Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        TimestamptzNs => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        String | Uuid => DataType::Utf8,
        Fixed(_) | Binary => DataType::Utf8,
    }
}

/// Map an Iceberg `Type` to an Arrow `DataType`.
/// Non-primitive types (List, Struct, Map) → `Utf8` (JSON fallback).
pub fn iceberg_type_to_arrow(ty: &iceberg::spec::Type) -> DataType {
    use iceberg::spec::Type;
    match ty {
        Type::Primitive(pt) => iceberg_primitive_to_arrow(pt),
        _ => DataType::Utf8,
    }
}

/// Convert an Arrow `DataType` to the compact string tag used in `ScanSpec::logical_schema`.
///
/// The tag vocabulary covers every type reachable from [`iceberg_type_to_arrow`]:
/// - `"bool"`, `"int32"`, `"int64"`, `"float32"`, `"float64"`, `"utf8"`, `"date32"`
/// - `"timestamp_us"`, `"timestamp_ns"`, `"timestamptz_us"`, `"timestamptz_ns"`
/// - `"decimal128(p,s)"` for in-range `Decimal128`
///
/// Unknown / other types fall back to `"utf8"` (JSON VARCHAR path).
pub fn arrow_type_to_tag(dt: &DataType) -> String {
    use arrow::datatypes::TimeUnit;
    match dt {
        DataType::Boolean => "bool".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_us".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp_ns".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamptz_us".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => "timestamptz_ns".to_string(),
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        _ => "utf8".to_string(),
    }
}

/// Parse a compact string tag (from `ScanSpec::logical_schema`) back to an Arrow `DataType`.
///
/// Returns `DataType::Utf8` for any unrecognised tag — the JSON VARCHAR fallback.
///
/// The lowercase `decimal128(p,s)` parse below is deliberately NOT routed through
/// [`parse_decimal_args`], despite the surface similarity. It reads the internal
/// `ScanSpec::logical_schema` tag vocabulary produced by [`arrow_type_to_tag`] —
/// a different wire format from the Exasol SQL type grammar, changing for a
/// different reason — and it requires BOTH arguments, where `parse_decimal_args`
/// defaults an absent scale to `0`. Sharing one parser between the two would
/// either accept a scale-less `decimal128(p)` tag no producer emits or force a
/// prefix/arity parameter onto the Exasol-side parser.
pub fn arrow_type_from_tag(tag: &str) -> DataType {
    use arrow::datatypes::TimeUnit;
    match tag {
        "bool" => DataType::Boolean,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "date32" => DataType::Date32,
        "timestamp_us" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamptz_us" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        other => {
            // Try to parse "decimal128(p,s)"
            if let Some(inner) = other
                .strip_prefix("decimal128(")
                .and_then(|s| s.strip_suffix(')'))
            {
                let mut parts = inner.splitn(2, ',');
                if let (Some(p_str), Some(s_str)) = (parts.next(), parts.next())
                    && let (Ok(p), Ok(s)) = (p_str.trim().parse::<u8>(), s_str.trim().parse::<i8>())
                {
                    return DataType::Decimal128(p, s);
                }
            }
            DataType::Utf8
        }
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

/// The family an Exasol SQL type string belongs to, as the pushdown guards
/// (`guard_like_subject`, `is_bare_decimal_column`, `coerce_string_position_arg` in
/// `adapter/pushdown/support.rs`) branch on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExaTypeClass {
    Character,
    Date,
    Decimal,
    Other,
}

/// Classify an Exasol SQL type string into the family the three pushdown guards in
/// `adapter/pushdown/support.rs` — `guard_like_subject`, `is_bare_decimal_column`,
/// and `coerce_string_position_arg` — branch on.
///
/// - [`ExaTypeClass::Character`] iff the string starts with `"VARCHAR"` or `"CHAR"`.
/// - [`ExaTypeClass::Decimal`] iff the string starts with `"DECIMAL"` — deliberately
///   NOT `"DECIMAL("`, because the bare string `DECIMAL` (no arguments) is also
///   classified as DECIMAL by all three guards.
/// - [`ExaTypeClass::Date`] iff the string equals `"DATE"` exactly.
/// - [`ExaTypeClass::Other`] otherwise.
///
/// The column-lookup-miss case the three guards also decline on is the caller's own
/// branch (an absent/unresolved column type), not part of this classifier, which
/// takes a resolved type string.
pub fn classify_exa_type(type_str: &str) -> ExaTypeClass {
    if type_str.starts_with("VARCHAR") || type_str.starts_with("CHAR") {
        ExaTypeClass::Character
    } else if type_str.starts_with("DECIMAL") {
        ExaTypeClass::Decimal
    } else if type_str == "DATE" {
        ExaTypeClass::Date
    } else {
        ExaTypeClass::Other
    }
}

/// Convert an Exasol type string to the VS column dataType JSON object.
/// Minimal implementation covering the types produced by our mapping.
pub(crate) fn exasol_type_to_json(exasol_type: &str) -> Json {
    let upper = exasol_type.to_uppercase();
    if upper == "BOOLEAN" {
        return json!({"type": "boolean"});
    }
    if upper == "DOUBLE PRECISION" {
        return json!({"type": "double"});
    }
    if upper == "DATE" {
        return json!({"type": "date"});
    }
    if upper == "TIMESTAMP" {
        return json!({"type": "timestamp"});
    }
    if upper == "TIMESTAMP WITH LOCAL TIME ZONE" {
        return json!({"type": "timestamp", "withLocalTimeZone": true});
    }
    if let Some((p, s)) = parse_decimal_args(&upper) {
        // `s` stays an `i8` here: serialized as a SIGNED JSON number so a negative
        // Arrow decimal scale can never wrap into a large unsigned value.
        return json!({"type": "decimal", "precision": p, "scale": s});
    }
    // Default: VARCHAR(size)
    let size = if let Some(inner) = upper
        .strip_prefix("VARCHAR(")
        .and_then(|s| s.strip_suffix(')'))
    {
        inner.trim().parse::<u64>().unwrap_or(2000000)
    } else {
        2000000
    };
    json!({"type": "varchar", "size": size})
}

/// Derive an Exasol type string from the VS column dataType JSON.
pub(crate) fn exasol_type_from_json(dt: &Json) -> String {
    let type_name = dt.get("type").and_then(|t| t.as_str()).unwrap_or("varchar");
    match type_name.to_lowercase().as_str() {
        "boolean" => "BOOLEAN".to_string(),
        "decimal" => {
            let p = dt.get("precision").and_then(|v| v.as_u64()).unwrap_or(18);
            let s = dt.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            if p <= 36 && s <= 36 {
                format!("DECIMAL({p},{s})")
            } else {
                "VARCHAR(2000000)".to_string()
            }
        }
        "double" => "DOUBLE PRECISION".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => {
            let with_local_time_zone = dt
                .get("withLocalTimeZone")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if with_local_time_zone {
                "TIMESTAMP WITH LOCAL TIME ZONE".to_string()
            } else {
                match dt
                    .get("fractionalSecondsPrecision")
                    .and_then(|v| v.as_u64())
                {
                    Some(p) => format!("TIMESTAMP({p})"),
                    None => "TIMESTAMP".to_string(),
                }
            }
        }
        _ => {
            // VARCHAR, CHAR, and all others.
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            let is_ascii = dt
                .get("characterSet")
                .and_then(|v| v.as_str())
                .is_some_and(|cs| cs.eq_ignore_ascii_case("ASCII"));
            if is_ascii {
                format!("VARCHAR({capped}) ASCII")
            } else {
                format!("VARCHAR({capped})")
            }
        }
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
            "TIMESTAMP"
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
    /// confirmed by `vs-expression`'s `renders_cast_char_as_varchar` test) and append
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
}
