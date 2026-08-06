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

/// Exasol's maximum CHAR width. A wider declaration is rejected outright:
/// `CAST('a' AS CHAR(2001))` fails live with "specified length too long for char
/// type - maximum is 2000". So a CHAR `size` is capped here rather than reusing
/// VARCHAR's 2,000,000 ceiling.
const EXASOL_CHAR_MAX_SIZE: u64 = 2000;

/// The character-set suffix a CHAR/VARCHAR declaration needs, `" ASCII"` or `""`.
///
/// Exasol treats an unsuffixed character declaration as UTF8, so a column it
/// declared `ASCII` must carry the suffix back or its type check reports a "Data
/// type mismatch" (issue #136 follow-up). Shared by the CHAR and the catch-all
/// VARCHAR arm: the rule must be identical for both, and a second copy would
/// drift (issue #52).
fn character_set_suffix(dt: &Json) -> &'static str {
    let is_ascii = dt
        .get("characterSet")
        .and_then(|v| v.as_str())
        .is_some_and(|cs| cs.eq_ignore_ascii_case("ASCII"));
    if is_ascii { " ASCII" } else { "" }
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
        "char" => {
            // A genuine CHAR must stay CHAR: Exasol validates the pushdown output
            // column type positionally, and it declares an equal-length CASE, a bare
            // string literal, and an explicit CAST-to-CHAR as CHAR(n) — rendering
            // those VARCHAR(n) is rejected with "Data type mismatch" (issue #192).
            //
            // An absent `size` is unreachable from a real Exasol `dataType` — but if
            // it occurred, it must NOT default to the maximum width: `CHAR(2000)`
            // would blank-pad every value of that column to 2,000 characters, the
            // most damaging default available. Instead fall back to the project's
            // "unknown width" convention (`VARCHAR(2000000)`), matching
            // `vs-expression`'s `render_cast_target` Exasol CHAR arm. The
            // `EXASOL_CHAR_MAX_SIZE` cap applies only to a PRESENT `size`.
            match dt.get("size").and_then(|v| v.as_u64()) {
                Some(size) => {
                    let capped = size.min(EXASOL_CHAR_MAX_SIZE);
                    format!("CHAR({capped}){}", character_set_suffix(dt))
                }
                None => "VARCHAR(2000000)".to_string(),
            }
        }
        _ => {
            // VARCHAR and all others.
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            format!("VARCHAR({capped}){}", character_set_suffix(dt))
        }
    }
}

#[cfg(test)]
#[path = "mapping_tests.rs"]
mod tests;
