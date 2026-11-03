//! Arrow-to-Exasol type mapping shared by `createVirtualSchema` and the scan's Arrow→Value
//! conversion. Pure: no I/O.
use arrow::datatypes::{DataType, TimeUnit};
use delta_kernel::schema::{DataType as SparkType, PrimitiveType as SparkPrimitive, StructField};
use lakehouse_catalog::ColumnSourceType;

use super::exasol_type::{ExasolType, MAX_CHAR_SIZE, MAX_VARCHAR_SIZE};
use super::hive_type::parse_hive_type;
use serde_json::{Value as Json, json};

/// Returns `VARCHAR(2000000)` for every incompatible Arrow type rather than erroring;
/// the scan serializes those values to JSON strings, and binary values to lowercase hex.
pub fn arrow_to_exasol_type(dt: &DataType) -> ExasolType {
    if let DataType::FixedSizeBinary(width) = dt {
        return hex_varchar(u64::try_from(*width).unwrap_or(0));
    }
    compatible_exasol_type(dt).unwrap_or(ExasolType::WIDEST_VARCHAR)
}

/// `None` is the JSON-fallback flag: `Utf8` and an out-of-range `Decimal128` both map to
/// `VARCHAR(2000000)`, so the type alone cannot tell them apart.
fn compatible_exasol_type(dt: &DataType) -> Option<ExasolType> {
    let exasol_type = match dt {
        DataType::Boolean => ExasolType::Boolean,

        DataType::Int8 | DataType::UInt8 => decimal(3, 0),
        DataType::Int16 | DataType::UInt16 => decimal(5, 0),
        DataType::Int32 => decimal(10, 0),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => decimal(20, 0),

        DataType::Float32 | DataType::Float64 => ExasolType::DoublePrecision,

        DataType::Utf8 | DataType::LargeUtf8 => ExasolType::WIDEST_VARCHAR,

        DataType::Date32 => ExasolType::Date,

        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS type; a tz-aware
        // value is a UTC instant, so plain TIMESTAMP emits it unchanged.
        DataType::Timestamp(_, _) => ExasolType::Timestamp { precision: None },

        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => decimal(*p, *s),

        _ => return None,
    };
    Some(exasol_type)
}

fn decimal(precision: u8, scale: i8) -> ExasolType {
    ExasolType::Decimal { precision, scale }
}

const EXASOL_DECIMAL_MAX_PRECISION: u32 = 36;

pub fn needs_json_fallback(dt: &DataType) -> bool {
    compatible_exasol_type(dt).is_none()
}

/// The largest binary value whose lowercase hex rendering fits `VARCHAR(2000000)`.
pub const MAX_BINARY_BYTES: usize = 1_000_000;

/// Two hex characters per byte, so a `fixed(L)` column declares `VARCHAR(2L)`.
pub(crate) fn hex_varchar(width: u64) -> ExasolType {
    let chars = width.saturating_mul(2);
    match u32::try_from(chars) {
        Ok(size) if (1..=MAX_VARCHAR_SIZE).contains(&size) => {
            ExasolType::Varchar { size, ascii: false }
        }
        _ => ExasolType::WIDEST_VARCHAR,
    }
}

/// Narrower than `needs_json_fallback`: binary renders as hex and an out-of-range `Decimal128`
/// keeps the `CAST(col AS VARCHAR)` Arrow-display path instead of JSON document rendering.
pub fn needs_nested_json_rendering(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    )
}

/// Exasol's DECIMAL domain for catalog-declared decimals: `1 <= p <= 36`, `s <= p` (live:
/// `DECIMAL(0,0)` and `DECIMAL(5,10)` are rejected with SQL state 42000). Every catalog
/// decimal consumer reads this so the Exasol type, Arrow tag, and default encoding agree.
/// The Arrow-input guards don't: an Arrow scale may be negative, and `exasol_type_from_json`
/// reads a type Exasol already accepted.
pub(crate) fn exasol_representable_catalog_decimal(precision: u32, scale: u32) -> bool {
    (1..=EXASOL_DECIMAL_MAX_PRECISION).contains(&precision) && scale <= precision
}

fn catalog_decimal_to_exasol(precision: u32, scale: u32) -> ExasolType {
    if exasol_representable_catalog_decimal(precision, scale) {
        decimal(precision as u8, scale as i8)
    } else {
        ExasolType::WIDEST_VARCHAR
    }
}

const FIRST_CALENDAR_VERSIONED_MAJOR: u32 = 2025;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampPrecision {
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// Significant fraction digits of an Exasol timestamp literal; trailing zeros add no precision.
pub fn timestamp_literal_digits(literal: &str) -> usize {
    literal
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len())
}

impl TimestampPrecision {
    pub fn declaration(self) -> ExasolType {
        let precision = match self {
            Self::Millisecond => None,
            Self::Microsecond => Some(6),
            Self::Nanosecond => Some(9),
        };
        ExasolType::Timestamp { precision }
    }

    pub fn arrow_unit(self) -> TimeUnit {
        match self {
            Self::Millisecond => TimeUnit::Millisecond,
            Self::Microsecond => TimeUnit::Microsecond,
            Self::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    /// `Second` floors at millisecond like [`Self::from_declared_digits`].
    pub fn from_arrow_unit(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second | TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }

    /// Rounds up to the coarsest width holding every declared digit, floored at millisecond
    /// because no emit path feeds Arrow's `Second` unit.
    pub fn from_declared_digits(digits: u32) -> Self {
        match digits {
            0..=3 => Self::Millisecond,
            4..=6 => Self::Microsecond,
            _ => Self::Nanosecond,
        }
    }
}

/// Exasol 8.x accepts a parameterized timestamp declaration but silently downgrades it to
/// `TIMESTAMP(3)`; clamping keeps `SYS.EXA_ALL_COLUMNS` honest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineTimestampSupport {
    MillisecondOnly,
    DeclaredPrecision,
}

impl EngineTimestampSupport {
    /// An unparseable version maps to `DeclaredPrecision`: misdeclaring on 8.x is harmless,
    /// whereas truncating on an unrecognised newer engine would lose data.
    pub fn from_database_version(version: &str) -> Self {
        match version
            .split('.')
            .next()
            .and_then(|major| major.parse::<u32>().ok())
        {
            Some(major) if major < FIRST_CALENDAR_VERSIONED_MAJOR => Self::MillisecondOnly,
            _ => Self::DeclaredPrecision,
        }
    }

    pub fn clamp(self, source: TimestampPrecision) -> TimestampPrecision {
        match self {
            Self::MillisecondOnly => TimestampPrecision::Millisecond,
            Self::DeclaredPrecision => source,
        }
    }
}

pub fn iceberg_primitive_to_exasol(
    pt: &iceberg::spec::PrimitiveType,
    engine: EngineTimestampSupport,
) -> ExasolType {
    use iceberg::spec::PrimitiveType::*;
    match pt {
        Boolean => ExasolType::Boolean,
        Int => decimal(10, 0),
        Long => decimal(20, 0),
        Float | Double => ExasolType::DoublePrecision,
        Decimal { precision, scale } => catalog_decimal_to_exasol(*precision, *scale),
        Date => ExasolType::Date,
        Time => ExasolType::WIDEST_VARCHAR,
        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS type; timestamptz is
        // a UTC instant, so plain TIMESTAMP emits it unchanged.
        Timestamp | Timestamptz => engine.clamp(TimestampPrecision::Microsecond).declaration(),
        TimestampNs | TimestamptzNs => engine.clamp(TimestampPrecision::Nanosecond).declaration(),
        String | Binary => ExasolType::WIDEST_VARCHAR,
        Fixed(width) => hex_varchar(*width),
        Uuid => hex_varchar(UUID_BYTES),
        // Refused at declaration; the type only fills the column's declared slot.
        Geometry(_) | Geography(_) | Unknown => ExasolType::WIDEST_VARCHAR,
    }
}

/// Must stay in lockstep with [`iceberg_primitive_to_exasol`] on the decimal domain: a
/// `Decimal128` tag for a column declared VARCHAR breaks `emit_batch`'s Arrow→ExaType
/// validation, and arrow-rs rejects `precision == 0` and `scale > precision` outright.
pub fn iceberg_primitive_to_arrow(pt: &iceberg::spec::PrimitiveType) -> DataType {
    use arrow::datatypes::TimeUnit;
    use iceberg::spec::PrimitiveType::*;
    match pt {
        Boolean => DataType::Boolean,
        Int => DataType::Int32,
        Long => DataType::Int64,
        Float => DataType::Float32,
        Double => DataType::Float64,
        Decimal { precision, scale } => {
            if exasol_representable_catalog_decimal(*precision, *scale) {
                DataType::Decimal128(*precision as u8, *scale as i8)
            } else {
                DataType::Utf8
            }
        }
        Date => DataType::Date32,
        Time => DataType::Utf8,
        Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        TimestamptzNs => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        String => DataType::Utf8,
        Binary => DataType::Binary,
        Fixed(width) => DataType::FixedSizeBinary(i32::try_from(*width).unwrap_or(i32::MAX)),
        Uuid => DataType::FixedSizeBinary(UUID_BYTES as i32),
        Geometry(_) | Geography(_) | Unknown => DataType::Utf8,
    }
}

const UUID_BYTES: u64 = 16;

pub fn iceberg_type_to_arrow(ty: &iceberg::spec::Type) -> DataType {
    use iceberg::spec::Type;
    match ty {
        Type::Primitive(pt) => iceberg_primitive_to_arrow(pt),
        _ => DataType::Utf8,
    }
}

pub fn arrow_type_to_tag(dt: &DataType) -> String {
    use arrow::datatypes::TimeUnit;
    match dt {
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Timestamp(TimeUnit::Second, None) => "timestamp_s".to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, None) => "timestamp_ms".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_us".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp_ns".to_string(),
        DataType::Timestamp(TimeUnit::Second, Some(_)) => "timestamptz_s".to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => "timestamptz_ms".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamptz_us".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => "timestamptz_ns".to_string(),
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".to_string(),
        DataType::FixedSizeBinary(width) => format!("fixed_size_binary({width})"),
        _ => "utf8".to_string(),
    }
}

/// Returns `DataType::Utf8` for any unrecognised tag. The `decimal128(p,s)` tag requires both
/// arguments.
pub fn arrow_type_from_tag(tag: &str) -> DataType {
    use arrow::datatypes::TimeUnit;
    match tag {
        "bool" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "large_utf8" => DataType::LargeUtf8,
        "date32" => DataType::Date32,
        "binary" => DataType::Binary,
        "timestamp_s" => DataType::Timestamp(TimeUnit::Second, None),
        "timestamp_ms" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "timestamp_us" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamptz_s" => DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        "timestamptz_ms" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        "timestamptz_us" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        other => {
            if let Some(width) = other
                .strip_prefix("fixed_size_binary(")
                .and_then(|s| s.strip_suffix(')'))
                .and_then(|s| s.parse::<i32>().ok())
            {
                return DataType::FixedSizeBinary(width);
            }
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

pub fn iceberg_type_to_exasol(
    ty: &iceberg::spec::Type,
    engine: EngineTimestampSupport,
) -> ExasolType {
    use iceberg::spec::Type;
    match ty {
        Type::Primitive(pt) => iceberg_primitive_to_exasol(pt, engine),
        _ => ExasolType::WIDEST_VARCHAR,
    }
}

pub(crate) fn column_source_type_to_exasol(
    source_type: &ColumnSourceType,
    engine: EngineTimestampSupport,
) -> ExasolType {
    match source_type {
        ColumnSourceType::Iceberg(field) => iceberg_type_to_exasol(&field.field_type, engine),
        ColumnSourceType::Unity { type_json } => spark_type_to_exasol(
            unity_spark_field(type_json.as_deref()).map(|field| field.data_type),
            engine,
        ),
        ColumnSourceType::Parquet(tag) => match arrow_type_from_tag(tag) {
            DataType::Timestamp(unit, _) => engine
                .clamp(TimestampPrecision::from_arrow_unit(unit))
                .declaration(),
            other => arrow_to_exasol_type(&other),
        },
        ColumnSourceType::Glue { hive_type } => {
            spark_type_to_exasol(parse_hive_type(hive_type), engine)
        }
    }
}

pub(crate) fn unity_spark_field(type_json: Option<&str>) -> Result<StructField, String> {
    let type_json = type_json.ok_or(
        "Unity Catalog reports no type_json descriptor for it, so its Spark type is unknown",
    )?;
    serde_json::from_str(type_json).map_err(|error| {
        format!("its type_json descriptor does not parse as a Spark StructField ({error})")
    })
}

/// A nested or unparseable type declares VARCHAR(2000000), so a column type never fails the listing.
fn spark_type_to_exasol(
    spark_type: Result<SparkType, String>,
    engine: EngineTimestampSupport,
) -> ExasolType {
    match spark_type {
        Ok(SparkType::Primitive(primitive)) => spark_primitive_to_exasol(&primitive, engine),
        _ => ExasolType::WIDEST_VARCHAR,
    }
}

fn spark_primitive_to_exasol(
    primitive: &SparkPrimitive,
    engine: EngineTimestampSupport,
) -> ExasolType {
    match primitive {
        SparkPrimitive::Boolean => ExasolType::Boolean,
        SparkPrimitive::Byte => decimal(3, 0),
        SparkPrimitive::Short => decimal(5, 0),
        SparkPrimitive::Integer => decimal(10, 0),
        SparkPrimitive::Long => decimal(20, 0),
        SparkPrimitive::Float | SparkPrimitive::Double => ExasolType::DoublePrecision,
        SparkPrimitive::Date => ExasolType::Date,
        // The Delta protocol defines no nanosecond timestamp type.
        SparkPrimitive::Timestamp | SparkPrimitive::TimestampNtz => {
            engine.clamp(TimestampPrecision::Microsecond).declaration()
        }
        SparkPrimitive::Decimal(decimal) => {
            catalog_decimal_to_exasol(decimal.precision().into(), decimal.scale().into())
        }
        SparkPrimitive::String
        | SparkPrimitive::Binary
        | SparkPrimitive::Void
        | SparkPrimitive::IntervalYearMonth
        | SparkPrimitive::IntervalDayTime => ExasolType::WIDEST_VARCHAR,
    }
}

/// The inverse of [`exasol_type_from_json`] for every variant with a non-negative decimal scale.
pub(crate) fn exasol_type_to_json(exasol_type: &ExasolType) -> Json {
    match exasol_type {
        ExasolType::Boolean => json!({"type": "boolean"}),
        // Signed on purpose: a negative Arrow scale must not wrap to a large unsigned value.
        ExasolType::Decimal { precision, scale } => {
            json!({"type": "decimal", "precision": precision, "scale": scale})
        }
        ExasolType::DoublePrecision => json!({"type": "double"}),
        ExasolType::Date => json!({"type": "date"}),
        ExasolType::Timestamp { precision: None } => json!({"type": "timestamp"}),
        ExasolType::Timestamp {
            precision: Some(digits),
        } => json!({"type": "timestamp", "fractionalSecondsPrecision": digits}),
        ExasolType::TimestampWithLocalTimeZone => {
            json!({"type": "timestamp", "withLocalTimeZone": true})
        }
        ExasolType::Char { size, ascii } => character_json("char", *size, *ascii),
        ExasolType::Varchar { size, ascii } => character_json("varchar", *size, *ascii),
    }
}

fn character_json(type_name: &str, size: u32, ascii: bool) -> Json {
    let mut character = json!({"type": type_name, "size": size});
    if ascii {
        character["characterSet"] = json!("ASCII");
    }
    character
}

fn is_ascii_character_set(dt: &Json) -> bool {
    dt.get("characterSet")
        .and_then(|v| v.as_str())
        .is_some_and(|cs| cs.eq_ignore_ascii_case("ASCII"))
}

pub(crate) fn exasol_type_from_json(dt: &Json) -> ExasolType {
    let type_name = dt.get("type").and_then(|t| t.as_str()).unwrap_or("varchar");
    match type_name.to_lowercase().as_str() {
        "boolean" => ExasolType::Boolean,
        "decimal" => {
            let p = dt.get("precision").and_then(|v| v.as_u64()).unwrap_or(18);
            let s = dt.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            if p <= 36 && s <= 36 {
                decimal(p as u8, s as i8)
            } else {
                ExasolType::WIDEST_VARCHAR
            }
        }
        "double" => ExasolType::DoublePrecision,
        "date" => ExasolType::Date,
        "timestamp" => {
            let with_local_time_zone = dt
                .get("withLocalTimeZone")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if with_local_time_zone {
                ExasolType::TimestampWithLocalTimeZone
            } else {
                let precision = dt
                    .get("fractionalSecondsPrecision")
                    .and_then(|v| v.as_u64())
                    // Saturates, so an out-of-range precision stays a declaration Exasol rejects
                    // and never reads as bare TIMESTAMP.
                    .map(|digits| u8::try_from(digits).unwrap_or(u8::MAX));
                ExasolType::Timestamp { precision }
            }
        }
        "char" => {
            // Exasol validates pushdown output types positionally, so CHAR(n) rendered as
            // VARCHAR(n) is rejected (#192). A missing size must not default to CHAR(2000),
            // which would blank-pad every value.
            match dt.get("size").and_then(|v| v.as_u64()) {
                Some(size) => ExasolType::Char {
                    size: size.min(MAX_CHAR_SIZE.into()) as u32,
                    ascii: is_ascii_character_set(dt),
                },
                None => ExasolType::WIDEST_VARCHAR,
            }
        }
        _ => {
            let size = dt
                .get("size")
                .and_then(|v| v.as_u64())
                .unwrap_or(MAX_VARCHAR_SIZE.into());
            ExasolType::Varchar {
                size: size.min(MAX_VARCHAR_SIZE.into()) as u32,
                ascii: is_ascii_character_set(dt),
            }
        }
    }
}

#[cfg(test)]
#[path = "mapping_tests.rs"]
mod tests;
