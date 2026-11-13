//! Arrow-to-Exasol type mapping shared by `createVirtualSchema` and the scan's Arrow→Value
//! conversion. Pure: no I/O.
use arrow::datatypes::{DataType, TimeUnit};
use delta_kernel::schema::{DataType as SparkType, PrimitiveType as SparkPrimitive, StructField};
use lakehouse_catalog::ColumnSourceType;

use super::exasol_type::{Decimal, ExasolType, MAX_VARCHAR_SIZE};
use super::hive_type::parse_hive_type;
use crate::scan::emit::emitted_unit;
use vs_expression::timestamp::TimestampUnit;

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

        DataType::Int8 | DataType::UInt8 => ExasolType::decimal(3, 0),
        DataType::Int16 | DataType::UInt16 => ExasolType::decimal(5, 0),
        DataType::Int32 => ExasolType::decimal(10, 0),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => ExasolType::decimal(20, 0),

        DataType::Float32 | DataType::Float64 => ExasolType::DoublePrecision,

        DataType::Utf8 | DataType::LargeUtf8 => ExasolType::WIDEST_VARCHAR,

        DataType::Date32 => ExasolType::Date,

        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS type; a tz-aware
        // value is a UTC instant, so plain TIMESTAMP emits it unchanged.
        DataType::Timestamp(_, _) => ExasolType::BARE_TIMESTAMP,

        // A negative Arrow scale has no Exasol DECIMAL, so it takes the JSON fallback.
        DataType::Decimal128(p, s) => {
            ExasolType::Decimal(u8::try_from(*s).ok().and_then(|s| Decimal::new(*p, s))?)
        }

        _ => return None,
    };
    Some(exasol_type)
}

pub fn needs_json_fallback(dt: &DataType) -> bool {
    compatible_exasol_type(dt).is_none()
}

/// The largest binary value whose lowercase hex rendering fits `VARCHAR(2000000)`.
pub const MAX_BINARY_BYTES: usize = MAX_VARCHAR_SIZE as usize / 2;

/// Two hex characters per byte, so a `fixed(L)` column declares `VARCHAR(2L)`.
pub(crate) fn hex_varchar(width: u64) -> ExasolType {
    let chars = width.saturating_mul(2);
    match u32::try_from(chars) {
        Ok(size) if (1..=MAX_VARCHAR_SIZE).contains(&size) => ExasolType::varchar(size),
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

/// An Iceberg catalog decimal inside Exasol's DECIMAL domain. Every Iceberg decimal consumer
/// reads this, so the Exasol type, the Arrow tag, and the default encoding agree.
pub(crate) fn iceberg_decimal(precision: u32, scale: u32) -> Option<Decimal> {
    Decimal::new(u8::try_from(precision).ok()?, u8::try_from(scale).ok()?)
}

const FIRST_CALENDAR_VERSIONED_MAJOR: u32 = 2025;

/// Whether `version` is an Exasol 8.x or older release. An unparseable version reads as a newer
/// one, because an unrecognised version is more likely a future release than an old one.
pub(crate) fn predates_calendar_versions(version: &str) -> bool {
    version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u32>().ok())
        .is_some_and(|major| major < FIRST_CALENDAR_VERSIONED_MAJOR)
}

pub(crate) fn arrow_time_unit(unit: TimestampUnit) -> TimeUnit {
    match unit {
        TimestampUnit::Second => TimeUnit::Second,
        TimestampUnit::Millisecond => TimeUnit::Millisecond,
        TimestampUnit::Microsecond => TimeUnit::Microsecond,
        TimestampUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

pub(crate) fn timestamp_unit(unit: TimeUnit) -> TimestampUnit {
    match unit {
        TimeUnit::Second => TimestampUnit::Second,
        TimeUnit::Millisecond => TimestampUnit::Millisecond,
        TimeUnit::Microsecond => TimestampUnit::Microsecond,
        TimeUnit::Nanosecond => TimestampUnit::Nanosecond,
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
    pub fn from_database_version(version: &str) -> Self {
        if predates_calendar_versions(version) {
            Self::MillisecondOnly
        } else {
            Self::DeclaredPrecision
        }
    }

    /// The TIMESTAMP declaration of a column the scan holds in `source`.
    pub fn declaration(self, source: TimestampUnit) -> ExasolType {
        let unit = match self {
            Self::MillisecondOnly => TimestampUnit::Millisecond,
            Self::DeclaredPrecision => emitted_unit(source),
        };
        ExasolType::timestamp(unit.digits())
    }
}

pub fn iceberg_primitive_to_exasol(
    pt: &iceberg::spec::PrimitiveType,
    engine: EngineTimestampSupport,
) -> ExasolType {
    use iceberg::spec::PrimitiveType::*;
    match pt {
        Boolean => ExasolType::Boolean,
        Int => ExasolType::decimal(10, 0),
        Long => ExasolType::decimal(20, 0),
        Float | Double => ExasolType::DoublePrecision,
        Decimal { precision, scale } => iceberg_decimal(*precision, *scale)
            .map_or(ExasolType::WIDEST_VARCHAR, ExasolType::Decimal),
        Date => ExasolType::Date,
        Time => ExasolType::WIDEST_VARCHAR,
        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS type; timestamptz is
        // a UTC instant, so plain TIMESTAMP emits it unchanged.
        Timestamp | Timestamptz => engine.declaration(TimestampUnit::Microsecond),
        TimestampNs | TimestamptzNs => engine.declaration(TimestampUnit::Nanosecond),
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
        Decimal { precision, scale } => match iceberg_decimal(*precision, *scale) {
            Some(decimal) => DataType::Decimal128(decimal.precision(), decimal.scale() as i8),
            None => DataType::Utf8,
        },
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
            DataType::Timestamp(unit, _) => engine.declaration(timestamp_unit(unit)),
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
        SparkPrimitive::Byte => ExasolType::decimal(3, 0),
        SparkPrimitive::Short => ExasolType::decimal(5, 0),
        SparkPrimitive::Integer => ExasolType::decimal(10, 0),
        SparkPrimitive::Long => ExasolType::decimal(20, 0),
        SparkPrimitive::Float | SparkPrimitive::Double => ExasolType::DoublePrecision,
        SparkPrimitive::Date => ExasolType::Date,
        // The Delta protocol defines no nanosecond timestamp type.
        SparkPrimitive::Timestamp | SparkPrimitive::TimestampNtz => {
            engine.declaration(TimestampUnit::Microsecond)
        }
        SparkPrimitive::Decimal(decimal) => Decimal::new(decimal.precision(), decimal.scale())
            .map_or(ExasolType::WIDEST_VARCHAR, ExasolType::Decimal),
        SparkPrimitive::String
        | SparkPrimitive::Binary
        | SparkPrimitive::Void
        | SparkPrimitive::IntervalYearMonth
        | SparkPrimitive::IntervalDayTime => ExasolType::WIDEST_VARCHAR,
    }
}

#[cfg(test)]
#[path = "mapping_tests.rs"]
mod tests;
