//! Arrow-to-Exasol type mapping shared by `createVirtualSchema` and the scan's Arrow→Value
//! conversion. Pure: no I/O.
use arrow::datatypes::{DataType, TimeUnit};
use lakehouse_catalog::ColumnSourceType;
use serde_json::{Value as Json, json};

/// Returns `"VARCHAR(2000000)"` for every incompatible Arrow type rather than erroring;
/// the scan serializes those values to JSON strings.
pub fn arrow_to_exasol_type(dt: &DataType) -> String {
    match compatible_exasol_type(dt) {
        Some(CompatibleExaType::Fixed(s)) => s.to_string(),
        Some(CompatibleExaType::Decimal(p, s)) => format!("DECIMAL({p},{s})"),
        None => "VARCHAR(2000000)".to_string(),
    }
}

enum CompatibleExaType {
    Fixed(&'static str),
    Decimal(u8, i8),
}

/// `None` is the JSON-fallback flag: `Utf8` and an out-of-range `Decimal128` both render as
/// `VARCHAR(2000000)`, so the rendered string cannot tell them apart.
fn compatible_exasol_type(dt: &DataType) -> Option<CompatibleExaType> {
    let exasol_type = match dt {
        DataType::Boolean => "BOOLEAN",

        DataType::Int8 | DataType::UInt8 => "DECIMAL(3,0)",
        DataType::Int16 | DataType::UInt16 => "DECIMAL(5,0)",
        DataType::Int32 => "DECIMAL(10,0)",
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "DECIMAL(20,0)",

        DataType::Float32 | DataType::Float64 => "DOUBLE PRECISION",

        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR(2000000)",

        DataType::Date32 => "DATE",

        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS type; a tz-aware
        // value is a UTC instant, so plain TIMESTAMP emits it unchanged.
        DataType::Timestamp(_, _) => "TIMESTAMP",

        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => {
            return Some(CompatibleExaType::Decimal(*p, *s));
        }

        _ => return None,
    };
    Some(CompatibleExaType::Fixed(exasol_type))
}

/// The Arrow type `emit_batch`'s strict Arrow→ExaType validation accepts for a column
/// declared with this EMITS type string (case-insensitive, whitespace-tolerant).
///
/// `None` for the string family: the right source coercion (JSON vs plain Utf8 cast)
/// depends on the source column, not the target.
///
/// Exasol bins a scale-0 `DECIMAL(p,0)` by precision, and `emit_batch` rejects any other
/// Arrow representation:
/// - scale 0, p ≤ [`DECIMAL_INT32_MAX_PRECISION`] → `Int32`
/// - scale 0, p ≤ [`DECIMAL_INT64_MAX_PRECISION`] → `Int64`
/// - otherwise → `Decimal128(p,s)`
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
    // Exasol's bare `TIMESTAMP` is `TIMESTAMP(3)`.
    let timestamp_digits = if upper == "TIMESTAMP" {
        Some(3)
    } else {
        upper
            .strip_prefix("TIMESTAMP(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|digits| digits.trim().parse::<u32>().ok())
    };
    if let Some(digits) = timestamp_digits {
        return Some(DataType::Timestamp(
            TimestampPrecision::from_declared_digits(digits).arrow_unit(),
            None,
        ));
    }
    if upper == "TIMESTAMP WITH LOCAL TIME ZONE" {
        return Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
    }
    if let Some((p, s)) = parse_decimal_args(&upper) {
        if s == 0 && p <= DECIMAL_INT32_MAX_PRECISION {
            return Some(DataType::Int32);
        }
        if s == 0 && p <= DECIMAL_INT64_MAX_PRECISION {
            return Some(DataType::Int64);
        }
        return Some(DataType::Decimal128(p, s));
    }

    None
}

pub const DECIMAL_INT32_MAX_PRECISION: u8 = 9;

pub const DECIMAL_INT64_MAX_PRECISION: u8 = 18;

const EXASOL_DECIMAL_MAX_PRECISION: u32 = 36;

/// Accepts `DECIMAL(p,s)` and `DECIMAL(p)` (scale 0). Input must be upper-cased with no
/// surrounding whitespace; whitespace around each argument is trimmed.
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

pub fn needs_json_fallback(dt: &DataType) -> bool {
    compatible_exasol_type(dt).is_none()
}

/// Narrower than `needs_json_fallback`: `Binary` and an out-of-range `Decimal128` keep the
/// `CAST(col AS VARCHAR)` Arrow-display path instead of JSON document rendering.
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

#[derive(Debug, Clone, Copy)]
struct CatalogDecimal {
    precision: u32,
    scale: u32,
}

fn catalog_decimal_to_exasol(precision: u32, scale: u32) -> String {
    if exasol_representable_catalog_decimal(precision, scale) {
        format!("DECIMAL({precision},{scale})")
    } else {
        "VARCHAR(2000000)".to_string()
    }
}

const FIRST_CALENDAR_VERSIONED_MAJOR: u32 = 2025;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampPrecision {
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimestampPrecision {
    pub fn declaration(self) -> &'static str {
        match self {
            Self::Millisecond => "TIMESTAMP",
            Self::Microsecond => "TIMESTAMP(6)",
            Self::Nanosecond => "TIMESTAMP(9)",
        }
    }

    pub fn arrow_unit(self) -> TimeUnit {
        match self {
            Self::Millisecond => TimeUnit::Millisecond,
            Self::Microsecond => TimeUnit::Microsecond,
            Self::Nanosecond => TimeUnit::Nanosecond,
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
) -> String {
    use iceberg::spec::PrimitiveType::*;
    match pt {
        Boolean => "BOOLEAN".to_string(),
        Int => "DECIMAL(10,0)".to_string(),
        Long => "DECIMAL(20,0)".to_string(),
        Float => "DOUBLE PRECISION".to_string(),
        Double => "DOUBLE PRECISION".to_string(),
        Decimal { precision, scale } => catalog_decimal_to_exasol(*precision, *scale),
        Date => "DATE".to_string(),
        Time => "VARCHAR(2000000)".to_string(),
        // Exasol rejects TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS type; timestamptz is
        // a UTC instant, so plain TIMESTAMP emits it unchanged.
        Timestamp | Timestamptz => engine
            .clamp(TimestampPrecision::Microsecond)
            .declaration()
            .to_string(),
        TimestampNs | TimestamptzNs => engine
            .clamp(TimestampPrecision::Nanosecond)
            .declaration()
            .to_string(),
        String | Uuid => "VARCHAR(2000000)".to_string(),
        Fixed(_) | Binary => "VARCHAR(2000000)".to_string(),
    }
}

/// Must stay in lockstep with [`iceberg_primitive_to_exasol`] on the decimal domain: a
/// `Decimal128` tag for a column declared VARCHAR breaks [`exasol_type_to_arrow`], and
/// arrow-rs rejects `precision == 0` and `scale > precision` outright.
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
        String | Uuid => DataType::Utf8,
        Fixed(_) | Binary => DataType::Utf8,
    }
}

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
        _ => "utf8".to_string(),
    }
}

/// Returns `DataType::Utf8` for any unrecognised tag. `decimal128(p,s)` deliberately does not
/// use [`parse_decimal_args`]: the tag grammar requires both arguments.
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
        "timestamp_s" => DataType::Timestamp(TimeUnit::Second, None),
        "timestamp_ms" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "timestamp_us" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamptz_s" => DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        "timestamptz_ms" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        "timestamptz_us" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        other => {
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

pub fn iceberg_type_to_exasol(ty: &iceberg::spec::Type, engine: EngineTimestampSupport) -> String {
    use iceberg::spec::Type;
    match ty {
        Type::Primitive(pt) => iceberg_primitive_to_exasol(pt, engine),
        _ => "VARCHAR(2000000)".to_string(),
    }
}

pub(crate) fn column_source_type_to_exasol(
    source_type: &ColumnSourceType,
    engine: EngineTimestampSupport,
) -> String {
    match source_type {
        ColumnSourceType::Iceberg(ty) => iceberg_type_to_exasol(ty, engine),
        ColumnSourceType::Unity {
            type_name,
            precision,
            scale,
        } => unity_type_name_to_exasol(
            type_name,
            CatalogDecimal {
                precision: *precision,
                scale: *scale,
            },
            engine,
        ),
        ColumnSourceType::Parquet(tag) => arrow_to_exasol_type(&arrow_type_from_tag(tag)),
    }
}

/// Unmappable Spark types fall back to VARCHAR(2000000) rather than failing enumeration.
fn unity_type_name_to_exasol(
    type_name: &str,
    decimal: CatalogDecimal,
    engine: EngineTimestampSupport,
) -> String {
    match type_name {
        "BOOLEAN" => "BOOLEAN".to_string(),
        "BYTE" => "DECIMAL(3,0)".to_string(),
        "SHORT" => "DECIMAL(5,0)".to_string(),
        "INT" => "DECIMAL(10,0)".to_string(),
        "LONG" => "DECIMAL(20,0)".to_string(),
        "FLOAT" | "DOUBLE" => "DOUBLE PRECISION".to_string(),
        "STRING" => "VARCHAR(2000000)".to_string(),
        "DATE" => "DATE".to_string(),
        // The Delta protocol defines no nanosecond timestamp type.
        "TIMESTAMP" | "TIMESTAMP_NTZ" => engine
            .clamp(TimestampPrecision::Microsecond)
            .declaration()
            .to_string(),
        "DECIMAL" => catalog_decimal_to_exasol(decimal.precision, decimal.scale),
        _ => "VARCHAR(2000000)".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExaTypeClass {
    Character,
    Date,
    Decimal,
    Other,
}

/// Matches the `"DECIMAL"` prefix, not `"DECIMAL("`, so a bare `DECIMAL` also classifies as
/// Decimal.
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
    if let Some(p) = upper
        .strip_prefix("TIMESTAMP(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|inner| inner.trim().parse::<u64>().ok())
    {
        return json!({"type": "timestamp", "fractionalSecondsPrecision": p});
    }
    if let Some((p, s)) = parse_decimal_args(&upper) {
        // Signed on purpose: a negative Arrow scale must not wrap to a large unsigned value.
        return json!({"type": "decimal", "precision": p, "scale": s});
    }
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

/// Exasol rejects `CHAR(2001)` and wider.
const EXASOL_CHAR_MAX_SIZE: u64 = 2000;

/// Exasol treats an unsuffixed character declaration as UTF8, so an `ASCII` column must carry
/// the suffix back or its type check reports "Data type mismatch".
fn character_set_suffix(dt: &Json) -> &'static str {
    let is_ascii = dt
        .get("characterSet")
        .and_then(|v| v.as_str())
        .is_some_and(|cs| cs.eq_ignore_ascii_case("ASCII"));
    if is_ascii { " ASCII" } else { "" }
}

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
            // Exasol validates pushdown output types positionally, so CHAR(n) rendered as
            // VARCHAR(n) is rejected (#192). A missing size must not default to CHAR(2000),
            // which would blank-pad every value.
            match dt.get("size").and_then(|v| v.as_u64()) {
                Some(size) => {
                    let capped = size.min(EXASOL_CHAR_MAX_SIZE);
                    format!("CHAR({capped}){}", character_set_suffix(dt))
                }
                None => "VARCHAR(2000000)".to_string(),
            }
        }
        _ => {
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            format!("VARCHAR({capped}){}", character_set_suffix(dt))
        }
    }
}

#[cfg(test)]
#[path = "mapping_tests.rs"]
mod tests;
