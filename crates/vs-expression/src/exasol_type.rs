//! The Exasol SQL type the adapter declares, emits, and casts to: read from a VS request's
//! `dataType` JSON and from UDF column metadata, rendered as Exasol SQL (`Display`) and as
//! `dataType` JSON. Pure: no I/O.
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{ColumnInfo, ExaType};
use serde_json::{Value as Json, json};
use std::fmt;

/// The widest `VARCHAR` Exasol accepts, and the size of the JSON-fallback column.
pub const MAX_VARCHAR_SIZE: u32 = 2_000_000;

/// Exasol rejects `CHAR(2001)` and wider.
const MAX_CHAR_SIZE: u32 = 2000;

/// Exasol rejects `DECIMAL(37,0)` and wider.
pub const MAX_DECIMAL_PRECISION: u8 = 36;

/// The most fractional-second digits a TIMESTAMP or an INTERVAL DAY TO SECOND holds.
const MAX_FRACTIONAL_DIGITS: u8 = 9;

/// The widest leading field an INTERVAL declares, as in `INTERVAL DAY(9) TO SECOND`.
const MAX_INTERVAL_PRECISION: u8 = 9;

/// Exasol rejects `HASHTYPE(1025 BYTE)` and wider.
const MAX_HASHTYPE_BYTES: u32 = 1024;

/// The fractional digits of Exasol's bare `TIMESTAMP`, the only precision Exasol 8.x has.
const BARE_TIMESTAMP_PRECISION: u8 = 3;

/// A `DECIMAL(p,s)` inside Exasol's domain, `1 <= p <= 36` and `s <= p`. The fields are private,
/// so [`Decimal::new`] is the only way to build one and no DECIMAL Exasol rejects can exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    precision: u8,
    scale: u8,
}

impl Decimal {
    /// `None` for a pair Exasol rejects, such as `DECIMAL(0,0)`, `DECIMAL(37,0)`, or
    /// `DECIMAL(5,10)`.
    pub const fn new(precision: u8, scale: u8) -> Option<Self> {
        if precision >= 1 && precision <= MAX_DECIMAL_PRECISION && scale <= precision {
            Some(Self { precision, scale })
        } else {
            None
        }
    }

    /// The total digit count, from 1 to 36.
    pub const fn precision(self) -> u8 {
        self.precision
    }

    /// The fractional digit count, at most the precision.
    pub const fn scale(self) -> u8 {
        self.scale
    }
}

/// Every Exasol type a catalog column, an `EMITS` column, a select-list item, or a `CAST`
/// target can carry. [`ExasolType::is_udf_emittable`] says which ones a scan UDF can produce.
///
/// `Display` renders the SQL spelling and is the only place that spelling is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExasolType {
    Boolean,
    Decimal(Decimal),
    DoublePrecision,
    Date,
    /// `precision` counts fractional-second digits; 3 is Exasol's bare `TIMESTAMP`.
    Timestamp {
        precision: u8,
    },
    /// `precision` as for `Timestamp`.
    TimestampWithLocalTimeZone {
        precision: u8,
    },
    Char {
        size: u32,
        /// Declares the `ASCII` character set. `false` is Exasol's default, `UTF8`.
        ascii: bool,
    },
    Varchar {
        size: u32,
        /// Declares the `ASCII` character set. `false` is Exasol's default, `UTF8`.
        ascii: bool,
    },
    IntervalYearToMonth {
        precision: u8,
    },
    IntervalDayToSecond {
        precision: u8,
        fraction: u8,
    },
    Geometry {
        srid: u32,
    },
    Hashtype {
        bytes: u32,
    },
}

impl ExasolType {
    /// `VARCHAR(2000000)`, the widest `VARCHAR`: the declaration of an unbounded string column,
    /// and of the JSON text the scan writes for every type Exasol cannot hold.
    pub const WIDEST_VARCHAR: Self = Self::varchar(MAX_VARCHAR_SIZE);

    /// The bare `TIMESTAMP`, which holds 3 fractional digits on every Exasol version.
    pub const BARE_TIMESTAMP: Self = Self::timestamp(BARE_TIMESTAMP_PRECISION);

    /// For a pair known to be valid: a pair outside Exasol's domain panics, at compile time in a
    /// const context. A pair read from input goes through [`Decimal::new`].
    pub const fn decimal(precision: u8, scale: u8) -> Self {
        match Decimal::new(precision, scale) {
            Some(decimal) => Self::Decimal(decimal),
            None => panic!("DECIMAL needs 1 <= precision <= 36 and scale <= precision"),
        }
    }

    /// `TIMESTAMP(precision)` without a range check; precision 3 renders as the bare `TIMESTAMP`.
    pub const fn timestamp(precision: u8) -> Self {
        Self::Timestamp { precision }
    }

    /// A UTF8 `CHAR(size)`.
    pub const fn char(size: u32) -> Self {
        Self::Char { size, ascii: false }
    }

    /// A UTF8 `VARCHAR(size)`.
    pub const fn varchar(size: u32) -> Self {
        Self::Varchar { size, ascii: false }
    }

    pub fn is_character(&self) -> bool {
        matches!(self, Self::Char { .. } | Self::Varchar { .. })
    }

    /// The fractional-second digits a plain `TIMESTAMP` holds.
    pub fn fractional_digits(&self) -> Option<u8> {
        match self {
            Self::Timestamp { precision } => Some(*precision),
            _ => None,
        }
    }

    /// Whether a scan UDF can declare this type in `EMITS`. Exasol rejects `TIMESTAMP WITH
    /// LOCAL TIME ZONE` there (`sqlCode 22002`), and no Arrow type maps to the other three.
    pub fn is_udf_emittable(&self) -> bool {
        !matches!(
            self,
            Self::TimestampWithLocalTimeZone { .. }
                | Self::IntervalYearToMonth { .. }
                | Self::IntervalDayToSecond { .. }
                | Self::Geometry { .. }
                | Self::Hashtype { .. }
        )
    }
}

impl fmt::Display for ExasolType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::Decimal(decimal) => {
                write!(f, "DECIMAL({},{})", decimal.precision, decimal.scale)
            }
            Self::DoublePrecision => f.write_str("DOUBLE PRECISION"),
            Self::Date => f.write_str("DATE"),
            Self::Timestamp { precision } => {
                write!(f, "TIMESTAMP{}", timestamp_precision_suffix(*precision))
            }
            Self::TimestampWithLocalTimeZone { precision } => write!(
                f,
                "TIMESTAMP{} WITH LOCAL TIME ZONE",
                timestamp_precision_suffix(*precision)
            ),
            Self::Char { size, ascii } => {
                write!(f, "CHAR({size}){}", character_set_suffix(*ascii))
            }
            Self::Varchar { size, ascii } => {
                write!(f, "VARCHAR({size}){}", character_set_suffix(*ascii))
            }
            Self::IntervalYearToMonth { precision } => {
                write!(f, "INTERVAL YEAR({precision}) TO MONTH")
            }
            Self::IntervalDayToSecond {
                precision,
                fraction,
            } => write!(f, "INTERVAL DAY({precision}) TO SECOND({fraction})"),
            Self::Geometry { srid } => write!(f, "GEOMETRY({srid})"),
            Self::Hashtype { bytes } => write!(f, "HASHTYPE({bytes} BYTE)"),
        }
    }
}

/// Exasol 8.x parses no TIMESTAMP precision, and 2025.x reads the bare `TIMESTAMP` as
/// `TIMESTAMP(3)`, so precision 3 renders bare and needs no Exasol version.
fn timestamp_precision_suffix(precision: u8) -> String {
    if precision == BARE_TIMESTAMP_PRECISION {
        String::new()
    } else {
        format!("({precision})")
    }
}

/// Exasol treats an unsuffixed character declaration as UTF8, so an `ASCII` column must carry
/// the suffix back or its type check reports "Data type mismatch".
fn character_set_suffix(ascii: bool) -> &'static str {
    if ascii { " ASCII" } else { "" }
}

/// The inverse of [`exasol_type_from_json`], in the wire form Exasol sends.
pub fn exasol_type_to_json(exasol_type: &ExasolType) -> Json {
    match exasol_type {
        ExasolType::Boolean => json!({"type": "boolean"}),
        ExasolType::Decimal(decimal) => json!({
            "type": "decimal", "precision": decimal.precision, "scale": decimal.scale,
        }),
        ExasolType::DoublePrecision => json!({"type": "double"}),
        ExasolType::Date => json!({"type": "date"}),
        ExasolType::Timestamp { precision } => timestamp_json(*precision, false),
        ExasolType::TimestampWithLocalTimeZone { precision } => timestamp_json(*precision, true),
        ExasolType::Char { size, ascii } => character_json("char", *size, *ascii),
        ExasolType::Varchar { size, ascii } => character_json("varchar", *size, *ascii),
        ExasolType::IntervalYearToMonth { precision } => json!({
            "type": "interval", "fromTo": "YEAR TO MONTH", "precision": precision,
        }),
        ExasolType::IntervalDayToSecond {
            precision,
            fraction,
        } => json!({
            "type": "interval", "fromTo": "DAY TO SECONDS",
            "precision": precision, "fraction": fraction,
        }),
        ExasolType::Geometry { srid } => json!({"type": "geometry", "srid": srid}),
        ExasolType::Hashtype { bytes } => json!({"type": "hashtype", "bytesize": bytes}),
    }
}

/// Exasol 8.x omits `fractionalSecondsPrecision` for its only precision, 3.
fn timestamp_json(precision: u8, with_local_time_zone: bool) -> Json {
    let mut timestamp = json!({"type": "timestamp"});
    if with_local_time_zone {
        timestamp["withLocalTimeZone"] = json!(true);
    }
    if precision != BARE_TIMESTAMP_PRECISION {
        timestamp["fractionalSecondsPrecision"] = json!(precision);
    }
    timestamp
}

fn character_json(type_name: &str, size: u32, ascii: bool) -> Json {
    let mut character = json!({"type": type_name, "size": size});
    if ascii {
        character["characterSet"] = json!("ASCII");
    }
    character
}

/// Reads the `dataType` object of a VS request or column declaration, and fails on a missing
/// field, a type name Exasol does not define, or a value outside Exasol's limits, so no consumer
/// holds a type Exasol rejects. Its only defaults are the fields Exasol omits: an absent TIMESTAMP
/// precision means 3 (8.x has no other), an absent `withLocalTimeZone` means false, and an absent
/// `characterSet` means UTF8 (Exasol sends it only for ASCII).
pub fn exasol_type_from_json(dt: &Json) -> Result<ExasolType, UdfError> {
    let type_name = dt
        .get("type")
        .and_then(Json::as_str)
        .ok_or_else(|| invalid(dt, "`type` must name an Exasol type"))?;
    match type_name.to_ascii_lowercase().as_str() {
        "boolean" => Ok(ExasolType::Boolean),
        "decimal" => decimal_from_json(dt),
        "double" => Ok(ExasolType::DoublePrecision),
        "date" => Ok(ExasolType::Date),
        "timestamp" => timestamp_from_json(dt),
        "char" => Ok(ExasolType::Char {
            size: bounded_field(dt, "size", 1, MAX_CHAR_SIZE)?,
            ascii: ascii_character_set(dt)?,
        }),
        "varchar" => Ok(ExasolType::Varchar {
            size: bounded_field(dt, "size", 1, MAX_VARCHAR_SIZE)?,
            ascii: ascii_character_set(dt)?,
        }),
        "interval" => interval_from_json(dt),
        "geometry" => Ok(ExasolType::Geometry {
            srid: bounded_field(dt, "srid", 0, u32::MAX)?,
        }),
        "hashtype" => Ok(ExasolType::Hashtype {
            bytes: bounded_field(dt, "bytesize", 1, MAX_HASHTYPE_BYTES)?,
        }),
        _ => Err(invalid(dt, "`type` names no type Exasol defines")),
    }
}

fn decimal_from_json(dt: &Json) -> Result<ExasolType, UdfError> {
    let precision = bounded_field(dt, "precision", 1, MAX_DECIMAL_PRECISION)?;
    let scale = bounded_field(dt, "scale", 0, MAX_DECIMAL_PRECISION)?;
    Decimal::new(precision, scale)
        .map(ExasolType::Decimal)
        .ok_or_else(|| {
            invalid(
                dt,
                "DECIMAL needs a precision from 1 to 36 and a scale from 0 to the precision",
            )
        })
}

fn timestamp_from_json(dt: &Json) -> Result<ExasolType, UdfError> {
    let precision = match dt.get("fractionalSecondsPrecision") {
        None => BARE_TIMESTAMP_PRECISION,
        Some(_) => bounded_field(dt, "fractionalSecondsPrecision", 0, MAX_FRACTIONAL_DIGITS)?,
    };
    match dt.get("withLocalTimeZone") {
        None | Some(Json::Bool(false)) => Ok(ExasolType::Timestamp { precision }),
        Some(Json::Bool(true)) => Ok(ExasolType::TimestampWithLocalTimeZone { precision }),
        Some(_) => Err(invalid(dt, "`withLocalTimeZone` must be a boolean")),
    }
}

fn interval_from_json(dt: &Json) -> Result<ExasolType, UdfError> {
    let precision = bounded_field(dt, "precision", 1, MAX_INTERVAL_PRECISION)?;
    match dt.get("fromTo").and_then(Json::as_str) {
        Some("YEAR TO MONTH") => Ok(ExasolType::IntervalYearToMonth { precision }),
        Some("DAY TO SECONDS") => Ok(ExasolType::IntervalDayToSecond {
            precision,
            fraction: bounded_field(dt, "fraction", 0, MAX_FRACTIONAL_DIGITS)?,
        }),
        _ => Err(invalid(
            dt,
            "`fromTo` must be YEAR TO MONTH or DAY TO SECONDS",
        )),
    }
}

fn ascii_character_set(dt: &Json) -> Result<bool, UdfError> {
    match dt.get("characterSet").map(Json::as_str) {
        None => Ok(false),
        Some(Some(set)) if set.eq_ignore_ascii_case("UTF8") => Ok(false),
        Some(Some(set)) if set.eq_ignore_ascii_case("ASCII") => Ok(true),
        Some(_) => Err(invalid(dt, "`characterSet` must be UTF8 or ASCII")),
    }
}

/// The integer `key` of `dt`, when it lies within `min..=max`.
fn bounded_field<T>(dt: &Json, key: &str, min: T, max: T) -> Result<T, UdfError>
where
    T: Copy + fmt::Display + Into<u64> + TryFrom<u64>,
{
    dt.get(key)
        .and_then(Json::as_u64)
        .filter(|value| (min.into()..=max.into()).contains(value))
        .and_then(|value| T::try_from(value).ok())
        .ok_or_else(|| {
            invalid(
                dt,
                &format!("`{key}` must be an integer from {min} to {max}"),
            )
        })
}

fn invalid(dt: &Json, constraint: &str) -> UdfError {
    UdfError::User(format!("invalid Exasol dataType {dt}: {constraint}"))
}

/// The type of a UDF input or output column. An unrecognized column keeps its `type_name`, so
/// each caller decides: the scan emits it as JSON text, and the export refuses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdfColumnType {
    Known(ExasolType),
    Unrecognized { type_name: String },
}

/// Reads a UDF column's metadata by the bin Exasol reported (`typ`), and reads `type_name` only
/// for what the bin lacks: an integer DECIMAL's absent precision, a bare `TIMESTAMP`'s
/// precision 3, a local-time-zone timestamp, and the `ASCII` character set.
pub fn exasol_type_from_column_info(column: &ColumnInfo) -> UdfColumnType {
    let type_name = column.type_name.as_str();
    let known = match &column.typ {
        ExaType::Boolean => Some(ExasolType::Boolean),
        ExaType::Double => Some(ExasolType::DoublePrecision),
        ExaType::Date => Some(ExasolType::Date),
        ExaType::Int32 | ExaType::Int64 => column
            .precision
            .or_else(|| integer_decimal_precision(type_name))
            .and_then(|precision| decimal_of(precision, 0)),
        ExaType::Numeric { precision, scale } => decimal_of(*precision, *scale),
        ExaType::Timestamp { .. } if type_name == "TIMESTAMP" => Some(ExasolType::BARE_TIMESTAMP),
        ExaType::Timestamp { precision } => u8::try_from(*precision)
            .ok()
            .filter(|precision| *precision <= MAX_FRACTIONAL_DIGITS)
            .map(|precision| {
                // Exasol reports a local-time-zone timestamp in the plain timestamp bin.
                if type_name.contains("WITH LOCAL TIME ZONE") {
                    ExasolType::TimestampWithLocalTimeZone { precision }
                } else {
                    ExasolType::timestamp(precision)
                }
            }),
        ExaType::String { size } => {
            (1..=MAX_VARCHAR_SIZE)
                .contains(size)
                .then(|| ExasolType::Varchar {
                    size: *size,
                    ascii: type_name.ends_with(" ASCII"),
                })
        }
        ExaType::Char { size } => (1..=MAX_CHAR_SIZE)
            .contains(size)
            .then(|| ExasolType::Char {
                size: *size,
                ascii: type_name.ends_with(" ASCII"),
            }),
        ExaType::Unsupported => None,
    };
    match known {
        Some(exasol_type) => UdfColumnType::Known(exasol_type),
        None => UdfColumnType::Unrecognized {
            type_name: column.type_name.clone(),
        },
    }
}

fn decimal_of(precision: u32, scale: u32) -> Option<ExasolType> {
    Decimal::new(u8::try_from(precision).ok()?, u8::try_from(scale).ok()?).map(ExasolType::Decimal)
}

/// The `p` of `DECIMAL(p,0)`.
fn integer_decimal_precision(type_name: &str) -> Option<u32> {
    let arguments = type_name.strip_prefix("DECIMAL(")?.strip_suffix(",0)")?;
    arguments.parse().ok()
}

#[cfg(test)]
#[path = "exasol_type_tests.rs"]
mod tests;
