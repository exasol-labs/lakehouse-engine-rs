//! The Exasol column type the adapter declares, emits, and casts to. One owned value read at
//! each input edge, rendered by one SQL renderer (`Display`). Pure: no I/O.
use std::fmt;

/// The widest `VARCHAR` Exasol accepts, and the size of the JSON-fallback column.
pub const MAX_VARCHAR_SIZE: u32 = 2_000_000;

/// Exasol rejects `CHAR(2001)` and wider.
pub const MAX_CHAR_SIZE: u32 = 2000;

/// Exasol rejects `TIMESTAMP(10)` and finer.
pub const MAX_TIMESTAMP_PRECISION: u8 = 9;

/// The Exasol types a catalog column, an `EMITS` column, or a `CAST` target can carry.
///
/// `Display` renders the SQL spelling and is the only place that spelling is written.
/// `Timestamp { precision: None }` is the bare `TIMESTAMP` Exasol 8.x declares, which is not
/// the same declaration as `TIMESTAMP(3)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExasolType {
    Boolean,
    /// The scale is signed because an Arrow `Decimal128` scale is.
    Decimal {
        precision: u8,
        scale: i8,
    },
    DoublePrecision,
    Date,
    Timestamp {
        precision: Option<u8>,
    },
    TimestampWithLocalTimeZone,
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
}

impl ExasolType {
    /// `VARCHAR(2000000)`, the widest `VARCHAR`: the declaration of an unbounded string column,
    /// and of the JSON text the scan writes for every type Exasol cannot hold.
    pub const WIDEST_VARCHAR: Self = Self::varchar(MAX_VARCHAR_SIZE);

    pub const fn decimal(precision: u8, scale: i8) -> Self {
        Self::Decimal { precision, scale }
    }

    /// `TIMESTAMP(precision)`. The bare 8.x `TIMESTAMP` is `Timestamp { precision: None }`.
    pub const fn timestamp(precision: u8) -> Self {
        Self::Timestamp {
            precision: Some(precision),
        }
    }

    /// A UTF8 `CHAR(size)`.
    pub const fn char(size: u32) -> Self {
        Self::Char { size, ascii: false }
    }

    /// A UTF8 `VARCHAR(size)`.
    pub const fn varchar(size: u32) -> Self {
        Self::Varchar { size, ascii: false }
    }

    /// `CHAR` or `VARCHAR`.
    pub fn is_character(&self) -> bool {
        matches!(self, Self::Char { .. } | Self::Varchar { .. })
    }

    /// `DECIMAL` at any precision and scale.
    pub fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal { .. })
    }
}

impl fmt::Display for ExasolType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::Decimal { precision, scale } => write!(f, "DECIMAL({precision},{scale})"),
            Self::DoublePrecision => f.write_str("DOUBLE PRECISION"),
            Self::Date => f.write_str("DATE"),
            Self::Timestamp { precision: None } => f.write_str("TIMESTAMP"),
            Self::Timestamp {
                precision: Some(digits),
            } => write!(f, "TIMESTAMP({digits})"),
            Self::TimestampWithLocalTimeZone => f.write_str("TIMESTAMP WITH LOCAL TIME ZONE"),
            Self::Char { size, ascii } => {
                write!(f, "CHAR({size}){}", character_set_suffix(*ascii))
            }
            Self::Varchar { size, ascii } => {
                write!(f, "VARCHAR({size}){}", character_set_suffix(*ascii))
            }
        }
    }
}

/// Exasol treats an unsuffixed character declaration as UTF8, so an `ASCII` column must carry
/// the suffix back or its type check reports "Data type mismatch".
fn character_set_suffix(ascii: bool) -> &'static str {
    if ascii { " ASCII" } else { "" }
}

#[cfg(test)]
#[path = "exasol_type_tests.rs"]
mod tests;
