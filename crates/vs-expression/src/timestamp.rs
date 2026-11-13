//! Timestamp precision: the Arrow unit that holds a count of fractional-second digits, and the
//! digits a timestamp literal spells.

/// An Arrow timestamp unit. This crate has no Arrow dependency, so the engine maps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimestampUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimestampUnit {
    pub const fn digits(self) -> u8 {
        match self {
            Self::Second => 0,
            Self::Millisecond => 3,
            Self::Microsecond => 6,
            Self::Nanosecond => 9,
        }
    }

    /// The coarsest unit that holds `precision` fractional digits.
    pub const fn holding(precision: u8) -> Self {
        match precision {
            0 => Self::Second,
            1..=3 => Self::Millisecond,
            4..=6 => Self::Microsecond,
            _ => Self::Nanosecond,
        }
    }
}

/// The value of a timestamp literal, `YYYY-MM-DD HH:MI:SS[.fraction]`.
#[derive(Debug, Clone, Copy)]
pub struct TimestampLiteral<'a>(pub &'a str);

impl<'a> TimestampLiteral<'a> {
    fn fraction(self) -> &'a str {
        self.0.split_once('.').map_or("", |(_, fraction)| fraction)
    }

    /// Exasol types a literal by every fractional digit it spells, trailing zeros included.
    pub fn type_precision(self) -> usize {
        self.fraction().len()
    }

    pub fn significant_digits(self) -> usize {
        self.fraction().trim_end_matches('0').len()
    }

    pub fn fits(self, unit: TimestampUnit) -> bool {
        self.significant_digits() <= usize::from(unit.digits())
    }

    /// The literal cut to `precision` fractional digits. `None` when the cut drops no nonzero
    /// digit.
    pub fn floor(self, precision: u8) -> Option<String> {
        let precision = usize::from(precision);
        if self.significant_digits() <= precision {
            return None;
        }
        let (instant, fraction) = self.0.split_once('.')?;
        Some(match precision {
            0 => instant.to_string(),
            p => format!("{instant}.{}", &fraction[..p]),
        })
    }
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
