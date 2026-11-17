//! The zones the adapter folds clock values in: a `SESSION_TIME_ZONE` property value, and the zone of the
//! `TZ` variable that Exasol sets to `DBTIMEZONE` in the adapter VM.

use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use chrono_tz::Tz;
use exasol_udf_sdk::error::UdfError;

/// A tz database zone, or the fixed offset of a POSIX `TZ` value such as `UTC-7`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TimeZone {
    Named(Tz),
    Fixed(FixedOffset),
}

impl TimeZone {
    /// The `SESSION_TIME_ZONE` value rule. Exasol's `EXA_TIME_ZONES` names are exactly the tz database
    /// names, upper-cased; `Err` carries the expectation of the invalid-value entry.
    pub(crate) fn exasol_name(raw: &str) -> Result<Self, String> {
        named(raw)
            .map(Self::Named)
            .ok_or_else(|| "a time zone name from EXA_TIME_ZONES".to_string())
    }

    /// The zone of the `TZ` value Exasol sets to `DBTIMEZONE`: a zone name, or for a numeric zone
    /// the POSIX `UTC±H[:MM]`, whose sign is inverted (`UTC-7` is seven hours east of UTC).
    pub(crate) fn of_tz_variable(raw: Option<&str>) -> Result<Self, UdfError> {
        let refused = |what: String| {
            UdfError::User(format!(
                "cannot fold a clock function or TIMESTAMP WITH LOCAL TIME ZONE literal in the \
                 adapter's TZ zone: TZ {what}; set a %env TZ line in the adapter script, or \
                 SESSION_TIME_ZONE for the session-zone values"
            ))
        };
        let raw = raw.ok_or_else(|| refused("is unset".to_string()))?;
        named(raw)
            .map(Self::Named)
            .or_else(|| posix_offset(raw).map(Self::Fixed))
            .ok_or_else(|| refused(format!("'{raw}' names no time zone")))
    }

    /// The date and time a clock in this zone shows at `instant`.
    pub(crate) fn wall_clock(self, instant: DateTime<Utc>) -> NaiveDateTime {
        match self {
            Self::Named(tz) => instant.with_timezone(&tz).naive_local(),
            Self::Fixed(offset) => instant.with_timezone(&offset).naive_local(),
        }
    }
}

fn named(raw: &str) -> Option<Tz> {
    chrono_tz::TZ_VARIANTS
        .iter()
        .copied()
        .find(|tz| tz.name().eq_ignore_ascii_case(raw))
}

fn posix_offset(raw: &str) -> Option<FixedOffset> {
    let rest = raw.strip_prefix("UTC")?;
    let (west, rest) = match rest.split_at_checked(1)? {
        ("+", rest) => (true, rest),
        ("-", rest) => (false, rest),
        _ => return None,
    };
    let (hours, minutes) = rest.split_once(':').unwrap_or((rest, "0"));
    let digits = |text: &str, max: i32| {
        (!text.is_empty() && text.len() <= 2 && text.bytes().all(|b| b.is_ascii_digit()))
            .then(|| text.parse::<i32>().ok())
            .flatten()
            .filter(|value| *value <= max)
    };
    let seconds = digits(hours, 24)? * 3600 + digits(minutes, 59)? * 60;
    FixedOffset::east_opt(if west { -seconds } else { seconds })
}

#[cfg(test)]
#[path = "time_zone_tests.rs"]
mod tests;
