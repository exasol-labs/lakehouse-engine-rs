//! The statement clock, and the fold that puts a plain date or timestamp literal in place of every
//! clock function and compared `TIMESTAMP WITH LOCAL TIME ZONE` literal of a pushdown request, so
//! no planning path and no scan reads a clock or a session zone.

use std::sync::OnceLock;

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use exasol_udf_sdk::error::UdfError;
use vs_expression::timestamp::TimestampLiteral;
use vs_expression::{Expr, Literal, LiteralKind, Scalar, ScalarCall, short};

use crate::adapter::time_zone::TimeZone;
use crate::types::exasol_type::ExasolType;

use super::request::{Clause, PushdownRequest};

/// Exasol's default fractional-seconds precision, and the only one Exasol 8 has.
const DEFAULT_DIGITS: usize = 3;
const MAX_DIGITS: usize = 9;
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Read once per adapter VM. Exasol runs one adapter VM per statement (#583), so every pushdown
/// request and virtual schema the VM serves folds the same instant.
pub(crate) fn statement_instant() -> DateTime<Utc> {
    static READING: OnceLock<DateTime<Utc>> = OnceLock::new();
    *READING.get_or_init(Utc::now)
}

/// The statement's instant with the two zones it folds in: `TZ`, which Exasol sets to
/// `DBTIMEZONE`, and `SESSION_TIME_ZONE`, which stands in for the session zone when set.
pub(crate) struct StatementClock {
    instant: DateTime<Utc>,
    tz: Option<String>,
    session_time_zone: Option<TimeZone>,
}

/// A `TIMESTAMP WITH LOCAL TIME ZONE` value: its UTC wall clock and its fractional digits.
#[derive(Clone, Copy)]
struct Tstz {
    utc: NaiveDateTime,
    digits: usize,
}

impl StatementClock {
    /// `tz` is the raw `TZ` value, parsed only when a node needs the `TZ` zone.
    pub(crate) fn new(
        instant: DateTime<Utc>,
        tz: Option<String>,
        session_time_zone: Option<TimeZone>,
    ) -> Self {
        Self {
            instant,
            tz,
            session_time_zone,
        }
    }

    /// Exasol shows a projected TSTZ value in the session zone, so a select-list item it types
    /// TSTZ keeps the UTC instant, which the Exasol dialect converts into `SESSIONTIMEZONE`.
    /// Everywhere else a TSTZ value compares as the plain wall clock of the folding zone.
    pub(super) fn fold(&self, pushdown: &PushdownRequest) -> Result<PushdownRequest, UdfError> {
        let keep_instant = |tstz| Ok(tstz_literal(tstz));
        let wall_clock = |tstz| self.wall_clock_literal(tstz);
        pushdown.try_map_expressions(&Clause::ALL, |clause, index, tree| {
            let tstz: &dyn Fn(Tstz) -> Result<Expr, UdfError> =
                match (clause, pushdown.select_type(index)) {
                    (Clause::SelectList, Some(ExasolType::TimestampWithLocalTimeZone { .. })) => {
                        &keep_instant
                    }
                    _ => &wall_clock,
                };
            tree.try_rewrite(&mut |node| self.folded(node, tstz))
        })
    }

    /// The literal in place of `node`, `None` for a node that is no clock call or TSTZ literal.
    fn folded(
        &self,
        node: &Expr,
        tstz: &dyn Fn(Tstz) -> Result<Expr, UdfError>,
    ) -> Result<Option<Expr>, UdfError> {
        let call = match node {
            Expr::Scalar(call) => call,
            Expr::Literal(Literal {
                kind: LiteralKind::TimestampUtc | LiteralKind::LocalTimeZoneTimestamp,
                value,
            }) => {
                return match value {
                    Scalar::String(value) => tstz(parse_tstz(value)?).map(Some),
                    _ => Ok(None),
                };
            }
            _ => return Ok(None),
        };
        let utc = self.instant.naive_utc();
        Ok(Some(match call.name.as_str() {
            "CURRENT_DATE" => date_literal(self.folding_zone()?.wall_clock(self.instant)),
            "SYSDATE" => date_literal(self.tz_zone()?.wall_clock(self.instant)),
            "SYSTIMESTAMP" => {
                timestamp_literal(self.tz_zone()?.wall_clock(self.instant), precision(call)?)
            }
            "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" => tstz(Tstz {
                utc,
                digits: precision(call)?,
            })?,
            _ => return Ok(None),
        }))
    }

    fn wall_clock_literal(&self, tstz: Tstz) -> Result<Expr, UdfError> {
        let wall_clock = self.folding_zone()?.wall_clock(tstz.utc.and_utc());
        Ok(timestamp_literal(wall_clock, tstz.digits))
    }

    fn tz_zone(&self) -> Result<TimeZone, UdfError> {
        TimeZone::of_tz_variable(self.tz.as_deref())
    }

    fn folding_zone(&self) -> Result<TimeZone, UdfError> {
        self.session_time_zone.map_or_else(|| self.tz_zone(), Ok)
    }
}

/// Exasol 2025 sends the precision of `SYSTIMESTAMP(6)` as the one argument; Exasol 8 sends none.
fn precision(call: &ScalarCall) -> Result<usize, UdfError> {
    let Some(argument) = call.arguments.first() else {
        return Ok(DEFAULT_DIGITS);
    };
    argument
        .as_literal()
        .and_then(|literal| literal.value.as_str())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|digits| *digits <= MAX_DIGITS)
        .ok_or_else(|| {
            UdfError::User(format!(
                "clock function precision {} is no whole number from 0 to {MAX_DIGITS}",
                short(argument)
            ))
        })
}

fn parse_tstz(value: &str) -> Result<Tstz, UdfError> {
    let utc = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").map_err(|e| {
        UdfError::User(format!(
            "TIMESTAMP WITH LOCAL TIME ZONE literal '{value}' is no timestamp: {e}"
        ))
    })?;
    let digits = TimestampLiteral(value).type_precision();
    Ok(Tstz { utc, digits })
}

fn date_literal(wall_clock: NaiveDateTime) -> Expr {
    literal(
        LiteralKind::Date,
        wall_clock.date().format("%Y-%m-%d").to_string(),
    )
}

fn timestamp_literal(wall_clock: NaiveDateTime, digits: usize) -> Expr {
    literal(LiteralKind::Timestamp, timestamp_text(wall_clock, digits))
}

fn tstz_literal(tstz: Tstz) -> Expr {
    literal(
        LiteralKind::LocalTimeZoneTimestamp,
        timestamp_text(tstz.utc, tstz.digits),
    )
}

fn literal(kind: LiteralKind, value: String) -> Expr {
    Expr::Literal(Literal::new(kind, Scalar::String(value)))
}

/// Truncated, not rounded, to `digits`, as Exasol truncates its clock.
fn timestamp_text(value: NaiveDateTime, digits: usize) -> String {
    let seconds = value.format(TIMESTAMP_FORMAT).to_string();
    if digits == 0 {
        return seconds;
    }
    // A leap second carries more than 999_999_999 nanoseconds.
    let nanos = format!("{:09}", value.nanosecond().min(999_999_999));
    format!("{seconds}.{}", &nanos[..digits.min(MAX_DIGITS)])
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;
