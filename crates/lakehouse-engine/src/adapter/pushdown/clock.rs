//! The statement clock, and the fold that puts a plain date or timestamp literal in place of every
//! clock function and compared `TIMESTAMP WITH LOCAL TIME ZONE` literal of a pushdown request, so
//! no planning path and no scan reads a clock or a session zone.

use std::sync::OnceLock;

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use exasol_udf_sdk::error::UdfError;
use serde_json::{Value as Json, json};

use crate::adapter::time_zone::TimeZone;
use crate::types::exasol_type::ExasolType;

use super::PushdownRequest;
use super::support::drop_constant_sort_keys;

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
    pub(super) fn fold(&self, pushdown: PushdownRequest) -> Result<PushdownRequest, UdfError> {
        let mut json = pushdown.json().clone();
        let mut select_list = json
            .as_object_mut()
            .and_then(|fields| fields.remove("selectList"));
        self.walk(&mut json, &|tstz| self.wall_clock_literal(tstz))?;
        if let Some(Json::Array(items)) = &mut select_list {
            for (index, item) in items.iter_mut().enumerate() {
                match pushdown.select_type(index) {
                    Some(ExasolType::TimestampWithLocalTimeZone { .. }) => {
                        self.walk(item, &|tstz| Ok(tstz_literal(tstz)))?;
                    }
                    _ => self.walk(item, &|tstz| self.wall_clock_literal(tstz))?,
                }
            }
        }
        if let (Some(list), Json::Object(fields)) = (select_list, &mut json) {
            fields.insert("selectList".to_string(), list);
        }
        // A clock sort key is a constant now, which parsing would have dropped.
        drop_constant_sort_keys(&mut json);
        Ok(pushdown.with_json(json))
    }

    fn walk(
        &self,
        node: &mut Json,
        tstz: &dyn Fn(Tstz) -> Result<Json, UdfError>,
    ) -> Result<(), UdfError> {
        if let Some(folded) = self.folded(node, tstz)? {
            *node = folded;
            return Ok(());
        }
        match node {
            Json::Array(items) => items.iter_mut().try_for_each(|item| self.walk(item, tstz)),
            Json::Object(fields) => fields
                .values_mut()
                .try_for_each(|value| self.walk(value, tstz)),
            _ => Ok(()),
        }
    }

    /// The literal in place of `node`, `None` for a node that is no clock call or TSTZ literal.
    fn folded(
        &self,
        node: &Json,
        tstz: &dyn Fn(Tstz) -> Result<Json, UdfError>,
    ) -> Result<Option<Json>, UdfError> {
        let field = |key| node.get(key).and_then(Json::as_str);
        match field("type") {
            Some("function_scalar") => {}
            Some("literal_timestamputc" | "literal_timestamp_utc") => {
                return match node.get("value") {
                    Some(Json::String(value)) => tstz(parse_tstz(value)?).map(Some),
                    _ => Ok(None),
                };
            }
            _ => return Ok(None),
        }
        let Some(name) = field("name").map(str::to_ascii_uppercase) else {
            return Ok(None);
        };
        let utc = self.instant.naive_utc();
        Ok(Some(match name.as_str() {
            "CURRENT_DATE" => date_literal(self.folding_zone()?.wall_clock(self.instant)),
            "SYSDATE" => date_literal(self.tz_zone()?.wall_clock(self.instant)),
            "SYSTIMESTAMP" => {
                timestamp_literal(self.tz_zone()?.wall_clock(self.instant), precision(node)?)
            }
            "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" => tstz(Tstz {
                utc,
                digits: precision(node)?,
            })?,
            _ => return Ok(None),
        }))
    }

    fn wall_clock_literal(&self, tstz: Tstz) -> Result<Json, UdfError> {
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
fn precision(call: &Json) -> Result<usize, UdfError> {
    let Some(argument) = call.pointer("/arguments/0") else {
        return Ok(DEFAULT_DIGITS);
    };
    argument
        .get("value")
        .and_then(Json::as_str)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|digits| *digits <= MAX_DIGITS)
        .ok_or_else(|| {
            UdfError::User(format!(
                "clock function precision {argument} is no whole number from 0 to {MAX_DIGITS}"
            ))
        })
}

fn parse_tstz(value: &str) -> Result<Tstz, UdfError> {
    let utc = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").map_err(|e| {
        UdfError::User(format!(
            "TIMESTAMP WITH LOCAL TIME ZONE literal '{value}' is no timestamp: {e}"
        ))
    })?;
    let digits = value
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    Ok(Tstz { utc, digits })
}

fn date_literal(wall_clock: NaiveDateTime) -> Json {
    json!({"type": "literal_date", "value": wall_clock.date().format("%Y-%m-%d").to_string()})
}

fn timestamp_literal(wall_clock: NaiveDateTime, digits: usize) -> Json {
    json!({"type": "literal_timestamp", "value": timestamp_text(wall_clock, digits)})
}

fn tstz_literal(tstz: Tstz) -> Json {
    json!({"type": "literal_timestamputc", "value": timestamp_text(tstz.utc, tstz.digits)})
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
