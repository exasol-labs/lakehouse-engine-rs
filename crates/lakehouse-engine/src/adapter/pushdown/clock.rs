//! The statement clock, and the fold that puts a plain date or timestamp literal in place of every
//! clock function and compared `TIMESTAMP WITH LOCAL TIME ZONE` literal of a pushdown request, so
//! no planning path and no scan reads a clock or a session zone.

use std::sync::OnceLock;

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use exasol_udf_sdk::error::UdfError;
use serde_json::{Value as Json, json};
use vs_expression::timestamp::TimestampLiteral;
use vs_expression::{ColumnNode, ExasolVersion};

use crate::adapter::column_notes::declares_utc_instants;
use crate::adapter::time_zone::TimeZone;
use crate::types::exasol_type::ExasolType;

use super::PushdownRequest;
use super::support::involved_columns;

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

/// The `(table, column)` names of a request's involved columns whose source type holds UTC
/// instants. Exasol declares them plain `TIMESTAMP` holding each instant's UTC date and time.
#[derive(Default)]
struct InstantColumns(Vec<(String, String)>);

impl InstantColumns {
    /// A column without a readable note counts as no instant column; planning refuses it later.
    fn of(request: &Json) -> Result<Self, UdfError> {
        let table_count = request
            .get("involvedTables")
            .and_then(Json::as_array)
            .map_or(0, Vec::len);
        let mut instants = Vec::new();
        for index in 0..table_count {
            let involved = involved_columns(request, |tables| tables.get(index))?;
            let columns = involved.types.iter().zip(&involved.notes.columns);
            for ((_, exa_type), (name, note)) in columns {
                if matches!(exa_type, ExasolType::Timestamp { .. })
                    && note.is_some_and(declares_utc_instants)
                {
                    instants.push((involved.notes.table.to_string(), name.to_string()));
                }
            }
        }
        Ok(Self(instants))
    }

    fn holds(&self, node: &Json) -> bool {
        let Some(column) = ColumnNode::parse(node) else {
            return false;
        };
        let table = column.table_name.unwrap_or_default();
        self.0
            .iter()
            .any(|(instant_table, name)| instant_table == table && name == column.name)
    }
}

/// A `TIMESTAMP WITH LOCAL TIME ZONE` value: its UTC wall clock and its fractional digits.
#[derive(Clone, Copy)]
struct Tstz {
    utc: NaiveDateTime,
    digits: usize,
}

/// How one walk folds: the instant columns a TSTZ value compares with as UTC, and the literal a
/// TSTZ value becomes anywhere else.
struct Fold<'a> {
    instants: &'a InstantColumns,
    version: ExasolVersion,
    tstz: &'a dyn Fn(Tstz) -> Result<Json, UdfError>,
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
    /// Elsewhere a TSTZ value compared directly with an instant column of `request` compares as
    /// its UTC date and time, and any other one as the plain wall clock of the folding zone.
    /// `version` decides whether `LOCALTIMESTAMP` is a TSTZ value.
    pub(super) fn fold(
        &self,
        request: &Json,
        pushdown: PushdownRequest,
        version: ExasolVersion,
    ) -> Result<PushdownRequest, UdfError> {
        let instants = InstantColumns::of(request)?;
        let no_instants = InstantColumns::default();
        let compared = Fold {
            instants: &instants,
            version,
            tstz: &|tstz| self.wall_clock_literal(tstz),
        };
        let projected = Fold {
            instants: &no_instants,
            version,
            tstz: &|tstz| Ok(tstz_literal(tstz)),
        };
        let mut json = pushdown.json().clone();
        let mut select_list = json
            .as_object_mut()
            .and_then(|fields| fields.remove("selectList"));
        self.walk(&mut json, &compared)?;
        if let Some(Json::Array(items)) = &mut select_list {
            for (index, item) in items.iter_mut().enumerate() {
                match pushdown.select_type(index) {
                    Some(ExasolType::TimestampWithLocalTimeZone { .. }) => {
                        self.walk(item, &projected)?;
                    }
                    _ => self.walk(item, &compared)?,
                }
            }
        }
        if let (Some(list), Json::Object(fields)) = (select_list, &mut json) {
            fields.insert("selectList".to_string(), list);
        }
        Ok(pushdown.with_json(json))
    }

    fn walk(&self, node: &mut Json, fold: &Fold) -> Result<(), UdfError> {
        if let Some(folded) = self.folded(node, fold.tstz)? {
            *node = folded;
            return Ok(());
        }
        for operand in compared_with_instant_column(node, fold.instants) {
            if let Some(tstz) = self.tstz_value(operand, fold.version)? {
                *operand = timestamp_literal(tstz.utc, tstz.digits);
            }
        }
        match node {
            Json::Array(items) => items.iter_mut().try_for_each(|item| self.walk(item, fold)),
            Json::Object(fields) => fields
                .values_mut()
                .try_for_each(|value| self.walk(value, fold)),
            _ => Ok(()),
        }
    }

    /// The value of a node Exasol types TSTZ: a TSTZ literal, `CURRENT_TIMESTAMP`, or, on
    /// Exasol 8 only, `LOCALTIMESTAMP`. `None` for any other node.
    fn tstz_value(&self, node: &Json, version: ExasolVersion) -> Result<Option<Tstz>, UdfError> {
        let field = |key| node.get(key).and_then(Json::as_str);
        if let Some(literal) = tstz_literal_value(node)? {
            return Ok(Some(literal));
        }
        match (field("type"), field("name").map(str::to_ascii_uppercase)) {
            (Some("function_scalar"), Some(name))
                if name == "CURRENT_TIMESTAMP"
                    || (name == "LOCALTIMESTAMP" && version == ExasolVersion::Exasol8) =>
            {
                self.now(node).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn now(&self, call: &Json) -> Result<Tstz, UdfError> {
        Ok(Tstz {
            utc: self.instant.naive_utc(),
            digits: precision(call)?,
        })
    }

    /// The literal in place of `node`, `None` for a node that is no clock call or TSTZ literal.
    fn folded(
        &self,
        node: &Json,
        tstz: &dyn Fn(Tstz) -> Result<Json, UdfError>,
    ) -> Result<Option<Json>, UdfError> {
        if let Some(literal) = tstz_literal_value(node)? {
            return tstz(literal).map(Some);
        }
        let field = |key| node.get(key).and_then(Json::as_str);
        if field("type") != Some("function_scalar") {
            return Ok(None);
        }
        let Some(name) = field("name").map(str::to_ascii_uppercase) else {
            return Ok(None);
        };
        Ok(Some(match name.as_str() {
            "CURRENT_DATE" => date_literal(self.folding_zone()?.wall_clock(self.instant)),
            "SYSDATE" => date_literal(self.tz_zone()?.wall_clock(self.instant)),
            "SYSTIMESTAMP" => {
                timestamp_literal(self.tz_zone()?.wall_clock(self.instant), precision(node)?)
            }
            "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" => tstz(self.now(node)?)?,
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

/// The operands that a comparison, `BETWEEN`, or `IN` sets directly against an instant column.
fn compared_with_instant_column<'n>(
    node: &'n mut Json,
    instants: &InstantColumns,
) -> Vec<&'n mut Json> {
    let kind = node.get("type").and_then(Json::as_str).unwrap_or_default();
    let comparison = matches!(
        kind,
        "predicate_equal"
            | "predicate_notequal"
            | "predicate_less"
            | "predicate_lessequal"
            | "predicate_greater"
            | "predicate_greaterequal"
    );
    let holds = |key: &str| node.get(key).is_some_and(|operand| instants.holds(operand));
    let keys: &[&str] = match kind {
        _ if comparison && holds("left") => &["right"],
        _ if comparison && holds("right") => &["left"],
        "predicate_between" if holds("expression") => &["left", "right"],
        "predicate_in_constlist" if holds("expression") => &["arguments"],
        _ => &[],
    };
    let Json::Object(fields) = node else {
        return Vec::new();
    };
    fields
        .iter_mut()
        .filter(|(key, _)| keys.contains(&key.as_str()))
        .flat_map(|(_, operand)| match operand {
            Json::Array(items) => items.iter_mut().collect(),
            operand => vec![operand],
        })
        .collect()
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

/// The value of a TSTZ literal node, `None` for any other node.
fn tstz_literal_value(node: &Json) -> Result<Option<Tstz>, UdfError> {
    match node.get("type").and_then(Json::as_str) {
        Some("literal_timestamputc" | "literal_timestamp_utc") => node
            .get("value")
            .and_then(Json::as_str)
            .map(parse_tstz)
            .transpose(),
        _ => Ok(None),
    }
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
