//! The statement clock, and the fold that puts a plain date or timestamp literal in place of every
//! clock function and compared `TIMESTAMP WITH LOCAL TIME ZONE` literal of a pushdown request, so
//! no planning path and no scan reads a clock or a session zone.

use std::sync::OnceLock;

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use exasol_udf_sdk::error::UdfError;
use serde_json::{Value as Json, json};
use vs_expression::timestamp::TimestampLiteral;
use vs_expression::{ColumnNode, ColumnTypeLookup, ExasolVersion, ScanTyping};

use crate::adapter::column_notes::declares_utc_instants;
use crate::adapter::time_zone::TimeZone;
use crate::types::exasol_type::ExasolType;

use super::PushdownRequest;
use super::support::involved_columns;

/// Exasol's default fractional-seconds precision, and the only one Exasol 8 has.
const DEFAULT_DIGITS: usize = 3;
const MAX_DIGITS: usize = 9;
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const CLOCK_FUNCTIONS: [&str; 5] = [
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "LOCALTIMESTAMP",
    "SYSDATE",
    "SYSTIMESTAMP",
];

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

/// A request's involved columns: table, name, declared type, and whether it is an instant column.
struct InvolvedColumns(Vec<(String, String, ExasolType, bool)>);

impl InvolvedColumns {
    /// A column without a readable note counts as no instant column; planning refuses it later.
    fn of(request: &Json) -> Result<Self, UdfError> {
        let table_count = request
            .get("involvedTables")
            .and_then(Json::as_array)
            .map_or(0, Vec::len);
        let mut columns = Vec::new();
        for index in 0..table_count {
            let involved = involved_columns(request, |tables| tables.get(index))?;
            let declared = involved.types.iter().zip(&involved.notes.columns);
            for ((_, exa_type), (name, note)) in declared {
                columns.push((
                    involved.notes.table.to_string(),
                    name.to_string(),
                    *exa_type,
                    note.is_some_and(declares_utc_instants),
                ));
            }
        }
        Ok(Self(columns))
    }

    /// Whether `operand` is a date or time that reads an instant column. An explicit TSTZ cast of
    /// a plain column reads no instant: Exasol evaluates it in the session zone.
    fn is_instant(&self, operand: &Json, version: ExasolVersion) -> bool {
        // The typed pass gives a clock value no type, and NULL leaves the other operands' type.
        self.reads_instant(operand)
            && ScanTyping::new(self, version)
                .exact_type(&without_clock_values(operand))
                .is_some_and(is_date_or_time)
    }

    /// A nested predicate, such as a `CASE` condition, gives the operand none of its value.
    fn reads_instant(&self, node: &Json) -> bool {
        if is_predicate(node) {
            return false;
        }
        if let Some(column) = ColumnNode::parse(node) {
            return self.column(&column).is_some_and(|(_, instant)| instant);
        }
        match node {
            Json::Array(items) => items.iter().any(|item| self.reads_instant(item)),
            Json::Object(fields) => fields.values().any(|value| self.reads_instant(value)),
            _ => false,
        }
    }

    fn column(&self, column: &ColumnNode) -> Option<(ExasolType, bool)> {
        let table = column.table_name.unwrap_or_default();
        self.0
            .iter()
            .find(|(column_table, name, ..)| column_table == table && name == column.name)
            .map(|(_, _, ty, instant)| (*ty, *instant))
    }
}

impl ColumnTypeLookup for InvolvedColumns {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        self.column(&ColumnNode::parse(column)?).map(|(ty, _)| ty)
    }
}

fn is_date_or_time(ty: ExasolType) -> bool {
    matches!(
        ty,
        ExasolType::Date
            | ExasolType::Timestamp { .. }
            | ExasolType::TimestampWithLocalTimeZone { .. }
    )
}

/// A `TIMESTAMP WITH LOCAL TIME ZONE` value: its UTC wall clock and its fractional digits.
#[derive(Clone, Copy)]
struct Tstz {
    utc: NaiveDateTime,
    digits: usize,
}

/// How one walk folds: the columns that tell an instant operand, beside which a clock value folds
/// to UTC, and the literal a TSTZ value becomes anywhere else.
struct Fold<'a> {
    columns: &'a InvolvedColumns,
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

    /// A clock value beside an instant operand folds to its UTC date or time. Elsewhere a TSTZ
    /// select-list item keeps the instant for Exasol to show, and other values fold to wall clock.
    pub(super) fn fold(
        &self,
        request: &Json,
        pushdown: PushdownRequest,
        version: ExasolVersion,
    ) -> Result<PushdownRequest, UdfError> {
        let columns = InvolvedColumns::of(request)?;
        let compared = Fold {
            columns: &columns,
            version,
            tstz: &|tstz| self.wall_clock_literal(tstz),
        };
        let projected = Fold {
            columns: &columns,
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
        if has_an_instant_operand(node, fold) {
            return self.walk_in_utc(node, fold);
        }
        match node {
            Json::Array(items) => items.iter_mut().try_for_each(|item| self.walk(item, fold)),
            Json::Object(fields) => fields
                .values_mut()
                .try_for_each(|value| self.walk(value, fold)),
            _ => Ok(()),
        }
    }

    /// Folds each clock value under `node` to UTC. A nested predicate decides for its operands.
    fn walk_in_utc(&self, node: &mut Json, fold: &Fold) -> Result<(), UdfError> {
        let children: Vec<&mut Json> = match node {
            Json::Array(items) => items.iter_mut().collect(),
            Json::Object(fields) => fields.values_mut().collect(),
            _ => return Ok(()),
        };
        for child in children {
            if let Some(utc) = self.utc_value(child)? {
                *child = utc;
            } else if is_predicate(child) {
                self.walk(child, fold)?;
            } else {
                self.walk_in_utc(child, fold)?;
            }
        }
        Ok(())
    }

    /// The literal of a clock value's UTC date or time, `None` for any other node.
    fn utc_value(&self, node: &Json) -> Result<Option<Json>, UdfError> {
        if let Some(tstz) = tstz_literal_value(node)? {
            return Ok(Some(timestamp_literal(tstz.utc, tstz.digits)));
        }
        let utc = self.instant.naive_utc();
        Ok(match clock_call(node).as_deref() {
            Some("CURRENT_DATE" | "SYSDATE") => Some(date_literal(utc)),
            Some(_) => Some(timestamp_literal(utc, precision(node)?)),
            None => None,
        })
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
        let Some(name) = clock_call(node) else {
            return Ok(None);
        };
        Ok(Some(match name.as_str() {
            "CURRENT_DATE" => date_literal(self.folding_zone()?.wall_clock(self.instant)),
            "SYSDATE" => date_literal(self.tz_zone()?.wall_clock(self.instant)),
            "SYSTIMESTAMP" => {
                timestamp_literal(self.tz_zone()?.wall_clock(self.instant), precision(node)?)
            }
            _ => tstz(self.now(node)?)?,
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

/// Whether `node` is a predicate or function call with an instant operand.
fn has_an_instant_operand(node: &Json, fold: &Fold) -> bool {
    let kind = node.get("type").and_then(Json::as_str).unwrap_or_default();
    if !(kind.starts_with("predicate_") || kind.starts_with("function_")) {
        return false;
    }
    let Json::Object(fields) = node else {
        return false;
    };
    fields
        .values()
        .flat_map(|value| match value {
            Json::Array(items) => items.iter().collect(),
            value => vec![value],
        })
        .any(|operand| operand.is_object() && fold.columns.is_instant(operand, fold.version))
}

fn is_predicate(node: &Json) -> bool {
    node.get("type")
        .and_then(Json::as_str)
        .is_some_and(|kind| kind.starts_with("predicate_"))
}

/// The upper-case name of a clock function call, `None` for any other node.
fn clock_call(node: &Json) -> Option<String> {
    let field = |key| node.get(key).and_then(Json::as_str);
    if field("type") != Some("function_scalar") {
        return None;
    }
    field("name")
        .map(str::to_ascii_uppercase)
        .filter(|name| CLOCK_FUNCTIONS.contains(&name.as_str()))
}

/// `node` with a NULL literal in place of each TSTZ literal and clock call.
fn without_clock_values(node: &Json) -> Json {
    if matches!(tstz_literal_value(node), Ok(Some(_))) || clock_call(node).is_some() {
        return json!({"type": "literal_null"});
    }
    match node {
        Json::Array(items) => items.iter().map(without_clock_values).collect(),
        Json::Object(fields) => Json::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), without_clock_values(value)))
                .collect(),
        ),
        other => other.clone(),
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
