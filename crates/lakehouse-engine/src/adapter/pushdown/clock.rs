//! The statement clock, and the fold that puts a plain date or timestamp literal in place of every
//! clock function and compared `TIMESTAMP WITH LOCAL TIME ZONE` literal of a pushdown request, so
//! no planning path and no scan reads a clock or a session zone.

use std::sync::OnceLock;

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use exasol_udf_sdk::error::UdfError;
use serde_json::{Value as Json, json};
use vs_expression::timestamp::TimestampLiteral;
use vs_expression::{ColumnNode, ColumnTypeLookup, ExasolVersion, ScanTyping, walk_column_nodes};

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

/// The `(table, column)` names and types of a request's involved columns, typed as native Exasol
/// types them over a table whose instant columns are TSTZ. Exasol declares an instant column a
/// plain `TIMESTAMP` holding each instant's UTC date and time, and no virtual column TSTZ.
struct NativeColumns(Vec<(String, String, ExasolType)>);

impl NativeColumns {
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
                let native = match *exa_type {
                    ExasolType::Timestamp { precision }
                        if note.is_some_and(declares_utc_instants) =>
                    {
                        ExasolType::TimestampWithLocalTimeZone { precision }
                    }
                    other => other,
                };
                columns.push((involved.notes.table.to_string(), name.to_string(), native));
            }
        }
        Ok(Self(columns))
    }

    /// Whether `operand` reads an instant column and native Exasol types it TSTZ. An explicit
    /// TSTZ cast of a plain column reads no instant: Exasol evaluates it in the session zone.
    fn is_instant(&self, operand: &Json, version: ExasolVersion) -> bool {
        let mut reads_instant = false;
        walk_column_nodes(operand, &mut |column| {
            reads_instant |= self.column(&column).is_some_and(is_tstz);
        });
        // The typed pass gives a TSTZ value no type, and NULL leaves the instant's type to unify.
        reads_instant
            && ScanTyping::new(self, version)
                .exact_type(&without_tstz_values(operand, version))
                .is_some_and(is_tstz)
    }

    fn column(&self, column: &ColumnNode) -> Option<ExasolType> {
        let table = column.table_name.unwrap_or_default();
        self.0
            .iter()
            .find(|(column_table, name, _)| column_table == table && name == column.name)
            .map(|(_, _, ty)| *ty)
    }
}

impl ColumnTypeLookup for NativeColumns {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        self.column(&ColumnNode::parse(column)?)
    }
}

fn is_tstz(ty: ExasolType) -> bool {
    matches!(ty, ExasolType::TimestampWithLocalTimeZone { .. })
}

/// A `TIMESTAMP WITH LOCAL TIME ZONE` value: its UTC wall clock and its fractional digits.
#[derive(Clone, Copy)]
struct Tstz {
    utc: NaiveDateTime,
    digits: usize,
}

/// How one walk folds: the columns that tell an instant operand, beside which a TSTZ value folds
/// to UTC, and the literal a TSTZ value becomes anywhere else.
struct Fold<'a> {
    columns: &'a NativeColumns,
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

    /// A TSTZ value beside an instant operand folds to its UTC date and time. Elsewhere a TSTZ
    /// select-list item keeps the instant for Exasol to show, and other values fold to wall clock.
    pub(super) fn fold(
        &self,
        request: &Json,
        pushdown: PushdownRequest,
        version: ExasolVersion,
    ) -> Result<PushdownRequest, UdfError> {
        let columns = NativeColumns::of(request)?;
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
        for operand in beside_an_instant(node, fold) {
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
        if let Some(literal) = tstz_literal_value(node)? {
            return Ok(Some(literal));
        }
        if is_tstz_call(node, version) {
            return self.now(node).map(Some);
        }
        Ok(None)
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

/// Every operand of a predicate or function call that has an instant operand, none otherwise.
/// Typing reads the operands before the walk folds them.
fn beside_an_instant<'n>(node: &'n mut Json, fold: &Fold) -> Vec<&'n mut Json> {
    let kind = node.get("type").and_then(Json::as_str).unwrap_or_default();
    if !(kind.starts_with("predicate_") || kind.starts_with("function_")) {
        return Vec::new();
    }
    let Json::Object(fields) = node else {
        return Vec::new();
    };
    let operands: Vec<&mut Json> = fields
        .values_mut()
        .flat_map(|value| match value {
            Json::Array(items) => items.iter_mut().collect(),
            value => vec![value],
        })
        .filter(|operand| operand.is_object())
        .collect();
    if operands
        .iter()
        .any(|operand| fold.columns.is_instant(operand, fold.version))
    {
        operands
    } else {
        Vec::new()
    }
}

/// `CURRENT_TIMESTAMP`, or `LOCALTIMESTAMP` on Exasol 8 only: the clock calls Exasol types TSTZ.
fn is_tstz_call(node: &Json, version: ExasolVersion) -> bool {
    let field = |key| node.get(key).and_then(Json::as_str);
    field("type") == Some("function_scalar")
        && field("name")
            .map(str::to_ascii_uppercase)
            .is_some_and(|name| {
                name == "CURRENT_TIMESTAMP"
                    || (name == "LOCALTIMESTAMP" && version == ExasolVersion::Exasol8)
            })
}

/// `node` with a NULL literal in place of each TSTZ literal and TSTZ clock call.
fn without_tstz_values(node: &Json, version: ExasolVersion) -> Json {
    let is_tstz_literal = matches!(tstz_literal_value(node), Ok(Some(_)));
    if is_tstz_literal || is_tstz_call(node, version) {
        return json!({"type": "literal_null"});
    }
    match node {
        Json::Array(items) => items
            .iter()
            .map(|item| without_tstz_values(item, version))
            .collect(),
        Json::Object(fields) => Json::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), without_tstz_values(value, version)))
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
