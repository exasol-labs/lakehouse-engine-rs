use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

mod column_node;
mod datetime_format;
pub mod exasol_type;
mod fragment;
mod json_value;
mod scalar_functions;
mod scan_typing;
pub use column_node::{ColumnNode, rewrite_table_aliases, strip_table_alias, walk_column_nodes};
use datetime_format::{FormatTarget, strftime_pattern};
use exasol_type::{ExasolType, exasol_type_from_json};
pub use fragment::SqlFragment;
use json_value::{Fallback, JsonValueCall, Returning};
pub use json_value::{JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path};
use scalar_functions::{ExasolForm, scalar_function};
use scan_typing::{
    BOOLEAN_NUMBER_NODE, BOOLEAN_TEXT_NODE, INTEGER_TEXT_NODE, SECOND_NODE, second_fraction,
    seconds_type,
};
pub use scan_typing::{
    ColumnTypeLookup, EXPR_ARRAY_FIELDS, EXPR_SINGLE_FIELDS, exact_expression_type,
};

/// Which parser consumes the rendered fragment: DataFusion inside the scan UDF
/// (`ScanSpec` filter/projection/group keys) or Exasol for the outer wrapper SQL.
/// The `Exasol` dialect re-emits what Exasol sent (name, argument order and count),
/// since Exasol cannot parse DataFusion-shaped rewrites.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    DataFusion,
    Exasol,
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn binary_op(kind: &str) -> Option<&'static str> {
    match kind {
        "predicate_equal" => Some("="),
        "predicate_notequal" => Some("<>"),
        "predicate_less" => Some("<"),
        "predicate_lessequal" => Some("<="),
        "predicate_greater" => Some(">"),
        "predicate_greaterequal" => Some(">="),
        _ => None,
    }
}

fn is_empty_opt(value: Option<&str>) -> bool {
    matches!(value, None | Some(""))
}

/// DataFusion dialect only: Exasol's `TRUE`/`FALSE` casing instead of DataFusion's lowercase;
/// NULL stays NULL rather than becoming `'NULL'` or `'FALSE'`.
fn render_bool_to_string_case(bool_expr: &str) -> String {
    format!("(CASE {bool_expr} WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)")
}

fn quote_literal(value: Option<&Json>) -> String {
    match value {
        None | Some(Json::Null) => "NULL".to_string(),
        Some(Json::String(s)) => format!("'{}'", sql_escape(s)),
        Some(other) => format!("'{}'", sql_escape(&json_scalar_to_string(other))),
    }
}

/// Shared by `literal_timestamp` and `literal_timestamp_utc` so they cannot drift.
/// Exasol's literal format has no offset field (22018 on `+00:00`), and
/// `TIMESTAMP NULL` is a syntax error (42000), so NULL renders bare.
fn render_exasol_timestamp_literal(value: Option<&Json>) -> String {
    match value {
        None | Some(Json::Null) => "NULL".to_string(),
        Some(v) => format!("TIMESTAMP {}", quote_literal(Some(v))),
    }
}

fn timestamp_literal_unit(value: Option<&Json>) -> &'static str {
    let fraction = value.and_then(Json::as_str).and_then(|v| v.split_once('.'));
    match fraction {
        Some((_, digits)) if digits.trim_end_matches('0').len() > 6 => "Nanosecond",
        _ => "Microsecond",
    }
}

/// Converting the UTC-normalized wire value into `SESSIONTIMEZONE` reproduces
/// Exasol's own TSTZ-vs-TIMESTAMP coercion, which reads the naive side as
/// session-local (verified live).
fn render_exasol_tstz_literal(value: Option<&Json>) -> String {
    match value {
        None | Some(Json::Null) => "NULL".to_string(),
        Some(_) => format!(
            "CAST(CONVERT_TZ({}, 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)",
            render_exasol_timestamp_literal(value)
        ),
    }
}

fn json_scalar_to_string(value: &Json) -> String {
    match value {
        Json::String(s) => s.clone(),
        Json::Number(n) => n.to_string(),
        Json::Bool(b) => b.to_string(),
        Json::Null => String::new(),
        other => other.to_string(),
    }
}

/// Exasol's `IN`/`NOT IN` ignores NULL list entries, while DataFusion's
/// three-valued logic would empty the result.
fn is_null_literal(arg: &Json) -> bool {
    match arg.get("type").and_then(|t| t.as_str()) {
        Some("literal_null") => true,
        Some(t) if t.starts_with("literal_") => {
            matches!(arg.get("value"), None | Some(Json::Null))
        }
        _ => false,
    }
}

fn render_args(args: &[Json], dialect: Dialect) -> Result<Vec<String>, UdfError> {
    args.iter()
        .enumerate()
        .map(|(i, arg)| {
            render_expression_inner(arg, dialect)?
                .ok_or_else(|| UdfError::User(format!("argument[{i}] rendered to null")))
        })
        .collect()
}

/// Renders only a `literal_string` format the translation covers, and errs otherwise so the
/// caller declines.
fn render_to_datetime(
    target: FormatTarget,
    args: Option<&Vec<Json>>,
    dialect: Dialect,
) -> Result<Option<String>, UdfError> {
    let (node, function) = match target {
        FormatTarget::Date => ("TO_DATE", "to_date"),
        FormatTarget::Timestamp => ("TO_TIMESTAMP", "to_timestamp"),
    };
    let args =
        args.ok_or_else(|| UdfError::User(format!("function_scalar {node} missing 'arguments'")))?;
    match args.as_slice() {
        [] => Err(UdfError::User(format!(
            "function_scalar {node} requires at least 1 argument"
        ))),
        [_] => Err(UdfError::User(format!(
            "function_scalar {node} without a format parses by the session's NLS settings"
        ))),
        [source, format] => {
            let pattern = format
                .get("type")
                .and_then(Json::as_str)
                .filter(|kind| *kind == "literal_string")
                .and_then(|_| format.get("value").and_then(Json::as_str))
                .and_then(|model| strftime_pattern(model, target))
                .ok_or_else(|| {
                    UdfError::User(format!(
                        "function_scalar {node} format {format} is not a string literal with an exact strftime equivalent"
                    ))
                })?;
            let source = render_conversion_source(node, source, dialect)?;
            Ok(Some(format!(
                "{function}(nullif({source}, ''), '{}')",
                sql_escape(&pattern)
            )))
        }
        _ => Err(UdfError::User(format!(
            "function_scalar {node} accepts at most 2 arguments, got {}",
            args.len()
        ))),
    }
}

fn render_conversion_source(
    function: &str,
    source: &Json,
    dialect: Dialect,
) -> Result<String, UdfError> {
    render_expression_inner(source, dialect)?
        .ok_or_else(|| UdfError::User(format!("{function} source rendered to null")))
}

/// The Exasol dialect renders the target's `Display`. The DataFusion dialect declines a target it
/// cannot represent, and renders CHAR or VARCHAR as unsized VARCHAR without Exasol's checks (#587).
fn render_cast_target(target: ExasolType, dialect: Dialect) -> Result<String, UdfError> {
    match dialect {
        Dialect::Exasol => Ok(target.to_string()),
        Dialect::DataFusion => datafusion_cast_target(target),
    }
}

fn datafusion_cast_target(target: ExasolType) -> Result<String, UdfError> {
    Ok(match target {
        // datafusion-sql rejects a length-qualified VARCHAR (`support_varchar_with_length` is
        // not enabled), and Arrow has no fixed-width CHAR type.
        ExasolType::Char { .. } | ExasolType::Varchar { .. } => "VARCHAR".to_string(),
        ExasolType::Decimal(_) | ExasolType::Boolean | ExasolType::Date => target.to_string(),
        ExasolType::DoublePrecision => "DOUBLE".to_string(),
        // DataFusion parses only these four and reads a bare TIMESTAMP as nanoseconds, so the
        // rest decline rather than approximate.
        ExasolType::Timestamp {
            precision: precision @ (0 | 3 | 6 | 9),
        } => format!("TIMESTAMP({precision})"),
        // WLTZ carries session-time-zone semantics DataFusion's TIMESTAMP cannot reproduce, and
        // Arrow has no INTERVAL DAY(p) TO SECOND(f), GEOMETRY, or HASHTYPE equivalent.
        ExasolType::Timestamp { .. }
        | ExasolType::TimestampWithLocalTimeZone { .. }
        | ExasolType::IntervalYearToMonth { .. }
        | ExasolType::IntervalDayToSecond { .. }
        | ExasolType::Geometry { .. }
        | ExasolType::Hashtype { .. } => {
            return Err(UdfError::User(format!(
                "unsupported CAST target type: {target}"
            )));
        }
    })
}

/// Only the typed scan pass knows the fractional digits a one-argument SECOND keeps.
const UNTYPED_SECOND: &str = "SECOND renders only as a one-argument call through the typed scan pass, which knows the argument's precision";

/// The seconds of `arg` with their first `fraction` digits, cut beyond as Exasol does. An exact
/// decimal product scales them, because Arrow's decimal quotient keeps only the scale s1 + 4.
fn render_seconds(arg: &str, fraction: u8) -> Result<String, UdfError> {
    let (unit, unit_digits) = match fraction {
        0 => {
            return Ok(format!(
                "CAST(date_part('second', {arg}) AS {})",
                seconds_type(0)
            ));
        }
        1..=6 => ("microsecond", 6),
        7..=9 => ("nanosecond", 9),
        _ => {
            return Err(UdfError::User(format!(
                "SECOND cannot keep {fraction} fractional digits"
            )));
        }
    };
    let units = format!("date_part('{unit}', {arg})");
    let cut = match 10u32.pow(u32::from(unit_digits - fraction)) {
        1 => units,
        divisor => format!("({units} / {divisor})"),
    };
    let whole = ExasolType::decimal(2 + fraction, 0);
    let step = ExasolType::decimal(fraction, fraction);
    let width = usize::from(fraction);
    Ok(format!(
        "CAST((CAST({cut} AS {whole}) * CAST('0.{:0>width$}' AS {step})) AS {})",
        1,
        seconds_type(fraction)
    ))
}

/// The scalar UDF the DataFusion dialect renders `FLOAT_DIV` as, registered
/// by `lakehouse-engine`. It MUST take two arguments coerced to `Float64` (Exasol's
/// `FN_FLOAT_DIV` is always float division), return `Float64`, propagate `NULL`, and
/// error on any other non-finite result, so division by zero fails in a filter
/// exactly as in a projection.
pub const CHECKED_FLOAT_DIV_FN: &str = "vs_checked_float_div";

/// The scalar UDF the DataFusion dialect renders `DIV` as, registered by `lakehouse-engine`.
/// It raises on a zero divisor, so division by zero fails in a filter exactly as in a projection.
pub const CHECKED_INT_DIV_FN: &str = "vs_checked_int_div";

fn render_cast(
    args: Option<&Vec<Json>>,
    data_type: Option<&Json>,
    dialect: Dialect,
) -> Result<Option<String>, UdfError> {
    let args = args.ok_or_else(|| UdfError::User("CAST missing 'arguments'".into()))?;
    if args.is_empty() {
        return Err(UdfError::User("CAST requires 1 argument".into()));
    }
    let inner = render_expression_inner(&args[0], dialect)?
        .ok_or_else(|| UdfError::User("CAST expression is null".into()))?;
    let data_type = data_type.ok_or_else(|| UdfError::User("CAST missing 'dataType'".into()))?;
    let target = exasol_type_from_json(data_type)?;
    let target_type = render_cast_target(target, dialect)?;
    Ok(Some(format!("CAST({inner} AS {target_type})")))
}

fn json_value_operand(node: &Json, dialect: Dialect) -> Result<String, UdfError> {
    render_expression_inner(node, dialect)?
        .ok_or_else(|| UdfError::User("JSON_VALUE operand is null".into()))
}

/// Every clause Exasol sent, so Exasol computes the forms the scan declines.
fn render_json_value_exasol(expr: &Json) -> Result<String, UdfError> {
    let operand = |node: &Json| json_value_operand(node, Dialect::Exasol);
    let args = expr
        .get("arguments")
        .and_then(Json::as_array)
        .filter(|args| args.len() == 2)
        .ok_or_else(|| UdfError::User("JSON_VALUE takes a document and a path".into()))?;
    let mut sql = format!("JSON_VALUE({}, {}", operand(&args[0])?, operand(&args[1])?);
    if let Some(data_type) = expr.get("returningDataType") {
        sql.push_str(&format!(" RETURNING {}", exasol_type_from_json(data_type)?));
    }
    for (field, clause) in [("emptyBehavior", "EMPTY"), ("errorBehavior", "ERROR")] {
        let Some(behavior) = expr.get(field) else {
            continue;
        };
        let rendered = match behavior.get("type").and_then(Json::as_str) {
            Some("DEFAULT") => format!(
                "DEFAULT {}",
                operand(behavior.get("expression").unwrap_or(&Json::Null))?
            ),
            Some(kind @ ("NULL" | "ERROR")) => kind.to_string(),
            _ => return Err(UdfError::User(format!("JSON_VALUE behavior {behavior}"))),
        };
        sql.push_str(&format!(" {rendered} ON {clause}"));
    }
    Ok(sql + ")")
}

fn render_json_value_scan(expr: &Json) -> Result<String, UdfError> {
    let call = JsonValueCall::parse(expr)?;
    let fallback = |fallback: &Fallback| match fallback {
        Fallback::Null => "NULL".to_string(),
        Fallback::Text(text) => format!("'{}'", sql_escape(text)),
        Fallback::Boolean(value) => value.to_string().to_uppercase(),
    };
    let doc = json_value_operand(call.document, Dialect::DataFusion)?;
    let path = format!("'{}'", sql_escape(call.path));
    let (on_empty, on_error) = (fallback(&call.on_empty), fallback(&call.on_error));
    Ok(match call.returning {
        Returning::Text { max_chars } => {
            format!("{JSON_VALUE_TEXT_FN}({doc}, {path}, {max_chars}, {on_empty}, {on_error})")
        }
        Returning::Boolean => {
            format!("{JSON_VALUE_BOOLEAN_FN}({doc}, {path}, {on_empty}, {on_error})")
        }
    })
}

/// `Ok(None)` for `Json::Null` (an absent optional child).
fn render_expression_inner(expr: &Json, dialect: Dialect) -> Result<Option<String>, UdfError> {
    if expr.is_null() {
        return Ok(None);
    }
    if !expr.is_object() {
        return Err(UdfError::User(
            "unexpected expression node (not an object)".into(),
        ));
    }
    let kind = match expr.get("type").and_then(|t| t.as_str()) {
        Some(k) => k,
        None => {
            return Err(UdfError::User(
                "expression node missing 'type' field".into(),
            ));
        }
    };

    let value = |key: &str| expr.get(key);

    match kind {
        "literal_null" => return Ok(Some("NULL".into())),
        "literal_bool" => {
            let v = value("value");
            let truthy = matches!(v, Some(Json::Bool(true)))
                || matches!(v, Some(Json::String(s)) if s == "true" || s == "TRUE")
                || matches!(v, Some(Json::Number(n)) if n.as_i64() == Some(1));
            return Ok(Some(if truthy {
                "TRUE".into()
            } else {
                "FALSE".into()
            }));
        }
        "literal_string" => return Ok(Some(quote_literal(value("value")))),
        // The scan parses bare numeric literals as DECIMAL, which rejects `1e+300`.
        "literal_double" if dialect == Dialect::DataFusion => {
            return Ok(Some(match value("value") {
                None | Some(Json::Null) => "NULL".to_string(),
                Some(v) => format!("CAST({} AS DOUBLE)", quote_literal(Some(v))),
            }));
        }
        "literal_exactnumeric" | "literal_double" => {
            return Ok(Some(match value("value") {
                None | Some(Json::Null) => "NULL".to_string(),
                Some(v) => json_scalar_to_string(v),
            }));
        }
        "literal_date" => return Ok(Some(format!("DATE {}", quote_literal(value("value"))))),
        // Synthesized by the adapter for a binary column, never sent by Exasol. A `width` types
        // it `FixedSizeBinary`, so comparing it leaves a fixed-length column uncast.
        "literal_binary" if dialect == Dialect::DataFusion => {
            let hex = value("value").and_then(|v| v.as_str()).unwrap_or_default();
            if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(UdfError::User(format!(
                    "literal_binary value '{hex}' is not hexadecimal"
                )));
            }
            return Ok(Some(match value("width").and_then(|w| w.as_u64()) {
                Some(width) => format!("arrow_cast(X'{hex}', 'FixedSizeBinary({width})')"),
                None => format!("X'{hex}'"),
            }));
        }
        "literal_timestamp" => {
            // Exasol has no `arrow_cast` (42000); re-emit the bare literal it sent.
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_timestamp_literal(value("value"))));
            }
            // Explicit precision: DataFusion types a bare `TIMESTAMP` literal as
            // nanoseconds, which overflows on far-future values when unified with
            // microsecond columns, so only sub-microsecond digits get
            // nanoseconds. A NULL value renders `arrow_cast(NULL, ...)`,
            // deliberately unlike `literal_timestamp_utc`.
            return Ok(Some(format!(
                "arrow_cast({}, 'Timestamp({}, None)')",
                quote_literal(value("value")),
                timestamp_literal_unit(value("value"))
            )));
        }
        "literal_timestamp_utc" => {
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_tstz_literal(value("value"))));
            }
            // `+00:00` parses the value as UTC; the "UTC" tz label matches the
            // scan's Timestamptz Arrow mapping, avoiding a tz-label mismatch.
            let raw = match value("value") {
                None | Some(Json::Null) => return Ok(Some("NULL".into())),
                Some(v) => json_scalar_to_string(v),
            };
            let quoted = quote_literal(Some(&Json::String(format!("{raw}+00:00"))));
            return Ok(Some(format!(
                "arrow_cast({quoted}, 'Timestamp(Microsecond, Some(\"UTC\"))')"
            )));
        }
        "literal_timestamputc" => {
            // Exasol's real wire name for a TSTZ literal. DataFusion keeps
            // declining it: TSTZ coercion against a naive `timestamp_us` column is
            // unverified (#242).
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_tstz_literal(value("value"))));
            }
            return Ok(None);
        }
        "column" => {
            let column = ColumnNode::parse(expr)
                .ok_or_else(|| UdfError::User("column node without a name".to_string()))?;
            let quoted = quote_ident(&column.upper_name());
            // `tableAlias` is set only for multi-table renders (join wrapper), to
            // disambiguate a name shared by two joined subqueries.
            return Ok(Some(match column.table_alias.filter(|a| !a.is_empty()) {
                Some(alias) => format!("{}.{}", quote_ident(alias), quoted),
                None => quoted,
            }));
        }
        _ => {}
    }

    if let Some(op) = binary_op(kind) {
        let left = render_expression_inner(value("left").unwrap_or(&Json::Null), dialect)?;
        let right = render_expression_inner(value("right").unwrap_or(&Json::Null), dialect)?;
        match (left, right) {
            (Some(l), Some(r)) => return Ok(Some(format!("({l} {op} {r})"))),
            _ => {
                return Err(UdfError::User(format!(
                    "binary predicate '{kind}' missing operand"
                )));
            }
        }
    }

    match kind {
        "predicate_and" => {
            render_junction(value("expressions"), " AND ", "TRUE", dialect).map(Some)
        }
        "predicate_or" => render_junction(value("expressions"), " OR ", "FALSE", dialect).map(Some),
        "predicate_not" => {
            let inner =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?
                    .ok_or_else(|| UdfError::User("predicate_not missing 'expression'".into()))?;
            Ok(Some(format!("(NOT {inner})")))
        }
        "predicate_is_null" => {
            let inner =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?
                    .ok_or_else(|| {
                        UdfError::User("predicate_is_null missing 'expression'".into())
                    })?;
            Ok(Some(format!("({inner} IS NULL)")))
        }
        "predicate_is_not_null" => {
            let inner =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?
                    .ok_or_else(|| {
                        UdfError::User("predicate_is_not_null missing 'expression'".into())
                    })?;
            Ok(Some(format!("({inner} IS NOT NULL)")))
        }
        "predicate_in_constlist" => {
            let target =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?
                    .ok_or_else(|| {
                        UdfError::User("predicate_in_constlist missing 'expression'".into())
                    })?;
            let mut rendered: Vec<String> = Vec::new();
            if let Some(Json::Array(args)) = value("arguments") {
                for arg in args {
                    if is_null_literal(arg) {
                        continue;
                    }
                    if let Some(r) = render_expression_inner(arg, dialect)? {
                        rendered.push(r);
                    }
                }
            }
            if rendered.is_empty() {
                Ok(Some("FALSE".into()))
            } else {
                Ok(Some(format!("({target} IN ({}))", rendered.join(", "))))
            }
        }
        "predicate_between" => {
            let target =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?;
            let low = render_expression_inner(value("left").unwrap_or(&Json::Null), dialect)?;
            let high = render_expression_inner(value("right").unwrap_or(&Json::Null), dialect)?;
            match (target, low, high) {
                (Some(t), Some(l), Some(h)) => Ok(Some(format!("({t} BETWEEN {l} AND {h})"))),
                _ => Err(UdfError::User(
                    "predicate_between requires expression/left/right".into(),
                )),
            }
        }
        "predicate_like" => {
            let left =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?;
            let pattern =
                render_expression_inner(value("pattern").unwrap_or(&Json::Null), dialect)?;
            match (left, pattern) {
                (Some(l), Some(p)) => {
                    let escape = value("escape_char").and_then(|e| e.as_str());
                    if !is_empty_opt(escape) {
                        Ok(Some(format!(
                            "({l} LIKE {p} ESCAPE {})",
                            quote_literal(value("escape_char"))
                        )))
                    } else {
                        Ok(Some(format!("({l} LIKE {p})")))
                    }
                }
                _ => Err(UdfError::User(
                    "predicate_like missing 'expression' or 'pattern'".into(),
                )),
            }
        }
        "function_scalar_extract" => {
            let field = value("toExtract")
                .and_then(|f| f.as_str())
                .ok_or_else(|| {
                    UdfError::User("function_scalar_extract missing 'toExtract'".into())
                })?
                .to_uppercase();
            let args = value("arguments")
                .and_then(|a| a.as_array())
                .ok_or_else(|| {
                    UdfError::User("function_scalar_extract missing 'arguments'".into())
                })?;
            if args.is_empty() {
                return Err(UdfError::User(
                    "function_scalar_extract requires 1 argument".into(),
                ));
            }
            let src = render_expression_inner(&args[0], dialect)?
                .ok_or_else(|| UdfError::User("EXTRACT source is null".into()))?;
            Ok(Some(match dialect {
                Dialect::Exasol => format!("EXTRACT({field} FROM {src})"),
                Dialect::DataFusion if field == "SECOND" => {
                    return Err(UdfError::User(UNTYPED_SECOND.into()));
                }
                // DataFusion has no EXTRACT(field FROM expr) planner by default.
                Dialect::DataFusion => format!("date_part('{field}', {src})"),
            }))
        }
        // `basis` present means simple CASE; `results` holds one THEN per WHEN plus
        // an optional trailing ELSE.
        "function_scalar_case" => {
            let whens = value("arguments")
                .and_then(|a| a.as_array())
                .ok_or_else(|| UdfError::User("function_scalar_case missing 'arguments'".into()))?;
            let results = value("results")
                .and_then(|r| r.as_array())
                .ok_or_else(|| UdfError::User("function_scalar_case missing 'results'".into()))?;
            if results.len() != whens.len() && results.len() != whens.len() + 1 {
                return Err(UdfError::User(format!(
                    "function_scalar_case results ({}) must equal WHEN count ({}) or +1 for ELSE",
                    results.len(),
                    whens.len()
                )));
            }
            let basis = match value("basis") {
                Some(b) if !b.is_null() => Some(
                    render_expression_inner(b, dialect)?
                        .ok_or_else(|| UdfError::User("CASE basis is null".into()))?,
                ),
                _ => None,
            };
            let mut sql = String::from("CASE");
            if let Some(b) = &basis {
                sql.push(' ');
                sql.push_str(b);
            }
            for (i, when) in whens.iter().enumerate() {
                let when_sql = render_expression_inner(when, dialect)?
                    .ok_or_else(|| UdfError::User("CASE WHEN value is null".into()))?;
                let then_sql = render_expression_inner(&results[i], dialect)?
                    .ok_or_else(|| UdfError::User("CASE THEN result is null".into()))?;
                sql.push_str(&format!(" WHEN {when_sql} THEN {then_sql}"));
            }
            if results.len() == whens.len() + 1 {
                let else_sql = render_expression_inner(&results[whens.len()], dialect)?
                    .ok_or_else(|| UdfError::User("CASE ELSE result is null".into()))?;
                sql.push_str(&format!(" ELSE {else_sql}"));
            }
            sql.push_str(" END");
            Ok(Some(format!("({sql})")))
        }
        "function_scalar_json_value" => match dialect {
            Dialect::Exasol => render_json_value_exasol(expr),
            Dialect::DataFusion => render_json_value_scan(expr),
        }
        .map(Some),
        // The CAST wire shape Exasol sends; the nested `function_scalar` CAST arm
        // is a defensive alternate encoding.
        "function_scalar_cast" => {
            let args = value("arguments").and_then(|a| a.as_array());
            render_cast(args, value("dataType"), dialect)
        }
        // Synthesized by the typed scan pass, DataFusion dialect only.
        INTEGER_TEXT_NODE | BOOLEAN_TEXT_NODE | BOOLEAN_NUMBER_NODE | SECOND_NODE
            if dialect == Dialect::DataFusion =>
        {
            let arg = value("arguments")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .ok_or_else(|| UdfError::User(format!("{kind} missing its argument")))?;
            let inner = render_expression_inner(arg, dialect)?
                .ok_or_else(|| UdfError::User(format!("{kind} argument is null")))?;
            Ok(Some(match kind {
                INTEGER_TEXT_NODE => format!("CAST(CAST({inner} AS DECIMAL(38,0)) AS VARCHAR)"),
                BOOLEAN_TEXT_NODE => render_bool_to_string_case(&inner),
                SECOND_NODE => {
                    let fraction = second_fraction(expr)
                        .ok_or_else(|| UdfError::User(format!("{kind} missing its fraction")))?;
                    render_seconds(&inner, fraction)?
                }
                _ => format!("(CASE {inner} WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)"),
            }))
        }
        "function_scalar" => {
            let fn_name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            let args = value("arguments").and_then(|a| a.as_array());

            // An Exasol-dialect `VerbatimCall` and a DataFusion call render here, ahead
            // of the per-name arms, so arm order carries no dialect precedence.
            let Some(function) = scalar_function(&fn_name) else {
                return Err(UdfError::User(format!(
                    "unsupported scalar function: {fn_name}"
                )));
            };
            let verbatim = match dialect {
                Dialect::Exasol => {
                    (function.exasol == ExasolForm::VerbatimCall).then_some(fn_name.as_str())
                }
                Dialect::DataFusion => function.datafusion_call,
            };
            if let Some(call_name) = verbatim {
                let args = args.ok_or_else(|| {
                    UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                })?;
                let rendered = render_args(args, dialect)?;
                return Ok(Some(format!("{call_name}({})", rendered.join(", "))));
            }

            match fn_name.as_str() {
                // Node names equal the capability names minus `FN_` and must stay
                // in lockstep with capabilities.rs (multiplication is `MULT`, not `MUL`).
                "ADD" | "SUB" | "MULT" | "FLOAT_DIV" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() < 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let left = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} left operand is null")))?;
                    let right = render_expression_inner(&args[1], dialect)?.ok_or_else(|| {
                        UdfError::User(format!("{fn_name} right operand is null"))
                    })?;
                    if fn_name == "FLOAT_DIV" && dialect == Dialect::DataFusion {
                        return Ok(Some(format!("{CHECKED_FLOAT_DIV_FN}({left}, {right})")));
                    }
                    let op = match fn_name.as_str() {
                        "ADD" => "+",
                        "SUB" => "-",
                        "MULT" => "*",
                        "FLOAT_DIV" => "/",
                        _ => unreachable!(),
                    };
                    Ok(Some(format!("({left} {op} {right})")))
                }
                "NEG" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar NEG missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar NEG requires 1 argument".into(),
                        ));
                    }
                    let operand = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("NEG operand is null".into()))?;
                    Ok(Some(format!("(-{operand})")))
                }
                "CAST" => render_cast(args, value("dataType"), dialect),
                "ABS" | "FLOOR" | "CEIL" | "SQRT" | "EXP" | "LN" | "SIGN" | "DEGREES"
                | "RADIANS" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "SINH"
                | "COSH" | "TANH" | "COT" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() != 1 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 1 argument, got {}",
                            args.len()
                        )));
                    }
                    let lower;
                    let df_name = match fn_name.as_str() {
                        "SIGN" => "signum",
                        other => {
                            lower = other.to_lowercase();
                            &lower
                        }
                    };
                    let arg = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} argument is null")))?;
                    Ok(Some(format!("{df_name}({arg})")))
                }
                "ROUND" | "TRUNC" | "LOG" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.is_empty() || args.len() > 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 1 or 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let df_name = fn_name.to_lowercase();
                    let rendered = render_args(args, dialect)?;
                    Ok(Some(format!("{df_name}({})", rendered.join(", "))))
                }
                "POWER" | "ATAN2" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let df_name = fn_name.to_lowercase();
                    let rendered = render_args(args, dialect)?;
                    Ok(Some(format!("{df_name}({})", rendered.join(", "))))
                }
                "MOD" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar MOD missing 'arguments'".into())
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar MOD requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let left = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("MOD left operand is null".into()))?;
                    let right = render_expression_inner(&args[1], dialect)?
                        .ok_or_else(|| UdfError::User("MOD right operand is null".into()))?;
                    Ok(Some(match dialect {
                        Dialect::Exasol => format!("MOD({left}, {right})"),
                        Dialect::DataFusion => format!("({left} % {right})"),
                    }))
                }
                // DataFusion's `concat` never collapses an all-empty result to NULL
                // as Exasol's `||` does.
                "CONCAT" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar CONCAT missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar CONCAT requires at least 1 argument, got 0".into(),
                        ));
                    }
                    let rendered = args
                        .iter()
                        .map(|arg| {
                            render_expression_inner(arg, dialect)?.ok_or_else(|| {
                                UdfError::User("CONCAT argument rendered to null".into())
                            })
                        })
                        .collect::<Result<Vec<String>, UdfError>>()?;
                    Ok(Some(match dialect {
                        Dialect::Exasol => format!("({})", rendered.join(" || ")),
                        Dialect::DataFusion => {
                            format!("nullif(concat({}), '')", rendered.join(", "))
                        }
                    }))
                }
                // DataFusion dialect only: INSTR(s, sub) and LOCATE(sub, s) both map to
                // strpos(s, sub), which takes no start or occurrence argument (#228).
                "INSTR" | "LOCATE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} renders exactly 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let (string, substr) = if fn_name == "INSTR" { (0, 1) } else { (1, 0) };
                    let string = render_expression_inner(&args[string], dialect)?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} string arg is null")))?;
                    let substr =
                        render_expression_inner(&args[substr], dialect)?.ok_or_else(|| {
                            UdfError::User(format!("{fn_name} substring arg is null"))
                        })?;
                    Ok(Some(format!("strpos({string}, {substr})")))
                }
                // Arguments interleave [cond, result, ...], with a trailing ELSE on an
                // odd count.
                "CASE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar CASE missing 'arguments'".into())
                    })?;
                    if args.len() < 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar CASE requires at least one WHEN branch (2 arguments), got {}",
                            args.len()
                        )));
                    }
                    let mut sql = "CASE".to_string();
                    let has_else = args.len() % 2 == 1;
                    let branch_end = if has_else { args.len() - 1 } else { args.len() };
                    let mut i = 0;
                    while i < branch_end {
                        let cond =
                            render_expression_inner(&args[i], dialect)?.ok_or_else(|| {
                                UdfError::User(format!("CASE WHEN cond[{i}] is null"))
                            })?;
                        let result =
                            render_expression_inner(&args[i + 1], dialect)?.ok_or_else(|| {
                                UdfError::User(format!("CASE THEN result[{}] is null", i + 1))
                            })?;
                        sql.push_str(&format!(" WHEN {cond} THEN {result}"));
                        i += 2;
                    }
                    if has_else {
                        let else_val = render_expression_inner(&args[args.len() - 1], dialect)?
                            .ok_or_else(|| UdfError::User("CASE ELSE value is null".into()))?;
                        sql.push_str(&format!(" ELSE {else_val}"));
                    }
                    sql.push_str(" END");
                    Ok(Some(sql))
                }
                "GREATEST" | "LEAST" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires at least 1 argument"
                        )));
                    }
                    let df_name = fn_name.to_lowercase();
                    let rendered = render_args(args, dialect)?;
                    let guard = rendered
                        .iter()
                        .map(|a| format!("{a} IS NULL"))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    Ok(Some(format!(
                        "CASE WHEN {guard} THEN NULL ELSE {df_name}({}) END",
                        rendered.join(", ")
                    )))
                }
                "NULLIFZERO" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar NULLIFZERO missing 'arguments'".into())
                    })?;
                    if args.len() != 1 {
                        return Err(UdfError::User(format!(
                            "function_scalar NULLIFZERO requires 1 argument, got {}",
                            args.len()
                        )));
                    }
                    let arg = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("NULLIFZERO argument is null".into()))?;
                    Ok(Some(format!("nullif({arg}, 0)")))
                }
                "ZEROIFNULL" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar ZEROIFNULL missing 'arguments'".into())
                    })?;
                    if args.len() != 1 {
                        return Err(UdfError::User(format!(
                            "function_scalar ZEROIFNULL requires 1 argument, got {}",
                            args.len()
                        )));
                    }
                    let arg = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("ZEROIFNULL argument is null".into()))?;
                    Ok(Some(format!("coalesce({arg}, 0)")))
                }
                "NULLIF" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar NULLIF missing 'arguments'".into())
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar NULLIF requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let left = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("NULLIF first argument is null".into()))?;
                    let right = render_expression_inner(&args[1], dialect)?
                        .ok_or_else(|| UdfError::User("NULLIF second argument is null".into()))?;
                    Ok(Some(format!("nullif({left}, {right})")))
                }
                "SECOND" => Err(UdfError::User(UNTYPED_SECOND.into())),
                "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() != 1 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 1 argument, got {}",
                            args.len()
                        )));
                    }
                    let src = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} argument is null")))?;
                    Ok(Some(format!("date_part('{fn_name}', {src})")))
                }
                // Exasol WEEK and DataFusion `date_part('week')` are both ISO-8601, so
                // year-boundary weeks agree.
                "WEEK" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar WEEK missing 'arguments'".into())
                    })?;
                    if args.len() != 1 {
                        return Err(UdfError::User(format!(
                            "function_scalar WEEK requires 1 argument, got {}",
                            args.len()
                        )));
                    }
                    let src = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("WEEK argument is null".into()))?;
                    Ok(Some(format!("date_part('week', {src})")))
                }
                "DATE_TRUNC" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar DATE_TRUNC missing 'arguments'".into())
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar DATE_TRUNC requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let unit = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("DATE_TRUNC unit is null".into()))?;
                    let src = render_expression_inner(&args[1], dialect)?
                        .ok_or_else(|| UdfError::User("DATE_TRUNC source is null".into()))?;
                    Ok(Some(format!("date_trunc({unit}, {src})")))
                }
                "TO_DATE" => render_to_datetime(FormatTarget::Date, args, dialect),
                "TO_TIMESTAMP" => render_to_datetime(FormatTarget::Timestamp, args, dialect),
                // ADD_HOURS/ADD_MINUTES are not translated: Exasol types ADD_HOURS(DATE, n) as
                // TIMESTAMP(0), and the typed pass does not tell a DATE from a TIMESTAMP.
                //
                // DATE - DATE yields an Int64 day count in DataFusion; Exasol uses only
                // the date part of a timestamp.
                "DAYS_BETWEEN" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar DAYS_BETWEEN missing 'arguments'".into())
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar DAYS_BETWEEN requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let first = render_expression_inner(&args[0], dialect)?.ok_or_else(|| {
                        UdfError::User("DAYS_BETWEEN first argument is null".into())
                    })?;
                    let second = render_expression_inner(&args[1], dialect)?.ok_or_else(|| {
                        UdfError::User("DAYS_BETWEEN second argument is null".into())
                    })?;
                    Ok(Some(format!(
                        "(CAST({first} AS DATE) - CAST({second} AS DATE))"
                    )))
                }
                "HOURS_BETWEEN" | "MINUTES_BETWEEN" | "SECONDS_BETWEEN" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() != 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let first = render_expression_inner(&args[0], dialect)?.ok_or_else(|| {
                        UdfError::User(format!("{fn_name} first argument is null"))
                    })?;
                    let second = render_expression_inner(&args[1], dialect)?.ok_or_else(|| {
                        UdfError::User(format!("{fn_name} second argument is null"))
                    })?;
                    let diff =
                        format!("(date_part('epoch', {first}) - date_part('epoch', {second}))");
                    Ok(Some(match fn_name.as_str() {
                        "HOURS_BETWEEN" => format!("({diff} / 3600)"),
                        "MINUTES_BETWEEN" => format!("({diff} / 60)"),
                        "SECONDS_BETWEEN" => diff,
                        _ => unreachable!(),
                    }))
                }
                // Backstop: reachable only if a declared name loses its per-name arm.
                other => Err(UdfError::User(format!(
                    "unsupported scalar function: {other}"
                ))),
            }
        }
        // The aggregate name Exasol pushed is valid in both engines, so it is spliced
        // verbatim; no arguments means `<NAME>(*)`.
        "function_aggregate" => {
            let name = value("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| UdfError::User("function_aggregate missing 'name'".into()))?
                .to_uppercase();
            match value("arguments").and_then(|a| a.as_array()) {
                None => Ok(Some(format!("{name}(*)"))),
                Some(args) if args.is_empty() => Ok(Some(format!("{name}(*)"))),
                Some(args) => {
                    let distinct = value("distinct").and_then(|d| d.as_bool()) == Some(true);
                    let distinct_kw = if distinct { "DISTINCT " } else { "" };
                    let rendered = render_args(args, dialect)?;
                    Ok(Some(format!(
                        "{name}({distinct_kw}{})",
                        rendered.join(", ")
                    )))
                }
            }
        }
        other => Err(UdfError::User(format!(
            "unsupported expression node type: {other}"
        ))),
    }
}

fn render_junction(
    expressions: Option<&Json>,
    op: &str,
    empty_value: &str,
    dialect: Dialect,
) -> Result<String, UdfError> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(Json::Array(items)) = expressions {
        for expr in items {
            let rendered = render_expression_inner(expr, dialect)?;
            if let Some(r) = rendered
                && !r.is_empty()
            {
                parts.push(r);
            }
        }
    }
    Ok(join_junction(parts, op, empty_value))
}

fn join_junction(mut parts: Vec<String>, op: &str, empty_value: &str) -> String {
    match parts.len() {
        0 => empty_value.to_string(),
        1 => parts.remove(0),
        _ => format!("({})", parts.join(op)),
    }
}

/// The only DataFusion-dialect entry point, so every scan expression is typed first. `Err` declines.
/// A CHAR or VARCHAR CAST renders unsized, so its fragment type is the widest VARCHAR.
pub fn render_scan_expression(
    expr: &Json,
    columns: &dyn ColumnTypeLookup,
) -> Result<SqlFragment, UdfError> {
    let (typed, _) = scan_typing::type_scan_tree(expr, columns)?;
    let ty = exact_expression_type(expr, columns).map(|ty| {
        if ty.is_character() {
            ExasolType::WIDEST_VARCHAR
        } else {
            ty
        }
    });
    Ok(SqlFragment::new(render_expression(&typed)?, ty))
}

/// [`render_scan_expression`] for a filter. Exasol drops a `WHERE` that folds to `TRUE`, so no
/// filter it sends renders as `TRUE`.
pub fn render_scan_filter(
    filter_expr: &Json,
    columns: &dyn ColumnTypeLookup,
) -> Result<String, UdfError> {
    Ok(null_keeps_no_row(
        render_scan_expression(filter_expr, columns)?.sql,
    ))
}

/// The filter [`render_scan_filter`] renders for the `AND` of conjuncts rendered one by one.
pub fn conjoin_scan_filters(conjuncts: Vec<String>) -> Option<String> {
    if conjuncts.is_empty() {
        return None;
    }
    Some(null_keeps_no_row(join_junction(conjuncts, " AND ", "TRUE")))
}

/// A `NULL` condition keeps no row, like `FALSE`. Exasol folds `''` to `NULL` too.
fn null_keeps_no_row(sql: String) -> String {
    if sql == "NULL" { "FALSE".into() } else { sql }
}

/// DataFusion dialect, untyped. Use [`render_scan_expression`].
pub(crate) fn render_expression(expr: &Json) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::DataFusion)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// Exasol dialect, for SQL parsed by Exasol itself (join and grouped-aggregate
/// wrappers), never for a DataFusion `ScanSpec`. Character CAST targets are
/// length-qualified, since Exasol has no length-less character type.
///
/// Panics on a tree Exasol does not send: Exasol pushes only the capabilities the adapter
/// advertises, and this dialect renders every one of them.
pub fn render_expression_exasol(expr: &Json) -> String {
    match render_expression_inner(expr, Dialect::Exasol) {
        Ok(Some(sql)) => sql,
        Ok(None) => panic!("{EXASOL_RENDERS}: a null node"),
        Err(err) => panic!("{EXASOL_RENDERS}: {err}"),
    }
}

const EXASOL_RENDERS: &str = "the Exasol dialect renders every advertised capability";

/// [`render_expression_exasol`] for an output select item, typed where the request states the
/// type: the Exasol dialect renders a CAST's target and a column as they are.
pub fn render_exasol_fragment(expr: &Json, columns: &dyn ColumnTypeLookup) -> SqlFragment {
    SqlFragment::new(
        render_expression_exasol(expr),
        exact_expression_type(expr, columns),
    )
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(any(test, feature = "test-support"))]
#[path = "test_support_tests.rs"]
pub mod test_support;

#[cfg(any(test, feature = "test-support"))]
#[path = "udf_column_test_support_tests.rs"]
pub mod udf_column_test_support;
