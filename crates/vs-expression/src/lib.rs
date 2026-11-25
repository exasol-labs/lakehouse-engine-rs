use exasol_udf_sdk::error::UdfError;

mod datetime_format;
pub mod exasol_type;
mod expr;
mod fragment;
mod json_value;
mod parse;
mod scalar_functions;
mod scan_typing;
pub mod timestamp;
mod version;
use datetime_format::{FormatTarget, strftime_pattern};
use exasol_type::ExasolType;
use expr::ScanKind;
pub use expr::{
    AggregateCall, CaseExpr, Column, CompareOp, Expr, JsonBehavior, JsonValueCall, Literal,
    LiteralKind, Scalar, ScalarCall, ScanNode,
};
pub use fragment::SqlFragment;
use json_value::{Fallback, Returning, ScanJsonValue};
pub use json_value::{JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path};
pub use parse::{RequestNode, invalid_field, parse_expression};
use scalar_functions::{ExasolForm, scalar_function};
pub use scan_typing::{ColumnTypeLookup, ScanTyping, is_session_free_integer_text};
use scan_typing::{Inferred, empty_as_null, render_integer_argument, seconds_type};
use timestamp::{TimestampLiteral, TimestampUnit};
pub use version::ExasolVersion;

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

/// DataFusion dialect only: Exasol's `TRUE`/`FALSE` casing instead of DataFusion's lowercase;
/// NULL stays NULL rather than becoming `'NULL'` or `'FALSE'`.
fn render_bool_to_string_case(bool_expr: &str) -> String {
    format!("(CASE {bool_expr} WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)")
}

fn quote_literal(value: &Scalar) -> String {
    match value {
        Scalar::Null => "NULL".to_string(),
        other => format!("'{}'", sql_escape(&other.text())),
    }
}

/// DataFusion's `LIKE` accepts only `\` as its escape (apache/datafusion#13291), so a pattern written
/// with another escape is rewritten to it. `None` unless both are string literals and the escape is
/// one character that never ends the pattern.
fn backslash_escaped_pattern(pattern: &Expr, escape: &Expr) -> Option<String> {
    let mut escape_chars = escape.as_string_literal()?.chars();
    let escape = escape_chars.next()?;
    if escape_chars.next().is_some() {
        return None;
    }
    let mut rewritten = String::new();
    let mut chars = pattern.as_string_literal()?.chars();
    while let Some(c) = chars.next() {
        match c {
            c if c == escape => {
                rewritten.push('\\');
                rewritten.push(chars.next()?);
            }
            '\\' => rewritten.push_str("\\\\"),
            c => rewritten.push(c),
        }
    }
    Some(rewritten)
}

/// Shared by `literal_timestamp` and `literal_timestamp_utc` so they cannot drift.
/// Exasol's literal format has no offset field (22018 on `+00:00`), and
/// `TIMESTAMP NULL` is a syntax error (42000), so NULL renders bare.
fn render_exasol_timestamp_literal(value: &Scalar) -> String {
    match value {
        Scalar::Null => "NULL".to_string(),
        v => format!("TIMESTAMP {}", quote_literal(v)),
    }
}

fn timestamp_literal_unit(value: &Scalar) -> &'static str {
    match value.as_str().map(TimestampLiteral) {
        Some(literal) if !literal.fits(TimestampUnit::Microsecond) => "Nanosecond",
        _ => "Microsecond",
    }
}

/// Converting the UTC-normalized wire value into `SESSIONTIMEZONE` reproduces
/// Exasol's own TSTZ-vs-TIMESTAMP coercion, which reads the naive side as
/// session-local (verified live). A bare TSTZ cast keeps 3 fractional digits, so a longer value
/// names its precision; Exasol 8 never sends more than 3 and has no precision syntax.
fn render_exasol_tstz_literal(value: &Scalar) -> String {
    match value {
        Scalar::Null => "NULL".to_string(),
        v => {
            let digits = v
                .as_str()
                .map_or(0, |s| TimestampLiteral(s).type_precision());
            let cast_type = ExasolType::TimestampWithLocalTimeZone {
                precision: u8::try_from(digits.max(3)).unwrap_or(u8::MAX),
            };
            format!(
                "CAST(CONVERT_TZ({}, 'UTC', SESSIONTIMEZONE) AS {cast_type})",
                render_exasol_timestamp_literal(value)
            )
        }
    }
}

/// Exasol's `IN`/`NOT IN` ignores NULL list entries, while DataFusion's
/// three-valued logic would empty the result.
fn is_null_literal(arg: &Expr) -> bool {
    arg.as_literal().is_some_and(Literal::is_null)
}

fn render_args(args: &[Expr], dialect: Dialect) -> Result<Vec<String>, UdfError> {
    args.iter()
        .enumerate()
        .map(|(i, arg)| {
            render_expression_inner(arg, dialect)?
                .ok_or_else(|| UdfError::User(format!("argument[{i}] rendered to null")))
        })
        .collect()
}

/// Renders only a `literal_string` format the translation covers, and errs otherwise so the
/// caller declines. `digits` are the fractional digits a `TO_TIMESTAMP` keeps, which only the
/// typed pass knows.
fn render_to_datetime(
    target: FormatTarget,
    args: &[Expr],
    digits: Option<u8>,
    dialect: Dialect,
) -> Result<Option<String>, UdfError> {
    let node = match target {
        FormatTarget::Date => "TO_DATE",
        FormatTarget::Timestamp => "TO_TIMESTAMP",
    };
    match args {
        [] => Err(UdfError::User(format!(
            "function_scalar {node} requires at least 1 argument"
        ))),
        [_] => Err(UdfError::User(format!(
            "function_scalar {node} without a format parses by the session's NLS settings"
        ))),
        [source, format] => {
            let pattern = format
                .as_string_literal()
                .and_then(|model| strftime_pattern(model, target))
                .ok_or_else(|| {
                    UdfError::User(format!(
                        "function_scalar {node} format {format:?} is not a string literal with an exact strftime equivalent"
                    ))
                })?;
            let source = render_conversion_source(node, source, dialect)?;
            let pattern = sql_escape(&pattern);
            Ok(Some(match target {
                FormatTarget::Date => format!("to_date({source}, '{pattern}')"),
                FormatTarget::Timestamp => {
                    let digits = digits.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {node} carries no typed digits"))
                    })?;
                    format!("{TO_TIMESTAMP_FN}({source}, '{pattern}', {digits})")
                }
            }))
        }
        _ => Err(UdfError::User(format!(
            "function_scalar {node} accepts at most 2 arguments, got {}",
            args.len()
        ))),
    }
}

fn render_conversion_source(
    function: &str,
    source: &Expr,
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
        // An Arrow string carries no length, and Arrow has no fixed-width CHAR type.
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

/// The scalar UDFs the DataFusion dialect renders `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and
/// `ADD_MINUTES` as, registered by `lakehouse-engine`. Each MUST take a first argument of `Date32`,
/// `Timestamp(Microsecond, None)`, or `Timestamp(Nanosecond, None)`, which the typed pass casts
/// to, and a number as its second.
pub const ADD_DAYS_FN: &str = "vs_add_days";
/// See [`ADD_DAYS_FN`].
pub const ADD_WEEKS_FN: &str = "vs_add_weeks";
/// See [`ADD_DAYS_FN`].
pub const ADD_HOURS_FN: &str = "vs_add_hours";
/// See [`ADD_DAYS_FN`].
pub const ADD_MINUTES_FN: &str = "vs_add_minutes";

/// The scalar UDF the DataFusion dialect renders `TO_TIMESTAMP(text, format)` as, registered by
/// `lakehouse-engine`. It takes the text, a strftime pattern, and the fractional digits to keep,
/// and returns `Timestamp(Microsecond, None)` for up to 6 digits, else nanoseconds.
pub const TO_TIMESTAMP_FN: &str = "vs_to_timestamp";

fn render_cast(
    argument: &Expr,
    target: ExasolType,
    dialect: Dialect,
) -> Result<Option<String>, UdfError> {
    let inner = render_expression_inner(argument, dialect)?
        .ok_or_else(|| UdfError::User("CAST expression is null".into()))?;
    let target_type = render_cast_target(target, dialect)?;
    Ok(Some(format!("CAST({inner} AS {target_type})")))
}

fn json_value_operand(node: &Expr, dialect: Dialect) -> Result<String, UdfError> {
    render_expression_inner(node, dialect)?
        .ok_or_else(|| UdfError::User("JSON_VALUE operand is null".into()))
}

/// Every clause Exasol sent, so Exasol computes the forms the scan declines.
fn render_json_value_exasol(call: &JsonValueCall) -> Result<String, UdfError> {
    let operand = |node: &Expr| json_value_operand(node, Dialect::Exasol);
    let mut sql = format!(
        "JSON_VALUE({}, {}",
        operand(&call.document)?,
        operand(&call.path)?
    );
    if let Some(data_type) = call.returning {
        sql.push_str(&format!(" RETURNING {data_type}"));
    }
    for (behavior, clause) in [(&call.on_empty, "EMPTY"), (&call.on_error, "ERROR")] {
        let Some(behavior) = behavior else {
            continue;
        };
        let rendered = match behavior {
            JsonBehavior::Default(expression) => format!("DEFAULT {}", operand(expression)?),
            JsonBehavior::Null => "NULL".to_string(),
            JsonBehavior::Error => "ERROR".to_string(),
        };
        sql.push_str(&format!(" {rendered} ON {clause}"));
    }
    Ok(sql + ")")
}

fn render_json_value_scan(call: &JsonValueCall) -> Result<String, UdfError> {
    let call = ScanJsonValue::of(call)?;
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

fn render_literal(literal: &Literal, dialect: Dialect) -> Result<Option<String>, UdfError> {
    let value = &literal.value;
    Ok(Some(match (literal.kind, dialect) {
        (LiteralKind::Null, _) => "NULL".into(),
        (LiteralKind::Bool, _) => if literal.is_true() { "TRUE" } else { "FALSE" }.into(),
        (LiteralKind::String, _) => quote_literal(value),
        // The scan parses bare numeric literals as DECIMAL, which rejects `1e+300`.
        (LiteralKind::Double, Dialect::DataFusion) => match value {
            Scalar::Null => "NULL".to_string(),
            v => format!("CAST({} AS DOUBLE)", quote_literal(v)),
        },
        (LiteralKind::ExactNumeric | LiteralKind::Double, _) => match value {
            Scalar::Null => "NULL".to_string(),
            v => v.text(),
        },
        (LiteralKind::Date, _) => format!("DATE {}", quote_literal(value)),
        // A `width` types it `FixedSizeBinary`, so comparing it leaves a fixed-length column
        // uncast.
        (LiteralKind::Binary { width }, Dialect::DataFusion) => {
            let hex = value.as_str().unwrap_or_default();
            if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(UdfError::User(format!(
                    "literal_binary value '{hex}' is not hexadecimal"
                )));
            }
            match width {
                Some(width) => format!("arrow_cast(X'{hex}', 'FixedSizeBinary({width})')"),
                None => format!("X'{hex}'"),
            }
        }
        (LiteralKind::Binary { .. }, Dialect::Exasol) => {
            return Err(UdfError::User(
                "unsupported expression node type: literal_binary".into(),
            ));
        }
        // Exasol has no `arrow_cast` (42000); re-emit the bare literal it sent.
        (LiteralKind::Timestamp, Dialect::Exasol) => render_exasol_timestamp_literal(value),
        // Explicit precision: DataFusion types a bare `TIMESTAMP` literal as nanoseconds, which
        // overflows on far-future values when unified with microsecond columns, so only
        // sub-microsecond digits get nanoseconds. A NULL value renders `arrow_cast(NULL, ...)`,
        // deliberately unlike `literal_timestamp_utc`.
        (LiteralKind::Timestamp, Dialect::DataFusion) => format!(
            "arrow_cast({}, 'Timestamp({}, None)')",
            quote_literal(value),
            timestamp_literal_unit(value)
        ),
        (LiteralKind::TimestampUtc | LiteralKind::LocalTimeZoneTimestamp, Dialect::Exasol) => {
            render_exasol_tstz_literal(value)
        }
        // `+00:00` parses the value as UTC; the "UTC" tz label matches the scan's Timestamptz
        // Arrow mapping, avoiding a tz-label mismatch.
        (LiteralKind::TimestampUtc, Dialect::DataFusion) => {
            let raw = match value {
                Scalar::Null => return Ok(Some("NULL".into())),
                v => v.text(),
            };
            let quoted = quote_literal(&Scalar::String(format!("{raw}+00:00")));
            format!("arrow_cast({quoted}, 'Timestamp(Microsecond, Some(\"UTC\"))')")
        }
        // Exasol's real wire name for a TSTZ literal. Its comparison depends on the session zone,
        // so the adapter folds a compared one into a plain timestamp literal and the DataFusion
        // dialect declines any left over.
        (LiteralKind::LocalTimeZoneTimestamp, Dialect::DataFusion) => return Ok(None),
    }))
}

fn required(
    node: &Expr,
    dialect: Dialect,
    what: impl FnOnce() -> String,
) -> Result<String, UdfError> {
    render_expression_inner(node, dialect)?.ok_or_else(|| UdfError::User(what()))
}

/// `Ok(None)` only for a node the dialect renders as no value: a TSTZ literal in the DataFusion
/// dialect.
fn render_expression_inner(expr: &Expr, dialect: Dialect) -> Result<Option<String>, UdfError> {
    let sql = match expr {
        Expr::Literal(literal) => return render_literal(literal, dialect),
        Expr::Column(column) => {
            let quoted = quote_ident(&column.upper_name());
            // `tableAlias` is set only for multi-table renders (join wrapper), to
            // disambiguate a name shared by two joined subqueries.
            match column.table_alias.as_deref().filter(|a| !a.is_empty()) {
                Some(alias) => format!("{}.{}", quote_ident(alias), quoted),
                None => quoted,
            }
        }
        Expr::Compare { op, left, right } => {
            let missing = || format!("binary predicate '{}' missing operand", op.sql());
            let left = required(left, dialect, missing)?;
            let right = required(right, dialect, missing)?;
            format!("({left} {} {right})", op.sql())
        }
        Expr::And(items) => render_junction(items, " AND ", "TRUE", dialect)?,
        Expr::Or(items) => render_junction(items, " OR ", "FALSE", dialect)?,
        Expr::Not(inner) => {
            let inner = required(inner, dialect, || {
                "predicate_not missing 'expression'".into()
            })?;
            format!("(NOT {inner})")
        }
        Expr::IsNull(inner) => {
            let inner = required(inner, dialect, || {
                "predicate_is_null missing 'expression'".into()
            })?;
            format!("({inner} IS NULL)")
        }
        Expr::IsNotNull(inner) => {
            let inner = required(inner, dialect, || {
                "predicate_is_not_null missing 'expression'".into()
            })?;
            format!("({inner} IS NOT NULL)")
        }
        Expr::InList { expression, values } => {
            let target = required(expression, dialect, || {
                "predicate_in_constlist missing 'expression'".into()
            })?;
            let mut rendered: Vec<String> = Vec::new();
            for value in values {
                if is_null_literal(value) {
                    continue;
                }
                if let Some(r) = render_expression_inner(value, dialect)? {
                    rendered.push(r);
                }
            }
            if rendered.is_empty() {
                "FALSE".into()
            } else {
                format!("({target} IN ({}))", rendered.join(", "))
            }
        }
        Expr::Between {
            expression,
            low,
            high,
        } => {
            let missing = || "predicate_between requires expression/left/right".to_string();
            let target = required(expression, dialect, missing)?;
            let low = required(low, dialect, missing)?;
            let high = required(high, dialect, missing)?;
            format!("({target} BETWEEN {low} AND {high})")
        }
        Expr::Like {
            expression,
            pattern,
            escape,
        } => {
            let missing = || "predicate_like missing 'expression' or 'pattern'".to_string();
            let l = required(expression, dialect, missing)?;
            let p = required(pattern, dialect, missing)?;
            match escape {
                None => format!("({l} LIKE {p})"),
                Some(escape) if dialect == Dialect::Exasol => {
                    let escape = required(escape, dialect, || {
                        "predicate_like with a null 'escapeChar'".into()
                    })?;
                    format!("({l} LIKE {p} ESCAPE {escape})")
                }
                Some(escape) => {
                    let rewritten =
                        backslash_escaped_pattern(pattern, escape).ok_or_else(|| {
                            UdfError::User(
                                "LIKE ESCAPE needs a string pattern and a one-character escape"
                                    .into(),
                            )
                        })?;
                    format!("({l} LIKE {})", quote_literal(&Scalar::String(rewritten)))
                }
            }
        }
        Expr::Extract { field, argument } => {
            let field = field.to_uppercase();
            let src = required(argument, dialect, || "EXTRACT source is null".into())?;
            match dialect {
                Dialect::Exasol => format!("EXTRACT({field} FROM {src})"),
                Dialect::DataFusion if field == "SECOND" => {
                    return Err(UdfError::User(UNTYPED_SECOND.into()));
                }
                // DataFusion has no EXTRACT(field FROM expr) planner by default.
                Dialect::DataFusion => format!("date_part('{field}', {src})"),
            }
        }
        Expr::Case(case) => render_case(case, dialect)?,
        Expr::JsonValue(call) => match dialect {
            Dialect::Exasol => render_json_value_exasol(call)?,
            Dialect::DataFusion => render_json_value_scan(call)?,
        },
        Expr::Cast { target, argument } => return render_cast(argument, *target, dialect),
        Expr::Scan(ScanNode { kind, argument }) if dialect == Dialect::DataFusion => {
            let inner = required(argument, dialect, || {
                "a typed-pass node's argument is null".into()
            })?;
            match *kind {
                ScanKind::IntegerText => {
                    format!("CAST(CAST({inner} AS DECIMAL(38,0)) AS VARCHAR)")
                }
                ScanKind::BooleanText => render_bool_to_string_case(&inner),
                ScanKind::EmptyAsNull => format!("nullif({inner}, '')"),
                ScanKind::IntegerArgument {
                    round,
                    negative_is_null,
                } => render_integer_argument(round, negative_is_null, &inner),
                ScanKind::Second { fraction } => render_seconds(&inner, fraction)?,
                ScanKind::BooleanNumber => {
                    format!("(CASE {inner} WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)")
                }
            }
        }
        Expr::Scan(_) => {
            return Err(UdfError::User(
                "unsupported expression node type: a typed-pass node in the Exasol dialect".into(),
            ));
        }
        Expr::Scalar(call) => return render_scalar(call, dialect),
        // The aggregate name Exasol pushed is valid in both engines, so it is spliced
        // verbatim; no arguments means `<NAME>(*)`.
        Expr::Aggregate(call) => {
            let name = &call.name;
            if call.arguments.is_empty() {
                format!("{name}(*)")
            } else {
                let distinct_kw = if call.distinct { "DISTINCT " } else { "" };
                let rendered = render_args(&call.arguments, dialect)?;
                format!("{name}({distinct_kw}{})", rendered.join(", "))
            }
        }
    };
    Ok(Some(sql))
}

/// `basis` present means simple CASE; `results` holds one THEN per WHEN plus an optional trailing
/// ELSE.
fn render_case(case: &CaseExpr, dialect: Dialect) -> Result<String, UdfError> {
    let mut sql = String::from("CASE");
    if let Some(basis) = &case.basis {
        sql.push(' ');
        sql.push_str(&required(basis, dialect, || "CASE basis is null".into())?);
    }
    for (when, result) in case.whens.iter().zip(&case.results) {
        let when_sql = required(when, dialect, || "CASE WHEN value is null".into())?;
        let then_sql = required(result, dialect, || "CASE THEN result is null".into())?;
        sql.push_str(&format!(" WHEN {when_sql} THEN {then_sql}"));
    }
    if let Some(else_result) = case.else_result() {
        let else_sql = required(else_result, dialect, || "CASE ELSE result is null".into())?;
        sql.push_str(&format!(" ELSE {else_sql}"));
    }
    sql.push_str(" END");
    Ok(format!("({sql})"))
}

fn arity(fn_name: &str, args: &[Expr], ok: bool, expected: &str) -> Result<(), UdfError> {
    if ok {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "function_scalar {fn_name} requires {expected}, got {}",
        args.len()
    )))
}

fn render_scalar(call: &ScalarCall, dialect: Dialect) -> Result<Option<String>, UdfError> {
    let fn_name = call.name.as_str();
    let args = call.arguments.as_slice();

    // An Exasol-dialect `VerbatimCall` and a DataFusion call render here, ahead
    // of the per-name arms, so arm order carries no dialect precedence.
    let Some(function) = scalar_function(fn_name) else {
        return Err(UdfError::User(format!(
            "unsupported scalar function: {fn_name}"
        )));
    };
    let verbatim = match dialect {
        Dialect::Exasol => (function.exasol == ExasolForm::VerbatimCall).then_some(fn_name),
        Dialect::DataFusion => function.datafusion_call,
    };
    if let Some(call_name) = verbatim {
        let rendered = render_args(args, dialect)?;
        return Ok(Some(format!("{call_name}({})", rendered.join(", "))));
    }
    let operand =
        |i: usize, what: &str| required(&args[i], dialect, || format!("{fn_name} {what} is null"));

    Ok(Some(match fn_name {
        // Node names equal the capability names minus `FN_` and must stay
        // in lockstep with capabilities.rs (multiplication is `MULT`, not `MUL`).
        "ADD" | "SUB" | "MULT" | "FLOAT_DIV" => {
            arity(fn_name, args, args.len() >= 2, "2 arguments")?;
            let left = operand(0, "left operand")?;
            let right = operand(1, "right operand")?;
            if fn_name == "FLOAT_DIV" && dialect == Dialect::DataFusion {
                return Ok(Some(format!("{CHECKED_FLOAT_DIV_FN}({left}, {right})")));
            }
            let op = match fn_name {
                "ADD" => "+",
                "SUB" => "-",
                "MULT" => "*",
                _ => "/",
            };
            format!("({left} {op} {right})")
        }
        "NEG" => {
            if args.is_empty() {
                return Err(UdfError::User(
                    "function_scalar NEG requires 1 argument".into(),
                ));
            }
            format!("(-{})", operand(0, "operand")?)
        }
        "CAST" => {
            return Err(UdfError::User(
                "function_scalar CAST renders only as a CAST node".into(),
            ));
        }
        "ABS" | "FLOOR" | "CEIL" | "SQRT" | "EXP" | "LN" | "SIGN" | "DEGREES" | "RADIANS"
        | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "SINH" | "COSH" | "TANH" | "COT" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            let df_name = match fn_name {
                "SIGN" => "signum".to_string(),
                other => other.to_lowercase(),
            };
            format!("{df_name}({})", operand(0, "argument")?)
        }
        "ROUND" | "TRUNC" | "LOG" => {
            arity(
                fn_name,
                args,
                !args.is_empty() && args.len() <= 2,
                "1 or 2 arguments",
            )?;
            let rendered = render_args(args, dialect)?;
            format!("{}({})", fn_name.to_lowercase(), rendered.join(", "))
        }
        "POWER" | "ATAN2" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let rendered = render_args(args, dialect)?;
            format!("{}({})", fn_name.to_lowercase(), rendered.join(", "))
        }
        "MOD" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let left = operand(0, "left operand")?;
            let right = operand(1, "right operand")?;
            match dialect {
                Dialect::Exasol => format!("MOD({left}, {right})"),
                Dialect::DataFusion => format!("({left} % {right})"),
            }
        }
        // DataFusion's `concat` skips a NULL argument as Exasol's `||` does. The typed
        // pass reads its empty result as NULL.
        "CONCAT" => {
            if args.is_empty() {
                return Err(UdfError::User(
                    "function_scalar CONCAT requires at least 1 argument, got 0".into(),
                ));
            }
            let rendered = args
                .iter()
                .map(|arg| required(arg, dialect, || "CONCAT argument rendered to null".into()))
                .collect::<Result<Vec<String>, UdfError>>()?;
            match dialect {
                Dialect::Exasol => format!("({})", rendered.join(" || ")),
                Dialect::DataFusion => format!("concat({})", rendered.join(", ")),
            }
        }
        // DataFusion dialect only. Exasol reads a NULL search text as "replace nothing"
        // and a NULL replacement as the empty string, and sends the two-argument form
        // with a NULL replacement. DataFusion's `replace` returns NULL for either (#203).
        "REPLACE" => {
            let rendered = render_args(args, dialect)?;
            let (subject, search, replacement) = match rendered.as_slice() {
                [subject, search] => (subject, search, "NULL"),
                [subject, search, replacement] => (subject, search, replacement.as_str()),
                _ => {
                    return Err(UdfError::User(format!(
                        "function_scalar REPLACE requires 2 or 3 arguments, got {}",
                        rendered.len()
                    )));
                }
            };
            format!(
                "CASE WHEN {search} IS NULL THEN {subject} ELSE replace({subject}, {search}, coalesce({replacement}, '')) END"
            )
        }
        // DataFusion dialect only: INSTR(s, sub) and LOCATE(sub, s) both map to
        // strpos(s, sub), which takes no start or occurrence argument (#228).
        "INSTR" | "LOCATE" => {
            if args.len() != 2 {
                return Err(UdfError::User(format!(
                    "function_scalar {fn_name} renders exactly 2 arguments, got {}",
                    args.len()
                )));
            }
            let (string, substr) = if fn_name == "INSTR" { (0, 1) } else { (1, 0) };
            let string = operand(string, "string arg")?;
            let substr = operand(substr, "substring arg")?;
            format!("strpos({string}, {substr})")
        }
        // Arguments interleave [cond, result, ...], with a trailing ELSE on an
        // odd count.
        "CASE" => {
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
                let cond = required(&args[i], dialect, || format!("CASE WHEN cond[{i}] is null"))?;
                let result = required(&args[i + 1], dialect, || {
                    format!("CASE THEN result[{}] is null", i + 1)
                })?;
                sql.push_str(&format!(" WHEN {cond} THEN {result}"));
                i += 2;
            }
            if has_else {
                let else_val = required(&args[args.len() - 1], dialect, || {
                    "CASE ELSE value is null".into()
                })?;
                sql.push_str(&format!(" ELSE {else_val}"));
            }
            sql.push_str(" END");
            sql
        }
        "GREATEST" | "LEAST" => {
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
            format!(
                "CASE WHEN {guard} THEN NULL ELSE {df_name}({}) END",
                rendered.join(", ")
            )
        }
        "NULLIFZERO" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("nullif({}, 0)", operand(0, "argument")?)
        }
        "ZEROIFNULL" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("coalesce({}, 0)", operand(0, "argument")?)
        }
        "NULLIF" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let left = operand(0, "first argument")?;
            let right = operand(1, "second argument")?;
            format!("nullif({left}, {right})")
        }
        "SECOND" => return Err(UdfError::User(UNTYPED_SECOND.into())),
        "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("date_part('{fn_name}', {})", operand(0, "argument")?)
        }
        // Exasol WEEK and DataFusion `date_part('week')` are both ISO-8601, so
        // year-boundary weeks agree.
        "WEEK" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("date_part('week', {})", operand(0, "argument")?)
        }
        "DATE_TRUNC" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let unit = operand(0, "unit")?;
            let src = operand(1, "source")?;
            format!("date_trunc({unit}, {src})")
        }
        "TO_DATE" => return render_to_datetime(FormatTarget::Date, args, None, dialect),
        "TO_TIMESTAMP" => {
            return render_to_datetime(
                FormatTarget::Timestamp,
                args,
                call.timestamp_digits,
                dialect,
            );
        }
        "ADD_DAYS" | "ADD_WEEKS" | "ADD_HOURS" | "ADD_MINUTES" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let scan_fn = match fn_name {
                "ADD_DAYS" => ADD_DAYS_FN,
                "ADD_WEEKS" => ADD_WEEKS_FN,
                "ADD_HOURS" => ADD_HOURS_FN,
                _ => ADD_MINUTES_FN,
            };
            let rendered = render_args(args, dialect)?;
            format!("{scan_fn}({})", rendered.join(", "))
        }
        // DATE - DATE yields an Int64 day count in DataFusion; Exasol uses only
        // the date part of a timestamp.
        "DAYS_BETWEEN" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let first = operand(0, "first argument")?;
            let second = operand(1, "second argument")?;
            format!("(CAST({first} AS DATE) - CAST({second} AS DATE))")
        }
        "HOURS_BETWEEN" | "MINUTES_BETWEEN" | "SECONDS_BETWEEN" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let first = operand(0, "first argument")?;
            let second = operand(1, "second argument")?;
            let diff = format!("(date_part('epoch', {first}) - date_part('epoch', {second}))");
            match fn_name {
                "HOURS_BETWEEN" => format!("({diff} / 3600)"),
                "MINUTES_BETWEEN" => format!("({diff} / 60)"),
                _ => diff,
            }
        }
        // Backstop: reachable only if a declared name loses its per-name arm.
        other => {
            return Err(UdfError::User(format!(
                "unsupported scalar function: {other}"
            )));
        }
    }))
}

fn render_junction(
    items: &[Expr],
    op: &str,
    empty_value: &str,
    dialect: Dialect,
) -> Result<String, UdfError> {
    let mut parts: Vec<String> = Vec::new();
    for expr in items {
        if let Some(r) = render_expression_inner(expr, dialect)?
            && !r.is_empty()
        {
            parts.push(r);
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
impl ScanTyping<'_> {
    pub fn render_expression(self, expr: &Expr) -> Result<SqlFragment, UdfError> {
        scan_fragment(self.type_scan_tree(expr)?)
    }

    /// [`Self::render_expression`] for an operand of a computation the caller renders around it,
    /// such as an aggregate argument: a bare string column reads its empty strings as NULL.
    pub fn render_operand(self, expr: &Expr) -> Result<SqlFragment, UdfError> {
        let (typed, ty) = self.type_scan_tree(expr)?;
        scan_fragment((empty_as_null(typed, ty), ty))
    }

    /// [`Self::render_expression`] for a filter or a join condition. Exasol drops a `WHERE` that
    /// folds to `TRUE`, so no filter it sends renders as `TRUE`.
    pub fn render_filter(self, filter_expr: &Expr) -> Result<String, UdfError> {
        Ok(null_keeps_no_row(self.render_filter_root(filter_expr)?))
    }

    /// A filter, a join condition, or one of their top-level conjuncts.
    pub(crate) fn render_filter_root(self, root: &Expr) -> Result<String, UdfError> {
        let (typed, _) = self.type_scan_filter(root)?;
        render_expression(&typed)
    }

    /// [`render_expression_exasol`] for an output select item, with its [`Self::exact_type`].
    pub fn exasol_fragment(self, expr: &Expr) -> Result<SqlFragment, UdfError> {
        Ok(SqlFragment::new(
            render_expression_exasol(expr)?,
            self.exact_type(expr),
        ))
    }
}

fn scan_fragment((typed, inferred): (Expr, Inferred)) -> Result<SqlFragment, UdfError> {
    let ty = inferred.exact().map(|ty| {
        if ty.is_character() {
            ExasolType::WIDEST_VARCHAR
        } else {
            ty
        }
    });
    Ok(SqlFragment::new(render_expression(&typed)?, ty))
}

/// The filter [`ScanTyping::render_filter`] renders for the `AND` of conjuncts rendered one by one.
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

/// DataFusion dialect, untyped. Use [`ScanTyping::render_expression`].
pub(crate) fn render_expression(expr: &Expr) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::DataFusion)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// Exasol dialect, for SQL parsed by Exasol itself (join and grouped-aggregate
/// wrappers), never for a DataFusion `ScanSpec`. Character CAST targets are
/// length-qualified, since Exasol has no length-less character type.
///
/// Fails only on a tree Exasol does not send, such as a function call of the wrong arity.
pub fn render_expression_exasol(expr: &Expr) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::Exasol)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// A short description of `node` for an error message: its Exasol SQL where it has one.
pub fn short(node: &Expr) -> String {
    let text = match render_expression_inner(node, Dialect::Exasol) {
        Ok(Some(sql)) => sql,
        _ => format!("{node:?}"),
    };
    match text.char_indices().nth(120) {
        Some((byte, _)) => format!("{}…", &text[..byte]),
        None => text,
    }
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
