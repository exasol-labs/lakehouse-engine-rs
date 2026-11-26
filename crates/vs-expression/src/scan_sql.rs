//! The DataFusion SQL of every form the scan writes differently from Exasol, over rendered
//! operands. Only the typed pass calls these, so every scan expression is typed before it renders.

use exasol_udf_sdk::error::UdfError;

use crate::datetime_format::{FormatTarget, strftime_pattern};
use crate::exasol_type::ExasolType;
use crate::expr::{Expr, Literal, ScalarCall};
use crate::json_value::{
    Fallback, JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, Returning, ScanJsonValue,
};
use crate::scalar_functions::scalar_function;
use crate::scan_typing::seconds_type;
use crate::timestamp::{TimestampLiteral, TimestampUnit};
use crate::{arity, exasol_literal, quote_literal, shaped_scalar, sql_escape};

/// The scalar UDF the scan renders `FLOAT_DIV` as, registered by `lakehouse-engine`. It MUST take
/// two arguments coerced to `Float64` (Exasol's `FN_FLOAT_DIV` is always float division), return
/// `Float64`, propagate `NULL`, and error on any other non-finite result, so division by zero
/// fails in a filter exactly as in a projection.
pub const CHECKED_FLOAT_DIV_FN: &str = "vs_checked_float_div";

/// The scalar UDF the scan renders `DIV` as, registered by `lakehouse-engine`.
/// It raises on a zero divisor, so division by zero fails in a filter exactly as in a projection.
pub const CHECKED_INT_DIV_FN: &str = "vs_checked_int_div";

/// The scalar UDFs the scan renders `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` as,
/// registered by `lakehouse-engine`. Each MUST take a first argument of `Date32`,
/// `Timestamp(Microsecond, None)`, or `Timestamp(Nanosecond, None)`, which the typed pass casts
/// to, and a number as its second.
pub const ADD_DAYS_FN: &str = "vs_add_days";
/// See [`ADD_DAYS_FN`].
pub const ADD_WEEKS_FN: &str = "vs_add_weeks";
/// See [`ADD_DAYS_FN`].
pub const ADD_HOURS_FN: &str = "vs_add_hours";
/// See [`ADD_DAYS_FN`].
pub const ADD_MINUTES_FN: &str = "vs_add_minutes";

/// The scalar UDF the scan renders `TO_TIMESTAMP(text, format)` as, registered by
/// `lakehouse-engine`. It takes the text, a strftime pattern, and the fractional digits to keep,
/// and returns `Timestamp(Microsecond, None)` for up to 6 digits, else nanoseconds.
pub const TO_TIMESTAMP_FN: &str = "vs_to_timestamp";

pub(crate) fn literal(literal: &Literal) -> Result<String, UdfError> {
    Ok(match literal {
        // The scan parses bare numeric literals as DECIMAL, which rejects `1e+300`.
        Literal::Double(value) => format!("CAST({} AS DOUBLE)", quote_literal(value)),
        // A `width` types it `FixedSizeBinary`, so comparing it leaves a fixed-length column
        // uncast.
        Literal::Binary { hex, width } => {
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
        Literal::Timestamp(value) => timestamp_literal(value),
        // `+00:00` parses the value as UTC; the "UTC" tz label matches the scan's Timestamptz
        // Arrow mapping, avoiding a tz-label mismatch.
        Literal::TimestampUtc(value) => {
            let quoted = quote_literal(&format!("{value}+00:00"));
            format!("arrow_cast({quoted}, 'Timestamp(Microsecond, Some(\"UTC\"))')")
        }
        // Its comparison depends on the session zone, so the adapter folds a compared one into a
        // plain timestamp literal and the scan declines any left over.
        Literal::LocalTimeZoneTimestamp(_) => {
            return Err(UdfError::User(
                "a TIMESTAMP WITH LOCAL TIME ZONE literal renders only in the Exasol dialect"
                    .into(),
            ));
        }
        Literal::Null
        | Literal::Bool(_)
        | Literal::String(_)
        | Literal::ExactNumeric(_)
        | Literal::Date(_) => return exasol_literal(literal),
    })
}

/// Explicit precision: DataFusion types a bare `TIMESTAMP` literal as nanoseconds, which overflows
/// on far-future values when unified with microsecond columns, so only sub-microsecond digits get
/// nanoseconds.
pub(crate) fn timestamp_literal(value: &str) -> String {
    let unit = if TimestampLiteral(value).fits(TimestampUnit::Microsecond) {
        "Microsecond"
    } else {
        "Nanosecond"
    };
    format!(
        "arrow_cast({}, 'Timestamp({unit}, None)')",
        quote_literal(value)
    )
}

/// Declines a target DataFusion cannot represent, and renders CHAR or VARCHAR as unsized VARCHAR
/// without Exasol's checks (#587).
pub(crate) fn cast(inner: &str, target: ExasolType) -> Result<String, UdfError> {
    let target = match target {
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
    };
    Ok(format!("CAST({inner} AS {target})"))
}

/// An integer operand converted to text.
pub(crate) fn integer_text(inner: &str) -> String {
    format!("CAST(CAST({inner} AS DECIMAL(38,0)) AS VARCHAR)")
}

/// A boolean operand converted to text in Exasol's `TRUE`/`FALSE` casing, not DataFusion's
/// lowercase. NULL stays NULL.
pub(crate) fn boolean_text(inner: &str) -> String {
    format!("(CASE {inner} WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)")
}

/// A boolean operand converted to Exasol's 1 or 0, which DataFusion does not do
/// (apache/datafusion#16797).
pub(crate) fn boolean_number(inner: &str) -> String {
    format!("(CASE {inner} WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)")
}

/// A text operand with an empty string read as NULL, because Exasol has no empty string and
/// DataFusion keeps one as a value (#203).
pub(crate) fn empty_as_null(inner: &str) -> String {
    format!("nullif({inner}, '')")
}

/// A string function's numeric argument as the `Int64` DataFusion requires, rounded as Exasol
/// rounds it.
pub(crate) fn integer_argument(round: bool, negative_is_null: bool, inner: &str) -> String {
    let value = if round {
        format!("CAST(round({inner}) AS BIGINT)")
    } else {
        format!("CAST({inner} AS BIGINT)")
    };
    if negative_is_null {
        format!("(CASE WHEN {value} >= 0 THEN {value} END)")
    } else {
        value
    }
}

/// The seconds of `arg` with their first `fraction` digits, cut beyond as Exasol does. An exact
/// decimal product scales them, because Arrow's decimal quotient keeps only the scale s1 + 4.
pub(crate) fn seconds(arg: &str, fraction: u8) -> Result<String, UdfError> {
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

/// DataFusion has no `EXTRACT(field FROM expr)` planner by default. The typed pass renders SECOND
/// through [`seconds`].
pub(crate) fn extract(field: &str, argument: &str) -> String {
    format!("date_part('{}', {argument})", field.to_uppercase())
}

/// `pattern_sql` renders `pattern`, the node the request sent.
pub(crate) fn like(
    expression: &str,
    pattern_sql: &str,
    pattern: &Expr,
    escape: Option<&Expr>,
) -> Result<String, UdfError> {
    let Some(escape) = escape else {
        return Ok(format!("({expression} LIKE {pattern_sql})"));
    };
    let rewritten = backslash_escaped_pattern(pattern, escape).ok_or_else(|| {
        UdfError::User("LIKE ESCAPE needs a string pattern and a one-character escape".into())
    })?;
    Ok(format!("({expression} LIKE {})", quote_literal(&rewritten)))
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

pub(crate) fn json_value(call: &ScanJsonValue, document: &str) -> String {
    let fallback = |fallback: &Fallback| match fallback {
        Fallback::Null => "NULL".to_string(),
        Fallback::Text(text) => quote_literal(text),
        Fallback::Boolean(value) => value.to_string().to_uppercase(),
    };
    let path = quote_literal(call.path);
    let (on_empty, on_error) = (fallback(&call.on_empty), fallback(&call.on_error));
    match call.returning {
        Returning::Text { max_chars } => {
            format!("{JSON_VALUE_TEXT_FN}({document}, {path}, {max_chars}, {on_empty}, {on_error})")
        }
        Returning::Boolean => {
            format!("{JSON_VALUE_BOOLEAN_FN}({document}, {path}, {on_empty}, {on_error})")
        }
    }
}

/// Renders only a `literal_string` format the translation covers, and errs otherwise so the
/// caller declines. `digits` are the fractional digits a `TO_TIMESTAMP` keeps.
fn to_datetime(
    target: FormatTarget,
    call: &ScalarCall,
    args: &[String],
    digits: Option<u8>,
) -> Result<String, UdfError> {
    let node = match target {
        FormatTarget::Date => "TO_DATE",
        FormatTarget::Timestamp => "TO_TIMESTAMP",
    };
    match (args, call.arguments.as_slice()) {
        ([], _) => Err(UdfError::User(format!(
            "function_scalar {node} requires at least 1 argument"
        ))),
        ([_], _) => Err(UdfError::User(format!(
            "function_scalar {node} without a format parses by the session's NLS settings"
        ))),
        ([source, _], [_, format]) => {
            let pattern = format
                .as_string_literal()
                .and_then(|model| strftime_pattern(model, target))
                .ok_or_else(|| {
                    UdfError::User(format!(
                        "function_scalar {node} format {format:?} is not a string literal with an exact strftime equivalent"
                    ))
                })?;
            let pattern = sql_escape(&pattern);
            Ok(match target {
                FormatTarget::Date => format!("to_date({source}, '{pattern}')"),
                FormatTarget::Timestamp => {
                    let digits = digits.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {node} carries no typed digits"))
                    })?;
                    format!("{TO_TIMESTAMP_FN}({source}, '{pattern}', {digits})")
                }
            })
        }
        _ => Err(UdfError::User(format!(
            "function_scalar {node} accepts at most 2 arguments, got {}",
            args.len()
        ))),
    }
}

/// `call` over its rendered arguments. `timestamp_digits` are the fractional digits a
/// `TO_TIMESTAMP` keeps.
pub(crate) fn scalar(
    call: &ScalarCall,
    args: &[String],
    timestamp_digits: Option<u8>,
) -> Result<String, UdfError> {
    let fn_name = call.name.as_str();
    let Some(function) = scalar_function(fn_name) else {
        return Err(UdfError::User(format!(
            "unsupported scalar function: {fn_name}"
        )));
    };
    // A same-arity call renders here, ahead of the per-name arms, so arm order carries no
    // precedence.
    if let Some(call_name) = function.datafusion_call {
        return Ok(format!("{call_name}({})", args.join(", ")));
    }
    Ok(match fn_name {
        "ADD" | "SUB" | "MULT" | "NEG" | "CASE" | "CAST" => shaped_scalar(fn_name, args)?,
        "FLOAT_DIV" => {
            arity(fn_name, args, args.len() >= 2, "2 arguments")?;
            format!("{CHECKED_FLOAT_DIV_FN}({}, {})", args[0], args[1])
        }
        "ABS" | "FLOOR" | "CEIL" | "SQRT" | "EXP" | "LN" | "SIGN" | "DEGREES" | "RADIANS"
        | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "SINH" | "COSH" | "TANH" | "COT" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            let df_name = match fn_name {
                "SIGN" => "signum".to_string(),
                other => other.to_lowercase(),
            };
            format!("{df_name}({})", args[0])
        }
        "ROUND" | "TRUNC" | "LOG" => {
            arity(
                fn_name,
                args,
                !args.is_empty() && args.len() <= 2,
                "1 or 2 arguments",
            )?;
            format!("{}({})", fn_name.to_lowercase(), args.join(", "))
        }
        "POWER" | "ATAN2" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            format!("{}({})", fn_name.to_lowercase(), args.join(", "))
        }
        "MOD" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            format!("({} % {})", args[0], args[1])
        }
        // DataFusion's `concat` skips a NULL argument as Exasol's `||` does. The typed
        // pass reads its empty result as NULL.
        "CONCAT" => {
            if args.is_empty() {
                return Err(UdfError::User(
                    "function_scalar CONCAT requires at least 1 argument, got 0".into(),
                ));
            }
            format!("concat({})", args.join(", "))
        }
        // Exasol reads a NULL search text as "replace nothing" and a NULL replacement as the
        // empty string, and sends the two-argument form with a NULL replacement. DataFusion's
        // `replace` returns NULL for either (#203).
        "REPLACE" => {
            let (subject, search, replacement) = match args {
                [subject, search] => (subject, search, "NULL"),
                [subject, search, replacement] => (subject, search, replacement.as_str()),
                _ => {
                    return Err(UdfError::User(format!(
                        "function_scalar REPLACE requires 2 or 3 arguments, got {}",
                        args.len()
                    )));
                }
            };
            format!(
                "CASE WHEN {search} IS NULL THEN {subject} ELSE replace({subject}, {search}, coalesce({replacement}, '')) END"
            )
        }
        // INSTR(s, sub) and LOCATE(sub, s) both map to strpos(s, sub), which takes no start or
        // occurrence argument (#228).
        "INSTR" | "LOCATE" => {
            if args.len() != 2 {
                return Err(UdfError::User(format!(
                    "function_scalar {fn_name} renders exactly 2 arguments, got {}",
                    args.len()
                )));
            }
            let (string, substr) = if fn_name == "INSTR" { (0, 1) } else { (1, 0) };
            format!("strpos({}, {})", args[string], args[substr])
        }
        "GREATEST" | "LEAST" => {
            if args.is_empty() {
                return Err(UdfError::User(format!(
                    "function_scalar {fn_name} requires at least 1 argument"
                )));
            }
            let guard = args
                .iter()
                .map(|a| format!("{a} IS NULL"))
                .collect::<Vec<_>>()
                .join(" OR ");
            format!(
                "CASE WHEN {guard} THEN NULL ELSE {}({}) END",
                fn_name.to_lowercase(),
                args.join(", ")
            )
        }
        "NULLIFZERO" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("nullif({}, 0)", args[0])
        }
        "ZEROIFNULL" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("coalesce({}, 0)", args[0])
        }
        "NULLIF" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            format!("nullif({}, {})", args[0], args[1])
        }
        // The typed pass renders the one-argument call, whose fraction it knows.
        "SECOND" => {
            return Err(UdfError::User(
                "SECOND renders only as a one-argument call, whose argument's precision gives its fraction".into(),
            ));
        }
        "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("date_part('{fn_name}', {})", args[0])
        }
        // Exasol WEEK and DataFusion `date_part('week')` are both ISO-8601, so
        // year-boundary weeks agree.
        "WEEK" => {
            arity(fn_name, args, args.len() == 1, "1 argument")?;
            format!("date_part('week', {})", args[0])
        }
        "DATE_TRUNC" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            format!("date_trunc({}, {})", args[0], args[1])
        }
        "TO_DATE" => to_datetime(FormatTarget::Date, call, args, None)?,
        "TO_TIMESTAMP" => to_datetime(FormatTarget::Timestamp, call, args, timestamp_digits)?,
        "ADD_DAYS" | "ADD_WEEKS" | "ADD_HOURS" | "ADD_MINUTES" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let scan_fn = match fn_name {
                "ADD_DAYS" => ADD_DAYS_FN,
                "ADD_WEEKS" => ADD_WEEKS_FN,
                "ADD_HOURS" => ADD_HOURS_FN,
                _ => ADD_MINUTES_FN,
            };
            format!("{scan_fn}({})", args.join(", "))
        }
        // DATE - DATE yields an Int64 day count in DataFusion; Exasol uses only
        // the date part of a timestamp.
        "DAYS_BETWEEN" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            format!("(CAST({} AS DATE) - CAST({} AS DATE))", args[0], args[1])
        }
        "HOURS_BETWEEN" | "MINUTES_BETWEEN" | "SECONDS_BETWEEN" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            let diff = format!(
                "(date_part('epoch', {}) - date_part('epoch', {}))",
                args[0], args[1]
            );
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
    })
}
