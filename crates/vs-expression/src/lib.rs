use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

/// Which parser consumes the rendered fragment: DataFusion inside the scan UDF
/// (`ScanSpec` filter/projection/group keys) or Exasol for the outer wrapper SQL.
/// The `Exasol` dialect re-emits what Exasol sent (name, argument order and count),
/// since Exasol cannot parse DataFusion-shaped rewrites.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    DataFusion,
    Exasol,
}

#[derive(Clone, Copy)]
enum ExasolForm {
    /// `<NAME>(<rendered args>)` with no arity check: Exasol emitted the call, so
    /// re-emitting it verbatim cannot be wrong.
    VerbatimCall,
    /// The per-name arm owns both dialects: the Exasol form is not a plain call
    /// (operator, infix predicate, `CASE`, per-dialect CAST target), or the
    /// DataFusion side is not (`MOD` renders as `%`).
    Shaped,
}

/// The single declaration of the translated surface. It gates dispatch: a name
/// absent here is declined in both dialects, so an arm without a row is unreachable.
const TRANSLATED_SCALAR_FNS: &[(&str, ExasolForm)] = &[
    ("ADD", ExasolForm::Shaped),
    ("SUB", ExasolForm::Shaped),
    ("MULT", ExasolForm::Shaped),
    ("FLOAT_DIV", ExasolForm::Shaped),
    ("NEG", ExasolForm::Shaped),
    ("CAST", ExasolForm::Shaped),
    ("REGEXP_LIKE", ExasolForm::Shaped),
    // MOD: Exasol requires `MOD(a, b)`, DataFusion the `%` operator (#197).
    ("MOD", ExasolForm::Shaped),
    // CONCAT: Exasol's `||` treats NULL as '' and yields NULL only when all-empty;
    // DataFusion reproduces that as `nullif(concat(...), '')` (#374).
    ("CONCAT", ExasolForm::Shaped),
    ("CASE", ExasolForm::Shaped),
    // VerbatimCall: applied to every remaining name uniformly, not only where the
    // DataFusion rendering fails on Exasol.
    //
    // DataFusion's `signum` does not exist in Exasol. The gate must precede the whole
    // name match, because the math arm matches SIGN (#209).
    ("ABS", ExasolForm::VerbatimCall),
    ("FLOOR", ExasolForm::VerbatimCall),
    ("CEIL", ExasolForm::VerbatimCall),
    ("SQRT", ExasolForm::VerbatimCall),
    ("EXP", ExasolForm::VerbatimCall),
    ("LN", ExasolForm::VerbatimCall),
    ("SIGN", ExasolForm::VerbatimCall),
    ("DEGREES", ExasolForm::VerbatimCall),
    ("RADIANS", ExasolForm::VerbatimCall),
    ("SIN", ExasolForm::VerbatimCall),
    ("COS", ExasolForm::VerbatimCall),
    ("TAN", ExasolForm::VerbatimCall),
    ("ASIN", ExasolForm::VerbatimCall),
    ("ACOS", ExasolForm::VerbatimCall),
    ("ATAN", ExasolForm::VerbatimCall),
    ("SINH", ExasolForm::VerbatimCall),
    ("COSH", ExasolForm::VerbatimCall),
    ("TANH", ExasolForm::VerbatimCall),
    ("COT", ExasolForm::VerbatimCall),
    ("ROUND", ExasolForm::VerbatimCall),
    ("TRUNC", ExasolForm::VerbatimCall),
    ("LOG", ExasolForm::VerbatimCall),
    ("POWER", ExasolForm::VerbatimCall),
    ("ATAN2", ExasolForm::VerbatimCall),
    // Exasol's INSTR/LOCATE accept the optional start argument natively (#210).
    ("LOWER", ExasolForm::VerbatimCall),
    ("UPPER", ExasolForm::VerbatimCall),
    ("SUBSTR", ExasolForm::VerbatimCall),
    ("TRIM", ExasolForm::VerbatimCall),
    ("LTRIM", ExasolForm::VerbatimCall),
    ("RTRIM", ExasolForm::VerbatimCall),
    ("REPLACE", ExasolForm::VerbatimCall),
    ("REPEAT", ExasolForm::VerbatimCall),
    ("REVERSE", ExasolForm::VerbatimCall),
    ("LPAD", ExasolForm::VerbatimCall),
    ("RPAD", ExasolForm::VerbatimCall),
    ("ASCII", ExasolForm::VerbatimCall),
    ("CHR", ExasolForm::VerbatimCall),
    ("INITCAP", ExasolForm::VerbatimCall),
    ("LEFT", ExasolForm::VerbatimCall),
    ("RIGHT", ExasolForm::VerbatimCall),
    ("TRANSLATE", ExasolForm::VerbatimCall),
    ("LENGTH", ExasolForm::VerbatimCall),
    ("OCTET_LENGTH", ExasolForm::VerbatimCall),
    ("UNICODE", ExasolForm::VerbatimCall),
    ("UNICODECHR", ExasolForm::VerbatimCall),
    ("INSTR", ExasolForm::VerbatimCall),
    ("LOCATE", ExasolForm::VerbatimCall),
    ("GREATEST", ExasolForm::VerbatimCall),
    ("LEAST", ExasolForm::VerbatimCall),
    ("NULLIF", ExasolForm::VerbatimCall),
    ("NULLIFZERO", ExasolForm::VerbatimCall),
    ("ZEROIFNULL", ExasolForm::VerbatimCall),
    // DataFusion renders these as `date_part`, which Exasol lacks (#209).
    ("YEAR", ExasolForm::VerbatimCall),
    ("MONTH", ExasolForm::VerbatimCall),
    ("DAY", ExasolForm::VerbatimCall),
    ("HOUR", ExasolForm::VerbatimCall),
    ("MINUTE", ExasolForm::VerbatimCall),
    ("SECOND", ExasolForm::VerbatimCall),
    ("WEEK", ExasolForm::VerbatimCall),
    ("DATE_TRUNC", ExasolForm::VerbatimCall),
    ("TO_DATE", ExasolForm::VerbatimCall),
    ("TO_TIMESTAMP", ExasolForm::VerbatimCall),
    // DataFusion emulates these via `date_part`, which Exasol lacks.
    ("DAYS_BETWEEN", ExasolForm::VerbatimCall),
    ("HOURS_BETWEEN", ExasolForm::VerbatimCall),
    ("MINUTES_BETWEEN", ExasolForm::VerbatimCall),
    ("SECONDS_BETWEEN", ExasolForm::VerbatimCall),
    // Not declared: CURRENT_DATE, SYSDATE, CURRENT_TIMESTAMP, SYSTIMESTAMP. The scan
    // UDF has no session time zone, statement anchor, or connect-back, so their
    // capabilities are withdrawn and Exasol evaluates them itself.
];

fn declared_scalar_fn(name: &str) -> Option<ExasolForm> {
    TRANSLATED_SCALAR_FNS
        .iter()
        .find(|(declared, _)| declared.eq_ignore_ascii_case(name))
        .map(|(_, form)| *form)
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

/// Detects a boolean operand converted to string (CAST or `||`), which must render
/// Exasol's `TRUE`/`FALSE` casing rather than DataFusion's lowercase cast (#200).
fn is_boolean_producing(kind: &str) -> bool {
    matches!(
        kind,
        "literal_bool"
            | "predicate_equal"
            | "predicate_notequal"
            | "predicate_less"
            | "predicate_lessequal"
            | "predicate_greater"
            | "predicate_greaterequal"
            | "predicate_and"
            | "predicate_or"
            | "predicate_not"
            | "predicate_is_null"
            | "predicate_is_not_null"
            | "predicate_in_constlist"
            | "predicate_between"
            | "predicate_like"
            | "predicate_like_regexp"
    )
}

/// Exasol's `TRUE`/`FALSE` casing instead of DataFusion's lowercase; NULL stays NULL
/// rather than becoming `'NULL'` or `'FALSE'` (#200).
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

/// Converting the UTC-normalized wire value into `SESSIONTIMEZONE` reproduces
/// Exasol's own TSTZ-vs-TIMESTAMP coercion, which reads the naive side as
/// session-local (verified live, #218).
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
/// three-valued logic would empty the result (#206).
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

const DOUBLE_TYPE: &str = "DOUBLE";

fn render_cast_target(data_type: &Json, dialect: Dialect) -> Result<String, UdfError> {
    let type_name = data_type.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match type_name.to_uppercase().as_str() {
        "VARCHAR" => match dialect {
            // datafusion-sql rejects a length-qualified VARCHAR
            // (`support_varchar_with_length` is not enabled).
            Dialect::DataFusion => Ok("VARCHAR".to_string()),
            // Exasol requires a length: echo the size Exasol sent, never clamped.
            Dialect::Exasol => Ok(match data_type.get("size").and_then(|v| v.as_u64()) {
                Some(size) => format!("VARCHAR({size})"),
                None => "VARCHAR(2000000)".to_string(),
            }),
        },
        "CHAR" => match dialect {
            // Arrow has no fixed-width CHAR type.
            Dialect::DataFusion => Ok("VARCHAR".to_string()),
            // Exasol validates column types positionally against the declared
            // `CHAR(n)`: `VARCHAR(n)` is a type mismatch and drops CHAR padding
            // (#192), and a missing ` ASCII` is a charset mismatch. The width is
            // echoed, never clamped.
            Dialect::Exasol => {
                let is_ascii = data_type
                    .get("characterSet")
                    .and_then(|v| v.as_str())
                    .is_some_and(|cs| cs.eq_ignore_ascii_case("ASCII"));
                Ok(match data_type.get("size").and_then(|v| v.as_u64()) {
                    Some(size) if is_ascii => format!("CHAR({size}) ASCII"),
                    Some(size) => format!("CHAR({size})"),
                    None => "VARCHAR(2000000)".to_string(),
                })
            }
        },
        "DECIMAL" => {
            let p = data_type
                .get("precision")
                .and_then(|v| v.as_u64())
                .unwrap_or(18);
            let s = data_type.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            Ok(format!("DECIMAL({p},{s})"))
        }
        "DOUBLE" | "DOUBLE PRECISION" => Ok(DOUBLE_TYPE.to_string()),
        "BOOLEAN" => Ok("BOOLEAN".to_string()),
        "DATE" => Ok("DATE".to_string()),
        "TIMESTAMP" => {
            // WLTZ (`withLocalTimeZone: true`) carries session-timezone semantics
            // DataFusion's TIMESTAMP cannot reproduce; decline so Exasol evaluates it.
            if data_type.get("withLocalTimeZone").and_then(|v| v.as_bool()) == Some(true) {
                return Err(UdfError::User(
                    "unsupported CAST target type: TIMESTAMP WITH LOCAL TIME ZONE".into(),
                ));
            }
            // `fractionalSecondsPrecision`, not `precision` (DECIMAL/INTERVAL only).
            // Absent means Exasol's default TIMESTAMP(3).
            match data_type
                .get("fractionalSecondsPrecision")
                .and_then(|v| v.as_u64())
            {
                None => Ok("TIMESTAMP".to_string()),
                // Exasol's own parser accepts any precision 0-9 verbatim.
                Some(p) => match dialect {
                    Dialect::Exasol => Ok(format!("TIMESTAMP({p})")),
                    // DataFusion parses only p in {0,3,6,9}; decline the rest
                    // rather than approximate.
                    Dialect::DataFusion => match p {
                        0 | 3 | 6 | 9 => Ok(format!("TIMESTAMP({p})")),
                        _ => Err(UdfError::User(format!(
                            "unsupported CAST target type: TIMESTAMP({p})"
                        ))),
                    },
                },
            }
        }
        other => Err(UdfError::User(format!(
            "unsupported CAST target type: {other}"
        ))),
    }
}

/// Reproduces Exasol's shortest-form DECIMAL->string conversion. The caller MUST
/// have confirmed `expr_sql` is DECIMAL-typed: it blindly strips zeros after a `.`.
fn format_decimal_exasol_style(expr_sql: &str) -> String {
    format!(
        "regexp_replace(regexp_replace(CAST({expr_sql} AS VARCHAR), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')"
    )
}

/// The scalar UDF the DataFusion dialect renders `FLOAT_DIV` as (#370), registered
/// by `lakehouse-engine`. It MUST take two arguments coerced to `Float64` (Exasol's
/// `FN_FLOAT_DIV` is always float division), return `Float64`, propagate `NULL`, and
/// error on any other non-finite result, so division by zero fails in a filter
/// exactly as in a projection.
pub const CHECKED_FLOAT_DIV_FN: &str = "vs_checked_float_div";

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
    let target_type = render_cast_target(data_type, dialect)?;

    let target_is_string = matches!(
        data_type
            .get("type")
            .and_then(|t| t.as_str())
            .map(str::to_uppercase)
            .as_deref(),
        Some("VARCHAR") | Some("CHAR")
    );
    let source_is_boolean = args[0]
        .get("type")
        .and_then(|t| t.as_str())
        .is_some_and(is_boolean_producing);
    if target_is_string && source_is_boolean {
        return Ok(Some(render_bool_to_string_case(&inner)));
    }

    Ok(Some(format!("CAST({inner} AS {target_type})")))
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
        "literal_exactnumeric" | "literal_double" => {
            return Ok(Some(match value("value") {
                None | Some(Json::Null) => "NULL".to_string(),
                Some(v) => json_scalar_to_string(v),
            }));
        }
        "literal_date" => return Ok(Some(format!("DATE {}", quote_literal(value("value"))))),
        "literal_timestamp" => {
            // Exasol has no `arrow_cast` (42000); re-emit the bare literal it sent.
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_timestamp_literal(value("value"))));
            }
            // Explicit microsecond precision: DataFusion types a bare `TIMESTAMP`
            // literal as nanoseconds, which overflows on far-future values when
            // unified with microsecond columns (#155). A NULL value renders
            // `arrow_cast(NULL, ...)`, deliberately unlike `literal_timestamp_utc`.
            return Ok(Some(format!(
                "arrow_cast({}, 'Timestamp(Microsecond, None)')",
                quote_literal(value("value"))
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
            // Exasol's real wire name for a TSTZ literal (#242). DataFusion keeps
            // declining it: TSTZ coercion against a naive `timestamp_us` column is
            // unverified (#242).
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_tstz_literal(value("value"))));
            }
            return Ok(None);
        }
        "column" => {
            let name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            let quoted = quote_ident(&name);
            // `tableAlias` is set only for multi-table renders (join wrapper), to
            // disambiguate a name shared by two joined subqueries.
            return Ok(Some(
                match value("tableAlias")
                    .and_then(|a| a.as_str())
                    .filter(|a| !a.is_empty())
                {
                    Some(alias) => format!("{}.{}", quote_ident(alias), quoted),
                    None => quoted,
                },
            ));
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
        // Exasol sends REGEXP_LIKE as node type `predicate_like_regexp`.
        "predicate_like_regexp" => {
            let subject =
                render_expression_inner(value("expression").unwrap_or(&Json::Null), dialect)?
                    .ok_or_else(|| {
                        UdfError::User("predicate_like_regexp missing 'expression'".into())
                    })?;
            let pattern =
                render_expression_inner(value("pattern").unwrap_or(&Json::Null), dialect)?
                    .ok_or_else(|| {
                        UdfError::User("predicate_like_regexp missing 'pattern'".into())
                    })?;
            Ok(Some(match dialect {
                // Exasol accepts only the infix form (42000 on a call).
                Dialect::Exasol => format!("({subject} REGEXP_LIKE {pattern})"),
                Dialect::DataFusion => format!("regexp_like({subject}, {pattern})"),
            }))
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
        // The CAST wire shape Exasol sends; the nested `function_scalar` CAST arm
        // is a defensive alternate encoding.
        "function_scalar_cast" => {
            let args = value("arguments").and_then(|a| a.as_array());
            render_cast(args, value("dataType"), dialect)
        }
        // Adapter-synthesized, never sent by Exasol (#211): the adapter has confirmed
        // the argument is a DECIMAL column being stringified.
        "decimal_to_varchar_exasol" => {
            let args = value("arguments")
                .and_then(|a| a.as_array())
                .ok_or_else(|| {
                    UdfError::User("decimal_to_varchar_exasol missing 'arguments'".into())
                })?;
            if args.len() != 1 {
                return Err(UdfError::User(format!(
                    "decimal_to_varchar_exasol requires exactly 1 argument, got {}",
                    args.len()
                )));
            }
            let inner = render_expression_inner(&args[0], dialect)?.ok_or_else(|| {
                UdfError::User("decimal_to_varchar_exasol argument is null".into())
            })?;
            Ok(Some(format_decimal_exasol_style(&inner)))
        }
        "function_scalar" => {
            let fn_name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            let args = value("arguments").and_then(|a| a.as_array());

            // An Exasol-dialect `VerbatimCall` renders here, ahead of the per-name
            // arms, so arm order carries no dialect precedence.
            match declared_scalar_fn(&fn_name) {
                None => {
                    return Err(UdfError::User(format!(
                        "unsupported scalar function: {fn_name}"
                    )));
                }
                Some(ExasolForm::VerbatimCall) if dialect == Dialect::Exasol => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    let rendered = render_args(args, dialect)?;
                    return Ok(Some(format!("{fn_name}({})", rendered.join(", "))));
                }
                _ => {}
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
                "REGEXP_LIKE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar REGEXP_LIKE missing 'arguments'".into())
                    })?;
                    if args.len() < 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar REGEXP_LIKE requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let subject = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("REGEXP_LIKE subject is null".into()))?;
                    let pattern = render_expression_inner(&args[1], dialect)?
                        .ok_or_else(|| UdfError::User("REGEXP_LIKE pattern is null".into()))?;
                    Ok(Some(match dialect {
                        // Must render byte-identically to `predicate_like_regexp`.
                        Dialect::Exasol => format!("({subject} REGEXP_LIKE {pattern})"),
                        Dialect::DataFusion => format!("regexp_like({subject}, {pattern})"),
                    }))
                }
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
                // as Exasol's `||` does (#374); boolean operands need Exasol casing (#200).
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
                            let r = render_expression_inner(arg, dialect)?.ok_or_else(|| {
                                UdfError::User("CONCAT argument rendered to null".into())
                            })?;
                            let is_bool = arg
                                .get("type")
                                .and_then(|t| t.as_str())
                                .is_some_and(is_boolean_producing);
                            Ok(if is_bool {
                                render_bool_to_string_case(&r)
                            } else {
                                r
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
                // DataFusion dialect only; the Exasol dialect renders these at the gate.
                "LOWER" | "UPPER" | "SUBSTR" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
                | "REPEAT" | "REVERSE" | "LPAD" | "RPAD" | "ASCII" | "CHR" | "INITCAP" | "LEFT"
                | "RIGHT" | "TRANSLATE" | "LENGTH" | "OCTET_LENGTH" | "UNICODE" | "UNICODECHR" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    let lower;
                    let df_name = match fn_name.as_str() {
                        "LENGTH" => "character_length",
                        "OCTET_LENGTH" => "octet_length",
                        "UNICODE" => "ascii",
                        "UNICODECHR" => "chr",
                        "SUBSTR" => "substr",
                        other => {
                            lower = other.to_lowercase();
                            &lower
                        }
                    };
                    let rendered = render_args(args, dialect)?;
                    Ok(Some(format!("{df_name}({})", rendered.join(", "))))
                }
                // DataFusion dialect only: INSTR(s, sub) and LOCATE(sub, s) both map to
                // strpos(s, sub).
                "INSTR" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar INSTR missing 'arguments'".into())
                    })?;
                    if args.len() < 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar INSTR requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let string = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("INSTR string arg is null".into()))?;
                    let substr = render_expression_inner(&args[1], dialect)?
                        .ok_or_else(|| UdfError::User("INSTR substring arg is null".into()))?;
                    Ok(Some(format!("strpos({string}, {substr})")))
                }
                "LOCATE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar LOCATE missing 'arguments'".into())
                    })?;
                    if args.len() < 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar LOCATE requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let substr = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("LOCATE substring arg is null".into()))?;
                    let string = render_expression_inner(&args[1], dialect)?
                        .ok_or_else(|| UdfError::User("LOCATE string arg is null".into()))?;
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
                "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" => {
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
                "TO_DATE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar TO_DATE missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar TO_DATE requires at least 1 argument".into(),
                        ));
                    }
                    let rendered = render_args(args, dialect)?;
                    Ok(Some(format!("to_date({})", rendered.join(", "))))
                }
                "TO_TIMESTAMP" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar TO_TIMESTAMP missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar TO_TIMESTAMP requires at least 1 argument".into(),
                        ));
                    }
                    let rendered = render_args(args, dialect)?;
                    Ok(Some(format!("to_timestamp({})", rendered.join(", "))))
                }
                // ADD_HOURS/ADD_MINUTES are not translated: Exasol types
                // ADD_HOURS(DATE, n) as TIMESTAMP(0), which a type-blind translator
                // cannot reproduce.
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
    if parts.is_empty() {
        Ok(empty_value.to_string())
    } else if parts.len() == 1 {
        Ok(parts.into_iter().next().unwrap())
    } else {
        Ok(format!("({})", parts.join(op)))
    }
}

/// DataFusion dialect. Use [`render_expression_exasol`] for SQL parsed by Exasol.
pub fn render_expression(expr: &Json) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::DataFusion)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// `None` on any failure; never panics.
pub fn render_expression_safe(expr: &Json) -> Option<String> {
    render_expression_inner(expr, Dialect::DataFusion).ok()?
}

/// `None` when rendering fails or the filter is trivially true (`TRUE`/`NULL`). The
/// two causes are indistinguishable; the caller decides whether a declined filter
/// must be self-applied.
pub fn render_df_filter_safe(filter_expr: &Json) -> Option<String> {
    let result = render_expression_inner(filter_expr, Dialect::DataFusion).ok()??;
    if result == "TRUE" || result == "NULL" {
        None
    } else {
        Some(result)
    }
}

/// Exasol dialect, for SQL parsed by Exasol itself (join and grouped-aggregate
/// wrappers), never for a DataFusion `ScanSpec`. Character CAST targets are
/// length-qualified, since Exasol has no length-less character type.
pub fn render_expression_exasol(expr: &Json) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::Exasol)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// `None` on any failure.
pub fn render_expression_exasol_safe(expr: &Json) -> Option<String> {
    render_expression_inner(expr, Dialect::Exasol).ok()?
}

/// Exasol-dialect [`render_df_filter_safe`], with the same `None` semantics.
pub fn render_df_filter_exasol_safe(filter_expr: &Json) -> Option<String> {
    let result = render_expression_inner(filter_expr, Dialect::Exasol).ok()??;
    if result == "TRUE" || result == "NULL" {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
