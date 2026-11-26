//! SQL for the two parsers of a pushdown: [`render_expression_exasol`] re-emits what Exasol sent
//! (name, argument order and count) for the wrapper SQL Exasol parses, since Exasol cannot parse a
//! DataFusion-shaped rewrite. [`ScanTyping`] types an expression and writes the DataFusion SQL of
//! a `ScanSpec` filter, projection, or group key.

use exasol_udf_sdk::error::UdfError;

mod datetime_format;
pub mod exasol_type;
mod expr;
mod fragment;
mod json_value;
mod parse;
mod request;
mod scalar_functions;
mod scan_sql;
mod scan_typing;
pub mod timestamp;
mod version;
use exasol_type::ExasolType;
pub use expr::{
    AggregateCall, CaseExpr, Column, CompareOp, Expr, JsonBehavior, JsonValueCall, Literal,
    ScalarCall,
};
pub use fragment::SqlFragment;
pub use json_value::{JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path};
pub use request::{
    AggregationType, FromClause, InvolvedColumn, InvolvedTable, JoinKind, ParsedPushdown,
    ParsedRequest, SortElement, parse_request,
};
use scalar_functions::{ExasolForm, scalar_function};
pub use scan_sql::{
    ADD_DAYS_FN, ADD_HOURS_FN, ADD_MINUTES_FN, ADD_WEEKS_FN, CHECKED_FLOAT_DIV_FN,
    CHECKED_INT_DIV_FN, TO_TIMESTAMP_FN,
};
pub use scan_typing::{ColumnTypeLookup, ScanTyping, is_session_free_integer_text};
use scan_typing::{Inferred, empty_as_null};
use timestamp::TimestampLiteral;
pub use version::ExasolVersion;

pub(crate) fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", sql_escape(value))
}

/// `tableAlias` is set only for multi-table renders (join wrapper), to disambiguate a name shared
/// by two joined subqueries.
pub(crate) fn column_sql(column: &Column) -> String {
    let quoted = quote_ident(&column.upper_name());
    match column.table_alias.as_deref().filter(|a| !a.is_empty()) {
        Some(alias) => format!("{}.{}", quote_ident(alias), quoted),
        None => quoted,
    }
}

pub(crate) fn compare_sql(op: CompareOp, left: &str, right: &str) -> String {
    format!("({left} {} {right})", op.sql())
}

/// An `AND` or `OR` of rendered operands. `empty` is its value over no operand.
pub(crate) fn junction_sql(mut parts: Vec<String>, op: &str, empty: &str) -> String {
    match parts.len() {
        0 => empty.to_string(),
        1 => parts.remove(0),
        _ => format!("({})", parts.join(op)),
    }
}

/// Exasol's `IN`/`NOT IN` ignores NULL list entries, while DataFusion's
/// three-valued logic would empty the result.
pub(crate) fn is_null_literal(arg: &Expr) -> bool {
    matches!(arg, Expr::Literal(Literal::Null))
}

/// `values` are the rendered entries that are no NULL literal ([`is_null_literal`]).
pub(crate) fn in_list_sql(target: &str, values: &[String]) -> String {
    if values.is_empty() {
        "FALSE".into()
    } else {
        format!("({target} IN ({}))", values.join(", "))
    }
}

pub(crate) fn between_sql(target: &str, low: &str, high: &str) -> String {
    format!("({target} BETWEEN {low} AND {high})")
}

/// `basis` present means simple CASE; `results` holds one THEN per WHEN plus an optional trailing
/// ELSE.
pub(crate) fn case_sql(basis: Option<&str>, whens: &[String], results: &[String]) -> String {
    let mut sql = String::from("CASE");
    if let Some(basis) = basis {
        sql.push_str(&format!(" {basis}"));
    }
    for (when, result) in whens.iter().zip(results) {
        sql.push_str(&format!(" WHEN {when} THEN {result}"));
    }
    if let Some(else_result) = results.get(whens.len()) {
        sql.push_str(&format!(" ELSE {else_result}"));
    }
    format!("({sql} END)")
}

/// The aggregate name Exasol pushed is valid in both engines, so it is spliced verbatim; no
/// arguments means `<NAME>(*)`.
pub(crate) fn aggregate_sql(call: &AggregateCall, args: &[String]) -> String {
    let name = &call.name;
    if args.is_empty() {
        format!("{name}(*)")
    } else {
        let distinct_kw = if call.distinct { "DISTINCT " } else { "" };
        format!("{name}({distinct_kw}{})", args.join(", "))
    }
}

pub(crate) fn arity(
    fn_name: &str,
    args: &[String],
    ok: bool,
    expected: &str,
) -> Result<(), UdfError> {
    if ok {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "function_scalar {fn_name} requires {expected}, got {}",
        args.len()
    )))
}

/// Shared by `literal_timestamp` and `literal_timestamp_utc` so they cannot drift.
/// Exasol's literal format has no offset field (22018 on `+00:00`).
fn render_exasol_timestamp_literal(value: &str) -> String {
    format!("TIMESTAMP {}", quote_literal(value))
}

/// Converting the UTC-normalized wire value into `SESSIONTIMEZONE` reproduces
/// Exasol's own TSTZ-vs-TIMESTAMP coercion, which reads the naive side as
/// session-local (verified live). A bare TSTZ cast keeps 3 fractional digits, so a longer value
/// names its precision; Exasol 8 never sends more than 3 and has no precision syntax.
fn render_exasol_tstz_literal(value: &str) -> String {
    let digits = TimestampLiteral(value).type_precision();
    let cast_type = ExasolType::TimestampWithLocalTimeZone {
        precision: u8::try_from(digits.max(3)).unwrap_or(u8::MAX),
    };
    format!(
        "CAST(CONVERT_TZ({}, 'UTC', SESSIONTIMEZONE) AS {cast_type})",
        render_exasol_timestamp_literal(value)
    )
}

/// A literal as Exasol parses it. The scan writes a NULL, a boolean, a string, an exact number,
/// and a date the same way.
pub(crate) fn exasol_literal(literal: &Literal) -> Result<String, UdfError> {
    Ok(match literal {
        Literal::Null => "NULL".into(),
        Literal::Bool(value) => if *value { "TRUE" } else { "FALSE" }.into(),
        Literal::String(value) => quote_literal(value),
        Literal::ExactNumeric(value) | Literal::Double(value) => value.clone(),
        Literal::Date(value) => format!("DATE {}", quote_literal(value)),
        Literal::Timestamp(value) => render_exasol_timestamp_literal(value),
        Literal::TimestampUtc(value) | Literal::LocalTimeZoneTimestamp(value) => {
            render_exasol_tstz_literal(value)
        }
        Literal::Binary { .. } => {
            return Err(UdfError::User(
                "unsupported expression node type: literal_binary".into(),
            ));
        }
    })
}

/// Every clause Exasol sent, so Exasol computes the forms the scan declines.
fn render_json_value_exasol(call: &JsonValueCall) -> Result<String, UdfError> {
    let operand = |node: &Expr| exasol_sql(node);
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

fn exasol_sql_all(nodes: &[Expr]) -> Result<Vec<String>, UdfError> {
    nodes.iter().map(exasol_sql).collect()
}

fn exasol_sql(expr: &Expr) -> Result<String, UdfError> {
    let render = exasol_sql;
    Ok(match expr {
        Expr::Literal(literal) => exasol_literal(literal)?,
        Expr::Column(column) => column_sql(column),
        Expr::Compare { op, left, right } => compare_sql(*op, &render(left)?, &render(right)?),
        Expr::And(items) => junction_sql(exasol_sql_all(items)?, " AND ", "TRUE"),
        Expr::Or(items) => junction_sql(exasol_sql_all(items)?, " OR ", "FALSE"),
        Expr::Not(inner) => format!("(NOT {})", render(inner)?),
        Expr::IsNull(inner) => format!("({} IS NULL)", render(inner)?),
        Expr::IsNotNull(inner) => format!("({} IS NOT NULL)", render(inner)?),
        Expr::InList { expression, values } => {
            let target = render(expression)?;
            let values = values
                .iter()
                .filter(|value| !is_null_literal(value))
                .map(render)
                .collect::<Result<Vec<String>, UdfError>>()?;
            in_list_sql(&target, &values)
        }
        Expr::Between {
            expression,
            low,
            high,
        } => between_sql(&render(expression)?, &render(low)?, &render(high)?),
        Expr::Like {
            expression,
            pattern,
            escape,
        } => {
            let (l, p) = (render(expression)?, render(pattern)?);
            match escape {
                None => format!("({l} LIKE {p})"),
                Some(escape) => format!("({l} LIKE {p} ESCAPE {})", render(escape)?),
            }
        }
        Expr::Extract { field, argument } => {
            format!(
                "EXTRACT({} FROM {})",
                field.to_uppercase(),
                render(argument)?
            )
        }
        Expr::Case(case) => {
            let basis = case.basis.as_deref().map(render).transpose()?;
            let whens = exasol_sql_all(&case.whens)?;
            case_sql(basis.as_deref(), &whens, &exasol_sql_all(&case.results)?)
        }
        Expr::JsonValue(call) => render_json_value_exasol(call)?,
        // Character targets are length-qualified, since Exasol has no length-less character type.
        Expr::Cast { target, argument } => format!("CAST({} AS {target})", render(argument)?),
        Expr::Scalar(call) => {
            let name = call.name.as_str();
            let Some(function) = scalar_function(name) else {
                return Err(UdfError::User(format!(
                    "unsupported scalar function: {name}"
                )));
            };
            let args = exasol_sql_all(&call.arguments)?;
            match function.exasol {
                ExasolForm::VerbatimCall => format!("{name}({})", args.join(", ")),
                ExasolForm::Shaped => shaped_scalar(name, &args)?,
            }
        }
        Expr::Aggregate(call) => aggregate_sql(call, &exasol_sql_all(&call.arguments)?),
    })
}

/// A `Shaped` function over its rendered arguments, as Exasol parses it. The scan writes `ADD`,
/// `SUB`, `MULT`, `NEG`, and `CASE` the same way.
pub(crate) fn shaped_scalar(fn_name: &str, args: &[String]) -> Result<String, UdfError> {
    Ok(match fn_name {
        // Node names equal the capability names minus `FN_` and must stay
        // in lockstep with capabilities.rs (multiplication is `MULT`, not `MUL`).
        "ADD" | "SUB" | "MULT" | "FLOAT_DIV" => {
            arity(fn_name, args, args.len() >= 2, "2 arguments")?;
            let op = match fn_name {
                "ADD" => "+",
                "SUB" => "-",
                "MULT" => "*",
                _ => "/",
            };
            format!("({} {op} {})", args[0], args[1])
        }
        "NEG" => {
            let [operand, ..] = args else {
                return Err(UdfError::User(
                    "function_scalar NEG requires 1 argument".into(),
                ));
            };
            format!("(-{operand})")
        }
        "CAST" => {
            return Err(UdfError::User(
                "function_scalar CAST renders only as a CAST node".into(),
            ));
        }
        "MOD" => {
            arity(fn_name, args, args.len() == 2, "2 arguments")?;
            format!("MOD({}, {})", args[0], args[1])
        }
        "CONCAT" => {
            if args.is_empty() {
                return Err(UdfError::User(
                    "function_scalar CONCAT requires at least 1 argument, got 0".into(),
                ));
            }
            format!("({})", args.join(" || "))
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
            let (branches, else_result) = args.as_chunks::<2>();
            for [when, then] in branches {
                sql.push_str(&format!(" WHEN {when} THEN {then}"));
            }
            if let [else_result] = else_result {
                sql.push_str(&format!(" ELSE {else_result}"));
            }
            sql + " END"
        }
        other => {
            return Err(UdfError::User(format!(
                "unsupported scalar function: {other}"
            )));
        }
    })
}

/// A CHAR or VARCHAR CAST renders unsized, so its fragment type is the widest VARCHAR.
impl ScanTyping<'_> {
    /// The DataFusion SQL of `expr` and Exasol's type of it. `Err` declines.
    pub fn render_expression(self, expr: &Expr) -> Result<SqlFragment, UdfError> {
        Ok(scan_fragment(self.type_scan_tree(expr)?))
    }

    /// [`Self::render_expression`] for an operand of a computation the caller renders around it,
    /// such as an aggregate argument: a bare string column reads its empty strings as NULL.
    pub fn render_operand(self, expr: &Expr) -> Result<SqlFragment, UdfError> {
        let (sql, ty) = self.type_scan_tree(expr)?;
        Ok(scan_fragment((empty_as_null(expr, sql, ty), ty)))
    }

    /// [`Self::render_expression`] for a filter or a join condition. Exasol drops a `WHERE` that
    /// folds to `TRUE`, so no filter it sends renders as `TRUE`.
    pub fn render_filter(self, filter_expr: &Expr) -> Result<String, UdfError> {
        Ok(null_keeps_no_row(self.render_filter_root(filter_expr)?))
    }

    /// A filter, a join condition, or one of their top-level conjuncts.
    pub(crate) fn render_filter_root(self, root: &Expr) -> Result<String, UdfError> {
        Ok(self.type_scan_filter(root)?.0)
    }

    /// [`render_expression_exasol`] for an output select item, with its [`Self::exact_type`].
    pub fn exasol_fragment(self, expr: &Expr) -> Result<SqlFragment, UdfError> {
        Ok(SqlFragment::new(
            render_expression_exasol(expr)?,
            self.exact_type(expr),
        ))
    }
}

fn scan_fragment((sql, inferred): (String, Inferred)) -> SqlFragment {
    let ty = inferred.exact().map(|ty| {
        if ty.is_character() {
            ExasolType::WIDEST_VARCHAR
        } else {
            ty
        }
    });
    SqlFragment::new(sql, ty)
}

/// The filter [`ScanTyping::render_filter`] renders for the `AND` of conjuncts rendered one by one.
pub fn conjoin_scan_filters(conjuncts: Vec<String>) -> Option<String> {
    if conjuncts.is_empty() {
        return None;
    }
    Some(null_keeps_no_row(junction_sql(conjuncts, " AND ", "TRUE")))
}

/// A `NULL` condition keeps no row, like `FALSE`. Exasol folds `''` to `NULL` too.
fn null_keeps_no_row(sql: String) -> String {
    if sql == "NULL" { "FALSE".into() } else { sql }
}

/// SQL parsed by Exasol itself (join and grouped-aggregate wrappers), never for a DataFusion
/// `ScanSpec`.
///
/// Fails only on a tree Exasol does not send, such as a function call of the wrong arity.
pub fn render_expression_exasol(expr: &Expr) -> Result<String, UdfError> {
    exasol_sql(expr)
}

/// A short description of `node` for an error message: its Exasol SQL where it has one.
pub fn short(node: &Expr) -> String {
    let text = exasol_sql(node).unwrap_or_else(|_| format!("{node:?}"));
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
