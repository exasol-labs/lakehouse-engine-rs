/// VS expression-tree → DataFusion SQL fragment translator.
///
/// Translates Exasol Virtual Schema pushdown expression-JSON nodes into
/// DataFusion SQL strings usable in WHERE clauses and GROUP BY clauses.
/// No SQL-parser dependency — only `serde_json` is used as the IR.
///
/// Public entry points:
/// - `render_expression` (raising): returns `Result<String, UdfError>`
/// - `render_expression_safe` (None-on-failure): returns `Option<String>`
/// - `render_df_filter_safe` (None-on-failure + trivially-true suppression)
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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

fn quote_literal(value: Option<&Json>) -> String {
    match value {
        None | Some(Json::Null) => "NULL".to_string(),
        Some(Json::String(s)) => format!("'{}'", sql_escape(s)),
        Some(other) => format!("'{}'", sql_escape(&json_scalar_to_string(other))),
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

/// Map a VS `dataType` JSON object to a DataFusion SQL type name.
fn render_cast_target(data_type: &Json) -> Result<String, UdfError> {
    let type_name = data_type.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match type_name.to_uppercase().as_str() {
        "VARCHAR" | "CHAR" => Ok("VARCHAR".to_string()),
        "DECIMAL" => {
            let p = data_type
                .get("precision")
                .and_then(|v| v.as_u64())
                .unwrap_or(18);
            let s = data_type.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            Ok(format!("DECIMAL({p},{s})"))
        }
        "DOUBLE" | "DOUBLE PRECISION" => Ok("DOUBLE".to_string()),
        "BOOLEAN" => Ok("BOOLEAN".to_string()),
        "DATE" => Ok("DATE".to_string()),
        "TIMESTAMP" => Ok("TIMESTAMP".to_string()),
        other => Err(UdfError::User(format!(
            "unsupported CAST target type: {other}"
        ))),
    }
}

/// Internal recursive translator.
///
/// Returns `Ok(None)` when `expr` is `Json::Null` (absent optional child).
/// Returns `Ok(Some(sql))` on success.
/// Returns `Err(UdfError::User(...))` for unsupported or malformed nodes.
fn render_expression_inner(expr: &Json) -> Result<Option<String>, UdfError> {
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

    // --- Literals ---
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
            return Ok(Some(format!("TIMESTAMP {}", quote_literal(value("value")))));
        }
        "column" => {
            let name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            return Ok(Some(quote_ident(&name)));
        }
        _ => {}
    }

    // --- Binary comparison predicates ---
    if let Some(op) = binary_op(kind) {
        let left = render_expression_inner(value("left").unwrap_or(&Json::Null))?;
        let right = render_expression_inner(value("right").unwrap_or(&Json::Null))?;
        match (left, right) {
            (Some(l), Some(r)) => return Ok(Some(format!("({l} {op} {r})"))),
            _ => {
                return Err(UdfError::User(format!(
                    "binary predicate '{kind}' missing operand"
                )));
            }
        }
    }

    // --- Logical connectives and other predicates ---
    match kind {
        "predicate_and" => render_junction(value("expressions"), " AND ", "TRUE").map(Some),
        "predicate_or" => render_junction(value("expressions"), " OR ", "FALSE").map(Some),
        "predicate_not" => {
            let inner = render_expression_inner(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| UdfError::User("predicate_not missing 'expression'".into()))?;
            Ok(Some(format!("(NOT {inner})")))
        }
        "predicate_is_null" => {
            let inner = render_expression_inner(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| UdfError::User("predicate_is_null missing 'expression'".into()))?;
            Ok(Some(format!("({inner} IS NULL)")))
        }
        "predicate_is_not_null" => {
            let inner = render_expression_inner(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| {
                    UdfError::User("predicate_is_not_null missing 'expression'".into())
                })?;
            Ok(Some(format!("({inner} IS NOT NULL)")))
        }
        "predicate_in_constlist" => {
            let target = render_expression_inner(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| {
                    UdfError::User("predicate_in_constlist missing 'expression'".into())
                })?;
            let mut rendered: Vec<String> = Vec::new();
            if let Some(Json::Array(args)) = value("arguments") {
                for arg in args {
                    if let Some(r) = render_expression_inner(arg)? {
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
            let target = render_expression_inner(value("expression").unwrap_or(&Json::Null))?;
            let low = render_expression_inner(value("left").unwrap_or(&Json::Null))?;
            let high = render_expression_inner(value("right").unwrap_or(&Json::Null))?;
            match (target, low, high) {
                (Some(t), Some(l), Some(h)) => Ok(Some(format!("({t} BETWEEN {l} AND {h})"))),
                _ => Err(UdfError::User(
                    "predicate_between requires expression/left/right".into(),
                )),
            }
        }
        "predicate_like" => {
            let left = render_expression_inner(value("expression").unwrap_or(&Json::Null))?;
            let pattern = render_expression_inner(value("pattern").unwrap_or(&Json::Null))?;
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
        "function_scalar" => {
            let fn_name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            let args = value("arguments").and_then(|a| a.as_array());

            match fn_name.as_str() {
                // Arithmetic binary operators
                "ADD" | "SUB" | "MUL" | "FLOAT_DIV" => {
                    let op = match fn_name.as_str() {
                        "ADD" => "+",
                        "SUB" => "-",
                        "MUL" => "*",
                        "FLOAT_DIV" => "/",
                        _ => unreachable!(),
                    };
                    let args = args.ok_or_else(|| {
                        UdfError::User(format!("function_scalar {fn_name} missing 'arguments'"))
                    })?;
                    if args.len() < 2 {
                        return Err(UdfError::User(format!(
                            "function_scalar {fn_name} requires 2 arguments, got {}",
                            args.len()
                        )));
                    }
                    let left = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} left operand is null")))?;
                    let right = render_expression_inner(&args[1])?.ok_or_else(|| {
                        UdfError::User(format!("{fn_name} right operand is null"))
                    })?;
                    Ok(Some(format!("({left} {op} {right})")))
                }
                // Unary negation
                "NEG" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar NEG missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar NEG requires 1 argument".into(),
                        ));
                    }
                    let operand = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("NEG operand is null".into()))?;
                    Ok(Some(format!("(-{operand})")))
                }
                // CAST
                "CAST" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar CAST missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar CAST requires 1 argument".into(),
                        ));
                    }
                    let inner = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("CAST expression is null".into()))?;
                    let data_type = value("dataType").ok_or_else(|| {
                        UdfError::User("function_scalar CAST missing 'dataType'".into())
                    })?;
                    let target_type = render_cast_target(data_type)?;
                    Ok(Some(format!("CAST({inner} AS {target_type})")))
                }
                other => Err(UdfError::User(format!(
                    "unsupported scalar function: {other}"
                ))),
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
) -> Result<String, UdfError> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(Json::Array(items)) = expressions {
        for expr in items {
            let rendered = render_expression_inner(expr)?;
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

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Render a VS expression node to a DataFusion SQL fragment.
///
/// Raises on unsupported or malformed nodes.
pub fn render_expression(expr: &Json) -> Result<String, UdfError> {
    render_expression_inner(expr)?.ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// Render a VS expression node to a DataFusion SQL fragment.
///
/// Returns `None` on any failure (unsupported node types, malformed input).
/// Never panics.
pub fn render_expression_safe(expr: &Json) -> Option<String> {
    render_expression_inner(expr).ok()?
}

/// Render a VS filter expression to a DataFusion SQL WHERE fragment.
///
/// Returns `None` when:
/// - rendering fails (unsupported node types, malformed input), or
/// - the filter is trivially true (`TRUE` or `NULL`) — the adapter omits
///   it from the scan spec and lets Exasol keep it as a correctness backstop.
pub fn render_df_filter_safe(filter_expr: &Json) -> Option<String> {
    let result = render_expression_inner(filter_expr).ok()??;
    if result == "TRUE" || result == "NULL" {
        None
    } else {
        Some(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- Column ---

    #[test]
    fn renders_column_as_quoted_uppercase_ident() {
        let expr = json!({"type": "column", "name": "region"});
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, r#""REGION""#);
    }

    #[test]
    fn renders_column_with_embedded_quotes() {
        let expr = json!({"type": "column", "name": r#"my"col"#});
        let sql = render_expression(&expr).unwrap();
        // embedded " must be doubled
        assert_eq!(sql, r#""MY""COL""#);
    }

    // --- Literals ---

    #[test]
    fn renders_string_literal() {
        let expr = json!({"type": "literal_string", "value": "hello"});
        assert_eq!(render_expression(&expr).unwrap(), "'hello'");
    }

    #[test]
    fn renders_string_literal_with_single_quote_escaped() {
        let expr = json!({"type": "literal_string", "value": "it's"});
        assert_eq!(render_expression(&expr).unwrap(), "'it''s'");
    }

    #[test]
    fn renders_null_literal() {
        let expr = json!({"type": "literal_null"});
        assert_eq!(render_expression(&expr).unwrap(), "NULL");
    }

    #[test]
    fn renders_bool_literal() {
        let t = json!({"type": "literal_bool", "value": true});
        let f = json!({"type": "literal_bool", "value": false});
        assert_eq!(render_expression(&t).unwrap(), "TRUE");
        assert_eq!(render_expression(&f).unwrap(), "FALSE");
    }

    #[test]
    fn renders_numeric_literal() {
        let expr = json!({"type": "literal_exactnumeric", "value": 42});
        assert_eq!(render_expression(&expr).unwrap(), "42");
    }

    #[test]
    fn renders_date_literal() {
        let expr = json!({"type": "literal_date", "value": "2024-01-15"});
        assert_eq!(render_expression(&expr).unwrap(), "DATE '2024-01-15'");
    }

    #[test]
    fn renders_timestamp_literal() {
        let expr = json!({"type": "literal_timestamp", "value": "2024-01-15 12:00:00"});
        assert_eq!(
            render_expression(&expr).unwrap(),
            "TIMESTAMP '2024-01-15 12:00:00'"
        );
    }

    // --- Comparison predicates ---

    #[test]
    fn renders_simple_equality() {
        let expr = json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "id"},
            "right": {"type": "literal_exactnumeric", "value": 10}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("ID" = 10)"#);
    }

    // --- Logical connectives ---

    #[test]
    fn renders_and_predicate() {
        let expr = json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_greater", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 18}},
                {"type": "predicate_less", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 65}}
            ]
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("AND"), "AND not found in: {sql}");
    }

    #[test]
    fn renders_or_predicate() {
        let expr = json!({
            "type": "predicate_or",
            "expressions": [
                {"type": "predicate_equal", "left": {"type": "column", "name": "status"}, "right": {"type": "literal_string", "value": "A"}},
                {"type": "predicate_equal", "left": {"type": "column", "name": "status"}, "right": {"type": "literal_string", "value": "B"}}
            ]
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("OR"), "OR not found in: {sql}");
    }

    #[test]
    fn renders_not_predicate() {
        let expr = json!({
            "type": "predicate_not",
            "expression": {"type": "predicate_equal", "left": {"type": "column", "name": "active"}, "right": {"type": "literal_bool", "value": true}}
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("NOT"), "NOT not found in: {sql}");
    }

    #[test]
    fn renders_empty_and_as_true() {
        let expr = json!({"type": "predicate_and", "expressions": []});
        assert_eq!(render_expression(&expr).unwrap(), "TRUE");
    }

    #[test]
    fn renders_empty_or_as_false() {
        let expr = json!({"type": "predicate_or", "expressions": []});
        assert_eq!(render_expression(&expr).unwrap(), "FALSE");
    }

    // --- IS NULL / IS NOT NULL ---

    #[test]
    fn renders_is_null() {
        let expr =
            json!({"type": "predicate_is_null", "expression": {"type": "column", "name": "x"}});
        assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NULL)"#);
    }

    #[test]
    fn renders_is_not_null() {
        let expr =
            json!({"type": "predicate_is_not_null", "expression": {"type": "column", "name": "x"}});
        assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NOT NULL)"#);
    }

    // --- IN ---

    #[test]
    fn renders_in_constlist() {
        let expr = json!({
            "type": "predicate_in_constlist",
            "expression": {"type": "column", "name": "status"},
            "arguments": [
                {"type": "literal_string", "value": "A"},
                {"type": "literal_string", "value": "B"}
            ]
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("IN"), "IN not found: {sql}");
        assert!(sql.contains("'A'"), "'A' not found: {sql}");
        assert!(sql.contains("'B'"), "'B' not found: {sql}");
    }

    #[test]
    fn renders_empty_in_as_false() {
        let expr = json!({
            "type": "predicate_in_constlist",
            "expression": {"type": "column", "name": "x"},
            "arguments": []
        });
        assert_eq!(render_expression(&expr).unwrap(), "FALSE");
    }

    // --- BETWEEN ---

    #[test]
    fn renders_between() {
        let expr = json!({
            "type": "predicate_between",
            "expression": {"type": "column", "name": "age"},
            "left": {"type": "literal_exactnumeric", "value": 18},
            "right": {"type": "literal_exactnumeric", "value": 65}
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("BETWEEN"), "BETWEEN not found: {sql}");
        assert!(sql.contains("18"), "low bound not found: {sql}");
        assert!(sql.contains("65"), "high bound not found: {sql}");
    }

    // --- LIKE ---

    #[test]
    fn renders_like_without_escape() {
        let expr = json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "A%"}
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
        assert!(!sql.contains("ESCAPE"), "ESCAPE should be absent: {sql}");
    }

    #[test]
    fn renders_like_with_escape() {
        let expr = json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "A!%"},
            "escape_char": "!"
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
        assert!(sql.contains("ESCAPE"), "ESCAPE not found: {sql}");
        assert!(sql.contains("'!'"), "escape char not found: {sql}");
    }

    // --- Arithmetic ---

    #[test]
    fn renders_arithmetic_add() {
        let expr = json!({
            "type": "function_scalar",
            "name": "ADD",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" + 1)"#);
    }

    #[test]
    fn renders_arithmetic_sub() {
        let expr = json!({
            "type": "function_scalar",
            "name": "SUB",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" - 1)"#);
    }

    #[test]
    fn renders_arithmetic_mul() {
        let expr = json!({
            "type": "function_scalar",
            "name": "MUL",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" * 2)"#);
    }

    #[test]
    fn renders_arithmetic_div() {
        let expr = json!({
            "type": "function_scalar",
            "name": "FLOAT_DIV",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" / 2)"#);
    }

    #[test]
    fn renders_arithmetic_neg() {
        let expr = json!({
            "type": "function_scalar",
            "name": "NEG",
            "arguments": [
                {"type": "column", "name": "a"}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"(-"A")"#);
    }

    // --- CAST ---

    #[test]
    fn renders_cast_varchar() {
        let expr = json!({
            "type": "function_scalar",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "VARCHAR", "size": 100}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
    }

    #[test]
    fn renders_cast_decimal() {
        let expr = json!({
            "type": "function_scalar",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "DECIMAL", "precision": 10, "scale": 2}
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS DECIMAL(10,2))"#
        );
    }

    #[test]
    fn renders_cast_double() {
        let expr = json!({
            "type": "function_scalar",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "DOUBLE"}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DOUBLE)"#);
    }

    #[test]
    fn renders_cast_date() {
        let expr = json!({
            "type": "function_scalar",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "DATE"}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DATE)"#);
    }

    // --- Error / safe-mode ---

    #[test]
    fn unsupported_node_returns_error() {
        let expr = json!({"type": "fn_sum", "operands": []});
        let err = render_expression(&expr).unwrap_err();
        assert!(
            err.to_string().contains("fn_sum"),
            "error must name the unsupported type: {err}"
        );
    }

    #[test]
    fn unsupported_node_returns_none_in_safe_mode() {
        let expr = json!({"type": "fn_sum", "operands": []});
        assert!(render_expression_safe(&expr).is_none());
    }

    #[test]
    fn true_filter_returns_none_in_safe_mode() {
        let expr = json!({"type": "literal_bool", "value": true});
        assert!(render_df_filter_safe(&expr).is_none());
    }

    #[test]
    fn null_filter_returns_none_in_safe_mode() {
        let expr = json!({"type": "literal_null"});
        assert!(render_df_filter_safe(&expr).is_none());
    }
}
