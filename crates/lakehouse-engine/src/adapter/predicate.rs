/// VS filter-expression tree → DataFusion SQL fragment.
///
/// Ported from strata-rs `predicate.rs` with DataFusion SQL quoting conventions.
/// Two modes:
/// - `render_df_filter`: raises on unsupported node types.
/// - `render_df_filter_safe`: returns None on any failure (safe fallback for
///   the adapter — Exasol keeps the predicate as a correctness backstop).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

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

fn render_expression(expr: &Json) -> Result<Option<String>, UdfError> {
    if expr.is_null() {
        return Ok(None);
    }
    if !expr.is_object() {
        return Err(UdfError::User(
            "unexpected predicate node (not an object)".into(),
        ));
    }
    let kind = match expr.get("type").and_then(|t| t.as_str()) {
        Some(k) => k,
        None => return Err(UdfError::User("predicate node missing 'type' field".into())),
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

    if let Some(op) = binary_op(kind) {
        let left = render_expression(value("left").unwrap_or(&Json::Null))?;
        let right = render_expression(value("right").unwrap_or(&Json::Null))?;
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
        "predicate_and" => render_junction(value("expressions"), " AND ", "TRUE").map(Some),
        "predicate_or" => render_junction(value("expressions"), " OR ", "FALSE").map(Some),
        "predicate_not" => {
            let inner = render_expression(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| UdfError::User("predicate_not missing 'expression'".into()))?;
            Ok(Some(format!("(NOT {inner})")))
        }
        "predicate_is_null" => {
            let inner = render_expression(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| UdfError::User("predicate_is_null missing 'expression'".into()))?;
            Ok(Some(format!("({inner} IS NULL)")))
        }
        "predicate_is_not_null" => {
            let inner = render_expression(value("expression").unwrap_or(&Json::Null))?.ok_or_else(
                || UdfError::User("predicate_is_not_null missing 'expression'".into()),
            )?;
            Ok(Some(format!("({inner} IS NOT NULL)")))
        }
        "predicate_in_constlist" => {
            let target = render_expression(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| {
                    UdfError::User("predicate_in_constlist missing 'expression'".into())
                })?;
            let mut rendered: Vec<String> = Vec::new();
            if let Some(Json::Array(args)) = value("arguments") {
                for arg in args {
                    if let Some(r) = render_expression(arg)? {
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
            let target = render_expression(value("expression").unwrap_or(&Json::Null))?;
            let low = render_expression(value("left").unwrap_or(&Json::Null))?;
            let high = render_expression(value("right").unwrap_or(&Json::Null))?;
            match (target, low, high) {
                (Some(t), Some(l), Some(h)) => Ok(Some(format!("({t} BETWEEN {l} AND {h})"))),
                _ => Err(UdfError::User(
                    "predicate_between requires expression/left/right".into(),
                )),
            }
        }
        "predicate_like" => {
            let left = render_expression(value("expression").unwrap_or(&Json::Null))?;
            let pattern = render_expression(value("pattern").unwrap_or(&Json::Null))?;
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
        other => Err(UdfError::User(format!(
            "unsupported predicate node type: {other}"
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
            let rendered = render_expression(expr)?;
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

/// Render a DataFusion SQL WHERE fragment from a VS filter expression.
/// Returns an error for unsupported node types.
///
/// Test-only: production uses `render_df_filter_safe`, the sole entry point that
/// omits (rather than errors on) untranslatable predicates.
#[cfg(test)]
pub fn render_df_filter(filter_expr: &Json) -> Result<String, UdfError> {
    Ok(render_expression(filter_expr)?.unwrap_or_else(|| "NULL".to_string()))
}

/// Render a DataFusion SQL WHERE fragment from a VS filter expression.
/// Returns None if rendering fails — the adapter will omit the filter from the
/// scan spec and let Exasol keep it as a correctness backstop.
pub fn render_df_filter_safe(filter_expr: &Json) -> Option<String> {
    let result = render_expression(filter_expr).ok()??;
    // Exclude trivially-true filters (no pushdown needed).
    if result == "TRUE" || result == "NULL" {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_simple_equality() {
        let expr = json!({"type": "predicate_equal", "left": {"type": "column", "name": "id"}, "right": {"type": "literal_exactnumeric", "value": 10}});
        let sql = render_df_filter(&expr).unwrap();
        assert_eq!(sql, r#"("ID" = 10)"#);
    }

    #[test]
    fn renders_and_predicate() {
        let expr = json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_greater", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 18}},
                {"type": "predicate_less", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 65}}
            ]
        });
        let sql = render_df_filter(&expr).unwrap();
        assert!(sql.contains("AND"), "AND predicate not rendered: {sql}");
    }

    #[test]
    fn unsupported_node_returns_none_in_safe_mode() {
        let expr = json!({"type": "fn_sum", "operands": []});
        let result = render_df_filter_safe(&expr);
        assert!(
            result.is_none(),
            "unsupported node should return None in safe mode"
        );
    }

    #[test]
    fn true_filter_returns_none_in_safe_mode() {
        let expr = json!({"type": "literal_bool", "value": true});
        assert!(render_df_filter_safe(&expr).is_none());
    }
}
