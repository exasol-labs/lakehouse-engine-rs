//! Translate an OPA Compile API (`POST /v1/compile`) partial-evaluation result
//! into a DataFusion WHERE predicate.
//!
//! This is the layer the spike concluded was missing. It replaces "trust a SQL
//! string from the policy" with "compile a structured AST we understand".
//!
//! Result shape, verified against OPA 1.20.2 (evidence/08):
//!   {"result": {"queries": [[expr, expr], [expr]]}}   -> OR of ANDs
//!   {"result": {"queries": [[]]}}                     -> unconditionally TRUE
//!   {"result": {}}                                    -> unconditionally FALSE
//!
//! An expression is `{"index": n, "terms": [<operator>, <operand>, <operand>],
//! "negated": bool?}`.

use serde_json::Value;

/// What the policy decided, as a three-way answer the caller cannot confuse.
#[derive(Debug, PartialEq)]
pub enum Decision {
    /// No residual condition: the user may see every row.
    AllowAll,
    /// Unsatisfiable: the user may see no row. This is the DENY case, and it is
    /// what an absent `queries` key means.
    DenyAll,
    /// A DataFusion WHERE predicate restricting the scan to permitted rows.
    Filter(String),
}

/// Why a policy could not be compiled. Every variant MUST be surfaced as a
/// refusal, never as a missing filter.
#[derive(Debug)]
pub struct Unsupported(pub String);

/// Operators this layer accepts. Anything else is refused by name, which is the
/// property the SQL-string contract could not offer.
fn sql_operator(name: &str) -> Option<&'static str> {
    match name {
        "eq" | "equal" => Some("="),
        "neq" => Some("<>"),
        "lt" => Some("<"),
        "lte" => Some("<="),
        "gt" => Some(">"),
        "gte" => Some(">="),
        _ => None,
    }
}

/// Mirror an operator when the column ends up on the right-hand side, so
/// `"EU" < input.row.x` compiles to `"X" > 'EU'` rather than being reversed.
fn mirror(op: &str) -> &'static str {
    match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        "=" => "=",
        "<>" => "<>",
        other => unreachable!("unmirrorable operator {other}"),
    }
}

fn operator_name(term: &Value) -> Option<String> {
    let parts = term.get("value")?.as_array()?;
    let names: Vec<String> = parts
        .iter()
        .map(|p| p.get("value").and_then(Value::as_str).unwrap_or("?").to_string())
        .collect();
    Some(names.join("."))
}

/// A `ref` term rooted at `input.row` denotes a column. The engine's scan
/// exposes every column as a quoted UPPERCASE alias (`build_alias_items`), so
/// that is what we emit. Quoting is what makes the identifier immune to the
/// case-normalisation that broke every filter in PROBE 1.
fn column_of(term: &Value, unknown_root: &str) -> Option<String> {
    if term.get("type")?.as_str()? != "ref" {
        return None;
    }
    let parts = term.get("value")?.as_array()?;
    let path: Vec<&str> = parts
        .iter()
        .map(|p| p.get("value").and_then(Value::as_str).unwrap_or("?"))
        .collect();
    let expected: Vec<&str> = unknown_root.split('.').collect();
    if path.len() != expected.len() + 1 || path[..expected.len()] != expected[..] {
        return None;
    }
    Some(format!("\"{}\"", path[expected.len()].to_uppercase().replace('"', "\"\"")))
}

/// Render a scalar literal. Strings are escaped by doubling the quote, never by
/// interpolation, so a literal can neither close the string nor start a comment.
fn literal_of(term: &Value) -> Result<String, Unsupported> {
    let ty = term.get("type").and_then(Value::as_str).unwrap_or("?");
    let v = term.get("value").unwrap_or(&Value::Null);
    match ty {
        "string" => Ok(format!("'{}'", v.as_str().unwrap_or("").replace('\'', "''"))),
        "number" => Ok(v.to_string()),
        "boolean" => Ok(if v.as_bool().unwrap_or(false) { "TRUE" } else { "FALSE" }.into()),
        "null" => Ok("NULL".into()),
        other => Err(Unsupported(format!("literal type `{other}`"))),
    }
}

/// Render a set or array literal as a parenthesised IN list.
fn in_list_of(term: &Value) -> Result<String, Unsupported> {
    let ty = term.get("type").and_then(Value::as_str).unwrap_or("?");
    if ty != "set" && ty != "array" {
        return Err(Unsupported(format!("`in` right-hand side is `{ty}`, not a set")));
    }
    let items = term
        .get("value")
        .and_then(Value::as_array)
        .ok_or_else(|| Unsupported("malformed set".into()))?;
    if items.is_empty() {
        // `x in {}` is unsatisfiable. Say so rather than emitting `IN ()`.
        return Ok(String::new());
    }
    let mut rendered: Vec<String> = Vec::new();
    for i in items {
        rendered.push(literal_of(i)?);
    }
    rendered.sort(); // OPA emits sets unordered; sort for a stable predicate.
    Ok(format!("({})", rendered.join(", ")))
}

fn compile_expr(expr: &Value, unknown_root: &str) -> Result<String, Unsupported> {
    let terms = expr
        .get("terms")
        .and_then(Value::as_array)
        .ok_or_else(|| Unsupported("expression has no `terms` array".into()))?;
    if terms.len() != 3 {
        return Err(Unsupported(format!(
            "only binary expressions are supported, got {} terms",
            terms.len()
        )));
    }
    let op_name = operator_name(&terms[0])
        .ok_or_else(|| Unsupported("operator term is not a ref".into()))?;
    let negated = expr.get("negated").and_then(Value::as_bool).unwrap_or(false);

    let rendered = if op_name == "internal.member_2" {
        // Rego `x in <set>`.
        let col = column_of(&terms[1], unknown_root).ok_or_else(|| {
            Unsupported("`in` left-hand side is not a column of the unknown row".into())
        })?;
        match in_list_of(&terms[2])? {
            s if s.is_empty() => "FALSE".to_string(),
            list => format!("{col} IN {list}"),
        }
    } else {
        let op = sql_operator(&op_name)
            .ok_or_else(|| Unsupported(format!("operator `{op_name}` is not translatable")))?;
        // The column may be on either side; OPA normalises constants leftward.
        match (
            column_of(&terms[1], unknown_root),
            column_of(&terms[2], unknown_root),
        ) {
            (Some(col), None) => format!("{col} {op} {}", literal_of(&terms[2])?),
            (None, Some(col)) => format!("{col} {} {}", mirror(op), literal_of(&terms[1])?),
            (Some(a), Some(b)) => format!("{a} {op} {b}"),
            (None, None) => {
                return Err(Unsupported(format!(
                    "expression `{op_name}` references no column of the unknown row"
                )))
            }
        }
    };
    Ok(if negated {
        format!("NOT ({rendered})")
    } else {
        rendered
    })
}

/// Compile a whole `/v1/compile` response body.
pub fn compile(body: &Value, unknown_root: &str) -> Result<Decision, Unsupported> {
    let result = body
        .get("result")
        .ok_or_else(|| Unsupported("response has no `result`".into()))?;

    // No `queries` key at all: the policy is unsatisfiable for this input.
    // Verified with an explicit `1 == 2` control (evidence/08).
    let Some(queries) = result.get("queries").and_then(Value::as_array) else {
        if result.get("support").is_some() {
            return Err(Unsupported(
                "response carries a `support` module (policy uses a default rule or \
                 recursion); not translatable by this layer"
                    .into(),
            ));
        }
        return Ok(Decision::DenyAll);
    };
    if queries.is_empty() {
        return Ok(Decision::DenyAll);
    }

    let mut disjuncts: Vec<String> = Vec::new();
    for q in queries {
        let conjuncts = q
            .as_array()
            .ok_or_else(|| Unsupported("query is not an array".into()))?;
        // An EMPTY conjunction is `true`, so the whole disjunction is `true`.
        if conjuncts.is_empty() {
            return Ok(Decision::AllowAll);
        }
        let mut parts: Vec<String> = Vec::new();
        for e in conjuncts {
            parts.push(compile_expr(e, unknown_root)?);
        }
        disjuncts.push(format!("({})", parts.join(" AND ")));
    }
    Ok(Decision::Filter(disjuncts.join(" OR ")))
}
