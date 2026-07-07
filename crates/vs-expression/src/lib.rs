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

/// Render a slice of argument nodes, returning an error if any fails.
fn render_args(args: &[Json]) -> Result<Vec<String>, UdfError> {
    args.iter()
        .enumerate()
        .map(|(i, arg)| {
            render_expression_inner(arg)?
                .ok_or_else(|| UdfError::User(format!("argument[{i}] rendered to null")))
        })
        .collect()
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
        "literal_timestamp_utc" => {
            // Append +00:00 so DataFusion parses it as a UTC timestamp-with-timezone.
            let raw = match value("value") {
                None | Some(Json::Null) => return Ok(Some("NULL".into())),
                Some(v) => json_scalar_to_string(v),
            };
            return Ok(Some(format!("TIMESTAMP '{raw}+00:00'")));
        }
        "column" => {
            let name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            let quoted = quote_ident(&name);
            // A `tableAlias` (injected by the caller for a multi-table render, e.g.
            // the join two-scan wrapper) qualifies the reference as
            // `"ALIAS"."NAME"`, disambiguating a name shared by two joined subqueries.
            // Absent (the default single-table path), a bare quoted name is rendered
            // exactly as before.
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
        // Exasol sends REGEXP_LIKE as node type `predicate_like_regexp`.
        "predicate_like_regexp" => {
            let subject = render_expression_inner(value("expression").unwrap_or(&Json::Null))?
                .ok_or_else(|| {
                    UdfError::User("predicate_like_regexp missing 'expression'".into())
                })?;
            let pattern = render_expression_inner(value("pattern").unwrap_or(&Json::Null))?
                .ok_or_else(|| UdfError::User("predicate_like_regexp missing 'pattern'".into()))?;
            Ok(Some(format!("regexp_like({subject}, {pattern})")))
        }
        // Exasol sends EXTRACT as its own node type with the field in `toExtract`:
        // {"type":"function_scalar_extract","name":"EXTRACT","toExtract":"DAY","arguments":[<src>]}
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
            let src = render_expression_inner(&args[0])?
                .ok_or_else(|| UdfError::User("EXTRACT source is null".into()))?;
            // DataFusion 54 (default features) has no EXTRACT(field FROM expr) ExprPlanner;
            // render the portable function form date_part('FIELD', expr) instead.
            Ok(Some(format!("date_part('{field}', {src})")))
        }
        // Exasol encodes CASE (and CASE-expanded functions like NULLIF/ZEROIFNULL) as
        // its own node type:
        //   {"type":"function_scalar_case","name":"CASE",
        //    "basis": <operand>?,            // present → "simple" CASE basis WHEN arg
        //    "arguments": [<when>, ...],     // WHEN comparison values / predicates
        //    "results":   [<then>, ..., <else>?]} // one THEN per WHEN; trailing = ELSE
        // Rendered to SQL CASE; with `basis` it is `CASE basis WHEN arg THEN res ...`,
        // without it the WHEN arguments are boolean predicates (`CASE WHEN pred ...`).
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
                    render_expression_inner(b)?
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
                let when_sql = render_expression_inner(when)?
                    .ok_or_else(|| UdfError::User("CASE WHEN value is null".into()))?;
                let then_sql = render_expression_inner(&results[i])?
                    .ok_or_else(|| UdfError::User("CASE THEN result is null".into()))?;
                sql.push_str(&format!(" WHEN {when_sql} THEN {then_sql}"));
            }
            if results.len() == whens.len() + 1 {
                let else_sql = render_expression_inner(&results[whens.len()])?
                    .ok_or_else(|| UdfError::User("CASE ELSE result is null".into()))?;
                sql.push_str(&format!(" ELSE {else_sql}"));
            }
            sql.push_str(" END");
            Ok(Some(format!("({sql})")))
        }
        "function_scalar" => {
            let fn_name = value("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_uppercase();
            let args = value("arguments").and_then(|a| a.as_array());

            match fn_name.as_str() {
                // Arithmetic binary operators. The `function_scalar` node name Exasol
                // emits equals the advertised capability name with `FN_` stripped
                // (verified live via FN_MOD; see decision-log entry [7]). These four
                // must stay in lockstep with capabilities.rs's FN_ADD / FN_SUB /
                // FN_MULT / FN_FLOAT_DIV — in particular multiplication is `MULT`
                // (from FN_MULT), NOT `MUL`. CAST (below) is still translated but not
                // advertised as a capability.
                "ADD" | "SUB" | "MULT" | "FLOAT_DIV" => {
                    let op = match fn_name.as_str() {
                        "ADD" => "+",
                        "SUB" => "-",
                        "MULT" => "*",
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
                // REGEXP_LIKE as a function_scalar (alternate encoding)
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
                    let subject = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("REGEXP_LIKE subject is null".into()))?;
                    let pattern = render_expression_inner(&args[1])?
                        .ok_or_else(|| UdfError::User("REGEXP_LIKE pattern is null".into()))?;
                    Ok(Some(format!("regexp_like({subject}, {pattern})")))
                }
                // Math functions: name-mapping table
                // Arity: 1-arg: ABS FLOOR CEIL SQRT EXP LN SIGN DEGREES RADIANS SIN COS TAN ASIN
                //               ACOS ATAN SINH COSH TANH COT
                // 1-or-2-arg: ROUND TRUNC LOG
                // 2-arg: POWER ATAN2
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
                        // SIGN → signum: DataFusion uses "signum" not "sign".
                        "SIGN" => "signum",
                        other => {
                            lower = other.to_lowercase();
                            &lower
                        }
                    };
                    let arg = render_expression_inner(&args[0])?
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
                    let rendered = render_args(args)?;
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
                    let rendered = render_args(args)?;
                    Ok(Some(format!("{df_name}({})", rendered.join(", "))))
                }
                // MOD → (<l> % <r>) — DataFusion 54 exposes modulo only as the % operator
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
                    let left = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("MOD left operand is null".into()))?;
                    let right = render_expression_inner(&args[1])?
                        .ok_or_else(|| UdfError::User("MOD right operand is null".into()))?;
                    Ok(Some(format!("({left} % {right})")))
                }
                // String functions: name-mapping table
                "CONCAT" | "LOWER" | "UPPER" | "SUBSTR" | "TRIM" | "LTRIM" | "RTRIM"
                | "REPLACE" | "REPEAT" | "REVERSE" | "LPAD" | "RPAD" | "ASCII" | "CHR"
                | "INITCAP" | "LEFT" | "RIGHT" | "TRANSLATE" | "LENGTH" | "OCTET_LENGTH"
                | "UNICODE" | "UNICODECHR" => {
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
                    let rendered = render_args(args)?;
                    Ok(Some(format!("{df_name}({})", rendered.join(", "))))
                }
                // INSTR(string, substring) and LOCATE(substring, string) both → strpos(string, substring)
                // INSTR: arg[0]=string, arg[1]=substring → strpos(arg[0], arg[1])
                // LOCATE: arg[0]=substring, arg[1]=string → strpos(arg[1], arg[0])
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
                    let string = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("INSTR string arg is null".into()))?;
                    let substr = render_expression_inner(&args[1])?
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
                    // Exasol LOCATE(substring, string) — reorder to strpos(string, substring)
                    let substr = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("LOCATE substring arg is null".into()))?;
                    let string = render_expression_inner(&args[1])?
                        .ok_or_else(|| UdfError::User("LOCATE string arg is null".into()))?;
                    Ok(Some(format!("strpos({string}, {substr})")))
                }
                // CASE WHEN ... THEN ... [ELSE ...] END
                // Exasol encodes CASE as function_scalar "CASE" with arguments interleaved:
                //   [cond1, result1, cond2, result2, ..., else_result (if odd count)]
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
                        let cond = render_expression_inner(&args[i])?.ok_or_else(|| {
                            UdfError::User(format!("CASE WHEN cond[{i}] is null"))
                        })?;
                        let result = render_expression_inner(&args[i + 1])?.ok_or_else(|| {
                            UdfError::User(format!("CASE THEN result[{}] is null", i + 1))
                        })?;
                        sql.push_str(&format!(" WHEN {cond} THEN {result}"));
                        i += 2;
                    }
                    if has_else {
                        let else_val = render_expression_inner(&args[args.len() - 1])?
                            .ok_or_else(|| UdfError::User("CASE ELSE value is null".into()))?;
                        sql.push_str(&format!(" ELSE {else_val}"));
                    }
                    sql.push_str(" END");
                    Ok(Some(sql))
                }
                // GREATEST / LEAST
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
                    let rendered = render_args(args)?;
                    Ok(Some(format!("{df_name}({})", rendered.join(", "))))
                }
                // NULLIFZERO / ZEROIFNULL
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
                    let arg = render_expression_inner(&args[0])?
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
                    let arg = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("ZEROIFNULL argument is null".into()))?;
                    Ok(Some(format!("coalesce({arg}, 0)")))
                }
                // NULLIF(a, b) → nullif(a, b): returns NULL when a = b, else a.
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
                    let left = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("NULLIF first argument is null".into()))?;
                    let right = render_expression_inner(&args[1])?
                        .ok_or_else(|| UdfError::User("NULLIF second argument is null".into()))?;
                    Ok(Some(format!("nullif({left}, {right})")))
                }
                // Field-shortcut date functions: YEAR(col) → date_part('YEAR', col)
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
                    let src = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} argument is null")))?;
                    Ok(Some(format!("date_part('{fn_name}', {src})")))
                }
                // DATE_TRUNC(unit, source) — note: Exasol arg order matches DataFusion
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
                    let unit = render_expression_inner(&args[0])?
                        .ok_or_else(|| UdfError::User("DATE_TRUNC unit is null".into()))?;
                    let src = render_expression_inner(&args[1])?
                        .ok_or_else(|| UdfError::User("DATE_TRUNC source is null".into()))?;
                    Ok(Some(format!("date_trunc({unit}, {src})")))
                }
                // Now-family: zero-argument date/time functions
                "CURRENT_DATE" | "SYSDATE" => Ok(Some("current_date()".into())),
                "CURRENT_TIMESTAMP" | "SYSTIMESTAMP" => Ok(Some("now()".into())),
                // TO_DATE / TO_TIMESTAMP — forward all args (source + optional format)
                "TO_DATE" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar TO_DATE missing 'arguments'".into())
                    })?;
                    if args.is_empty() {
                        return Err(UdfError::User(
                            "function_scalar TO_DATE requires at least 1 argument".into(),
                        ));
                    }
                    let rendered = render_args(args)?;
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
                    let rendered = render_args(args)?;
                    Ok(Some(format!("to_timestamp({})", rendered.join(", "))))
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

    #[test]
    fn renders_table_qualified_column_when_alias_present() {
        let expr = json!({"type": "column", "name": "id", "tableAlias": "LHS_FACT"});
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, r#""LHS_FACT"."ID""#);
    }

    #[test]
    fn empty_table_alias_falls_back_to_bare_column() {
        let expr = json!({"type": "column", "name": "id", "tableAlias": ""});
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, r#""ID""#);
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
        // Exasol emits the multiplication node as "MULT" (from capability FN_MULT),
        // not "MUL" — verified via the FN_-strip convention in decision-log [7].
        let expr = json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" * 2)"#);
    }

    /// Regression guard: the legacy node name "MUL" must NOT be recognized. Exasol
    /// never emits it (the capability is FN_MULT → node "MULT"); if the match arm
    /// ever regresses back to "MUL", the advertised set and the translator would
    /// silently diverge and multiplication pushdown would fall back to a row scan.
    #[test]
    fn legacy_mul_name_is_not_recognized() {
        let expr = json!({
            "type": "function_scalar",
            "name": "MUL",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert!(
            render_expression_safe(&expr).is_none(),
            "the obsolete \"MUL\" node name must not translate; Exasol emits \"MULT\""
        );
    }

    /// Two-column binary arithmetic (both operands are column references), the exact
    /// NQ1 shape `L_EXTENDEDPRICE * L_DISCOUNT`. This is what unblocks the two-column
    /// SUM(col * col) pushdown once FN_MULT is advertised (capabilities.rs, task 1.2):
    /// the expression-argument aggregate path renders this fragment for the scan SQL.
    #[test]
    fn renders_two_column_arithmetic_product() {
        let expr = json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [
                {"type": "column", "name": "l_extendedprice"},
                {"type": "column", "name": "l_discount"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#
        );
    }

    /// Lockstep guard (translator side): the arithmetic binary-operator node names the
    /// translator recognizes must correspond 1:1 to the arithmetic capabilities
    /// advertised in `crates/lakehouse-engine/src/adapter/capabilities.rs`
    /// (`FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`) — each capability name with the
    /// `FN_` prefix stripped. If capabilities advertises an operator the translator
    /// doesn't render (or renders a name that isn't advertised), Exasol either declines
    /// the pushdown (silent row-scan fallback, no speedup) or the fragment never reaches
    /// a live query. Both operands are columns to exercise the two-column shape.
    ///
    /// The advertised capability strings live in a different crate; the authoritative
    /// cross-crate assertion (reading `CAPABILITIES` and driving `render_expression`)
    /// is deferred until task 1.2 populates the const — see decision-log deferred note.
    /// This table is the translator-side half kept in sync by construction.
    #[test]
    fn arithmetic_operator_set_matches_advertised_capabilities() {
        // (capability name, node name = capability minus FN_, rendered operator)
        let arithmetic = [
            ("FN_ADD", "ADD", "+"),
            ("FN_SUB", "SUB", "-"),
            ("FN_MULT", "MULT", "*"),
            ("FN_FLOAT_DIV", "FLOAT_DIV", "/"),
        ];
        for (cap, node, op) in arithmetic {
            // node name must be the capability with the FN_ prefix removed
            assert_eq!(
                node,
                cap.strip_prefix("FN_").unwrap(),
                "node name must equal capability {cap} minus FN_ prefix"
            );
            let expr = json!({
                "type": "function_scalar",
                "name": node,
                "arguments": [
                    {"type": "column", "name": "l_extendedprice"},
                    {"type": "column", "name": "l_discount"}
                ]
            });
            assert_eq!(
                render_expression(&expr).unwrap(),
                format!(r#"("L_EXTENDEDPRICE" {op} "L_DISCOUNT")"#),
                "translator must render advertised capability {cap} (node {node})"
            );
        }
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

    // --- UTC timestamp literal ---

    #[test]
    fn renders_timestamp_utc_literal() {
        let expr = json!({"type": "literal_timestamp_utc", "value": "2024-03-01 10:00:00"});
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, "TIMESTAMP '2024-03-01 10:00:00+00:00'");
    }

    // --- REGEXP_LIKE predicate and function_scalar ---

    #[test]
    fn renders_regexp_like() {
        // Test as predicate node (Exasol's infix REGEXP_LIKE encoding)
        let expr = json!({
            "type": "predicate_like_regexp",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "^A.*"}
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, r#"regexp_like("NAME", '^A.*')"#);

        // Test as function_scalar REGEXP_LIKE
        let expr2 = json!({
            "type": "function_scalar",
            "name": "REGEXP_LIKE",
            "arguments": [
                {"type": "column", "name": "name"},
                {"type": "literal_string", "value": "^B.*"}
            ]
        });
        let sql2 = render_expression(&expr2).unwrap();
        assert_eq!(sql2, r#"regexp_like("NAME", '^B.*')"#);
    }

    // --- Math scalar functions (ABS/ROUND/SIGN→signum/trig/...) ---

    #[test]
    fn renders_math_scalar_functions() {
        // 1-arg functions
        let cases_1arg = [
            ("ABS", "abs"),
            ("FLOOR", "floor"),
            ("CEIL", "ceil"),
            ("SQRT", "sqrt"),
            ("EXP", "exp"),
            ("LN", "ln"),
            ("SIGN", "signum"),
            ("DEGREES", "degrees"),
            ("RADIANS", "radians"),
            ("SIN", "sin"),
            ("COS", "cos"),
            ("TAN", "tan"),
            ("ASIN", "asin"),
            ("ACOS", "acos"),
            ("ATAN", "atan"),
            ("SINH", "sinh"),
            ("COSH", "cosh"),
            ("TANH", "tanh"),
            ("COT", "cot"),
        ];
        for (exasol, df) in cases_1arg {
            let expr = json!({
                "type": "function_scalar",
                "name": exasol,
                "arguments": [{"type": "column", "name": "x"}]
            });
            let sql = render_expression(&expr).unwrap();
            assert_eq!(sql, format!(r#"{df}("X")"#), "failed for {exasol}");
        }

        // 2-arg: POWER, ATAN2
        let expr = json!({
            "type": "function_scalar",
            "name": "POWER",
            "arguments": [
                {"type": "column", "name": "x"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"power("X", 2)"#);

        let expr = json!({
            "type": "function_scalar",
            "name": "ATAN2",
            "arguments": [
                {"type": "column", "name": "y"},
                {"type": "column", "name": "x"}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"atan2("Y", "X")"#);

        // 1-or-2-arg: ROUND, TRUNC, LOG
        let expr = json!({
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [{"type": "column", "name": "v"}, {"type": "literal_exactnumeric", "value": 2}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"round("V", 2)"#);

        let expr = json!({
            "type": "function_scalar",
            "name": "TRUNC",
            "arguments": [{"type": "column", "name": "v"}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"trunc("V")"#);

        // Arity error: ABS with 2 args
        let expr = json!({
            "type": "function_scalar",
            "name": "ABS",
            "arguments": [
                {"type": "column", "name": "x"},
                {"type": "column", "name": "y"}
            ]
        });
        assert!(render_expression_safe(&expr).is_none());
    }

    // --- MOD → % operator ---

    #[test]
    fn renders_mod_as_operator() {
        let expr = json!({
            "type": "function_scalar",
            "name": "MOD",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
    }

    // --- String scalar functions (CONCAT/LENGTH→character_length/INSTR+LOCATE→strpos/...) ---

    #[test]
    fn renders_string_scalar_functions() {
        // Pass-through lowercased
        let cases_lower = [
            "CONCAT",
            "LOWER",
            "UPPER",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "REPLACE",
            "REPEAT",
            "REVERSE",
            "LPAD",
            "RPAD",
            "ASCII",
            "CHR",
            "INITCAP",
            "LEFT",
            "RIGHT",
            "TRANSLATE",
        ];
        for name in cases_lower {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [{"type": "column", "name": "s"}]
            });
            let sql = render_expression(&expr).unwrap();
            assert_eq!(
                sql,
                format!(r#"{}("S")"#, name.to_lowercase()),
                "failed for {name}"
            );
        }

        // LENGTH → character_length
        let expr = json!({
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"character_length("S")"#
        );

        // OCTET_LENGTH → octet_length
        let expr = json!({
            "type": "function_scalar",
            "name": "OCTET_LENGTH",
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"octet_length("S")"#);

        // UNICODE → ascii
        let expr = json!({
            "type": "function_scalar",
            "name": "UNICODE",
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"ascii("S")"#);

        // UNICODECHR → chr
        let expr = json!({
            "type": "function_scalar",
            "name": "UNICODECHR",
            "arguments": [{"type": "column", "name": "n"}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"chr("N")"#);

        // SUBSTR → substr (same name, but explicit mapping)
        let expr = json!({
            "type": "function_scalar",
            "name": "SUBSTR",
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"substr("S", 1, 3)"#);

        // INSTR: INSTR(string, substring) → strpos(string, substring)
        let expr = json!({
            "type": "function_scalar",
            "name": "INSTR",
            "arguments": [
                {"type": "literal_string", "value": "hello"},
                {"type": "literal_string", "value": "ll"}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");

        // LOCATE: LOCATE(substring, string) → strpos(string, substring) — operands reordered
        let expr = json!({
            "type": "function_scalar",
            "name": "LOCATE",
            "arguments": [
                {"type": "literal_string", "value": "ll"},
                {"type": "literal_string", "value": "hello"}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");
    }

    // --- CASE WHEN ... THEN ... ELSE ... END ---

    #[test]
    fn renders_case_when() {
        // CASE WHEN cond THEN result END (no else)
        let expr = json!({
            "type": "function_scalar",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "status"},
                 "right": {"type": "literal_string", "value": "A"}},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, r#"CASE WHEN ("STATUS" = 'A') THEN 1 END"#);

        // CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE else END
        let expr2 = json!({
            "type": "function_scalar",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "x"},
                 "right": {"type": "literal_exactnumeric", "value": 1}},
                {"type": "literal_string", "value": "one"},
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "x"},
                 "right": {"type": "literal_exactnumeric", "value": 2}},
                {"type": "literal_string", "value": "two"},
                {"type": "literal_string", "value": "other"}
            ]
        });
        let sql2 = render_expression(&expr2).unwrap();
        assert_eq!(
            sql2,
            r#"CASE WHEN ("X" = 1) THEN 'one' WHEN ("X" = 2) THEN 'two' ELSE 'other' END"#
        );

        // Empty CASE (< 2 args) → error
        let expr3 = json!({
            "type": "function_scalar",
            "name": "CASE",
            "arguments": []
        });
        assert!(render_expression_safe(&expr3).is_none());
    }

    // --- GREATEST / LEAST ---

    #[test]
    fn renders_greatest_least() {
        let expr = json!({
            "type": "function_scalar",
            "name": "GREATEST",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"},
                {"type": "column", "name": "c"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"greatest("A", "B", "C")"#
        );

        let expr2 = json!({
            "type": "function_scalar",
            "name": "LEAST",
            "arguments": [
                {"type": "column", "name": "x"},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        });
        assert_eq!(render_expression(&expr2).unwrap(), r#"least("X", 0)"#);
    }

    // --- NULLIFZERO / ZEROIFNULL ---

    #[test]
    fn renders_nullifzero_zeroifnull() {
        let expr = json!({
            "type": "function_scalar",
            "name": "NULLIFZERO",
            "arguments": [{"type": "column", "name": "v"}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"nullif("V", 0)"#);

        let expr2 = json!({
            "type": "function_scalar",
            "name": "ZEROIFNULL",
            "arguments": [{"type": "column", "name": "v"}]
        });
        assert_eq!(render_expression(&expr2).unwrap(), r#"coalesce("V", 0)"#);
    }

    // --- NULLIF (two-arg) ---

    /// NULLIF(MOD(id,5),0) — the group key from test_group_by_null_key_grouping —
    /// must render so the grouped-aggregate path (not the row-scan fallback) handles it.
    #[test]
    fn renders_nullif_of_mod() {
        let expr = json!({
            "type": "function_scalar",
            "name": "NULLIF",
            "arguments": [
                {
                    "type": "function_scalar",
                    "name": "MOD",
                    "arguments": [
                        {"type": "column", "name": "id"},
                        {"type": "literal_exactnumeric", "value": "5"}
                    ]
                },
                {"type": "literal_exactnumeric", "value": "0"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"nullif(("ID" % 5), 0)"#
        );
    }

    // --- CASE (function_scalar_case) ---

    /// Exasol expands NULLIF(MOD(id,5),0) into a simple CASE before pushdown:
    ///   CASE MOD(id,5) WHEN 0 THEN NULL ELSE MOD(id,5) END
    /// This is the actual group key Exasol pushes in test_group_by_null_key_grouping
    /// (FN_CASE is advertised), so the grouped-aggregate path — not the row-scan
    /// fallback — must render it.
    #[test]
    fn renders_simple_case_from_nullif_expansion() {
        let mod_node = json!({
            "type": "function_scalar",
            "name": "MOD",
            "arguments": [
                {"type": "column", "name": "ID"},
                {"type": "literal_exactnumeric", "value": "5"}
            ]
        });
        let expr = json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "basis": mod_node,
            "arguments": [{"type": "literal_exactnumeric", "value": "0"}],
            "results": [
                {"type": "literal_null"},
                mod_node
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CASE ("ID" % 5) WHEN 0 THEN NULL ELSE ("ID" % 5) END)"#
        );
    }

    /// Searched CASE (no `basis`): WHEN arguments are boolean predicates.
    #[test]
    fn renders_searched_case_without_basis() {
        let expr = json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_less",
                 "left": {"type": "column", "name": "SCORE"},
                 "right": {"type": "literal_exactnumeric", "value": "50"}}
            ],
            "results": [
                {"type": "literal_string", "value": "low"},
                {"type": "literal_string", "value": "high"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CASE WHEN ("SCORE" < 50) THEN 'low' ELSE 'high' END)"#
        );
    }

    /// CASE with no ELSE branch: results.len() == arguments.len().
    #[test]
    fn renders_case_without_else() {
        let expr = json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "basis": {"type": "column", "name": "ID"},
            "arguments": [{"type": "literal_exactnumeric", "value": "1"}],
            "results": [{"type": "literal_string", "value": "one"}]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CASE "ID" WHEN 1 THEN 'one' END)"#
        );
    }

    // --- EXTRACT and field-shortcut date functions ---

    #[test]
    fn renders_extract() {
        // Exasol sends EXTRACT as its own node type with the field in `toExtract`.
        let expr = json!({
            "type": "function_scalar_extract",
            "name": "EXTRACT",
            "toExtract": "YEAR",
            "arguments": [{"type": "column", "name": "ts"}]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"date_part('YEAR', "TS")"#
        );

        let expr2 = json!({
            "type": "function_scalar_extract",
            "name": "EXTRACT",
            "toExtract": "MONTH",
            "arguments": [{"type": "column", "name": "ts"}]
        });
        assert_eq!(
            render_expression(&expr2).unwrap(),
            r#"date_part('MONTH', "TS")"#
        );
    }

    #[test]
    fn renders_year_month_day_extract() {
        let shortcuts = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];
        for field in shortcuts {
            let expr = json!({
                "type": "function_scalar",
                "name": field,
                "arguments": [{"type": "column", "name": "ts"}]
            });
            let sql = render_expression(&expr).unwrap();
            assert_eq!(
                sql,
                format!(r#"date_part('{field}', "TS")"#),
                "failed for {field}"
            );
        }
    }

    // --- DATE_TRUNC ---

    #[test]
    fn renders_date_trunc() {
        let expr = json!({
            "type": "function_scalar",
            "name": "DATE_TRUNC",
            "arguments": [
                {"type": "literal_string", "value": "month"},
                {"type": "column", "name": "ts"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"date_trunc('month', "TS")"#
        );
    }

    // --- CURRENT_DATE / SYSDATE / CURRENT_TIMESTAMP / SYSTIMESTAMP ---

    #[test]
    fn renders_now_family() {
        for name in ["CURRENT_DATE", "SYSDATE"] {
            let expr = json!({"type": "function_scalar", "name": name, "arguments": []});
            assert_eq!(
                render_expression(&expr).unwrap(),
                "current_date()",
                "failed for {name}"
            );
        }
        for name in ["CURRENT_TIMESTAMP", "SYSTIMESTAMP"] {
            let expr = json!({"type": "function_scalar", "name": name, "arguments": []});
            assert_eq!(
                render_expression(&expr).unwrap(),
                "now()",
                "failed for {name}"
            );
        }
    }

    // --- TO_DATE / TO_TIMESTAMP with optional format arg ---

    #[test]
    fn renders_to_date_to_timestamp() {
        // TO_DATE with 1 arg
        let expr = json!({
            "type": "function_scalar",
            "name": "TO_DATE",
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"to_date("S")"#);

        // TO_DATE with format
        let expr2 = json!({
            "type": "function_scalar",
            "name": "TO_DATE",
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": "%Y-%m-%d"}
            ]
        });
        assert_eq!(
            render_expression(&expr2).unwrap(),
            r#"to_date("S", '%Y-%m-%d')"#
        );

        // TO_TIMESTAMP with 1 arg
        let expr3 = json!({
            "type": "function_scalar",
            "name": "TO_TIMESTAMP",
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(render_expression(&expr3).unwrap(), r#"to_timestamp("S")"#);

        // TO_TIMESTAMP with format
        let expr4 = json!({
            "type": "function_scalar",
            "name": "TO_TIMESTAMP",
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": "%Y-%m-%d %H:%M:%S"}
            ]
        });
        assert_eq!(
            render_expression(&expr4).unwrap(),
            r#"to_timestamp("S", '%Y-%m-%d %H:%M:%S')"#
        );
    }

    // --- Unsupported date functions return an error ---

    #[test]
    fn unsupported_date_fn_falls_through() {
        let unsupported = ["ADD_DAYS", "DAYS_BETWEEN", "CONVERT_TZ", "POSIX_TIME"];
        for name in unsupported {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [{"type": "column", "name": "x"}]
            });
            let err = render_expression(&expr).unwrap_err();
            assert!(
                err.to_string().contains(name),
                "error must name the unsupported function '{name}': {err}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "safe mode must return None for '{name}'"
            );
        }
    }
}
