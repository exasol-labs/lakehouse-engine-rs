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

/// Which SQL parser the rendered fragment must satisfy.
///
/// The SAME recursive translator feeds two different parsers depending on the
/// call site. Threaded through every node (not just CAST) so any future
/// rendering rule that differs by target parser has a place to branch;
/// currently only the CAST target (`render_cast_target`) actually does,
/// where the two dialects have OPPOSITE requirements for character-type CAST
/// targets:
/// - `DataFusion`: the rendered fragment is embedded in a `ScanSpec`
///   (`filter`/`projection`/`group_keys`) and parsed by DataFusion's SQL
///   frontend INSIDE the scan UDF. datafusion-sql rejects `VARCHAR(n)` with a
///   length unless `support_varchar_with_length` is enabled (this project does
///   not enable it), so a character CAST target must be bare `VARCHAR`.
/// - `Exasol`: the rendered fragment becomes part of the outer wrapper SQL text
///   parsed by Exasol's own core engine (the qualified single-table / N-scan
///   join wrapper in `joins.rs`, the grouped-aggregate outer-merge wrapper in
///   `grouped_agg.rs`). Exasol has no length-less VARCHAR/CHAR type — `VARCHAR`
///   MUST be followed by `(n)` — so a character CAST target needs an explicit
///   length.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    DataFusion,
    Exasol,
}

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
fn render_args(args: &[Json], dialect: Dialect) -> Result<Vec<String>, UdfError> {
    args.iter()
        .enumerate()
        .map(|(i, arg)| {
            render_expression_inner(arg, dialect)?
                .ok_or_else(|| UdfError::User(format!("argument[{i}] rendered to null")))
        })
        .collect()
}

/// Map a VS `dataType` JSON object to a DataFusion SQL type name.
fn render_cast_target(data_type: &Json, dialect: Dialect) -> Result<String, UdfError> {
    let type_name = data_type.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match type_name.to_uppercase().as_str() {
        "VARCHAR" | "CHAR" => match dialect {
            // DataFusion's SQL frontend rejects VARCHAR(n) with a length (no
            // `support_varchar_with_length`); only bare VARCHAR parses there.
            Dialect::DataFusion => Ok("VARCHAR".to_string()),
            // Exasol's parser has the OPPOSITE requirement: VARCHAR/CHAR MUST
            // carry a length. Render `VARCHAR(<size>)` from the width Exasol
            // itself sent (`{"type":"VARCHAR","size":n}` /
            // `{"type":"CHAR","size":n,...}`). If `size` is somehow absent, fall
            // back to the project's "unknown/incompatible width" convention.
            // Do NOT clamp to Exasol's 2,000,000 max — trust the value Exasol
            // sent.
            Dialect::Exasol => Ok(match data_type.get("size").and_then(|v| v.as_u64()) {
                Some(size) => format!("VARCHAR({size})"),
                None => "VARCHAR(2000000)".to_string(),
            }),
        },
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
        "TIMESTAMP" => {
            // Exasol serialises TIMESTAMP WITH LOCAL TIME ZONE as type "TIMESTAMP"
            // with `withLocalTimeZone: true` (not a distinct type string). WLTZ
            // carries session-timezone / UTC-normalisation semantics that
            // DataFusion's plain TIMESTAMP does not reproduce, so it is not a
            // faithful target: decline it and let Exasol evaluate the CAST.
            if data_type.get("withLocalTimeZone").and_then(|v| v.as_bool()) == Some(true) {
                Err(UdfError::User(
                    "unsupported CAST target type: TIMESTAMP WITH LOCAL TIME ZONE".into(),
                ))
            } else {
                Ok("TIMESTAMP".to_string())
            }
        }
        other => Err(UdfError::User(format!(
            "unsupported CAST target type: {other}"
        ))),
    }
}

/// Render a CAST node body to `CAST(<expr> AS <target>)`.
///
/// Shared by both CAST encodings (see the `function_scalar_cast` top-level arm
/// and the defensive nested `function_scalar`+name=CAST arm) so the target-type
/// faithfulness rules in `render_cast_target` are applied identically on both
/// paths and cannot drift.
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
    Ok(Some(format!("CAST({inner} AS {target_type})")))
}

/// Internal recursive translator.
///
/// Returns `Ok(None)` when `expr` is `Json::Null` (absent optional child).
/// Returns `Ok(Some(sql))` on success.
/// Returns `Err(UdfError::User(...))` for unsupported or malformed nodes.
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
            // Render via arrow_cast at explicit microsecond precision: a bare
            // `TIMESTAMP '...'` is typed Timestamp(Nanosecond) by DataFusion's SQL
            // frontend, which overflows in simplify_expressions when unified with
            // the scan's microsecond-typed columns on far-future values (#155).
            return Ok(Some(format!(
                "arrow_cast({}, 'Timestamp(Microsecond, None)')",
                quote_literal(value("value"))
            )));
        }
        "literal_timestamp_utc" => {
            // Append +00:00 so the value parses as UTC, then render via arrow_cast
            // at explicit microsecond precision (see literal_timestamp above). The
            // cast target tz label is "UTC" (not "+00:00") to match the scan's
            // Timestamptz Arrow mapping (types/mapping.rs) and avoid a tz-label
            // mismatch during DataFusion type unification.
            let raw = match value("value") {
                None | Some(Json::Null) => return Ok(Some("NULL".into())),
                Some(v) => json_scalar_to_string(v),
            };
            let quoted = quote_literal(Some(&Json::String(format!("{raw}+00:00"))));
            return Ok(Some(format!(
                "arrow_cast({quoted}, 'Timestamp(Microsecond, Some(\"UTC\"))')"
            )));
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

    // --- Logical connectives and other predicates ---
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
            let src = render_expression_inner(&args[0], dialect)?
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
        // Exasol sends CAST as its own top-level node type carrying the target
        // in `dataType` (verified against the engine source
        // `Compiler/src/querygraph/scalar/qecast.cpp:101` — `QECastBasis::toJson`
        // is the sole CAST emitter):
        //   {"type":"function_scalar_cast","name":"CAST","dataType":{...},"arguments":[<src>]}
        // This is the shape real Exasol traffic hits; the nested
        // `function_scalar`+name=CAST arm below is a defensive alternate encoding.
        "function_scalar_cast" => {
            let args = value("arguments").and_then(|a| a.as_array());
            render_cast(args, value("dataType"), dialect)
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
                    let left = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} left operand is null")))?;
                    let right = render_expression_inner(&args[1], dialect)?.ok_or_else(|| {
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
                    let operand = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("NEG operand is null".into()))?;
                    Ok(Some(format!("(-{operand})")))
                }
                // CAST as a function_scalar (defensive alternate encoding).
                // Real Exasol traffic emits CAST as the top-level
                // `function_scalar_cast` node handled above (engine source
                // `qecast.cpp` emits CAST exclusively that way); this arm is kept
                // defensively — like the REGEXP_LIKE alternate encoding below — and
                // shares the same body via `render_cast`.
                "CAST" => render_cast(args, value("dataType"), dialect),
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
                    let subject = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("REGEXP_LIKE subject is null".into()))?;
                    let pattern = render_expression_inner(&args[1], dialect)?
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
                    let left = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("MOD left operand is null".into()))?;
                    let right = render_expression_inner(&args[1], dialect)?
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
                    let rendered = render_args(args, dialect)?;
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
                    // Exasol LOCATE(substring, string) — reorder to strpos(string, substring)
                    let substr = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("LOCATE substring arg is null".into()))?;
                    let string = render_expression_inner(&args[1], dialect)?
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
                    let rendered = render_args(args, dialect)?;
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
                    let left = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("NULLIF first argument is null".into()))?;
                    let right = render_expression_inner(&args[1], dialect)?
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
                    let src = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User(format!("{fn_name} argument is null")))?;
                    Ok(Some(format!("date_part('{fn_name}', {src})")))
                }
                // WEEK(datetime) → date_part('week', datetime). Both Exasol WEEK
                // and DataFusion 54 date_part('week') are ISO-8601 (weeks begin
                // Monday, week 1 contains the year's first Thursday, range 1-53):
                // DataFusion maps 'week' → IntervalUnit::Week → DatePart::Week →
                // chrono iso_week().week(), so year-boundary weeks agree. Only the
                // parity target is rendered; the other Exasol date functions
                // diverge and fall through as unsupported (see date-fns spec).
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
                    let unit = render_expression_inner(&args[0], dialect)?
                        .ok_or_else(|| UdfError::User("DATE_TRUNC unit is null".into()))?;
                    let src = render_expression_inner(&args[1], dialect)?
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
                // ADD_HOURS / ADD_MINUTES are deliberately NOT translated. The
                // microsecond round-trip rendering executed correctly for a TIMESTAMP
                // argument, but E2E parity against live Exasol (task 3.1) showed it
                // diverges on a DATE argument: Exasol infers ADD_HOURS(DATE, n) →
                // TIMESTAMP(0), while the rendering always yields TIMESTAMP(3), so
                // Exasol rejects the pushdown ("Data type mismatch ... Expected
                // TIMESTAMP(0), but got TIMESTAMP(3)"). A type-blind string translator
                // has no argument type and cannot vary the result precision, so these
                // fall through — same input-type-dependent class as ADD_DAYS/ADD_WEEKS.
                // DAYS_BETWEEN — whole-day date difference. Exasol uses only the date
                // part of a timestamp; DATE - DATE yields an Int64 day count in
                // DataFusion 54.0.0 (is_date_minus_date in type_coercion/binary.rs →
                // ret: Int64). Wrapped in outer parens so the difference composes
                // safely as an operand (same convention as the FN_ADD/SUB/MULT arms).
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
                // HOURS_BETWEEN / MINUTES_BETWEEN / SECONDS_BETWEEN — fractional
                // differences over full timestamps, from date_part('epoch', …)
                // (Float64 seconds) differences. The epoch difference is divided by
                // the unit's seconds (undivided for SECONDS_BETWEEN); the whole
                // expression is fully parenthesized so it composes safely as an
                // operand (first minus second → negative when arg1 precedes arg2).
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
                other => Err(UdfError::User(format!(
                    "unsupported scalar function: {other}"
                ))),
            }
        }
        // Aggregate function node. Unlike `function_scalar`, the `name` is NOT
        // mapped to a DataFusion alias — Exasol pushed a valid aggregate name
        // (SUM, COUNT, AVG, MIN, MAX, the STDDEV/VARIANCE family), so it is spliced
        // verbatim, uppercased. Arguments are rendered by recursion (so a nested
        // CASE, arithmetic, or `tableAlias`-qualified column argument renders in
        // full), letting a scalar expression that wraps aggregates render instead
        // of failing at the nested aggregate. An empty argument list is the
        // COUNT(*) star case (`<NAME>(*)`); `distinct: true` prefixes DISTINCT.
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

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Render a VS expression node to a DataFusion SQL fragment.
///
/// Raises on unsupported or malformed nodes.
///
/// Renders CAST targets in the DataFusion dialect: a character CAST target is
/// bare `VARCHAR` with NO length, because datafusion-sql rejects a length on
/// VARCHAR (no `support_varchar_with_length`). For the code paths whose output
/// is parsed by Exasol's core engine, use the Exasol-dialect twin
/// [`render_expression_exasol`].
pub fn render_expression(expr: &Json) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::DataFusion)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// Render a VS expression node to a DataFusion SQL fragment.
///
/// Returns `None` on any failure (unsupported node types, malformed input).
/// Never panics. DataFusion dialect — see [`render_expression`]; the
/// Exasol-dialect twin is [`render_expression_exasol_safe`].
pub fn render_expression_safe(expr: &Json) -> Option<String> {
    render_expression_inner(expr, Dialect::DataFusion).ok()?
}

/// Render a VS filter expression to a DataFusion SQL WHERE fragment.
///
/// Returns `None` when:
/// - rendering fails (unsupported node types, malformed input), or
/// - the filter is trivially true (`TRUE` or `NULL`) — the adapter omits
///   it from the scan spec and lets Exasol keep it as a correctness backstop.
///
/// DataFusion dialect — see [`render_expression`]; the Exasol-dialect twin is
/// [`render_df_filter_exasol_safe`].
pub fn render_df_filter_safe(filter_expr: &Json) -> Option<String> {
    let result = render_expression_inner(filter_expr, Dialect::DataFusion).ok()??;
    if result == "TRUE" || result == "NULL" {
        None
    } else {
        Some(result)
    }
}

/// Render a VS expression node to an **Exasol** SQL fragment.
///
/// Identical to [`render_expression`] except CAST targets are rendered in the
/// Exasol dialect: a character CAST target is length-qualified (`VARCHAR(n)`;
/// `CHAR(n)` also maps to `VARCHAR(n)` per the mission data-type table), because
/// Exasol's own parser has no length-less VARCHAR type. Use this on the code
/// paths whose rendered SQL is parsed by Exasol's core engine directly — the
/// qualified single-table / N-scan join wrapper (`joins.rs`) and the
/// grouped-aggregate outer-merge wrapper (`grouped_agg.rs`) — NOT for fragments
/// embedded in a DataFusion `ScanSpec`, which must use [`render_expression`].
pub fn render_expression_exasol(expr: &Json) -> Result<String, UdfError> {
    render_expression_inner(expr, Dialect::Exasol)?
        .ok_or_else(|| UdfError::User("expression node is null".into()))
}

/// Render a VS expression node to an **Exasol** SQL fragment.
///
/// Returns `None` on any failure. Exasol dialect — see
/// [`render_expression_exasol`]; the DataFusion-dialect twin is
/// [`render_expression_safe`].
pub fn render_expression_exasol_safe(expr: &Json) -> Option<String> {
    render_expression_inner(expr, Dialect::Exasol).ok()?
}

/// Render a VS filter expression to an **Exasol** SQL WHERE fragment.
///
/// Returns `None` when rendering fails or the filter is trivially true
/// (`TRUE`/`NULL`), mirroring [`render_df_filter_safe`] exactly. Exasol
/// dialect — see [`render_expression_exasol`]; the DataFusion-dialect twin is
/// [`render_df_filter_safe`].
pub fn render_df_filter_exasol_safe(filter_expr: &Json) -> Option<String> {
    let result = render_expression_inner(filter_expr, Dialect::Exasol).ok()??;
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
            "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
        );
    }

    #[test]
    fn renders_far_future_timestamp_literal() {
        // Literal reproduction of issue #155's overflow scenario: a bare
        // `TIMESTAMP '...'` form types as Timestamp(Nanosecond) and overflows on
        // far-future values during simplify_expressions; arrow_cast pins
        // microsecond precision so this renders cleanly. Optimizer behavior is
        // covered by `timestamp_literal_precision_test` in `lakehouse-engine`.
        let expr = json!({"type": "literal_timestamp", "value": "9999-12-31 23:59:59"});
        assert_eq!(
            render_expression(&expr).unwrap(),
            "arrow_cast('9999-12-31 23:59:59', 'Timestamp(Microsecond, None)')"
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

    #[test]
    fn neg_composes_with_aggregate_decomposition() {
        // SUM(-col): the NEG arm must render correctly as an aggregate argument,
        // not only standalone. `function_aggregate` recurses into its argument
        // (the same arithmetic-aggregate decomposition path exercised by
        // `render_expression_renders_scalar_wrapping_aggregates`), so a NEG node
        // nested under SUM must flow through unchanged.
        let sum_neg = json!({
            "type": "function_aggregate",
            "name": "SUM",
            "arguments": [{
                "type": "function_scalar",
                "name": "NEG",
                "arguments": [{"type": "column", "name": "col"}]
            }],
            "distinct": false
        });
        assert_eq!(render_expression(&sum_neg).unwrap(), r#"SUM((-"COL"))"#);
    }

    // --- CAST ---
    //
    // Fixtures use the real Exasol wire shape `{"type":"function_scalar_cast",
    // "name":"CAST","dataType":{...},"arguments":[...]}` — the shape the engine
    // actually emits (verified against `exasol-db` `qecast.cpp`
    // `QECastBasis::toJson`), NOT the earlier `{"type":"function_scalar",...}`
    // shape whose mismatch let a dispatch bug hide (CAST never reached its arm).

    #[test]
    fn renders_cast_varchar() {
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "VARCHAR", "size": 100}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
    }

    #[test]
    fn renders_cast_decimal() {
        let expr = json!({
            "type": "function_scalar_cast",
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
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "DOUBLE"}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DOUBLE)"#);
    }

    #[test]
    fn renders_cast_date() {
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "DATE"}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DATE)"#);
    }

    #[test]
    fn renders_cast_char_as_varchar() {
        // Exasol sends CHAR as {"type":"CHAR","size":n,"characterSet":...}. This
        // project maps CHAR to VARCHAR everywhere (see mission data-type table),
        // so the CAST target renders as VARCHAR, consistent with that mapping.
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
    }

    #[test]
    fn renders_cast_boolean() {
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "BOOLEAN"}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS BOOLEAN)"#);
    }

    #[test]
    fn renders_cast_timestamp_without_local_time_zone() {
        // Plain TIMESTAMP: Exasol sends {"type":"TIMESTAMP","withLocalTimeZone":false}.
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 3}
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#
        );
    }

    #[test]
    fn cast_to_unsupported_target_falls_back() {
        // Exasol CAST targets with no faithful DataFusion 54 equivalent. Each is
        // sent with the dataType descriptor shape shown (verified against the
        // Exasol virtual-schema data-types API). The translator must decline
        // (Err in raising mode, None in safe mode) so the adapter omits the CAST
        // and Exasol evaluates it as a correctness backstop.
        //
        // TIMESTAMP WITH LOCAL TIME ZONE is the trap: Exasol serialises it as
        // type "TIMESTAMP" with `withLocalTimeZone: true` — NOT a distinct type
        // string — so a naive "TIMESTAMP" arm would silently render it as plain
        // TIMESTAMP and drop its session-timezone/UTC-normalisation semantics.
        let unsupported = [
            json!({"type": "INTERVAL", "fromTo": "YEAR TO MONTH", "precision": 2}),
            json!({"type": "INTERVAL", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 2}),
            json!({"type": "GEOMETRY", "srid": 4326}),
            json!({"type": "HASHTYPE", "bytesize": 16}),
            json!({"type": "TIMESTAMP", "withLocalTimeZone": true, "fractionalSecondsPrecision": 9}),
        ];
        for data_type in unsupported {
            let expr = json!({
                "type": "function_scalar_cast",
                "name": "CAST",
                "arguments": [{"type": "column", "name": "x"}],
                "dataType": data_type.clone()
            });
            assert!(
                render_expression(&expr).is_err(),
                "CAST to {data_type} must raise in raising mode"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "CAST to {data_type} must be None in safe mode"
            );
        }
    }

    #[test]
    fn renders_cast_nested_function_scalar_defensive() {
        // Defensive alternate encoding: CAST nested inside a generic
        // `function_scalar` node. Real Exasol traffic uses `function_scalar_cast`
        // (see the fixtures above), but the nested arm is kept — like the
        // REGEXP_LIKE alternate encoding — and must still render identically via
        // the shared `render_cast` body.
        let expr = json!({
            "type": "function_scalar",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "VARCHAR", "size": 100}
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
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
        assert_eq!(
            sql,
            "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
        );
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

    // --- WEEK (ISO-8601) ---

    #[test]
    fn renders_week_as_iso_date_part() {
        let expr = json!({
            "type": "function_scalar",
            "name": "WEEK",
            "arguments": [{"type": "column", "name": "d"}]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"date_part('week', "D")"#
        );
    }

    #[test]
    fn renders_week_at_year_boundary_dates() {
        // ISO-8601 parity is what gates FN_WEEK. The translator emits
        // date_part('week', <arg>); DataFusion 54 maps 'week' → DatePart::Week →
        // chrono iso_week().week(), and Exasol WEEK is documented ISO-8601, so the
        // two agree at year boundaries. Verified by executing the rendered call in
        // DataFusion 54 for these boundary dates:
        //   2021-01-01 (Fri) → 53   (ISO week 53 of 2020)
        //   2020-12-31 (Thu) → 53
        //   2019-12-30 (Mon) → 1    (ISO week 1 of 2020)
        //   2023-01-01 (Sun) → 52   (ISO week 52 of 2022)
        // The translator itself only renders the call; this test pins the
        // rendering for boundary-date arguments so the parity target stays fixed.
        let boundary_dates = ["2021-01-01", "2020-12-31", "2019-12-30", "2023-01-01"];
        for date in boundary_dates {
            let expr = json!({
                "type": "function_scalar",
                "name": "WEEK",
                "arguments": [{"type": "literal_date", "value": date}]
            });
            assert_eq!(
                render_expression(&expr).unwrap(),
                format!("date_part('week', DATE '{date}')"),
                "failed for boundary date {date}"
            );
        }
    }

    #[test]
    fn week_with_wrong_arity_falls_back() {
        let expr = json!({
            "type": "function_scalar",
            "name": "WEEK",
            "arguments": [
                {"type": "column", "name": "d"},
                {"type": "column", "name": "e"}
            ]
        });
        assert!(render_expression(&expr).is_err());
        assert!(render_expression_safe(&expr).is_none());
    }

    // ADD_HOURS / ADD_MINUTES have no rendering test: they were withdrawn after
    // E2E parity (task 3.1) showed the microsecond round-trip diverges on a DATE
    // argument (Exasol expects TIMESTAMP(0), the rendering yields TIMESTAMP(3)).
    // They now fall through — see `unsupported_date_fn_falls_through`.

    // --- DAYS_BETWEEN (whole-day date difference) ---

    #[test]
    fn renders_days_between_as_date_difference() {
        // DATE - DATE yields an Int64 day count in DataFusion 54.0.0
        // (is_date_minus_date in type_coercion/binary.rs → ret: Int64). Outer parens
        // keep the difference composition-safe as an operand (same convention as the
        // FN_ADD/SUB/MULT arms).
        let expr = json!({
            "type": "function_scalar",
            "name": "DAYS_BETWEEN",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
        );
    }

    // --- HOURS/MINUTES/SECONDS_BETWEEN (epoch-second differences) ---

    #[test]
    fn renders_time_between_as_epoch_difference() {
        let hours = json!({
            "type": "function_scalar",
            "name": "HOURS_BETWEEN",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression(&hours).unwrap(),
            r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 3600)"#
        );

        let minutes = json!({
            "type": "function_scalar",
            "name": "MINUTES_BETWEEN",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression(&minutes).unwrap(),
            r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 60)"#
        );

        let seconds = json!({
            "type": "function_scalar",
            "name": "SECONDS_BETWEEN",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression(&seconds).unwrap(),
            r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#
        );
    }

    #[test]
    fn between_fns_reject_wrong_arity() {
        for name in [
            "DAYS_BETWEEN",
            "HOURS_BETWEEN",
            "MINUTES_BETWEEN",
            "SECONDS_BETWEEN",
        ] {
            let one_arg = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [{"type": "column", "name": "a"}]
            });
            assert!(
                render_expression(&one_arg).is_err(),
                "{name} 1-arg must raise"
            );
            assert!(
                render_expression_safe(&one_arg).is_none(),
                "{name} 1-arg must be None in safe mode"
            );

            let three_args = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"},
                    {"type": "column", "name": "c"}
                ]
            });
            assert!(
                render_expression(&three_args).is_err(),
                "{name} 3-arg must raise"
            );
            assert!(
                render_expression_safe(&three_args).is_none(),
                "{name} 3-arg must be None in safe mode"
            );
        }
    }

    // --- Integer division DIV is deliberately not translated ---

    #[test]
    fn div_falls_through_as_unsupported() {
        // Exasol DIV truncates toward zero (verified live: DIV(-7,2) = -3, not
        // floor's -4) and matches DataFusion's integer `/`. But DataFusion 54 has
        // no `div` builtin, and a `TRUNC(m/n)` emulation would diverge from
        // Exasol on DOUBLE-operand division by zero (Exasol raises SQL state
        // 22012; DataFusion float division yields infinity). DIV operand types
        // aren't carried in the expression node, so the translator can't
        // selectively render only the safe integer case. DIV must therefore
        // decline so Exasol evaluates it.
        let expr = json!({
            "type": "function_scalar",
            "name": "DIV",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        let err = render_expression(&expr).unwrap_err();
        assert!(
            err.to_string().contains("DIV"),
            "error must name DIV as unsupported: {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "DIV must be None in safe mode without panicking"
        );
    }

    // --- Conversion format functions TO_CHAR/TO_NUMBER are deliberately not translated ---

    #[test]
    fn to_char_and_to_number_fall_through_as_unsupported() {
        // DataFusion 54 `to_char` uses strftime masks (not Exasol's Oracle-style
        // format models) and rejects numeric formatting; DataFusion 54 has no
        // `to_number` at all. Both must therefore decline so Exasol evaluates
        // them; a no-format string-to-number conversion stays reachable via CAST.
        let unsupported = ["TO_CHAR", "TO_NUMBER"];
        for name in unsupported {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "literal_string", "value": "999.99"}
                ]
            });
            let err = render_expression(&expr).unwrap_err();
            assert!(
                err.to_string().contains(name),
                "error must name the unsupported function '{name}': {err}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "{name} must be None in safe mode without panicking"
            );
        }
    }

    // --- Regexp scalar functions are deliberately not translated (issue #106) ---

    #[test]
    fn regexp_scalar_functions_fall_through() {
        // The Rust `regex` crate (DataFusion 54) rejects backreferences and
        // lookaround that Exasol's PCRE dialect accepts (blocks all four),
        // lacks regexp_substr (blocks REGEXP_SUBSTR), and REGEXP_REPLACE /
        // REGEXP_INSTR's argument shapes differ from Exasol's position/
        // occurrence/return options (REGEXP_COUNT's shape actually aligns) —
        // so all four scalar regexp functions decline (issue #106).
        let unsupported = [
            "REGEXP_REPLACE",
            "REGEXP_SUBSTR",
            "REGEXP_INSTR",
            "REGEXP_COUNT",
        ];
        for name in unsupported {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [
                    {"type": "column", "name": "s"},
                    {"type": "literal_string", "value": "a+"}
                ]
            });
            let err = render_expression(&expr).unwrap_err();
            assert!(
                err.to_string().contains(name),
                "error must name the unsupported function '{name}': {err}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "{name} must be None in safe mode without panicking"
            );
        }
    }

    // --- Bitwise operator functions are deliberately not translated (issue #108) ---

    #[test]
    fn bitwise_operator_functions_fall_through() {
        // Exasol's eleven bit functions operate on an UNSIGNED 64-bit domain
        // (0..=18446744073709551615, result DECIMAL(20,0)); none has a faithful
        // DataFusion 54.0.0 translation, so all eleven decline (issue #108). Two
        // distinct blocker classes:
        //
        //   1. Operator-backed but signed-domain (BIT_AND/OR/XOR/LSHIFT/RSHIFT):
        //      DataFusion's `&`/`|`/`#`/`<<`/`>>` (Operator::BitwiseAnd/Or/Xor/
        //      ShiftLeft/ShiftRight) act on the SIGNED operand type. Any bit-63-set
        //      result is unsigned-large in Exasol but negative under Int64, and
        //      Int64 -> DECIMAL(20,0) carries the negative value; `>>` is arithmetic
        //      (sign-extend) vs Exasol's logical (zero-fill). Operand types aren't
        //      carried in the expression node, so the type/value-blind translator
        //      cannot restrict to the safe subset (the recorded DIV limitation).
        //   2. No DataFusion 54.0.0 operator or builtin at all (BIT_NOT/LROTATE/
        //      RROTATE/CHECK/SET/TO_NUM): unary `~` is `not_impl_err`, and
        //      datafusion-functions registers no rotate/bit-test/bit-set/bits-to-
        //      number scalar (only the unrelated string `bit_length`).
        //
        // Both classes fall through the generic unsupported-`function_scalar` path;
        // this test pins that decline (no dedicated production arm exists).
        let unsupported = [
            "BIT_AND",
            "BIT_OR",
            "BIT_XOR",
            "BIT_NOT",
            "BIT_LSHIFT",
            "BIT_RSHIFT",
            "BIT_LROTATE",
            "BIT_RROTATE",
            "BIT_CHECK",
            "BIT_SET",
            "BIT_TO_NUM",
        ];
        for name in unsupported {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            });
            let err = render_expression(&expr).unwrap_err();
            let err_string = err.to_string();
            // Pinned to the generic fallthrough text, not just the function name: a
            // future dedicated arm that merely validates arity (e.g. modeled on NEG's
            // arity-check error) would also produce a message containing `name`, which
            // would silently defeat this decline-lock for the six functions with no
            // DataFusion builtin at all (BIT_NOT/LROTATE/RROTATE/CHECK/SET/TO_NUM).
            assert!(
                err_string.contains("unsupported scalar function"),
                "{name} must fall through the generic unsupported-scalar-function path: {err}"
            );
            assert!(
                err_string.contains(name),
                "error must name the unsupported function '{name}': {err}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "{name} must be None in safe mode without panicking"
            );
        }
    }

    #[test]
    fn regexp_scalar_exclusion_leaves_regexp_like_untouched() {
        // The scalar-regexp exclusion (issue #106) must not affect the REGEXP_LIKE
        // predicate path (FN_PRED_REGEXP_LIKE stays advertised): both encodings
        // still render.
        let predicate = json!({
            "type": "predicate_like_regexp",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "^A.*"}
        });
        assert_eq!(
            render_expression(&predicate).unwrap(),
            r#"regexp_like("NAME", '^A.*')"#
        );
        let scalar = json!({
            "type": "function_scalar",
            "name": "REGEXP_LIKE",
            "arguments": [
                {"type": "column", "name": "name"},
                {"type": "literal_string", "value": "^B.*"}
            ]
        });
        assert_eq!(
            render_expression(&scalar).unwrap(),
            r#"regexp_like("NAME", '^B.*')"#
        );
    }

    // --- Unsupported date functions return an error ---

    #[test]
    fn unsupported_date_fn_falls_through() {
        // Remaining excluded set per the date-fns spec Background: the date-arithmetic,
        // date-difference, and other date scalars whose DataFusion 54 equivalents still
        // diverge from Exasol (or don't exist at all). DAYS_BETWEEN, HOURS_BETWEEN,
        // MINUTES_BETWEEN, and SECONDS_BETWEEN are no longer here — they now have real
        // translator arms (see the disposition table in `add-date-arithmetic-pushdown`)
        // and are covered by their own rendering tests instead. ADD_HOURS/ADD_MINUTES
        // ARE still here: their arm was withdrawn after E2E parity (task 3.1) showed
        // the microsecond round-trip diverges on a DATE argument (Exasol expects
        // TIMESTAMP(0), the rendering yields TIMESTAMP(3)).
        let unsupported = [
            // Date-arithmetic
            "ADD_HOURS",
            "ADD_MINUTES",
            "ADD_DAYS",
            "ADD_SECONDS",
            "ADD_WEEKS",
            "ADD_MONTHS",
            "ADD_YEARS",
            // Date-difference
            "MONTHS_BETWEEN",
            "YEARS_BETWEEN",
            // Other date scalars
            "DAYOFWEEK",
            "LAST_DAY",
            "CONVERT_TZ",
            "POSIX_TIME",
        ];
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

    // --- Aggregate function nodes (function_aggregate) ---

    #[test]
    fn render_expression_renders_aggregate_nodes() {
        // SUM(col) — aggregate name spliced verbatim, bare column argument recursed.
        let sum = json!({
            "type": "function_aggregate",
            "name": "SUM",
            "arguments": [{"type": "column", "name": "col"}],
            "distinct": false
        });
        assert_eq!(render_expression(&sum).unwrap(), r#"SUM("COL")"#);

        // COUNT(*) — empty argument list is the star case.
        let count_star = json!({
            "type": "function_aggregate",
            "name": "COUNT",
            "arguments": [],
            "distinct": false
        });
        assert_eq!(render_expression(&count_star).unwrap(), "COUNT(*)");

        // COUNT(DISTINCT col) — distinct keyword precedes the rendered argument.
        let count_distinct = json!({
            "type": "function_aggregate",
            "name": "COUNT",
            "arguments": [{"type": "column", "name": "col"}],
            "distinct": true
        });
        assert_eq!(
            render_expression(&count_distinct).unwrap(),
            r#"COUNT(DISTINCT "COL")"#
        );

        // AVG(col).
        let avg = json!({
            "type": "function_aggregate",
            "name": "AVG",
            "arguments": [{"type": "column", "name": "col"}],
            "distinct": false
        });
        assert_eq!(render_expression(&avg).unwrap(), r#"AVG("COL")"#);

        // A column argument carrying a tableAlias renders table-qualified via the
        // shared `column` arm — nested aggregate arguments qualify over a join.
        let sum_qualified = json!({
            "type": "function_aggregate",
            "name": "SUM",
            "arguments": [{"type": "column", "name": "amount", "tableAlias": "O"}],
            "distinct": false
        });
        assert_eq!(
            render_expression(&sum_qualified).unwrap(),
            r#"SUM("O"."AMOUNT")"#
        );
    }

    #[test]
    fn render_expression_renders_scalar_wrapping_aggregates() {
        // The reported failing select item:
        //   ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)
        let sum_case = json!({
            "type": "function_aggregate",
            "name": "SUM",
            "arguments": [{
                "type": "function_scalar",
                "name": "CASE",
                "arguments": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "l_returnflag"},
                     "right": {"type": "literal_string", "value": "R"}},
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}
                ]
            }],
            "distinct": false
        });
        let count_star = json!({
            "type": "function_aggregate",
            "name": "COUNT",
            "arguments": [],
            "distinct": false
        });
        let round = json!({
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    {"type": "function_scalar", "name": "MULT", "arguments": [
                        {"type": "literal_double", "value": 100.0},
                        sum_case
                    ]},
                    count_star
                ]},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });

        let sql = render_expression_safe(&round).expect("scalar-over-aggregate must render");
        assert!(
            sql.contains(r#"SUM(CASE WHEN ("L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
            "nested SUM(CASE ...) must be spliced verbatim: {sql}"
        );
        assert!(
            sql.contains("COUNT(*)"),
            "nested COUNT(*) must render as the star case: {sql}"
        );
    }

    #[test]
    fn aggregate_with_unrenderable_argument_declines() {
        let bad = json!({
            "type": "function_aggregate",
            "name": "SUM",
            "arguments": [{"type": "totally_unknown_node"}],
            "distinct": false
        });
        assert!(
            render_expression(&bad).is_err(),
            "an unrenderable argument must raise in raising mode"
        );
        assert!(
            render_expression_safe(&bad).is_none(),
            "an unrenderable argument must be None in safe mode"
        );
    }

    // --- CAST dialect split (DataFusion vs Exasol) ---
    //
    // The SAME expression node renders differently depending on which parser
    // will consume the fragment: DataFusion's SQL frontend rejects a length on
    // VARCHAR (bare `VARCHAR`), while Exasol's own parser REQUIRES a length
    // (`VARCHAR(n)`). These guard the Exasol-dialect entry points and the
    // divergence between the two so a future refactor cannot silently collapse
    // them back together.

    #[test]
    fn renders_cast_varchar_exasol_dialect_includes_length() {
        let expr = json!({
            "type": "function_scalar_cast", "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "VARCHAR", "size": 100}
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("X" AS VARCHAR(100))"#
        );
    }

    #[test]
    fn renders_cast_char_exasol_dialect_includes_length() {
        let expr = json!({
            "type": "function_scalar_cast", "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("X" AS VARCHAR(3))"#
        );
    }

    /// Divergence guard: the SAME node must render bare `VARCHAR` in the
    /// DataFusion dialect and length-qualified `VARCHAR(n)` in the Exasol
    /// dialect. If a future change collapses the two dialects together, exactly
    /// one of these assertions fails, catching the regression that reintroduces
    /// the "unexpected ')', expecting '(' " Exasol parse error (or the
    /// datafusion-sql "length not supported" error, depending on direction).
    #[test]
    fn cast_char_target_diverges_between_dialects() {
        let expr = json!({
            "type": "function_scalar_cast", "name": "CAST",
            "arguments": [{"type": "column", "name": "c_varchar"}],
            "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
        });
        // DataFusion dialect: bare VARCHAR, no length.
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("C_VARCHAR" AS VARCHAR)"#
        );
        // Exasol dialect: length-qualified.
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("C_VARCHAR" AS VARCHAR(20))"#
        );
    }

    /// Defensive fallback: a character CAST target with no `size` (which a real
    /// Exasol-sent dataType always carries, but be defensive) renders the
    /// project's "unknown/incompatible width" default `VARCHAR(2000000)` in the
    /// Exasol dialect — never bare `VARCHAR`, which Exasol would reject.
    #[test]
    fn renders_cast_varchar_exasol_dialect_without_size_falls_back() {
        let expr = json!({
            "type": "function_scalar_cast", "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "VARCHAR"}
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("X" AS VARCHAR(2000000))"#
        );
    }
}
