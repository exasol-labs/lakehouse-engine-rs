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
/// call site, threaded through every node:
/// - `DataFusion`: the rendered fragment is embedded in a `ScanSpec`
///   (`filter`/`projection`/`group_keys`) and parsed by DataFusion's SQL
///   frontend INSIDE the scan UDF.
/// - `Exasol`: the rendered fragment becomes part of the outer wrapper SQL text
///   parsed by Exasol's own core engine (the qualified single-table / N-scan
///   join wrapper in `joins.rs`, the grouped-aggregate outer-merge wrapper in
///   `grouped_agg.rs`).
///
/// The governing rule for the `Exasol` dialect: render what Exasol sent —
/// reproduce the original name, argument order, and argument count, so
/// Exasol evaluates exactly the call it emitted rather than a
/// DataFusion-shaped rewrite its own engine cannot parse.
///
/// `TRANSLATED_SCALAR_FNS` is the one declaration the `function_scalar` gate
/// reads (76 names). Each declared name carries an `ExasolForm`:
/// - `VerbatimCall` — rendered ahead of the per-name dispatch as
///   `<NAME>(<rendered args>)` from the node's own uppercased name, with NO
///   arity check.
/// - `Shaped` — falls through to the per-name arm, which owns both dialects.
///
/// Ten names have an Exasol form the gate's `<NAME>(<rendered args>)` rule
/// cannot derive — either because it is not a call at all, or because the
/// DataFusion side is not — and are `Shaped`. They fall into six groups:
/// - the five operator wire names `ADD`, `SUB`, `MULT`, `FLOAT_DIV`, `NEG` —
///   SQL operators, not calls;
/// - `MOD` — Exasol requires the `MOD(a, b)` form, DataFusion the `%`
///   operator;
/// - `CONCAT` — the wire encoding of Exasol's `||` operator, so it renders as
///   chained `||` in both dialects rather than a `CONCAT(...)` call;
/// - `CAST` — dispatches to `render_cast_target`, which branches on dialect
///   in its own right: the two dialects have OPPOSITE requirements for
///   character-type CAST targets. datafusion-sql rejects a length-qualified
///   `VARCHAR(n)` unless `support_varchar_with_length` is enabled (this
///   project does not enable it), so the DataFusion target must be bare
///   `VARCHAR`; Exasol has no length-less VARCHAR/CHAR type, so `VARCHAR`
///   MUST be followed by `(n)`;
/// - the `REGEXP_LIKE` alternate `function_scalar` encoding — Exasol's own
///   form is the infix predicate `(<subject> REGEXP_LIKE <pattern>)`, not a
///   call;
/// - `CASE` — renders as `CASE WHEN ... THEN ... [ELSE ...] END`, not a call.
///
/// Absence from `TRANSLATED_SCALAR_FNS` is how a translation is retired: the
/// gate declines the name in both dialects with `unsupported scalar
/// function: <name>`, and no per-name arm is reachable without a row here.
/// The now-family — `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `SYSDATE`, and
/// `SYSTIMESTAMP` — is the current instance: the scan UDF receives no time
/// zone, no clock, and no statement anchor, so these four are unadvertised
/// and left for Exasol to evaluate itself.
///
/// Six node types outside `function_scalar` also branch on dialect —
/// `function_scalar_extract`, `function_scalar_cast`, `predicate_like_regexp`,
/// `literal_timestamp`, `literal_timestamp_utc`, and `literal_timestamputc` (the
/// last two are the same TSTZ literal shape under two wire names — `literal_timestamputc`
/// is the one Exasol actually sends, #242) — but none of them is
/// declared here: they are covered by their own rows in the
/// `exasol_dialect_renders_declared_verbatim_surface` sweep test (the first five) or
/// their own dedicated test (`literal_timestamputc`, which unlike the other five
/// declines rather than renders in the DataFusion dialect), not by this
/// declaration.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    DataFusion,
    Exasol,
}

/// How a translated `function_scalar` name is rendered in the Exasol dialect.
#[derive(Clone, Copy)]
enum ExasolForm {
    /// Rendered ahead of the per-name dispatch as `<NAME>(<rendered args>)`, from
    /// the node's own uppercased name and with NO arity check: Exasol's own
    /// compiler emitted the call and Exasol's own engine evaluates it, so
    /// reproducing the name, argument order, and argument count cannot be wrong.
    VerbatimCall,
    /// Rendered by the name's own per-name arm, which owns BOTH dialects. This is
    /// for names whose Exasol form the gate's `<NAME>(<rendered args>)` rule
    /// cannot derive: either because it is not a call at all — an operator, an
    /// infix predicate, a `CASE`, or a per-dialect CAST target — or because the
    /// DataFusion side is not, as with `MOD`, whose Exasol form IS the call
    /// `MOD(a, b)` but whose DataFusion side is the `%` operator, so the arm
    /// must own both dialects.
    Shaped,
}

/// Every `function_scalar` name this translator translates, with its
/// Exasol-dialect form.
///
/// This is the single declaration of the translated surface, and it GATES the
/// dispatch: a name absent from it is declined in both dialects before any
/// per-name arm is reached. A per-name arm added without a row here is therefore
/// unreachable, rather than silently rendering DataFusion SQL on the
/// Exasol-parsed path — and absence is likewise how a translation is retired.
///
/// The ten [`ExasolForm::Shaped`] names lead the list, because they are the
/// exceptions a reader needs first: every other name renders verbatim. Order is
/// otherwise immaterial — the lookup is by name, not by position, which is the
/// whole point of moving the decision out of arm ordering.
const TRANSLATED_SCALAR_FNS: &[(&str, ExasolForm)] = &[
    // ---- Shaped: the ten names whose Exasol form is NOT a `<NAME>(<args>)` call,
    // so their own arm owns both dialects.
    //
    // Arithmetic and unary negation: SQL operators, not calls.
    ("ADD", ExasolForm::Shaped),
    ("SUB", ExasolForm::Shaped),
    ("MULT", ExasolForm::Shaped),
    ("FLOAT_DIV", ExasolForm::Shaped),
    ("NEG", ExasolForm::Shaped),
    // CAST: the target type is rendered per dialect by `render_cast_target`.
    ("CAST", ExasolForm::Shaped),
    // REGEXP_LIKE: Exasol's own form is an infix predicate, not a call.
    ("REGEXP_LIKE", ExasolForm::Shaped),
    // MOD: Exasol requires the MOD(a, b) form, DataFusion the `%` operator. The
    // Exasol side happens to be a call, but the DataFusion side is not, so the arm
    // owns both dialects rather than the gate owning one of them (#197).
    ("MOD", ExasolForm::Shaped),
    // CONCAT: the wire encoding of Exasol's `||` operator, so it renders as
    // chained `||` in both dialects rather than as a CONCAT(...) call.
    ("CONCAT", ExasolForm::Shaped),
    // CASE: `CASE WHEN ... THEN ... [ELSE ...] END`, not a call.
    ("CASE", ExasolForm::Shaped),
    // ---- VerbatimCall: every remaining name. Exasol has all of them, so the
    // Exasol dialect re-emits the name, argument order, and argument count Exasol
    // itself sent, and each per-name arm below serves the DataFusion dialect alone.
    // The rule is applied to the whole set rather than only to the names whose
    // DataFusion rendering fails to compile on Exasol: a rule applied to some names
    // and not others cannot be reasoned about, because the next reader cannot tell
    // which renderings are principled and which merely happen to work.
    //
    // Math family. DataFusion renders SIGN as `signum`, which Exasol does not have
    // ("function or script SIGNUM not found", 42000). SIGN is also why the gate
    // sits ahead of the WHOLE `match fn_name.as_str()` rather than being a guard
    // inside it: the math arm matches SIGN, so any guard placed after that arm
    // would still render `signum` (issue #209).
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
    // String family. This is what lets a 3-argument `INSTR(s, sub, start)` or
    // `LOCATE(sub, s, start)` evaluate correctly on the Exasol-parsed path (issue
    // #210): Exasol's own INSTR/LOCATE already understand the optional start
    // argument, so there is nothing to translate and an arity check could only
    // reject valid input. The DataFusion dialect needs the name-mapping and
    // argument-reordering arms instead, having no function of these exact names and
    // arities (LENGTH -> character_length, UNICODE -> ascii, INSTR/LOCATE -> strpos).
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
    // Comparison and null-handling family. All five already parsed in Exasol, so
    // the rendering changes only in name case for GREATEST/LEAST/NULLIF —
    // NULLIFZERO and ZEROIFNULL additionally shed their DataFusion emulations
    // (`nullif(v, 0)` and `coalesce(v, 0)`), gaining parity by construction.
    ("GREATEST", ExasolForm::VerbatimCall),
    ("LEAST", ExasolForm::VerbatimCall),
    ("NULLIF", ExasolForm::VerbatimCall),
    ("NULLIFZERO", ExasolForm::VerbatimCall),
    ("ZEROIFNULL", ExasolForm::VerbatimCall),
    // Date field shortcuts. DataFusion renders these as `date_part('<FIELD>', x)`,
    // and Exasol has no DATE_PART at all ("function or script DATE_PART not
    // found", 42000), so this is the second family issue #209 reported failing.
    ("YEAR", ExasolForm::VerbatimCall),
    ("MONTH", ExasolForm::VerbatimCall),
    ("DAY", ExasolForm::VerbatimCall),
    ("HOUR", ExasolForm::VerbatimCall),
    ("MINUTE", ExasolForm::VerbatimCall),
    ("SECOND", ExasolForm::VerbatimCall),
    ("WEEK", ExasolForm::VerbatimCall),
    // DATE_TRUNC, TO_DATE, and TO_TIMESTAMP share Exasol's name and argument order,
    // and both dialects forward the format/unit literal untouched, so the verbatim
    // rendering differs only in name case.
    ("DATE_TRUNC", ExasolForm::VerbatimCall),
    ("TO_DATE", ExasolForm::VerbatimCall),
    ("TO_TIMESTAMP", ExasolForm::VerbatimCall),
    // Date-difference family. DataFusion has none of these names and emulates them
    // (a CAST-to-DATE difference, or a `date_part('epoch', …)` difference), so the
    // *_BETWEEN pushdown shipped in `add-date-arithmetic-pushdown` was broken on the
    // Exasol-parsed path from day one — DATE_PART again.
    ("DAYS_BETWEEN", ExasolForm::VerbatimCall),
    ("HOURS_BETWEEN", ExasolForm::VerbatimCall),
    ("MINUTES_BETWEEN", ExasolForm::VerbatimCall),
    ("SECONDS_BETWEEN", ExasolForm::VerbatimCall),
    // NOT declared, and therefore not translated in either dialect: CURRENT_DATE,
    // SYSDATE, CURRENT_TIMESTAMP, and SYSTIMESTAMP. The verbatim rule works because
    // Exasol's compiler emitted the call and Exasol's engine will evaluate it; it
    // cannot help a function whose value depends on context the scan never receives.
    // The scan UDF gets neither SESSIONTIMEZONE nor DBTIMEZONE, opens no
    // connect-back session, and holds no statement anchor, so it read its container
    // clock in UTC once per shard. Their capabilities are withdrawn too
    // (`capabilities.rs`), so Exasol keeps the work and evaluates its own clock
    // once per statement in its own zones. See `now_family_falls_through`.
];

/// The declared Exasol-dialect form of a `function_scalar` name, or `None` when
/// the translator does not translate that name at all.
fn declared_scalar_fn(name: &str) -> Option<ExasolForm> {
    TRANSLATED_SCALAR_FNS
        .iter()
        .find(|(declared, _)| declared.eq_ignore_ascii_case(name))
        .map(|(_, form)| *form)
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

/// Whether a VS expression node type always yields a boolean result —
/// literal booleans and the predicate family. Used to detect a boolean
/// operand being converted to string (CAST or CONCAT/`||`) so the rendering
/// can substitute Exasol's TRUE/FALSE casing instead of leaking DataFusion's
/// lowercase boolean->Utf8 cast (#200).
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

/// Render a boolean SQL fragment as an Exasol-cased, NULL-preserving string.
///
/// Exasol renders BOOLEAN as `TRUE`/`FALSE`; DataFusion's boolean->Utf8 cast
/// kernel renders lowercase `true`/`false`. The simple-CASE form evaluates
/// `bool_expr` once and falls through to `ELSE NULL` when it is NULL, so a
/// NULL boolean converts to NULL rather than the string `'NULL'` or a
/// coerced `'FALSE'` (#200).
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

/// The Exasol-dialect rendering shared by `literal_timestamp` and
/// `literal_timestamp_utc`.
///
/// Both node types render the SAME bare `TIMESTAMP '<value>'` literal — the value
/// Exasol's own compiler sent, single-quoted with internal quotes doubled as for
/// `literal_string`. Owned in one place so the two arms cannot drift into
/// rendering different strings for the same instant, which is the defect this
/// form fixes: the DataFusion dialect appends a `+00:00` offset for the UTC node
/// type, and Exasol's literal format (`YYYY-MM-DD HH24:MI:SS.FF9`) has no offset
/// field, rejecting one with `data exception - invalid character value for cast`
/// (22018).
///
/// An absent or JSON-null value renders as the bare `NULL` keyword rather than a
/// typed literal, because `TIMESTAMP NULL` is a syntax error on Exasol
/// (`unexpected TIMESTAMP_`, 42000) while `NULL` is valid in every position a
/// timestamp literal can occupy.
fn render_exasol_timestamp_literal(value: Option<&Json>) -> String {
    match value {
        None | Some(Json::Null) => "NULL".to_string(),
        Some(v) => format!("TIMESTAMP {}", quote_literal(Some(v))),
    }
}

/// The Exasol-dialect rendering for a TSTZ literal (`literal_timestamp_utc` /
/// `literal_timestamputc`). The wire value is UTC-normalized, so converting it
/// into `SESSIONTIMEZONE` and re-declaring it TSTZ reproduces the SAME value
/// Exasol's own engine computes for the equivalent native expression —
/// verified live (Exasol 2025.2.1) against both a projected constant and a
/// self-applied filter comparison (#218): a bare comparison of this literal
/// against a plain-`TIMESTAMP` column disagrees with Exasol's own
/// `TIMESTAMP` vs `TIMESTAMP WITH LOCAL TIME ZONE` coercion rule, which reads
/// the naive side as session-local rather than comparing raw values.
/// `SESSIONTIMEZONE` is referenced symbolically, never resolved by the
/// adapter. NULL stays bare (`NULL`, matching `render_exasol_timestamp_literal`
/// above) — `CAST`/`CONVERT_TZ` add nothing to a three-valued NULL comparison
/// or projection.
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

/// Whether an argument node is a NULL-valued literal: a `literal_null` node,
/// or any `literal_*` node whose `value` is JSON `null` or absent. Used to
/// strip NULL entries from a const list before rendering, since Exasol's
/// `IN`/`NOT IN` ignores NULL list entries while DataFusion's three-valued
/// logic would otherwise silently empty the result (#206).
fn is_null_literal(arg: &Json) -> bool {
    match arg.get("type").and_then(|t| t.as_str()) {
        Some("literal_null") => true,
        Some(t) if t.starts_with("literal_") => {
            matches!(arg.get("value"), None | Some(Json::Null))
        }
        _ => false,
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

/// Snap a TIMESTAMP fractional-seconds precision to the nearest unit
/// DataFusion 54's SQL frontend can parse in `CAST(x AS TIMESTAMP(p))`.
///
/// DataFusion 54 parses `TIMESTAMP(p)` only for `p` in `{0,3,6,9}`; any other
/// value (1,2,4,5,7,8) is a parse error. This maps each precision to the
/// nearest supported unit — `0→0, 1→0, 2→3, 4→3, 5→6, 7→6, 8→9`, with
/// `0/3/6/9` mapping to themselves — and clamps anything above 9 to 9. The gaps
/// have non-integer midpoints (1.5/4.5/7.5), so "nearest" is unambiguous. Only
/// the DataFusion dialect needs this; Exasol's parser accepts every precision
/// 0-9 verbatim. Pure integer arithmetic — no DataFusion types (this crate has
/// no DataFusion dependency). Mirrors the colocated-pure-helper precedent of
/// `format_decimal_exasol_style` (issue #211).
fn snap_timestamp_precision(p: u64) -> u64 {
    match p {
        0..=1 => 0,
        2..=4 => 3,
        5..=7 => 6,
        // 8, 9, and anything above 9 clamp to 9 (DataFusion's max).
        _ => 9,
    }
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
            //
            // WLTZ is evaluated FIRST and short-circuits before any precision
            // logic — regardless of a present `fractionalSecondsPrecision`.
            if data_type.get("withLocalTimeZone").and_then(|v| v.as_bool()) == Some(true) {
                return Err(UdfError::User(
                    "unsupported CAST target type: TIMESTAMP WITH LOCAL TIME ZONE".into(),
                ));
            }
            // Read the TIMESTAMP fractional-seconds precision. The field is
            // `fractionalSecondsPrecision` (a u64) — NOT `precision`, which
            // Exasol uses only for DECIMAL/INTERVAL. Absent → bare TIMESTAMP
            // (== Exasol's default TIMESTAMP(3)), unchanged in both dialects.
            match data_type
                .get("fractionalSecondsPrecision")
                .and_then(|v| v.as_u64())
            {
                None => Ok("TIMESTAMP".to_string()),
                // Exasol's own parser accepts any precision 0-9 verbatim.
                Some(p) => match dialect {
                    Dialect::Exasol => Ok(format!("TIMESTAMP({p})")),
                    // DataFusion 54's SQL frontend parses TIMESTAMP(p) only for
                    // p in {0,3,6,9}; snap to the nearest supported unit.
                    Dialect::DataFusion => {
                        Ok(format!("TIMESTAMP({})", snap_timestamp_precision(p)))
                    }
                },
            }
        }
        other => Err(UdfError::User(format!(
            "unsupported CAST target type: {other}"
        ))),
    }
}

/// Wrap an already-rendered SQL fragment so it reproduces Exasol's
/// shortest-form DECIMAL→string conversion (trailing scale zeros trimmed).
///
/// This is a pure syntactic helper with NO type information of its own: it
/// blindly casts `expr_sql` to text and strips a trailing run of zeros after
/// a literal `.`, then drops a now-empty `.` entirely. The caller MUST have
/// already confirmed `expr_sql` is a DECIMAL-typed expression before calling
/// this — applying it to a non-decimal string that happens to end in zeros
/// (e.g. `'foo100'`) would corrupt it. It is the shared reusable primitive
/// behind issue #211's decimal-trim fix, wrapped by the `decimal_to_varchar_exasol`
/// node below.
fn format_decimal_exasol_style(expr_sql: &str) -> String {
    format!(
        "regexp_replace(regexp_replace(CAST({expr_sql} AS VARCHAR), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')"
    )
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

    // A boolean source cast to a string type must render Exasol's TRUE/FALSE
    // casing, not DataFusion's lowercase boolean->Utf8 cast kernel (#200).
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
            // Exasol's core engine parses the wrapper SQL and has no `arrow_cast`
            // ("function or script ARROW_CAST not found", 42000), so re-emit the
            // bare literal Exasol itself sent.
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_timestamp_literal(value("value"))));
            }
            // Render via arrow_cast at explicit microsecond precision: a bare
            // `TIMESTAMP '...'` is typed Timestamp(Nanosecond) by DataFusion's SQL
            // frontend, which overflows in simplify_expressions when unified with
            // the scan's microsecond-typed columns on far-future values (#155).
            //
            // An absent or JSON-null `value` reaches `quote_literal` as the bare
            // NULL keyword, so this arm renders `arrow_cast(NULL, …)` whereas the
            // `literal_timestamp_utc` arm below short-circuits to a bare `NULL`.
            // Both DataFusion renderings predate the dialect split and are frozen
            // by `renders_null_valued_timestamp_literal_per_dialect`; the two are
            // deliberately NOT aligned with each other.
            return Ok(Some(format!(
                "arrow_cast({}, 'Timestamp(Microsecond, None)')",
                quote_literal(value("value"))
            )));
        }
        "literal_timestamp_utc" => {
            // The Exasol dialect converts the UTC-normalized wire value into
            // SESSIONTIMEZONE and re-declares it TSTZ — see
            // `render_exasol_tstz_literal`'s doc for the live-verified reasoning
            // (#218).
            if dialect == Dialect::Exasol {
                return Ok(Some(render_exasol_tstz_literal(value("value"))));
            }
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
        "literal_timestamputc" => {
            // Exasol's REAL wire node name for a TSTZ literal — no underscore before
            // `utc` — which `literal_timestamp_utc` above never matches on real
            // traffic (#242). Accept it in the Exasol dialect only, rendering
            // identically to the arm above (`render_exasol_tstz_literal`). The
            // DataFusion dialect keeps declining it — `None`, the same unmatched
            // outcome as today — so the pushed `ScanSpec.filter` stays
            // byte-identical: accepting it there would start pushing TSTZ
            // predicates into DataFusion, whose coercion against a naive
            // `timestamp_us` column is unverified (#242, deliberately deferred).
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
            // A `tableAlias` (injected by the caller for a multi-table render, e.g.
            // the join two-scan wrapper) qualifies the reference as
            // `"ALIAS"."NAME"`, disambiguating a name shared by two joined subqueries.
            // For a single-relation target the caller strips `tableAlias` before
            // calling this renderer, which then falls through to a bare quoted name.
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
                // Exasol's own parser accepts only the infix REGEXP_LIKE
                // predicate form, not a `regexp_like(...)` call ("syntax error,
                // unexpected REGEXP_LIKE_", 42000).
                Dialect::Exasol => format!("({subject} REGEXP_LIKE {pattern})"),
                Dialect::DataFusion => format!("regexp_like({subject}, {pattern})"),
            }))
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
            Ok(Some(match dialect {
                // Exasol's own EXTRACT takes the field as a bare keyword, not a
                // quoted string literal.
                Dialect::Exasol => format!("EXTRACT({field} FROM {src})"),
                // DataFusion 54 (default features) has no EXTRACT(field FROM expr)
                // ExprPlanner; render the portable function form
                // date_part('FIELD', expr) instead.
                Dialect::DataFusion => format!("date_part('{field}', {src})"),
            }))
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
        // in `dataType` (verified against the real Exasol wire shape):
        //   {"type":"function_scalar_cast","name":"CAST","dataType":{...},"arguments":[<src>]}
        // This is the shape real Exasol traffic hits; the nested
        // `function_scalar`+name=CAST arm below is a defensive alternate encoding.
        "function_scalar_cast" => {
            let args = value("arguments").and_then(|a| a.as_array());
            render_cast(args, value("dataType"), dialect)
        }
        // Adapter-synthesized node (issue #211) — NEVER emitted by Exasol on the
        // wire. Marks an expression argument already confirmed (by the adapter,
        // `lakehouse-engine::adapter::pushdown::support`) to be a bare
        // DECIMAL-typed column being stringified (CAST to VARCHAR/CHAR, CONCAT,
        // LENGTH, ...); wraps the recursively-rendered argument with
        // `format_decimal_exasol_style` to reproduce Exasol's shortest-form
        // DECIMAL→string conversion (trailing scale zeros trimmed) instead of
        // DataFusion's fixed-scale formatting.
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

            // The declaration gates the dispatch. An undeclared name is not
            // translated in either dialect, so a per-name arm below with no
            // `TRANSLATED_SCALAR_FNS` row is unreachable. In the Exasol dialect, a
            // declared `VerbatimCall` renders here, AHEAD of the per-name arms, so
            // arm order carries no dialect precedence; a declared `Shaped` name (or
            // any name in the DataFusion dialect) falls through to its own arm,
            // which owns both dialects.
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
                // `function_scalar_cast` node handled above; this arm is kept
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
                    Ok(Some(match dialect {
                        // Same infix form as the predicate_like_regexp node type
                        // above — this alternate encoding must render
                        // byte-identically to it within a dialect.
                        Dialect::Exasol => format!("({subject} REGEXP_LIKE {pattern})"),
                        Dialect::DataFusion => format!("regexp_like({subject}, {pattern})"),
                    }))
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
                // MOD: DataFusion 54 exposes modulo only as the % operator, but
                // Exasol's own parser rejects %  — it requires the MOD(a, b) form.
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
                // CONCAT → the wire encoding of Exasol's `||` operator, so it
                // is rendered as chained `||`, not DataFusion's concat()
                // function: concat() silently ignores NULL arguments
                // (`concat(NULL, 'x')` = `'x'`), while both Exasol's `||` and
                // DataFusion's `||` operator propagate NULL (`NULL || 'x'` =
                // `NULL`) — using concat() would drop the NULL-preservation
                // this rewrite depends on. A boolean operand is rewritten to
                // the Exasol-cased form before joining, since DataFusion's
                // boolean->Utf8 cast (which `||` falls back to for a raw
                // boolean operand) renders lowercase `true`/`false` (#200).
                "CONCAT" => {
                    let args = args.ok_or_else(|| {
                        UdfError::User("function_scalar CONCAT missing 'arguments'".into())
                    })?;
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
                    Ok(Some(format!("({})", rendered.join(" || "))))
                }
                // String functions: name-mapping table (DataFusion dialect). The
                // Exasol dialect never reaches this arm — those names are declared
                // `VerbatimCall`, so the gate above renders them from Exasol's own
                // name, order, and argument count.
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
                // INSTR(string, substring) and LOCATE(substring, string) both → strpos(string, substring).
                // DataFusion dialect only — the Exasol dialect renders both verbatim
                // at the gate, ahead of this dispatch.
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
                // Backstop only: the gate above already declined every undeclared
                // name with this exact message, so this arm is reachable only if a
                // declared name loses its per-name arm.
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
/// - the filter is trivially true (`TRUE` or `NULL`).
///
/// These two causes are NOT distinguishable from this function's `None` alone, and
/// this function does not decide what a `None` means for the request as a whole —
/// that is the caller's responsibility (see `datafusion_renderable` in the adapter's
/// `pushdown/support.rs`), which is the only place that knows whether the filter was
/// absent, trivially true, or actually declined and therefore needs self-applying
/// rather than omitting.
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
/// (`TRUE`/`NULL`), mirroring [`render_df_filter_safe`] exactly — including that
/// those two causes are not distinguishable here, and what a `None` means for the
/// request as a whole is the caller's responsibility, not this function's. Exasol
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

    #[test]
    fn renders_in_constlist_strips_null() {
        let expr = json!({
            "type": "predicate_in_constlist",
            "expression": {"type": "column", "name": "status"},
            "arguments": [
                {"type": "literal_string", "value": "A"},
                {"type": "literal_null"},
                {"type": "literal_date", "value": null}
            ]
        });
        let sql = render_expression(&expr).unwrap();
        assert!(sql.contains("'A'"), "'A' not found: {sql}");
        assert!(!sql.contains("NULL"), "NULL should not survive: {sql}");
    }

    #[test]
    fn renders_all_null_in_as_false() {
        let expr = json!({
            "type": "predicate_in_constlist",
            "expression": {"type": "column", "name": "x"},
            "arguments": [
                {"type": "literal_null"},
                {"type": "literal_date", "value": null}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), "FALSE");
    }

    #[test]
    fn renders_not_in_constlist_strips_null() {
        let expr = json!({
            "type": "predicate_not",
            "expression": {
                "type": "predicate_in_constlist",
                "expression": {"type": "column", "name": "status"},
                "arguments": [
                    {"type": "literal_string", "value": "A"},
                    {"type": "literal_null"},
                    {"type": "literal_date", "value": null}
                ]
            }
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, r#"(NOT ("STATUS" IN ('A')))"#);
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
    // actually emits, NOT the earlier `{"type":"function_scalar",...}` shape
    // whose mismatch let a dispatch bug hide (CAST never reached its arm).

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
    fn renders_cast_bool_to_varchar_as_exasol_case_uppercase() {
        // #200: CAST(<bool> AS VARCHAR) must render Exasol's TRUE/FALSE
        // casing, not DataFusion's lowercase boolean->Utf8 cast.
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{
                "type": "predicate_greater",
                "left": {"type": "column", "name": "c_acctbal"},
                "right": {"type": "literal_exactnumeric", "value": 0}
            }],
            "dataType": {"type": "VARCHAR", "size": 10}
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CASE ("C_ACCTBAL" > 0) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#
        );
    }

    #[test]
    fn renders_cast_bool_to_varchar_uses_case_for_any_predicate_source() {
        // A boolean-producing predicate other than a comparison (here
        // `predicate_is_null`) is detected the same way, confirming the CASE
        // rewrite isn't special-cased to `predicate_greater` alone. Runtime
        // NULL-preservation itself (a NULL comparison falling through the
        // CASE's `ELSE NULL`, never 'NULL' or a coerced 'FALSE') is exercised
        // end-to-end in `boolean_to_string_casing_test.rs`.
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{
                "type": "predicate_is_null",
                "expression": {"type": "column", "name": "x"}
            }],
            "dataType": {"type": "VARCHAR", "size": 10}
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CASE ("X" IS NULL) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#
        );
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
        // Plain TIMESTAMP with a present fractionalSecondsPrecision of 3:
        // Exasol sends {"type":"TIMESTAMP","withLocalTimeZone":false,
        // "fractionalSecondsPrecision":3}. A present precision now renders
        // verbatim `TIMESTAMP(3)` (issue #212); 3 is a DataFusion-supported unit,
        // so the DataFusion dialect renders it identically (identity snap).
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": {"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 3}
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP(3))"#
        );
    }

    #[test]
    fn renders_cast_timestamp_precision_per_dialect() {
        // Build a CAST-to-TIMESTAMP expression node with the given dataType.
        fn cast(data_type: Json) -> Json {
            json!({
                "type": "function_scalar_cast",
                "name": "CAST",
                "arguments": [{"type": "column", "name": "x"}],
                "dataType": data_type
            })
        }

        // Exasol dialect renders any precision 0-9 VERBATIM (Exasol's parser
        // accepts every fractional-seconds precision).
        for p in [0u64, 6, 9] {
            let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                format!(r#"CAST("X" AS TIMESTAMP({p}))"#),
                "Exasol dialect must render TIMESTAMP({p}) verbatim"
            );
        }

        // DataFusion dialect renders a supported precision VERBATIM (identity
        // snap for 6).
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": 6}));
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP(6))"#
        );

        // DataFusion dialect SNAPS an unsupported precision to the nearest
        // supported unit: 5 -> 6 (DataFusion 54 parses TIMESTAMP(p) only for
        // {0,3,6,9}).
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": 5}));
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP(6))"#
        );

        // Absent precision renders bare TIMESTAMP in BOTH dialects (unchanged),
        // whether the dataType omits withLocalTimeZone entirely or sets it false.
        for data_type in [
            json!({"type": "TIMESTAMP"}),
            json!({"type": "TIMESTAMP", "withLocalTimeZone": false}),
        ] {
            let expr = cast(data_type.clone());
            assert_eq!(
                render_expression(&expr).unwrap(),
                r#"CAST("X" AS TIMESTAMP)"#,
                "DataFusion dialect: absent precision must render bare TIMESTAMP for {data_type}"
            );
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                r#"CAST("X" AS TIMESTAMP)"#,
                "Exasol dialect: absent precision must render bare TIMESTAMP for {data_type}"
            );
        }
    }

    #[test]
    fn cast_to_unsupported_target_declines() {
        // Exasol CAST targets with no faithful DataFusion 54 equivalent. Each is
        // sent with the dataType descriptor shape shown (verified against the
        // Exasol virtual-schema data-types API). The translator declines these
        // targets (Err in raising mode, None in safe mode); there is no
        // Exasol-side re-check of an advertised capability, so it is the
        // caller's job to decide what to do with that `None`/`Err` — the
        // adapter's declined-predicate route errors rather than omitting the
        // CAST.
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

    // --- decimal_to_varchar_exasol (issue #211) ---
    //
    // Adapter-synthesized node, never sent by Exasol on the wire. Fixtures
    // exercise the render arm in isolation (arity + wrapping), independent of
    // the adapter-side rewrite that synthesizes this node (a later task).

    #[test]
    fn renders_decimal_to_varchar_exasol() {
        let expr = json!({
            "type": "decimal_to_varchar_exasol",
            "arguments": [{"type": "column", "name": "c_decimal_a"}]
        });
        let expected = format_decimal_exasol_style(r#""C_DECIMAL_A""#);
        assert_eq!(render_expression(&expr).unwrap(), expected);
        assert_eq!(render_expression_safe(&expr).unwrap(), expected);
    }

    #[test]
    fn decimal_to_varchar_exasol_wrong_arity_errors() {
        let no_args = json!({
            "type": "decimal_to_varchar_exasol",
            "arguments": []
        });
        let two_args = json!({
            "type": "decimal_to_varchar_exasol",
            "arguments": [
                {"type": "column", "name": "c_decimal_a"},
                {"type": "column", "name": "c_decimal_b"}
            ]
        });
        for expr in [&no_args, &two_args] {
            assert!(
                render_expression(expr).is_err(),
                "decimal_to_varchar_exasol with non-unary arguments must raise: {expr}"
            );
            assert!(
                render_expression_safe(expr).is_none(),
                "decimal_to_varchar_exasol with non-unary arguments must be None in safe mode: {expr}"
            );
        }
    }

    #[test]
    fn format_decimal_exasol_style_renders_exact_regex_sql() {
        // Pins the emitted SQL text itself (no DataFusion runtime involved).
        // Runtime correctness of this regex against a real engine is covered
        // by the E2E tests in `crates/lakehouse-engine/tests/e2e_capability_test.rs`
        // (`e2e_decimal_cast_trims_trailing_zeros` and friends).
        assert_eq!(
            format_decimal_exasol_style("some_col"),
            r#"regexp_replace(regexp_replace(CAST(some_col AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')"#
        );
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

    #[test]
    fn renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect() {
        // `arrow_cast` is DataFusion-only: Exasol's core engine rejects the
        // wrapper SQL with "function or script ARROW_CAST not found" (42000,
        // verified on live Exasol 2025.2.1). The Exasol dialect re-emits the bare
        // `TIMESTAMP '<value>'` literal Exasol's own compiler sent, while the
        // DataFusion rendering stays byte-identical so the scan keeps the
        // explicit microsecond typing that issue #155 depends on.
        //
        // `literal_timestamp_utc` is NOT bare in the Exasol dialect — see
        // `renders_timestamp_utc_literal_via_convert_tz_in_exasol_dialect` below.
        let ts = json!({"type": "literal_timestamp", "value": "2024-01-15 12:00:00"});
        let ts_utc = json!({"type": "literal_timestamp_utc", "value": "2024-03-01 10:00:00"});

        let ts_exasol = render_expression_exasol(&ts).unwrap();
        assert_eq!(ts_exasol, "TIMESTAMP '2024-01-15 12:00:00'");
        assert!(
            !ts_exasol.contains("arrow_cast"),
            "Exasol rejects arrow_cast with sqlCode 42000: {ts_exasol}"
        );

        // Internal single quotes are doubled, exactly as for `literal_string`, so
        // no literal value can terminate the quoted literal early.
        assert_eq!(
            render_expression_exasol(&json!({
                "type": "literal_timestamp",
                "value": "2024-01-15 12:00:00' OR '1'='1"
            }))
            .unwrap(),
            "TIMESTAMP '2024-01-15 12:00:00'' OR ''1''=''1'"
        );

        // The DataFusion dialect is frozen.
        assert_eq!(
            render_expression(&ts).unwrap(),
            "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
        );
        assert_eq!(
            render_expression(&ts_utc).unwrap(),
            "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
        );
    }

    #[test]
    fn renders_timestamp_utc_literal_via_convert_tz_in_exasol_dialect() {
        // The wire value is UTC-normalized (Exasol's TIMESTAMP literal format,
        // `YYYY-MM-DD HH24:MI:SS.FF9`, has no offset field and rejects one with
        // sqlCode 22018, so the value carries no zone marker of its own).
        // Converting it into SESSIONTIMEZONE and re-declaring it TSTZ reproduces
        // the value Exasol's own engine computes for the equivalent native
        // expression — verified live (#218): a BARE comparison of this literal
        // against a plain-TIMESTAMP column disagrees with Exasol's own
        // TIMESTAMP-vs-TSTZ coercion rule (session-local interpretation of the
        // naive side), so the Exasol dialect must NOT render it bare like
        // `literal_timestamp` does.
        let value = "2024-03-01 10:00:00";
        let ts_utc = json!({"type": "literal_timestamp_utc", "value": value});

        let exasol = render_expression_exasol(&ts_utc).unwrap();
        assert_eq!(
            exasol,
            "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 10:00:00', 'UTC', SESSIONTIMEZONE) \
             AS TIMESTAMP WITH LOCAL TIME ZONE)"
        );
        assert!(
            !exasol.contains("+00:00"),
            "Exasol rejects an offset in a TIMESTAMP literal with sqlCode 22018: {exasol}"
        );

        assert!(
            render_expression(&ts_utc).unwrap().contains("+00:00"),
            "the DataFusion dialect keeps the offset that types the literal UTC"
        );
    }

    #[test]
    fn literal_timestamputc_wire_name_renders_exasol_only() {
        // Exasol's real wire node name for a TSTZ literal is `literal_timestamputc`
        // (no underscore before `utc`) — `literal_timestamp_utc` above never
        // matches real traffic (#242). The Exasol dialect accepts the real name
        // and renders it identically to `literal_timestamp_utc`.
        let value = "2024-03-01 09:00:00";
        let real_wire_name = json!({"type": "literal_timestamputc", "value": value});
        assert_eq!(
            render_expression_exasol(&real_wire_name).unwrap(),
            "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) \
             AS TIMESTAMP WITH LOCAL TIME ZONE)"
        );

        // The DataFusion dialect keeps declining it — the SAME unmatched/`None`
        // outcome as an entirely unknown node type — so the pushed `ScanSpec.filter`
        // stays byte-identical for every request. Locked here so a later change
        // cannot silently widen the scan filter without also touching this test
        // (#242 stays a deliberate, tracked deferral, not accepted by accident).
        assert!(render_expression_safe(&real_wire_name).is_none());
    }

    #[test]
    fn renders_null_valued_tstz_literal_bare_in_exasol_dialect() {
        // NULL stays bare (no CONVERT_TZ/CAST) for both TSTZ literal node
        // types: CAST/CONVERT_TZ add nothing to a three-valued NULL comparison
        // or projection, and `TIMESTAMP NULL` is a syntax error on Exasol
        // (`unexpected TIMESTAMP_`, 42000), so `render_exasol_timestamp_literal`'s
        // existing bare-`NULL` short-circuit is reused rather than duplicated.
        for node_type in ["literal_timestamp_utc", "literal_timestamputc"] {
            for node in [
                json!({"type": node_type, "value": null}),
                json!({"type": node_type}),
            ] {
                assert_eq!(
                    render_expression_exasol(&node).unwrap(),
                    "NULL",
                    "{node_type} with a null/absent value must render bare NULL"
                );
            }
        }
    }

    #[test]
    fn renders_null_valued_timestamp_literal_per_dialect() {
        // `TIMESTAMP NULL` is a syntax error on Exasol ("unexpected TIMESTAMP_",
        // 42000, verified live on 2025.2.1), so an absent or JSON-null `value`
        // renders as the bare NULL keyword rather than as a typed literal.
        //
        // The DataFusion dialect is ASYMMETRIC across the two node types, and
        // that asymmetry is frozen rather than a defect to align:
        // `literal_timestamp` wraps the NULL keyword in `arrow_cast`, while
        // `literal_timestamp_utc` short-circuits to bare `NULL` before it can
        // build a cast. Pinning both stops a later reader from "fixing" one to
        // match the other and silently changing a frozen DataFusion rendering.
        let cases = [
            (
                "literal_timestamp",
                "arrow_cast(NULL, 'Timestamp(Microsecond, None)')",
            ),
            ("literal_timestamp_utc", "NULL"),
        ];
        for (node_type, expected_datafusion) in cases {
            for (variant, node) in [
                (
                    "carrying a JSON-null value",
                    json!({"type": node_type, "value": null}),
                ),
                ("with no value key at all", json!({"type": node_type})),
            ] {
                assert_eq!(
                    render_expression_exasol(&node).unwrap(),
                    "NULL",
                    "{node_type} {variant}, Exasol dialect"
                );
                assert_eq!(
                    render_expression(&node).unwrap(),
                    expected_datafusion,
                    "{node_type} {variant}, DataFusion dialect"
                );
            }
        }
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

    #[test]
    fn renders_regexp_like_as_infix_predicate_in_exasol_dialect() {
        // Exasol's parser has no `regexp_like(...)` function; it accepts only the
        // infix `(<subject> REGEXP_LIKE <pattern>)` predicate form
        // ("syntax error, unexpected REGEXP_LIKE_", 42000). Both wire encodings —
        // the dedicated `predicate_like_regexp` node type and the alternate
        // `function_scalar` REGEXP_LIKE encoding — must render that same infix
        // form on the Exasol-parsed path, byte-identically to each other, while
        // the DataFusion-dialect rendering of both stays `regexp_like(s, p)`.
        let predicate = json!({
            "type": "predicate_like_regexp",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "^A.*"}
        });
        let scalar = json!({
            "type": "function_scalar",
            "name": "REGEXP_LIKE",
            "arguments": [
                {"type": "column", "name": "name"},
                {"type": "literal_string", "value": "^A.*"}
            ]
        });

        let predicate_exasol = render_expression_exasol(&predicate).unwrap();
        let scalar_exasol = render_expression_exasol(&scalar).unwrap();
        assert_eq!(predicate_exasol, r#"("NAME" REGEXP_LIKE '^A.*')"#);
        assert_eq!(
            scalar_exasol, predicate_exasol,
            "the two REGEXP_LIKE encodings must render byte-identically in the Exasol dialect"
        );

        assert_eq!(
            render_expression(&predicate).unwrap(),
            r#"regexp_like("NAME", '^A.*')"#
        );
        assert_eq!(
            render_expression(&scalar).unwrap(),
            r#"regexp_like("NAME", '^A.*')"#
        );
    }

    #[test]
    fn regexp_like_predicate_missing_operand_errors_in_both_dialects() {
        let missing_pattern = json!({
            "type": "predicate_like_regexp",
            "expression": {"type": "column", "name": "name"}
        });
        assert!(render_expression(&missing_pattern).is_err());
        assert!(render_expression_exasol(&missing_pattern).is_err());

        let missing_expression = json!({
            "type": "predicate_like_regexp",
            "pattern": {"type": "literal_string", "value": "^A.*"}
        });
        assert!(render_expression(&missing_expression).is_err());
        assert!(render_expression_exasol(&missing_expression).is_err());
    }

    #[test]
    fn regexp_like_scalar_arity_errors_in_both_dialects() {
        let one_arg = json!({
            "type": "function_scalar",
            "name": "REGEXP_LIKE",
            "arguments": [{"type": "column", "name": "name"}]
        });
        assert!(render_expression(&one_arg).is_err());
        assert!(render_expression_exasol(&one_arg).is_err());
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

    // --- MOD → % operator (DataFusion) / MOD(...) (Exasol) ---

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

    #[test]
    fn renders_mod_as_function_call_in_exasol_dialect() {
        // https://github.com/exasol-labs/lakehouse-engine-rs/issues/197
        // Exasol's parser rejects `%` — an Exasol-side wrapper (e.g. the
        // COUNT(DISTINCT ...) outer wrapper) must render MOD(a, b) instead.
        let expr = json!({
            "type": "function_scalar",
            "name": "MOD",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        assert_eq!(render_expression_exasol(&expr).unwrap(), r#"MOD("A", 3)"#);
        // DataFusion-dialect rendering of the same node must stay unchanged.
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
    }

    // --- String scalar functions (CONCAT/LENGTH→character_length/INSTR+LOCATE→strpos/...) ---

    #[test]
    fn renders_string_scalar_functions() {
        // Pass-through lowercased
        let cases_lower = [
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

    // --- CONCAT → chained `||` (NULL-propagating, unlike DataFusion's concat()) ---

    #[test]
    fn renders_concat_as_chained_pipe_operator() {
        // Two args: joined with `||`, not concat() — concat() silently turns a
        // NULL operand into empty string (#200's GROUP BY repro shape).
        let expr = json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": ""}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("S" || '')"#);

        // Three args: chained, still no concat() call.
        let expr = json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"},
                {"type": "column", "name": "c"}
            ]
        });
        assert_eq!(render_expression(&expr).unwrap(), r#"("A" || "B" || "C")"#);
    }

    #[test]
    fn renders_concat_bool_operand_as_exasol_case() {
        // A boolean-producing argument (here `predicate_equal`) is rewritten to
        // the Exasol-cased CASE form before joining — DataFusion's `||` falls
        // back to its lowercase boolean->Utf8 cast for a raw boolean operand
        // otherwise (#200).
        let expr = json!({
            "type": "function_scalar",
            "name": "CONCAT",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "active"},
                 "right": {"type": "literal_bool", "value": true}},
                {"type": "literal_string", "value": ""}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"((CASE ("ACTIVE" = TRUE) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) || '')"#
        );
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
    fn renders_extract_as_exasol_extract_from_in_exasol_dialect() {
        // Exasol's parser has no DATE_PART function; the EXTRACT-carrying node
        // must render Exasol's own EXTRACT(<FIELD> FROM <src>) form, with the
        // field as a bare keyword (not a quoted string literal), on the
        // Exasol-parsed path. The DataFusion-dialect rendering of the same
        // node stays unchanged (date_part('<FIELD>', <src>)).
        let expr = json!({
            "type": "function_scalar_extract",
            "name": "EXTRACT",
            "toExtract": "DAY",
            "arguments": [{"type": "column", "name": "ts"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"EXTRACT(DAY FROM "TS")"#
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"date_part('DAY', "TS")"#
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

    // --- CURRENT_DATE / SYSDATE / CURRENT_TIMESTAMP / SYSTIMESTAMP: withdrawn ---

    #[test]
    fn now_family_falls_through() {
        // The now-family is the one translation this change RETIRES rather than
        // re-renders, because no rendering can be right in either dialect. Exasol's
        // four names are three semantics over one instant — CURRENT_TIMESTAMP reads
        // it in the session zone, SYSTIMESTAMP the same instant in the database
        // zone, and CURRENT_DATE/SYSDATE are TO_DATE of each — while the scan UDF
        // receives neither SESSIONTIMEZONE nor DBTIMEZONE, opens no connect-back
        // session, and holds no statement anchor. It read its container clock in UTC
        // once per shard, so a select-list SYSTIMESTAMP returned 15:02:02 through the
        // virtual schema against 17:02:03 natively in the same session, and one
        // statement returned two different timestamps over a two-file table.
        //
        // Withdrawal is total and paired: the four names carry no
        // TRANSLATED_SCALAR_FNS row, so the gate declines them before any per-name
        // arm, and capabilities.rs advertises none of them, so Exasol never delegates
        // one and evaluates its own clock instead. Pinned to the generic decline text
        // as well as the name (same reason as
        // `bitwise_operator_functions_fall_through`): a future arm that merely
        // validated arity would also name the function, and would silently defeat
        // this decline-lock.
        for name in [
            "CURRENT_DATE",
            "SYSDATE",
            "CURRENT_TIMESTAMP",
            "SYSTIMESTAMP",
        ] {
            let expr = json!({"type": "function_scalar", "name": name, "arguments": []});
            for (dialect, rendered) in [
                ("DataFusion", render_expression(&expr)),
                ("Exasol", render_expression_exasol(&expr)),
            ] {
                let err = rendered.unwrap_err().to_string();
                assert!(
                    err.contains("unsupported scalar function"),
                    "{name} must fall through the generic unsupported-scalar-function \
                     path in the {dialect} dialect: {err}"
                );
                assert!(
                    err.contains(name),
                    "the {dialect}-dialect error must name '{name}': {err}"
                );
            }
            assert!(
                render_expression_safe(&expr).is_none(),
                "{name} must be None in the DataFusion safe variant"
            );
            assert!(
                render_expression_exasol_safe(&expr).is_none(),
                "{name} must be None in the Exasol safe variant"
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
    // They now fall through — see `unsupported_date_functions_decline_in_both_dialects`.

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

    /// Pins the dialect asymmetry `fix-declined-filter-self-apply` relies on:
    /// `SECOND` is a DataFusion field-shortcut (exactly 1 argument) but an Exasol
    /// `VerbatimCall` (any arity, rendered as written). A 2-argument `SECOND(ts, 3)`
    /// therefore declines under the DataFusion dialect while still rendering under
    /// the Exasol dialect — the asymmetry a declined filter must be self-applied
    /// through, in Exasol's own dialect, rather than omitted.
    #[test]
    fn second_with_precision_declines_for_datafusion_renders_for_exasol() {
        let expr = json!({
            "type": "function_scalar",
            "name": "SECOND",
            "arguments": [
                {"type": "column", "name": "ts"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });

        assert!(
            render_expression_safe(&expr).is_none(),
            "SECOND(ts, 3) must decline under the DataFusion dialect"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_some(),
            "SECOND(ts, 3) must still render under the Exasol dialect"
        );
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
    fn regexp_scalar_functions_decline_in_both_dialects() {
        // The Rust `regex` crate (DataFusion 54) rejects backreferences and
        // lookaround that Exasol's PCRE dialect accepts (blocks all four),
        // lacks regexp_substr (blocks REGEXP_SUBSTR), and REGEXP_REPLACE /
        // REGEXP_INSTR's argument shapes differ from Exasol's position/
        // occurrence/return options (REGEXP_COUNT's shape actually aligns) —
        // so all four scalar regexp functions decline (issue #106).
        //
        // The decline is a property of the declaration, not of a dialect: these
        // names carry no TRANSLATED_SCALAR_FNS row, so the gate declines them
        // identically in both dialects. Asserting the Exasol dialect too is what
        // stops the verbatim rule from quietly re-admitting a name whose Exasol
        // form would parse but whose semantics were never the reason it declined.
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
            let expected = format!("unsupported scalar function: {name}");
            assert_eq!(
                render_expression(&expr).unwrap_err().to_string(),
                expected,
                "the DataFusion dialect must decline {name}"
            );
            assert_eq!(
                render_expression_exasol(&expr).unwrap_err().to_string(),
                expected,
                "the Exasol dialect must decline {name}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "{name} must be None in the DataFusion safe variant"
            );
            assert!(
                render_expression_exasol_safe(&expr).is_none(),
                "{name} must be None in the Exasol safe variant"
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

    // --- The declaration gates the dispatch (issue #209) ---

    #[test]
    fn undeclared_scalar_function_declines_in_both_dialects() {
        // `TRANSLATED_SCALAR_FNS` declares the whole translated `function_scalar`
        // surface, and the gate at the head of that arm reads it BEFORE any
        // per-name arm runs. A name the declaration does not carry is therefore
        // declined in BOTH dialects, with the same `unsupported scalar function:
        // <name>` message the generic fall-through raised before the gate existed.
        // That is what makes a per-name arm added without a declaration row
        // unreachable, rather than silently rendering DataFusion SQL on the
        // Exasol-parsed path.
        //
        // SUBSTRING and SOUNDEX are real Exasol functions this translator does not
        // translate. The remaining rows pin the gate's own edges: the name is
        // uppercased before the lookup, the declaration is consulted before the
        // `arguments` key (so an undeclared name declines as undeclared, not as
        // malformed), and a node carrying no `name` key declines under the empty
        // name.
        let arg = json!([{"type": "column", "name": "a"}]);
        let cases = [
            (
                "SUBSTRING",
                json!({"type": "function_scalar", "name": "SUBSTRING", "arguments": arg.clone()}),
            ),
            (
                "SOUNDEX",
                json!({"type": "function_scalar", "name": "SOUNDEX", "arguments": arg.clone()}),
            ),
            (
                "SUBSTRING",
                json!({"type": "function_scalar", "name": "substring", "arguments": arg.clone()}),
            ),
            (
                "SUBSTRING",
                json!({"type": "function_scalar", "name": "SUBSTRING"}),
            ),
            ("", json!({"type": "function_scalar", "arguments": arg})),
        ];
        for (declined_name, expr) in cases {
            let expected = format!("unsupported scalar function: {declined_name}");
            assert_eq!(
                render_expression(&expr).unwrap_err().to_string(),
                expected,
                "DataFusion dialect must decline the undeclared node {expr}"
            );
            assert_eq!(
                render_expression_exasol(&expr).unwrap_err().to_string(),
                expected,
                "Exasol dialect must decline the undeclared node {expr}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "DataFusion safe variant must be None for {expr}"
            );
            assert!(
                render_expression_exasol_safe(&expr).is_none(),
                "Exasol safe variant must be None for {expr}"
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
    fn unsupported_date_functions_decline_in_both_dialects() {
        // Remaining excluded set per the date-fns spec Background: the date-arithmetic,
        // date-difference, and other date scalars whose DataFusion 54 equivalents still
        // diverge from Exasol (or don't exist at all). DAYS_BETWEEN, HOURS_BETWEEN,
        // MINUTES_BETWEEN, and SECONDS_BETWEEN are no longer here — they now have real
        // translator arms (see the disposition table in `add-date-arithmetic-pushdown`)
        // and are covered by their own rendering tests instead. ADD_HOURS/ADD_MINUTES
        // ARE still here: their arm was withdrawn after E2E parity (task 3.1) showed
        // the microsecond round-trip diverges on a DATE argument (Exasol expects
        // TIMESTAMP(0), the rendering yields TIMESTAMP(3)).
        //
        // Every one of these names EXISTS in Exasol, so the Exasol-dialect assertion
        // is the load-bearing half: the verbatim rule could render each of them as a
        // compiling call, and it deliberately does not. Absence from
        // TRANSLATED_SCALAR_FNS is what keeps them Exasol's own work. The four
        // now-family names decline the same way but have their own test,
        // `now_family_falls_through`, which records why no rendering can be right.
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
            let expected = format!("unsupported scalar function: {name}");
            assert_eq!(
                render_expression(&expr).unwrap_err().to_string(),
                expected,
                "the DataFusion dialect must decline {name}"
            );
            assert_eq!(
                render_expression_exasol(&expr).unwrap_err().to_string(),
                expected,
                "the Exasol dialect must decline {name}"
            );
            assert!(
                render_expression_safe(&expr).is_none(),
                "{name} must be None in the DataFusion safe variant"
            );
            assert!(
                render_expression_exasol_safe(&expr).is_none(),
                "{name} must be None in the Exasol safe variant"
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

    // --- Exasol-dialect verbatim rendering, per family (issue #209) ---
    //
    // In the Exasol dialect the translator renders what Exasol sent: the same name,
    // argument order, and argument count, taken from the node's own uppercased
    // `name`. The expression tree came from Exasol's own compiler, so reproducing
    // its call means Exasol's engine evaluates exactly the call it emitted — which
    // is why these renderings need no arity check and cannot be wrong.
    //
    // Every test below is PAIRED: it asserts the Exasol-dialect rendering and, on
    // the SAME node, that the DataFusion-dialect rendering is unchanged. That
    // pairing is what freezes the DataFusion output while the Exasol output moves,
    // and it is the convention `renders_cast_timestamp_precision_per_dialect`
    // established. `renders_mod_as_function_call_in_exasol_dialect` above is the
    // same shape for the one arm (#197) that already owned both dialects.

    #[test]
    fn renders_math_family_verbatim_in_exasol_dialect() {
        // Exasol has every one of these names natively, so the Exasol dialect
        // re-emits the call. The DataFusion dialect keeps its lowercase mapping,
        // including SIGN -> signum, whose name Exasol does not have at all.
        let one_arg = [
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
        for (exasol_name, df_name) in one_arg {
            let expr = json!({
                "type": "function_scalar",
                "name": exasol_name,
                "arguments": [{"type": "column", "name": "x"}]
            });
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                format!(r#"{exasol_name}("X")"#),
                "the Exasol dialect must render {exasol_name} verbatim"
            );
            assert_eq!(
                render_expression(&expr).unwrap(),
                format!(r#"{df_name}("X")"#),
                "the DataFusion dialect must stay unchanged for {exasol_name}"
            );
        }

        // ROUND / TRUNC / LOG take 1 or 2 arguments and POWER / ATAN2 exactly 2;
        // the Exasol dialect reproduces whichever count Exasol sent.
        let two_arg = [
            ("ROUND", "round"),
            ("TRUNC", "trunc"),
            ("LOG", "log"),
            ("POWER", "power"),
            ("ATAN2", "atan2"),
        ];
        for (exasol_name, df_name) in two_arg {
            let expr = json!({
                "type": "function_scalar",
                "name": exasol_name,
                "arguments": [
                    {"type": "column", "name": "v"},
                    {"type": "literal_exactnumeric", "value": 2}
                ]
            });
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                format!(r#"{exasol_name}("V", 2)"#),
                "the Exasol dialect must render {exasol_name} verbatim"
            );
            assert_eq!(
                render_expression(&expr).unwrap(),
                format!(r#"{df_name}("V", 2)"#),
                "the DataFusion dialect must stay unchanged for {exasol_name}"
            );
        }
    }

    #[test]
    fn renders_sign_as_native_sign_in_exasol_dialect() {
        // The headline failure of issue #209: `SELECT l_returnflag, SIGN(SUM(
        // l_discount) - 0.5) ... GROUP BY l_returnflag` aborted with "function or
        // script SIGNUM not found" (42000), because the grouped-aggregate wrapper
        // splices this rendering into SQL that Exasol's own core engine parses.
        //
        // SIGN is also why the gate sits AHEAD of `match fn_name.as_str()` instead
        // of being a widened guard inside it: the math arm matches SIGN and precedes
        // any such guard, so an in-place widening would still have rendered `signum`.
        // Arm order now carries no dialect precedence at all.
        let expr = json!({
            "type": "function_scalar",
            "name": "SIGN",
            "arguments": [{
                "type": "function_scalar",
                "name": "SUB",
                "arguments": [
                    {"type": "function_aggregate", "name": "SUM",
                     "arguments": [{"type": "column", "name": "l_discount"}]},
                    {"type": "literal_double", "value": 0.5}
                ]
            }]
        });
        let exasol = render_expression_exasol(&expr).unwrap();
        assert_eq!(exasol, r#"SIGN((SUM("L_DISCOUNT") - 0.5))"#);
        assert!(
            !exasol.contains("signum"),
            "the Exasol dialect must not emit DataFusion's signum: {exasol}"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"signum((SUM("L_DISCOUNT") - 0.5))"#
        );
    }

    #[test]
    fn renders_string_family_verbatim_in_exasol_dialect() {
        // Issue #210 shipped this family's Exasol rendering with no translator-side
        // test; this is that test. Four of the names have no DataFusion function of
        // the same name at all (LENGTH -> character_length, OCTET_LENGTH ->
        // octet_length, UNICODE -> ascii, UNICODECHR -> chr), which is why the
        // DataFusion dialect keeps its name-mapping arm.
        let one_arg = [
            ("LOWER", "lower"),
            ("UPPER", "upper"),
            ("TRIM", "trim"),
            ("LTRIM", "ltrim"),
            ("RTRIM", "rtrim"),
            ("REPLACE", "replace"),
            ("REPEAT", "repeat"),
            ("REVERSE", "reverse"),
            ("LPAD", "lpad"),
            ("RPAD", "rpad"),
            ("ASCII", "ascii"),
            ("CHR", "chr"),
            ("INITCAP", "initcap"),
            ("LEFT", "left"),
            ("RIGHT", "right"),
            ("TRANSLATE", "translate"),
            ("LENGTH", "character_length"),
            ("OCTET_LENGTH", "octet_length"),
            ("UNICODE", "ascii"),
            ("UNICODECHR", "chr"),
        ];
        for (exasol_name, df_name) in one_arg {
            let expr = json!({
                "type": "function_scalar",
                "name": exasol_name,
                "arguments": [{"type": "column", "name": "s"}]
            });
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                format!(r#"{exasol_name}("S")"#),
                "the Exasol dialect must render {exasol_name} verbatim"
            );
            assert_eq!(
                render_expression(&expr).unwrap(),
                format!(r#"{df_name}("S")"#),
                "the DataFusion dialect must stay unchanged for {exasol_name}"
            );
        }

        // SUBSTR carries its own explicit mapping and a 3-argument shape.
        let substr = json!({
            "type": "function_scalar",
            "name": "SUBSTR",
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        assert_eq!(
            render_expression_exasol(&substr).unwrap(),
            r#"SUBSTR("S", 1, 3)"#
        );
        assert_eq!(render_expression(&substr).unwrap(), r#"substr("S", 1, 3)"#);
    }

    #[test]
    fn renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect() {
        // Exasol's INSTR(string, substring [, start]) and LOCATE(substring, string
        // [, start]) already understand the optional start position, so the Exasol
        // dialect has nothing to translate: reproducing the name, order, and count is
        // the whole rendering, and an arity check there could only reject valid input
        // Exasol's own compiler emitted (issue #210).
        //
        // The DataFusion dialect maps both onto strpos(string, substring), which
        // takes no start position — so it reorders LOCATE's operands and DROPS a
        // third argument. That drop is a pre-existing limitation of the DataFusion
        // rendering, outside this change's scope (which freezes DataFusion output);
        // it is pinned here so the Exasol side cannot silently regress onto it.
        let instr = json!({
            "type": "function_scalar",
            "name": "INSTR",
            "arguments": [
                {"type": "literal_string", "value": "hello"},
                {"type": "literal_string", "value": "l"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        assert_eq!(
            render_expression_exasol(&instr).unwrap(),
            "INSTR('hello', 'l', 3)"
        );
        assert_eq!(render_expression(&instr).unwrap(), "strpos('hello', 'l')");

        let locate = json!({
            "type": "function_scalar",
            "name": "LOCATE",
            "arguments": [
                {"type": "literal_string", "value": "l"},
                {"type": "literal_string", "value": "hello"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        });
        assert_eq!(
            render_expression_exasol(&locate).unwrap(),
            "LOCATE('l', 'hello', 3)"
        );
        assert_eq!(render_expression(&locate).unwrap(), "strpos('hello', 'l')");
    }

    #[test]
    fn renders_greatest_least_verbatim_in_exasol_dialect() {
        // Both names already parse in Exasol, so the rendering changes only in case.
        // They join the verbatim rule anyway: a rule applied to some names and not
        // others cannot be reasoned about, because the next reader cannot tell which
        // renderings are principled and which merely happen to work.
        let greatest = json!({
            "type": "function_scalar",
            "name": "GREATEST",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"},
                {"type": "column", "name": "c"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&greatest).unwrap(),
            r#"GREATEST("A", "B", "C")"#
        );
        assert_eq!(
            render_expression(&greatest).unwrap(),
            r#"greatest("A", "B", "C")"#
        );

        let least = json!({
            "type": "function_scalar",
            "name": "LEAST",
            "arguments": [
                {"type": "column", "name": "x"},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        });
        assert_eq!(
            render_expression_exasol(&least).unwrap(),
            r#"LEAST("X", 0)"#
        );
        assert_eq!(render_expression(&least).unwrap(), r#"least("X", 0)"#);
    }

    #[test]
    fn renders_nullifzero_zeroifnull_verbatim_in_exasol_dialect() {
        // DataFusion has neither name, so it emulates: NULLIFZERO(v) ->
        // nullif(v, 0) and ZEROIFNULL(v) -> coalesce(v, 0). Exasol has both
        // natively, and the verbatim rendering gains parity by construction — there
        // is no emulation left on that path to diverge.
        let nullifzero = json!({
            "type": "function_scalar",
            "name": "NULLIFZERO",
            "arguments": [{"type": "column", "name": "v"}]
        });
        assert_eq!(
            render_expression_exasol(&nullifzero).unwrap(),
            r#"NULLIFZERO("V")"#
        );
        assert_eq!(render_expression(&nullifzero).unwrap(), r#"nullif("V", 0)"#);

        let zeroifnull = json!({
            "type": "function_scalar",
            "name": "ZEROIFNULL",
            "arguments": [{"type": "column", "name": "v"}]
        });
        assert_eq!(
            render_expression_exasol(&zeroifnull).unwrap(),
            r#"ZEROIFNULL("V")"#
        );
        assert_eq!(
            render_expression(&zeroifnull).unwrap(),
            r#"coalesce("V", 0)"#
        );
    }

    #[test]
    fn renders_nullif_verbatim_in_exasol_dialect() {
        // NULLIF is one of the names where the two dialects differ only in case, so
        // this test carries the composition check as well: the verbatim gate renders
        // its arguments in the SAME dialect it was called with, which is why the
        // nested MOD becomes Exasol's MOD(a, b) and not DataFusion's `%`. Without
        // that, a wrapper-bound NULLIF(MOD(id, 5), 0) group key would still splice
        // `%` — which Exasol's parser rejects — into Exasol-parsed SQL.
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
            render_expression_exasol(&expr).unwrap(),
            r#"NULLIF(MOD("ID", 5), 0)"#
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"nullif(("ID" % 5), 0)"#
        );
    }

    #[test]
    fn renders_date_field_shortcuts_verbatim_in_exasol_dialect() {
        // `SELECT COUNT(DISTINCT YEAR(l_shipdate)) FROM <vs>.LINEITEM` aborted with
        // "function or script DATE_PART not found" (42000): Exasol has no DATE_PART
        // at all, but it has every one of these six shortcuts natively.
        for field in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"] {
            let expr = json!({
                "type": "function_scalar",
                "name": field,
                "arguments": [{"type": "column", "name": "ts"}]
            });
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                format!(r#"{field}("TS")"#),
                "the Exasol dialect must render {field} verbatim"
            );
            assert_eq!(
                render_expression(&expr).unwrap(),
                format!(r#"date_part('{field}', "TS")"#),
                "the DataFusion dialect must stay unchanged for {field}"
            );
        }
    }

    #[test]
    fn renders_week_as_native_week_in_exasol_dialect() {
        // Exasol's own WEEK is what the DataFusion date_part('week') rendering was
        // chosen to match (both ISO-8601, weeks beginning Monday, week 1 containing
        // the year's first Thursday), so on the Exasol-parsed path re-emitting WEEK
        // is both the form that compiles and the one that is exactly equivalent.
        let expr = json!({
            "type": "function_scalar",
            "name": "WEEK",
            "arguments": [{"type": "column", "name": "d"}]
        });
        assert_eq!(render_expression_exasol(&expr).unwrap(), r#"WEEK("D")"#);
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"date_part('week', "D")"#
        );
    }

    #[test]
    fn renders_date_trunc_verbatim_in_exasol_dialect() {
        // Exasol's DATE_TRUNC(format, datetime) has the same name and the same
        // argument order as DataFusion's, so the verbatim rendering differs only in
        // case — and the format literal Exasol sent is forwarded untouched by both
        // dialects rather than being re-interpreted by either.
        let expr = json!({
            "type": "function_scalar",
            "name": "DATE_TRUNC",
            "arguments": [
                {"type": "literal_string", "value": "month"},
                {"type": "column", "name": "ts"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"DATE_TRUNC('month', "TS")"#
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"date_trunc('month', "TS")"#
        );
    }

    #[test]
    fn renders_to_date_to_timestamp_verbatim_in_exasol_dialect() {
        // Both names exist in Exasol and both dialects forward the optional format
        // argument unchanged. The format model in the node is Exasol's own
        // ('YYYY-MM-DD'), which is exactly why re-emitting Exasol's call is safe on
        // the Exasol-parsed path: Exasol parses the model it wrote itself.
        for (exasol_name, df_name) in [("TO_DATE", "to_date"), ("TO_TIMESTAMP", "to_timestamp")] {
            let bare = json!({
                "type": "function_scalar",
                "name": exasol_name,
                "arguments": [{"type": "column", "name": "s"}]
            });
            assert_eq!(
                render_expression_exasol(&bare).unwrap(),
                format!(r#"{exasol_name}("S")"#),
                "the Exasol dialect must render {exasol_name} verbatim"
            );
            assert_eq!(
                render_expression(&bare).unwrap(),
                format!(r#"{df_name}("S")"#),
                "the DataFusion dialect must stay unchanged for {exasol_name}"
            );

            let formatted = json!({
                "type": "function_scalar",
                "name": exasol_name,
                "arguments": [
                    {"type": "column", "name": "s"},
                    {"type": "literal_string", "value": "YYYY-MM-DD"}
                ]
            });
            assert_eq!(
                render_expression_exasol(&formatted).unwrap(),
                format!(r#"{exasol_name}("S", 'YYYY-MM-DD')"#),
                "the Exasol dialect must forward {exasol_name}'s format model verbatim"
            );
            assert_eq!(
                render_expression(&formatted).unwrap(),
                format!(r#"{df_name}("S", 'YYYY-MM-DD')"#),
                "the DataFusion dialect must stay unchanged for {exasol_name} with a format"
            );
        }
    }

    #[test]
    fn renders_days_between_verbatim_in_exasol_dialect() {
        // The DataFusion rendering is a CAST-to-DATE difference — an emulation of a
        // function DataFusion does not have. Exasol has DAYS_BETWEEN, so the Exasol
        // dialect re-emits it and the emulation stays on the DataFusion side only.
        let expr = json!({
            "type": "function_scalar",
            "name": "DAYS_BETWEEN",
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"DAYS_BETWEEN("A", "B")"#
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
        );
    }

    #[test]
    fn renders_between_family_verbatim_in_exasol_dialect() {
        // The *_BETWEEN pushdown shipped in `add-date-arithmetic-pushdown` was broken
        // on the Exasol-parsed path from day one: its epoch-difference emulation
        // calls DATE_PART, which Exasol does not have (42000). Exasol has all three
        // names natively, so the Exasol dialect re-emits them.
        let df_epoch = r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#;
        let cases = [
            ("HOURS_BETWEEN", format!("({df_epoch} / 3600)")),
            ("MINUTES_BETWEEN", format!("({df_epoch} / 60)")),
            ("SECONDS_BETWEEN", df_epoch.to_string()),
        ];
        for (name, df_expected) in cases {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            });
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                format!(r#"{name}("A", "B")"#),
                "the Exasol dialect must render {name} verbatim"
            );
            assert_eq!(
                render_expression(&expr).unwrap(),
                df_expected,
                "the DataFusion dialect must stay unchanged for {name}"
            );
        }
    }

    // --- Systemic sweep over the whole declared surface (issue #209) ---

    /// The verbatim rule is only durable if a forgotten name fails a test rather
    /// than a review, so this test iterates `TRANSLATED_SCALAR_FNS` itself rather
    /// than a parallel hand-written name list: a name added to the declaration
    /// with no fixture fails here BY NAME, and a fixture for a name nobody
    /// declared fails here too. A `VerbatimCall` expectation is DERIVED from the
    /// node's own uppercased `name` by the same rule the gate applies, so the 66
    /// verbatim names cannot be blessed one hand-written string at a time — a
    /// rewrite back to a DataFusion name (`signum`, `date_part`, `strpos`) fails
    /// the derived comparison. Only the ten `Shaped` names and the five
    /// dialect-branching node types outside `function_scalar` declare an expected
    /// string, because each of those has a shape of its own.
    ///
    /// Every fixture argument is a dialect-invariant node (a column or a plain
    /// literal) on purpose: the derivation renders the arguments through the same
    /// entry point under test, so a dialect-sensitive argument would make that
    /// half of the expectation self-fulfilling. The dialect-sensitive nodes get
    /// their own rows instead.
    #[test]
    fn exasol_dialect_renders_declared_verbatim_surface() {
        struct ScalarFixture {
            name: &'static str,
            node: Json,
            shaped_exasol: Option<&'static str>,
        }

        fn col(name: &str) -> Json {
            json!({"type": "column", "name": name})
        }
        fn num(value: i64) -> Json {
            json!({"type": "literal_exactnumeric", "value": value})
        }
        fn text(value: &str) -> Json {
            json!({"type": "literal_string", "value": value})
        }
        fn scalar(name: &str, args: Vec<Json>) -> Json {
            json!({"type": "function_scalar", "name": name, "arguments": args})
        }
        fn verbatim(name: &'static str, args: Vec<Json>) -> ScalarFixture {
            ScalarFixture {
                name,
                node: scalar(name, args),
                shaped_exasol: None,
            }
        }
        fn shaped(name: &'static str, node: Json, exasol: &'static str) -> ScalarFixture {
            ScalarFixture {
                name,
                node,
                shaped_exasol: Some(exasol),
            }
        }

        let mut fixtures: Vec<ScalarFixture> = Vec::new();

        // Math family: the one-argument names, then the five taking a second.
        for name in [
            "ABS", "FLOOR", "CEIL", "SQRT", "EXP", "LN", "SIGN", "DEGREES", "RADIANS", "SIN",
            "COS", "TAN", "ASIN", "ACOS", "ATAN", "SINH", "COSH", "TANH", "COT",
        ] {
            fixtures.push(verbatim(name, vec![col("x")]));
        }
        for name in ["ROUND", "TRUNC", "LOG", "POWER", "ATAN2"] {
            fixtures.push(verbatim(name, vec![col("v"), num(2)]));
        }

        // String family.
        for name in [
            "LOWER",
            "UPPER",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "REVERSE",
            "ASCII",
            "INITCAP",
            "LENGTH",
            "OCTET_LENGTH",
            "UNICODE",
        ] {
            fixtures.push(verbatim(name, vec![col("s")]));
        }
        for name in ["CHR", "UNICODECHR"] {
            fixtures.push(verbatim(name, vec![num(65)]));
        }
        for name in ["SUBSTR", "LEFT", "RIGHT", "REPEAT", "LPAD", "RPAD"] {
            fixtures.push(verbatim(name, vec![col("s"), num(3)]));
        }
        fixtures.push(verbatim("REPLACE", vec![col("s"), text("a")]));
        fixtures.push(verbatim(
            "TRANSLATE",
            vec![col("s"), text("ab"), text("xy")],
        ));
        // INSTR(string, substring) against LOCATE(substring, string): opposite
        // argument orders, which is exactly what the verbatim rule preserves and
        // what the DataFusion dialect has to reorder into strpos.
        fixtures.push(verbatim("INSTR", vec![col("s"), text("a")]));
        fixtures.push(verbatim("LOCATE", vec![text("a"), col("s")]));

        // Comparison and null-handling family.
        for name in ["GREATEST", "LEAST", "NULLIF"] {
            fixtures.push(verbatim(name, vec![col("a"), col("b")]));
        }
        for name in ["NULLIFZERO", "ZEROIFNULL"] {
            fixtures.push(verbatim(name, vec![col("v")]));
        }

        // Date field shortcuts, then the conversion and truncation names.
        for name in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "WEEK"] {
            fixtures.push(verbatim(name, vec![col("ts")]));
        }
        fixtures.push(verbatim("DATE_TRUNC", vec![text("month"), col("ts")]));
        fixtures.push(verbatim("TO_DATE", vec![col("s"), text("YYYY-MM-DD")]));
        fixtures.push(verbatim(
            "TO_TIMESTAMP",
            vec![col("s"), text("YYYY-MM-DD HH24:MI:SS")],
        ));

        // Date-difference family.
        for name in [
            "DAYS_BETWEEN",
            "HOURS_BETWEEN",
            "MINUTES_BETWEEN",
            "SECONDS_BETWEEN",
        ] {
            fixtures.push(verbatim(name, vec![col("a"), col("b")]));
        }

        // The ten Shaped names, each declaring the string its own arm renders.
        fixtures.extend([
            shaped(
                "ADD",
                scalar("ADD", vec![col("a"), col("b")]),
                r#"("A" + "B")"#,
            ),
            shaped(
                "SUB",
                scalar("SUB", vec![col("a"), col("b")]),
                r#"("A" - "B")"#,
            ),
            shaped(
                "MULT",
                scalar("MULT", vec![col("a"), col("b")]),
                r#"("A" * "B")"#,
            ),
            shaped(
                "FLOAT_DIV",
                scalar("FLOAT_DIV", vec![col("a"), col("b")]),
                r#"("A" / "B")"#,
            ),
            shaped("NEG", scalar("NEG", vec![col("a")]), r#"(-"A")"#),
            shaped(
                "MOD",
                scalar("MOD", vec![col("a"), col("b")]),
                r#"MOD("A", "B")"#,
            ),
            shaped(
                "CONCAT",
                scalar("CONCAT", vec![col("a"), col("b")]),
                r#"("A" || "B")"#,
            ),
            shaped(
                "CAST",
                json!({
                    "type": "function_scalar", "name": "CAST",
                    "arguments": [col("v")],
                    "dataType": {"type": "VARCHAR", "size": 50}
                }),
                r#"CAST("V" AS VARCHAR(50))"#,
            ),
            shaped(
                "REGEXP_LIKE",
                scalar("REGEXP_LIKE", vec![col("s"), text("^a")]),
                r#"("S" REGEXP_LIKE '^a')"#,
            ),
            shaped(
                "CASE",
                scalar(
                    "CASE",
                    vec![
                        json!({"type": "predicate_greater", "left": col("x"), "right": num(0)}),
                        num(1),
                        num(0),
                    ],
                ),
                r#"CASE WHEN ("X" > 0) THEN 1 ELSE 0 END"#,
            ),
        ]);

        // Completeness in both directions, before anything is rendered: a
        // declared name with no fixture, and a fixture nobody declared, must each
        // fail by name.
        let missing: Vec<&str> = TRANSLATED_SCALAR_FNS
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| !fixtures.iter().any(|f| f.name == *name))
            .collect();
        assert!(
            missing.is_empty(),
            "every name declared in TRANSLATED_SCALAR_FNS needs a sweep fixture; missing: \
             {missing:?}"
        );
        let undeclared: Vec<&str> = fixtures
            .iter()
            .map(|f| f.name)
            .filter(|name| declared_scalar_fn(name).is_none())
            .collect();
        assert!(
            undeclared.is_empty(),
            "every sweep fixture must name a declared function; undeclared: {undeclared:?}"
        );
        assert_eq!(
            fixtures.len(),
            TRANSLATED_SCALAR_FNS.len(),
            "the fixture map and the declaration must line up one to one; a duplicated row on \
             either side is the only way both subset checks above can pass at different sizes"
        );

        let mut swept: Vec<String> = Vec::new();

        for (declared_name, form) in TRANSLATED_SCALAR_FNS {
            let fixture = fixtures
                .iter()
                .find(|f| f.name == *declared_name)
                .expect("fixture completeness is asserted above");
            // One declaration lookup gates BOTH dialects, so a declaration row is
            // a promise about the DataFusion dialect too. A `VerbatimCall` returns
            // AHEAD of the per-name arms in the Exasol dialect, so every Exasol
            // assertion below passes whether or not the arm still exists — this
            // call is the only one that reaches the arms. No expected string is
            // asserted: the per-family paired tests own the frozen DataFusion
            // output, and a second copy here would drift from them.
            if let Err(err) = render_expression(&fixture.node) {
                panic!(
                    "{declared_name} is declared in TRANSLATED_SCALAR_FNS, which gates BOTH \
                     dialects from one lookup, so it MUST render in the DataFusion dialect too; \
                     it declined with {err:?}. A declared name that has lost its per-name arm \
                     still renders in the Exasol dialect through the verbatim gate, so this is \
                     the only assertion that catches it."
                );
            }
            let rendered = render_expression_exasol(&fixture.node)
                .unwrap_or_else(|err| panic!("{declared_name} failed to render: {err:?}"));
            match form {
                ExasolForm::VerbatimCall => {
                    assert!(
                        fixture.shaped_exasol.is_none(),
                        "{declared_name} is declared VerbatimCall, so its expectation MUST be \
                         derived from the node, never hand-written"
                    );
                    let node_name = fixture
                        .node
                        .get("name")
                        .and_then(|n| n.as_str())
                        .expect("fixture node carries a name")
                        .to_uppercase();
                    let rendered_args: Vec<String> = fixture
                        .node
                        .get("arguments")
                        .and_then(|a| a.as_array())
                        .expect("fixture node carries arguments")
                        .iter()
                        .map(|arg| render_expression_exasol(arg).expect("argument renders"))
                        .collect();
                    assert_eq!(
                        rendered,
                        format!("{node_name}({})", rendered_args.join(", ")),
                        "the Exasol dialect must re-emit {declared_name} as the call Exasol sent"
                    );
                }
                ExasolForm::Shaped => {
                    let expected = fixture.shaped_exasol.unwrap_or_else(|| {
                        panic!(
                            "{declared_name} is declared Shaped, so its fixture MUST declare the \
                             expected Exasol string"
                        )
                    });
                    assert_eq!(
                        rendered, expected,
                        "{declared_name} is outside the <NAME>(<args>) shape and must render its \
                         own declared form"
                    );
                }
            }
            swept.push(rendered);
        }

        // The five dialect-branching node types outside `function_scalar`. They
        // are node types rather than function names, so the declaration does not
        // carry them and each row asserts both dialects itself.
        let node_type_rows = [
            (
                json!({
                    "type": "function_scalar_extract", "name": "EXTRACT",
                    "toExtract": "YEAR", "arguments": [col("ts")]
                }),
                r#"EXTRACT(YEAR FROM "TS")"#,
                r#"date_part('YEAR', "TS")"#,
            ),
            (
                json!({
                    "type": "function_scalar_cast", "name": "CAST",
                    "arguments": [col("v")],
                    "dataType": {"type": "VARCHAR", "size": 50}
                }),
                r#"CAST("V" AS VARCHAR(50))"#,
                r#"CAST("V" AS VARCHAR)"#,
            ),
            (
                json!({
                    "type": "predicate_like_regexp",
                    "expression": col("s"), "pattern": text("^a")
                }),
                r#"("S" REGEXP_LIKE '^a')"#,
                r#"regexp_like("S", '^a')"#,
            ),
            (
                json!({"type": "literal_timestamp", "value": "2024-03-01 12:34:56.789"}),
                "TIMESTAMP '2024-03-01 12:34:56.789'",
                "arrow_cast('2024-03-01 12:34:56.789', 'Timestamp(Microsecond, None)')",
            ),
            (
                json!({"type": "literal_timestamp_utc", "value": "2024-03-01 12:34:56.789"}),
                "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 12:34:56.789', 'UTC', SESSIONTIMEZONE) \
                 AS TIMESTAMP WITH LOCAL TIME ZONE)",
                r#"arrow_cast('2024-03-01 12:34:56.789+00:00', 'Timestamp(Microsecond, Some("UTC"))')"#,
            ),
        ];
        for (node, expected_exasol, expected_datafusion) in node_type_rows {
            let node_type = node["type"]
                .as_str()
                .expect("row carries a node type")
                .to_string();
            let rendered = render_expression_exasol(&node)
                .unwrap_or_else(|err| panic!("{node_type} failed to render: {err:?}"));
            assert_eq!(
                rendered, expected_exasol,
                "{node_type} in the Exasol dialect"
            );
            assert_eq!(
                render_expression(&node).unwrap(),
                expected_datafusion,
                "{node_type} in the DataFusion dialect"
            );
            swept.push(rendered);
        }

        // Secondary guard over everything swept above. The comparison is
        // deliberately case-SENSITIVE: `OCTET_LENGTH("S")` and `NULLIF("A", "B")`
        // are correct Exasol renderings, and it is their lowercase DataFusion
        // twins that must never reach an Exasol-parsed wrapper. `current_date()`
        // and `now()` are live guards now that the now-family is undeclared —
        // re-adding a DataFusion-shaped now-family arm trips them.
        for rendered in &swept {
            for token in [
                "signum",
                "date_part",
                "strpos",
                "arrow_cast",
                "character_length",
                "octet_length",
                "regexp_like(",
                "current_date()",
                "now()",
                "nullif(",
                "coalesce(",
            ] {
                assert!(
                    !rendered.contains(token),
                    "Exasol-dialect output must not contain the DataFusion-only token `{token}`, \
                     but rendered: {rendered}"
                );
            }
        }
    }

    // --- Dialect-invariant surface (regression freeze) ---
    //
    // This plan branches `function_scalar_extract`, `predicate_like_regexp`, the
    // `REGEXP_LIKE` alternate encoding, and the two timestamp-literal node types
    // on `dialect`. Everything else was not meant to move. These three tests
    // freeze the surface that MUST stay dialect-invariant, so a future change
    // that accidentally starts branching one of these paths on `dialect` fails
    // here instead of only showing up as a silent divergence downstream.

    #[test]
    fn arithmetic_operators_render_identically_in_both_dialects() {
        // The five operator wire names never inspect `dialect` in their own
        // arm — `render_expression_inner` renders the same `(<left> <op>
        // <right>)` / `(-<operand>)` shape regardless of which dialect is
        // requested. Pins that invariance directly, on the same node, for
        // both dialects at once.
        let binary = [
            ("ADD", "+"),
            ("SUB", "-"),
            ("MULT", "*"),
            ("FLOAT_DIV", "/"),
        ];
        for (name, op) in binary {
            let expr = json!({
                "type": "function_scalar",
                "name": name,
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "literal_exactnumeric", "value": 1}
                ]
            });
            let expected = format!(r#"("A" {op} 1)"#);
            assert_eq!(
                render_expression(&expr).unwrap(),
                expected,
                "{name} DataFusion dialect"
            );
            assert_eq!(
                render_expression_exasol(&expr).unwrap(),
                expected,
                "{name} Exasol dialect"
            );
        }

        let neg = json!({
            "type": "function_scalar",
            "name": "NEG",
            "arguments": [{"type": "column", "name": "a"}]
        });
        let expected_neg = r#"(-"A")"#;
        assert_eq!(
            render_expression(&neg).unwrap(),
            expected_neg,
            "NEG DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&neg).unwrap(),
            expected_neg,
            "NEG Exasol dialect"
        );
    }

    #[test]
    fn non_timestamp_literals_render_identically_in_both_dialects() {
        // Every literal node type except `literal_timestamp` and
        // `literal_timestamp_utc` (branched on dialect by task 5) renders the
        // same string in both dialects, because none of these arms reads
        // `dialect` at all.
        let cases: [(Json, &str); 7] = [
            (json!({"type": "literal_null"}), "NULL"),
            (json!({"type": "literal_bool", "value": true}), "TRUE"),
            (json!({"type": "literal_bool", "value": false}), "FALSE"),
            (
                json!({"type": "literal_string", "value": "it's"}),
                "'it''s'",
            ),
            (json!({"type": "literal_exactnumeric", "value": 42}), "42"),
            (json!({"type": "literal_double", "value": 0.5}), "0.5"),
            (
                json!({"type": "literal_date", "value": "2024-01-15"}),
                "DATE '2024-01-15'",
            ),
        ];
        for (node, expected) in cases {
            let node_type = node["type"].as_str().unwrap();
            assert_eq!(
                render_expression(&node).unwrap(),
                expected,
                "{node_type} DataFusion dialect"
            );
            assert_eq!(
                render_expression_exasol(&node).unwrap(),
                expected,
                "{node_type} Exasol dialect"
            );
        }
    }

    #[test]
    fn exasol_df_filter_suppresses_trivially_true() {
        // Exasol-dialect twin of `true_filter_returns_none_in_safe_mode` /
        // `null_filter_returns_none_in_safe_mode` above: `render_df_filter_exasol_safe`
        // suppresses a trivially-true (`TRUE` or `NULL`) filter exactly like
        // `render_df_filter_safe` does. A trivially-true filter is a correct
        // no-op to omit from the scan spec — but that is one of two
        // distinguishable causes of a `None` return, regardless of which
        // dialect rendered the fragment. The other cause, a genuine decline,
        // must be self-applied by the caller — a declined predicate omitted
        // here would be silently lost, not backstopped.
        let true_filter = json!({"type": "literal_bool", "value": true});
        assert!(render_df_filter_exasol_safe(&true_filter).is_none());

        let null_filter = json!({"type": "literal_null"});
        assert!(render_df_filter_exasol_safe(&null_filter).is_none());
    }
}
