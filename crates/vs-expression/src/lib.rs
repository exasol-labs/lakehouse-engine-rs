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
///   SQL operators, not calls (`FLOAT_DIV` diverges by dialect: a
///   DataFusion-only `CAST(... AS DOUBLE)` on its left operand forces true
///   float division; `ADD`/`SUB`/`MULT`/`NEG` render identically in both);
/// - `MOD` — Exasol requires the `MOD(a, b)` form, DataFusion the `%`
///   operator;
/// - `CONCAT` — the wire encoding of Exasol's `||` operator, which also
///   diverges by dialect: Exasol's `||` treats a NULL operand as the empty
///   string and yields NULL only when the whole result is empty, so the
///   DataFusion side renders `nullif(concat(...), '')` to reproduce that
///   contract, while the Exasol side keeps chained `||` (#374);
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
    // CONCAT: Shaped because Exasol's own form is the `||` operator, not a call.
    // The dialects diverge on NULL too: Exasol's `||` treats NULL as '' and
    // yields NULL only when all-empty, reproduced as `nullif(concat(...), '')` (#374).
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

const DOUBLE_TYPE: &str = "DOUBLE";

/// Map a VS `dataType` JSON object to a DataFusion SQL type name.
fn render_cast_target(data_type: &Json, dialect: Dialect) -> Result<String, UdfError> {
    let type_name = data_type.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match type_name.to_uppercase().as_str() {
        "VARCHAR" => match dialect {
            // DataFusion's SQL frontend rejects VARCHAR(n) with a length (no
            // `support_varchar_with_length`); only bare VARCHAR parses there.
            Dialect::DataFusion => Ok("VARCHAR".to_string()),
            // Exasol's parser has the OPPOSITE requirement: a character type MUST
            // carry a length. Render `VARCHAR(<size>)` from the width Exasol
            // itself sent (`{"type":"VARCHAR","size":n}`; a genuine
            // `{"type":"CHAR","size":n,...}` has its own arm below). If `size` is
            // somehow absent, fall back to the project's "unknown/incompatible
            // width" convention. Do NOT clamp to Exasol's 2,000,000 max — trust
            // the value Exasol sent.
            Dialect::Exasol => Ok(match data_type.get("size").and_then(|v| v.as_u64()) {
                Some(size) => format!("VARCHAR({size})"),
                None => "VARCHAR(2000000)".to_string(),
            }),
        },
        "CHAR" => match dialect {
            // Arrow has only `Utf8` — no fixed-width CHAR type — and
            // datafusion-sql rejects a length-qualified character target, so the
            // DataFusion side renders the same bare VARCHAR as above.
            Dialect::DataFusion => Ok("VARCHAR".to_string()),
            // Exasol declares a CHAR-target result column `CHAR(n)` and validates
            // the pushdown's column types positionally against that declaration,
            // so collapsing to `VARCHAR(n)` here is a "Data type mismatch"
            // rejection AND drops the blank padding CHAR(n) carries (#192). The
            // ` ASCII` suffix mirrors the charset rule the adapter seam already
            // applies: without it an ASCII-declared CHAR merely trades a
            // VARCHAR mismatch for a UTF8 one. As for VARCHAR, the width Exasol
            // sent is trusted and never clamped — Exasol's own 2,000 CHAR maximum
            // is enforced where a declaration is SYNTHESISED, not here where one
            // is echoed back. A `size`-absent dataType (never sent by Exasol) keeps
            // the project's `VARCHAR(2000000)` "unknown width" convention rather
            // than inventing a CHAR width.
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

fn cast_to_double(expr_sql: &str) -> String {
    format!("CAST({expr_sql} AS {DOUBLE_TYPE})")
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
                // advertised as a capability. FLOAT_DIV alone renders a DataFusion-only
                // CAST-to-DOUBLE below; ADD/SUB/MULT render identically in both dialects.
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
                    let left = match (fn_name.as_str(), dialect) {
                        ("FLOAT_DIV", Dialect::DataFusion) => cast_to_double(&left),
                        _ => left,
                    };
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
                // CONCAT → the wire encoding of Exasol's `||` operator.
                // Exasol's `||` does NOT propagate NULL: a NULL operand is
                // treated as the empty string, and the result is NULL only
                // when the whole concatenation is empty (verified live,
                // #374). DataFusion's own concat() also treats NULL as the
                // empty string but never re-collapses an all-empty result to
                // NULL, so the DataFusion dialect wraps it as
                // `nullif(concat(...), '')` to reproduce Exasol's contract;
                // the Exasol dialect keeps chained `||`, which already has
                // the real behavior. A boolean operand is rewritten to the
                // Exasol-cased form before joining, since DataFusion's
                // boolean->Utf8 cast (which `||`/`concat()` fall back to for
                // a raw boolean operand) renders lowercase `true`/`false`
                // (#200).
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
/// Exasol dialect: a character CAST target is length-qualified — a `VARCHAR`
/// target renders `VARCHAR(n)`, a `CHAR` target renders `CHAR(n)` (plus an
/// ` ASCII` suffix when `dataType.characterSet` is ASCII case-insensitively) —
/// because Exasol's own parser has no length-less character type. This
/// supersedes the older "CHAR maps to VARCHAR" rule; see
/// `renders_cast_char_as_exasol_char` and
/// `cast_char_target_diverges_between_dialects` for the tests that guard it.
/// Use this on the code paths whose rendered SQL is parsed by Exasol's core
/// engine directly — the qualified single-table / N-scan join wrapper
/// (`joins.rs`) and the grouped-aggregate outer-merge wrapper
/// (`grouped_agg.rs`) — NOT for fragments embedded in a DataFusion
/// `ScanSpec`, which must use [`render_expression`].
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
#[path = "lib_tests.rs"]
mod tests;
