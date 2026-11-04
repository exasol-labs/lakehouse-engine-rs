//! `EXPORT ... INTO SCRIPT` through one SET script: its export hook answers a SELECT that
//! calls the same script as the writer. No log event, script output, or CONNECTION value.

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::{ExportSpec, Parameter};
use serde::Deserialize;

/// The script name the installer creates and the writer SELECT calls.
pub const EXPORT_SCRIPT_NAME: &str = "LAKEHOUSE_EXPORT";

/// Answers Exasol's `generate_sql_for_export_spec` call with the writer SELECT. It refuses
/// an invalid statement, and a CONNECTION the script cannot read, before any source row
/// is read. The SELECT carries the CONNECTION name and no CONNECTION value.
pub fn generate_export_sql(ctx: &mut dyn UdfContext, json: &str) -> Result<String, UdfError> {
    let spec = ExportSpec::from_json(json)?;
    let options = ExportOptions::from_spec(&spec)?;
    require_connection_access(ctx, &options.connection_name)?;
    let writer_spec = WriterSpec::render(&options.connection_name);
    Ok(WriterSelect {
        script_schema: &ctx.script_schema(),
        writer_spec: &writer_spec,
        source_columns: &spec.source_column_names,
        writers: options.writers,
        node_count: ctx.node_count() as usize,
    }
    .render())
}

/// Runs one writer invocation over its SET group and returns the group's row count. It
/// reads the CONNECTION again, because a direct `SELECT` call skips the export hook.
pub fn run_writer(ctx: &mut dyn UdfContext) -> Result<i64, UdfError> {
    // The host opens a SET group on its first row and its first next() returns true
    // without advancing, so counting the true next() calls counts every row once.
    if !ctx.next()? {
        return Ok(0);
    }
    let spec = WriterSpec::read(ctx)?;
    require_connection_access(ctx, &spec.connection)?;
    let mut rows = 1;
    while ctx.next()? {
        rows += 1;
    }
    Ok(rows)
}

/// The first argument of every writer call. A direct `SELECT` call can forge it, so the
/// writer parses it strictly and never echoes it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterSpec {
    connection: String,
}

impl WriterSpec {
    fn render(connection: &str) -> String {
        serde_json::json!({ "connection": connection }).to_string()
    }

    fn read(ctx: &dyn UdfContext) -> Result<Self, UdfError> {
        let text = ctx
            .get_string(0)
            .map_err(|_| not_an_export_target())?
            .ok_or_else(not_an_export_target)?;
        serde_json::from_str(text).map_err(|_| not_an_export_target())
    }
}

fn not_an_export_target() -> UdfError {
    refusal(format_args!(
        "{EXPORT_SCRIPT_NAME} runs as the target of EXPORT ... INTO SCRIPT, not as a function in a query: its first argument is not the writer spec that the export generates"
    ))
}

fn require_connection_access(ctx: &dyn UdfContext, connection: &str) -> Result<(), UdfError> {
    let Err(cause) = ctx.connection(connection) else {
        return Ok(());
    };
    let script = format!("{}.{EXPORT_SCRIPT_NAME}", ctx.script_schema());
    let user = ctx.current_user().unwrap_or_else(|| "<user>".to_string());
    Err(refusal(format_args!(
        "user {user} cannot read CONNECTION {connection} in script {script} ({cause}); \
         a DBA grants it with GRANT ACCESS ON CONNECTION {connection} FOR SCRIPT {script} TO {user}, \
         or to a role that {user} holds"
    )))
}

fn refusal(message: impl std::fmt::Display) -> UdfError {
    UdfError::User(format!("{EXPORT_SCRIPT_NAME}: {message}"))
}

const TABLE_NAME: &str = "TABLE_NAME";
const WRITERS_PER_NODE: &str = "WRITERS_PER_NODE";
const SINGLE_COMMIT: &str = "SINGLE_COMMIT";

/// How the export spreads the source rows over writer invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterLayout {
    /// `SINGLE_COMMIT = 'true'`: one writer for the whole cluster.
    SingleCommit,
    /// `WRITERS_PER_NODE` writers per node, 1 when the option is absent.
    PerNode(usize),
}

/// An export statement the engine accepts: append-only, `AT` a named CONNECTION, with a
/// valid `TABLE_NAME`. It keeps only what the writer SELECT needs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExportOptions {
    connection_name: String,
    writers: WriterLayout,
}

impl ExportOptions {
    /// Fails with a `UdfError::User` that names the refused clause or option and its
    /// accepted form. It never echoes a value of an inline CONNECTION.
    fn from_spec(spec: &ExportSpec) -> Result<Self, UdfError> {
        refuse_rewrite_clauses(spec)?;
        let connection_name = named_connection(spec)?;
        let with_options = WithOptions::collect(&spec.parameters)?;
        check_table_name(with_options.table_name)?;
        let writers = with_options.writer_layout()?;
        Ok(Self {
            connection_name,
            writers,
        })
    }
}

fn refuse_rewrite_clauses(spec: &ExportSpec) -> Result<(), UdfError> {
    let clauses = [
        (spec.has_truncate, "TRUNCATE"),
        (spec.has_replace, "REPLACE"),
        (spec.created_by.is_some(), "CREATED BY"),
    ];
    match clauses.into_iter().find(|(given, _)| *given) {
        Some((_, clause)) => Err(refusal(format_args!(
            "{clause} is not supported, because the export only appends rows; remove {clause} from the EXPORT statement"
        ))),
        None => Ok(()),
    }
}

fn named_connection(spec: &ExportSpec) -> Result<String, UdfError> {
    if let Some(name) = &spec.connection_name {
        return Ok(name.clone());
    }
    let problem = if spec.connection_information.is_some() {
        "an inline AT '<address>' USER '<user>' IDENTIFIED BY '<password>' clause is not supported"
    } else {
        "the EXPORT statement names no CONNECTION"
    };
    Err(refusal(format_args!(
        "{problem}; add AT <connection>, naming the CONNECTION object the virtual schema uses"
    )))
}

struct WithOptions<'a> {
    table_name: Option<&'a str>,
    writers_per_node: Option<&'a str>,
    single_commit: Option<&'a str>,
}

impl<'a> WithOptions<'a> {
    fn collect(parameters: &'a [Parameter]) -> Result<Self, UdfError> {
        let mut options = Self {
            table_name: None,
            writers_per_node: None,
            single_commit: None,
        };
        for parameter in parameters {
            let slot = match parameter.key.as_str() {
                TABLE_NAME => &mut options.table_name,
                WRITERS_PER_NODE => &mut options.writers_per_node,
                SINGLE_COMMIT => &mut options.single_commit,
                unknown => {
                    return Err(refusal(format_args!(
                        "unknown option {unknown}; the accepted options are {TABLE_NAME}, {WRITERS_PER_NODE}, and {SINGLE_COMMIT}"
                    )));
                }
            };
            *slot = Some(parameter.value.as_str());
        }
        Ok(options)
    }

    fn writer_layout(&self) -> Result<WriterLayout, UdfError> {
        let per_node = self
            .writers_per_node
            .map(parse_writers_per_node)
            .transpose()?;
        let single_commit = self.single_commit.map(parse_single_commit).transpose()?;
        match (single_commit, per_node) {
            (Some(true), Some(_)) => Err(refusal(format_args!(
                "{WRITERS_PER_NODE} cannot be combined with {SINGLE_COMMIT} = 'true', which runs one writer for the whole cluster; remove one of the two options"
            ))),
            (Some(true), None) => Ok(WriterLayout::SingleCommit),
            (_, per_node) => Ok(WriterLayout::PerNode(per_node.unwrap_or(1))),
        }
    }
}

fn check_table_name(value: Option<&str>) -> Result<(), UdfError> {
    let form = "'<namespace>.<table>', a namespace of one or more '.'-separated levels and a table name, with no empty part";
    let Some(value) = value else {
        return Err(refusal(format_args!(
            "option {TABLE_NAME} is required; set {TABLE_NAME} = {form}"
        )));
    };
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() < 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(refusal(format_args!(
            "{TABLE_NAME} = '{value}' is not of the form {form}"
        )));
    }
    Ok(())
}

fn parse_writers_per_node(value: &str) -> Result<usize, UdfError> {
    let all_digits = !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    match value.parse::<usize>() {
        Ok(writers) if all_digits && writers >= 1 => Ok(writers),
        _ => Err(refusal(format_args!(
            "{WRITERS_PER_NODE} = '{value}' is not an integer of at least 1 that fits in 64 bits, for example {WRITERS_PER_NODE} = '4'"
        ))),
    }
}

fn parse_single_commit(value: &str) -> Result<bool, UdfError> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(refusal(format_args!(
            "{SINGLE_COMMIT} = '{value}' is not 'true' or 'false'"
        )))
    }
}

/// Exasol distributes groups round-robin up to this count; above it they are
/// hash-partitioned (unbalanced).
const MAX_WRITER_COUNT: usize = 300;

/// `TYPEOF` patterns of the types Exasol does not pass to a UDF reliably.
const UNSUPPORTED_TYPE_PATTERNS: [&str; 4] = [
    "GEOMETRY%",
    "HASHTYPE%",
    "INTERVAL%",
    "%WITH LOCAL TIME ZONE",
];

/// The SELECT the export hook answers with. It calls the export script itself as the
/// writer, so `run` sees the writer spec first and then the source columns.
struct WriterSelect<'a> {
    script_schema: &'a str,
    writer_spec: &'a str,
    /// Quoted column references as Exasol passes them, for example `"T"."A"`.
    source_columns: &'a [String],
    writers: WriterLayout,
    node_count: usize,
}

impl WriterSelect<'_> {
    /// Exasol binds `DUAL` to the export source. Every source row passes a type guard and
    /// reaches exactly one of G writers through the `MOD(ROWNUM, G)` group key.
    fn render(&self) -> String {
        let arguments = std::iter::once(string_literal(self.writer_spec))
            .chain(self.source_columns.iter().cloned())
            .collect::<Vec<_>>()
            .join(", ");
        let guards = self
            .source_columns
            .iter()
            .map(|column| type_guard(column))
            .collect::<Vec<_>>()
            .join(" AND ");
        // The key stays top-level: an export SELECT over a subquery of DUAL fails.
        format!(
            "SELECT {}.{}({arguments}) FROM DUAL WHERE {guards} GROUP BY MOD(ROWNUM, {})",
            quote_identifier(self.script_schema),
            quote_identifier(EXPORT_SCRIPT_NAME),
            self.group_count(),
        )
    }

    fn group_count(&self) -> usize {
        match self.writers {
            WriterLayout::SingleCommit => 1,
            WriterLayout::PerNode(per_node) => writer_count(self.node_count, per_node),
        }
    }
}

fn writer_count(node_count: usize, per_node: usize) -> usize {
    node_count
        .saturating_mul(per_node)
        .clamp(1, MAX_WRITER_COUNT)
}

// Exasol has no error function, so the guard fails the cast of its message. The message
// concatenates TYPEOF, because Exasol folds a constant CAST eagerly and fails every export.
fn type_guard(column: &str) -> String {
    let column_type = format!("TYPEOF({column})");
    let unsupported = UNSUPPORTED_TYPE_PATTERNS
        .iter()
        .map(|pattern| format!("{column_type} LIKE {}", string_literal(pattern)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let message_head = string_literal(&format!(
        "{EXPORT_SCRIPT_NAME}: column \"{}\" has type ",
        column_display_name(column)
    ));
    let message_tail =
        string_literal(", which the export does not support; cast it to a supported type");
    format!(
        "(CASE WHEN {unsupported} THEN CAST({message_head} || {column_type} || {message_tail} \
         AS DECIMAL(1,0)) ELSE 0 END) = 0"
    )
}

/// The last quoted identifier of `reference`, unescaped; `reference` itself when it holds
/// no quoted identifier.
fn column_display_name(reference: &str) -> String {
    let mut last = None;
    let mut chars = reference.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut identifier = String::new();
        while let Some(c) = chars.next() {
            match c {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    identifier.push('"');
                }
                '"' => break,
                c => identifier.push(c),
            }
        }
        last = Some(identifier);
    }
    last.unwrap_or_else(|| reference.to_string())
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn string_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
