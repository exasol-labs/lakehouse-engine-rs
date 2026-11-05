//! `EXPORT ... INTO SCRIPT` through one SET script: its export hook answers a SELECT that
//! calls the same script as the writer. No log event, script output, or CONNECTION value.

use crate::exasol::{
    CATALOG_CONNECTION, catalog_connection_name, group_count, parse_boolean, parse_whole_number,
    quote_ident, sql_string_literal,
};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::{ExportSpec, Parameter};
use serde::Deserialize;
use std::num::NonZeroUsize;

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

/// An export statement the engine accepts: append-only, with no `AT` clause, a
/// `CATALOG_CONNECTION` option, and a valid `TABLE_NAME`. It keeps only what the writer SELECT needs.
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
        refuse_at_clause(spec)?;
        let with_options = WithOptions::collect(&spec.parameters)?;
        let connection_name = with_options.connection_name()?;
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

// `AT` makes Exasol check the CONNECTION privilege without handing the script the
// credentials, so the connection travels as a WITH option instead.
fn refuse_at_clause(spec: &ExportSpec) -> Result<(), UdfError> {
    if spec.connection_name.is_none() && spec.connection_information.is_none() {
        return Ok(());
    }
    Err(refusal(format_args!(
        "the AT clause is not supported; remove it and name the CONNECTION with {CATALOG_CONNECTION} = '<connection>' in the WITH options"
    )))
}

struct WithOptions<'a> {
    catalog_connection: Option<&'a str>,
    table_name: Option<&'a str>,
    writers_per_node: Option<&'a str>,
    single_commit: Option<&'a str>,
}

impl<'a> WithOptions<'a> {
    fn collect(parameters: &'a [Parameter]) -> Result<Self, UdfError> {
        let mut options = Self {
            catalog_connection: None,
            table_name: None,
            writers_per_node: None,
            single_commit: None,
        };
        for parameter in parameters {
            let slot = match parameter.key.as_str() {
                CATALOG_CONNECTION => &mut options.catalog_connection,
                TABLE_NAME => &mut options.table_name,
                WRITERS_PER_NODE => &mut options.writers_per_node,
                SINGLE_COMMIT => &mut options.single_commit,
                unknown => {
                    return Err(refusal(format_args!(
                        "unknown option {unknown}; the accepted options are {CATALOG_CONNECTION}, {TABLE_NAME}, {WRITERS_PER_NODE}, and {SINGLE_COMMIT}"
                    )));
                }
            };
            *slot = Some(parameter.value.as_str());
        }
        Ok(options)
    }

    fn connection_name(&self) -> Result<String, UdfError> {
        catalog_connection_name(self.catalog_connection)
            .map(str::to_string)
            .ok_or_else(|| {
                refusal(format_args!(
                    "option {CATALOG_CONNECTION} is required; set {CATALOG_CONNECTION} = '<connection>', naming the CONNECTION object the virtual schema uses"
                ))
            })
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
    parse_whole_number::<NonZeroUsize>(value)
        .map(NonZeroUsize::get)
        .map_err(|expected| {
            refusal(format_args!(
                "{WRITERS_PER_NODE} = '{value}' is not {expected}, for example {WRITERS_PER_NODE} = '4'"
            ))
        })
}

fn parse_single_commit(value: &str) -> Result<bool, UdfError> {
    parse_boolean(value).ok_or_else(|| {
        refusal(format_args!(
            "{SINGLE_COMMIT} = '{value}' is not 'true' or 'false'"
        ))
    })
}

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
    /// Exasol binds `DUAL` to the export source. Every source row reaches exactly one of
    /// G writers through the `MOD(ROWNUM, G)` group key.
    fn render(&self) -> String {
        let arguments = std::iter::once(sql_string_literal(self.writer_spec))
            .chain(self.source_columns.iter().cloned())
            .collect::<Vec<_>>()
            .join(", ");
        // The key stays top-level: an export SELECT over a subquery of DUAL fails.
        format!(
            "SELECT {}.{}({arguments}) FROM DUAL GROUP BY MOD(ROWNUM, {})",
            quote_ident(self.script_schema),
            quote_ident(EXPORT_SCRIPT_NAME),
            self.group_count(),
        )
    }

    fn group_count(&self) -> usize {
        match self.writers {
            WriterLayout::SingleCommit => 1,
            WriterLayout::PerNode(per_node) => group_count(self.node_count, per_node, usize::MAX),
        }
    }
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
