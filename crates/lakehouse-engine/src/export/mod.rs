//! `EXPORT ... INTO SCRIPT` through one SET script: its export hook answers a SELECT that
//! calls the same script as the writer. No log event, script output, or CONNECTION value.

mod options;
mod sql;

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::ExportSpec;
use serde::Deserialize;

use options::ExportOptions;
use sql::WriterSelect;

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

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
