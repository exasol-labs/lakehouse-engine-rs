//! `EXPORT ... INTO SCRIPT` through one SET script: its export hook answers a SELECT that
//! calls the same script as the writer. No log event, script output, or CONNECTION value.

mod arrow_schema;
mod batch;
mod iceberg_rest;
mod iceberg_schema;
mod iceberg_sink;
mod options;
mod target;
mod writer;

use crate::exasol::{group_count, quote_ident, sql_string_literal};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::ExportSpec;
use iceberg::metadata_columns::is_metadata_column_name;
use options::{ExportOptions, WriterLayout, check_table_name};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::Deserialize;
use target::{ExportConnection, ExportTarget, read_connection};

/// The script name the installer creates and the writer SELECT calls.
pub const EXPORT_SCRIPT_NAME: &str = "LAKEHOUSE_EXPORT";

/// Answers Exasol's `generate_sql_for_export_spec` call with the writer SELECT. It refuses
/// an invalid statement, a CONNECTION the script cannot read, and a table the catalog already
/// holds, before any source row is read. The SELECT carries the CONNECTION name and no value.
pub fn generate_export_sql(ctx: &mut dyn UdfContext, json: &str) -> Result<String, UdfError> {
    export_hook(ctx, json, iceberg_rest::target)
}

/// Runs one writer invocation over its SET group and returns the group's row count. It
/// reads the CONNECTION again, because a direct `SELECT` call skips the export hook.
pub fn run_writer(ctx: &mut dyn UdfContext) -> Result<i64, UdfError> {
    writer::write_group(ctx, iceberg_rest::target)
}

fn export_hook(
    ctx: &mut dyn UdfContext,
    json: &str,
    open_target: impl FnOnce(ExportConnection, &str) -> Result<Box<dyn ExportTarget>, UdfError>,
) -> Result<String, UdfError> {
    let spec = ExportSpec::from_json(json)?;
    let options = ExportOptions::from_spec(&spec)?;
    let columns = column_names(&spec.source_column_names)?;
    let connection = read_connection(ctx, &options.connection_name, options.allow_http)?;
    let target = open_target(connection, &options.table_name)?;
    export_runtime()?.block_on(target.prepare())?;
    let writer_spec = WriterSpec {
        connection: options.connection_name,
        table: options.table_name,
        export_id: new_export_id(),
        columns,
        single_commit: options.writers == WriterLayout::SingleCommit,
        allow_http: options.allow_http,
    };
    Ok(WriterSelect {
        script_schema: &ctx.script_schema(),
        writer_spec: &writer_spec.render(),
        source_columns: &spec.source_column_names,
        writers: options.writers,
        node_count: ctx.node_count() as usize,
    }
    .render())
}

/// The first argument of every writer call. A direct `SELECT` call can forge it, so the
/// writer parses it strictly and never echoes it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterSpec {
    connection: String,
    table: String,
    /// Identifies the export statement in the snapshots its writers commit.
    export_id: String,
    /// The source column names, in input order after the writer spec.
    columns: Vec<String>,
    single_commit: bool,
    allow_http: bool,
}

impl WriterSpec {
    fn render(&self) -> String {
        serde_json::json!({
            "connection": self.connection,
            "table": self.table,
            "export_id": self.export_id,
            "columns": self.columns,
            "single_commit": self.single_commit,
            "allow_http": self.allow_http,
        })
        .to_string()
    }

    fn read(ctx: &dyn UdfContext) -> Result<Self, UdfError> {
        let text = ctx
            .get_string(0)
            .map_err(|_| not_an_export_target())?
            .ok_or_else(not_an_export_target)?;
        let spec: Self = serde_json::from_str(text).map_err(|_| not_an_export_target())?;
        check_table_name(Some(&spec.table))?;
        Ok(spec)
    }
}

/// 16 random bytes in hex, drawn once per statement.
fn new_export_id() -> String {
    random_hex::<16>()
}

/// 8 random bytes in hex, drawn once per writer invocation.
fn new_writer_id() -> String {
    random_hex::<8>()
}

fn random_hex<const BYTES: usize>() -> String {
    let mut bytes = [0u8; BYTES];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// The export keeps no state across calls, so each call builds its own runtime.
fn export_runtime() -> Result<tokio::runtime::Runtime, UdfError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| refusal(format_args!("could not build the Tokio runtime: {error}")))
}

/// The column names of the new table: the last identifier of each source column reference,
/// such as `A` of `"T"."A"`. Iceberg resolves a reserved metadata column name to that column.
fn column_names(references: &[String]) -> Result<Vec<String>, UdfError> {
    let names: Vec<String> = references.iter().map(|r| last_identifier(r)).collect();
    for (index, name) in names.iter().enumerate() {
        if names[..index].contains(name) {
            return Err(refusal(format_args!(
                "column {name} appears twice in the export source; give each column a distinct alias"
            )));
        }
        if is_metadata_column_name(name) {
            return Err(refusal(format_args!(
                "column {name} has a name reserved for an Iceberg metadata column; give it another alias"
            )));
        }
    }
    Ok(names)
}

/// The last double-quoted identifier of `reference`, with `""` read as `"`.
fn last_identifier(reference: &str) -> String {
    let mut last = None;
    let mut chars = reference.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut identifier = String::new();
        while let Some(c) = chars.next() {
            match c {
                '"' if chars.next_if_eq(&'"').is_some() => identifier.push('"'),
                '"' => break,
                c => identifier.push(c),
            }
        }
        last = Some(identifier);
    }
    last.unwrap_or_else(|| reference.to_string())
}

fn not_an_export_target() -> UdfError {
    refusal(format_args!(
        "{EXPORT_SCRIPT_NAME} runs as the target of EXPORT ... INTO SCRIPT, not as a function in a query: its first argument is not the writer spec that the export generates"
    ))
}

fn refusal(message: impl std::fmt::Display) -> UdfError {
    UdfError::User(format!("{EXPORT_SCRIPT_NAME}: {message}"))
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
