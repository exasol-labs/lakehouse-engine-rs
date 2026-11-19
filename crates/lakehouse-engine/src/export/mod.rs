//! `EXPORT ... INTO SCRIPT` through one SET script: its export hook answers a SELECT that
//! calls the same script as the writer. No log event, script output, or CONNECTION value.

mod batch;
mod fanout;
mod iceberg_rest;
mod iceberg_sink;
mod options;
mod partition_by;
mod target;

use crate::exasol::{group_count, quote_ident, sql_string_literal};
use arrow::datatypes::Schema;
use batch::{BatchBuilder, SourceColumns, arrow_schema};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::spec::ExportSpec;
use exasol_udf_sdk::value::ColumnInfo;
use iceberg::metadata_columns::is_metadata_column_name;
use lakehouse_catalog_write::CommitTag;
use options::{ExportOptions, WriterLayout, check_table_name};
use partition_by::{PARTITION_BY, PartitionField};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::Deserialize;
use target::{ExportConnection, ExportSink, ExportTarget, TableMode, read_connection};
use tokio::runtime::Runtime;

/// The script name the installer creates and the writer SELECT calls.
pub const EXPORT_SCRIPT_NAME: &str = "LAKEHOUSE_EXPORT";

/// Answers `generate_sql_for_export_spec` with the writer SELECT, which holds no CONNECTION value.
/// Before any row is read, it checks the statement and CONNECTION, and whether the table exists.
pub fn generate_export_sql(ctx: &mut dyn UdfContext, json: &str) -> Result<String, UdfError> {
    let ExportPlan {
        connection,
        mut spec,
        select,
    } = plan_export(ctx, json)?;
    let target = iceberg_rest::target(connection, &spec.table)?;
    spec.set_mode(export_runtime()?.block_on(target.prepare())?)?;
    Ok(select.render(&spec.render()))
}

/// Runs one writer over its SET group and returns its row count. It reads the CONNECTION again,
/// as a direct `SELECT` skips the hook, and opens the table at its first row, so never for none.
pub fn run_writer(ctx: &mut dyn UdfContext) -> Result<i64, UdfError> {
    // The host opens a SET group on its first row, and its first next() returns true without
    // advancing.
    if !ctx.next()? {
        return Ok(0);
    }
    let plan = plan_writer(ctx)?;
    let runtime = export_runtime()?;
    let target = iceberg_rest::target(plan.connection, &plan.spec.table)?;
    let tag = CommitTag {
        export_id: plan.spec.export_id.clone(),
        writer_id: new_writer_id(),
    };
    let budget = fanout::buffer_budget(ctx.memory_limit());
    let mut sink = runtime.block_on(plan.table.open(target.as_ref(), &tag, budget))?;
    write_rows(ctx, &runtime, sink.as_mut(), &plan.source)
}

/// What the hook decides before its first network call.
struct ExportPlan {
    connection: ExportConnection,
    /// The writer spec, complete once the catalog check sets `append`.
    spec: WriterSpec,
    select: WriterSelect,
}

fn plan_export(ctx: &mut dyn UdfContext, json: &str) -> Result<ExportPlan, UdfError> {
    let spec = ExportSpec::from_json(json)?;
    let options = ExportOptions::from_spec(&spec)?;
    let columns = column_names(&spec.source_column_names)?;
    let partition_by = match &options.partition_by {
        Some(value) => partition_by::parse(value, &columns)?,
        None => Vec::new(),
    };
    let connection = read_connection(ctx, &options.connection_name, options.allow_http)?;
    let writer_spec = WriterSpec {
        connection: options.connection_name,
        table: options.table_name,
        export_id: new_export_id(),
        columns,
        single_commit: options.writers == WriterLayout::SingleCommit,
        allow_http: options.allow_http,
        append: false,
        partition_by,
    };
    let select = WriterSelect {
        script_schema: ctx.script_schema(),
        source_columns: spec.source_column_names,
        writers: options.writers,
        node_count: ctx.node_count() as usize,
    };
    Ok(ExportPlan {
        connection,
        spec: writer_spec,
        select,
    })
}

/// What a writer decides before its first network call. `ctx` must already stand on the group's
/// first row.
struct WriterPlan {
    spec: WriterSpec,
    connection: ExportConnection,
    source: SourceColumns,
    table: WriterTable,
}

/// The table a writer opens: a new one of the source's schema and partition fields, or the one
/// the hook found.
enum WriterTable {
    New {
        schema: Schema,
        partition_by: Vec<PartitionField>,
        single_commit: bool,
    },
    Existing,
}

impl WriterTable {
    async fn open(
        &self,
        target: &dyn ExportTarget,
        tag: &CommitTag,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        match self {
            Self::New {
                schema,
                partition_by,
                single_commit,
            } => {
                target
                    .open_new_table(schema, partition_by, tag, *single_commit, budget)
                    .await
            }
            Self::Existing => target.open_existing_table(tag, budget).await,
        }
    }
}

fn plan_writer(ctx: &dyn UdfContext) -> Result<WriterPlan, UdfError> {
    let spec = WriterSpec::read(ctx)?;
    let connection = read_connection(ctx, &spec.connection, spec.allow_http)?;
    let source = SourceColumns {
        names: spec.columns.clone(),
        columns: source_columns(ctx, spec.columns.len())?,
    };
    // An existing table's columns, not the source's, decide what the writer accepts.
    let table = if spec.append {
        WriterTable::Existing
    } else {
        let schema = arrow_schema(&source.names, &source.columns)?;
        partition_by::check_types(&spec.partition_by, &source)?;
        WriterTable::New {
            schema,
            partition_by: spec.partition_by.clone(),
            single_commit: spec.single_commit,
        }
    };
    Ok(WriterPlan {
        spec,
        connection,
        source,
        table,
    })
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
    /// Whether the table existed when the hook checked it, so the writers append to it.
    append: bool,
    /// The partition fields of a table the writers create, empty for an unpartitioned one.
    partition_by: Vec<PartitionField>,
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
            "append": self.append,
            "partition_by": self.partition_by,
        })
        .to_string()
    }

    /// Records whether the writers append, refusing `PARTITION_BY` for a table that exists.
    fn set_mode(&mut self, mode: TableMode) -> Result<(), UdfError> {
        self.append = mode == TableMode::Append;
        if self.append && !self.partition_by.is_empty() {
            return Err(refusal(format_args!(
                "table {} exists, and {PARTITION_BY} applies only to a table the export creates; \
                 remove {PARTITION_BY} to append to the table",
                self.table
            )));
        }
        Ok(())
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

/// The source column names: the last identifier of each source column reference, such as `A` of
/// `"T"."A"`. Iceberg resolves a reserved metadata column name to that column.
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
struct WriterSelect {
    script_schema: String,
    /// Quoted column references as Exasol passes them, for example `"T"."A"`.
    source_columns: Vec<String>,
    writers: WriterLayout,
    node_count: usize,
}

impl WriterSelect {
    /// Exasol binds `DUAL` to the export source. Every source row reaches exactly one of
    /// G writers through the `MOD(ROWNUM, G)` group key.
    fn render(&self, writer_spec: &str) -> String {
        let arguments = std::iter::once(sql_string_literal(writer_spec))
            .chain(self.source_columns.iter().cloned())
            .collect::<Vec<_>>()
            .join(", ");
        // The key stays top-level: an export SELECT over a subquery of DUAL fails.
        format!(
            "SELECT {}.{}({arguments}) FROM DUAL GROUP BY MOD(ROWNUM, {})",
            quote_ident(&self.script_schema),
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

/// The metadata of the source columns, input columns 1 to `count`.
fn source_columns(ctx: &dyn UdfContext, count: usize) -> Result<Vec<ColumnInfo>, UdfError> {
    if ctx.input_column_count() != count + 1 {
        return Err(not_an_export_target());
    }
    (1..=count)
        .map(|index| ctx.input_column(index).cloned())
        .collect()
}

/// Streams the group's rows into the sink and commits them once the last row is in. A failure
/// aborts the sink.
fn write_rows(
    ctx: &mut dyn UdfContext,
    runtime: &Runtime,
    sink: &mut dyn ExportSink,
    source: &SourceColumns,
) -> Result<i64, UdfError> {
    let written = stream_rows(ctx, runtime, sink, source);
    if written.is_err() {
        runtime.block_on(sink.abort());
    }
    written
}

fn stream_rows(
    ctx: &mut dyn UdfContext,
    runtime: &Runtime,
    sink: &mut dyn ExportSink,
    source: &SourceColumns,
) -> Result<i64, UdfError> {
    let mut batch = BatchBuilder::bind(sink.columns(), source)?;
    let mut rows = 0;
    loop {
        batch.append_row(ctx)?;
        rows += 1;
        if batch.is_full() {
            runtime.block_on(sink.write(&batch.take()?))?;
        }
        if !ctx.next()? {
            break;
        }
    }
    if !batch.is_empty() {
        runtime.block_on(sink.write(&batch.take()?))?;
    }
    runtime.block_on(sink.finish())?;
    Ok(rows)
}

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod test_support;

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
