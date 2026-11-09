//! One writer invocation: the input rows of its SET group become Arrow batches that stream into
//! the sink its target opens, and the sink commits them once the last row is in.

use super::arrow_schema::arrow_schema;
use super::batch::BatchBuilder;
use super::target::{ExportConnection, ExportSink, ExportTarget, WriterTag, read_connection};
use super::{WriterSpec, export_runtime, new_writer_id, not_an_export_target};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::ColumnInfo;
use tokio::runtime::Runtime;

/// Writes the group's rows and returns their count. A writer opens its table at its first row,
/// so an empty group creates no table and checks no column type.
pub(super) fn write_group(
    ctx: &mut dyn UdfContext,
    open_target: impl FnOnce(ExportConnection, &str) -> Result<Box<dyn ExportTarget>, UdfError>,
) -> Result<i64, UdfError> {
    // The host opens a SET group on its first row, and its first next() returns true without
    // advancing.
    if !ctx.next()? {
        return Ok(0);
    }
    let spec = WriterSpec::read(ctx)?;
    let connection = read_connection(ctx, &spec.connection, spec.allow_http)?;
    let schema = arrow_schema(&spec.columns, &source_columns(ctx, spec.columns.len())?)?;
    let runtime = export_runtime()?;
    let target = open_target(connection, &spec.table)?;
    let tag = WriterTag {
        export_id: spec.export_id.clone(),
        writer_id: new_writer_id(),
    };
    let mut sink = if spec.single_commit {
        runtime.block_on(target.stage_table(&schema, &tag))?
    } else {
        runtime.block_on(target.open_table(&schema, &tag))?
    };
    let written = write_rows(ctx, &runtime, sink.as_mut(), &spec.columns);
    if written.is_err() {
        runtime.block_on(sink.abort());
    }
    written
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

fn write_rows(
    ctx: &mut dyn UdfContext,
    runtime: &Runtime,
    sink: &mut dyn ExportSink,
    columns: &[String],
) -> Result<i64, UdfError> {
    let mut batch = BatchBuilder::bind(sink.schema(), columns)?;
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
#[path = "writer_tests.rs"]
pub(super) mod tests;
