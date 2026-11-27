//! The seam of the export's fixed flow: a target checks the catalog and opens the
//! table, and the sink it returns takes Arrow batches and writes and commits them its own way.
//! Also the export's CONNECTION, read and checked before any request.

use super::batch::TableColumns;
use super::iceberg_rest;
use super::options::ALLOW_HTTP;
use super::partition_by::PartitionField;
use super::{EXPORT_SCRIPT_NAME, refusal};
use crate::connection::{parse_creds, password_object, validate_shared_creds};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{ConnectionCreds, scheme_of};

/// One writer of one export statement, as the snapshots and data files it writes name it.
pub(super) struct WriterIdentity {
    /// The export statement's ID, drawn once by the hook.
    pub(super) export_id: String,
    /// The writer's own ID, drawn once per writer invocation.
    pub(super) writer_id: String,
}

/// Where an export writes: one catalog and table format.
#[async_trait(?Send)]
pub(super) trait ExportTarget {
    /// Connects and authenticates and answers whether the table exists, with catalog calls only.
    /// For a missing table it also creates the missing namespace levels.
    async fn prepare(&self) -> Result<bool, UdfError>;

    /// Creates the table, partitioned by `partition_by`, or takes the one another writer of the
    /// same export created, so every writer's sink commits into the one table. The sink buffers
    /// at most `budget` bytes of rows.
    async fn create(
        &self,
        schema: &Schema,
        partition_by: &[PartitionField],
        writer: &WriterIdentity,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError>;

    /// Prepares the table as `create` does without creating it: the table appears only when the
    /// sink commits its data.
    async fn create_with_data(
        &self,
        schema: &Schema,
        partition_by: &[PartitionField],
        writer: &WriterIdentity,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError>;

    /// Loads the existing table and refuses one the export cannot append to, before any data is
    /// written. With `SINGLE_COMMIT` the one writer appends the same way, in its one commit.
    async fn append(
        &self,
        writer: &WriterIdentity,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError>;
}

/// One writer's files in the table its target opened.
#[async_trait(?Send)]
pub(super) trait ExportSink {
    /// The table's columns, which every batch must have.
    fn columns(&self) -> &TableColumns;

    async fn write(&mut self, batch: &RecordBatch) -> Result<(), UdfError>;

    /// Closes the files and commits them; returns the file count.
    async fn finish(&mut self) -> Result<usize, UdfError>;

    /// Deletes the files the table does not hold, best effort.
    async fn abort(&mut self);
}

/// A CONNECTION the export can write through: an Iceberg REST catalog that needs no SigV4
/// signing, on S3-compatible storage or ADLS Gen2, with plain HTTP only under the export's
/// `ALLOW_HTTP`.
#[derive(Clone)]
pub(super) struct ExportConnection {
    /// The catalog address.
    pub(super) address: String,
    pub(super) creds: ConnectionCreds,
    /// The statement's consent to plain-HTTP requests.
    pub(super) allow_http: bool,
}

/// Reads CONNECTION `name` through the export script's own grant and refuses, before any
/// request, a CONNECTION the export does not write through. No error carries a CONNECTION value.
pub(super) fn read_connection(
    ctx: &dyn UdfContext,
    name: &str,
    allow_http: bool,
) -> Result<ExportConnection, UdfError> {
    let object = ctx
        .connection(name)
        .map_err(|cause| missing_access(ctx, name, cause))?;
    let creds = parse_creds(&password_object(name, &object.password).map_err(refusal)?);
    check_connection(name, &creds)?;
    validate_shared_creds(name, &creds, &object.address).map_err(refusal)?;
    let connection = ExportConnection {
        address: object.address,
        creds,
        allow_http,
    };
    refuse_plain_http(name, &connection)?;
    Ok(connection)
}

fn missing_access(ctx: &dyn UdfContext, connection: &str, cause: UdfError) -> UdfError {
    let script = format!("{}.{EXPORT_SCRIPT_NAME}", ctx.script_schema());
    let user = ctx.current_user().unwrap_or_else(|| "<user>".to_string());
    refusal(format_args!(
        "user {user} cannot read CONNECTION {connection} in script {script} ({cause}); \
         a DBA grants it with GRANT ACCESS ON CONNECTION {connection} FOR SCRIPT {script} TO {user}, \
         or to a role that {user} holds"
    ))
}

/// The target of `table_name` through `connection`, which the hook and every writer open alike.
pub(super) fn of(
    connection: ExportConnection,
    table_name: &str,
) -> Result<Box<dyn ExportTarget>, UdfError> {
    iceberg_rest::target(connection, table_name)
}

/// Refuses a `TABLE_NAME` value of a form the target does not take.
pub(super) fn check_table_name(value: &str) -> Result<(), UdfError> {
    iceberg_rest::check_table_name(value)
}

/// The form of a `TABLE_NAME` value, for the error of a missing one.
pub(super) fn table_name_form() -> &'static str {
    iceberg_rest::TABLE_NAME_FORM
}

/// The checks of the CONNECTION fields that the export's one target needs.
pub(super) fn check_connection(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    iceberg_rest::refuse_unwritable(name, creds)
}

/// The export's plaintext gate. The catalog crate's own checks cover only vended storage and
/// the STS client, and name the virtual schema property, so the export checks every field.
fn refuse_plain_http(name: &str, connection: &ExportConnection) -> Result<(), UdfError> {
    if connection.allow_http {
        return Ok(());
    }
    let creds = &connection.creds;
    let fields = [
        ("address", Some(connection.address.as_str())),
        ("oauth2_server_uri", creds.oauth2_server_uri.as_deref()),
        ("aws_sts_endpoint", creds.aws_sts_endpoint.as_deref()),
        ("endpoint", Some(creds.endpoint.as_str())),
    ];
    match fields
        .into_iter()
        .find(|(_, value)| value.is_some_and(|value| scheme_of(value) == "http"))
    {
        Some((field, _)) => Err(refusal(format_args!(
            "field {field} of CONNECTION '{name}' names a plain HTTP address; set \
             {ALLOW_HTTP} = 'true' to let the export send requests over plain HTTP"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "target_tests.rs"]
mod tests;
