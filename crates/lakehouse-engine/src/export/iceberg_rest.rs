//! The export's first target: an Iceberg table that the export creates through an Iceberg REST
//! catalog, or an existing one it appends to.

use super::iceberg_sink::IcebergSink;
use super::options::TABLE_NAME;
use super::partition_by::{PartitionField, unbound_spec};
use super::refusal;
use super::target::{ExportConnection, ExportSink, ExportTarget, WriterIdentity};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::UnboundPartitionSpec;
use lakehouse_catalog::{CatalogSession, ConnectionCreds, ResolvedIdentity, resolve_aws_identity};
use lakehouse_catalog_write::{
    CommitTag, Refusal, TableWriteSession, WriteStorage, refuse_unappendable,
};

/// The form of a `TABLE_NAME` value of an Iceberg REST table.
pub(super) const TABLE_NAME_FORM: &str = "'<namespace>.<table>', a namespace of one or more '.'-separated levels and a table name, with no empty part";

/// Refuses a `TABLE_NAME` value that is not a namespace and a table name joined by `.`.
pub(super) fn check_table_name(value: &str) -> Result<(), UdfError> {
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() < 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(refusal(format_args!(
            "{TABLE_NAME} = '{value}' is not of the form {TABLE_NAME_FORM}"
        )));
    }
    Ok(())
}

/// The Iceberg REST target of `table`, a `TABLE_NAME` value.
pub(super) fn target(
    connection: ExportConnection,
    table: &str,
) -> Result<Box<dyn ExportTarget>, UdfError> {
    let ident = TableIdent::from_strs(table.split('.'))
        .map_err(|error| refusal(format_args!("{TABLE_NAME} = '{table}': {error}")))?;
    Ok(Box::new(IcebergRestTarget { connection, ident }))
}

struct IcebergRestTarget {
    connection: ExportConnection,
    ident: TableIdent,
}

impl IcebergRestTarget {
    /// The one catalog session of this call; it renews its own OAuth2 token.
    async fn session(&self) -> Result<TableWriteSession, UdfError> {
        let ExportConnection {
            address,
            creds,
            allow_http,
        } = &self.connection;
        // A writer can outlive the assumed role's session, so its storage renews through `role`.
        let ResolvedIdentity { creds, role } =
            resolve_aws_identity(creds.clone(), address, *allow_http).await?;
        let storage = WriteStorage::of(&creds, *allow_http, role);
        let session = CatalogSession::resolve(address, &creds.warehouse, None, &creds)
            .await
            .map_err(|error| {
                refusal(format_args!(
                    "connecting to the catalog {address} failed: {error}"
                ))
            })?;
        Ok(TableWriteSession::new(session, storage, *allow_http))
    }
}

#[async_trait(?Send)]
impl ExportTarget for IcebergRestTarget {
    async fn prepare(&self) -> Result<bool, UdfError> {
        let session = self.session().await?;
        if session.table_exists(&self.ident).await? {
            return Ok(true);
        }
        session.ensure_namespace(&self.ident.namespace).await?;
        Ok(false)
    }

    async fn create(
        &self,
        schema: &Schema,
        partition_by: &[PartitionField],
        writer: &WriterIdentity,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let tag = commit_tag(writer);
        let (schema, spec) = table_definition(schema, partition_by)?;
        let session = self.session().await?;
        let created = session
            .create_table(&self.ident, schema, spec, &tag.export_id)
            .await?;
        Ok(Box::new(
            IcebergSink::open(session, created, tag, budget).await?,
        ))
    }

    async fn create_with_data(
        &self,
        schema: &Schema,
        partition_by: &[PartitionField],
        writer: &WriterIdentity,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let tag = commit_tag(writer);
        let (schema, spec) = table_definition(schema, partition_by)?;
        let session = self.session().await?;
        let staged = session
            .stage_table(&self.ident, schema, spec, &tag.export_id)
            .await?;
        Ok(Box::new(
            IcebergSink::open(session, staged, tag, budget).await?,
        ))
    }

    async fn append(
        &self,
        writer: &WriterIdentity,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let session = self.session().await?;
        let table = session.load_table(&self.ident).await?;
        refuse_unappendable(table.table().metadata())
            .map_err(|refused| unappendable(&self.ident, refused))?;
        Ok(Box::new(
            IcebergSink::open(session, table, commit_tag(writer), budget).await?,
        ))
    }
}

/// Refuses a CONNECTION without a warehouse, or one that asks for SigV4 signing.
pub(super) fn refuse_unwritable(name: &str, creds: &ConnectionCreds) -> Result<(), UdfError> {
    if creds.warehouse.is_empty() {
        return Err(refusal(format_args!(
            "CONNECTION '{name}' password is missing required field: warehouse, the Iceberg REST \
             warehouse the export writes into"
        )));
    }
    if creds.use_sigv4 {
        return Err(refusal(format_args!(
            "CONNECTION '{name}' sets use_sigv4, but the export does not sign catalog requests \
             with AWS SigV4, so it cannot write through AWS Glue's Iceberg REST endpoint"
        )));
    }
    Ok(())
}

/// The Iceberg schema and partition spec of a table the export creates.
fn table_definition(
    schema: &Schema,
    partition_by: &[PartitionField],
) -> Result<(iceberg::spec::Schema, Option<UnboundPartitionSpec>), UdfError> {
    let schema = iceberg_schema(schema)?;
    let spec = match partition_by {
        [] => None,
        fields => Some(unbound_spec(fields, &schema)?),
    };
    Ok((schema, spec))
}

/// The tag of `writer`'s commit.
fn commit_tag(writer: &WriterIdentity) -> CommitTag {
    CommitTag {
        export_id: writer.export_id.clone(),
        writer_id: writer.writer_id.clone(),
    }
}

/// The refusal of `table` at open, before any data is written.
fn unappendable(table: &TableIdent, refused: Refusal) -> UdfError {
    match refused {
        Refusal::FormatVersion(version) => refusal(format_args!(
            "table {table} has format version {version}; the export appends only to tables of \
             format version 1 or 2"
        )),
        Refusal::FieldsNamedLikeColumns(fields) => refusal(format_args!(
            "table {table} keeps the dropped partition field {} under the name of a column; the \
             export appends only to tables whose partition fields are not named like a column",
            fields.join(", ")
        )),
    }
}

/// The Iceberg schema of an export table, derived from the export's Arrow schema with field IDs
/// 1 to n in input order. The type rules live in `types::export_mapping`, not here.
fn iceberg_schema(arrow: &Schema) -> Result<iceberg::spec::Schema, UdfError> {
    arrow_schema_to_schema_auto_assign_ids(arrow).map_err(|error| {
        UdfError::User(format!(
            "could not derive the Iceberg schema of the export columns: {error}"
        ))
    })
}

#[cfg(test)]
#[path = "iceberg_rest_tests.rs"]
mod tests;
