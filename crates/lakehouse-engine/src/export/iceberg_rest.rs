//! The export's first target: an Iceberg table that the export creates through an Iceberg REST
//! catalog, or an existing one it appends to.

use super::iceberg_sink::{IcebergSink, OpenedTable};
use super::partition_by::{PartitionField, unbound_spec};
use super::refusal;
use super::target::{ExportConnection, ExportSink, ExportTarget, TableMode};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{FormatVersion, PartitionSpec, Transform};
use lakehouse_catalog::{CatalogSession, ResolvedIdentity, resolve_aws_identity};
use lakehouse_catalog_write::{CommitTag, TableWriteSession, WriteStorage};

/// The Iceberg REST target of `table`, a `TABLE_NAME` value.
pub(super) fn target(
    connection: ExportConnection,
    table: &str,
) -> Result<Box<dyn ExportTarget>, UdfError> {
    let ident = TableIdent::from_strs(table.split('.'))
        .map_err(|error| refusal(format_args!("TABLE_NAME = '{table}': {error}")))?;
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
    async fn prepare(&self) -> Result<TableMode, UdfError> {
        let session = self.session().await?;
        if session.table_exists(&self.ident).await? {
            return Ok(TableMode::Append);
        }
        session.ensure_namespace(&self.ident.namespace).await?;
        Ok(TableMode::Create)
    }

    async fn open_new_table(
        &self,
        schema: &Schema,
        partition_by: &[PartitionField],
        tag: &CommitTag,
        single_commit: bool,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let schema = iceberg_schema(schema)?;
        let spec = match partition_by {
            [] => None,
            fields => Some(unbound_spec(fields, &schema)?),
        };
        let session = self.session().await?;
        let opened = if single_commit {
            OpenedTable::Staged(
                session
                    .stage_table(&self.ident, schema, spec, &tag.export_id)
                    .await?,
            )
        } else {
            OpenedTable::Listed(
                session
                    .create_table(&self.ident, schema, spec, &tag.export_id)
                    .await?,
            )
        };
        Ok(Box::new(
            IcebergSink::open(session, opened, tag.clone(), budget).await?,
        ))
    }

    async fn open_existing_table(
        &self,
        tag: &CommitTag,
        budget: usize,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let session = self.session().await?;
        let table = session.load_table(&self.ident).await?;
        let metadata = table.table().metadata();
        refuse_unappendable(
            &self.ident,
            metadata.format_version(),
            metadata.default_partition_spec(),
            metadata.current_schema(),
        )?;
        let opened = OpenedTable::Listed(table);
        Ok(Box::new(
            IcebergSink::open(session, opened, tag.clone(), budget).await?,
        ))
    }
}

/// Refuses format version 3 or later, whose row lineage and encryption keys the export does not
/// maintain, and a partition field named like a column unless it is that column's identity, which
/// iceberg-rust cannot commit to.
fn refuse_unappendable(
    table: &TableIdent,
    version: FormatVersion,
    spec: &PartitionSpec,
    schema: &iceberg::spec::Schema,
) -> Result<(), UdfError> {
    if version > FormatVersion::V2 {
        return Err(refusal(format_args!(
            "table {table} has format version {}; the export appends only to tables of format \
             version 1 or 2",
            version as u8
        )));
    }
    // iceberg-rust's commit rebinds the default spec and refuses a field named like a column unless
    // it is that column's identity, which Spark breaks when it drops a version 1 identity field.
    let named_like_columns: Vec<String> = spec
        .fields()
        .iter()
        .filter(|field| {
            schema.field_by_name(&field.name).is_some_and(|column| {
                field.transform != Transform::Identity || column.id != field.source_id
            })
        })
        .map(|field| format!("{} ({})", field.name, field.transform))
        .collect();
    if !named_like_columns.is_empty() {
        return Err(refusal(format_args!(
            "table {table} keeps the dropped partition field {} under the name of a column; the \
             export appends only to tables whose partition fields are not named like a column",
            named_like_columns.join(", ")
        )));
    }
    Ok(())
}

/// The Iceberg schema of an export table, derived from the export's Arrow schema with field IDs
/// 1 to n in input order. The type rules live in `batch.rs`, not here.
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
