//! The export's first target: an Iceberg table created through an Iceberg REST catalog.

use super::iceberg_sink::{IcebergSink, OpenedTable};
use super::refusal;
use super::target::{ExportConnection, ExportSink, ExportTarget};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
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
    async fn prepare(&self) -> Result<(), UdfError> {
        let session = self.session().await?;
        session.refuse_existing_table(&self.ident).await?;
        session.ensure_namespace(&self.ident.namespace).await
    }

    async fn open_table(
        &self,
        schema: &Schema,
        tag: &CommitTag,
        single_commit: bool,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let session = self.session().await?;
        let schema = iceberg_schema(schema)?;
        let opened = if single_commit {
            OpenedTable::Staged(
                session
                    .stage_table(&self.ident, schema, &tag.export_id)
                    .await?,
            )
        } else {
            OpenedTable::Created(
                session
                    .create_table(&self.ident, schema, &tag.export_id)
                    .await?,
            )
        };
        Ok(Box::new(
            IcebergSink::open(session, opened, tag.clone()).await?,
        ))
    }
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
