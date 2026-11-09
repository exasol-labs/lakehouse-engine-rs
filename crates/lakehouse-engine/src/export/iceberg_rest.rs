//! The export's first target: an Iceberg table created through an Iceberg REST catalog.

use super::iceberg_schema::iceberg_schema;
use super::iceberg_sink::{IcebergSink, OpenedTable};
use super::refusal;
use super::target::{ExportConnection, ExportSink, ExportTarget, WriterTag};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use exasol_udf_sdk::error::UdfError;
use iceberg::TableIdent;
use lakehouse_catalog::{CatalogSession, resolve_aws_identity};
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
        let creds = resolve_aws_identity(creds.clone(), address, *allow_http).await?;
        let storage = WriteStorage::of(&creds, *allow_http);
        let session = CatalogSession::resolve(address, &creds.warehouse, &creds)
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
        tag: &WriterTag,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let session = self.session().await?;
        let schema = iceberg_schema(schema)?;
        let created = session
            .create_table(&self.ident, schema, &tag.export_id)
            .await?;
        sink(session, OpenedTable::Created(created), tag).await
    }

    async fn stage_table(
        &self,
        schema: &Schema,
        tag: &WriterTag,
    ) -> Result<Box<dyn ExportSink>, UdfError> {
        let session = self.session().await?;
        let schema = iceberg_schema(schema)?;
        let staged = session
            .stage_table(&self.ident, schema, &tag.export_id)
            .await?;
        sink(session, OpenedTable::Staged(staged), tag).await
    }
}

async fn sink(
    session: TableWriteSession,
    opened: OpenedTable,
    tag: &WriterTag,
) -> Result<Box<dyn ExportSink>, UdfError> {
    let tag = CommitTag {
        export_id: tag.export_id.clone(),
        writer_id: tag.writer_id.clone(),
    };
    Ok(Box::new(IcebergSink::open(session, opened, tag).await?))
}

#[cfg(test)]
#[path = "iceberg_rest_tests.rs"]
mod tests;
