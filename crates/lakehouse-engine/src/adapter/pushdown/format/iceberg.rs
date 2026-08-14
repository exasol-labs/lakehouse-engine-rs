use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{CatalogProps, CatalogSession};
use serde_json::Value as Json;

use super::super::resolve_file_list;
use super::{ConnectionStorage, FormatReader, ResolvedScan};

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;

pub(super) struct IcebergFormatReader<'a> {
    pub(super) session: &'a CatalogSession,
    pub(super) catalog_props: &'a CatalogProps,
    pub(super) connection: ConnectionStorage<'a>,
}

impl FormatReader for IcebergFormatReader<'_> {
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        let ConnectionStorage {
            storage,
            creds,
            allow_http,
        } = self.connection;
        Box::pin(async move {
            let (files, effective_storage, logical_schema, table_root, name_mapping) =
                resolve_file_list(
                    self.session,
                    self.catalog_props,
                    storage,
                    creds,
                    allow_http,
                    filter_json,
                )
                .await?;

            Ok(ResolvedScan {
                files,
                effective_storage,
                logical_schema,
                table_root,
                name_mapping,
                partition_columns: Vec::new(),
            })
        })
    }
}
