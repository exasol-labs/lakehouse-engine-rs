//! The `CatalogClient` implementor for `CatalogKind::DirectStorage`: a plain object-storage
//! prefix holding directories of Parquet files, with no catalog service.
use crate::adapter::direct_storage_properties::join_storage_path;
use crate::adapter::parquet_directory::{MergeMode, resolve_parquet_directory, store_prefix};
use crate::scan::spec::StorageBackend;
use crate::types::mapping::{arrow_type_to_tag, needs_json_fallback};
use arrow::datatypes::DataType;
use exasol_udf_sdk::error::UdfError;
use futures::future::try_join_all;
use lakehouse_catalog::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogTable, CatalogTableIdent,
    CatalogTableType, ColumnSourceType, SkipReason, SkippedTable, TableFormat,
};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A [`CatalogClient`] over a plain object-storage prefix, with no catalog service behind it.
///
/// Lives here, not in `lakehouse-catalog`, because building it needs the engine's
/// admission-limited `object_store` builder and that crate may not depend on `object_store`
/// directly (`vs-adapter/catalog-crate-structure`).
pub struct DirectStorageCatalogClient {
    store: Arc<dyn ObjectStore>,
    prefix: StorePath,
    base_path: String,
    merge_mode: MergeMode,
}

impl DirectStorageCatalogClient {
    /// `base_path` is the CONNECTION address already joined with `NAMESPACE` by the caller.
    pub fn new(
        backend: &StorageBackend,
        base_path: &str,
        merge_mode: MergeMode,
        all_secrets: &[&str],
    ) -> Result<Self, UdfError> {
        let store_url = crate::scan::store_root_url(base_path)?;
        let store = crate::scan::build_admission_limited_store(backend, &store_url, all_secrets)?;
        Self::over_store(store, base_path, merge_mode)
    }

    fn over_store(
        store: Arc<dyn ObjectStore>,
        base_path: &str,
        merge_mode: MergeMode,
    ) -> Result<Self, UdfError> {
        Ok(Self {
            store,
            prefix: store_prefix(base_path)?,
            base_path: base_path.to_string(),
            merge_mode,
        })
    }
}

impl CatalogClient for DirectStorageCatalogClient {
    /// Each first-level directory under the base path is a table with an empty namespace;
    /// `namespace` is unread since the base path already encodes `CATALOG_CONNECTION` + `NAMESPACE`.
    fn list_tables(
        &self,
        _namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        Box::pin(async move {
            let listed = self
                .store
                .list_with_delimiter(Some(&self.prefix))
                .await
                .map_err(|e| UdfError::User(format!("failed to list '{}': {e}", self.prefix)))?;

            let mut names: Vec<String> = listed
                .common_prefixes
                .iter()
                .filter_map(|p| p.filename().map(str::to_string))
                .collect();
            names.sort();

            // Fan out per-table directory resolution instead of serializing it.
            let directories = try_join_all(names.iter().map(|name| {
                let table_prefix = self.prefix.clone().join(name.as_str());
                async move {
                    resolve_parquet_directory(&self.store, &table_prefix, self.merge_mode).await
                }
            }))
            .await?;

            let mut tables = Vec::with_capacity(names.len());
            let mut skipped = Vec::new();
            for (name, directory) in names.into_iter().zip(directories) {
                let ident = CatalogTableIdent {
                    namespace: Vec::new(),
                    name: name.clone(),
                };
                if directory.files.is_empty() {
                    skipped.push(SkippedTable {
                        ident,
                        reason: SkipReason::NoDataFile,
                    });
                    continue;
                }
                let columns = resolve_columns(&directory.schema);
                tables.push(CatalogTable {
                    ident,
                    table_type: CatalogTableType::Table,
                    storage_location: Some(join_storage_path(&self.base_path, Some(&name))),
                    format: TableFormat::Parquet,
                    vended_credential_key: None,
                    columns,
                });
            }
            Ok(CatalogListing { tables, skipped })
        })
    }

    /// Unreachable on the happy path: pushdown resolves a direct-storage table from its own
    /// composed root instead of a catalog load, so this just returns a named error.
    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let name = ident.name.clone();
        Box::pin(async move {
            Err(UdfError::User(format!(
                "direct-storage catalog kind: table '{name}' is not loaded through \
                 CatalogClient::load_table; the pushdown path resolves a direct-storage table \
                 from its composed root instead"
            )))
        })
    }
}

/// Falls back to Utf8 for nested/unrepresentable columns before tagging, matching the plan path's
/// `logical_schema` normalization — refusing them here would fail `CREATE VIRTUAL SCHEMA` outright.
fn resolve_columns(schema: &arrow::datatypes::SchemaRef) -> Vec<CatalogColumn> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let declared = if needs_json_fallback(field.data_type()) {
                DataType::Utf8
            } else {
                field.data_type().clone()
            };
            CatalogColumn {
                name: field.name().clone(),
                source_type: ColumnSourceType::Parquet(arrow_type_to_tag(&declared)),
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "direct_storage_tests.rs"]
mod tests;
