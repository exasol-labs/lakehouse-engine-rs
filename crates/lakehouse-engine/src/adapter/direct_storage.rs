//! The `CatalogClient` implementor for `CatalogKind::DirectStorage`: a plain object-storage
//! prefix holding directories of Parquet files, with no catalog service.
use crate::adapter::direct_storage_properties::join_storage_path;
use crate::adapter::parquet_directory::{MergeMode, resolve_parquet_directory, store_prefix};
use crate::scan::spec::StorageBackend;
use crate::types::mapping::{arrow_type_to_tag, needs_json_fallback};
use arrow::datatypes::DataType;
use exasol_udf_sdk::error::UdfError;
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
/// Declared here rather than in `lakehouse-catalog`: constructing it needs the engine's
/// admission-limited `object_store` builder, and `vs-adapter/catalog-crate-structure` forbids the
/// catalog crate's manifest from declaring `object_store` as a direct dependency. Declaring the
/// client there would point that dependency edge backwards. Rust's orphan rule permits this `impl`
/// because the TYPE is local even though the trait is not, and no recorded rule requires an
/// implementor of `CatalogClient` to live in the catalog crate — only that the engine reach every
/// enumeration and table-load operation THROUGH the trait, which this placement keeps unchanged:
/// the single `Box<dyn CatalogClient>` construction site is already engine-side.
pub struct DirectStorageCatalogClient {
    store: Arc<dyn ObjectStore>,
    prefix: StorePath,
    base_path: String,
    merge_mode: MergeMode,
}

impl DirectStorageCatalogClient {
    /// Opens the ONE admission-limited object store this client, and every table it enumerates,
    /// read through — rooted at `base_path` (the CONNECTION address already joined with
    /// `NAMESPACE` by the caller).
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

    /// The half of [`Self::new`] that derives the client's listing prefix from `base_path`,
    /// through the same seam the plan path lists a table root with.
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
    /// Enumerates the first-level directories under this client's base path: each one is a table,
    /// named for the directory alone (an EMPTY namespace), so the shared flatten and `TABLE_MAP`
    /// helpers produce the bare directory name with no branch on catalog kind. `namespace` is
    /// unread: the base path was already fully composed from `CATALOG_CONNECTION` and `NAMESPACE`
    /// at construction.
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

            let mut tables = Vec::with_capacity(names.len());
            let mut skipped = Vec::new();
            for name in names {
                let table_prefix = self.prefix.clone().join(name.as_str());
                let directory =
                    resolve_parquet_directory(&self.store, &table_prefix, self.merge_mode).await?;
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

    /// Unreachable on this kind's happy path: the pushdown path resolves a direct-storage table
    /// from its own composed root rather than from a catalog load, so this always returns a clear
    /// named error instead of panicking or synthesizing a table.
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

/// Map a folded Parquet schema's fields to the neutral, ordered column list the shared listing
/// pipeline consumes, applying the string substitution for a nested or unrepresentable column
/// BEFORE rendering each Arrow tag, so every tag this client emits names a type the vocabulary can
/// express.
///
/// The tag, not the footer's own Arrow type, is what this kind declares: a type the vocabulary
/// normalizes rather than reproduces — a timezone-aware timestamp, whose specific timezone label
/// the tag intentionally discards — is declared at its normalized form, exactly as the plan path's
/// `logical_schema` renders the same footer. Refusing such a column here instead would fail
/// `CREATE VIRTUAL SCHEMA` for a whole directory over a legal Parquet file the plan path accepts.
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
