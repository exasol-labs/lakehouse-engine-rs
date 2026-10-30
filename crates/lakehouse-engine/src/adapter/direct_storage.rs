use crate::adapter::column_notes::{ColumnDeclaration, column_refusal, unread_type_cause};
use crate::adapter::parquet_directory::{
    DirectoryOptions, ParquetDirectory, RefusedLeaf, resolve_parquet_directory,
};
use crate::scan::spec::{LogicalField, NestedField, NestedMembers, StorageBackend};
use crate::scan::{DIRECT_STORAGE_ADMISSION_LIMIT, ObjectLocation};
use crate::types::mapping::{arrow_type_to_tag, needs_json_fallback};
use arrow::datatypes::{DataType, Field, FieldRef};
use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt, stream};
use lakehouse_catalog::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogTable, CatalogTableIdent,
    CatalogTableType, ColumnSourceType, SkipReason, SkippedTable, TableAdmission, TableFormat,
    error_chain,
};
use object_store::ObjectStore;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Lives here, not in `lakehouse-catalog`, because it needs the engine's admission-limited
/// `object_store` builder, which that crate may not depend on.
pub struct DirectStorageCatalogClient {
    store: Arc<dyn ObjectStore>,
    /// The CONNECTION address names its key raw, like the folder names listed below it.
    root: ObjectLocation,
    options: DirectoryOptions,
}

impl DirectStorageCatalogClient {
    /// `base_path` is the CONNECTION address already joined with `NAMESPACE` by the caller.
    pub fn new(
        backend: &StorageBackend,
        base_path: &str,
        options: DirectoryOptions,
        all_secrets: &[&str],
    ) -> Result<Self, UdfError> {
        let store_url = ObjectLocation::from_raw(base_path)?.store_root().clone();
        let store = crate::scan::build_admission_limited_store(backend, &store_url, all_secrets)?;
        Self::over_store(store, base_path, options)
    }

    pub(crate) fn over_store(
        store: Arc<dyn ObjectStore>,
        base_path: &str,
        options: DirectoryOptions,
    ) -> Result<Self, UdfError> {
        Ok(Self {
            store,
            root: ObjectLocation::from_raw(base_path)?,
            options,
        })
    }

    /// The one admission rule both listings share, so `SKIPPED_TABLES` reads the same after either.
    async fn admit(&self, ident: CatalogTableIdent) -> Result<TableAdmission, UdfError> {
        let table_root = self.root.child(&ident.name)?;
        let directory =
            resolve_parquet_directory(&self.store, table_root.key(), self.options).await?;
        if directory.files.is_empty() {
            return Ok(TableAdmission::Skipped(SkippedTable {
                ident,
                reason: SkipReason::NoDataFile,
            }));
        }
        Ok(TableAdmission::Admitted(CatalogTable {
            ident,
            table_type: CatalogTableType::Table,
            storage_location: Some(table_root.to_string()),
            format: TableFormat::Parquet,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            metadata_location: None,
            columns: resolve_columns(&directory),
            properties: Default::default(),
        }))
    }
}

impl CatalogClient for DirectStorageCatalogClient {
    /// Each first-level directory under the base path, sorted by name, is a table with an empty
    /// namespace; one delimiter listing reads below no directory, and `namespace` is unread.
    fn list_table_idents(
        &self,
        _namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CatalogTableIdent>, UdfError>> + Send + '_>> {
        Box::pin(async move {
            let listed = self
                .store
                .list_with_delimiter(Some(self.root.key()))
                .await
                .map_err(|e| {
                    UdfError::User(format!(
                        "failed to list '{}': {}",
                        self.root,
                        error_chain(&e)
                    ))
                })?;

            let mut names: Vec<String> = listed
                .common_prefixes
                .iter()
                .filter_map(|p| p.filename().map(str::to_string))
                .collect();
            names.sort();
            Ok(names
                .into_iter()
                .map(|name| CatalogTableIdent {
                    namespace: Vec::new(),
                    name,
                })
                .collect())
        })
    }

    fn load_listed_tables(
        &self,
        idents: &[CatalogTableIdent],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        let idents = idents.to_vec();
        Box::pin(async move {
            let admissions: Vec<TableAdmission> = stream::iter(idents)
                .map(|ident| self.admit(ident))
                .buffered(DIRECT_STORAGE_ADMISSION_LIMIT)
                .try_collect()
                .await?;
            Ok(admissions.into_iter().collect())
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

/// The tag and the note come from the one folded schema, so they cannot disagree. Nested or
/// unrepresentable columns tag as Utf8 (JSON text) instead of failing CREATE VIRTUAL SCHEMA, and
/// binary columns keep their type, which declares hex text.
fn resolve_columns(directory: &ParquetDirectory) -> Vec<CatalogColumn> {
    directory
        .schema
        .fields()
        .iter()
        .map(|field| {
            let declared =
                if needs_json_fallback(field.data_type()) && !field.data_type().is_binary() {
                    DataType::Utf8
                } else {
                    field.data_type().clone()
                };
            let declaration = ColumnDeclaration {
                source_type: logical_field(field),
                partition: directory
                    .partition_columns
                    .iter()
                    .position(|key| key == field.name()),
                refused: directory
                    .refused_leaves
                    .iter()
                    .find(|leaf| leaf.column == *field.name())
                    .map(leaf_refusal),
            };
            CatalogColumn {
                name: field.name().clone(),
                source_type: ColumnSourceType::Parquet(arrow_type_to_tag(&declared)),
                declaration: Some(declaration.encode()),
            }
        })
        .collect()
}

fn leaf_refusal(leaf: &RefusedLeaf) -> String {
    let mut reason = column_refusal(
        "Parquet",
        &leaf.column,
        leaf.member_path.as_deref(),
        &unread_type_cause(&leaf.declared),
    );
    if leaf.nested_enum {
        reason
            .push_str("; a Parquet ENUM is read as text only as a top-level, non-repeated column");
    }
    reason
}

/// No ordinal field-id is synthesized: it could falsely match a file's `PARQUET:field_id`.
fn logical_field(field: &Field) -> LogicalField {
    LogicalField {
        field_id: None,
        name: field.name().clone(),
        arrow_type: arrow_type_to_tag(field.data_type()),
        nullable: field.is_nullable(),
        initial_default: None,
        nested: nested_members(field.data_type()),
        physical_name: None,
    }
}

/// Its presence routes nested columns to the JSON renderer; the cast path has no string kernel
/// for struct/list/map.
fn nested_members(data_type: &DataType) -> Option<NestedMembers> {
    match data_type {
        DataType::Struct(fields) => Some(NestedMembers::Struct {
            fields: fields
                .iter()
                .map(|field| NestedField {
                    field_id: None,
                    name: field.name().clone(),
                    physical_name: None,
                    nested: nested_members(field.data_type()),
                })
                .collect(),
        }),
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Some(NestedMembers::List {
            element: nested_members(element.data_type()).map(Box::new),
        }),
        DataType::Map(entries, _) => {
            let pair: &[FieldRef] = match entries.data_type() {
                DataType::Struct(fields) => fields,
                _ => &[],
            };
            Some(NestedMembers::Map {
                key: pair
                    .first()
                    .and_then(|field| nested_members(field.data_type()))
                    .map(Box::new),
                value: pair
                    .get(1)
                    .and_then(|field| nested_members(field.data_type()))
                    .map(Box::new),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "direct_storage_tests.rs"]
mod tests;
