use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageBackend;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde_json::Value as Json;
use std::collections::BTreeMap;

use super::{ConnectionStorage, FormatReader, ResolvedScan};
use crate::adapter::parquet_directory::{
    MergeMode, ParquetFile, resolve_parquet_directory, store_prefix,
};
use crate::scan::spec::{FileEntry, LogicalField, NestedField, NestedMembers};
use crate::scan::store_root_url;
use crate::types::mapping::arrow_type_to_tag;

#[cfg(test)]
#[path = "parquet_format_reader_tests.rs"]
mod tests;

/// Reads every directory as raw Parquet (no Iceberg/Delta detection); reuses [`resolve_parquet_directory`] so declared and scanned schemas can't diverge.
pub(super) struct ParquetFormatReader<'a> {
    store: &'a Arc<dyn ObjectStore>,
    table_root: &'a str,
    merge_mode: MergeMode,
    storage: &'a StorageBackend,
}

impl<'a> ParquetFormatReader<'a> {
    /// `store` is shared across every leg of the request (one admission limiter); this kind reaches no credential-vending catalog, so `connection`'s static backend is the effective one.
    pub(super) fn new(
        store: &'a Arc<dyn ObjectStore>,
        table_root: &'a str,
        merge_mode: MergeMode,
        connection: &ConnectionStorage<'a>,
    ) -> Self {
        Self {
            store,
            table_root,
            merge_mode,
            storage: connection.storage,
        }
    }
}

impl FormatReader for ParquetFormatReader<'_> {
    /// `filter_json` is deliberately unread: with no manifest or catalog statistics, the plan-time file list is every file under the table root regardless of filter.
    fn resolve_scan<'a>(
        &'a self,
        _filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let store_root = store_root_url(self.table_root)?;
            let prefix = store_prefix(self.table_root)?;
            let directory = resolve_parquet_directory(self.store, &prefix, self.merge_mode).await?;

            Ok(ResolvedScan {
                files: directory
                    .files
                    .iter()
                    .map(|file| file_entry(file, &prefix, store_root.as_str()))
                    .collect(),
                effective_storage: self.storage.clone(),
                logical_schema: logical_schema(&directory.schema),
                table_root: self.table_root.to_string(),
                name_mapping: Vec::new(),
                partition_columns: Vec::new(),
                refused_columns: Vec::new(),
            })
        })
    }
}

/// Size comes from the listing response (no extra HEAD or Parquet read); path is encoded relative to the table root, or as an absolute URI if outside it.
fn file_entry(file: &ParquetFile, prefix: &StorePath, store_root: &str) -> FileEntry {
    let relative = file.path.prefix_match(prefix).map(|parts| {
        parts
            .map(|part| part.as_ref().to_string())
            .collect::<Vec<String>>()
            .join("/")
    });
    let store_root = store_root.trim_end_matches('/');
    FileEntry {
        path: relative.unwrap_or_else(|| format!("{store_root}/{}", file.path)),
        size: file.size,
        deletes: Vec::new(),
        partition_values: BTreeMap::new(),
    }
}

/// Fields bind by name (identity), like Delta's `none` column-mapping mode; no ordinal field-id is synthesized, to avoid a false `PARQUET:field_id` match.
fn logical_schema(schema: &Schema) -> Vec<LogicalField> {
    schema
        .fields()
        .iter()
        .map(|field| LogicalField {
            field_id: None,
            name: field.name().clone(),
            arrow_type: arrow_type_to_tag(field.data_type()),
            nullable: field.is_nullable(),
            initial_default: None,
            nested: nested_members(field.data_type()),
            physical_name: None,
        })
        .collect()
}

/// This descriptor's presence selects the JSON renderer for nested columns (declared with the string Arrow tag); without it, a struct/list/map would reach the cast path, which has no string kernel.
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
            let pair: &[arrow::datatypes::FieldRef] = match entries.data_type() {
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
