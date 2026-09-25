use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageBackend;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde_json::Value as Json;
use std::collections::HashSet;

use super::partition_predicate::PartitionPredicate;
use super::{FormatReader, ResolvedScan};
use crate::adapter::parquet_directory::{
    DirectoryOptions, ParquetDirectory, ParquetFile, resolve_parquet_directory, store_prefix,
};
use crate::scan::spec::{FileEntry, LogicalField, NestedField, NestedMembers};
use crate::scan::{encode_file_path, store_root_url};
use crate::types::mapping::{arrow_type_to_tag, exasol_type_to_arrow};

#[cfg(test)]
#[path = "parquet_format_reader_tests.rs"]
mod tests;

/// Reads every directory as raw Parquet (no Iceberg/Delta detection); reuses [`resolve_parquet_directory`] so declared and scanned schemas can't diverge.
pub(super) struct ParquetFormatReader<'a> {
    /// Shared across every leg of the request (one admission limiter).
    pub(super) store: &'a Arc<dyn ObjectStore>,
    pub(super) table_root: &'a str,
    pub(super) options: DirectoryOptions,
    pub(super) declared_columns: &'a [(String, String)],
    /// The CONNECTION's static backend; no credential-vending catalog overrides it.
    pub(super) storage: &'a StorageBackend,
}

impl FormatReader for ParquetFormatReader<'_> {
    /// Prunes files by partition-column predicates before any footer is read; other predicates
    /// never prune (no file statistics).
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let store_root = store_root_url(self.table_root)?;
            let prefix = store_prefix(self.table_root)?;
            let predicate = PartitionPredicate::from_filter(filter_json);
            let ParquetDirectory {
                files,
                schema,
                partition_columns,
            } = resolve_parquet_directory(self.store, &prefix, self.options, &move |values| {
                predicate.keeps(values)
            })
            .await?;

            let mut logical = logical_schema(&schema);
            logical.extend(logical_schema(&Schema::new(absent_declared_fields(
                self.declared_columns,
                &schema,
            ))));
            Ok(ResolvedScan {
                files: files
                    .into_iter()
                    .map(|file| file_entry(file, &prefix, store_root.as_str()))
                    .collect(),
                effective_storage: self.storage.clone(),
                logical_schema: logical,
                table_root: self.table_root.to_string(),
                name_mapping: Vec::new(),
                partition_columns,
                refused_columns: Vec::new(),
            })
        })
    }
}

/// Size comes from the listing response (no extra HEAD or Parquet read); path is encoded relative to the table root, or as an absolute URI if outside it.
pub(super) fn file_entry(file: ParquetFile, prefix: &StorePath, store_root: &str) -> FileEntry {
    let len_hint = file.path.as_ref().len();
    let relative = file
        .path
        .prefix_match(prefix)
        .map(|parts| encode_file_path(parts, len_hint));
    let store_root = store_root.trim_end_matches('/');
    FileEntry {
        path: relative.unwrap_or_else(|| {
            format!(
                "{store_root}/{}",
                encode_file_path(file.path.parts(), len_hint)
            )
        }),
        size: file.size,
        deletes: Vec::new(),
        partition_values: file.partition_values,
    }
}

/// Declared columns absent from `schema` (uppercase fold), typed as declared; they read NULL.
fn absent_declared_fields(declared_columns: &[(String, String)], schema: &Schema) -> Vec<Field> {
    let present: HashSet<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().to_uppercase())
        .collect();
    declared_columns
        .iter()
        .filter(|(name, _)| !present.contains(&name.to_uppercase()))
        .map(|(name, exasol_type)| {
            let data_type = exasol_type_to_arrow(exasol_type).unwrap_or(DataType::Utf8);
            Field::new(name, data_type, true)
        })
        .collect()
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
