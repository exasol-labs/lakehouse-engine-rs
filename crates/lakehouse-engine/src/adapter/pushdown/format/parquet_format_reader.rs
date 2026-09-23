use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageBackend;
use object_store::ObjectStore;
use object_store::path::{Path as StorePath, PathPart};
use percent_encoding::{AsciiSet, utf8_percent_encode};
use serde_json::Value as Json;
use std::collections::HashSet;

use super::partition_predicate::PartitionPredicate;
use super::{FormatReader, ResolvedScan};
use crate::adapter::parquet_directory::{
    DirectoryOptions, ParquetFile, resolve_parquet_directory, store_prefix,
};
use crate::scan::spec::{FileEntry, LogicalField, NestedField, NestedMembers};
use crate::scan::store_root_url;
use crate::types::mapping::{arrow_type_to_tag, exasol_type_to_arrow};

#[cfg(test)]
#[path = "parquet_format_reader_tests.rs"]
mod tests;

/// Reads every directory as raw Parquet (no Iceberg/Delta detection); reuses [`resolve_parquet_directory`] so declared and scanned schemas can't diverge.
pub(super) struct ParquetFormatReader<'a> {
    /// Shared across every leg of the request, so every leg shares one admission limiter.
    pub(super) store: &'a Arc<dyn ObjectStore>,
    pub(super) table_root: &'a str,
    pub(super) options: DirectoryOptions,
    pub(super) declared_columns: &'a [(String, String)],
    /// The CONNECTION's static backend, which is the effective one: this kind reaches no
    /// credential-vending catalog.
    pub(super) storage: &'a StorageBackend,
}

impl FormatReader for ParquetFormatReader<'_> {
    /// Prunes by `filter_json`'s partition-column predicates before any footer is read (see [`PartitionPredicate`]); with no manifest or catalog statistics, a filter on no partition column narrows the rows the scan emits, never the files it reads.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let store_root = store_root_url(self.table_root)?;
            let prefix = store_prefix(self.table_root)?;
            let predicate = PartitionPredicate::from_filter(filter_json);
            let directory =
                resolve_parquet_directory(self.store, &prefix, self.options, &move |values| {
                    predicate.keeps(values)
                })
                .await?;

            let mut logical_schema = logical_schema(&directory.schema);
            logical_schema.extend(absent_declared_fields(
                self.declared_columns,
                &directory.schema,
            ));
            Ok(ResolvedScan {
                files: directory
                    .files
                    .iter()
                    .map(|file| file_entry(file, &prefix, store_root.as_str()))
                    .collect(),
                effective_storage: self.storage.clone(),
                logical_schema,
                table_root: self.table_root.to_string(),
                name_mapping: Vec::new(),
                partition_columns: directory.partition_columns,
                refused_columns: Vec::new(),
            })
        })
    }
}

/// `ListingTableUrl::parse` percent-decodes the path and reads a raw `#` or `?` as a fragment or query, so exactly these three are encoded and every other path stays byte-identical.
const URL_PARSE_UNSAFE: &AsciiSet = &AsciiSet::EMPTY.add(b'%').add(b'#').add(b'?');

/// Size comes from the listing response (no extra HEAD or Parquet read); path is encoded relative to the table root, or as an absolute URI if outside it, so the scan resolves it to the listed object.
fn file_entry(file: &ParquetFile, prefix: &StorePath, store_root: &str) -> FileEntry {
    let relative = file.path.prefix_match(prefix).map(encoded_path);
    let store_root = store_root.trim_end_matches('/');
    FileEntry {
        path: relative
            .unwrap_or_else(|| format!("{store_root}/{}", encoded_path(file.path.parts()))),
        size: file.size,
        deletes: Vec::new(),
        partition_values: file.partition_values.clone(),
    }
}

fn encoded_path<'p>(parts: impl Iterator<Item = PathPart<'p>>) -> String {
    parts
        .map(|part| utf8_percent_encode(part.as_ref(), URL_PARSE_UNSAFE).to_string())
        .collect::<Vec<String>>()
        .join("/")
}

/// Pruning narrows which files a query reads, never the table's schema: a declared column no kept footer and no partition column carries (uppercase fold) still resolves, typed as Exasol declared it, and reads NULL from every scanned file.
fn absent_declared_fields(
    declared_columns: &[(String, String)],
    schema: &Schema,
) -> Vec<LogicalField> {
    let present: HashSet<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().to_uppercase())
        .collect();
    declared_columns
        .iter()
        .filter(|(name, _)| !present.contains(&name.to_uppercase()))
        .map(|(name, exasol_type)| LogicalField {
            field_id: None,
            name: name.clone(),
            arrow_type: arrow_type_to_tag(
                &exasol_type_to_arrow(exasol_type).unwrap_or(DataType::Utf8),
            ),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
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
