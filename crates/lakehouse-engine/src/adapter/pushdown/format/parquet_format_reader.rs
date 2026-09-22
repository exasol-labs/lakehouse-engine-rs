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

/// The raw-Parquet-directory reader: one storage directory resolved into the scan
/// the pushdown layer plans against.
///
/// It implements NO table format and claims conformance to none. A directory that
/// happens to hold an Iceberg or a Delta table is read here as raw Parquet — no
/// snapshot selection, no delete file, no deletion vector — because the operator
/// selected a catalog kind that names no table format, and a layout heuristic would
/// fail a supported read on a directory it guessed wrong about.
///
/// It owns NO listing and NO footer parsing of its own: both come from
/// [`resolve_parquet_directory`], the same seam table enumeration reads, so the
/// schema a query plans against and the schema the virtual schema declared cannot be
/// folded by two different policies.
pub(super) struct ParquetFormatReader<'a> {
    store: &'a Arc<dyn ObjectStore>,
    table_root: &'a str,
    merge_mode: MergeMode,
    storage: &'a StorageBackend,
}

impl<'a> ParquetFormatReader<'a> {
    /// `store` is the request's ONE admission-limited object store, whose limiter
    /// therefore bounds every leg of the request rather than each leg separately.
    /// `connection` is the CONNECTION's static storage decision; this kind reaches
    /// no credential-vending catalog, so that backend is also the effective one.
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
    /// `filter_json` prunes nothing here and is deliberately unread: this kind has no
    /// catalog statistics and no manifest, so the plan-time file list is every data
    /// file under the table root and a filter narrows the rows the scan emits without
    /// narrowing the files it reads. Path-based partition pruning is issue #408 and
    /// footer-statistics pruning is issue #412; both are tracked exceptions rather
    /// than silent gaps.
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

/// One listed file as a scan entry: its path, its LISTED byte size, no delete
/// mechanism, and no partition value.
///
/// The size comes from the listing response the seam already carried, so neither the
/// scan's `ObjectMeta` construction nor a broadcast join's side sizing costs an
/// object-store HEAD or a Parquet read.
///
/// The path is encoded relative to the table root, the compact form every
/// unpartitioned delete-free scan already produces. A path the root does not cover
/// is encoded as the absolute URI `FileEntry::path` documents for that case.
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

/// The folded schema as logical fields bound by IDENTITY.
///
/// Each field carries neither a field-id nor a declared physical name, so the scan
/// binds it by its own name through the binding Delta's `none` column-mapping mode
/// already ships. No ordinal field-id is synthesized: an ordinal is a value no writer
/// ever wrote into any file, so tagging the logical schema with one would invite a
/// false `PARQUET:field_id` match against a file that does carry ids.
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

/// The nested member descriptor a container column carries, or `None` for a
/// primitive.
///
/// A nested column is declared with the string Arrow tag, so the scan reaches it
/// through the JSON renderer rather than through a cast — and the renderer is
/// selected by this descriptor's PRESENCE, so a struct, list, or map column without
/// one would reach the cast path, where no string kernel exists. Every member binds
/// by identity, matching the top-level fields.
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
