//! Single source of truth for a prefix's Parquet file list and folded schema, so table
//! enumeration and query planning can't disagree about a table's columns.
use crate::types::widening::widen;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use futures::future::try_join_all;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::sync::Arc;

/// Which footers the fold reads; both modes return the same file list — only the footer set differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    /// Every listed file's footer; a column at several types resolves to the widest reachable one.
    FoldEveryFile,
    /// Only the first listed file's footer; the rest are listed and scanned but never read.
    SampleOneFile,
}

impl MergeMode {
    /// The ONE owner of this decision, so enumeration and planning can't resolve `MERGE_SCHEMA`
    /// into two different modes.
    pub fn for_merge_schema(merge_schema: bool) -> Self {
        match merge_schema {
            true => Self::FoldEveryFile,
            false => Self::SampleOneFile,
        }
    }
}

/// One Parquet data file under the prefix.
pub struct ParquetFile {
    pub path: StorePath,
    /// Carried from the listing response, so no consumer issues an object-store HEAD for it.
    pub size: u64,
    /// The file's `key=value` path segments below the prefix, deepest occurrence winning; unread
    /// so far.
    pub partition_segments: HashMap<String, String>,
    /// Present iff this file's footer was read under the merge mode — check presence, not
    /// position, since [`MergeMode::SampleOneFile`] leaves most files' footers unset.
    pub footer: Option<Arc<ParquetMetaData>>,
}

/// The data files under one prefix and the one schema their footers fold to.
pub struct ParquetDirectory {
    pub files: Vec<ParquetFile>,
    /// Every column NULLABLE, named and ordered exactly as the files declare them.
    pub schema: SchemaRef,
}

/// The store-relative prefix a storage URI names (pairs with [`crate::scan::store_root_url`]).
/// Derived here once rather than per caller, so enumeration and planning can't disagree and list
/// a table's files from two different prefixes.
pub fn store_prefix(uri: &str) -> Result<StorePath, UdfError> {
    let url = url::Url::parse(uri)
        .map_err(|e| UdfError::User(format!("invalid storage URI '{uri}': {e}")))?;
    StorePath::from_url_path(url.path())
        .map_err(|e| UdfError::User(format!("invalid storage path in '{uri}': {e}")))
}

/// A prefix holding no data file returns an empty file list and schema; whether that counts as a
/// table is the caller's decision.
pub async fn resolve_parquet_directory(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    mode: MergeMode,
) -> Result<ParquetDirectory, UdfError> {
    let mut files = list_data_files(store, prefix).await?;
    let footer_count = match mode {
        MergeMode::FoldEveryFile => files.len(),
        MergeMode::SampleOneFile => files.len().min(1),
    };

    // Concurrency is bounded by the caller's store's admission limiter, not a second one here.
    let read = try_join_all(
        files
            .iter()
            .take(footer_count)
            .map(|file| read_footer(Arc::clone(store), file)),
    )
    .await?;

    let schema = fold_schemas(&files[..footer_count], &read)?;
    for (file, footer) in files.iter_mut().zip(read) {
        file.footer = Some(Arc::clone(footer.metadata()));
    }

    Ok(ParquetDirectory { files, schema })
}

async fn list_data_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
) -> Result<Vec<ParquetFile>, UdfError> {
    let listed: Vec<object_store::ObjectMeta> = store
        .list(Some(prefix))
        .try_collect()
        .await
        .map_err(|e| UdfError::User(format!("failed to list '{prefix}': {e}")))?;

    let mut files: Vec<ParquetFile> = listed
        .into_iter()
        .filter_map(|meta| {
            let segments = data_file_segments(&meta.location, prefix)?;
            Some(ParquetFile {
                partition_segments: partition_segments(&segments),
                path: meta.location,
                size: meta.size,
                footer: None,
            })
        })
        .collect();

    // Listing order isn't a contract; sort so SampleOneFile picks the same footer every time.
    files.sort_by(|left, right| left.path.as_ref().cmp(right.path.as_ref()));
    Ok(files)
}

/// A data file's name ends in `.parquet` and no segment below the prefix starts with `_` or `.`,
/// which excludes hidden/staging directories regardless of their own names.
fn data_file_segments(location: &StorePath, prefix: &StorePath) -> Option<Vec<String>> {
    let segments: Vec<String> = location
        .prefix_match(prefix)?
        .map(|part| part.as_ref().to_string())
        .collect();
    let name = segments.last()?;
    if !name.ends_with(".parquet") {
        return None;
    }
    if segments
        .iter()
        .any(|segment| segment.starts_with('_') || segment.starts_with('.'))
    {
        return None;
    }
    Some(segments)
}

fn partition_segments(segments: &[String]) -> HashMap<String, String> {
    let Some((_, directories)) = segments.split_last() else {
        return HashMap::new();
    };
    directories
        .iter()
        .filter_map(|segment| segment.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

async fn read_footer(
    store: Arc<dyn ObjectStore>,
    file: &ParquetFile,
) -> Result<ArrowReaderMetadata, UdfError> {
    let mut reader = ParquetObjectReader::new(store, file.path.clone()).with_file_size(file.size);
    ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "failed to read the Parquet footer of '{}': {e}",
                file.path
            ))
        })
}

/// One column of the folded schema, with the file that contributed its current type so a later
/// conflict can name both sides.
struct FoldedColumn {
    name: String,
    data_type: DataType,
    source: StorePath,
}

fn fold_schemas(
    files: &[ParquetFile],
    read: &[ArrowReaderMetadata],
) -> Result<SchemaRef, UdfError> {
    let mut columns: Vec<FoldedColumn> = Vec::new();
    let mut by_uppercase: HashMap<String, usize> = HashMap::new();

    for (file, metadata) in files.iter().zip(read) {
        for field in metadata.schema().fields() {
            let folded = field.name().to_uppercase();
            match by_uppercase.get(&folded).copied() {
                Some(index) if columns[index].name == *field.name() => {
                    let column = &mut columns[index];
                    let Some(wider) = widen(&column.data_type, field.data_type()) else {
                        return Err(unfoldable_pair(column, field, &file.path));
                    };
                    if wider != column.data_type {
                        column.data_type = wider;
                        column.source = file.path.clone();
                    }
                }
                Some(index) => return Err(colliding_names(&columns[index], field, &file.path)),
                None => {
                    by_uppercase.insert(folded, columns.len());
                    columns.push(FoldedColumn {
                        name: field.name().clone(),
                        data_type: field.data_type().clone(),
                        source: file.path.clone(),
                    });
                }
            }
        }
    }

    // A column absent from one file is filled with NULL for that file's rows, so a column declared
    // required but absent would fail the scan instead.
    Ok(Arc::new(Schema::new(
        columns
            .into_iter()
            .map(|column| Field::new(column.name, column.data_type, true))
            .collect::<Vec<Field>>(),
    )))
}

fn unfoldable_pair(column: &FoldedColumn, field: &Field, path: &StorePath) -> UdfError {
    UdfError::User(format!(
        "Parquet column '{}' is declared as {:?} in '{}' and as {:?} in '{}', and no supported \
         type relaxation widens either to the other. Exclude the offending file or rewrite it at \
         one type: declaring the column as a string, dropping it, or taking one file's type would \
         each answer a correctness question by guessing.",
        column.name,
        column.data_type,
        column.source,
        field.data_type(),
        path,
    ))
}

fn colliding_names(column: &FoldedColumn, field: &Field, path: &StorePath) -> UdfError {
    UdfError::User(format!(
        "Parquet columns '{}' in '{}' and '{}' in '{}' are the same name once uppercased, so the \
         declaration would advertise a duplicate column. Rename one of them.",
        column.name,
        column.source,
        field.name(),
        path,
    ))
}

#[cfg(test)]
#[path = "parquet_directory_tests.rs"]
mod tests;
