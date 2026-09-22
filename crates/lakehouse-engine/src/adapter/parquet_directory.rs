/// Answers "what are the data files under this storage prefix, and what is their combined
/// schema" in ONE place, from an object store and a prefix alone.
///
/// Table enumeration and query planning both ask that question, so both read the same files and
/// fold the same footers and cannot disagree about a table's columns. Neither caller carries its
/// own listing filter, its own footer reader, or its own merge policy: two policies over one
/// `MERGE_SCHEMA` value is the drift this module exists to prevent.
///
/// The module names no catalog kind, no table format, no Exasol connection, and no virtual-schema
/// property — it takes a store and a prefix rather than a configuration. It receives no credential
/// either: the caller's store holds them, so no message produced here can carry one.
///
/// The declaration decides the emitted width and the footer decides the structure. A fold taken
/// over a narrower file set than the one that produced a stored declaration is not a defect to be
/// repaired by narrowing that declaration: the emit boundary coerces every column to its declared
/// `EMITS` type, and a plan-time fold WIDER than the declaration is the ordinary stale-declaration
/// case `REFRESH VIRTUAL SCHEMA` already owns.
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

/// Which of the listed files' footers the fold reads. Both modes return the SAME file list: the
/// mode narrows which footers are read, never which files are scanned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    /// Every listed file's footer, so a column appearing at several types resolves to the widest
    /// one successive supported widenings reach.
    FoldEveryFile,
    /// Exactly the first listed file's footer. The other files are listed and scanned, but their
    /// declared types are never read.
    SampleOneFile,
}

impl MergeMode {
    /// The mode a resolved `MERGE_SCHEMA` value selects.
    ///
    /// The ONE owner of that decision, so the enumeration path and the plan path cannot resolve
    /// one property value into two modes and declare a table at two different schemas.
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
    /// The file's `key=value` path segments below the prefix, deepest occurrence winning. Parsed
    /// here so the parser exists once; read by nobody yet.
    pub partition_segments: HashMap<String, String>,
    /// The PARSED footer, present exactly for the files the merge mode read and absent for every
    /// other listed file. A consumer reads this presence rather than indexing positionally against
    /// the file list: under [`MergeMode::SampleOneFile`] the two sequences have different lengths.
    /// Under [`MergeMode::FoldEveryFile`] every file carries one, so a consumer pruning files from
    /// footer statistics re-reads no footer. A consumer needing statistics for a file the
    /// sample-one-file mode left unread asks this module for them rather than opening that footer
    /// behind its back.
    pub footer: Option<Arc<ParquetMetaData>>,
}

/// The data files under one prefix and the one schema their footers fold to.
pub struct ParquetDirectory {
    pub files: Vec<ParquetFile>,
    /// Every column NULLABLE, named and ordered exactly as the files declare them.
    pub schema: SchemaRef,
}

/// The store-relative prefix a storage URI names — the second half of the pair
/// [`crate::scan::store_root_url`] answers the first half of.
///
/// A store built for a URI is scoped to that URI's `scheme://userinfo@host:port` slice: the
/// bucket on S3, the container on ADLS. Everything below that slice is the prefix this module
/// lists under, percent-decoded exactly as the object store itself addresses a key. Table
/// enumeration and query planning both need it, so it is derived HERE rather than once per
/// caller: two derivations could disagree about a key and list a table's files from two
/// different prefixes.
pub fn store_prefix(uri: &str) -> Result<StorePath, UdfError> {
    let url = url::Url::parse(uri)
        .map_err(|e| UdfError::User(format!("invalid storage URI '{uri}': {e}")))?;
    StorePath::from_url_path(url.path())
        .map_err(|e| UdfError::User(format!("invalid storage path in '{uri}': {e}")))
}

/// Lists `prefix`'s Parquet data files and folds the footers `mode` selects into one schema.
///
/// A prefix holding no data file answers an empty file list and an empty schema: whether that is
/// a table at all is the caller's decision, not this module's.
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

    // One fan-out rather than one serialized round-trip per file. The concurrency bound is the
    // admission limiter the caller's store carries, so this module adds no second one.
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

    // The store's own listing order is not a contract, and the sample-one-file mode must sample
    // the same footer on the enumeration path and the plan path.
    files.sort_by(|left, right| left.path.as_ref().cmp(right.path.as_ref()));
    Ok(files)
}

/// `location`'s path segments below `prefix`, or `None` when it is not a data file of that prefix.
///
/// A data file's name ends in `.parquet` and NO segment below the prefix begins with `_` or `.`,
/// which excludes a hidden or staging directory's contents however their own names read.
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
