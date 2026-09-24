//! Single source of truth for a prefix's Parquet file list, partition columns, and folded schema,
//! so table enumeration and query planning can't disagree about a table's columns.
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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Which files' footers are folded and whose paths declare the partition keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    /// Every listed file's footer; a column at several types resolves to the widest reachable one.
    FoldEveryFile,
    /// Only the first listed file's footer; the rest are listed and scanned but never read.
    SampleOneFile,
}

/// The seam's layout switches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryOptions {
    pub merge_mode: MergeMode,
    pub hive_partitioning: bool,
}

/// Decides whether to keep a file, given its filled partition values; runs before any footer read.
pub type PartitionKeepPredicate = dyn Fn(&BTreeMap<String, Option<String>>) -> bool + Send + Sync;

/// One Parquet data file under the prefix.
pub struct ParquetFile {
    pub path: StorePath,
    /// Carried from the listing response, so no consumer issues an object-store HEAD for it.
    pub size: u64,
    /// Every declared partition key; `None` where this file's path lacks the segment or its value
    /// is empty/`__HIVE_DEFAULT_PARTITION__`.
    pub partition_values: BTreeMap<String, Option<String>>,
    /// Present iff this file's footer was read under the merge mode — check presence, not
    /// position, since [`MergeMode::SampleOneFile`] leaves most files' footers unset.
    pub footer: Option<Arc<ParquetMetaData>>,
}

/// The data files under one prefix, their partition columns, and their folded schema.
pub struct ParquetDirectory {
    pub files: Vec<ParquetFile>,
    /// Every column NULLABLE, named and ordered exactly as the files declare them, followed by the
    /// partition columns.
    pub schema: SchemaRef,
    /// The declared partition-key names, in the order appended to `schema`.
    pub partition_columns: Vec<String>,
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
///
/// Partition keys and the `SampleOneFile` footer come from the unfiltered listing, so `keep` never
/// changes the declared schema.
pub async fn resolve_parquet_directory(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    options: DirectoryOptions,
    keep: &PartitionKeepPredicate,
) -> Result<ParquetDirectory, UdfError> {
    let raw_files = list_data_files(store, prefix, options.hive_partitioning).await?;
    let sampled = &raw_files[..raw_files.len().min(1)];
    let declared_keys = declared_partition_keys(match options.merge_mode {
        MergeMode::FoldEveryFile => &raw_files,
        MergeMode::SampleOneFile => sampled,
    })?;
    // Even under SampleOneFile, a stored column named like a key any listed file carries is
    // dropped for the key.
    let key_columns = uppercase_index(raw_files.iter().flat_map(segment_keys));

    let kept: Vec<(&RawFile, BTreeMap<String, Option<String>>)> = raw_files
        .iter()
        .filter_map(|raw| {
            let filled = fill_partition_values(&raw.partition_segments, &declared_keys);
            keep(&filled).then_some((raw, filled))
        })
        .collect();
    let sources: Vec<&RawFile> = match options.merge_mode {
        MergeMode::FoldEveryFile => kept.iter().map(|(raw, _)| *raw).collect(),
        MergeMode::SampleOneFile => sampled.iter().collect(),
    };

    // Concurrency is bounded by the caller's store's admission limiter, not a second one here.
    let read: Vec<ArrowReaderMetadata> = try_join_all(
        sources
            .iter()
            .map(|raw| read_footer(Arc::clone(store), &raw.path, raw.size)),
    )
    .await?;

    let folded_fields = fold_schemas(&sources, &read, &key_columns)?;
    let footers: HashMap<&StorePath, &Arc<ParquetMetaData>> = sources
        .iter()
        .zip(&read)
        .map(|(raw, metadata)| (&raw.path, metadata.metadata()))
        .collect();
    let files = kept
        .into_iter()
        .map(|(raw, partition_values)| ParquetFile {
            path: raw.path.clone(),
            size: raw.size,
            partition_values,
            footer: footers.get(&raw.path).map(|footer| Arc::clone(footer)),
        })
        .collect();
    Ok(ParquetDirectory {
        files,
        schema: schema_with_partition_columns(folded_fields, &declared_keys),
        partition_columns: declared_keys,
    })
}

/// A listed file with its own raw partition segments, before filling against the declared keys.
struct RawFile {
    path: StorePath,
    size: u64,
    partition_segments: Vec<(String, Option<String>)>,
}

fn segment_keys(raw: &RawFile) -> impl Iterator<Item = &str> {
    raw.partition_segments.iter().map(|(key, _)| key.as_str())
}

async fn list_data_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    hive_partitioning: bool,
) -> Result<Vec<RawFile>, UdfError> {
    let listed: Vec<object_store::ObjectMeta> = store
        .list(Some(prefix))
        .try_collect()
        .await
        .map_err(|e| UdfError::User(format!("failed to list '{prefix}': {e}")))?;

    let mut files: Vec<RawFile> = listed
        .into_iter()
        .filter_map(|meta| {
            let segments = data_file_segments(&meta.location, prefix)?;
            let partition_segments = if hive_partitioning {
                let (_, directories) = segments.split_last()?;
                parse_partition_segments(directories)
            } else {
                Vec::new()
            };
            Some(RawFile {
                path: meta.location,
                size: meta.size,
                partition_segments,
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

/// A key repeated within one path takes its deepest value but keeps its first position.
fn parse_partition_segments(directories: &[String]) -> Vec<(String, Option<String>)> {
    let mut ordered: Vec<(String, Option<String>)> = Vec::new();
    for segment in directories {
        let Some((key, value)) = segment.split_once('=') else {
            continue;
        };
        if key.is_empty() {
            continue;
        }
        let decoded = decode_partition_value(value);
        match ordered.iter_mut().find(|(existing, _)| existing == key) {
            Some((_, existing_value)) => *existing_value = decoded,
            None => ordered.push((key.to_string(), decoded)),
        }
    }
    ordered
}

fn decode_partition_value(raw: &str) -> Option<String> {
    let decoded = percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(|value| value.into_owned())
        .unwrap_or_else(|_| raw.to_string());
    if decoded.is_empty() || decoded == HIVE_DEFAULT_PARTITION {
        None
    } else {
        Some(decoded)
    }
}

/// The distinct keys `scope`'s paths carry, in first-seen order; two spellings of one uppercased
/// name are an error.
fn declared_partition_keys(scope: &[RawFile]) -> Result<Vec<String>, UdfError> {
    let mut keys: Vec<String> = Vec::new();
    let mut seen_spellings: HashSet<&str> = HashSet::new();
    let mut by_uppercase: HashMap<String, (&str, &StorePath)> = HashMap::new();
    for raw in scope {
        for key in segment_keys(raw) {
            if !seen_spellings.insert(key) {
                continue;
            }
            let folded = key.to_uppercase();
            match by_uppercase.get(&folded) {
                Some(&(first, first_path)) => {
                    return Err(UdfError::User(format!(
                        "partition keys '{first}' (from '{first_path}') and '{key}' (from '{}') \
                         are the same name once uppercased, so the declaration would advertise a \
                         duplicate partition column. Neither directory spelling is preferred over \
                         the other: rename one of them.",
                        raw.path
                    )));
                }
                None => {
                    by_uppercase.insert(folded, (key, &raw.path));
                    keys.push(key.to_string());
                }
            }
        }
    }
    Ok(keys)
}

/// Uppercased key → its first-seen spelling.
fn uppercase_index<'a>(keys: impl Iterator<Item = &'a str>) -> HashMap<String, &'a str> {
    let mut seen_spellings: HashSet<&str> = HashSet::new();
    let mut index: HashMap<String, &str> = HashMap::new();
    for key in keys {
        if seen_spellings.insert(key) {
            index.entry(key.to_uppercase()).or_insert(key);
        }
    }
    index
}

fn fill_partition_values(
    raw: &[(String, Option<String>)],
    declared_keys: &[String],
) -> BTreeMap<String, Option<String>> {
    declared_keys
        .iter()
        .map(|key| {
            let value = raw
                .iter()
                .find(|(candidate, _)| candidate == key)
                .and_then(|(_, value)| value.clone());
            (key.clone(), value)
        })
        .collect()
}

fn missing_segment_error(column: &str, key: &str, path: &StorePath) -> UdfError {
    UdfError::User(format!(
        "Parquet column '{column}' in '{path}' collides with the partition key '{key}=' once \
         uppercased, but this file's own path carries no '{key}=' segment — it has neither a \
         directory value nor permission to fall back to its own stored value. Add the '{key}=' \
         segment to this file's path or rename the column."
    ))
}

async fn read_footer(
    store: Arc<dyn ObjectStore>,
    path: &StorePath,
    size: u64,
) -> Result<ArrowReaderMetadata, UdfError> {
    let mut reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
    ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "failed to read the Parquet footer of '{path}': {e}"
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

/// A stored column named like a partition key (`key_columns`, keyed uppercase) is dropped in favor
/// of the key; a read file that stores the column but lacks the key's segment is an error.
fn fold_schemas(
    sources: &[&RawFile],
    read: &[ArrowReaderMetadata],
    key_columns: &HashMap<String, &str>,
) -> Result<Vec<Field>, UdfError> {
    let mut columns: Vec<FoldedColumn> = Vec::new();
    let mut by_uppercase: HashMap<String, usize> = HashMap::new();

    for (source, metadata) in sources.iter().zip(read) {
        let path = &source.path;
        for field in metadata.schema().fields() {
            let folded = field.name().to_uppercase();
            if let Some(key) = key_columns.get(&folded) {
                if !segment_keys(source).any(|own| own.to_uppercase() == folded) {
                    return Err(missing_segment_error(field.name(), key, path));
                }
                continue;
            }
            match by_uppercase.get(&folded).copied() {
                Some(index) if columns[index].name == *field.name() => {
                    let column = &mut columns[index];
                    let Some(wider) = widen(&column.data_type, field.data_type()) else {
                        return Err(unfoldable_pair(column, field, path));
                    };
                    if wider != column.data_type {
                        column.data_type = wider;
                        column.source = path.clone();
                    }
                }
                Some(index) => return Err(colliding_names(&columns[index], field, path)),
                None => {
                    by_uppercase.insert(folded, columns.len());
                    columns.push(FoldedColumn {
                        name: field.name().clone(),
                        data_type: field.data_type().clone(),
                        source: path.clone(),
                    });
                }
            }
        }
    }

    // A column absent from one file is filled with NULL for that file's rows, so a column declared
    // required but absent would fail the scan instead.
    Ok(columns
        .into_iter()
        .map(|column| Field::new(column.name, column.data_type, true))
        .collect())
}

fn schema_with_partition_columns(mut fields: Vec<Field>, declared_keys: &[String]) -> SchemaRef {
    fields.extend(
        declared_keys
            .iter()
            .map(|key| Field::new(key.clone(), DataType::Utf8, true)),
    );
    Arc::new(Schema::new(fields))
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
