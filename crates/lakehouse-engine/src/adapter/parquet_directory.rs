//! Single source of truth for a prefix's Parquet file list, partition columns, and folded schema,
//! so table enumeration and query planning can't disagree about a table's columns.
use crate::types::widening::widen;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use futures::future::try_join_all;
use lakehouse_catalog::HIVE_DEFAULT_PARTITION;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::ColumnDescriptor;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Which files' footers are folded and whose paths declare the partition keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    /// Every listed file's footer; a column at several types resolves to the widest reachable one.
    FoldEveryFile,
    /// Only the first listed file's footer; the rest are listed and scanned but never read.
    SampleOneFile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryOptions {
    pub merge_mode: MergeMode,
    pub hive_partitioning: bool,
}

/// Decides whether to keep a file, given its filled partition values; runs before any footer read.
pub type PartitionKeepPredicate = dyn Fn(&BTreeMap<String, Option<String>>) -> bool + Send + Sync;

/// Which listed objects below the prefix are data files. Internal, so no virtual-schema property
/// can set it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilePattern {
    /// Direct storage and Unity Parquet read Spark- and Hive-written `.parquet` trees.
    ParquetAtAnyDepth,
    /// A Glue partition location's data files: Trino writes extensionless ones, and a nested
    /// partition location must not be read twice.
    AnyDirectChild,
}

/// An object the plain listing step keeps as a data file.
pub struct ListedFile {
    pub path: StorePath,
    /// Carried from the listing response, so no consumer issues an object-store HEAD for it.
    pub size: u64,
    /// Every `key=value` directory segment below the listed prefix, in path order.
    partition_segments: Vec<(String, Option<String>)>,
}

impl ListedFile {
    pub fn with_partition_values(
        self,
        partition_values: BTreeMap<String, Option<String>>,
    ) -> ParquetFile {
        ParquetFile {
            path: self.path,
            size: self.size,
            partition_values,
            footer: None,
        }
    }
}

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

pub struct ParquetDirectory {
    pub files: Vec<ParquetFile>,
    /// Every column NULLABLE, named and ordered exactly as the files declare them, followed by the
    /// partition columns.
    pub schema: SchemaRef,
    /// The declared partition-key names, in the order appended to `schema`.
    pub partition_columns: Vec<String>,
    /// In `schema` order; the listing still declares these columns, only a read refuses them.
    pub binary_columns: Vec<BinaryColumn>,
}

/// Read from the footer's annotations, not the folded Arrow type: `parquet` folds an unannotated
/// `BYTE_ARRAY` and an `ENUM` one alike to `Binary` (`vs-adapter/binary-column-refusal`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryColumn {
    pub column: String,
    /// The member's Parquet column path, `None` when the column's own leaf is binary.
    pub member_path: Option<String>,
    /// In the file's own terms: `binary`, `fixed(L)`, `uuid`, `bson`, `geometry`, `geography`,
    /// or `enum`.
    pub declared: String,
    /// An `ENUM` leaf below the top level or under a repeated one, which only a top-level
    /// cast reads as text.
    pub nested_enum: bool,
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

/// A Glue location names the literal object key, so unlike [`store_prefix`] the key is never
/// percent-decoded; `s3a` reads as `s3`.
pub fn raw_location_prefix(location: &str) -> Result<(String, StorePath), UdfError> {
    let refused =
        |cause: String| UdfError::User(format!("invalid storage location '{location}': {cause}"));
    let (scheme, rest) = location
        .split_once("://")
        .filter(|(scheme, _)| !scheme.is_empty())
        .ok_or_else(|| refused("it names no scheme".to_string()))?;
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(refused("it names no bucket".to_string()));
    }
    let scheme = match scheme.to_ascii_lowercase().as_str() {
        "s3a" => "s3".to_string(),
        other => other.to_string(),
    };
    let prefix = StorePath::parse(key).map_err(|error| refused(error.to_string()))?;
    Ok((format!("{scheme}://{bucket}"), prefix))
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

    let kept: Vec<(&ListedFile, BTreeMap<String, Option<String>>)> = raw_files
        .iter()
        .filter_map(|raw| {
            let filled = fill_partition_values(
                &raw.partition_segments,
                &declared_keys,
                &declared_keys,
                |candidate, key| candidate == key,
            );
            keep(&filled).then_some((raw, filled))
        })
        .collect();
    let sources: Vec<&ListedFile> = match options.merge_mode {
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
    let binary_columns = binary_columns(&read, &folded_fields);
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
        binary_columns,
    })
}

/// [`resolve_parquet_directory`]'s file selection for a caller declaring its own schema and
/// partition columns, with no footer read. Values come from the deepest case-folded path match.
pub async fn list_parquet_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    partition_columns: &[String],
    keep: &PartitionKeepPredicate,
) -> Result<Vec<ParquetFile>, UdfError> {
    let raw_files = list_data_files(store, prefix, true).await?;
    let folded_columns: Vec<String> = partition_columns
        .iter()
        .map(|column| column.to_uppercase())
        .collect();
    Ok(raw_files
        .into_iter()
        .filter_map(|raw| {
            let partition_values = fill_partition_values(
                &raw.partition_segments,
                partition_columns,
                &folded_columns,
                |candidate, folded| {
                    candidate
                        .chars()
                        .flat_map(char::to_uppercase)
                        .eq(folded.chars())
                },
            );
            keep(&partition_values).then(|| raw.with_partition_values(partition_values))
        })
        .collect())
}

fn segment_keys(raw: &ListedFile) -> impl Iterator<Item = &str> {
    raw.partition_segments.iter().map(|(key, _)| key.as_str())
}

async fn list_data_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    hive_partitioning: bool,
) -> Result<Vec<ListedFile>, UdfError> {
    let mut files = list_location_files(store, prefix, FilePattern::ParquetAtAnyDepth).await?;
    if !hive_partitioning {
        for file in &mut files {
            file.partition_segments.clear();
        }
    }
    Ok(files)
}

/// Sorted by path. A segment below the prefix starting with `_` or `.`, or a zero-byte object (a
/// Hadoop `_$folder$` marker), is never a data file.
pub async fn list_location_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    pattern: FilePattern,
) -> Result<Vec<ListedFile>, UdfError> {
    // A delimiter listing fetches no object below a subdirectory.
    let listed: Vec<object_store::ObjectMeta> = match pattern {
        FilePattern::ParquetAtAnyDepth => store.list(Some(prefix)).try_collect().await,
        FilePattern::AnyDirectChild => store
            .list_with_delimiter(Some(prefix))
            .await
            .map(|listing| listing.objects),
    }
    .map_err(|e| UdfError::User(format!("failed to list '{prefix}': {e}")))?;

    let mut files: Vec<ListedFile> = listed
        .into_iter()
        .filter(|meta| meta.size > 0)
        .filter_map(|meta| {
            let mut segments = data_file_segments(&meta.location, prefix)?;
            let name = segments.pop()?;
            let is_data_file = match pattern {
                FilePattern::ParquetAtAnyDepth => name.ends_with(".parquet"),
                // The delimiter listing returns direct children only.
                FilePattern::AnyDirectChild => true,
            };
            is_data_file.then(|| ListedFile {
                path: meta.location,
                size: meta.size,
                partition_segments: parse_partition_segments(&segments),
            })
        })
        .collect();

    // Listing order isn't a contract; sort so SampleOneFile picks the same footer every time.
    files.sort_by(|left, right| left.path.as_ref().cmp(right.path.as_ref()));
    Ok(files)
}

fn data_file_segments(location: &StorePath, prefix: &StorePath) -> Option<Vec<String>> {
    let segments: Vec<String> = location
        .prefix_match(prefix)?
        .map(|part| part.as_ref().to_string())
        .collect();
    if segments.is_empty()
        || segments
            .iter()
            .any(|segment| segment.starts_with('_') || segment.starts_with('.'))
    {
        return None;
    }
    Some(segments)
}

/// Every `key=value` segment in path order, repeats included; the fill step picks the deepest.
fn parse_partition_segments(directories: &[String]) -> Vec<(String, Option<String>)> {
    directories
        .iter()
        .filter_map(|segment| segment.split_once('='))
        .filter(|(key, _)| !key.is_empty())
        .map(|(key, value)| (key.to_string(), decode_partition_value(value)))
        .collect()
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
fn declared_partition_keys(scope: &[ListedFile]) -> Result<Vec<String>, UdfError> {
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
    match_keys: &[String],
    key_matches: impl Fn(&str, &str) -> bool,
) -> BTreeMap<String, Option<String>> {
    declared_keys
        .iter()
        .zip(match_keys)
        .map(|(key, match_key)| {
            let value = raw
                .iter()
                .rfind(|(candidate, _)| key_matches(candidate, match_key))
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
    sources: &[&ListedFile],
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

/// Only columns of `folded` count, so a stored column dropped for a partition key is never
/// refused; the first listed file's leaf names a column's type.
fn binary_columns(read: &[ArrowReaderMetadata], folded: &[Field]) -> Vec<BinaryColumn> {
    let mut first_by_column: HashMap<String, BinaryColumn> = HashMap::new();
    for metadata in read {
        for leaf in metadata.metadata().file_metadata().schema_descr().columns() {
            let parts = leaf.path().parts();
            let is_top_level_scalar = parts.len() == 1 && leaf.max_rep_level() == 0;
            let (declared, nested_enum) = match classify_leaf(leaf) {
                LeafKind::Readable => continue,
                LeafKind::Enum if is_top_level_scalar => continue,
                LeafKind::Enum => ("enum".to_string(), true),
                LeafKind::Binary(declared) => (declared, false),
            };
            first_by_column
                .entry(parts[0].clone())
                .or_insert_with(|| BinaryColumn {
                    column: parts[0].clone(),
                    member_path: (parts.len() > 1).then(|| leaf.path().string()),
                    declared,
                    nested_enum,
                });
        }
    }
    folded
        .iter()
        .filter_map(|field| first_by_column.remove(field.name()))
        .collect()
}

enum LeafKind {
    Readable,
    /// Text per Parquet LogicalTypes § ENUM, but folded to Arrow `Binary`, so only the top-level
    /// cast reads it as text; the JSON renderer would print a member's bytes as hexadecimal.
    Enum,
    Binary(String),
}

/// Mirrors `parquet` 58's `from_byte_array` and `from_fixed_len_byte_array`: the logical type
/// decides, and the converted type only when a file carries no logical type.
fn classify_leaf(leaf: &ColumnDescriptor) -> LeafKind {
    let binary = |declared: &str| LeafKind::Binary(declared.to_string());
    match (
        leaf.physical_type(),
        leaf.logical_type_ref(),
        leaf.converted_type(),
    ) {
        (_, Some(LogicalType::Unknown), _) => LeafKind::Readable,
        (PhysicalType::BYTE_ARRAY, Some(logical), _) => match logical {
            LogicalType::String | LogicalType::Json | LogicalType::Decimal { .. } => {
                LeafKind::Readable
            }
            LogicalType::Enum => LeafKind::Enum,
            LogicalType::Bson => binary("bson"),
            LogicalType::Geometry { .. } => binary("geometry"),
            LogicalType::Geography { .. } => binary("geography"),
            _ => binary("binary"),
        },
        (PhysicalType::BYTE_ARRAY, None, converted) => match converted {
            ConvertedType::UTF8 | ConvertedType::JSON | ConvertedType::DECIMAL => {
                LeafKind::Readable
            }
            ConvertedType::ENUM => LeafKind::Enum,
            ConvertedType::BSON => binary("bson"),
            _ => binary("binary"),
        },
        (
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            Some(LogicalType::Decimal { .. } | LogicalType::Float16),
            _,
        )
        | (
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            None,
            ConvertedType::DECIMAL | ConvertedType::INTERVAL,
        ) => LeafKind::Readable,
        (PhysicalType::FIXED_LEN_BYTE_ARRAY, Some(LogicalType::Uuid), _) => binary("uuid"),
        (PhysicalType::FIXED_LEN_BYTE_ARRAY, _, _) => {
            LeafKind::Binary(format!("fixed({})", leaf.type_length()))
        }
        _ => LeafKind::Readable,
    }
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
