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

/// Which files' footers the fold reads and whose paths declare the partition keys: every listed
/// file, or only the first.
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

/// The seam's two layout switches, resolved once by
/// [`crate::adapter::direct_storage_properties::DirectStorageProperties::directory_options`], so
/// both callers apply the identical policy — the seam itself names no virtual-schema property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryOptions {
    pub merge_mode: MergeMode,
    pub hive_partitioning: bool,
}

/// A file-keep decision over its filled partition values, evaluated after key declaration and
/// before any footer read; `Send + Sync` because the seam holds a reference to it across `.await`
/// inside the `Send` futures its callers return.
pub type PartitionKeepPredicate = dyn Fn(&BTreeMap<String, Option<String>>) -> bool + Send + Sync;

/// One Parquet data file under the prefix.
pub struct ParquetFile {
    pub path: StorePath,
    /// Carried from the listing response, so no consumer issues an object-store HEAD for it.
    pub size: u64,
    /// Every key `hive_partitioning` declares for this table, filled from this file's own
    /// `key=value` path segments where present and `None` where the file's path lacks the segment
    /// or the segment decodes to `__HIVE_DEFAULT_PARTITION__`/empty; empty when `hive_partitioning`
    /// is `false`.
    pub partition_values: BTreeMap<String, Option<String>>,
    /// Present iff this file's footer was read under the merge mode — check presence, not
    /// position, since [`MergeMode::SampleOneFile`] leaves most files' footers unset.
    pub footer: Option<Arc<ParquetMetaData>>,
}

/// The data files under one prefix, their declared partition columns, and the one schema their
/// footers fold to (folded columns followed by the partition columns).
pub struct ParquetDirectory {
    pub files: Vec<ParquetFile>,
    /// Every column NULLABLE, named and ordered exactly as the files declare them, followed by the
    /// partition columns.
    pub schema: SchemaRef,
    /// The declared partition-key names, in the order appended to `schema`; empty when
    /// `hive_partitioning` is `false` or no file in the merge mode's declaration scope (every
    /// file, or the first file under `SampleOneFile`) carries a partition segment.
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
/// `keep` narrows the files this call returns to those it accepts, evaluated against each file's
/// partition values after key declaration and before any footer read; the partition keys are
/// declared from the UNFILTERED listing and never depend on `keep`, while the fold-every-file
/// mode reads only kept files' footers, so a rejected file costs no footer read. The
/// sample-one-file mode still reads the footer of the first file in the UNFILTERED listing, kept
/// or not, so the enumeration and plan paths sample the same file regardless of which predicate
/// either one supplies.
pub async fn resolve_parquet_directory(
    store: &Arc<dyn ObjectStore>,
    prefix: &StorePath,
    options: DirectoryOptions,
    keep: &PartitionKeepPredicate,
) -> Result<ParquetDirectory, UdfError> {
    let raw_files = list_data_files(store, prefix, options.hive_partitioning).await?;
    let declaring_files = declaration_scope(&raw_files, options.merge_mode);
    check_key_spelling_collisions(declaring_files)?;
    let declared_keys = union_of_partition_keys(declaring_files);
    let keys_any_file_carries = union_of_partition_keys(&raw_files);
    let kept = kept_files(&raw_files, &declared_keys, keep);
    let fold_sources = fold_sources_for(options.merge_mode, &raw_files, &kept);

    // Concurrency is bounded by the caller's store's admission limiter, not a second one here.
    let read: Vec<ArrowReaderMetadata> = try_join_all(
        fold_sources
            .iter()
            .map(|source| read_footer(Arc::clone(store), source.path.clone(), source.size)),
    )
    .await?;

    let dropped = validate_key_column_overrides(&fold_sources, &read, &keys_any_file_carries)?;
    let folded_fields = fold_schemas(&fold_sources, &read, &dropped)?;
    Ok(ParquetDirectory {
        files: attach_footers(kept, &fold_sources, &read),
        schema: schema_with_partition_columns(folded_fields, &declared_keys),
        partition_columns: declared_keys,
    })
}

/// A listed data file before partition-key declaration narrows and fills its map; kept separate
/// from [`ParquetFile`] so a file's OWN raw carried-keys set survives past the fill step for the
/// collision checks in [`validate_key_column_overrides`], which need it undiluted.
struct RawFile {
    path: StorePath,
    size: u64,
    partition_segments: Vec<(String, Option<String>)>,
}

fn raw_key_set(raw: &RawFile) -> HashSet<String> {
    raw.partition_segments
        .iter()
        .map(|(key, _)| key.clone())
        .collect()
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
                parse_partition_segments(&segments)
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

/// A directory segment declares a partition key iff it matches `^[^/=]+=[^/]*$`; the file's own
/// name (the last segment) is never a candidate. The value is percent-decoded, keeping the raw
/// text when it does not decode to valid UTF-8; `__HIVE_DEFAULT_PARTITION__` and an empty value
/// both decode to `None`. A key repeated within one path takes its deepest value, keeping the
/// position of its first occurrence so cross-file union ordering stays shallow-to-deep.
fn parse_partition_segments(segments: &[String]) -> Vec<(String, Option<String>)> {
    let Some((_, directories)) = segments.split_last() else {
        return Vec::new();
    };
    let mut ordered: Vec<(String, Option<String>)> = Vec::new();
    let mut index_of: HashMap<String, usize> = HashMap::new();
    for segment in directories {
        let Some((key, value)) = segment.split_once('=') else {
            continue;
        };
        if key.is_empty() {
            continue;
        }
        let decoded = decode_partition_value(value);
        match index_of.get(key) {
            Some(&index) => ordered[index].1 = decoded,
            None => {
                index_of.insert(key.to_string(), ordered.len());
                ordered.push((key.to_string(), decoded));
            }
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

/// The files whose paths declare the partition keys: every file under
/// [`MergeMode::FoldEveryFile`], or the first file alone under [`MergeMode::SampleOneFile`] —
/// always a slice of the UNFILTERED listing, so the declared columns never depend on a keep
/// predicate.
fn declaration_scope(raw_files: &[RawFile], mode: MergeMode) -> &[RawFile] {
    match mode {
        MergeMode::FoldEveryFile => raw_files,
        MergeMode::SampleOneFile => &raw_files[..raw_files.len().min(1)],
    }
}

fn union_of_partition_keys(raw_files: &[RawFile]) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut keys = Vec::new();
    for raw in raw_files {
        for (key, _) in &raw.partition_segments {
            if seen.insert(key.clone()) {
                keys.push(key.clone());
            }
        }
    }
    keys
}

/// Every declared key, filled from `raw`'s own value where present and `None` where `raw` carries
/// no such key — so a consumer never checks for a missing key, and an undeclared key `raw` happens
/// to carry is dropped.
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

fn kept_files<'a>(
    raw_files: &'a [RawFile],
    declared_keys: &[String],
    keep: &PartitionKeepPredicate,
) -> Vec<(&'a RawFile, ParquetFile)> {
    raw_files
        .iter()
        .filter_map(|raw| {
            let filled = fill_partition_values(&raw.partition_segments, declared_keys);
            keep(&filled).then(|| {
                let file = ParquetFile {
                    path: raw.path.clone(),
                    size: raw.size,
                    partition_values: filled,
                    footer: None,
                };
                (raw, file)
            })
        })
        .collect()
}

/// A file whose footer the fold reads; `kept_index` is its position among the returned files, or
/// `None` for a sampled file `keep` rejected.
struct FoldSource {
    path: StorePath,
    size: u64,
    raw_keys: HashSet<String>,
    kept_index: Option<usize>,
}

impl FoldSource {
    fn of(raw: &RawFile, kept_index: Option<usize>) -> Self {
        Self {
            path: raw.path.clone(),
            size: raw.size,
            raw_keys: raw_key_set(raw),
            kept_index,
        }
    }
}

fn fold_sources_for(
    mode: MergeMode,
    raw_files: &[RawFile],
    kept: &[(&RawFile, ParquetFile)],
) -> Vec<FoldSource> {
    match mode {
        MergeMode::FoldEveryFile => kept
            .iter()
            .enumerate()
            .map(|(index, (raw, _))| FoldSource::of(raw, Some(index)))
            .collect(),
        MergeMode::SampleOneFile => raw_files
            .first()
            .map(|sampled| {
                let sampled_was_kept = kept
                    .first()
                    .is_some_and(|(first_kept, _)| first_kept.path == sampled.path);
                FoldSource::of(sampled, sampled_was_kept.then_some(0))
            })
            .into_iter()
            .collect(),
    }
}

fn attach_footers(
    kept: Vec<(&RawFile, ParquetFile)>,
    fold_sources: &[FoldSource],
    read: &[ArrowReaderMetadata],
) -> Vec<ParquetFile> {
    let mut files: Vec<ParquetFile> = kept.into_iter().map(|(_, file)| file).collect();
    for (source, footer) in fold_sources.iter().zip(read) {
        if let Some(index) = source.kept_index {
            files[index].footer = Some(Arc::clone(footer.metadata()));
        }
    }
    files
}

fn check_key_spelling_collisions(scope: &[RawFile]) -> Result<(), UdfError> {
    let mut declared: HashMap<String, (&str, &StorePath)> = HashMap::new();
    for raw in scope {
        for (key, _) in &raw.partition_segments {
            let folded = key.to_uppercase();
            match declared.get(&folded) {
                Some(&(existing_spelling, existing_path)) if existing_spelling != key => {
                    return Err(key_spelling_collision(
                        existing_spelling,
                        existing_path,
                        key,
                        &raw.path,
                    ));
                }
                Some(_) => {}
                None => {
                    declared.insert(folded, (key, &raw.path));
                }
            }
        }
    }
    Ok(())
}

fn key_spelling_collision(
    first: &str,
    first_path: &StorePath,
    second: &str,
    second_path: &StorePath,
) -> UdfError {
    UdfError::User(format!(
        "partition keys '{first}' (from '{first_path}') and '{second}' (from '{second_path}') are \
         the same name once uppercased, so the declaration would advertise a duplicate partition \
         column. Neither directory spelling is preferred over the other: rename one of them."
    ))
}

/// A folded Parquet column whose uppercase fold equals a `candidates` key is dropped from the fold
/// (the key's own nullable partition column is the only column of that name) when EVERY file whose
/// footer was read and that carries the stored column also carries the key's segment in its own
/// raw carried-keys set; a qualifying file that carries the column but not the segment fails the
/// whole resolution instead, since it has neither a directory value nor permission to fall back to
/// its own stored value. `candidates` holds every listed file's keys whatever the merge mode,
/// because a key only an unsampled file carries still collides with a column the sampled footer
/// stores.
fn validate_key_column_overrides(
    fold_sources: &[FoldSource],
    read: &[ArrowReaderMetadata],
    candidates: &[String],
) -> Result<HashSet<String>, UdfError> {
    let mut dropped = HashSet::new();
    for key in candidates {
        let folded_key = key.to_uppercase();
        for (source, metadata) in fold_sources.iter().zip(read) {
            let Some(field) = metadata
                .schema()
                .fields()
                .iter()
                .find(|field| field.name().to_uppercase() == folded_key)
            else {
                continue;
            };
            let carries_segment = source
                .raw_keys
                .iter()
                .any(|k| k.to_uppercase() == folded_key);
            if !carries_segment {
                return Err(missing_segment_error(field.name(), key, &source.path));
            }
            dropped.insert(folded_key.clone());
        }
    }
    Ok(dropped)
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
    path: StorePath,
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

fn fold_schemas(
    fold_sources: &[FoldSource],
    read: &[ArrowReaderMetadata],
    dropped: &HashSet<String>,
) -> Result<Vec<Field>, UdfError> {
    let mut columns: Vec<FoldedColumn> = Vec::new();
    let mut by_uppercase: HashMap<String, usize> = HashMap::new();

    for (source, metadata) in fold_sources.iter().zip(read) {
        let path = &source.path;
        for field in metadata.schema().fields() {
            let folded = field.name().to_uppercase();
            if dropped.contains(&folded) {
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
