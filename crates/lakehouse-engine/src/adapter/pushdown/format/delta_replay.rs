//! Delta log replay: the data files active at a table's current version.
//!
//! The object store is injected so the credential decision stays with the reader that
//! made it. Replayed rows are read in `delta_kernel`'s documented scan-row schema
//! because the kernel's `ScanFile` view drops NULL partition values and hides the
//! verbatim deletion-vector descriptor.
//!
//! Kernel contract relied on:
//! - `without_row_transforms()`: the scan side rebuilds partition columns and applies
//!   deletion vectors itself. Default `StatsOptions` keep the kernel's own skipping and
//!   partition pruning; no statistic reaches `FileEntry`.
//! - A selection vector shorter than the batch leaves its remaining rows selected.
//! - `path` is NULL on a row carrying no `add` action.
//! - Replay is newest-first, so the first row per path is its latest `add`.
//! - Deletion-vector presence is keyed on `storageType`, not the struct null mask, which
//!   can be incomplete when read from checkpoint parquet (mirrors the kernel's visitor).
//! - Partition-value offsets are read totally: a panic aborts the VM and the engine
//!   SIGKILLs every sibling VM of the statement part.
//! - A logged NULL partition value stays absent; the directory literal is a naming
//!   artifact, never the value.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, AsArray, MapArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Int32Type, Int64Type};
use delta_kernel::PredicateRef;
use delta_kernel::Snapshot;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use exasol_udf_sdk::error::UdfError;
use object_store::ObjectStore;

use super::delta_protocol::ensure_readable;
use crate::scan::spec::{DeleteMechanism, DeltaDeletionVectorStorage, FileEntry};

#[cfg(test)]
#[path = "delta_replay_tests.rs"]
mod tests;

/// Holds the engine next to the snapshot so a query reads the log once. Every method
/// blocks: the kernel read path is synchronous and drives its own runtime.
pub(super) struct DeltaSnapshot {
    engine: DefaultEngine<TokioBackgroundExecutor>,
    snapshot: SnapshotRef,
}

impl DeltaSnapshot {
    /// The protocol gate runs here, before anything reads the snapshot, so a `DeltaSnapshot`'s
    /// existence proves its protocol was checked.
    pub(super) fn open(store: Arc<dyn ObjectStore>, table_root: &str) -> Result<Self, UdfError> {
        let engine = DefaultEngine::builder(store).build();
        let snapshot = Snapshot::builder_for(table_root)
            .build(&engine)
            .map_err(|cause| {
                UdfError::User(format!(
                    "failed to resolve the current Delta version for table root \
                     '{table_root}': {cause}"
                ))
            })?;
        let protocol = snapshot.table_configuration().protocol();
        ensure_readable(protocol.min_reader_version(), protocol.reader_features()).map_err(
            |refusal| {
                UdfError::User(format!(
                    "cannot read Delta table root '{table_root}': {refusal}"
                ))
            },
        )?;
        Ok(Self { engine, snapshot })
    }

    pub(super) fn schema(&self) -> SchemaRef {
        self.snapshot.schema()
    }

    /// In metadata declaration order (neither schema nor sorted order). Read via the kernel's
    /// `internal-api` because 0.26 exposes `partitionColumns` nowhere else; re-reading the log
    /// ourselves would duplicate the kernel's current-metadata and checkpoint resolution.
    pub(super) fn partition_columns(&self) -> Vec<String> {
        self.snapshot
            .table_configuration()
            .partition_columns()
            .to_vec()
    }

    /// The mode in force, not the raw `delta.columnMapping.mode` property: the protocol says
    /// to ignore it unless the `columnMapping` reader feature is supported, and the kernel's
    /// `table_properties()` reports it ungated.
    pub(super) fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.snapshot.table_configuration().column_mapping_mode()
    }

    /// One entry per active path, no statistics, ordered by path so the list depends on log
    /// content rather than replay internals. `prune = None` returns every active file.
    pub(super) fn active_files(
        &self,
        prune: Option<PredicateRef>,
    ) -> Result<Vec<FileEntry>, UdfError> {
        let scan = self
            .snapshot
            .clone()
            .scan_builder()
            .with_predicate(prune)
            .without_row_transforms()
            .build()
            .map_err(|cause| self.failed_to("plan the Delta scan", &cause))?;

        let mut active = Vec::new();
        let mut listed_paths = HashSet::new();
        for replayed in scan
            .scan_metadata(&self.engine)
            .map_err(|cause| self.failed_to("replay the Delta log", &cause))?
        {
            let (data, selected) = replayed
                .map_err(|cause| self.failed_to("replay the Delta log", &cause))?
                .scan_files
                .into_parts();
            let batch = ArrowEngineData::try_from_engine_data(data)
                .map_err(|cause| self.failed_to("read the replayed Delta log", &cause))?;
            append_active_files(
                batch.record_batch(),
                &selected,
                &mut listed_paths,
                &mut active,
            )?;
        }
        active.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        Ok(active)
    }

    fn failed_to(&self, attempted: &str, cause: &delta_kernel::Error) -> UdfError {
        UdfError::User(format!(
            "failed to {attempted} for Delta table root '{}': {cause}",
            self.snapshot.table_root()
        ))
    }
}

fn append_active_files(
    batch: &RecordBatch,
    selected: &[bool],
    listed_paths: &mut HashSet<String>,
    active: &mut Vec<FileEntry>,
) -> Result<(), UdfError> {
    let paths = scan_row_column(batch, "path")?;
    let sizes = scan_row_column(batch, "size")?;
    let deletion_vectors = struct_column(batch, "deletionVector")?;
    let partition_values = partition_values_column(batch)?;

    for row in 0..batch.num_rows() {
        if !selected.get(row).copied().unwrap_or(true) {
            continue;
        }
        let Some(path) = string_at(paths, row, "path")? else {
            continue;
        };
        if !listed_paths.insert(path.clone()) {
            continue;
        }
        let size = size_at(sizes, row, &path)?;
        let partition_values = partition_values_at(partition_values, row, &path)?;
        let deletes = deletion_vector_at(deletion_vectors, row, &path)?
            .into_iter()
            .collect();
        active.push(FileEntry {
            path,
            size,
            deletes,
            partition_values,
        });
    }
    Ok(())
}

fn deletion_vector_at(
    deletion_vectors: Option<&StructArray>,
    row: usize,
    path: &str,
) -> Result<Option<DeleteMechanism>, UdfError> {
    let Some(deletion_vectors) = deletion_vectors else {
        return Ok(None);
    };
    let kind = child_column(deletion_vectors, "deletionVector", "storageType")?;
    let Some(kind) = string_at(kind, row, "deletionVector.storageType")? else {
        return Ok(None);
    };
    let storage = match kind.as_str() {
        "u" => DeltaDeletionVectorStorage::UuidRelative,
        "i" => DeltaDeletionVectorStorage::Inline,
        "p" => DeltaDeletionVectorStorage::AbsolutePath,
        _ => {
            return Err(UdfError::User(format!(
                "the `add` action for Delta data file '{path}' carries a deletion vector whose \
                 storage kind '{kind}' is none of the Delta protocol's 'u', 'i' and 'p'"
            )));
        }
    };
    let path_or_inline_dv = string_at(
        child_column(deletion_vectors, "deletionVector", "pathOrInlineDv")?,
        row,
        "deletionVector.pathOrInlineDv",
    )?
    .ok_or_else(|| incomplete_deletion_vector(path, "pathOrInlineDv"))?;
    let offset = i32_at(
        child_column(deletion_vectors, "deletionVector", "offset")?,
        row,
        "deletionVector.offset",
    )?;
    let size_in_bytes = i32_at(
        child_column(deletion_vectors, "deletionVector", "sizeInBytes")?,
        row,
        "deletionVector.sizeInBytes",
    )?
    .ok_or_else(|| incomplete_deletion_vector(path, "sizeInBytes"))?;
    let cardinality = i64_at(
        child_column(deletion_vectors, "deletionVector", "cardinality")?,
        row,
        "deletionVector.cardinality",
    )?
    .ok_or_else(|| incomplete_deletion_vector(path, "cardinality"))?;

    Ok(Some(DeleteMechanism::DeltaDeletionVector {
        storage,
        path_or_inline_dv,
        offset,
        size_in_bytes,
        cardinality,
    }))
}

fn partition_values_at(
    partition_values: Option<&MapArray>,
    row: usize,
    path: &str,
) -> Result<BTreeMap<String, Option<String>>, UdfError> {
    let mut carried = BTreeMap::new();
    let Some(partition_values) = partition_values else {
        return Ok(carried);
    };
    if partition_values.is_null(row) {
        return Ok(carried);
    }
    let offsets = partition_values.value_offsets();
    let (Some(&first), Some(&last)) = (offsets.get(row), offsets.get(row + 1)) else {
        return Err(UdfError::User(format!(
            "the replayed Delta log carries partition values for {} rows, short of the row \
             holding data file '{path}'",
            offsets.len().saturating_sub(1)
        )));
    };
    let columns = partition_values.keys().as_ref();
    let logged = partition_values.values().as_ref();
    for entry in first as usize..last as usize {
        let column = string_at(columns, entry, "partitionValues key")?.ok_or_else(|| {
            UdfError::User(format!(
                "the `add` action for Delta data file '{path}' logs a partition value under a \
                 NULL partition-column name"
            ))
        })?;
        carried.insert(column, string_at(logged, entry, "partitionValues value")?);
    }
    Ok(carried)
}

fn size_at(sizes: &dyn Array, row: usize, path: &str) -> Result<u64, UdfError> {
    let size = i64_at(sizes, row, "size")?.ok_or_else(|| {
        UdfError::User(format!(
            "the `add` action for Delta data file '{path}' logs no size"
        ))
    })?;
    u64::try_from(size).map_err(|_| {
        UdfError::User(format!(
            "the `add` action for Delta data file '{path}' logs a negative size {size}"
        ))
    })
}

fn scan_row_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, UdfError> {
    batch
        .column_by_name(name)
        .map(AsRef::as_ref)
        .ok_or_else(|| missing_scan_row_column(name))
}

fn struct_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a StructArray>, UdfError> {
    let column = scan_row_column(batch, name)?;
    if column.data_type() == &DataType::Null {
        return Ok(None);
    }
    column
        .as_struct_opt()
        .map(Some)
        .ok_or_else(|| unreadable_scan_row_column(name, column.data_type()))
}

fn partition_values_column(batch: &RecordBatch) -> Result<Option<&MapArray>, UdfError> {
    let Some(constants) = struct_column(batch, "fileConstantValues")? else {
        return Ok(None);
    };
    let column = child_column(constants, "fileConstantValues", "partitionValues")?;
    if column.data_type() == &DataType::Null {
        return Ok(None);
    }
    column
        .as_map_opt()
        .map(Some)
        .ok_or_else(|| unreadable_scan_row_column("partitionValues", column.data_type()))
}

fn child_column<'a>(
    parent: &'a StructArray,
    parent_name: &str,
    name: &str,
) -> Result<&'a dyn Array, UdfError> {
    parent
        .column_by_name(name)
        .map(AsRef::as_ref)
        .ok_or_else(|| missing_scan_row_column(&format!("{parent_name}.{name}")))
}

fn string_at(column: &dyn Array, row: usize, name: &str) -> Result<Option<String>, UdfError> {
    if column.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = column.as_string_opt::<i32>() {
        return Ok(Some(values.value(row).to_string()));
    }
    if let Some(values) = column.as_string_view_opt() {
        return Ok(Some(values.value(row).to_string()));
    }
    if let Some(values) = column.as_string_opt::<i64>() {
        return Ok(Some(values.value(row).to_string()));
    }
    Err(unreadable_scan_row_column(name, column.data_type()))
}

fn i32_at(column: &dyn Array, row: usize, name: &str) -> Result<Option<i32>, UdfError> {
    if column.is_null(row) {
        return Ok(None);
    }
    column
        .as_primitive_opt::<Int32Type>()
        .map(|values| Some(values.value(row)))
        .ok_or_else(|| unreadable_scan_row_column(name, column.data_type()))
}

fn i64_at(column: &dyn Array, row: usize, name: &str) -> Result<Option<i64>, UdfError> {
    if column.is_null(row) {
        return Ok(None);
    }
    column
        .as_primitive_opt::<Int64Type>()
        .map(|values| Some(values.value(row)))
        .ok_or_else(|| unreadable_scan_row_column(name, column.data_type()))
}

fn incomplete_deletion_vector(path: &str, field: &str) -> UdfError {
    UdfError::User(format!(
        "the `add` action for Delta data file '{path}' carries a deletion vector with no \
         `{field}`, which the Delta protocol requires"
    ))
}

fn missing_scan_row_column(name: &str) -> UdfError {
    UdfError::User(format!(
        "the replayed Delta log carries no '{name}' column, so it is not the scan-row schema \
         delta_kernel documents"
    ))
}

fn unreadable_scan_row_column(name: &str, found: &DataType) -> UdfError {
    UdfError::User(format!(
        "the replayed Delta log carries column '{name}' as {found}, which is not the type \
         delta_kernel's scan-row schema documents"
    ))
}
