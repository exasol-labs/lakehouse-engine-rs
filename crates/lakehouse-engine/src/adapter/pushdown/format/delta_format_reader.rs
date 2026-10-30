use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use delta_kernel::schema::{ColumnMetadataKey, MetadataValue, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::delta_predicate::to_delta_predicate;
use super::delta_replay::DeltaSnapshot;
use super::unity_table_storage::{UnityTableStorage, redacted};
use super::{FormatReader, PlannedFiles, hex_binary_partition_values};
use crate::adapter::column_notes::DeclaredSchema;
use crate::scan::build_table_root_store;
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField};

#[cfg(test)]
#[path = "delta_format_reader_tests.rs"]
mod tests;

/// Plans the declaration it receives; the log supplies only the gate, the active files, and their
/// deletion vectors and partition values. The effective backend leaves with the planned files:
/// the scan must read files through the same backend the log was read through.
pub(super) struct DeltaFormatReader<'a> {
    pub(super) storage: UnityTableStorage<'a>,
}

impl FormatReader for DeltaFormatReader<'_> {
    /// `name_mapping` is always empty: a physical-name binding lives on its own
    /// [`LogicalField`], which the scan side consults before any table-level mapping, so a
    /// mapping here would be unreachable and a second home for the same decision.
    fn resolve_scan<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<PlannedFiles, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let (table_root, effective_storage) = self.storage.resolve().await?;
            // This layer made the credential decision, so it knows which secrets an object-store
            // error could echo; replay and schema know none.
            let secrets = effective_storage.secret_values();
            let redact = |error: UdfError| redacted(error, &secrets);
            let store = build_table_root_store(
                &effective_storage,
                table_root,
                DEFAULT_S3_MAX_CONNECTIONS,
                &secrets,
            )
            .map_err(redact)?;
            // Blocks: `delta_kernel`'s read path is synchronous and drives its own runtime.
            let snapshot = DeltaSnapshot::open(store, table_root).map_err(redact)?;
            let files = declared_files(&snapshot, declared, filter_json).map_err(redact)?;
            Ok(PlannedFiles {
                files,
                effective_storage,
                table_root: table_root.to_string(),
                name_mapping: Vec::new(),
            })
        })
    }
}

/// Filter columns and partition values resolve through each column's recorded binding key, as
/// PROTOCOL.md § Column Mapping resolves "partition values and column level statistics".
fn declared_files(
    snapshot: &DeltaSnapshot,
    declared: &DeclaredSchema,
    filter_json: Option<&Json>,
) -> Result<Vec<FileEntry>, UdfError> {
    let schema = snapshot.schema();
    ensure_binding_keys_follow_mode(declared, snapshot.column_mapping_mode())?;
    let partition_fields = checked_partition_fields(&schema, snapshot, declared)?;
    ensure_no_relaxed_nullability(&schema, declared)?;
    let columns: Vec<(&str, &StructField)> = declared
        .logical_schema
        .iter()
        .filter_map(|field| Some((field.name.as_str(), snapshot_field(&schema, field)?)))
        .collect();
    let prune = filter_json
        .and_then(|filter| to_delta_predicate(filter, &columns))
        .map(Arc::new);
    let mapped = snapshot.column_mapping_mode() != ColumnMappingMode::None;
    let logged_keys: Vec<(&String, &String)> = declared
        .partition_columns
        .iter()
        .zip(partition_fields)
        .map(|(name, field)| {
            let physical_name = physical_name_of(field).filter(|_| mapped);
            (name, physical_name.unwrap_or(field.name()))
        })
        .collect();

    let mut files: Vec<FileEntry> = snapshot
        .active_files(prune)?
        .into_iter()
        .map(|mut file| {
            let mut logged = std::mem::take(&mut file.partition_values);
            file.partition_values = logged_keys
                .iter()
                .filter_map(|(name, key)| Some(((*name).clone(), logged.remove(*key)?)))
                .collect();
            file
        })
        .collect();
    hex_binary_partition_values(&mut files, &declared.logical_schema);
    Ok(files)
}

/// Refuses rather than derives: a declaration binding by the wrong key would bind the scan to the
/// wrong file columns after a rename.
fn ensure_binding_keys_follow_mode(
    declared: &DeclaredSchema,
    mode: ColumnMappingMode,
) -> Result<(), UdfError> {
    let mode_key = (
        mode == ColumnMappingMode::Id,
        mode == ColumnMappingMode::Name,
    );
    let key = |field: &&LogicalField| (field.field_id.is_some(), field.physical_name.is_some());
    match declared
        .logical_schema
        .iter()
        .find(|field| key(field) != mode_key)
    {
        Some(field) => Err(disagreement(
            &declared.table,
            format!(
                "its column-mapping mode is '{}', but the declaration of column '{}' does not \
                 carry exactly the binding key that mode selects",
                format!("{mode:?}").to_lowercase(),
                field.name
            ),
        )),
        None => Ok(()),
    }
}

/// The snapshot fields of the declared partition columns, in the log's order. A mismatch would bind
/// partition values to the wrong columns.
fn checked_partition_fields<'s>(
    schema: &'s StructType,
    snapshot: &DeltaSnapshot,
    declared: &DeclaredSchema,
) -> Result<Vec<&'s StructField>, UdfError> {
    let log_partitions = snapshot.partition_columns();
    let partition_fields: Vec<Option<&StructField>> = declared
        .partition_columns
        .iter()
        .map(|name| {
            snapshot_field(
                schema,
                declared
                    .logical_schema
                    .iter()
                    .find(|field| field.name == *name)?,
            )
        })
        .collect();
    let partitioned_by = partition_fields
        .iter()
        .map(|field| field.map(StructField::name));
    if !partitioned_by.eq(log_partitions.iter().map(Some)) {
        return Err(disagreement(
            &declared.table,
            format!(
                "it partitions by {log_partitions:?}, but the declaration partitions by {:?}",
                declared.partition_columns
            ),
        ));
    }
    Ok(partition_fields.into_iter().flatten().collect())
}

/// DataFusion folds `c IS NULL` to `false` on a non-nullable field, so a column the log allows
/// NULL in cannot be declared required.
fn ensure_no_relaxed_nullability(
    schema: &StructType,
    declared: &DeclaredSchema,
) -> Result<(), UdfError> {
    let relaxed = |field: &&LogicalField| {
        !field.nullable && snapshot_field(schema, field).is_some_and(StructField::is_nullable)
    };
    match declared.logical_schema.iter().find(relaxed) {
        Some(field) => Err(UdfError::User(format!(
            "column '{}' of virtual table '{}' is declared non-nullable, but the Delta log \
             allows NULL in it; ALTER VIRTUAL SCHEMA ... REFRESH re-declares it",
            field.name.to_uppercase(),
            declared.table
        ))),
        None => Ok(()),
    }
}

fn disagreement(virtual_table: &str, part: String) -> UdfError {
    UdfError::User(format!(
        "the declaration of virtual table '{virtual_table}' disagrees with its Delta log: {part}; \
         give the Unity Catalog entry the Delta schema's column metadata in type_json and the \
         table's properties, then run ALTER VIRTUAL SCHEMA ... REFRESH"
    ))
}

/// The snapshot field a declared field names: by its recorded physical name or field id, or by
/// its name when it records neither, because without column mapping the name is the identity.
fn snapshot_field<'s>(schema: &'s StructType, declared: &LogicalField) -> Option<&'s StructField> {
    schema
        .fields()
        .find(|field| match (&declared.physical_name, declared.field_id) {
            (Some(physical_name), _) => physical_name_of(field) == Some(physical_name),
            (None, Some(id)) => field.column_mapping_id() == Some(i64::from(id)),
            (None, None) => field.name().eq_ignore_ascii_case(&declared.name),
        })
}

fn physical_name_of(field: &StructField) -> Option<&String> {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(physical_name)) => Some(physical_name),
        _ => None,
    }
}
