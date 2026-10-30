use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::ArrayRef;
use datafusion::logical_expr::ColumnarValue;
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::spec::TableMetadata;
use iceberg::{NamespaceIdent, TableIdent};
use lakehouse_catalog::{
    CatalogProps, CatalogSession, CatalogTable, StaticStoreAddress, StorageBackend, error_chain,
    load_table_any_auth, parse_table_ident, read_iceberg_metadata_file, redact_credentials,
    redact_error_text, resolve_vended_storage,
};
use serde_json::Value as Json;

use super::{ConnectionStorage, FormatReader, PlannedFiles};
use crate::adapter::column_notes::{
    ColumnDeclaration, DeclaredSchema, column_refusal, unread_type_cause, variant_cause,
};
use crate::adapter::iceberg_predicate::to_iceberg_predicate;
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{
    DeleteMechanism, FileEntry, LogicalField, NameMappingEntry, NestedField, NestedMembers,
    encode_location,
};

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;

/// Current metadata serves only snapshot, files, mapping, and the checks of the declaration.
pub(super) struct IcebergFormatReader<'a> {
    pub(super) metadata: IcebergMetadataSource<'a>,
    pub(super) connection: ConnectionStorage<'a>,
}

/// Where an Iceberg table's current metadata comes from. Every source's metadata passes the same
/// checks and the same planner, so every Iceberg reader rule applies to every source.
pub(super) enum IcebergMetadataSource<'a> {
    /// An Iceberg REST `loadTable`, whose response may also vend the table's credential.
    RestLoadTable {
        session: &'a CatalogSession,
        catalog_props: &'a CatalogProps,
    },
    /// A metastore's pointer to the current `metadata.json` (Iceberg § Metastore Tables), read
    /// once through the CONNECTION's static storage.
    MetadataFile { table: &'a CatalogTable },
}

/// A table's identity and its metadata once it passed the pre-storage checks, with the storage
/// its files are read through.
struct CheckedMetadata {
    table_name: String,
    table_ident: TableIdent,
    metadata: TableMetadata,
    metadata_location: Option<String>,
    table_root: String,
    effective_storage: StorageBackend,
}

impl FormatReader for IcebergFormatReader<'_> {
    fn resolve_scan<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<PlannedFiles, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let checked = match self.metadata {
                IcebergMetadataSource::RestLoadTable {
                    session,
                    catalog_props,
                } => rest_metadata(session, catalog_props, &self.connection).await?,
                IcebergMetadataSource::MetadataFile { table } => {
                    file_metadata(table, self.connection.storage).await?
                }
            };
            let current = checked.metadata.current_schema();
            refuse_relaxed_nullability(current, declared)?;
            plan_checked_table(checked, &declared.logical_schema, filter_json).await
        })
    }
}

/// Vending is gated on `creds.use_vended_credentials` alone and never falls back to the static
/// credential; only the CONNECTION's addressing may override it ([`StaticStoreAddress`]).
async fn rest_metadata(
    session: &CatalogSession,
    catalog_props: &CatalogProps,
    connection: &ConnectionStorage<'_>,
) -> Result<CheckedMetadata, UdfError> {
    let ConnectionStorage {
        storage,
        creds,
        allow_http,
    } = *connection;
    let table_name = &catalog_props.table;
    let result = load_table_any_auth(session, catalog_props, creds).await?;
    // The REST URI names no object store and `warehouse` is only a routing identifier.
    let table_root = checked_table_root(&result.metadata, table_name, || {
        UdfError::User(format!(
            "the loadTable response for table '{table_name}' carries an EMPTY table \
             `location`; the catalog `warehouse` is a routing identifier, not a \
             table location, and is not a valid substitute"
        ))
    })?;
    let effective_storage = if creds.use_vended_credentials {
        resolve_vended_storage(
            &result,
            &table_root,
            allow_http,
            &StaticStoreAddress::from(creds),
        )?
    } else {
        storage.clone()
    };
    let (namespace, name) = parse_table_ident(table_name)?;
    Ok(CheckedMetadata {
        table_name: table_name.clone(),
        table_ident: TableIdent::new(namespace, name),
        metadata: result.metadata,
        metadata_location: result.metadata_location,
        table_root,
        effective_storage,
    })
}

/// A metadata file vends no credential, so the table is read through the static storage.
async fn file_metadata(
    table: &CatalogTable,
    storage: &StorageBackend,
) -> Result<CheckedMetadata, UdfError> {
    let table_name = catalog_identifier_string(&table.ident);
    let location = table
        .metadata_location
        .as_deref()
        .filter(|location| !location.trim().is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "Iceberg table '{table_name}' names no current metadata file, so its snapshot \
                 is unknown"
            ))
        })?;
    let metadata = read_iceberg_metadata_file(storage, location, &table_name).await?;
    let table_root = checked_table_root(&metadata, &table_name, || {
        UdfError::User(format!(
            "the Iceberg metadata file '{location}' of table '{table_name}' carries an EMPTY \
             table `location`; the catalog's storage location is not a valid substitute, \
             because the metadata file is the table's authority"
        ))
    })?;
    let namespace = NamespaceIdent::from_vec(table.ident.namespace.clone())
        .map_err(|error| UdfError::User(format!("invalid namespace in '{table_name}': {error}")))?;
    Ok(CheckedMetadata {
        table_ident: TableIdent::new(namespace, table.ident.name.clone()),
        metadata,
        metadata_location: Some(location.to_string()),
        table_root,
        effective_storage: storage.clone(),
        table_name,
    })
}

/// Every source's metadata passes these, in this order, before any storage decision. The anchor
/// is the table's own location: what vended `prefix`es match against.
fn checked_table_root(
    metadata: &TableMetadata,
    table_name: &str,
    empty_location: impl FnOnce() -> UdfError,
) -> Result<String, UdfError> {
    // Decided from schema history alone, before any manifest is read, so filtered and
    // unfiltered requests are refused identically.
    refuse_date_promotion(metadata, table_name)?;
    let location = metadata.location();
    if location.is_empty() {
        return Err(empty_location());
    }
    Ok(location.to_string())
}

/// DataFusion folds `c IS NULL` to `false` on a non-nullable field, so a declaration older than a
/// source-side relaxation to optional would drop NULL rows silently. Matched by declared field id.
fn refuse_relaxed_nullability(
    current: &iceberg::spec::Schema,
    declared: &DeclaredSchema,
) -> Result<(), UdfError> {
    let optional_now = |id| current.field_by_id(id).is_some_and(|now| !now.required);
    let stale = |field: &&LogicalField| !field.nullable && field.field_id.is_some_and(optional_now);
    let Some(field) = declared.logical_schema.iter().find(stale) else {
        return Ok(());
    };
    Err(UdfError::User(format!(
        "column '{}' of virtual table '{}' is declared non-nullable, but the current Iceberg \
         schema allows NULL in it; ALTER VIRTUAL SCHEMA ... REFRESH re-declares it",
        field.name.to_uppercase(),
        declared.table
    )))
}

/// The one Iceberg planner. Errors are redacted against the effective storage's secrets, since
/// its `file_io` is what talks to object storage.
async fn plan_checked_table(
    checked: CheckedMetadata,
    declared: &[LogicalField],
    filter_json: Option<&Json>,
) -> Result<PlannedFiles, UdfError> {
    let CheckedMetadata {
        table_name,
        table_ident,
        metadata,
        metadata_location,
        table_root,
        effective_storage,
    } = checked;
    let secrets = effective_storage.secret_values();

    let file_io = effective_storage.file_io();
    let runtime = iceberg::Runtime::try_current().map_err(|e| {
        UdfError::User(format!(
            "failed to build Iceberg table: {}",
            redact_error_text(&e.to_string(), &secrets)
        ))
    })?;
    let table_builder = iceberg::table::Table::builder()
        .identifier(table_ident)
        .file_io(file_io)
        .runtime(runtime)
        .metadata(metadata);
    let table = if let Some(loc) = metadata_location {
        table_builder.metadata_location(loc).build()
    } else {
        table_builder.build()
    }
    .map_err(|e| {
        UdfError::User(format!(
            "failed to build Iceberg table: {}",
            redact_error_text(&e.to_string(), &secrets)
        ))
    })?;

    // Absent ⇒ empty; malformed ⇒ plan-time error.
    let name_mapping = parse_name_mapping(
        table
            .metadata()
            .properties()
            .get(iceberg::spec::TableProperties::PROPERTY_DEFAULT_NAME_MAPPING)
            .map(String::as_str),
    )?;

    // A scan without a pinned snapshot binds its filter by name against the current schema,
    // so each declared field id is named as that schema names it.
    let metadata = table.metadata();
    let scanned = metadata
        .current_snapshot()
        .map(|_| metadata.current_schema().clone());
    let predicate = filter_json
        .zip(scanned)
        .and_then(|(filter, scanned)| to_iceberg_predicate(filter, declared, &scanned));
    let tasks = plan_file_tasks(table, &table_name, predicate, &secrets).await?;
    // Delete paths are relativized later in `relativize_shards_to_root`, like data-file paths.
    let files = tasks
        .iter()
        .map(|task| {
            let mut entry = planned_file_entry(task)
                .map_err(|mechanism| unsupported_delete_error(mechanism, &table_name))?;
            entry.partition_values = identity_partition_values(task, declared, &table_name)?;
            Ok(entry)
        })
        .collect::<Result<_, UdfError>>()?;

    Ok(PlannedFiles {
        files,
        effective_storage,
        table_root,
        name_mapping,
    })
}

/// Keeps the mapping's own tree shape; an entry without a `field-id` maps nothing and is skipped
/// with its children. A malformed property is a credential-free plan-time error (`serde_json`
/// reports only a position).
fn parse_name_mapping(raw: Option<&str>) -> Result<Vec<NameMappingEntry>, UdfError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mapping: iceberg::spec::NameMapping = serde_json::from_str(raw).map_err(|e| {
        UdfError::User(format!(
            "failed to parse Iceberg '{}' table property: {e}",
            iceberg::spec::TableProperties::PROPERTY_DEFAULT_NAME_MAPPING
        ))
    })?;
    Ok(convert_mapped_fields(mapping.fields()))
}

fn convert_mapped_fields<'a>(
    fields: impl IntoIterator<Item = &'a iceberg::spec::MappedField>,
) -> Vec<NameMappingEntry> {
    fields
        .into_iter()
        .filter_map(|field| {
            Some(NameMappingEntry {
                names: field.names().to_vec(),
                field_id: field.field_id()?,
                fields: convert_mapped_fields(field.fields().iter().map(AsRef::as_ref)),
            })
        })
        .collect()
}

/// Only Parquet positional deletes and Puffin deletion vectors over Parquet data files are
/// applied; everything else must fail loud at plan time. Carries no path or secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnsupportedDeleteMechanism {
    EqualityDelete,
    OrcDataFile,
    AvroDataFile,
    OrcDeleteFile,
    AvroDeleteFile,
    NonParquetDataFile,
}

impl UnsupportedDeleteMechanism {
    fn describe(self) -> &'static str {
        match self {
            UnsupportedDeleteMechanism::EqualityDelete => "Iceberg equality deletes",
            UnsupportedDeleteMechanism::OrcDataFile => "ORC data files",
            UnsupportedDeleteMechanism::AvroDataFile => "Avro data files",
            UnsupportedDeleteMechanism::OrcDeleteFile => "ORC delete files",
            UnsupportedDeleteMechanism::AvroDeleteFile => "Avro delete files",
            UnsupportedDeleteMechanism::NonParquetDataFile => "non-Parquet data files",
        }
    }
}

fn classify_planned_file(
    content: iceberg::spec::DataContentType,
    format: iceberg::spec::DataFileFormat,
) -> Result<(), UnsupportedDeleteMechanism> {
    use UnsupportedDeleteMechanism as U;
    use iceberg::spec::DataContentType::{Data, EqualityDeletes, PositionDeletes};
    use iceberg::spec::DataFileFormat::{Avro, Orc, Parquet, Puffin};
    match content {
        Data => match format {
            Parquet => Ok(()),
            Orc => Err(U::OrcDataFile),
            Avro => Err(U::AvroDataFile),
            Puffin => Err(U::NonParquetDataFile),
        },
        PositionDeletes => match format {
            Parquet | Puffin => Ok(()),
            Orc => Err(U::OrcDeleteFile),
            Avro => Err(U::AvroDeleteFile),
        },
        EqualityDeletes => Err(U::EqualityDelete),
    }
}

/// Names only the mechanism, never a file path (which could embed a presigned credential).
fn unsupported_delete_error(mechanism: UnsupportedDeleteMechanism, table_name: &str) -> UdfError {
    let msg = format!(
        "lakehouse pushdown declined for table '{}': it uses {}, which this engine \
         cannot apply on read (only Parquet positional deletes and deletion vectors are \
         supported); \
         this is a hard error, not a native re-plan",
        table_name,
        mechanism.describe(),
    );
    UdfError::User(redact_credentials(&msg))
}

/// Refuses a recorded `date` → `timestamp`/`timestamp_ns` promotion (#355).
///
/// `iceberg` reads such bounds as 8 bytes, skipping the spec's bounds-width
/// inference, so a pre-promotion file's 4-byte bound fails manifest decoding with an
/// unhelpful error, and a second decode path `unwrap()`s. Deciding from schema history
/// alone avoids that decode and spends no object-store byte; it must run before `plan_files`
/// reads any manifest.
///
/// Any schema declaring the field id as `date` counts, at any nesting depth (manifest bounds
/// are keyed by field id). Deliberately conservative: a table whose files were all rewritten
/// after the promotion is refused too, since proving that requires reading every manifest.
fn refuse_date_promotion(
    metadata: &iceberg::spec::TableMetadata,
    table_name: &str,
) -> Result<(), UdfError> {
    use iceberg::spec::PrimitiveType;

    let current = metadata.current_schema();
    let mut fields: Vec<_> = current.field_id_to_fields().values().collect();
    fields.sort_by_key(|field| field.id);

    for field in fields {
        let Some(current_type @ (PrimitiveType::Timestamp | PrimitiveType::TimestampNs)) =
            field.field_type.as_primitive_type()
        else {
            continue;
        };
        let promoted_from_date = metadata.schemas_iter().any(|schema| {
            matches!(
                schema
                    .field_by_id(field.id)
                    .and_then(|earlier| earlier.field_type.as_primitive_type()),
                Some(PrimitiveType::Date)
            )
        });
        if !promoted_from_date {
            continue;
        }
        let column = current
            .name_by_field_id(field.id)
            .unwrap_or(field.name.as_str());
        let msg = format!(
            "lakehouse pushdown declined for table '{table_name}': column '{column}' is \
             declared Iceberg type '{current_type}' in the current schema and 'date' in an \
             earlier one, and this engine cannot read that promotion — `iceberg` \
             omits the spec's bounds-width inference for '{current_type}', so a \
             pre-promotion data file's 4-byte manifest bound fails to decode; tracked as \
             issue #355. The refusal is deliberately conservative: it fires on the recorded \
             promotion alone, even for a table whose data files have all been rewritten \
             since"
        );
        return Err(UdfError::User(redact_credentials(&msg)));
    }

    Ok(())
}

/// Plan time refuses equality deletes; that arm lets the read-time backstop refuse one that
/// slips past. `Data` maps to the same non-positional sentinel.
fn iceberg_delete_mechanism(delete: &iceberg::scan::FileScanTaskDeleteFile) -> DeleteMechanism {
    use iceberg::spec::DataContentType::{Data, EqualityDeletes, PositionDeletes};
    let path = encode_location(delete.file_path());
    let size = delete.file_size_in_bytes();
    match (delete.file_type(), delete.file_format()) {
        // iceberg-rust refuses to build a deletion-vector task missing these, so the defaults
        // never apply.
        (PositionDeletes, iceberg::spec::DataFileFormat::Puffin) => {
            DeleteMechanism::IcebergPuffinDeletionVector {
                path,
                size,
                content_offset: delete.content_offset().unwrap_or_default(),
                content_size_in_bytes: delete.content_size_in_bytes().unwrap_or_default(),
                record_count: delete.record_count().unwrap_or_default(),
            }
        }
        (PositionDeletes, _) => DeleteMechanism::IcebergPositionalDelete { path, size },
        (EqualityDeletes | Data, _) => DeleteMechanism::IcebergEqualityDelete { path, size },
    }
}

async fn plan_file_tasks(
    table: iceberg::table::Table,
    table_name: &str,
    predicate: Option<iceberg::expr::Predicate>,
    secrets: &[&str],
) -> Result<Vec<iceberg::scan::FileScanTask>, UdfError> {
    let mut scan_builder = table.scan();
    if let Some(predicate) = predicate {
        scan_builder = scan_builder.with_filter(predicate);
    }
    let scan = scan_builder.select_all().build().map_err(|e| {
        UdfError::User(format!(
            "failed to build Iceberg scan: {}",
            redact_error_text(&e.to_string(), secrets)
        ))
    })?;

    let task_stream = scan.plan_files().await.map_err(|e| {
        UdfError::User(format!(
            "failed to plan Iceberg files for '{}': {}",
            table_name,
            redact_error_text(&error_chain(&e), secrets)
        ))
    })?;

    task_stream.try_collect().await.map_err(|e| {
        UdfError::User(format!(
            "failed to collect Iceberg file tasks: {}",
            redact_error_text(&error_chain(&e), secrets)
        ))
    })
}

fn planned_file_entry(
    task: &iceberg::scan::FileScanTask,
) -> Result<FileEntry, UnsupportedDeleteMechanism> {
    classify_planned_file(
        iceberg::spec::DataContentType::Data,
        task.data_file_format(),
    )?;
    let deletes = task
        .deletes()
        .iter()
        .map(|d| {
            classify_planned_file(d.file_type(), d.file_format())?;
            Ok(iceberg_delete_mechanism(d))
        })
        .collect::<Result<_, _>>()?;
    Ok(FileEntry::with_deletes(
        encode_location(task.data_file_path()),
        task.file_size_in_bytes(),
        deletes,
    ))
}

/// Iceberg Column Projection rule (1) reads the partition struct of the spec the file was written
/// with, so each file carries that spec's `identity` values for top-level logical fields, keyed by
/// logical name; a null stays an explicit absent value. Every other transform loses the source value.
fn identity_partition_values(
    task: &iceberg::scan::FileScanTask,
    logical_fields: &[LogicalField],
    table_name: &str,
) -> Result<BTreeMap<String, Option<String>>, UdfError> {
    let (Some(spec), Some(partition)) = (task.partition_spec(), task.partition()) else {
        return Ok(BTreeMap::new());
    };
    spec.fields()
        .iter()
        .zip(partition.iter())
        .filter(|(field, _)| field.transform == iceberg::spec::Transform::Identity)
        .filter_map(|(field, value)| {
            let logical = logical_fields
                .iter()
                .find(|logical| logical.field_id == Some(field.source_id))?;
            let source_type = task
                .schema()
                .field_by_id(field.source_id)?
                .field_type
                .as_primitive_type()?;
            let rendered = value
                .map(|value| render_identity_value(value, source_type, &logical.arrow_type))
                .transpose()
                .map_err(|cause| {
                    // Dropping the value would read the file's column, or NULL, in its place.
                    UdfError::User(redact_credentials(&format!(
                        "lakehouse pushdown declined for table '{table_name}': the identity \
                         partition value of column '{}' cannot be read at the column's declared \
                         type ({cause})",
                        logical.name
                    )))
                });
            Some(rendered.map(|rendered| (logical.name.clone(), rendered)))
        })
        .collect()
}

/// A partition value's string form cannot hold arbitrary bytes, so a binary value travels as the
/// lowercase hex that `scan::partition_scalar` decodes.
fn identity_hex(
    value: &iceberg::spec::PrimitiveLiteral,
    source_type: &iceberg::spec::PrimitiveType,
) -> Option<String> {
    use iceberg::spec::{PrimitiveLiteral as L, PrimitiveType as T};
    match (value, source_type) {
        (L::Binary(bytes), T::Binary | T::Fixed(_)) => Some(hex::encode(bytes)),
        // iceberg-rust holds a uuid as the big-endian `u128` of its 16 bytes.
        (L::UInt128(uuid), T::Uuid) => Some(hex::encode(uuid.to_be_bytes())),
        _ => None,
    }
}

/// Casts as the scan's expression adapter casts a file column of another type than the declared
/// one, so a reconstructed value reads exactly as a stored one. The result is the string
/// `scan::partition_scalar` parses back at the declared type.
fn render_identity_value(
    value: &iceberg::spec::Literal,
    source_type: &iceberg::spec::PrimitiveType,
    arrow_type_tag: &str,
) -> Result<String, String> {
    let iceberg::spec::Literal::Primitive(value) = value else {
        return Err(format!(
            "the partition struct holds a non-primitive value for a {source_type} column"
        ));
    };
    if let Some(hex) = identity_hex(value, source_type) {
        return Ok(hex);
    }
    let file_value = file_column_value(value, source_type).ok_or_else(|| {
        format!("the partition struct holds a value of another type than the {source_type} column")
    })?;
    let declared = crate::types::mapping::arrow_type_from_tag(arrow_type_tag);
    let read = ColumnarValue::Array(file_value)
        .cast_to(&declared, None)
        .and_then(|read| read.into_array(1))
        .map_err(|e| e.to_string())?;
    arrow::util::display::array_value_to_string(&read, 0).map_err(|e| e.to_string())
}

/// What a Parquet column of the file's own Iceberg type reads as. The literal's variant is that
/// type, because a manifest decodes its partition structs with the schema it was written under, so
/// an `int` value of a column promoted to `long` stays an `Int`.
fn file_column_value(
    value: &iceberg::spec::PrimitiveLiteral,
    source_type: &iceberg::spec::PrimitiveType,
) -> Option<ArrayRef> {
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use iceberg::spec::{PrimitiveLiteral as L, PrimitiveType as T};
    // The zone the Parquet reader gives a column whose timestamp `isAdjustedToUTC`.
    const ADJUSTED_TO_UTC: &str = "UTC";

    let array: ArrayRef = match (value, source_type) {
        (L::Boolean(v), T::Boolean) => Arc::new(BooleanArray::from(vec![*v])),
        (L::Int(v), T::Int | T::Long) => Arc::new(Int32Array::from(vec![*v])),
        (L::Int(v), T::Date) => Arc::new(Date32Array::from(vec![*v])),
        (L::Long(v), T::Long) => Arc::new(Int64Array::from(vec![*v])),
        (L::Long(v), T::Time) => Arc::new(Time64MicrosecondArray::from(vec![*v])),
        (L::Long(v), T::Timestamp) => Arc::new(TimestampMicrosecondArray::from(vec![*v])),
        (L::Long(v), T::Timestamptz) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]).with_timezone(ADJUSTED_TO_UTC))
        }
        (L::Long(v), T::TimestampNs) => Arc::new(TimestampNanosecondArray::from(vec![*v])),
        (L::Long(v), T::TimestamptzNs) => {
            Arc::new(TimestampNanosecondArray::from(vec![*v]).with_timezone(ADJUSTED_TO_UTC))
        }
        (L::Float(v), T::Float | T::Double) => Arc::new(Float32Array::from(vec![v.0])),
        (L::Double(v), T::Double) => Arc::new(Float64Array::from(vec![v.0])),
        (L::Int128(v), T::Decimal { precision, scale }) => Arc::new(
            Decimal128Array::from(vec![*v])
                .with_precision_and_scale(
                    u8::try_from(*precision).ok()?,
                    i8::try_from(*scale).ok()?,
                )
                .ok()?,
        ),
        (L::String(v), T::String) => Arc::new(StringArray::from(vec![v.as_str()])),
        _ => return None,
    };
    Some(array)
}

fn first_refused_member(field_type: &iceberg::spec::Type, path: &str) -> Option<(String, String)> {
    use iceberg::spec::Type;

    match field_type {
        Type::Primitive(primitive) => {
            refused_primitive_cause(primitive).map(|cause| (path.to_string(), cause))
        }
        Type::Variant(_) => Some((path.to_string(), variant_cause())),
        Type::Struct(members) => members.fields().iter().find_map(|member| {
            first_refused_member(&member.field_type, &format!("{path}.{}", member.name))
        }),
        Type::List(list) => {
            first_refused_member(&list.element_field.field_type, &format!("{path}.element"))
        }
        Type::Map(map) => first_refused_member(&map.key_field.field_type, &format!("{path}.key"))
            .or_else(|| {
                first_refused_member(&map.value_field.field_type, &format!("{path}.value"))
            }),
    }
}

/// Exhaustive, so a new Iceberg primitive must be classified before the build passes.
fn refused_primitive_cause(primitive: &iceberg::spec::PrimitiveType) -> Option<String> {
    use iceberg::spec::PrimitiveType::*;

    match primitive {
        Geometry(_) => Some(unread_type_cause("geometry")),
        Geography(_) => Some(unread_type_cause("geography")),
        Unknown => Some(
            "has type 'unknown', which holds only nulls and has no Exasol column type".to_string(),
        ),
        Boolean
        | Int
        | Long
        | Float
        | Double
        | Decimal { .. }
        | Date
        | Time
        | Timestamp
        | Timestamptz
        | TimestampNs
        | TimestamptzNs
        | String
        | Binary
        | Fixed(_)
        | Uuid => None,
    }
}

/// The Iceberg arm of [`super::declare_columns`]. Refuses every `geometry`, `geography`,
/// `variant`, and `unknown` column at any depth.
pub(super) fn declaration(field: &iceberg::spec::NestedField) -> ColumnDeclaration {
    let refused = first_refused_member(&field.field_type, &field.name).map(|(path, cause)| {
        let member_path = (path != field.name).then_some(path.as_str());
        column_refusal("Iceberg", &field.name, member_path, &cause)
    });
    let arrow_type = crate::types::mapping::iceberg_type_to_arrow(&field.field_type);
    ColumnDeclaration {
        source_type: LogicalField {
            field_id: Some(field.id),
            name: field.name.clone(),
            arrow_type: crate::types::mapping::arrow_type_to_tag(&arrow_type),
            nullable: !field.required,
            initial_default: encode_initial_default(field),
            nested: derive_nested_members(&field.field_type),
            physical_name: None,
        },
        partition: None,
        refused,
    }
}

fn derive_nested_members(ty: &iceberg::spec::Type) -> Option<NestedMembers> {
    use iceberg::spec::Type;

    match ty {
        // A variant is refused at declaration, so it never reaches the nested renderer.
        Type::Primitive(_) | Type::Variant(_) => None,
        Type::Struct(s) => Some(NestedMembers::Struct {
            fields: s
                .fields()
                .iter()
                .map(|f| NestedField {
                    field_id: Some(f.id),
                    name: f.name.clone(),
                    physical_name: None,
                    nested: derive_nested_members(&f.field_type),
                })
                .collect(),
        }),
        // List elements and map key/value are positional: the pseudo-field's Iceberg id is never
        // carried across.
        Type::List(l) => Some(NestedMembers::List {
            element: derive_nested_members(&l.element_field.field_type).map(Box::new),
        }),
        Type::Map(m) => Some(NestedMembers::Map {
            key: derive_nested_members(&m.key_field.field_type).map(Box::new),
            value: derive_nested_members(&m.value_field.field_type).map(Box::new),
        }),
    }
}

/// Reads `initial_default` only (`write_default` governs writes). Gated on the
/// `PrimitiveType`, not the Arrow tag: several primitives collapse onto `"utf8"` and the
/// scan side dispatches on the tag alone, so only primitives with a first-class Arrow tag
/// are encoded (mirroring `iceberg_predicate::literal_to_datum`). Temporals carry their raw
/// integer and decimals their unscaled `i128`, so the scan side needs no second parse.
fn encode_initial_default(field: &iceberg::spec::NestedField) -> Option<String> {
    use iceberg::spec::{PrimitiveLiteral, PrimitiveType};

    let primitive = field.field_type.as_primitive_type()?;
    let literal = field.initial_default.as_ref()?.as_primitive_literal()?;

    let encoded = match (primitive, &literal) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => v.to_string(),
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => v.to_string(),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => v.to_string(),
        (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => v.0.to_string(),
        (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => v.0.to_string(),
        (PrimitiveType::String, PrimitiveLiteral::String(v)) => v.clone(),
        (PrimitiveType::Date, PrimitiveLiteral::Int(days)) => days.to_string(),
        (
            PrimitiveType::Timestamp
            | PrimitiveType::TimestampNs
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(v),
        ) => v.to_string(),
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(v))
            if crate::types::mapping::exasol_representable_catalog_decimal(*precision, *scale) =>
        {
            v.to_string()
        }
        _ => return None,
    };
    Some(encoded)
}
