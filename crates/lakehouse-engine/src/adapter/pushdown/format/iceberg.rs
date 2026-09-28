use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::TableIdent;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, StaticStoreAddress, load_table_any_auth, parse_table_ident,
    redact_credentials, redact_error_text, resolve_vended_storage,
};
use serde_json::Value as Json;

use super::{ConnectionStorage, FormatReader, ResolvedScan};
use crate::scan::spec::{
    DeleteMechanism, FileEntry, LogicalField, NameMappingEntry, NestedField, NestedMembers,
};

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;

pub(super) struct IcebergFormatReader<'a> {
    pub(super) session: &'a CatalogSession,
    pub(super) catalog_props: &'a CatalogProps,
    pub(super) connection: ConnectionStorage<'a>,
}

impl FormatReader for IcebergFormatReader<'_> {
    /// Vended-credential extraction is gated solely on `creds.use_vended_credentials`,
    /// orthogonal to the catalog-auth mode. Credentials stay vended-only (a missing vended
    /// credential errors rather than falling back to the static one), while the CONNECTION's
    /// `endpoint` and `region` may override vended addressing via [`StaticStoreAddress`], which
    /// cannot carry a credential.
    ///
    /// Errors are redacted against the effective storage's secrets, since its `file_io` is
    /// what talks to object storage.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        let ConnectionStorage {
            storage,
            creds,
            allow_http,
        } = self.connection;
        Box::pin(async move {
            let result = load_table_any_auth(self.session, self.catalog_props, creds).await?;

            // Decided from schema history alone, before any manifest is read, so filtered and
            // unfiltered requests are refused identically.
            refuse_date_promotion(&result.metadata, &self.catalog_props.table)?;

            // The anchor is the table's own location: what vended `prefix`es match against.
            // The REST URI names no object store and `warehouse` is only a routing identifier.
            let table_location = result.metadata.location();
            if table_location.is_empty() {
                return Err(UdfError::User(format!(
                    "the loadTable response for table '{}' carries an EMPTY table \
                     `location`; the catalog `warehouse` is a routing identifier, not a \
                     table location, and is not a valid substitute",
                    self.catalog_props.table
                )));
            }
            let table_root = table_location.to_string();
            let effective_storage = if creds.use_vended_credentials {
                resolve_vended_storage(
                    &result,
                    table_location,
                    allow_http,
                    &StaticStoreAddress::from(creds),
                )?
            } else {
                storage.clone()
            };
            let secrets = effective_storage.secret_values();

            let (namespace, table_name) = parse_table_ident(&self.catalog_props.table)?;
            let table_ident = TableIdent::new(namespace, table_name);
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
                .metadata(result.metadata);
            let table = if let Some(loc) = result.metadata_location {
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

            let logical_schema = build_logical_schema(table.metadata().current_schema());

            // Absent ⇒ empty; malformed ⇒ plan-time error.
            let name_mapping = parse_name_mapping(
                table
                    .metadata()
                    .properties()
                    .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
                    .map(String::as_str),
            )?;

            // Must run before `plan_files_from_table`, which drops the information needed to tell
            // a Puffin deletion vector from a Parquet positional delete.
            ensure_supported_delete_mechanisms(&table, &self.catalog_props.table, &secrets).await?;

            let files =
                plan_files_from_table(table, &self.catalog_props.table, filter_json, &secrets)
                    .await?;

            Ok(ResolvedScan {
                files,
                effective_storage,
                logical_schema,
                table_root,
                name_mapping,
                partition_columns: Vec::new(),
                refused_columns: Vec::new(),
            })
        })
    }
}

/// Only top-level entries with a `field-id` are flattened; id-less entries exist only in
/// the schema. Nested child mappings are not recursed (#83). A malformed property is a
/// credential-free plan-time error (`serde_json` reports only a position).
fn parse_name_mapping(raw: Option<&str>) -> Result<Vec<NameMappingEntry>, UdfError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mapping: iceberg::spec::NameMapping = serde_json::from_str(raw).map_err(|e| {
        UdfError::User(format!(
            "failed to parse Iceberg '{}' table property: {e}",
            iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING
        ))
    })?;
    let mut entries = Vec::new();
    for field in mapping.fields() {
        let Some(field_id) = field.field_id() else {
            continue;
        };
        for name in field.names() {
            entries.push(NameMappingEntry {
                name: name.clone(),
                field_id,
            });
        }
    }
    Ok(entries)
}

/// Only Parquet positional deletes over Parquet data files are applied; everything else
/// must fail loud at plan time. Carries no path or secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnsupportedDeleteMechanism {
    EqualityDelete,
    /// Iceberg v3 Puffin deletion vector.
    DeletionVector,
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
            UnsupportedDeleteMechanism::DeletionVector => "Iceberg v3 Puffin deletion vectors",
            UnsupportedDeleteMechanism::OrcDataFile => "ORC data files",
            UnsupportedDeleteMechanism::AvroDataFile => "Avro data files",
            UnsupportedDeleteMechanism::OrcDeleteFile => "ORC delete files",
            UnsupportedDeleteMechanism::AvroDeleteFile => "Avro delete files",
            UnsupportedDeleteMechanism::NonParquetDataFile => "non-Parquet data files",
        }
    }
}

/// Must run at manifest level: `plan_files` drops the Puffin discriminator and file format,
/// making a deletion vector indistinguishable from a Parquet positional delete.
fn classify_manifest_file(
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
            Parquet => Ok(()),
            // A position delete stored as a Puffin blob IS a v3 deletion vector.
            Puffin => Err(U::DeletionVector),
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
         cannot apply on read (only Parquet positional deletes are supported); \
         this is a hard error, not a native re-plan",
        table_name,
        mechanism.describe(),
    );
    UdfError::User(redact_credentials(&msg))
}

/// Refuses a recorded `date` → `timestamp`/`timestamp_ns` promotion (#355).
///
/// `iceberg` 0.10.0 reads such bounds as 8 bytes, skipping the spec's bounds-width
/// inference, so a pre-promotion file's 4-byte bound fails manifest decoding with an
/// unhelpful error, and a second decode path `unwrap()`s. Deciding from schema history
/// alone avoids that decode and spends no object-store byte; it must run before
/// [`ensure_supported_delete_mechanisms`].
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
             earlier one, and this engine cannot read that promotion — `iceberg` 0.10.0 \
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

/// Classifies every alive manifest `DataFile`. This is the authoritative correctness gate:
/// `plan_files` drops the information needed to detect unsupported mechanisms.
async fn ensure_supported_delete_mechanisms(
    table: &iceberg::table::Table,
    table_name: &str,
    secrets: &[&str],
) -> Result<(), UdfError> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(());
    };
    let file_io = table.file_io();

    let manifest_list_bytes = file_io
        .new_input(snapshot.manifest_list())
        .map_err(|e| {
            UdfError::User(format!(
                "failed to open Iceberg manifest list for '{}': {}",
                table_name,
                redact_error_text(&e.to_string(), secrets)
            ))
        })?
        .read()
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "failed to read Iceberg manifest list for '{}': {}",
                table_name,
                redact_error_text(&e.to_string(), secrets)
            ))
        })?;

    let manifest_list = iceberg::spec::ManifestList::parse_with_version(
        &manifest_list_bytes,
        metadata.format_version(),
    )
    .map_err(|e| {
        UdfError::User(format!(
            "failed to parse Iceberg manifest list for '{}': {}",
            table_name,
            redact_error_text(&e.to_string(), secrets)
        ))
    })?;

    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            UdfError::User(format!(
                "failed to load Iceberg manifest for '{}': {}",
                table_name,
                redact_error_text(&e.to_string(), secrets)
            ))
        })?;
        for entry in manifest.entries() {
            // A DELETED entry no longer applies; failing on it would spuriously reject queries.
            if !entry.is_alive() {
                continue;
            }
            let data_file = entry.data_file();
            classify_manifest_file(data_file.content_type(), data_file.file_format())
                .map_err(|mechanism| unsupported_delete_error(mechanism, table_name))?;
        }
    }

    Ok(())
}

/// The plan-time gate has already rejected equality deletes and deletion vectors; the other
/// arms exist for defense-in-depth so the read-time backstop rejects anything that slips
/// past. `Data` never appears here and maps to a non-positional sentinel for the same reason.
fn iceberg_delete_mechanism(
    path: String,
    size: u64,
    content_type: iceberg::spec::DataContentType,
) -> DeleteMechanism {
    match content_type {
        iceberg::spec::DataContentType::PositionDeletes => {
            DeleteMechanism::IcebergPositionalDelete { path, size }
        }
        iceberg::spec::DataContentType::EqualityDeletes => {
            DeleteMechanism::IcebergEqualityDelete { path, size }
        }
        iceberg::spec::DataContentType::Data => {
            DeleteMechanism::IcebergEqualityDelete { path, size }
        }
    }
}

/// Iceberg predicate pruning is best-effort; DataFusion remains the row-level backstop.
async fn plan_files_from_table(
    table: iceberg::table::Table,
    table_name: &str,
    filter_json: Option<&Json>,
    secrets: &[&str],
) -> Result<Vec<FileEntry>, UdfError> {
    let mut scan_builder = table.scan();
    if let Some(fj) = filter_json {
        let schema = table.metadata().current_schema();
        if let Some(pred) = crate::adapter::iceberg_predicate::to_iceberg_predicate(fj, schema) {
            scan_builder = scan_builder.with_filter(pred);
        }
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
            redact_error_text(&e.to_string(), secrets)
        ))
    })?;

    let tasks: Vec<_> = task_stream.try_collect().await.map_err(|e| {
        UdfError::User(format!(
            "failed to collect Iceberg file tasks: {}",
            redact_error_text(&e.to_string(), secrets)
        ))
    })?;

    // Delete paths are relativized later in `relativize_shards_to_root`, like data-file paths.
    Ok(tasks
        .into_iter()
        .map(|t| {
            let deletes: Vec<DeleteMechanism> = t
                .deletes
                .iter()
                .map(|d| {
                    iceberg_delete_mechanism(d.file_path.clone(), d.file_size_in_bytes, d.file_type)
                })
                .collect();
            FileEntry::with_deletes(
                t.data_file_path().to_string(),
                t.file_size_in_bytes,
                deletes,
            )
        })
        .collect())
}

pub(crate) fn build_logical_schema(schema: &iceberg::spec::Schema) -> Vec<LogicalField> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let arrow_dt = crate::types::mapping::iceberg_type_to_arrow(&f.field_type);
            let arrow_type = crate::types::mapping::arrow_type_to_tag(&arrow_dt);
            LogicalField {
                field_id: Some(f.id),
                name: f.name.clone(),
                arrow_type,
                nullable: !f.required,
                initial_default: encode_initial_default(f),
                nested: derive_nested_members(&f.field_type),
                physical_name: None,
            }
        })
        .collect()
}

fn derive_nested_members(ty: &iceberg::spec::Type) -> Option<NestedMembers> {
    use iceberg::spec::Type;

    match ty {
        Type::Primitive(_) => None,
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
