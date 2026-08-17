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
use crate::scan::spec::{DeleteMechanism, FileEntry, LogicalField, NameMappingEntry};

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;

pub(super) struct IcebergFormatReader<'a> {
    pub(super) session: &'a CatalogSession,
    pub(super) catalog_props: &'a CatalogProps,
    pub(super) connection: ConnectionStorage<'a>,
}

impl FormatReader for IcebergFormatReader<'_> {
    /// Resolve this table's data-file list from the Iceberg REST catalog, on the
    /// [`CatalogSession`] the resolver already built.
    ///
    /// The catalog load_table request is self-issued via `load_table_any_auth`, which
    /// chooses how to authenticate (SigV4 | static bearer | OAuth2-derived bearer |
    /// none). Vended-credential extraction is gated SOLELY on
    /// `creds.use_vended_credentials` — orthogonal to the catalog-auth mode. When it is
    /// true, `resolve_vended_storage` builds the whole `StorageBackend` from the loadTable
    /// response, the anchor's URI scheme, and the CONNECTION's store ADDRESS alone.
    /// Credentials stay vended-only — one the catalog does not vend is an error here
    /// rather than a silent fall-back to the static one — while addressing may cross
    /// over: the CONNECTION's `endpoint` and `region` reach the selector through a
    /// [`StaticStoreAddress`], which cannot carry a credential, and each wins over the
    /// vended value independently when the CONNECTION sets it. When it is false, returns
    /// the static `storage` unchanged — byte-identical to the no-vending behaviour on
    /// every auth mode.
    ///
    /// An empty table `location` is rejected above the vended/static split, so both
    /// values of `use_vended_credentials` report the identical error.
    ///
    /// Every error surfaced from here on is redacted against the secret values of the
    /// EFFECTIVE storage, not the static one: the `file_io` built from it is what talks
    /// to object storage, so those are exactly the values an underlying provider error
    /// can echo back.
    ///
    /// `filter_json` is the raw pushdown filter JSON forwarded to `plan_files_from_table`
    /// for Iceberg-level file pruning. `None` disables that pruning.
    ///
    /// This reader carries no partition columns: an Iceberg scan's
    /// [`ResolvedScan::partition_columns`] is always empty, which is what keeps an
    /// Iceberg spec's encoding byte-identical to its pre-Delta form.
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
            // Single auth-mode-agnostic path: self-issue the loadTable GET under
            // whatever catalog-auth mode applies, then derive the effective storage
            // gated SOLELY on `use_vended_credentials` (orthogonal to the auth mode),
            // and build the Table from the response metadata so plan_files() can read
            // manifests from S3.
            let result = load_table_any_auth(self.session, self.catalog_props, creds).await?;

            // Resolve the effective storage (vended or static). The anchor is the
            // TABLE'S OWN location: what `storage_credentials[*].prefix` is matched
            // against, and the sole input the backend variant is read from. Nothing
            // else can stand in — the catalog REST URI names no object store, and the
            // REST `warehouse` is a routing identifier.
            let table_location = result.metadata.location();
            if table_location.is_empty() {
                return Err(UdfError::User(format!(
                    "the loadTable response for table '{}' carries an EMPTY table \
                     `location`; the catalog `warehouse` is a routing identifier, not a \
                     table location, and is not a valid substitute",
                    self.catalog_props.table
                )));
            }
            // Own the table root before `result.metadata` is moved into the table
            // builder below. Returned so the adapter can carry it once in the common
            // blob and emit per-shard file paths relative to it (non-empty, per the
            // guard above).
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

            // Build the iceberg Table so plan_files() can read manifests from S3.
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

            // Extract the logical schema before `plan_files_from_table` consumes
            // `table`.
            let logical_schema = build_logical_schema(table.metadata().current_schema());

            // Resolve the Iceberg name-mapping fallback (`schema.name-mapping.default`)
            // ONCE per query here — alongside `logical_schema`, and likewise before
            // `plan_files_from_table` consumes `table` — so it is resolved in the VS
            // planning layer, never per UDF invocation. Absent property ⇒ empty; a
            // present-but-malformed property fails loud with a clean plan-time error.
            let name_mapping = parse_name_mapping(
                table
                    .metadata()
                    .properties()
                    .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
                    .map(String::as_str),
            )?;

            // AUTHORITATIVE correctness gate: fail loud at the manifest/`DataFile`
            // level on any delete/data mechanism this engine cannot apply (equality
            // delete, Puffin/v3 deletion vector, ORC/Avro data or delete file) BEFORE
            // building any scan-driving SQL. This must run before
            // `plan_files_from_table` so the deletes it associates are guaranteed to
            // be applicable Parquet positional deletes.
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
            })
        })
    }
}

/// Parse the Iceberg `schema.name-mapping.default` table property into the flat
/// `Vec<NameMappingEntry>` the scan-side resolver looks up by physical name.
///
/// `raw` is the property's raw JSON value (`None` when the property is absent).
///
/// Behaviour (Iceberg column-projection rule #2 scope — see the plan):
/// - Absent property (`None`) → an empty `Vec` (NOT an error): a table with no
///   name-mapping is the common, fully-supported case.
/// - Present but malformed JSON → a clean, credential-free plan-time `UdfError`
///   (mirrors the fail-loud discipline of `ensure_supported_delete_mechanisms`;
///   the property carries only column names + field-ids, never credentials, and
///   `serde_json`'s error reports only a parse position).
/// - Present and valid → flatten ONLY the TOP-LEVEL entries: for each top-level
///   mapping that HAS a `field-id`, emit one `NameMappingEntry { name, field_id }`
///   per name in its `names` list. Entries without a `field-id` are skipped (they
///   exist only in the Iceberg schema, not in imported files — nothing to map to).
///   Nested `fields` (struct/map/list child mappings) are deliberately NOT
///   recursed into — out of scope for this phase (deferred to issue #83).
///
/// Parsed via the `iceberg` crate's own spec-accurate `NameMapping` deserializer
/// (kebab-case `field-id`, `DefaultOnNull` nested `fields`), never a hand-rolled
/// struct. Resolved ONCE per query in the VS planning layer.
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
        // Skip id-less entries (schema-only, not present in imported files) and do
        // NOT recurse into `field.fields()` (nested child mappings, out of scope).
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

/// A data- or delete-file mechanism the lakehouse engine cannot apply on read.
///
/// This engine applies ONLY Parquet positional deletes over Parquet data files.
/// Every other mechanism must fail loud at plan time — invalid results must never
/// be returned (mission: "correctness and safety are first-class"). The variant is
/// used solely to name the mechanism in a clean, credential-free error; it never
/// carries a file path or any secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnsupportedDeleteMechanism {
    /// Iceberg equality deletes (`DataContentType::EqualityDeletes`).
    EqualityDelete,
    /// Iceberg v3 Puffin deletion vector (position delete stored as a Puffin blob).
    DeletionVector,
    /// An ORC data file (`DataFileFormat::Orc`).
    OrcDataFile,
    /// An Avro data file (`DataFileFormat::Avro`).
    AvroDataFile,
    /// An ORC positional-delete file.
    OrcDeleteFile,
    /// An Avro positional-delete file.
    AvroDeleteFile,
    /// A data file in a format this engine does not read as columnar Parquet.
    NonParquetDataFile,
}

impl UnsupportedDeleteMechanism {
    /// A stable, credential-free English name for the mechanism, spliced into the
    /// plan-time fail-loud error. Never includes a file path or any secret value.
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

/// Classify one manifest `DataFile` by its content type and file format, at the
/// authoritative manifest level (where the Puffin discriminator and file format
/// are still visible — `plan_files` drops them, so a deletion vector would be
/// indistinguishable from a Parquet positional delete at read time).
///
/// Returns `Ok(())` ONLY for the two mechanisms this engine can apply correctly:
/// a Parquet DATA file and a Parquet POSITION-DELETE file. Every other
/// (content, format) combination returns the specific unsupported mechanism so
/// the caller can fail loud before building any scan-driving SQL.
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

/// Build the plan-time fail-loud error for an unsupported delete mechanism.
///
/// The message names ONLY the mechanism (never a file path, which could in
/// principle embed a presigned credential) and is defensively passed through
/// [`redact_credentials`] so no secret can survive into surfaced SQL/error text.
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

/// Fail loud at plan time if the table's current snapshot uses ANY delete/data
/// mechanism this engine cannot apply, detected at the manifest/`DataFile` level.
///
/// This is the AUTHORITATIVE correctness gate (invalid results must never be
/// returned). It enumerates the current snapshot's manifest list, loads each
/// manifest, and classifies every ALIVE `DataFile` (both data and delete
/// manifests) via [`classify_manifest_file`]. Detection happens here — before any
/// scan-driving SQL is built — because `plan_files` collapses each task to a bare
/// path and drops the Puffin discriminator and file format needed to tell a
/// Parquet positional delete from a deletion vector.
///
/// A table with no current snapshot (empty table) trivially passes.
///
/// Every manifest read here goes through the caller's object-store credentials, so
/// `secrets` carries their literal values for the value-based half of
/// [`redact_error_text`].
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
            // Skip entries removed in this snapshot: a DELETED manifest entry no
            // longer applies, so failing on it would spuriously reject queries.
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

/// Build the [`DeleteMechanism`] for one iceberg task-level delete of `size` bytes
/// at `path`.
///
/// By the time a `FileScanTask`'s deletes reach here, the plan-time fail-loud gate
/// ([`ensure_supported_delete_mechanisms`]) has already rejected any table that
/// uses equality deletes or Puffin deletion vectors, so every `PositionDeletes`
/// task delete is guaranteed to be a Parquet positional delete. The other arms
/// are mapped honestly for defense-in-depth: they can only be produced if a
/// mechanism somehow slips past the gate, and the scan reader's read-time backstop
/// then rejects them cleanly. `Data` never appears in a task's delete list; it is
/// mapped to a non-positional sentinel so it is likewise rejected rather than
/// silently applied.
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

/// Drive the iceberg scan and collect the data-file paths with their sizes.
///
/// When `filter_json` is `Some`, an Iceberg pruning predicate is applied before
/// `plan_files` so manifests and files that cannot match are skipped. DataFusion
/// remains the row-level correctness backstop; this is pruning-only.
///
/// `secrets` carries the literal values of the object-store credentials the scan
/// planning below reads manifests with, for the value-based half of
/// [`redact_error_text`].
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

    // Associate each data file's Parquet positional-delete files into its entry.
    // The plan-time fail-loud gate (`ensure_supported_delete_mechanisms`) has
    // already run, so any `.deletes` present here are applicable Parquet
    // positional deletes. Absolute delete paths are relativized later, in
    // `relativize_shards_to_root`, EXACTLY like the data-file path.
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

/// Build the logical schema (`Vec<LogicalField>`) from an Iceberg current schema.
///
/// Iterates over the top-level struct fields of `schema` and maps each to a
/// `LogicalField` carrying its Iceberg field-id, current name, Arrow type tag,
/// and nullability (required → `false`, optional → `true`).
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
                physical_name: None,
            }
        })
        .collect()
}

/// Encode a field's Iceberg `initial-default` as the raw primitive scalar in
/// plain text, or `None` when there is nothing to carry.
///
/// Reads `initial_default` ONLY (never `write_default`, which governs writes,
/// not reads). Returns `None` when the field has no `initial-default`, when the
/// default is non-primitive (struct/list/map — `as_primitive_literal` yields
/// `None`), or when the field's `PrimitiveType` reaches only the JSON-fallback
/// `"utf8"` path (`uuid`/`time`/`fixed`/`binary`/oversized `decimal`).
///
/// The `(PrimitiveType, PrimitiveLiteral)` match is deliberately gated on the
/// PrimitiveType, NOT on the computed Arrow tag: several distinct primitives
/// collapse onto the `"utf8"` tag, and the scan-side reconstruction dispatches
/// on that tag alone — so encoding a non-`String` value under `"utf8"` would be
/// misread. Only the exact set that maps to a first-class Arrow tag in
/// `iceberg_primitive_to_arrow` is encoded, mirroring the `PrimitiveType`
/// dispatch in `iceberg_predicate::literal_to_datum`. Temporals carry their raw
/// integer (days / micros / nanos) and a decimal carries its `i128` unscaled
/// mantissa, so the scan side reconstructs a `ScalarValue` against the Arrow tag
/// with no second temporal/decimal parse. The encoded text is a bare scalar, so
/// it is inherently credential-free.
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
