use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    AggKind, CatalogProps, DeleteFileContentType, DeleteFileRef, FileEntry, LogicalField,
    NameMappingEntry, ProjectionItem, StorageBackend,
};
use crate::types::mapping::exasol_type_from_json;
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::TableIdent;
use serde_json::Value as Json;

use lakehouse_catalog::{
    CatalogSession, load_table_any_auth, parse_table_ident, redact_credentials, redact_error_text,
    resolve_vended_storage,
};

use super::grouped_agg::{group_key_exasol_types, select_item_index};
use super::request_shape::{RequestShape, classify_request_shape};
use super::single_group_agg::SingleGroupItem;
use super::support::{aggregate_exasol_types, cast_to_declared_type, emits_ident};
use super::{GroupedSelectItem, build_logical_schema};

/// Emit a file path relative to `table_root` when the file lives under it,
/// otherwise pass the absolute path through unchanged.
///
/// Stripping happens ONLY at a real path-segment boundary: the root must be a
/// prefix AND either end with `/` or be followed by a `/` in the path. A path that
/// merely shares the root as a bare string prefix (e.g. `<root>-archive/...`,
/// `<root>2/...`) or one exactly equal to the root does NOT match, so it stays
/// absolute — this keeps the round-trip with the scan UDF's single-`/` join lossless
/// and avoids emitting an empty relative entry. After a boundary match the root
/// prefix and then a single leading `/` are stripped, so the relative path has no
/// leading slash. An empty `table_root` (legacy / no resolved root) always yields an
/// absolute path.
fn relativize_path_to_root(path: &str, table_root: &str) -> String {
    let at_segment_boundary = !table_root.is_empty()
        && path.starts_with(table_root)
        && (table_root.ends_with('/') || path[table_root.len()..].starts_with('/'));
    if at_segment_boundary {
        let rest = &path[table_root.len()..];
        rest.strip_prefix('/').unwrap_or(rest).to_string()
    } else {
        path.to_string()
    }
}

/// Strip `table_root` from every under-root file path in each shard (see
/// [`relativize_path_to_root`]) while preserving byte sizes and shard membership.
/// Paths not under the root stay absolute.
///
/// Each data file's associated positional-delete file paths are relativized by
/// the SAME [`relativize_path_to_root`] rule as the data-file path, so the scan
/// UDF rejoins them onto `table_root` identically (delete files written by the
/// same engine live under the same table root). Delete byte sizes and content
/// types are preserved unchanged.
pub(super) fn relativize_shards_to_root(
    shards: Vec<Vec<FileEntry>>,
    table_root: &str,
) -> Vec<Vec<FileEntry>> {
    shards
        .into_iter()
        .map(|shard| {
            shard
                .into_iter()
                .map(|mut entry| {
                    entry.path = relativize_path_to_root(&entry.path, table_root);
                    for delete in &mut entry.deletes {
                        delete.path = relativize_path_to_root(&delete.path, table_root);
                    }
                    entry
                })
                .collect()
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
pub(super) fn encode_initial_default(field: &iceberg::spec::NestedField) -> Option<String> {
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
            if *precision <= 36 && *scale <= 36 =>
        {
            v.to_string()
        }
        _ => return None,
    };
    Some(encoded)
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

/// Resolve the data-file list from the Iceberg REST catalog for one table, on a
/// [`CatalogSession`] the caller already built.
///
/// This is the resolve-once seam AND the only file-resolution entry point: the
/// single-table pushdown path, every join leg, and the external E2E callers all come
/// through here; the resolved file list is passed explicitly to the scan UDF. Taking
/// the session rather than a `catalog_uri` is what makes a per-table session rebuild
/// inexpressible — the catalog-auth strategy, `/v1/config` prefix, and pooled HTTP
/// client are resolved once per query into the passed session and reused across every
/// table's `loadTable` GET (e.g. each leg of a join). A `catalog_uri` parameter
/// alongside the session would be a second copy of a value the session already
/// carries, free to disagree with it.
///
/// The parse-before-config guarantee therefore belongs to the CALLER: because the
/// session is built outside this function, the involved-table identifier must be
/// validated at the `handle_pushdown` seam BEFORE `CatalogSession::resolve`, so a
/// malformed identifier issues zero catalog HTTP and surfaces a parse error rather
/// than a transport error from an unreachable catalog. This function parses the
/// identifier again below to build the `TableIdent`, so skipping the caller-side
/// check costs the guarantee, never correctness.
///
/// The catalog load_table request is self-issued via `load_table_any_auth`, which
/// chooses how to authenticate (SigV4 | static bearer | OAuth2-derived bearer |
/// none). Vended-credential extraction is gated SOLELY on
/// `creds.use_vended_credentials` — orthogonal to the catalog-auth mode. That flag
/// is the ONE decision point between two storage selectors reading disjoint
/// inputs. When it is true, `resolve_vended_storage` builds the whole
/// `StorageBackend` from the loadTable response and the anchor's URI scheme: it
/// reads no CONNECTION storage field and preserves no static value, so a
/// credential or a store address the catalog does not vend is an error here rather
/// than a silent fall-back to the static one. When it is false, returns
/// `(files, storage.clone())` — byte-identical to the no-vending behaviour on
/// every auth mode.
///
/// Every error surfaced from here on is redacted against the secret values of the
/// EFFECTIVE storage, not the static one: the `file_io` built from it is what talks
/// to object storage, so those are exactly the values an underlying provider error
/// can echo back.
///
/// `allow_http` is the resolved `ALLOW_HTTP` virtual-schema property. It travels
/// beside `creds` because both selectors read it: it is already baked into the
/// static `storage` passed in, and the vended selector takes it as the operator's
/// consent gate for plaintext transport. It is a virtual-schema property and not a
/// CONNECTION field, so passing it does not reintroduce a CONNECTION-derived read
/// on the vended path.
///
/// `filter_json` is the raw pushdown filter JSON forwarded to `plan_files_from_table`
/// for Iceberg-level file pruning. Pass `None` to disable pruning (e.g. `createVirtualSchema`).
pub async fn resolve_file_list(
    session: &CatalogSession,
    catalog_props: &CatalogProps,
    storage: &StorageBackend,
    creds: &ConnectionCreds,
    allow_http: bool,
    filter_json: Option<&Json>,
) -> Result<
    (
        Vec<FileEntry>,
        StorageBackend,
        Vec<LogicalField>,
        String,
        Vec<NameMappingEntry>,
    ),
    UdfError,
> {
    // Single auth-mode-agnostic path: self-issue the loadTable GET under whatever
    // catalog-auth mode applies, then derive the effective storage gated SOLELY on
    // `use_vended_credentials` (orthogonal to the auth mode), and build the Table
    // from the response metadata so plan_files() can read manifests from S3.
    let result = load_table_any_auth(session, catalog_props, creds).await?;

    // Resolve the effective storage (vended or static). The anchor is the TABLE'S
    // OWN location from the parsed metadata, which under vending carries two jobs:
    // it is what `storage_credentials[*].prefix` is matched against, and it is the
    // sole input the backend variant is read from. Nothing else can stand in for
    // it: the catalog REST URI names no object store at all, and the REST
    // `warehouse` is a routing identifier rather than a storage location — so an
    // absent location is its own error on the vended branch below, never a
    // substituted CONNECTION-derived string fed through the scheme matcher.
    let table_location = result.metadata.location();
    // Own the table root before `result.metadata` is moved into the table builder
    // below. Returned so the adapter can carry it once in the common blob and emit
    // per-shard file paths relative to it (empty ⇒ every path stays absolute).
    let table_root = table_location.to_string();
    let effective_storage = if creds.use_vended_credentials {
        if table_location.is_empty() {
            return Err(UdfError::User(
                "the loadTable response carries no table location, so the storage backend \
                 cannot be resolved; the catalog `warehouse` is a routing identifier, not a \
                 storage location, and is not a valid fallback"
                    .into(),
            ));
        }
        resolve_vended_storage(&result, table_location, allow_http)?
    } else {
        storage.clone()
    };
    let secrets = effective_storage.secret_values();

    // Build the iceberg Table so plan_files() can read manifests from S3.
    let (namespace, table_name) = parse_table_ident(&catalog_props.table)?;
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

    // Extract the logical schema before `plan_files_from_table` consumes `table`.
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

    // AUTHORITATIVE correctness gate: fail loud at the manifest/`DataFile` level on
    // any delete/data mechanism this engine cannot apply (equality delete, Puffin/v3
    // deletion vector, ORC/Avro data or delete file) BEFORE building any
    // scan-driving SQL. This must run before `plan_files_from_table` so the deletes
    // it associates are guaranteed to be applicable Parquet positional deletes.
    ensure_supported_delete_mechanisms(&table, &catalog_props.table, &secrets).await?;

    let files = plan_files_from_table(table, &catalog_props.table, filter_json, &secrets).await?;
    Ok((
        files,
        effective_storage,
        logical_schema,
        table_root,
        name_mapping,
    ))
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

/// Map an iceberg task-level delete content type to the wire [`DeleteFileContentType`].
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
fn map_delete_content_type(t: iceberg::spec::DataContentType) -> DeleteFileContentType {
    match t {
        iceberg::spec::DataContentType::PositionDeletes => DeleteFileContentType::PositionDeletes,
        iceberg::spec::DataContentType::EqualityDeletes => DeleteFileContentType::EqualityDeletes,
        iceberg::spec::DataContentType::Data => DeleteFileContentType::EqualityDeletes,
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
            let deletes: Vec<DeleteFileRef> = t
                .deletes
                .iter()
                .map(|d| DeleteFileRef {
                    path: d.file_path.clone(),
                    size: d.file_size_in_bytes,
                    content_type: map_delete_content_type(d.file_type),
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

/// Resolve one Iceberg table's schema for `createVirtualSchema` on the
/// [`CatalogSession`] the caller already built.
///
/// Returns (field_name, exasol_type_string) pairs. The table metadata is loaded
/// via the unified `load_table_any_auth` (SigV4 | bearer | OAuth2-bearer | none).
/// Schema resolution only reads `table.metadata().current_schema()` — no S3
/// manifest access is needed, so vended credentials do not affect this path.
///
/// Takes the session by shared reference and holds no means to build one, so a
/// per-table OAuth2 grant is structurally inexpressible: `adapter/mod.rs` builds
/// ONE session ahead of the table-enumeration loop and every table's schema
/// resolves on it, and a grant failure surfaces there — once, before the loop —
/// rather than at whichever table happened to be resolved first. There is no
/// `catalog_uri` parameter because the session already carries it and a second
/// copy could disagree with it.
///
/// `catalog_props.table` names the table; `load_table_any_auth` parses that
/// identifier before it issues any HTTP, so a malformed identifier still returns
/// the parse error without a `loadTable` GET.
pub async fn resolve_table_schema(
    session: &CatalogSession,
    catalog_props: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<Vec<(String, String)>, UdfError> {
    let result = load_table_any_auth(session, catalog_props, creds).await?;
    let table_metadata = result.metadata;

    let schema = table_metadata.current_schema();
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let exasol_ty = crate::types::mapping::iceberg_type_to_exasol(&f.field_type);
            // Declare columns in Exasol's canonical (uppercase) identifier casing
            // so unquoted user SQL (`SELECT id` → `ID`) resolves. The scan maps
            // projection names back to the Parquet field casing case-insensitively.
            (f.name.to_uppercase(), exasol_ty)
        })
        .collect();

    Ok(fields)
}

/// Build the shape-correct empty-result response for a fully-pruned file list.
///
/// Routing goes through the SAME shared [`classify_request_shape`] the non-empty
/// dispatcher uses, so the empty and non-empty positional column shapes are
/// identical by construction — the 3-tier priority (grouped → single-group → row
/// scan), the `validate_agg_col_types` numeric gates, and the grouped HAVING
/// merge-render — whose failure routes to `GroupByWrapper` rather than erroring —
/// all live in the classifier, never re-derived here. Each arm renders only its own
/// empty shape:
/// - `Grouped` → zero rows in the full grouped output shape (`empty_grouped_sql`);
/// - `GroupByWrapper` → a zero-row result typed from `selectListDataTypes`
///   (`empty_select_list_typed_sql`), falling back to the full-row empty shape when
///   `selectListDataTypes` is absent or empty;
/// - `SingleGroupAgg` → one shape-correct empty aggregate row (`empty_agg_sql`);
/// - `RowScan` → a typed empty projection (`empty_pushdown_sql`), or — when
///   `projection_widened` — the same `selectListDataTypes` zero-row shape as
///   `GroupByWrapper`.
///
/// `projection_widened` is `project_columns`'s widening signal for the
/// `proj_cols`/`proj_types` pair: `true` means they are the full base row rather
/// than one item per select-list item (#196).
///
/// No scan or distinct-merge UDF is referenced: with zero files there is nothing to
/// scan or merge, and a zero-row result already satisfies any HAVING/ORDER BY/LIMIT.
pub(super) fn empty_result_sql(
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    projection_widened: bool,
    col_types: &[(String, String)],
) -> Result<Json, UdfError> {
    match classify_request_shape(pushdown_req, col_types) {
        // A zero-row result satisfies any HAVING, so the classifier's `having` is
        // deliberately ignored on the empty path.
        RequestShape::Grouped { detection, .. } => {
            let group_key_types = group_key_exasol_types(
                pushdown_req,
                &detection.group_keys,
                &detection.select_items,
            );
            // Per-plan declared types, aligned 1:1 with `detection.plans` (includes
            // aggregates nested inside a scalar-over-aggregate item) — the same
            // aligned source the non-empty grouped path uses.
            Ok(empty_grouped_sql(
                &group_key_types,
                &detection.plan_types,
                &detection.select_items,
            ))
        }
        // The non-empty path routes such a request to the qualified single-table
        // wrapper whose output columns ARE the `selectList` items. Mirror that shape
        // with a zero-row result typed from `selectListDataTypes`, so the empty and
        // non-empty column shapes never diverge (never a full-row `04000` mismatch).
        // When `selectListDataTypes` is absent or empty this falls back to the
        // full-row empty shape, byte-for-byte with the pre-refactor behaviour.
        RequestShape::GroupByWrapper => Ok(empty_select_list_typed_sql(pushdown_req)
            .unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types))),
        RequestShape::SingleGroupAgg { items } => {
            Ok(empty_agg_sql(&items, &aggregate_exasol_types(pushdown_req)))
        }
        // A widened derived projection is the full base row, so the non-empty path
        // routes it to the qualified single-table wrapper whose output columns ARE
        // the `selectList` items (#196). Mirror that shape here for the same reason
        // the `GroupByWrapper` arm above does: emitting the full base row instead
        // would diverge from the non-empty column shape and trip Exasol's positional
        // `04000` check.
        RequestShape::RowScan if projection_widened => {
            Ok(empty_select_list_typed_sql(pushdown_req)
                .unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types)))
        }
        RequestShape::RowScan => Ok(empty_pushdown_sql(proj_cols, proj_types)),
    }
}

/// A zero-row result whose columns are `CAST(NULL AS <ty>)` for each
/// `selectListDataTypes` entry, in order — the empty-result shape matching the
/// grouped qualified-wrapper fallback (whose output columns are the `selectList`
/// items). `None` when `selectListDataTypes` is absent or empty (the caller then
/// falls back to the full-row empty shape).
fn empty_select_list_typed_sql(pushdown_req: &Json) -> Option<Json> {
    let types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;
    let items: Vec<String> = types
        .iter()
        .map(|dt| format!("CAST(NULL AS {})", exasol_type_from_json(dt)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    Some(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// The empty-result literal for an aggregate evaluated over zero input rows.
///
/// The COUNT family yields `0`; every other kind yields `NULL` — single-node SQL
/// semantics over zero rows, mirroring the zero-count NULL guard (ADR-008).
fn empty_agg_literal(kind: &AggKind) -> &'static str {
    match kind {
        AggKind::Count | AggKind::CountCol => "0",
        AggKind::Sum
        | AggKind::Min
        | AggKind::Max
        | AggKind::Avg
        | AggKind::VarPop
        | AggKind::VarSamp
        | AggKind::StddevPop
        | AggKind::StddevSamp => "NULL",
    }
}

/// Build the single-group aggregate empty-result response: exactly one row whose
/// columns are each select-list item's zero-row literal cast to its declared
/// result type (from `aggregate_exasol_types`/`selectListDataTypes`), in
/// select-list order. A `COUNT(DISTINCT ...)` item yields `0` (no merge UDF and
/// no fan-out: with zero files there is nothing to scan or deduplicate); every
/// ordinary aggregate yields its per-`AggKind` empty literal. `FROM DUAL` alone
/// already yields one row, so no `WHERE` is emitted.
///
/// The cast decision mirrors `cast_merge_items` (cast when a declared type is
/// present and not the `VARCHAR(2000000)` default) so the empty column types can
/// never drift from the non-empty single-group shape.
fn empty_agg_sql(items: &[SingleGroupItem], aggregate_types: &[String]) -> Json {
    let literals: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let literal = match item {
                SingleGroupItem::Distinct(_) => "0",
                SingleGroupItem::Aggregate(plan) => empty_agg_literal(&plan.kind),
            };
            cast_to_declared_type(literal, aggregate_types.get(i).map(String::as_str))
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL", literals.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Build the grouped aggregate empty-result response: zero rows
/// (`FROM DUAL WHERE 1=0`) whose columns are the full grouped output shape —
/// group-key, merged-aggregate, and constant-projection columns assembled in the
/// user's select-list order via `select_items`, exactly as the non-empty grouped
/// merge assembles its outer SELECT.
///
/// Group-key and aggregate columns are `CAST(NULL AS <declared-type>)` (types from
/// `group_key_exasol_types` / `aggregate_exasol_types`); a constant projection
/// reuses its already-rendered, type-cast expression. A zero-row result satisfies
/// any HAVING / ORDER BY / LIMIT, so none of those need rendering.
fn empty_grouped_sql(
    group_key_types: &[String],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
) -> Json {
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let items: Vec<String> = ordered
        .iter()
        .filter_map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => group_key_types
                .get(*group_key_slot)
                .map(|ty| format!("CAST(NULL AS {ty})")),
            GroupedSelectItem::Aggregate { plan_slot, .. } => aggregate_types
                .get(*plan_slot)
                .map(|ty| format!("CAST(NULL AS {ty})")),
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            // A scalar-over-aggregate column is NULL over zero rows and goes through
            // the shared `cast_to_declared_type`, so — unlike the GroupKey/Aggregate
            // arms above, which cast unconditionally — it emits a bare NULL when the
            // item's declared type is the VARCHAR(2000000) default.
            GroupedSelectItem::ScalarOverAggregate { declared_type, .. } => {
                Some(cast_to_declared_type("NULL", Some(declared_type)))
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Build a pushdown response with an empty result (no matching files).
fn empty_pushdown_sql(proj_cols: &[ProjectionItem], proj_types: &[String]) -> Json {
    let items: Vec<String> = proj_cols
        .iter()
        .zip(proj_types.iter())
        .enumerate()
        .map(|(i, (item, ty))| format!("CAST(NULL AS {ty}) AS {}", emits_ident(item, i)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

#[cfg(test)]
mod tests {
    use super::super::detect_aggregates;
    use super::super::single_group_agg::DistinctCount;
    use super::super::support::quote_ident;
    use super::super::test_support::*;
    use super::*;
    use crate::scan::spec::AggregatePlan;
    use iceberg::spec::{DataContentType, DataFileFormat};

    // ---------------------------------------------------------------------------
    // Task 1.3 — fail-loud on unsupported delete/data mechanisms (manifest level)
    // ---------------------------------------------------------------------------

    /// The two mechanisms this engine CAN apply — a Parquet data file and a
    /// Parquet positional-delete file — classify as supported (`Ok`).
    #[test]
    fn classify_accepts_parquet_data_and_parquet_positional_delete() {
        assert!(
            classify_manifest_file(DataContentType::Data, DataFileFormat::Parquet).is_ok(),
            "Parquet data file must be supported"
        );
        assert!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Parquet)
                .is_ok(),
            "Parquet positional delete must be supported"
        );
    }

    /// Equality deletes fail loud regardless of file format.
    #[test]
    fn classify_rejects_equality_deletes() {
        for fmt in [
            DataFileFormat::Parquet,
            DataFileFormat::Avro,
            DataFileFormat::Orc,
        ] {
            assert_eq!(
                classify_manifest_file(DataContentType::EqualityDeletes, fmt),
                Err(UnsupportedDeleteMechanism::EqualityDelete),
                "equality delete ({fmt:?}) must fail loud"
            );
        }
    }

    /// A position delete stored as a Puffin blob is a v3 deletion vector — the
    /// exact case indistinguishable from a Parquet positional delete once
    /// `plan_files` has dropped the format discriminator, so it MUST be caught at
    /// the manifest level.
    #[test]
    fn classify_rejects_puffin_deletion_vector() {
        assert_eq!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Puffin),
            Err(UnsupportedDeleteMechanism::DeletionVector),
            "Puffin position delete (deletion vector) must fail loud"
        );
    }

    /// ORC/Avro data and delete files fail loud.
    #[test]
    fn classify_rejects_orc_and_avro_data_and_delete_files() {
        assert_eq!(
            classify_manifest_file(DataContentType::Data, DataFileFormat::Orc),
            Err(UnsupportedDeleteMechanism::OrcDataFile),
        );
        assert_eq!(
            classify_manifest_file(DataContentType::Data, DataFileFormat::Avro),
            Err(UnsupportedDeleteMechanism::AvroDataFile),
        );
        assert_eq!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Orc),
            Err(UnsupportedDeleteMechanism::OrcDeleteFile),
        );
        assert_eq!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Avro),
            Err(UnsupportedDeleteMechanism::AvroDeleteFile),
        );
    }

    /// The fail-loud error names the mechanism, names the table, and leaks no
    /// credential (defensively redacted).
    #[test]
    fn unsupported_delete_error_names_mechanism_and_redacts() {
        let err = unsupported_delete_error(
            UnsupportedDeleteMechanism::DeletionVector,
            "db.mor_dv_table",
        );
        let msg = match err {
            UdfError::User(m) => m,
            other => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            msg.contains("Iceberg v3 Puffin deletion vectors"),
            "error must name the mechanism: {msg}"
        );
        assert!(
            msg.contains("db.mor_dv_table"),
            "error must name the offending table: {msg}"
        );
        // No credential label may survive the defensive redaction.
        assert!(
            !msg.contains("access_key"),
            "must not leak access_key: {msg}"
        );
        assert!(
            !msg.contains("secret_key"),
            "must not leak secret_key: {msg}"
        );
    }

    /// A manifest-read error that echoes Azure static credentials verbatim has
    /// BOTH literal values stripped — not merely their labels.
    ///
    /// The two credentials fail the label heuristic in different ways, so each
    /// independently requires the value-based pass:
    ///   - the account key is echoed bare inside a string-to-sign, with no
    ///     recognizable label anywhere near it;
    ///   - the SAS token carries its OWN `sig=` label, so a label-only pass
    ///     rewrites the middle of the token and leaves its permission and expiry
    ///     fields verbatim.
    #[test]
    fn manifest_read_errors_redact_the_literal_azure_secret_values() {
        let account_key = "Zm9vYmFyYmF6cXV1eGNvcmdlc2VjcmV0QUNDT1VOVEtFWT09";
        let sas_permissions = "sp=racwdlmeop";
        let sas_token = format!(
            "sv=2024-11-04&ss=bf&srt=sco&{sas_permissions}&se=2026-12-31T23:59:59Z&sig=aB3%2FxQ7"
        );
        let raw = format!(
            "AuthenticationFailed: Server failed to authenticate the request. \
             String to sign used was: {account_key}. \
             Request URL: https://acct.dfs.core.windows.net/c/meta/snap.avro?{sas_token}"
        );
        let secrets = [account_key, sas_token.as_str()];

        let surfaced = format!(
            "failed to read Iceberg manifest list for 'ns.tbl': {}",
            redact_error_text(&raw, &secrets)
        );

        assert!(
            !surfaced.contains(account_key),
            "account key value must not survive: {surfaced}"
        );
        assert!(
            !surfaced.contains(&sas_token),
            "SAS token value must not survive: {surfaced}"
        );
        assert!(
            !surfaced.contains(sas_permissions),
            "the SAS token's permission field must not survive either: {surfaced}"
        );
        assert!(
            surfaced.contains("failed to read Iceberg manifest list for 'ns.tbl'"),
            "the actionable context must be preserved: {surfaced}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 1.2 — adapter carries positional deletes into the per-shard scan spec
    // ---------------------------------------------------------------------------

    /// `map_delete_content_type` maps the iceberg task-level content type onto the
    /// wire enum honestly (position → position; equality → equality).
    #[test]
    fn map_delete_content_type_maps_position_and_equality() {
        use iceberg::spec::DataContentType;
        assert_eq!(
            map_delete_content_type(DataContentType::PositionDeletes),
            DeleteFileContentType::PositionDeletes
        );
        assert_eq!(
            map_delete_content_type(DataContentType::EqualityDeletes),
            DeleteFileContentType::EqualityDeletes
        );
    }

    /// A data file's associated positional-delete file paths are relativized by
    /// the SAME rule as the data-file path: an under-root path is stripped to a
    /// root-relative path, a path not under the root stays absolute. Delete size
    /// and content type are preserved.
    #[test]
    fn delete_file_paths_use_relative_absolute_encoding() {
        let root = "s3://warehouse/db/table";
        let entry = FileEntry::with_deletes(
            format!("{root}/data/part-0.parquet"),
            1000,
            vec![
                // under the table root — must relativize exactly like the data path
                pos_delete(&format!("{root}/data/deletes/del-0.parquet"), 50),
                // not under the root — must stay absolute
                pos_delete("s3://other-bucket/del-x.parquet", 60),
            ],
        );
        let shards = relativize_shards_to_root(vec![vec![entry]], root);
        let e = &shards[0][0];
        assert_eq!(e.path, "data/part-0.parquet", "data path must relativize");
        assert_eq!(
            e.deletes[0].path, "data/deletes/del-0.parquet",
            "under-root delete path must relativize EXACTLY like the data path"
        );
        assert_eq!(e.deletes[0].size, 50, "delete size preserved");
        assert_eq!(
            e.deletes[0].content_type,
            DeleteFileContentType::PositionDeletes,
            "delete content type preserved"
        );
        assert_eq!(
            e.deletes[1].path, "s3://other-bucket/del-x.parquet",
            "a delete path not under the root must stay absolute"
        );
    }

    /// Mirror of the scan UDF's `reconstruct_abs_uri` join rule, so the round-trip
    /// invariant can be asserted here without a cross-crate dependency: an entry that
    /// already carries a scheme (`"://"`) is absolute and returned unchanged; any
    /// other entry is joined onto the root with exactly one `/`.
    fn reconstruct_abs_uri_mirror(entry_path: &str, table_root: &str) -> String {
        if entry_path.contains("://") {
            return entry_path.to_string();
        }
        let root = table_root.strip_suffix('/').unwrap_or(table_root);
        let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
        format!("{root}/{rel}")
    }

    /// A path that shares the table root only as a bare STRING prefix (no `/`
    /// segment boundary) must NOT be relativized: stripping it and rejoining with a
    /// single `/` corrupts the URI (finding R.1). Only true under-root paths are
    /// stripped; everything else stays absolute and round-trips to itself.
    #[test]
    fn sibling_prefix_paths_are_not_relativized() {
        let root = "s3://w/db/events";

        // A genuine under-root path IS relativized (existing behavior preserved).
        let under = format!("{root}/data/f.parquet");
        assert_eq!(
            relativize_path_to_root(&under, root),
            "data/f.parquet",
            "under-root path must be relativized"
        );

        // Sibling directories that share the root as a bare prefix but break at no
        // `/` boundary stay ABSOLUTE (not stripped).
        let archive = format!("{root}-archive/f.parquet");
        assert_eq!(
            relativize_path_to_root(&archive, root),
            archive,
            "sibling '-archive' path must stay absolute"
        );
        let sibling2 = format!("{root}2/data/f.parquet");
        assert_eq!(
            relativize_path_to_root(&sibling2, root),
            sibling2,
            "sibling '2' path must stay absolute"
        );

        // A path exactly equal to the root stays absolute (no empty entry).
        assert_eq!(
            relativize_path_to_root(root, root),
            root,
            "path equal to the root must stay absolute, not become an empty entry"
        );

        // Every case round-trips back to the original absolute path through the
        // scan UDF's reconstruct rule.
        for original in [&under, &archive, &sibling2, &root.to_string()] {
            let emitted = relativize_path_to_root(original, root);
            assert_eq!(
                reconstruct_abs_uri_mirror(&emitted, root),
                *original,
                "round-trip must be identity for {original}"
            );
        }
    }

    /// The `abfss://` scheme carries userinfo (the container name) in its
    /// authority (`abfss://<container>@<account>.dfs.core.windows.net/...`),
    /// unlike `s3://`'s bare-bucket authority. The relativize/reconstruct round
    /// trip must still be lossless: relativizing an under-root `abfss://` file
    /// path against its table root and reconstructing via the scan UDF's join
    /// rule must reproduce the original URI byte-for-byte, exactly like the
    /// `s3://` case above.
    #[test]
    fn abfss_paths_relativize_and_reconstruct_losslessly() {
        let root = "abfss://container@account.dfs.core.windows.net/db/table";
        let original = format!("{root}/data/part-0.parquet");

        let relative = relativize_path_to_root(&original, root);
        assert_eq!(
            relative, "data/part-0.parquet",
            "abfss path under the root must relativize just like s3"
        );

        let reconstructed = reconstruct_abs_uri_mirror(&relative, root);
        assert_eq!(
            reconstructed, original,
            "reconstructed abfss URI must equal the original byte-for-byte"
        );
    }

    // ---------------------------------------------------------------------------
    // Pre-existing helpers tests (unchanged)
    // ---------------------------------------------------------------------------

    #[test]
    fn empty_file_list_returns_empty_select() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let resp = empty_pushdown_sql(&proj, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("WHERE 1=0"));
        assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
    }

    /// A pruned query with repeated literals in the projection (e.g.
    /// `SELECT 1, name, 1 ... WHERE <all files pruned>`) keeps unique EMITS
    /// aliases via `emits_ident`: the two `Expr` positions get distinct
    /// positional synthetic names, never a duplicated `AS "1"` collision
    /// (issue #190).
    #[test]
    fn empty_pushdown_sql_repeated_literals_unique_aliases() {
        let proj_cols: Vec<ProjectionItem> = vec![
            ProjectionItem::Expr { expr: "1".into() },
            ProjectionItem::Column("NAME".into()),
            ProjectionItem::Expr { expr: "1".into() },
        ];
        let proj_types = vec![
            "DECIMAL(18,0)".to_string(),
            "VARCHAR(2000000)".to_string(),
            "DECIMAL(18,0)".to_string(),
        ];
        let resp = empty_pushdown_sql(&proj_cols, &proj_types);
        let sql = resp["sql"].as_str().unwrap();

        assert_eq!(
            sql.matches("CAST(NULL AS").count(),
            3,
            "must emit three CAST(NULL AS ...) items, one per select-list item: {sql}"
        );
        assert!(
            sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_0"))),
            "position 0's literal must get a positional-unique alias: {sql}"
        );
        assert!(
            sql.contains(&format!("AS {}", quote_ident("NAME"))),
            "the column item must keep its real quoted name: {sql}"
        );
        assert!(
            sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_2"))),
            "position 2's literal must get a distinct positional-unique alias: {sql}"
        );
        assert!(
            !sql.contains(&format!("AS {}", quote_ident("1"))),
            "must never alias a literal by its rendered value text (would collide): {sql}"
        );
    }

    /// Single-group empty result: one row, per-`AggKind` literal cast to its
    /// declared type — COUNT → `0`, SUM → `NULL` — with no `WHERE 1=0` (a bare
    /// `FROM DUAL` already yields exactly one row).
    #[test]
    fn empty_agg_sql_emits_zero_and_null_row_cast_to_declared_types() {
        let items = vec![
            SingleGroupItem::Aggregate(AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }),
            SingleGroupItem::Aggregate(AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            }),
        ];
        let types = vec!["DECIMAL(18,0)".to_string(), "DECIMAL(36,2)".to_string()];
        let resp = empty_agg_sql(&items, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("FROM DUAL"), "must select from DUAL: {sql}");
        assert!(
            !sql.contains("WHERE 1=0"),
            "single-group empty is one row, not zero rows: {sql}"
        );
        assert!(
            sql.contains("CAST(0 AS DECIMAL(18,0))"),
            "COUNT empty literal must be 0 cast to declared type: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS DECIMAL(36,2))"),
            "SUM empty literal must be NULL cast to declared type: {sql}"
        );
    }

    /// COUNT(DISTINCT) over zero files yields a plain `0` literal row — no distinct
    /// fan-out, no scan, and no merge step (with zero files there is nothing to scan
    /// or deduplicate).
    #[test]
    fn empty_agg_sql_count_distinct_emits_zero_no_merge_udf() {
        let items = vec![SingleGroupItem::Distinct(DistinctCount {
            column: Some("ID".into()),
            arg_expr: None,
        })];
        let types = vec!["DECIMAL(18,0)".to_string()];
        let resp = empty_agg_sql(&items, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert_eq!(
            sql, "SELECT CAST(0 AS DECIMAL(18,0)) FROM DUAL",
            "COUNT(DISTINCT) over zero files must be a plain 0 literal row with no fan-out \
             or merge step: {sql}"
        );
    }

    /// Issue #57 shape-consistency (task 6.7): when EVERY file is pruned, a Case 2/3
    /// single-group request (more than one `COUNT(DISTINCT)`, or a distinct mixed with
    /// an ordinary aggregate) must return the SAME N-aggregate-column shape
    /// (`empty_agg_sql`, one column per select item) that the non-empty qualified
    /// single-table wrapper returns — NEVER the full-row empty shape
    /// (`empty_pushdown_sql`), whose different column count trips Exasol's positional
    /// pushdown validation (`sqlCode 04000`, "Expected number of columns is N but
    /// pushdown query has M"), since Exasol never re-aggregates a declined pushdown.
    #[test]
    fn empty_case_2_3_matches_non_empty_aggregate_shape() {
        fn count_top_level_cols(select_span: &str) -> usize {
            let mut depth = 0i32;
            let mut cols = 1usize;
            for ch in select_span.chars() {
                match ch {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    ',' if depth == 0 => cols += 1,
                    _ => {}
                }
            }
            cols
        }

        // Case 3: two COUNT(DISTINCT) + one ordinary SUM → N = 3 output columns.
        let pushdown_req = serde_json::json!({
            "selectList": [
                agg_item("COUNT", Some("A"), true),
                agg_item("COUNT", Some("B"), true),
                agg_item("SUM", Some("C"), false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 18, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 0},
                {"type": "decimal", "precision": 36, "scale": 2},
            ],
        });
        let col_types = vec![
            ("A".to_string(), "DECIMAL(18,0)".to_string()),
            ("B".to_string(), "DECIMAL(18,0)".to_string()),
            ("C".to_string(), "DECIMAL(36,2)".to_string()),
        ];

        // The fixture must be a Case 2/3 shape: distinct present, but not a lone one
        // (so the non-empty path declines the fan-out and routes to the wrapper).
        let items = detect_aggregates(&pushdown_req).expect("a Case 3 select list detects");
        assert!(
            super::super::single_group_agg::has_distinct(&items)
                && !super::super::single_group_agg::is_lone_count_distinct(&items),
            "the fixture must be a Case 2/3 shape"
        );
        let n = pushdown_req["selectList"].as_array().unwrap().len();

        // A deliberately WIDER full-row projection (5 columns): if the empty dispatch
        // wrongly returned the full-row shape, its column count would be 5, not N = 3.
        let proj_cols: Vec<ProjectionItem> = ["A", "B", "C", "D", "E"]
            .iter()
            .map(|c| ProjectionItem::from(*c))
            .collect();
        let proj_types = vec![
            "DECIMAL(18,0)".to_string(),
            "DECIMAL(18,0)".to_string(),
            "DECIMAL(36,2)".to_string(),
            "VARCHAR(10)".to_string(),
            "VARCHAR(10)".to_string(),
        ];

        let empty = empty_result_sql(&pushdown_req, &proj_cols, &proj_types, false, &col_types)
            .expect("empty Case 2/3 result must build");
        let empty_sql = empty["sql"].as_str().unwrap();

        // Routes to the N-aggregate-column shape (empty_agg_sql), NOT the full-row shape.
        let direct = empty_agg_sql(&items, &aggregate_exasol_types(&pushdown_req));
        assert_eq!(
            empty_sql,
            direct["sql"].as_str().unwrap(),
            "the empty Case 2/3 dispatch must route to empty_agg_sql: {empty_sql}"
        );
        assert_ne!(
            empty_sql,
            empty_pushdown_sql(&proj_cols, &proj_types)["sql"]
                .as_str()
                .unwrap(),
            "the empty Case 2/3 dispatch must NOT return the full-row empty shape (#57): {empty_sql}"
        );

        // Exactly N columns — the same one-per-select-item shape the non-empty wrapper
        // returns, so empty and non-empty column shapes never diverge.
        let select_span = &empty_sql["SELECT ".len()..empty_sql.find(" FROM").expect("has FROM")];
        assert_eq!(
            count_top_level_cols(select_span),
            n,
            "the empty shape must have exactly N={n} aggregate columns (one per select \
             item): {empty_sql}"
        );
        // COUNT(DISTINCT) over zero files → 0; the ordinary SUM → NULL, each cast to
        // its declared type.
        assert!(
            empty_sql.contains("CAST(0 AS DECIMAL(18,0))")
                && empty_sql.contains("CAST(NULL AS DECIMAL(36,2))"),
            "COUNT(DISTINCT) empties to 0 and the ordinary SUM to NULL: {empty_sql}"
        );
    }

    /// Every non-COUNT `AggKind` maps to the `NULL` empty literal — single-node
    /// SQL semantics over zero rows (only the COUNT family yields `0`).
    #[test]
    fn empty_agg_literal_maps_non_count_kinds_to_null() {
        for kind in [
            AggKind::Sum,
            AggKind::Min,
            AggKind::Max,
            AggKind::Avg,
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            assert_eq!(
                empty_agg_literal(&kind),
                "NULL",
                "{kind:?} empty literal must be NULL"
            );
        }
        for kind in [AggKind::Count, AggKind::CountCol] {
            assert_eq!(
                empty_agg_literal(&kind),
                "0",
                "{kind:?} empty literal must be 0"
            );
        }
    }

    /// Grouped empty result: zero rows (`WHERE 1=0`) with one `CAST(NULL AS <ty>)`
    /// per grouped output column, assembled in select-list order.
    #[test]
    fn empty_grouped_sql_emits_zero_rows_in_grouped_shape() {
        let select_items = vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 1,
            },
        ];
        let group_key_types = vec!["DECIMAL(20,0)".to_string()];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
        let sql = resp["sql"].as_str().unwrap();
        assert!(
            sql.contains("WHERE 1=0"),
            "grouped empty is zero rows: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS DECIMAL(20,0))"),
            "group-key column typed from group_key_types: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS DECIMAL(18,0))"),
            "aggregate column typed from aggregate_types: {sql}"
        );
        let select_clause = sql
            .strip_prefix("SELECT ")
            .and_then(|s| s.split(" FROM").next())
            .unwrap();
        assert_eq!(
            select_clause.matches("CAST(NULL AS").count(),
            2,
            "one output column per grouped select item: {sql}"
        );
    }

    /// A `GroupedSelectItem::Constant` (Exasol's "count the groups" literal
    /// rewrite) reuses its already-rendered projection expression verbatim,
    /// slotted into select-list order alongside the group-key and aggregate
    /// columns — it contributes no aggregate plan and is not re-typed here.
    #[test]
    fn empty_grouped_sql_includes_constant_projection_column() {
        let select_items = vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::Constant {
                select_index: 1,
                projection: "CAST(NULL AS BOOLEAN)".to_string(),
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 2,
            },
        ];
        let group_key_types = vec!["DECIMAL(20,0)".to_string()];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
        let sql = resp["sql"].as_str().unwrap();
        let select_clause = sql
            .strip_prefix("SELECT ")
            .and_then(|s| s.split(" FROM").next())
            .unwrap();
        let columns: Vec<&str> = select_clause.split(", ").collect();
        assert_eq!(
            columns,
            vec![
                "CAST(NULL AS DECIMAL(20,0))",
                "CAST(NULL AS BOOLEAN)",
                "CAST(NULL AS DECIMAL(18,0))",
            ],
            "constant column is reused verbatim in select-list order: {sql}"
        );
    }

    /// Dispatch priority mirrors the non-empty path: grouped first, then
    /// single-group aggregate (only when `validate_agg_col_types` passes), then
    /// row scan.
    #[test]
    fn empty_result_sql_dispatches_by_plan_shape() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(18,2)".to_string())];

        let grouped = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "K"}],
            "selectList": [
                {"type": "column", "name": "K"},
                agg_item("COUNT", None, false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 0},
            ],
        });
        let grouped_sql = empty_result_sql(&grouped, &proj, &proj_types, false, &col_types)
            .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            grouped_sql.contains("WHERE 1=0"),
            "grouped shape is zero rows: {grouped_sql}"
        );

        let single = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), false)],
            "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
        });
        let single_sql = empty_result_sql(&single, &proj, &proj_types, false, &col_types).unwrap()
            ["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            single_sql.contains("FROM DUAL") && !single_sql.contains("WHERE 1=0"),
            "single-group shape is one row: {single_sql}"
        );
        assert!(single_sql.contains("CAST(NULL AS DECIMAL(36,2))"));

        // Non-numeric SUM target demotes to the row-scan empty shape (gate honored).
        let non_numeric = serde_json::json!({
            "selectList": [agg_item("SUM", Some("name"), false)],
            "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
        });
        let non_numeric_col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
        let row_sql = empty_result_sql(
            &non_numeric,
            &proj,
            &proj_types,
            false,
            &non_numeric_col_types,
        )
        .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            row_sql.contains("CAST(NULL AS DECIMAL(20,0))") && row_sql.contains(&quote_ident("ID")),
            "non-numeric single-group aggregate must fall through to the row-scan shape: {row_sql}"
        );
    }

    /// A grouped aggregate over a non-numeric column with all files pruned no longer
    /// demotes to the full-row empty shape: since issue #82's fix, a grouped request
    /// that cannot push down (here, a non-numeric SUM with no HAVING) routes on the
    /// NON-empty path to the qualified single-table wrapper, whose output columns are
    /// the `selectList` items. The empty path must MIRROR that shape — a zero-row
    /// result typed per `selectListDataTypes` (the `selectList` column count/types),
    /// NOT the full base row — so the empty and non-empty shapes never diverge.
    #[test]
    fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

        let grouped_non_numeric = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "K"}],
            "selectList": [
                {"type": "column", "name": "K"},
                agg_item("SUM", Some("name"), false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 36, "scale": 2},
            ],
        });

        let row_sql = empty_result_sql(&grouped_non_numeric, &proj, &proj_types, false, &col_types)
            .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            row_sql,
            "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
            "declined grouped aggregate over zero files must produce the selectList-typed \
             empty shape (matching the qualified wrapper), not the full base row"
        );
    }

    /// A non-numeric grouped aggregate that also carries a HAVING no longer hard
    /// errors: the classifier routes it to `GroupByWrapper` (the HAVING renders
    /// natively over the wrapper rather than being dropped), so the empty path must
    /// mirror the SAME selectList-typed empty shape as the no-HAVING sibling above,
    /// not an `Err`.
    #[test]
    fn empty_files_grouped_non_numeric_aggregate_with_having_yields_typed_empty() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

        let grouped_having = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "K"}],
            "selectList": [
                {"type": "column", "name": "K"},
                agg_item("SUM", Some("name"), false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 36, "scale": 2},
            ],
            "having": {"type": "predicate_greater"},
        });

        let row_sql = empty_result_sql(&grouped_having, &proj, &proj_types, false, &col_types)
            .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            row_sql,
            "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
            "declined grouped aggregate with HAVING over zero files must produce the same \
             selectList-typed empty shape as the wrapper it now falls through to, not an error"
        );
    }

    /// A row-scan request whose derived projection WIDENED to the full base row is
    /// routed on the non-empty path to the qualified single-table wrapper, whose
    /// output columns are the `selectList` items (#196). The empty path must mirror
    /// that shape — one `selectListDataTypes`-typed zero-row column — never the
    /// wider full base row, whose column count trips Exasol's positional `04000`
    /// check. The widening signal alone decides this: the identical request with a
    /// non-widened projection still gets the full-row shape.
    #[test]
    fn empty_result_sql_widened_row_scan_uses_select_list_types() {
        let pushdown_req = serde_json::json!({
            "selectList": [
                {"type": "function_scalar", "name": "LENGTH", "arguments": [
                    {"type": "column", "name": "SCORE", "tableName": "T"}]},
            ],
            "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
        });
        let col_types = vec![
            ("ID".to_string(), "DECIMAL(20,0)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        // No aggregate anywhere, so the shared classifier picks `RowScan` — the arm
        // under test, not the `GroupByWrapper` arm that already emits this shape.
        assert!(
            matches!(
                classify_request_shape(&pushdown_req, &col_types),
                RequestShape::RowScan
            ),
            "the fixture must classify as RowScan for this test to exercise its arm"
        );

        // The widened projection IS the full base row: three columns for one item.
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into(), "SCORE".into()];
        let proj_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();

        let widened = empty_result_sql(&pushdown_req, &proj, &proj_types, true, &col_types)
            .expect("the widened empty row-scan result must build")["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            widened, "SELECT CAST(NULL AS DECIMAL(18,0)) FROM DUAL WHERE 1=0",
            "a widened row-scan projection over zero files must produce ONE \
             selectListDataTypes-typed column, not the 3-column base row: {widened}"
        );

        let not_widened = empty_result_sql(&pushdown_req, &proj, &proj_types, false, &col_types)
            .expect("the non-widened empty row-scan result must build")["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            not_widened,
            empty_pushdown_sql(&proj, &proj_types)["sql"]
                .as_str()
                .unwrap(),
            "the non-widened path must stay byte-identical to the full-row empty \
             shape: {not_widened}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 2.2 — `parse_name_mapping` flattens `schema.name-mapping.default`
    // ---------------------------------------------------------------------------

    /// A representative `schema.name-mapping.default` payload — mirroring the
    /// Iceberg spec's own example shape — flattens to one `NameMappingEntry` per
    /// TOP-LEVEL name. Multi-name entries expand to one entry per name (Avro field
    /// aliases); an entry's nested `fields` children are excluded, but the entry's
    /// OWN top-level name(s) are still included; an entry with no `field-id` at
    /// all (schema-only, not present in imported files) is fully excluded.
    #[test]
    fn resolves_name_mapping_flat_entries_once() {
        let raw = r#"
        [
            { "field-id": 1, "names": ["id", "record_id"] },
            {
                "field-id": 3,
                "names": ["location"],
                "fields": [
                    { "field-id": 4, "names": ["latitude", "lat"] },
                    { "field-id": 5, "names": ["longitude", "long"] }
                ]
            },
            { "names": ["schema_only_no_field_id"] }
        ]
        "#;

        let entries = parse_name_mapping(Some(raw)).expect("valid name-mapping JSON must parse");

        assert_eq!(
            entries,
            vec![
                NameMappingEntry {
                    name: "id".to_string(),
                    field_id: 1,
                },
                NameMappingEntry {
                    name: "record_id".to_string(),
                    field_id: 1,
                },
                NameMappingEntry {
                    name: "location".to_string(),
                    field_id: 3,
                },
            ],
            "multi-name entry expands per name; nested `fields` children (lat/lat, \
             long/long) are excluded while the parent's own top-level name is kept; \
             the id-less entry is fully excluded"
        );
    }

    /// An absent `schema.name-mapping.default` property (`None`) yields an empty
    /// mapping, not an error — a table with no name-mapping is the common,
    /// fully-supported case.
    #[test]
    fn absent_name_mapping_is_empty() {
        assert_eq!(
            parse_name_mapping(None).expect("absent property must not error"),
            Vec::new()
        );
    }

    /// A present-but-malformed `schema.name-mapping.default` value fails loud with
    /// a clean, credential-free plan-time error that names the offending property.
    #[test]
    fn malformed_name_mapping_errors_cleanly() {
        let err = parse_name_mapping(Some("{ not valid json mapping shape"))
            .expect_err("malformed name-mapping JSON must error");

        let msg = match err {
            UdfError::User(m) => m,
            other => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            msg.contains(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING),
            "error must name the offending property: {msg}"
        );
        assert!(
            !msg.contains("access_key") && !msg.contains("secret_key"),
            "error must not leak credentials: {msg}"
        );
    }
}
