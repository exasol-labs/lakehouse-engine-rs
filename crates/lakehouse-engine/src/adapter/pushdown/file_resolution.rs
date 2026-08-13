use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    AggKind, CatalogProps, DeleteFileContentType, DeleteFileRef, FileEntry, LogicalField,
    NameMappingEntry, ProjectionItem, StorageBackend,
};
use crate::types::mapping::{exasol_representable_catalog_decimal, exasol_type_from_json};
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::TableIdent;
use serde_json::Value as Json;

use lakehouse_catalog::{
    CatalogSession, StaticStoreAddress, load_table_any_auth, parse_table_ident, redact_credentials,
    redact_error_text, resolve_vended_storage,
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
            if exasol_representable_catalog_decimal(*precision, *scale) =>
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
/// `creds.use_vended_credentials` — orthogonal to the catalog-auth mode. When it is
/// true, `resolve_vended_storage` builds the whole `StorageBackend` from the loadTable
/// response, the anchor's URI scheme, and the CONNECTION's store ADDRESS alone.
/// Credentials stay vended-only — one the catalog does not vend is an error here
/// rather than a silent fall-back to the static one — while addressing may cross
/// over: the CONNECTION's `endpoint` and `region` reach the selector through a
/// [`StaticStoreAddress`], which cannot carry a credential, and each wins over the
/// vended value independently when the CONNECTION sets it. When it is false, returns
/// `(files, storage.clone())` — byte-identical to the no-vending behaviour on every
/// auth mode.
///
/// An empty table `location` is rejected above the vended/static split, so both
/// values of `use_vended_credentials` report the identical error.
///
/// Every error surfaced from here on is redacted against the secret values of the
/// EFFECTIVE storage, not the static one: the `file_io` built from it is what talks
/// to object storage, so those are exactly the values an underlying provider error
/// can echo back.
///
/// `allow_http` is the resolved `ALLOW_HTTP` virtual-schema property — already baked
/// into the static `storage`, and the vended selector's consent gate for plaintext
/// transport.
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

    // Resolve the effective storage (vended or static). The anchor is the TABLE'S OWN
    // location: what `storage_credentials[*].prefix` is matched against, and the sole
    // input the backend variant is read from. Nothing else can stand in — the catalog
    // REST URI names no object store, and the REST `warehouse` is a routing identifier.
    let table_location = result.metadata.location();
    if table_location.is_empty() {
        return Err(UdfError::User(format!(
            "the loadTable response for table '{}' carries an EMPTY table `location`; the \
             catalog `warehouse` is a routing identifier, not a table location, and is not a \
             valid substitute",
            catalog_props.table
        )));
    }
    // Own the table root before `result.metadata` is moved into the table builder
    // below. Returned so the adapter can carry it once in the common blob and emit
    // per-shard file paths relative to it (non-empty, per the guard above).
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
#[path = "file_resolution_tests.rs"]
mod tests;
