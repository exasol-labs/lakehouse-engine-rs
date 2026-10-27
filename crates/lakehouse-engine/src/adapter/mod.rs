//! Credential values never appear in error messages.

pub mod capabilities;
pub mod catalog_kind;
pub mod column_notes;
pub mod connection;
pub mod direct_storage;
pub mod direct_storage_properties;
pub mod iceberg_predicate;
pub mod parquet_directory;
mod permission;
mod properties;
pub mod pushdown;
#[cfg(test)]
#[path = "pushdown_surface_probe_tests.rs"]
mod pushdown_surface_probe;
pub mod scoped_refresh;
pub mod sharding;
pub mod skipped_tables;
pub mod table_identity;
pub mod tables;

use crate::adapter::capabilities::get_capabilities_response;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::column_notes::checked_note;
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::connection::{catalog_block, read_connection, storage_block};
use crate::adapter::direct_storage::DirectStorageCatalogClient;
use crate::adapter::permission::{PermissionCheck, PermissionSettings};
use crate::adapter::properties::{
    PROP_CATALOG_CONNECTION, PROP_NAMESPACE, ThreadingMode, TuningProperties, check_properties,
    resolve_allow_http,
};
use crate::adapter::pushdown::{declare_columns, handle_pushdown};
use crate::adapter::scoped_refresh::{RequestedTables, ScopedRefresh};
use crate::adapter::skipped_tables::{SkippedTables, insert_skipped_tables, skip_kind_and_reason};
use crate::adapter::table_identity::{encode_identifier_note, read_identifier};
use crate::adapter::tables::{
    catalog_identifier_string, flatten_table_name, reject_name_collisions,
};
use crate::scan::sealed::SealedStorageKey;
use crate::scan::spec::DEFAULT_S3_MAX_CONNECTIONS;
use crate::scan::spec::StorageBackend;
use crate::types::mapping::{
    EngineTimestampSupport, column_source_type_to_exasol, exasol_type_to_json,
};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::udf_log;
use lakehouse_catalog::{
    CatalogClient, CatalogListing, GlueCatalogSession, IcebergRestCatalogClient, SkippedTable,
    UnityCatalogSession, resolve_aws_identity,
};
use serde_json::{Value as Json, json};
use std::num::NonZeroUsize;

const NOTE_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
const DEFAULT_PARALLELISM_FACTOR: usize = 1;
const NOTE_DF_TARGET_PARTITIONS: &str = "DF_TARGET_PARTITIONS";
const NOTE_DF_THREADS_PER_UDF: &str = "DF_THREADS_PER_UDF";
const NOTE_DF_THREADING_MODE: &str = "DF_THREADING_MODE";
const NOTE_DF_BATCH_SIZE: &str = "DF_BATCH_SIZE";
/// Pushdown-path fallback; matches DataFusion's default rows per RecordBatch.
const DEFAULT_DF_BATCH_SIZE: usize = 8192;
const NOTE_MEMORY_POOL_FRACTION: &str = "MEMORY_POOL_FRACTION";
const NOTE_INSTANCE_OVERHEAD_MB: &str = "INSTANCE_OVERHEAD_MB";
/// Fraction of the net per-instance RSS budget allocated to the DataFusion memory pool.
const DEFAULT_MEMORY_POOL_FRACTION: f64 = 0.6;
/// Fixed container/binary overhead (MB) subtracted from the per-instance RSS limit before
/// applying the pool fraction.
const DEFAULT_INSTANCE_OVERHEAD_MB: u64 = 200;
const NOTE_JOIN_BROADCAST_MAX_BYTES: &str = "JOIN_BROADCAST_MAX_BYTES";
const DEFAULT_JOIN_BROADCAST_MAX_BYTES: u64 = 134_217_728;
const NOTE_S3_MAX_CONNECTIONS: &str = "S3_MAX_CONNECTIONS";
/// Connections per decode thread: S3 range GETs are latency-bound, so several in flight per
/// thread keep the NIC busy, and idle pooled connections are far cheaper than threads.
const S3_CONNECTIONS_PER_THREAD: usize = 4;

pub fn adapter_call(ctx: &mut dyn UdfContext, json_arg: &str) -> Result<String, UdfError> {
    let request: Json = serde_json::from_str(json_arg)
        .map_err(|e| UdfError::User(format!("VS request is not valid JSON: {e}")))?;
    let response = dispatch(ctx, &request)?;
    Ok(response.to_string())
}

fn dispatch(ctx: &mut dyn UdfContext, request: &Json) -> Result<Json, UdfError> {
    match request.get("type").and_then(|t| t.as_str()) {
        Some("getCapabilities") => Ok(get_capabilities_response()),
        Some("createVirtualSchema") => handle_create_virtual_schema(ctx, request),
        Some("refresh") => {
            // Stateless: re-resolve the schema, same as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("setProperties") => {
            // Stateless: re-resolve the schema with the new properties, same as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("dropVirtualSchema") => Ok(json!({"type": "dropVirtualSchema"})),
        Some("pushdown") => {
            let props = get_properties(request);
            // The user is mapped before the CONNECTION is read, so a refused user costs no request.
            let permission = PermissionSettings::parse(&props)?;
            let check = permission.check_for(|| ctx.current_user())?;
            // Built before `resolve_connection_config`, which blocks on it to assume the role.
            let rt = build_runtime()?;
            let config = resolve_connection_config(ctx, &props, &permission, check, &rt)?;
            let script_schema = ctx.script_schema();
            let cluster_nodes = cluster_nodes_from_context(ctx);

            rt.block_on(async {
                handle_pushdown_request(request, &config, &script_schema, cluster_nodes).await
            })
        }
        other => Err(UdfError::User(format!(
            "unsupported VS request type: {}",
            other.unwrap_or("(none)")
        ))),
    }
}

/// `creds` and `storage` carry the AWS identity the request acts as — a named
/// role's session, not the stated key pair.
pub struct ResolvedConnectionConfig {
    pub(crate) catalog_uri: String,
    pub(crate) storage: StorageBackend,
    pub(crate) creds: ConnectionCreds,
    pub(crate) allow_http: bool,
    pub(crate) catalog_kind: CatalogKind,
    pub(crate) connection_name: String,
    pub(crate) sealed_storage_key: Option<SealedStorageKey>,
    pub(crate) permission_check: PermissionCheck,
}

fn build_runtime() -> Result<tokio::runtime::Runtime, UdfError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))
}

/// Both entry points call this, so neither reaches the catalog or storage before
/// a named role is assumed; `rt` then assumes the role once per request.
fn resolve_connection_config(
    ctx: &dyn UdfContext,
    props: &Json,
    permission: &PermissionSettings,
    check: PermissionCheck,
    rt: &tokio::runtime::Runtime,
) -> Result<ResolvedConnectionConfig, UdfError> {
    let kind = catalog_kind::resolve_catalog_kind(props)?;
    permission.require_supported_kind(kind)?;
    let connection_name = nonempty_str(props, PROP_CATALOG_CONNECTION)
        .ok_or_else(|| UdfError::User("CATALOG_CONNECTION is required".into()))?;
    let resolved = read_connection(ctx, Some(connection_name), kind)?;
    permission.require_management_url(&resolved.uri)?;
    let allow_http = resolve_allow_http(props);
    let creds = rt.block_on(resolve_aws_identity(
        resolved.creds,
        &resolved.uri,
        allow_http,
    ))?;
    let storage = storage_block(&creds, allow_http);
    Ok(ResolvedConnectionConfig {
        catalog_uri: resolved.uri,
        storage,
        creds,
        allow_http,
        catalog_kind: kind,
        connection_name: connection_name.to_string(),
        sealed_storage_key: resolved.sealed_storage_key,
        permission_check: check,
    })
}

fn handle_create_virtual_schema(
    ctx: &mut dyn UdfContext,
    request: &Json,
) -> Result<Json, UdfError> {
    // `setProperties` carries ALTER ... SET values, which must win over the persisted ones.
    let props = if request.get("type").and_then(|t| t.as_str()) == Some("setProperties") {
        merge_set_properties(request)
    } else {
        get_properties(request)
    };
    let checked = check_properties(&props)?;
    // Validated only: listing runs as the CONNECTION's identity, with no permission check.
    let permission = PermissionSettings::parse(&props)?;
    let rt = build_runtime()?;
    let config = resolve_connection_config(ctx, &props, &permission, PermissionCheck::Off, &rt)?;

    // Optional under direct storage: the CONNECTION address alone denotes the storage subtree.
    let configured_ns: Vec<String> = match config.catalog_kind {
        CatalogKind::DirectStorage => Vec::new(),
        CatalogKind::IcebergRest | CatalogKind::UnityCatalogNative | CatalogKind::Glue => {
            let namespace = nonempty_str(&props, PROP_NAMESPACE).ok_or_else(|| {
                UdfError::User(format!("property '{PROP_NAMESPACE}' is required"))
            })?;
            namespace.split('.').map(|s| s.to_string()).collect()
        }
    };

    let client = construct_catalog_client(
        config.catalog_kind,
        config.catalog_uri,
        config.storage.clone(),
        config.creds,
        &props,
    )
    .map_err(|e| redact_error(&config.storage, e))?;
    let (listing, skipped_tables) = rt
        .block_on(list_source(client.as_ref(), &configured_ns, request))
        .map_err(|e| redact_error(&config.storage, e))?;

    let engine_timestamps = EngineTimestampSupport::from_database_version(&ctx.database_version());

    let tables_json = build_listing_virtual_tables(&configured_ns, &listing, engine_timestamps)
        .map_err(|e| redact_error(&config.storage, e))?;

    for entry in &listing.skipped {
        udf_log!(ctx, warn, "{}", skip_warning(entry));
    }

    let tuning = resolve_tuning_notes(&checked, resolve_nr_of_cores());
    let adapter_notes = build_adapter_notes(request, &tuning, &skipped_tables);

    let schema_metadata = json!({
        "tables": tables_json,
        "adapterNotes": adapter_notes,
    });

    Ok(build_schema_response(request, schema_metadata))
}

/// A partial refresh lists identifiers only and loads just the requested tables, so a broken
/// unrequested table cannot fail it. It reads no identifier note: each name resolves afresh.
async fn list_source(
    client: &dyn CatalogClient,
    configured_ns: &[String],
    request: &Json,
) -> Result<(CatalogListing, SkippedTables), UdfError> {
    let Some(requested) = RequestedTables::from_request(request)? else {
        let listing = client.list_tables(configured_ns).await?;
        let skipped_tables = SkippedTables::listed(&listing.skipped);
        return Ok((listing, skipped_tables));
    };
    let listed = client.list_table_idents(configured_ns).await?;
    let scope = ScopedRefresh::new(&requested, configured_ns, &listed);
    let listing = client.load_listed_tables(&scope.selection()).await?;
    let persisted = SkippedTables::persisted(&parse_adapter_notes(request));
    let skipped_tables = scope.merge_skipped_tables(&listing.skipped, &persisted);
    Ok((listing, skipped_tables))
}

fn skip_warning(entry: &SkippedTable) -> String {
    let (skipped_kind, reason) = skip_kind_and_reason(entry);
    format!(
        "createVirtualSchema: skipping {skipped_kind} '{}' ({reason})",
        catalog_identifier_string(&entry.ident),
    )
}

/// The `requestedTables` echo marks a partial refresh, which Exasol scopes to the requested names:
/// it replaces or creates each returned table, drops a requested one the response omits, and keeps
/// every other virtual table (verified live, #425).
fn build_schema_response(request: &Json, schema_metadata: Json) -> Json {
    let response_type = request
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("createVirtualSchema");
    let mut response = json!({
        "type": response_type,
        "schemaMetadata": schema_metadata,
    });
    if let Some(requested_tables) = request.get("requestedTables") {
        response["requestedTables"] = requested_tables.clone();
    }
    response
}

async fn handle_pushdown_request(
    request: &Json,
    config: &ResolvedConnectionConfig,
    script_schema: &str,
    cluster_nodes: usize,
) -> Result<Json, UdfError> {
    // Tuning values come from adapterNotes, which Exasol persists; properties are dropped.
    let parallelism_factor = adapter_note(request, NOTE_PARALLELISM_FACTOR)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_PARALLELISM_FACTOR);
    let (auto_partitions, auto_threads) = auto_df_threading(resolve_nr_of_cores());
    let df_target_partitions = adapter_note(request, NOTE_DF_TARGET_PARTITIONS)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(auto_partitions);
    let df_batch_size = adapter_note(request, NOTE_DF_BATCH_SIZE)
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_DF_BATCH_SIZE);
    let df_threads_per_udf = adapter_note(request, NOTE_DF_THREADS_PER_UDF)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(auto_threads);
    let memory_pool_fraction = adapter_note(request, NOTE_MEMORY_POOL_FRACTION)
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&x| x > 0.0 && x <= 1.0)
        .unwrap_or(DEFAULT_MEMORY_POOL_FRACTION);
    let instance_overhead_mb = adapter_note(request, NOTE_INSTANCE_OVERHEAD_MB)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INSTANCE_OVERHEAD_MB);
    let s3_max_connections = adapter_note(request, NOTE_S3_MAX_CONNECTIONS)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_S3_MAX_CONNECTIONS);
    let join_broadcast_max_bytes = adapter_note(request, NOTE_JOIN_BROADCAST_MAX_BYTES)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_JOIN_BROADCAST_MAX_BYTES);

    let iceberg_identifier = resolve_pushdown_identifier(request)?;
    let catalog = catalog_block(&config.creds, &iceberg_identifier);

    handle_pushdown(
        request,
        config,
        &catalog,
        Some(script_schema),
        cluster_nodes,
        parallelism_factor,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        join_broadcast_max_bytes,
    )
    .await
    .map_err(|e| redact_error(&config.storage, e))
}

/// `schemaMetadataInfo.properties` wins on conflict.
fn get_properties(request: &Json) -> Json {
    let mut merged = match request.get("properties") {
        Some(Json::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Some(Json::Object(smi)) = request.get("schemaMetadataInfo")
        && let Some(Json::Object(props)) = smi.get("properties")
    {
        for (k, v) in props {
            merged.insert(k.clone(), v.clone());
        }
    }
    Json::Object(merged)
}

/// Inverse precedence of [`get_properties`]: request values win, and a `null` removes the
/// property, so a null-unset required property fails its check instead of keeping its old value.
fn merge_set_properties(request: &Json) -> Json {
    let mut merged = match request
        .get("schemaMetadataInfo")
        .and_then(|smi| smi.get("properties"))
    {
        Some(Json::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Some(Json::Object(props)) = request.get("properties") {
        for (k, v) in props {
            if v.is_null() {
                merged.remove(k);
            } else {
                merged.insert(k.clone(), v.clone());
            }
        }
    }
    Json::Object(merged)
}

fn nonempty_str<'a>(obj: &'a Json, key: &str) -> Option<&'a str> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

/// adapterNotes arrives as a JSON-encoded string, not an object.
fn parse_adapter_notes(request: &Json) -> serde_json::Map<String, Json> {
    request
        .get("schemaMetadataInfo")
        .and_then(|smi| smi.get("adapterNotes"))
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
        .and_then(|s| serde_json::from_str::<Json>(s).ok())
        .and_then(|v| match v {
            Json::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default()
}

fn adapter_note(request: &Json, key: &str) -> Option<String> {
    parse_adapter_notes(request)
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// The only site that matches a [`CatalogKind`]; the listing pipeline after it never re-matches
/// the kind.
fn construct_catalog_client(
    kind: CatalogKind,
    catalog_uri: String,
    storage: StorageBackend,
    creds: ConnectionCreds,
    props: &Json,
) -> Result<Box<dyn CatalogClient>, UdfError> {
    match kind {
        CatalogKind::IcebergRest => Ok(Box::new(IcebergRestCatalogClient::new(catalog_uri, creds))),
        CatalogKind::UnityCatalogNative => {
            Ok(Box::new(UnityCatalogSession::new(&catalog_uri, creds)))
        }
        CatalogKind::DirectStorage => {
            let resolved =
                direct_storage_properties::resolve_direct_storage_properties(props, &catalog_uri)?;
            let secrets = storage.secret_values();
            let client = DirectStorageCatalogClient::new(
                &storage,
                &resolved.base_path,
                resolved.directory_options(),
                &secrets,
            )?;
            Ok(Box::new(client))
        }
        CatalogKind::Glue => Ok(Box::new(GlueCatalogSession::new(
            &catalog_uri,
            storage,
            creds,
        )?)),
    }
}

/// The full-Unicode `to_uppercase` fold is a deliberate Exasol-target trade-off: `ß` expands to
/// `SS`, so two columns differing only in that expansion collapse to one name with no check.
fn build_listing_virtual_tables(
    configured_ns: &[String],
    listing: &CatalogListing,
    engine_timestamps: EngineTimestampSupport,
) -> Result<Vec<Json>, UdfError> {
    reject_name_collisions(configured_ns, listing.tables.iter().map(|t| &t.ident))?;
    let mut tables_json: Vec<Json> = Vec::with_capacity(listing.tables.len());

    for table in &listing.tables {
        let exasol_name = flatten_table_name(configured_ns, &table.ident);
        let columns = table
            .columns
            .iter()
            .zip(declare_columns(table))
            .map(|(col, note)| {
                let column_name = col.name.to_uppercase();
                let mut column = json!({
                    "name": column_name,
                    "dataType": exasol_type_to_json(&column_source_type_to_exasol(
                        &col.source_type,
                        engine_timestamps,
                    )),
                });
                if let Some(note) = note {
                    column["adapterNotes"] =
                        Json::String(checked_note(&exasol_name, &column_name, note)?);
                }
                Ok(column)
            })
            .collect::<Result<Vec<Json>, UdfError>>()?;
        tables_json.push(json!({
            "name": exasol_name,
            "adapterNotes": encode_identifier_note(&table.ident),
            "columns": columns,
        }));
    }

    Ok(tables_json)
}

fn resolve_pushdown_identifier(request: &Json) -> Result<String, UdfError> {
    let involved_table_name = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|t| t.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| UdfError::User("pushdown request missing involvedTables[0].name".into()))?;

    read_identifier(request, involved_table_name)
}

/// The tuning a schema request resolves from its properties; pushdowns read it back from the notes.
struct TuningNotes {
    parallelism_factor: usize,
    df_threading_mode: ThreadingMode,
    df_target_partitions: usize,
    df_threads_per_udf: usize,
    df_batch_size: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
}

/// Exasol rejects a raw-object adapterNotes, so it is a JSON string; pre-existing notes are merged.
fn build_adapter_notes(request: &Json, tuning: &TuningNotes, skipped: &SkippedTables) -> Json {
    let &TuningNotes {
        parallelism_factor,
        df_threading_mode,
        df_target_partitions,
        df_threads_per_udf,
        df_batch_size,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        join_broadcast_max_bytes,
    } = tuning;
    let mut notes = parse_adapter_notes(request);
    notes.insert(
        NOTE_PARALLELISM_FACTOR.to_string(),
        Json::String(parallelism_factor.to_string()),
    );
    notes.insert(
        NOTE_DF_THREADING_MODE.to_string(),
        Json::String(df_threading_mode.as_note().to_string()),
    );
    notes.insert(
        NOTE_DF_TARGET_PARTITIONS.to_string(),
        Json::String(df_target_partitions.to_string()),
    );
    notes.insert(
        NOTE_DF_THREADS_PER_UDF.to_string(),
        Json::String(df_threads_per_udf.to_string()),
    );
    notes.insert(
        NOTE_DF_BATCH_SIZE.to_string(),
        Json::String(df_batch_size.to_string()),
    );
    notes.insert(
        NOTE_MEMORY_POOL_FRACTION.to_string(),
        Json::String(memory_pool_fraction.to_string()),
    );
    notes.insert(
        NOTE_INSTANCE_OVERHEAD_MB.to_string(),
        Json::String(instance_overhead_mb.to_string()),
    );
    notes.insert(
        NOTE_S3_MAX_CONNECTIONS.to_string(),
        Json::String(s3_max_connections.to_string()),
    );
    notes.insert(
        NOTE_JOIN_BROADCAST_MAX_BYTES.to_string(),
        Json::String(join_broadcast_max_bytes.to_string()),
    );
    insert_skipped_tables(&mut notes, skipped);
    Json::String(Json::Object(notes).to_string())
}

impl ThreadingMode {
    fn as_note(self) -> &'static str {
        match self {
            ThreadingMode::Auto => "AUTO",
            ThreadingMode::Fixed => "FIXED",
        }
    }
}

/// Applies each absent property's default and the AUTO/FIXED derivation. Absent FIXED counts take
/// the AUTO values, and an absent `S3_MAX_CONNECTIONS` is `S3_CONNECTIONS_PER_THREAD` per thread.
fn resolve_tuning_notes(checked: &TuningProperties, nr_of_cores: u32) -> TuningNotes {
    let df_threading_mode = checked.threading_mode.unwrap_or(ThreadingMode::Auto);
    let (auto_partitions, auto_threads) = auto_df_threading(nr_of_cores);
    let (df_target_partitions, df_threads_per_udf) = match df_threading_mode {
        ThreadingMode::Fixed => (
            checked
                .df_target_partitions
                .map_or(auto_partitions, |n| usize::from(n.get())),
            checked
                .df_threads_per_udf
                .map_or(auto_threads, |n| usize::from(n.get())),
        ),
        ThreadingMode::Auto => (auto_partitions, auto_threads),
    };
    TuningNotes {
        parallelism_factor: checked
            .parallelism_factor
            .map_or(DEFAULT_PARALLELISM_FACTOR, |n| usize::from(n.get())),
        df_threading_mode,
        df_target_partitions,
        df_threads_per_udf,
        df_batch_size: checked
            .df_batch_size
            .map_or(DEFAULT_DF_BATCH_SIZE, |n| n.get() as usize),
        memory_pool_fraction: checked
            .memory_pool_fraction
            .unwrap_or(DEFAULT_MEMORY_POOL_FRACTION),
        instance_overhead_mb: checked
            .instance_overhead_mb
            .map_or(DEFAULT_INSTANCE_OVERHEAD_MB, u64::from),
        s3_max_connections: checked
            .s3_max_connections
            .map_or(df_threads_per_udf * S3_CONNECTIONS_PER_THREAD, |n| {
                usize::from(n.get())
            }),
        join_broadcast_max_bytes: checked
            .join_broadcast_max_bytes
            .map_or(DEFAULT_JOIN_BROADCAST_MAX_BYTES, |n| n.get()),
    }
}

/// Exasol runs one scalar scan VM per node, so that instance owns every core. Partitions
/// beyond threads overlap object-store I/O with decoding.
fn auto_df_threading(nr_of_cores: u32) -> (usize, usize) {
    let threads = (nr_of_cores as usize).max(1);
    (2 * threads, threads)
}

/// `available_parallelism()` honours the CPU quota of the adapter VM's container.
fn resolve_nr_of_cores() -> u32 {
    core_count_or_default(std::thread::available_parallelism())
}

fn core_count_or_default(detected: std::io::Result<NonZeroUsize>) -> u32 {
    detected.map_or(1, |n| n.get() as u32)
}

/// `0` (stub or missing handshake) maps to `1`, the single-shard fallback.
fn cluster_nodes_from_context(ctx: &dyn UdfContext) -> usize {
    match ctx.node_count() {
        0 => 1,
        n => n as usize,
    }
}

/// Value-based stripping runs first to catch secrets the label-based heuristic misses.
fn redact_error(storage: &StorageBackend, e: UdfError) -> UdfError {
    match e {
        UdfError::User(msg) => {
            let stripped = crate::scan::emit::redact_secret_values(&msg, &storage.secret_values());
            UdfError::User(crate::scan::emit::redact_credentials(&stripped))
        }
        other => other,
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "catalog_client_tests.rs"]
mod catalog_client_tests;

#[cfg(test)]
#[path = "unity_schema_tests.rs"]
mod unity_schema_tests;
