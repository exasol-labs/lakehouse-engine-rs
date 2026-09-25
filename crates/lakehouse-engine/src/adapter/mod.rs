//! Credential values never appear in error messages.

pub mod capabilities;
pub mod catalog_kind;
pub mod connection;
pub mod direct_storage;
pub mod direct_storage_properties;
pub mod iceberg_predicate;
pub mod parquet_directory;
pub mod pushdown;
#[cfg(test)]
#[path = "pushdown_surface_probe_tests.rs"]
mod pushdown_surface_probe;
pub mod sharding;
pub mod tables;

use crate::adapter::capabilities::get_capabilities_response;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::connection::{catalog_block, read_connection, storage_block};
use crate::adapter::direct_storage::DirectStorageCatalogClient;
use crate::adapter::pushdown::handle_pushdown;
use crate::adapter::tables::{catalog_identifier_string, flatten_table_name};
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
    CatalogClient, CatalogListing, CatalogTableIdent, IcebergRestCatalogClient, SkipReason,
    SkippedTable, UnityCatalogSession,
};
use serde_json::{Value as Json, json};
use std::collections::HashMap;
use std::num::NonZeroUsize;

const PROP_NAMESPACE: &str = "NAMESPACE";
// CONNECTION whose address is the catalog URI and whose password is the credential JSON.
const PROP_CATALOG_CONNECTION: &str = "CATALOG_CONNECTION";
const PROP_ALLOW_HTTP: &str = "ALLOW_HTTP";
const PROP_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
const NOTE_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
const DEFAULT_PARALLELISM_FACTOR: usize = 8;
const PROP_DF_TARGET_PARTITIONS: &str = "DATAFUSION_TARGET_PARTITIONS";
const PROP_DF_THREADS_PER_UDF: &str = "DATAFUSION_THREADS_PER_UDF";
const PROP_DF_THREADING_MODE: &str = "DATAFUSION_THREADING_MODE";
const NOTE_DF_TARGET_PARTITIONS: &str = "DF_TARGET_PARTITIONS";
const NOTE_DF_THREADS_PER_UDF: &str = "DF_THREADS_PER_UDF";
const NOTE_DF_THREADING_MODE: &str = "DF_THREADING_MODE";
/// Pushdown-path fallback when the adapterNote is absent or unparseable.
const DEFAULT_DF_TARGET_PARTITIONS: usize = 1;
/// Pushdown-path fallback when the adapterNote is absent or unparseable.
const DEFAULT_DF_THREADS_PER_UDF: usize = 1;
const PROP_DF_BATCH_SIZE: &str = "DATAFUSION_BATCH_SIZE";
const NOTE_DF_BATCH_SIZE: &str = "DF_BATCH_SIZE";
/// Pushdown-path fallback; matches DataFusion's default rows per RecordBatch.
const DEFAULT_DF_BATCH_SIZE: usize = 8192;
const PROP_MEMORY_POOL_FRACTION: &str = "MEMORY_POOL_FRACTION";
const PROP_INSTANCE_OVERHEAD_MB: &str = "INSTANCE_OVERHEAD_MB";
const NOTE_MEMORY_POOL_FRACTION: &str = "MEMORY_POOL_FRACTION";
const NOTE_INSTANCE_OVERHEAD_MB: &str = "INSTANCE_OVERHEAD_MB";
/// Fraction of the net per-instance RSS budget allocated to the DataFusion memory pool.
const DEFAULT_MEMORY_POOL_FRACTION: f64 = 0.6;
/// Fixed container/binary overhead (MB) subtracted from the per-instance RSS limit before
/// applying the pool fraction.
const DEFAULT_INSTANCE_OVERHEAD_MB: u64 = 200;
// A join's smaller side is broadcast into every shard when its total file bytes are at or below
// this threshold; larger joins fall back to an unaccelerated two-scan join.
const PROP_JOIN_BROADCAST_MAX_BYTES: &str = "JOIN_BROADCAST_MAX_BYTES";
const NOTE_JOIN_BROADCAST_MAX_BYTES: &str = "JOIN_BROADCAST_MAX_BYTES";
const DEFAULT_JOIN_BROADCAST_MAX_BYTES: u64 = 134_217_728;
// Mirrors the native `IMPORT FROM PARQUET` `MaxConnections` knob.
const PROP_S3_MAX_CONNECTIONS: &str = "S3_MAX_CONNECTIONS";
const NOTE_S3_MAX_CONNECTIONS: &str = "S3_MAX_CONNECTIONS";
/// Connections per decode thread: S3 range GETs are latency-bound, so several in flight per
/// thread keep the NIC busy, and idle pooled connections are far cheaper than threads.
const S3_CONNECTIONS_PER_THREAD: usize = 4;
const NOTE_TABLE_MAP: &str = "TABLE_MAP";

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
            // ctx.connection() is a blocking connect-back round-trip, so resolve it before
            // building the tokio runtime.
            let props = get_properties(request);
            let config = resolve_connection_config(ctx, &props)?;
            let script_schema = ctx.script_schema();
            let cluster_nodes = cluster_nodes_from_context(ctx);

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;
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

pub struct ResolvedConnectionConfig {
    pub(crate) catalog_uri: String,
    pub(crate) storage: StorageBackend,
    pub(crate) creds: ConnectionCreds,
    pub(crate) allow_http: bool,
    pub(crate) catalog_kind: CatalogKind,
    pub(crate) connection_name: String,
    pub(crate) sealed_storage_key: Option<SealedStorageKey>,
}

/// `ctx.connection()` is synchronous and must be called before entering any async runtime.
fn resolve_connection_config(
    ctx: &dyn UdfContext,
    props: &Json,
) -> Result<ResolvedConnectionConfig, UdfError> {
    let kind = catalog_kind::resolve_catalog_kind(props)?;
    let connection_name = nonempty_str(props, PROP_CATALOG_CONNECTION)
        .ok_or_else(|| UdfError::User("CATALOG_CONNECTION is required".into()))?;
    let resolved = read_connection(ctx, Some(connection_name), kind)?;
    let allow_http = nonempty_str(props, PROP_ALLOW_HTTP)
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let storage = storage_block(&resolved.creds, allow_http);
    Ok(ResolvedConnectionConfig {
        catalog_uri: resolved.uri,
        storage,
        creds: resolved.creds,
        allow_http,
        catalog_kind: kind,
        connection_name: connection_name.to_string(),
        sealed_storage_key: resolved.sealed_storage_key,
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
    let config = resolve_connection_config(ctx, &props)?;

    // Optional under direct storage: the CONNECTION address alone denotes the storage subtree.
    let configured_ns: Vec<String> = match config.catalog_kind {
        CatalogKind::DirectStorage => Vec::new(),
        _ => {
            let namespace = nonempty_str(&props, PROP_NAMESPACE).ok_or_else(|| {
                UdfError::User(format!("property '{PROP_NAMESPACE}' is required"))
            })?;
            namespace.split('.').map(|s| s.to_string()).collect()
        }
    };

    let nr_of_cores = resolve_nr_of_cores();
    let parallelism_factor = resolve_parallelism_factor(&props, nr_of_cores);
    // The file-count clamp is unknown here, so the un-clamped factor (maximal per-node fan-out)
    // keeps the AUTO thread and connection budgets from oversubscribing a node.
    let df_threading_mode = resolve_threading_mode(&props);
    let (df_target_partitions, df_threads_per_udf) =
        resolve_df_threading(df_threading_mode, &props, nr_of_cores, parallelism_factor);
    let df_batch_size = resolve_df_batch_size(&props);
    let memory_pool_fraction = resolve_memory_pool_fraction(&props);
    let instance_overhead_mb = resolve_instance_overhead_mb(&props);
    let join_broadcast_max_bytes = resolve_join_broadcast_max_bytes(&props);
    let s3_max_connections = resolve_s3_max_connections(&props, nr_of_cores, parallelism_factor);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;

    let client = construct_catalog_client(
        config.catalog_kind,
        config.catalog_uri,
        config.storage.clone(),
        config.creds,
        &props,
    )
    .map_err(|e| redact_error(&config.storage, e))?;
    let listing = rt
        .block_on(async { client.list_tables(&configured_ns).await })
        .map_err(|e| redact_error(&config.storage, e))?;

    let engine_timestamps = EngineTimestampSupport::from_database_version(&ctx.database_version());

    let (tables_json, table_map, skipped) =
        build_listing_virtual_tables(&configured_ns, &listing, engine_timestamps)
            .map_err(|e| redact_error(&config.storage, e))?;

    for entry in &skipped {
        udf_log!(ctx, warn, "{}", skip_warning(entry));
    }

    let adapter_notes = build_adapter_notes(
        request,
        parallelism_factor,
        df_threading_mode,
        df_target_partitions,
        df_threads_per_udf,
        df_batch_size,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        join_broadcast_max_bytes,
        &table_map,
    );

    let schema_metadata = json!({
        "tables": tables_json,
        "adapterNotes": adapter_notes,
    });

    Ok(build_schema_response(request, schema_metadata))
}

fn skip_warning(entry: &SkippedTable) -> String {
    match &entry.reason {
        SkipReason::NotLoadableIcebergTable => format!(
            "createVirtualSchema: skipping non-Iceberg table '{}' (catalog reported it is not a loadable Iceberg table)",
            catalog_identifier_string(&entry.ident)
        ),
        SkipReason::NotDeltaBaseTable { detail } => format!(
            "createVirtualSchema: skipping non-Delta-base entry '{}' ({})",
            catalog_identifier_string(&entry.ident),
            detail
        ),
        SkipReason::NoDataFile => format!(
            "createVirtualSchema: skipping directory '{}' (holds no data file)",
            catalog_identifier_string(&entry.ident)
        ),
    }
}

/// `requestedTables` is echoed only because the protocol requires mirroring request fields;
/// Exasol applies the full `tables` list to the whole namespace regardless (verified live).
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
    let df_target_partitions = adapter_note(request, NOTE_DF_TARGET_PARTITIONS)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_DF_TARGET_PARTITIONS);
    let df_batch_size = adapter_note(request, NOTE_DF_BATCH_SIZE)
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_DF_BATCH_SIZE);
    let df_threads_per_udf = adapter_note(request, NOTE_DF_THREADS_PER_UDF)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_DF_THREADS_PER_UDF);
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

fn read_table_map(request: &Json) -> HashMap<String, String> {
    parse_adapter_notes(request)
        .get(NOTE_TABLE_MAP)
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn build_table_map(
    configured_ns: &[String],
    idents: &[CatalogTableIdent],
) -> Result<Vec<(String, String)>, UdfError> {
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut table_map: Vec<(String, String)> = Vec::with_capacity(idents.len());
    for ident in idents {
        let exasol_name = flatten_table_name(configured_ns, ident);
        let catalog_id = catalog_identifier_string(ident);
        if let Some(existing) = seen.get(&exasol_name) {
            return Err(UdfError::User(format!(
                "table name collision: '{exasol_name}' maps to both '{existing}' and '{catalog_id}'"
            )));
        }
        seen.insert(exasol_name.clone(), catalog_id.clone());
        table_map.push((exasol_name, catalog_id));
    }
    Ok(table_map)
}

type VirtualTables = (Vec<Json>, Vec<(String, String)>, Vec<SkippedTable>);

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
        CatalogKind::IcebergRest => Ok(Box::new(IcebergRestCatalogClient::new(
            catalog_uri,
            storage,
            creds,
        ))),
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
    }
}

/// The full-Unicode `to_uppercase` fold is a deliberate Exasol-target trade-off: `ß` expands to
/// `SS`, so two columns differing only in that expansion collapse to one name with no check.
fn build_listing_virtual_tables(
    configured_ns: &[String],
    listing: &CatalogListing,
    engine_timestamps: EngineTimestampSupport,
) -> Result<VirtualTables, UdfError> {
    let mut tables_json: Vec<Json> = Vec::with_capacity(listing.tables.len());
    let mut survivors: Vec<CatalogTableIdent> = Vec::with_capacity(listing.tables.len());

    for table in &listing.tables {
        let exasol_name = flatten_table_name(configured_ns, &table.ident);
        let columns: Vec<Json> = table
            .columns
            .iter()
            .map(|col| {
                json!({
                    "name": col.name.to_uppercase(),
                    "dataType": exasol_type_to_json(&column_source_type_to_exasol(
                        &col.source_type,
                        engine_timestamps,
                    )),
                })
            })
            .collect();
        tables_json.push(json!({
            "name": exasol_name,
            "columns": columns,
        }));
        survivors.push(table.ident.clone());
    }

    let table_map = build_table_map(configured_ns, &survivors)?;
    Ok((tables_json, table_map, listing.skipped.clone()))
}

/// Errors when the name is absent from `TABLE_MAP` rather than scanning a different or stale table.
fn resolve_pushdown_identifier(request: &Json) -> Result<String, UdfError> {
    let involved_table_name = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|t| t.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| UdfError::User("pushdown request missing involvedTables[0].name".into()))?;

    read_table_map(request)
        .get(involved_table_name)
        .cloned()
        .ok_or_else(|| {
            UdfError::User(format!(
                "pushdown: virtual table '{involved_table_name}' is not in TABLE_MAP; \
                 drop and recreate the virtual schema"
            ))
        })
}

/// Exasol rejects a raw-object adapterNotes, so it is a JSON string; pre-existing notes are merged.
// Args mirror the notes fields one-to-one; a params struct is boilerplate for one private callee.
#[allow(clippy::too_many_arguments)]
fn build_adapter_notes(
    request: &Json,
    parallelism_factor: usize,
    df_threading_mode: ThreadingMode,
    df_target_partitions: usize,
    df_threads_per_udf: usize,
    df_batch_size: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
    table_map: &[(String, String)],
) -> Json {
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
    let map_obj: serde_json::Map<String, Json> = table_map
        .iter()
        .map(|(k, v)| (k.clone(), Json::String(v.clone())))
        .collect();
    notes.insert(NOTE_TABLE_MAP.to_string(), Json::Object(map_obj));
    Json::String(Json::Object(notes).to_string())
}

fn resolve_parallelism_factor(props: &Json, nr_of_cores: u32) -> usize {
    nonempty_str(props, PROP_PARALLELISM_FACTOR)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| ((nr_of_cores as usize) * 2).max(DEFAULT_PARALLELISM_FACTOR))
}

/// Planning-time only: just the resulting integers reach the scan UDF, which stays mode-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThreadingMode {
    Auto,
    Fixed,
}

impl ThreadingMode {
    fn as_note(self) -> &'static str {
        match self {
            ThreadingMode::Auto => "AUTO",
            ThreadingMode::Fixed => "FIXED",
        }
    }
}

fn resolve_threading_mode(props: &Json) -> ThreadingMode {
    match nonempty_str(props, PROP_DF_THREADING_MODE) {
        Some(s) if s.eq_ignore_ascii_case("FIXED") => ThreadingMode::Fixed,
        _ => ThreadingMode::Auto,
    }
}

fn resolve_df_threading(
    mode: ThreadingMode,
    props: &Json,
    nr_of_cores: u32,
    udf_instances_per_node: usize,
) -> (usize, usize) {
    match mode {
        ThreadingMode::Fixed => (
            resolve_df_fixed_count(props, PROP_DF_TARGET_PARTITIONS, nr_of_cores),
            resolve_df_fixed_count(props, PROP_DF_THREADS_PER_UDF, nr_of_cores),
        ),
        ThreadingMode::Auto => {
            let threads = auto_threads_per_udf(nr_of_cores, udf_instances_per_node);
            (threads, threads)
        }
    }
}

/// Guarantees `instances × threads ≤ nr_of_cores` whenever cores ≥ instances; otherwise each
/// instance stays single-threaded and the engine multiplexes the surplus onto the core pool.
fn auto_threads_per_udf(nr_of_cores: u32, udf_instances_per_node: usize) -> usize {
    let instances = udf_instances_per_node.max(1);
    ((nr_of_cores as usize) / instances).max(1)
}

fn resolve_df_fixed_count(props: &Json, key: &str, nr_of_cores: u32) -> usize {
    nonempty_str(props, key)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| (nr_of_cores as usize).max(1))
}

/// An explicit positive value wins; otherwise the aggregate per-node budget is
/// ≈ `nr_of_cores × S3_CONNECTIONS_PER_THREAD` however the node is sharded into instances.
fn resolve_s3_max_connections(
    props: &Json,
    nr_of_cores: u32,
    udf_instances_per_node: usize,
) -> usize {
    if let Some(explicit) = nonempty_str(props, PROP_S3_MAX_CONNECTIONS)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
    {
        return explicit;
    }

    let per_instance_threads = auto_threads_per_udf(nr_of_cores, udf_instances_per_node);
    per_instance_threads * S3_CONNECTIONS_PER_THREAD
}

fn resolve_df_batch_size(props: &Json) -> usize {
    nonempty_str(props, PROP_DF_BATCH_SIZE)
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_DF_BATCH_SIZE)
}

fn resolve_memory_pool_fraction(props: &Json) -> f64 {
    nonempty_str(props, PROP_MEMORY_POOL_FRACTION)
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&x| x > 0.0 && x <= 1.0)
        .unwrap_or(DEFAULT_MEMORY_POOL_FRACTION)
}

fn resolve_instance_overhead_mb(props: &Json) -> u64 {
    nonempty_str(props, PROP_INSTANCE_OVERHEAD_MB)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INSTANCE_OVERHEAD_MB)
}

fn resolve_join_broadcast_max_bytes(props: &Json) -> u64 {
    nonempty_str(props, PROP_JOIN_BROADCAST_MAX_BYTES)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_JOIN_BROADCAST_MAX_BYTES)
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
