/// VS adapter logic: createVirtualSchema, getCapabilities, pushdown,
/// refresh, setProperties, dropVirtualSchema.
///
/// Credentials (access_key, secret_key, session_token) NEVER appear in error messages.
pub mod capabilities;
pub mod connection;
pub mod iceberg_predicate;
pub mod pushdown;
#[cfg(test)]
mod pushdown_surface_probe;
pub mod sharding;
pub mod tables;

use crate::adapter::capabilities::get_capabilities_response;
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::connection::{catalog_block, read_connection, storage_block};
use crate::adapter::pushdown::{handle_pushdown, resolve_table_schema};
use crate::adapter::tables::{flatten_table_name, iceberg_identifier_string};
use crate::scan::spec::DEFAULT_S3_MAX_CONNECTIONS;
use crate::scan::spec::StorageProps;
use crate::types::mapping::exasol_type_to_json;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::udf_log;
use lakehouse_catalog::{CatalogSession, list_namespace_tables};
use serde_json::{Value as Json, json};
use std::collections::HashMap;

// The Iceberg namespace to expose. Replaces the old TABLE_NAME property.
// (TABLE is an Exasol reserved keyword; ICEBERG_NAMESPACE is not.)
const PROP_ICEBERG_NAMESPACE: &str = "ICEBERG_NAMESPACE";
// Required: name of the Exasol CONNECTION object that holds the catalog URI
// (as its address) and the credential JSON (as its password).
const PROP_CATALOG_CONNECTION: &str = "CATALOG_CONNECTION";
// Allow HTTP to the catalog/storage endpoint (opt-in; defaults to false).
const PROP_ALLOW_HTTP: &str = "ALLOW_HTTP";
// Key written into the createVirtualSchema response under
// schemaMetadata.adapterNotes (a stringified JSON object) so that subsequent
// requests (pushdown, refresh, setProperties) can read the resolved node count back from
// `schemaMetadataInfo.adapterNotes`.
//
// adapterNotes is used rather than schemaMetadata.properties because Exasol
// (2025.2.1) does NOT persist adapter-returned schemaMetadata.properties — they
// are silently dropped and never appear in any catalog view. adapterNotes, by
// contrast, is persisted at the schema level, passed back in
// schemaMetadataInfo.adapterNotes, and is queryable via
// SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES. Exasol requires adapterNotes to be
// a JSON *string* (a raw JSON object fails with "No valid json string").
const NOTE_CLUSTER_NODES: &str = "CLUSTER_NODES";
// adapterNotes key for the per-node CPU core count captured at createVirtualSchema time.
const NOTE_NR_OF_CORES: &str = "NR_OF_CORES";
// VS property name for the parallelism factor (oversubscription multiplier).
// Default: max(NR_OF_CORES * 2, 8). Stored in adapterNotes so the pushdown path
// can read it back.
const PROP_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
const NOTE_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
/// Minimum parallelism factor (floor applied when NR_OF_CORES is 0 or very small).
const DEFAULT_PARALLELISM_FACTOR: usize = 8;
// VS property name for the per-node CPU core count override. When set to a
// positive integer it is used directly; absent, empty, zero, or non-numeric
// values fall through to the `available_parallelism()` auto-detect path.
const PROP_NR_OF_CORES: &str = "NR_OF_CORES";
// VS property names for DataFusion per-instance thread configuration.
const PROP_DF_TARGET_PARTITIONS: &str = "DATAFUSION_TARGET_PARTITIONS";
const PROP_DF_THREADS_PER_UDF: &str = "DATAFUSION_THREADS_PER_UDF";
// VS/connection property selecting how the DataFusion thread/partition budget is
// derived: AUTO (adapter derives a non-oversubscribing per-instance budget) or
// FIXED (operator-supplied values used verbatim). Default AUTO. Case-insensitive.
const PROP_DF_THREADING_MODE: &str = "DATAFUSION_THREADING_MODE";
// adapterNotes keys for the DataFusion thread configuration.
const NOTE_DF_TARGET_PARTITIONS: &str = "DF_TARGET_PARTITIONS";
const NOTE_DF_THREADS_PER_UDF: &str = "DF_THREADS_PER_UDF";
// adapterNotes key recording the resolved threading mode (AUTO|FIXED).
const NOTE_DF_THREADING_MODE: &str = "DF_THREADING_MODE";
/// Pushdown-path fallback for `target_partitions` when the adapterNote is absent or
/// unparseable. (The createVirtualSchema default is now `max(nr_of_cores, 1)` — see
/// `resolve_df_fixed_count`.)
const DEFAULT_DF_TARGET_PARTITIONS: usize = 1;
/// Pushdown-path fallback for threads-per-UDF when the adapterNote is absent or
/// unparseable. (The createVirtualSchema default is now `max(nr_of_cores, 1)` — see
/// `resolve_df_fixed_count`.)
const DEFAULT_DF_THREADS_PER_UDF: usize = 1;
// VS property and adapterNotes key names for the DataFusion batch_size parameter.
const PROP_DF_BATCH_SIZE: &str = "DATAFUSION_BATCH_SIZE";
const NOTE_DF_BATCH_SIZE: &str = "DF_BATCH_SIZE";
/// Pushdown-path fallback for `batch_size` when the adapterNote is absent or unparseable.
/// Matches DataFusion's own default of 8192 rows per RecordBatch.
const DEFAULT_DF_BATCH_SIZE: usize = 8192;
// VS property and adapterNotes key names for the DataFusion memory pool sizing parameters.
const PROP_MEMORY_POOL_FRACTION: &str = "MEMORY_POOL_FRACTION";
const PROP_INSTANCE_OVERHEAD_MB: &str = "INSTANCE_OVERHEAD_MB";
const NOTE_MEMORY_POOL_FRACTION: &str = "MEMORY_POOL_FRACTION";
const NOTE_INSTANCE_OVERHEAD_MB: &str = "INSTANCE_OVERHEAD_MB";
/// Fraction of the net per-instance RSS budget allocated to the DataFusion memory pool.
const DEFAULT_MEMORY_POOL_FRACTION: f64 = 0.6;
/// Fixed container/binary overhead (MB) subtracted from the per-instance RSS limit before
/// applying the pool fraction.
const DEFAULT_INSTANCE_OVERHEAD_MB: u64 = 200;
// VS property and adapterNotes key names for the join-broadcast byte-size threshold: the
// smaller side of a two-table inner equi-join is broadcast (replicated into every shard's
// common spec) when its Iceberg-manifest byte size is at or below this threshold; larger
// joins fall back to an unaccelerated two-scan join. See backlog BL-001 / plan
// `add-join-pushdown-broadcast`.
const PROP_JOIN_BROADCAST_MAX_BYTES: &str = "JOIN_BROADCAST_MAX_BYTES";
const NOTE_JOIN_BROADCAST_MAX_BYTES: &str = "JOIN_BROADCAST_MAX_BYTES";
/// Default join-broadcast byte-size threshold: 128 MiB.
const DEFAULT_JOIN_BROADCAST_MAX_BYTES: u64 = 134_217_728;
// VS/connection property and adapterNotes key for the object-store connection-concurrency
// budget (mirrors the native `IMPORT FROM PARQUET` `MaxConnections` vocabulary). An explicit
// positive integer pins the per-instance budget (FIXED-like); absent/empty/zero/invalid
// triggers the AUTO derivation in `resolve_s3_max_connections`.
const PROP_S3_MAX_CONNECTIONS: &str = "S3_MAX_CONNECTIONS";
const NOTE_S3_MAX_CONNECTIONS: &str = "S3_MAX_CONNECTIONS";
/// AUTO-mode oversubscription multiplier: concurrent object-store connections per DataFusion
/// decode thread. S3 fetches are latency-bound (each byte-range GET spends most of its wall
/// clock waiting on a network round-trip), so a decode thread that fetched one range at a time
/// would leave the NIC idle between requests. By Little's law the concurrency needed to fill a
/// pipe is `bandwidth × latency`, which for S3-class latency and NIC bandwidth is several
/// in-flight requests per thread — and since an idle pooled TCP connection is far cheaper than
/// an OS thread, the connection budget can be a small multiple of the thread budget rather than
/// a 1:1 mirror. `4` keeps enough requests in flight to hide S3 latency while staying bounded.
const S3_CONNECTIONS_PER_THREAD: usize = 4;
// adapterNotes key for the Exasol-name → Iceberg-identifier map persisted at create time.
const NOTE_TABLE_MAP: &str = "TABLE_MAP";

/// Main adapter dispatch function.
///
/// Signature matches the `vs_adapter(fn)` macro requirement:
/// `fn(&mut dyn UdfContext, &str) -> Result<String, UdfError>`.
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
            // Stateless: refresh = re-resolve schema, same as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("setProperties") => {
            // Stateless: setProperties = re-resolve schema with the new
            // properties applied, same enumeration as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("dropVirtualSchema") => Ok(json!({"type": "dropVirtualSchema"})),
        Some("pushdown") => {
            // Resolve credentials synchronously before entering the async runtime.
            // ctx.connection() is a synchronous call that must not be invoked inside
            // an async context (it may block on the UDF host). Mirror the pattern
            // used by resolve_cluster_nodes. ctx.script_schema() is likewise a
            // synchronous handshake read, and is the schema that qualifies the
            // scan/distributor/merge UDF names in the generated pushdown SQL.
            let props = get_properties(request);
            let (catalog_uri, storage, creds) = resolve_connection_config(ctx, &props)?;
            let script_schema = ctx.script_schema();

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;
            rt.block_on(async {
                handle_pushdown_request(request, &catalog_uri, &storage, &creds, &script_schema)
                    .await
            })
        }
        other => Err(UdfError::User(format!(
            "unsupported VS request type: {}",
            other.unwrap_or("(none)")
        ))),
    }
}

/// Resolve the catalog/storage configuration from the `CATALOG_CONNECTION` object.
///
/// Shared by the createVirtualSchema and pushdown entry points. `ctx.connection()`
/// is synchronous and must be called before entering any async runtime.
/// Table identity is no longer fixed at config-resolution time; callers build
/// `CatalogProps` with the specific per-table identifier when known.
fn resolve_connection_config(
    ctx: &dyn UdfContext,
    props: &Json,
) -> Result<(String, StorageProps, ConnectionCreds), UdfError> {
    let resolved = read_connection(ctx, nonempty_str(props, PROP_CATALOG_CONNECTION))?;
    let mut storage = storage_block(&resolved.creds);
    storage.allow_http = nonempty_str(props, PROP_ALLOW_HTTP)
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    Ok((resolved.uri, storage, resolved.creds))
}

fn handle_create_virtual_schema(
    ctx: &mut dyn UdfContext,
    request: &Json,
) -> Result<Json, UdfError> {
    // `setProperties` must let the incoming ALTER ... SET values win over the
    // persisted properties (and delete on an explicit null); every other request
    // type keeps the pushdown-oriented persisted-wins precedence.
    let props = if request.get("type").and_then(|t| t.as_str()) == Some("setProperties") {
        merge_set_properties(request)
    } else {
        get_properties(request)
    };
    let (catalog_uri, storage, creds) = resolve_connection_config(ctx, &props)?;

    let iceberg_namespace = nonempty_str(&props, PROP_ICEBERG_NAMESPACE)
        .ok_or_else(|| UdfError::User(format!("property '{PROP_ICEBERG_NAMESPACE}' is required")))?
        .to_string();

    let configured_ns: Vec<String> = iceberg_namespace
        .split('.')
        .map(|s| s.to_string())
        .collect();

    let (cluster_nodes, nr_of_cores) = resolve_cluster_nodes(ctx, &props);
    let parallelism_factor = resolve_parallelism_factor(&props, nr_of_cores);
    // At createVirtualSchema the file list is not yet known, so the per-node UDF
    // instance share cannot use the file-count clamp. Before that clamp,
    // G = node_count × parallelism_factor distributes round-robin, so the per-node
    // share is exactly `parallelism_factor`. Using the un-clamped factor is the
    // conservative choice for AUTO derivation: it assumes the maximal per-node
    // fan-out, so the derived thread budget never oversubscribes a node even when
    // the shard fan-out reaches its configured maximum.
    let df_threading_mode = resolve_threading_mode(&props);
    let (df_target_partitions, df_threads_per_udf) =
        resolve_df_threading(df_threading_mode, &props, nr_of_cores, parallelism_factor);
    let df_batch_size = resolve_df_batch_size(&props);
    let memory_pool_fraction = resolve_memory_pool_fraction(&props);
    let instance_overhead_mb = resolve_instance_overhead_mb(&props);
    let join_broadcast_max_bytes = resolve_join_broadcast_max_bytes(&props);
    // Same un-clamped `parallelism_factor` used as `udf_instances_per_node` above:
    // the conservative maximal per-node fan-out keeps the AUTO connection budget
    // from oversubscribing a node even at the configured shard-fan-out ceiling.
    let s3_max_connections = resolve_s3_max_connections(&props, nr_of_cores, parallelism_factor);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;

    let table_idents = rt
        .block_on(async {
            list_namespace_tables(&catalog_uri, &configured_ns, &storage, &creds).await
        })
        .map_err(|e| redact_error(&storage, e))?;

    let (tables_json, table_map, skipped_idents) =
        resolve_namespace_virtual_tables(&rt, &catalog_uri, &creds, &configured_ns, &table_idents)
            .map_err(|e| redact_error(&storage, e))?;

    for ident in &skipped_idents {
        udf_log!(
            ctx,
            warn,
            "createVirtualSchema: skipping non-Iceberg table '{}' (catalog reported it is not a loadable Iceberg table)",
            iceberg_identifier_string(ident)
        );
    }

    // Build adapterNotes including TABLE_MAP (merge, not clobber).
    let adapter_notes = build_adapter_notes(
        request,
        cluster_nodes,
        nr_of_cores,
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

/// Resolve every enumerated table's schema into its `createVirtualSchema` entry.
///
/// Owns the catalog session for the whole enumeration, so the schema loop's OAuth2
/// grant and `/v1/config` lookup each run once for the namespace instead of once per
/// table — sound because a session is scoped to one `(catalog_uri, warehouse)` tuple
/// and every table here shares both, varying only `CatalogProps.table`.
///
/// An EMPTY namespace builds NO session and therefore makes no catalog contact at
/// all. That input is reachable (a namespace with no tables) and it is what the
/// pre-hoist per-table loop cost for it: hoisting the build unconditionally would
/// charge an empty enumeration an OAuth2 grant it never needed and would fail a
/// request that used to succeed with an empty table list. With at least one table the
/// build happens before the loop, so a grant failure surfaces once for the whole
/// request rather than at whichever table happened to be resolved first.
///
/// `list_namespace_tables` keeps its own independent auth path and is deliberately
/// NOT folded onto this session, so on the OAuth2 client-credentials mode its
/// `RestCatalog` grant remains and such a request still performs two grants in total,
/// not one.
///
/// Errors propagate unredacted — no `ctx` and no `StorageProps` reach here, so the
/// caller applies the same `redact_error` the old inline loop applied per-table, once
/// over the whole enumeration result, preserving the no-credential-leak guarantee.
fn resolve_namespace_virtual_tables(
    rt: &tokio::runtime::Runtime,
    catalog_uri: &str,
    creds: &ConnectionCreds,
    configured_ns: &[String],
    table_idents: &[iceberg::TableIdent],
) -> Result<VirtualTables, UdfError> {
    if table_idents.is_empty() {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }

    let session =
        rt.block_on(async { CatalogSession::resolve(catalog_uri, &creds.warehouse, creds).await })?;

    build_virtual_tables(
        configured_ns,
        table_idents,
        |ident: &iceberg::TableIdent| {
            let iceberg_id = iceberg_identifier_string(ident);
            let per_table_catalog = catalog_block(creds, &iceberg_id);
            rt.block_on(async { resolve_table_schema(&session, &per_table_catalog, creds).await })
        },
    )
}

/// Assemble the createVirtualSchema / refresh / setProperties response.
///
/// The response `type` mirrors the request `type` per the Exasol VS adapter
/// protocol (`createVirtualSchema` | `refresh` | `setProperties`). When the
/// request carries `requestedTables` (a partial-refresh subset) it is echoed
/// back verbatim only because the protocol requires a well-formed response to
/// mirror the fields of the request it answers — it is NOT relied upon to
/// scope the resulting refresh: verified against the live engine, Exasol
/// applies the adapter's full `schemaMetadata.tables` response to the whole
/// namespace regardless of `requestedTables`. It is omitted when the request
/// did not include it.
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
    catalog_uri: &str,
    storage: &StorageProps,
    creds: &ConnectionCreds,
    script_schema: &str,
) -> Result<Json, UdfError> {
    // CLUSTER_NODES and PARALLELISM_FACTOR are carried in adapterNotes (persisted
    // by Exasol), NOT in properties (dropped by Exasol). Read them from
    // schemaMetadataInfo.adapterNotes; default to safe values when absent.
    let cluster_nodes = adapter_note(request, NOTE_CLUSTER_NODES)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(1);
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

    // Derive the scanned Iceberg table from involvedTables[0].name via TABLE_MAP.
    let iceberg_identifier = resolve_pushdown_identifier(request)?;
    let catalog = catalog_block(creds, &iceberg_identifier);

    handle_pushdown(
        request,
        catalog_uri,
        storage,
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
        creds,
    )
    .await
    .map_err(|e| redact_error(storage, e))
}

// ---------------------------------------------------------------------------
// Property extraction helpers
// ---------------------------------------------------------------------------

/// Merge VS `properties` with `schemaMetadataInfo.properties`.
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

/// Merge persisted and request properties for a `setProperties` request.
///
/// The persisted `schemaMetadataInfo.properties` are the base; the request
/// `properties` override on conflict (request wins), and a request value of
/// `null` unsets — removes — that property. This is the inverse precedence of
/// [`get_properties`]: `setProperties` carries the incoming
/// `ALTER VIRTUAL SCHEMA ... SET` values, which must take effect, and an
/// explicit NULL must delete the property (so a required property that is
/// null-unset then correctly fails the required-property check rather than
/// silently retaining its old persisted value).
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

/// Read `key` from a JSON object as a non-empty string.
///
/// Returns `Some` only for a present, string-typed, non-empty value — absent,
/// null, non-string, and empty-string all fall through to `None`, so callers
/// can chain `.unwrap_or(default)` and treat every one of those cases as
/// "use the default" uniformly.
fn nonempty_str<'a>(obj: &'a Json, key: &str) -> Option<&'a str> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

/// Parse `request.schemaMetadataInfo.adapterNotes` (a JSON *string*) into a JSON
/// object. Returns an empty object when adapterNotes is absent, empty, or not a
/// parseable JSON object — callers fall back to their own defaults.
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

/// Read a single string value from the persisted adapterNotes.
fn adapter_note(request: &Json, key: &str) -> Option<String> {
    parse_adapter_notes(request)
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Read the TABLE_MAP nested object from adapterNotes.
///
/// Returns a `HashMap<String, String>` mapping Exasol table name → original-cased
/// Iceberg identifier. Returns an empty map when TABLE_MAP is absent or malformed.
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

/// Flatten each `TableIdent` to an Exasol name, detect `__` collisions, and
/// return the `(exasol_name, iceberg_identifier_string)` pairs.
///
/// Returns an error naming the colliding Exasol table name when two distinct
/// identifiers flatten to the same Exasol name.
fn build_table_map(
    configured_ns: &[String],
    idents: &[iceberg::TableIdent],
) -> Result<Vec<(String, String)>, UdfError> {
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut table_map: Vec<(String, String)> = Vec::with_capacity(idents.len());
    for ident in idents {
        let exasol_name = flatten_table_name(configured_ns, ident);
        let iceberg_id = iceberg_identifier_string(ident);
        if let Some(existing) = seen.get(&exasol_name) {
            return Err(UdfError::User(format!(
                "table name collision: '{exasol_name}' maps to both '{existing}' and '{iceberg_id}'"
            )));
        }
        seen.insert(exasol_name.clone(), iceberg_id.clone());
        table_map.push((exasol_name, iceberg_id));
    }
    Ok(table_map)
}

/// Enumerate a namespace's tables into the createVirtualSchema table list and
/// `TABLE_MAP`, skipping non-Iceberg tables (catalog HTTP 404) rather than
/// aborting the whole schema.
///
/// Pure and `ctx`-free so it is unit-testable with an injected `resolver`: for
/// each identifier the resolver yields the table's `(column, exasol_type)`
/// schema, and its outcome decides the identifier's fate —
/// - `Ok(fields)` → keep: emit the table entry and count it a survivor.
/// - `Err` classified by [`is_table_not_found`] (HTTP 404) → skip: record the
///   identifier in `skipped_idents` for the handler to warn on; no table entry,
///   no `TABLE_MAP` entry.
/// - any other `Err` → abort: propagate it unchanged so genuine catalog faults
///   still fail loudly. The error is returned raw; the caller applies
///   `redact_error` before it leaves the handler, preserving the
///   no-credential-leak guarantee.
///
/// `TABLE_MAP` and the `__`-collision check are built from the surviving
/// identifiers ONLY (via [`build_table_map`]), so a skipped table can never be
/// advertised as an unqueryable virtual table and a collision between two
/// survivors still aborts.
type VirtualTables = (Vec<Json>, Vec<(String, String)>, Vec<iceberg::TableIdent>);

fn build_virtual_tables(
    configured_ns: &[String],
    idents: &[iceberg::TableIdent],
    resolver: impl Fn(&iceberg::TableIdent) -> Result<Vec<(String, String)>, UdfError>,
) -> Result<VirtualTables, UdfError> {
    let mut tables_json: Vec<Json> = Vec::with_capacity(idents.len());
    let mut survivors: Vec<iceberg::TableIdent> = Vec::with_capacity(idents.len());
    let mut skipped_idents: Vec<iceberg::TableIdent> = Vec::new();

    for ident in idents {
        let fields = match resolver(ident) {
            Ok(fields) => fields,
            Err(err) if is_table_not_found(&err) => {
                skipped_idents.push(ident.clone());
                continue;
            }
            Err(err) => return Err(err),
        };

        let exasol_name = flatten_table_name(configured_ns, ident);
        let columns: Vec<Json> = fields
            .iter()
            .map(|(name, ty)| {
                json!({
                    "name": name,
                    "dataType": exasol_type_to_json(ty),
                })
            })
            .collect();
        tables_json.push(json!({
            "name": exasol_name,
            "columns": columns,
        }));
        survivors.push(ident.clone());
    }

    let table_map = build_table_map(configured_ns, &survivors)?;
    Ok((tables_json, table_map, skipped_idents))
}

/// Resolve the Iceberg identifier for the pushdown's involved virtual table.
///
/// Reads `involvedTables[0].name`, looks it up in the persisted `TABLE_MAP`, and
/// returns the original-cased Iceberg identifier. Errors when the request carries
/// no involved table, or the name is absent from `TABLE_MAP` (never silently
/// scans a different or stale table).
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

/// Build the adapterNotes value for the createVirtualSchema response: a JSON
/// *string* (Exasol rejects a raw object) carrying CLUSTER_NODES, NR_OF_CORES,
/// PARALLELISM_FACTOR, DF_TARGET_PARTITIONS, DF_THREADS_PER_UDF, DF_BATCH_SIZE,
/// MEMORY_POOL_FRACTION, INSTANCE_OVERHEAD_MB, S3_MAX_CONNECTIONS,
/// JOIN_BROADCAST_MAX_BYTES, and TABLE_MAP (a nested JSON object mapping Exasol
/// table names to original-cased Iceberg identifiers). Any pre-existing notes on
/// the request are preserved (merge, not clobber).
// ponytail: args mirror the resolved notes fields one-to-one; a params struct is
// pure boilerplate for a single private callee.
#[allow(clippy::too_many_arguments)]
fn build_adapter_notes(
    request: &Json,
    cluster_nodes: u32,
    nr_of_cores: u32,
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
        NOTE_CLUSTER_NODES.to_string(),
        Json::String(cluster_nodes.to_string()),
    );
    notes.insert(
        NOTE_NR_OF_CORES.to_string(),
        Json::String(nr_of_cores.to_string()),
    );
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
    // TABLE_MAP: nested JSON object within the notes string.
    let map_obj: serde_json::Map<String, Json> = table_map
        .iter()
        .map(|(k, v)| (k.clone(), Json::String(v.clone())))
        .collect();
    notes.insert(NOTE_TABLE_MAP.to_string(), Json::Object(map_obj));
    Json::String(Json::Object(notes).to_string())
}

/// Read and validate the PARALLELISM_FACTOR VS property.
///
/// When the property is absent, empty, zero, or invalid, the default is
/// `max(nr_of_cores * 2, DEFAULT_PARALLELISM_FACTOR)` — hardware-aware but
/// floored at `DEFAULT_PARALLELISM_FACTOR` so a dev VM or failed core-count
/// lookup (nr_of_cores = 0) never collapses the factor below a useful minimum.
fn resolve_parallelism_factor(props: &Json, nr_of_cores: u32) -> usize {
    nonempty_str(props, PROP_PARALLELISM_FACTOR)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| ((nr_of_cores as usize) * 2).max(DEFAULT_PARALLELISM_FACTOR))
}

/// How the DataFusion per-instance thread/partition budget is derived.
///
/// `Auto` lets the adapter compute a non-oversubscribing budget from the node's
/// core count and the per-node UDF-instance share; `Fixed` uses operator-supplied
/// property values verbatim (the pre-mode behaviour). The mode is a planning-time
/// concept resolved at `createVirtualSchema`; only the resulting integers reach
/// the scan UDF, which stays mode-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThreadingMode {
    Auto,
    Fixed,
}

impl ThreadingMode {
    /// The adapterNotes string form (`AUTO` / `FIXED`).
    fn as_note(self) -> &'static str {
        match self {
            ThreadingMode::Auto => "AUTO",
            ThreadingMode::Fixed => "FIXED",
        }
    }
}

/// Resolve the DATAFUSION_THREADING_MODE VS property.
///
/// Parses `AUTO` / `FIXED` case-insensitively. An absent, empty, or unrecognized
/// value resolves to `Auto`.
fn resolve_threading_mode(props: &Json) -> ThreadingMode {
    match nonempty_str(props, PROP_DF_THREADING_MODE) {
        Some(s) if s.eq_ignore_ascii_case("FIXED") => ThreadingMode::Fixed,
        _ => ThreadingMode::Auto,
    }
}

/// Resolve the `(df_target_partitions, df_threads_per_udf)` pair for the selected
/// threading mode.
///
/// In `Fixed` mode each field is the supplied property when it is a positive
/// integer, else `max(nr_of_cores, 1)` (the pre-mode behaviour). In `Auto` mode
/// the adapter derives a per-instance thread budget that does not oversubscribe a
/// node — `threads = max(1, floor(nr_of_cores / udf_instances_per_node))` — and
/// holds the target partition count in lockstep with it, ignoring any supplied
/// `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF` values. When
/// `nr_of_cores` is `0` (unknown) both fields are `1`.
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

/// Derive the AUTO-mode per-instance thread budget.
///
/// `max(1, floor(nr_of_cores / udf_instances_per_node))`, with `0` cores (unknown)
/// yielding `1`. The floor guarantees the non-oversubscription invariant
/// `udf_instances_per_node × threads ≤ nr_of_cores` whenever the node has at least
/// as many cores as instances; when instances exceed cores the `max(1, …)` floor
/// keeps each instance single-threaded (the engine multiplexes the surplus
/// instances onto the core pool).
fn auto_threads_per_udf(nr_of_cores: u32, udf_instances_per_node: usize) -> usize {
    let instances = udf_instances_per_node.max(1);
    ((nr_of_cores as usize) / instances).max(1)
}

/// Read and validate a FIXED-mode DataFusion count property (target partitions or
/// threads-per-UDF, selected by `key`).
///
/// An explicit positive-integer property wins. When absent, empty, zero, or
/// invalid the default is `max(nr_of_cores, 1)` so scans auto-parallelize to
/// the detected or overridden core count; when `nr_of_cores` is `0` (unknown)
/// the default falls back to `1`, preserving prior single-threaded behavior.
fn resolve_df_fixed_count(props: &Json, key: &str, nr_of_cores: u32) -> usize {
    nonempty_str(props, key)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| (nr_of_cores as usize).max(1))
}

/// Resolve the `S3_MAX_CONNECTIONS` object-store connection-concurrency budget.
///
/// Explicit-wins-else-AUTO (Design Decision [3]), a single knob with no separate
/// MODE property (connection concurrency is one field, unlike the coupled
/// partition/thread pair behind `DATAFUSION_THREADING_MODE`):
///
/// * An explicit positive-integer `S3_MAX_CONNECTIONS` property is used verbatim
///   (FIXED-like) — same `nonempty_str → parse → filter(>=1)` shape as
///   `resolve_df_fixed_count`.
/// * Absent/empty/zero/invalid triggers an AUTO derivation from `nr_of_cores` and
///   the per-node UDF-instance share. When `nr_of_cores == 0` (unknown) it falls
///   back to `DEFAULT_S3_MAX_CONNECTIONS`, mirroring the `0`-cores handling across
///   the adapter.
///
/// # AUTO formula
///
/// `per_instance_threads × S3_CONNECTIONS_PER_THREAD`, where `per_instance_threads`
/// is exactly the AUTO thread budget from [`auto_threads_per_udf`] (reused here so
/// the two knobs stay in lockstep and share the same `0`-instances handling).
///
/// The connection budget is a *multiple* of the thread budget, not a 1:1 mirror,
/// because S3 data fetching is latency-bound rather than CPU-bound: a decode thread
/// spends most of a byte-range GET waiting on a network round-trip, so keeping
/// `S3_CONNECTIONS_PER_THREAD` requests in flight per thread hides that latency and
/// keeps the NIC busy (Little's law: fill-the-pipe concurrency ≈ bandwidth × latency).
/// Idle pooled TCP connections are cheap relative to OS threads, so oversubscribing
/// the IO axis relative to the CPU axis is the correct asymmetry for approaching the
/// native `IMPORT FROM PARQUET` throughput ceiling.
///
/// This yields a clean invariant: because `per_instance_threads ≈ nr_of_cores /
/// instances`, the *aggregate* per-node connection budget
/// (`instances × per_instance_threads × mult`) is ≈ `nr_of_cores × mult` regardless
/// of how the node is sharded into instances — so the node-wide fetch concurrency
/// tracks node capacity and lands in the native importer's low-double-digit
/// `MaxConnections` range (e.g. 8 cores, one instance → 8 × 4 = 32; 8 cores, eight
/// single-thread instances → 8 × (1 × 4) = 32 aggregate).
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

    if nr_of_cores == 0 {
        return DEFAULT_S3_MAX_CONNECTIONS;
    }

    let per_instance_threads = auto_threads_per_udf(nr_of_cores, udf_instances_per_node);
    (per_instance_threads * S3_CONNECTIONS_PER_THREAD).max(1)
}

/// Read and validate the DATAFUSION_BATCH_SIZE VS property.
///
/// An explicit positive-integer property wins. When absent, empty, zero, or
/// invalid the default is `DEFAULT_DF_BATCH_SIZE` (8192, matching DataFusion's
/// built-in default). A supplied value is clamped to ≥1.
fn resolve_df_batch_size(props: &Json) -> usize {
    nonempty_str(props, PROP_DF_BATCH_SIZE)
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_DF_BATCH_SIZE)
}

/// Read and validate the MEMORY_POOL_FRACTION VS property.
///
/// Accepts any value in the range (0.0, 1.0]. When the property is absent, empty,
/// zero, out-of-range, or unparseable the default is `DEFAULT_MEMORY_POOL_FRACTION`.
fn resolve_memory_pool_fraction(props: &Json) -> f64 {
    nonempty_str(props, PROP_MEMORY_POOL_FRACTION)
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&x| x > 0.0 && x <= 1.0)
        .unwrap_or(DEFAULT_MEMORY_POOL_FRACTION)
}

/// Read and validate the INSTANCE_OVERHEAD_MB VS property.
///
/// Any successfully parsed u64 value (including zero) is accepted. When the
/// property is absent, empty, or unparseable the default is
/// `DEFAULT_INSTANCE_OVERHEAD_MB`.
fn resolve_instance_overhead_mb(props: &Json) -> u64 {
    nonempty_str(props, PROP_INSTANCE_OVERHEAD_MB)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INSTANCE_OVERHEAD_MB)
}

/// Read and validate the JOIN_BROADCAST_MAX_BYTES VS property.
///
/// A positive `u64` byte count wins. When the property is absent, empty,
/// non-numeric, zero, or (since `u64` cannot hold one) negative, the default is
/// `DEFAULT_JOIN_BROADCAST_MAX_BYTES` (128 MiB). See backlog BL-001 / plan
/// `add-join-pushdown-broadcast`.
fn resolve_join_broadcast_max_bytes(props: &Json) -> u64 {
    nonempty_str(props, PROP_JOIN_BROADCAST_MAX_BYTES)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_JOIN_BROADCAST_MAX_BYTES)
}

/// Parse the `NR_OF_CORES` VS property into an override value.
///
/// Returns `Some(n)` when the property is present, non-empty, and parses to a
/// `u32` that is ≥ 1. Returns `None` for absent, empty, zero, negative, or
/// non-numeric values, signalling that the caller should fall back to
/// auto-detection via `std::thread::available_parallelism()`.
fn parse_nr_of_cores_override(props: &Json) -> Option<u32> {
    nonempty_str(props, PROP_NR_OF_CORES)
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&n| n >= 1)
}

/// Resolve the cluster topology `(cluster_nodes, nr_of_cores)` entirely
/// in-process — no SQL round-trip, no connect-back session.
///
/// `cluster_nodes` comes from the UDF handshake metadata via
/// [`UdfContext::node_count`]. A live cluster reports its node count directly
/// (`1` on a single node); a `0` (stub, test double, or missing handshake) maps
/// to `1` so `createVirtualSchema` keeps the single-shard fallback behaviour.
///
/// `nr_of_cores` comes from the `NR_OF_CORES` VS property override (via
/// [`parse_nr_of_cores_override`]) when it resolves to a positive integer;
/// otherwise it is auto-detected from `std::thread::available_parallelism()`.
/// A `nr_of_cores` of `0` signals "unknown" (auto-detect unavailable); callers
/// must handle the floor case.
fn resolve_cluster_nodes(ctx: &mut dyn UdfContext, props: &Json) -> (u32, u32) {
    let cluster_nodes = match ctx.node_count() {
        0 => 1,
        n => n,
    };

    let nr_of_cores = parse_nr_of_cores_override(props).unwrap_or_else(available_parallelism_or_0);

    (cluster_nodes, nr_of_cores)
}

/// Per-node CPU core count from `std::thread::available_parallelism()`, or `0`
/// when the platform cannot report it. `0` signals "unknown" to callers.
fn available_parallelism_or_0() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(0)
}

/// Returns `true` iff `err` is the catalog's "table not found" (HTTP 404)
/// signal — the deterministic prefix the single catalog error site
/// (`authed_get_json`'s non-success branch in `lakehouse_catalog`'s
/// `iceberg_io`) emits as
/// `format!("catalog returned HTTP {}: {}", status.as_u16(), redact(&body))`.
///
/// A 404 marks a namespace entry that is absent or not an Iceberg table (AWS
/// Glue returns 404 `NoSuchIcebergTableException` for a Hive table), so such a
/// table is skipped from the virtual schema. Every other outcome — a non-404
/// HTTP status, or a transport/parse failure — returns `false` so enumeration
/// aborts loudly, preserving the unreachable-catalog contract.
///
/// Matching is `starts_with` (not `contains`) against the full pinned prefix
/// `catalog returned HTTP 404: `, including the `": "` separator that always
/// follows the status code in the emitted message: a non-404 response whose
/// redacted body merely contains the substring `404` must not false-match, and
/// the trailing separator ensures a differently-formatted reuse of `HTTP 404`
/// elsewhere cannot accidentally satisfy this prefix. Only `UdfError::User` can
/// carry this message; the other `UdfError` variants are never produced by the
/// catalog error site, so they classify as `false`. `redact_error` preserves
/// this prefix — it strips only secret/credential substrings from the message
/// body, never the leading literal — so classification is unaffected by the
/// redaction the caller applies before propagating the error.
fn is_table_not_found(err: &UdfError) -> bool {
    matches!(err, UdfError::User(msg) if msg.starts_with("catalog returned HTTP 404: "))
}

/// Redact credential values from a UdfError message.
///
/// Strips the literal secret values held in `storage` (value-based) and then
/// applies the label-based heuristic, so credentials cannot leak through error
/// shapes the label heuristic misses.
fn redact_error(storage: &StorageProps, e: UdfError) -> UdfError {
    match e {
        UdfError::User(msg) => {
            let stripped = crate::scan::emit::redact_secret_values(&msg, &storage.secret_values());
            UdfError::User(crate::scan::emit::redact_credentials(&stripped))
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Public test surface
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// `is_table_not_found` skips ONLY the code-authored HTTP 404 prefix.
    ///
    /// 404 → `true` (the sole skippable status: absent / non-Iceberg table).
    /// Every other HTTP status and any transport/parse failure → `false`, so
    /// enumeration aborts loudly on genuine catalog faults. The `starts_with`
    /// discipline is pinned: a non-404 error whose body merely *contains* "404"
    /// must NOT match, and non-`User` variants classify as `false`.
    #[test]
    fn is_table_not_found_true_only_for_404() {
        // The exact shape the catalog error site emits for a Hive/absent table.
        assert!(
            is_table_not_found(&UdfError::User(
                "catalog returned HTTP 404: NoSuchIcebergTableException: Input table is not an iceberg table"
                    .into()
            )),
            "HTTP 404 is the skippable not-an-Iceberg-table signal"
        );

        // Every other HTTP status must abort, not skip.
        for status in ["401", "403", "500", "503"] {
            assert!(
                !is_table_not_found(&UdfError::User(format!(
                    "catalog returned HTTP {status}: some body"
                ))),
                "HTTP {status} must NOT be classified as table-not-found"
            );
        }

        // A non-404 response whose redacted body merely CONTAINS "404" must not
        // false-match — this is why the classifier uses starts_with, not contains.
        assert!(
            !is_table_not_found(&UdfError::User(
                "catalog returned HTTP 500: upstream said error 404 in its body".into()
            )),
            "a 404 substring inside a non-404 error body must not false-match"
        );

        // The prefix must include the ": " separator that always follows the
        // status code — a message that merely starts with the digits "404"
        // but lacks the separator (e.g. a differently-formatted reuse of
        // "HTTP 404" elsewhere) must not false-match.
        assert!(
            !is_table_not_found(&UdfError::User(
                "catalog returned HTTP 404 Not Found".into()
            )),
            "the prefix match requires the ': ' separator, not just the status digits"
        );

        // Transport/parse failures (the other error sites in authed_get_json)
        // carry different prefixes and must abort.
        assert!(
            !is_table_not_found(&UdfError::User(
                "catalog request failed: connection refused".into()
            )),
            "a transport error must abort, not skip"
        );
        assert!(
            !is_table_not_found(&UdfError::User(
                "failed to parse catalog response: expected value".into()
            )),
            "a parse error must abort, not skip"
        );

        // Non-`User` variants are never produced by the catalog error site.
        assert!(
            !is_table_not_found(&UdfError::ConnectBack("catalog returned HTTP 404".into())),
            "only UdfError::User carries the catalog status prefix"
        );
    }

    #[test]
    fn dispatch_get_capabilities() {
        let req = serde_json::json!({"type": "getCapabilities"});
        let resp = dispatch(&mut NoopCtx, &req).unwrap();
        assert_eq!(resp["type"].as_str().unwrap(), "getCapabilities");
        let caps = resp["capabilities"].as_array().unwrap();
        assert!(!caps.is_empty());
    }

    #[test]
    fn dispatch_drop_returns_correct_type() {
        let req = serde_json::json!({"type": "dropVirtualSchema"});
        let resp = dispatch(&mut NoopCtx, &req).unwrap();
        assert_eq!(resp["type"].as_str().unwrap(), "dropVirtualSchema");
    }

    #[test]
    fn dispatch_unknown_type_errors() {
        let req = serde_json::json!({"type": "unsupported"});
        let err = dispatch(&mut NoopCtx, &req).unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    /// `refresh` and `setProperties` are recognised protocol types: dispatch
    /// routes them into `handle_create_virtual_schema`, so they fail (if at all)
    /// on connection resolution — never with the `unsupported VS request type`
    /// error the dead `refreshVirtualSchema` arm used to produce.
    #[test]
    fn refresh_and_set_properties_dispatched_not_unsupported() {
        for req_type in ["refresh", "setProperties"] {
            let req = serde_json::json!({
                "type": req_type,
                "properties": { PROP_CATALOG_CONNECTION: "no_such_conn" },
            });
            let err = dispatch(&mut NoopCtx, &req)
                .expect_err("no live catalog is available in a unit test");
            assert!(
                !err.to_string().contains("unsupported"),
                "{req_type} must not be rejected as an unsupported request type, got: {err}"
            );
        }
    }

    /// The response `type` mirrors the request `type` for every enumeration
    /// request type (Exasol VS protocol requirement).
    #[test]
    fn build_schema_response_type_mirrors_request() {
        let schema_metadata = serde_json::json!({"tables": [], "adapterNotes": "{}"});
        for req_type in ["createVirtualSchema", "refresh", "setProperties"] {
            let req = serde_json::json!({"type": req_type});
            let resp = build_schema_response(&req, schema_metadata.clone());
            assert_eq!(
                resp["type"].as_str(),
                Some(req_type),
                "response type must equal request type"
            );
        }
    }

    /// `requestedTables` is echoed verbatim when the request carries it and is
    /// absent from the response otherwise (pure pass-through).
    #[test]
    fn build_schema_response_echoes_requested_tables_present_and_absent() {
        let schema_metadata = serde_json::json!({"tables": [], "adapterNotes": "{}"});

        let with = serde_json::json!({
            "type": "refresh",
            "requestedTables": ["T1", "T2"],
        });
        let resp = build_schema_response(&with, schema_metadata.clone());
        assert_eq!(
            resp["requestedTables"],
            serde_json::json!(["T1", "T2"]),
            "requestedTables must be echoed verbatim"
        );

        let without = serde_json::json!({"type": "refresh"});
        let resp = build_schema_response(&without, schema_metadata);
        assert!(
            resp.get("requestedTables").is_none(),
            "requestedTables must be omitted when the request did not include it"
        );
    }

    /// `merge_set_properties`: request props win over persisted props, and an
    /// explicit `null` in the request unsets (removes) the property — the
    /// inverse precedence of `get_properties`.
    #[test]
    fn merge_set_properties_new_wins_and_null_unsets() {
        let req = serde_json::json!({
            "type": "setProperties",
            "properties": {
                "ICEBERG_NAMESPACE": "new_ns",
                "ALLOW_HTTP": null,
            },
            "schemaMetadataInfo": {
                "properties": {
                    "ICEBERG_NAMESPACE": "old_ns",
                    "ALLOW_HTTP": "true",
                    "CATALOG_CONNECTION": "keep_me",
                }
            },
        });
        let merged = merge_set_properties(&req);

        // Request value wins over the persisted value.
        assert_eq!(nonempty_str(&merged, "ICEBERG_NAMESPACE"), Some("new_ns"));
        // A null request value removes the persisted property entirely.
        assert!(
            merged.get("ALLOW_HTTP").is_none(),
            "a null request value must unset the property"
        );
        // A persisted property the request does not mention is retained.
        assert_eq!(nonempty_str(&merged, "CATALOG_CONNECTION"), Some("keep_me"));
    }

    // Stub UdfContext whose `connection()` resolves successfully, so a
    // `setProperties` dispatch can pass connection resolution and reach the
    // downstream required-property check instead of failing earlier on a
    // missing/unresolvable CONNECTION.
    struct ConnResolvingCtx;
    impl UdfContext for ConnResolvingCtx {
        fn num_columns(&self) -> usize {
            0
        }
        fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
            Err(UdfError::Type("none".into()))
        }
        fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
            Ok(())
        }
        fn next(&mut self) -> Result<bool, UdfError> {
            Ok(false)
        }
        fn connection(
            &self,
            _name: &str,
        ) -> Result<exasol_udf_sdk::connect_back::ConnectionObject, UdfError> {
            Ok(exasol_udf_sdk::connect_back::ConnectionObject {
                kind: "PASSWORD".into(),
                address: "http://catalog.example.com".into(),
                user: String::new(),
                password: serde_json::json!({
                    "warehouse": "wh",
                    "endpoint": "http://s3.example.com",
                    "region": "us-east-1",
                    "access_key": "AKID",
                    "secret_key": "SECRET",
                })
                .to_string(),
            })
        }
    }

    /// [human-requested, PR #153 review, adversarial-review finding A2] A
    /// `setProperties` request that null-unsets a required property
    /// (`ICEBERG_NAMESPACE`) must fail with the normal required-property
    /// error — never a panic, and never a silent fallback to the stale
    /// persisted value. `merge_set_properties` on its own only proves the key
    /// is removed from the merged map; this drives the null-unset through the
    /// real `setProperties` dispatch path so the removal is proven to reach
    /// `handle_create_virtual_schema`'s required-property check end-to-end.
    #[test]
    fn set_properties_null_unset_required_property_errors_not_panic() {
        let req = serde_json::json!({
            "type": "setProperties",
            "properties": {
                "ICEBERG_NAMESPACE": null,
            },
            "schemaMetadataInfo": {
                "properties": {
                    "ICEBERG_NAMESPACE": "old_ns",
                    "CATALOG_CONNECTION": "MY_CONN",
                }
            },
        });

        let err = dispatch(&mut ConnResolvingCtx, &req)
            .expect_err("null-unsetting a required property must error, not succeed");

        assert!(
            err.to_string().contains(PROP_ICEBERG_NAMESPACE),
            "expected the required-property error to name '{PROP_ICEBERG_NAMESPACE}', got: {err}"
        );
        assert!(
            err.to_string().contains("is required"),
            "expected handle_create_virtual_schema's normal required-property error, got: {err}"
        );
    }

    // Minimal UdfContext for dispatch tests that need no I/O. Its `node_count()`
    // uses the trait default (0), exercising the `0 → 1` topology fallback.
    struct NoopCtx;
    impl UdfContext for NoopCtx {
        fn num_columns(&self) -> usize {
            0
        }
        fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
            Err(UdfError::Type("none".into()))
        }
        fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
            Ok(())
        }
        fn next(&mut self) -> Result<bool, UdfError> {
            Ok(false)
        }
    }

    // Like `NoopCtx` but with a configurable `node_count()`, so tests can drive
    // both the `0 → default 1` fallback and a `> 1` real-cluster pass-through.
    struct StubCtx {
        node_count: u32,
    }
    impl UdfContext for StubCtx {
        fn num_columns(&self) -> usize {
            0
        }
        fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
            Err(UdfError::Type("none".into()))
        }
        fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
            Ok(())
        }
        fn next(&mut self) -> Result<bool, UdfError> {
            Ok(false)
        }
        fn node_count(&self) -> u32 {
            self.node_count
        }
    }

    #[test]
    fn cluster_nodes_defaults_to_one_when_node_count_zero() {
        // A context reporting node_count() == 0 (no live handshake — the trait
        // default, as on NoopCtx) maps to CLUSTER_NODES == 1.
        let props = serde_json::json!({});
        let (count, _cores) = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(count, 1u32);

        // The same default holds for a StubCtx explicitly reporting 0.
        let (count, _cores) = resolve_cluster_nodes(&mut StubCtx { node_count: 0 }, &props);
        assert_eq!(count, 1u32);
    }

    #[test]
    fn cluster_nodes_passes_through_reported_node_count() {
        // A live cluster reporting node_count() == N (> 1) is passed through
        // verbatim to CLUSTER_NODES with no defaulting.
        let props = serde_json::json!({});
        let (count, _cores) = resolve_cluster_nodes(&mut StubCtx { node_count: 4 }, &props);
        assert_eq!(count, 4u32);
    }

    /// Verifies that the createVirtualSchema response JSON carries CLUSTER_NODES
    /// in schemaMetadata.adapterNotes (a JSON *string*, the only channel Exasol
    /// persists), driven off the node count reported by the context.
    ///
    /// Exercises the JSON-assembly seam without catalog or network I/O.
    #[test]
    fn create_response_carries_cluster_nodes_property() {
        let props = serde_json::json!({});
        let (cluster_nodes, nr_of_cores) =
            resolve_cluster_nodes(&mut StubCtx { node_count: 1 }, &props);
        assert_eq!(cluster_nodes, 1u32, "stubbed cluster_nodes must be 1");

        // Replicate the schema_metadata construction from handle_create_virtual_schema.
        // The request has no pre-existing adapterNotes (clean set path).
        let request = serde_json::json!({"type": "createVirtualSchema"});
        let adapter_notes = build_adapter_notes(
            &request,
            cluster_nodes,
            nr_of_cores,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let schema_metadata = serde_json::json!({
            "tables": [],
            "adapterNotes": adapter_notes,
        });
        let response = serde_json::json!({
            "type": "createVirtualSchema",
            "schemaMetadata": schema_metadata,
        });

        // adapterNotes MUST be a JSON string (Exasol rejects a raw object).
        let notes_str = response["schemaMetadata"]["adapterNotes"]
            .as_str()
            .unwrap_or_else(|| {
                panic!("schemaMetadata.adapterNotes must be a JSON string: {response}")
            });
        // The string parses to an object carrying CLUSTER_NODES = "1".
        let parsed: serde_json::Value =
            serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
        let val = parsed[NOTE_CLUSTER_NODES]
            .as_str()
            .unwrap_or_else(|| panic!("adapterNotes.CLUSTER_NODES must be a string: {parsed}"));
        assert_eq!(
            val, "1",
            "CLUSTER_NODES must be \"1\" for a single-node context, got \"{val}\""
        );
    }

    /// Verifies the round-trip: a CLUSTER_NODES written into adapterNotes by
    /// createVirtualSchema is read back by the pushdown path from
    /// schemaMetadataInfo.adapterNotes (the channel Exasol actually persists).
    #[test]
    fn adapter_notes_cluster_nodes_round_trips() {
        // createVirtualSchema produces the adapterNotes string for, say, 4 nodes.
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            4,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

        // Exasol persists that string and hands it back under
        // schemaMetadataInfo.adapterNotes on the next pushdown request.
        let pushdown_req = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes_str },
        });
        assert_eq!(
            adapter_note(&pushdown_req, NOTE_CLUSTER_NODES).as_deref(),
            Some("4"),
            "CLUSTER_NODES must round-trip through adapterNotes"
        );
    }

    /// Verifies the default-to-1 fallback when adapterNotes is absent or
    /// unparseable on a pushdown request.
    #[test]
    fn adapter_note_absent_or_unparseable_yields_none() {
        // No schemaMetadataInfo at all.
        let bare = serde_json::json!({"type": "pushdown"});
        assert!(adapter_note(&bare, NOTE_CLUSTER_NODES).is_none());

        // adapterNotes present but not valid JSON.
        let garbage = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": "not json" },
        });
        assert!(adapter_note(&garbage, NOTE_CLUSTER_NODES).is_none());

        // adapterNotes empty string.
        let empty = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": "" },
        });
        assert!(adapter_note(&empty, NOTE_CLUSTER_NODES).is_none());
    }

    /// Verifies merge-not-clobber: a pre-existing adapterNotes key survives when
    /// createVirtualSchema rewrites the notes with the resolved node count.
    #[test]
    fn build_adapter_notes_merges_existing() {
        let req = serde_json::json!({
            "type": "refresh",
            "schemaMetadataInfo": {
                "adapterNotes": "{\"OTHER_KEY\":\"keep-me\",\"CLUSTER_NODES\":\"1\"}"
            },
        });
        let notes = build_adapter_notes(
            &req,
            3,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed["OTHER_KEY"].as_str(),
            Some("keep-me"),
            "pre-existing adapterNotes keys must be preserved"
        );
        assert_eq!(
            parsed[NOTE_CLUSTER_NODES].as_str(),
            Some("3"),
            "CLUSTER_NODES must be updated to the freshly resolved value"
        );
    }

    /// Refresh re-enumerates the namespace and passes the freshly resolved
    /// `table_map` into `build_adapter_notes` on every call; TABLE_MAP must be
    /// rebuilt from that fresh map (not merged with whatever was persisted
    /// from the prior enumeration), while unrelated adapterNotes keys survive
    /// the rewrite untouched.
    #[test]
    fn refresh_rebuilds_table_map_preserves_notes() {
        let req = serde_json::json!({
            "type": "refresh",
            "schemaMetadataInfo": {
                "adapterNotes": serde_json::json!({
                    "OTHER_KEY": "keep-me",
                    "TABLE_MAP": {"OLD_TABLE": "ns.old_table"},
                })
                .to_string(),
            },
        });

        let fresh_table_map = vec![("NEW_TABLE".to_string(), "ns.new_table".to_string())];
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &fresh_table_map,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");

        assert_eq!(
            parsed["OTHER_KEY"].as_str(),
            Some("keep-me"),
            "an unrelated adapterNotes key must survive a refresh's TABLE_MAP rebuild"
        );

        let table_map = parsed[NOTE_TABLE_MAP]
            .as_object()
            .expect("TABLE_MAP must be an object");
        assert_eq!(
            table_map.len(),
            1,
            "TABLE_MAP must be rebuilt from the fresh enumeration, not merged with the stale one"
        );
        assert_eq!(
            table_map.get("NEW_TABLE").and_then(|v| v.as_str()),
            Some("ns.new_table"),
            "the freshly resolved table must appear in the rebuilt TABLE_MAP"
        );
        assert!(
            table_map.get("OLD_TABLE").is_none(),
            "the stale TABLE_MAP entry must not survive a refresh rebuild"
        );
    }

    /// Task 2.2 — Adapter records the parallelism factor in the virtual-schema adapterNotes.
    /// Covers scenario `create_vs_records_parallelism_factor`.
    #[test]
    fn create_vs_records_parallelism_factor() {
        // Request with an explicit PARALLELISM_FACTOR property — nr_of_cores is
        // irrelevant because the explicit property wins.
        let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "4" });
        let factor = resolve_parallelism_factor(&props, 16);
        assert_eq!(factor, 4, "factor must be read from the property");

        // Build adapterNotes and verify PARALLELISM_FACTOR is present.
        let request = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &request,
            2,
            16,
            factor,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
        let parsed: serde_json::Value =
            serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
        assert_eq!(
            parsed[NOTE_PARALLELISM_FACTOR].as_str(),
            Some("4"),
            "PARALLELISM_FACTOR must be recorded in adapterNotes"
        );

        // Default when property absent and nr_of_cores = 0 → floor at DEFAULT_PARALLELISM_FACTOR.
        let empty_props = serde_json::json!({});
        let default_factor = resolve_parallelism_factor(&empty_props, 0);
        assert_eq!(
            default_factor, DEFAULT_PARALLELISM_FACTOR,
            "must default to {DEFAULT_PARALLELISM_FACTOR} when property absent and cores=0"
        );

        // Zero or invalid value also defaults (explicit "0" is treated as absent).
        let zero_props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "0" });
        let zero_factor = resolve_parallelism_factor(&zero_props, 0);
        assert_eq!(
            zero_factor, DEFAULT_PARALLELISM_FACTOR,
            "zero must fall back to default"
        );
    }

    /// Task 2.2 — Both CLUSTER_NODES and PARALLELISM_FACTOR round-trip through adapterNotes.
    /// Covers scenario `adapter_notes_carry_cluster_nodes_and_parallelism_factor`.
    #[test]
    fn adapter_notes_carry_cluster_nodes_and_parallelism_factor() {
        // createVirtualSchema records both values.
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            6,
            0,
            12,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

        // Exasol persists that string and hands it back on the next pushdown request.
        let pushdown_req = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes_str },
        });
        assert_eq!(
            adapter_note(&pushdown_req, NOTE_CLUSTER_NODES).as_deref(),
            Some("6"),
            "CLUSTER_NODES must round-trip through adapterNotes"
        );
        assert_eq!(
            adapter_note(&pushdown_req, NOTE_PARALLELISM_FACTOR).as_deref(),
            Some("12"),
            "PARALLELISM_FACTOR must round-trip through adapterNotes"
        );
    }

    // ---------------------------------------------------------------------------
    // T5 — NR_OF_CORES note tests
    // ---------------------------------------------------------------------------

    /// Scenario: Adapter records the per-node core count in the virtual-schema adapterNotes.
    #[test]
    fn adapter_notes_records_nr_of_cores() {
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            2,
            16,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed[NOTE_NR_OF_CORES].as_str(),
            Some("16"),
            "NR_OF_CORES must be written into adapterNotes"
        );
    }

    /// Scenario: with no NR_OF_CORES override, the core count is auto-detected
    /// from `std::thread::available_parallelism()` — a positive, host-sourced
    /// value (not injectable, so we assert positivity rather than an exact count).
    #[test]
    fn nr_of_cores_from_available_parallelism_when_unavailable() {
        let props = serde_json::json!({});
        let (_nodes, nr_of_cores) = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert!(
            nr_of_cores >= 1,
            "nr_of_cores must be auto-detected from available_parallelism() (>= 1), got {nr_of_cores}"
        );
    }

    // ---------------------------------------------------------------------------
    // T5 — parallelism factor formula tests
    // ---------------------------------------------------------------------------

    /// Scenario: Default parallelism factor equals NR_OF_CORES × 2 when cores > 4.
    #[test]
    fn default_parallelism_factor_is_cores_times_two() {
        let props = serde_json::json!({});
        // 10 cores × 2 = 20, which is > DEFAULT_PARALLELISM_FACTOR (8), so 20 wins.
        let factor = resolve_parallelism_factor(&props, 10);
        assert_eq!(
            factor, 20,
            "factor must equal nr_of_cores × 2 when that exceeds 8"
        );
    }

    /// Scenario: Default parallelism factor is floored at DEFAULT_PARALLELISM_FACTOR (8)
    /// when NR_OF_CORES × 2 would produce a smaller value (e.g., 0 or 2).
    #[test]
    fn default_parallelism_factor_floors_at_eight() {
        let props = serde_json::json!({});
        // 0 cores × 2 = 0; must floor to DEFAULT_PARALLELISM_FACTOR.
        let factor_zero = resolve_parallelism_factor(&props, 0);
        assert_eq!(
            factor_zero, DEFAULT_PARALLELISM_FACTOR,
            "must floor at 8 when cores=0"
        );

        // 2 cores × 2 = 4; still below floor.
        let factor_small = resolve_parallelism_factor(&props, 2);
        assert_eq!(
            factor_small, DEFAULT_PARALLELISM_FACTOR,
            "must floor at 8 when cores×2 < 8"
        );
    }

    /// Scenario: An explicit PARALLELISM_FACTOR property overrides the default formula.
    #[test]
    fn explicit_parallelism_factor_overrides_default() {
        let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "5" });
        // Even with 32 cores (32×2=64 > 8), the explicit prop wins.
        let factor = resolve_parallelism_factor(&props, 32);
        assert_eq!(
            factor, 5,
            "explicit property must override the NR_OF_CORES formula"
        );
    }

    // ---------------------------------------------------------------------------
    // T8 — DF_TARGET_PARTITIONS and DF_THREADS_PER_UDF note tests
    // ---------------------------------------------------------------------------

    /// Scenario: DF_TARGET_PARTITIONS defaults to 1 when property is absent/zero/invalid
    /// and nr_of_cores is 0 (unknown).
    #[test]
    fn df_target_partitions_defaults_to_one() {
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_df_fixed_count(&absent, PROP_DF_TARGET_PARTITIONS, 0),
            1,
            "absent → 1"
        );

        let zero = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "0" });
        assert_eq!(
            resolve_df_fixed_count(&zero, PROP_DF_TARGET_PARTITIONS, 0),
            1,
            "zero → 1"
        );

        let invalid = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "bad" });
        assert_eq!(
            resolve_df_fixed_count(&invalid, PROP_DF_TARGET_PARTITIONS, 0),
            1,
            "invalid → 1"
        );
    }

    /// Scenario: An explicit positive DATAFUSION_TARGET_PARTITIONS property is used as-is.
    #[test]
    fn df_target_partitions_uses_supplied_value() {
        let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "4" });
        let val = resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 0);
        assert_eq!(val, 4, "explicit value must be returned");

        // Verify it round-trips through adapterNotes.
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            val,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed[NOTE_DF_TARGET_PARTITIONS].as_str(),
            Some("4"),
            "DF_TARGET_PARTITIONS must round-trip through adapterNotes"
        );
    }

    /// R1: Supplied DATAFUSION_BATCH_SIZE flows create → adapterNote → pushdown → ScanSpec.
    ///
    /// Verifies the full round-trip: resolve_df_batch_size reads the VS property,
    /// build_adapter_notes persists it as NOTE_DF_BATCH_SIZE, and the pushdown path
    /// reads it back via adapter_note. Also checks default and zero-clamp behaviour.
    #[test]
    fn df_batch_size_uses_supplied_value() {
        // Explicit value is returned as-is (clamped to ≥1, but 4096 is already ≥1).
        let props = serde_json::json!({ PROP_DF_BATCH_SIZE: "4096" });
        let val = resolve_df_batch_size(&props);
        assert_eq!(val, 4096, "explicit DATAFUSION_BATCH_SIZE must be returned");

        // Zero is clamped to 1.
        let zero_props = serde_json::json!({ PROP_DF_BATCH_SIZE: "0" });
        assert_eq!(
            resolve_df_batch_size(&zero_props),
            1,
            "DATAFUSION_BATCH_SIZE=0 must be clamped to 1"
        );

        // Absent → default.
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_df_batch_size(&absent),
            DEFAULT_DF_BATCH_SIZE,
            "absent property must return DEFAULT_DF_BATCH_SIZE (8192)"
        );

        // Verify it round-trips through adapterNotes (create → note → pushdown).
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            val,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

        // Pushdown reads it back.
        let pushdown_req = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes_str },
        });
        assert_eq!(
            adapter_note(&pushdown_req, NOTE_DF_BATCH_SIZE).as_deref(),
            Some("4096"),
            "DF_BATCH_SIZE must round-trip through adapterNotes"
        );
    }

    /// Scenario: DF_THREADS_PER_UDF defaults to 1 when property is absent/zero/invalid
    /// and nr_of_cores is 0 (unknown).
    #[test]
    fn df_threads_per_udf_defaults_to_one() {
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_df_fixed_count(&absent, PROP_DF_THREADS_PER_UDF, 0),
            1,
            "absent → 1"
        );

        let zero = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "0" });
        assert_eq!(
            resolve_df_fixed_count(&zero, PROP_DF_THREADS_PER_UDF, 0),
            1,
            "zero → 1"
        );

        let invalid = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "not-a-number" });
        assert_eq!(
            resolve_df_fixed_count(&invalid, PROP_DF_THREADS_PER_UDF, 0),
            1,
            "invalid → 1"
        );
    }

    /// Scenario: An explicit positive DATAFUSION_THREADS_PER_UDF property is used as-is.
    #[test]
    fn df_threads_per_udf_uses_supplied_value() {
        let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
        let val = resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 0);
        assert_eq!(val, 2, "explicit value must be returned");

        // Verify it round-trips through adapterNotes.
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            val,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed[NOTE_DF_THREADS_PER_UDF].as_str(),
            Some("2"),
            "DF_THREADS_PER_UDF must round-trip through adapterNotes"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 5.1 — MEMORY_POOL_FRACTION and INSTANCE_OVERHEAD_MB resolver tests
    // ---------------------------------------------------------------------------

    /// Scenario: resolve_memory_pool_fraction defaults/validates.
    #[test]
    fn resolve_memory_pool_fraction_defaults_and_validates() {
        // Absent → default.
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_memory_pool_fraction(&absent),
            DEFAULT_MEMORY_POOL_FRACTION,
            "absent → default 0.6"
        );

        // Empty string → default (nonempty_str filters empty strings).
        let empty = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "" });
        assert_eq!(
            resolve_memory_pool_fraction(&empty),
            DEFAULT_MEMORY_POOL_FRACTION,
            "empty → default 0.6"
        );

        // "0" → out of range (must be > 0.0) → default.
        let zero = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0" });
        assert_eq!(
            resolve_memory_pool_fraction(&zero),
            DEFAULT_MEMORY_POOL_FRACTION,
            "\"0\" is out of range → default 0.6"
        );

        // "1.5" → > 1.0, out of range → default.
        let too_large = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.5" });
        assert_eq!(
            resolve_memory_pool_fraction(&too_large),
            DEFAULT_MEMORY_POOL_FRACTION,
            "\"1.5\" is out of range → default 0.6"
        );

        // "0.5" → valid.
        let valid = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0.5" });
        assert_eq!(
            resolve_memory_pool_fraction(&valid),
            0.5,
            "\"0.5\" must be accepted"
        );

        // "1.0" → exactly 1.0, boundary valid.
        let one = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.0" });
        assert_eq!(
            resolve_memory_pool_fraction(&one),
            1.0,
            "\"1.0\" is exactly at the upper bound and must be accepted"
        );
    }

    /// Scenario: resolve_instance_overhead_mb defaults/validates.
    #[test]
    fn resolve_instance_overhead_mb_defaults_and_validates() {
        // Absent → default.
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_instance_overhead_mb(&absent),
            DEFAULT_INSTANCE_OVERHEAD_MB,
            "absent → default 200"
        );

        // Empty string → default (nonempty_str filters empty strings).
        let empty = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "" });
        assert_eq!(
            resolve_instance_overhead_mb(&empty),
            DEFAULT_INSTANCE_OVERHEAD_MB,
            "empty → default 200"
        );

        // "0" → valid (zero overhead is permitted).
        let zero = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "0" });
        assert_eq!(
            resolve_instance_overhead_mb(&zero),
            0,
            "\"0\" is a valid overhead (zero)"
        );

        // "256" → valid.
        let valid = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "256" });
        assert_eq!(
            resolve_instance_overhead_mb(&valid),
            256,
            "\"256\" must be returned as-is"
        );

        // Garbage → default.
        let garbage = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "not-a-number" });
        assert_eq!(
            resolve_instance_overhead_mb(&garbage),
            DEFAULT_INSTANCE_OVERHEAD_MB,
            "unparseable value → default 200"
        );
    }

    /// Scenario: resolve_join_broadcast_max_bytes defaults/validates.
    /// Task 3.6 — property present + valid numeric parses correctly; absent
    /// defaults to 128 MiB; invalid (non-numeric or zero/negative) falls back
    /// to the default. See backlog BL-001 / plan `add-join-pushdown-broadcast`.
    #[test]
    fn resolve_join_broadcast_max_bytes_defaults_and_validates() {
        // Absent → default 128 MiB.
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_join_broadcast_max_bytes(&absent),
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            "absent → default 128 MiB"
        );
        assert_eq!(
            DEFAULT_JOIN_BROADCAST_MAX_BYTES, 134_217_728,
            "default must be exactly 128 MiB"
        );

        // Empty string → default (nonempty_str filters empty strings).
        let empty = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "" });
        assert_eq!(
            resolve_join_broadcast_max_bytes(&empty),
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            "empty → default 128 MiB"
        );

        // Present + valid numeric → parsed correctly.
        let valid = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "67108864" });
        assert_eq!(
            resolve_join_broadcast_max_bytes(&valid),
            67_108_864,
            "\"67108864\" (64 MiB) must be parsed as-is"
        );

        // Non-numeric → default.
        let garbage = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "not-a-number" });
        assert_eq!(
            resolve_join_broadcast_max_bytes(&garbage),
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            "unparseable value → default 128 MiB"
        );

        // Zero → invalid (must be positive) → default.
        let zero = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "0" });
        assert_eq!(
            resolve_join_broadcast_max_bytes(&zero),
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            "\"0\" is not positive → default 128 MiB"
        );

        // Negative → invalid (u64 parse fails) → default.
        let negative = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "-1" });
        assert_eq!(
            resolve_join_broadcast_max_bytes(&negative),
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            "\"-1\" is negative (unparseable as u64) → default 128 MiB"
        );
    }

    /// Scenario: JOIN_BROADCAST_MAX_BYTES round-trips through build_adapter_notes →
    /// adapter_note (mirroring memory_budget_params_round_trip_through_adapter_notes).
    #[test]
    fn join_broadcast_max_bytes_round_trips_through_adapter_notes() {
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            67_108_864,
            &[],
        );

        let pushdown_req = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes.as_str().unwrap() },
        });

        assert_eq!(
            adapter_note(&pushdown_req, NOTE_JOIN_BROADCAST_MAX_BYTES).as_deref(),
            Some("67108864"),
            "JOIN_BROADCAST_MAX_BYTES must round-trip through adapterNotes"
        );
    }

    /// Scenario: MEMORY_POOL_FRACTION and INSTANCE_OVERHEAD_MB round-trip through
    /// build_adapter_notes → adapter_note (mirroring adapter_notes_cluster_nodes_round_trips).
    #[test]
    fn memory_budget_params_round_trip_through_adapter_notes() {
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            0.5,
            256,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

        let pushdown_req = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes_str },
        });
        assert_eq!(
            adapter_note(&pushdown_req, NOTE_MEMORY_POOL_FRACTION).as_deref(),
            Some("0.5"),
            "MEMORY_POOL_FRACTION must round-trip through adapterNotes"
        );
        assert_eq!(
            adapter_note(&pushdown_req, NOTE_INSTANCE_OVERHEAD_MB).as_deref(),
            Some("256"),
            "INSTANCE_OVERHEAD_MB must round-trip through adapterNotes"
        );
    }

    // ---------------------------------------------------------------------------
    // Tasks 2.1–2.8 — NR_OF_CORES property override and cores-driven defaults.
    // ---------------------------------------------------------------------------

    /// Task 2.1 — NR_OF_CORES VS property ≥ 1 is used directly, overriding the
    /// `available_parallelism()` auto-detect (tested via the pure helper and the
    /// override-wins path).
    #[test]
    fn nr_of_cores_property_overrides_auto_detect() {
        // A positive integer property must parse to Some(n).
        let props_4 = serde_json::json!({ PROP_NR_OF_CORES: "4" });
        assert_eq!(
            parse_nr_of_cores_override(&props_4),
            Some(4u32),
            "NR_OF_CORES=4 must return Some(4)"
        );

        let props_1 = serde_json::json!({ PROP_NR_OF_CORES: "1" });
        assert_eq!(
            parse_nr_of_cores_override(&props_1),
            Some(1u32),
            "NR_OF_CORES=1 (minimum valid) must return Some(1)"
        );

        // When the override is present, resolve_cluster_nodes returns it directly
        // instead of auto-detecting; the node count comes from ctx.node_count().
        let (nodes, cores) = resolve_cluster_nodes(
            &mut StubCtx { node_count: 3 },
            &serde_json::json!({ PROP_NR_OF_CORES: "8" }),
        );
        assert_eq!(nodes, 3u32, "cluster nodes come from ctx.node_count()");
        assert_eq!(cores, 8u32, "NR_OF_CORES override must be returned");
    }

    /// Task 2.2 — NR_OF_CORES absent, empty, zero, or negative falls back to
    /// auto-detect (tested via the pure helper returning None, and the
    /// `available_parallelism()` fallback returning a positive count).
    #[test]
    fn nr_of_cores_property_falls_back_to_auto_detect() {
        // Absent → None.
        assert_eq!(
            parse_nr_of_cores_override(&serde_json::json!({})),
            None,
            "absent NR_OF_CORES must return None"
        );

        // Empty string → None (nonempty_str filters empty strings).
        assert_eq!(
            parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "" })),
            None,
            "empty NR_OF_CORES must return None"
        );

        // Zero → None (fails the ≥ 1 filter).
        assert_eq!(
            parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "0" })),
            None,
            "NR_OF_CORES=0 must return None"
        );

        // Negative (u32 parse fails) → None.
        assert_eq!(
            parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "-1" })),
            None,
            "NR_OF_CORES=-1 must return None"
        );

        // Non-numeric → None.
        assert_eq!(
            parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "bad" })),
            None,
            "NR_OF_CORES=bad must return None"
        );

        // With no override, resolve_cluster_nodes auto-detects the core count from
        // available_parallelism() (positive, host-sourced) and defaults the node
        // count to 1 when node_count() is 0.
        let (nodes, cores) = resolve_cluster_nodes(&mut NoopCtx, &serde_json::json!({}));
        assert_eq!(nodes, 1u32);
        assert!(
            cores >= 1,
            "no override must fall back to available_parallelism() (>= 1), got {cores}"
        );
    }

    /// Task 2.3 — Explicit DATAFUSION_TARGET_PARTITIONS wins over cores-driven default.
    #[test]
    fn df_target_partitions_explicit_wins() {
        let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "3" });
        // Even with nr_of_cores=8, explicit "3" must win.
        assert_eq!(
            resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 8),
            3,
            "explicit DATAFUSION_TARGET_PARTITIONS must override nr_of_cores default"
        );
    }

    /// Task 2.4 — Absent DATAFUSION_TARGET_PARTITIONS with nr_of_cores=8 defaults to 8.
    #[test]
    fn df_target_partitions_defaults_to_nr_of_cores() {
        let props = serde_json::json!({});
        assert_eq!(
            resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 8),
            8,
            "absent property with nr_of_cores=8 must default to 8"
        );
    }

    /// Task 2.5 — Absent DATAFUSION_TARGET_PARTITIONS with nr_of_cores=0 defaults to 1.
    #[test]
    fn df_target_partitions_unknown_cores_defaults_to_1() {
        let props = serde_json::json!({});
        assert_eq!(
            resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 0),
            1,
            "absent property with nr_of_cores=0 (unknown) must default to 1"
        );
    }

    /// Task 2.6 — Explicit DATAFUSION_THREADS_PER_UDF wins over cores-driven default.
    #[test]
    fn df_threads_per_udf_explicit_wins() {
        let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
        // Even with nr_of_cores=16, explicit "2" must win.
        assert_eq!(
            resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 16),
            2,
            "explicit DATAFUSION_THREADS_PER_UDF must override nr_of_cores default"
        );
    }

    /// Task 2.7 — Absent DATAFUSION_THREADS_PER_UDF with nr_of_cores=8 defaults to 8.
    #[test]
    fn df_threads_per_udf_defaults_to_nr_of_cores() {
        let props = serde_json::json!({});
        assert_eq!(
            resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 8),
            8,
            "absent property with nr_of_cores=8 must default to 8"
        );
    }

    /// Task 2.8 — Absent DATAFUSION_THREADS_PER_UDF with nr_of_cores=0 defaults to 1.
    #[test]
    fn df_threads_per_udf_unknown_cores_defaults_to_1() {
        let props = serde_json::json!({});
        assert_eq!(
            resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 0),
            1,
            "absent property with nr_of_cores=0 (unknown) must default to 1"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 1 — Threading mode AUTO/FIXED tests
    // ---------------------------------------------------------------------------

    /// 1.1 — DATAFUSION_THREADING_MODE parses case-insensitively; absent / empty /
    /// unrecognized values resolve to AUTO.
    #[test]
    fn threading_mode_parses_case_insensitively() {
        assert_eq!(
            resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "fixed" })),
            ThreadingMode::Fixed,
            "lowercase 'fixed' must parse to Fixed"
        );
        assert_eq!(
            resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "FiXeD" })),
            ThreadingMode::Fixed,
            "mixed-case 'FiXeD' must parse to Fixed"
        );
        assert_eq!(
            resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "AUTO" })),
            ThreadingMode::Auto,
            "'AUTO' must parse to Auto"
        );
    }

    /// 1.5 — Threading mode defaults to AUTO when the property is absent, empty,
    /// or holds an unrecognized value; the resolved mode is recorded in adapterNotes.
    #[test]
    fn threading_mode_defaults_to_auto() {
        assert_eq!(
            resolve_threading_mode(&serde_json::json!({})),
            ThreadingMode::Auto,
            "absent property → Auto"
        );
        assert_eq!(
            resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "" })),
            ThreadingMode::Auto,
            "empty property → Auto"
        );
        assert_eq!(
            resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "garbage" })),
            ThreadingMode::Auto,
            "unrecognized value → Auto"
        );

        // The resolved AUTO mode is recorded in adapterNotes.
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[],
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed[NOTE_DF_THREADING_MODE].as_str(),
            Some("AUTO"),
            "DF_THREADING_MODE: AUTO must be recorded in adapterNotes"
        );
    }

    /// 1.5 — AUTO mode derives a per-instance thread budget that does not
    /// oversubscribe a node: instances × threads ≤ NR_OF_CORES, with target
    /// partitions held in lockstep with threads.
    #[test]
    fn auto_mode_derives_non_oversubscribing_threads() {
        // 16 cores, parallelism_factor (= udf_instances_per_node) = 4 → 16/4 = 4.
        let (target_partitions, threads) =
            resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 16, 4);
        assert_eq!(threads, 4, "16 cores / 4 instances → 4 threads");
        assert_eq!(
            target_partitions, threads,
            "target_partitions must equal threads (lockstep)"
        );
        // The oversubscription invariant must hold explicitly.
        assert!(
            4 * threads <= 16,
            "udf_instances_per_node × threads must not exceed NR_OF_CORES"
        );

        // Non-divisible case: 10 cores / 3 instances → floor(10/3) = 3; 3×3=9 ≤ 10.
        let (tp, th) = resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 10, 3);
        assert_eq!(th, 3, "floor(10/3) = 3");
        assert_eq!(tp, th, "lockstep");
        assert!(3 * th <= 10, "invariant: 3 × 3 = 9 ≤ 10");

        // A supplied DATAFUSION_TARGET_PARTITIONS is ignored in AUTO mode.
        let (tp_ignored, th_ignored) = resolve_df_threading(
            ThreadingMode::Auto,
            &serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "99", PROP_DF_THREADS_PER_UDF: "99" }),
            16,
            4,
        );
        assert_eq!(th_ignored, 4, "AUTO ignores supplied threads");
        assert_eq!(tp_ignored, 4, "AUTO ignores supplied target partitions");
    }

    /// 1.5 — AUTO mode falls back to a single thread / partition when the core
    /// count is unknown (NR_OF_CORES = 0).
    #[test]
    fn auto_mode_falls_back_to_one_when_cores_zero() {
        let (target_partitions, threads) =
            resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 0, 8);
        assert_eq!(threads, 1, "cores=0 → 1 thread");
        assert_eq!(target_partitions, 1, "cores=0 → 1 target partition");
    }

    /// 1.5 — FIXED mode uses the operator-supplied values verbatim; absent or
    /// non-positive values fall back to max(NR_OF_CORES, 1) per field.
    #[test]
    fn fixed_mode_uses_supplied_values() {
        // Explicit positive values are used verbatim, regardless of cores.
        let props = serde_json::json!({
            PROP_DF_TARGET_PARTITIONS: "3",
            PROP_DF_THREADS_PER_UDF: "2",
        });
        let (tp, th) = resolve_df_threading(ThreadingMode::Fixed, &props, 16, 4);
        assert_eq!(tp, 3, "FIXED uses supplied target partitions verbatim");
        assert_eq!(th, 2, "FIXED uses supplied threads verbatim");

        // Absent values fall back to max(NR_OF_CORES, 1) — the pre-mode behaviour.
        let (tp_d, th_d) = resolve_df_threading(ThreadingMode::Fixed, &serde_json::json!({}), 8, 4);
        assert_eq!(tp_d, 8, "absent target partitions → max(cores,1) = 8");
        assert_eq!(th_d, 8, "absent threads → max(cores,1) = 8");

        // Unknown cores → 1.
        let (tp_z, th_z) = resolve_df_threading(ThreadingMode::Fixed, &serde_json::json!({}), 0, 4);
        assert_eq!(tp_z, 1, "absent target partitions, cores=0 → 1");
        assert_eq!(th_z, 1, "absent threads, cores=0 → 1");
    }

    // ---------------------------------------------------------------------------
    // TABLE_MAP round-trip, pushdown table derivation, and collision tests.
    // ---------------------------------------------------------------------------

    /// TABLE_MAP round-trips through build_adapter_notes → read_table_map.
    #[test]
    fn table_map_round_trips_through_adapter_notes() {
        let table_map = vec![
            ("ORDERS".to_string(), "prod.finance.orders".to_string()),
            (
                "EU__ORDERS".to_string(),
                "prod.finance.eu.orders".to_string(),
            ),
        ];
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &table_map,
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

        let pushdown_req = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes_str },
        });
        let recovered = read_table_map(&pushdown_req);
        assert_eq!(
            recovered.get("ORDERS").map(|s| s.as_str()),
            Some("prod.finance.orders"),
            "ORDERS must map to prod.finance.orders"
        );
        assert_eq!(
            recovered.get("EU__ORDERS").map(|s| s.as_str()),
            Some("prod.finance.eu.orders"),
            "EU__ORDERS must map to prod.finance.eu.orders"
        );
        assert_eq!(recovered.len(), 2, "map must have exactly two entries");
    }

    /// TABLE_MAP is stored as a nested JSON object, not a string.
    #[test]
    fn table_map_stored_as_nested_json_object() {
        let table_map = vec![("EVENTS".to_string(), "db.events".to_string())];
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &table_map,
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
        let parsed: serde_json::Value =
            serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
        // TABLE_MAP must be a JSON object, not a string.
        assert!(
            parsed[NOTE_TABLE_MAP].is_object(),
            "TABLE_MAP must be a nested JSON object: {parsed}"
        );
        assert_eq!(
            parsed[NOTE_TABLE_MAP]["EVENTS"].as_str(),
            Some("db.events"),
            "TABLE_MAP.EVENTS must equal 'db.events'"
        );
    }

    /// TABLE_MAP round-trip preserves other adapterNotes entries (merge, not clobber).
    #[test]
    fn table_map_merges_with_existing_notes() {
        let req = serde_json::json!({
            "type": "refresh",
            "schemaMetadataInfo": {
                "adapterNotes": "{\"CLUSTER_NODES\":\"5\",\"OTHER\":\"preserved\"}"
            },
        });
        let notes = build_adapter_notes(
            &req,
            5,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &[("T".to_string(), "ns.t".to_string())],
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(parsed["OTHER"].as_str(), Some("preserved"));
        assert_eq!(parsed[NOTE_CLUSTER_NODES].as_str(), Some("5"));
        assert!(parsed[NOTE_TABLE_MAP].is_object());
    }

    /// read_table_map returns an empty map when TABLE_MAP is absent from adapterNotes.
    #[test]
    fn read_table_map_absent_returns_empty() {
        let req = serde_json::json!({"type": "pushdown"});
        let map = read_table_map(&req);
        assert!(map.is_empty(), "absent TABLE_MAP must return empty map");

        // adapterNotes present but no TABLE_MAP key.
        let req2 = serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": {
                "adapterNotes": "{\"CLUSTER_NODES\":\"1\"}"
            },
        });
        let map2 = read_table_map(&req2);
        assert!(
            map2.is_empty(),
            "missing TABLE_MAP key must return empty map"
        );
    }

    /// Build a pushdown request whose adapterNotes carry `table_map` and whose
    /// involved virtual table is `involved`.
    fn pushdown_request_with_table_map(table_map: &[(String, String)], involved: &str) -> Json {
        let create_req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &create_req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            table_map,
        );
        let notes_str = notes.as_str().unwrap().to_string();
        serde_json::json!({
            "type": "pushdown",
            "schemaMetadataInfo": { "adapterNotes": notes_str },
            "involvedTables": [{"name": involved, "columns": []}],
        })
    }

    /// Pushdown with an unknown virtual table name returns a clear error naming it.
    #[test]
    fn pushdown_unknown_involved_table_errors() {
        let table_map = vec![("EVENTS".to_string(), "db.events".to_string())];
        let request = pushdown_request_with_table_map(&table_map, "UNKNOWN_TABLE");

        let err = resolve_pushdown_identifier(&request).unwrap_err();
        assert!(
            err.to_string().contains("UNKNOWN_TABLE"),
            "error must name the unknown table: {err}"
        );
    }

    /// TABLE_MAP lookup succeeds for a known virtual table name.
    #[test]
    fn pushdown_known_involved_table_resolves_identifier() {
        let table_map = vec![("ORDERS".to_string(), "prod.finance.orders".to_string())];
        let request = pushdown_request_with_table_map(&table_map, "ORDERS");

        assert_eq!(
            resolve_pushdown_identifier(&request).unwrap(),
            "prod.finance.orders",
            "ORDERS must resolve to prod.finance.orders"
        );
    }

    /// Multi-level namespace flattening is deterministic and collision detection
    /// returns a clear error naming the colliding Exasol table name.
    ///
    /// Scenario: configured `prod.finance` namespace.
    /// - ns `prod.finance` table `orders`   → Exasol name `ORDERS`
    /// - ns `prod.finance.eu` table `orders` → Exasol name `EU__ORDERS`
    ///
    /// Collision pair: ns `prod.finance` table `eu__orders` AND
    /// ns `prod.finance.eu` table `orders` both flatten to `EU__ORDERS`.
    #[test]
    fn flatten_multilevel_namespace_and_detect_collision() {
        use iceberg::{NamespaceIdent, TableIdent};

        let configured_ns = vec!["prod".to_string(), "finance".to_string()];

        let direct = TableIdent::new(
            NamespaceIdent::from_vec(vec!["prod".into(), "finance".into()]).unwrap(),
            "orders".into(),
        );
        let descendant = TableIdent::new(
            NamespaceIdent::from_vec(vec!["prod".into(), "finance".into(), "eu".into()]).unwrap(),
            "orders".into(),
        );

        let result = build_table_map(&configured_ns, &[direct, descendant]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            ("ORDERS".to_string(), "prod.finance.orders".to_string())
        );
        assert_eq!(
            result[1],
            (
                "EU__ORDERS".to_string(),
                "prod.finance.eu.orders".to_string()
            )
        );

        // Collision: ns `prod.finance` table `eu__orders` clashes with
        // ns `prod.finance.eu` table `orders` — both flatten to `EU__ORDERS`.
        let collider_a = TableIdent::new(
            NamespaceIdent::from_vec(vec!["prod".into(), "finance".into()]).unwrap(),
            "eu__orders".into(),
        );
        let collider_b = TableIdent::new(
            NamespaceIdent::from_vec(vec!["prod".into(), "finance".into(), "eu".into()]).unwrap(),
            "orders".into(),
        );
        let err = build_table_map(&configured_ns, &[collider_a, collider_b]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("EU__ORDERS"),
            "error must name the colliding Exasol table name: {msg}"
        );
        assert!(
            msg.contains("collision"),
            "error must mention 'collision': {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // build_virtual_tables — mixed Iceberg / non-Iceberg enumeration
    // -----------------------------------------------------------------------

    fn ns_ident(ns: Vec<&str>, table: &str) -> iceberg::TableIdent {
        use iceberg::{NamespaceIdent, TableIdent};
        TableIdent::new(
            NamespaceIdent::from_vec(ns.into_iter().map(|s| s.to_string()).collect()).unwrap(),
            table.to_string(),
        )
    }

    /// Injected resolver: an Iceberg table returns a one-column schema; a table
    /// whose name contains `hive` returns the exact code-authored HTTP 404 prefix
    /// the classifier keys on (the Glue "not an Iceberg table" signal).
    fn iceberg_or_404_resolver(
        ident: &iceberg::TableIdent,
    ) -> Result<Vec<(String, String)>, UdfError> {
        if ident.name.contains("hive") {
            Err(UdfError::User(
                "catalog returned HTTP 404: NoSuchIcebergTableException: Input table is not an iceberg table"
                    .into(),
            ))
        } else {
            Ok(vec![("ID".to_string(), "DECIMAL(20, 0)".to_string())])
        }
    }

    /// Every table in the namespace is Iceberg → all kept, nothing skipped.
    #[test]
    fn build_virtual_tables_keeps_all_when_every_table_is_iceberg() {
        let configured_ns = vec!["prod".to_string(), "finance".to_string()];
        let idents = vec![
            ns_ident(vec!["prod", "finance"], "orders"),
            ns_ident(vec!["prod", "finance"], "customers"),
        ];

        let (tables_json, table_map, skipped) =
            build_virtual_tables(&configured_ns, &idents, iceberg_or_404_resolver).unwrap();

        assert_eq!(tables_json.len(), 2);
        assert_eq!(table_map.len(), 2);
        assert!(
            skipped.is_empty(),
            "no table is skipped when all are Iceberg"
        );
        assert_eq!(
            table_map[0],
            ("ORDERS".to_string(), "prod.finance.orders".to_string())
        );
    }

    /// A non-Iceberg (HTTP 404) table is skipped, not aborted: the surviving
    /// Iceberg tables are kept, the skipped name is excluded from TABLE_MAP, and
    /// the skipped identifier is returned so the handler can warn on it.
    #[test]
    fn build_virtual_tables_skips_non_iceberg_table_and_warns() {
        let configured_ns = vec!["prod".to_string(), "finance".to_string()];
        let idents = vec![
            ns_ident(vec!["prod", "finance"], "orders"),
            ns_ident(vec!["prod", "finance"], "hive_events"),
            ns_ident(vec!["prod", "finance"], "customers"),
        ];

        let (tables_json, table_map, skipped) =
            build_virtual_tables(&configured_ns, &idents, iceberg_or_404_resolver).unwrap();

        let survivor_names: Vec<&str> = tables_json
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            survivor_names,
            vec!["ORDERS", "CUSTOMERS"],
            "only the Iceberg tables are emitted, in listing order"
        );

        // TABLE_MAP is built from survivors only — the skipped name is absent.
        let mapped: Vec<&str> = table_map.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(mapped, vec!["ORDERS", "CUSTOMERS"]);
        assert!(
            !mapped.contains(&"HIVE_EVENTS"),
            "a skipped table must never appear in TABLE_MAP"
        );

        // The skipped identifier is returned verbatim for the handler's warning.
        assert_eq!(skipped.len(), 1);
        assert_eq!(
            iceberg_identifier_string(&skipped[0]),
            "prod.finance.hive_events"
        );
    }

    /// A namespace whose every table is non-Iceberg yields an empty schema — the
    /// call still SUCCEEDS (an all-Hive namespace is not a catalog fault).
    #[test]
    fn build_virtual_tables_all_non_iceberg_yields_empty_schema() {
        let configured_ns = vec!["prod".to_string(), "finance".to_string()];
        let idents = vec![
            ns_ident(vec!["prod", "finance"], "hive_a"),
            ns_ident(vec!["prod", "finance"], "hive_b"),
        ];

        let (tables_json, table_map, skipped) =
            build_virtual_tables(&configured_ns, &idents, iceberg_or_404_resolver).unwrap();

        assert!(tables_json.is_empty(), "no Iceberg table → empty tables");
        assert!(table_map.is_empty(), "no survivor → empty TABLE_MAP");
        assert_eq!(skipped.len(), 2, "every non-Iceberg table is skipped");
    }

    /// A namespace the catalog reports as holding NO table costs zero catalog
    /// contact: there is nothing to resolve, so no session is built.
    ///
    /// Driven against an unreachable `catalog_uri` with OAuth2 client-credentials —
    /// the mode whose session build issues a token grant — so an unguarded build
    /// could only fail. The call must still succeed with an empty table list, which
    /// is the boundary an unconditionally hoisted session build silently changed:
    /// one grant instead of none, and a request that used to return an empty schema
    /// now failing on the grant.
    #[test]
    fn create_virtual_schema_over_empty_namespace_contacts_no_catalog_session() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        let creds = ConnectionCreds {
            warehouse: "warehouse".into(),
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            path_style: true,
            use_sigv4: false,
            use_vended_credentials: false,
            token: None,
            client_id: Some("oauth-client-id-sentinel".into()),
            client_secret: Some("oauth-client-secret-sentinel".into()),
            oauth2_server_uri: None,
            scope: None,
        };
        let configured_ns = vec!["prod".to_string(), "finance".to_string()];

        let (tables_json, table_map, skipped) = resolve_namespace_virtual_tables(
            &rt,
            "http://127.0.0.1:1",
            &creds,
            &configured_ns,
            &[],
        )
        .expect("an empty namespace must resolve without contacting the catalog");

        assert!(
            tables_json.is_empty(),
            "an empty namespace advertises no virtual table"
        );
        assert!(table_map.is_empty(), "no table → empty TABLE_MAP");
        assert!(skipped.is_empty(), "no table → nothing skipped");
    }

    /// A non-404 per-table failure (transport / non-404 HTTP) aborts the whole
    /// enumeration — genuine catalog faults must still fail loudly.
    #[test]
    fn build_virtual_tables_aborts_on_non_404_error() {
        let configured_ns = vec!["prod".to_string(), "finance".to_string()];
        let idents = vec![
            ns_ident(vec!["prod", "finance"], "orders"),
            ns_ident(vec!["prod", "finance"], "boom"),
        ];

        let resolver = |ident: &iceberg::TableIdent| -> Result<Vec<(String, String)>, UdfError> {
            if ident.name == "boom" {
                Err(UdfError::User(
                    "catalog returned HTTP 503: service unavailable".into(),
                ))
            } else {
                Ok(vec![("ID".to_string(), "DECIMAL(20, 0)".to_string())])
            }
        };

        let err = build_virtual_tables(&configured_ns, &idents, resolver).unwrap_err();
        assert!(
            err.to_string().contains("503"),
            "a non-404 per-table failure must abort and propagate: {err}"
        );
    }

    /// Collision detection still fires over the surviving set even when a skipped
    /// (non-Iceberg) table sits between two survivors that flatten to one name.
    #[test]
    fn build_virtual_tables_survivors_keep_collision_detection() {
        let configured_ns = vec!["prod".to_string(), "finance".to_string()];
        let idents = vec![
            ns_ident(vec!["prod", "finance"], "eu__orders"),
            ns_ident(vec!["prod", "finance"], "hive_events"),
            ns_ident(vec!["prod", "finance", "eu"], "orders"),
        ];

        let err =
            build_virtual_tables(&configured_ns, &idents, iceberg_or_404_resolver).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("collision"),
            "a survivor collision must still abort: {msg}"
        );
        assert!(
            msg.contains("EU__ORDERS"),
            "the error must name the colliding Exasol table: {msg}"
        );
    }

    /// Build a table_map, write it through build_adapter_notes, parse the
    /// adapterNotes JSON string, and assert:
    /// - TABLE_MAP contains the expected Exasol-name → Iceberg-identifier entries.
    /// - A pre-existing note (CLUSTER_NODES) is still present after the merge.
    #[test]
    fn create_vs_records_table_map_in_adapter_notes() {
        use iceberg::{NamespaceIdent, TableIdent};

        let configured_ns = vec!["prod".to_string(), "finance".to_string()];
        let idents = vec![
            TableIdent::new(
                NamespaceIdent::from_vec(vec!["prod".into(), "finance".into()]).unwrap(),
                "orders".into(),
            ),
            TableIdent::new(
                NamespaceIdent::from_vec(vec!["prod".into(), "finance".into(), "eu".into()])
                    .unwrap(),
                "orders".into(),
            ),
        ];
        let table_map = build_table_map(&configured_ns, &idents).unwrap();

        // Simulate a request with a pre-existing CLUSTER_NODES note.
        let request = serde_json::json!({
            "type": "createVirtualSchema",
            "schemaMetadataInfo": {
                "adapterNotes": "{\"CLUSTER_NODES\":\"3\"}"
            }
        });
        let notes = build_adapter_notes(
            &request,
            3,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            ThreadingMode::Auto,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_DF_BATCH_SIZE,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
            DEFAULT_S3_MAX_CONNECTIONS,
            DEFAULT_JOIN_BROADCAST_MAX_BYTES,
            &table_map,
        );
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
        let parsed: serde_json::Value =
            serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");

        // TABLE_MAP must be a nested object mapping Exasol names → Iceberg identifiers.
        let table_map_obj = parsed[NOTE_TABLE_MAP]
            .as_object()
            .expect("TABLE_MAP must be a JSON object");
        assert_eq!(
            table_map_obj.get("ORDERS").and_then(|v| v.as_str()),
            Some("prod.finance.orders"),
            "TABLE_MAP must map ORDERS → prod.finance.orders"
        );
        assert_eq!(
            table_map_obj.get("EU__ORDERS").and_then(|v| v.as_str()),
            Some("prod.finance.eu.orders"),
            "TABLE_MAP must map EU__ORDERS → prod.finance.eu.orders"
        );

        // Pre-existing CLUSTER_NODES must survive the merge (not clobbered).
        assert_eq!(
            parsed[NOTE_CLUSTER_NODES].as_str(),
            Some("3"),
            "CLUSTER_NODES must be preserved after build_adapter_notes"
        );
    }

    // ---------------------------------------------------------------------------
    // S3_MAX_CONNECTIONS resolution (Task 2.3 / Scenario Coverage rows 3–5)
    // ---------------------------------------------------------------------------

    /// Scenario: FIXED value overrides the AUTO derivation at createVirtualSchema.
    ///
    /// An explicit positive-integer property is used verbatim regardless of the
    /// node capacity that AUTO would otherwise derive a different budget from.
    #[test]
    fn resolve_s3_max_connections_fixed_value_wins() {
        let props = serde_json::json!({ PROP_S3_MAX_CONNECTIONS: "64" });
        // Cores/instances would AUTO-derive 8 * 4 = 32; the explicit value must win.
        assert_eq!(
            resolve_s3_max_connections(&props, 8, 1),
            64,
            "explicit S3_MAX_CONNECTIONS must be used verbatim"
        );
        // Independent of node capacity (even the unknown-cores path).
        assert_eq!(
            resolve_s3_max_connections(&props, 0, 4),
            64,
            "explicit value wins even when cores are unknown"
        );
    }

    /// Scenario: AUTO derivation sizes the per-instance budget from node capacity.
    ///
    /// With no explicit property the budget is `per_instance_threads * mult`, and
    /// the aggregate per-node budget (`instances * per_instance`) tracks
    /// `nr_of_cores * mult` regardless of the instance/thread split.
    #[test]
    fn resolve_s3_max_connections_auto_scales_with_cores() {
        let absent = serde_json::json!({});

        // One instance on an 8-core node: 8 threads * 4 = 32 connections.
        assert_eq!(
            resolve_s3_max_connections(&absent, 8, 1),
            8 * S3_CONNECTIONS_PER_THREAD,
            "single instance gets the whole node's core count * multiplier"
        );

        // Eight single-thread instances on the same node: 1 thread * 4 = 4 each,
        // and the aggregate (8 * 4 = 32) matches the one-instance case above.
        let per_instance = resolve_s3_max_connections(&absent, 8, 8);
        assert_eq!(
            per_instance, S3_CONNECTIONS_PER_THREAD,
            "each of eight instances gets one thread's worth of connections"
        );
        assert_eq!(
            8 * per_instance,
            8 * S3_CONNECTIONS_PER_THREAD,
            "aggregate per-node budget is invariant across the instance/thread split"
        );

        // A larger node scales the budget up.
        assert_eq!(
            resolve_s3_max_connections(&absent, 16, 1),
            16 * S3_CONNECTIONS_PER_THREAD,
            "budget scales with core count"
        );

        // Empty / zero / invalid property strings all fall through to AUTO.
        for bad in ["", "0", "not-a-number", "-4"] {
            let props = serde_json::json!({ PROP_S3_MAX_CONNECTIONS: bad });
            assert_eq!(
                resolve_s3_max_connections(&props, 8, 1),
                8 * S3_CONNECTIONS_PER_THREAD,
                "invalid property {bad:?} must AUTO-derive, not pin a bad value"
            );
        }

        // Never collapses below 1 (more instances than cores → 1 thread each).
        assert!(
            resolve_s3_max_connections(&absent, 2, 8) >= 1,
            "AUTO budget must never collapse below 1"
        );
    }

    /// Scenario: AUTO derivation falls back to the default budget when the core
    /// count is unknown (the `0` sentinel), rather than producing a zero/negative
    /// budget.
    #[test]
    fn resolve_s3_max_connections_auto_zero_cores_defaults() {
        let absent = serde_json::json!({});
        assert_eq!(
            resolve_s3_max_connections(&absent, 0, 1),
            DEFAULT_S3_MAX_CONNECTIONS,
            "unknown cores (0) must fall back to the built-in default"
        );
        // Instance share is irrelevant once cores are unknown.
        assert_eq!(
            resolve_s3_max_connections(&absent, 0, 8),
            DEFAULT_S3_MAX_CONNECTIONS,
            "0-cores fallback ignores the instance share"
        );
    }
}
