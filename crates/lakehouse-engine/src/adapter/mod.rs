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
use crate::scan::spec::StorageBackend;
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
            // Resolve credentials synchronously before entering the async runtime:
            // ctx.connection(), reached via resolve_connection_config, is a
            // connect-back round-trip that may block on the UDF host, so it must
            // not run inside the tokio runtime built below.
            //
            // ctx.script_schema() and cluster_nodes_from_context(ctx) are captured
            // here too, but for a different reason — they are plain handshake-
            // metadata field reads, not connect-back calls. Capturing them outside
            // the async block keeps the planning body free of ambient-state reads
            // and of a dependency on the UDF delivery mechanism. script_schema is
            // the schema that qualifies the scan/distributor/merge UDF names in the
            // generated pushdown SQL.
            let props = get_properties(request);
            let (catalog_uri, storage, creds, allow_http) = resolve_connection_config(ctx, &props)?;
            let script_schema = ctx.script_schema();
            let cluster_nodes = cluster_nodes_from_context(ctx);

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;
            rt.block_on(async {
                handle_pushdown_request(
                    request,
                    &catalog_uri,
                    &storage,
                    &creds,
                    allow_http,
                    &script_schema,
                    cluster_nodes,
                )
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
///
/// The returned `bool` is the resolved `ALLOW_HTTP` property, read in this one place
/// because both storage selectors need it: `storage_block` bakes it into the static
/// S3 payload, the vended selector uses it as its plaintext-transport consent gate.
fn resolve_connection_config(
    ctx: &dyn UdfContext,
    props: &Json,
) -> Result<(String, StorageBackend, ConnectionCreds, bool), UdfError> {
    let resolved = read_connection(ctx, nonempty_str(props, PROP_CATALOG_CONNECTION))?;
    let allow_http = nonempty_str(props, PROP_ALLOW_HTTP)
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let storage = storage_block(&resolved.creds, allow_http);
    Ok((resolved.uri, storage, resolved.creds, allow_http))
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
    // `ALLOW_HTTP` is discarded here: schema enumeration reaches no vended selector,
    // and `storage_block` already baked it into `storage`.
    let (catalog_uri, storage, creds, _) = resolve_connection_config(ctx, &props)?;

    let iceberg_namespace = nonempty_str(&props, PROP_ICEBERG_NAMESPACE)
        .ok_or_else(|| UdfError::User(format!("property '{PROP_ICEBERG_NAMESPACE}' is required")))?
        .to_string();

    let configured_ns: Vec<String> = iceberg_namespace
        .split('.')
        .map(|s| s.to_string())
        .collect();

    let nr_of_cores = resolve_nr_of_cores(&props);
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
/// Errors propagate unredacted — no `ctx` and no `StorageBackend` reach here, so the
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
    storage: &StorageBackend,
    creds: &ConnectionCreds,
    allow_http: bool,
    script_schema: &str,
    cluster_nodes: usize,
) -> Result<Json, UdfError> {
    // PARALLELISM_FACTOR and the other tuning values below are carried in
    // adapterNotes (persisted by Exasol), NOT in properties (dropped by
    // Exasol). Read them from schemaMetadataInfo.adapterNotes; default to
    // safe values when absent. cluster_nodes is no longer one of them — it
    // arrives as a parameter, captured from the live handshake in dispatch.
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
        allow_http,
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
/// *string* (Exasol rejects a raw object) carrying NR_OF_CORES,
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

/// Per-node CPU core count used to derive the AUTO parallelism, DataFusion
/// threading, and S3 connection budgets.
///
/// Comes from the `NR_OF_CORES` VS property override (via
/// [`parse_nr_of_cores_override`]) when it resolves to a positive integer;
/// otherwise it is auto-detected from `std::thread::available_parallelism()`.
/// A result of `0` signals "unknown" (auto-detect unavailable); callers must
/// handle the floor case.
fn resolve_nr_of_cores(props: &Json) -> u32 {
    parse_nr_of_cores_override(props).unwrap_or_else(available_parallelism_or_0)
}

/// Cluster node count for pushdown sharding, read directly from the UDF
/// handshake via [`UdfContext::node_count`] — no persisted note, no
/// create-time capture. A live cluster reports its node count directly (`1`
/// on a single node); a `0` (stub, test double, or missing handshake) maps
/// to `1` so sharding keeps the single-shard fallback behaviour.
fn cluster_nodes_from_context(ctx: &dyn UdfContext) -> usize {
    match ctx.node_count() {
        0 => 1,
        n => n as usize,
    }
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
fn redact_error(storage: &StorageBackend, e: UdfError) -> UdfError {
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
#[path = "adapter_tests.rs"]
mod tests;
