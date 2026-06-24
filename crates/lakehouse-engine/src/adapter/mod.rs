/// VS adapter logic: createVirtualSchema, getCapabilities, pushdown,
/// refreshVirtualSchema, dropVirtualSchema.
///
/// Credentials (access_key, secret_key, session_token) NEVER appear in error messages.
pub mod capabilities;
pub mod connection;
pub mod pushdown;
pub mod sharding;
pub mod sigv4;

use crate::adapter::capabilities::get_capabilities_response;
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::connection::{catalog_block, read_connection, storage_block};
use crate::adapter::pushdown::{handle_pushdown, resolve_table_schema};
use crate::scan::spec::{CatalogProps, StorageProps};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use serde_json::{Value as Json, json};

// Property key names sent in VS request `properties` / `schemaMetadataInfo.properties`.
// `TABLE` is an Exasol reserved keyword and cannot be used as a bare VS property
// name in CREATE VIRTUAL SCHEMA, so the property is named TABLE_NAME.
const PROP_TABLE: &str = "TABLE_NAME";
// Required: name of the Exasol CONNECTION object that holds the catalog URI
// (as its address) and the credential JSON (as its password).
const PROP_CATALOG_CONNECTION: &str = "CATALOG_CONNECTION";
// Allow HTTP to the catalog/storage endpoint (opt-in; defaults to false).
const PROP_ALLOW_HTTP: &str = "ALLOW_HTTP";
// Schema that holds the LAKEHOUSE_SCAN SET script. The pushdown SQL must
// reference the scan UDF schema-qualified, because it executes outside the
// adapter script's schema context. Optional: unqualified when unset.
const PROP_SCAN_SCHEMA: &str = "SCAN_SCHEMA";
// Optional: name of an Exasol CONNECTION object whose credentials are used to
// open a connect-back session for `SELECT NPROC()`. When absent, CLUSTER_NODES
// defaults to 1 without error.
const PROP_CONNECTION_NAME: &str = "CONNECTION_NAME";
// Key written into the createVirtualSchema response under
// schemaMetadata.adapterNotes (a stringified JSON object) so that subsequent
// requests (pushdown, refresh) can read the resolved node count back from
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
// VS property names for DataFusion per-instance thread configuration.
const PROP_DF_TARGET_PARTITIONS: &str = "DATAFUSION_TARGET_PARTITIONS";
const PROP_DF_THREADS_PER_UDF: &str = "DATAFUSION_THREADS_PER_UDF";
// adapterNotes keys for the DataFusion thread configuration.
const NOTE_DF_TARGET_PARTITIONS: &str = "DF_TARGET_PARTITIONS";
const NOTE_DF_THREADS_PER_UDF: &str = "DF_THREADS_PER_UDF";
/// Default DataFusion `target_partitions` per UDF instance (1 = no intra-instance partitioning).
const DEFAULT_DF_TARGET_PARTITIONS: usize = 1;
/// Default Tokio worker threads per UDF instance (1 = current-thread runtime).
const DEFAULT_DF_THREADS_PER_UDF: usize = 1;
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
        Some("refreshVirtualSchema") => {
            // Stateless: refresh = re-resolve schema, same as create.
            handle_create_virtual_schema(ctx, request)
        }
        Some("dropVirtualSchema") => Ok(json!({"type": "dropVirtualSchema"})),
        Some("pushdown") => {
            // Resolve credentials synchronously before entering the async runtime.
            // ctx.connection() is a synchronous call that must not be invoked inside
            // an async context (it may block on the UDF host). Mirror the pattern
            // used by resolve_cluster_nodes.
            let props = get_properties(request);
            let (catalog_uri, storage, catalog, creds) = resolve_connection_config(ctx, &props)?;

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;
            rt.block_on(async {
                handle_pushdown_request(request, &catalog_uri, &storage, &catalog, &creds).await
            })
        }
        other => Err(UdfError::User(format!(
            "unsupported VS request type: {}",
            other.unwrap_or("(none)")
        ))),
    }
}

/// Resolve the catalog/storage configuration from the `CATALOG_CONNECTION`
/// object and the `TABLE_NAME` property. Shared by the createVirtualSchema and
/// pushdown entry points. `ctx.connection()` is synchronous and must be called
/// before entering any async runtime.
fn resolve_connection_config(
    ctx: &dyn UdfContext,
    props: &Json,
) -> Result<(String, StorageProps, CatalogProps, ConnectionCreds), UdfError> {
    let resolved = read_connection(ctx, str_prop(props, PROP_CATALOG_CONNECTION))?;
    let mut storage = storage_block(&resolved.creds);
    let table = str_prop(props, PROP_TABLE)
        .ok_or_else(|| UdfError::User(format!("property '{PROP_TABLE}' is required")))?
        .to_string();
    let catalog = catalog_block(&resolved.creds, &resolved.uri, &table);
    storage.allow_http = str_prop(props, PROP_ALLOW_HTTP)
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    Ok((resolved.uri, storage, catalog, resolved.creds))
}

fn handle_create_virtual_schema(
    ctx: &mut dyn UdfContext,
    request: &Json,
) -> Result<Json, UdfError> {
    let props = get_properties(request);
    let (catalog_uri, storage, catalog, creds) = resolve_connection_config(ctx, &props)?;

    let (cluster_nodes, nr_of_cores) = resolve_cluster_nodes(ctx, &props);
    let parallelism_factor = resolve_parallelism_factor(&props, nr_of_cores);
    let df_target_partitions = resolve_df_target_partitions(&props);
    let df_threads_per_udf = resolve_df_threads_per_udf(&props);
    let memory_pool_fraction = resolve_memory_pool_fraction(&props);
    let instance_overhead_mb = resolve_instance_overhead_mb(&props);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;

    let fields: Vec<(String, String)> = rt
        .block_on(async { resolve_table_schema(&catalog_uri, &catalog, &storage, &creds).await })
        .map_err(|e| redact_error(&storage, e))?;

    // Build the virtual table columns JSON.
    let columns: Vec<Json> = fields
        .iter()
        .map(|(name, ty)| {
            json!({
                "name": name,
                "dataType": exasol_type_to_json(ty),
            })
        })
        .collect();

    let table_name = catalog
        .table
        .split('.')
        .next_back()
        .unwrap_or(&catalog.table)
        .to_uppercase();
    // Exasol persists `adapterNotes` (a JSON *string*) at the schema level and
    // passes it back in `schemaMetadataInfo.adapterNotes` on later requests.
    // Carry the resolved node count and parallelism factor there; merge into any
    // pre-existing notes so we never clobber state another channel may have written.
    let adapter_notes = build_adapter_notes(
        request,
        cluster_nodes,
        nr_of_cores,
        parallelism_factor,
        df_target_partitions,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
    );
    let schema_metadata = json!({
        "tables": [{
            "name": table_name,
            "columns": columns,
        }],
        "adapterNotes": adapter_notes,
    });

    let response_type =
        if request.get("type").and_then(|t| t.as_str()) == Some("createVirtualSchema") {
            "createVirtualSchema"
        } else {
            "refreshVirtualSchema"
        };

    Ok(json!({
        "type": response_type,
        "schemaMetadata": schema_metadata,
    }))
}

async fn handle_pushdown_request(
    request: &Json,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<Json, UdfError> {
    let props = get_properties(request);
    let scan_schema = str_prop(&props, PROP_SCAN_SCHEMA).map(|s| s.to_string());
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
    handle_pushdown(
        request,
        catalog_uri,
        storage,
        catalog,
        scan_schema.as_deref(),
        cluster_nodes,
        parallelism_factor,
        df_target_partitions,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
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

fn str_prop<'a>(props: &'a Json, key: &str) -> Option<&'a str> {
    props
        .get(key)
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

/// Build the adapterNotes value for the createVirtualSchema response: a JSON
/// *string* (Exasol rejects a raw object) carrying CLUSTER_NODES, NR_OF_CORES,
/// PARALLELISM_FACTOR, DF_TARGET_PARTITIONS, DF_THREADS_PER_UDF,
/// MEMORY_POOL_FRACTION, and INSTANCE_OVERHEAD_MB. Any pre-existing notes on
/// the request are preserved (merge, not clobber).
// ponytail: args mirror the resolved notes fields one-to-one; a params struct is
// pure boilerplate for a single private callee.
#[allow(clippy::too_many_arguments)]
fn build_adapter_notes(
    request: &Json,
    cluster_nodes: u32,
    nr_of_cores: u32,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
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
        NOTE_DF_TARGET_PARTITIONS.to_string(),
        Json::String(df_target_partitions.to_string()),
    );
    notes.insert(
        NOTE_DF_THREADS_PER_UDF.to_string(),
        Json::String(df_threads_per_udf.to_string()),
    );
    notes.insert(
        NOTE_MEMORY_POOL_FRACTION.to_string(),
        Json::String(memory_pool_fraction.to_string()),
    );
    notes.insert(
        NOTE_INSTANCE_OVERHEAD_MB.to_string(),
        Json::String(instance_overhead_mb.to_string()),
    );
    Json::String(Json::Object(notes).to_string())
}

/// Read and validate the PARALLELISM_FACTOR VS property.
///
/// When the property is absent, empty, zero, or invalid, the default is
/// `max(nr_of_cores * 2, DEFAULT_PARALLELISM_FACTOR)` — hardware-aware but
/// floored at `DEFAULT_PARALLELISM_FACTOR` so a dev VM or failed core-count
/// lookup (nr_of_cores = 0) never collapses the factor below a useful minimum.
fn resolve_parallelism_factor(props: &Json, nr_of_cores: u32) -> usize {
    str_prop(props, PROP_PARALLELISM_FACTOR)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| ((nr_of_cores as usize) * 2).max(DEFAULT_PARALLELISM_FACTOR))
}

/// Read and validate the DATAFUSION_TARGET_PARTITIONS VS property.
///
/// When the property is absent, empty, zero, or invalid the default is 1 (one
/// DataFusion partition per UDF instance, which prevents intra-instance CPU
/// fan-out from multiplying with the cluster-level shard fan-out).
fn resolve_df_target_partitions(props: &Json) -> usize {
    str_prop(props, PROP_DF_TARGET_PARTITIONS)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_DF_TARGET_PARTITIONS)
}

/// Read and validate the DATAFUSION_THREADS_PER_UDF VS property.
///
/// When the property is absent, empty, zero, or invalid the default is 1 (one
/// Tokio worker thread per UDF instance, matching `new_current_thread()` behaviour).
fn resolve_df_threads_per_udf(props: &Json) -> usize {
    str_prop(props, PROP_DF_THREADS_PER_UDF)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_DF_THREADS_PER_UDF)
}

/// Read and validate the MEMORY_POOL_FRACTION VS property.
///
/// Accepts any value in the range (0.0, 1.0]. When the property is absent, empty,
/// zero, out-of-range, or unparseable the default is `DEFAULT_MEMORY_POOL_FRACTION`.
fn resolve_memory_pool_fraction(props: &Json) -> f64 {
    str_prop(props, PROP_MEMORY_POOL_FRACTION)
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
    str_prop(props, PROP_INSTANCE_OVERHEAD_MB)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INSTANCE_OVERHEAD_MB)
}

/// Open a connect-back session and run `SELECT NPROC()` and
/// `SELECT PARAM_VALUE('NR_OF_CORES')` to obtain the active cluster node count
/// and the per-node CPU core count.
///
/// Returns `(1, 0)` when `CONNECTION_NAME` is absent and `(1, 0)` on any
/// connect-back failure so `createVirtualSchema` never fails due to an
/// unreachable or misconfigured connect-back path. A `nr_of_cores` of `0`
/// signals "unknown"; callers must handle the floor case.
fn resolve_cluster_nodes(ctx: &mut dyn UdfContext, props: &Json) -> (u32, u32) {
    let Some(conn_name) = str_prop(props, PROP_CONNECTION_NAME) else {
        return (1, 0);
    };
    let result = (|| -> Result<(u32, u32), UdfError> {
        let conn_obj = ctx.connection(conn_name)?;
        let mut session = ctx.connect_back(&conn_obj)?;

        let nproc_rows = session.query("SELECT NPROC()")?;
        let nproc_value = nproc_rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next());
        let cluster_nodes = nproc_value_to_count(nproc_value);

        let cores_rows = session.query("SELECT PARAM_VALUE('NR_OF_CORES')")?;
        let cores_value = cores_rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next());
        let nr_of_cores = varchar_value_to_u32(cores_value);

        Ok((cluster_nodes, nr_of_cores))
    })();
    result.unwrap_or((1, 0))
}

/// Convert the first cell of a `SELECT NPROC()` result to a positive node count.
/// Returns 1 for NULL, zero, negative, or unrecognised value variants.
fn nproc_value_to_count(value: Option<exasol_udf_sdk::value::Value>) -> u32 {
    use exasol_udf_sdk::value::Value;
    let n: i64 = match value {
        Some(Value::Int32(v)) => v as i64,
        Some(Value::Int64(v)) => v,
        Some(Value::Numeric(d)) if d.scale == 0 => i64::try_from(d.unscaled).unwrap_or(0),
        _ => 0,
    };
    if n >= 1 { n as u32 } else { 1 }
}

/// Convert the first cell of a `SELECT PARAM_VALUE(...)` result (a VARCHAR) to a
/// `u32`. Returns `0` for NULL, empty, non-numeric, zero, or negative values.
fn varchar_value_to_u32(value: Option<exasol_udf_sdk::value::Value>) -> u32 {
    use exasol_udf_sdk::value::Value;
    let s = match value {
        Some(Value::String(s)) => s,
        _ => return 0,
    };
    s.trim().parse::<u32>().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// JSON type serialization helpers
// ---------------------------------------------------------------------------

/// Convert an Exasol type string to the VS column dataType JSON object.
/// Minimal implementation covering the types produced by our mapping.
fn exasol_type_to_json(exasol_type: &str) -> Json {
    let upper = exasol_type.to_uppercase();
    if upper == "BOOLEAN" {
        return json!({"type": "boolean"});
    }
    if upper == "DOUBLE PRECISION" {
        return json!({"type": "double"});
    }
    if upper == "DATE" {
        return json!({"type": "date"});
    }
    if upper == "TIMESTAMP" {
        return json!({"type": "timestamp"});
    }
    if upper == "TIMESTAMP WITH LOCAL TIME ZONE" {
        return json!({"type": "timestamp with local time zone"});
    }
    if let Some(inner) = upper
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2
            && let (Ok(p), Ok(s)) = (
                parts[0].trim().parse::<u64>(),
                parts[1].trim().parse::<u64>(),
            )
        {
            return json!({"type": "decimal", "precision": p, "scale": s});
        }
    }
    // Default: VARCHAR(size)
    let size = if let Some(inner) = upper
        .strip_prefix("VARCHAR(")
        .and_then(|s| s.strip_suffix(')'))
    {
        inner.trim().parse::<u64>().unwrap_or(2000000)
    } else {
        2000000
    };
    json!({"type": "varchar", "size": size})
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

    #[test]
    fn exasol_type_to_json_roundtrip() {
        let cases = [
            ("BOOLEAN", "boolean"),
            ("DOUBLE PRECISION", "double"),
            ("DATE", "date"),
            ("TIMESTAMP", "timestamp"),
        ];
        for (ty, expected_type) in cases {
            let j = exasol_type_to_json(ty);
            assert_eq!(
                j["type"].as_str().unwrap().to_lowercase(),
                expected_type,
                "type mismatch for {ty}"
            );
        }
        let dec = exasol_type_to_json("DECIMAL(18,4)");
        assert_eq!(dec["precision"].as_u64().unwrap(), 18);
        assert_eq!(dec["scale"].as_u64().unwrap(), 4);
    }

    // Minimal UdfContext for dispatch tests that need no I/O.
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

    #[test]
    fn cluster_nodes_defaults_to_one_on_connect_back_failure() {
        // NoopCtx returns UdfError::Unimplemented for all connect-back methods,
        // exercising the default-to-1 path without any network I/O.
        let props = serde_json::json!({
            PROP_CONNECTION_NAME: "SOME_CONNECTION"
        });
        let (count, _cores) = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(count, 1u32);
    }

    #[test]
    fn cluster_nodes_defaults_to_one_when_no_connection_name() {
        let props = serde_json::json!({});
        let (count, cores) = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(count, 1u32);
        assert_eq!(cores, 0u32);
    }

    /// Verifies that the createVirtualSchema response JSON carries CLUSTER_NODES
    /// in schemaMetadata.adapterNotes (a JSON *string*, the only channel Exasol
    /// persists) under the default-1 path (no CONNECTION_NAME).
    ///
    /// Exercises the JSON-assembly seam without catalog or network I/O.
    #[test]
    fn create_response_carries_cluster_nodes_property() {
        let props = serde_json::json!({});
        let (cluster_nodes, nr_of_cores) = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(cluster_nodes, 1u32, "default cluster_nodes must be 1");

        // Replicate the schema_metadata construction from handle_create_virtual_schema.
        // The request has no pre-existing adapterNotes (clean set path).
        let request = serde_json::json!({"type": "createVirtualSchema"});
        let adapter_notes = build_adapter_notes(
            &request,
            cluster_nodes,
            nr_of_cores,
            DEFAULT_PARALLELISM_FACTOR,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
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
            "CLUSTER_NODES must be \"1\" on the default path, got \"{val}\""
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
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
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
            "type": "refreshVirtualSchema",
            "schemaMetadataInfo": {
                "adapterNotes": "{\"OTHER_KEY\":\"keep-me\",\"CLUSTER_NODES\":\"1\"}"
            },
        });
        let notes = build_adapter_notes(
            &req,
            3,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
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
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
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
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
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
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed[NOTE_NR_OF_CORES].as_str(),
            Some("16"),
            "NR_OF_CORES must be written into adapterNotes"
        );
    }

    /// Scenario: NR_OF_CORES defaults to 0 when resolve_cluster_nodes cannot reach
    /// the database (NoopCtx returns an error for all connect-back calls).
    #[test]
    fn nr_of_cores_defaults_to_zero_when_unavailable() {
        let props = serde_json::json!({ PROP_CONNECTION_NAME: "SOME_CONNECTION" });
        let (_nodes, nr_of_cores) = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(
            nr_of_cores, 0u32,
            "nr_of_cores must default to 0 on connect-back failure"
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

    /// Scenario: DF_TARGET_PARTITIONS defaults to 1 when property is absent/zero/invalid.
    #[test]
    fn df_target_partitions_defaults_to_one() {
        let absent = serde_json::json!({});
        assert_eq!(resolve_df_target_partitions(&absent), 1, "absent → 1");

        let zero = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "0" });
        assert_eq!(resolve_df_target_partitions(&zero), 1, "zero → 1");

        let invalid = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "bad" });
        assert_eq!(resolve_df_target_partitions(&invalid), 1, "invalid → 1");
    }

    /// Scenario: An explicit positive DATAFUSION_TARGET_PARTITIONS property is used as-is.
    #[test]
    fn df_target_partitions_uses_supplied_value() {
        let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "4" });
        let val = resolve_df_target_partitions(&props);
        assert_eq!(val, 4, "explicit value must be returned");

        // Verify it round-trips through adapterNotes.
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            val,
            DEFAULT_DF_THREADS_PER_UDF,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
        );
        let parsed: serde_json::Value =
            serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
        assert_eq!(
            parsed[NOTE_DF_TARGET_PARTITIONS].as_str(),
            Some("4"),
            "DF_TARGET_PARTITIONS must round-trip through adapterNotes"
        );
    }

    /// Scenario: DF_THREADS_PER_UDF defaults to 1 when property is absent/zero/invalid.
    #[test]
    fn df_threads_per_udf_defaults_to_one() {
        let absent = serde_json::json!({});
        assert_eq!(resolve_df_threads_per_udf(&absent), 1, "absent → 1");

        let zero = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "0" });
        assert_eq!(resolve_df_threads_per_udf(&zero), 1, "zero → 1");

        let invalid = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "not-a-number" });
        assert_eq!(resolve_df_threads_per_udf(&invalid), 1, "invalid → 1");
    }

    /// Scenario: An explicit positive DATAFUSION_THREADS_PER_UDF property is used as-is.
    #[test]
    fn df_threads_per_udf_uses_supplied_value() {
        let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
        let val = resolve_df_threads_per_udf(&props);
        assert_eq!(val, 2, "explicit value must be returned");

        // Verify it round-trips through adapterNotes.
        let req = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(
            &req,
            1,
            0,
            DEFAULT_PARALLELISM_FACTOR,
            DEFAULT_DF_TARGET_PARTITIONS,
            val,
            DEFAULT_MEMORY_POOL_FRACTION,
            DEFAULT_INSTANCE_OVERHEAD_MB,
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

        // Empty string → default (str_prop filters empty strings).
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

        // Empty string → default (str_prop filters empty strings).
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
            DEFAULT_DF_TARGET_PARTITIONS,
            DEFAULT_DF_THREADS_PER_UDF,
            0.5,
            256,
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
}
