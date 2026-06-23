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
// VS property name for the parallelism factor (oversubscription multiplier).
// Default: 8. Stored in adapterNotes so the pushdown path can read it back.
const PROP_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
const NOTE_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
/// Default parallelism factor when not supplied or invalid.
const DEFAULT_PARALLELISM_FACTOR: usize = 8;

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

    let cluster_nodes = resolve_cluster_nodes(ctx, &props);
    let parallelism_factor = resolve_parallelism_factor(&props);

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
    let adapter_notes = build_adapter_notes(request, cluster_nodes, parallelism_factor);
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
    handle_pushdown(
        request,
        catalog_uri,
        storage,
        catalog,
        scan_schema.as_deref(),
        cluster_nodes,
        parallelism_factor,
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
/// *string* (Exasol rejects a raw object) carrying CLUSTER_NODES and
/// PARALLELISM_FACTOR. Any pre-existing notes on the request are preserved
/// (merge, not clobber).
fn build_adapter_notes(request: &Json, cluster_nodes: u32, parallelism_factor: usize) -> Json {
    let mut notes = parse_adapter_notes(request);
    notes.insert(
        NOTE_CLUSTER_NODES.to_string(),
        Json::String(cluster_nodes.to_string()),
    );
    notes.insert(
        NOTE_PARALLELISM_FACTOR.to_string(),
        Json::String(parallelism_factor.to_string()),
    );
    Json::String(Json::Object(notes).to_string())
}

/// Read and validate the PARALLELISM_FACTOR VS property.
/// Returns DEFAULT_PARALLELISM_FACTOR when absent, empty, zero, or not a valid integer.
fn resolve_parallelism_factor(props: &Json) -> usize {
    str_prop(props, PROP_PARALLELISM_FACTOR)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_PARALLELISM_FACTOR)
}

/// Open a connect-back session and run `SELECT NPROC()` to obtain the active
/// cluster node count. Returns 1 on any failure so `createVirtualSchema` never
/// fails due to an unreachable or misconfigured connect-back path.
fn resolve_cluster_nodes(ctx: &mut dyn UdfContext, props: &Json) -> u32 {
    let Some(conn_name) = str_prop(props, PROP_CONNECTION_NAME) else {
        return 1;
    };
    let result = (|| -> Result<u32, UdfError> {
        let conn_obj = ctx.connection(conn_name)?;
        let mut session = ctx.connect_back(&conn_obj)?;
        let rows = session.query("SELECT NPROC()")?;
        let value = rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next());
        Ok(nproc_value_to_count(value))
    })();
    result.unwrap_or(1)
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
        let count = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(count, 1u32);
    }

    #[test]
    fn cluster_nodes_defaults_to_one_when_no_connection_name() {
        let props = serde_json::json!({});
        let count = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(count, 1u32);
    }

    /// Verifies that the createVirtualSchema response JSON carries CLUSTER_NODES
    /// in schemaMetadata.adapterNotes (a JSON *string*, the only channel Exasol
    /// persists) under the default-1 path (no CONNECTION_NAME).
    ///
    /// Exercises the JSON-assembly seam without catalog or network I/O.
    #[test]
    fn create_response_carries_cluster_nodes_property() {
        let props = serde_json::json!({});
        let cluster_nodes = resolve_cluster_nodes(&mut NoopCtx, &props);
        assert_eq!(cluster_nodes, 1u32, "default cluster_nodes must be 1");

        // Replicate the schema_metadata construction from handle_create_virtual_schema.
        // The request has no pre-existing adapterNotes (clean set path).
        let request = serde_json::json!({"type": "createVirtualSchema"});
        let adapter_notes =
            build_adapter_notes(&request, cluster_nodes, DEFAULT_PARALLELISM_FACTOR);
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
        let notes = build_adapter_notes(&create_req, 4, DEFAULT_PARALLELISM_FACTOR);
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
        let notes = build_adapter_notes(&req, 3, DEFAULT_PARALLELISM_FACTOR);
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
        // Request with an explicit PARALLELISM_FACTOR property.
        let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "4" });
        let factor = resolve_parallelism_factor(&props);
        assert_eq!(factor, 4, "factor must be read from the property");

        // Build adapterNotes and verify PARALLELISM_FACTOR is present.
        let request = serde_json::json!({"type": "createVirtualSchema"});
        let notes = build_adapter_notes(&request, 2, factor);
        let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
        let parsed: serde_json::Value =
            serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
        assert_eq!(
            parsed[NOTE_PARALLELISM_FACTOR].as_str(),
            Some("4"),
            "PARALLELISM_FACTOR must be recorded in adapterNotes"
        );

        // Default when property absent.
        let empty_props = serde_json::json!({});
        let default_factor = resolve_parallelism_factor(&empty_props);
        assert_eq!(
            default_factor, DEFAULT_PARALLELISM_FACTOR,
            "must default to {DEFAULT_PARALLELISM_FACTOR} when property absent"
        );

        // Zero or invalid value also defaults.
        let zero_props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "0" });
        let zero_factor = resolve_parallelism_factor(&zero_props);
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
        let notes = build_adapter_notes(&create_req, 6, 12);
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
}
