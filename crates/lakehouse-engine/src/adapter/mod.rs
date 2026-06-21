/// VS adapter logic: createVirtualSchema, getCapabilities, pushdown,
/// refreshVirtualSchema, dropVirtualSchema.
///
/// Credentials (access_key, secret_key, session_token) NEVER appear in error messages.
pub mod capabilities;
pub mod predicate;
pub mod pushdown;

use crate::adapter::capabilities::get_capabilities_response;
use crate::adapter::pushdown::{handle_pushdown, resolve_table_schema};
use crate::scan::spec::{CatalogProps, StorageProps};
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use serde_json::{Value as Json, json};

// Property key names sent in VS request `properties` / `schemaMetadataInfo.properties`.
const PROP_CATALOG_URI: &str = "CATALOG_URI";
const PROP_WAREHOUSE: &str = "WAREHOUSE";
// `TABLE` is an Exasol reserved keyword and cannot be used as a bare VS property
// name in CREATE VIRTUAL SCHEMA, so the property is named TABLE_NAME.
const PROP_TABLE: &str = "TABLE_NAME";
const PROP_S3_ENDPOINT: &str = "S3_ENDPOINT";
const PROP_S3_REGION: &str = "S3_REGION";
const PROP_ACCESS_KEY: &str = "ACCESS_KEY";
const PROP_SECRET_KEY: &str = "SECRET_KEY";
const PROP_SESSION_TOKEN: &str = "SESSION_TOKEN";
const PROP_ALLOW_HTTP: &str = "ALLOW_HTTP";
// Schema that holds the LAKEHOUSE_SCAN SET script. The pushdown SQL must
// reference the scan UDF schema-qualified, because it executes outside the
// adapter script's schema context. Optional: unqualified when unset.
const PROP_SCAN_SCHEMA: &str = "SCAN_SCHEMA";

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

fn dispatch(_ctx: &mut dyn UdfContext, request: &Json) -> Result<Json, UdfError> {
    match request.get("type").and_then(|t| t.as_str()) {
        Some("getCapabilities") => Ok(get_capabilities_response()),
        Some("createVirtualSchema") => handle_create_virtual_schema(request),
        Some("refreshVirtualSchema") => {
            // Stateless: refresh = re-resolve schema, same as create.
            handle_create_virtual_schema(request)
        }
        Some("dropVirtualSchema") => Ok(json!({"type": "dropVirtualSchema"})),
        Some("pushdown") => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;
            rt.block_on(async { handle_pushdown_request(request).await })
        }
        other => Err(UdfError::User(format!(
            "unsupported VS request type: {}",
            other.unwrap_or("(none)")
        ))),
    }
}

fn handle_create_virtual_schema(request: &Json) -> Result<Json, UdfError> {
    let props = get_properties(request);
    let (catalog_uri, storage, catalog) = extract_connection_props(&props)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build tokio runtime: {e}")))?;

    let fields: Vec<(String, String)> = rt
        .block_on(async { resolve_table_schema(&catalog_uri, &catalog, &storage).await })
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
    let schema_metadata = json!({
        "tables": [{
            "name": table_name,
            "columns": columns,
        }]
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

async fn handle_pushdown_request(request: &Json) -> Result<Json, UdfError> {
    let props = get_properties(request);
    let (catalog_uri, storage, catalog) = extract_connection_props(&props)?;
    let scan_schema = str_prop(&props, PROP_SCAN_SCHEMA).map(|s| s.to_string());
    handle_pushdown(
        request,
        &catalog_uri,
        &storage,
        &catalog,
        scan_schema.as_deref(),
    )
    .await
    .map_err(|e| redact_error(&storage, e))
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

/// Extract catalog URI, StorageProps, and CatalogProps from the VS properties.
/// Returns credential-safe errors.
fn extract_connection_props(
    props: &Json,
) -> Result<(String, StorageProps, CatalogProps), UdfError> {
    let catalog_uri = str_prop(props, PROP_CATALOG_URI)
        .ok_or_else(|| {
            UdfError::User(format!(
                "property '{PROP_CATALOG_URI}' is required (the Iceberg REST catalog endpoint)"
            ))
        })?
        .to_string();

    let warehouse = str_prop(props, PROP_WAREHOUSE)
        .ok_or_else(|| UdfError::User(format!("property '{PROP_WAREHOUSE}' is required")))?
        .to_string();

    let table = str_prop(props, PROP_TABLE)
        .ok_or_else(|| UdfError::User(format!("property '{PROP_TABLE}' is required")))?
        .to_string();

    let access_key = str_prop(props, PROP_ACCESS_KEY).unwrap_or("").to_string();
    let secret_key = str_prop(props, PROP_SECRET_KEY).unwrap_or("").to_string();

    let storage = StorageProps {
        endpoint: str_prop(props, PROP_S3_ENDPOINT).unwrap_or("").to_string(),
        region: str_prop(props, PROP_S3_REGION)
            .unwrap_or("us-east-1")
            .to_string(),
        access_key,
        secret_key,
        session_token: str_prop(props, PROP_SESSION_TOKEN).map(|s| s.to_string()),
        allow_http: str_prop(props, PROP_ALLOW_HTTP)
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(true),
        path_style: true, // MinIO requires path-style access.
    };

    let catalog = CatalogProps {
        uri: catalog_uri.clone(),
        warehouse,
        table,
    };

    Ok((catalog_uri, storage, catalog))
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
}
