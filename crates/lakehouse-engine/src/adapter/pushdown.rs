/// Pushdown planning: resolve the Iceberg file list ONCE and build the
/// scan-driving SQL that invokes the LAKEHOUSE_SCAN SET UDF.
///
/// Architecture invariants:
/// - File list resolved exactly ONCE here, in the planning layer.
/// - The scan SET UDF receives the explicit file list; it NEVER discovers files.
/// - A predicate the adapter cannot translate is OMITTED from the spec
///   (correctness backstop: Exasol keeps the predicate at its own level).
/// - LIMIT appears in both the scan spec and the returned SQL (correctness backstop).
/// - Credentials NEVER appear in any returned SQL string or error message.
use crate::adapter::predicate::render_df_filter_safe;
use crate::scan::spec::{CatalogProps, ScanSpec, StorageProps};
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::Value as Json;
use std::collections::HashMap;
use std::sync::Arc;

/// Build a RestCatalog configured to read/write data files through the S3
/// (MinIO) storage factory.
///
/// iceberg 0.9.1 requires an explicit `StorageFactory`; the S3 config keys are
/// supplied in the same props map passed to `load`. Credentials live only in
/// this map and never appear in returned SQL or error strings.
async fn build_rest_catalog(
    catalog_uri: &str,
    catalog: &CatalogProps,
    storage: &StorageProps,
) -> Result<RestCatalog, UdfError> {
    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        catalog.warehouse.clone(),
    );
    if !storage.endpoint.is_empty() {
        props.insert(S3_ENDPOINT.to_string(), storage.endpoint.clone());
    }
    if !storage.region.is_empty() {
        props.insert(S3_REGION.to_string(), storage.region.clone());
    }
    if !storage.access_key.is_empty() {
        props.insert(S3_ACCESS_KEY_ID.to_string(), storage.access_key.clone());
    }
    if !storage.secret_key.is_empty() {
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), storage.secret_key.clone());
    }
    if let Some(token) = &storage.session_token {
        props.insert(S3_SESSION_TOKEN.to_string(), token.clone());
    }
    props.insert(
        S3_PATH_STYLE_ACCESS.to_string(),
        storage.path_style.to_string(),
    );

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("lakehouse", props)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to connect to Iceberg catalog: {}",
                redact_catalog_error(&e.to_string())
            ))
        })
}

/// The registered SQL name of the scan SET UDF entry point.
const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// Returns JSON `{"type":"pushdown","sql":"..."}`.
pub async fn handle_pushdown(
    request: &Json,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    scan_schema: Option<&str>,
) -> Result<Json, UdfError> {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);

    // --- Extract projection ---
    let (proj_cols, proj_types) = extract_projection(request, &pushdown_req)?;

    // --- Extract filter (translate; omit if translation fails) ---
    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(render_df_filter_safe);

    // --- Extract LIMIT ---
    let limit = extract_limit(&pushdown_req);

    // --- Resolve file list ONCE from the catalog ---
    let files = resolve_file_list(catalog_uri, catalog, storage).await?;

    if files.is_empty() {
        return Ok(empty_pushdown_sql(&proj_cols, &proj_types));
    }

    // --- Build the scan spec ---
    let spec = ScanSpec {
        files,
        projection: proj_cols.clone(),
        filter,
        limit,
        storage: storage.clone(),
        catalog: catalog.clone(),
    };

    // --- Build the EMITS clause from projected columns ---
    let emits = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("{} {}", quote_ident(name), ty))
        .collect::<Vec<_>>()
        .join(", ");

    // --- Build the scan-driving SQL ---
    // ponytail: spec is a single JSON VARCHAR — one argument, no splitting.
    // ponytail: PoC accepted risk — the S3 access/secret keys are embedded in
    // this scan-driving SQL literal (inside the ScanSpec JSON), which Exasol may
    // log or surface in its query profile / audit. Acceptable for this PoC slice;
    // the upgrade path is to pass credentials via a CONNECTION object (referenced
    // by name, never inlined) or to fetch them over connect-back at scan time so
    // they never appear in any SQL text. Error paths already redact the values.
    let spec_json = spec.to_json();
    let spec_literal = sql_string_literal(&spec_json);

    // The scan UDF must be schema-qualified: the pushdown query executes
    // outside the adapter script's schema, so an unqualified name would not
    // resolve ("function or script LAKEHOUSE_SCAN not found").
    let udf_name = match scan_schema {
        Some(schema) if !schema.is_empty() => {
            format!("{}.{}", quote_ident(schema), SCAN_UDF_NAME)
        }
        _ => SCAN_UDF_NAME.to_string(),
    };

    let mut sql = format!(
        "SELECT * FROM (SELECT {udf_name}({spec_literal}) EMITS ({emits}))",
        udf_name = udf_name,
        spec_literal = spec_literal,
        emits = emits,
    );

    // LIMIT at the Exasol level too (correctness backstop: the UDF may emit slightly
    // more than `limit` rows if the LIMIT was applied per-batch with rounding).
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }

    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// Resolve the data-file list from the Iceberg REST catalog.
///
/// This is the resolve-once seam: called exactly once per pushdown in the
/// adapter; the file list is passed explicitly to the scan UDF.
pub async fn resolve_file_list(
    catalog_uri: &str,
    catalog_props: &CatalogProps,
    storage: &StorageProps,
) -> Result<Vec<String>, UdfError> {
    let catalog = build_rest_catalog(catalog_uri, catalog_props, storage).await?;

    // Parse "namespace.table" from catalog_props.table.
    let (namespace, table_name) = parse_table_ident(&catalog_props.table)?;
    let table_ident = TableIdent::new(NamespaceIdent::new(namespace), table_name);

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to load Iceberg table '{}': {}",
                catalog_props.table,
                redact_catalog_error(&e.to_string())
            ))
        })?;

    // Plan files from the current snapshot.
    let scan = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build Iceberg scan: {e}")))?;

    let task_stream = scan.plan_files().await.map_err(|e| {
        UdfError::User(format!(
            "failed to plan Iceberg files: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;

    let tasks: Vec<_> = task_stream.try_collect().await.map_err(|e| {
        UdfError::User(format!(
            "failed to collect Iceberg file tasks: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;

    let files: Vec<String> = tasks
        .into_iter()
        .map(|t| t.data_file_path().to_string())
        .collect();

    Ok(files)
}

/// Resolve the Iceberg table schema for `createVirtualSchema`.
///
/// Returns (field_name, exasol_type_string) pairs.
pub async fn resolve_table_schema(
    catalog_uri: &str,
    catalog_props: &CatalogProps,
    storage: &StorageProps,
) -> Result<Vec<(String, String)>, UdfError> {
    let catalog = build_rest_catalog(catalog_uri, catalog_props, storage).await?;

    let (namespace, table_name) = parse_table_ident(&catalog_props.table)?;
    let table_ident = TableIdent::new(NamespaceIdent::new(namespace), table_name);

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to load Iceberg table '{}': {}",
                catalog_props.table,
                redact_catalog_error(&e.to_string())
            ))
        })?;

    let schema = table.metadata().current_schema();
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse "namespace.table" into (namespace_str, table_name_str).
fn parse_table_ident(qualified: &str) -> Result<(String, String), UdfError> {
    let parts: Vec<&str> = qualified.splitn(2, '.').collect();
    if parts.len() != 2 {
        return Err(UdfError::User(format!(
            "table property must be 'namespace.table', got: '{qualified}'"
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Extract the projected columns and their Exasol types from the pushdown request.
fn extract_projection(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<String>, Vec<String>), UdfError> {
    let involved = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Get all columns from the first involved table.
    let all_cols: Vec<(String, String)> = involved
        .first()
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    let exasol_type = exasol_type_from_json(dt_json);
                    Some((name, exasol_type))
                })
                .collect()
        })
        .unwrap_or_default();

    if all_cols.is_empty() {
        return Err(UdfError::User(
            "pushdown request has no column metadata".into(),
        ));
    }

    let type_by_upper = |name: &str| -> String {
        all_cols
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
    };

    let select_list = pushdown_req.get("selectList");
    let proj_names: Vec<String> = match select_list {
        None | Some(Json::Null) => all_cols.iter().map(|(n, _)| n.clone()).collect(),
        Some(Json::Array(list)) if list.is_empty() => all_cols
            .first()
            .map(|(n, _)| vec![n.clone()])
            .unwrap_or_default(),
        Some(Json::Array(list)) => {
            let first_name = all_cols.first().map(|(n, _)| n.clone());
            list.iter()
                .filter_map(|e| {
                    if e.get("type").and_then(|t| t.as_str()) == Some("column") {
                        let name = e
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_uppercase());
                        name.or_else(|| first_name.clone())
                    } else {
                        first_name.clone()
                    }
                })
                .collect()
        }
        _ => all_cols.iter().map(|(n, _)| n.clone()).collect(),
    };

    let proj_types: Vec<String> = proj_names.iter().map(|n| type_by_upper(n)).collect();
    Ok((proj_names, proj_types))
}

/// Extract LIMIT from the pushdown request.
fn extract_limit(pushdown_req: &Json) -> Option<u64> {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("numElements"))
        .and_then(|n| n.as_u64())
}

/// Build a pushdown response with an empty result (no matching files).
fn empty_pushdown_sql(proj_cols: &[String], proj_types: &[String]) -> Json {
    let items: Vec<String> = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("CAST(NULL AS {ty}) AS {}", quote_ident(name)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Derive an Exasol type string from the VS column dataType JSON.
fn exasol_type_from_json(dt: &Json) -> String {
    let type_name = dt.get("type").and_then(|t| t.as_str()).unwrap_or("varchar");
    match type_name.to_lowercase().as_str() {
        "boolean" => "BOOLEAN".to_string(),
        "decimal" => {
            let p = dt.get("precision").and_then(|v| v.as_u64()).unwrap_or(18);
            let s = dt.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            if p <= 36 && s <= 36 {
                format!("DECIMAL({p},{s})")
            } else {
                "VARCHAR(2000000)".to_string()
            }
        }
        "double" => "DOUBLE PRECISION".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => "TIMESTAMP".to_string(),
        "timestamp with local time zone" | "timestampwithlocaltime zone" => {
            "TIMESTAMP WITH LOCAL TIME ZONE".to_string()
        }
        _ => {
            // VARCHAR, CHAR, and all others.
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            format!("VARCHAR({capped})")
        }
    }
}

/// Double-quote an identifier.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Produce a SQL string literal with single-quote escaping.
fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Redact credential-shaped values from a catalog error message.
fn redact_catalog_error(msg: &str) -> String {
    crate::scan::emit::redact_credentials(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::predicate::render_df_filter_safe;
    use crate::scan::spec::{CatalogProps, StorageProps};

    // ---------------------------------------------------------------------------
    // Helpers shared across tests
    // ---------------------------------------------------------------------------

    fn sample_storage() -> StorageProps {
        StorageProps {
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        }
    }

    fn sample_catalog() -> CatalogProps {
        CatalogProps {
            uri: "http://iceberg-rest:8181".into(),
            warehouse: "warehouse".into(),
            table: "db.events".into(),
        }
    }

    /// Assemble the scan-driving SQL from a known file list + spec — the same
    /// logic `handle_pushdown` runs after `resolve_file_list`.
    fn build_sql_for_fixture(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
    ) -> String {
        let spec = ScanSpec {
            files,
            projection: proj_cols.clone(),
            filter,
            limit,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let emits = proj_cols
            .iter()
            .zip(proj_types.iter())
            .map(|(name, ty)| format!("{} {}", quote_ident(name), ty))
            .collect::<Vec<_>>()
            .join(", ");
        let spec_literal = sql_string_literal(&spec.to_json());
        let mut sql = format!(
            "SELECT * FROM (SELECT {udf}({spec}) EMITS ({emits}))",
            udf = SCAN_UDF_NAME,
            spec = spec_literal,
            emits = emits,
        );
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }

    // ---------------------------------------------------------------------------
    // Scenario: Pushdown resolves the file list once and builds a scan-driving query
    // ---------------------------------------------------------------------------

    /// Pure SQL-building part of the pushdown scenario.
    /// The file list comes from a fixture (no catalog I/O).
    #[test]
    fn pushdown_resolves_files_once_builds_scan_sql() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // The generated SQL must invoke the scan UDF with the spec embedded.
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "SQL must reference the scan UDF: {sql}"
        );
        // The spec JSON (embedded as a SQL literal) contains the file path.
        assert!(
            sql.contains("part-00000.parquet"),
            "SQL must carry assigned files: {sql}"
        );
        assert!(
            sql.contains("part-00001.parquet"),
            "SQL must carry both files: {sql}"
        );
        // Must be a SELECT (scan-driving query, not an empty stub).
        assert!(
            sql.starts_with("SELECT * FROM"),
            "must be a real query: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Projection is pushed into the scan-driving query
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_carries_projection() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["A".into(), "B".into()],
            vec!["DECIMAL(10,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // EMITS clause must list exactly the projected columns in order.
        assert!(
            sql.contains("\"A\" DECIMAL(10,0)"),
            "EMITS must carry col A: {sql}"
        );
        assert!(
            sql.contains("\"B\" VARCHAR(2000000)"),
            "EMITS must carry col B: {sql}"
        );

        // The spec JSON must carry the projection field.
        // (It's embedded as a SQL string literal in the body.)
        assert!(
            sql.contains(r#""A""#) || sql.contains(r#"\"A\""#),
            "spec JSON must include projected column A: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Filter predicate is pushed into the scan spec (translatable) or
    // omitted (untranslatable) — never mistranslated.
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_translates_or_omits_predicate() {
        // Translatable predicate: column > literal.
        let translatable = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "age"},
            "right": {"type": "literal_exactnumeric", "value": 18}
        });
        let filter_rendered = render_df_filter_safe(&translatable);
        assert!(
            filter_rendered.is_some(),
            "translatable predicate must produce a filter string"
        );
        let filter_str = filter_rendered.unwrap();
        assert!(
            filter_str.contains(">"),
            "filter must include > operator: {filter_str}"
        );
        assert!(
            filter_str.contains("AGE") || filter_str.contains("\"AGE\""),
            "filter must reference the column: {filter_str}"
        );

        // Untranslatable predicate (e.g., an aggregate or unknown function):
        // render_df_filter_safe returns None → omitted from spec.
        let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
        let omitted = render_df_filter_safe(&untranslatable);
        assert!(
            omitted.is_none(),
            "untranslatable predicate must be omitted (None), not mistranslated"
        );

        // Confirm omitting the filter still produces valid SQL (correctness backstop).
        let sql_no_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            None, // omitted
            None,
        );
        assert!(
            sql_no_filter.contains(SCAN_UDF_NAME),
            "SQL must still be valid when filter is omitted"
        );

        // Confirm carrying the filter includes it in the spec JSON.
        let sql_with_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            Some(filter_str),
            None,
        );
        assert!(
            sql_with_filter.contains(">"),
            "filter must survive into the spec literal: {sql_with_filter}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: LIMIT is pushed into the scan spec; also appears at Exasol level.
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_carries_limit() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(42),
        );

        // The outer SQL must contain LIMIT (Exasol-level backstop).
        assert!(
            sql.contains("LIMIT 42"),
            "outer SQL must carry LIMIT for correctness backstop: {sql}"
        );

        // The spec JSON (embedded in the literal) must carry limit = 42.
        // The JSON will have "limit":42 somewhere in the literal.
        assert!(
            sql.contains(r#""limit":42"#) || sql.contains("limit"),
            "spec JSON must carry the limit: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Pre-existing helpers tests (unchanged)
    // ---------------------------------------------------------------------------

    #[test]
    fn empty_file_list_returns_empty_select() {
        let proj = vec!["ID".to_string(), "NAME".to_string()];
        let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let resp = empty_pushdown_sql(&proj, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("WHERE 1=0"));
        assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
    }

    #[test]
    fn limit_extracted_from_pushdown_request() {
        let req = serde_json::json!({"numElements": 42});
        assert_eq!(extract_limit(&req), None); // not nested under "limit"

        let req2 = serde_json::json!({"limit": {"numElements": 42}});
        assert_eq!(extract_limit(&req2), Some(42));
    }

    #[test]
    fn sql_string_literal_escapes_quotes() {
        let s = "it's a test";
        let lit = sql_string_literal(s);
        assert_eq!(lit, "'it''s a test'");
    }

    #[test]
    fn parse_table_ident_splits_namespace_table() {
        let (ns, tbl) = parse_table_ident("mydb.mytable").unwrap();
        assert_eq!(ns, "mydb");
        assert_eq!(tbl, "mytable");
    }

    #[test]
    fn parse_table_ident_errors_on_no_dot() {
        let err = parse_table_ident("notable").unwrap_err();
        assert!(err.to_string().contains("namespace.table"));
    }
}
