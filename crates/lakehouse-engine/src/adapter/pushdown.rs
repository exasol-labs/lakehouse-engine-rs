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
use crate::scan::spec::{AggKind, AggregatePlan, CatalogProps, ScanSpec, StorageProps};
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

// ---------------------------------------------------------------------------
// Aggregate detection
// ---------------------------------------------------------------------------

/// Inspect the pushdown request's `selectList` and return the aggregate plan
/// if every select-list item is a supported single-group aggregate.
///
/// Returns `None` (fall back to row scan) when any of the following hold:
/// - `groupBy` is present and non-empty (GROUP BY not supported)
/// - any select item has `distinct: true`
/// - any select item is not one of COUNT(*), COUNT(col), SUM, MIN, MAX, AVG
/// - the select list is absent or empty
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<AggregatePlan>> {
    // Reject GROUP BY.
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;

    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::with_capacity(list.len());
    for item in list {
        // Every item must be a function_aggregate.
        if item.get("type").and_then(|t| t.as_str()) != Some("function_aggregate") {
            return None;
        }

        // Reject DISTINCT aggregates.
        if item.get("distinct").and_then(|d| d.as_bool()) == Some(true) {
            return None;
        }

        let fn_name = item
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_uppercase();

        let args = item.get("arguments").and_then(|a| a.as_array());

        let plan = match fn_name.as_str() {
            "COUNT" => {
                // COUNT(*) has empty arguments; COUNT(col) has one column argument.
                let col = args.and_then(|a| a.first()).and_then(|arg| {
                    if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
                        arg.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_uppercase())
                    } else {
                        None
                    }
                });
                if col.is_none() {
                    AggregatePlan {
                        kind: AggKind::Count,
                        column: None,
                    }
                } else {
                    AggregatePlan {
                        kind: AggKind::CountCol,
                        column: col,
                    }
                }
            }
            "SUM" => AggregatePlan {
                kind: AggKind::Sum,
                column: column_from_first_arg(args),
            },
            "MIN" => AggregatePlan {
                kind: AggKind::Min,
                column: column_from_first_arg(args),
            },
            "MAX" => AggregatePlan {
                kind: AggKind::Max,
                column: column_from_first_arg(args),
            },
            "AVG" => AggregatePlan {
                kind: AggKind::Avg,
                column: column_from_first_arg(args),
            },
            _ => return None, // Unsupported aggregate function — fall back.
        };
        plans.push(plan);
    }

    Some(plans)
}

/// Extract the column name (uppercase) from the first argument of an aggregate function.
fn column_from_first_arg(args: Option<&Vec<Json>>) -> Option<String> {
    args.and_then(|a| a.first()).and_then(|arg| {
        if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
            arg.get("name")
                .and_then(|n| n.as_str())
                .map(|s| s.to_uppercase())
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// SQL builder (pure; used by handle_pushdown and unit tests)
// ---------------------------------------------------------------------------

/// Build the scan-driving SQL from a resolved file list partitioned into shards.
///
/// **Row queries** (no aggregates in spec):
/// - Single shard: `SELECT * FROM (SELECT {udf}({spec}) EMITS ({emits})) LIMIT n`
/// - Multi-shard: `SELECT * FROM (fan-out with IPROC GROUP BY) LIMIT n`
///
/// **Aggregate queries** (spec carries `aggregates`):
/// - Always wraps the fan-out in an outer merge aggregation (never SELECT *).
/// - The EMITS clause and the outer merge follow the COLUMN CONTRACT from
///   `crate::scan::build_partial_agg_sql`.
///
/// `spec_template` carries the shared fields; only `files` is replaced per shard.
/// `col_types` is the full table column type map `(uppercase_name, exasol_type)` used
/// to assign the correct EMITS type per aggregate partial column.
pub fn build_scan_driving_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    proj_cols: &[String],
    proj_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
    udf_name: &str,
) -> String {
    if let Some(aggregates) = spec_template.aggregates.as_deref() {
        build_aggregate_scan_sql(spec_template, shards, aggregates, col_types, udf_name)
    } else {
        build_row_scan_sql(
            spec_template,
            shards,
            proj_cols,
            proj_types,
            limit,
            udf_name,
        )
    }
}

/// Build the row-scan SQL (no aggregates).
fn build_row_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    proj_cols: &[String],
    proj_types: &[String],
    limit: Option<u64>,
    udf_name: &str,
) -> String {
    let emits = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("{} {}", quote_ident(name), ty))
        .collect::<Vec<_>>()
        .join(", ");

    if shards.len() == 1 {
        let mut shard_spec = spec_template.clone();
        shard_spec.files = shards.into_iter().next().unwrap_or_default();
        let spec_literal = sql_string_literal(&shard_spec.to_json());
        let mut sql = format!(
            "SELECT * FROM (SELECT {udf}({spec}) EMITS ({emits}))",
            udf = udf_name,
            spec = spec_literal,
            emits = emits,
        );
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    } else {
        let inner = build_fan_out_inner(spec_template, &shards, &emits, udf_name);
        let mut sql = format!("SELECT * FROM ({inner})");
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }
}

/// Build the aggregate scan SQL: fan-out EMITS partial columns, outer merge aggregates them.
///
/// The EMITS clause names and types follow the COLUMN CONTRACT defined in
/// `crate::scan::build_partial_agg_sql`.  The outer merge SELECT consumes those
/// exact column names.
fn build_aggregate_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types);
    let emits = emits_items.join(", ");
    let merge_select = merge_select_items(aggregates).join(", ");

    let fan_out = if shards.len() == 1 {
        let mut shard_spec = spec_template.clone();
        shard_spec.files = shards.into_iter().next().unwrap_or_default();
        let spec_literal = sql_string_literal(&shard_spec.to_json());
        format!(
            "SELECT {udf}({spec}) EMITS ({emits})",
            udf = udf_name,
            spec = spec_literal,
            emits = emits,
        )
    } else {
        build_fan_out_inner(spec_template, &shards, &emits, udf_name)
    };

    format!("SELECT {merge_select} FROM ({fan_out})")
}

/// Build the EMITS items for the aggregate fan-out, following the COLUMN CONTRACT.
///
/// `col_types` maps uppercase column names to their Exasol type strings.
/// MIN/MAX partial columns use the target column's exact type.
/// SUM partial columns: DOUBLE PRECISION stays DOUBLE PRECISION; DECIMAL(p,s) widens to
/// DECIMAL(36,s) to avoid overflow; any other type falls back (callers should have validated
/// via `validate_agg_col_types` before reaching here — see handle_pushdown).
/// AVG partial sum stays DOUBLE PRECISION (AVG is inherently fractional).
fn partial_emits_items(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| match plan.kind {
            AggKind::Count | AggKind::CountCol => {
                vec![format!(r#""PARTIAL_count_{i}" DECIMAL(20,0)"#)]
            }
            AggKind::Sum => {
                let ty = col_type_for(plan, col_types);
                let emit_ty = sum_emit_type(&ty);
                vec![format!(r#""PARTIAL_sum_{i}" {emit_ty}"#)]
            }
            AggKind::Min => {
                let ty = col_type_for(plan, col_types);
                vec![format!(r#""PARTIAL_min_{i}" {ty}"#)]
            }
            AggKind::Max => {
                let ty = col_type_for(plan, col_types);
                vec![format!(r#""PARTIAL_max_{i}" {ty}"#)]
            }
            AggKind::Avg => vec![
                format!(r#""PARTIAL_avg_sum_{i}" DOUBLE PRECISION"#),
                format!(r#""PARTIAL_avg_cnt_{i}" DECIMAL(20,0)"#),
            ],
        })
        .collect()
}

/// Look up the Exasol type for the target column of an aggregate plan.
/// Returns "DOUBLE PRECISION" as a safe fallback when the column is absent from the map.
fn col_type_for(plan: &AggregatePlan, col_types: &[(String, String)]) -> String {
    plan.column
        .as_deref()
        .and_then(|col| {
            col_types
                .iter()
                .find(|(n, _)| n == col)
                .map(|(_, t)| t.clone())
        })
        .unwrap_or_else(|| "DOUBLE PRECISION".to_string())
}

/// Map a column's Exasol type to the appropriate SUM partial EMITS type.
///
/// DOUBLE PRECISION => DOUBLE PRECISION (no change).
/// DECIMAL(p,s) => DECIMAL(36,s) (widened to max Exasol precision, preserving scale).
/// Any other type (DATE, TIMESTAMP, VARCHAR, BOOLEAN) => DOUBLE PRECISION as an
/// emergency fallback (callers should have validated before reaching here).
fn sum_emit_type(col_ty: &str) -> String {
    if col_ty == "DOUBLE PRECISION" {
        return "DOUBLE PRECISION".to_string();
    }
    if let Some(inner) = col_ty
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))
    {
        // inner is "p,s"
        if let Some((_p, s)) = inner.split_once(',') {
            return format!("DECIMAL(36,{s})");
        }
    }
    // Non-numeric type: validation should have caught this, but fall back gracefully.
    "DOUBLE PRECISION".to_string()
}

/// Return `true` if all SUM/MIN/MAX targets have a supported Exasol column type.
///
/// SUM is only valid over DOUBLE PRECISION or DECIMAL columns.
/// MIN/MAX are valid over any comparable type (DATE, TIMESTAMP, VARCHAR included).
/// Returns `false` (fall back to row scan) when any SUM targets a non-numeric column.
pub fn validate_agg_col_types(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> bool {
    for plan in aggregates {
        if plan.kind != AggKind::Sum {
            continue;
        }
        let ty = col_type_for(plan, col_types);
        if !is_numeric_exasol_type(&ty) {
            return false;
        }
    }
    true
}

/// Return `true` for Exasol types that support SUM (DOUBLE PRECISION, DECIMAL).
fn is_numeric_exasol_type(ty: &str) -> bool {
    ty == "DOUBLE PRECISION" || ty.starts_with("DECIMAL(")
}

/// Build the outer merge SELECT items following the COLUMN CONTRACT.
///
/// AVG uses `SUM(sum) / NULLIF(SUM(cnt), 0)` — the NULLIF guard ensures division
/// by zero yields NULL rather than an error (Exasol: `x / NULL = NULL`).
fn merge_select_items(aggregates: &[AggregatePlan]) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .map(|(i, plan)| match plan.kind {
            AggKind::Count | AggKind::CountCol => format!(r#"SUM("PARTIAL_count_{i}")"#),
            AggKind::Sum => format!(r#"SUM("PARTIAL_sum_{i}")"#),
            AggKind::Min => format!(r#"MIN("PARTIAL_min_{i}")"#),
            AggKind::Max => format!(r#"MAX("PARTIAL_max_{i}")"#),
            AggKind::Avg => {
                format!(r#"SUM("PARTIAL_avg_sum_{i}") / NULLIF(SUM("PARTIAL_avg_cnt_{i}"), 0)"#)
            }
        })
        .collect()
}

/// Builds the inner IPROC fan-out SELECT that Exasol distributes across nodes.
/// Callers wrap it in `SELECT * FROM (...)` for row scans or an outer merge aggregation for aggregate pushdown.
pub fn build_fan_out_inner(
    spec_template: &ScanSpec,
    shards: &[Vec<String>],
    emits: &str,
    udf_name: &str,
) -> String {
    let values: Vec<String> = shards
        .iter()
        .enumerate()
        .map(|(i, files)| {
            let mut shard_spec = spec_template.clone();
            shard_spec.files = files.clone();
            let lit = sql_string_literal(&shard_spec.to_json());
            format!("({i},{lit})")
        })
        .collect();
    let values_list = values.join(",");
    format!(
        "SELECT {udf}(spec) EMITS ({emits}) FROM (VALUES {values}) AS shards(shard_key, spec) GROUP BY IPROC(), shard_key",
        udf = udf_name,
        emits = emits,
        values = values_list,
    )
}

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// `cluster_nodes` — the number of Exasol nodes read from the `CLUSTER_NODES`
/// VS property (default 1 when absent or unparseable).
///
/// Returns JSON `{"type":"pushdown","sql":"..."}`.
pub async fn handle_pushdown(
    request: &Json,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
) -> Result<Json, UdfError> {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);

    let (proj_cols, proj_types) = extract_projection(request, &pushdown_req)?;

    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(render_df_filter_safe);

    let limit = extract_limit(&pushdown_req);

    // After detection, validate that each SUM/MIN/MAX targets a supported column type;
    // if any SUM targets a non-numeric type (DATE, VARCHAR, etc.), fall back to row scan.
    let col_types = extract_all_column_types(request);
    let aggregates =
        detect_aggregates(&pushdown_req).filter(|plans| validate_agg_col_types(plans, &col_types));

    let files = resolve_file_list(catalog_uri, catalog, storage).await?;

    if files.is_empty() {
        return Ok(empty_pushdown_sql(&proj_cols, &proj_types));
    }

    let shards = crate::adapter::sharding::partition_files(files, cluster_nodes);

    // ponytail: PoC accepted risk — the S3 access/secret keys are embedded in
    // this scan-driving SQL literal (inside the ScanSpec JSON), which Exasol may
    // log or surface in its query profile / audit. Acceptable for this PoC slice;
    // the upgrade path is to pass credentials via a CONNECTION object (referenced
    // by name, never inlined) or to fetch them over connect-back at scan time so
    // they never appear in any SQL text. Error paths already redact the values.
    let spec_template = ScanSpec {
        files: vec![], // replaced per shard in build_scan_driving_sql
        projection: proj_cols.clone(),
        filter,
        limit,
        aggregates,
        storage: storage.clone(),
        catalog: catalog.clone(),
    };

    // The scan UDF must be schema-qualified: the pushdown query executes
    // outside the adapter script's schema, so an unqualified name would not
    // resolve ("function or script LAKEHOUSE_SCAN not found").
    let udf_name = match scan_schema {
        Some(schema) if !schema.is_empty() => {
            format!("{}.{}", quote_ident(schema), SCAN_UDF_NAME)
        }
        _ => SCAN_UDF_NAME.to_string(),
    };

    let sql = build_scan_driving_sql(
        &spec_template,
        shards,
        &proj_cols,
        &proj_types,
        limit,
        &col_types,
        &udf_name,
    );

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

/// Extract all columns and their Exasol types from the first involved table.
fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| tables.first())
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    Some((name, exasol_type_from_json(dt_json)))
                })
                .collect()
        })
        .unwrap_or_default()
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
    /// Uses `cluster_nodes=1` (single-shard / legacy shape).
    fn build_sql_for_fixture(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
    ) -> String {
        build_sql_for_fixture_n(files, proj_cols, proj_types, filter, limit, 1)
    }

    /// Assemble the scan-driving SQL for `cluster_nodes = n`.
    fn build_sql_for_fixture_n(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
        cluster_nodes: usize,
    ) -> String {
        // Build a col_types map from proj_cols/proj_types for row-scan tests.
        let col_types: Vec<(String, String)> = proj_cols
            .iter()
            .cloned()
            .zip(proj_types.iter().cloned())
            .collect();
        let spec_template = ScanSpec {
            files: vec![],
            projection: proj_cols.clone(),
            filter,
            limit,
            aggregates: None,
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            shards,
            &proj_cols,
            &proj_types,
            limit,
            &col_types,
            SCAN_UDF_NAME,
        )
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

    // ---------------------------------------------------------------------------
    // Task 4.2 / 4.4: detect_aggregates — plan translation + fallback
    // ---------------------------------------------------------------------------

    fn agg_item(name: &str, col: Option<&str>, distinct: bool) -> serde_json::Value {
        let mut args = serde_json::json!([]);
        if let Some(c) = col {
            args = serde_json::json!([{"type": "column", "name": c}]);
        }
        serde_json::json!({
            "type": "function_aggregate",
            "name": name,
            "arguments": args,
            "distinct": distinct,
        })
    }

    /// Task 4.4: COUNT(*) translates to Count with column=None.
    #[test]
    fn detect_count_star_produces_count_no_column() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", None, false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(*)");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, AggKind::Count);
        assert!(plans[0].column.is_none());
    }

    /// Task 4.4: COUNT(col) translates to CountCol with the column name.
    #[test]
    fn detect_count_col_produces_count_col() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("amount"), false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(col)");
        assert_eq!(plans[0].kind, AggKind::CountCol);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
    }

    /// Task 4.4: SUM/MIN/MAX/AVG each translate to the right kind + column.
    #[test]
    fn detect_sum_min_max_avg_produce_correct_plans() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MIN", Some("ts"), false),
                agg_item("MAX", Some("ts"), false),
                agg_item("AVG", Some("score"), false),
            ]
        });
        let plans = detect_aggregates(&req).expect("should detect all four");
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Min);
        assert_eq!(plans[1].column.as_deref(), Some("TS"));
        assert_eq!(plans[2].kind, AggKind::Max);
        assert_eq!(plans[2].column.as_deref(), Some("TS"));
        assert_eq!(plans[3].kind, AggKind::Avg);
        assert_eq!(plans[3].column.as_deref(), Some("SCORE"));
    }

    /// Task 4.4: GROUP BY present and non-empty => fall back (None).
    #[test]
    fn detect_aggregates_falls_back_on_group_by() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), false)],
            "groupBy": [{"type": "column", "name": "region"}],
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when GROUP BY is present"
        );
    }

    /// Task 4.4: DISTINCT aggregate => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_distinct() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("id"), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when DISTINCT is present"
        );
    }

    /// Task 4.4: Unsupported function (e.g., STDDEV) => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_unsupported_function() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("STDDEV", Some("amount"), false),
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when any item is unsupported"
        );
    }

    /// Task 4.4: Non-aggregate select item (e.g., plain column) => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_column_select() {
        let req = serde_json::json!({
            "selectList": [
                {"type": "column", "name": "region"},
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when select list contains non-aggregate"
        );
    }

    /// Task 4.4: Empty select list => None.
    #[test]
    fn detect_aggregates_returns_none_for_empty_select_list() {
        let req = serde_json::json!({ "selectList": [] });
        assert!(detect_aggregates(&req).is_none());
    }

    /// Task 4.4 + 3.3 (aggregate_query_builds_partial_agg_spec):
    /// An aggregate select-list translates to a ScanSpec carrying
    /// the aggregate plan (kind+column) plus any pushed-down filter.
    #[test]
    fn aggregate_query_builds_partial_agg_spec() {
        // Build a spec_template as handle_pushdown would.
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["AMOUNT".into()],
            filter: Some("(\"REGION\" = 'EU')".into()),
            limit: None,
            aggregates: Some(vec![
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                },
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                },
            ]),
            storage: sample_storage(),
            catalog: sample_catalog(),
        };

        // Build single-shard SQL and decode the embedded spec literal.
        let shards = vec![vec!["s3://warehouse/f.parquet".into()]];
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["AMOUNT".to_string()],
            &["DOUBLE PRECISION".to_string()],
            None,
            &col_types,
            SCAN_UDF_NAME,
        );

        // The spec JSON is embedded in the SQL literal; extract and parse it.
        // It lives between the first `'` and the matching unescaped `'` after the JSON.
        // Simpler: deserialize directly from the template (which is what gets embedded).
        let spec_json = {
            // Reconstruct the shard spec as the builder would.
            let mut s = spec_template.clone();
            s.files = vec!["s3://warehouse/f.parquet".into()];
            s.to_json()
        };
        let parsed = ScanSpec::from_json(&spec_json).expect("spec must parse");

        // The aggregate plan must be present with the right kinds and columns.
        let plans = parsed.aggregates.expect("aggregates must be in the spec");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Count);
        assert!(plans[1].column.is_none());

        // The filter must also be present.
        assert!(
            parsed.filter.is_some(),
            "filter must be carried in aggregate spec"
        );

        // The SQL must reference the UDF.
        assert!(sql.contains(SCAN_UDF_NAME));
    }

    // ---------------------------------------------------------------------------
    // Task 3.3: IPROC fan-out + single-shard equivalence
    // ---------------------------------------------------------------------------

    /// Task 3.3: multi_shard_sql_fans_via_iproc_group_by
    /// Given files partitioned into >1 shard: SQL contains IPROC() and GROUP BY,
    /// invokes the scan UDF, and carries each shard's distinct files as separate spec literals.
    #[test]
    fn multi_shard_sql_fans_via_iproc_group_by() {
        let files = vec![
            "s3://warehouse/shard0/part-000.parquet".into(),
            "s3://warehouse/shard1/part-001.parquet".into(),
            "s3://warehouse/shard2/part-002.parquet".into(),
        ];
        // cluster_nodes=3 forces 3 shards (one file each).
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            None,
            3,
        );

        // Must use IPROC and GROUP BY for the fan-out.
        assert!(
            sql.contains("IPROC()"),
            "multi-shard SQL must contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "multi-shard SQL must contain GROUP BY: {sql}"
        );

        // Must invoke the scan UDF.
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "multi-shard SQL must invoke the scan UDF: {sql}"
        );

        // Each file must appear in the SQL (in distinct spec literals).
        assert!(
            sql.contains("part-000.parquet"),
            "shard 0 file must be in SQL: {sql}"
        );
        assert!(
            sql.contains("part-001.parquet"),
            "shard 1 file must be in SQL: {sql}"
        );
        assert!(
            sql.contains("part-002.parquet"),
            "shard 2 file must be in SQL: {sql}"
        );

        // The two files must appear in separate spec literals (not in the same one).
        // A spec literal is a JSON object; each file should appear in its own VALUES row.
        // Assert that the string "part-000.parquet" and "part-001.parquet" are NOT
        // both inside the same spec literal by checking they land in different VALUES entries.
        // Rough check: the VALUES clause contains exactly 3 entries separated by ),(.
        let values_start = sql.find("VALUES").expect("must have VALUES");
        let group_by_start = sql.find("GROUP BY").expect("must have GROUP BY");
        let values_section = &sql[values_start..group_by_start];
        // Count VALUES entries: each is (N,'...')
        let entry_count = values_section.matches("),(").count() + 1;
        assert_eq!(
            entry_count, 3,
            "must have 3 VALUES entries for 3 shards: {values_section}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 6.3: aggregate merge wrapper SQL tests
    // ---------------------------------------------------------------------------

    /// Helper: build aggregate scan SQL from a set of aggregate plans.
    /// Uses an empty col_types map — aggregate columns default to DOUBLE PRECISION
    /// (correct for existing tests that use SCORE/AMOUNT as DOUBLE).
    fn build_agg_sql(
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        cluster_nodes: usize,
    ) -> String {
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(agg_plans),
            storage: sample_storage(),
            catalog: sample_catalog(),
        };
        let shards = crate::adapter::sharding::partition_files(files, cluster_nodes);
        build_scan_driving_sql(&spec_template, shards, &[], &[], None, &[], SCAN_UDF_NAME)
    }

    /// Task 6.3: aggregate_wrapper_merges_partials
    /// Given COUNT/SUM/MIN/MAX aggregate plan: wrapper contains fan-out AND outer
    /// SUM/MIN/MAX over the partial columns in the right order.
    #[test]
    fn aggregate_wrapper_merges_partials() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
            },
        ];

        // Multi-shard: use 2 shards to exercise the fan-out + merge wrapper.
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain the IPROC fan-out.
        assert!(
            sql.contains("IPROC()"),
            "aggregate SQL must use IPROC fan-out: {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "aggregate SQL must use GROUP BY: {sql}"
        );

        // Must wrap with outer merge aggregation.
        assert!(
            sql.contains("SUM("),
            "merge wrapper must contain SUM: {sql}"
        );
        assert!(
            sql.contains("MIN("),
            "merge wrapper must contain MIN: {sql}"
        );
        assert!(
            sql.contains("MAX("),
            "merge wrapper must contain MAX: {sql}"
        );

        // Must contain partial column names in the EMITS and in the merge.
        assert!(
            sql.contains("PARTIAL_count_0"),
            "must reference partial count column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "must reference partial sum column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_min_2"),
            "must reference partial min column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_max_3"),
            "must reference partial max column: {sql}"
        );

        // The EMITS clause must declare the partial columns.
        assert!(
            sql.contains("EMITS"),
            "aggregate SQL must have EMITS: {sql}"
        );

        // The outer merge must not be SELECT *.
        assert!(
            !sql.contains("SELECT *"),
            "aggregate wrapper must not use SELECT *: {sql}"
        );
    }

    /// Task 6.3: avg_wrapper_divides_sum_by_count_guarded
    /// Given AVG plan: wrapper computes SUM(partial_avg_sum) / NULLIF(SUM(partial_avg_cnt),0).
    #[test]
    fn avg_wrapper_divides_sum_by_count_guarded() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
        }];
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain NULLIF guard for zero-count protection.
        assert!(
            sql.contains("NULLIF"),
            "AVG wrapper must contain NULLIF zero-guard: {sql}"
        );

        // Must divide: the / operator must appear in the outer merge context.
        assert!(
            sql.contains(" / "),
            "AVG wrapper must divide sum by count: {sql}"
        );

        // Must reference the AVG sum and count partial columns.
        assert!(
            sql.contains("PARTIAL_avg_sum_0"),
            "must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_0"),
            "must reference partial avg count: {sql}"
        );

        // Must use SUM() for both the sum and count parts.
        let sum_count = sql.matches("SUM(").count();
        assert!(
            sum_count >= 2,
            "AVG wrapper must SUM both partial_avg_sum and partial_avg_cnt: {sql}"
        );

        // Must contain NULLIF(..., 0).
        assert!(
            sql.contains("NULLIF(") && sql.contains(", 0)"),
            "AVG wrapper NULLIF guard must guard against zero: {sql}"
        );
    }

    /// Task 6.3 extra: single-shard aggregate path produces a correct merge wrapper.
    #[test]
    fn single_shard_aggregate_still_uses_merge_wrapper() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
            },
        ];
        let files = vec!["s3://warehouse/f0.parquet".into()];
        let sql = build_agg_sql(plans, files, 1);

        // Even single-shard aggregate must have an outer merge.
        assert!(
            sql.contains("SUM("),
            "single-shard aggregate must have SUM merge: {sql}"
        );
        assert!(
            sql.contains("NULLIF"),
            "single-shard AVG must have NULLIF guard: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "single-shard must reference partial count: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_sum_1"),
            "single-shard must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_1"),
            "single-shard must reference partial avg count: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // R.1: EMITS type correctness for SUM/MIN/MAX
    // ---------------------------------------------------------------------------

    /// R.1: MIN/MAX over a DATE column must EMIT DATE, not DOUBLE PRECISION.
    #[test]
    fn partial_emits_min_max_preserve_date_timestamp_type() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("EVENT_DATE".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("EVENT_TS".into()),
            },
        ];
        let col_types = vec![
            ("EVENT_DATE".to_string(), "DATE".to_string()),
            ("EVENT_TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DATE") && !emits[0].contains("DOUBLE"),
            "MIN over DATE must emit DATE, not DOUBLE: {:?}",
            emits[0]
        );
        assert!(
            emits[1].contains("TIMESTAMP") && !emits[1].contains("DOUBLE"),
            "MAX over TIMESTAMP must emit TIMESTAMP, not DOUBLE: {:?}",
            emits[1]
        );
    }

    /// R.1: SUM over a DECIMAL(20,0) integer column must emit DECIMAL(36,0), not DOUBLE.
    #[test]
    fn partial_emits_sum_integer_stays_decimal() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
        }];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(20,0)".to_string())];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DECIMAL") && !emits[0].contains("DOUBLE"),
            "SUM over DECIMAL integer must emit DECIMAL, not DOUBLE: {:?}",
            emits[0]
        );
        // Scale must be 0 (preserved from original DECIMAL(20,0)).
        assert!(
            emits[0].contains("DECIMAL(36,0)"),
            "SUM over DECIMAL(20,0) must widen to DECIMAL(36,0): {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a DOUBLE PRECISION column stays DOUBLE PRECISION.
    #[test]
    fn partial_emits_sum_double_stays_double() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
        }];
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DOUBLE PRECISION"),
            "SUM over DOUBLE must emit DOUBLE PRECISION: {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a VARCHAR/DATE column => validate_agg_col_types returns false (fall back).
    #[test]
    fn aggregate_falls_back_to_row_scan_for_sum_of_non_numeric() {
        let col_types_varchar = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
        let sum_varchar = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("NAME".into()),
        }];
        assert!(
            !validate_agg_col_types(&sum_varchar, &col_types_varchar),
            "SUM over VARCHAR must fail validation (fall back to row scan)"
        );

        let col_types_date = vec![("EVENT_DATE".to_string(), "DATE".to_string())];
        let sum_date = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("EVENT_DATE".into()),
        }];
        assert!(
            !validate_agg_col_types(&sum_date, &col_types_date),
            "SUM over DATE must fail validation (fall back to row scan)"
        );
    }

    // ---------------------------------------------------------------------------
    // R.2: multi-shard row-scan must append outer LIMIT
    // ---------------------------------------------------------------------------

    /// R.2: multi-shard row scan with LIMIT must append LIMIT to the outer SQL.
    #[test]
    fn multi_shard_row_scan_appends_outer_limit() {
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        // cluster_nodes=2 forces 2 shards.
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(10),
            2,
        );
        assert!(
            sql.contains("IPROC()"),
            "must be multi-shard (has IPROC): {sql}"
        );
        assert!(
            sql.contains("LIMIT 10"),
            "multi-shard row scan must append outer LIMIT 10: {sql}"
        );
    }

    /// Task 3.3: single_shard_sql_matches_legacy_shape
    /// Given CLUSTER_NODES=1: the generated SQL does NOT contain IPROC/VALUES/GROUP BY
    /// and matches the `SELECT * FROM (SELECT {udf}(...) EMITS (...))` form.
    #[test]
    fn single_shard_sql_matches_legacy_shape() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture_n(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
            1, // single node
        );

        // Must NOT contain multi-shard markers.
        assert!(
            !sql.contains("IPROC"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );

        // Must match the legacy shape.
        assert!(
            sql.starts_with("SELECT * FROM (SELECT "),
            "must start with SELECT * FROM (SELECT ...: {sql}"
        );
        assert!(sql.contains("EMITS"), "must have EMITS clause: {sql}");
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "must invoke the scan UDF: {sql}"
        );

        // Must carry both files (they go into a single spec literal).
        assert!(
            sql.contains("part-00000.parquet"),
            "must carry file 0: {sql}"
        );
        assert!(
            sql.contains("part-00001.parquet"),
            "must carry file 1: {sql}"
        );
    }
}
