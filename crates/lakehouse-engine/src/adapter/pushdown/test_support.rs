//! Test-only fixtures shared across the `pushdown` submodule test modules.
//!
//! Extracted verbatim from the former flat `mod tests` "Helpers shared across
//! tests" block. Each capability submodule's `#[cfg(test)] mod tests` reaches
//! these through `super::test_support`.

use super::*;
use crate::scan::spec::{DeleteFileContentType, DeleteFileRef};

pub(super) fn sample_storage() -> StorageProps {
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

/// A baseline `ConnectionCreds` with no catalog auth (all auth fields `None`).
/// Individual tests set only the auth fields under test.
pub(super) fn base_creds() -> ConnectionCreds {
    ConnectionCreds {
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
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
    }
}

/// Static storage with the sentinel keys `STATIC_AK_SENTINEL` / `STATIC_SK_SENTINEL`
/// (matching the credentials-cluster test sentinels in `credentials::tests`).
pub(super) fn static_storage() -> StorageProps {
    StorageProps {
        endpoint: "https://s3.amazonaws.com".into(),
        region: "us-east-1".into(),
        access_key: "STATIC_AK_SENTINEL".into(),
        secret_key: "STATIC_SK_SENTINEL".into(),
        session_token: None,
        allow_http: false,
        path_style: false,
    }
}

/// Assemble the scan-driving SQL from a known file list + spec — the same
/// logic `handle_pushdown` runs after `resolve_file_list`.
/// Uses `cluster_nodes=1` (single-shard / legacy shape).
pub(super) fn build_sql_for_fixture(
    files: Vec<String>,
    proj_cols: Vec<String>,
    proj_types: Vec<String>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    build_sql_for_fixture_n(files, proj_cols, proj_types, filter, limit, 1)
}

/// Assemble the scan-driving SQL for `cluster_nodes = n`.
pub(super) fn build_sql_for_fixture_n(
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
    let proj_items: Vec<ProjectionItem> = proj_cols
        .iter()
        .cloned()
        .map(ProjectionItem::Column)
        .collect();
    let spec_template = ScanSpec {
        table_root: String::new(),
        files: vec![],
        projection: proj_items.clone(),
        filter,
        limit,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: None,
        storage: sample_storage(),
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    };
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards =
        crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &proj_items,
        &proj_types,
        limit,
        &col_types,
        &[],
        SCAN_UDF_NAME,
        DISTINCT_MERGE_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

/// The UDF's first-argument literal (the shard-invariant common blob), extracted
/// as the substring between the first two single quotes. Valid for the test
/// fixtures here, whose common JSON contains no embedded single quote (JSON uses
/// double quotes; the rendered filters used in these tests carry none).
pub(super) fn common_arg_literal(sql: &str) -> &str {
    let start = sql.find('\'').expect("SQL must contain a literal") + 1;
    let rest = &sql[start..];
    let end = rest.find('\'').expect("common literal must be closed");
    &rest[..end]
}

/// A single-table request with the NQ4 shape: two projected columns and an
/// `ORDER BY <projected col> DESC NULLS LAST LIMIT n`.
pub(super) fn nq4_request() -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 2},
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                "isAscending": false,
                "nullsLast": true
            }],
            "limit": {"numElements": 20}
        }
    })
}

/// The `pushdownRequest` sub-object of a request (for direct detector calls).
pub(super) fn pd(request: &Json) -> Json {
    request.get("pushdownRequest").cloned().unwrap()
}

/// Build row-scan SQL the way `handle_pushdown` does for a resolved
/// `(path, size)` file list under `table_root`: partition into shards,
/// relativize under-root paths, then build. Exercises the SAME production
/// stripping (`relativize_shards_to_root`) that runs in `handle_pushdown`, so
/// the emitted per-shard paths match production exactly.
pub(super) fn build_row_sql_with_root(
    files: Vec<(String, u64)>,
    table_root: &str,
    proj_cols: Vec<String>,
    proj_types: Vec<String>,
    cluster_nodes: usize,
) -> String {
    let col_types: Vec<(String, String)> = proj_cols
        .iter()
        .cloned()
        .zip(proj_types.iter().cloned())
        .collect();
    let proj_items: Vec<ProjectionItem> = proj_cols
        .iter()
        .cloned()
        .map(ProjectionItem::Column)
        .collect();
    let spec_template = ScanSpec {
        table_root: table_root.to_string(),
        files: vec![],
        projection: proj_items.clone(),
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: proj_types.clone(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: None,
        storage: sample_storage(),
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    };
    let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
    let g = shard_count(cluster_nodes, 1, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let shards = relativize_shards_to_root(shards, table_root);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &proj_items,
        &proj_types,
        None,
        &col_types,
        &[],
        SCAN_UDF_NAME,
        DISTINCT_MERGE_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

/// A `function_aggregate` select-list item over an optional bare column,
/// shared by the single-group and grouped aggregate test modules.
pub(super) fn agg_item(name: &str, col: Option<&str>, distinct: bool) -> serde_json::Value {
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

/// A Parquet positional-delete file ref.
pub(super) fn pos_delete(path: &str, size: u64) -> DeleteFileRef {
    DeleteFileRef {
        path: path.into(),
        size,
        content_type: DeleteFileContentType::PositionDeletes,
    }
}

/// An aggregate over a single explicit argument NODE (a scalar expression,
/// e.g. `LENGTH(L_COMMENT)`), used to exercise expression-argument pushdown.
pub(super) fn agg_item_expr(
    name: &str,
    arg: serde_json::Value,
    distinct: bool,
) -> serde_json::Value {
    serde_json::json!({
        "type": "function_aggregate",
        "name": name,
        "arguments": [arg],
        "distinct": distinct,
    })
}
