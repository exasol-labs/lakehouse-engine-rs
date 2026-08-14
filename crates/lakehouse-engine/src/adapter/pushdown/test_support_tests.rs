//! Test-only fixtures shared across the `pushdown` submodule test modules.
//!
//! Extracted verbatim from the former flat `mod tests` "Helpers shared across
//! tests" block. Each capability submodule's `#[cfg(test)] mod tests` reaches
//! these through `super::test_support`.

use super::*;
use crate::scan::spec::{DeleteMechanism, StorageProps};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A loopback HTTP/1.1 catalog answering every request from a caller-supplied
/// responder and recording each request target in arrival order.
///
/// Every response closes its connection, so the pooled client opens a fresh one
/// per request and one accept loop serves the whole sequential stream in order.
pub(super) struct RecordingCatalog {
    pub(super) uri: String,
    targets: Arc<Mutex<Vec<String>>>,
}

impl RecordingCatalog {
    pub(super) async fn spawn<F>(responder: F) -> Self
    where
        F: Fn(&str) -> (u16, String) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let uri = format!(
            "http://127.0.0.1:{}",
            listener.local_addr().expect("local_addr").port()
        );
        let targets: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let recorded = targets.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let read = stream.read(&mut buf).await.unwrap_or(0);
                if read == 0 {
                    continue;
                }
                let raw = String::from_utf8_lossy(&buf[..read]).to_string();
                let target = raw.split_whitespace().nth(1).unwrap_or("").to_string();
                let (status, body) = responder(&target);
                recorded.lock().expect("recorded targets").push(target);
                let reason = if (200..300).contains(&status) {
                    "OK"
                } else {
                    "ERROR"
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        Self { uri, targets }
    }

    pub(super) fn targets(&self) -> Vec<String> {
        self.targets.lock().expect("recorded targets").clone()
    }
}

/// Credentials supplying no catalog authentication at all, so a session issues
/// exactly the requests its own resolution needs and no token grant.
pub(super) fn unauthenticated_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "wh".into(),
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
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// The `/v1/config` target every non-SigV4 Iceberg session resolves its prefix
/// from, exactly once per session.
pub(super) const ICEBERG_CONFIG_TARGET: &str = "/v1/config?warehouse=wh";

/// The `loadTable` target the recorded identifier `db.t` addresses under an
/// empty prefix.
pub(super) const ICEBERG_LOAD_TABLE_TARGET: &str = "/v1/namespaces/db/tables/t";

/// The Unity Catalog get-table target the recorded identifier `cat.sch.orders`
/// addresses.
pub(super) const UNITY_TABLE_TARGET: &str = "/api/2.1/unity-catalog/tables/cat.sch.orders";

/// A `loadTable` response for a snapshotless single-column table.
///
/// The absent snapshot is what keeps this offline: an empty table scan reads no
/// manifest, so resolution completes without touching the store its location
/// names.
pub(super) fn snapshotless_load_table_body(location: &str) -> String {
    serde_json::json!({
        "metadata-location": format!("{location}/metadata/v1.json"),
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000003",
            "location": location,
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "snapshots": []
        }
    })
    .to_string()
}

/// A loaded Unity Catalog Delta table carrying NO storage location, so the Delta
/// reader refuses at its first check and reaches no object store.
pub(super) fn locationless_delta_table_body() -> String {
    serde_json::json!({
        "name": "orders",
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "table_id": "table-1",
        "columns": []
    })
    .to_string()
}

/// An Iceberg REST catalog serving its config and one snapshotless table per
/// requested identifier.
pub(super) async fn iceberg_catalog() -> RecordingCatalog {
    RecordingCatalog::spawn(|target| {
        if target.starts_with("/v1/config") {
            return (200, "{}".to_string());
        }
        match target.rsplit_once('/') {
            Some((_, table)) => (
                200,
                snapshotless_load_table_body(&format!("s3://bucket/{table}")),
            ),
            None => (404, r#"{"message":"no such table"}"#.to_string()),
        }
    })
    .await
}

pub(super) fn sample_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// Assemble the scan-driving SQL from a known file list + spec — the same
/// logic `handle_pushdown` runs after resolution.
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
        common: CommonScanSpec {
            projection: proj_items.clone(),
            filter,
            limit,
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
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
        None,
        &col_types,
        &[],
        SCAN_UDF_NAME,
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

/// The contents of the scan UDF's `EMITS (...)` clause — the scan's EMITTED column
/// set, which on a declined-`ORDER BY` path is WIDER than the query's visible column
/// set (it also carries the appended hidden sort-key columns).
///
/// Extracted paren-balanced: the declared types carry their own parentheses
/// (`DECIMAL(20,0)`), so the clause does not end at the first `)`. Exactly one
/// `EMITS (` appears in a fan-out — the distributor call carries none (its LUA SET
/// script declares a static EMITS).
pub(super) fn emits_clause(sql: &str) -> &str {
    let open = sql.find("EMITS (").expect("SQL must carry an EMITS clause") + "EMITS ".len();
    let mut depth = 0usize;
    for (offset, ch) in sql[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &sql[open + 1..open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("EMITS clause must be closed: {sql}");
}

/// The declined-`ORDER BY` wrapper's VISIBLE select list: everything between the
/// leading `SELECT ` and the first ` FROM (`. A visible select list never contains
/// ` FROM (` itself, so the first occurrence is always the wrapper's own — even for a
/// multi-shard fan-out, which nests a second ` FROM (` inside.
///
/// Panics when the SQL carries no wrapper; use a `!sql.contains(" FROM (")`
/// assertion for the no-wrapper cases instead.
pub(super) fn outer_select_list(sql: &str) -> &str {
    let list = sql
        .strip_prefix("SELECT ")
        .expect("SQL must start with SELECT");
    let end = list
        .find(" FROM (")
        .expect("SQL must carry a wrapping outer SELECT … FROM (");
    &list[..end]
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
        common: CommonScanSpec {
            table_root: table_root.to_string(),
            projection: proj_items.clone(),
            emit_exa_types: proj_types.clone(),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
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
        None,
        &col_types,
        &[],
        SCAN_UDF_NAME,
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
pub(super) fn pos_delete(path: &str, size: u64) -> DeleteMechanism {
    DeleteMechanism::IcebergPositionalDelete {
        path: path.into(),
        size,
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
