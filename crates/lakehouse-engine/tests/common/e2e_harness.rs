//! Shared provisioning for the `exasol-e2e` binaries. Helpers panic, never skip,
//! when the local stack is unavailable.

use super::exasol_ws::ExaConn;
use super::seed::{E2E_DIM_TABLE, E2E_FACT_TABLE};
use super::stack::{
    CatalogConnectionPassword, bucketfs_port, bucketfs_write_password, build_create_connection_sql,
    exasol_host, exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal,
    lakehouse_engine_so_path, local_stack_connection_password, minio_url, upload_to_bucketfs,
};

use lakehouse_catalog::CatalogSession;
use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::{ConnectionStorage, ScanSource, format_reader};
use lakehouse_engine::scan::spec::{CatalogProps, FileEntry, StorageBackend, StorageProps};

use std::collections::HashMap;
use std::time::Duration;

pub const SYS_PASSWORD: &str = "exasol";
pub const SCHEMA_NAME: &str = "LHVS";
pub const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
pub const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
/// Plain LUA DDL, not a Rust entry point: does the cross-node `GROUP BY shard_key` fan-out.
pub const DISTRIBUTOR_SCRIPT_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";

pub const VERSION_SCRIPT_NAME: &str = "LAKEHOUSE_VERSION";
pub const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
/// No leading `/`, as `%udf_object` requires.
pub const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
pub const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
/// The `.so` only loads against an SLC with a matching SDK fingerprint.
pub const SLC_VERSION: &str = sdk_version_from_fingerprint();

/// `const` so `SLC_VERSION` stays a `&'static str` usable in inline format captures.
const fn sdk_version_from_fingerprint() -> &'static str {
    let bytes = exasol_udf_sdk::abi::EXA_SDK_FINGERPRINT.as_bytes();
    let mut end = 0;
    while end < bytes.len() && bytes[end] != b':' {
        end += 1;
    }
    assert!(end > 0 && end < bytes.len(), "malformed SDK fingerprint");
    match str::from_utf8(bytes.split_at(end).0) {
        Ok(version) => version,
        Err(_) => panic!("SDK fingerprint version field is not UTF-8"),
    }
}

pub const LANG_ALIAS: &str = "RUST";

pub const DEFAULT_CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

/// Replaces any existing `RUST=` entry; this Exasol is dedicated to lakehouse-engine.
pub fn install_slc() {
    let slc_url = format!(
        "https://github.com/exasol-labs/language-container-rs/releases/download/v{SLC_VERSION}/lc-rust-{SLC_VERSION}.tar.gz"
    );
    let tarball_bytes = reqwest::blocking::get(&slc_url)
        .unwrap_or_else(|e| panic!("download SLC {SLC_VERSION} from {slc_url}: {e}"))
        .bytes()
        .unwrap_or_else(|e| panic!("read SLC tarball bytes: {e}"));
    assert!(
        !tarball_bytes.is_empty(),
        "SLC tarball is empty — download failed"
    );

    let password = bucketfs_write_password();
    let bfs_url = format!(
        "https://{}:{}{}",
        exasol_host(),
        bucketfs_port(),
        SLC_BUCKETFS_PUT_PATH
    );
    let client = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(120))
        .build()
        .expect("BucketFS client");
    let resp = client
        .put(&bfs_url)
        .basic_auth("w", Some(&password))
        .body(tarball_bytes.to_vec())
        .send()
        .unwrap_or_else(|e| panic!("BucketFS PUT SLC to {bfs_url}: {e}"));
    assert!(
        resp.status().is_success(),
        "BucketFS PUT SLC returned {} — expected 2xx",
        resp.status()
    );

    let mut conn = exa_conn();
    let rust_def = format!(
        "{LANG_ALIAS}=localzmq+protobuf:///bfsdefault/default/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/default/slc/lakehouse-rustslc/exaudf/exaudfclient"
    );
    let current = conn.query_columns(
        "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'",
    );
    let current_val = current
        .first()
        .and_then(|col| col.first())
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let preserved = current_val
        .split_whitespace()
        .filter(|s| !s.starts_with(&format!("{LANG_ALIAS}=")))
        .collect::<Vec<_>>()
        .join(" ");
    let new_val = format!("{preserved} {rust_def}");
    conn.execute(&format!(
        "ALTER SYSTEM SET SCRIPT_LANGUAGES = '{}'",
        new_val.trim()
    ));
}

pub fn upload_so() {
    let so_path = lakehouse_engine_so_path();
    upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);
}

pub fn exa_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

/// All `CREATE OR REPLACE`, so concurrent recreation across binaries is harmless.
pub fn create_schema_and_scripts(conn: &mut ExaConn) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} ADAPTER SCRIPT {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{VERSION_SCRIPT_NAME}()
RETURNS VARCHAR(100) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE LUA SET SCRIPT {SCHEMA_NAME}.{DISTRIBUTOR_SCRIPT_NAME}(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/"#
    ));
}

pub struct VsProps<'a> {
    vs_name: &'a str,
    namespace: &'a str,
    catalog_conn_name: &'a str,
    parallelism_factor: Option<usize>,
    join_broadcast_max_bytes: Option<&'a str>,
    catalog_kind: Option<&'a str>,
    merge_schema: Option<&'a str>,
    hive_partitioning: Option<&'a str>,
}

impl<'a> VsProps<'a> {
    pub fn new(vs_name: &'a str, namespace: &'a str) -> Self {
        Self {
            vs_name,
            namespace,
            catalog_conn_name: DEFAULT_CATALOG_CONN_NAME,
            parallelism_factor: None,
            join_broadcast_max_bytes: None,
            catalog_kind: None,
            merge_schema: None,
            hive_partitioning: None,
        }
    }

    pub fn with_parallelism_factor(mut self, factor: usize) -> Self {
        self.parallelism_factor = Some(factor);
        self
    }

    pub fn with_join_broadcast_max_bytes(mut self, bytes: &'a str) -> Self {
        self.join_broadcast_max_bytes = Some(bytes);
        self
    }

    pub fn with_catalog_conn_name(mut self, name: &'a str) -> Self {
        self.catalog_conn_name = name;
        self
    }

    pub fn with_catalog_kind(mut self, kind: &'a str) -> Self {
        self.catalog_kind = Some(kind);
        self
    }

    pub fn with_merge_schema(mut self, merge_schema: &'a str) -> Self {
        self.merge_schema = Some(merge_schema);
        self
    }

    pub fn with_hive_partitioning(mut self, hive_partitioning: &'a str) -> Self {
        self.hive_partitioning = Some(hive_partitioning);
        self
    }
}

/// VS properties use docker-network-internal URLs because the adapter UDF runs inside
/// the Exasol container.
pub fn create_virtual_schema(conn: &mut ExaConn, props: &VsProps) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    create_virtual_schema_with_password(conn, props, &catalog_uri, &password);
}

fn optional_clause(key: &str, value: Option<impl std::fmt::Display>) -> String {
    value
        .map(|v| format!("\n  {key} = '{v}'"))
        .unwrap_or_default()
}

pub fn create_virtual_schema_with_password(
    conn: &mut ExaConn,
    props: &VsProps,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) {
    let create_vs_sql = prepare_create_virtual_schema(conn, props, catalog_uri, password);
    conn.execute(&create_vs_sql);
}

pub fn try_create_virtual_schema_with_password(
    conn: &mut ExaConn,
    props: &VsProps,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) -> serde_json::Value {
    let create_vs_sql = prepare_create_virtual_schema(conn, props, catalog_uri, password);
    conn.try_execute(&create_vs_sql)
}

fn prepare_create_virtual_schema(
    conn: &mut ExaConn,
    props: &VsProps,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) -> String {
    let create_conn_sql =
        build_create_connection_sql(props.catalog_conn_name, catalog_uri, password);
    conn.execute(&create_conn_sql);

    grant_connection_access_to_vs_owner(conn, props.catalog_conn_name);

    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {} CASCADE",
        props.vs_name
    ));

    let parallelism_clause = optional_clause("PARALLELISM_FACTOR ", props.parallelism_factor);
    let join_clause = optional_clause("JOIN_BROADCAST_MAX_BYTES", props.join_broadcast_max_bytes);
    let catalog_kind_clause = optional_clause("CATALOG_KIND", props.catalog_kind);
    let merge_schema_clause = optional_clause("MERGE_SCHEMA", props.merge_schema);
    let hive_partitioning_clause = optional_clause("HIVE_PARTITIONING", props.hive_partitioning);
    // Exasol rejects `NAMESPACE = ''`.
    let namespace_clause = if props.namespace.is_empty() {
        String::new()
    } else {
        format!("\n  NAMESPACE   = '{}'", props.namespace)
    };

    format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{catalog_conn_name}'
  ALLOW_HTTP          = 'true'{namespace_clause}{parallelism_clause}{join_clause}{catalog_kind_clause}{merge_schema_clause}{hive_partitioning_clause}"#,
        vs_name = props.vs_name,
        catalog_conn_name = props.catalog_conn_name,
    )
}

pub fn current_user(conn: &mut ExaConn) -> String {
    let cols = conn.query_columns("SELECT CURRENT_USER");
    cols.first()
        .and_then(|col| col.first())
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| panic!("SELECT CURRENT_USER returned no value: {cols:?}"))
}

pub fn grant_connection_access_to_vs_owner(conn: &mut ExaConn, conn_name: &str) {
    let owner = current_user(conn);
    if owner.eq_ignore_ascii_case("SYS") {
        return;
    }
    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        conn.execute(&format!(
            "GRANT ACCESS ON CONNECTION {conn_name} FOR SCRIPT {SCHEMA_NAME}.{script} TO {owner}"
        ));
    }
}

pub fn explain_virtual_sql(conn: &mut ExaConn, query_sql: &str) -> String {
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    conn.fetch_result_columns(result_set)
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The complete, directly submittable adapter-generated statement, unlike
/// `explain_virtual_sql`'s joined blob.
pub fn isolated_pushdown_statement(conn: &mut ExaConn, query_sql: &str) -> String {
    const PUSHDOWN_SQL_COLUMN: usize = 1;
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    if cols.len() != 4 || cols[0].len() != 1 {
        panic!(
            "EXPLAIN VIRTUAL returned an unexpected layout ({} cols, {} rows) for:\n{query_sql}",
            cols.len(),
            cols.first().map(Vec::len).unwrap_or(0)
        );
    }
    cols[PUSHDOWN_SQL_COLUMN][0]
        .as_str()
        .unwrap_or_else(|| {
            panic!(
                "EXPLAIN VIRTUAL PUSHDOWN_SQL cell was not a string for:\n{query_sql}\ngot: {:?}",
                cols[PUSHDOWN_SQL_COLUMN][0]
            )
        })
        .to_string()
}

pub fn parse_numeric(v: &serde_json::Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected numeric value, got: {v:?}"))
}

pub fn parse_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

pub fn local_stack_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        path_style: Some(true),
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

pub fn local_stack_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        allow_http: true,
        ..Default::default()
    })
}

pub fn split_s3_bucket_and_key(uri: &str) -> (&str, &str) {
    let without_scheme = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or_else(|| panic!("expected an s3/s3a URI, got: {uri}"));
    without_scheme
        .split_once('/')
        .unwrap_or_else(|| panic!("expected a <bucket>/<key> URI, got: {uri}"))
}

pub fn local_stack_s3_store(bucket: &str) -> object_store::aws::AmazonS3 {
    let StorageBackend::S3(storage) = local_stack_storage() else {
        panic!("local_stack_storage() must be S3 to build a MinIO object store")
    };
    object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_endpoint(&storage.endpoint)
        .with_allow_http(storage.allow_http)
        .with_virtual_hosted_style_request(!storage.path_style)
        .build()
        .unwrap_or_else(|e| panic!("configure MinIO object store for bucket '{bucket}': {e}"))
}

pub fn local_stack_catalog(table: &str) -> CatalogProps {
    CatalogProps {
        warehouse: "s3://warehouse/".to_string(),
        table: table.to_string(),
    }
}

/// Resolves through the same format-reader seam the adapter uses, bypassing Exasol.
/// Each `FileEntry` carries an absolute data-file URI.
pub async fn resolve_fixture_files(namespace: &str, table: &str) -> Vec<FileEntry> {
    let catalog_uri = iceberg_catalog_url();
    let catalog_props = local_stack_catalog(&format!("{namespace}.{table}"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();
    let session = CatalogSession::resolve(&catalog_uri, &creds.warehouse, &creds)
        .await
        .unwrap_or_else(|e| panic!("CatalogSession::resolve({table}) must succeed: {e}"));

    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    let reader = format_reader(
        ScanSource::Iceberg {
            session: &session,
            catalog_props: &catalog_props,
        },
        &connection,
    )
    .unwrap_or_else(|e| panic!("format_reader({table}) must succeed: {e}"));
    let resolved = reader
        .resolve_scan(None)
        .await
        .unwrap_or_else(|e| panic!("resolve_scan({table}) must succeed: {e}"));
    resolved.files
}

/// Straddles both fact-side data files, so per-shard join results must merge across a
/// shard boundary.
pub const ORDERDATE_LOWER_BOUND: &str = "2024-01-05";

pub fn vs_dim_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_DIM_TABLE.to_uppercase())
}

pub fn vs_fact_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_FACT_TABLE.to_uppercase())
}

pub fn join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// The compact lowercase `"join":{` token is unique to the generated ScanSpec JSON:
/// Exasol's echoed request uses `"type" : "join"` and capabilities use `"JOIN"`.
pub fn has_broadcast_join_block(pushed_sql: &str) -> bool {
    pushed_sql.contains("\"join\":{")
}

/// The `LHS_T*` aliases appear only in the generated N-scan wrapper, never in a native
/// retry or the broadcast path.
pub fn has_two_scan_wrapper(pushed_sql: &str) -> bool {
    has_n_scan_wrapper(pushed_sql, 2)
}

/// Also requires no `LHS_T{n}`, so a 3-table wrapper is never mistaken for a 4-table one.
pub fn has_n_scan_wrapper(pushed_sql: &str, n: usize) -> bool {
    (0..n).all(|i| pushed_sql.contains(&format!(r#"AS "LHS_T{i}""#)))
        && !pushed_sql.contains(&format!(r#"AS "LHS_T{n}""#))
}

pub fn fetch_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<(String, String)> {
    let cols = conn.query_columns(&join_query(vs_name));
    columns_to_sorted_pairs(&cols)
}

pub fn columns_to_sorted_pairs(cols: &[Vec<serde_json::Value>]) -> Vec<(String, String)> {
    assert_eq!(
        cols.len(),
        2,
        "expected 2 result columns, got {}",
        cols.len()
    );
    let mut rows: Vec<(String, String)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(name, date)| (value_to_string(name), value_to_string(date)))
        .collect();
    rows.sort();
    rows
}

pub fn value_to_string(v: &serde_json::Value) -> String {
    v.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_string())
}

/// Ground truth independent of join pushdown: both tables read un-joined through the VS
/// and joined in-process.
pub fn expected_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<(String, String)> {
    expected_join_rows_with_fact_where(
        conn,
        vs_name,
        &format!("O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'"),
    )
}

/// Applies `fact_where` through the single-table WHERE path, independent of the join
/// pushdown under test.
pub fn expected_join_rows_with_fact_where(
    conn: &mut ExaConn,
    vs_name: &str,
    fact_where: &str,
) -> Vec<(String, String)> {
    let dim_cols = conn.query_columns(&format!(
        "SELECT C_CUSTKEY, C_NAME FROM {}",
        vs_dim_table(vs_name)
    ));
    assert_eq!(dim_cols.len(), 2, "dim query must return 2 columns");
    let custkey_to_name: HashMap<String, String> = dim_cols[0]
        .iter()
        .zip(dim_cols[1].iter())
        .map(|(k, n)| (value_to_string(k), value_to_string(n)))
        .collect();

    let fact_cols = conn.query_columns(&format!(
        "SELECT O_CUSTKEY, O_ORDERDATE FROM {} WHERE {fact_where}",
        vs_fact_table(vs_name)
    ));
    assert_eq!(fact_cols.len(), 2, "fact query must return 2 columns");

    let mut rows: Vec<(String, String)> = fact_cols[0]
        .iter()
        .zip(fact_cols[1].iter())
        .map(|(custkey, date)| {
            let key = value_to_string(custkey);
            let name = custkey_to_name
                .get(&key)
                .unwrap_or_else(|| panic!("fact O_CUSTKEY {key} has no matching customer"))
                .clone();
            (name, value_to_string(date))
        })
        .collect();
    rows.sort();
    rows
}
