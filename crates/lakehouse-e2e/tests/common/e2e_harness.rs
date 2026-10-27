//! Shared provisioning for the `exasol-e2e` binaries. Helpers panic, never skip,
//! when the local stack is unavailable.

use super::exasol_ws::ExaConn;
pub use super::exasol_ws::error_text;
use super::seed::{DECIMAL_18_0_VALUES_TEXT, E2E_DIM_TABLE, E2E_FACT_TABLE, build_seed_catalog};
use super::stack::{
    CatalogConnectionPassword, bucketfs_port, bucketfs_write_password, build_create_connection_sql,
    exasol_host, exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal,
    lakehouse_engine_so_path, local_stack_connection_password, seaweedfs_url, upload_to_bucketfs,
};

use futures::TryStreamExt;
use iceberg::scan::FileScanTaskDeleteFile;
use iceberg::spec::{DataContentType, DataFileFormat};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::collections::HashMap;
use std::sync::OnceLock;
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

/// The scalar UDF the pushed DataFusion SQL renders Exasol's `FLOAT_DIV` as (#370). A test-owned
/// copy, so a renamed engine constant fails the pushdown-shape checks.
pub const CHECKED_FLOAT_DIV_FN: &str = "vs_checked_float_div";

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
    create_schema_and_scripts_in(conn, SCHEMA_NAME);
}

pub fn create_schema_and_scripts_in(conn: &mut ExaConn, schema: &str) {
    create_schema_and_scripts_with_env(conn, schema, &[], &[]);
}

/// Each `(name, value)` pair becomes one `%env` line of the adapter or the scan script.
pub fn create_schema_and_scripts_with_env(
    conn: &mut ExaConn,
    schema: &str,
    adapter_env: &[(&str, &str)],
    scan_env: &[(&str, &str)],
) {
    let env_lines = |env: &[(&str, &str)]| -> String {
        env.iter()
            .map(|(name, value)| format!("%env {name}={value};\n"))
            .collect()
    };
    let adapter_env = env_lines(adapter_env);
    let scan_env = env_lines(scan_env);
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} ADAPTER SCRIPT {schema}.{ADAPTER_SCRIPT_NAME} AS
{adapter_env}%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {schema}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
{scan_env}%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {schema}.{VERSION_SCRIPT_NAME}()
RETURNS VARCHAR(100) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    conn.execute(&format!(
        r#"CREATE OR REPLACE LUA SET SCRIPT {schema}.{DISTRIBUTOR_SCRIPT_NAME}(files VARCHAR(2000000))
EMITS (files VARCHAR(2000000)) AS
function run(ctx)
    repeat
        ctx.emit(ctx.files)
    until not ctx.next()
end
/"#
    ));
}

// The E2E stacks are single-node, where the default factor of 1 yields one shard, so the
// suites pin the smallest factor that still fans out through the distributor.
pub const E2E_PARALLELISM_FACTOR: usize = 2;

pub struct VsProps<'a> {
    vs_name: &'a str,
    adapter_schema: &'a str,
    namespace: &'a str,
    catalog_conn_name: &'a str,
    parallelism_factor: Option<usize>,
    join_broadcast_max_bytes: Option<&'a str>,
    catalog_kind: Option<&'a str>,
    merge_schema: Option<&'a str>,
    hive_partitioning: Option<&'a str>,
    properties: Vec<(&'a str, &'a str)>,
}

impl<'a> VsProps<'a> {
    pub fn new(vs_name: &'a str, namespace: &'a str) -> Self {
        Self {
            vs_name,
            adapter_schema: SCHEMA_NAME,
            namespace,
            catalog_conn_name: DEFAULT_CATALOG_CONN_NAME,
            parallelism_factor: Some(E2E_PARALLELISM_FACTOR),
            join_broadcast_max_bytes: None,
            catalog_kind: None,
            merge_schema: None,
            hive_partitioning: None,
            properties: Vec::new(),
        }
    }

    pub fn with_adapter_schema(mut self, schema: &'a str) -> Self {
        self.adapter_schema = schema;
        self
    }

    pub fn with_parallelism_factor(mut self, factor: usize) -> Self {
        self.parallelism_factor = Some(factor);
        self
    }

    /// Leaves the adapter's own default in force.
    pub fn without_parallelism_factor(mut self) -> Self {
        self.parallelism_factor = None;
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

    /// Any further property; the value reaches Exasol verbatim, multi-line text included.
    pub fn with_property(mut self, name: &'a str, value: &'a str) -> Self {
        self.properties.push((name, value));
        self
    }
}

/// VS properties use docker-network-internal URLs because the adapter UDF runs inside
/// the Exasol container.
#[track_caller]
pub fn create_virtual_schema(conn: &mut ExaConn, props: &VsProps) {
    let resp = try_create_virtual_schema(conn, props);
    assert_virtual_schema_created(conn, props, &resp);
}

pub fn try_create_virtual_schema(conn: &mut ExaConn, props: &VsProps) -> serde_json::Value {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    try_create_virtual_schema_with_password(conn, props, &catalog_uri, &password)
}

fn optional_clause(key: &str, value: Option<impl std::fmt::Display>) -> String {
    value
        .map(|v| format!("\n  {key} = '{v}'"))
        .unwrap_or_default()
}

#[track_caller]
pub fn create_virtual_schema_with_password(
    conn: &mut ExaConn,
    props: &VsProps,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) {
    let resp = try_create_virtual_schema_with_password(conn, props, catalog_uri, password);
    assert_virtual_schema_created(conn, props, &resp);
}

#[track_caller]
fn assert_virtual_schema_created(conn: &ExaConn, props: &VsProps, resp: &serde_json::Value) {
    conn.assert_ok(resp, &format!("CREATE VIRTUAL SCHEMA {}", props.vs_name));
}

pub fn try_create_virtual_schema_with_password(
    conn: &mut ExaConn,
    props: &VsProps,
    catalog_uri: &str,
    password: &CatalogConnectionPassword,
) -> serde_json::Value {
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
    let property_clauses: String = props
        .properties
        .iter()
        .map(|(name, value)| format!("\n  {name} = '{}'", value.replace('\'', "''")))
        .collect();
    // Exasol rejects `NAMESPACE = ''`.
    let namespace_clause = if props.namespace.is_empty() {
        String::new()
    } else {
        format!("\n  NAMESPACE   = '{}'", props.namespace)
    };

    conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {adapter_schema}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{catalog_conn_name}'
  ALLOW_HTTP          = 'true'{namespace_clause}{parallelism_clause}{join_clause}{catalog_kind_clause}{merge_schema_clause}{hive_partitioning_clause}{property_clauses}"#,
        vs_name = props.vs_name,
        adapter_schema = props.adapter_schema,
        catalog_conn_name = props.catalog_conn_name,
    ))
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

pub fn projection_rows(conn: &mut ExaConn, table: &str) -> Vec<(i64, String, f64)> {
    let cols = conn.query_columns(&format!("SELECT id, name, score FROM {table} ORDER BY id"));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    (0..cols[0].len())
        .map(|i| {
            let id = parse_int(&cols[0][i]);
            let name = cols[1][i]
                .as_str()
                .unwrap_or_else(|| panic!("name not string: {:?}", cols[1][i]))
                .to_string();
            let score = cols[2][i]
                .as_f64()
                .or_else(|| cols[2][i].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("score not numeric: {:?}", cols[2][i]));
            (id, name, score)
        })
        .collect()
}

pub fn enumerated_table_names(conn: &mut ExaConn, vs_name: &str) -> Vec<String> {
    let cols = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = '{vs_name}'"
    ));
    cols.first()
        .map(|c| {
            c.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_uppercase()))
                .collect()
        })
        .unwrap_or_default()
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

pub fn split_s3_bucket_and_key(uri: &str) -> (&str, &str) {
    let without_scheme = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or_else(|| panic!("expected an s3/s3a URI, got: {uri}"));
    without_scheme
        .split_once('/')
        .unwrap_or_else(|| panic!("expected a <bucket>/<key> URI, got: {uri}"))
}

pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

pub fn local_stack_s3_store(bucket: &str) -> AmazonS3 {
    let creds = local_stack_connection_password();
    AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&creds.region)
        .with_access_key_id(&creds.access_key)
        .with_secret_access_key(&creds.secret_key)
        // The CONNECTION names the Docker-network endpoint; the test process needs the host one.
        .with_endpoint(seaweedfs_url())
        .with_allow_http(true)
        .with_virtual_hosted_style_request(!creds.path_style)
        .build()
        .unwrap_or_else(|e| panic!("configure SeaweedFS object store for bucket '{bucket}': {e}"))
}

/// A committed data file and the delete files iceberg-rust's scan planner applies to it.
#[derive(Debug, Clone)]
pub struct FixtureFile {
    /// Absolute, as the table metadata records it.
    pub path: String,
    pub deletes: Vec<FixtureDelete>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixtureDelete {
    Positional {
        path: String,
    },
    DeletionVector {
        path: String,
        content_offset: u64,
        record_count: u64,
    },
    Other {
        path: String,
        content: DataContentType,
    },
}

impl FixtureDelete {
    pub fn path(&self) -> &str {
        match self {
            Self::Positional { path }
            | Self::DeletionVector { path, .. }
            | Self::Other { path, .. } => path,
        }
    }

    fn from_task(delete: &FileScanTaskDeleteFile) -> Self {
        let path = delete.file_path().to_string();
        match (delete.file_type(), delete.file_format()) {
            (DataContentType::PositionDeletes, DataFileFormat::Puffin) => Self::DeletionVector {
                path,
                content_offset: delete
                    .content_offset()
                    .expect("a deletion vector records its blob offset"),
                record_count: delete
                    .record_count()
                    .expect("a deletion vector records its cardinality"),
            },
            (DataContentType::PositionDeletes, _) => Self::Positional { path },
            (content, _) => Self::Other { path, content },
        }
    }
}

/// Plans through iceberg-rust, not the adapter, so a fixture guard holds whatever the engine does.
pub async fn resolve_fixture_files(namespace: &str, table: &str) -> Vec<FixtureFile> {
    let catalog = build_seed_catalog(&iceberg_catalog_url(), "s3://warehouse/", "fixture-files")
        .await
        .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog for {table}: {e:#}"));
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table.to_string(),
    );
    let loaded = catalog
        .load_table(&ident)
        .await
        .unwrap_or_else(|e| panic!("load {namespace}.{table}: {e}"));
    let tasks: Vec<_> = loaded
        .scan()
        .select_all()
        .build()
        .unwrap_or_else(|e| panic!("build a scan of {table}: {e}"))
        .plan_files()
        .await
        .unwrap_or_else(|e| panic!("plan the files of {table}: {e}"))
        .try_collect()
        .await
        .unwrap_or_else(|e| panic!("collect the file tasks of {table}: {e}"));
    tasks
        .iter()
        .map(|task| FixtureFile {
            path: task.data_file_path().to_string(),
            deletes: task
                .deletes()
                .iter()
                .map(FixtureDelete::from_task)
                .collect(),
        })
        .collect()
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

/// Each column's raw persisted note, keyed by its Exasol column name.
fn persisted_column_notes(conn: &mut ExaConn, vs_name: &str, table: &str) -> Vec<(String, String)> {
    let cols = conn.query_columns(&format!(
        "SELECT COLUMN_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_COLUMNS \
         WHERE COLUMN_SCHEMA='{vs_name}' AND COLUMN_TABLE='{table}' ORDER BY COLUMN_NAME"
    ));
    assert!(
        !cols[0].is_empty(),
        "{vs_name}.{table}: no virtual columns persisted"
    );
    cols[0]
        .iter()
        .zip(&cols[1])
        .map(|(name, notes)| {
            let notes = notes
                .as_str()
                .unwrap_or_else(|| panic!("{name}: no column adapterNotes persisted"));
            (value_to_string(name), notes.to_string())
        })
        .collect()
}

/// Each column's persisted note, keyed by its Exasol column name.
pub fn column_adapter_notes(
    conn: &mut ExaConn,
    vs_name: &str,
    table: &str,
) -> Vec<(String, serde_json::Value)> {
    persisted_column_notes(conn, vs_name, table)
        .into_iter()
        .map(|(name, notes)| {
            let notes = serde_json::from_str(&notes).expect("column adapterNotes is JSON");
            (name, notes)
        })
        .collect()
}

/// Each column with its recorded partition position, `None` for a data column.
pub fn partition_positions(notes: &[(String, serde_json::Value)]) -> Vec<(&str, Option<u64>)> {
    notes
        .iter()
        .map(|(column, note)| {
            let position = note.get("partition").map(|position| {
                position
                    .as_u64()
                    .unwrap_or_else(|| panic!("{column}: a partition position is a number"))
            });
            (column.as_str(), position)
        })
        .collect()
}

/// A note carries only its known members and never a credential value.
pub fn assert_notes_well_formed(notes: &[(String, serde_json::Value)], secret: &str) {
    for (column, note) in notes {
        let members = note.as_object().expect("a note is one JSON object");
        assert!(
            members
                .keys()
                .all(|key| ["sourceType", "partition", "refused"].contains(&key.as_str())),
            "{column}: a note carries only sourceType, partition, and refused: {note}"
        );
        assert!(
            !note.to_string().contains(secret),
            "{column}: a note carries no credential value"
        );
    }
}

/// `column`'s note records a refusal naming `fragments`, the column stays declared, and
/// selecting it fails with that refusal while `ID` stays readable.
pub fn assert_refused_column(
    conn: &mut ExaConn,
    vs_name: &str,
    table: &str,
    column: &str,
    fragments: &[&str],
) {
    let notes = column_adapter_notes(conn, vs_name, table);
    let reason = note_of(&notes, column)["refused"]
        .as_str()
        .unwrap_or_else(|| panic!("{table}.{column} must record its refusal: {notes:?}"))
        .to_string();
    assert!(
        fragments.iter().all(|fragment| reason.contains(fragment)),
        "{table}.{column}'s refusal must name {fragments:?}: {reason}"
    );
    assert!(
        note_of(&notes, "ID").get("refused").is_none(),
        "{table}.ID is readable: {notes:?}"
    );
    assert_eq!(
        declared_type(conn, vs_name, table, column),
        VARCHAR_JSON,
        "{table}.{column} stays declared"
    );
    let table_ref = format!("{vs_name}.{table}");
    assert_query_fails(
        conn,
        &format!("SELECT {column} FROM {table_ref}"),
        &[&reason],
    );
    conn.query_columns(&format!("SELECT ID FROM {table_ref}"));
}

/// The schema-level adapterNotes of `vs_name`, decoded from the JSON string Exasol stores. Every
/// schema-level note assertion reads through it, so no test parses that encoding itself.
pub fn schema_notes(
    conn: &mut ExaConn,
    vs_name: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let cols = conn.query_columns(&format!(
        "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{vs_name}'"
    ));
    let raw = cols
        .first()
        .and_then(|col| col.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("{vs_name} must have ADAPTER_NOTES: {cols:?}"));
    match serde_json::from_str(raw) {
        Ok(serde_json::Value::Object(notes)) => notes,
        Ok(_) => panic!("ADAPTER_NOTES of {vs_name} must be a JSON object: {raw}"),
        Err(e) => panic!("ADAPTER_NOTES of {vs_name} must be JSON ({e}): {raw}"),
    }
}

/// Every `(table, reason)` entry of `SKIPPED_TABLES`, in its recorded order.
pub fn skipped_entries(conn: &mut ExaConn, vs_name: &str) -> Vec<(String, String)> {
    let notes = schema_notes(conn, vs_name);
    notes
        .get("SKIPPED_TABLES")
        .and_then(|entries| entries.as_array())
        .unwrap_or_else(|| {
            panic!("ADAPTER_NOTES of {vs_name} must carry SKIPPED_TABLES: {notes:?}")
        })
        .iter()
        .map(|entry| {
            (
                value_to_string(&entry["table"]),
                value_to_string(&entry["reason"]),
            )
        })
        .collect()
}

/// The reason `SKIPPED_TABLES` records for `table`, or `None` when the table is not skipped.
pub fn skip_reason(conn: &mut ExaConn, vs_name: &str, table: &str) -> Option<String> {
    skipped_entries(conn, vs_name)
        .into_iter()
        .find(|(skipped, _)| skipped == table)
        .map(|(_, reason)| reason)
}

/// The catalog identifier in the `name` member of the identifier note on `table`'s
/// `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES`, or `None` when the table has no note or no virtual
/// table. Every identifier-note assertion goes through it, so no test parses the note.
pub fn identifier_note(conn: &mut ExaConn, vs_name: &str, table: &str) -> Option<String> {
    let notes = conn.query_columns(&format!(
        "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_TABLES \
         WHERE TABLE_SCHEMA = '{vs_name}' AND TABLE_NAME = '{table}'"
    ));
    let note = notes[0].first().filter(|note| !note.is_null())?;
    let note: serde_json::Value = serde_json::from_str(&value_to_string(note))
        .unwrap_or_else(|e| panic!("the identifier note of {vs_name}.{table} must be JSON: {e}"));
    note["name"].as_str().map(str::to_string)
}

pub fn note_of<'a>(
    notes: &'a [(String, serde_json::Value)],
    column: &str,
) -> &'a serde_json::Value {
    &notes
        .iter()
        .find(|(name, _)| name == column)
        .unwrap_or_else(|| panic!("no note for column {column}: {notes:?}"))
        .1
}

pub fn value_to_string(v: &serde_json::Value) -> String {
    v.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_string())
}

pub fn nullable_text(v: &serde_json::Value) -> Option<String> {
    (!v.is_null()).then(|| value_to_string(v))
}

pub fn text_column(column: &[serde_json::Value]) -> Vec<Option<String>> {
    column.iter().map(nullable_text).collect()
}

pub fn int_column(column: &[serde_json::Value]) -> Vec<i64> {
    column.iter().map(parse_int).collect()
}

/// `(COLUMN_NAME, COLUMN_TYPE)` of one virtual table in column order, the type with its
/// whitespace and character-set suffix removed, so `VARCHAR(2000000) UTF8` reads `VARCHAR(2000000)`.
pub fn declared_types(conn: &mut ExaConn, vs_name: &str, table: &str) -> Vec<(String, String)> {
    let columns = conn.query_columns(&format!(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA = '{vs_name}' AND COLUMN_TABLE = '{}' \
         ORDER BY COLUMN_ORDINAL_POSITION",
        table.to_uppercase()
    ));
    let [names, types] = columns.as_slice() else {
        return Vec::new();
    };
    names
        .iter()
        .zip(types)
        .map(|(name, declared)| {
            let declared: String = value_to_string(declared)
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            let declared = declared
                .strip_suffix("UTF8")
                .or_else(|| declared.strip_suffix("ASCII"))
                .unwrap_or(&declared)
                .to_string();
            (value_to_string(name), declared)
        })
        .collect()
}

pub fn declared_type(conn: &mut ExaConn, vs_name: &str, table: &str, column: &str) -> String {
    declared_types(conn, vs_name, table)
        .into_iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .map(|(_, declared)| declared)
        .unwrap_or_else(|| panic!("{vs_name}.{table}.{column} has no declared type"))
}

pub const VARCHAR_JSON: &str = "VARCHAR(2000000)";

pub fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[track_caller]
pub fn query_error(conn: &mut ExaConn, sql: &str) -> String {
    error_text(&conn.try_execute(sql), sql)
}

pub fn assert_query_fails(conn: &mut ExaConn, sql: &str, fragments: &[&str]) {
    let error = query_error(conn, sql);
    assert!(
        fragments.iter().all(|f| error.contains(f)),
        "{sql} must fail naming {fragments:?}: {error}"
    );
}

/// Compares each result column of `sql`, as text with NULL as `None`, with `expected`.
pub fn assert_text_columns<C, S>(conn: &mut ExaConn, sql: &str, expected: &[C])
where
    C: AsRef<[Option<S>]>,
    S: AsRef<str>,
{
    let observed: Vec<Vec<Option<String>>> = conn
        .query_columns(sql)
        .iter()
        .map(|column| text_column(column))
        .collect();
    let expected: Vec<Vec<Option<String>>> = expected
        .iter()
        .map(|column| {
            column
                .as_ref()
                .iter()
                .map(|v| v.as_ref().map(|s| s.as_ref().to_string()))
                .collect()
        })
        .collect();
    assert_eq!(observed, expected, "{sql}");
}

/// One column of an all-types table: its exact declared type and what reading it yields.
pub struct TypeCase<'a> {
    column: &'a str,
    declared: &'a str,
    outcome: TypeOutcome<'a>,
}

enum TypeOutcome<'a> {
    Text([Option<&'a str>; 3]),
    Refused(&'a [&'a str]),
}

/// A column whose three `ID`-ordered values read as `values`, as text with NULL as `None`.
pub fn reads<'a>(column: &'a str, declared: &'a str, values: [Option<&'a str>; 3]) -> TypeCase<'a> {
    TypeCase {
        column,
        declared,
        outcome: TypeOutcome::Text(values),
    }
}

/// A column that, read alone, fails naming every fragment.
pub fn refuses<'a>(column: &'a str, declared: &'a str, fragments: &'a [&'a str]) -> TypeCase<'a> {
    TypeCase {
        column,
        declared,
        outcome: TypeOutcome::Refused(fragments),
    }
}

/// The type-coverage check of `specs/testing.md`: `cases` lists every column in declared order, the
/// readable ones are read by one `SELECT ID, ... ORDER BY ID`, and each refused one alone.
pub fn assert_type_matrix(conn: &mut ExaConn, vs_name: &str, table: &str, cases: &[TypeCase]) {
    let declared: Vec<(&str, &str)> = cases.iter().map(|c| (c.column, c.declared)).collect();
    assert_eq!(
        declared_types(conn, vs_name, table),
        pairs(&declared),
        "{vs_name}.{table} declared types"
    );
    let qualified = format!("{vs_name}.{}", table.to_uppercase());
    let (columns, values): (Vec<&str>, Vec<[Option<&str>; 3]>) = cases
        .iter()
        .filter_map(|c| match c.outcome {
            TypeOutcome::Text(values) => Some((c.column, values)),
            TypeOutcome::Refused(_) => None,
        })
        .unzip();
    assert_text_columns(
        conn,
        &format!("SELECT {} FROM {qualified} ORDER BY ID", columns.join(", ")),
        &values,
    );
    for case in cases {
        if let TypeOutcome::Refused(fragments) = case.outcome {
            assert_query_fails(
                conn,
                &format!("SELECT {} FROM {qualified}", case.column),
                fragments,
            );
        }
    }
}

/// `column` holds `DECIMAL_18_0_VALUES_TEXT`, whose text order inverts its numeric order.
pub fn assert_decimal_orders_numerically(
    conn: &mut ExaConn,
    vs_name: &str,
    table: &str,
    column: &str,
) {
    let qualified = format!("{vs_name}.{}", table.to_uppercase());
    for sql in [
        format!("SELECT {column} FROM {qualified} ORDER BY {column} LIMIT 1"),
        format!("SELECT MIN({column}) FROM {qualified}"),
    ] {
        assert_text_columns(conn, &sql, &[[DECIMAL_18_0_VALUES_TEXT[1]]]);
    }
}

/// `binary_values` declares `C_BYTES` as text over bytes that are not valid UTF-8, so reading
/// it fails naming `fragment`.
pub fn assert_binary_values_refused(
    conn: &mut ExaConn,
    vs_name: &str,
    table: &str,
    fragment: &str,
) {
    assert_eq!(
        declared_types(conn, vs_name, table),
        pairs(&[("ID", "DECIMAL(20,0)"), ("C_BYTES", VARCHAR_JSON)])
    );
    assert_query_fails(
        conn,
        &format!("SELECT C_BYTES FROM {vs_name}.{}", table.to_uppercase()),
        &[fragment],
    );
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
