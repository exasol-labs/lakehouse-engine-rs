//! Shared provisioning harness for the `exasol-e2e` integration-test binaries.
//!
//! Every `exasol-e2e` binary under `tests/` re-declared the same connection
//! constants, SLC install, schema/script DDL, Virtual Schema creation, and
//! catalog-inspection helpers. They are defined once here and `use`d by each
//! binary; per-binary variation (VS names, namespaces, extra VS properties,
//! seeding, `OnceLock` orchestration) stays local to each binary.
//!
//! Fail-loud, never-skip is preserved: the helpers panic (never return `Err`)
//! when the local stack is unavailable, per project rules.

use super::exasol_ws::ExaConn;
use super::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, minio_url, upload_to_bucketfs,
};

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::resolve_file_list;
use lakehouse_engine::scan::spec::{CatalogProps, FileEntry, StorageProps};

use std::time::Duration;

// ---------------------------------------------------------------------------
// Shared connection / provisioning constants (byte-identical across every
// `exasol-e2e` binary). Per-binary values that legitimately diverge — the VS
// name and the catalog CONNECTION name — are NOT here: the VS name stays a
// file-local constant, and the CONNECTION name is a `VsProps` field.
// ---------------------------------------------------------------------------

/// `sys` password for the local Exasol Docker container.
pub const SYS_PASSWORD: &str = "exasol";
/// Schema hosting the adapter/scan/distributor scripts.
pub const SCHEMA_NAME: &str = "LHVS";
/// RUST ADAPTER SCRIPT name.
pub const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
/// RUST SCALAR scan-UDF script name.
pub const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
/// LUA SET passthrough distributor doing the cross-node `GROUP BY shard_key`
/// fan-out. Not a Rust entry point — created by plain DDL, no `.so` involved.
pub const DISTRIBUTOR_SCRIPT_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";
/// BucketFS path for the `.so` (as PUT target).
pub const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
/// BucketFS path for the `.so` as referenced in `%udf_object` (no leading `/`).
pub const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
/// BucketFS path for the SLC tarball.
pub const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
/// SLC version linked against.
pub const SLC_VERSION: &str = "0.21.0";
/// Language alias for the SLC.
pub const LANG_ALIAS: &str = "RUST";

/// Default catalog CONNECTION name; `VsProps::with_catalog_conn_name` overrides
/// it (only the refresh binary does, with `REFRESH_CATALOG_CREDS`).
pub const DEFAULT_CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

// ---------------------------------------------------------------------------
// Provisioning helpers (byte-identical merges)
// ---------------------------------------------------------------------------

/// Download SLC `SLC_VERSION`, upload it to BucketFS, and register the RUST
/// language alias, replacing any existing `RUST=` entry so the alias points at
/// the freshly-uploaded SLC. This Exasol is dedicated to lakehouse-engine, so a
/// clean replacement is correct.
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

/// Upload the built `.so` to its BucketFS path (`SO_BUCKETFS_PUT_PATH`).
pub fn upload_so() {
    let so_path = lakehouse_engine_so_path();
    upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);
}

/// Open an Exasol connection using `sys` credentials.
pub fn exa_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

/// Create the dedicated schema, RUST adapter script, RUST scan SCALAR script,
/// and the LUA SET passthrough distributor. All idempotent (`CREATE OR
/// REPLACE`), so concurrent recreation across binaries is harmless.
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

// ---------------------------------------------------------------------------
// Virtual Schema creation — one `VsProps`-parameterized helper collapsing the
// five per-binary `create_virtual_schema` signatures.
// ---------------------------------------------------------------------------

/// Parameters for `create_virtual_schema`, collapsing the five per-binary
/// signatures into one. Build with `VsProps::new(vs_name, namespace)` and layer
/// the optional properties via the `with_*` setters.
pub struct VsProps<'a> {
    vs_name: &'a str,
    namespace: &'a str,
    catalog_conn_name: &'a str,
    parallelism_factor: Option<usize>,
    join_broadcast_max_bytes: Option<&'a str>,
}

impl<'a> VsProps<'a> {
    /// Base properties: a VS named `vs_name` over Iceberg `namespace`, using the
    /// default catalog CONNECTION and no optional VS properties.
    pub fn new(vs_name: &'a str, namespace: &'a str) -> Self {
        Self {
            vs_name,
            namespace,
            catalog_conn_name: DEFAULT_CATALOG_CONN_NAME,
            parallelism_factor: None,
            join_broadcast_max_bytes: None,
        }
    }

    /// Set the `PARALLELISM_FACTOR` VS property.
    pub fn with_parallelism_factor(mut self, factor: usize) -> Self {
        self.parallelism_factor = Some(factor);
        self
    }

    /// Set the `JOIN_BROADCAST_MAX_BYTES` VS property.
    pub fn with_join_broadcast_max_bytes(mut self, bytes: &'a str) -> Self {
        self.join_broadcast_max_bytes = Some(bytes);
        self
    }

    /// Override the catalog CONNECTION name (default `LAKEHOUSE_CATALOG_CREDS`).
    pub fn with_catalog_conn_name(mut self, name: &'a str) -> Self {
        self.catalog_conn_name = name;
        self
    }
}

/// Create (or replace) a Virtual Schema from `props`.
///
/// Re-issues the idempotent `CREATE OR REPLACE CONNECTION` for the catalog
/// credentials (harmless to repeat, and folds the join binary's separate
/// `create_connection`), drops any existing VS, then emits `CREATE VIRTUAL
/// SCHEMA` with the base properties plus the optional `PARALLELISM_FACTOR` /
/// `JOIN_BROADCAST_MAX_BYTES` clauses when set. VS properties use
/// docker-network-internal URLs because the adapter UDF runs inside the Exasol
/// container.
pub fn create_virtual_schema(conn: &mut ExaConn, props: &VsProps) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql =
        build_create_connection_sql(props.catalog_conn_name, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {} CASCADE",
        props.vs_name
    ));

    let parallelism_clause = props
        .parallelism_factor
        .map(|f| format!("\n  PARALLELISM_FACTOR  = '{f}'"))
        .unwrap_or_default();
    let join_clause = props
        .join_broadcast_max_bytes
        .map(|b| format!("\n  JOIN_BROADCAST_MAX_BYTES = '{b}'"))
        .unwrap_or_default();

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{catalog_conn_name}'
  ICEBERG_NAMESPACE   = '{namespace}'
  ALLOW_HTTP          = 'true'{parallelism_clause}{join_clause}"#,
        vs_name = props.vs_name,
        catalog_conn_name = props.catalog_conn_name,
        namespace = props.namespace,
    ));
}

// ---------------------------------------------------------------------------
// Query / result helpers
// ---------------------------------------------------------------------------

/// Run `EXPLAIN VIRTUAL <query_sql>` and flatten the pushed SQL (the generated
/// scan-driving plan plus Exasol's echoed pushdown request) into one string.
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

/// Parse a JSON result value as `f64`, accepting both numeric and
/// string-encoded numbers (Exasol renders large DECIMALs as strings).
pub fn parse_numeric(v: &serde_json::Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected numeric value, got: {v:?}"))
}

/// Parse a JSON result value as `i64`, accepting both numeric and
/// string-encoded integers.
pub fn parse_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

// ---------------------------------------------------------------------------
// Adapter-level catalog inspection helpers (host-visible URLs) — used by tests
// that call `resolve_file_list` directly rather than going through Exasol.
// ---------------------------------------------------------------------------

/// `ConnectionCreds` for the host-visible local Docker stack (MinIO + Iceberg
/// REST catalog).
pub fn local_stack_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
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

/// `StorageProps` for the host-visible local Docker stack.
pub fn local_stack_storage() -> StorageProps {
    StorageProps {
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

/// `CatalogProps` for the host-visible local Docker stack, for `table`.
pub fn local_stack_catalog(table: &str) -> CatalogProps {
    CatalogProps {
        uri: iceberg_catalog_url(),
        warehouse: "s3://warehouse/".to_string(),
        table: table.to_string(),
    }
}

/// Resolve a fixture table's current data files directly from the Iceberg REST
/// catalog, bypassing Exasol — the same `resolve_file_list` seam the adapter
/// uses. `resolve_file_list` returns each `FileEntry` with an ABSOLUTE data-file
/// URI, so the returned paths can be opened as-is.
///
/// Async (runtime-agnostic): callers drive it with whatever runtime they hold
/// (e.g. `rt.block_on(resolve_fixture_files(NAMESPACE, table))`). `namespace` is
/// passed explicitly rather than closed over a module constant.
pub async fn resolve_fixture_files(namespace: &str, table: &str) -> Vec<FileEntry> {
    let catalog_uri = iceberg_catalog_url();
    let catalog_props = local_stack_catalog(&format!("{namespace}.{table}"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();

    let (files, ..) = resolve_file_list(&catalog_uri, &catalog_props, &storage, &creds, None)
        .await
        .unwrap_or_else(|e| panic!("resolve_file_list({table}) must succeed: {e}"));
    files
}
