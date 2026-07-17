//! End-to-end coverage for the far-future INT96 timestamp fix (issue #143 —
//! `Cast error: Overflow converting 9999-12-31 23:59:59 to Nanosecond`).
//!
//! Two concerns share this binary, both driven against the local Apache Spark
//! Iceberg fixtures stack (`scripts/spark-fixtures/`, MinIO + Iceberg REST
//! catalog):
//!
//! 1. `e2e_int96_fixture_present_and_int96_encoded` (this file, task 2.3) — the
//!    fixture-shape guard. It resolves the committed
//!    `e2e_lakehouse.int96_ts_far_future` table's data file via
//!    `resolve_file_list` (exactly as `e2e_positional_deletes_test.rs`'s
//!    `fixture_spark_file_granularity_delete_table` does), opens that Parquet
//!    file directly from MinIO (NOT through the scan UDF), and asserts the
//!    timestamp column's PHYSICAL type is `INT96`.
//! 2. `e2e_int96_far_future_timestamp_scans_without_overflow` (task 3.2) — the
//!    full-stack scan test. It drives the same fixture through the VS →
//!    adapter → scan-UDF stack, needing Exasol, the scan `.so`, and the
//!    Virtual Schema; those pieces are NOT provisioned by this file's
//!    `setup()` (which only waits for the MinIO + catalog the fixture-shape
//!    guard reads). `setup_full_stack()` layers that Exasol/VS provisioning
//!    on top of `setup()`.
//!
//! WHY A SEPARATE PHYSICAL-ENCODING GUARD (the fail-loud rationale): the scan
//! test (3.2) proves the value decodes to `9999-12-31 23:59:59` without the
//! nanosecond overflow — but an INT64-microseconds column ALSO decodes that
//! value without overflow. So the scan test alone cannot prove the fixture is
//! genuinely INT96; a silent INT64 import (or an `add_files` rewrite to INT64)
//! would let it pass VACUOUSLY, exercising nothing. This test closes that hole:
//! it fails loud the moment the committed data file is not physically INT96,
//! which is the ONLY shape that exercises arrow-rs's INT96→Nanosecond overflow
//! path the fix targets.
//!
//! The `int96_ts_far_future` table is authored ONCE, at stack bring-up, by the
//! `spark-iceberg-fixtures` one-shot Compose job (see
//! `scripts/spark-fixtures/create_int96_timestamp_fixture.sql`, wired into
//! `run_fixtures.sh`) — this harness never seeds it. Ground truth lives in
//! `tests/common/int96_fixtures.rs` and MUST stay in lockstep with that SQL.
//!
//! Per project rules this test FAILS (never skips) when its stack is
//! unreachable: `setup()` calls `wait_for_minio`/`wait_for_iceberg_catalog`,
//! which panic (never return `Err`) on a dependency that never comes up.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::int96_fixtures::{
    INT96_TS_FAR_FUTURE_COLUMN, INT96_TS_FAR_FUTURE_EXPECTED_VALUE, INT96_TS_FAR_FUTURE_TABLE,
    NAMESPACE,
};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, minio_url, upload_to_bucketfs, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::resolve_file_list;
use lakehouse_engine::scan::spec::{CatalogProps, FileEntry, StorageProps};

use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use parquet::file::reader::{FileReader, SerializedFileReader};

use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// One-time setup (idempotent). Waits ONLY for the pieces the fixture-shape
// guard actually reads — the MinIO object store and the Iceberg REST catalog.
// `setup_full_stack()` below (the full-stack scan test, task 3.2) layers
// Exasol + .so upload + Virtual Schema creation on top of this base.
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_minio();
        wait_for_iceberg_catalog();
    });
}

// ---------------------------------------------------------------------------
// Full-stack provisioning (task 3.2) — layered ON TOP of `setup()` above.
//
// The fixture-shape guard (2.3) only needs MinIO + the Iceberg catalog. The
// full-stack scan test additionally needs Exasol up, the scan `.so` uploaded to
// BucketFS, the adapter/scan scripts created, and a Virtual Schema over the
// `e2e_lakehouse` namespace. Every constant, DDL statement, and helper below
// mirrors `e2e_positional_deletes_test.rs` so the provisioned VS is identical
// to the one the rest of the E2E suite drives (same `.so`, same idempotent
// CREATE OR REPLACE objects, shared across E2E binaries).
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
/// Shared VS used by every other E2E test binary too (idempotent recreation
/// with an identical body, so concurrent recreation across binaries is
/// harmless).
const VS_NAME: &str = "MY_LAKEHOUSE";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
const MERGE_SCRIPT_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";
/// LUA SET passthrough distributor doing the cross-node `GROUP BY shard_key`
/// fan-out. Not a Rust entry point — created by plain DDL, no `.so` involved.
const DISTRIBUTOR_SCRIPT_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.21.0";
const LANG_ALIAS: &str = "RUST";
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

static FULL_STACK_SETUP_DONE: OnceLock<()> = OnceLock::new();

/// Provision the full VS → adapter → scan-UDF stack for the scan test (3.2).
///
/// Reuses `setup()` for the MinIO + Iceberg-catalog wait (never duplicated),
/// then layers Exasol readiness, the SLC install, the scan `.so` upload, the
/// adapter/scan scripts, and the Virtual Schema on top. Idempotent and
/// fail-loud: `setup()` and `wait_for_exasol` panic (never skip) when the stack
/// is unavailable, per project rules.
fn setup_full_stack() {
    setup();
    FULL_STACK_SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        install_slc();

        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, VS_NAME);
    });
}

fn install_slc() {
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

fn exa_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

fn create_schema_and_scripts(conn: &mut ExaConn) {
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
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{MERGE_SCRIPT_NAME}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
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

/// Create (or replace) the Virtual Schema over the shared `e2e_lakehouse`
/// namespace. The emitted DDL is byte-identical to what
/// `e2e_positional_deletes_test.rs` emits for its default (`None`
/// parallelism) VS, so the adapter's hardware-derived default parallelism
/// applies — the fan-out-forcing `PARALLELISM_FACTOR` variants that file needs
/// have no bearing on this timestamp-decode test and are deliberately omitted.
fn create_virtual_schema(conn: &mut ExaConn, vs_name: &str) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{NAMESPACE}'
  ALLOW_HTTP          = 'true'"#
    ));
}

fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

// ---------------------------------------------------------------------------
// Adapter-level catalog inspection helpers — mirror
// `e2e_positional_deletes_test.rs`'s local_stack_creds/local_stack_storage/
// local_stack_catalog + resolve_fixture_files. Kept as async, runtime-agnostic
// helpers (no internal `block_on`) so both this `#[test]` and the future
// full-stack test can drive them from whichever runtime shape they use.
// ---------------------------------------------------------------------------

fn local_stack_creds() -> ConnectionCreds {
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

fn local_stack_storage() -> StorageProps {
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

fn local_stack_catalog(table: &str) -> CatalogProps {
    CatalogProps {
        uri: iceberg_catalog_url(),
        warehouse: "s3://warehouse/".to_string(),
        table: table.to_string(),
    }
}

/// Resolve a fixture table's current data files directly from the Iceberg REST
/// catalog, bypassing Exasol — the same `resolve_file_list` seam the adapter
/// uses. `resolve_file_list` returns each `FileEntry` with an ABSOLUTE data-file
/// URI (relativization to the table root happens later in the adapter, not
/// here), so the returned paths can be opened as-is.
async fn resolve_fixture_files(table: &str) -> Vec<FileEntry> {
    let catalog_uri = iceberg_catalog_url();
    let catalog_props = local_stack_catalog(&format!("{NAMESPACE}.{table}"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();

    let (files, ..) = resolve_file_list(&catalog_uri, &catalog_props, &storage, &creds, None)
        .await
        .unwrap_or_else(|e| panic!("resolve_file_list({table}) must succeed: {e}"));
    files
}

/// Read a data file's raw bytes from MinIO through the SAME `object_store` S3
/// client the scan UDF uses (`build_s3_store` in `scan/mod.rs`), so the
/// fixture-shape guard inspects the exact bytes the scan path would decode.
///
/// `resolve_file_list` yields an absolute `s3://<bucket>/<key>` (or `s3a://…`)
/// URI; this splits off the bucket and reads the object by its key.
async fn fetch_object_bytes(uri: &str) -> bytes::Bytes {
    let without_scheme = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or_else(|| panic!("data file URI must be an s3/s3a URI, got: {uri}"));
    let (bucket, key) = without_scheme
        .split_once('/')
        .unwrap_or_else(|| panic!("data file URI must have a <bucket>/<key> form, got: {uri}"));

    let storage = local_stack_storage();
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_endpoint(&storage.endpoint)
        .with_allow_http(storage.allow_http)
        .with_virtual_hosted_style_request(!storage.path_style)
        .build()
        .unwrap_or_else(|e| panic!("configure MinIO object store for {uri}: {e}"));

    store
        .get(&ObjectStorePath::from(key))
        .await
        .unwrap_or_else(|e| panic!("GET {uri} from MinIO: {e}"))
        .bytes()
        .await
        .unwrap_or_else(|e| panic!("read bytes of {uri}: {e}"))
}

// ---------------------------------------------------------------------------
// Fixture-shape guard (packaging/int96-timestamp-fixture) — inspects the
// Spark-committed data file's Parquet footer directly, bypassing Exasol, to
// prove the committed fixture is GENUINELY INT96-encoded (not a silently
// degraded INT64 import that would make the scan test pass vacuously).
// ---------------------------------------------------------------------------

/// The committed `int96_ts_far_future` data file's timestamp column is
/// physically encoded as Parquet `INT96`.
///
/// Native Spark write (`spark.sql.parquet.outputTimestampType=INT96`) +
/// Iceberg `add_files` (register as-is, no rewrite) is the only path that lands
/// a genuinely INT96 column in an Iceberg table. This asserts that path held: a
/// silent INT64 import — Iceberg's Spark writer emits INT64 regardless of
/// `outputTimestampType`, and `add_files` could in principle rewrite — fails
/// HERE rather than letting the scan test (3.2) pass without exercising the
/// arrow-rs INT96→Nanosecond overflow the fix targets.
#[test]
fn e2e_int96_fixture_present_and_int96_encoded() {
    setup();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let files = rt.block_on(resolve_fixture_files(INT96_TS_FAR_FUTURE_TABLE));
    // REPARTITION(1) in the fixture SQL forces a single output file, and
    // `add_files` registers exactly that one file — so the table must resolve to
    // exactly one data file to inspect.
    assert_eq!(
        files.len(),
        1,
        "{INT96_TS_FAR_FUTURE_TABLE} must resolve exactly 1 data file, got {}: {files:?}",
        files.len()
    );

    let data_uri = files[0].path.clone();
    let bytes = rt.block_on(fetch_object_bytes(&data_uri));

    let reader = SerializedFileReader::new(bytes)
        .unwrap_or_else(|e| panic!("open committed Parquet file {data_uri}: {e}"));
    let schema_descr = reader.metadata().file_metadata().schema_descr();

    let ts_col = schema_descr
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(INT96_TS_FAR_FUTURE_COLUMN))
        .unwrap_or_else(|| {
            let names: Vec<&str> = schema_descr.columns().iter().map(|c| c.name()).collect();
            panic!(
                "committed Parquet file {data_uri} must have a '{INT96_TS_FAR_FUTURE_COLUMN}' \
                 column, got columns {names:?}"
            )
        });

    assert_eq!(
        ts_col.physical_type(),
        parquet::basic::Type::INT96,
        "fixture guard: the '{INT96_TS_FAR_FUTURE_COLUMN}' column of {data_uri} must be \
         physically INT96-encoded (a silent INT64 import decodes without overflow at nanosecond \
         too, so it would make the scan test pass vacuously — see issue #143), got {:?}",
        ts_col.physical_type()
    );
}

// ---------------------------------------------------------------------------
// Full-stack scan test (datafusion-scan/scan-execution, task 3.2) — drives the
// far-future INT96 fixture through the whole VS → adapter → scan-UDF →
// DataFusion path and proves the value decodes WITHOUT the issue #143
// nanosecond overflow. Unlike the fixture-shape guard above (which inspects the
// Parquet footer directly), this exercises the exact scan path the fix lives
// in; the guard proves the column is genuinely INT96, so this test cannot pass
// vacuously against a silently-degraded INT64 import.
// ---------------------------------------------------------------------------

/// A `SELECT` of the far-future INT96 `timestamp` column through the Virtual
/// Schema decodes at microsecond resolution and returns
/// `9999-12-31 23:59:59`, WITHOUT the `Cast error: Overflow converting
/// 9999-12-31 23:59:59 to Nanosecond` that arrow-rs's default INT96→Nanosecond
/// decode raises on far-future timestamps (issue #143).
///
/// End-to-end proof of the `coerce_int96 = "us"` fix: a plain `SELECT` of the
/// INT96 column is the exact reproducer — the overflow is at decode time,
/// independent of any predicate.
#[test]
fn e2e_int96_far_future_timestamp_scans_without_overflow() {
    setup_full_stack();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT {INT96_TS_FAR_FUTURE_COLUMN} FROM {}",
        vs_table(VS_NAME, INT96_TS_FAR_FUTURE_TABLE)
    );

    // `try_execute` (not `query_columns`) so a regression surfaces as an
    // explicit, issue-#143-named assertion rather than the generic execute
    // panic: before the fix, decoding the far-future INT96 value fails here
    // with `Cast error: Overflow converting 9999-12-31 23:59:59 to Nanosecond`.
    let resp = conn.try_execute(&sql);
    assert_eq!(
        resp["status"].as_str(),
        Some("ok"),
        "scanning the far-future INT96 fixture must NOT fail with the issue #143 \
         nanosecond-overflow error — the coerce_int96=\"us\" fix must decode it at \
         microsecond resolution; got: {resp}"
    );

    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    assert_eq!(
        cols.len(),
        1,
        "SELECT {INT96_TS_FAR_FUTURE_COLUMN} must return exactly 1 column, got {}: {cols:?}",
        cols.len()
    );
    assert_eq!(
        cols[0].len(),
        1,
        "{INT96_TS_FAR_FUTURE_TABLE} holds exactly 1 row, got {}: {:?}",
        cols[0].len(),
        cols[0]
    );

    let ts = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("timestamp value must be a string, got: {:?}", cols[0][0]));
    // Exasol renders TIMESTAMP with fractional seconds (e.g. `…59.000000`), so
    // match the seconds-resolution prefix: this proves the exact instant
    // (year 9999, no overflow/wrap) independent of the fractional-second suffix.
    assert!(
        ts.starts_with(INT96_TS_FAR_FUTURE_EXPECTED_VALUE),
        "scanned far-future INT96 timestamp must be {INT96_TS_FAR_FUTURE_EXPECTED_VALUE}, got {ts:?}"
    );
}
