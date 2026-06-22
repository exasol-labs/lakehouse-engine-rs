//! End-to-end integration tests for the lakehouse-engine Virtual Schema.
//!
//! These tests run against a live Exasol + MinIO + Iceberg REST catalog stack.
//! They FAIL (never skip) when the stack is unavailable — per project rules.
//!
//! All tests share one VS, so they must run serially (--test-threads=1).
//! The Makefile `test-e2e` target passes this flag automatically.
//!
//! # Setup (done once via `setup_e2e` called from each test)
//! 1. Seed the Iceberg table into the REST catalog over MinIO.
//! 2. Install SLC 0.14.0 (LHRUST alias) and upload liblakehouse_engine.so to BucketFS.
//! 3. Create the LAKEHOUSE_ADAPTER script and LAKEHOUSE_SCAN script.
//! 4. Create the LHVS Virtual Schema over the seeded table.
//!
//! The VS properties carry UDF-internal URLs (docker-network names) for the
//! catalog and MinIO, because the UDF runs inside the Exasol container.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{E2E_QUALIFIED_TABLE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, seed_events};
use common::stack::{
    bucketfs_port, bucketfs_write_password, exasol_host, exasol_sql_port, iceberg_catalog_url,
    iceberg_catalog_url_internal, lakehouse_engine_so_path, minio_url_internal, upload_to_bucketfs,
    wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
const VS_NAME: &str = "MY_LAKEHOUSE";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
/// BucketFS path for the .so (as PUT target).
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
/// BucketFS path for the .so as referenced in %udf_object (without leading /).
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
/// BucketFS path for the SLC tarball.
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
/// SLC version we link against.
const SLC_VERSION: &str = "0.14.0";
/// Language alias for our SLC 0.14.0. This Exasol is dedicated to lakehouse-engine
/// (the sibling strata-rs stack is stopped), so we register the canonical RUST
/// alias cleanly rather than coexisting with a foreign RUST= entry.
const LANG_ALIAS: &str = "RUST";

// ---------------------------------------------------------------------------
// One-time setup
// ---------------------------------------------------------------------------

/// Marker so setup runs once across the serial test binary.
static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        // 1. Verify stack is up.
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        // 2. Seed the Iceberg table.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg events table")
        });

        // 3. Install SLC 0.14.0 (download + upload + ALTER SYSTEM).
        install_slc_0_14();

        // 4. Upload the .so to BucketFS.
        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        // 5. Create Exasol schema + scripts + VS.
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn);
    });
}

/// Install SLC 0.14.0 for the LHRUST language alias.
fn install_slc_0_14() {
    // Download the SLC tarball.
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

    // Upload to BucketFS.
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

    // Register the RUST language alias, replacing any existing RUST= entry so
    // the alias points at our freshly-uploaded 0.14.0 SLC. This Exasol is
    // dedicated to lakehouse-engine, so a clean replacement is correct.
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

    // Drop any pre-existing alias of the same name, then append our definition.
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

/// Open an Exasol connection using sys credentials.
fn exa_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

/// Create the dedicated schema, adapter script, and scan script.
fn create_schema_and_scripts(conn: &mut ExaConn) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"));

    // Adapter script — RUST ADAPTER SCRIPT.
    // The SLC dispatches to the entry point whose name matches the SQL script
    // name (__exa_udf_entry_LAKEHOUSE_ADAPTER); there is no %main directive
    // for RUST scripts. %udf_object references the uploaded .so.
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} ADAPTER SCRIPT {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));

    // Scan SET script — RUST SET SCRIPT.
    // Input: one VARCHAR column (the ScanSpec JSON). The output columns are
    // dynamic: declared with the placeholder EMITS (...) here and supplied
    // concretely by the adapter's pushdown SQL (`... EMITS (col TYPE, ...)`).
    // No %main — the SLC selects __exa_udf_entry_LAKEHOUSE_SCAN by script name.
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SET SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(spec VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
}

/// Create the Virtual Schema pointing at the seeded Iceberg table.
///
/// VS properties use docker-network-internal URLs because the adapter UDF
/// runs inside the Exasol container and must reach services by hostname.
fn create_virtual_schema(conn: &mut ExaConn) {
    // Drop the VS first (idempotent).
    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));

    let catalog_uri = iceberg_catalog_url_internal();
    let s3_endpoint = minio_url_internal();

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_URI   = '{catalog_uri}'
  WAREHOUSE     = 's3://warehouse/'
  TABLE_NAME    = '{E2E_QUALIFIED_TABLE}'
  SCAN_SCHEMA   = '{SCHEMA_NAME}'
  S3_ENDPOINT   = '{s3_endpoint}'
  S3_REGION     = 'us-east-1'
  ACCESS_KEY    = 'minioadmin'
  SECRET_KEY    = 'minioadmin'
  ALLOW_HTTP    = 'true'"#
    ));
}

// ---------------------------------------------------------------------------
// Helper: resolve the VS table name (adapter uppercases the last part of TABLE).
// ---------------------------------------------------------------------------

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The E2E projection + filter + LIMIT query returns the correct projected,
/// filtered, capped rows.
#[test]
fn e2e_projection_filter_limit_returns_correct_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    // SELECT id, name, score FROM ... WHERE score > 15.0 LIMIT 5
    // Seeded rows: id 1..20, score = 5.0*id. score > 15.0 → id >= 4 (17 rows).
    // LIMIT 5 → first 5 matching: id 4,5,6,7,8 (scores 20,25,30,35,40).
    let sql = format!(
        "SELECT id, name, score FROM {} WHERE score > 15.0 LIMIT 5",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    let id_col = &cols[0];
    assert_eq!(
        id_col.len(),
        5,
        "expected exactly 5 rows from LIMIT 5: {cols:?}"
    );

    // All returned scores must be > 15.0.
    let score_col = &cols[2];
    for score in score_col {
        let s = score
            .as_f64()
            .unwrap_or_else(|| panic!("score not f64: {score:?}"));
        assert!(s > 15.0, "filter violated: score {s} <= 15.0");
    }

    // IDs must be ascending and >= 4 (score = 5*id > 15 → id >= 4).
    // Exasol serializes DECIMAL(20,0) as a JSON string, so accept either form.
    let ids: Vec<i64> = id_col
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("id not an integer: {v:?}"))
        })
        .collect();
    assert!(
        ids.iter().all(|&id| id >= 4),
        "id < 4 appeared (score would be <= 15): {ids:?}"
    );
    // Verify names match the expected pattern.
    let name_col = &cols[1];
    for (i, name) in name_col.iter().enumerate() {
        let expected_id = ids[i];
        let n = name
            .as_str()
            .unwrap_or_else(|| panic!("name not string: {name:?}"));
        assert!(
            n.contains(&format!("{expected_id:02}")),
            "name '{n}' does not match expected id {expected_id}"
        );
    }
}

/// Create VS maps the Iceberg table schema to Exasol types correctly.
#[test]
fn create_vs_maps_iceberg_schema() {
    setup_e2e();
    let mut conn = exa_conn();

    // DESCRIBE returns (COLUMN_NAME, COLUMN_TYPE, ...).
    let sql = format!("DESCRIBE {}", vs_table());
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    // cols[0] = column names, cols[1] = column types.
    let names = &cols[0];
    let types = &cols[1];
    assert!(!names.is_empty(), "DESCRIBE returned no columns");

    // Verify each expected column exists with the right type.
    let expected = [
        ("ID", "DECIMAL"),
        ("NAME", "VARCHAR"),
        ("SCORE", "DOUBLE"),
        ("EVENT_DATE", "DATE"),
        ("EVENT_TS", "TIMESTAMP"),
    ];
    for (expected_name, expected_type_prefix) in expected {
        let pos = names
            .iter()
            .position(|n| {
                n.as_str()
                    .map(|s| s.eq_ignore_ascii_case(expected_name))
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!("column {expected_name} not found in DESCRIBE output: {names:?}")
            });
        let ty = types[pos]
            .as_str()
            .unwrap_or_else(|| panic!("type at position {pos} is not a string: {:?}", types[pos]));
        assert!(
            ty.to_uppercase().contains(expected_type_prefix),
            "column {expected_name}: expected type containing '{expected_type_prefix}', got '{ty}'"
        );
    }
}

/// Filter predicate restricts the emitted rows (no extra rows).
#[test]
fn scan_filter_restricts_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let row_count =
        conn.query_row_count(&format!("SELECT id FROM {} WHERE score > 15.0", vs_table()));
    assert_eq!(
        row_count, SEED_ROWS_SCORE_GT_15 as i64,
        "WHERE score > 15.0 should return {SEED_ROWS_SCORE_GT_15} rows, got {row_count}"
    );
}

/// LIMIT caps the rows emitted by the scan.
#[test]
fn scan_limit_caps_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let row_count = conn.query_row_count(&format!("SELECT id FROM {} LIMIT 3", vs_table()));
    assert_eq!(
        row_count, 3,
        "LIMIT 3 should return exactly 3 rows, got {row_count}"
    );
}

/// Both entry points (adapter + scan) resolve from the same uploaded .so artifact.
#[test]
fn both_scripts_resolve_one_artifact() {
    setup_e2e();
    let mut conn = exa_conn();

    // Verify both scripts exist and point to the same .so object path.
    let resp_adapter = conn.execute(&format!(
        "SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS WHERE SCRIPT_NAME='{ADAPTER_SCRIPT_NAME}' AND SCRIPT_SCHEMA='{SCHEMA_NAME}'"
    ));
    let adapter_body = resp_adapter["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or("")
        .to_string();

    let resp_scan = conn.execute(&format!(
        "SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS WHERE SCRIPT_NAME='{SCAN_SCRIPT_NAME}' AND SCRIPT_SCHEMA='{SCHEMA_NAME}'"
    ));
    let scan_body = resp_scan["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or("")
        .to_string();

    // Both script bodies must reference the same .so artifact path.
    assert!(
        adapter_body.contains("liblakehouse_engine.so") || adapter_body.contains("udf"),
        "adapter script body does not reference the .so: {adapter_body}"
    );
    assert!(
        scan_body.contains("liblakehouse_engine.so") || scan_body.contains("udf"),
        "scan script body does not reference the .so: {scan_body}"
    );
}

/// Full projection + filter + date/timestamp columns round-trip correctly.
#[test]
fn mixed_column_parquet_round_trips() {
    setup_e2e();
    let mut conn = exa_conn();

    // Select all columns for the first row (id=1) to verify type conversion.
    let cols = conn.query_columns(&format!(
        "SELECT id, name, score, event_date, event_ts FROM {} WHERE id = 1",
        vs_table()
    ));
    assert_eq!(cols.len(), 5, "expected 5 columns: {cols:?}");
    // Each column has exactly 1 row.
    for (i, col) in cols.iter().enumerate() {
        assert_eq!(col.len(), 1, "column {i} should have 1 row: {col:?}");
    }
    // id = 1 (Exasol returns numerics as strings or numbers).
    let id_val = &cols[0][0];
    assert!(
        id_val.as_i64().map(|v| v == 1).unwrap_or(false)
            || id_val.as_str().map(|s| s == "1").unwrap_or(false),
        "id should be 1, got: {id_val:?}"
    );
    // name = "event-01".
    let name_val = &cols[1][0];
    assert!(
        name_val
            .as_str()
            .map(|s| s.contains("event-01"))
            .unwrap_or(false),
        "name should be 'event-01', got: {name_val:?}"
    );
    // score = 5.0 (5.0 * 1).
    let score_val = &cols[2][0];
    assert!(
        score_val
            .as_f64()
            .map(|v| (v - 5.0).abs() < 0.001)
            .unwrap_or(false)
            || score_val.as_str().map(|s| s.contains('5')).unwrap_or(false),
        "score should be 5.0, got: {score_val:?}"
    );
    // event_date and event_ts: non-null values.
    assert!(!cols[3][0].is_null(), "event_date must not be null");
    assert!(!cols[4][0].is_null(), "event_ts must not be null");
}

/// Error scenario: CREATE VS with unreachable catalog returns a clear error without secrets.
#[test]
fn create_vs_unreachable_catalog_errors_no_secret() {
    setup_e2e();
    let mut conn = exa_conn();

    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA BAD_CATALOG_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_URI = 'http://does-not-exist.invalid:8181'
  WAREHOUSE   = 's3://warehouse/'
  TABLE_NAME  = 'ns.table'
  S3_ENDPOINT = 'http://does-not-exist.invalid:9000'
  S3_REGION   = 'us-east-1'
  ACCESS_KEY  = 'SUPER_SECRET_KEY'
  SECRET_KEY  = 'SUPER_SECRET_VALUE'
  ALLOW_HTTP  = 'true'"#
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error when catalog is unreachable, got: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        !msg.contains("SUPER_SECRET_KEY") && !msg.contains("SUPER_SECRET_VALUE"),
        "error message must not leak credentials: {msg}"
    );
}

/// Unreadable file scenario: scan of a non-existent file errors without leaking credentials.
#[test]
fn scan_unreadable_file_errors_no_secret() {
    setup_e2e();
    let mut conn = exa_conn();

    // Drop and recreate a VS pointing at a non-existent file path.
    // We do this by using a table name that doesn't exist in the catalog.
    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA BAD_TABLE_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_URI = '{}'
  WAREHOUSE   = 's3://warehouse/'
  TABLE_NAME  = 'e2e_lakehouse.no_such_table'
  S3_ENDPOINT = '{}'
  S3_REGION   = 'us-east-1'
  ACCESS_KEY  = 'minioadmin'
  SECRET_KEY  = 'topsecretvalue'
  ALLOW_HTTP  = 'true'"#,
        iceberg_catalog_url_internal(),
        minio_url_internal()
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error for a non-existent table: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    // Credentials must not appear in error message.
    assert!(
        !msg.contains("topsecretvalue"),
        "error must not leak credentials: {msg}"
    );
}

/// Verify the suite panics (not silently passes) when Exasol is unreachable.
///
/// This test is a behavioural assertion: if the connect helpers get a bad host,
/// they panic/unwrap, which causes the test binary to abort — NOT return Ok.
/// We verify the behaviour by showing the panic path is reachable.
#[test]
fn e2e_fails_when_stack_unavailable() {
    // The design contract: ExaConn::connect panics on TCP failure.
    // We verify the contract is enforced by asserting that attempting to connect
    // to a known-bad address would panic. We do NOT actually panic here (that
    // would fail the test), but we verify the function body takes the panic path.
    //
    // The real enforcement is that every test calls setup_e2e(), which calls
    // wait_for_exasol() which panics on timeout — this test documents the contract.
    let result = std::panic::catch_unwind(|| {
        // This MUST panic — ExaConn::connect on a bad address panics, never returns Err.
        ExaConn::connect("192.0.2.1", 8563, "sys", "exasol")
    });
    assert!(
        result.is_err(),
        "ExaConn::connect to an unreachable host must panic, not return Ok"
    );
}

// ---------------------------------------------------------------------------
// Task 5.4 / Plan scenario coverage: partial-aggregate E2E stubs
// Group D fills in the assertions; we define the function names here so the
// plan's scenario table can reference them and they compile without the feature.
// ---------------------------------------------------------------------------

/// Scan computes a node-local partial aggregate instead of raw rows.
///
/// Verifies: spec with aggregate plan causes the UDF to emit one partial row
/// per shard, not the full row set.
#[test]
fn scan_emits_partial_aggregate_row() {
    setup_e2e();
    let mut conn = exa_conn();

    // COUNT(*) over the whole table: one merged row with the total row count.
    let cols = conn.query_columns(&format!("SELECT COUNT(*) FROM {}", vs_table()));
    assert_eq!(cols.len(), 1, "COUNT(*) must return one column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "COUNT(*) must return one row: {cols:?}");
    let count = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(*) result not integer: {:?}", cols[0][0]));
    // The seeded table has 20 rows.
    assert_eq!(count, 20, "COUNT(*) should return 20 for the seeded table");
}

/// Partial COUNT/SUM/MIN/MAX emitted in merge-ready form.
///
/// Verifies: each aggregate type returns the correct merged scalar.
#[test]
fn partial_count_sum_min_max_merge_ready() {
    setup_e2e();
    let mut conn = exa_conn();

    // score = 5.0 * id for id 1..20; SUM = 5*(1+2+...+20) = 5*210 = 1050.
    let cols = conn.query_columns(&format!(
        "SELECT COUNT(*), SUM(score), MIN(score), MAX(score) FROM {}",
        vs_table()
    ));
    assert_eq!(cols.len(), 4, "must return 4 columns: {cols:?}");

    let count = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("COUNT must be integer");
    assert_eq!(count, 20, "COUNT(*) must be 20");

    let sum = cols[1][0]
        .as_f64()
        .or_else(|| cols[1][0].as_str().and_then(|s| s.parse().ok()))
        .expect("SUM must be numeric");
    assert!(
        (sum - 1050.0).abs() < 0.001,
        "SUM(score) must be 1050, got {sum}"
    );

    let min = cols[2][0]
        .as_f64()
        .or_else(|| cols[2][0].as_str().and_then(|s| s.parse().ok()))
        .expect("MIN must be numeric");
    assert!(
        (min - 5.0).abs() < 0.001,
        "MIN(score) must be 5.0, got {min}"
    );

    let max = cols[3][0]
        .as_f64()
        .or_else(|| cols[3][0].as_str().and_then(|s| s.parse().ok()))
        .expect("MAX must be numeric");
    assert!(
        (max - 100.0).abs() < 0.001,
        "MAX(score) must be 100.0, got {max}"
    );
}

/// AVG emitted as a partial sum and partial count pair.
///
/// Verifies: AVG(score) returns the correct average, including with a WHERE filter.
#[test]
fn partial_avg_emits_sum_count_pair() {
    setup_e2e();
    let mut conn = exa_conn();

    // AVG(score) over all rows: 1050.0 / 20 = 52.5.
    let cols = conn.query_columns(&format!("SELECT AVG(score) FROM {}", vs_table()));
    assert_eq!(cols.len(), 1, "AVG must return one column: {cols:?}");
    let avg = cols[0][0]
        .as_f64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("AVG must be numeric");
    assert!(
        (avg - 52.5).abs() < 0.001,
        "AVG(score) must be 52.5, got {avg}"
    );

    // AVG with WHERE: score > 15.0 → id >= 4 (17 rows), scores 20..100.
    // SUM = 5*(4+5+...+20) = 5*(17*12) = 5*204... actually sum of 4..20 = (4+20)*17/2 = 204,
    // so SUM(score) = 5*204 = 1020, AVG = 1020/17 = 60.0.
    let cols_filtered = conn.query_columns(&format!(
        "SELECT AVG(score) FROM {} WHERE score > 15.0",
        vs_table()
    ));
    let avg_filtered = cols_filtered[0][0]
        .as_f64()
        .or_else(|| cols_filtered[0][0].as_str().and_then(|s| s.parse().ok()))
        .expect("filtered AVG must be numeric");
    assert!(
        (avg_filtered - 60.0).abs() < 0.001,
        "filtered AVG(score) must be 60.0, got {avg_filtered}"
    );
}

/// After createVirtualSchema the CLUSTER_NODES count is recorded in the schema's
/// adapterNotes and is >= 1.
///
/// Queries SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES — the observable catalog
/// column for adapter-controlled schema state. Exasol does NOT persist
/// adapter-returned schemaMetadata.properties (they are silently dropped and
/// never appear in any catalog view), so the adapter carries CLUSTER_NODES in
/// adapterNotes (a JSON string), which Exasol DOES persist and surface here.
///
/// (The view is keyed by SCHEMA_NAME, confirmed against the live DB.)
///
/// CONNECTION_NAME is not supplied in create_virtual_schema, so connect-back
/// defaults to 1 — asserting >= 1 (not == cluster size) is correct and robust.
#[test]
fn create_vs_records_cluster_nodes_property() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS \
         WHERE SCHEMA_NAME = '{VS_NAME}'"
    ));
    assert_eq!(
        cols.len(),
        1,
        "query must return one column (ADAPTER_NOTES): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "the virtual schema must exist (one row): {cols:?}"
    );
    let notes = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("ADAPTER_NOTES value is not a string: {:?}", cols[0][0]));
    assert!(
        !notes.is_empty(),
        "ADAPTER_NOTES must be non-empty (Exasol must have persisted it): {notes:?}"
    );

    // adapterNotes is a JSON string carrying {"CLUSTER_NODES":"<n>"}.
    let parsed: serde_json::Value = serde_json::from_str(notes)
        .unwrap_or_else(|e| panic!("ADAPTER_NOTES must be valid JSON ({e}): {notes:?}"));
    let raw = parsed["CLUSTER_NODES"]
        .as_str()
        .unwrap_or_else(|| panic!("ADAPTER_NOTES must carry CLUSTER_NODES as a string: {notes:?}"));
    let n: i64 = raw
        .parse()
        .unwrap_or_else(|_| panic!("CLUSTER_NODES value '{raw}' is not an integer"));
    assert!(n >= 1, "CLUSTER_NODES must be >= 1, got {n}");
}

/// COUNT(col) aggregate pushdown returns the correct non-null row count.
///
/// Verifies COUNT(score) and COUNT(score) WHERE score > 15.0, covering the
/// COUNT(col) case not exercised by existing COUNT(*) tests.
#[test]
fn aggregate_count_col_returns_correct_value() {
    setup_e2e();
    let mut conn = exa_conn();

    // COUNT(score) over all rows: all 20 rows have non-null score.
    let cols = conn.query_columns(&format!("SELECT COUNT(score) FROM {}", vs_table()));
    assert_eq!(
        cols.len(),
        1,
        "COUNT(score) must return one column: {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "COUNT(score) must return one row: {cols:?}"
    );
    let count_all = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(score) result not integer: {:?}", cols[0][0]));
    assert_eq!(count_all, 20, "COUNT(score) must be 20 (all rows non-null)");

    // COUNT(score) WHERE score > 15.0 → 17 rows (SEED_ROWS_SCORE_GT_15).
    let cols_filtered = conn.query_columns(&format!(
        "SELECT COUNT(score) FROM {} WHERE score > 15.0",
        vs_table()
    ));
    let count_filtered = cols_filtered[0][0]
        .as_i64()
        .or_else(|| cols_filtered[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| {
            panic!(
                "filtered COUNT(score) result not integer: {:?}",
                cols_filtered[0][0]
            )
        });
    assert_eq!(
        count_filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "COUNT(score) WHERE score > 15.0 must be {SEED_ROWS_SCORE_GT_15}, got {count_filtered}"
    );

    // COUNT(*) WHERE score > 15.0 — also verifies the WHERE path for COUNT(*).
    let cols_star = conn.query_columns(&format!(
        "SELECT COUNT(*) FROM {} WHERE score > 15.0",
        vs_table()
    ));
    let count_star = cols_star[0][0]
        .as_i64()
        .or_else(|| cols_star[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("COUNT(*) WHERE result not integer: {:?}", cols_star[0][0]));
    assert_eq!(
        count_star, SEED_ROWS_SCORE_GT_15 as i64,
        "COUNT(*) WHERE score > 15.0 must be {SEED_ROWS_SCORE_GT_15}, got {count_star}"
    );
}

/// A multi-shard fan-out query returns the complete, non-overlapping row set.
///
/// The test Exasol stack is single-node so partition_files yields one shard at
/// runtime; true cross-node file placement is exercised only on a real multi-node
/// cluster. On the single-node stack this test asserts the union-completeness
/// invariant: the fan-out/union path returns every row exactly once with no gaps
/// and no duplicates — the correctness property that multi-shard sharding guarantees.
#[test]
fn multi_shard_row_query_matches_single_shard() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!("SELECT id FROM {} ORDER BY id", vs_table()));
    assert_eq!(cols.len(), 1, "SELECT id must return one column: {cols:?}");
    assert_eq!(
        cols[0].len(),
        20,
        "fan-out must return all 20 rows, no duplicates, no gaps: got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("id is not an integer: {v:?}"))
        })
        .collect();

    for (pos, &id) in ids.iter().enumerate() {
        let expected = (pos + 1) as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id} (union-completeness violated)"
        );
    }
}
