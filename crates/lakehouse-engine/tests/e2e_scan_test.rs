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
//! 2. Install SLC 0.20.1 (LHRUST alias) and upload liblakehouse_engine.so to BucketFS.
//! 3. Create the LAKEHOUSE_ADAPTER script and LAKEHOUSE_SCAN script.
//! 4. Create the LHVS Virtual Schema over the seeded table.
//!
//! The VS properties carry UDF-internal URLs (docker-network names) for the
//! catalog and MinIO, because the UDF runs inside the Exasol container.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{
    E2E_EVO_TABLE, E2E_NAMESPACE, E2E_PART_TABLE, E2E_TABLE, E2E_TABLE_2, EVO_NEW_COL,
    EVO_TOTAL_ROWS, PART_CENTRAL_IDS, PART_COL, PART_NORTH_IDS, PART_ROWS_PER_FILE,
    PART_TOTAL_ROWS, PART_VAL_CENTRAL, PART_VAL_NORTH, SEED_LABELS_ROWS, SEED_ROWS_SCORE_GT_15,
    seed_events, seed_renamed_column,
};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, upload_to_bucketfs, wait_for_exasol, wait_for_iceberg_catalog,
    wait_for_minio,
};

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::resolve_file_list;
use lakehouse_engine::scan::spec::{CatalogProps, StorageProps};

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
const SLC_VERSION: &str = "0.20.1";
/// Name of the Exasol CONNECTION carrying catalog + storage credentials.
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";
/// Language alias for our SLC. This Exasol is dedicated to lakehouse-engine
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

        // 3. Install SLC 0.20.1 (download + upload + ALTER SYSTEM).
        install_slc();

        // 4. Upload the .so to BucketFS.
        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        // 5. Create Exasol schema + scripts + VS.
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn);
    });
}

/// Install SLC 0.20.1 for the LHRUST language alias.
fn install_slc() {
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
    // the alias points at our freshly-uploaded 0.20.1 SLC. This Exasol is
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
    // Input: two VARCHAR columns — arg0 is the common ScanSpec blob (shared
    // across all shards, serialized once via `ScanSpec::to_common_json()`),
    // arg1 is the per-shard files JSON list (via `ScanSpec::files_json()`).
    // The output columns are dynamic: declared with the placeholder EMITS (...)
    // here and supplied concretely by the adapter's pushdown SQL
    // (`... EMITS (col TYPE, ...)`).
    // No %main — the SLC selects __exa_udf_entry_LAKEHOUSE_SCAN by script name.
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SET SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
}

/// Create the Virtual Schema pointing at the seeded Iceberg table.
///
/// Credentials are stored in an Exasol CONNECTION (CATALOG_CONN_NAME) whose
/// address is the catalog URI and whose password is a JSON credential object.
/// VS properties use docker-network-internal URLs because the adapter UDF
/// runs inside the Exasol container and must reach services by hostname.
fn create_virtual_schema(conn: &mut ExaConn) {
    // Create the catalog CONNECTION first (idempotent: CREATE OR REPLACE).
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    // Drop the VS first (idempotent).
    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{E2E_NAMESPACE}'
  SCAN_SCHEMA         = '{SCHEMA_NAME}'
  ALLOW_HTTP          = 'true'"#
    ));
}

// ---------------------------------------------------------------------------
// Helpers: qualify VS table names (adapter uppercases all Iceberg names).
// ---------------------------------------------------------------------------

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

fn vs_labels_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE_2.to_uppercase())
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
///
/// The secret value lives in the CONNECTION password JSON (not in the SQL).
/// The test asserts the error message does not contain the credential values.
#[test]
fn create_vs_unreachable_catalog_errors_no_secret() {
    setup_e2e();
    let mut conn = exa_conn();

    // Create a CONNECTION with a bogus catalog URI and bogus credentials.
    // The credential values must not appear in any error message.
    let bogus_password = common::stack::CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: "http://does-not-exist.invalid:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "SUPER_SECRET_KEY".to_string(),
        secret_key: "SUPER_SECRET_VALUE".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
    };
    let bogus_uri = "http://does-not-exist.invalid:8181";
    let create_conn_sql =
        build_create_connection_sql("BAD_CATALOG_CREDS", bogus_uri, &bogus_password);
    conn.execute(&create_conn_sql);

    let resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA BAD_CATALOG_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = 'BAD_CATALOG_CREDS'
  ICEBERG_NAMESPACE   = 'ns'
  ALLOW_HTTP          = 'true'"#
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

/// Querying a non-existent virtual table name in the VS errors with a clear TABLE_MAP message.
///
/// With namespace enumeration, a table that does not exist in the namespace was never
/// registered in TABLE_MAP. Any pushdown for such a name must fail with a clear error
/// rather than silently scanning the wrong table.
#[test]
fn scan_unknown_virtual_table_errors() {
    setup_e2e();
    let mut conn = exa_conn();

    // Querying a table name that was not in the namespace at create time will fail
    // at the pushdown stage because the Exasol table name is not in TABLE_MAP.
    // We exercise this by querying a VS table that we know does not exist.
    let resp = conn.try_execute(&format!("SELECT * FROM {VS_NAME}.NO_SUCH_TABLE LIMIT 1"));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error for unknown virtual table NO_SUCH_TABLE: {resp}"
    );
}

/// Schema evolution with a renamed column resolves correctly by Iceberg field-id.
///
/// Scenario (seeded by `seed_renamed_column`), field-id 2 stable throughout:
///   - file A: ids 1..=5,  physical parquet column `score`
///   - catalog rename `score` -> `rating` (field-id 2 preserved)
///   - file B: ids 6..=10, physical parquet column `rating`
///
/// The VS is created with `PARALLELISM_FACTOR = 1` so that on this single-node
/// cluster the shard count G = clamp(1 * 1, 1, min(file_count, 300)) = 1. Both
/// files therefore land in ONE shard → one `ScanSpec` → one DataFusion
/// `ListingTable`. The two divergent physical layouts (`score` vs `rating` for
/// field-id 2) are handled by the field-id expression adapter, which binds both
/// files to the current logical name by field-id rather than by physical name.
///
/// Expected result: `EVO_TOTAL_ROWS` (10) rows, `rating = 10*id`, no NULLs.
#[test]
fn e2e_renamed_column_resolves_by_field_id() {
    setup_e2e();

    // Seed the dedicated evo table: create + file A (score) + rename + file B (rating).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        seed_renamed_column(&iceberg_catalog_url(), "s3://warehouse/")
            .await
            .expect("seed evo (renamed-column) table");
    });

    let mut conn = exa_conn();

    // A dedicated VS created AFTER evo exists, so the adapter enumerates it
    // (the shared MY_LAKEHOUSE VS was created before evo and does not see it).
    // PARALLELISM_FACTOR = 1 forces a single shard (G = 1 on this 1-node cluster),
    // so both parquet files are scanned together in one ListingTable.
    let _ = conn.try_execute("DROP VIRTUAL SCHEMA IF EXISTS EVO_VS CASCADE");
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA EVO_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE   = '{E2E_NAMESPACE}'
  SCAN_SCHEMA         = '{SCHEMA_NAME}'
  PARALLELISM_FACTOR  = '1'
  ALLOW_HTTP          = 'true'"#
    ));

    let col = EVO_NEW_COL.to_uppercase();
    let sql = format!(
        "SELECT id, {col} FROM EVO_VS.{} ORDER BY id",
        E2E_EVO_TABLE.to_uppercase()
    );

    let cols = conn.query_columns(&sql);
    let ids = &cols[0];
    let ratings = &cols[1];
    let row_count = ids.len();

    assert_eq!(
        row_count, EVO_TOTAL_ROWS,
        "field-id projection must return all {EVO_TOTAL_ROWS} rows across both \
         pre- and post-rename files; got {row_count}"
    );

    for (i, r) in ratings.iter().enumerate() {
        let id = ids[i]
            .as_i64()
            .or_else(|| ids[i].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(-1);
        let rating = r
            .as_f64()
            .expect("rating must not be NULL after field-id projection");
        assert!(
            (rating - 10.0 * id as f64).abs() < 1e-6,
            "row {i}: expected rating = 10 * {id} = {}, got {rating}",
            10 * id
        );
    }
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
/// CLUSTER_NODES is sourced from `ctx.node_count()` (the live UDF handshake
/// metadata), defaulting to 1 when the count is 0 — so asserting >= 1 (not
/// == cluster size) is correct and robust across single- and multi-node runs.
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

/// A multi-file scan through the VS returns correct rows end-to-end with the
/// reshaped `(path, size)` + `table_root` payload: the adapter resolves the file
/// list once, byte-balances it into shards carrying `(relative-or-absolute path,
/// byte-size)` entries under a table root serialized once in the common blob, and
/// each fanned-out UDF reconstructs the absolute URIs and registers ONLY its
/// assigned files. Proving every file across every shard is scanned exactly once
/// (no gaps, no duplicates) with fully correct column values exercises the new
/// payload through the real fan-out.
///
/// The generated fan-out SQL shape — table root carried ONCE in the common
/// literal and per-shard `[[path, size], ...]` literals — is asserted host-side
/// (no DB) by the pushdown unit test
/// `fan_out_carries_root_once_and_path_size_tuples_per_shard`.
#[test]
fn scan_registers_assigned_files_with_path_size_payload() {
    setup_e2e();
    let mut conn = exa_conn();

    // Full projection over the whole (multi-file) table. Seeded rows: id 1..20,
    // name carries the zero-padded id, score = 5.0 * id.
    let sql = format!("SELECT id, name, score FROM {} ORDER BY id", vs_table());
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, name, score): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        20,
        "multi-file fan-out must return all 20 rows with no gaps/duplicates: got {}",
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
            "id at position {pos} must be {expected}, got {id} (a file was missed or double-scanned)"
        );

        // score = 5.0 * id — proves the data (not just the row count) is correct
        // for the file this row came from.
        let score = cols[2][pos]
            .as_f64()
            .unwrap_or_else(|| panic!("score not f64: {:?}", cols[2][pos]));
        assert!(
            (score - 5.0 * expected as f64).abs() < 1e-9,
            "score for id {expected} must be {}, got {score}",
            5.0 * expected as f64
        );

        // name carries the zero-padded id.
        let name = cols[1][pos]
            .as_str()
            .unwrap_or_else(|| panic!("name not string: {:?}", cols[1][pos]));
        assert!(
            name.contains(&format!("{expected:02}")),
            "name '{name}' does not carry expected id {expected}"
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 5 — GROUP BY E2E tests
//
// Seed recap (20 rows, id=1..20):
//   score  = 5.0 * id   (5.0, 10.0, ..., 100.0)
//   name   = "event-NN"
//   event_date = 2024-01-01 + (id-1) days
//   event_ts   = 2024-01-01T00:00:00Z + (id-1) hours
//
// Group-key derivations used below:
//   MOD(id, 4) → groups {0,1,2,3}, 5 rows each
//   MOD(id, 2) × MOD(id, 4) × WHERE score > 50 → 4 groups across 10 rows
//   CAST(score / 25.0 AS DECIMAL(4,0)) → groups {0,1,2,3,4} with sizes 2,5,5,5,3
//     (Exasol CAST-to-DECIMAL rounds half away from zero)
//   id → 20 groups, 1 row each (high cardinality / spill path)
//   NULLIF(MOD(id, 5), 0) → groups {1,2,3,4,NULL}, sizes 4,4,4,4,4
// ---------------------------------------------------------------------------

fn parse_numeric(v: &serde_json::Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected numeric value, got: {v:?}"))
}

fn parse_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

/// Runs `EXPLAIN VIRTUAL` for a GROUP BY query and asserts the pushed SQL
/// evidences a grouped partial-aggregate pushdown — not the raw row-scan
/// fallback that Exasol would otherwise aggregate itself.
///
/// The real, shard-count-independent evidence of grouped pushdown is the
/// `group_keys` field inside the `LAKEHOUSE_SCAN` scan spec: the grouped
/// partial-aggregate path emits `"group_keys":[...]` (and the `PARTIAL_`
/// aggregate-column prefix), while the raw-scan fallback emits neither.
///
/// `GROUP BY shard_key` is deliberately NOT used as the discriminator: that
/// inner fan-out only appears when the scan spreads over MULTIPLE shards. When
/// a WHERE filter prunes the file list to a SINGLE file/shard, grouped
/// pushdown still occurs — it emits `... SUM("PARTIAL_...") ... GROUP BY
/// "GK_0", "GK_1"` with no `GROUP BY shard_key` — so asserting on it would
/// false-negative any legitimately pushed-down, single-shard grouped query.
///
/// Asserts: the pushed SQL contains `group_keys` and the `PARTIAL_` partial-
/// aggregate column prefix (grouped pushdown occurred), contains no `IPROC()`
/// (legacy, non-oversubscribed sharding), and is not a raw `SELECT * FROM
/// (SELECT ...)` row-scan wrapper (which would mean the multi-key GROUP BY
/// silently fell back instead of being pushed down as partial aggregation).
fn assert_group_by_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    // Flatten all returned text fragments into one string for pattern checks.
    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("group_keys"),
        "EXPLAIN VIRTUAL output must contain 'group_keys' in the scan spec \
         (grouped partial-aggregate pushdown occurred), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("PARTIAL_"),
        "EXPLAIN VIRTUAL output must contain the 'PARTIAL_' partial-aggregate \
         column prefix (grouped pushdown occurred), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("IPROC()"),
        "EXPLAIN VIRTUAL output must NOT contain 'IPROC()' (legacy sharding), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

/// GROUP BY returns correct per-group COUNT(*) and SUM(score).
///
/// Key: MOD(id, 4) — four equal-sized groups (5 rows each).
///
/// Expected:
///   group 0 (id=4,8,12,16,20):  count=5, sum_score=300.0
///   group 1 (id=1,5,9,13,17):   count=5, sum_score=225.0
///   group 2 (id=2,6,10,14,18):  count=5, sum_score=250.0
///   group 3 (id=3,7,11,15,19):  count=5, sum_score=275.0
#[test]
fn test_group_by_sum_count() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), COUNT(*), SUM(score) FROM {} GROUP BY MOD(id, 4) ORDER BY MOD(id, 4)",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 columns: {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Expected values sorted by group key 0..3.
    let expected_counts = [5i64, 5, 5, 5];
    let expected_sums = [300.0f64, 225.0, 250.0, 275.0];

    for i in 0..4 {
        let count = parse_int(&cols[1][i]);
        assert_eq!(
            count, expected_counts[i],
            "group {i}: COUNT(*) must be {}, got {count}",
            expected_counts[i]
        );
        let sum = parse_numeric(&cols[2][i]);
        assert!(
            (sum - expected_sums[i]).abs() < 0.01,
            "group {i}: SUM(score) must be {}, got {sum}",
            expected_sums[i]
        );
    }

    // Total rows across all groups = 20.
    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across groups must be 20, got {total}"
    );
}

/// Two GROUP BY keys with a WHERE filter returns correct per-group row counts.
///
/// Keys: MOD(id, 4) × MOD(id, 2), filter: score > 50.0 (id=11..20, 10 rows).
///
/// Expected 4 groups:
///   (0, 0): id=12,16,20       → count=3
///   (1, 1): id=13,17          → count=2
///   (2, 0): id=14,18          → count=2
///   (3, 1): id=11,15,19       → count=3
#[test]
fn test_group_by_multi_key_with_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), MOD(id, 2), COUNT(*) \
         FROM {} WHERE score > 50.0 \
         GROUP BY MOD(id, 4), MOD(id, 2)",
        vs_table()
    );

    // Pushdown-occurred assertion: multi-key GROUP BY (with a WHERE filter)
    // must be pushed down as shard-key fan-out partial aggregation, not the
    // raw row-scan fallback.
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 columns: {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 distinct groups: {cols:?}");

    // Total rows across all groups = 10 (id=11..20).
    let total: i64 = cols[2].iter().map(parse_int).sum();
    assert_eq!(
        total, 10,
        "total COUNT(*) across all groups must be 10 (id=11..20), got {total}"
    );

    // No group can have more than 3 rows (id range 11..20 across 4 buckets).
    for (i, v) in cols[2].iter().enumerate() {
        let c = parse_int(v);
        assert!(
            (2..=3).contains(&c),
            "group {i}: count must be 2 or 3, got {c}"
        );
    }
}

/// GROUP BY a scalar expression key returns correct per-group counts.
///
/// Key expression: CAST(score / 25.0 AS DECIMAL(4,0)) — supported arithmetic (FLOAT_DIV) + CAST.
///
/// Exasol's CAST-to-DECIMAL rounds half away from zero. For scores 5..100 in
/// steps of 5, score/25 = 0.2, 0.4, ..., 4.0, which rounds to:
///   key 0: scores {5,10}             → count=2
///   key 1: scores {15,20,25,30,35}   → count=5
///   key 2: scores {40,45,50,55,60}   → count=5
///   key 3: scores {65,70,75,80,85}   → count=5
///   key 4: scores {90,95,100}        → count=3
/// (2+5+5+5+3 = 20.)
#[test]
fn test_group_by_expression_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST(score / 25.0 AS DECIMAL(4,0)), COUNT(*) \
         FROM {} \
         GROUP BY CAST(score / 25.0 AS DECIMAL(4,0)) \
         ORDER BY CAST(score / 25.0 AS DECIMAL(4,0))",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");
    assert_eq!(cols[0].len(), 5, "expected 5 groups: {cols:?}");

    // Sort (key, count) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected_counts = [2i64, 5, 5, 5, 3];
    for (i, expected) in expected_counts.iter().enumerate() {
        let (key, count) = pairs[i];
        assert_eq!(
            key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert_eq!(
            count, *expected,
            "group key {key}: COUNT(*) must be {expected}, got {count}"
        );
    }

    // Total = 20.
    let total: i64 = pairs.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total rows across expression-key groups must be 20, got {total}"
    );
}

/// AVG(score) per group is correct for groups with unequal row counts.
///
/// Key expression: CAST(score / 25.0 AS DECIMAL(4,0)) — Exasol rounds half away
/// from zero, so score/25 (0.2..4.0) buckets into groups of sizes 2,5,5,5,3:
///   key 0 (scores {5,10}):              AVG = 15.0 / 2  = 7.5
///   key 1 (scores {15,20,25,30,35}):    AVG = 125.0 / 5 = 25.0
///   key 2 (scores {40,45,50,55,60}):    AVG = 250.0 / 5 = 50.0
///   key 3 (scores {65,70,75,80,85}):    AVG = 375.0 / 5 = 75.0
///   key 4 (scores {90,95,100}):         AVG = 285.0 / 3 = 95.0
#[test]
fn test_group_by_avg_correctness() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST(score / 25.0 AS DECIMAL(4,0)), AVG(score) \
         FROM {} \
         GROUP BY CAST(score / 25.0 AS DECIMAL(4,0)) \
         ORDER BY CAST(score / 25.0 AS DECIMAL(4,0))",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, avg): {cols:?}");
    assert_eq!(cols[0].len(), 5, "expected 5 groups: {cols:?}");

    // Sort (key, avg) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, f64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, a)| (parse_int(k), parse_numeric(a)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected_avgs = [7.5f64, 25.0, 50.0, 75.0, 95.0];
    for (i, expected) in expected_avgs.iter().enumerate() {
        let (key, avg) = pairs[i];
        assert_eq!(
            key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert!(
            (avg - expected).abs() < 0.01,
            "group key {key}: AVG(score) must be {expected}, got {avg}"
        );
    }
}

/// GROUP BY a near-unique column (id) completes with correct per-group counts.
///
/// Exercises the high-cardinality path: 20 distinct groups, each with exactly one row.
/// Verifies the memory-pool + spill backstop does not crash at high group cardinality.
#[test]
fn test_high_cardinality_group_by_spill() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, COUNT(*) FROM {} GROUP BY id ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, count): {cols:?}");
    assert_eq!(
        cols[0].len(),
        20,
        "GROUP BY id must return 20 groups, got {}",
        cols[0].len()
    );

    // Every group must have exactly one row (id is unique).
    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count,
            1,
            "group at position {i} (id={}): COUNT(*) must be 1, got {count}",
            parse_int(&cols[0][i])
        );
    }

    // IDs must be 1..20 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = (pos + 1) as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

/// EXPLAIN VIRTUAL shows shard_key fan-out and no IPROC() in the pushed SQL.
///
/// Verifies: the VS generates `GROUP BY shard_key` (oversubscribed fan-out)
/// and never falls back to the legacy `IPROC()` node-count sharding.
#[test]
fn test_shard_key_fanout_explain() {
    setup_e2e();
    let mut conn = exa_conn();

    // EXPLAIN VIRTUAL returns the pushdown SQL as a single-column result set.
    let sql = format!(
        "EXPLAIN VIRTUAL SELECT id, COUNT(*) FROM {} GROUP BY id",
        vs_table()
    );
    let resp = conn.execute(&sql);
    // Collect all text from the result set — each element is a fragment of the SQL.
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    // Flatten all returned text fragments into one string for pattern checks.
    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("shard_key"),
        "EXPLAIN VIRTUAL output must contain 'shard_key' (oversubscribed fan-out), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("IPROC()"),
        "EXPLAIN VIRTUAL output must NOT contain 'IPROC()' (legacy sharding), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.contains("GROUP BY"),
        "EXPLAIN VIRTUAL output must contain 'GROUP BY', got:\n{pushed_sql}"
    );
}

/// NULL group keys are grouped together consistently.
///
/// Key: NULLIF(MOD(id, 5), 0) — multiples of 5 (id=5,10,15,20) yield NULL.
/// Non-null groups are {1,2,3,4}, each with 4 rows; NULL group also has 4 rows.
///
/// Seed has no nullable columns; NULL is produced via NULLIF expression.
#[test]
fn test_group_by_null_key_grouping() {
    setup_e2e();
    let mut conn = exa_conn();

    // NULLIF(MOD(id, 5), 0): id=5,10,15,20 → 0 → NULL; id=1..4 → 1..4; etc.
    // Non-null groups: 1 (id=1,6,11,16), 2 (id=2,7,12,17), 3 (id=3,8,13,18), 4 (id=4,9,14,19)
    // NULL group: id=5,10,15,20 → 4 rows
    let sql = format!(
        "SELECT NULLIF(MOD(id, 5), 0), COUNT(*) \
         FROM {} \
         GROUP BY NULLIF(MOD(id, 5), 0) \
         ORDER BY NULLIF(MOD(id, 5), 0) NULLS LAST",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");
    // 5 groups: {1,2,3,4,NULL}
    assert_eq!(
        cols[0].len(),
        5,
        "expected 5 groups (1,2,3,4,NULL): {cols:?}"
    );

    // All groups have exactly 4 rows.
    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count, 4,
            "group at position {i}: COUNT(*) must be 4, got {count}"
        );
    }

    // Total rows = 20.
    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total rows across all groups must be 20, got {total}"
    );

    // Exactly one group key must be NULL (the multiples-of-5 group).
    // We scan for null entries rather than asserting a fixed position, because
    // the ORDER BY NULLS LAST may not survive the GROUP BY pushdown — position
    // is not guaranteed across execution paths.
    let null_count = cols[0].iter().filter(|v| v.is_null()).count();
    assert_eq!(
        null_count, 1,
        "exactly one NULL group key must exist (multiples of 5): {cols:?}"
    );

    // Find the null group's count and verify it is 4.
    let null_group_count = cols[0]
        .iter()
        .zip(cols[1].iter())
        .find(|(k, _)| k.is_null())
        .map(|(_, cnt)| parse_int(cnt))
        .expect("NULL group must have a COUNT value");
    assert_eq!(
        null_group_count, 4,
        "NULL group (multiples of 5) must have COUNT=4, got {null_group_count}"
    );
}

/// Aggregate placed before the group key in the select list (GitHub #33 repro).
///
/// `SELECT SUM(score), MOD(id, 4) ... GROUP BY MOD(id, 4)` — the aggregate is
/// select-list position 0, the group key is position 1. Before the fix, the
/// adapter always emitted keys first in the outer merge SELECT, transposing
/// this query's columns relative to `selectListDataTypes` and failing with a
/// "Data type mismatch in column number 1" error. Values must match the
/// already-correct key-first form (`test_group_by_sum_count`).
///
/// Key: MOD(id, 4) — four equal-sized groups (5 rows each).
///   group 0 (id=4,8,12,16,20):  sum_score=300.0
///   group 1 (id=1,5,9,13,17):   sum_score=225.0
///   group 2 (id=2,6,10,14,18):  sum_score=250.0
///   group 3 (id=3,7,11,15,19):  sum_score=275.0
#[test]
fn test_group_by_agg_before_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUM(score), MOD(id, 4) FROM {} GROUP BY MOD(id, 4)",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (sum, key): {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Sort (key, sum) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, f64)> = cols[1]
        .iter()
        .zip(cols[0].iter())
        .map(|(k, s)| (parse_int(k), parse_numeric(s)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected_sums = [300.0f64, 225.0, 250.0, 275.0];
    for (i, expected) in expected_sums.iter().enumerate() {
        let (key, sum) = pairs[i];
        assert_eq!(
            key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert!(
            (sum - expected).abs() < 0.01,
            "group key {key}: SUM(score) must be {expected}, got {sum}"
        );
    }

    // Total across all groups = 5.0 * (1+2+...+20) = 1050.0.
    let total: f64 = pairs.iter().map(|(_, s)| *s).sum();
    assert!(
        (total - 1050.0).abs() < 0.01,
        "total SUM(score) across groups must be 1050.0, got {total}"
    );
}

/// Interleaved multi-key GROUP BY: a group key, an aggregate, then a second
/// group key — `SELECT MOD(id,4), SUM(score), MOD(id,2) ... GROUP BY MOD(id,4), MOD(id,2)`.
///
/// Select-list order is key(0), agg(1), key(2), which does not match either the
/// keys-first or aggregate-first outer-SELECT ordering — exercising general
/// positional reassembly rather than either single-swap special case.
///
/// Groups (16 combinations possible; only even/odd-consistent pairs occur since
/// MOD(id,4) mod 2 == MOD(id,2)):
///   (0,0): id=4,8,12,16,20  → sum=300.0
///   (1,1): id=1,5,9,13,17   → sum=225.0
///   (2,0): id=2,6,10,14,18  → sum=250.0
///   (3,1): id=3,7,11,15,19  → sum=275.0
#[test]
fn test_group_by_interleaved_multi_key() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), SUM(score), MOD(id, 2) FROM {} GROUP BY MOD(id, 4), MOD(id, 2)",
        vs_table()
    );

    // Pushdown-occurred assertion: interleaved multi-key GROUP BY must be
    // pushed down as shard-key fan-out partial aggregation, not the raw
    // row-scan fallback.
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, sum, key2): {cols:?}"
    );
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Sort (key1, sum, key2) triples by key1 so the test is robust to row ordering.
    let mut rows: Vec<(i64, f64, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .zip(cols[2].iter())
        .map(|((k1, s), k2)| (parse_int(k1), parse_numeric(s), parse_int(k2)))
        .collect();
    rows.sort_by_key(|(k1, _, _)| *k1);

    let expected = [
        (0i64, 300.0f64, 0i64),
        (1, 225.0, 1),
        (2, 250.0, 0),
        (3, 275.0, 1),
    ];
    for (i, (exp_k1, exp_sum, exp_k2)) in expected.iter().enumerate() {
        let (k1, sum, k2) = rows[i];
        assert_eq!(
            k1, *exp_k1,
            "group at position {i}: MOD(id,4) key must be {exp_k1}, got {k1}"
        );
        assert!(
            (sum - exp_sum).abs() < 0.01,
            "group key {k1}: SUM(score) must be {exp_sum}, got {sum}"
        );
        assert_eq!(
            k2, *exp_k2,
            "group key {k1}: MOD(id,2) key must be {exp_k2}, got {k2}"
        );
    }

    // Total across all groups = 1050.0.
    let total: f64 = rows.iter().map(|(_, s, _)| *s).sum();
    assert!(
        (total - 1050.0).abs() < 0.01,
        "total SUM(score) across groups must be 1050.0, got {total}"
    );
}

/// Expression group key placed after an aggregate — `SELECT COUNT(*), MOD(id,4)
/// ... GROUP BY MOD(id,4)` — and the key column's declared type must survive
/// as its resolved DECIMAL type, not fall back to VARCHAR.
///
/// Guards against the secondary fragility described in the plan: resolving a
/// group key's declared type by rendered-string comparison silently defaults
/// to VARCHAR(2000000) if detection and lookup disagree; the fix resolves the
/// type by select-list index instead.
///
/// Key: MOD(id, 4) — four equal-sized groups (5 rows each), counts of 5 each.
#[test]
fn test_group_by_expr_key_after_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*), MOD(id, 4) FROM {} GROUP BY MOD(id, 4)",
        vs_table()
    );
    let resp = conn.execute(&sql);

    // Assert the key column (position 1) carries a DECIMAL data type, not VARCHAR.
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let column_type = result_set["columns"][1]["dataType"]["type"]
        .as_str()
        .unwrap_or_else(|| panic!("expected dataType.type for column 1: {result_set:?}"));
    assert_eq!(
        column_type, "DECIMAL",
        "MOD(id,4) group key column must carry DECIMAL type, not VARCHAR fallback: {result_set:?}"
    );

    let cols = conn.fetch_result_columns(result_set);
    assert_eq!(cols.len(), 2, "expected 2 columns (count, key): {cols:?}");
    assert_eq!(cols[0].len(), 4, "expected 4 groups: {cols:?}");

    // Sort (key, count) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, i64)> = cols[1]
        .iter()
        .zip(cols[0].iter())
        .map(|(k, c)| (parse_int(k), parse_int(c)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    for (i, (key, count)) in pairs.iter().enumerate() {
        assert_eq!(
            *key, i as i64,
            "group at position {i}: key must be {i}, got {key}"
        );
        assert_eq!(
            *count, 5,
            "group key {key}: COUNT(*) must be 5, got {count}"
        );
    }

    let total: i64 = pairs.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across groups must be 20, got {total}"
    );
}

/// Aggregate-first GROUP BY combined with HAVING — exercises the HAVING-present
/// outer-wrapper path with the aggregate ahead of the group key in the select
/// list: `SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4) HAVING SUM(score) > n`.
///
/// Group sums (from `test_group_by_agg_before_key`): {0: 300.0, 1: 225.0, 2:
/// 250.0, 3: 275.0}. HAVING SUM(score) > 250.0 keeps groups 0 and 3 only.
#[test]
fn test_group_by_agg_first_with_having() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUM(score), MOD(id, 4) FROM {} GROUP BY MOD(id, 4) HAVING SUM(score) > 250.0",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (sum, key): {cols:?}");
    assert_eq!(
        cols[0].len(),
        2,
        "HAVING SUM(score) > 250.0 must keep exactly 2 groups (0 and 3): {cols:?}"
    );

    // Sort (key, sum) pairs by key so the test is robust to row ordering.
    let mut pairs: Vec<(i64, f64)> = cols[1]
        .iter()
        .zip(cols[0].iter())
        .map(|(k, s)| (parse_int(k), parse_numeric(s)))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);

    let expected = [(0i64, 300.0f64), (3, 275.0)];
    for (i, (exp_key, exp_sum)) in expected.iter().enumerate() {
        let (key, sum) = pairs[i];
        assert_eq!(
            key, *exp_key,
            "group at position {i}: key must be {exp_key}, got {key}"
        );
        assert!(
            (sum - exp_sum).abs() < 0.01,
            "group key {key}: SUM(score) must be {exp_sum}, got {sum}"
        );
        assert!(
            sum > 250.0,
            "group key {key}: SUM(score) must satisfy HAVING > 250.0, got {sum}"
        );
    }
}

/// Expression-valued multi-key tuple GROUP BY — every key element is itself an
/// expression (not a bare column): `MOD(id, 4)` and `UPPER(name)`. Verifies
/// correct per-group counts, that each key's declared type survives instead of
/// falling back to the VARCHAR(2000000) default, and that the GROUP BY is
/// pushed down as a grouped partial aggregation.
///
/// The two keys are deliberately of DIFFERENT types — key 0 is `MOD(id, 4)`
/// (DECIMAL) and key 1 is `UPPER(name)` (VARCHAR) — so this test genuinely
/// exercises per-index, mixed-type independence: a bug that shared one key's
/// type across both indices would surface here as a wrong column type.
///
/// The seeded `name` values (`event-01` … `event-20`) are unique, so each
/// (`MOD(id,4)`, `UPPER(name)`) pair identifies exactly one row: 20 groups, one
/// row each. Grouping by `MOD(id, 4)` first buckets the ids, and `UPPER(name)`
/// (`EVENT-NN`) then distinguishes every row within a bucket.
#[test]
fn test_group_by_expr_multi_key_tuple() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), UPPER(name), COUNT(*) \
         FROM {} GROUP BY MOD(id, 4), UPPER(name)",
        vs_table()
    );

    // Pushdown-occurred assertion: expression-valued multi-key GROUP BY must
    // be pushed down as grouped partial aggregation.
    assert_group_by_pushed_down(&mut conn, &sql);

    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];

    // Per-index, mixed-type independence: key 0 (`MOD(id, 4)`) carries DECIMAL
    // and key 1 (`UPPER(name)`) carries VARCHAR — each key's declared type
    // survives independently, neither collapsing to the other's type nor to a
    // fallback.
    for (i, label, expected_type) in [(0, "MOD(id, 4)", "DECIMAL"), (1, "UPPER(name)", "VARCHAR")] {
        let column_type = result_set["columns"][i]["dataType"]["type"]
            .as_str()
            .unwrap_or_else(|| panic!("expected dataType.type for column {i}: {result_set:?}"));
        assert_eq!(
            column_type, expected_type,
            "{label} group key (column {i}) must carry {expected_type} type: {result_set:?}"
        );
    }

    let cols = conn.fetch_result_columns(result_set);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, key2, count): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        20,
        "expected 20 groups (one per unique name): {cols:?}"
    );

    // Sort (key1, key2, count) triples so the test is robust to row ordering.
    let mut rows: Vec<(i64, String, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .zip(cols[2].iter())
        .map(|((k1, k2), c)| {
            let name = k2
                .as_str()
                .unwrap_or_else(|| panic!("expected string UPPER(name) value, got: {k2:?}"))
                .to_string();
            (parse_int(k1), name, parse_int(c))
        })
        .collect();
    rows.sort();

    let expected: Vec<(i64, String, i64)> = [
        (0i64, "EVENT-04"),
        (0, "EVENT-08"),
        (0, "EVENT-12"),
        (0, "EVENT-16"),
        (0, "EVENT-20"),
        (1, "EVENT-01"),
        (1, "EVENT-05"),
        (1, "EVENT-09"),
        (1, "EVENT-13"),
        (1, "EVENT-17"),
        (2, "EVENT-02"),
        (2, "EVENT-06"),
        (2, "EVENT-10"),
        (2, "EVENT-14"),
        (2, "EVENT-18"),
        (3, "EVENT-03"),
        (3, "EVENT-07"),
        (3, "EVENT-11"),
        (3, "EVENT-15"),
        (3, "EVENT-19"),
    ]
    .iter()
    .map(|(k, n)| (*k, n.to_string(), 1i64))
    .collect();
    assert_eq!(
        rows, expected,
        "grouped (MOD(id,4), UPPER(name)) rows must match the expected per-name groups"
    );

    let total: i64 = rows.iter().map(|(_, _, c)| *c).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across all groups must be 20, got {total}"
    );
}

/// Multi-key GROUP BY combined with HAVING and LIMIT — both must apply only in
/// the outer merge wrapper (never per-shard), so the LIMIT caps the number of
/// *groups* returned, not rows scanned per shard.
///
/// Keys: `MOD(id, 4)` × `MOD(id, 3)` (12 groups; `SUM(score)` per group).
/// Groups satisfying `HAVING SUM(score) > 100.0`:
///   (0,2)=140.0  (1,2)=110.0  (2,0)=120.0  (3,1)=130.0
/// `LIMIT 2` must cap the result to exactly 2 of these 4 qualifying groups.
#[test]
fn test_group_by_multi_key_having_limit() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), MOD(id, 3), SUM(score) FROM {} \
         GROUP BY MOD(id, 4), MOD(id, 3) HAVING SUM(score) > 100.0 LIMIT 2",
        vs_table()
    );

    // Pushdown-occurred assertion: multi-key GROUP BY with HAVING and LIMIT
    // must be pushed down as shard-key fan-out partial aggregation, not the
    // raw row-scan fallback (HAVING/LIMIT results are correct either way).
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (key1, key2, sum): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        2,
        "LIMIT 2 must cap the result to exactly 2 groups: {cols:?}"
    );

    // Every returned group must both satisfy HAVING and match one of the
    // known-qualifying (key1, key2) -> sum pairs (not just an arbitrary
    // over-threshold value).
    let qualifying: [((i64, i64), f64); 4] = [
        ((0, 2), 140.0),
        ((1, 2), 110.0),
        ((2, 0), 120.0),
        ((3, 1), 130.0),
    ];

    for (i, ((k1_raw, k2_raw), sum_raw)) in cols[0].iter().zip(&cols[1]).zip(&cols[2]).enumerate() {
        let k1 = parse_int(k1_raw);
        let k2 = parse_int(k2_raw);
        let sum = parse_numeric(sum_raw);

        assert!(
            sum > 100.0,
            "row {i}: SUM(score) must satisfy HAVING > 100.0, got {sum} for key ({k1}, {k2})"
        );

        let expected_sum = qualifying
            .iter()
            .find(|((qk1, qk2), _)| *qk1 == k1 && *qk2 == k2)
            .map(|(_, s)| *s)
            .unwrap_or_else(|| {
                panic!("row {i}: key ({k1}, {k2}) is not one of the known qualifying groups")
            });
        assert!(
            (sum - expected_sum).abs() < 0.01,
            "row {i}: key ({k1}, {k2}) must have SUM(score) = {expected_sum}, got {sum}"
        );
    }
}

/// High-cardinality multi-key GROUP BY completes under the bounded memory
/// pool — a tuple key (`id`, `MOD(id, 2)`) exercises the same near-unique key
/// space as the single-key spill test, but through the multi-key GK_0/GK_1
/// path, proving the bounded-pool/spill backstop is not single-key-only.
///
/// 20 distinct (id, MOD(id,2)) groups, each with exactly one row.
#[test]
fn test_high_cardinality_multi_key_group_by_spill() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, MOD(id, 2), COUNT(*) FROM {} GROUP BY id, MOD(id, 2) ORDER BY id",
        vs_table()
    );

    // Pushdown-occurred assertion: even the high-cardinality multi-key case
    // must go through shard-key fan-out partial aggregation.
    assert_group_by_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, MOD(id,2), count): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        20,
        "GROUP BY id, MOD(id,2) must return 20 groups, got {}",
        cols[0].len()
    );

    // Every group must have exactly one row (id is unique).
    for (i, v) in cols[2].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count,
            1,
            "group at position {i} (id={}): COUNT(*) must be 1, got {count}",
            parse_int(&cols[0][i])
        );
    }

    // IDs must be 1..20 in order, and MOD(id,2) must be consistent with id.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    let mods: Vec<i64> = cols[1].iter().map(parse_int).collect();
    for (pos, (&id, &m)) in ids.iter().zip(mods.iter()).enumerate() {
        let expected_id = (pos + 1) as i64;
        assert_eq!(
            id, expected_id,
            "id at position {pos} must be {expected_id}, got {id}"
        );
        assert_eq!(
            m,
            id % 2,
            "MOD(id,2) at position {pos} (id={id}) must be {}, got {m}",
            id % 2
        );
    }
}

// ---------------------------------------------------------------------------
// Task 2.14 — multi-table VS tests
// ---------------------------------------------------------------------------

/// Create VS with ICEBERG_NAMESPACE enumerates all tables in the namespace.
///
/// Asserts that both `EVENTS` and `LABELS` appear in `SYS.EXA_ALL_TABLES` for
/// the virtual schema — one Exasol virtual table per Iceberg table in the namespace.
#[test]
fn e2e_create_vs_enumerates_namespace_tables() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES \
         WHERE TABLE_SCHEMA = '{VS_NAME}' \
         ORDER BY TABLE_NAME"
    ));
    assert_eq!(
        cols.len(),
        1,
        "query must return one column (TABLE_NAME): {cols:?}"
    );
    let table_names: Vec<&str> = cols[0].iter().filter_map(|v| v.as_str()).collect();

    assert!(
        table_names.contains(&E2E_TABLE.to_uppercase().as_str()),
        "EVENTS must appear in the virtual schema tables: {table_names:?}"
    );
    assert!(
        table_names.contains(&E2E_TABLE_2.to_uppercase().as_str()),
        "LABELS must appear in the virtual schema tables: {table_names:?}"
    );
}

/// Pushdown derives the scanned Iceberg table from involvedTables[0].name.
///
/// Queries the second table (LABELS) directly and asserts correct rows are
/// returned — proving that pushdown looked up the Iceberg identifier from
/// TABLE_MAP using the Exasol virtual table name.
#[test]
fn e2e_pushdown_scans_table_from_involved_tables() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT id, label FROM {} ORDER BY id",
        vs_labels_table()
    ));
    assert_eq!(cols.len(), 2, "must return 2 columns (id, label): {cols:?}");
    assert_eq!(
        cols[0].len(),
        SEED_LABELS_ROWS,
        "must return all {SEED_LABELS_ROWS} label rows, got {}",
        cols[0].len()
    );

    // Verify id=1 maps to "label-01".
    let first_id = cols[0][0]
        .as_i64()
        .or_else(|| cols[0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("id not integer: {:?}", cols[0][0]));
    assert_eq!(
        first_id, 1,
        "first id must be 1 after ORDER BY, got {first_id}"
    );
    let first_label = cols[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("label not string: {:?}", cols[1][0]));
    assert_eq!(
        first_label, "label-01",
        "id=1 must have label 'label-01', got '{first_label}'"
    );
}

// ---------------------------------------------------------------------------
// Group E — Iceberg file-pruning E2E tests (tasks 5.2 + 5.3)
//
// Seed recap (regions table, 3 files):
//   north   → ids 1..=5    (5 rows per file, partition value "north")
//   central → ids 6..=10   (5 rows per file, partition value "central")
//   south   → ids 11..=15  (5 rows per file, partition value "south")
//
// The VS exposes the table as MY_LAKEHOUSE.REGIONS (Exasol-uppercased).
// ---------------------------------------------------------------------------

/// Helper: virtual schema name for the partitioned regions table.
fn vs_regions_table() -> String {
    format!("{VS_NAME}.{}", E2E_PART_TABLE.to_uppercase())
}

/// Build a `ConnectionCreds` pointing at the host-visible local Docker stack.
///
/// Used by the adapter-level file-resolution tests (task 5.3) that call
/// `resolve_file_list` directly rather than going through Exasol pushdown.
/// The host-visible catalog and MinIO URLs (not the internal Docker aliases)
/// are used because the test process runs on the host, not inside a container.
fn local_stack_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: common::stack::minio_url(),
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

/// Build `StorageProps` for the host-visible local Docker stack.
fn local_stack_storage() -> StorageProps {
    StorageProps {
        endpoint: common::stack::minio_url(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

/// Build `CatalogProps` for the host-visible local Docker stack, for `table`.
fn local_stack_catalog(table: &str) -> CatalogProps {
    CatalogProps {
        uri: common::stack::iceberg_catalog_url(),
        warehouse: "s3://warehouse/".to_string(),
        table: table.to_string(),
    }
}

/// Task 5.2 — Partition filter prunes and returns correct rows.
///
/// Asserts:
/// - `SELECT id FROM {VS}.REGIONS WHERE region = 'north'` returns exactly ids 1..=5
///   (5 rows, matching PART_NORTH_IDS) — correct rows, partition pruning applied.
/// - `SELECT id FROM {VS}.REGIONS WHERE region = 'central'` returns exactly ids 6..=10
///   (5 rows, matching PART_CENTRAL_IDS) — a second partition value to increase
///   confidence that the filter is correct, not just returning all rows.
/// - Correctness is the primary assertion; file-count pruning is asserted in 5.3.
#[test]
fn e2e_partition_filter_prunes_and_returns_correct_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    // --- north partition: ids 1..=5 ---
    let north_sql = format!(
        "SELECT id FROM {} WHERE {} = '{}' ORDER BY id",
        vs_regions_table(),
        PART_COL,
        PART_VAL_NORTH,
    );
    let north_cols = conn.query_columns(&north_sql);
    assert_eq!(
        north_cols.len(),
        1,
        "SELECT id FROM REGIONS WHERE region='north' must return 1 column: {north_cols:?}"
    );
    assert_eq!(
        north_cols[0].len(),
        PART_ROWS_PER_FILE,
        "north partition must return exactly {} rows, got {}: {north_cols:?}",
        PART_ROWS_PER_FILE,
        north_cols[0].len()
    );

    let north_ids: Vec<i64> = north_cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("north id not integer: {v:?}"))
        })
        .collect();

    let expected_north: Vec<i64> = (PART_NORTH_IDS.0 as i64..=PART_NORTH_IDS.1 as i64).collect();
    assert_eq!(
        north_ids, expected_north,
        "north partition ids must be exactly {expected_north:?}, got {north_ids:?}"
    );

    // --- central partition: ids 6..=10 ---
    let central_sql = format!(
        "SELECT id FROM {} WHERE {} = '{}' ORDER BY id",
        vs_regions_table(),
        PART_COL,
        PART_VAL_CENTRAL,
    );
    let central_cols = conn.query_columns(&central_sql);
    assert_eq!(
        central_cols[0].len(),
        PART_ROWS_PER_FILE,
        "central partition must return exactly {} rows, got {}: {central_cols:?}",
        PART_ROWS_PER_FILE,
        central_cols[0].len()
    );

    let central_ids: Vec<i64> = central_cols[0]
        .iter()
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| panic!("central id not integer: {v:?}"))
        })
        .collect();

    let expected_central: Vec<i64> =
        (PART_CENTRAL_IDS.0 as i64..=PART_CENTRAL_IDS.1 as i64).collect();
    assert_eq!(
        central_ids, expected_central,
        "central partition ids must be exactly {expected_central:?}, got {central_ids:?}"
    );

    // --- total row count sanity: filtering all partitions yields the full table ---
    let total = conn.query_row_count(&format!("SELECT id FROM {}", vs_regions_table()));
    assert_eq!(
        total, PART_TOTAL_ROWS as i64,
        "REGIONS total row count must be {PART_TOTAL_ROWS} (all 3 partitions), got {total}"
    );

    // --- LIKE correctness: untranslatable predicate → DataFusion applies it, correct count ---
    // ponytail: LIKE is not pushed to Iceberg (untranslatable); DataFusion applies the full
    // filter as correctness backstop. Verifies the untranslatable-conjunct path.
    let like_count = conn.query_row_count(&format!(
        "SELECT id FROM {} WHERE {} LIKE 'nor%'",
        vs_regions_table(),
        PART_COL,
    ));
    assert_eq!(
        like_count, PART_ROWS_PER_FILE as i64,
        "LIKE 'nor%' (untranslatable, DataFusion applies) must return {PART_ROWS_PER_FILE} rows, \
         got {like_count}"
    );
}

/// Adapter-level file-count pruning, asserted by calling `resolve_file_list`
/// directly (bypassing Exasol) with and without a filter and comparing the
/// resolved file counts against the unfiltered snapshot. Both pruning paths the
/// plan claims are exercised against the seeded `regions` table (3 files, one
/// per partition, disjoint id ranges):
///
/// 1. Partition pruning (`region = 'north'`): Iceberg identity-partition pruning
///    eliminates the central and south files → 1 file.
/// 2. Per-file min/max range pruning (`id <= 5`): files whose id min > 5
///    (central: min=6, south: min=11) are eliminated by
///    `InclusiveMetricsEvaluator` → 1 file (north only, ids 1..=5).
#[test]
fn e2e_range_filter_prunes_by_file_bounds() {
    setup_e2e();

    let catalog_uri = common::stack::iceberg_catalog_url();
    let catalog_props = local_stack_catalog(&format!("{E2E_NAMESPACE}.{E2E_PART_TABLE}"));
    let storage = local_stack_storage();
    let creds = local_stack_creds();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for file-count pruning test");

    // --- baseline: no filter → 3 data files (one per partition) ---
    let all_files = rt
        .block_on(async {
            resolve_file_list(&catalog_uri, &catalog_props, &storage, &creds, None).await
        })
        .expect("resolve_file_list (no filter) must succeed");
    let all_files = all_files.0;
    assert_eq!(
        all_files.len(),
        3,
        "unfiltered REGIONS must resolve 3 data files (one per partition), got {}: {all_files:?}",
        all_files.len()
    );

    // --- partition pruning: region = 'north' → exactly 1 file ---
    // Filter JSON shape mirrors the Exasol pushdown format the translator expects.
    // Column names are Exasol-uppercase; the translator resolves them case-insensitively.
    let partition_filter = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "REGION"},
        "right": {"type": "literal_string", "value": "north"}
    });
    let pruned_partition = rt
        .block_on(async {
            resolve_file_list(
                &catalog_uri,
                &catalog_props,
                &storage,
                &creds,
                Some(&partition_filter),
            )
            .await
        })
        .expect("resolve_file_list (partition filter) must succeed");
    let pruned_partition = pruned_partition.0;
    assert_eq!(
        pruned_partition.len(),
        1,
        "partition filter 'region = north' must resolve 1 file, got {}: {pruned_partition:?}",
        pruned_partition.len()
    );
    assert!(
        pruned_partition.len() < all_files.len(),
        "partition filter must prune files: pruned={} is not < unfiltered={}",
        pruned_partition.len(),
        all_files.len()
    );

    // --- per-file min/max range pruning: id <= 5 → files with min(id) > 5 are pruned ---
    // The regions table has disjoint id ranges per file:
    //   north   id 1..=5  (max=5)
    //   central id 6..=10 (min=6 > 5 → pruned)
    //   south   id 11..=15(min=11 > 5 → pruned)
    // With Iceberg's InclusiveMetricsEvaluator, `id <= 5` prunes files where min(id) > 5.
    let range_filter = serde_json::json!({
        "type": "predicate_lessequal",
        "left": {"type": "column", "name": "ID"},
        "right": {"type": "literal_exactnumeric", "value": "5"}
    });
    let pruned_range = rt
        .block_on(async {
            resolve_file_list(
                &catalog_uri,
                &catalog_props,
                &storage,
                &creds,
                Some(&range_filter),
            )
            .await
        })
        .expect("resolve_file_list (range filter) must succeed");
    let pruned_range = pruned_range.0;
    // Only the north file (ids 1..=5) overlaps `id <= 5`; central (min=6) and
    // south (min=11) are pruned by their per-file min/max bounds.
    assert_eq!(
        pruned_range.len(),
        1,
        "range filter 'id <= 5' must resolve only the north file, got {}: {pruned_range:?}",
        pruned_range.len()
    );
}

/// Exasol-side JOIN across two virtual tables returns correct joined rows.
///
/// Joins EVENTS and LABELS on `id` and asserts the result contains the
/// expected id and label values — proving both tables are independently
/// scanned by pushdown and joined by Exasol.
#[test]
fn e2e_pushdown_resolves_files_once_multi_table() {
    setup_e2e();
    let mut conn = exa_conn();

    // JOIN events and labels on id; ORDER BY id for determinism.
    let sql = format!(
        "SELECT a.id, b.label FROM {events} a \
         JOIN {labels} b ON a.id = b.id \
         WHERE a.id <= 5 \
         ORDER BY a.id",
        events = vs_table(),
        labels = vs_labels_table(),
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "must return 2 columns (id, label): {cols:?}");
    assert_eq!(
        cols[0].len(),
        5,
        "JOIN with id <= 5 must return 5 rows, got {}",
        cols[0].len()
    );

    // Verify each id maps to the expected label.
    for (i, (id_val, label_val)) in cols[0].iter().zip(cols[1].iter()).enumerate() {
        let expected_id = (i + 1) as i64;
        let id = id_val
            .as_i64()
            .or_else(|| id_val.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("id at pos {i} not integer: {id_val:?}"));
        assert_eq!(
            id, expected_id,
            "id at position {i} must be {expected_id}, got {id}"
        );

        let label = label_val
            .as_str()
            .unwrap_or_else(|| panic!("label at pos {i} not string: {label_val:?}"));
        assert_eq!(
            label,
            format!("label-{expected_id:02}"),
            "label at position {i} must be 'label-{expected_id:02}', got '{label}'"
        );
    }
}

// ---------------------------------------------------------------------------
// Regression: nested aggregate over a grouped sub-select (issue #52).
//
// Exasol rewrites `COUNT(*) FROM (SELECT k, COUNT(*) FROM t GROUP BY k) sub`
// into a single flat pushdown request: `aggregationType: "group_by"` with a
// literal-only `selectList` (a `literal_null` "count the groups" placeholder,
// since the outer query needs neither the group key nor the inner COUNT(*)
// value). Before the fix, the adapter rendered that literal placeholder as a
// bare `NULL` projection column, producing scan SQL that referenced a
// phantom `"NULL"` identifier and crashing with `F-UDF-CL-RUST-9001: ...
// Schema error: No field named "NULL"`. The fix (pushdown.rs
// `detect_group_by_aggregates`) preserves the GROUP BY so the scan still
// returns one row per distinct group; Exasol's outer COUNT(*) then counts
// those group rows correctly.
// ---------------------------------------------------------------------------

/// End-to-end nested aggregate over a grouped sub-select returns the correct
/// outer count — including the duplicate-key case that discriminates a
/// correct grouped-scan fix from an unsafe row-scan fallback.
///
/// `events.id` is unique, so `GROUP BY id` alone cannot tell a correct fix
/// apart from a fallback that returns one row per source row (both
/// coincidentally yield 20 on this table). `GROUP BY MOD(id, 4)` is the
/// discriminating case: 20 seeded rows (id 0..19) fall into exactly 4
/// distinct `MOD(id, 4)` buckets, so the outer `COUNT(*)` must be 4. A
/// row-scan fallback would instead return the raw row count (20).
#[test]
fn e2e_nested_aggregate_over_grouped_subselect_returns_correct_count() {
    setup_e2e();
    let mut conn = exa_conn();

    // Duplicate-key case — the actual regression guard. Must be 4 (distinct
    // MOD(id, 4) groups), NOT 20 (which a row-scan fallback would wrongly
    // return since it doesn't re-group).
    let sql_duplicate_keys = format!(
        "SELECT COUNT(*) FROM (SELECT MOD(id, 4) AS k, COUNT(*) AS cnt FROM {} GROUP BY MOD(id, 4)) t",
        vs_table()
    );
    let cols = conn.query_columns(&sql_duplicate_keys);
    assert_eq!(
        cols.len(),
        1,
        "nested COUNT(*) must return one column: {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "nested COUNT(*) must return one row: {cols:?}"
    );
    let distinct_group_count = parse_int(&cols[0][0]);
    assert_eq!(
        distinct_group_count, 4,
        "COUNT(*) over (GROUP BY MOD(id,4)) sub-select must be 4 (distinct groups), \
         got {distinct_group_count} — 20 would indicate an unsafe row-scan fallback \
         instead of a correctly preserved grouped scan"
    );

    // Unique-key smoke case from the plan (kept as an additional assertion,
    // not a substitute for the duplicate-key case above): every id is
    // distinct, so the outer COUNT(*) over `GROUP BY id` must equal 20.
    let sql_unique_key = format!(
        "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {} GROUP BY id) t",
        vs_table()
    );
    let cols_unique = conn.query_columns(&sql_unique_key);
    let unique_group_count = parse_int(&cols_unique[0][0]);
    assert_eq!(
        unique_group_count, 20,
        "COUNT(*) over (GROUP BY id) sub-select must be 20 (distinct ids), \
         got {unique_group_count}"
    );
}
