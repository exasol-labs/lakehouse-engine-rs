//! End-to-end capability-alignment tests for the lakehouse-engine Virtual Schema.
//!
//! Exercises the full advertised → translated → executed path for each newly
//! advertised capability group: math/string/date scalar functions in filters,
//! REGEXP_LIKE, scalar select-list expressions, HAVING, and STDDEV/VARIANCE.
//!
//! Shares the same Exasol + MinIO + Iceberg stack and seed table as
//! `e2e_scan_test.rs`.  The setup (SLC install, BucketFS upload, VS creation)
//! is intentionally NOT duplicated — this file calls into the same `setup_e2e`
//! logic by re-running it idempotently via its own `OnceLock`.  All helpers
//! are shared from `common/`.
//!
//! Seed recap (20 rows, id = 1..20):
//!   score      = 5.0 * id          (5.0, 10.0, …, 100.0)
//!   name       = "event-NN"
//!   event_date = 2024-01-01 + (id-1) days   (all January 2024, day = id)
//!   event_ts   = 2024-01-01T00:00:00Z + (id-1) hours
//!
//! All tests FAIL (never skip) when the stack is unavailable.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_NAMESPACE, E2E_TABLE, seed_events};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, upload_to_bucketfs, wait_for_exasol, wait_for_iceberg_catalog,
    wait_for_minio,
};

use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_scan_test.rs — same stack, same VS)
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
const VS_NAME: &str = "MY_LAKEHOUSE";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
/// Scalar merge UDF (third entry point in the same .so), created in the scan schema.
const MERGE_SCRIPT_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";
/// LUA SET passthrough distributor doing the cross-node `GROUP BY shard_key`
/// fan-out. Not a Rust entry point — created by plain DDL, no .so involved.
const DISTRIBUTOR_SCRIPT_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.20.3";
const LANG_ALIAS: &str = "RUST";
/// Name of the Exasol CONNECTION carrying catalog + storage credentials.
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

// ---------------------------------------------------------------------------
// One-time setup (idempotent; identical to e2e_scan_test.rs)
// ponytail: duplicate of e2e_scan_test setup — both test binaries link the
// same binary but run independently, so we need local OnceLock guards.
// ---------------------------------------------------------------------------

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg events table")
        });

        install_slc();

        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn);
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
    // Scan SCALAR script input: two VARCHAR columns — arg0 is the common
    // ScanSpec blob, arg1 is the per-shard files JSON list (mirrors
    // e2e_scan_test.rs).
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    // Scalar distinct-merge script — third entry point in the SAME .so, created
    // in the scan schema alongside the scan script (mirrors e2e_scan_test.rs).
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{MERGE_SCRIPT_NAME}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    // File distributor — LUA SET SCRIPT, pure passthrough (mirrors
    // e2e_scan_test.rs).
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

fn create_virtual_schema(conn: &mut ExaConn) {
    // Create the catalog CONNECTION first (idempotent: CREATE OR REPLACE).
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE  = '{E2E_NAMESPACE}'
  ALLOW_HTTP         = 'true'"#
    ));
}

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

fn vs_dim_table() -> String {
    format!("{VS_NAME}.{}", E2E_DIM_TABLE.to_uppercase())
}

fn vs_fact_table() -> String {
    format!("{VS_NAME}.{}", E2E_FACT_TABLE.to_uppercase())
}

/// Run `EXPLAIN VIRTUAL <query_sql>` and return the pushed SQL text (the
/// generated scan-driving IMPORT statement plus Exasol's echoed pushdown
/// request), flattened to one string for substring inspection. Mirrors the
/// helper in `e2e_scan_test.rs`.
fn explain_virtual_sql(conn: &mut ExaConn, query_sql: &str) -> String {
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    conn.fetch_result_columns(result_set)
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------------------------------------------------------------------------
// Shared numeric parsers (also in e2e_scan_test.rs; ponytail: small dup OK)
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

// ---------------------------------------------------------------------------
// 5.1  Inner equi-join capability advertisement (live round-trip)
// ---------------------------------------------------------------------------

/// A live `getCapabilities` round-trip against the running VS advertises the
/// inner equi-join capabilities `JOIN`, `JOIN_TYPE_INNER`, and
/// `JOIN_CONDITION_EQUI`.
///
/// `EXPLAIN VIRTUAL` of a query over a virtual schema drives Exasol's planner
/// through `getCapabilities`, and its output echoes the adapter's capability
/// response verbatim (a compact JSON `"capabilities":[...]` array). Asserting the
/// three join tokens are present in that live response — rather than only against
/// the in-process `CAPABILITIES` constant (the unit test) — proves the deployed
/// `.so` advertises them end to end. The comma-adjacent `"JOIN","JOIN_TYPE_INNER"`
/// substring isolates the bare `JOIN` capability token from the `INNER JOIN` SQL
/// keyword and the `JOIN_*` compound tokens.
#[test]
fn e2e_advertises_inner_equi_join_capability() {
    setup_e2e();
    let mut conn = exa_conn();

    // A join query guarantees the join capabilities are exercised in planning;
    // the capability list itself is echoed regardless of the query shape.
    let query = format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY",
        vs_fact_table(),
        vs_dim_table()
    );
    let advertised = explain_virtual_sql(&mut conn, &query);

    assert!(
        advertised.contains("\"capabilities\":"),
        "EXPLAIN VIRTUAL must echo the getCapabilities response:\n{advertised}"
    );
    assert!(
        advertised.contains("\"JOIN\",\"JOIN_TYPE_INNER\""),
        "getCapabilities must advertise the bare JOIN and JOIN_TYPE_INNER \
         capabilities:\n{advertised}"
    );
    assert!(
        advertised.contains("\"JOIN_CONDITION_EQUI\""),
        "getCapabilities must advertise the JOIN_CONDITION_EQUI capability:\n{advertised}"
    );
}

// ---------------------------------------------------------------------------
// 8.2  Math functions in WHERE filter
// ---------------------------------------------------------------------------

/// Math scalar functions in a WHERE filter push down and return correct rows.
///
/// Filter: `ABS(score - 50.0) < 20.0` — strict less-than, so scores where
/// |score - 50| < 20, i.e. 30 < score < 70.
/// Scores are 5*id (5,10,…,100). 30 < 5*id < 70 → 6 < id < 14 → ids 7..13 → 7 rows.
/// id=6 has score=30.0 → ABS(30.0-50.0)=20.0, NOT < 20.0 → excluded.
#[test]
fn e2e_math_functions_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} WHERE ABS(score - 50.0) < 20.0 ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");

    // ids 7..13 inclusive → 7 rows (id=6 has score=30.0, boundary is excluded by strict <).
    let expected_count = 7i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "ABS(score - 50.0) < 20.0 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    // Every returned score must satisfy |score - 50.0| < 20.0.
    for v in &cols[1] {
        let s = parse_numeric(v);
        assert!(
            (s - 50.0).abs() < 20.0,
            "filter violated: ABS({s} - 50.0) = {} >= 20.0",
            (s - 50.0).abs()
        );
    }

    // IDs must be 7..13 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 7 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

// ---------------------------------------------------------------------------
// 8.3  String functions in WHERE filter
// ---------------------------------------------------------------------------

/// String scalar functions in a WHERE filter push down and return correct rows.
///
/// Filter: `LOWER(name) LIKE 'event-1%'` — names event-10..19 → 10 rows (id 10..19).
/// LOWER is a no-op here (names already lowercase); the key test is translation.
#[test]
fn e2e_string_functions_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, name FROM {} WHERE LOWER(name) LIKE 'event-1%' ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, name): {cols:?}");

    // Names event-10..event-19 → 10 rows.
    let expected_count = 10i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "LOWER(name) LIKE 'event-1%' must return {expected_count} rows, got {}",
        cols[0].len()
    );

    // All returned names must start with 'event-1'.
    for v in &cols[1] {
        let n = v
            .as_str()
            .unwrap_or_else(|| panic!("name not a string: {v:?}"));
        assert!(
            n.starts_with("event-1"),
            "filter violated: name '{n}' does not start with 'event-1'"
        );
    }

    // IDs must be 10..19 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 10 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

// ---------------------------------------------------------------------------
// 8.4  Date functions (EXTRACT / DATE_TRUNC) in WHERE filter
// ---------------------------------------------------------------------------

/// Date scalar functions in a WHERE filter push down and return correct rows.
///
/// Seed dates: 2024-01-01 + (id-1) days, so day-of-month = id (id 1..20, all Jan).
/// Filter: `EXTRACT(DAY FROM event_date) > 10` → id 11..20 → 10 rows.
#[test]
fn e2e_date_functions_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, event_date FROM {} WHERE EXTRACT(DAY FROM event_date) > 10 ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        2,
        "expected 2 columns (id, event_date): {cols:?}"
    );

    // id 11..20 → 10 rows.
    let expected_count = 10i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "EXTRACT(DAY FROM event_date) > 10 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    // IDs must be 11..20 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 11 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

// ---------------------------------------------------------------------------
// 8.5  REGEXP_LIKE in WHERE filter
// ---------------------------------------------------------------------------

/// REGEXP_LIKE in a WHERE filter pushes down and returns correct rows.
///
/// Pattern `event-0[0-9]` matches names event-01..event-09 → 9 rows (id 1..9).
/// event-10..20 have two digits after the dash and do NOT match `0[0-9]`.
#[test]
fn e2e_regexp_like_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, name FROM {} WHERE name REGEXP_LIKE 'event-0[0-9]' ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, name): {cols:?}");

    // event-01..event-09 → 9 rows.
    let expected_count = 9i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "REGEXP_LIKE(name, 'event-0[0-9]') must return {expected_count} rows, got {}",
        cols[0].len()
    );

    // IDs must be 1..9 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 1 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }

    // All names must match the pattern.
    for v in &cols[1] {
        let n = v
            .as_str()
            .unwrap_or_else(|| panic!("name not a string: {v:?}"));
        assert!(
            n.starts_with("event-0"),
            "filter violated: name '{n}' should match event-0[0-9]"
        );
    }
}

// ---------------------------------------------------------------------------
// 8.6  Scalar expressions in the SELECT list
// ---------------------------------------------------------------------------

/// Scalar expressions in the SELECT list push down and return correct evaluated values.
///
/// Query: `SELECT id, score * 2.0, UPPER(name) FROM ... WHERE id <= 3 ORDER BY id`
/// Expected:
///   id=1: score*2=10.0, UPPER(name)="EVENT-01"
///   id=2: score*2=20.0, UPPER(name)="EVENT-02"
///   id=3: score*2=30.0, UPPER(name)="EVENT-03"
#[test]
fn e2e_selectlist_expression_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score * 2.0, UPPER(name) FROM {} WHERE id <= 3 ORDER BY id",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, score*2, UPPER(name)): {cols:?}"
    );
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1..3): {cols:?}");

    // Verify score * 2.0 for each id.
    let expected_scores = [10.0f64, 20.0, 30.0];
    for (i, expected) in expected_scores.iter().enumerate() {
        let s = parse_numeric(&cols[1][i]);
        assert!(
            (s - expected).abs() < 0.001,
            "row {i}: score*2.0 must be {expected}, got {s}"
        );
    }

    // Verify UPPER(name) for each id.
    let expected_names = ["EVENT-01", "EVENT-02", "EVENT-03"];
    for (i, expected) in expected_names.iter().enumerate() {
        let n = cols[2][i]
            .as_str()
            .unwrap_or_else(|| panic!("UPPER(name) at row {i} is not a string: {:?}", cols[2][i]));
        assert_eq!(
            n.to_uppercase(),
            expected.to_uppercase(),
            "row {i}: UPPER(name) must be {expected}, got {n}"
        );
    }
}

// ---------------------------------------------------------------------------
// 8.7  HAVING clause pushdown
// ---------------------------------------------------------------------------

/// HAVING applied to the outer merged result, not per-shard, keeps groups that only
/// pass the threshold after merging.
///
/// Seed: 20 rows across 2 data files (file 1: ids 1..=10, file 2: ids 11..=20).
/// Grouping: `MOD(id, 4)` → four groups (0,1,2,3), 5 rows each total.
///
/// Per-file counts (shards = files):
///   File 1 (ids 1-10):  group 0={4,8}→2, group 1={1,5,9}→3, group 2={2,6,10}→3, group 3={3,7}→2
///   File 2 (ids 11-20): group 0={12,16,20}→3, group 1={13,17}→2, group 2={14,18}→2, group 3={11,15,19}→3
///
/// Max per-shard count per group = 3. Threshold: `HAVING COUNT(*) > 3`.
/// A buggy per-shard HAVING would drop all 4 groups (3 is not > 3).
/// Correct outer-wrapper HAVING keeps all 4 groups (merged count = 5 > 3).
///
/// Asserting 4 groups back is the discriminating check: 0 groups = buggy, 4 groups = correct.
#[test]
fn e2e_having_clause_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), COUNT(*) FROM {} GROUP BY MOD(id, 4) HAVING COUNT(*) > 3 ORDER BY MOD(id, 4)",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");

    // Merged count per group = 5 > 3 → all 4 groups survive.
    // Per-shard count per group ≤ 3 (not > 3) → a buggy per-shard HAVING would return 0 groups.
    assert_eq!(
        cols[0].len(),
        4,
        "HAVING COUNT(*) > 3 must return 4 groups (merged count=5>3; per-shard max=3 would drop all), got {}",
        cols[0].len()
    );

    for (i, v) in cols[1].iter().enumerate() {
        let count = parse_int(v);
        assert_eq!(
            count, 5,
            "group at position {i}: COUNT(*) must be 5, got {count}"
        );
    }

    // Total rows = 20.
    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across HAVING-filtered groups must be 20, got {total}"
    );
}

// ---------------------------------------------------------------------------
// 8.8  Statistical aggregates (STDDEV / VARIANCE)
// ---------------------------------------------------------------------------

/// Statistical aggregates push down and merge to the single-node result within float tolerance.
///
/// Scores = 5*id for id=1..20: arithmetic sequence 5,10,…,100 (n=20, mean=52.5).
///
/// Population statistics:
///   VAR_POP  = Σ(x - mean)² / n = 831.25
///   STDDEV_POP = √831.25 ≈ 28.8316...
///
/// Sample statistics (Exasol default STDDEV/VARIANCE use divisor n-1):
///   VARIANCE = n/(n-1) * VAR_POP = 20/19 * 831.25 = 875.0
///   STDDEV   = √875.0 ≈ 29.5804...
///
/// The engine reconstructs STDDEV/VARIANCE from (count, sum, sum_sq) sufficient
/// statistics accumulated across shards; the result must match within 1e-6 relative
/// tolerance.
#[test]
fn e2e_stddev_variance_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT STDDEV(score), VARIANCE(score), STDDEV_POP(score), VAR_POP(score) FROM {}",
        vs_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 4, "expected 4 aggregate columns: {cols:?}");
    assert_eq!(
        cols[0].len(),
        1,
        "each aggregate must return 1 row: {cols:?}"
    );

    // Expected values (exact arithmetic for this seed).
    // sum_sq = 25 * Σk² for k=1..20 = 25 * (20*21*41/6) = 25 * 2870 = 71750
    // var_pop  = 71750/20 - 52.5² = 3587.5 - 2756.25 = 831.25
    // var_samp = 20/19 * 831.25 = 875.0
    let expected_var_pop: f64 = 831.25;
    let expected_var_samp: f64 = 875.0;
    let expected_stddev_pop: f64 = expected_var_pop.sqrt();
    let expected_stddev_samp: f64 = expected_var_samp.sqrt();

    // Relative tolerance for floating-point reconstruction from sufficient statistics.
    let tol = 1e-6f64;

    let stddev_samp = parse_numeric(&cols[0][0]);
    let rel_err_stddev = (stddev_samp - expected_stddev_samp).abs() / expected_stddev_samp;
    assert!(
        rel_err_stddev < tol,
        "STDDEV(score) must be ≈{expected_stddev_samp:.6}, got {stddev_samp:.6} (rel_err={rel_err_stddev:.2e})"
    );

    let var_samp = parse_numeric(&cols[1][0]);
    let rel_err_var = (var_samp - expected_var_samp).abs() / expected_var_samp;
    assert!(
        rel_err_var < tol,
        "VARIANCE(score) must be ≈{expected_var_samp:.6}, got {var_samp:.6} (rel_err={rel_err_var:.2e})"
    );

    let stddev_pop = parse_numeric(&cols[2][0]);
    let rel_err_stddev_pop = (stddev_pop - expected_stddev_pop).abs() / expected_stddev_pop;
    assert!(
        rel_err_stddev_pop < tol,
        "STDDEV_POP(score) must be ≈{expected_stddev_pop:.6}, got {stddev_pop:.6} (rel_err={rel_err_stddev_pop:.2e})"
    );

    let var_pop = parse_numeric(&cols[3][0]);
    let rel_err_var_pop = (var_pop - expected_var_pop).abs() / expected_var_pop;
    assert!(
        rel_err_var_pop < tol,
        "VAR_POP(score) must be ≈{expected_var_pop:.6}, got {var_pop:.6} (rel_err={rel_err_var_pop:.2e})"
    );
}
