//! End-to-end correctness tests for single-group `COUNT(DISTINCT col)` pushdown
//! and its combination with expression-argument aggregates (Q9b's shape).
//!
//! Exercises the scenarios in
//! `specs/_plans/add-count-distinct-and-expression-aggregate-pushdown/vs-adapter/pushdown-planning-count-distinct/spec.md`
//! that unit tests cannot reach: the scalar merge UDF actually unioning
//! per-shard local distinct sets computed by real DataFusion scans over real
//! Iceberg/Parquet data, and the per-shard safety cap tripping against a real
//! oversized local set.
//!
//! Shares the same Exasol + MinIO + Iceberg REST catalog stack as
//! `e2e_scan_test.rs` / `e2e_capability_test.rs`. The setup (SLC install,
//! BucketFS upload, VS creation) is intentionally NOT deduplicated across
//! files — each E2E test binary runs in its own process, so each needs its
//! own `OnceLock`-guarded setup; this mirrors `e2e_capability_test.rs`.
//!
//! In addition to the shared `events`/`labels`/`regions` seed tables, this
//! file seeds two more tables used ONLY here (`common::seed::seed_distinct_probe`,
//! `seed_high_card_probe`) so the other E2E binaries do not pay for data they
//! do not need.
//!
//! All tests FAIL (never skip) when the stack is unavailable — per project rules.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{
    DISTINCT_CATEGORY_COL, DISTINCT_CATEGORY_COUNT, DISTINCT_COMMENT_COL,
    DISTINCT_COMMENT_LENGTH_SUM, DISTINCT_REGION_COL, DISTINCT_REGION_COUNT, E2E_DISTINCT_TABLE,
    E2E_HIGH_CARD_TABLE, E2E_NAMESPACE, HIGH_CARD_COL, seed_distinct_probe, seed_events,
    seed_high_card_probe,
};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, upload_to_bucketfs, wait_for_exasol, wait_for_iceberg_catalog,
    wait_for_minio,
};

use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_scan_test.rs / e2e_capability_test.rs — same stack, same VS)
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
const VS_NAME: &str = "MY_LAKEHOUSE";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
/// Scalar merge UDF for single-group COUNT(DISTINCT): third entry point in the
/// same .so, created in the scan schema alongside the adapter and scan scripts.
const MERGE_SCRIPT_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.20.1";
const LANG_ALIAS: &str = "RUST";
/// Name of the Exasol CONNECTION carrying catalog + storage credentials.
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

// ---------------------------------------------------------------------------
// One-time setup (idempotent; mirrors e2e_scan_test.rs / e2e_capability_test.rs)
// ponytail: duplicate setup — each E2E test binary runs independently, so
// each needs its own OnceLock guard.
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
                .expect("seed Iceberg events table");
            seed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg distinct_probe table");
            seed_high_card_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg high_card_probe table");
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
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SET SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}(common VARCHAR(2000000), files VARCHAR(2000000))
EMITS (...) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
    // Scalar distinct-merge script — third entry point in the SAME .so, created
    // in the scan schema alongside the adapter and scan scripts.
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{MERGE_SCRIPT_NAME}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
}

fn create_virtual_schema(conn: &mut ExaConn) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);

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

fn distinct_table() -> String {
    format!("{VS_NAME}.{}", E2E_DISTINCT_TABLE.to_uppercase())
}

fn high_card_table() -> String {
    format!("{VS_NAME}.{}", E2E_HIGH_CARD_TABLE.to_uppercase())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn parse_int(v: &serde_json::Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

/// Runs `EXPLAIN VIRTUAL` for a query and asserts the pushed SQL carries an
/// `aggregates` field in the scan spec — i.e. single-group aggregate pushdown
/// occurred — rather than falling back to a raw row-scan that ships every
/// projected column to Exasol for it to aggregate itself.
fn assert_aggregate_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("aggregates"),
        "EXPLAIN VIRTUAL output must contain an 'aggregates' field in the scan \
         spec (single-group aggregate pushdown occurred), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `COUNT(DISTINCT category)` over `distinct_probe` (seeded across TWO data
/// files / shards) merges to the correct distinct count, proving:
///   - dedup ACROSS shards: "A" appears as a non-NULL `category` value in both
///     shards (file 1: ids 3,6,9; file 2: ids 12,15,18) — a merge that just
///     summed per-shard distinct counts would overcount (e.g. 2 + 2 = 4
///     instead of the correct 3).
///   - NULL exclusion: 7 of the 20 rows have `category IS NULL`, and none of
///     them may contribute to the distinct set.
///   - empty result: a WHERE filter matching zero rows returns a distinct
///     count of 0, not an error.
///   - an all-NULL local set edge case (`WHERE category IS NULL`) also merges
///     to 0, exercising the "shard's local set is empty" path explicitly.
#[test]
fn count_distinct_merges_across_shards_dedup_null_empty() {
    setup_e2e();
    let mut conn = exa_conn();

    // Dedup across shards + NULL exclusion.
    let sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {}",
        distinct_table()
    );
    assert_aggregate_pushed_down(&mut conn, &sql);
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");
    let distinct_count = parse_int(&cols[0][0]);
    assert_eq!(
        distinct_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) must be {DISTINCT_CATEGORY_COUNT} \
         (values {{A,B,C}}, with A shared across both shards and NULLs excluded), \
         got {distinct_count}"
    );

    // Empty result (WHERE filter matches zero rows, but NOT zero files): 'AA'
    // is a category value that never appears in the seeded data, but it still
    // falls inside both files' min/max column-stats range ('A'..'B' for file 1,
    // 'A'..'C' for file 2 — "AA" lexicographically sorts between "A" and both
    // "B" and "C"), so Iceberg-level file pruning does NOT eliminate either
    // file. A predicate like `id > 1000` would instead prune BOTH files
    // entirely, which trips the unrelated, pre-existing empty-pushdown shape
    // bug tracked in issue #57 — this sub-case must exercise "the merge UDF
    // unions per-shard EMPTY `[]` partials", not that bug, so it deliberately
    // avoids 100%-file-pruning predicates.
    let empty_sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {} WHERE {DISTINCT_CATEGORY_COL} = 'AA'",
        distinct_table()
    );
    let empty_cols = conn.query_columns(&empty_sql);
    let empty_count = parse_int(&empty_cols[0][0]);
    assert_eq!(
        empty_count, 0,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) over an empty result set must be 0, \
         got {empty_count}"
    );

    // All-NULL local set edge case: every matching row has category IS NULL.
    let all_null_sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {} WHERE {DISTINCT_CATEGORY_COL} IS NULL",
        distinct_table()
    );
    let all_null_cols = conn.query_columns(&all_null_sql);
    let all_null_count = parse_int(&all_null_cols[0][0]);
    assert_eq!(
        all_null_count, 0,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) WHERE {DISTINCT_CATEGORY_COL} IS NULL \
         must be 0 (an all-NULL local set), got {all_null_count}"
    );
}

/// `COUNT(DISTINCT token)` over `high_card_probe` — a single shard whose local
/// distinct set exceeds the per-shard safety cap — fails with a clean,
/// descriptive error instead of crashing, hanging, or silently returning a
/// wrong (truncated) count.
///
/// `high_card_probe` is seeded as ONE data file (`HIGH_CARD_ROWS` = 12,000
/// unique 100-byte `token` values), so with a single file the adapter's
/// sharding always yields exactly one shard — the WHOLE oversized set lands
/// on one shard, deterministically tripping `MAX_DISTINCT_BYTES_PER_SHARD`
/// (`crates/lakehouse-engine/src/scan/mod.rs`) well before
/// `MAX_DISTINCT_ELEMENTS_PER_SHARD`.
#[test]
fn high_cardinality_count_distinct_fails_cleanly() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {HIGH_CARD_COL}) FROM {}",
        high_card_table()
    );
    let resp = conn.try_execute(&sql);
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "COUNT(DISTINCT {HIGH_CARD_COL}) over a per-shard-cap-exceeding column \
         must fail cleanly, not succeed with a (necessarily wrong) answer: {resp}"
    );

    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        !msg.is_empty(),
        "the safety-cap error must carry a descriptive message, got empty: {resp}"
    );
    assert!(
        msg.to_uppercase().contains(&HIGH_CARD_COL.to_uppercase()),
        "the safety-cap error should name the offending column '{HIGH_CARD_COL}' \
         (column names surface uppercase in pushdown SQL/errors): {msg}"
    );
    assert!(
        msg.to_lowercase().contains("cap") || msg.to_lowercase().contains("exceed"),
        "the safety-cap error should describe a bounded-resource cap being exceeded: {msg}"
    );
}

/// A single query combining multiple `COUNT(DISTINCT ...)` columns AND a
/// `SUM(LENGTH(...))`-shaped expression aggregate — the TPC-H Q9b shape
/// (see `bench/run.sh`'s "Q9b wide projection" query) — pushes down as ONE
/// aggregate plan and returns all values correctly together.
///
/// `distinct_probe`: `COUNT(DISTINCT category)` = 3, `COUNT(DISTINCT region)`
/// = 4 (independent columns, merged independently), `SUM(LENGTH(comment))`
/// = 210 (comment length == id, summed 1..=20).
#[test]
fn q9b_multiple_count_distinct_and_expression_agg() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}), COUNT(DISTINCT {DISTINCT_REGION_COL}), \
         SUM(LENGTH({DISTINCT_COMMENT_COL})) FROM {}",
        distinct_table()
    );
    assert_aggregate_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 aggregate columns: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let category_count = parse_int(&cols[0][0]);
    assert_eq!(
        category_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) must be {DISTINCT_CATEGORY_COUNT}, \
         got {category_count}"
    );

    let region_count = parse_int(&cols[1][0]);
    assert_eq!(
        region_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT {DISTINCT_REGION_COL}) must be {DISTINCT_REGION_COUNT}, \
         got {region_count}"
    );

    let comment_length_sum = cols[2][0]
        .as_i64()
        .or_else(|| cols[2][0].as_f64().map(|f| f.round() as i64))
        .or_else(|| {
            cols[2][0]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round() as i64)
        })
        .unwrap_or_else(|| panic!("expected numeric SUM(LENGTH(...)), got: {:?}", cols[2][0]));
    assert_eq!(
        comment_length_sum, DISTINCT_COMMENT_LENGTH_SUM,
        "SUM(LENGTH({DISTINCT_COMMENT_COL})) must be {DISTINCT_COMMENT_LENGTH_SUM}, \
         got {comment_length_sum}"
    );
}
