//! End-to-end inner equi-join pushdown tests for the lakehouse-engine Virtual
//! Schema, against the local Exasol + MinIO + Iceberg Docker stack.
//!
//! Covers the two correctness-critical paths of the broadcast join feature
//! (plan `add-join-pushdown-broadcast`, tasks 5.4 / 5.5):
//!
//!   * BROADCAST — the smaller (dimension) side is within
//!     `JOIN_BROADCAST_MAX_BYTES`, so the join is pushed down as a SINGLE
//!     scan-UDF-driving query: the fact side is sharded and the dimension side
//!     rides as a file list in the common ScanSpec blob's join block, joined
//!     node-locally in DataFusion.
//!   * UNACCELERATED FALLBACK — with `JOIN_BROADCAST_MAX_BYTES = '1'` the
//!     dimension side exceeds the threshold, so the adapter emits a deterministic
//!     two-scan join (each side its own sharded fan-out, joined by Exasol's core
//!     engine). This must return the IDENTICAL result to the broadcast path —
//!     the plan's promise to "never regress correctness for any inner equi-join
//!     Exasol pushes".
//!
//! Seed tables: `dim_customer` (C_CUSTKEY, C_NAME; 5 rows, 1 file) and
//! `fact_orders` (O_ORDERKEY, O_CUSTKEY, O_ORDERDATE; 10 rows, 2 files), with
//! DISJOINT column-name prefixes so the adapter's disjoint-column guard lets it
//! render the join. Every order references a valid customer. See `common/seed.rs`.
//!
//! All tests FAIL (never skip) when the stack is unavailable.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::exasol_ws::ExaConn;
use common::seed::{E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_NAMESPACE, seed_events};
use common::stack::{
    bucketfs_port, bucketfs_write_password, build_create_connection_sql, exasol_host,
    exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal, lakehouse_engine_so_path,
    local_stack_connection_password, upload_to_bucketfs, wait_for_exasol, wait_for_iceberg_catalog,
    wait_for_minio,
};

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_capability_test.rs — same stack, same scan schema)
// ---------------------------------------------------------------------------

const SYS_PASSWORD: &str = "exasol";
const SCHEMA_NAME: &str = "LHVS";
/// Virtual schema with the DEFAULT broadcast threshold (128 MiB): the small
/// dimension side is broadcast-eligible.
const VS_NAME: &str = "MY_LAKEHOUSE_JOIN";
/// Virtual schema forced ABOVE the broadcast threshold (`JOIN_BROADCAST_MAX_BYTES
/// = '1'`): every dimension candidate exceeds 1 byte → unaccelerated two-scan.
const VS_NAME_LOW: &str = "MY_LAKEHOUSE_JOIN_LOW";
const ADAPTER_SCRIPT_NAME: &str = "LAKEHOUSE_ADAPTER";
const SCAN_SCRIPT_NAME: &str = "LAKEHOUSE_SCAN";
const MERGE_SCRIPT_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";
const SO_BUCKETFS_PUT_PATH: &str = "/default/udf/liblakehouse_engine.so";
const SO_UDF_OBJECT_PATH: &str = "buckets/bfsdefault/default/udf/liblakehouse_engine.so";
const SLC_BUCKETFS_PUT_PATH: &str = "/default/slc/lakehouse-rustslc.tar.gz";
const SLC_VERSION: &str = "0.20.2";
const LANG_ALIAS: &str = "RUST";
const CATALOG_CONN_NAME: &str = "LAKEHOUSE_CATALOG_CREDS";

/// WHERE-clause lower bound applied to `O_ORDERDATE` in the join queries. Chosen
/// to straddle both fact-side data files (orders 1..=5 vs 6..=10), so the
/// broadcast fan-out's per-shard join results must merge across a shard boundary.
const ORDERDATE_LOWER_BOUND: &str = "2024-01-05";

// ---------------------------------------------------------------------------
// One-time setup (idempotent; identical stack to e2e_capability_test.rs, plus a
// second virtual schema forced above the broadcast threshold)
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
                .expect("seed Iceberg star-schema tables")
        });

        install_slc();

        let so_path = lakehouse_engine_so_path();
        upload_to_bucketfs(&so_path, SO_BUCKETFS_PUT_PATH);

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_connection(&mut conn);
        // Broadcast VS (default threshold) and low-threshold VS (forced fallback).
        create_virtual_schema(&mut conn, VS_NAME, None);
        create_virtual_schema(&mut conn, VS_NAME_LOW, Some("1"));
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
    conn.execute(&format!(
        r#"CREATE OR REPLACE {LANG_ALIAS} SCALAR SCRIPT {SCHEMA_NAME}.{MERGE_SCRIPT_NAME}(partials VARCHAR(2000000))
RETURNS DECIMAL(20,0) AS
%udf_object {SO_UDF_OBJECT_PATH}
/"#
    ));
}

fn create_connection(conn: &mut ExaConn) {
    let password = local_stack_connection_password();
    let catalog_uri = iceberg_catalog_url_internal();
    let create_conn_sql = build_create_connection_sql(CATALOG_CONN_NAME, &catalog_uri, &password);
    conn.execute(&create_conn_sql);
}

/// Create (or replace) a virtual schema over the star-schema namespace. When
/// `join_broadcast_max_bytes` is `Some`, it is passed as the
/// `JOIN_BROADCAST_MAX_BYTES` adapter note that gates broadcast eligibility.
fn create_virtual_schema(
    conn: &mut ExaConn,
    vs_name: &str,
    join_broadcast_max_bytes: Option<&str>,
) {
    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs_name} CASCADE"));
    let join_threshold_prop = match join_broadcast_max_bytes {
        Some(bytes) => format!("\n  JOIN_BROADCAST_MAX_BYTES = '{bytes}'"),
        None => String::new(),
    };
    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {vs_name}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION = '{CATALOG_CONN_NAME}'
  ICEBERG_NAMESPACE  = '{E2E_NAMESPACE}'
  SCAN_SCHEMA        = '{SCHEMA_NAME}'
  ALLOW_HTTP         = 'true'{join_threshold_prop}"#
    ));
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

fn vs_dim_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_DIM_TABLE.to_uppercase())
}

fn vs_fact_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_FACT_TABLE.to_uppercase())
}

/// The `SELECT C_NAME, O_ORDERDATE FROM fact JOIN dim ...` query for one VS.
fn join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// Run `EXPLAIN VIRTUAL <query>` and flatten the pushed SQL (generated
/// scan-driving IMPORT plus Exasol's echoed pushdown request) into one string.
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

/// Whether the pushed SQL carries a broadcast join: the fact-side ScanSpec's
/// common blob embeds a `"join"` block (dimension file list + condition), joined
/// node-locally in one DataFusion session. The lowercase compact `"join":{` token
/// is unique to the generated ScanSpec JSON — Exasol's pretty-printed echoed
/// request uses `"type" : "join"` / `"join_type"`, and the capability list uses
/// uppercase `"JOIN"`, so neither collides.
fn has_broadcast_join_block(pushed_sql: &str) -> bool {
    pushed_sql.contains("\"join\":{")
}

/// Whether the pushed SQL is the deterministic two-scan fallback: each side its
/// own sharded fan-out, wrapped in an Exasol-executed `INNER JOIN` with the
/// `LHS_FACT`/`LHS_DIM` aliases. These aliases appear only in this generated
/// wrapper, never in a native retry or the broadcast path.
fn has_two_scan_wrapper(pushed_sql: &str) -> bool {
    pushed_sql.contains("LHS_FACT") && pushed_sql.contains("LHS_DIM")
}

/// Fetch the join result as a sorted `Vec<(C_NAME, O_ORDERDATE)>` for
/// order-independent multiset comparison.
fn fetch_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<(String, String)> {
    let cols = conn.query_columns(&join_query(vs_name));
    columns_to_sorted_pairs(&cols)
}

fn columns_to_sorted_pairs(cols: &[Vec<serde_json::Value>]) -> Vec<(String, String)> {
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

fn value_to_string(v: &serde_json::Value) -> String {
    v.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_string())
}

/// Compute the expected join result INDEPENDENTLY of the join pushdown: read both
/// tables un-joined through the VS and join them in-process. This is the ground
/// truth both the broadcast and fallback join results must match.
fn expected_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<(String, String)> {
    // custkey -> name
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

    // fact rows (custkey, orderdate) with the same WHERE filter, un-joined.
    let fact_cols = conn.query_columns(&format!(
        "SELECT O_CUSTKEY, O_ORDERDATE FROM {} WHERE O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'",
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

// ---------------------------------------------------------------------------
// 5.4  Broadcast join: single scan-UDF-driving shape + correct result
// ---------------------------------------------------------------------------

/// EXPLAIN VIRTUAL of a broadcast-eligible inner equi-join shows the SINGLE
/// scan-UDF-driving broadcast fan-out (matching the plan's first Manual Testing
/// row): one `LAKEHOUSE_SCAN` invocation, NOT the two-scan Exasol-joined shape.
#[test]
fn e2e_broadcast_join_pushdown_shape() {
    setup_e2e();
    let mut conn = exa_conn();

    let pushed = explain_virtual_sql(&mut conn, &join_query(VS_NAME));
    assert!(
        has_broadcast_join_block(&pushed),
        "broadcast join must drive ONE scan UDF carrying a common-blob join block \
         (fact sharded, dimension file list in the join block):\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "broadcast join must NOT emit the two-scan Exasol-joined fallback \
         (LHS_FACT/LHS_DIM):\n{pushed}"
    );
}

/// The broadcast join returns the correct result: identical (as a sorted
/// multiset) to the join computed independently from the two tables read
/// un-joined through the same VS.
#[test]
fn e2e_broadcast_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let actual = fetch_join_rows(&mut conn, VS_NAME);
    let expected = expected_join_rows(&mut conn, VS_NAME);

    // Every order with O_ORDERDATE >= 2024-01-05 (order keys 5..=10) matches a
    // customer → 6 result rows.
    assert_eq!(
        actual.len(),
        6,
        "expected 6 joined rows (orders 5..=10), got {}: {actual:?}",
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "broadcast join result must equal the independently computed join.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}

// ---------------------------------------------------------------------------
// 5.5  Above-threshold unaccelerated fallback: two-scan shape + same result
// ---------------------------------------------------------------------------

/// With `JOIN_BROADCAST_MAX_BYTES = '1'` the dimension side exceeds the
/// threshold, so EXPLAIN VIRTUAL shows the deterministic two-scan fallback (two
/// independent per-table fan-outs joined by Exasol's core engine): the
/// `LHS_FACT`/`LHS_DIM` wrapper and TWO scan-UDF invocations. It must NOT be the
/// broadcast shape and must NOT be a native retry (which would carry no
/// `LHS_FACT` wrapper).
#[test]
fn e2e_above_threshold_unaccelerated_fallback_shape() {
    setup_e2e();
    let mut conn = exa_conn();

    let pushed = explain_virtual_sql(&mut conn, &join_query(VS_NAME_LOW));
    assert!(
        has_two_scan_wrapper(&pushed),
        "above-threshold join must emit the deterministic two-scan fallback \
         (LHS_FACT/LHS_DIM wrapper), not a broadcast join or a native retry:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "above-threshold fallback must NOT carry a broadcast common-blob join \
         block (each side is scanned independently and joined by Exasol):\n{pushed}"
    );
}

/// The above-threshold unaccelerated fallback returns the IDENTICAL result to
/// the broadcast path — the plan's promise never to regress correctness for any
/// inner equi-join Exasol pushes.
#[test]
fn e2e_above_threshold_result_matches_broadcast() {
    setup_e2e();
    let mut conn = exa_conn();

    let broadcast = fetch_join_rows(&mut conn, VS_NAME);
    let fallback = fetch_join_rows(&mut conn, VS_NAME_LOW);

    assert!(
        !fallback.is_empty(),
        "fallback join returned no rows — expected the same 6 rows as broadcast"
    );
    assert_eq!(
        fallback, broadcast,
        "unaccelerated fallback result must equal the broadcast result.\n\
         fallback:  {fallback:?}\nbroadcast: {broadcast:?}"
    );
}

// ---------------------------------------------------------------------------
// Aggregate over a join (the plan's second Manual Testing query). Exasol pushes
// the whole `COUNT(*), MIN(o.O_ORDERDATE) FROM fact JOIN dim ON ...` — aggregate
// AND join — to the adapter. It is served by the two-scan wrapper with the
// aggregate rendered as ordinary Exasol SQL over the materialized join (Exasol
// aggregates the joined-and-materialized rows, exactly as before any `JOIN`
// capability existed), NEVER the broadcast in-UDF join.
// ---------------------------------------------------------------------------

/// `SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM fact JOIN dim ON ...` for one VS.
fn aggregate_join_query(vs_name: &str) -> String {
    format!(
        "SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// An aggregate over a join is routed to the two-scan wrapper (aggregate executed
/// by Exasol over the join), NOT the broadcast in-UDF join — even on the
/// broadcast-eligible VS. This is the routing fix: an aggregate select list cannot
/// ride the broadcast join, so it forces the qualified two-scan path.
#[test]
fn e2e_aggregate_over_join_uses_two_scan_wrapper() {
    setup_e2e();
    let mut conn = exa_conn();

    let pushed = explain_virtual_sql(&mut conn, &aggregate_join_query(VS_NAME));
    assert!(
        has_two_scan_wrapper(&pushed),
        "aggregate-over-join must emit the two-scan wrapper so Exasol aggregates \
         over the join (LHS_FACT/LHS_DIM), even on the broadcast VS:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "an aggregate cannot ride the broadcast in-UDF join — no common-blob join \
         block may appear:\n{pushed}"
    );
    assert!(
        pushed.contains("COUNT(*)"),
        "the aggregate must be rendered as Exasol SQL over the join:\n{pushed}"
    );
}

/// The aggregate-over-join result is correct: COUNT(*) and MIN(O_ORDERDATE) over
/// the join equal the same aggregate over the fact table alone — every order has a
/// matching customer, so the inner join neither drops nor duplicates a fact row.
/// Asserted on BOTH the broadcast-eligible VS and the forced-fallback VS (both take
/// the two-scan aggregate path), so neither regresses the correctness the plan
/// promises. This is the `SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM CUSTOMER JOIN
/// ORDERS ...` query that previously failed with "expected 2 columns but pushdown
/// query has 5".
#[test]
fn e2e_aggregate_over_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    // Ground truth: the single-table aggregate over the fact table (already served
    // by the working single-table aggregate pushdown).
    let truth = conn.query_columns(&format!(
        "SELECT COUNT(*), MIN(O_ORDERDATE) FROM {}",
        vs_fact_table(VS_NAME)
    ));
    assert_eq!(
        truth.len(),
        2,
        "ground-truth aggregate must return 2 columns"
    );
    let expected_count = value_to_string(&truth[0][0]);
    let expected_min = value_to_string(&truth[1][0]);

    for vs_name in [VS_NAME, VS_NAME_LOW] {
        let cols = conn.query_columns(&aggregate_join_query(vs_name));
        assert_eq!(
            cols.len(),
            2,
            "aggregate-over-join must return 2 columns for {vs_name}: {cols:?}"
        );
        assert_eq!(
            cols[0].len(),
            1,
            "a single-group aggregate returns exactly one row for {vs_name}: {cols:?}"
        );
        assert_eq!(
            value_to_string(&cols[0][0]),
            expected_count,
            "COUNT(*) over the join must equal the single-table COUNT for {vs_name}"
        );
        assert_eq!(
            value_to_string(&cols[1][0]),
            expected_min,
            "MIN(O_ORDERDATE) over the join must equal the single-table MIN for {vs_name}"
        );
    }
}
