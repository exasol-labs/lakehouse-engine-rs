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
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_LINEITEM_TABLE, E2E_NAMESPACE, E2E_SUPPLIER_TABLE,
    FACT_ORDERS_ROWS, LINEITEM_ROWS, seed_events,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use std::collections::HashMap;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_capability_test.rs — same stack, same scan schema)
// ---------------------------------------------------------------------------

/// Virtual schema with the DEFAULT broadcast threshold (128 MiB): the small
/// dimension side is broadcast-eligible.
const VS_NAME: &str = "MY_LAKEHOUSE_JOIN";
/// Virtual schema forced ABOVE the broadcast threshold (`JOIN_BROADCAST_MAX_BYTES
/// = '1'`): every dimension candidate exceeds 1 byte → unaccelerated two-scan.
const VS_NAME_LOW: &str = "MY_LAKEHOUSE_JOIN_LOW";

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
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        // Broadcast VS (default threshold) and low-threshold VS (forced fallback).
        // The catalog CONNECTION is re-issued idempotently inside each
        // `create_virtual_schema`, so no separate create_connection step is needed.
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
        create_virtual_schema(
            &mut conn,
            &VsProps::new(VS_NAME_LOW, E2E_NAMESPACE).with_join_broadcast_max_bytes("1"),
        );
    });
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

fn vs_lineitem_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_LINEITEM_TABLE.to_uppercase())
}

fn vs_supplier_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_SUPPLIER_TABLE.to_uppercase())
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

/// Whether the pushed SQL carries a broadcast join: the fact-side ScanSpec's
/// common blob embeds a `"join"` block (dimension file list + condition), joined
/// node-locally in one DataFusion session. The lowercase compact `"join":{` token
/// is unique to the generated ScanSpec JSON — Exasol's pretty-printed echoed
/// request uses `"type" : "join"` / `"join_type"`, and the capability list uses
/// uppercase `"JOIN"`, so neither collides.
fn has_broadcast_join_block(pushed_sql: &str) -> bool {
    pushed_sql.contains("\"join\":{")
}

/// Whether the pushed SQL is the deterministic two-table unaccelerated fallback:
/// each side its own sharded fan-out, wrapped in an Exasol-executed `INNER JOIN`
/// with the unified renderer's `LHS_T0`/`LHS_T1` aliases (the two-table case is
/// simply N = 2 of the single N-scan wrapper; see `has_n_scan_wrapper`). These
/// aliases appear only in this generated wrapper, never in a native retry or the
/// broadcast path.
fn has_two_scan_wrapper(pushed_sql: &str) -> bool {
    has_n_scan_wrapper(pushed_sql, 2)
}

/// Whether the pushed SQL is the N-scan unaccelerated wrapper for exactly `n`
/// base tables: `n` distinct `LHS_T0..LHS_T{n-1}` fan-out aliases, and no
/// `LHS_T{n}` (so a 3-table wrapper is never mistaken for a 4-table one). These
/// aliases (`build_n_scan_alias_map`) are unique to the N-scan wrapper's
/// generated SQL — never present in a native retry or a broadcast join.
fn has_n_scan_wrapper(pushed_sql: &str, n: usize) -> bool {
    (0..n).all(|i| pushed_sql.contains(&format!(r#"AS "LHS_T{i}""#)))
        && !pushed_sql.contains(&format!(r#"AS "LHS_T{n}""#))
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
         (LHS_T0/LHS_T1):\n{pushed}"
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
/// independent per-table fan-outs joined by Exasol's core engine): the unified
/// renderer's `LHS_T0`/`LHS_T1` wrapper and TWO scan-UDF invocations. It must
/// NOT be the broadcast shape and must NOT be a native retry (which would carry
/// no `LHS_T*` wrapper).
#[test]
fn e2e_above_threshold_unaccelerated_fallback_shape() {
    setup_e2e();
    let mut conn = exa_conn();

    let pushed = explain_virtual_sql(&mut conn, &join_query(VS_NAME_LOW));
    assert!(
        has_two_scan_wrapper(&pushed),
        "above-threshold join must emit the deterministic two-scan fallback \
         (LHS_T0/LHS_T1 wrapper), not a broadcast join or a native retry:\n{pushed}"
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
         over the join (LHS_T0/LHS_T1), even on the broadcast VS:\n{pushed}"
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

// ---------------------------------------------------------------------------
// 6.2  N-scan unaccelerated fallback: 3-table and 4-table inner joins actually
// succeed end-to-end (no F-UDF-CL-RUST-9001) and return correct results.
//
// Chain: dim_customer ⋈ fact_orders ⋈ fact_lineitem [⋈ dim_supplier], joined on
// C_CUSTKEY=O_CUSTKEY, then O_ORDERKEY=L_ORDERKEY, then (4-table only)
// L_SUPPKEY=S_SUPPKEY. See `common/seed.rs::seed_multi_table_join_extension`:
// every line item references exactly one seeded order and one seeded supplier,
// so both joins yield every seeded `fact_lineitem` row — `LINEITEM_ROWS`.
// ---------------------------------------------------------------------------

/// Fetch a query's result columns as a sorted `Vec<Vec<String>>` (row-major,
/// order-independent multiset comparison), generalizing `columns_to_sorted_pairs`
/// past a fixed 2-column shape.
fn fetch_rows_as_vecs(cols: &[Vec<serde_json::Value>]) -> Vec<Vec<String>> {
    let row_count = cols.first().map_or(0, Vec::len);
    let mut rows: Vec<Vec<String>> = (0..row_count)
        .map(|i| cols.iter().map(|col| value_to_string(&col[i])).collect())
        .collect();
    rows.sort();
    rows
}

/// Build a `key -> value` map from a 2-column `(key, value)` query result,
/// generalizing the map-building step `expected_join_rows` inlines for a single
/// pair, reused across the 3-table and 4-table expected-result computations.
fn build_key_to_value_map(cols: &[Vec<serde_json::Value>]) -> HashMap<String, String> {
    assert_eq!(
        cols.len(),
        2,
        "expected 2 columns (key, value), got {}",
        cols.len()
    );
    cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, v)| (value_to_string(k), value_to_string(v)))
        .collect()
}

/// `dim_customer ⋈ fact_orders ⋈ fact_lineitem` for one VS.
fn three_table_join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY FROM {} c \
         JOIN {} o ON c.C_CUSTKEY = o.O_CUSTKEY \
         JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY",
        vs_dim_table(vs_name),
        vs_fact_table(vs_name),
        vs_lineitem_table(vs_name)
    )
}

/// `dim_customer ⋈ fact_orders ⋈ fact_lineitem ⋈ dim_supplier` for one VS —
/// the same chain as [`three_table_join_query`] extended with the supplier side.
fn four_table_join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY, s.S_NAME FROM {} c \
         JOIN {} o ON c.C_CUSTKEY = o.O_CUSTKEY \
         JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY \
         JOIN {} s ON l.L_SUPPKEY = s.S_SUPPKEY",
        vs_dim_table(vs_name),
        vs_fact_table(vs_name),
        vs_lineitem_table(vs_name),
        vs_supplier_table(vs_name)
    )
}

fn fetch_three_table_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<Vec<String>> {
    let cols = conn.query_columns(&three_table_join_query(vs_name));
    assert_eq!(
        cols.len(),
        3,
        "expected 3 result columns, got {}",
        cols.len()
    );
    fetch_rows_as_vecs(&cols)
}

fn fetch_four_table_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<Vec<String>> {
    let cols = conn.query_columns(&four_table_join_query(vs_name));
    assert_eq!(
        cols.len(),
        4,
        "expected 4 result columns, got {}",
        cols.len()
    );
    fetch_rows_as_vecs(&cols)
}

/// Compute the expected 3-table join result INDEPENDENTLY of the join pushdown:
/// read all three tables un-joined through the same VS and join them in-process
/// — the ground truth the N-scan wrapper result must match.
fn expected_three_table_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<Vec<String>> {
    let custkey_to_name = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT C_CUSTKEY, C_NAME FROM {}",
        vs_dim_table(vs_name)
    )));
    let orderkey_to_custkey = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT O_ORDERKEY, O_CUSTKEY FROM {}",
        vs_fact_table(vs_name)
    )));

    let line_cols = conn.query_columns(&format!(
        "SELECT L_ORDERKEY, L_LINENUMBER, L_QUANTITY FROM {}",
        vs_lineitem_table(vs_name)
    ));
    assert_eq!(line_cols.len(), 3, "lineitem query must return 3 columns");

    let mut rows: Vec<Vec<String>> = (0..line_cols[0].len())
        .map(|i| {
            let order_key = value_to_string(&line_cols[0][i]);
            let line_number = value_to_string(&line_cols[1][i]);
            let quantity = value_to_string(&line_cols[2][i]);
            let cust_key = orderkey_to_custkey
                .get(&order_key)
                .unwrap_or_else(|| panic!("L_ORDERKEY {order_key} has no matching order"));
            let name = custkey_to_name
                .get(cust_key)
                .unwrap_or_else(|| panic!("O_CUSTKEY {cust_key} has no matching customer"))
                .clone();
            vec![name, line_number, quantity]
        })
        .collect();
    rows.sort();
    rows
}

/// Compute the expected 4-table join result INDEPENDENTLY of the join pushdown
/// — the same ground-truth approach as [`expected_three_table_join_rows`],
/// extended with the supplier side.
fn expected_four_table_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<Vec<String>> {
    let custkey_to_name = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT C_CUSTKEY, C_NAME FROM {}",
        vs_dim_table(vs_name)
    )));
    let orderkey_to_custkey = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT O_ORDERKEY, O_CUSTKEY FROM {}",
        vs_fact_table(vs_name)
    )));
    let suppkey_to_name = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT S_SUPPKEY, S_NAME FROM {}",
        vs_supplier_table(vs_name)
    )));

    let line_cols = conn.query_columns(&format!(
        "SELECT L_ORDERKEY, L_LINENUMBER, L_QUANTITY, L_SUPPKEY FROM {}",
        vs_lineitem_table(vs_name)
    ));
    assert_eq!(line_cols.len(), 4, "lineitem query must return 4 columns");

    let mut rows: Vec<Vec<String>> = (0..line_cols[0].len())
        .map(|i| {
            let order_key = value_to_string(&line_cols[0][i]);
            let line_number = value_to_string(&line_cols[1][i]);
            let quantity = value_to_string(&line_cols[2][i]);
            let supp_key = value_to_string(&line_cols[3][i]);
            let cust_key = orderkey_to_custkey
                .get(&order_key)
                .unwrap_or_else(|| panic!("L_ORDERKEY {order_key} has no matching order"));
            let name = custkey_to_name
                .get(cust_key)
                .unwrap_or_else(|| panic!("O_CUSTKEY {cust_key} has no matching customer"))
                .clone();
            let supplier_name = suppkey_to_name
                .get(&supp_key)
                .unwrap_or_else(|| panic!("L_SUPPKEY {supp_key} has no matching supplier"))
                .clone();
            vec![name, line_number, quantity, supplier_name]
        })
        .collect();
    rows.sort();
    rows
}

/// A three-table inner-join pushdown (`dim_customer ⋈ fact_orders ⋈
/// fact_lineitem`) succeeds end-to-end (no `F-UDF-CL-RUST-9001` — issue #76's
/// hard failure) via the N-scan unaccelerated wrapper (three distinct `LHS_T*`
/// aliases), never a broadcast join or the two-table `LHS_T0`/`LHS_T1`
/// shape, and returns the result computed independently from the un-joined
/// tables.
#[test]
fn e2e_three_table_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let pushed = explain_virtual_sql(&mut conn, &three_table_join_query(VS_NAME));
    assert!(
        has_n_scan_wrapper(&pushed, 3),
        "three-table inner join must emit the N-scan wrapper with three distinct \
         LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a three-table join must NOT carry a broadcast common-blob join block \
         (broadcast stays strictly two-table):\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "a three-table join must NOT emit the two-table LHS_T0/LHS_T1 \
         wrapper:\n{pushed}"
    );

    let actual = fetch_three_table_join_rows(&mut conn, VS_NAME);
    let expected = expected_three_table_join_rows(&mut conn, VS_NAME);

    // Every line item matches exactly one order and every order matches exactly
    // one customer, so the inner join drops nothing and duplicates nothing.
    assert_eq!(
        actual.len(),
        LINEITEM_ROWS,
        "expected {LINEITEM_ROWS} joined rows (one per seeded line item), got {}: {actual:?}",
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "three-table N-scan join result must equal the independently computed join.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// A four-table inner-join pushdown (`dim_customer ⋈ fact_orders ⋈
/// fact_lineitem ⋈ dim_supplier`) succeeds end-to-end (no `F-UDF-CL-RUST-9001`)
/// via the N-scan unaccelerated wrapper (four distinct `LHS_T*` aliases),
/// never a broadcast join or the two-table wrapper, and returns the result
/// computed independently from the un-joined tables.
#[test]
fn e2e_four_table_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let pushed = explain_virtual_sql(&mut conn, &four_table_join_query(VS_NAME));
    assert!(
        has_n_scan_wrapper(&pushed, 4),
        "four-table inner join must emit the N-scan wrapper with four distinct \
         LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a four-table join must NOT carry a broadcast common-blob join block \
         (broadcast stays strictly two-table):\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "a four-table join must NOT emit the two-table LHS_T0/LHS_T1 \
         wrapper:\n{pushed}"
    );

    let actual = fetch_four_table_join_rows(&mut conn, VS_NAME);
    let expected = expected_four_table_join_rows(&mut conn, VS_NAME);

    // Every line item matches exactly one order, one customer, and one
    // supplier, so the inner join drops nothing and duplicates nothing.
    assert_eq!(
        actual.len(),
        LINEITEM_ROWS,
        "expected {LINEITEM_ROWS} joined rows (one per seeded line item), got {}: {actual:?}",
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "four-table N-scan join result must equal the independently computed join.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}

// ---------------------------------------------------------------------------
// Scalar function wrapping aggregates in a grouped join select list (PR #78
// review finding #4 / plan `fix-join-decline-hard-fail`, spec scenario "A
// scalar function wrapping aggregates in a grouped join select list is
// rendered, not declined"). The reported query is TPC-H-Q1-shaped:
// `ROUND(100.0 * SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END) /
// COUNT(*), 2)` alongside plain `SUM`/`AVG` aggregates, `GROUP BY`, `HAVING`,
// `ORDER BY`, and `LIMIT` — over a JOIN rather than a single table. Before the
// fix, the join select-list renderer could not recurse a scalar function around
// a nested `function_aggregate` node and declined the request, which the FFI
// shim turns into a hard `F-UDF-CL-RUST-9001` client error (`ExaConn::execute`
// panics on any non-"ok" status, surfacing that error verbatim). The fix routes
// this rendering through `crates/vs-expression`'s shared aggregate arm, so
// these tests fail before the fix (query panics with F-UDF-CL-RUST-9001) and
// pass after.
//
// Ground truth: every seeded `fact_lineitem` row matches exactly one order
// (and, for the three-table case, exactly one customer), so neither join drops
// nor duplicates a row — the grouped aggregate over either join must equal the
// SAME select list evaluated directly over the un-joined `fact_lineitem` table.
// ---------------------------------------------------------------------------

/// The scalar-over-aggregate grouped select list, with each `fact_lineitem`
/// column referenced through `col_prefix` (a table alias like `"l."`, or `""`
/// for an unqualified single-table query).
fn scalar_over_aggregate_select_list(col_prefix: &str) -> String {
    format!(
        "{col_prefix}L_RETURNFLAG, \
         SUM({col_prefix}L_QUANTITY) AS SUM_QTY, \
         SUM(CASE WHEN {col_prefix}L_RETURNFLAG = 'R' THEN 1 ELSE 0 END) AS RETURN_COUNT, \
         AVG({col_prefix}L_EXTENDEDPRICE) AS AVG_PRICE, \
         ROUND(100.0 * SUM(CASE WHEN {col_prefix}L_RETURNFLAG = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2) AS RETURN_PCT"
    )
}

/// Two-table (N=2) grouped join: `fact_orders ⋈ fact_lineitem` on
/// `O_ORDERKEY = L_ORDERKEY`.
fn scalar_over_aggregate_join_query(vs_name: &str) -> String {
    format!(
        "SELECT {} FROM {} o JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY \
         GROUP BY l.L_RETURNFLAG HAVING COUNT(*) > 0 ORDER BY 1 LIMIT 2",
        scalar_over_aggregate_select_list("l."),
        vs_fact_table(vs_name),
        vs_lineitem_table(vs_name)
    )
}

/// Three-table (N>=3) grouped join: `dim_customer ⋈ fact_orders ⋈
/// fact_lineitem`, extending [`scalar_over_aggregate_join_query`] with the
/// customer side exactly as [`three_table_join_query`] extends [`join_query`].
fn scalar_over_aggregate_n_table_join_query(vs_name: &str) -> String {
    format!(
        "SELECT {} FROM {} c \
         JOIN {} o ON c.C_CUSTKEY = o.O_CUSTKEY \
         JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY \
         GROUP BY l.L_RETURNFLAG HAVING COUNT(*) > 0 ORDER BY 1 LIMIT 2",
        scalar_over_aggregate_select_list("l."),
        vs_dim_table(vs_name),
        vs_fact_table(vs_name),
        vs_lineitem_table(vs_name)
    )
}

/// Native (non-virtual) table the ground truth is materialized into — see
/// [`ensure_ground_truth_lineitem_table`].
const GROUND_TRUTH_LINEITEM_TABLE: &str = "GROUND_TRUTH_LINEITEM";

/// Materialize the `fact_lineitem` columns the ground truth needs into a
/// NATIVE Exasol table (in the same schema as the adapter scripts), via a
/// plain projection over the VS.
///
/// This sidesteps a separate, pre-existing single-table limitation that is
/// explicitly out of scope for this join-focused plan (see its Non-Goals):
/// the single-table grouped-aggregate pushdown (`detect_group_by_aggregates`)
/// declines any select list containing a non-`function_aggregate` item — such
/// as the `ROUND(100.0*SUM(CASE..)/COUNT(*),2)` scalar-over-aggregate used
/// here — and falls back to a raw full-row scan with the wrong column count,
/// hard-failing with "Expected number of columns is 5 but pushdown query has
/// 6" if the scalar-over-aggregate select list were run directly against the
/// virtual `fact_lineitem` table. Projection pushdown (a plain column list,
/// no aggregates) over the VS works fine, so once the base columns are
/// materialized natively, Exasol computes the scalar-over-aggregate itself —
/// correct, and formatted identically to the join wrapper's Exasol-side
/// aggregation, so plain string comparison stays valid.
///
/// `CREATE OR REPLACE TABLE` is idempotent and always rebuilds from the same
/// source VS data, so both scalar-over-aggregate tests can safely share and
/// re-run this under the suite's `--test-threads=1` serial execution.
fn ensure_ground_truth_lineitem_table(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE} AS \
         SELECT L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
        vs_lineitem_table(VS_NAME)
    ));
}

/// The same select list evaluated directly over the natively materialized
/// `fact_lineitem` columns (see [`ensure_ground_truth_lineitem_table`]) — the
/// ground truth both grouped-join queries above must match, since every
/// `fact_lineitem` row appears in exactly one result row of either join,
/// independent of how many tables are joined.
fn scalar_over_aggregate_ground_truth_query() -> String {
    format!(
        "SELECT {} FROM {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE} \
         GROUP BY L_RETURNFLAG HAVING COUNT(*) > 0 ORDER BY 1 LIMIT 2",
        scalar_over_aggregate_select_list("")
    )
}

/// Fetch a scalar-over-aggregate query's 5 result columns
/// (`L_RETURNFLAG, SUM_QTY, RETURN_COUNT, AVG_PRICE, RETURN_PCT`) as a sorted
/// `Vec<Vec<String>>`, reusing [`fetch_rows_as_vecs`]'s order-independent
/// row-major comparison shape.
fn fetch_scalar_over_aggregate_rows(conn: &mut ExaConn, query_sql: &str) -> Vec<Vec<String>> {
    let cols = conn.query_columns(query_sql);
    assert_eq!(
        cols.len(),
        5,
        "expected 5 result columns (L_RETURNFLAG, SUM_QTY, RETURN_COUNT, AVG_PRICE, \
         RETURN_PCT), got {}",
        cols.len()
    );
    fetch_rows_as_vecs(&cols)
}

/// A scalar function wrapping aggregates (`ROUND(100.0 * SUM(CASE …) /
/// COUNT(*), 2)`) in a grouped two-table join select list is rendered, not
/// declined: the query succeeds (no `F-UDF-CL-RUST-9001`), the pushed SQL is
/// the unified N-scan wrapper (N=2, `LHS_T0`/`LHS_T1`) rather than a broadcast
/// join, and the result equals the same select list evaluated over the
/// un-joined `fact_lineitem` table.
#[test]
fn e2e_scalar_over_aggregate_grouped_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = scalar_over_aggregate_join_query(VS_NAME);

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2),
        "a scalar-over-aggregate grouped join must be served by the unified \
         N-scan wrapper (N=2, LHS_T0/LHS_T1), not declined:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a grouped aggregate cannot ride the broadcast in-UDF join — no \
         common-blob join block may appear:\n{pushed}"
    );

    ensure_ground_truth_lineitem_table(&mut conn);
    let actual = fetch_scalar_over_aggregate_rows(&mut conn, &query);
    let expected =
        fetch_scalar_over_aggregate_rows(&mut conn, &scalar_over_aggregate_ground_truth_query());

    assert!(
        !actual.is_empty(),
        "expected at least one L_RETURNFLAG group, got none"
    );
    assert_eq!(
        actual, expected,
        "scalar-over-aggregate grouped two-table join result must equal the \
         same select list evaluated over the un-joined fact_lineitem table.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// The N>=3-table counterpart of
/// [`e2e_scalar_over_aggregate_grouped_join_result_correct`]: the identical
/// scalar-over-aggregate grouped select list over a three-table inner join
/// (`dim_customer ⋈ fact_orders ⋈ fact_lineitem`) is rendered by the SAME
/// unified fallback renderer (N=3, `LHS_T0..LHS_T2`), not declined, and returns
/// the same result as the ground truth (and, transitively, as the two-table
/// case).
#[test]
fn e2e_scalar_over_aggregate_grouped_join_n_table_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = scalar_over_aggregate_n_table_join_query(VS_NAME);

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 3),
        "a scalar-over-aggregate grouped join over three tables must be served \
         by the unified N-scan wrapper (N=3), not declined:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "broadcast stays strictly two-table; a three-table join must never \
         carry a common-blob join block:\n{pushed}"
    );

    ensure_ground_truth_lineitem_table(&mut conn);
    let actual = fetch_scalar_over_aggregate_rows(&mut conn, &query);
    let expected =
        fetch_scalar_over_aggregate_rows(&mut conn, &scalar_over_aggregate_ground_truth_query());

    assert!(
        !actual.is_empty(),
        "expected at least one L_RETURNFLAG group, got none"
    );
    assert_eq!(
        actual, expected,
        "scalar-over-aggregate grouped three-table join result must equal the \
         same select list evaluated over the un-joined fact_lineitem table.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}

// ---------------------------------------------------------------------------
// Declined-filter self-apply at the join render sites (plan
// fix-declined-filter-self-apply, tasks 2.8/2.9, #279). A side-local WHERE
// conjunct DataFusion's dialect cannot render — `SECOND(<col>, 3)`, the same
// 2-argument arity refusal `vs-expression`'s
// second_with_precision_declines_for_datafusion_renders_for_exasol pins —
// must still be applied: at the broadcast site by declining the broadcast
// plan altogether (task 2.3), and at the N-scan site as a residual
// outer-WHERE conjunct alongside a rendering conjunct that still reaches its
// own leg's scan-spec filter (task 2.4). `O_ORDERDATE` is a plain DATE column
// (no time component), so `SECOND(O_ORDERDATE, 3)` is always `0` — verified
// live against the Docker Exasol container (`SELECT SECOND(DATE
// '2024-01-05', 3)` = `0`) — making the declined predicate always-true over
// the seeded data. The correctness assertion is therefore that the declined
// filter costs no rows, not that it narrows them.
// ---------------------------------------------------------------------------

/// A below-threshold two-table inner equi-join with a single declined
/// side-local conjunct (`SECOND(O_ORDERDATE, 3) = 0`, always true for the
/// seeded DATE column) and no postprocessing.
fn broadcast_declined_filter_join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE SECOND(o.O_ORDERDATE, 3) = 0",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// The full (unfiltered) `fact_orders ⋈ dim_customer` join, computed
/// independently of the join pushdown — the ground truth
/// [`broadcast_declined_filter_join_query`] must match, since its sole filter
/// is always true over the seeded data. Same shape as [`expected_join_rows`]
/// with the `O_ORDERDATE` WHERE bound dropped.
fn expected_full_join_rows(conn: &mut ExaConn, vs_name: &str) -> Vec<(String, String)> {
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
        "SELECT O_CUSTKEY, O_ORDERDATE FROM {}",
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

/// A below-threshold two-table inner equi-join carrying a declined side-local
/// WHERE conjunct (`SECOND(O_ORDERDATE, 3)`, a 2-argument arity refusal under
/// the DataFusion dialect) declines the broadcast plan altogether (task 2.3)
/// rather than silently dropping the predicate and riding the broadcast
/// in-UDF join unfiltered: the pushed plan is the N-scan wrapper, never a
/// broadcast common-blob join block, and the result is correct.
#[test]
fn e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = broadcast_declined_filter_join_query(VS_NAME);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2),
        "a declined side-local filter must decline the broadcast plan and fall \
         back to the N-scan wrapper (LHS_T0/LHS_T1), not omit the predicate:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a declined filter must NOT ride the broadcast in-UDF join unfiltered \
         (no common-blob join block may appear):\n{pushed}"
    );
    assert!(
        pushed.contains("SECOND("),
        "the fallback wrapper must self-apply the declined conjunct in its own \
         outer WHERE, not just fall back and drop it:\n{pushed}"
    );

    let cols = conn.query_columns(&query);
    let actual = columns_to_sorted_pairs(&cols);
    let expected = expected_full_join_rows(&mut conn, VS_NAME);

    assert_eq!(
        actual.len(),
        FACT_ORDERS_ROWS,
        "SECOND(O_ORDERDATE, 3) is always 0 for the seeded DATE column, so the \
         declined filter must cost no rows: expected {FACT_ORDERS_ROWS}, got {}: {actual:?}",
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "the N-scan fallback result must equal the independently computed \
         unfiltered join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// A below-threshold two-table inner equi-join carrying a declined side-local
/// WHERE conjunct that is FALSE for every seeded row (`SECOND(O_ORDERDATE, 3)
/// = 1`, since `O_ORDERDATE` is a plain DATE with no time component so
/// `SECOND(..., 3)` is always `0`). Unlike
/// [`e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters`], whose
/// always-true conjunct cannot distinguish "self-applied" from "silently
/// dropped", this always-false conjunct can: a build that dropped the
/// declined predicate instead of self-applying it would return every row,
/// while the correct self-apply excludes them all.
#[test]
fn e2e_broadcast_declined_filter_excludes_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE SECOND(o.O_ORDERDATE, 3) = 1",
        vs_fact_table(VS_NAME),
        vs_dim_table(VS_NAME)
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2),
        "a declined side-local filter must decline the broadcast plan and fall \
         back to the N-scan wrapper (LHS_T0/LHS_T1):\n{pushed}"
    );

    let row_count = conn.query_row_count(&query);
    assert_eq!(
        row_count, 0,
        "SECOND(O_ORDERDATE, 3) = 1 is false for every seeded DATE row, so a \
         correctly self-applied declined conjunct must exclude every row \
         (a build that silently dropped it instead would return all \
         {FACT_ORDERS_ROWS}): got {row_count}"
    );
}

/// A three-table inner equi-join carrying BOTH a rendering side-local
/// conjunct (`O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'`, unchanged from
/// [`join_query`]) and a declined side-local conjunct (`SECOND(O_ORDERDATE,
/// 3) = 0`, always true), both local to the `fact_orders` side.
fn three_table_join_with_mixed_filters_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY FROM {} c \
         JOIN {} o ON c.C_CUSTKEY = o.O_CUSTKEY \
         JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY \
         WHERE o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}' \
         AND SECOND(o.O_ORDERDATE, 3) = 0",
        vs_dim_table(vs_name),
        vs_fact_table(vs_name),
        vs_lineitem_table(vs_name)
    )
}

/// The expected result of [`three_table_join_with_mixed_filters_query`],
/// computed independently of the join pushdown: the same
/// `O_ORDERDATE >= {ORDERDATE_LOWER_BOUND}` bound narrows `fact_orders`
/// before joining against `dim_customer` and `fact_lineitem`; the declined
/// `SECOND(...) = 0` conjunct contributes no additional narrowing (always
/// true for the seeded DATE column), so it is not applied here.
fn expected_three_table_join_rows_with_orderdate_filter(
    conn: &mut ExaConn,
    vs_name: &str,
) -> Vec<Vec<String>> {
    let custkey_to_name = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT C_CUSTKEY, C_NAME FROM {}",
        vs_dim_table(vs_name)
    )));
    let orderkey_to_custkey = build_key_to_value_map(&conn.query_columns(&format!(
        "SELECT O_ORDERKEY, O_CUSTKEY FROM {} WHERE O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'",
        vs_fact_table(vs_name)
    )));

    let line_cols = conn.query_columns(&format!(
        "SELECT L_ORDERKEY, L_LINENUMBER, L_QUANTITY FROM {}",
        vs_lineitem_table(vs_name)
    ));
    assert_eq!(line_cols.len(), 3, "lineitem query must return 3 columns");

    let mut rows: Vec<Vec<String>> = (0..line_cols[0].len())
        .filter_map(|i| {
            let order_key = value_to_string(&line_cols[0][i]);
            let cust_key = orderkey_to_custkey.get(&order_key)?;
            let name = custkey_to_name
                .get(cust_key)
                .unwrap_or_else(|| panic!("O_CUSTKEY {cust_key} has no matching customer"))
                .clone();
            let line_number = value_to_string(&line_cols[1][i]);
            let quantity = value_to_string(&line_cols[2][i]);
            Some(vec![name, line_number, quantity])
        })
        .collect();
    rows.sort();
    rows
}

/// A three-table inner-join whose WHERE carries both a rendering side-local
/// conjunct and a declined side-local conjunct — both local to the
/// `fact_orders` side — is served by the N-scan wrapper with the two
/// conjuncts partitioned correctly (task 2.4): the declined conjunct
/// (`SECOND(..., 3)`) is carried by the outer wrapper's `WHERE` in Exasol
/// dialect (it can only appear there — DataFusion never renders it, so it
/// cannot reach any leg's `ScanSpec.filter`), while the rendering conjunct
/// (`O_ORDERDATE >= DATE '...'`) still reaches its own leg's DataFusion
/// `ScanSpec.filter`, unchanged from the single-conjunct case. The result is
/// correct.
///
/// Exasol canonicalizes the rendering conjunct before the adapter ever sees
/// it — the pushdown request carries `predicate_lessequal(literal_date,
/// column)`, i.e. `DATE '...' <= O_ORDERDATE`, not the `>=` form as written —
/// so the leg's rendered filter is asserted in that canonical (flipped)
/// shape, confirmed against the live EXPLAIN VIRTUAL output.
#[test]
fn e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = three_table_join_with_mixed_filters_query(VS_NAME);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 3),
        "a three-table join must emit the N-scan wrapper with three distinct \
         LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a three-table join must NOT carry a broadcast common-blob join \
         block:\n{pushed}"
    );

    // The declined conjunct is rendered as a verbatim Exasol function call
    // (`SECOND(...)`) — it can appear ONLY in the outer wrapper's WHERE,
    // never inside a leg's DataFusion-rendered `ScanSpec.filter` (that
    // dialect refuses the 2-argument form), so this substring alone proves
    // the declined conjunct was self-applied rather than silently dropped.
    assert!(
        pushed.contains("SECOND("),
        "the declined SECOND(..., 3) conjunct must be rendered as a verbatim \
         Exasol call in the outer wrapper's WHERE:\n{pushed}"
    );
    // The rendering conjunct is rendered under the DataFusion dialect
    // (bare/unqualified, `render_df_filter_safe`) into its leg's
    // `ScanSpec.filter` as a SQL string, itself embedded in the outer JSON
    // scan-spec blob — hence the doubled single-quote (SQL string-literal
    // escaping) around the DATE literal. This exact form cannot appear in
    // Exasol's own echoed pushdown-request JSON, which encodes the DATE
    // literal as a plain `"value" : "2024-01-05"` field, never wrapped in
    // `DATE ''...''`.
    assert!(
        pushed.contains(&format!("DATE ''{ORDERDATE_LOWER_BOUND}''")),
        "the rendering O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}' conjunct \
         must still reach its leg's scan-spec filter:\n{pushed}"
    );

    let cols = conn.query_columns(&query);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 result columns, got {}",
        cols.len()
    );
    let actual = fetch_rows_as_vecs(&cols);
    let expected = expected_three_table_join_rows_with_orderdate_filter(&mut conn, VS_NAME);

    assert!(
        !actual.is_empty(),
        "expected at least one row (orders on/after {ORDERDATE_LOWER_BOUND}), got none"
    );
    assert_eq!(
        actual, expected,
        "the mixed rendering/declined-filter three-table join result must \
         equal the independently computed join.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}
