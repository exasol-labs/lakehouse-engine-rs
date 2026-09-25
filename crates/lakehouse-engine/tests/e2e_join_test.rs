//! E2E inner equi-join pushdown tests: broadcast join and the unaccelerated fallback,
//! which must return identical results. Fail, never skip, without the stack.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    DIM_CUSTOMER_ROWS, E2E_FACT_TABLE, E2E_LINEITEM_TABLE, E2E_NAMESPACE, E2E_SUPPLIER_TABLE,
    FACT_ORDERS_ROWS, LINEITEM_ROWS, O_TOTALPRICE_PS, order_custkey, order_date_days,
    order_totalprice_unscaled, seed_events,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use std::collections::HashMap;
use std::sync::OnceLock;

/// Default broadcast threshold: the small dimension side is broadcast-eligible.
const VS_NAME: &str = "MY_LAKEHOUSE_JOIN";
/// `JOIN_BROADCAST_MAX_BYTES = '1'` forces every join onto the unaccelerated fallback.
const VS_NAME_LOW: &str = "MY_LAKEHOUSE_JOIN_LOW";

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
        // Each `create_virtual_schema` re-issues the catalog CONNECTION idempotently.
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
        create_virtual_schema(
            &mut conn,
            &VsProps::new(VS_NAME_LOW, E2E_NAMESPACE).with_join_broadcast_max_bytes("1"),
        );
    });
}

fn vs_lineitem_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_LINEITEM_TABLE.to_uppercase())
}

fn vs_supplier_table(vs_name: &str) -> String {
    format!("{vs_name}.{}", E2E_SUPPLIER_TABLE.to_uppercase())
}

/// Preserves Exasol's row order, for `ORDER BY` assertions.
fn fetch_join_rows_in_query_order(conn: &mut ExaConn, query_sql: &str) -> Vec<(String, String)> {
    let cols = conn.query_columns(query_sql);
    assert_eq!(
        cols.len(),
        2,
        "expected 2 result columns, got {}: {cols:?}",
        cols.len()
    );
    cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(name, date)| (value_to_string(name), value_to_string(date)))
        .collect()
}

/// Dates are one day apart per order key, so there are no ties and ISO text sorts
/// chronologically.
fn expected_join_rows_by_orderdate_desc(
    conn: &mut ExaConn,
    vs_name: &str,
) -> Vec<(String, String)> {
    let mut rows = expected_join_rows(conn, vs_name);
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    rows
}

/// Scenario: a broadcast-eligible join is pushed as a single scan-UDF broadcast fan-out
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

/// Scenario: the broadcast join matches the join computed from the un-joined tables
#[test]
fn e2e_broadcast_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let actual = fetch_join_rows(&mut conn, VS_NAME);
    let expected = expected_join_rows(&mut conn, VS_NAME);

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

/// Scenario: a bare LIMIT over a broadcast-eligible join stays broadcast and returns exactly n join rows
#[test]
fn e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates() {
    setup_e2e();
    let mut conn = exa_conn();

    const LIMIT_ROWS: usize = 3;
    let query = format!("{} LIMIT {LIMIT_ROWS}", join_query(VS_NAME));

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        pushed.contains("\"numElements\""),
        "the SQL LIMIT must reach the adapter as a pushdown limit, else this test \
         proves nothing about the bare-LIMIT broadcast shape:\n{pushed}"
    );
    assert!(
        has_broadcast_join_block(&pushed),
        "a bare LIMIT must still drive the broadcast fan-out (one scan UDF, \
         common-blob join block):\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "a bare LIMIT must NOT force the two-scan Exasol-joined fallback \
         (LHS_T0/LHS_T1):\n{pushed}"
    );

    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));
    let unbounded = expected_join_rows(&mut conn, VS_NAME);
    assert_eq!(
        actual.len(),
        LIMIT_ROWS,
        "LIMIT {LIMIT_ROWS} must truncate to exactly {LIMIT_ROWS} rows: {actual:?}"
    );
    for row in &actual {
        assert!(
            unbounded.contains(row),
            "truncated row {row:?} must be one of the unbounded join's rows: \
             {unbounded:?}"
        );
    }
}

/// Scenario: ORDER BY … LIMIT over a broadcast join returns the exact top-N via an outer wrapper
#[test]
fn e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    const TOP_N: usize = 3;
    let query = format!(
        "{} ORDER BY o.O_ORDERDATE DESC LIMIT {TOP_N}",
        join_query(VS_NAME)
    );

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_broadcast_join_block(&pushed),
        "ORDER BY ... LIMIT must still drive the broadcast fan-out (one scan \
         UDF, common-blob join block):\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "ORDER BY ... LIMIT must NOT force the two-scan Exasol-joined fallback \
         (LHS_T0/LHS_T1):\n{pushed}"
    );

    let actual = fetch_join_rows_in_query_order(&mut conn, &query);
    let expected: Vec<(String, String)> = expected_join_rows_by_orderdate_desc(&mut conn, VS_NAME)
        .into_iter()
        .take(TOP_N)
        .collect();
    assert_eq!(
        actual, expected,
        "ORDER BY O_ORDERDATE DESC LIMIT {TOP_N} must return the exact top-{TOP_N} \
         rows of the unwindowed ordered join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: division by zero in a broadcast join's fact-leg filter fails the query from inside the broadcast plan (#370)
#[test]
fn e2e_broadcast_join_float_div_by_zero_in_fact_leg_filter_fails() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!(
        "{} AND 0 < o.O_ORDERKEY / (o.O_CUSTKEY - o.O_CUSTKEY)",
        join_query(VS_NAME)
    );

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_broadcast_join_block(&pushed),
        "the fact-leg division must ride INSIDE the broadcast fan-out (one scan \
         UDF, common-blob join block), else the division never reaches the \
         scan:\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "a fact-leg division must NOT force the two-scan Exasol-joined fallback \
         (LHS_T0/LHS_T1), where Exasol would evaluate the division \
         itself:\n{pushed}"
    );
    assert!(
        pushed.contains(vs_expression::CHECKED_FLOAT_DIV_FN),
        "the pushed broadcast plan must carry the checked-division call:\n{pushed}"
    );

    let resp = conn.try_execute(&query);
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a division by zero in a broadcast join's fact-leg filter must fail the \
         query rather than change the joined row count: {resp}"
    );
    let message = resp["exception"]["text"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        message.contains("division by zero"),
        "the surfaced message must name a division by zero: {resp}"
    );
}

/// Scenario: a bare ORDER BY and ORDER BY … LIMIT … OFFSET over the join stay broadcast
#[test]
fn e2e_broadcast_join_order_by_without_limit_and_with_offset_stay_broadcast() {
    setup_e2e();
    let mut conn = exa_conn();

    let unlimited_query = format!("{} ORDER BY o.O_ORDERDATE DESC", join_query(VS_NAME));
    let pushed = explain_virtual_sql(&mut conn, &unlimited_query);
    assert!(
        has_broadcast_join_block(&pushed),
        "a bare ORDER BY (no LIMIT) must still drive the broadcast fan-out:\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "a bare ORDER BY (no LIMIT) must NOT force the two-scan fallback:\n{pushed}"
    );

    let actual_unlimited = fetch_join_rows_in_query_order(&mut conn, &unlimited_query);
    let expected_ordered = expected_join_rows_by_orderdate_desc(&mut conn, VS_NAME);
    assert_eq!(
        actual_unlimited, expected_ordered,
        "a bare ORDER BY (no LIMIT) must return the full join, ordered by \
         O_ORDERDATE DESC.\nactual:   {actual_unlimited:?}\nexpected: {expected_ordered:?}"
    );

    const WINDOW_LIMIT: usize = 5;
    const WINDOW_OFFSET: usize = 3;
    let windowed_query = format!(
        "{} ORDER BY o.O_ORDERDATE DESC LIMIT {WINDOW_LIMIT} OFFSET {WINDOW_OFFSET}",
        join_query(VS_NAME)
    );
    let pushed = explain_virtual_sql(&mut conn, &windowed_query);
    assert!(
        has_broadcast_join_block(&pushed),
        "ORDER BY ... LIMIT ... OFFSET ... must still drive the broadcast \
         fan-out:\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "ORDER BY ... LIMIT ... OFFSET ... must NOT force the two-scan \
         fallback:\n{pushed}"
    );

    let actual_windowed = fetch_join_rows_in_query_order(&mut conn, &windowed_query);
    let expected_windowed: Vec<(String, String)> = expected_ordered
        .into_iter()
        .skip(WINDOW_OFFSET)
        .take(WINDOW_LIMIT)
        .collect();
    assert!(
        !expected_windowed.is_empty(),
        "the offset window must be non-empty, else this test proves nothing \
         about the exact window"
    );
    assert_eq!(
        actual_windowed, expected_windowed,
        "ORDER BY O_ORDERDATE DESC LIMIT {WINDOW_LIMIT} OFFSET {WINDOW_OFFSET} must \
         return the exact offset window of the unwindowed ordered join.\n\
         actual:   {actual_windowed:?}\nexpected: {expected_windowed:?}"
    );
}

/// Scenario: an aggregate over the join falls back to two-scan, and Exasol rejects LIMIT … OFFSET without ORDER BY before pushdown
#[test]
fn e2e_join_offset_and_aggregate_shapes_still_use_two_scan_fallback() {
    setup_e2e();
    let mut conn = exa_conn();

    let offset_without_order_by = format!("{} LIMIT 3 OFFSET 2", join_query(VS_NAME));
    let resp = conn.try_execute(&offset_without_order_by);
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "LIMIT ... OFFSET ... with no ORDER BY must be rejected by Exasol itself \
         (the adapter is never consulted), got: {resp}"
    );
    assert_eq!(
        resp["exception"]["sqlCode"].as_str(),
        Some("42000"),
        "expected sqlCode 42000, got: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("OFFSET") && msg.contains("ORDER BY"),
        "expected Exasol's 'OFFSET not allowed in LIMIT without ORDER BY' \
         message, got: {msg}"
    );

    let pushed = explain_virtual_sql(&mut conn, &aggregate_join_query(VS_NAME));
    assert!(
        has_two_scan_wrapper(&pushed),
        "an aggregate over a join must still fall back to the two-scan wrapper \
         (LHS_T0/LHS_T1):\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "an aggregate over a join must NOT carry a broadcast common-blob join \
         block:\n{pushed}"
    );
}

/// Scenario: above the broadcast threshold the join uses the two-scan `LHS_T0`/`LHS_T1` wrapper
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

/// Scenario: the above-threshold fallback returns the same result as the broadcast path
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

// An aggregate over a join is served by the two-scan wrapper with Exasol aggregating
// the materialized join, never by the broadcast in-UDF join.

fn aggregate_join_query(vs_name: &str) -> String {
    format!(
        "SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// Scenario: an aggregate over a join routes to the two-scan wrapper even on the broadcast-eligible VS
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

/// Scenario: an aggregate over a join equals the same aggregate over the fact table on both VSs
#[test]
fn e2e_aggregate_over_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

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

// Every line item references exactly one order and one supplier, so the 3- and 4-table
// joins both yield every `fact_lineitem` row.

fn fetch_rows_as_vecs(cols: &[Vec<serde_json::Value>]) -> Vec<Vec<String>> {
    let row_count = cols.first().map_or(0, Vec::len);
    let mut rows: Vec<Vec<String>> = (0..row_count)
        .map(|i| cols.iter().map(|col| value_to_string(&col[i])).collect())
        .collect();
    rows.sort();
    rows
}

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

/// Ground truth independent of join pushdown: tables read un-joined and joined in-process.
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

/// Scenario: a three-table join succeeds via the N-scan wrapper and matches the independently computed result (#76)
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

/// Scenario: a four-table join succeeds via the N-scan wrapper and matches the independently computed result
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

// A scalar function wrapping aggregates in a grouped join select list must be rendered,
// not declined. Every `fact_lineitem` row matches exactly one order and customer, so
// the grouped result must equal the same select list over `fact_lineitem` alone.

/// `col_prefix` is a table alias like `"l."`, or `""` for a single-table query.
fn scalar_over_aggregate_select_list(col_prefix: &str) -> String {
    format!(
        "{col_prefix}L_RETURNFLAG, \
         SUM({col_prefix}L_QUANTITY) AS SUM_QTY, \
         SUM(CASE WHEN {col_prefix}L_RETURNFLAG = 'R' THEN 1 ELSE 0 END) AS RETURN_COUNT, \
         AVG({col_prefix}L_EXTENDEDPRICE) AS AVG_PRICE, \
         ROUND(100.0 * SUM(CASE WHEN {col_prefix}L_RETURNFLAG = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2) AS RETURN_PCT"
    )
}

fn scalar_over_aggregate_join_query(vs_name: &str) -> String {
    format!(
        "SELECT {} FROM {} o JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY \
         GROUP BY l.L_RETURNFLAG HAVING COUNT(*) > 0 ORDER BY 1 LIMIT 2",
        scalar_over_aggregate_select_list("l."),
        vs_fact_table(vs_name),
        vs_lineitem_table(vs_name)
    )
}

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

const GROUND_TRUTH_LINEITEM_TABLE: &str = "GROUND_TRUTH_LINEITEM";

/// Materialized natively so Exasol computes the ground truth independently of the
/// pushdown path, formatted identically to the wrapper's Exasol-side aggregation.
fn ensure_ground_truth_lineitem_table(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE} AS \
         SELECT L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
        vs_lineitem_table(VS_NAME)
    ));
}

fn scalar_over_aggregate_ground_truth_query() -> String {
    format!(
        "SELECT {} FROM {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE} \
         GROUP BY L_RETURNFLAG HAVING COUNT(*) > 0 ORDER BY 1 LIMIT 2",
        scalar_over_aggregate_select_list("")
    )
}

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

/// Scenario: a scalar over aggregates in a grouped two-table join select list is rendered via the N-scan wrapper and correct
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

/// Scenario: a scalar over aggregates in a grouped three-table join select list is rendered via the N-scan wrapper and correct
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

// A side-local conjunct DataFusion cannot render (`SECOND(<col>, 3)`) must still be
// applied: the broadcast plan is declined, and the N-scan wrapper applies it in the
// outer WHERE. `O_ORDERDATE` is a DATE, so `SECOND(O_ORDERDATE, 3)` is always 0
// (verified live).

fn broadcast_declined_filter_join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE SECOND(o.O_ORDERDATE, 3) = 0",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// The declined filter in [`broadcast_declined_filter_join_query`] is always true, so
/// the full join is its ground truth.
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

/// Scenario: a declined side-local conjunct declines the broadcast plan and falls back to the N-scan wrapper with correct rows
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

/// Scenario: an always-false declined conjunct returns no rows, proving it is self-applied rather than dropped
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

/// The declined `SECOND(...) = 0` conjunct is always true, so it is not applied here.
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

/// Scenario: mixed rendering and declined conjuncts split between the leg filter and the outer wrapper WHERE
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

    // DataFusion refuses the 2-argument `SECOND`, so it can only appear in the outer
    // wrapper's WHERE: this proves the conjunct was self-applied, not dropped.
    assert!(
        pushed.contains("SECOND("),
        "the declined SECOND(..., 3) conjunct must be rendered as a verbatim \
         Exasol call in the outer wrapper's WHERE:\n{pushed}"
    );
    // The doubled quotes come from the leg filter being embedded in the scan-spec blob;
    // Exasol's echoed request encodes the DATE literal as a plain `"value"` field instead.
    // Exasol canonicalizes `>=` to `DATE '...' <= O_ORDERDATE` before the adapter sees it.
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

// `apply_type_rewrites` runs at both join WHERE sites: each case renders correctly or
// declines and self-applies (#215, #223, #228, #285).

/// `like_subject_type_guard` has no DECIMAL coercion, so this declines the broadcast plan.
fn like_on_custkey_join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE o.O_CUSTKEY LIKE '1%'",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// The subject is rewrapped as `CAST(<col> AS VARCHAR)`, so the broadcast plan survives.
/// Expected rows run the same pattern through the single-table WHERE path, so the
/// result is independent of the session's `NLS_DATE_FORMAT`.
fn like_on_orderdate_join_query(vs_name: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE o.O_ORDERDATE LIKE '2024-01-0%'",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// Scenario: LIKE over a DECIMAL column declines the broadcast plan and is self-applied in the wrapper WHERE
#[test]
fn e2e_broadcast_like_on_decimal_column_falls_back_and_filters() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = like_on_custkey_join_query(VS_NAME);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_two_scan_wrapper(&pushed),
        "LIKE over the DECIMAL O_CUSTKEY column must decline the broadcast \
         plan and fall back to the N-scan wrapper (LHS_T0/LHS_T1):\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "LIKE over a DECIMAL side column must NOT ride the broadcast in-UDF \
         join unfiltered (no common-blob join block may appear):\n{pushed}"
    );
    assert!(
        !pushed.contains(r#""filter":""#),
        "a type-declined LIKE-over-DECIMAL conjunct must not reach any leg's \
         scan-spec filter:\n{pushed}"
    );
    assert!(
        pushed.contains("LIKE"),
        "the type-declined conjunct must still be self-applied in the \
         wrapper's own outer WHERE, not dropped:\n{pushed}"
    );

    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));
    let expected = expected_join_rows_with_fact_where(&mut conn, VS_NAME, "O_CUSTKEY LIKE '1%'");
    assert!(
        !expected.is_empty(),
        "expected at least one row for O_CUSTKEY LIKE '1%' (order keys 1 and \
         6 reference customer 1)"
    );
    assert_eq!(
        actual, expected,
        "the N-scan fallback result must equal the independently computed \
         (single-table ground truth) join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: LIKE over a DATE column keeps the broadcast plan with a CAST-rewritten filter
#[test]
fn e2e_broadcast_like_on_date_column_stays_broadcast_and_filters() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = like_on_orderdate_join_query(VS_NAME);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_broadcast_join_block(&pushed),
        "LIKE over a DATE side column must KEEP the broadcast plan (the CAST \
         rewrite still renders for DataFusion):\n{pushed}"
    );
    assert!(
        pushed.contains("CAST(") && pushed.contains("O_ORDERDATE"),
        "the DATE LIKE subject must be rewrapped in CAST(...) before the \
         LIKE:\n{pushed}"
    );

    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));
    let expected =
        expected_join_rows_with_fact_where(&mut conn, VS_NAME, "O_ORDERDATE LIKE '2024-01-0%'");
    assert!(
        !expected.is_empty() && expected.len() < FACT_ORDERS_ROWS,
        "O_ORDERDATE LIKE '2024-01-0%' must genuinely narrow the \
         {FACT_ORDERS_ROWS}-row fact table (order 10, 2024-01-10, must be \
         excluded), got {} matching rows",
        expected.len()
    );
    assert_eq!(
        actual, expected,
        "the broadcast join result must equal the independently computed \
         (single-table ground truth) join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: on the forced-fallback VS, LIKE over a DECIMAL column is screened out of its leg and applied only in the wrapper WHERE
#[test]
fn e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = like_on_custkey_join_query(VS_NAME_LOW);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_two_scan_wrapper(&pushed),
        "VS_NAME_LOW must always emit the N-scan wrapper (LHS_T0/LHS_T1):\n{pushed}"
    );
    assert!(
        !pushed.contains(r#""filter":""#),
        "a type-declined LIKE-over-DECIMAL side-local conjunct must not reach \
         its leg's scan-spec filter:\n{pushed}"
    );
    assert!(
        pushed.contains("LIKE"),
        "the type-declined conjunct must still be self-applied in the \
         wrapper's own outer WHERE, not dropped:\n{pushed}"
    );

    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));
    let expected =
        expected_join_rows_with_fact_where(&mut conn, VS_NAME_LOW, "O_CUSTKEY LIKE '1%'");
    assert!(
        !expected.is_empty(),
        "expected at least one row for O_CUSTKEY LIKE '1%' (order keys 1 and \
         6 reference customer 1)"
    );
    assert_eq!(
        actual, expected,
        "the N-scan result must equal the independently computed (single-table \
         ground truth) join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: a three-argument INSTR in a join filter declines and Exasol evaluates it natively (#228)
#[test]
fn e2e_join_instr_with_start_position_returns_native_result() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE INSTR(c.C_NAME, 'c', 3) = 0",
        vs_fact_table(VS_NAME),
        vs_dim_table(VS_NAME)
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_two_scan_wrapper(&pushed),
        "the 3-argument INSTR must decline the broadcast plan and fall through \
         to the N-scan wrapper:\n{pushed}"
    );
    // `'c'` occurs only at position 1, so native `INSTR(C_NAME, 'c', 3)` is 0 for every row;
    // a `strpos` rewrite dropping the start position would answer 1 and return no rows.
    assert!(
        pushed.contains(r#"INSTR("LHS_T1"."C_NAME", 'c', 3)"#),
        "the outer WHERE must carry the verbatim 3-argument INSTR call with the \
         literal start position 3:\n{pushed}"
    );
    assert!(
        !pushed.contains("strpos("),
        "the declined INSTR must never reach a leg's DataFusion-dialect render, \
         which would drop the start-position argument:\n{pushed}"
    );
    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));
    let expected = expected_full_join_rows(&mut conn, VS_NAME);

    assert_eq!(
        actual.len(),
        FACT_ORDERS_ROWS,
        "INSTR(C_NAME, 'c', 3) = 0 must hold for every seeded customer (no \
         'c' at or after position 3 in 'customer-0N'), so the native result \
         must be all {FACT_ORDERS_ROWS} rows; a start-position-ignoring strpos \
         rewrite would instead find 'c' at position 1 and return 0 rows: got {}",
        actual.len()
    );
    assert_eq!(
        actual, expected,
        "the join result under the natively-evaluated INSTR filter must equal \
         the unfiltered join.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Exasol drops an all-zero fractional part (#211); asserts the seed invariant so a
/// non-zero scale digit fails here rather than silently changing the oracle.
fn expected_totalprice_text(order_key: usize) -> String {
    let divisor = 10_i64
        .pow(u32::try_from(O_TOTALPRICE_PS.1).expect("the O_TOTALPRICE scale is non-negative"));
    let unscaled = order_totalprice_unscaled(order_key);
    assert_eq!(
        unscaled % divisor,
        0,
        "seed invariant: every O_TOTALPRICE must have an all-zero fractional part, \
         so its trimmed text is the integer part alone; order {order_key} \
         (unscaled {unscaled}, scale {}) breaks it",
        O_TOTALPRICE_PS.1
    );
    (unscaled / divisor).to_string()
}

fn expected_orderdate_text(order_key: usize) -> String {
    const SECONDS_PER_DAY: i64 = 86_400;
    chrono::DateTime::from_timestamp(i64::from(order_date_days(order_key)) * SECONDS_PER_DAY, 0)
        .expect("a seeded O_ORDERDATE is a representable days-since-epoch value")
        .format("%Y-%m-%d")
        .to_string()
}

/// Scenario: a DECIMAL-stringifying join filter matches Exasol's trimmed LENGTH semantics on both join surfaces (#223)
#[test]
fn e2e_join_decimal_stringification_matches_native_at_both_surfaces() {
    setup_e2e();
    let mut conn = exa_conn();
    let where_clause = "LENGTH(O_TOTALPRICE) > 3";

    let mut expected: Vec<(String, String)> = (1..=FACT_ORDERS_ROWS)
        .filter(|&key| expected_totalprice_text(key).len() > 3)
        .map(|key| {
            (
                format!("customer-{:02}", order_custkey(key)),
                expected_orderdate_text(key),
            )
        })
        .collect();
    expected.sort();

    assert_eq!(
        expected.len(),
        4,
        "seed invariant: exactly 4 of the {FACT_ORDERS_ROWS} orders (keys 7-10, \
         trimmed 2912/5120/7290/10000) may have a trimmed O_TOTALPRICE text \
         longer than 3 characters — otherwise the filter no longer discriminates \
         trimmed from untrimmed text and this test proves nothing: {expected:?}"
    );

    for vs_name in [VS_NAME, VS_NAME_LOW] {
        let query = format!(
            "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
             JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
             WHERE {where_clause}",
            vs_fact_table(vs_name),
            vs_dim_table(vs_name)
        );
        let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

        assert_eq!(
            actual, expected,
            "{vs_name}: the join result under a DECIMAL-stringification WHERE \
             filter must equal the seed-derived expectation.\nactual:   \
             {actual:?}\nexpected: {expected:?}"
        );
    }
}

fn fetch_order_rows(conn: &mut ExaConn) -> Vec<(String, String)> {
    let cols = conn.query_columns(&format!(
        "SELECT O_ORDERKEY, O_CUSTKEY FROM {}",
        vs_fact_table(VS_NAME)
    ));
    assert_eq!(
        cols.len(),
        2,
        "expected 2 result columns, got {}",
        cols.len()
    );
    cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(key, custkey)| (value_to_string(key), value_to_string(custkey)))
        .collect()
}

/// Scenario: a two-leg self-join on the unique key matches each row only to itself (#361)
#[test]
fn e2e_self_join_on_primitive_column_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();
    let fact = vs_fact_table(VS_NAME);

    let query = format!(
        "SELECT a.O_ORDERKEY, a.O_CUSTKEY FROM {fact} a JOIN {fact} b \
         ON a.O_ORDERKEY = b.O_ORDERKEY"
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2),
        "a two-leg self-join must emit the N-scan wrapper with two distinct \
         LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a self-join must NOT ride the broadcast in-UDF join (self-joins are \
         never broadcast-eligible):\n{pushed}"
    );
    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

    let mut expected = fetch_order_rows(&mut conn);
    expected.sort();

    assert_eq!(
        actual.len(),
        FACT_ORDERS_ROWS,
        "expected {FACT_ORDERS_ROWS} self-matched rows (O_ORDERKEY is unique \
         per row), not the pre-fix cross product of {}: {actual:?}",
        FACT_ORDERS_ROWS * FACT_ORDERS_ROWS
    );
    assert_eq!(
        actual, expected,
        "self-join on the unique O_ORDERKEY must equal each row matched only \
         with itself, computed independently by reading fact_orders \
         un-joined.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: a self-join with one unaliased occurrence resolves to two distinct legs (#361)
#[test]
fn e2e_self_join_with_one_unaliased_occurrence_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();
    let fact = vs_fact_table(VS_NAME);
    let bare = E2E_FACT_TABLE.to_uppercase();

    let query = format!(
        "SELECT {bare}.O_ORDERKEY, b.O_ORDERKEY FROM {fact} JOIN {fact} b \
         ON {bare}.O_CUSTKEY = b.O_CUSTKEY"
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2),
        "a self-join with one unaliased occurrence must emit the N-scan \
         wrapper with two distinct LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a self-join must NOT ride the broadcast in-UDF join (self-joins are \
         never broadcast-eligible):\n{pushed}"
    );
    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

    let orders = fetch_order_rows(&mut conn);
    let mut expected: Vec<(String, String)> = orders
        .iter()
        .flat_map(|(a_key, a_cust)| {
            orders
                .iter()
                .filter(move |(_, b_cust)| b_cust == a_cust)
                .map(move |(b_key, _)| (a_key.clone(), b_key.clone()))
        })
        .collect();
    expected.sort();

    assert_eq!(
        expected.len(),
        FACT_ORDERS_ROWS * (FACT_ORDERS_ROWS / DIM_CUSTOMER_ROWS),
        "seed invariant: {FACT_ORDERS_ROWS} orders over {DIM_CUSTOMER_ROWS} \
         customers must form {DIM_CUSTOMER_ROWS} groups of 2, each \
         contributing 4 self-matched pairs (20 total), not the pre-fix cross \
         product of {}: {expected:?}",
        FACT_ORDERS_ROWS * FACT_ORDERS_ROWS
    );
    assert_eq!(
        actual, expected,
        "a self-join with one unaliased occurrence must equal the pairs \
         sharing O_CUSTKEY, computed independently by reading fact_orders \
         un-joined.\nactual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: a three-leg self-join attaches each condition to its own join point (#361)
#[test]
fn e2e_three_leg_self_join_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();
    let fact = vs_fact_table(VS_NAME);

    let query = format!(
        "SELECT a.O_ORDERKEY, a.O_CUSTKEY FROM {fact} a \
         JOIN {fact} b ON a.O_ORDERKEY = b.O_ORDERKEY \
         JOIN {fact} c ON b.O_ORDERKEY = c.O_ORDERKEY"
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 3),
        "a three-leg self-join must emit the N-scan wrapper with three \
         distinct LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "a three-leg self-join must NOT emit the two-table LHS_T0/LHS_T1 \
         wrapper:\n{pushed}"
    );
    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

    let mut expected = fetch_order_rows(&mut conn);
    expected.sort();

    assert_eq!(
        actual.len(),
        FACT_ORDERS_ROWS,
        "expected {FACT_ORDERS_ROWS} self-matched rows, not the pre-fix \
         three-way cross product of {}: {actual:?}",
        FACT_ORDERS_ROWS * FACT_ORDERS_ROWS * FACT_ORDERS_ROWS
    );
    assert_eq!(
        actual, expected,
        "a three-leg self-join on the unique O_ORDERKEY must equal each row \
         matched only with itself at both join points, computed \
         independently by reading fact_orders un-joined.\n\
         actual:   {actual:?}\nexpected: {expected:?}"
    );
}

/// Scenario: a WHERE conjunct on one self-join alias is pushed only into that occurrence's leg (#361)
#[test]
fn e2e_self_join_with_one_sided_filter_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();
    let fact = vs_fact_table(VS_NAME);
    let threshold = DIM_CUSTOMER_ROWS as i64;

    let query = format!(
        "SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {fact} a JOIN {fact} b \
         ON a.O_CUSTKEY = b.O_CUSTKEY WHERE a.O_ORDERKEY <= {threshold}"
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2),
        "a self-join with a one-sided WHERE conjunct must emit the N-scan \
         wrapper with two distinct LHS_T* fan-out aliases:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "a self-join must NOT ride the broadcast in-UDF join (self-joins are \
         never broadcast-eligible):\n{pushed}"
    );
    let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

    let orders = fetch_order_rows(&mut conn);
    let mut expected: Vec<(String, String)> = orders
        .iter()
        .filter(|(a_key, _)| a_key.parse::<i64>().expect("O_ORDERKEY is numeric") <= threshold)
        .flat_map(|(a_key, a_cust)| {
            orders
                .iter()
                .filter(move |(_, b_cust)| b_cust == a_cust)
                .map(move |(b_key, _)| (a_key.clone(), b_key.clone()))
        })
        .collect();
    expected.sort();

    assert!(
        !expected.is_empty(),
        "the WHERE conjunct must leave a non-empty result, else this test \
         proves nothing about leg-local filtering: {expected:?}"
    );
    assert_eq!(
        actual, expected,
        "a WHERE conjunct local to alias `a` must restrict only `a`'s rows, \
         leaving `b` free to match any row sharing O_CUSTKEY — including \
         rows the filter excludes for `a`.\nactual:   {actual:?}\n\
         expected: {expected:?}"
    );
}

/// Scenario: an ungrouped scalar over an aggregate on a broadcast-eligible join routes to the N-scan wrapper and returns one row
#[test]
fn e2e_scalar_over_aggregate_ungrouped_join_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!(
        "SELECT ROUND(SUM(l.L_QUANTITY), 2) FROM {} o \
         JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY",
        vs_fact_table(VS_NAME),
        vs_lineitem_table(VS_NAME)
    );

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_two_scan_wrapper(&pushed),
        "an ungrouped scalar-over-aggregate join must be served by the N-scan \
         wrapper (N=2, LHS_T0/LHS_T1) so Exasol aggregates over the join:\n{pushed}"
    );
    assert!(
        !has_broadcast_join_block(&pushed),
        "an aggregate — even one hidden inside a scalar function — cannot ride \
         the broadcast in-UDF join, which renders projection only:\n{pushed}"
    );

    ensure_ground_truth_lineitem_table(&mut conn);
    let actual = conn.query_columns(&query);
    assert_eq!(actual.len(), 1, "expected 1 column: {actual:?}");
    assert_eq!(
        actual[0].len(),
        1,
        "an ungrouped aggregate over a join must return exactly ONE row, not one \
         partial row per shard: {actual:?}"
    );

    let expected = conn.query_columns(&format!(
        "SELECT ROUND(SUM(L_QUANTITY), 2) FROM {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE}"
    ));
    let (got, want) = (parse_numeric(&actual[0][0]), parse_numeric(&expected[0][0]));
    assert!(
        (got - want).abs() <= 1e-9 * want.abs().max(1.0),
        "ROUND(SUM(L_QUANTITY), 2) over the join must equal the native oracle \
         {want}, got {got}"
    );
}
