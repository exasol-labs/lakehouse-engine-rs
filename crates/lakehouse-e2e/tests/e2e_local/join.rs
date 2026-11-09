//! E2E inner equi-join pushdown tests: broadcast join and the unaccelerated fallback,
//! which must return identical results. Fail, never skip, without the stack.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::pruning_cases::read_footer;
use crate::common::seed::{
    CLUSTERED_DIM_TABLE, CLUSTERED_FACT_TABLE, CLUSTERED_HOT_KEYS, CLUSTERED_JOIN_NAMESPACE,
    CLUSTERED_KEYS, CLUSTERED_ROWS_PER_KEY, DIM_CUSTOMER_ROWS, E2E_BLANK_LABEL_TABLE,
    E2E_FACT_TABLE, E2E_LINEITEM_TABLE, E2E_NAMESPACE, E2E_SUPPLIER_TABLE, FACT_ORDERS_ROWS,
    LINEITEM_ROWS, O_TOTALPRICE_PS, SEED_LABELS_ROWS, labels_score, order_custkey, order_date_days,
    order_totalprice_unscaled, seed_clustered_join,
};
use crate::common::stack::iceberg_catalog_url;
use crate::fixtures::{events, stack, vs_table};

use parquet::file::statistics::Statistics;

use std::collections::HashMap;
use std::sync::OnceLock;

/// Default broadcast threshold: the small dimension side is broadcast-eligible.
const VS_NAME: &str = "MY_LAKEHOUSE_JOIN";
/// `JOIN_BROADCAST_MAX_BYTES = '1'` forces every join onto the unaccelerated fallback.
const VS_NAME_LOW: &str = "MY_LAKEHOUSE_JOIN_LOW";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        stack();
        events();
        let mut conn = exa_conn();
        // Each `create_virtual_schema` re-issues the catalog CONNECTION idempotently.
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
        create_virtual_schema(
            &mut conn,
            &VsProps::new(VS_NAME_LOW, E2E_NAMESPACE).with_join_broadcast_max_bytes("1"),
        );
    });
}

fn blank_label_join_query(vs_name: &str, order_by: &str, limit: usize) -> String {
    format!(
        "SELECT b.B_LABEL, o.O_ORDERKEY FROM {} o \
         JOIN {} b ON o.O_CUSTKEY = b.B_CUSTKEY \
         ORDER BY {order_by} LIMIT {limit}",
        vs_fact_table(vs_name),
        vs_table(vs_name, E2E_BLANK_LABEL_TABLE)
    )
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
        .map(|(first, second)| (value_to_string(first), value_to_string(second)))
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

/// Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out
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

/// Scenario: A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap
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
    assert!(
        pushed.contains("\"post_join_order_by\""),
        "a zero-offset ORDER BY ... LIMIT must bound each shard with a post-join \
         ordering:\n{pushed}"
    );
    assert!(
        pushed.contains(&format!("\"post_join_limit\":{TOP_N}")),
        "a zero-offset ORDER BY ... LIMIT must bound each shard to its own \
         top-{TOP_N}:\n{pushed}"
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
        pushed.contains(CHECKED_FLOAT_DIV_FN),
        "the pushed broadcast plan must carry the checked-division call:\n{pushed}"
    );

    let resp = conn.try_execute(&query);
    let message = error_text(
        &resp,
        &format!("a fact-leg division by zero in a broadcast join ({query})"),
    )
    .to_ascii_lowercase();
    assert!(
        message.contains("division by zero"),
        "the surfaced message must name a division by zero: {resp}"
    );
}

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
    assert!(
        !pushed.contains("\"post_join_order_by\"") && !pushed.contains("\"post_join_limit\""),
        "a bare ORDER BY with no LIMIT must leave every shard unbounded and \
         unsorted, since only the wrapper carries the window:\n{pushed}"
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
    assert!(
        !pushed.contains("\"post_join_order_by\"") && !pushed.contains("\"post_join_limit\""),
        "a non-zero OFFSET must leave every shard unbounded and unsorted, since \
         a per-shard OFFSET does not compose:\n{pushed}"
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

#[test]
fn e2e_broadcast_join_top_n_ranks_empty_string_as_null() {
    setup_e2e();
    let mut conn = exa_conn();

    for (order_by, expected_rows) in [
        (
            "b.B_LABEL ASC NULLS LAST, o.O_ORDERKEY ASC",
            vec![
                ("alpha".to_string(), "3".to_string()),
                ("alpha".to_string(), "8".to_string()),
            ],
        ),
        (
            "b.B_LABEL DESC NULLS FIRST, o.O_ORDERKEY ASC",
            // The NULL label reads back as the text "null"; the tie breaks across both blank
            // custkeys, so orders 1 and 2 (not 1 and 6) survive.
            vec![
                ("null".to_string(), "1".to_string()),
                ("null".to_string(), "2".to_string()),
            ],
        ),
    ] {
        let broadcast_query = blank_label_join_query(VS_NAME, order_by, 2);
        let fallback_query = blank_label_join_query(VS_NAME_LOW, order_by, 2);

        let pushed = explain_virtual_sql(&mut conn, &broadcast_query);
        assert!(
            has_broadcast_join_block(&pushed),
            "a zero-offset ORDER BY ... LIMIT over dim_blank_label must still \
             drive the broadcast fan-out:\n{pushed}"
        );
        assert!(
            pushed.contains("\"post_join_order_by\""),
            "the broadcast plan must bound each shard with a post-join \
             ordering:\n{pushed}"
        );

        let pushed_low = explain_virtual_sql(&mut conn, &fallback_query);
        assert!(
            has_two_scan_wrapper(&pushed_low),
            "above the broadcast threshold this join must fall back to the \
             two-scan wrapper:\n{pushed_low}"
        );

        let broadcast_rows = fetch_join_rows_in_query_order(&mut conn, &broadcast_query);
        let fallback_rows = fetch_join_rows_in_query_order(&mut conn, &fallback_query);
        assert_eq!(
            broadcast_rows, fallback_rows,
            "the broadcast and two-scan plans must return the same rows in the \
             same order:\nbroadcast: {broadcast_rows:?}\nfallback:  {fallback_rows:?}"
        );
        assert_eq!(
            broadcast_rows, expected_rows,
            "order_by={order_by:?} must return the exact top-2 rows: {broadcast_rows:?}"
        );
    }
}

#[test]
fn e2e_broadcast_join_top_n_keeps_the_single_node_sort_keys_across_a_tie() {
    setup_e2e();
    let mut conn = exa_conn();

    let tie_query = blank_label_join_query(VS_NAME, "b.B_LABEL ASC NULLS LAST", 3);
    let pushed_tie = explain_virtual_sql(&mut conn, &tie_query);
    assert!(
        pushed_tie.contains("\"post_join_order_by\""),
        "the tie-case plan must bound each shard with a post-join ordering:\n{pushed_tie}"
    );

    let tie_rows = fetch_join_rows_in_query_order(&mut conn, &tie_query);
    assert_eq!(
        tie_rows.len(),
        3,
        "LIMIT 3 must return exactly 3 rows: {tie_rows:?}"
    );
    let labels: Vec<&str> = tie_rows.iter().map(|(l, _)| l.as_str()).collect();
    assert_eq!(
        labels,
        vec!["alpha", "alpha", "bravo"],
        "the tie-case label sequence must be alpha, alpha, bravo: {tie_rows:?}"
    );
    let first_two: std::collections::BTreeSet<&str> =
        tie_rows[..2].iter().map(|(_, k)| k.as_str()).collect();
    assert_eq!(
        first_two,
        std::collections::BTreeSet::from(["3", "8"]),
        "the first two rows must be orders 3 and 8, in either order: {tie_rows:?}"
    );
    assert!(
        tie_rows[2].1 == "4" || tie_rows[2].1 == "9",
        "the third row must be one of the two tied bravo rows (orders 4 or 9): \
         {tie_rows:?}"
    );

    let tie_rows_low = fetch_join_rows_in_query_order(
        &mut conn,
        &blank_label_join_query(VS_NAME_LOW, "b.B_LABEL ASC NULLS LAST", 3),
    );
    let labels_low: Vec<&str> = tie_rows_low.iter().map(|(l, _)| l.as_str()).collect();
    assert_eq!(
        labels_low, labels,
        "VS_NAME_LOW must return the same label sequence: {tie_rows_low:?}"
    );
}

#[test]
fn e2e_join_offset_and_aggregate_shapes_still_use_two_scan_fallback() {
    setup_e2e();
    let mut conn = exa_conn();

    let offset_without_order_by = format!("{} LIMIT 3 OFFSET 2", join_query(VS_NAME));
    let resp = conn.try_execute(&offset_without_order_by);
    let msg = error_text(
        &resp,
        &format!("LIMIT ... OFFSET with no ORDER BY ({offset_without_order_by})"),
    );
    assert_eq!(
        resp["exception"]["sqlCode"].as_str(),
        Some("42000"),
        "expected sqlCode 42000, got: {resp}"
    );
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

/// Scenario: Small-side selection uses table-format metadata and the broadcast threshold
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
        vs_table(vs_name, E2E_LINEITEM_TABLE)
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
        vs_table(vs_name, E2E_LINEITEM_TABLE),
        vs_table(vs_name, E2E_SUPPLIER_TABLE)
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
        vs_table(vs_name, E2E_LINEITEM_TABLE)
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
        vs_table(vs_name, E2E_SUPPLIER_TABLE)
    )));

    let line_cols = conn.query_columns(&format!(
        "SELECT L_ORDERKEY, L_LINENUMBER, L_QUANTITY, L_SUPPKEY FROM {}",
        vs_table(vs_name, E2E_LINEITEM_TABLE)
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
        vs_table(vs_name, E2E_LINEITEM_TABLE)
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
        vs_table(vs_name, E2E_LINEITEM_TABLE)
    )
}

const GROUND_TRUTH_LINEITEM_TABLE: &str = "GROUND_TRUTH_LINEITEM";

/// Materialized natively so Exasol computes the ground truth independently of the
/// pushdown path, formatted identically to the wrapper's Exasol-side aggregation.
fn ensure_ground_truth_lineitem_table(conn: &mut ExaConn) {
    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_LINEITEM_TABLE} AS \
         SELECT L_RETURNFLAG, L_QUANTITY, L_EXTENDEDPRICE FROM {}",
        vs_table(VS_NAME, E2E_LINEITEM_TABLE)
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

/// Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback
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
        vs_table(vs_name, E2E_LINEITEM_TABLE)
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
        vs_table(vs_name, E2E_LINEITEM_TABLE)
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

// The typed scan check runs at both join WHERE sites: each case renders correctly or
// declines and self-applies (#215, #223, #228, #285).

fn like_join_query(vs_name: &str, filter: &str) -> String {
    format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY WHERE {filter}",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// A LIKE over the join column `O_CUSTKEY`, an integer DECIMAL.
const CUSTKEY_LIKE: &str = "o.O_CUSTKEY LIKE '1%'";
/// Matches the first nine order dates as the default `NLS_DATE_FORMAT` writes them, and no date
/// under `DMY`.
const ORDERDATE_LIKE: &str = "o.O_ORDERDATE LIKE '2024-01-0%'";
const DEFAULT_SESSION: &str = "";
const DMY: &str = "NLS_DATE_FORMAT = 'DD.MM.YYYY'";
/// [`ORDERDATE_LIKE`] for dates written under `DMY`, so it matches no date by default.
const ORDERDATE_LIKE_DMY: &str = "o.O_ORDERDATE LIKE '0_.01.2024'";

fn custkey_like(custkey: &str) -> bool {
    custkey.starts_with('1')
}

/// Holds native copies of the fact and dimension tables under their virtual names, so a join
/// query that names its schema `{T}` runs over either.
const JOIN_ORACLE: &str = "JOIN_ORACLE";

fn copy_join_tables_natively(conn: &mut ExaConn) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {JOIN_ORACLE}"));
    for (native, virtual_table) in [
        (vs_fact_table(JOIN_ORACLE), vs_fact_table(VS_NAME)),
        (vs_dim_table(JOIN_ORACLE), vs_dim_table(VS_NAME)),
    ] {
        conn.execute(&format!(
            "CREATE OR REPLACE TABLE {native} AS SELECT * FROM {virtual_table}"
        ));
    }
}

/// `query` names its schema `{T}`. Over `vs_name` it must return what it returns natively and
/// take the N-scan fallback, whose pushdown statement this returns.
#[track_caller]
fn assert_n_scan_join_matches_native(conn: &mut ExaConn, vs_name: &str, query: &str) -> String {
    let path = PathCheck::NScan(2);
    if let Some(failure) = native_copy_mismatch(conn, vs_name, JOIN_ORACLE, query, path) {
        panic!("{failure}");
    }
    isolated_pushdown_statement(conn, &query.replace("{T}", vs_name))
}

/// Proves `query`, which names its schema `{T}`, cannot match its native result vacuously.
#[track_caller]
fn assert_narrows_fact_table(conn: &mut ExaConn, query: &str) {
    let native_rows = conn.query_row_count(&query.replace("{T}", JOIN_ORACLE));
    assert!(
        native_rows > 0 && native_rows < FACT_ORDERS_ROWS as i64,
        "{query} must narrow the fact table, got {native_rows} rows natively"
    );
}

/// The oracle applies the filter in the test process, so no engine render computes it.
fn assert_join_rows_kept(
    conn: &mut ExaConn,
    vs_name: &str,
    query: &str,
    keep: impl Fn(&str, &str) -> bool,
) {
    let actual = columns_to_sorted_pairs(&conn.query_columns(query));
    let expected = expected_join_rows_kept(conn, vs_name, keep);
    assert!(
        !expected.is_empty() && expected.len() < FACT_ORDERS_ROWS,
        "{query} must narrow the fact table, got {} rows",
        expected.len()
    );
    assert_eq!(actual, expected);
}

/// Scenario: A broadcast join keeps its plan for an integer LIKE
#[test]
fn e2e_broadcast_like_on_decimal_column_stays_broadcast_and_filters() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = like_join_query(VS_NAME, CUSTKEY_LIKE);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(has_broadcast_join_block(&pushed), "{pushed}");
    assert!(pushed.contains(SCAN_FILTER), "{pushed}");

    assert_join_rows_kept(&mut conn, VS_NAME, &query, |custkey, _| {
        custkey_like(custkey)
    });
}

/// Scenario: A join condition or filter that converts a date to text takes the N-scan fallback
#[test]
fn e2e_broadcast_like_on_date_column_falls_back_and_filters() {
    setup_e2e();
    copy_join_tables_natively(&mut exa_conn());

    for (setting, own_format_filter) in
        [(DEFAULT_SESSION, ORDERDATE_LIKE), (DMY, ORDERDATE_LIKE_DMY)]
    {
        let mut conn = exa_session(setting);
        assert_narrows_fact_table(&mut conn, &like_join_query("{T}", own_format_filter));
        for filter in [ORDERDATE_LIKE, ORDERDATE_LIKE_DMY] {
            let query = like_join_query("{T}", filter);
            let pushed = assert_n_scan_join_matches_native(&mut conn, VS_NAME, &query);
            assert!(
                !pushed.contains(SCAN_FILTER) && pushed.contains(r#"."O_ORDERDATE" LIKE"#),
                "[{setting}] the outer WHERE, not a scan, applies {filter}: {pushed}"
            );
        }
    }
}

/// Scenario: An N-scan leg filter keeps the conjuncts the scan computes
#[test]
fn e2e_n_scan_leg_keeps_its_computed_conjunct_beside_a_declined_one() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn);

    let query = like_join_query("{T}", &format!("{CUSTKEY_LIKE} AND {ORDERDATE_LIKE}"));
    assert_narrows_fact_table(&mut conn, &query);
    let pushed = assert_n_scan_join_matches_native(&mut conn, VS_NAME_LOW, &query);

    // A scan-spec filter doubles the quotes of its literals, and `'1%'` also occurs inside `''1%''`.
    let count = |needle: &str| pushed.matches(needle).count();
    assert!(
        count(SCAN_FILTER) == 1 && count("''1%''") == 1 && count("'1%'") == 1,
        "the integer LIKE must filter once, in its leg's scan, and nowhere else: {pushed}"
    );
    assert!(
        count("''2024-01-0%''") == 0
            && count(r#"."O_ORDERDATE" LIKE '2024-01-0%'"#) == 1
            && count("'2024-01-0%'") == 1,
        "the DATE LIKE must filter once, in the outer WHERE, and nowhere else: {pushed}"
    );
}

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

/// Exasol writes a whole DECIMAL without its zero fraction, so the oracle is the integer part;
/// asserts the seed keeps every value whole.
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

/// Scenario: Scan projects rendered select-list expressions
#[test]
fn e2e_join_select_list_with_a_column_and_its_cast_matches_seed() {
    setup_e2e();
    let mut conn = exa_conn();
    let select = |vs_name: &str| {
        format!(
            "SELECT o.O_ORDERKEY, o.O_CUSTKEY, CAST(o.O_CUSTKEY AS DECIMAL(18,0)), \
             c.C_NAME, CAST(c.C_NAME AS VARCHAR(20)) FROM {} o \
             JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY",
            vs_fact_table(vs_name),
            vs_dim_table(vs_name)
        )
    };
    let seed_row = |key: usize| {
        let custkey = order_custkey(key);
        let name = format!("customer-{custkey:02}");
        vec![
            key.to_string(),
            custkey.to_string(),
            custkey.to_string(),
            name.clone(),
            name,
        ]
    };

    for (suffix, keys) in [
        ("", (1..=FACT_ORDERS_ROWS).collect::<Vec<_>>()),
        (" ORDER BY o.O_ORDERKEY DESC LIMIT 3", vec![10, 9, 8]),
    ] {
        for vs_name in [VS_NAME, VS_NAME_LOW] {
            let query = format!("{}{suffix}", select(vs_name));
            if vs_name == VS_NAME {
                let pushed = explain_virtual_sql(&mut conn, &query);
                assert!(
                    has_broadcast_join_block(&pushed),
                    "the join must drive the broadcast fan-out:\n{pushed}"
                );
            }
            let mut expected: Vec<Vec<String>> = keys.iter().map(|&key| seed_row(key)).collect();
            expected.sort();
            let actual = fetch_rows_as_vecs(&conn.query_columns(&query));
            assert_eq!(actual, expected, "{query}");
        }
    }
}

/// Which join path the adapter must choose for a query on the default-threshold schema.
#[derive(Clone, Copy, PartialEq)]
enum JoinPath {
    Broadcast,
    NScan,
}

/// One shared-column-name join. `@VS@` in `template` is replaced by the virtual schema name.
struct SharedNameJoin<'a> {
    label: &'a str,
    template: &'a str,
    path_on_default_threshold: JoinPath,
    /// Columns whose `LHS_T0`/`LHS_T1` pair the pushed join condition must compare.
    condition_columns: &'a [&'a str],
    /// Output names the EMITS list must declare, `"LHS_T0_ID"` with its quotes.
    emits: &'a [&'a str],
    /// Text the pushed SQL must contain, for the per-shard top-N.
    pushed_contains: &'a [&'a str],
    column_types: &'a [&'a str],
    ordered: bool,
    expected: Vec<Vec<String>>,
}

/// Compares numbers as `f64`, because Exasol renders a DOUBLE as `5` or `5.0`.
fn normalized_cell(cell: &str) -> String {
    cell.parse::<f64>()
        .map(|number| number.to_string())
        .unwrap_or_else(|_| cell.to_string())
}

fn normalized_rows(columns: &[Vec<serde_json::Value>], ordered: bool) -> Vec<Vec<String>> {
    let row_count = columns.first().map_or(0, Vec::len);
    let mut rows: Vec<Vec<String>> = (0..row_count)
        .map(|row| {
            columns
                .iter()
                .map(|column| normalized_cell(&value_to_string(&column[row])))
                .collect()
        })
        .collect();
    if !ordered {
        rows.sort();
    }
    rows
}

fn expected_rows(rows: Vec<Vec<String>>, ordered: bool) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|row| row.iter().map(|cell| normalized_cell(cell)).collect())
        .collect();
    if !ordered {
        rows.sort();
    }
    rows
}

/// The `condition` value of the join block, still JSON-escaped.
fn pushed_join_condition(pushed: &str) -> &str {
    const KEY: &str = "\"condition\":\"";
    let start = pushed
        .find(KEY)
        .unwrap_or_else(|| panic!("no join condition in the pushed SQL:\n{pushed}"))
        + KEY.len();
    let tail = &pushed[start..];
    let mut escaped = false;
    for (offset, byte) in tail.bytes().enumerate() {
        match (escaped, byte) {
            (true, _) => escaped = false,
            (false, b'\\') => escaped = true,
            (false, b'"') => return &tail[..offset],
            _ => {}
        }
    }
    panic!("unterminated join condition in the pushed SQL:\n{pushed}")
}

fn result_column_types(conn: &mut ExaConn, query_sql: &str) -> Vec<String> {
    let resp = conn.execute(query_sql);
    resp["responseData"]["results"][0]["resultSet"]["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no result columns for {query_sql}: {resp:?}"))
        .iter()
        .map(|column| {
            column["dataType"]["type"]
                .as_str()
                .unwrap_or_else(|| panic!("no dataType.type in {column:?}"))
                .to_string()
        })
        .collect()
}

fn assert_shared_name_join(conn: &mut ExaConn, case: &SharedNameJoin) {
    let label = case.label;
    let default_sql = case.template.replace("@VS@", VS_NAME);
    let pushed = explain_virtual_sql(conn, &default_sql);

    match case.path_on_default_threshold {
        JoinPath::Broadcast => {
            assert!(
                has_broadcast_join_block(&pushed),
                "{label}: a join over shared column names must take the broadcast path:\n{pushed}"
            );
            assert!(
                !has_two_scan_wrapper(&pushed),
                "{label}: the broadcast path names LHS_T* relations inside its scan spec, never \
                 as a subquery alias:\n{pushed}"
            );
            let condition = pushed_join_condition(&pushed);
            for column in case.condition_columns {
                for alias in ["LHS_T0", "LHS_T1"] {
                    let side = format!(r#"\"{alias}\".\"{column}\""#);
                    assert!(
                        condition.contains(&side),
                        "{label}: the join condition must compare {side}, one per relation: \
                         {condition}\n{pushed}"
                    );
                }
            }
            let emits = pushed
                .split_once("EMITS (")
                .unwrap_or_else(|| panic!("{label}: no EMITS list:\n{pushed}"))
                .1;
            for output in case.emits {
                assert!(
                    emits.contains(output),
                    "{label}: the EMITS list must declare {output}, the relation alias joined to \
                     the column name:\n{emits}"
                );
            }
            for needle in case.pushed_contains {
                assert!(
                    pushed.contains(needle),
                    "{label}: the pushed SQL must contain {needle}:\n{pushed}"
                );
            }
        }
        JoinPath::NScan => {
            assert!(
                has_n_scan_wrapper(&pushed, 2),
                "{label}: this join must take the N-scan fallback:\n{pushed}"
            );
            assert!(
                !has_broadcast_join_block(&pushed),
                "{label}: the N-scan fallback must not carry a broadcast join block:\n{pushed}"
            );
        }
    }

    let expected = expected_rows(case.expected.clone(), case.ordered);
    for vs_name in [VS_NAME, VS_NAME_LOW] {
        let sql = case.template.replace("@VS@", vs_name);
        if vs_name == VS_NAME_LOW {
            let low_pushed = explain_virtual_sql(conn, &sql);
            assert!(
                has_n_scan_wrapper(&low_pushed, 2) && !has_broadcast_join_block(&low_pushed),
                "{label}: a 1-byte broadcast threshold must force the N-scan fallback:\n{low_pushed}"
            );
        }
        let actual = normalized_rows(&conn.query_columns(&sql), case.ordered);
        assert_eq!(
            actual, expected,
            "{label} on {vs_name}: rows must equal the seed-derived rows.\nquery: {sql}"
        );
        if !case.column_types.is_empty() {
            assert_eq!(
                result_column_types(conn, &sql),
                case.column_types,
                "{label} on {vs_name}: each column keeps its own relation's declared type"
            );
        }
    }
}

fn event_name(id: usize) -> String {
    format!("event-{id:02}")
}

fn event_score(id: usize) -> String {
    (5.0 * id as f64).to_string()
}

fn label_name(id: usize) -> String {
    format!("label-{id:02}")
}

fn order_pairs_sharing_a_customer() -> Vec<(usize, usize)> {
    let orders = 1..=FACT_ORDERS_ROWS;
    orders
        .clone()
        .flat_map(|a| {
            orders
                .clone()
                .filter(move |&b| order_custkey(b) == order_custkey(a))
                .map(move |b| (a, b))
        })
        .collect()
}

fn pair_rows(pairs: impl IntoIterator<Item = (usize, usize)>) -> Vec<Vec<String>> {
    pairs
        .into_iter()
        .map(|(a, b)| vec![a.to_string(), b.to_string()])
        .collect()
}

/// Scenario: Legs that share column names are planned as a broadcast fan-out
/// Scenario: Broadcast join projection and filter are rendered per involved table
/// Scenario: Join condition is rendered via the vs-expression translator
/// Scenario: Scan registers both tables and executes the inner equi-join
#[test]
fn e2e_broadcast_join_over_shared_column_names_matches_native() {
    setup_e2e();
    let mut conn = exa_conn();
    let fact_join = "@VS@.FACT_ORDERS a JOIN @VS@.FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY";
    let events_labels = "@VS@.EVENTS e JOIN @VS@.LABELS l ON e.ID = l.ID";
    let ids = 1..=SEED_LABELS_ROWS;
    let per_id = |row: fn(usize) -> Vec<String>, ids: Vec<usize>| -> Vec<Vec<String>> {
        ids.into_iter().map(row).collect()
    };
    let top_pairs = {
        let mut pairs = order_pairs_sharing_a_customer();
        pairs.sort_by(|x, y| y.1.cmp(&x.1).then(x.0.cmp(&y.0)));
        pairs.truncate(3);
        pairs
    };

    let cases = [
        SharedNameJoin {
            label: "(a) shared join key and a filter on it",
            template: &format!(
                "SELECT e.ID, l.ID, e.NAME, l.LABEL FROM {events_labels} WHERE l.ID >= 8"
            ),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["ID"],
            emits: &[
                r#""LHS_T0_ID""#,
                r#""LHS_T1_ID""#,
                r#""LHS_T0_NAME""#,
                r#""LHS_T1_LABEL""#,
            ],
            pushed_contains: &[],
            column_types: &[],
            ordered: false,
            expected: per_id(
                |id| {
                    vec![
                        id.to_string(),
                        id.to_string(),
                        event_name(id),
                        label_name(id),
                    ]
                },
                (8..=SEED_LABELS_ROWS).collect(),
            ),
        },
        SharedNameJoin {
            label: "(b) self-join",
            template: &format!("SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {fact_join}"),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["O_CUSTKEY"],
            emits: &[r#""LHS_T0_O_ORDERKEY""#, r#""LHS_T1_O_ORDERKEY""#],
            pushed_contains: &[],
            column_types: &[],
            ordered: false,
            expected: pair_rows(order_pairs_sharing_a_customer()),
        },
        SharedNameJoin {
            label: "(c) self-join top-N over the second occurrence",
            template: &format!(
                "SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {fact_join} \
                 ORDER BY b.O_ORDERKEY DESC, a.O_ORDERKEY LIMIT 3"
            ),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["O_CUSTKEY"],
            emits: &[r#""LHS_T0_O_ORDERKEY""#, r#""LHS_T1_O_ORDERKEY""#],
            pushed_contains: &[],
            column_types: &[],
            ordered: true,
            expected: pair_rows(top_pairs),
        },
        SharedNameJoin {
            label: "(d1) shared non-key column projected from both sides",
            template: &format!("SELECT e.SCORE, l.SCORE FROM {events_labels}"),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["ID"],
            emits: &[r#""LHS_T0_SCORE" DOUBLE"#, r#""LHS_T1_SCORE" VARCHAR("#],
            pushed_contains: &[],
            column_types: &["DOUBLE", "VARCHAR"],
            ordered: false,
            expected: per_id(
                |id| vec![event_score(id), labels_score(id)],
                ids.clone().collect(),
            ),
        },
        SharedNameJoin {
            label: "(d2) filter on the shared column of one side",
            template: &format!(
                "SELECT e.SCORE, l.SCORE FROM {events_labels} WHERE l.SCORE = 'score-07'"
            ),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["ID"],
            emits: &[r#""LHS_T0_SCORE""#, r#""LHS_T1_SCORE""#],
            pushed_contains: &[],
            column_types: &["DOUBLE", "VARCHAR"],
            ordered: false,
            expected: vec![vec![event_score(14), labels_score(14)]],
        },
        SharedNameJoin {
            label: "(d3) shared column unread",
            template: &format!("SELECT e.NAME, l.LABEL FROM {events_labels}"),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["ID"],
            emits: &[r#""LHS_T0_NAME""#, r#""LHS_T1_LABEL""#],
            pushed_contains: &[],
            column_types: &[],
            ordered: false,
            expected: per_id(
                |id| vec![event_name(id), label_name(id)],
                ids.clone().collect(),
            ),
        },
        SharedNameJoin {
            label: "(d4) top-N ordered by the shared column of the second relation",
            template: &format!(
                "SELECT e.SCORE, l.SCORE FROM {events_labels} ORDER BY l.SCORE DESC LIMIT 3"
            ),
            path_on_default_threshold: JoinPath::Broadcast,
            condition_columns: &["ID"],
            emits: &[r#""LHS_T0_SCORE""#, r#""LHS_T1_SCORE""#],
            pushed_contains: &["\"post_join_order_by\"", "\"post_join_limit\":3"],
            column_types: &[],
            ordered: true,
            expected: per_id(|id| vec![event_score(id), labels_score(id)], vec![1, 2, 3]),
        },
        SharedNameJoin {
            label: "(d5) sort key not projected",
            template: &format!("SELECT e.SCORE FROM {events_labels} ORDER BY l.SCORE LIMIT 3"),
            path_on_default_threshold: JoinPath::NScan,
            condition_columns: &[],
            emits: &[],
            pushed_contains: &[],
            column_types: &[],
            ordered: true,
            expected: per_id(
                |id| vec![event_score(id)],
                vec![SEED_LABELS_ROWS, SEED_LABELS_ROWS - 1, SEED_LABELS_ROWS - 2],
            ),
        },
    ];

    for case in &cases {
        assert_shared_name_join(&mut conn, case);
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

/// Scenario: A table joined to itself renders each occurrence as its own leg
#[test]
fn e2e_self_join_on_primitive_column_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let mut expected = fetch_order_rows(&mut conn);
    expected.sort();
    assert_eq!(
        expected.len(),
        FACT_ORDERS_ROWS,
        "seed invariant: O_ORDERKEY is unique per row"
    );

    for vs_name in [VS_NAME, VS_NAME_LOW] {
        let fact = vs_fact_table(vs_name);
        let query = format!(
            "SELECT a.O_ORDERKEY, a.O_CUSTKEY FROM {fact} a JOIN {fact} b \
             ON a.O_ORDERKEY = b.O_ORDERKEY"
        );
        let pushed = explain_virtual_sql(&mut conn, &query);
        assert_self_join_path(vs_name, &pushed);
        let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

        assert_eq!(
            actual,
            expected,
            "{vs_name}: a self-join on the unique O_ORDERKEY must equal each row matched \
             only with itself, computed independently by reading fact_orders un-joined, \
             not the cross product of {}.\nactual:   {actual:?}\nexpected: {expected:?}",
            FACT_ORDERS_ROWS * FACT_ORDERS_ROWS
        );
    }
}

/// Broadcast on the default-threshold schema, the N-scan wrapper on the low-threshold one.
fn assert_self_join_path(vs_name: &str, pushed: &str) {
    if vs_name == VS_NAME_LOW {
        assert!(
            has_n_scan_wrapper(pushed, 2) && !has_broadcast_join_block(pushed),
            "{vs_name}: a two-leg self-join above the broadcast threshold must emit the \
             N-scan wrapper with two distinct LHS_T* fan-out aliases:\n{pushed}"
        );
    } else {
        assert!(
            has_broadcast_join_block(pushed) && !has_two_scan_wrapper(pushed),
            "{vs_name}: a two-leg self-join within the broadcast threshold must take the \
             broadcast in-UDF join:\n{pushed}"
        );
    }
}

/// Scenario: A shared-column-name join on the fallback renders each column qualified by its leg
#[test]
fn e2e_self_join_with_one_unaliased_occurrence_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();
    let bare = E2E_FACT_TABLE.to_uppercase();

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
        "seed invariant: {FACT_ORDERS_ROWS} orders over {DIM_CUSTOMER_ROWS} customers form \
         {DIM_CUSTOMER_ROWS} groups of 2, each contributing 4 self-matched pairs (20 total), \
         not the cross product of {}: {expected:?}",
        FACT_ORDERS_ROWS * FACT_ORDERS_ROWS
    );

    for vs_name in [VS_NAME, VS_NAME_LOW] {
        let fact = vs_fact_table(vs_name);
        let query = format!(
            "SELECT {bare}.O_ORDERKEY, b.O_ORDERKEY FROM {fact} JOIN {fact} b \
             ON {bare}.O_CUSTKEY = b.O_CUSTKEY"
        );
        let pushed = explain_virtual_sql(&mut conn, &query);
        assert_self_join_path(vs_name, &pushed);
        let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

        assert_eq!(
            actual, expected,
            "{vs_name}: a column with no tableAlias must bind to its own occurrence, so the \
             result equals the pairs sharing O_CUSTKEY, computed independently by reading \
             fact_orders un-joined.\nactual:   {actual:?}\nexpected: {expected:?}"
        );
    }
}

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
        "expected {FACT_ORDERS_ROWS} self-matched rows, not the three-way cross \
         product of {}: {actual:?}",
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

#[test]
fn e2e_self_join_with_one_sided_filter_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();
    let threshold = DIM_CUSTOMER_ROWS as i64;

    let orders = fetch_order_rows(&mut conn);
    let mut expected: Vec<(String, String)> = orders
        .iter()
        .flat_map(|(a_key, a_cust)| {
            orders
                .iter()
                .filter(|(b_key, _)| {
                    b_key.parse::<i64>().expect("O_ORDERKEY is numeric") <= threshold
                })
                .filter(move |(_, b_cust)| b_cust == a_cust)
                .map(move |(b_key, _)| (a_key.clone(), b_key.clone()))
        })
        .collect();
    expected.sort();
    assert!(
        !expected.is_empty(),
        "the WHERE conjunct must leave a non-empty result, else this test proves nothing \
         about leg-local filtering: {expected:?}"
    );

    for vs_name in [VS_NAME, VS_NAME_LOW] {
        let fact = vs_fact_table(vs_name);
        let query = format!(
            "SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {fact} a JOIN {fact} b \
             ON a.O_CUSTKEY = b.O_CUSTKEY WHERE b.O_ORDERKEY <= {threshold}"
        );
        let pushed = explain_virtual_sql(&mut conn, &query);
        assert_self_join_path(vs_name, &pushed);
        if vs_name == VS_NAME {
            // The filter prunes fact_orders' second file from leg 1 only, so leg 1 has fewer
            // bytes; the byte tie-break alone would pick leg 0.
            assert!(
                pushed.contains("\"dimension_alias\":\"LHS_T1\""),
                "{vs_name}: the pruned second occurrence must be the dimension:\n{pushed}"
            );
        }
        let actual = columns_to_sorted_pairs(&conn.query_columns(&query));

        assert_eq!(
            actual, expected,
            "{vs_name}: a WHERE conjunct local to alias `b` must restrict only `b`'s rows, \
             leaving `a` free to match any row sharing O_CUSTKEY, including rows the filter \
             excludes for `b`.\nactual:   {actual:?}\nexpected: {expected:?}"
        );
    }
}

#[test]
fn e2e_scalar_over_aggregate_ungrouped_join_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!(
        "SELECT ROUND(SUM(l.L_QUANTITY), 2) FROM {} o \
         JOIN {} l ON o.O_ORDERKEY = l.L_ORDERKEY",
        vs_fact_table(VS_NAME),
        vs_table(VS_NAME, E2E_LINEITEM_TABLE)
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

const VS_CLUSTERED: &str = "MY_LAKEHOUSE_JOIN_CLUSTERED";

fn setup_clustered() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        runtime()
            .block_on(seed_clustered_join(
                &iceberg_catalog_url(),
                "s3://warehouse/",
            ))
            .expect("seed the clustered join fixture");
        create_virtual_schema(
            &mut exa_conn(),
            &VsProps::new(VS_CLUSTERED, CLUSTERED_JOIN_NAMESPACE),
        );
    });
}

/// Scenario: The fact scan skips row groups that no dimension join key matches
#[test]
fn e2e_selective_broadcast_join_over_a_fact_clustered_by_the_join_key() {
    setup_clustered();

    let files = runtime().block_on(resolve_fixture_files(
        CLUSTERED_JOIN_NAMESPACE,
        CLUSTERED_FACT_TABLE,
    ));
    let [file] = &files[..] else {
        panic!("the clustered fact must be one data file, got {files:?}");
    };
    let footer = read_footer(&file.path);
    let group_keys: Vec<(Option<i64>, Option<i64>)> = footer
        .row_groups()
        .iter()
        .map(|group| match group.column(1).statistics() {
            Some(Statistics::Int64(stats)) => (stats.min_opt().copied(), stats.max_opt().copied()),
            other => panic!("O_CUSTKEY row-group statistics missing: {other:?}"),
        })
        .collect();
    let expected_keys: Vec<_> = (0..CLUSTERED_KEYS).map(|k| (Some(k), Some(k))).collect();
    assert_eq!(
        group_keys, expected_keys,
        "each fact row group must hold one join key, or the fixture cannot prove row-group pruning"
    );

    let query = format!(
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY WHERE c.C_REGION = 'hot'",
        vs_table(VS_CLUSTERED, CLUSTERED_FACT_TABLE),
        vs_table(VS_CLUSTERED, CLUSTERED_DIM_TABLE)
    );
    let mut conn = exa_conn();
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_broadcast_join_block(&pushed),
        "the selective join must stay on the broadcast route:\n{pushed}"
    );

    let mut actual: Vec<(i64, String)> = fetch_join_rows_in_query_order(&mut conn, &query)
        .into_iter()
        .map(|(key, name)| (key.parse().expect("O_ORDERKEY is an integer"), name))
        .collect();
    actual.sort();
    let expected: Vec<(i64, String)> = CLUSTERED_HOT_KEYS
        .iter()
        .flat_map(|&k| {
            (k * CLUSTERED_ROWS_PER_KEY..(k + 1) * CLUSTERED_ROWS_PER_KEY)
                .map(move |order| (order, format!("c{k}")))
        })
        .collect();
    assert_eq!(actual, expected);
}
