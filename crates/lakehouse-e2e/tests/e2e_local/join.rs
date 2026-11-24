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
use crate::fixtures::{
    JOIN_ORACLE, assert_two_scan_join_wrapper, copy_join_tables_natively, events,
    events_labels_join, stack, vs_table,
};
use crate::fixtures::{emits_clause, parenthesized};

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
    copy_join_tables_natively(&mut conn, VS_NAME);

    const LIMIT_ROWS: usize = 3;
    let composite_key = "SELECT a.O_ORDERKEY, b.O_ORDERDATE FROM {T}.FACT_ORDERS a \
                         JOIN {T}.FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY \
                         AND YEAR(a.O_ORDERDATE) = YEAR(b.O_ORDERDATE)";
    for unbounded_query in [join_query("{T}"), composite_key.to_string()] {
        let query = format!("{unbounded_query} LIMIT {LIMIT_ROWS}").replace("{T}", VS_NAME);

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
        let unbounded = columns_to_sorted_pairs(
            &conn.query_columns(&unbounded_query.replace("{T}", JOIN_ORACLE)),
        );
        assert!(
            unbounded.len() > LIMIT_ROWS,
            "the unbounded join must exceed the limit: {unbounded:?}"
        );
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

/// Scenario: Small-side selection uses table-format metadata and the broadcast threshold
/// Scenario: A join outside the broadcast contract is declined safely
/// Scenario: Pushdown resolves every tuning value from the properties
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

/// Scenario: A constant sort key does not become a select-list position
#[test]
fn e2e_join_fallback_constant_sort_key_returns_the_native_order() {
    setup_e2e();
    let mut conn = exa_conn();

    for constant in [1, 5] {
        let query = events_labels_join(
            VS_NAME_LOW,
            &format!("e.ID, {constant}"),
            "ORDER BY 2 DESC, 1 LIMIT 3",
        );
        assert_two_scan_join_wrapper(&mut conn, &query);
        assert_eq!(
            int_column(&conn.query_columns(&query)[0]),
            [1, 2, 3],
            "{query}"
        );
    }
}

fn aggregate_join_query(vs_name: &str) -> String {
    format!(
        "SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY",
        vs_fact_table(vs_name),
        vs_dim_table(vs_name)
    )
}

/// Scenario: An aggregating broadcast-eligible join is aggregated by Exasol over the broadcast fan-out
#[test]
fn e2e_aggregating_join_takes_the_broadcast_path_and_matches_native() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let orders = "{T}.FACT_ORDERS o JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY";
    let self_join = "{T}.FACT_ORDERS a JOIN {T}.FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY";
    let events_labels = "{T}.EVENTS e JOIN {T}.LABELS l ON e.ID = l.ID";
    // A literal list bounds the text column's width, so the join stays broadcast.
    let named = "c.C_NAME IN ('customer-01', 'customer-02', 'customer-03')";
    let having_only = format!(
        "SELECT c.C_NAME FROM {orders} WHERE o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}' \
         AND {named} GROUP BY c.C_NAME HAVING SUM(o.O_TOTALPRICE) > 100"
    );
    let broadcast = [
        format!(
            "SELECT c.C_NAME, SUM(o.O_TOTALPRICE) FROM {orders} WHERE {named} GROUP BY c.C_NAME"
        ),
        // Select items whose exact type the typed pass derives.
        format!(
            "SELECT MAX(CASE WHEN o.O_CUSTKEY > 3 THEN o.O_ORDERDATE ELSE DATE '1900-03-01' END), \
             MIN(COALESCE(o.O_ORDERDATE, DATE '2000-01-01')) FROM {orders}"
        ),
        format!(
            "SELECT MAX(CASE WHEN o.O_CUSTKEY > 3 THEN o.O_ORDERDATE \
             ELSE TIMESTAMP '1960-01-01 10:00:00.123456' END) FROM {orders}"
        ),
        format!(
            "SELECT c.C_CUSTKEY, COUNT(*) FROM {orders} WHERE o.O_ORDERDATE >= DATE \
             '{ORDERDATE_LOWER_BOUND}' GROUP BY c.C_CUSTKEY HAVING COUNT(*) > 1 \
             ORDER BY COUNT(*) DESC, c.C_CUSTKEY LIMIT 2"
        ),
        format!(
            "SELECT c.C_NAME, o.O_ORDERDATE, MAX(c.C_CUSTKEY), MIN(o.O_TOTALPRICE) \
             FROM {orders} WHERE c.C_NAME = 'customer-02' GROUP BY c.C_NAME, o.O_ORDERDATE"
        ),
        format!(
            "SELECT c.C_CUSTKEY, SUM(o.O_TOTALPRICE * c.C_CUSTKEY) FROM {orders} \
             GROUP BY c.C_CUSTKEY"
        ),
        format!(
            "SELECT c.C_CUSTKEY, SUM(o.O_TOTALPRICE) FROM {orders} GROUP BY c.C_CUSTKEY \
             ORDER BY SUM(o.O_TOTALPRICE) DESC, c.C_CUSTKEY LIMIT 1"
        ),
        format!("SELECT COUNT(*), SUM(o.O_TOTALPRICE) FROM {orders}"),
        format!(
            "SELECT COUNT(*), MAX(c.C_NAME) FROM {orders} \
             WHERE o.O_ORDERKEY + c.C_CUSTKEY < 0 AND {named}"
        ),
        format!("SELECT COUNT(DISTINCT c.C_CUSTKEY) FROM {orders}"),
        format!("SELECT COUNT(*) FROM (SELECT SUM(o.O_TOTALPRICE) FROM {orders})"),
        format!(
            "SELECT a.O_CUSTKEY, COUNT(*), SUM(b.O_TOTALPRICE) FROM {self_join} GROUP BY a.O_CUSTKEY"
        ),
        format!(
            "SELECT l.SCORE, MAX(e.SCORE), COUNT(e.ID) FROM {events_labels} \
             WHERE l.SCORE IN ('score-01', 'score-07') GROUP BY l.SCORE"
        ),
        format!("SELECT e.ID, MIN(l.ID) FROM {events_labels} GROUP BY e.ID HAVING MIN(l.ID) > 3"),
        format!("SELECT COUNT(*) FROM {orders} GROUP BY c.C_CUSTKEY"),
        having_only.clone(),
        format!(
            "SELECT c.C_CUSTKEY, COUNT(*) FROM {orders} GROUP BY c.C_CUSTKEY \
             ORDER BY MAX(o.O_TOTALPRICE) DESC, c.C_CUSTKEY LIMIT 2"
        ),
        format!("SELECT DISTINCT * FROM {orders} WHERE {named}"),
        format!(
            "SELECT c.C_NAME, COUNT(*) FROM {orders} AND o.O_ORDERKEY > c.C_CUSTKEY \
             WHERE {named} GROUP BY c.C_NAME"
        ),
        // Exasol's WHERE applies the conjunct the scan declines.
        format!(
            "SELECT c.C_NAME, COUNT(*) FROM {orders} WHERE {SECOND_IS_ZERO} \
             AND {named} GROUP BY c.C_NAME"
        ),
    ];
    let mut failures = Vec::new();
    for query in &broadcast {
        for (vs_name, path) in [
            (VS_NAME, PathCheck::Has(BROADCAST_JOIN)),
            (VS_NAME_LOW, PathCheck::NScan(2)),
        ] {
            failures.extend(native_copy_mismatch(
                &mut conn,
                vs_name,
                JOIN_ORACLE,
                query,
                path,
                Expect::Rows,
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    // The scan applies the condition and WHERE, so it emits only the HAVING's and select list's.
    let pushed = explain_virtual_sql(&mut conn, &having_only.replace("{T}", VS_NAME));
    for read in [r#""LHS_T0_O_TOTALPRICE""#, r#""LHS_T1_C_NAME""#] {
        assert!(
            pushed.contains(read),
            "the fan-out must emit {read}:\n{pushed}"
        );
    }
    for unread in [
        r#""LHS_T0_O_CUSTKEY""#,
        r#""LHS_T0_O_ORDERDATE""#,
        r#""LHS_T1_C_CUSTKEY""#,
    ] {
        assert!(
            !pushed.contains(unread),
            "the fan-out must not emit {unread}:\n{pushed}"
        );
    }
}

/// Scenario: An aggregating join that reads an unbounded text column of the small side keeps the N-scan
#[test]
fn e2e_aggregating_join_reading_a_wide_dimension_text_column_keeps_the_n_scan() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let orders = "{T}.FACT_ORDERS o JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY";
    let events_labels = "{T}.EVENTS e JOIN {T}.LABELS l ON e.ID = l.ID";
    let n_scan = [
        format!("SELECT c.C_NAME, SUM(o.O_TOTALPRICE) FROM {orders} GROUP BY c.C_NAME"),
        format!("SELECT COUNT(*), MAX(c.C_NAME) FROM {orders}"),
        format!("SELECT COUNT(DISTINCT c.C_NAME) FROM {orders}"),
        format!(
            "SELECT SUBSTR(c.C_NAME, 1, 10), COUNT(*) FROM {orders} \
             GROUP BY SUBSTR(c.C_NAME, 1, 10)"
        ),
        format!(
            "SELECT c.C_NAME, COUNT(*) FROM {orders} WHERE c.C_NAME LIKE 'customer-0%' \
             GROUP BY c.C_NAME"
        ),
        format!("SELECT DISTINCT * FROM {orders}"),
        // LABELS, the smaller side, declares SCORE as text, while EVENTS declares it DOUBLE.
        format!("SELECT l.SCORE, MAX(e.SCORE), COUNT(e.ID) FROM {events_labels} GROUP BY l.SCORE"),
    ];

    let mut failures = Vec::new();
    for query in &n_scan {
        failures.extend(native_copy_mismatch(
            &mut conn,
            VS_NAME,
            JOIN_ORACLE,
            query,
            PathCheck::NScan(2),
            Expect::Rows,
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Scenario: Aggregate over a join routes through the unified qualified wrapper
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

    // Over the 1-byte budget the join is outside the broadcast contract.
    let query = aggregate_join_query(VS_NAME_LOW);
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 2) && !has_broadcast_join_block(&pushed),
        "an aggregate over a join outside the broadcast contract must take the N-scan \
         wrapper:\n{pushed}"
    );
    let cols = conn.query_columns(&query);
    assert_eq!(
        cols.len(),
        2,
        "aggregate-over-join must return 2 columns: {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        1,
        "a single-group aggregate returns exactly one row: {cols:?}"
    );
    assert_eq!(
        value_to_string(&cols[0][0]),
        expected_count,
        "COUNT(*) over the join must equal the single-table COUNT"
    );
    assert_eq!(
        value_to_string(&cols[1][0]),
        expected_min,
        "MIN(O_ORDERDATE) over the join must equal the single-table MIN"
    );

    let nested = format!("SELECT COUNT(*) FROM ({query})");
    let cols = conn.query_columns(&nested);
    assert_eq!(cols[0].len(), 1, "{nested} must return one row: {cols:?}");
    assert_eq!(value_to_string(&cols[0][0]), "1", "{nested}");

    copy_join_tables_natively(&mut conn, VS_NAME);
    let union_items = format!(
        "SELECT MAX(CASE WHEN o.O_CUSTKEY > 3 THEN o.O_ORDERDATE ELSE DATE '1900-03-01' END), \
         MIN(COALESCE(o.O_ORDERDATE, DATE '2000-01-01')) FROM {} o JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY",
        vs_fact_table("{T}"),
        vs_dim_table("{T}")
    );
    let pushed = matching_pushdown(&mut conn, VS_NAME_LOW, PathCheck::NScan(2), &union_items)
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(
        !pushed.contains("CAST(MAX("),
        "a select item of an exact DATE type needs no CAST: {pushed}"
    );
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

/// Scenario: An aggregating broadcast-eligible join is aggregated by Exasol over the broadcast fan-out
#[test]
fn e2e_scalar_over_aggregate_grouped_join_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = scalar_over_aggregate_join_query(VS_NAME);

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_broadcast_join_block(&pushed),
        "a scalar-over-aggregate grouped join must aggregate over the broadcast fan-out:\n{pushed}"
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

/// Scenario: A scalar function wrapping aggregates in a grouped join select list is rendered, not declined
#[test]
fn e2e_scalar_over_aggregate_grouped_join_n_table_result_correct() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = scalar_over_aggregate_n_table_join_query(VS_NAME_LOW);

    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_n_scan_wrapper(&pushed, 3) && !has_broadcast_join_block(&pushed),
        "with no eligible pair a scalar-over-aggregate grouped join over three tables must \
         be served by the unified N-scan wrapper (N=3), not declined:\n{pushed}"
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

/// `SECOND(<date>, 3)` is always 0 (verified live), and DataFusion refuses the two-argument
/// `SECOND`, so the scan declines both conjuncts.
const SECOND_IS_ZERO: &str = "SECOND(o.O_ORDERDATE, 3) = 0";
const SECOND_IS_ONE: &str = "SECOND(o.O_ORDERDATE, 3) = 1";

/// Whether Exasol's WHERE over the fan-outs of `pushed`, a join's pushdown statement, holds
/// `needle`.
fn exasol_where_holds(pushed: &str, needle: &str) -> bool {
    pushed
        .rsplit_once(") WHERE (")
        .is_some_and(|(_, outer)| outer.contains(needle))
}

/// The scan spec keys that cut or sort a shard's rows.
const SHARD_WINDOW: [&str; 4] = [
    r#""limit""#,
    r#""order_by""#,
    "post_join_limit",
    "post_join_order_by",
];

/// Scenario: A broadcast join applies a declined filter conjunct in Exasol's WHERE
/// Scenario: Broadcast join projection and filter are rendered per involved table
/// Scenario: A single-table WHERE filter splits at AND between the scan and the wrapper
#[test]
fn e2e_broadcast_join_declined_conjunct_runs_in_exasol_where() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let select = format!("SELECT c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE FROM {ORDERS_JOIN}");
    // The scan renders the date conjunct and declines SECOND.
    let dated = format!("{SECOND_IS_ZERO} AND o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'");
    let cases = [
        (2, format!("{select} WHERE {dated}")),
        (2, format!("{select} WHERE {SECOND_IS_ONE}")),
        (
            2,
            format!("{select} WHERE {SECOND_IS_ZERO} ORDER BY o.O_ORDERKEY DESC LIMIT 3"),
        ),
        (
            2,
            format!(
                "SELECT c.C_CUSTKEY, COUNT(*) FROM {ORDERS_JOIN} WHERE {SECOND_IS_ZERO} \
                 GROUP BY c.C_CUSTKEY"
            ),
        ),
        (
            3,
            format!("SELECT c.C_CUSTKEY, l.L_LINENUMBER FROM {CHAIN} WHERE {dated}"),
        ),
    ];
    let mut failures = Vec::new();
    for (legs, query) in &cases {
        // SECOND(O_ORDERDATE, 3) = 1 holds for no row, so a dropped conjunct would return rows.
        let native_rows = conn.query_row_count(&query.replace("{T}", JOIN_ORACLE));
        if (native_rows == 0) != query.contains(SECOND_IS_ONE) {
            failures.push(format!("`{query}` returns {native_rows} rows natively"));
        }
        failures.extend(broadcast_mismatches(&mut conn, *legs, query));
        for vs_name in [VS_NAME, VS_NAME_LOW] {
            let pushed = isolated_pushdown_statement(&mut conn, &query.replace("{T}", vs_name));
            if !exasol_where_holds(&pushed, "SECOND(") {
                failures.push(format!(
                    "{vs_name} `{query}`: Exasol's WHERE must apply SECOND: {pushed}"
                ));
            }
            let filters = pushed_scan_filters(&pushed);
            if query.contains(&dated) && !filters.iter().any(|f| f.contains("O_ORDERDATE")) {
                failures.push(format!(
                    "{vs_name} `{query}`: the conjunct the scan renders stays in its scan: {pushed}"
                ));
            }
            if vs_name == VS_NAME && SHARD_WINDOW.iter().any(|key| pushed.contains(key)) {
                failures.push(format!(
                    "`{query}`: no shard may cut or sort rows: {pushed}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
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
/// [`ORDERDATE_LIKE`] for dates written under `DMY`, so it matches no date by default.
const ORDERDATE_LIKE_DMY: &str = "o.O_ORDERDATE LIKE '0_.01.2024'";

fn custkey_like(custkey: &str) -> bool {
    custkey.starts_with('1')
}

/// `query`, which names its schema `{T}`, over `vs_name`: its pushdown statement when it returns
/// its native rows and takes `path`, else what differs.
fn matching_pushdown(
    conn: &mut ExaConn,
    vs_name: &str,
    path: PathCheck<'_>,
    query: &str,
) -> Result<String, String> {
    match native_copy_mismatch(conn, vs_name, JOIN_ORACLE, query, path, Expect::Rows) {
        Some(failure) => Err(failure),
        None => Ok(isolated_pushdown_statement(
            conn,
            &query.replace("{T}", vs_name),
        )),
    }
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

/// Scenario: A join filter that converts a date to text runs in Exasol, and such a join key keeps the N-scan
#[test]
fn e2e_join_date_to_text_filter_runs_in_exasol_and_key_keeps_the_n_scan() {
    setup_e2e();
    copy_join_tables_natively(&mut exa_conn(), VS_NAME);
    let filtered = |filter: &str| {
        format!("SELECT c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE FROM {ORDERS_JOIN} WHERE {filter}")
    };
    let text_key = "SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {T}.FACT_ORDERS a JOIN {T}.FACT_ORDERS b \
                    ON CAST(a.O_ORDERDATE AS VARCHAR(20)) = CAST(b.O_ORDERDATE AS VARCHAR(20))";

    for (setting, own_format_filter) in
        [(DEFAULT_SESSION, ORDERDATE_LIKE), (DMY, ORDERDATE_LIKE_DMY)]
    {
        let mut conn = exa_session(setting);
        assert_narrows_fact_table(&mut conn, &filtered(own_format_filter));
        for filter in [ORDERDATE_LIKE, ORDERDATE_LIKE_DMY] {
            let pushed = matching_pushdown(
                &mut conn,
                VS_NAME,
                PathCheck::Has(BROADCAST_JOIN),
                &filtered(filter),
            )
            .unwrap_or_else(|failure| panic!("[{setting}] {failure}"));
            assert!(
                exasol_where_holds(&pushed, r#""LHS_T0_O_ORDERDATE" LIKE '"#),
                "[{setting}] Exasol's WHERE over the broadcast fan-out applies {filter}: {pushed}"
            );
        }
        if let Err(failure) = matching_pushdown(&mut conn, VS_NAME, PathCheck::NScan(2), text_key) {
            panic!("[{setting}] {failure}");
        }
    }
}

/// Scenario: Join conditions attach greedily by LEG set and leg-local filters push into each leg
#[test]
fn e2e_n_scan_leg_keeps_its_computed_conjunct_beside_a_declined_one() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);

    let query = like_join_query("{T}", &format!("{CUSTKEY_LIKE} AND {ORDERDATE_LIKE}"));
    assert_narrows_fact_table(&mut conn, &query);
    let pushed = matching_pushdown(&mut conn, VS_NAME_LOW, PathCheck::NScan(2), &query)
        .unwrap_or_else(|failure| panic!("{failure}"));

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

/// Every order joined to its customer: the ground truth of a filter that holds for every row.
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

#[test]
fn e2e_join_instr_with_start_position_returns_native_result() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!(
        "SELECT c.C_NAME, o.O_ORDERDATE FROM {} o \
         JOIN {} c ON o.O_CUSTKEY = c.C_CUSTKEY \
         WHERE INSTR(c.C_NAME, 'c', 3) = 0",
        vs_fact_table(VS_NAME_LOW),
        vs_dim_table(VS_NAME_LOW)
    );
    let pushed = explain_virtual_sql(&mut conn, &query);
    assert!(
        has_two_scan_wrapper(&pushed),
        "with no eligible pair the join takes the N-scan wrapper:\n{pushed}"
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
    let expected = expected_full_join_rows(&mut conn, VS_NAME_LOW);

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

/// Exasol writes a DECIMAL by `NLS_NUMERIC_CHARACTERS`, so a join filter over its text loses the
/// broadcast plan and Exasol applies it.
#[test]
fn e2e_join_filter_over_decimal_text_runs_in_exasol() {
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
            "{vs_name}: the join result under a WHERE filter over DECIMAL text \
             must equal the seed-derived expectation.\nactual:   \
             {actual:?}\nexpected: {expected:?}"
        );
        let pushed = explain_virtual_sql(&mut conn, &query);
        assert!(
            !has_broadcast_join_block(&pushed),
            "{vs_name}: a filter over DECIMAL text must not run in the broadcast scan: {pushed}"
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
    pushed_string_values(pushed, "condition")
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no join condition in the pushed SQL:\n{pushed}"))
}

/// Every scan spec's `filter` value in the pushed SQL, still JSON-escaped.
fn pushed_scan_filters(pushed: &str) -> Vec<&str> {
    pushed_string_values(pushed, "filter")
}

/// Every JSON string value of `key` in the scan specs of `pushed`, still JSON-escaped.
fn pushed_string_values<'a>(pushed: &'a str, key: &str) -> Vec<&'a str> {
    let marker = format!("\"{key}\":\"");
    pushed
        .match_indices(&marker)
        .map(|(start, _)| {
            let tail = &pushed[start + marker.len()..];
            let mut escaped = false;
            for (offset, byte) in tail.bytes().enumerate() {
                match (escaped, byte) {
                    (true, _) => escaped = false,
                    (false, b'\\') => escaped = true,
                    (false, b'"') => return &tail[..offset],
                    _ => {}
                }
            }
            panic!("unterminated {key} value in the pushed SQL:\n{pushed}")
        })
        .collect()
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

/// Scenario: An aggregating broadcast-eligible join is aggregated by Exasol over the broadcast fan-out
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
        has_broadcast_join_block(&pushed),
        "an ungrouped scalar-over-aggregate join must aggregate over the broadcast fan-out:\n{pushed}"
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

/// Scenario: The fact scan skips row groups that no dimension join key matches
#[test]
fn e2e_composite_key_broadcast_join_over_a_clustered_fact_matches_seed() {
    setup_clustered();

    let query = format!(
        "SELECT o.O_ORDERKEY, c.C_NAME FROM {} o JOIN {} c \
         ON o.O_CUSTKEY = c.C_CUSTKEY AND o.O_SEGMENT = c.C_SEGMENT WHERE c.C_REGION = 'hot'",
        vs_table(VS_CLUSTERED, CLUSTERED_FACT_TABLE),
        vs_table(VS_CLUSTERED, CLUSTERED_DIM_TABLE)
    );
    let mut conn = exa_conn();
    let pushed = isolated_pushdown_statement(&mut conn, &query);
    assert!(
        has_broadcast_join_block(&pushed),
        "the composite-key join must stay on the broadcast route:\n{pushed}"
    );
    let condition = pushed_join_condition(&pushed);
    for key in ["O_CUSTKEY", "O_SEGMENT"] {
        assert!(
            condition.contains(key),
            "the join block must carry the {key} key: {condition}"
        );
    }

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

const ORDERS_JOIN: &str = "{T}.FACT_ORDERS o JOIN {T}.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY";
const SELF_JOIN: &str = "{T}.FACT_ORDERS a JOIN {T}.FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY";

/// What differs when `query`, a join of `legs` legs that names its schema `{T}`, broadcasts a pair
/// on [`VS_NAME`] and takes the N-scan on [`VS_NAME_LOW`], and returns its native rows on both.
fn broadcast_mismatches(conn: &mut ExaConn, legs: usize, query: &str) -> Vec<String> {
    [
        (VS_NAME, PathCheck::Has(BROADCAST_JOIN)),
        (VS_NAME_LOW, PathCheck::NScan(legs)),
    ]
    .into_iter()
    .filter_map(|(vs_name, path)| {
        native_copy_mismatch(conn, vs_name, JOIN_ORACLE, query, path, Expect::Rows)
    })
    .collect()
}

/// `plan` with each shard list's file lists sorted, since a pushdown assigns files to shards in
/// no fixed order.
fn shard_order_free(plan: &str) -> String {
    const VALUES: &str = "VALUES ";
    const SHARDS: &str = ") AS shards";
    let mut normalized = String::new();
    let mut rest = plan;
    while let Some(end) = rest.find(SHARDS) {
        let Some(start) = rest[..end].rfind(VALUES) else {
            break;
        };
        normalized.push_str(&rest[..start + VALUES.len()]);
        let mut file_lists: Vec<&str> = rest[start + VALUES.len()..end]
            .split('\'')
            .skip(1)
            .step_by(2)
            .collect();
        file_lists.sort_unstable();
        normalized.push_str(&file_lists.join(","));
        normalized.push_str(SHARDS);
        rest = &rest[end + SHARDS.len()..];
    }
    normalized.push_str(rest);
    normalized
}

/// The data files a pushdown statement scans, counted by their Parquet paths.
fn scanned_file_count(pushed: &str) -> usize {
    pushed.matches(".parquet").count()
}

/// Scenario: The filters of an inner join's condition are planned as WHERE conjuncts
#[test]
fn e2e_join_on_filters_plan_as_where_conjuncts() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let orders = format!("SELECT o.O_ORDERKEY, c.C_NAME FROM {ORDERS_JOIN}");
    let self_join = format!("SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {SELF_JOIN}");
    let cases = [
        (&orders, "c.C_NAME = 'customer-02'".to_string()),
        (
            &orders,
            format!("o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'"),
        ),
        (&orders, "o.O_ORDERKEY > c.C_CUSTKEY".to_string()),
        (
            &orders,
            "(c.C_NAME = 'customer-01' OR o.O_TOTALPRICE > 100)".to_string(),
        ),
        // `b`'s filter skips the first fact file for `b` only.
        (&self_join, "b.O_ORDERKEY > 5".to_string()),
    ];
    let mut failures = Vec::new();
    for (select, filter) in cases {
        let on = format!("{select} AND {filter}");
        let where_ = format!("{select} WHERE {filter}");
        for vs_name in [VS_NAME, VS_NAME_LOW] {
            let on_plan = shard_order_free(&isolated_pushdown_statement(
                &mut conn,
                &on.replace("{T}", vs_name),
            ));
            let where_plan = shard_order_free(&isolated_pushdown_statement(
                &mut conn,
                &where_.replace("{T}", vs_name),
            ));
            if on_plan != where_plan {
                let at = on_plan
                    .bytes()
                    .zip(where_plan.bytes())
                    .take_while(|(a, b)| a == b)
                    .count();
                failures.push(format!(
                    "{vs_name}: `{on}` must plan as `{where_}`, from byte {at}:\n{}\n{}",
                    &on_plan[at..],
                    &where_plan[at..]
                ));
            }
            failures.extend(native_copy_mismatch(
                &mut conn,
                vs_name,
                JOIN_ORACLE,
                &on,
                PathCheck::Unchecked,
                Expect::Rows,
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    let unfiltered = isolated_pushdown_statement(&mut conn, &self_join.replace("{T}", VS_NAME));
    let filtered = isolated_pushdown_statement(
        &mut conn,
        &format!("{self_join} AND b.O_ORDERKEY > 5").replace("{T}", VS_NAME),
    );
    assert_eq!(
        scanned_file_count(&filtered),
        scanned_file_count(&unfiltered) - 1,
        "the ON filter on `b` must prune one of `b`'s two files and none of `a`'s:\n\
         {filtered}\n{unfiltered}"
    );
}

/// Scenario: An ON filter on one leg of a join of three or more legs reaches that leg's scan
#[test]
fn e2e_three_table_join_on_filters_reach_their_leg_scans() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let query = "SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY FROM {T}.DIM_CUSTOMER c \
                 JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY AND c.C_NAME = 'customer-02' \
                 JOIN {T}.FACT_LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY AND l.L_LINENUMBER = 1";

    if let Some(failure) = native_copy_mismatch(
        &mut conn,
        VS_NAME_LOW,
        JOIN_ORACLE,
        query,
        PathCheck::NScan(3),
        Expect::Rows,
    ) {
        panic!("{failure}");
    }
    assert!(
        conn.query_row_count(&query.replace("{T}", JOIN_ORACLE)) > 0,
        "the filtered join must return rows, or the comparison proves nothing"
    );

    let pushed = isolated_pushdown_statement(&mut conn, &query.replace("{T}", VS_NAME_LOW));
    let where_query = "SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY FROM {T}.DIM_CUSTOMER c \
                       JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY \
                       JOIN {T}.FACT_LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY \
                       WHERE c.C_NAME = 'customer-02' AND l.L_LINENUMBER = 1";
    assert_eq!(
        shard_order_free(&pushed),
        shard_order_free(&isolated_pushdown_statement(
            &mut conn,
            &where_query.replace("{T}", VS_NAME_LOW)
        )),
        "the three-leg ON filters must plan, and prune, as the same WHERE filters"
    );
    let filters = pushed_scan_filters(&pushed);
    for (column, other) in [("C_NAME", "L_LINENUMBER"), ("L_LINENUMBER", "C_NAME")] {
        assert!(
            filters
                .iter()
                .any(|filter| filter.contains(column) && !filter.contains(other)),
            "the {column} ON filter must reach its own leg's scan filter: {filters:?}\n{pushed}"
        );
    }
}

/// Every fan-out `EMITS` column list in the pushed SQL, one per leg.
fn leg_emits_clauses(pushed: &str) -> Vec<String> {
    pushed
        .match_indices("EMITS (")
        .map(|(at, _)| emits_clause(&pushed[at..]))
        .collect()
}

/// Scenario: Join conditions attach greedily by LEG set and leg-local filters push into each leg
/// Scenario: A join of three or more legs broadcasts one pair and scans the other legs
#[test]
fn e2e_join_fan_out_emits_no_column_that_only_its_scan_filter_reads() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    // (WHERE, a LINEITEM column, whether the outer wrapper reads it, so the leg emits it). The
    // scan declines a three-argument INSTR, so that conjunct is the wrapper's.
    let cases = [
        ("l.L_LINENUMBER = 1", "L_LINENUMBER", false),
        ("INSTR(l.L_RETURNFLAG, 'R', 2) = 0", "L_RETURNFLAG", true),
        (
            "(l.L_LINENUMBER = 1 OR c.C_NAME = 'customer-02')",
            "L_LINENUMBER",
            true,
        ),
    ];

    let mut failures = Vec::new();
    for (filter, column, emitted) in cases {
        let query = format!(
            "SELECT c.C_NAME, l.L_QUANTITY FROM {{T}}.DIM_CUSTOMER c \
             JOIN {{T}}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY \
             JOIN {{T}}.FACT_LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY WHERE {filter}"
        );
        failures.extend(broadcast_mismatches(&mut conn, 3, &query));
        for vs_name in [VS_NAME, VS_NAME_LOW] {
            let pushed = isolated_pushdown_statement(&mut conn, &query.replace("{T}", vs_name));
            let legs = leg_emits_clauses(&pushed);
            // Unquoted: a broadcast pair emits the column under its leg's prefix.
            if legs.iter().any(|emits| emits.contains(column)) != emitted {
                failures.push(format!(
                    "{vs_name} `{filter}`: a fan-out must emit {column} only when the outer \
                     WHERE reads it ({emitted}): {legs:?}\n{pushed}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Scenario: A join equality that reads several legs on one side stays in the condition
#[test]
fn e2e_three_table_join_equality_over_two_legs_stays_in_on() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let query = "SELECT c.C_NAME, o.O_ORDERKEY, l.L_LINENUMBER FROM {T}.DIM_CUSTOMER c \
                 JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY \
                 JOIN {T}.FACT_LINEITEM l ON c.C_CUSTKEY + o.O_ORDERKEY = l.L_ORDERKEY \
                 AND l.L_LINENUMBER = 1";

    if let Some(failure) = native_copy_mismatch(
        &mut conn,
        VS_NAME_LOW,
        JOIN_ORACLE,
        query,
        PathCheck::NScan(3),
        Expect::Rows,
    ) {
        panic!("{failure}");
    }
    assert!(
        conn.query_row_count(&query.replace("{T}", JOIN_ORACLE)) > 0,
        "the join must return rows, or the comparison proves nothing"
    );

    let pushed = isolated_pushdown_statement(&mut conn, &query.replace("{T}", VS_NAME_LOW));
    let (_, chain) = pushed
        .rsplit_once(r#"AS "LHS_T2""#)
        .unwrap_or_else(|| panic!("the N-scan must alias the third leg LHS_T2:\n{pushed}"));
    assert!(
        chain.contains(" ON ") && chain.contains('+') && chain.contains(r#""L_ORDERKEY""#),
        "the two-leg equality must stay in the third leg's ON:\n{pushed}"
    );
    assert!(
        !chain.contains("L_LINENUMBER"),
        "the L_LINENUMBER filter must leave the ON:\n{pushed}"
    );
    let filters = pushed_scan_filters(&pushed);
    assert!(
        filters.iter().any(|filter| filter.contains("L_LINENUMBER")),
        "the L_LINENUMBER filter must reach the lineitem leg's scan filter: {filters:?}\n{pushed}"
    );
}

/// Scenario: A two-leg join on a composite key is planned as a broadcast fan-out
#[test]
fn e2e_composite_key_join_takes_the_broadcast_path_and_matches_native() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let two_keys = format!("{ORDERS_JOIN} AND o.O_ORDERKEY = c.C_CUSTKEY");
    let reversed = "{T}.FACT_ORDERS o JOIN {T}.DIM_CUSTOMER c \
                    ON c.C_CUSTKEY = o.O_ORDERKEY AND c.C_CUSTKEY = o.O_CUSTKEY";
    let dated = format!("{SELF_JOIN} AND a.O_ORDERDATE = b.O_ORDERDATE");
    let three_keys = format!("{dated} AND a.O_TOTALPRICE = b.O_TOTALPRICE");
    // Every order of a customer shares the year, so each side holds every key tuple twice.
    let duplicated = format!("{SELF_JOIN} AND YEAR(a.O_ORDERDATE) = YEAR(b.O_ORDERDATE)");
    let queries = [
        format!("SELECT o.O_ORDERKEY, c.C_NAME FROM {two_keys}"),
        format!("SELECT o.O_ORDERKEY, c.C_NAME FROM {reversed}"),
        format!("SELECT a.O_ORDERKEY, b.O_ORDERDATE FROM {dated}"),
        format!("SELECT a.O_ORDERKEY, b.O_TOTALPRICE FROM {three_keys}"),
        format!("SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {duplicated}"),
        format!("SELECT COUNT(*), SUM(b.O_TOTALPRICE) FROM {dated}"),
        format!("SELECT a.O_CUSTKEY, COUNT(*) FROM {duplicated} GROUP BY a.O_CUSTKEY"),
        format!(
            "SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {duplicated} \
             ORDER BY a.O_ORDERKEY DESC, b.O_ORDERKEY LIMIT 3"
        ),
    ];
    let failures: Vec<String> = queries
        .iter()
        .flat_map(|query| broadcast_mismatches(&mut conn, 2, query))
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    assert!(
        conn.query_row_count(&format!("SELECT * FROM {duplicated}").replace("{T}", JOIN_ORACLE))
            > FACT_ORDERS_ROWS as i64,
        "the duplicated-key self-join must match each order more than once"
    );
    let pushed = isolated_pushdown_statement(
        &mut conn,
        &format!("SELECT a.O_ORDERKEY, b.O_ORDERDATE FROM {dated}").replace("{T}", VS_NAME),
    );
    let condition = pushed_join_condition(&pushed);
    for key in ["O_CUSTKEY", "O_ORDERDATE", " AND "] {
        assert!(
            condition.contains(key),
            "the join block must carry every key: {condition}"
        );
    }
}

/// Scenario: A join key that compares expressions is planned as a broadcast fan-out
#[test]
fn e2e_expression_key_join_takes_the_broadcast_path_and_matches_native() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let orders = "{T}.FACT_ORDERS o JOIN {T}.DIM_CUSTOMER c";
    let queries = [
        format!("SELECT o.O_ORDERKEY, c.C_NAME FROM {orders} ON o.O_CUSTKEY = c.C_CUSTKEY + 1"),
        format!("SELECT o.O_ORDERKEY, c.C_NAME FROM {orders} ON o.O_CUSTKEY - 1 = c.C_CUSTKEY"),
        format!(
            "SELECT o.O_ORDERKEY, c.C_NAME FROM {orders} ON o.O_CUSTKEY = c.C_CUSTKEY + 1 \
             AND c.C_NAME <> 'customer-01'"
        ),
    ];
    let failures: Vec<String> = queries
        .iter()
        .flat_map(|query| broadcast_mismatches(&mut conn, 2, query))
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Scenario: A two-leg join with filters in its condition is planned as a broadcast fan-out
#[test]
fn e2e_join_with_on_filters_takes_the_broadcast_path_and_matches_native() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let select = format!("SELECT o.O_ORDERKEY, c.C_NAME FROM {ORDERS_JOIN}");
    let dimension_filter = format!("{select} AND c.C_NAME = 'customer-02'");
    // The scan declines the two-argument SECOND, as it does in WHERE, so Exasol's WHERE applies it.
    let declined =
        format!("SELECT o.O_ORDERKEY, c.C_CUSTKEY FROM {ORDERS_JOIN} AND {SECOND_IS_ZERO}");
    let broadcast = [
        dimension_filter.clone(),
        format!("{select} AND o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'"),
        format!("{select} AND o.O_ORDERKEY > c.C_CUSTKEY"),
        format!("{select} AND (c.C_NAME = 'customer-01' OR o.O_TOTALPRICE > 100)"),
        format!("{select} AND c.C_NAME <> 'customer-01' WHERE o.O_ORDERKEY > 2"),
        // The moved literal filter bounds C_NAME's width, so the aggregate stays broadcast.
        format!(
            "SELECT c.C_NAME, COUNT(*) FROM {ORDERS_JOIN} AND c.C_NAME = 'customer-02' \
             GROUP BY c.C_NAME"
        ),
        format!("SELECT a.O_ORDERKEY, b.O_ORDERKEY FROM {SELF_JOIN} AND b.O_ORDERKEY > 5"),
        declined.clone(),
    ];
    let mut failures: Vec<String> = broadcast
        .iter()
        .flat_map(|query| broadcast_mismatches(&mut conn, 2, query))
        .collect();
    let pushed = isolated_pushdown_statement(&mut conn, &declined.replace("{T}", VS_NAME));
    if !exasol_where_holds(&pushed, "SECOND(") {
        failures.push(format!(
            "Exasol's WHERE must apply the declined ON conjunct: {pushed}"
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    let pushed = isolated_pushdown_statement(&mut conn, &dimension_filter.replace("{T}", VS_NAME));
    let filters = pushed_scan_filters(&pushed);
    assert!(
        filters.iter().any(|filter| filter.contains("C_NAME")),
        "the common spec's filter must carry the moved C_NAME conjunct: {filters:?}\n{pushed}"
    );
}

/// CUSTOMER, ORDERS, and LINEITEM in a chain. ORDERS is broadcast into LINEITEM's shards.
const CHAIN: &str = "{T}.DIM_CUSTOMER c JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY \
                     JOIN {T}.FACT_LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY";
/// LINEITEM joined to ORDERS and to SUPPLIER on keys of its own.
const STAR: &str = "{T}.FACT_LINEITEM l JOIN {T}.FACT_ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY \
                    JOIN {T}.DIM_SUPPLIER s ON l.L_SUPPKEY = s.S_SUPPKEY";

/// The fan-out of `pushed` that carries the join block, as a statement of its own.
fn pair_fan_out(pushed: &str) -> &str {
    let block = pushed
        .find(BROADCAST_JOIN)
        .unwrap_or_else(|| panic!("no broadcast pair:\n{pushed}"));
    let open = pushed[..block]
        .rfind("(SELECT ")
        .unwrap_or_else(|| panic!("the pair's fan-out must be a subquery:\n{pushed}"));
    parenthesized(pushed, open)
}

/// The rows the broadcast pair's fan-out of `query` emits, `query` over [`VS_NAME`].
fn pair_fan_out_rows(conn: &mut ExaConn, query: &str) -> i64 {
    let pushed = isolated_pushdown_statement(conn, &query.replace("{T}", VS_NAME));
    conn.query_row_count(pair_fan_out(&pushed))
}

/// Scenario: A join of three or more legs broadcasts one pair and scans the other legs
/// Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper
/// Scenario: A fallback leg's fan-out scan spec never carries a row limit
#[test]
fn e2e_multi_way_join_broadcasts_one_pair_and_matches_native() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let cases = [
        (
            3,
            format!("SELECT l.L_LINENUMBER, l.L_QUANTITY, o.O_ORDERDATE, s.S_SUPPKEY FROM {STAR}"),
        ),
        (
            3,
            format!("SELECT c.C_NAME, l.L_LINENUMBER, l.L_QUANTITY FROM {CHAIN}"),
        ),
        (
            4,
            format!(
                "SELECT c.C_NAME, l.L_LINENUMBER, s.S_NAME FROM {CHAIN} \
                 JOIN {{T}}.DIM_SUPPLIER s ON l.L_SUPPKEY = s.S_SUPPKEY"
            ),
        ),
        (
            3,
            "SELECT a.O_ORDERKEY, b.O_ORDERKEY, c.O_ORDERKEY FROM {T}.FACT_ORDERS a \
             JOIN {T}.FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY \
             JOIN {T}.FACT_ORDERS c ON b.O_ORDERKEY = c.O_ORDERKEY"
                .to_string(),
        ),
        (
            3,
            "SELECT e.ID, l.ID, f.SCORE FROM {T}.EVENTS e JOIN {T}.LABELS l ON e.ID = l.ID \
             JOIN {T}.EVENTS f ON l.ID = f.ID"
                .to_string(),
        ),
        // ORDERS ranks first by its own filter and holds each O_CUSTKEY twice.
        (
            3,
            format!(
                "SELECT l.L_LINENUMBER, o.O_ORDERKEY, s.S_SUPPKEY FROM {{T}}.FACT_LINEITEM l \
                 JOIN {{T}}.FACT_ORDERS o ON l.L_SUPPKEY = o.O_CUSTKEY \
                 JOIN {{T}}.DIM_SUPPLIER s ON l.L_SUPPKEY = s.S_SUPPKEY \
                 WHERE o.O_ORDERDATE >= DATE '{ORDERDATE_LOWER_BOUND}'"
            ),
        ),
        (
            3,
            "SELECT c.C_CUSTKEY, o.O_ORDERKEY, l.L_LINENUMBER FROM {T}.DIM_CUSTOMER c \
             JOIN {T}.FACT_ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY \
             JOIN {T}.FACT_LINEITEM l ON c.C_CUSTKEY + o.O_ORDERKEY = l.L_ORDERKEY"
                .to_string(),
        ),
        (
            3,
            format!(
                "SELECT c.C_NAME, l.L_LINENUMBER FROM {CHAIN} WHERE c.C_CUSTKEY < l.L_QUANTITY"
            ),
        ),
        (
            3,
            format!(
                "SELECT c.C_NAME, SUM(l.L_QUANTITY) FROM {CHAIN} \
                 GROUP BY c.C_NAME HAVING SUM(l.L_QUANTITY) > 5"
            ),
        ),
        (
            3,
            format!(
                "SELECT o.O_ORDERKEY, l.L_LINENUMBER, c.C_NAME FROM {CHAIN} \
                 ORDER BY o.O_ORDERKEY DESC, l.L_LINENUMBER LIMIT 5 OFFSET 2"
            ),
        ),
    ];
    let mut failures: Vec<String> = Vec::new();
    for (legs, query) in &cases {
        if conn.query_row_count(&query.replace("{T}", JOIN_ORACLE)) == 0 {
            failures.push(format!("`{query}` must return rows natively"));
        }
        failures.extend(broadcast_mismatches(&mut conn, *legs, query));
        for vs_name in [VS_NAME, VS_NAME_LOW] {
            let pushed = isolated_pushdown_statement(&mut conn, &query.replace("{T}", vs_name));
            if let Some(key) = SHARD_WINDOW.iter().find(|key| pushed.contains(*key)) {
                failures.push(format!(
                    "{vs_name} `{query}`: no fan-out carries {key}: {pushed}"
                ));
            }
            if let Some((_, window)) = query.split_once(" LIMIT ")
                && vs_name == VS_NAME
                && !pushed.ends_with(&format!(" LIMIT {window}"))
            {
                failures.push(format!(
                    "`{query}`: Exasol applies LIMIT {window}: {pushed}"
                ));
            }
        }
    }
    // Every ORDERS file is pruned, so the join is empty before any plan.
    failures.extend(native_copy_mismatch(
        &mut conn,
        VS_NAME,
        JOIN_ORACLE,
        &format!(
            "SELECT c.C_NAME, l.L_LINENUMBER FROM {CHAIN} WHERE o.O_ORDERDATE < DATE '1900-01-01'"
        ),
        PathCheck::Unchecked,
        Expect::Rows,
    ));
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    assert_eq!(
        pair_fan_out_rows(&mut conn, &cases[1].1),
        LINEITEM_ROWS as i64,
        "the pair emits each line item that matches an order, once"
    );
}

/// Scenario: The broadcast pair is the eligible pair whose dimension has a filter of its own
#[test]
fn e2e_multi_way_join_broadcasts_the_filtered_dimension_pair() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    let query = format!(
        "SELECT l.L_LINENUMBER, o.O_ORDERDATE, s.S_NAME FROM {STAR} WHERE s.S_NAME = 'supplier-02'"
    );

    let failures = broadcast_mismatches(&mut conn, 3, &query);
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    let pushed = isolated_pushdown_statement(&mut conn, &query.replace("{T}", VS_NAME));
    let fan_out = pair_fan_out(&pushed);
    let (fact, join) = fan_out
        .split_once(BROADCAST_JOIN)
        .expect("the pair carries a join block");
    assert!(
        fact.contains(E2E_LINEITEM_TABLE) && join.contains(E2E_SUPPLIER_TABLE),
        "SUPPLIER, filtered, is broadcast into LINEITEM's shards, and ORDERS keeps its own \
         fan-out:\n{pushed}"
    );
    let matching = conn.query_row_count(&format!(
        "SELECT * FROM {JOIN_ORACLE}.FACT_LINEITEM l JOIN {JOIN_ORACLE}.DIM_SUPPLIER s \
         ON l.L_SUPPKEY = s.S_SUPPKEY WHERE s.S_NAME = 'supplier-02'"
    ));
    assert!(matching > 0 && matching < LINEITEM_ROWS as i64);
    assert_eq!(
        pair_fan_out_rows(&mut conn, &query),
        matching,
        "the pair emits only the line items of the filtered supplier"
    );
}

/// Scenario: A pair that is not eligible is not broadcast
#[test]
fn e2e_multi_way_join_without_eligible_pair_keeps_the_n_scan() {
    setup_e2e();
    let mut conn = exa_conn();
    copy_join_tables_natively(&mut conn, VS_NAME);
    // Every dimension would carry an unbounded text column into Exasol's join. Each case of
    // `e2e_multi_way_join_broadcasts_one_pair_and_matches_native` proves a pair over
    // `JOIN_BROADCAST_MAX_BYTES` on `VS_NAME_LOW`.
    let wide = "SELECT e.NAME, l.LABEL, f.NAME FROM {T}.EVENTS e JOIN {T}.LABELS l ON e.ID = l.ID \
                JOIN {T}.EVENTS f ON l.ID = f.ID";
    if let Err(failure) = matching_pushdown(&mut conn, VS_NAME, PathCheck::NScan(3), wide) {
        panic!("{failure}");
    }
}

/// Scenario: A wrapped derived projection routes to a native wrapper on every path
#[test]
fn e2e_broadcast_join_with_a_declined_select_item_keeps_the_broadcast() {
    setup_e2e();
    copy_join_tables_natively(&mut exa_conn(), VS_NAME);
    let queries = [
        format!("SELECT o.O_ORDERKEY, CAST(o.O_ORDERDATE AS VARCHAR(20)) FROM {ORDERS_JOIN}"),
        format!("SELECT c.C_NAME, CAST(o.O_ORDERDATE AS VARCHAR(20)), l.L_LINENUMBER FROM {CHAIN}"),
    ];
    for setting in [DEFAULT_SESSION, DMY] {
        let mut conn = exa_session(setting);
        for query in &queries {
            let pushed =
                matching_pushdown(&mut conn, VS_NAME, PathCheck::Has(BROADCAST_JOIN), query)
                    .unwrap_or_else(|failure| panic!("[{setting}] {failure}"));
            let select = pushed.split(" FROM (").next().unwrap_or_default();
            assert!(
                select.contains("CAST("),
                "[{setting}] Exasol computes the item over the fan-out: {pushed}"
            );
        }
    }
}
