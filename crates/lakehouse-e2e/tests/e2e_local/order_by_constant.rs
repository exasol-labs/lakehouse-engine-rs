//! Constant `ORDER BY` keys through Exasol: a constant sort key never reaches the pushed
//! SQL as a bare number that Exasol reads as a select-list position. Fail, never skip, without
//! the stack.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::SEED_TOTAL_ROWS;
use crate::fixtures::{
    LAKEHOUSE_VS, assert_two_scan_join_wrapper, events_labels_join, events_table,
    explain_virtual_pushdown_sql, lakehouse_vs,
};

/// Expected first column of a query.
enum Rows {
    /// These ids, in this order.
    InOrder(&'static [i64]),
    /// This many distinct seeded ids, in any order.
    DistinctSeeded(usize),
}

fn assert_rows(conn: &mut ExaConn, query: &str, expected: &Rows) {
    let ids = int_column(&conn.query_columns(query)[0]);
    match expected {
        Rows::InOrder(want) => assert_eq!(ids, *want, "{query}"),
        Rows::DistinctSeeded(count) => {
            let mut distinct = ids.clone();
            distinct.sort_unstable();
            distinct.dedup();
            assert_eq!(
                distinct.len(),
                *count,
                "{query} must return distinct ids: {ids:?}"
            );
            assert_eq!(
                ids.len(),
                *count,
                "{query} must return {count} rows: {ids:?}"
            );
            assert!(
                ids.iter()
                    .all(|id| (1..=SEED_TOTAL_ROWS as i64).contains(id)),
                "{query} must return seeded ids: {ids:?}"
            );
        }
    }
}

/// Scenario: A constant sort key does not become a select-list position
/// Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe
#[test]
fn constant_sort_key_returns_the_native_order() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let t = events_table();

    for (query, expected) in [
        (
            format!("SELECT id, 1 FROM {t} ORDER BY 2 DESC, 1 LIMIT 3"),
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            format!("SELECT id, 5 FROM {t} ORDER BY 2 DESC, 1 LIMIT 3"),
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            format!("SELECT id, 1 FROM {t} WHERE id <= 5 ORDER BY 2 DESC, 1"),
            Rows::InOrder(&[1, 2, 3, 4, 5]),
        ),
        (
            format!("SELECT id FROM {t} ORDER BY 1+0 DESC, id LIMIT 3"),
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            format!("SELECT id, 1, COUNT(*) FROM {t} GROUP BY id ORDER BY 2 DESC, 1 LIMIT 3"),
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            format!("SELECT id, 5, COUNT(*) FROM {t} GROUP BY id ORDER BY 2 DESC, 1 LIMIT 3"),
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            format!("SELECT id FROM {t} ORDER BY RANDOM() LIMIT 3"),
            Rows::DistinctSeeded(3),
        ),
    ] {
        assert_rows(&mut conn, &query, &expected);
    }
}

/// Scenario: A constant sort key does not decide the bounded top-N path
/// Scenario: Per-shard row limit is emitted only alongside the matching per-shard sort
/// Scenario: Unsupported ordered-query shapes decline the ordered-top-N path
/// Scenario: A constant sort key does not become a select-list position
/// Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe
#[test]
fn constant_sort_key_planning_follows_the_other_keys() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let t = events_table();

    for (query, bounded) in [
        (
            format!("SELECT id, 1 FROM {t} ORDER BY 2 DESC, 1 LIMIT 3"),
            true,
        ),
        (
            format!("SELECT id, 1 FROM {t} ORDER BY 2 DESC, ABS(score) LIMIT 3"),
            false,
        ),
    ] {
        let pushed = explain_virtual_pushdown_sql(&mut conn, &query);
        assert_eq!(
            pushed.contains("\"order_by\":"),
            bounded,
            "{query}: the scan spec's order_by field:\n{pushed}"
        );
        assert_eq!(
            pushed.contains("\"limit\":3"),
            bounded,
            "{query}: the scan spec's limit field:\n{pushed}"
        );
        let columns = conn.query_columns(&query);
        assert_eq!(columns.len(), 2, "{query}: result columns");
        assert_eq!(int_column(&columns[0]), [1, 2, 3], "{query}");
    }
}

/// Scenario: Constant sort keys leave a broadcast-eligible join on the broadcast path
/// Scenario: Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback
/// Scenario: A constant sort key does not become a select-list position
#[test]
fn constant_sort_key_join_routes_by_the_other_keys() {
    lakehouse_vs();
    let mut conn = exa_conn();

    for (query, broadcast, expected) in [
        (
            events_labels_join(LAKEHOUSE_VS, "e.ID, 1", "ORDER BY 2 DESC, 1 LIMIT 3"),
            true,
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            events_labels_join(LAKEHOUSE_VS, "e.ID, 5", "ORDER BY 2 DESC, 1 LIMIT 3"),
            true,
            Rows::InOrder(&[1, 2, 3]),
        ),
        (
            events_labels_join(LAKEHOUSE_VS, "e.ID, 1", "ORDER BY 2 LIMIT 3"),
            true,
            Rows::DistinctSeeded(3),
        ),
        (
            events_labels_join(
                LAKEHOUSE_VS,
                "e.ID, 1",
                "ORDER BY 2 DESC, ABS(e.SCORE) LIMIT 3",
            ),
            false,
            Rows::InOrder(&[1, 2, 3]),
        ),
    ] {
        let pushed = explain_virtual_sql(&mut conn, &query);
        assert_eq!(
            has_broadcast_join_block(&pushed),
            broadcast,
            "{query}: broadcast route:\n{pushed}"
        );
        assert_eq!(
            has_two_scan_wrapper(&pushed),
            !broadcast,
            "{query}: two-scan route:\n{pushed}"
        );
        assert_rows(&mut conn, &query, &expected);
    }
}

/// Scenario: An ORDER BY of constants only runs as an unordered LIMIT
#[test]
fn constant_only_order_by_runs_as_an_unordered_limit() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let query = format!("SELECT id, 1 FROM {} ORDER BY 2 LIMIT 3", events_table());

    let pushed = explain_virtual_pushdown_sql(&mut conn, &query);
    assert!(pushed.contains("\"limit\":3"), "{query}:\n{pushed}");
    assert!(!pushed.contains("\"order_by\":"), "{query}:\n{pushed}");
    assert!(!pushed.contains("ORDER BY"), "{query}:\n{pushed}");

    let columns = conn.query_columns(&query);
    assert_eq!(int_column(&columns[1]), [1, 1, 1], "{query}: the constant");
    assert_rows(&mut conn, &query, &Rows::DistinctSeeded(3));
}

/// Scenario: An ORDER BY of constants only keeps its OFFSET window
/// Scenario: Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback
#[test]
fn constant_only_order_by_keeps_its_offset_window() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let t = events_table();
    const WINDOW: &str = "ORDER BY 2 LIMIT 3 OFFSET 18";

    let join =
        |constant: i32| events_labels_join(LAKEHOUSE_VS, &format!("e.ID, {constant}"), WINDOW);

    for constant in [1, 5] {
        for query in [
            format!("SELECT id, {constant} FROM {t} {WINDOW}"),
            format!("SELECT id, {constant}, COUNT(*) FROM {t} GROUP BY id {WINDOW}"),
            join(constant),
        ] {
            let pushed = explain_virtual_sql(&mut conn, &query);
            assert!(
                pushed.contains("ORDER BY FALSE"),
                "{query} must order by a constant Exasol does not read as a position:\n{pushed}"
            );
            assert_rows(&mut conn, &query, &Rows::DistinctSeeded(2));
        }
        assert_two_scan_join_wrapper(&mut conn, &join(constant));
    }
}
