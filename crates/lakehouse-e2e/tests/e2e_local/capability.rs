//! E2E capability-alignment tests: each advertised capability group runs the full
//! advertised → translated → executed path. Seed: id 1..=20, score = 5.0 * id,
//! name = "event-NN", event_date = 2024-01-01 + (id-1) days. Fail, never skip,
//! without the stack.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{
    CHAR_PAD_COL, CHAR_PAD_OVER_LENGTH, CHAR_PAD_SHORT_TRAILING_SPACE, ExpectedValue,
    TYPED_TABLE_TOTAL_ROWS,
};
use crate::common::stack::{ExasolFlavor, exasol_flavor};
use crate::common::timestamp_precision::{
    engine_honors_declared_precision, expected_timestamp_precision,
};
use crate::fixtures::{
    TYPED_COLUMNS, assert_broadcast_join_wrapper, assert_single_table_wrapper,
    echoed_pushdown_requests, events_table, explain_virtual_pushdown_sql, lakehouse_vs,
    typed_table, vs_char_pad_table, vs_dim_table, vs_fact_table,
};

/// The measured worst-case divergence of the scan's float division from native Exasol is
/// ~1 ULP (3.17e-16).
const FLOAT_DIV_ORACLE_REL_TOLERANCE: f64 = 1e-15;

#[test]
fn e2e_advertises_inner_equi_join_capability() {
    lakehouse_vs();
    let mut conn = exa_conn();

    // EXPLAIN VIRTUAL echoes the adapter's capability response; the comma-adjacent
    // `"JOIN","JOIN_TYPE_INNER"` substring isolates the bare `JOIN` token.
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

#[test]
fn advertises_count_distinct_capability() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let query = format!("SELECT COUNT(DISTINCT name) FROM {}", events_table());
    let advertised = explain_virtual_sql(&mut conn, &query);

    assert!(
        advertised.contains("\"capabilities\":"),
        "EXPLAIN VIRTUAL must echo the getCapabilities response:\n{advertised}"
    );
    assert!(
        advertised.contains("\"FN_AGG_COUNT_DISTINCT\""),
        "getCapabilities must advertise the FN_AGG_COUNT_DISTINCT capability:\n{advertised}"
    );
    assert!(
        advertised.contains("\"AGGREGATE_SINGLE_GROUP\""),
        "getCapabilities must advertise AGGREGATE_SINGLE_GROUP alongside \
         FN_AGG_COUNT_DISTINCT:\n{advertised}"
    );
}

#[test]
fn e2e_math_functions_in_filter() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} WHERE ABS(score - 50.0) < 20.0 ORDER BY id",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");

    let expected_count = 7i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "ABS(score - 50.0) < 20.0 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    for v in &cols[1] {
        let s = parse_numeric(v);
        assert!(
            (s - 50.0).abs() < 20.0,
            "filter violated: ABS({s} - 50.0) = {} >= 20.0",
            (s - 50.0).abs()
        );
    }

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 7 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

#[test]
fn e2e_string_functions_in_filter() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, name FROM {} WHERE LOWER(name) LIKE 'event-1%' ORDER BY id",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, name): {cols:?}");

    let expected_count = 10i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "LOWER(name) LIKE 'event-1%' must return {expected_count} rows, got {}",
        cols[0].len()
    );

    for v in &cols[1] {
        let n = v
            .as_str()
            .unwrap_or_else(|| panic!("name not a string: {v:?}"));
        assert!(
            n.starts_with("event-1"),
            "filter violated: name '{n}' does not start with 'event-1'"
        );
    }

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 10 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

#[test]
fn e2e_date_functions_in_filter() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, event_date FROM {} WHERE EXTRACT(DAY FROM event_date) > 10 ORDER BY id",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        2,
        "expected 2 columns (id, event_date): {cols:?}"
    );

    let expected_count = 10i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "EXTRACT(DAY FROM event_date) > 10 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 11 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

/// Sorted integer values of the first result column; an empty result set
/// carries no columns, so it yields an empty list.
fn ids_of_first_column(conn: &mut ExaConn, sql: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = conn
        .query_columns(sql)
        .first()
        .map(|col| col.iter().map(parse_int).collect())
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

/// Scenario: Regexp scalar functions are deliberately not translated
#[test]
fn e2e_regexp_like_returns_native_rows() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let cases: [(&str, Vec<i64>); 4] = [
        ("name REGEXP_LIKE 'event-1'", vec![]),
        ("NOT (name REGEXP_LIKE 'event-1')", (1..=20).collect()),
        ("name REGEXP_LIKE '(e|ev)(e|ent-01)?'", vec![]),
        ("name REGEXP_LIKE '(?=event-0[1-3]).*'", vec![1, 2, 3]),
    ];

    for (predicate, expected_ids) in cases {
        let native_sql = format!(
            "SELECT COUNT(*) FROM (SELECT 'event-' || LPAD(TO_CHAR(LEVEL), 2, '0') AS name \
             FROM DUAL CONNECT BY LEVEL <= 20) WHERE {predicate}"
        );
        let native_count = conn.query_scalar_i64(&native_sql);
        assert_eq!(
            native_count,
            expected_ids.len() as i64,
            "native Exasol must return {} rows for `{predicate}`; the fixture no longer \
             discriminates this predicate",
            expected_ids.len()
        );

        let sql = format!("SELECT id FROM {} WHERE {predicate}", events_table());
        let ids = ids_of_first_column(&mut conn, &sql);
        assert_eq!(
            ids, expected_ids,
            "`{predicate}` over the virtual schema must return the native rows"
        );
    }

    let sql = format!(
        "SELECT id, name FROM {} WHERE id < 5 AND name REGEXP_LIKE 'event-0[1-3]'",
        events_table()
    );

    let request = explain_virtual_pushdown_request(&mut conn, &sql);
    let filter = &request["filter"];
    assert_eq!(
        filter["type"], "predicate_less",
        "only the id < 5 conjunct may be pushed: {request:#}"
    );
    assert_eq!(
        filter["left"]["name"], "ID",
        "the pushed conjunct must compare column ID: {request:#}"
    );
    assert!(
        !filter.to_string().to_lowercase().contains("regexp"),
        "the REGEXP_LIKE conjunct must not reach the adapter: {request:#}"
    );

    let advertised = explain_virtual_sql(&mut conn, &sql);
    assert!(
        advertised.contains("\"capabilities\":"),
        "EXPLAIN VIRTUAL must echo the getCapabilities response:\n{advertised}"
    );
    assert!(
        !advertised.contains("\"FN_PRED_REGEXP_LIKE\""),
        "getCapabilities must not advertise FN_PRED_REGEXP_LIKE:\n{advertised}"
    );

    let ids = ids_of_first_column(&mut conn, &sql);
    assert_eq!(ids, vec![1, 2, 3], "only ids 1..=3 match both conjuncts");
}

#[test]
fn e2e_selectlist_expression_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score * 2.0, UPPER(name) FROM {} WHERE id <= 3 ORDER BY id",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (id, score*2, UPPER(name)): {cols:?}"
    );
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1..3): {cols:?}");

    let expected_scores = [10.0f64, 20.0, 30.0];
    for (i, expected) in expected_scores.iter().enumerate() {
        let s = parse_numeric(&cols[1][i]);
        assert!(
            (s - expected).abs() < 0.001,
            "row {i}: score*2.0 must be {expected}, got {s}"
        );
    }

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

/// Not an adjacent-substring match: `explain_virtual_sql` joins result cells with a space,
/// so JSON tokens can be split across a cell boundary.
fn has_expr_after_projection(pushed_sql: &str) -> bool {
    pushed_sql
        .find(r#""projection""#)
        .and_then(|idx| pushed_sql[idx..].find(r#""expr""#))
        .is_some()
}

#[test]
fn e2e_selectlist_predicate_projection_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    {
        let sql = format!(
            "SELECT id, id IN (1,2,3) FROM {} WHERE id <= 5 ORDER BY id",
            events_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            2,
            "expected 2 columns (id, IN predicate): {cols:?}"
        );
        assert_eq!(cols[0].len(), 5, "expected 5 rows (id 1..5): {cols:?}");
        for (i, id_val) in cols[0].iter().enumerate() {
            let id = parse_int(id_val);
            let expected = [1i64, 2, 3].contains(&id);
            assert!(
                ExpectedValue::Bool(expected).matches(&cols[1][i]),
                "row {i} (id={id}): id IN (1,2,3) must be {expected}, got {:?}",
                cols[1][i]
            );
        }
        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            has_expr_after_projection(&pushed_sql),
            "IN predicate select-list item must push a positional Expr \
             projection, not the full-base-row fallback (#196), got:\n{pushed_sql}"
        );
    }

    {
        let sql = format!(
            "SELECT id, id BETWEEN 2 AND 4 FROM {} WHERE id <= 5 ORDER BY id",
            events_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            2,
            "expected 2 columns (id, BETWEEN predicate): {cols:?}"
        );
        assert_eq!(cols[0].len(), 5, "expected 5 rows (id 1..5): {cols:?}");
        for (i, id_val) in cols[0].iter().enumerate() {
            let id = parse_int(id_val);
            let expected = (2..=4).contains(&id);
            assert!(
                ExpectedValue::Bool(expected).matches(&cols[1][i]),
                "row {i} (id={id}): id BETWEEN 2 AND 4 must be {expected}, got {:?}",
                cols[1][i]
            );
        }
        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            has_expr_after_projection(&pushed_sql),
            "BETWEEN predicate select-list item must push a positional Expr \
             projection, not the full-base-row fallback (#196), got:\n{pushed_sql}"
        );
    }

    // id=3 is the only NULL `c_decimal_a` among ids 1..=4.
    {
        let sql = format!(
            "SELECT id, c_decimal_a IS NULL FROM {} WHERE id <= 4 ORDER BY id",
            typed_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            2,
            "expected 2 columns (id, IS NULL predicate): {cols:?}"
        );
        assert_eq!(cols[0].len(), 4, "expected 4 rows (id 1..4): {cols:?}");
        for (i, id_val) in cols[0].iter().enumerate() {
            let id = parse_int(id_val);
            let expected = TYPED_DECIMAL_A_UNSCALED
                .iter()
                .find(|&&(row_id, _)| row_id == id)
                .unwrap_or_else(|| panic!("no TYPED_DECIMAL_A_UNSCALED entry for id={id}"))
                .1
                .is_none();
            assert!(
                ExpectedValue::Bool(expected).matches(&cols[1][i]),
                "row {i} (id={id}): c_decimal_a IS NULL must be {expected}, got {:?}",
                cols[1][i]
            );
        }
        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            has_expr_after_projection(&pushed_sql),
            "IS NULL predicate select-list item must push a positional Expr \
             projection, not the full-base-row fallback (#196), got:\n{pushed_sql}"
        );
    }

    {
        let sql = format!(
            "SELECT id, c_decimal_a IS NOT NULL FROM {} WHERE id <= 4 ORDER BY id",
            typed_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            2,
            "expected 2 columns (id, IS NOT NULL predicate): {cols:?}"
        );
        assert_eq!(cols[0].len(), 4, "expected 4 rows (id 1..4): {cols:?}");
        for (i, id_val) in cols[0].iter().enumerate() {
            let id = parse_int(id_val);
            let expected = TYPED_DECIMAL_A_UNSCALED
                .iter()
                .find(|&&(row_id, _)| row_id == id)
                .unwrap_or_else(|| panic!("no TYPED_DECIMAL_A_UNSCALED entry for id={id}"))
                .1
                .is_some();
            assert!(
                ExpectedValue::Bool(expected).matches(&cols[1][i]),
                "row {i} (id={id}): c_decimal_a IS NOT NULL must be {expected}, got {:?}",
                cols[1][i]
            );
        }
        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            has_expr_after_projection(&pushed_sql),
            "IS NOT NULL predicate select-list item must push a positional \
             Expr projection, not the full-base-row fallback (#196), got:\n{pushed_sql}"
        );
    }

    {
        let sql = format!(
            "SELECT id, id <> 3 FROM {} WHERE id <= 5 ORDER BY id",
            events_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            2,
            "expected 2 columns (id, <> predicate): {cols:?}"
        );
        assert_eq!(cols[0].len(), 5, "expected 5 rows (id 1..5): {cols:?}");
        for (i, id_val) in cols[0].iter().enumerate() {
            let id = parse_int(id_val);
            let expected = id != 3;
            assert!(
                ExpectedValue::Bool(expected).matches(&cols[1][i]),
                "row {i} (id={id}): id <> 3 must be {expected}, got {:?}",
                cols[1][i]
            );
        }
        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            has_expr_after_projection(&pushed_sql),
            "<> predicate select-list item must push a positional Expr \
             projection, not the full-base-row fallback (#196), got:\n{pushed_sql}"
        );
    }
}

/// `c_decimal_a` by `id`, unscaled at scale 2.
const TYPED_DECIMAL_A_UNSCALED: [(i64, Option<i128>); 12] = [
    (1, Some(1050)),
    (2, Some(2025)),
    (3, None),
    (4, Some(3000)),
    (5, Some(1050)),
    (6, Some(4099)),
    (7, Some(1050)),
    (8, Some(5000)),
    (9, Some(2025)),
    (10, None),
    (11, Some(6000)),
    (12, Some(3000)),
];

/// `c_decimal_a` is absent: `TYPED_DECIMAL_A_UNSCALED` already tracks it.
struct TypedProbeRow {
    id: i64,
    decimal_b: Option<i128>,
    double: Option<f64>,
    varchar: Option<&'static str>,
    boolean: Option<bool>,
    price: Option<f64>,
    qty: i64,
}

const TYPED_ROWS_1_TO_3: [TypedProbeRow; 3] = [
    TypedProbeRow {
        id: 1,
        decimal_b: Some(1_000_000_001),
        double: Some(0.5),
        varchar: Some("aa"),
        boolean: Some(true),
        price: Some(2.0),
        qty: 3,
    },
    TypedProbeRow {
        id: 2,
        decimal_b: Some(2_000_000_002),
        double: Some(1.5),
        varchar: Some("AA"),
        boolean: Some(true),
        price: Some(3.0),
        qty: 2,
    },
    TypedProbeRow {
        id: 3,
        decimal_b: None,
        double: None,
        varchar: None,
        boolean: None,
        price: None,
        qty: 5,
    },
];

/// `c_date`/`c_ts` are checked only for NULL-ness; their rendering is covered elsewhere.
fn assert_typed_probe_prefix(cols: &[Vec<serde_json::Value>], i: usize) {
    let row = &TYPED_ROWS_1_TO_3[i];
    assert_eq!(parse_int(&cols[0][i]), row.id, "row {i}: id mismatch");

    let decimal_a = TYPED_DECIMAL_A_UNSCALED
        .iter()
        .find(|&&(row_id, _)| row_id == row.id)
        .unwrap_or_else(|| panic!("no TYPED_DECIMAL_A_UNSCALED entry for id={}", row.id))
        .1;
    match decimal_a {
        Some(unscaled) => assert!(
            (parse_numeric(&cols[1][i]) - unscaled as f64 / 100.0).abs() < 0.001,
            "row {i}: c_decimal_a mismatch, got {:?}",
            cols[1][i]
        ),
        None => assert!(
            cols[1][i].is_null(),
            "row {i}: c_decimal_a must be NULL, got {:?}",
            cols[1][i]
        ),
    }
    match row.decimal_b {
        Some(unscaled) => assert!(
            (parse_numeric(&cols[2][i]) - unscaled as f64 / 10_000.0).abs() < 0.001,
            "row {i}: c_decimal_b mismatch, got {:?}",
            cols[2][i]
        ),
        None => assert!(
            cols[2][i].is_null(),
            "row {i}: c_decimal_b must be NULL, got {:?}",
            cols[2][i]
        ),
    }
    match row.double {
        Some(d) => assert!(
            (parse_numeric(&cols[3][i]) - d).abs() < 0.001,
            "row {i}: c_double mismatch, got {:?}",
            cols[3][i]
        ),
        None => assert!(
            cols[3][i].is_null(),
            "row {i}: c_double must be NULL, got {:?}",
            cols[3][i]
        ),
    }
    match row.varchar {
        Some(s) => assert_eq!(
            cols[4][i].as_str(),
            Some(s),
            "row {i}: c_varchar mismatch, got {:?}",
            cols[4][i]
        ),
        None => assert!(
            cols[4][i].is_null(),
            "row {i}: c_varchar must be NULL, got {:?}",
            cols[4][i]
        ),
    }
    // Valid only because row 3 is NULL in every optional column of the seed, so
    // `decimal_a`'s null flag doubles as the oracle for `c_date`/`c_ts`.
    assert_eq!(
        cols[5][i].is_null(),
        decimal_a.is_none(),
        "row {i}: c_date null-ness mismatch"
    );
    assert_eq!(
        cols[6][i].is_null(),
        decimal_a.is_none(),
        "row {i}: c_ts null-ness mismatch"
    );
    match row.boolean {
        Some(b) => assert!(
            ExpectedValue::Bool(b).matches(&cols[7][i]),
            "row {i}: c_bool mismatch, got {:?}",
            cols[7][i]
        ),
        None => assert!(
            cols[7][i].is_null(),
            "row {i}: c_bool must be NULL, got {:?}",
            cols[7][i]
        ),
    }
    match row.price {
        Some(p) => assert!(
            (parse_numeric(&cols[8][i]) - p).abs() < 0.001,
            "row {i}: c_price mismatch, got {:?}",
            cols[8][i]
        ),
        None => assert!(
            cols[8][i].is_null(),
            "row {i}: c_price must be NULL, got {:?}",
            cols[8][i]
        ),
    }
}

#[test]
fn e2e_selectlist_between_at_matching_arity_projects_as_expr() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, c_decimal_a, c_decimal_b, c_double, c_varchar, c_date, \
         c_ts, c_bool, c_price, (c_qty BETWEEN 1 AND 3) FROM {} WHERE id <= 3 \
         ORDER BY id",
        typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 10, "expected 10 columns: {cols:?}");
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1..3): {cols:?}");

    for (i, row) in TYPED_ROWS_1_TO_3.iter().enumerate() {
        assert_typed_probe_prefix(&cols, i);

        let expected_between = (1..=3).contains(&row.qty);
        assert!(
            ExpectedValue::Bool(expected_between).matches(&cols[9][i]),
            "row {i} (qty={}): c_qty BETWEEN 1 AND 3 must be \
             {expected_between}, got {:?}",
            row.qty,
            cols[9][i]
        );
    }

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        has_expr_after_projection(&pushed_sql),
        "the coincidental-arity BETWEEN item must push a positional Expr \
         projection on the ordinary scan path, not widen to the full base \
         row, got:\n{pushed_sql}"
    );
}

#[test]
fn e2e_widened_projection_with_declined_order_by_routes_to_wrapper() {
    lakehouse_vs();
    let mut conn = exa_conn();

    {
        let sql = format!(
            "SELECT id, c_decimal_a, c_decimal_b, c_double, c_varchar, c_date, \
             c_ts, c_bool, c_price, LENGTH(c_double) FROM {} WHERE id <= 3 \
             ORDER BY id",
            typed_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(cols.len(), 10, "expected 10 columns: {cols:?}");
        assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1..3): {cols:?}");

        for (i, row) in TYPED_ROWS_1_TO_3.iter().enumerate() {
            assert_typed_probe_prefix(&cols, i);

            // LENGTH over DOUBLE depends on Exasol's implementation-defined DOUBLE-to-VARCHAR
            // rendering, so the expectation comes from a native oracle.
            let oracle_sql = match row.double {
                Some(d) => format!("SELECT LENGTH(CAST({d} AS DOUBLE))"),
                None => "SELECT LENGTH(CAST(NULL AS DOUBLE))".to_string(),
            };
            let oracle_cols = conn.query_columns(&oracle_sql);
            let oracle_value = &oracle_cols[0][0];
            if oracle_value.is_null() {
                assert!(
                    cols[9][i].is_null(),
                    "row {i}: LENGTH(c_double) must be NULL to match the \
                     native oracle, got {:?}",
                    cols[9][i]
                );
            } else {
                assert_eq!(
                    parse_int(&cols[9][i]),
                    parse_int(oracle_value),
                    "row {i}: LENGTH(c_double) must match the native oracle"
                );
            }
        }

        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            pushed_sql.contains("LHS_T0"),
            "a widened select list at matching arity must route through the \
             qualified single-table wrapper (alias LHS_T0), got:\n{pushed_sql}"
        );
    }

    // #234 variant: a select-list arity (10) that differs from `EVENTS`'s column count (5).
    {
        let sql = format!(
            "SELECT id, score, name, event_date, event_ts, id, score, name, \
             event_date, LENGTH(score) FROM {} WHERE id <= 3 ORDER BY id",
            events_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(cols.len(), 10, "expected 10 columns: {cols:?}");
        assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1..3): {cols:?}");

        for (i, id_val) in cols[0].iter().enumerate() {
            let id = parse_int(id_val);
            assert_eq!(id, (i + 1) as i64, "row {i}: id mismatch");

            let score = 5.0 * id as f64;
            let oracle_cols =
                conn.query_columns(&format!("SELECT LENGTH(CAST({score} AS DOUBLE))"));
            let expected_len = parse_int(&oracle_cols[0][0]);
            assert_eq!(
                parse_int(&cols[9][i]),
                expected_len,
                "row {i}: LENGTH(score) must match the native oracle"
            );
        }

        let pushed_sql = explain_virtual_sql(&mut conn, &sql);
        assert!(
            pushed_sql.contains("LHS_T0"),
            "the (#234) arity-mismatch variant must also route through the \
             qualified single-table wrapper (alias LHS_T0), got:\n{pushed_sql}"
        );
    }
}

#[test]
fn e2e_selectlist_literal_projection_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = events_table();

    {
        let sql = format!("SELECT 1 FROM {table}");
        let cols = conn.query_columns(&sql);
        assert_eq!(cols.len(), 1, "expected 1 column (bare literal): {cols:?}");
        assert_eq!(cols[0].len(), 20, "expected 20 rows: {cols:?}");
        for (i, v) in cols[0].iter().enumerate() {
            let n = parse_numeric(v);
            assert!(
                (n - 1.0).abs() < f64::EPSILON,
                "row {i}: literal column must be 1, got {n}"
            );
        }
    }

    {
        let sql = format!("SELECT 1, name FROM {table}");
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            2,
            "expected 2 columns (literal, name): {cols:?}"
        );
        assert_eq!(cols[0].len(), 20, "expected 20 rows: {cols:?}");
        assert_eq!(cols[1].len(), 20, "expected 20 rows: {cols:?}");
        for (i, v) in cols[0].iter().enumerate() {
            let n = parse_numeric(v);
            assert!(
                (n - 1.0).abs() < f64::EPSILON,
                "row {i}: literal column must be 1, got {n}"
            );
        }
        for (i, v) in cols[1].iter().enumerate() {
            let name = v
                .as_str()
                .unwrap_or_else(|| panic!("row {i}: name is not a string: {v:?}"));
            assert!(
                name.to_lowercase().starts_with("event"),
                "row {i}: name must start with \"event\", got {name}"
            );
        }
    }

    // Both literal positions must carry 1; a value-based dedup would collapse to arity 2.
    {
        let sql = format!("SELECT 1, name, 1 FROM {table}");
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            3,
            "expected 3 columns (literal, name, literal): {cols:?}"
        );
        assert_eq!(cols[0].len(), 20, "expected 20 rows: {cols:?}");
        assert_eq!(cols[2].len(), 20, "expected 20 rows: {cols:?}");
        for (i, (a, b)) in cols[0].iter().zip(cols[2].iter()).enumerate() {
            let a = parse_numeric(a);
            let b = parse_numeric(b);
            assert!(
                (a - 1.0).abs() < f64::EPSILON,
                "row {i}: column 0 (first duplicated literal) must be 1, got {a}"
            );
            assert!(
                (b - 1.0).abs() < f64::EPSILON,
                "row {i}: column 2 (second duplicated literal) must be 1, got {b}"
            );
        }
    }
}

#[test]
fn e2e_count_star_over_limited_subselect_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = events_table();

    let primary_sql = format!("SELECT COUNT(*) FROM (SELECT id FROM {table} LIMIT 5)");
    assert_eq!(
        conn.query_scalar_i64(&primary_sql),
        5,
        "COUNT(*) over a single-column LIMITed derived table must be 5: {primary_sql}"
    );

    let two_col_sql = format!("SELECT COUNT(*) FROM (SELECT id, name FROM {table} LIMIT 5)");
    assert_eq!(
        conn.query_scalar_i64(&two_col_sql),
        5,
        "COUNT(*) over a two-column LIMITed derived table must be 5: {two_col_sql}"
    );

    let where_limit_sql =
        format!("SELECT COUNT(*) FROM (SELECT id FROM {table} WHERE id <= 10 LIMIT 5)");
    assert_eq!(
        conn.query_scalar_i64(&where_limit_sql),
        5,
        "COUNT(*) over a WHERE+LIMITed derived table must be 5: {where_limit_sql}"
    );

    // The inner scan must push a positional literal projection, not the full base row.
    let pushed_sql = explain_virtual_sql(&mut conn, &primary_sql);
    assert!(
        has_expr_after_projection(&pushed_sql),
        "{primary_sql}'s inner derived-table scan must push a positional \
         literal projection (an '\"expr\"' key after 'projection'), \
         not the full-base-row fallback (#205), got:\n{pushed_sql}"
    );
}

#[test]
fn e2e_all_files_pruned_literal_projection_empty_shape() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!("SELECT 1, name, 1 FROM {} WHERE id > 1000", events_table());
    // `execute` panics if Exasol rejects the shape. `query_columns` cannot observe the
    // column count of a zero-row result, so the metadata is asserted instead.
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let num_columns = result_set["numColumns"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected numColumns in resultSet: {resp}"));
    assert_eq!(
        num_columns, 3,
        "expected 3 columns (literal, name, literal) even with all files pruned: {resp}"
    );
    let num_rows = result_set["numRows"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected numRows in resultSet: {resp}"));
    assert_eq!(
        num_rows, 0,
        "all-files-pruned predicate must return zero rows: {resp}"
    );
}

fn scalar_seconds(conn: &mut ExaConn, sql: &str) -> f64 {
    let cols = conn.query_columns(sql);
    parse_numeric(&cols[0][0])
}

fn pin_session_time_zone(conn: &mut ExaConn) {
    conn.execute("ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'");
}

/// Converts a fixed local wall-clock anchor to UTC, so `SESSIONTIMEZONE` is not
/// conflated with `DBTIMEZONE`.
fn session_utc_offset_seconds(conn: &mut ExaConn) -> f64 {
    scalar_seconds(
        conn,
        "SELECT SECONDS_BETWEEN(TIMESTAMP '2024-01-01 00:00:00', \
         CONVERT_TZ(TIMESTAMP '2024-01-01 00:00:00', SESSIONTIMEZONE, 'UTC'))",
    )
}

/// Scenario: Routed session-dependent projection returns the value Exasol computes natively
#[test]
fn e2e_clock_projection_matches_native_session_local_value() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = events_table();
    let db_timezone = conn.query_columns("SELECT DBTIMEZONE")[0][0]
        .as_str()
        .expect("DBTIMEZONE is text")
        .to_string();
    conn.execute(&format!("ALTER SESSION SET TIME_ZONE = '{db_timezone}'"));
    let offset_seconds = session_utc_offset_seconds(&mut conn);
    // A zero offset lets the assertion below pass whichever zone the adapter applies (#238), so
    // docker-db's DBTIMEZONE is no UTC zone. Nano runs with `sysTZ=UTC` (docker-compose.nano.yml).
    assert_eq!(
        offset_seconds == 0.0,
        exasol_flavor() == ExasolFlavor::Nano,
        "fixture precondition: the session's UTC offset is zero exactly on Nano, is {offset_seconds}s"
    );

    for expr in ["CURRENT_TIMESTAMP", "SYSTIMESTAMP"] {
        // CURRENT_TIMESTAMP is evaluated by Exasol in this statement; SYSTIMESTAMP comes from the
        // adapter's clock, read when it plans the statement.
        let deviation = scalar_seconds(
            &mut conn,
            &format!("SELECT SECONDS_BETWEEN({expr}, (SELECT {expr} FROM {table} WHERE id = 1))"),
        )
        .abs();
        let within = if expr == "CURRENT_TIMESTAMP" {
            deviation < 1.0
        } else {
            deviation <= 60.0
                && (offset_seconds == 0.0 || (deviation - offset_seconds.abs()).abs() > 60.0)
        };
        assert!(
            within,
            "{expr}'s deviation between the native value and the VS-projected value \
             ({deviation}s) must be within its tolerance — this is the assertion that \
             fails if the adapter ever ships the UTC instant instead of Exasol's own \
             session-local value ({offset_seconds}s off under this session's zone, #238)"
        );
        let pruned = format!("SELECT {expr} FROM {table} WHERE id = 999999");
        assert_eq!(conn.query_row_count(&pruned), 0, "{pruned}");
    }

    let routed = explain_virtual_pushdown_sql(
        &mut conn,
        &format!("SELECT CURRENT_TIMESTAMP FROM {table} WHERE id = 1"),
    );
    assert!(
        routed.contains(r#"AS "LHS_T0""#) && !routed.contains("_LH_PROJ_0"),
        "CURRENT_TIMESTAMP must route to the qualified wrapper: {routed}"
    );
    let emitted = explain_virtual_pushdown_sql(
        &mut conn,
        &format!("SELECT SYSTIMESTAMP FROM {table} WHERE id = 1"),
    );
    assert!(
        emitted.contains("_LH_PROJ_0") && !emitted.to_uppercase().contains("SYSTIMESTAMP"),
        "the folded SYSTIMESTAMP must be a column the scan emits: {emitted}"
    );
}

#[test]
fn e2e_projected_tstz_literal_matches_native_and_pruned_scan_succeeds() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = events_table();
    pin_session_time_zone(&mut conn);

    let projection = "CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)";

    let native = conn
        .query_columns(&format!("SELECT {projection}"))
        .into_iter()
        .next()
        .and_then(|col| col.into_iter().next())
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| panic!("expected a string TSTZ value from the native query"));
    let projected_sql = format!("SELECT {projection} FROM {table} WHERE id = 1");
    let projected = conn
        .query_columns(&projected_sql)
        .into_iter()
        .next()
        .and_then(|col| col.into_iter().next())
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| panic!("expected a string TSTZ value from the VS query"));
    assert_eq!(
        projected, native,
        "the projected TSTZ literal must equal Exasol's own native value for the \
         same expression in the same session"
    );

    // Rendered in the Exasol dialect over a qualified `LHS_T0` scan, not a positional
    // `_LH_PROJ_0` EMITS column.
    let pushed = explain_virtual_sql(&mut conn, &projected_sql);
    assert!(
        pushed.contains(r#"AS "LHS_T0""#),
        "expected the qualified single-table wrapper in the pushed SQL: {pushed}"
    );
    assert!(
        !pushed.contains("_LH_PROJ_0"),
        "must not be a narrowed positional EMITS projection: {pushed}"
    );

    // The defect class is a column-count mismatch, so numColumns is asserted too.
    let resp = conn.execute(&format!("SELECT {projection} FROM {table} WHERE id > 1000"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let num_columns = result_set["numColumns"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected numColumns in resultSet: {resp}"));
    assert_eq!(
        num_columns, 1,
        "expected 1 column (the projected TSTZ literal) even with all files pruned: {resp}"
    );
    let num_rows = result_set["numRows"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected numRows in resultSet: {resp}"));
    assert_eq!(
        num_rows, 0,
        "an all-pruning predicate over the projected TSTZ literal must succeed \
         with zero rows, never fail with 04000: {resp}"
    );
}

#[test]
fn e2e_having_clause_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, 4), COUNT(*) FROM {} GROUP BY MOD(id, 4) HAVING COUNT(*) > 3 ORDER BY MOD(id, 4)",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (key, count): {cols:?}");

    // Per-shard counts are at most 3, so a per-shard HAVING would return 0 groups.
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

    let total: i64 = cols[1].iter().map(parse_int).sum();
    assert_eq!(
        total, 20,
        "total COUNT(*) across HAVING-filtered groups must be 20, got {total}"
    );
}

#[test]
fn e2e_stddev_variance_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT STDDEV(score), VARIANCE(score), STDDEV_POP(score), VAR_POP(score) FROM {}",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 4, "expected 4 aggregate columns: {cols:?}");
    assert_eq!(
        cols[0].len(),
        1,
        "each aggregate must return 1 row: {cols:?}"
    );

    let expected_var_pop: f64 = 831.25;
    let expected_var_samp: f64 = 875.0;
    let expected_stddev_pop: f64 = expected_var_pop.sqrt();
    let expected_stddev_samp: f64 = expected_var_samp.sqrt();

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

#[test]
fn e2e_stddev_over_expression_falls_back_and_returns_correct_value() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!("SELECT STDDEV(score + id) FROM {}", events_table());

    // A statistical partial column here is the accepted-then-failing shape the decline removes.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed_sql.contains("PARTIAL_stat_"),
        "STDDEV over an expression argument must NOT push a statistical \
         partial/merge decomposition, got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row: {cols:?}");
    let actual = parse_numeric(&cols[0][0]);

    let row_cols = conn.query_columns(&format!("SELECT score, id FROM {}", events_table()));
    let values: Vec<f64> = row_cols[0]
        .iter()
        .zip(row_cols[1].iter())
        .map(|(score, id)| parse_numeric(score) + parse_numeric(id))
        .collect();
    let n = values.len() as f64;
    assert!(
        n > 1.0,
        "the seed must return more than one row: {values:?}"
    );
    let mean = values.iter().sum::<f64>() / n;
    let expected = (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0)).sqrt();

    let closed_form = 6.0 * 35.0f64.sqrt();
    assert!(
        (expected - closed_form).abs() / closed_form < 1e-9,
        "the reference computed from the returned rows must match the seed's closed \
         form {closed_form:.6}, got {expected:.6}"
    );

    let rel_err = (actual - expected).abs() / expected;
    assert!(
        rel_err < 1e-6,
        "STDDEV(score + id) must be ≈{expected:.6} (sample standard deviation over \
         the same rows), got {actual:.6} (rel_err={rel_err:.2e})"
    );
}

/// Panics below two values rather than returning a NaN that cannot distinguish a
/// missing row from a wrong statistic.
fn sample_stddev(values: &[f64]) -> f64 {
    assert!(
        values.len() > 1,
        "a sample standard deviation needs more than one value: {values:?}"
    );
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0)).sqrt()
}

#[test]
fn e2e_grouped_stddev_over_expression_falls_back_and_returns_correct_value() {
    const GROUP_MODULUS: i64 = 4;

    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, {GROUP_MODULUS}), STDDEV(score + id) FROM {} \
         GROUP BY MOD(id, {GROUP_MODULUS}) ORDER BY 1",
        events_table()
    );

    // A statistical partial column here is the accepted-then-failing shape the decline removes.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed_sql.contains("PARTIAL_stat_"),
        "grouped STDDEV over an expression argument must NOT push a statistical \
         partial/merge decomposition, got:\n{pushed_sql}"
    );

    let row_cols = conn.query_columns(&format!("SELECT id, score FROM {}", events_table()));
    let mut group_values: std::collections::BTreeMap<i64, Vec<f64>> =
        std::collections::BTreeMap::new();
    for (id, score) in row_cols[0].iter().zip(row_cols[1].iter()) {
        let id = parse_int(id);
        group_values
            .entry(id % GROUP_MODULUS)
            .or_default()
            .push(parse_numeric(score) + id as f64);
    }
    assert!(
        !group_values.is_empty(),
        "the seed must return rows through a plain projection"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        2,
        "expected 2 columns (group key, STDDEV): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        group_values.len(),
        "expected one row per distinct MOD(id, {GROUP_MODULUS}) group, got {} rows \
         for {} groups — a dropped group must not pass silently: {cols:?}",
        cols[0].len(),
        group_values.len()
    );

    let actual: std::collections::BTreeMap<i64, f64> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(key, value)| (parse_numeric(key) as i64, parse_numeric(value)))
        .collect();
    assert_eq!(
        actual.keys().copied().collect::<Vec<i64>>(),
        group_values.keys().copied().collect::<Vec<i64>>(),
        "the returned group keys must be exactly the projected rows' distinct \
         MOD(id, {GROUP_MODULUS}) values"
    );

    let closed_form = 12.0 * 10.0f64.sqrt();
    for (key, values) in &group_values {
        let reference = sample_stddev(values);
        assert!(
            (reference - closed_form).abs() / closed_form < 1e-9,
            "group {key}'s reference computed from the returned rows must match the \
             seed's closed form {closed_form:.6}, got {reference:.6}"
        );

        let got = actual[key];
        let rel_err = (got - reference).abs() / reference;
        assert!(
            rel_err < 1e-6,
            "group {key}: STDDEV(score + id) must be ≈{reference:.6} (sample standard \
             deviation over the same rows), got {got:.6} (rel_err={rel_err:.2e})"
        );
    }
}

/// `filter` is omitted when `None`. Callers use a WHERE clause with a single expression,
/// so field presence alone attributes the pushdown to that expression.
fn assert_filter_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let pushed_sql = explain_virtual_sql(conn, query_sql);
    assert!(
        pushed_sql.contains("\"filter\":\""),
        "EXPLAIN VIRTUAL output must contain a non-empty 'filter' field in the \
         scan spec (predicate pushdown occurred), not a raw row-scan fallback, \
         got:\n{pushed_sql}"
    );
}

#[test]
fn e2e_cast_in_filter() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} WHERE CAST(id AS VARCHAR(2000000)) = '15' ORDER BY id",
        events_table()
    );
    assert_filter_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");
    assert_eq!(
        cols[0].len(),
        1,
        "CAST(id AS VARCHAR(2000000)) = '15' must return exactly 1 row, got {}",
        cols[0].len()
    );
    assert_eq!(
        parse_int(&cols[0][0]),
        15,
        "the matched row must have id=15"
    );

    let score = parse_numeric(&cols[1][0]);
    assert!(
        (score - 75.0).abs() < 0.001,
        "id=15 must have score=75.0 (5.0*15), got {score}"
    );
}

#[test]
fn e2e_unary_minus_in_filter() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} WHERE -score < -50.0 ORDER BY id",
        events_table()
    );
    assert_filter_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, score): {cols:?}");

    let expected_count = 10i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "-score < -50.0 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 11 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }

    for v in &cols[1] {
        let s = parse_numeric(v);
        assert!(s > 50.0, "filter violated: score {s} must be > 50.0");
    }
}

#[test]
fn e2e_week_in_filter() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, event_date FROM {} WHERE WEEK(event_date) = 2 ORDER BY id",
        events_table()
    );
    assert_filter_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        2,
        "expected 2 columns (id, event_date): {cols:?}"
    );

    let expected_count = 7i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "WEEK(event_date) = 2 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 8 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

fn assert_select_pushed_down(conn: &mut ExaConn, query_sql: &str, fragment: &str) {
    let pushed = explain_virtual_sql(conn, query_sql);
    assert!(
        pushed.contains(fragment),
        "expected pushed SQL to contain {fragment:?}, got: {pushed}"
    );
}

#[test]
fn e2e_days_between_matches_exasol() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let t = events_table();

    let sql = format!("SELECT DAYS_BETWEEN(event_date, DATE '2024-01-10') FROM {t} WHERE id = 1");
    // "AS DATE" can also match Exasol's echoed request; "- CAST(" only appears in the
    // adapter's own rendering.
    assert_select_pushed_down(&mut conn, &sql, "- CAST(");

    let cols = conn.query_columns(&sql);
    let value = parse_int(&cols[0][0]);
    assert_eq!(
        value, -9,
        "DAYS_BETWEEN(2024-01-01, 2024-01-10) must be -9 (Exasol sign convention), got {value}"
    );
}

#[test]
fn e2e_time_between_matches_exasol() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let t = events_table();

    let anchor = "TIMESTAMP '2024-01-01 02:30:00'";

    let hours_sql = format!("SELECT HOURS_BETWEEN(event_ts, {anchor}) FROM {t} WHERE id = 6");
    // The expression is embedded in the single-quoted `LAKEHOUSE_SCAN('…')` argument, so
    // its quotes are doubled.
    assert_select_pushed_down(&mut conn, &hours_sql, "date_part(''epoch''");
    let hours = parse_numeric(&conn.query_columns(&hours_sql)[0][0]);
    assert!(
        (hours - 2.5).abs() < 1e-9,
        "HOURS_BETWEEN over a 2.5h gap must be 2.5, got {hours}"
    );

    let minutes_sql = format!("SELECT MINUTES_BETWEEN(event_ts, {anchor}) FROM {t} WHERE id = 6");
    assert_select_pushed_down(&mut conn, &minutes_sql, "date_part(''epoch''");
    let minutes = parse_numeric(&conn.query_columns(&minutes_sql)[0][0]);
    assert!(
        (minutes - 150.0).abs() < 1e-9,
        "MINUTES_BETWEEN over a 2.5h gap must be 150, got {minutes}"
    );

    let seconds_sql = format!("SELECT SECONDS_BETWEEN(event_ts, {anchor}) FROM {t} WHERE id = 6");
    assert_select_pushed_down(&mut conn, &seconds_sql, "date_part(''epoch''");
    let seconds = parse_numeric(&conn.query_columns(&seconds_sql)[0][0]);
    assert!(
        (seconds - 9000.0).abs() < 1e-9,
        "SECONDS_BETWEEN over a 2.5h gap must be 9000, got {seconds}"
    );
}

#[test]
fn e2e_selectlist_cast_extract_case_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, CAST(id AS VARCHAR(2000000)), EXTRACT(YEAR FROM event_date), \
         CASE WHEN id > 10 THEN 'high' ELSE 'low' END FROM {} WHERE id <= 3 ORDER BY id",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        4,
        "expected 4 columns (id, CAST(id), EXTRACT(YEAR), CASE): {cols:?}"
    );
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1..3): {cols:?}");

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (i, &id) in ids.iter().enumerate() {
        let cast_str = cols[1][i].as_str().unwrap_or_else(|| {
            panic!(
                "CAST(id AS VARCHAR) at row {i} is not a string: {:?}",
                cols[1][i]
            )
        });
        assert_eq!(
            cast_str,
            id.to_string(),
            "row {i}: CAST(id AS VARCHAR(2000000)) must be \"{id}\", got {cast_str}"
        );
    }

    for (i, v) in cols[2].iter().enumerate() {
        let year = parse_int(v);
        assert_eq!(
            year, 2024,
            "row {i}: EXTRACT(YEAR FROM event_date) must be 2024, got {year}"
        );
    }

    for (i, v) in cols[3].iter().enumerate() {
        let case_val = v
            .as_str()
            .unwrap_or_else(|| panic!("CASE result at row {i} is not a string: {v:?}"));
        assert_eq!(
            case_val, "low",
            "row {i}: CASE WHEN id > 10 THEN 'high' ELSE 'low' END must be 'low' for id<=3, got {case_val}"
        );
    }
}

#[test]
fn e2e_order_by_unprojected_column_bare_projection() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT score FROM {} WHERE id = 1 ORDER BY id",
        events_table()
    );

    // Scoped to the adapter's own `"projection":[...]`: EVENTS field names are lowercase,
    // so an uppercase whole-string check would pass by casing accident.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("\"projection\":[\"SCORE\",\"ID\"]"),
        "the scan spec's projection must carry the hidden sort key ID \
         appended after the visible SCORE column, got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM ("),
        "the row-scan fan-out must not widen to a full-base-row SELECT * \
         (the #225 bug: 04000 arity mismatch), got:\n{pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected exactly 1 column (score): {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row (id=1): {cols:?}");
    let score = parse_numeric(&cols[0][0]);
    assert!(
        (score - 5.0).abs() < 0.001,
        "id=1 must have score=5.0 (5.0*1), got {score}"
    );

    let sql_desc = format!(
        "SELECT name FROM {} WHERE id <= 5 ORDER BY id DESC",
        events_table()
    );
    let cols_desc = conn.query_columns(&sql_desc);
    assert_eq!(
        cols_desc.len(),
        1,
        "expected exactly 1 column (name): {cols_desc:?}"
    );
    assert_eq!(
        cols_desc[0].len(),
        5,
        "expected exactly 5 rows (id 1..5): {cols_desc:?}"
    );

    let expected_names_desc = ["event-05", "event-04", "event-03", "event-02", "event-01"];
    for (i, expected) in expected_names_desc.iter().enumerate() {
        let n = cols_desc[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("name at row {i} is not a string: {:?}", cols_desc[0][i]));
        assert_eq!(
            n, *expected,
            "row {i}: ORDER BY id DESC over ids 1..5 must yield name {expected}, got {n}"
        );
    }
}

#[test]
fn e2e_order_by_column_referenced_only_in_projected_expression() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id || '-' || name FROM {} WHERE id <= 3 ORDER BY id",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        1,
        "expected exactly 1 column (id || '-' || name): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        3,
        "expected exactly 3 rows (id 1..3): {cols:?}"
    );

    let expected = ["1-event-01", "2-event-02", "3-event-03"];
    for (i, want) in expected.iter().enumerate() {
        let v = cols[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("row {i} is not a string: {:?}", cols[0][i]));
        assert_eq!(
            v, *want,
            "row {i}: id || '-' || name must be {want}, got {v}"
        );
    }
}

#[test]
fn e2e_issue_189_shape_equivalent_local_verification() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT c_name FROM {} WHERE c_custkey <= 5 ORDER BY c_custkey",
        vs_dim_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        1,
        "expected exactly 1 column (c_name): {cols:?}"
    );
    assert_eq!(
        cols[0].len(),
        5,
        "expected exactly 5 rows (custkey 1..=5): {cols:?}"
    );

    let expected = [
        "customer-01",
        "customer-02",
        "customer-03",
        "customer-04",
        "customer-05",
    ];
    for (i, want) in expected.iter().enumerate() {
        let v = cols[0][i]
            .as_str()
            .unwrap_or_else(|| panic!("row {i} is not a string: {:?}", cols[0][i]));
        assert_eq!(v, *want, "row {i}: c_name must be {want}, got {v}");
    }
}

// String functions over non-string arguments: Exasol converts them to text implicitly,
// which DataFusion refuses, so the scan writes an integer's text itself.

#[test]
fn e2e_upper_varchar_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT UPPER(c_varchar) FROM {} WHERE id = 1",
        typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        1,
        "expected 1 column (UPPER(c_varchar)): {cols:?}"
    );
    assert_eq!(cols[0].len(), 1, "expected 1 row (id=1): {cols:?}");

    let upper = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("UPPER(c_varchar) not a string: {:?}", cols[0][0]));
    assert_eq!(
        upper, "AA",
        "UPPER(c_varchar) for id=1 (\"aa\") must be \"AA\", got {upper:?}"
    );
}

#[test]
fn e2e_upper_id_trims_to_plain_integer_string() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!("SELECT UPPER(id) FROM {} WHERE id = 4", typed_table());
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column (UPPER(id)): {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row (id=4): {cols:?}");

    let upper = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("UPPER(id) not a string: {:?}", cols[0][0]));
    assert_eq!(
        upper, "4",
        "UPPER(id) for id=4 must be \"4\" (scale-0 DECIMAL, no decimal point), got {upper:?}"
    );
}

// The scan writes a BOOLEAN as `TRUE`/`FALSE`, and Exasol computes a DOUBLE or TIMESTAMP
// string-function argument. Each result is compared with a native oracle over a bare literal,
// because DataFusion's own formatting of these types differs from Exasol's.

#[test]
fn e2e_upper_double_declines_to_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let vs_sql = format!("SELECT UPPER(c_double) FROM {} WHERE id = 1", typed_table());
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(
        vs_cols.len(),
        1,
        "expected 1 column (UPPER(c_double)): {vs_cols:?}"
    );
    assert_eq!(vs_cols[0].len(), 1, "expected 1 row (id=1): {vs_cols:?}");
    let vs_value = vs_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("UPPER(c_double) not a string: {:?}", vs_cols[0][0]));

    let oracle_cols = conn.query_columns("SELECT UPPER(CAST(0.5 AS DOUBLE))");
    let oracle_value = oracle_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("native oracle not a string: {:?}", oracle_cols[0][0]));

    assert_eq!(
        vs_value, oracle_value,
        "UPPER(c_double) over the VS must match the native Exasol oracle \
         SELECT UPPER(CAST(0.5 AS DOUBLE)) (declined pushdown falls back to \
         native evaluation), got vs={vs_value:?} oracle={oracle_value:?}"
    );
}

#[test]
fn e2e_upper_timestamp_declines_to_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let vs_sql = format!("SELECT UPPER(c_ts) FROM {} WHERE id = 1", typed_table());
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(
        vs_cols.len(),
        1,
        "expected 1 column (UPPER(c_ts)): {vs_cols:?}"
    );
    assert_eq!(vs_cols[0].len(), 1, "expected 1 row (id=1): {vs_cols:?}");
    let vs_value = vs_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("UPPER(c_ts) not a string: {:?}", vs_cols[0][0]));

    let declared_column_type = expected_timestamp_precision(&mut conn).declared_column_type;
    let oracle_sql = format!(
        "SELECT UPPER(CAST(TIMESTAMP '2024-01-01 00:00:00.100' AS {declared_column_type}))"
    );
    let oracle_cols = conn.query_columns(&oracle_sql);
    let oracle_value = oracle_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("native oracle not a string: {:?}", oracle_cols[0][0]));

    assert_eq!(
        vs_value, oracle_value,
        "UPPER(c_ts) over the VS must match the native Exasol oracle, got \
         vs={vs_value:?} oracle={oracle_value:?}"
    );
}

#[test]
fn e2e_upper_boolean_matches_native() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let vs_sql = format!("SELECT UPPER(c_bool) FROM {} WHERE id = 1", typed_table());
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(
        vs_cols.len(),
        1,
        "expected 1 column (UPPER(c_bool)): {vs_cols:?}"
    );
    assert_eq!(vs_cols[0].len(), 1, "expected 1 row (id=1): {vs_cols:?}");
    let vs_value = vs_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("UPPER(c_bool) not a string: {:?}", vs_cols[0][0]));

    let oracle_cols = conn.query_columns("SELECT UPPER(CAST(TRUE AS BOOLEAN))");
    let oracle_value = oracle_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("native oracle not a string: {:?}", oracle_cols[0][0]));

    assert_eq!(
        vs_value, oracle_value,
        "UPPER(c_bool) over the VS must match the native Exasol oracle, got \
         vs={vs_value:?} oracle={oracle_value:?}"
    );
}

#[test]
fn e2e_substr_left_pushdown() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUBSTR(name, 1, 5), LEFT(name, 5), SUBSTR(name, 7, 2) FROM {} WHERE id = 1",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(
        cols.len(),
        3,
        "expected 3 columns (SUBSTR, LEFT, SUBSTR): {cols:?}"
    );
    assert_eq!(cols[0].len(), 1, "expected 1 row (id=1): {cols:?}");

    let substr_prefix = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("SUBSTR(name, 1, 5) not a string: {:?}", cols[0][0]));
    let left_prefix = cols[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("LEFT(name, 5) not a string: {:?}", cols[1][0]));
    let substr_id = cols[2][0]
        .as_str()
        .unwrap_or_else(|| panic!("SUBSTR(name, 7, 2) not a string: {:?}", cols[2][0]));

    assert_eq!(
        substr_prefix, "event",
        "SUBSTR(name, 1, 5) for id=1 (\"event-01\") must be \"event\", got {substr_prefix:?}"
    );
    assert_eq!(
        left_prefix, "event",
        "LEFT(name, 5) for id=1 (\"event-01\") must be \"event\", got {left_prefix:?}"
    );
    assert_eq!(
        substr_id, "01",
        "SUBSTR(name, 7, 2) for id=1 (\"event-01\") must be the two-digit id \"01\", got {substr_id:?}"
    );

    let pushdown_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushdown_sql.contains("substr("),
        "pushdown SQL must contain a rendered substr( expression: {pushdown_sql}"
    );
}

// INSTR/LOCATE beyond 2 arguments always decline: the renderer drops extra arguments
// (#228), which would silently return a wrong position.

#[test]
fn e2e_instr_arity_decline_selectlist_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = parse_int(&conn.query_columns("SELECT INSTR('aa', 'a', 2)")[0][0]);
    assert_eq!(
        oracle, 2,
        "native oracle INSTR('aa', 'a', 2) must be 2, got {oracle}"
    );

    let vs_sql = format!(
        "SELECT INSTR(c_varchar, 'a', 2) FROM {} WHERE id = 1",
        typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(vs_cols.len(), 1, "expected 1 column (INSTR): {vs_cols:?}");
    assert_eq!(vs_cols[0].len(), 1, "expected 1 row (id=1): {vs_cols:?}");

    let vs_value = parse_int(&vs_cols[0][0]);
    assert_eq!(
        vs_value, 2,
        "INSTR(c_varchar, 'a', 2) for id=1 (\"aa\") must be 2 (native decline), \
         got {vs_value} (a strpos that drops the start position returns 1)"
    );
}

#[test]
fn e2e_instr_arity_decline_where_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = parse_int(&conn.query_columns("SELECT INSTR('bb', 'b', 3)")[0][0]);
    assert_eq!(
        oracle, 0,
        "native oracle INSTR('bb', 'b', 3) must be 0, got {oracle}"
    );

    let vs_sql = format!(
        "SELECT id FROM {} WHERE INSTR(c_varchar, 'b', 3) = 0",
        typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(vs_cols.len(), 1, "expected 1 column (id): {vs_cols:?}");

    let ids: Vec<i64> = vs_cols[0].iter().map(parse_int).collect();
    assert!(
        ids.contains(&4),
        "WHERE INSTR(c_varchar, 'b', 3) = 0 must include id=4 (\"bb\", native \
         decline gives 0), got ids={ids:?} (a strpos that drops the start \
         position returns 1 for 'bb', never matching id=4)"
    );
}

// ORDER BY an expression or aggregate outside the select list (#198). Each case asserts
// no leaked `HIDDEN_COL_n` and that the ordering applied: advertising the capability
// without a backing path returns raw file order with no error.
//
//   id       1  2     3  4  5  6  |  7  8  9  10    11  12
//   c_price  2  3  NULL  4  2  5  |  2  3  6   4  NULL   5
//   c_qty    3  2     5  1  3  2  |  6  4  1   2     3   4
//
// Exasol defaults to NULLS FIRST under DESC and NULLS LAST under ASC.

/// A `HIDDEN_COL_n` leak is visible only in the column names, not the arity.
fn query_named_columns(
    conn: &mut ExaConn,
    sql: &str,
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    let resp = conn.execute(sql);
    let result_set = resp["responseData"]["results"][0]["resultSet"].clone();
    let names: Vec<String> = result_set["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("expected result-set column metadata for:\n{sql}"))
        .iter()
        .map(|c| c["name"].as_str().unwrap_or_default().to_string())
        .collect();
    let cols = conn.fetch_result_columns(&result_set);
    (names, cols)
}

fn assert_no_hidden_columns(names: &[String], sql: &str) {
    assert!(
        !names.iter().any(|n| n.starts_with("HIDDEN_COL")),
        "result must not leak a synthetic HIDDEN_COL_n column (#198), got \
         columns {names:?} for:\n{sql}"
    );
}

/// `explain_virtual_sql`'s flattened blob is too coarse for asserting Exasol's wire
/// payload: `contains("limit")` would also match the adapter's own scan-spec keys.
fn explain_virtual_pushdown_request(conn: &mut ExaConn, sql: &str) -> serde_json::Value {
    echoed_pushdown_requests(conn, sql)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("EXPLAIN VIRTUAL carried no echoed pushdownRequest for:\n{sql}"))
}

fn int_set(values: &[serde_json::Value]) -> std::collections::HashSet<i64> {
    values.iter().map(parse_int).collect()
}

#[test]
fn e2e_order_by_expression_not_selected_leaks_no_hidden_column() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, c_price FROM {} WHERE id<=5 ORDER BY ABS(c_price) DESC",
        typed_table()
    );

    // The sort expression references only the already-projected C_PRICE, so the projection
    // stays at the two visible columns.
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains("\"projection\":[\"ID\",\"C_PRICE\"]"),
        "ABS(c_price) references only the already-visible C_PRICE, so no extra \
         hidden scan column may be appended, got:\n{pushed}"
    );

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    assert_eq!(
        names,
        vec!["ID".to_string(), "C_PRICE".to_string()],
        "expected exactly the two visible select-list columns"
    );
    assert_eq!(cols[0].len(), 5, "expected 5 rows (id 1..5): {cols:?}");

    // ids 1..5 prices 2, 3, NULL, 4, 2 → DESC NULLS FIRST:
    // id 3 (NULL), id 4 (4), id 2 (3), then ids 1 and 5 tied at 2.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        &ids[..3],
        &[3, 4, 2],
        "ORDER BY ABS(c_price) DESC over ids 1..5 must yield NULL first then \
         4.0, 3.0, got ids={ids:?}"
    );
    assert_eq!(
        int_set(&cols[0][3..5]),
        std::collections::HashSet::from([1, 5]),
        "ids 1 and 5 both have c_price 2.0 (tied last), got ids={ids:?}"
    );

    let sql2 = format!(
        "SELECT id, c_price FROM {} ORDER BY ABS(c_price) DESC, c_qty+1 ASC",
        typed_table()
    );

    let pushed2 = explain_virtual_sql(&mut conn, &sql2);
    assert!(
        pushed2.contains("\"projection\":[\"ID\",\"C_PRICE\",\"C_QTY\"]"),
        "the second key's base column C_QTY must be appended ONCE after the \
         visible items, and C_PRICE must not be duplicated, got:\n{pushed2}"
    );

    let (names2, cols2) = query_named_columns(&mut conn, &sql2);
    assert_no_hidden_columns(&names2, &sql2);
    assert_eq!(
        names2,
        vec!["ID".to_string(), "C_PRICE".to_string()],
        "the hidden C_QTY scan column must not reach the visible result"
    );
    assert_eq!(cols2[0].len(), 12, "expected all 12 rows: {cols2:?}");

    // ABS(c_price) DESC NULLS FIRST, then c_qty+1 ASC:
    //   NULL: id 11 (qty 3), id 3 (qty 5)
    //   6.0:  id 9   | 5.0: id 6 (qty 2), id 12 (qty 4)
    //   4.0:  id 4 (qty 1), id 10 (qty 2)
    //   3.0:  id 2 (qty 2), id 8 (qty 4)
    //   2.0:  ids 1 and 5 (both qty 3, fully tied), then id 7 (qty 6)
    let ids2: Vec<i64> = cols2[0].iter().map(parse_int).collect();
    assert_eq!(
        &ids2[..9],
        &[11, 3, 9, 6, 12, 4, 10, 2, 8],
        "both sort keys must render: primary ABS(c_price) DESC NULLS FIRST, \
         secondary c_qty+1 ASC as the tie-break, got ids={ids2:?}"
    );
    assert_eq!(
        int_set(&cols2[0][9..11]),
        std::collections::HashSet::from([1, 5]),
        "ids 1 and 5 tie on BOTH keys (price 2.0, qty 3), got ids={ids2:?}"
    );
    assert_eq!(
        ids2[11], 7,
        "id 7 (price 2.0, qty 6) must sort last under the ASC tie-break, got \
         ids={ids2:?}"
    );
}

/// Scenario: A grouped ORDER BY over an aggregate absent from the select list keeps the partial/merge path
#[test]
fn e2e_grouped_order_by_aggregate_not_selected_top_n_groups_limit_applies() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} GROUP BY id ORDER BY SUM(c_qty) DESC LIMIT 4",
        typed_table()
    );
    assert_sorts_on_merged_partial(&mut conn, &sql);

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    assert_eq!(
        names,
        vec!["ID".to_string()],
        "expected exactly the one visible group-key column"
    );
    assert_eq!(
        cols[0].len(),
        4,
        "LIMIT 4 over 12 groups must return exactly 4 rows: {cols:?}"
    );

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        int_set(&cols[0]),
        std::collections::HashSet::from([7, 3, 8, 12]),
        "the 4 returned groups must be the true top 4 by SUM(c_qty) \
         (6, 5, 4, 4), got ids={ids:?} — a LIMIT applied before the ORDER BY, \
         or not rendered at all, returns a different set"
    );
    assert_eq!(
        &ids[..2],
        &[7, 3],
        "the two groups above the tie are unambiguously ordered: id 7 \
         (SUM=6) then id 3 (SUM=5), got ids={ids:?}"
    );

    // A HAVING over the sort-only aggregate merges over the same partial column.
    let having = format!(
        "SELECT id FROM {} GROUP BY id HAVING SUM(c_qty) > 3 ORDER BY SUM(c_qty) DESC",
        typed_table()
    );
    assert_sorts_on_merged_partial(&mut conn, &having);
    let (_, cols) = query_named_columns(&mut conn, &having);
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(&ids[..2], &[7, 3], "HAVING SUM(c_qty) > 3, got ids={ids:?}");
    assert_eq!(
        int_set(&cols[0][2..]),
        std::collections::HashSet::from([8, 12]),
        "the groups with SUM(c_qty) 4 must follow, got ids={ids:?}"
    );
}

/// Scenario: A grouped ORDER BY over an aggregate absent from the select list keeps the partial/merge path
#[test]
fn e2e_grouped_order_by_aggregate_not_selected_leaks_no_hidden_column() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT c_bool, COUNT(*) FROM {} GROUP BY c_bool ORDER BY SUM(c_price) DESC",
        typed_table()
    );
    assert_sorts_on_merged_partial(&mut conn, &sql);

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    assert_eq!(
        names.len(),
        2,
        "expected exactly the 2 visible select-list columns, got {names:?}"
    );
    assert_eq!(cols[0].len(), 3, "expected 3 c_bool groups: {cols:?}");

    let bools: Vec<Option<bool>> = cols[0].iter().map(|v| v.as_bool()).collect();
    assert_eq!(
        bools,
        vec![Some(true), Some(false), None],
        "groups must be ordered by the UNSELECTED SUM(c_price) DESC: \
         true=25, false=7, NULL-group=4"
    );
    let counts: Vec<i64> = cols[1].iter().map(parse_int).collect();
    assert_eq!(
        counts,
        vec![8, 2, 2],
        "COUNT(*) per c_bool group must be 8 / 2 / 2 in that order"
    );

    let sql2 = format!(
        "SELECT c_bool, SUM(c_price) FROM {} GROUP BY c_bool ORDER BY SUM(c_price) DESC",
        typed_table()
    );

    assert_sorts_on_merged_partial(&mut conn, &sql2);

    let (names2, cols2) = query_named_columns(&mut conn, &sql2);
    assert_no_hidden_columns(&names2, &sql2);
    assert_eq!(
        names2.len(),
        2,
        "expected exactly the 2 visible select-list columns, got {names2:?}"
    );
    let bools2: Vec<Option<bool>> = cols2[0].iter().map(|v| v.as_bool()).collect();
    assert_eq!(
        bools2,
        vec![Some(true), Some(false), None],
        "partial/merge path must produce the same DESC group order"
    );
    let sums: Vec<f64> = cols2[1].iter().map(parse_numeric).collect();
    for (got, want) in sums.iter().zip([25.0, 7.0, 4.0]) {
        assert!(
            (got - want).abs() < 1e-9,
            "SUM(c_price) per group must be 25 / 7 / 4, got {sums:?}"
        );
    }
}

/// The scan aggregates per shard, and the merge sorts on the merged partial column.
fn assert_sorts_on_merged_partial(conn: &mut ExaConn, sql: &str) {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        pushed.contains("\"group_keys\":["),
        "the scan spec must carry group_keys, not a raw-row scan, got:\n{pushed}"
    );
    assert!(
        pushed.contains("ORDER BY CAST(SUM(\"PARTIAL_sum_"),
        "the merge ORDER BY must sort on the merged partial column, got:\n{pushed}"
    );
}

/// Scenario: A grouped ORDER BY the merge cannot express routes to the qualified single-table wrapper instead of erroring
#[test]
fn e2e_grouped_order_by_distinct_aggregate_not_selected_routes_to_wrapper() {
    lakehouse_vs();
    let mut conn = exa_conn();

    // Distinct c_qty per c_bool group: true 5, false 2, NULL 2; the c_bool tie-break puts
    // false before NULL, so the LIMIT cuts the NULL group.
    let sql = format!(
        "SELECT c_bool FROM {} GROUP BY c_bool \
         ORDER BY COUNT(DISTINCT c_qty) DESC, c_bool LIMIT 2",
        typed_table()
    );

    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains("COUNT(DISTINCT \"LHS_T0\"") && !pushed.contains("\"group_keys\""),
        "a DISTINCT sort aggregate must route to the qualified single-table wrapper, \
         got:\n{pushed}"
    );

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    let bools: Vec<Option<bool>> = cols[0].iter().map(|v| v.as_bool()).collect();
    assert_eq!(
        bools,
        vec![Some(true), Some(false)],
        "the top 2 groups by COUNT(DISTINCT c_qty) must be true (5) then false (2)"
    );
}

#[test]
fn e2e_order_by_expression_also_selected_control() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, c_price, ABS(c_price) AS a FROM {} ORDER BY ABS(c_price)",
        typed_table()
    );

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    assert_eq!(
        names,
        vec!["ID".to_string(), "C_PRICE".to_string(), "A".to_string()],
        "the genuinely selected sort expression must survive as column A"
    );
    assert_eq!(cols[0].len(), 12, "expected all 12 rows: {cols:?}");

    // ASC NULLS LAST: 2,2,2,3,3,4,4,5,5,6 then the two NULLs (ids 3, 11).
    let a: Vec<Option<f64>> = cols[2]
        .iter()
        .map(|v| {
            if v.is_null() {
                None
            } else {
                Some(parse_numeric(v))
            }
        })
        .collect();
    assert_eq!(
        a,
        vec![
            Some(2.0),
            Some(2.0),
            Some(2.0),
            Some(3.0),
            Some(3.0),
            Some(4.0),
            Some(4.0),
            Some(5.0),
            Some(5.0),
            Some(6.0),
            None,
            None,
        ],
        "ABS(c_price) ASC must be non-decreasing with the two NULLs last"
    );
    assert_eq!(
        int_set(&cols[0][10..12]),
        std::collections::HashSet::from([3, 11]),
        "the trailing NULL rows must be ids 3 and 11"
    );
}

#[test]
fn e2e_multi_count_distinct_order_by_expression_renders_on_wrapper() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT c_bool), COUNT(DISTINCT id) FROM {} \
         ORDER BY COUNT(DISTINCT id) DESC",
        typed_table()
    );

    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains("COUNT(DISTINCT \"LHS_T0\""),
        "multi-COUNT(DISTINCT) must route to the qualified single-table \
         wrapper, got:\n{pushed}"
    );

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    assert_eq!(
        names.len(),
        2,
        "expected exactly the 2 visible select-list columns, got {names:?}"
    );
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row: {cols:?}");
    assert_eq!(
        parse_int(&cols[0][0]),
        2,
        "COUNT(DISTINCT c_bool) must be 2 (true/false, NULLs excluded)"
    );
    assert_eq!(parse_int(&cols[1][0]), 12, "COUNT(DISTINCT id) must be 12");
}

#[test]
fn e2e_order_by_aggregate_with_limit_zero_returns_no_rows() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*) FROM {} ORDER BY COUNT(*) DESC LIMIT 0",
        typed_table()
    );

    // Exasol pushes no `orderBy`/`limit` for a single-group aggregate; these keys do appear
    // on the wire when pushed (verified on an ORDER BY … LIMIT row scan).
    let request = explain_virtual_pushdown_request(&mut conn, &sql);
    assert!(
        request.get("orderBy").is_none(),
        "measured (decision-log [10]): Exasol pushes NO orderBy for a \
         single-group aggregate; if this now fails, the shape changed and the \
         zero-row assertion below needs re-deriving:\n{request:#}"
    );
    assert!(
        request.get("limit").is_none(),
        "measured (decision-log [10]): Exasol pushes NO limit for a \
         single-group aggregate, so no LIMIT is rendered on the merge \
         SELECT:\n{request:#}"
    );

    let (names, cols) = query_named_columns(&mut conn, &sql);
    assert_no_hidden_columns(&names, &sql);
    assert_eq!(
        names.len(),
        1,
        "expected exactly 1 result column (COUNT(*)), got {names:?}"
    );
    assert!(
        cols.iter().all(|c| c.is_empty()),
        "LIMIT 0 must return ZERO rows, not one COUNT = 0 row: {cols:?}"
    );
    assert_eq!(
        conn.query_row_count(&sql),
        0,
        "LIMIT 0 over a one-row aggregate must return zero rows"
    );
}

// Dialect-aware rendering in Exasol-parsed wrapper SQL (#209): Exasol rejects a DataFusion
// function name with 42000. The TPC-H-shaped repros are
// retargeted onto `events`/`typed_distinct_probe`, each compared with an in-session
// native oracle.

fn parse_int_opt(v: &serde_json::Value) -> Option<i64> {
    if v.is_null() {
        None
    } else {
        Some(parse_int(v))
    }
}

fn grouped_by_label<T>(
    keys: &[serde_json::Value],
    values: &[serde_json::Value],
    parse_value: impl Fn(&serde_json::Value) -> T,
) -> std::collections::BTreeMap<String, T> {
    keys.iter()
        .zip(values.iter())
        .map(|(k, v)| {
            let label = match k {
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => "NULL".to_string(),
                other => other.to_string(),
            };
            (label, parse_value(v))
        })
        .collect()
}

#[test]
fn e2e_count_distinct_sign_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT SIGN(v - 3)) FROM (\
         SELECT 2.0 AS v UNION ALL SELECT 3.0 UNION ALL SELECT CAST(NULL AS DOUBLE) \
         UNION ALL SELECT 4.0 UNION ALL SELECT 2.0 UNION ALL SELECT 5.0 \
         UNION ALL SELECT 2.0 UNION ALL SELECT 3.0 UNION ALL SELECT 6.0 \
         UNION ALL SELECT 4.0 UNION ALL SELECT CAST(NULL AS DOUBLE) UNION ALL SELECT 5.0\
         )",
    );
    assert_eq!(
        oracle, 3,
        "native oracle COUNT(DISTINCT SIGN(v - 3)) must be 3 ({{-1,0,1}}), got {oracle}"
    );

    let vs_sql = format!(
        "SELECT COUNT(DISTINCT SIGN(c_price - 3)) FROM {}",
        typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 3,
        "COUNT(DISTINCT SIGN(c_price - 3)) over the VS must be 3 (matching the \
         native oracle), got {vs_value}"
    );
}

#[test]
fn e2e_count_distinct_date_field_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle_year = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT y) FROM (SELECT YEAR(DATE '2024-01-01' + (LEVEL-1)) AS y \
         FROM DUAL CONNECT BY LEVEL <= 20)",
    );
    assert_eq!(
        oracle_year, 1,
        "native oracle COUNT(DISTINCT YEAR(...)) over the 20-day span must be 1 \
         (every date falls in 2024), got {oracle_year}"
    );
    let oracle_week = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT w) FROM (SELECT WEEK(DATE '2024-01-01' + (LEVEL-1)) AS w \
         FROM DUAL CONNECT BY LEVEL <= 20)",
    );
    assert_eq!(
        oracle_week, 3,
        "native oracle COUNT(DISTINCT WEEK(...)) over the 20-day span must be 3 \
         (Jan 1-7 / Jan 8-14 / Jan 15-20), got {oracle_week}"
    );

    let year_sql = format!(
        "SELECT COUNT(DISTINCT YEAR(event_date)) FROM {}",
        events_table()
    );
    let vs_year = conn.query_scalar_i64(&year_sql);
    assert_eq!(
        vs_year, oracle_year,
        "COUNT(DISTINCT YEAR(event_date)) over the VS must match the native \
         oracle, got vs={vs_year} oracle={oracle_year}"
    );

    let week_sql = format!(
        "SELECT COUNT(DISTINCT WEEK(event_date)) FROM {}",
        events_table()
    );
    let vs_week = conn.query_scalar_i64(&week_sql);
    assert_eq!(
        vs_week, oracle_week,
        "COUNT(DISTINCT WEEK(event_date)) over the VS must match the native \
         oracle, got vs={vs_week} oracle={oracle_week}"
    );
}

#[test]
fn e2e_count_distinct_hours_between_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT h) FROM (SELECT HOURS_BETWEEN(\
         TIMESTAMP '2024-01-01 00:00:00' + ((LEVEL-1) * INTERVAL '1' HOUR), \
         TIMESTAMP '2024-01-01 00:00:00') AS h FROM DUAL CONNECT BY LEVEL <= 20)",
    );
    assert_eq!(
        oracle, 20,
        "native oracle COUNT(DISTINCT HOURS_BETWEEN(...)) over the 20-hour \
         span must be 20 (all offsets distinct), got {oracle}"
    );

    let vs_sql = format!(
        "SELECT COUNT(DISTINCT HOURS_BETWEEN(event_ts, TIMESTAMP '2024-01-01 00:00:00')) \
         FROM {}",
        events_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, oracle,
        "COUNT(DISTINCT HOURS_BETWEEN(event_ts, ...)) over the VS must match \
         the native oracle, got vs={vs_value} oracle={oracle}"
    );
}

#[test]
fn e2e_count_distinct_instr_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT INSTR(v, 'a')) FROM (\
         SELECT 'aa' AS v UNION ALL SELECT 'AA' UNION ALL SELECT CAST(NULL AS VARCHAR(10)) \
         UNION ALL SELECT 'bb' UNION ALL SELECT 'aa' UNION ALL SELECT 'cc' \
         UNION ALL SELECT 'Aa' UNION ALL SELECT 'dd' UNION ALL SELECT 'BB' \
         UNION ALL SELECT CAST(NULL AS VARCHAR(10)) UNION ALL SELECT 'ee' UNION ALL SELECT 'cc'\
         )",
    );
    assert_eq!(
        oracle, 3,
        "native oracle COUNT(DISTINCT INSTR(v, 'a')) must be 3 ({{0,1,2}}), got {oracle}"
    );

    let vs_sql = format!(
        "SELECT COUNT(DISTINCT INSTR(c_varchar, 'a')) FROM {}",
        typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 3,
        "COUNT(DISTINCT INSTR(c_varchar, 'a')) over the VS must be 3 (matching \
         the native oracle), got {vs_value}"
    );
}

#[test]
fn e2e_grouped_scalar_over_aggregate_sign_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle_sql = "\
        SELECT b, SIGN(SUM(p) - 10) FROM (\
        SELECT TRUE AS b, 2.0 AS p UNION ALL SELECT TRUE, 3.0 \
        UNION ALL SELECT CAST(NULL AS BOOLEAN), CAST(NULL AS DOUBLE) \
        UNION ALL SELECT FALSE, 4.0 UNION ALL SELECT TRUE, 2.0 \
        UNION ALL SELECT TRUE, 5.0 UNION ALL SELECT TRUE, 2.0 \
        UNION ALL SELECT FALSE, 3.0 UNION ALL SELECT TRUE, 6.0 \
        UNION ALL SELECT CAST(NULL AS BOOLEAN), 4.0 \
        UNION ALL SELECT TRUE, CAST(NULL AS DOUBLE) UNION ALL SELECT TRUE, 5.0\
        ) GROUP BY b";
    let oracle_cols = conn.query_columns(oracle_sql);
    let oracle_map = grouped_by_label(&oracle_cols[0], &oracle_cols[1], parse_int);
    assert_eq!(
        oracle_map,
        std::collections::BTreeMap::from([
            ("NULL".to_string(), -1i64),
            ("false".to_string(), -1),
            ("true".to_string(), 1),
        ]),
        "native oracle grouped SIGN(SUM(p) - 10) must be {{NULL: -1, false: \
         -1, true: 1}}, got {oracle_map:?}"
    );

    let vs_sql = format!(
        "SELECT c_bool, SIGN(SUM(c_price) - 10) FROM {} GROUP BY c_bool",
        typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_map = grouped_by_label(&vs_cols[0], &vs_cols[1], parse_int);
    assert_eq!(
        vs_map, oracle_map,
        "grouped SIGN(SUM(c_price) - 10) over the VS must match the native \
         oracle, got vs={vs_map:?} oracle={oracle_map:?}"
    );
}

#[test]
fn e2e_grouped_scalar_over_aggregate_year_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle_sql = "\
        SELECT b, YEAR(MIN(t)) FROM (\
        SELECT TRUE AS b, TIMESTAMP '2024-01-01 00:00:00.100' AS t \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.200' \
        UNION ALL SELECT CAST(NULL AS BOOLEAN), CAST(NULL AS TIMESTAMP) \
        UNION ALL SELECT FALSE, TIMESTAMP '2024-01-01 00:00:00.300' \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.100' \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.400' \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.100' \
        UNION ALL SELECT FALSE, TIMESTAMP '2024-01-01 00:00:00.500' \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.200' \
        UNION ALL SELECT CAST(NULL AS BOOLEAN), CAST(NULL AS TIMESTAMP) \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.600' \
        UNION ALL SELECT TRUE, TIMESTAMP '2024-01-01 00:00:00.300'\
        ) GROUP BY b";
    let oracle_cols = conn.query_columns(oracle_sql);
    let oracle_map = grouped_by_label(&oracle_cols[0], &oracle_cols[1], parse_int_opt);
    assert_eq!(
        oracle_map,
        std::collections::BTreeMap::from([
            ("NULL".to_string(), None),
            ("false".to_string(), Some(2024i64)),
            ("true".to_string(), Some(2024)),
        ]),
        "native oracle grouped YEAR(MIN(t)) must be {{NULL: NULL, false: 2024, \
         true: 2024}}, got {oracle_map:?}"
    );

    let vs_sql = format!(
        "SELECT c_bool, YEAR(MIN(c_ts)) FROM {} GROUP BY c_bool",
        typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_map = grouped_by_label(&vs_cols[0], &vs_cols[1], parse_int_opt);
    assert_eq!(
        vs_map, oracle_map,
        "grouped YEAR(MIN(c_ts)) over the VS must match the native oracle, \
         got vs={vs_map:?} oracle={oracle_map:?}"
    );
}

#[test]
fn e2e_count_distinct_regexp_like_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT (v REGEXP_LIKE 'a.*')) FROM (\
         SELECT 'aa' AS v UNION ALL SELECT 'AA' UNION ALL SELECT CAST(NULL AS VARCHAR(10)) \
         UNION ALL SELECT 'bb' UNION ALL SELECT 'aa' UNION ALL SELECT 'cc' \
         UNION ALL SELECT 'Aa' UNION ALL SELECT 'dd' UNION ALL SELECT 'BB' \
         UNION ALL SELECT CAST(NULL AS VARCHAR(10)) UNION ALL SELECT 'ee' UNION ALL SELECT 'cc'\
         )",
    );
    assert_eq!(
        oracle, 2,
        "native oracle COUNT(DISTINCT (v REGEXP_LIKE 'a.*')) must be 2 (TRUE \
         and FALSE both occur), got {oracle}"
    );

    let vs_sql = format!(
        "SELECT COUNT(DISTINCT (c_varchar REGEXP_LIKE 'a.*')) FROM {}",
        typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 2,
        "COUNT(DISTINCT (c_varchar REGEXP_LIKE 'a.*')) over the VS must be 2 \
         (matching the native oracle), got {vs_value}"
    );
}

#[test]
fn e2e_count_distinct_timestamp_literal_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle = conn.query_scalar_i64(
        "SELECT COUNT(DISTINCT (CASE WHEN t > TIMESTAMP '2020-01-01 00:00:00' \
         THEN 1 ELSE 0 END)) FROM (\
         SELECT TIMESTAMP '2024-01-01 00:00:00.100' AS t \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.200' \
         UNION ALL SELECT CAST(NULL AS TIMESTAMP) \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.300' \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.100' \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.400' \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.100' \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.500' \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.200' \
         UNION ALL SELECT CAST(NULL AS TIMESTAMP) \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.600' \
         UNION ALL SELECT TIMESTAMP '2024-01-01 00:00:00.300'\
         )",
    );
    assert_eq!(
        oracle, 2,
        "native oracle COUNT(DISTINCT CASE ...) must be 2 ({{0,1}}), got {oracle}"
    );

    let vs_sql = format!(
        "SELECT COUNT(DISTINCT (CASE WHEN c_ts > TIMESTAMP '2020-01-01 00:00:00' \
         THEN 1 ELSE 0 END)) FROM {}",
        typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 2,
        "COUNT(DISTINCT CASE WHEN c_ts > TIMESTAMP '2020-01-01 00:00:00' ...) \
         over the VS must be 2 (matching the native oracle), got {vs_value}"
    );
}

// Declined-filter self-apply (#279): a WHERE predicate the adapter cannot push into the
// scan must be applied in its own Exasol-dialect SQL, since Exasol never re-applies it.

/// Scenario: A declined single-table WHERE filter is applied in the adapter's own outer WHERE
#[test]
fn e2e_declined_filter_second_arity_returns_filtered_rows() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!("SELECT ID FROM {} WHERE SECOND(C_TS, 3) > 1", typed_table());
    let row_count = conn.query_row_count(&sql);
    assert_eq!(
        row_count, 0,
        "WHERE SECOND(c_ts, 3) > 1 must return 0 rows (declined filter \
         self-applied by the adapter), not all 12 seeded rows"
    );

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("LHS_T0"),
        "a declined single-table WHERE filter must route through the qualified \
         wrapper (alias LHS_T0), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("\"filter\":\""),
        "a declined WHERE filter must NOT appear as a scan-spec \"filter\" — \
         it is applied only in the wrapper's own WHERE, got:\n{pushed_sql}"
    );
}

#[test]
fn e2e_declined_filter_like_on_decimal_returns_filtered_rows() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID",
        typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column (ID): {cols:?}");
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        vec![1, 5, 7],
        "WHERE c_decimal_a LIKE '1%' must return exactly ids 1, 5, 7 \
         (unscaled value 1050 -> \"10.50\"), got {ids:?}"
    );
}

#[test]
fn e2e_declined_filter_instr_three_arg_returns_filtered_rows() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE INSTR(C_VARCHAR, 'a', 2) = 0 ORDER BY ID",
        typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column (ID): {cols:?}");
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        vec![2, 4, 6, 8, 9, 11, 12],
        "WHERE INSTR(c_varchar, 'a', 2) = 0 must return exactly ids \
         2, 4, 6, 8, 9, 11, 12, got {ids:?}"
    );
}

#[test]
fn e2e_declined_filter_under_aggregate_filters_before_aggregating() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE C_DECIMAL_A LIKE '1%'",
        typed_table()
    ));
    assert_eq!(
        count, 3,
        "COUNT(*) under a declined WHERE filter must be 3 (ids 1, 5, 7), \
         not 12 — the filter must be applied before aggregating"
    );
}

#[test]
fn e2e_declined_filter_under_order_by_limit_filters_before_truncating() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT ID FROM {} WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID LIMIT 2",
        typed_table()
    ));
    assert_eq!(cols.len(), 1, "expected 1 column (ID): {cols:?}");
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        vec![1, 5],
        "ORDER BY id LIMIT 2 under a declined WHERE filter must return \
         [1, 5] (the first two of the FILTERED set 1, 5, 7), not [1, 2] \
         (the first two of the unfiltered 12-row set), got {ids:?}"
    );
}

/// Scenario: The wrapper's fan-out emits only the columns the wrapper reads
#[test]
fn e2e_declined_filter_select_star_returns_full_row_shape() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!("SELECT * FROM {} WHERE SECOND(C_TS, 3) > 1", typed_table());
    // `execute` panics on a rejected pushdown, so reaching here proves Exasol accepted it.
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let num_columns = result_set["numColumns"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected numColumns in resultSet: {resp}"));
    assert_eq!(
        num_columns, 10,
        "SELECT * with a declined filter must project the FULL 10-column \
         base row (id, c_decimal_a, c_decimal_b, c_double, c_varchar, \
         c_date, c_ts, c_bool, c_price, c_qty), got {num_columns}: {resp}"
    );
    let num_rows = result_set["numRows"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected numRows in resultSet: {resp}"));
    assert_eq!(
        num_rows, 0,
        "SELECT * WHERE SECOND(c_ts, 3) > 1 must return 0 rows: {resp}"
    );
}

/// How a case's result is compared with the native copy's.
#[derive(Clone, Copy, Debug)]
enum Oracle {
    Rows,
    /// For a select list whose values differ on every run.
    RowCount,
    /// Native Exasol rejects the query, and the virtual table must raise the same error class.
    Error,
}

/// A query over `{T}` whose WHERE holds a `LIKE` over a DECIMAL column, which the scan declines,
/// beside the conjunct `ID > 1`, which it renders.
struct DeclinedConjunctCase {
    query: &'static str,
    oracle: Oracle,
    /// Whether a clause other than the rendered conjunct names `ID`, so the fan-out emits it.
    emits_id: bool,
}

const DECLINED_CONJUNCT_CASES: [DeclinedConjunctCase; 9] = [
    DeclinedConjunctCase {
        query: "SELECT ID FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 ORDER BY ID",
        oracle: Oracle::Rows,
        emits_id: true,
    },
    DeclinedConjunctCase {
        query: "SELECT C_VARCHAR FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1",
        oracle: Oracle::Rows,
        emits_id: false,
    },
    DeclinedConjunctCase {
        query: "SELECT COUNT(*) FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1",
        oracle: Oracle::Rows,
        emits_id: false,
    },
    DeclinedConjunctCase {
        query: "SELECT C_BOOL, COUNT(*) FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 \
                GROUP BY C_BOOL",
        oracle: Oracle::Rows,
        emits_id: false,
    },
    DeclinedConjunctCase {
        query: "SELECT COUNT(DISTINCT ID) FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1",
        oracle: Oracle::Rows,
        emits_id: true,
    },
    DeclinedConjunctCase {
        query: "SELECT ID FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 ORDER BY ID LIMIT 1",
        oracle: Oracle::Rows,
        emits_id: true,
    },
    DeclinedConjunctCase {
        query: "SELECT RANDOM() FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1",
        oracle: Oracle::RowCount,
        emits_id: false,
    },
    DeclinedConjunctCase {
        query: "SELECT ID FROM {T} WHERE (C_DECIMAL_A LIKE '1%' OR ID = 2) AND ID > 1 ORDER BY ID",
        oracle: Oracle::Rows,
        emits_id: true,
    },
    DeclinedConjunctCase {
        query: "SELECT ID FROM {T} WHERE C_VARCHAR AND C_DECIMAL_A LIKE '1%' AND ID > 1",
        oracle: Oracle::Error,
        emits_id: true,
    },
];

/// The scan spec's filter SQL in `plan`, `None` when the scan applies no WHERE conjunct.
fn scan_spec_filter(plan: &str) -> Option<&str> {
    let (_, rest) = plan.split_once(SCAN_FILTER)?;
    rest.split_once(r#"",""#).map(|(filter, _)| filter)
}

/// The column list of the fan-out's `EMITS` clause in `plan`, `None` when the plan has none.
fn fan_out_emits(plan: &str) -> Option<&str> {
    plan.split_once("EMITS (")
        .and_then(|(_, rest)| rest.split_once(") FROM"))
        .map(|(emits, _)| emits)
}

/// What differs from a WHERE split between the `LHS_T0` wrapper and the scan, for `case`.
fn where_split_mismatch(case: &DeclinedConjunctCase, plan: &str) -> Option<String> {
    let wrapper_where = plan
        .split_once(r#"AS "LHS_T0" WHERE "#)
        .map(|(_, clause)| clause);
    if !wrapper_where.is_some_and(|clause| clause.contains("LIKE")) {
        return Some(format!(
            "`{}`: the LHS_T0 wrapper's WHERE must apply the LIKE: {plan}",
            case.query
        ));
    }
    if !scan_spec_filter(plan)
        .is_some_and(|filter| filter.contains("ID") && !filter.contains("LIKE"))
    {
        return Some(format!(
            "`{}`: the scan filter must name ID and hold no LIKE: {plan}",
            case.query
        ));
    }
    let Some(emits) = fan_out_emits(plan) else {
        return Some(format!(
            "`{}`: the plan has no fan-out EMITS clause: {plan}",
            case.query
        ));
    };
    let emits_id = emits.contains(r#""ID""#);
    (emits_id != case.emits_id).then(|| {
        format!(
            "`{}`: the fan-out must emit ID only when a clause beside the rendered conjunct \
             names it ({}): {plan}",
            case.query, case.emits_id
        )
    })
}

/// What differs for `case` over the virtual `table` from the native copy, or from the split.
fn declined_conjunct_mismatch(
    conn: &mut ExaConn,
    table: &str,
    case: &DeclinedConjunctCase,
) -> Option<String> {
    let vs_query = case.query.replace("{T}", table);
    let outcome = match case.oracle {
        Oracle::Rows => native_copy_mismatch(
            conn,
            table,
            NATIVE_COPY,
            case.query,
            PathCheck::Wrapper,
            Expect::Rows,
        ),
        Oracle::RowCount => {
            let actual = conn.query_row_count(&vs_query);
            let expected = conn.query_row_count(&case.query.replace("{T}", NATIVE_COPY));
            (actual != expected)
                .then(|| format!("`{}`: got {actual} rows, native {expected}", case.query))
        }
        Oracle::Error => native_copy_mismatch(
            conn,
            table,
            NATIVE_COPY,
            case.query,
            PathCheck::Wrapper,
            Expect::Error,
        ),
    };
    outcome.or_else(|| where_split_mismatch(case, &isolated_pushdown_statement(conn, &vs_query)))
}

/// Scenario: A single-table WHERE filter splits at AND between the scan and the wrapper
/// Scenario: Filter predicate is pushed into the scan spec
/// Scenario: A declined WHERE filter routes the single-table request to the qualified wrapper
/// Scenario: A declined WHERE filter under an aggregate request is applied ahead of the aggregate
/// Scenario: The wrapper's fan-out emits only the columns the wrapper reads
#[test]
fn e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let typed = typed_table();
    create_native_copy(&mut conn, &typed, TYPED_COLUMNS);
    assert_eq!(
        int_column(
            &conn.query_columns(&format!(
                "SELECT ID FROM {NATIVE_COPY} WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID"
            ))[0]
        ),
        vec![1, 5, 7],
        "fixture precondition: ID 1 must match the LIKE, so a scan that drops `ID > 1` returns \
         a row the native copy does not"
    );
    let failures: Vec<String> = DECLINED_CONJUNCT_CASES
        .iter()
        .filter_map(|case| declined_conjunct_mismatch(&mut conn, &typed, case))
        .collect();

    assert!(
        failures.is_empty(),
        "{} of {} declined-conjunct cases failed:\n{}",
        failures.len(),
        DECLINED_CONJUNCT_CASES.len(),
        failures.join("\n")
    );
}

/// Scenario: The wrapper's fan-out emits only the columns the wrapper reads
#[test]
fn e2e_a_wrapper_over_a_pushed_filter_emits_no_column_only_the_filter_reads() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let typed = typed_table();
    create_native_copy(&mut conn, &typed, TYPED_COLUMNS);
    // Two COUNT(DISTINCT)s need the wrapper, and the scan applies the whole WHERE.
    let query = "SELECT COUNT(DISTINCT ID), COUNT(DISTINCT C_VARCHAR) FROM {T} WHERE C_DOUBLE > 0";
    let kept = conn.query_row_count(&format!("SELECT ID FROM {NATIVE_COPY} WHERE C_DOUBLE > 0"));
    let all = conn.query_row_count(&format!("SELECT ID FROM {NATIVE_COPY}"));
    assert!(
        0 < kept && kept < all,
        "fixture precondition: the filter must keep a strict subset of the rows, got {kept} of \
         {all}"
    );

    if let Some(failure) = native_copy_mismatch(
        &mut conn,
        &typed,
        NATIVE_COPY,
        query,
        PathCheck::Wrapper,
        Expect::Rows,
    ) {
        panic!("{failure}");
    }
    let plan = isolated_pushdown_statement(&mut conn, &query.replace("{T}", &typed));
    assert!(
        scan_spec_filter(&plan).is_some_and(|filter| filter.contains("C_DOUBLE")),
        "the scan must apply the whole WHERE: {plan}"
    );
    assert!(
        fan_out_emits(&plan).is_some_and(|emits| !emits.contains("C_DOUBLE")),
        "the fan-out must not emit C_DOUBLE, which only the scan's filter reads: {plan}"
    );
}

/// Native Exasol raises this for a value that is no hash, and the virtual table must raise it too.
const INVALID_HASH_ERROR_PREFIX: &str = "data exception - Invalid hash format: ";

/// Scenario: CAST renders the mapped target type per dialect
#[test]
fn cast_to_a_non_emittable_target_matches_native() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = typed_table();

    // A cast of `C_VARCHAR` itself raises on this fixture, so each filter casts a value that
    // converts, or a difference of timestamps.
    let filters = [
        "CAST(CASE WHEN C_VARCHAR = 'aa' THEN 'POINT (1 2)' END AS GEOMETRY) IS NULL",
        "CAST(CASE WHEN C_VARCHAR = 'aa' THEN '00000000000000000000000000000000' END AS HASHTYPE) IS NULL",
        "CAST(C_TS - C_TS AS INTERVAL DAY(2) TO SECOND(3)) IS NOT NULL",
    ];
    assert_filters_match_native_copy(&mut conn, &table, "C_VARCHAR, C_TS", &filters);
    for filter in filters {
        assert_single_table_wrapper(&mut conn, &format!("SELECT ID FROM {table} WHERE {filter}"));
    }

    // The error names the first offending value and the wrapper's alias of the cast column,
    // and the first value depends on row order, so only the leading text is compared.
    let query = "SELECT ID, CAST(C_VARCHAR AS HASHTYPE) FROM {}";
    let virtual_error = error_text(
        &conn.try_execute(&query.replace("{}", &table)),
        &format!("{query} over the virtual table"),
    );
    create_native_copy(&mut conn, &table, "C_VARCHAR");
    let native_error = error_text(
        &conn.try_execute(&query.replace("{}", NATIVE_COPY)),
        &format!("{query} over the native copy"),
    );
    for (path, text) in [("virtual", virtual_error), ("native", native_error)] {
        assert!(
            text.starts_with(INVALID_HASH_ERROR_PREFIX),
            "the {path} error must start with {INVALID_HASH_ERROR_PREFIX:?}, got: {text}"
        );
    }
}

/// Scenario: A NULL filter keeps no row
#[test]
fn a_null_condition_keeps_no_row() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = typed_table();

    // Exasol folds each condition to a bare `literal_null` filter; `''` is NULL in Exasol.
    let queries = [
        "SELECT ID FROM {T} WHERE NULL LIKE '%b%' ORDER BY ID",
        "SELECT ID FROM {T} WHERE '' LIKE '%b%' ORDER BY ID",
        "SELECT ID FROM {T} WHERE NULL = 1 ORDER BY ID",
        "SELECT ID FROM {T} WHERE NULL BETWEEN 1 AND 2 ORDER BY ID",
        "SELECT ID FROM {T} WHERE NOT (NULL LIKE 'a') ORDER BY ID",
        "SELECT COUNT(*) FROM {T} WHERE NULL = 1",
        "SELECT C_VARCHAR, COUNT(*) FROM {T} WHERE NULL = 1 GROUP BY C_VARCHAR ORDER BY 1",
        "SELECT a.ID, b.ID FROM {T} a JOIN {T} b ON a.ID = b.ID WHERE NULL = 1 ORDER BY 1",
    ];
    assert_queries_match_native_copy(&mut conn, &table, TYPED_COLUMNS, &queries);
}

/// Scenario: CAST renders the mapped target type per dialect
/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn boolean_to_text_in_the_single_table_wrapper_matches_native() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = typed_table();

    // A date converted to text routes each query into the Exasol-dialect wrapper.
    let queries = [
        "SELECT ID, C_DATE || 'x', CAST(C_DECIMAL_A > 10 AS VARCHAR(10)) FROM {T} ORDER BY ID",
        "SELECT ID, C_DATE || 'x', (C_DECIMAL_A > 10) || 'y' FROM {T} ORDER BY ID",
    ];
    assert_queries_match_native_copy(&mut conn, &table, TYPED_COLUMNS, &queries);
    for query in queries {
        assert_single_table_wrapper(&mut conn, &query.replace("{T}", &table));
    }
}

/// Scenario: An INTERVAL, GEOMETRY, or HASHTYPE select-list item, or a TIMESTAMP WITH LOCAL TIME ZONE aggregate or GROUP BY key, is computed by Exasol over the sharded scan
/// Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR
/// Scenario: Empty-result shape matches the plan the non-empty path would commit to
#[test]
fn non_emittable_select_items_match_native() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = typed_table();

    let single_table = [
        "SELECT ID, C_TS - C_TS FROM {T} ORDER BY ID",
        "SELECT C_TS - C_TS, COUNT(*) FROM {T} GROUP BY C_TS - C_TS ORDER BY 1",
        "SELECT MIN(C_TS - C_TS) FROM {T}",
        "SELECT MIN(CAST(C_TS AS TIMESTAMP WITH LOCAL TIME ZONE)) FROM {T}",
        "SELECT CAST(C_TS AS TIMESTAMP WITH LOCAL TIME ZONE), COUNT(*) FROM {T} \
         GROUP BY CAST(C_TS AS TIMESTAMP WITH LOCAL TIME ZONE) ORDER BY 1",
        // The INTERVAL item routes both ASCII casts into the Exasol-dialect wrapper.
        "SELECT ID, C_TS - C_TS, CAST(C_VARCHAR AS VARCHAR(10) ASCII), \
         CAST(C_VARCHAR AS CHAR(5) ASCII) FROM {T} ORDER BY ID",
        "SELECT ID, CAST(CASE WHEN C_VARCHAR = 'aa' THEN 'POINT (1 2)' END AS GEOMETRY) \
         FROM {T} ORDER BY ID",
        "SELECT ID, CAST(CASE WHEN C_VARCHAR = 'aa' THEN '00000000000000000000000000000000' \
         END AS HASHTYPE) FROM {T} ORDER BY ID",
    ];
    // The pairs of `cc` rows hold different timestamps, so the interval is not always zero.
    let join = "SELECT a.ID, b.ID, a.C_TS - b.C_TS FROM {T} a JOIN {T} b ON a.C_VARCHAR = b.C_VARCHAR \
                ORDER BY a.ID, b.ID";
    let pruned = "SELECT C_TS - C_TS, COUNT(*) FROM {T} WHERE ID < 0 GROUP BY C_TS - C_TS";
    let queries: Vec<&str> = single_table.iter().copied().chain([join, pruned]).collect();
    assert_queries_match_native_copy(&mut conn, &table, TYPED_COLUMNS, &queries);

    for query in single_table {
        assert_single_table_wrapper(&mut conn, &query.replace("{T}", &table));
    }
    assert_broadcast_join_wrapper(&mut conn, &join.replace("{T}", &table));
    let pruned_sql = pruned.replace("{T}", &table);
    assert_eq!(
        conn.query_row_count(&pruned_sql),
        0,
        "{pruned_sql} must return no rows"
    );
    assert!(
        !explain_virtual_sql(&mut conn, &pruned_sql).contains(SCAN_SCRIPT_NAME),
        "{pruned_sql} prunes every file, so its pushdown must not name {SCAN_SCRIPT_NAME}"
    );
}

/// Scenario: A NULL select item in an Exasol-rendered wrapper keeps its declared type
#[test]
fn typed_null_select_item_keeps_its_declared_type() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let table = typed_table();

    let single_table = format!("SELECT ID, CAST(NULL AS GEOMETRY) FROM {table} ORDER BY ID");
    let join = format!(
        "SELECT a.ID, CAST(NULL AS GEOMETRY) FROM {table} a JOIN {table} b ON a.ID = b.ID \
         ORDER BY a.ID"
    );
    assert_single_table_wrapper(&mut conn, &single_table);
    assert_broadcast_join_wrapper(&mut conn, &join);

    for sql in [single_table, join] {
        let resp = conn.execute(&sql);
        let result_set = &resp["responseData"]["results"][0]["resultSet"];
        assert_eq!(
            result_set["columns"][1]["dataType"]["type"].as_str(),
            Some("GEOMETRY"),
            "the NULL item of {sql} must keep the GEOMETRY type Exasol declared for it: {resp}"
        );
        let columns = conn.fetch_result_columns(result_set);
        assert_eq!(
            columns[1].len(),
            TYPED_TABLE_TOTAL_ROWS,
            "{sql} must return one row per seeded row: {columns:?}"
        );
        assert!(
            columns[1].iter().all(serde_json::Value::is_null),
            "every row of the NULL item of {sql} must be NULL: {columns:?}"
        );
    }
}

/// Scenario: Every request of a supported Exasol version passes the strict type check
#[test]
fn strict_type_check_accepts_every_request_shape() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let table = typed_table();

    // `SELECT *` sends no `selectList`, a bare TIMESTAMP cast arrives on 8.x without a
    // precision, and a UTF8 VARCHAR arrives without a `characterSet`.
    assert_queries_match_native_copy(
        &mut conn,
        &table,
        TYPED_COLUMNS,
        &[
            "SELECT * FROM {T} ORDER BY ID",
            "SELECT ID, CAST(C_TS AS TIMESTAMP) FROM {T} ORDER BY ID",
            "SELECT ID, CAST(C_VARCHAR AS VARCHAR(2000000)) FROM {T} ORDER BY ID",
        ],
    );

    // Exasol sends an empty `selectList` for `RANDOM()`, and its values differ per run.
    let random = "SELECT RANDOM() FROM {T}";
    assert_eq!(
        conn.query_row_count(&random.replace("{T}", &table)),
        conn.query_row_count(&random.replace("{T}", NATIVE_COPY)),
        "{random} must return as many rows as native Exasol"
    );
}

// #192: CHAR-declared pushdown shapes.

/// Scenario: An equal-length CASE group key resolves to a CHAR group-key type
/// Scenario: The CHAR declaration query shapes execute end to end through the Virtual Schema
#[test]
fn char_declared_pushdown_shapes_match_native() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CASE WHEN score <= 50.0 THEN 'LOW' ELSE 'BIG' END g, COUNT(*) c \
         FROM {} GROUP BY 1 ORDER BY 1",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (g, c): {cols:?}");
    assert_eq!(
        cols[0].len(),
        2,
        "expected 2 groups (BIG, LOW), not a data type mismatch failure: {cols:?}"
    );
    let case_groups: Vec<(String, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(g, c)| (g.as_str().unwrap_or_default().to_string(), parse_int(c)))
        .collect();
    assert_eq!(
        case_groups,
        vec![("BIG".to_string(), 10), ("LOW".to_string(), 10)],
        "expected BIG/LOW to each cover 10 rows: {case_groups:?}"
    );

    // Assert the pushdown carries CHAR(3); a declined pushdown would pass the value checks.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains(r#""GK_0" CHAR(3) ASCII"#),
        "expected the scan to emit the equal-length CASE GROUP BY key as \
         \"GK_0\" CHAR(3) ASCII, got: {pushed_sql}"
    );

    let sql = format!(
        "SELECT CAST(name AS CHAR(20)) FROM {} WHERE id = 1",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row (id=1): {cols:?}");
    let padded = cols[0][0].as_str().unwrap_or_else(|| {
        panic!("CAST(name AS CHAR(20)) is not a string: {:?}", cols[0][0]);
    });
    assert_eq!(
        padded.len(),
        20,
        "CAST(name AS CHAR(20)) must be padded to exactly 20 characters, got {padded:?} (len {})",
        padded.len()
    );
    assert_eq!(
        padded,
        format!("{:<20}", "event-01"),
        "CAST(name AS CHAR(20)) must be \"event-01\" plus 12 trailing spaces, got {padded:?}"
    );

    // The scan does not blank-pad, so the wrapper applies the CAST, rendered as CHAR.
    let pushed_sql = isolated_pushdown_statement(&mut conn, &sql);
    assert!(
        PathCheck::Wrapper.holds(&sql, &pushed_sql) && !pushed_sql.contains(SCAN_ITEM),
        "Exasol must compute CAST(name AS CHAR(20)) in the wrapper, not the scan: {pushed_sql}"
    );
    assert!(
        pushed_sql.contains("CHAR(20)") && !pushed_sql.contains("VARCHAR(20)"),
        "the wrapper must cast to CHAR(20), not VARCHAR(20): {pushed_sql}"
    );

    let sql = format!(
        "SELECT 'X' g, COUNT(*) c FROM {} GROUP BY 1",
        events_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (g, c): {cols:?}");
    assert_eq!(
        cols[0].len(),
        1,
        "a bare literal GROUP BY key must fold every row into ONE group: {cols:?}"
    );
    assert_eq!(
        cols[0][0].as_str(),
        Some("X"),
        "expected the literal group key to be 'X': {cols:?}"
    );
    assert_eq!(
        parse_int(&cols[1][0]),
        20,
        "expected all 20 rows folded into the single literal group: {cols:?}"
    );

    // Assert the pushdown carries CHAR(1); a declined pushdown would pass the value checks.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("CHAR(1)") && !pushed_sql.contains("VARCHAR(1)"),
        "expected the pushed SQL for the bare literal GROUP BY key to declare \
         CHAR(1) (not VARCHAR(1)), got: {pushed_sql}"
    );

    let sql = format!("SELECT name, COUNT(*) c FROM {} GROUP BY 1", events_table());
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (name, c): {cols:?}");
    assert_eq!(
        cols[0].len(),
        20,
        "expected 20 distinct name groups (VARCHAR control unaffected): {cols:?}"
    );
    for c in &cols[1] {
        assert_eq!(
            parse_int(c),
            1,
            "each distinct `name` value must have exactly 1 row: {cols:?}"
        );
    }
}

#[test]
fn e2e_float_div_decimal_over_int_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle_cols = conn.query_columns("SELECT CAST(40.99 AS DECIMAL(9,2)) / 7");
    let oracle_value = parse_numeric(&oracle_cols[0][0]);
    let expected = 5.855714285714286;
    assert!(
        (oracle_value - expected).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * expected.abs(),
        "native oracle 40.99/7 must be ~5.855714285714286, got {oracle_value}"
    );

    let vs_sql = format!("SELECT C_DECIMAL_A/7 FROM {} WHERE ID=6", typed_table());
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_value = parse_numeric(&vs_cols[0][0]);
    assert!(
        (vs_value - oracle_value).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * oracle_value.abs(),
        "pushed-down C_DECIMAL_A/7 at id=6 must match the native oracle \
         {oracle_value} (decimal/int FLOAT_DIV must not truncate to scale 6), \
         got {vs_value}"
    );
}

#[test]
fn e2e_float_div_decimal_over_decimal_matches_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let oracle_cols = conn
        .query_columns("SELECT CAST(40.99 AS DECIMAL(9,2)) / CAST(400000.0004 AS DECIMAL(20,4))");
    let oracle_value = parse_numeric(&oracle_cols[0][0]);
    let expected = 0.000102474999897525;
    assert!(
        (oracle_value - expected).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * expected.abs(),
        "native oracle 40.99/400000.0004 must be ~0.000102474999897525, got {oracle_value}"
    );

    let vs_sql = format!(
        "SELECT C_DECIMAL_A/C_DECIMAL_B FROM {} WHERE ID=6",
        typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_value = parse_numeric(&vs_cols[0][0]);
    assert!(
        (vs_value - oracle_value).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * oracle_value.abs(),
        "pushed-down C_DECIMAL_A/C_DECIMAL_B at id=6 must match the native \
         oracle {oracle_value} (decimal/decimal FLOAT_DIV must not truncate \
         to scale 6), got {vs_value}"
    );
}

/// `Unchecked` is a shape whose routing Exasol decides.
const SPLIT_AGGREGATE_CASES: &[(&str, PathCheck<'static>)] = &[
    (
        "SELECT LENGTH(MAX(UPPER(C_VARCHAR))) FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT C_BOOL, LENGTH(MAX(UPPER(C_VARCHAR))) FROM {T} GROUP BY C_BOOL",
        PathCheck::Wrapper,
    ),
    (
        "SELECT MAX(UPPER(C_VARCHAR)) || '!' FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT MIN(LOWER(C_VARCHAR)) || MAX(UPPER(C_VARCHAR)) FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT CASE WHEN MAX(CASE WHEN C_VARCHAR = 'aa' THEN 'Y' ELSE 'N' END) = 'Y' \
         THEN 1 ELSE 0 END FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT YEAR(MAX(DATE_TRUNC('month', C_DATE))) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, YEAR(MAX(DATE_TRUNC('month', C_DATE))) FROM {T} GROUP BY C_BOOL",
        PathCheck::Split,
    ),
    (
        "SELECT ROUND(SUM(C_DECIMAL_B * C_DECIMAL_B), 8) FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT C_BOOL, ROUND(SUM(C_DECIMAL_B * C_DECIMAL_B), 8) FROM {T} GROUP BY C_BOOL",
        PathCheck::Wrapper,
    ),
    (
        "SELECT MAX(C_DOUBLE * 2) + 1, SUM(C_DOUBLE * 3) + 1 FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT 'X', LENGTH(MAX(UPPER(C_VARCHAR))) FROM {T}",
        PathCheck::Wrapper,
    ),
    (
        "SELECT MONTH(MIN(CAST(C_DATE AS TIMESTAMP))) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT MAX(UPPER(C_VARCHAR)), LENGTH(MAX(UPPER(C_VARCHAR))) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT LENGTH(MAX(C_VARCHAR)), YEAR(MAX(C_DATE)) FROM {T}",
        PathCheck::Split,
    ),
    ("SELECT AVG(LENGTH(C_VARCHAR)) FROM {T}", PathCheck::Split),
    (
        "SELECT COUNT(UPPER(C_VARCHAR)) * 2 FROM {T}",
        PathCheck::Split,
    ),
    ("SELECT SUM(C_DECIMAL_B) / 2 FROM {T}", PathCheck::Split),
    (
        "SELECT C_BOOL, SUM(C_DECIMAL_B) / 2 FROM {T} GROUP BY C_BOOL",
        PathCheck::Split,
    ),
    (
        "SELECT C_DECIMAL_A, COUNT(*) FROM {T} GROUP BY C_DECIMAL_A ORDER BY C_DECIMAL_A",
        PathCheck::Split,
    ),
    (
        "SELECT C_DOUBLE, COUNT(*) FROM {T} GROUP BY C_DOUBLE ORDER BY C_DOUBLE DESC",
        PathCheck::Split,
    ),
    (
        "SELECT (C_DOUBLE - 2) * 0, COUNT(*) FROM {T} GROUP BY (C_DOUBLE - 2) * 0",
        PathCheck::Split,
    ),
    (
        "SELECT C_VARCHAR, COUNT(*) FROM {T} GROUP BY C_VARCHAR HAVING C_VARCHAR > 'b'",
        PathCheck::Unchecked,
    ),
    (
        "SELECT C_QTY, SUM(ID) FROM {T} GROUP BY C_QTY HAVING C_QTY + SUM(ID) > 10",
        PathCheck::Split,
    ),
    (
        "SELECT SUM(ID) FROM {T} GROUP BY C_QTY HAVING C_QTY + SUM(ID) > 10",
        PathCheck::Split,
    ),
    (
        "SELECT C_QTY, SUM(ID) FROM {T} GROUP BY C_QTY ORDER BY C_QTY + SUM(ID) DESC, C_QTY LIMIT 3",
        PathCheck::Split,
    ),
    (
        "SELECT C_DATE, COUNT(*) FROM {T} GROUP BY C_DATE ORDER BY C_DATE",
        PathCheck::Split,
    ),
    (
        "SELECT C_TS, COUNT(*) FROM {T} GROUP BY C_TS ORDER BY 1 DESC",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, COUNT(*) FROM {T} GROUP BY C_BOOL HAVING COUNT(*) > 1",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, C_DATE, SUM(C_QTY) FROM {T} GROUP BY C_BOOL, C_DATE ORDER BY 2, 1",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY C_DOUBLE ORDER BY C_DOUBLE",
        PathCheck::Split,
    ),
    (
        "SELECT UPPER(C_VARCHAR), COUNT(*) FROM {T} GROUP BY UPPER(C_VARCHAR)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY UPPER(C_VARCHAR)",
        PathCheck::Wrapper,
    ),
    (
        "SELECT ADD_DAYS(MAX(CASE WHEN ID > 2 THEN C_DATE ELSE DATE '1900-03-01' END), 1) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, YEAR(MIN(LEAST(C_DATE, DATE '2024-01-02'))) FROM {T} GROUP BY C_BOOL",
        PathCheck::Split,
    ),
    (
        "SELECT MONTH(MAX(COALESCE(C_TS, TIMESTAMP '9999-12-30 10:00:00.5'))) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT SUM(GREATEST(ID, C_QTY)) + 1, MAX(CASE WHEN C_BOOL THEN 1 ELSE 10 END) * 2 FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY CASE WHEN ID > 2 THEN C_DATE ELSE DATE '2024-01-01' END",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY NVL(C_TS, TIMESTAMP '1960-01-01 00:00:00.12') \
         HAVING NVL(C_TS, TIMESTAMP '1960-01-01 00:00:00.12') > TIMESTAMP '2024-01-02 00:00:00' \
         OR COUNT(*) > 1 ORDER BY NVL(C_TS, TIMESTAMP '1960-01-01 00:00:00.12')",
        PathCheck::Split,
    ),
    (
        "SELECT SUM(ID) FROM {T} GROUP BY GREATEST(ID, C_QTY) ORDER BY 1",
        PathCheck::Split,
    ),
    // A predicate is BOOLEAN.
    (
        "SELECT COUNT(*) FROM {T} GROUP BY C_QTY > 5",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY ID BETWEEN 2 AND 4 OR C_BOOL",
        PathCheck::Split,
    ),
    // A union of a DOUBLE with numbers is DOUBLE.
    (
        "SELECT COUNT(*) FROM {T} GROUP BY COALESCE(C_DOUBLE, 2.5E0)",
        PathCheck::Split,
    ),
    (
        "SELECT MAX(COALESCE(C_DOUBLE, 1E300)) + 1 FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, ROUND(SUM(COALESCE(C_DOUBLE, 0)), 2) FROM {T} GROUP BY C_BOOL",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY COALESCE(C_DOUBLE, 0.5)",
        PathCheck::Split,
    ),
    (
        "SELECT MAX(GREATEST(C_DOUBLE, C_DECIMAL_A)) + 1 FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY LEAST(C_DOUBLE, 1E1)",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, MIN(CASE WHEN ID > 4 THEN C_DOUBLE ELSE 1 END) * 2 FROM {T} GROUP BY C_BOOL",
        PathCheck::Split,
    ),
    // Each form below takes the type of the Exasol version it runs on.
    (
        "SELECT YEAR(MAX(CASE WHEN ID > 6 THEN C_DATE ELSE C_TS END)) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY CASE WHEN ID > 6 THEN C_DATE ELSE C_TS END",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY CASE WHEN ID > 6 THEN C_TS ELSE C_DATE END",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} \
         GROUP BY CASE WHEN ID > 6 THEN NULL WHEN ID > 2 THEN C_DATE ELSE C_TS END",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY GREATEST(C_DATE, C_TS)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY TO_TIMESTAMP(CASE WHEN ID > 2 \
         THEN '2024-01-01 10:00:00' ELSE '1960-01-01 00:00:00' END, 'YYYY-MM-DD HH24:MI:SS')",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY TO_TIMESTAMP(CASE WHEN ID > 2 \
         THEN '2024-01-01 10:00:00.123456' ELSE '1960-01-01 00:00:00.999999' END, \
         'YYYY-MM-DD HH24:MI:SS.FF6')",
        PathCheck::Split,
    ),
    (
        "SELECT C_BOOL, YEAR(MAX(ADD_HOURS(C_DATE, 1))) FROM {T} GROUP BY C_BOOL",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY ADD_MINUTES(C_DATE, ID)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY CASE WHEN ID > 2 THEN C_TS \
         ELSE TIMESTAMP '1960-01-01 00:00:00.123456' END",
        PathCheck::Split,
    ),
    (
        "SELECT MAX(CASE WHEN ID > 2 THEN C_DATE \
         ELSE TIMESTAMP '1960-01-01 10:00:00.123456' END) FROM {T}",
        PathCheck::Split,
    ),
    ("SELECT SUM(SECOND(C_DATE)) + 1 FROM {T}", PathCheck::Split),
    // TO_TIMESTAMP holds values beyond 2262 and reads text that ends before its format.
    (
        "SELECT COUNT(*) FROM {T} GROUP BY TO_TIMESTAMP(CASE WHEN ID > 2 \
         THEN '9999-12-30' ELSE '2024-01-31 10' END, 'YYYY-MM-DD HH24:MI:SS')",
        PathCheck::Split,
    ),
    (
        "SELECT MAX(TO_TIMESTAMP(CASE WHEN ID > 2 THEN '9999-12-30 10:11:12.1234567' \
         ELSE '0001-01-01 10:11:12.5' END, 'YYYY-MM-DD HH24:MI:SS.FF6')) FROM {T}",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY SECOND(C_DATE)",
        PathCheck::Split,
    ),
    // A date part takes DECIMAL(4,0) for YEAR and DECIMAL(2,0) for the rest on every version.
    (
        "SELECT COUNT(*) FROM {T} GROUP BY YEAR(C_DATE)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY MONTH(C_TS)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY WEEK(C_DATE)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY HOUR(C_TS)",
        PathCheck::Split,
    ),
    (
        "SELECT COUNT(*) FROM {T} GROUP BY EXTRACT(MINUTE FROM C_TS)",
        PathCheck::Split,
    ),
    ("SELECT MAX(DAY(C_DATE)) + 1 FROM {T}", PathCheck::Split),
];

/// Over the CHAR pad table. The scan does not blank-pad, so Exasol applies each CAST to CHAR, and
/// the over-length value fails a `CHAR(20)` with Exasol's right-truncation error.
fn char_cast_cases() -> Vec<(String, Expect)> {
    let cast = |n: u32| format!("CAST({CHAR_PAD_COL} AS CHAR({n}))");
    let fits = format!("{CHAR_PAD_COL} <> '{CHAR_PAD_OVER_LENGTH}'");
    [
        (
            format!(
                "SELECT {}, COUNT(*) FROM {{T}} WHERE {fits} GROUP BY {}",
                cast(4),
                cast(4)
            ),
            Expect::Rows,
        ),
        (
            format!(
                "SELECT COUNT(*) FROM {{T}} WHERE {fits} GROUP BY {}",
                cast(4)
            ),
            Expect::Rows,
        ),
        (
            format!(
                "SELECT {} FROM {{T}} WHERE {CHAR_PAD_COL} = '{CHAR_PAD_SHORT_TRAILING_SPACE}'",
                cast(4)
            ),
            Expect::Rows,
        ),
        (
            format!("SELECT {} g, COUNT(*) c FROM {{T}} GROUP BY 1", cast(30)),
            Expect::Rows,
        ),
        (format!("SELECT {} FROM {{T}}", cast(20)), Expect::Error),
        (
            format!("SELECT {} g, COUNT(*) c FROM {{T}} GROUP BY 1", cast(20)),
            Expect::Error,
        ),
        (
            format!("SELECT COUNT(*) c FROM {{T}} GROUP BY {}", cast(20)),
            Expect::Error,
        ),
    ]
    .into()
}

/// Scenario: A nested aggregate over an expression declines the split
/// Scenario: A nested aggregate takes the result type Exasol gives it
/// Scenario: A merged aggregate under a scalar keeps its native type
/// Scenario: A group key travels with its exact type
/// Scenario: An unprojected bare-column group key takes its column type
/// Scenario: An unprojected group key with a derived exact type keeps the split
/// Scenario: A date-part function takes its fixed native type
/// Scenario: An unprojected group key without an exact type declines the split
/// Scenario: A form whose type differs by Exasol version takes the type of the version it runs on
/// Scenario: A CAST to CHAR returns Exasol's padded value
#[test]
fn e2e_split_aggregate_columns_match_native_oracle() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let typed = typed_table();
    create_native_copy(&mut conn, &typed, TYPED_COLUMNS);
    let mut failures: Vec<String> = SPLIT_AGGREGATE_CASES
        .iter()
        .filter_map(|(query, path)| {
            native_copy_mismatch(&mut conn, &typed, NATIVE_COPY, query, *path, Expect::Rows)
        })
        .collect();

    let char_pad = vs_char_pad_table();
    create_full_native_copy(&mut conn, &char_pad);
    let char_cases = char_cast_cases();
    failures.extend(char_cases.iter().filter_map(|(query, expect)| {
        native_copy_mismatch(
            &mut conn,
            &char_pad,
            NATIVE_COPY,
            query,
            PathCheck::Wrapper,
            *expect,
        )
    }));

    assert!(
        failures.is_empty(),
        "{} of {} split-aggregate cases failed:\n{}",
        failures.len(),
        SPLIT_AGGREGATE_CASES.len() + char_cases.len(),
        failures.join("\n")
    );
}

fn first_cell(conn: &mut ExaConn, sql: &str) -> serde_json::Value {
    conn.query_columns(sql)[0][0].clone()
}

/// The scan function `TO_TIMESTAMP` renders as.
const TO_TIMESTAMP_SCAN_FUNCTION: &str = "vs_to_timestamp(";

fn assert_pushed_into_scan(conn: &mut ExaConn, sql: &str, pattern: &str) {
    let statement = isolated_pushdown_statement(conn, sql);
    assert!(
        statement.contains("LAKEHOUSE_SCAN") && statement.contains(pattern),
        "the scan call must carry `{pattern}`:\n{statement}"
    );
    for exasol_format in ["YYYY", "yyyy"] {
        assert!(
            !statement.contains(exasol_format),
            "the Exasol format model must not reach the scan:\n{statement}"
        );
    }
}

/// Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls
/// Scenario: A string-literal format translates to a strftime pattern
#[test]
fn e2e_to_date_to_timestamp_translated_format_matches_native() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let date_sql = format!(
        "SELECT TO_DATE('2024-01-' || LPAD(id, 2, '0'), 'YYYY-MM-DD') FROM {} WHERE id = 1",
        events_table()
    );
    let native_date = first_cell(&mut conn, "SELECT TO_DATE('2024-01-01', 'YYYY-MM-DD')");
    assert_eq!(first_cell(&mut conn, &date_sql), native_date);
    assert_eq!(native_date, "2024-01-01");

    let timestamp_sql = format!(
        "SELECT TO_TIMESTAMP('2024-01-' || LPAD(id, 2, '0') || ' 10:11:12', \
         'YYYY-MM-DD HH24:MI:SS') FROM {} WHERE id = 1",
        events_table()
    );
    let native_timestamp = first_cell(
        &mut conn,
        "SELECT TO_TIMESTAMP('2024-01-01 10:11:12', 'YYYY-MM-DD HH24:MI:SS')",
    );
    assert_eq!(first_cell(&mut conn, &timestamp_sql), native_timestamp);

    let filter_sql = format!(
        "SELECT id FROM {} WHERE TO_TIMESTAMP('2024-01-' || LPAD(id, 2, '0') || ' 10:11:12.250', \
         'yyyy-mm-dd hh24:mi:ss.ff3') > TIMESTAMP '2024-01-15 10:11:12.000' ORDER BY id",
        events_table()
    );
    assert_eq!(
        ids_of_first_column(&mut conn, &filter_sql),
        (15..=20).collect::<Vec<i64>>()
    );

    assert_pushed_into_scan(&mut conn, &date_sql, "%Y-%m-%d");
    assert_pushed_into_scan(&mut conn, &timestamp_sql, "%Y-%m-%d %H:%M:%S");
    assert_pushed_into_scan(&mut conn, &filter_sql, "%H:%M:%S%.f");

    assert_eq!(
        first_cell(&mut conn, "SELECT TO_DATE('', 'YYYY-MM-DD')"),
        serde_json::Value::Null
    );
    let empty_sql = format!(
        "SELECT TO_DATE(SUBSTR(name, 1, 0), 'YYYY-MM-DD') FROM {} WHERE id = 1",
        events_table()
    );
    assert_eq!(first_cell(&mut conn, &empty_sql), serde_json::Value::Null);
    assert_pushed_into_scan(&mut conn, &empty_sql, "nullif(");
}

/// Scenario: A string-literal format translates to a strftime pattern
#[test]
fn e2e_to_timestamp_microsecond_fraction_filters_in_scan() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} WHERE TO_TIMESTAMP('2024-01-' || LPAD(id, 2, '0') || ' 10:11:12.000500', \
         'YYYY-MM-DD HH24:MI:SS.FF6') > TIMESTAMP '2024-01-15 10:11:12.000' ORDER BY id",
        events_table()
    );
    // Exasol 8 cuts the microseconds, so 10:11:12.000500 is not after 10:11:12.000 there.
    let first = if engine_honors_declared_precision(&mut conn) {
        15
    } else {
        16
    };
    assert_eq!(
        ids_of_first_column(&mut conn, &sql),
        (first..=20).collect::<Vec<i64>>()
    );
    assert_pushed_into_scan(&mut conn, &sql, "%H:%M:%S%.f");
}

/// `TO_TIMESTAMP` over values outside 1677 to 2262, text that ends before its format, and
/// fractions shorter or longer than `FFn`.
const TO_TIMESTAMP_CASES: &[&str] = &[
    "SELECT ID, TO_TIMESTAMP(CASE ID WHEN 1 THEN '9999-12-31' WHEN 2 THEN '0001-01-01' \
     ELSE '1600-01-01' END, 'YYYY-MM-DD') FROM {T}",
    "SELECT ID, TO_TIMESTAMP(CASE ID WHEN 1 THEN '2024-01-31' WHEN 2 THEN '2024-01-31 10' \
     ELSE '2024-01-31 10:11:' END, 'YYYY-MM-DD HH24:MI:SS') FROM {T}",
    "SELECT ID, TO_TIMESTAMP(CASE ID WHEN 1 THEN '2024-01-31 10:11:12.5' \
     WHEN 2 THEN '2024-01-31 10:11:12.1234567' ELSE '1960-01-01 00:00:00.999999' END, \
     'YYYY-MM-DD HH24:MI:SS.FF6') FROM {T}",
    "SELECT ID, TO_TIMESTAMP(CASE ID WHEN 1 THEN '2024-01-31 10:11:12.123456789' \
     ELSE '1960-01-01 00:00:00.5' END, 'YYYY-MM-DD HH24:MI:SS.FF9') FROM {T}",
    "SELECT ID, ADD_DAYS(TO_TIMESTAMP(CASE ID WHEN 1 THEN '9999-12-30' ELSE '2024-01-31' END, \
     'YYYY-MM-DD'), 1) FROM {T}",
    "SELECT ID, ADD_HOURS(TO_TIMESTAMP(CASE ID WHEN 1 THEN '2024-01-31' \
     ELSE '0001-01-01 10:11' END, 'YYYY-MM-DD HH24:MI:SS'), 1) FROM {T}",
    "SELECT ID, CASE WHEN ID > 1 THEN TO_TIMESTAMP(CASE ID WHEN 2 THEN '9999-12-30 10' \
     ELSE '0001-01-01 1' END, 'YYYY-MM-DD HH24:MI') ELSE C_DATE END FROM {T}",
    "SELECT ID FROM {T} WHERE TO_TIMESTAMP(CASE ID WHEN 1 THEN '9999-12-31' ELSE '2024-01-31' END, \
     'YYYY-MM-DD') > TIMESTAMP '2262-04-12 00:00:00'",
];

/// A second of 60, which Exasol rejects, and an `FFn` without its dot, which runs in Exasol.
const TO_TIMESTAMP_EDGE_CASES: &[(&str, PathCheck<'static>, Expect)] = &[
    (
        "SELECT ID, TO_TIMESTAMP(CASE ID WHEN 1 THEN '2024-01-31 10:11:60' \
         ELSE '2024-01-31 10:11:12' END, 'YYYY-MM-DD HH24:MI:SS') FROM {T}",
        PathCheck::Has(TO_TIMESTAMP_SCAN_FUNCTION),
        Expect::Error,
    ),
    (
        "SELECT ID, TO_TIMESTAMP(CASE ID WHEN 1 THEN '2024-01-31 10:11:12125' \
         ELSE '2024-01-31 10:11:12' END, 'YYYY-MM-DD HH24:MI:SSFF6') FROM {T}",
        PathCheck::Lacks(TO_TIMESTAMP_SCAN_FUNCTION),
        Expect::Rows,
    ),
];

/// Scenario: The scan reads a TO_TIMESTAMP string as Exasol does
/// Scenario: A TIMESTAMP computes at the unit of the digits its values hold
/// Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch
#[test]
fn e2e_to_timestamp_reads_text_as_native_does() {
    lakehouse_vs();
    let mut conn = exa_conn();
    let typed = typed_table();
    create_native_copy(&mut conn, &typed, TYPED_COLUMNS);

    let cases: Vec<(&str, PathCheck, Expect)> = TO_TIMESTAMP_CASES
        .iter()
        .map(|query| {
            (
                *query,
                PathCheck::Has(TO_TIMESTAMP_SCAN_FUNCTION),
                Expect::Rows,
            )
        })
        .chain(TO_TIMESTAMP_EDGE_CASES.iter().copied())
        .collect();
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|(query, path, expect)| {
            native_copy_mismatch(&mut conn, &typed, NATIVE_COPY, query, *path, *expect)
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// Scenario: A string Exasol rejects for a translated format fails the query
#[test]
fn e2e_to_date_translated_format_fails_on_rejected_input() {
    lakehouse_vs();
    let mut conn = exa_conn();

    query_error(&mut conn, "SELECT TO_DATE('event-01', 'YYYY-MM-DD')");

    let select_sql = format!(
        "SELECT TO_DATE(name, 'YYYY-MM-DD') FROM {} WHERE id = 1",
        events_table()
    );
    assert_query_fails(&mut conn, &select_sql, &["event-01", "%Y-%m-%d"]);
    assert_query_fails(
        &mut conn,
        &format!(
            "SELECT TO_TIMESTAMP(name, 'YYYY-MM-DD') FROM {} WHERE id = 1",
            events_table()
        ),
        &["event-01", "TO_TIMESTAMP"],
    );
    for text in ["2024-01", "10000-01-01"] {
        assert_query_fails(
            &mut conn,
            &format!(
                "SELECT TO_TIMESTAMP(CASE ID WHEN 1 THEN '{text}' ELSE '2024-01-31' END, \
                 'YYYY-MM-DD') FROM {}",
                typed_table()
            ),
            &[text, "TO_TIMESTAMP"],
        );
    }
    assert_query_fails(
        &mut conn,
        &format!(
            "SELECT COUNT(*) FROM {} WHERE TO_DATE(name, 'YYYY-MM-DD') > DATE '2000-01-01'",
            events_table()
        ),
        &["Error parsing timestamp", "%Y-%m-%d"],
    );
    assert_query_fails(
        &mut conn,
        &format!(
            "SELECT TO_DATE('2024-01-' || LPAD(id, 2, '0'), 'YYYY-DD-MM') FROM {} WHERE id = 13",
            events_table()
        ),
        &["2024-01-13", "%Y-%d-%m"],
    );

    assert_pushed_into_scan(&mut conn, &select_sql, "%Y-%m-%d");
}

fn assert_evaluated_in_exasol(conn: &mut ExaConn, sql: &str) {
    let pushed = isolated_pushdown_statement(conn, sql);
    assert!(
        pushed.contains("TO_DATE("),
        "Exasol must evaluate the declined call:\n{pushed}"
    );
    assert!(
        !pushed.contains("to_date("),
        "the declined call must not reach the scan:\n{pushed}"
    );
}

/// Scenario: A declined format returns the native result in the select list and in a filter
#[test]
fn e2e_to_date_declined_format_matches_native() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let filter_sql = format!(
        "SELECT id FROM {} WHERE TO_DATE(CAST(id AS VARCHAR(2)) || '-JAN-2024', 'DD-MON-YYYY') \
         > DATE '2024-01-17' ORDER BY id",
        events_table()
    );
    assert_eq!(
        ids_of_first_column(&mut conn, &filter_sql),
        vec![18, 19, 20]
    );
    assert_evaluated_in_exasol(&mut conn, &filter_sql);

    let select_sql = format!(
        "SELECT TO_DATE(CAST(id AS VARCHAR(2)) || '-JAN-2024', 'DD-MON-YYYY') FROM {} \
         WHERE id = 18",
        events_table()
    );
    let native = first_cell(&mut conn, "SELECT TO_DATE('18-JAN-2024', 'DD-MON-YYYY')");
    assert_eq!(first_cell(&mut conn, &select_sql), native);
    assert_eq!(native, "2024-01-18");
    assert_evaluated_in_exasol(&mut conn, &select_sql);
}
