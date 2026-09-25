//! E2E capability-alignment tests: each advertised capability group runs the full
//! advertised → translated → executed path. Seed: id 1..=20, score = 5.0 * id,
//! name = "event-NN", event_date = 2024-01-01 + (id-1) days. Fail, never skip,
//! without the stack.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    CHAR_PAD_COL, CHAR_PAD_OTHER, CHAR_PAD_OVER_LENGTH, CHAR_PAD_SHORT,
    CHAR_PAD_SHORT_TRAILING_SPACE, CHAR_PAD_TOTAL_ROWS, E2E_CHAR_PAD_TABLE, E2E_DIM_TABLE,
    E2E_FACT_TABLE, E2E_NAMESPACE, E2E_TABLE, E2E_TYPED_TABLE, ExpectedValue, seed_events,
    seed_typed_distinct_probe,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};
use common::timestamp_precision::expected_timestamp_precision;

use std::sync::OnceLock;

const VS_NAME: &str = "MY_LAKEHOUSE";

/// The fix's measured worst-case divergence from native Exasol is ~1 ULP (3.17e-16).
const FLOAT_DIV_ORACLE_REL_TOLERANCE: f64 = 1e-15;

// Each test binary runs independently, so this file needs its own OnceLock setup.

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
            // Same namespace as `events`, so it is queryable through the one `MY_LAKEHOUSE` VS.
            seed_typed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed typed_distinct_probe table")
        });

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
    });
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

fn vs_typed_table() -> String {
    format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase())
}

fn vs_char_pad_table() -> String {
    format!("{VS_NAME}.{}", E2E_CHAR_PAD_TABLE.to_uppercase())
}

/// Scenario: a live getCapabilities round-trip advertises JOIN, JOIN_TYPE_INNER and JOIN_CONDITION_EQUI
#[test]
fn e2e_advertises_inner_equi_join_capability() {
    setup_e2e();
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

/// Scenario: a live getCapabilities round-trip advertises FN_AGG_COUNT_DISTINCT and AGGREGATE_SINGLE_GROUP (#56)
#[test]
fn advertises_count_distinct_capability() {
    setup_e2e();
    let mut conn = exa_conn();

    let query = format!("SELECT COUNT(DISTINCT name) FROM {}", vs_table());
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

/// Scenario: ABS(score - 50.0) < 20.0 in a WHERE filter pushes down and returns ids 7..=13
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

/// Scenario: LOWER(name) LIKE 'event-1%' in a WHERE filter pushes down and returns ids 10..=19
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

/// Scenario: EXTRACT(DAY FROM event_date) > 10 in a WHERE filter pushes down and returns ids 11..=20
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

/// Scenario: REGEXP_LIKE(name, 'event-0[0-9]') in a WHERE filter pushes down and returns ids 1..=9
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

    let expected_count = 9i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "REGEXP_LIKE(name, 'event-0[0-9]') must return {expected_count} rows, got {}",
        cols[0].len()
    );

    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 1 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }

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

/// Scenario: scalar expressions in the SELECT list push down and return correct values
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

/// Scenario: IN, BETWEEN, IS NULL, IS NOT NULL and <> select-list items push down as positional expressions (#196)
#[test]
fn e2e_selectlist_predicate_projection_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    {
        let sql = format!(
            "SELECT id, id IN (1,2,3) FROM {} WHERE id <= 5 ORDER BY id",
            vs_table()
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
            vs_table()
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
            vs_typed_table()
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
            vs_typed_table()
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
            vs_table()
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

/// Scenario: a 10-item select list ending in a BETWEEN over a 10-column table projects positionally without widening (#196, #234)
#[test]
fn e2e_selectlist_between_at_matching_arity_projects_as_expr() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, c_decimal_a, c_decimal_b, c_double, c_varchar, c_date, \
         c_ts, c_bool, c_price, (c_qty BETWEEN 1 AND 3) FROM {} WHERE id <= 3 \
         ORDER BY id",
        vs_typed_table()
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

/// Scenario: a LENGTH(DOUBLE) widening at the coincidental arity, and over a table of different arity with ORDER BY, returns correct rows (#196, #234)
#[test]
fn e2e_widened_projection_with_declined_order_by_routes_to_wrapper() {
    setup_e2e();
    let mut conn = exa_conn();

    {
        let sql = format!(
            "SELECT id, c_decimal_a, c_decimal_b, c_double, c_varchar, c_date, \
             c_ts, c_bool, c_price, LENGTH(c_double) FROM {} WHERE id <= 3 \
             ORDER BY id",
            vs_typed_table()
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
            vs_table()
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

/// Scenario: projected literal select-list items, including a duplicated literal, keep their arity and values (#190)
#[test]
fn e2e_selectlist_literal_projection_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table();

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

/// Scenario: COUNT(*) over a LIMIT-bearing derived table does not fail with a column-count mismatch (#205)
#[test]
fn e2e_count_star_over_limited_subselect_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table();

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

/// Scenario: an all-files-pruned predicate still accepts repeated literals plus a real column (#190)
#[test]
fn e2e_all_files_pruned_literal_projection_empty_shape() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT 1, name, 1 FROM {} WHERE id > 1000", vs_table());
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

/// Scenario: a projected CURRENT_TIMESTAMP/SYSTIMESTAMP matches Exasol's native session-local value (#238)
#[test]
fn e2e_now_family_projection_matches_native_session_local_value() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table();
    pin_session_time_zone(&mut conn);
    let offset_seconds = session_utc_offset_seconds(&mut conn);
    assert!(
        offset_seconds != 0.0,
        "fixture precondition: the session's UTC offset must be non-zero, or the \
         assertion below passes regardless of whether #238 is fixed"
    );

    for expr in ["CURRENT_TIMESTAMP", "SYSTIMESTAMP"] {
        // Measured within one statement, so no wall-clock time elapses between the two values
        // that could mask a UTC-shift regression.
        let deviation = scalar_seconds(
            &mut conn,
            &format!("SELECT SECONDS_BETWEEN({expr}, (SELECT {expr} FROM {table} WHERE id = 1))"),
        )
        .abs();
        assert!(
            deviation < 1.0,
            "{expr}'s deviation between the native value and the VS-projected value \
             ({deviation}s) must be near zero — this is the sharp assertion that \
             fails if the adapter ever ships the UTC instant instead of Exasol's own \
             session-local value (the pre-#238 defect, {offset_seconds}s under this \
             session's zone)"
        );
    }
}

/// Scenario: a projected TIMESTAMP WITH LOCAL TIME ZONE constant returns the exact session-local value, also with all files pruned (#218)
#[test]
fn e2e_projected_tstz_literal_matches_native_and_pruned_scan_succeeds() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table();
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

/// Scenario: HAVING COUNT(*) > 3 applies to the merged result, keeping groups whose per-shard counts are at most 3
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

/// Scenario: STDDEV/VARIANCE and their POP variants merge to the single-node result within tolerance
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

/// Scenario: STDDEV(score + id) declines decomposition and Exasol computes the correct sample standard deviation (#179)
#[test]
fn e2e_stddev_over_expression_falls_back_and_returns_correct_value() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT STDDEV(score + id) FROM {}", vs_table());

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

    let row_cols = conn.query_columns(&format!("SELECT score, id FROM {}", vs_table()));
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

/// Scenario: grouped STDDEV(score + id) declines decomposition and returns each group's correct sample standard deviation (#179)
#[test]
fn e2e_grouped_stddev_over_expression_falls_back_and_returns_correct_value() {
    const GROUP_MODULUS: i64 = 4;

    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT MOD(id, {GROUP_MODULUS}), STDDEV(score + id) FROM {} \
         GROUP BY MOD(id, {GROUP_MODULUS}) ORDER BY 1",
        vs_table()
    );

    // A statistical partial column here is the accepted-then-failing shape the decline removes.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed_sql.contains("PARTIAL_stat_"),
        "grouped STDDEV over an expression argument must NOT push a statistical \
         partial/merge decomposition, got:\n{pushed_sql}"
    );

    let row_cols = conn.query_columns(&format!("SELECT id, score FROM {}", vs_table()));
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

/// Scenario: CAST(id AS VARCHAR(2000000)) = '15' in a WHERE filter pushes down and returns id 15 (#104)
#[test]
fn e2e_cast_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} WHERE CAST(id AS VARCHAR(2000000)) = '15' ORDER BY id",
        vs_table()
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

/// Scenario: -score < -50.0 in a WHERE filter pushes down and returns ids 11..=20 (#105)
#[test]
fn e2e_unary_minus_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, score FROM {} WHERE -score < -50.0 ORDER BY id",
        vs_table()
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

/// Scenario: WEEK(event_date) = 2 in a WHERE filter pushes down and returns ISO week 2, ids 8..=14 (#107)
#[test]
fn e2e_week_in_filter() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, event_date FROM {} WHERE WEEK(event_date) = 2 ORDER BY id",
        vs_table()
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

// Only the four *_BETWEEN functions are advertised: ADD_HOURS/ADD_MINUTES over a DATE
// would render TIMESTAMP(3) where Exasol infers TIMESTAMP(0), so Exasol rejects them.

fn assert_select_pushed_down(conn: &mut ExaConn, query_sql: &str, fragment: &str) {
    let pushed = explain_virtual_sql(conn, query_sql);
    assert!(
        pushed.contains(fragment),
        "expected pushed SQL to contain {fragment:?}, got: {pushed}"
    );
}

/// Scenario: DAYS_BETWEEN pushes down with Exasol's sign convention, earlier first argument yields -9
#[test]
fn e2e_days_between_matches_exasol() {
    setup_e2e();
    let mut conn = exa_conn();
    let t = vs_table();

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

/// Scenario: HOURS/MINUTES/SECONDS_BETWEEN push down and match Exasol's fractional values over a 2.5-hour gap
#[test]
fn e2e_time_between_matches_exasol() {
    setup_e2e();
    let mut conn = exa_conn();
    let t = vs_table();

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

/// Scenario: CAST, EXTRACT and CASE together in the SELECT list push down with correct values (#136)
#[test]
fn e2e_selectlist_cast_extract_case_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, CAST(id AS VARCHAR(2000000)), EXTRACT(YEAR FROM event_date), \
         CASE WHEN id > 10 THEN 'high' ELSE 'low' END FROM {} WHERE id <= 3 ORDER BY id",
        vs_table()
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

/// Scenario: ORDER BY a column outside the select list pushes down via a hidden sort column that drives the order (#225)
#[test]
fn e2e_order_by_unprojected_column_bare_projection() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT score FROM {} WHERE id = 1 ORDER BY id", vs_table());

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
        vs_table()
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

/// Scenario: ORDER BY a column referenced only inside a projected expression pushes down correctly (#225)
#[test]
fn e2e_order_by_column_referenced_only_in_projected_expression() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id || '-' || name FROM {} WHERE id <= 3 ORDER BY id",
        vs_table()
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

// Exasol's DECIMAL→VARCHAR conversion trims trailing scale zeros and drops an all-zero
// fraction (`30.00` → `"30"`) (#211). Expected strings come from an independent Rust
// oracle, never from the production `format_decimal_exasol_style`.

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

/// From scratch, not `format_decimal_exasol_style`, so it is an independent oracle.
fn exasol_trim_decimal_string(unscaled: i128, scale: u32) -> String {
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let digits = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits
    };
    let (int_part, frac_part) = digits.split_at(digits.len() - scale);
    let frac_trimmed = frac_part.trim_end_matches('0');

    let mut out = int_part.to_string();
    if !frac_trimmed.is_empty() {
        out.push('.');
        out.push_str(frac_trimmed);
    }
    if negative {
        out = format!("-{out}");
    }
    out
}

/// Scenario: the `exasol_trim_decimal_string` oracle matches Exasol's trimming rule for every `c_decimal_a` value
#[test]
fn exasol_trim_decimal_string_matches_documented_values() {
    assert_eq!(exasol_trim_decimal_string(1050, 2), "10.5");
    assert_eq!(exasol_trim_decimal_string(2025, 2), "20.25");
    assert_eq!(exasol_trim_decimal_string(3000, 2), "30");
    assert_eq!(exasol_trim_decimal_string(4099, 2), "40.99");
    assert_eq!(exasol_trim_decimal_string(5000, 2), "50");
    assert_eq!(exasol_trim_decimal_string(6000, 2), "60");
}

/// Scenario: CAST(c_decimal_a AS VARCHAR(20)) trims trailing scale zeros like native Exasol (#211)
#[test]
fn e2e_decimal_cast_trims_trailing_zeros() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, CAST(c_decimal_a AS VARCHAR(20)) FROM {} WHERE id IN (1,4,6) ORDER BY id",
        vs_typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, CAST): {cols:?}");
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1,4,6): {cols:?}");

    let expected = ["10.5", "30", "40.99"];
    for (i, exp) in expected.iter().enumerate() {
        let s = cols[1][i]
            .as_str()
            .unwrap_or_else(|| panic!("CAST result at row {i} is not a string: {:?}", cols[1][i]));
        assert_eq!(
            s, *exp,
            "row {i}: CAST(c_decimal_a AS VARCHAR(20)) must be {exp:?}, got {s:?} \
             (pre-fix code returned the untrimmed fixed-scale string, e.g. \"10.50\")"
        );
    }
}

/// Scenario: projecting one column while ordering by a different unprojected column pushes down correctly (#189)
#[test]
fn e2e_issue_189_shape_equivalent_local_verification() {
    setup_e2e();
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

/// Scenario: CONCAT over a DECIMAL operand trims trailing scale zeros like native Exasol (#211)
#[test]
fn e2e_decimal_concat_trims_trailing_zeros() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, id||'-'||c_decimal_a FROM {} WHERE id IN (1,4) ORDER BY id",
        vs_typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, CONCAT): {cols:?}");
    assert_eq!(cols[0].len(), 2, "expected 2 rows (id 1,4): {cols:?}");

    let expected = ["1-10.5", "4-30"];
    for (i, exp) in expected.iter().enumerate() {
        let s = cols[1][i].as_str().unwrap_or_else(|| {
            panic!("CONCAT result at row {i} is not a string: {:?}", cols[1][i])
        });
        assert_eq!(
            s, *exp,
            "row {i}: id||'-'||c_decimal_a must be {exp:?}, got {s:?} \
             (pre-fix code returned \"1-10.50\" / \"4-30.00\")"
        );
    }
}

/// Scenario: LENGTH(c_decimal_a) reflects the trimmed string's length (#211)
#[test]
fn e2e_decimal_length_reflects_trimmed_string() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, LENGTH(c_decimal_a) FROM {} WHERE id IN (1,4,6) ORDER BY id",
        vs_typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, LENGTH): {cols:?}");
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1,4,6): {cols:?}");

    // Untrimmed "10.50" / "30.00" / "40.99" would all be 5 characters.
    let expected = [4i64, 2, 5];
    for (i, exp) in expected.iter().enumerate() {
        let len = parse_int(&cols[1][i]);
        assert_eq!(
            len, *exp,
            "row {i}: LENGTH(c_decimal_a) must be {exp}, got {len} \
             (pre-fix code returned 5 uniformly)"
        );
    }
}

/// Scenario: COUNT(*) WHERE LENGTH(c_decimal_a) > N matches Exasol's trimmed LENGTH semantics per row and in aggregate (#211)
#[test]
fn e2e_decimal_length_where_count_matches_trimmed_semantics() {
    setup_e2e();
    let mut conn = exa_conn();
    let t = vs_typed_table();

    let expected_lengths: Vec<(i64, Option<i64>)> = TYPED_DECIMAL_A_UNSCALED
        .iter()
        .map(|&(id, unscaled)| {
            (
                id,
                unscaled.map(|v| exasol_trim_decimal_string(v, 2).len() as i64),
            )
        })
        .collect();

    let expected_count = expected_lengths
        .iter()
        .filter(|(_, len)| len.is_some_and(|l| l > 4))
        .count() as i64;
    let untrimmed_count = expected_lengths
        .iter()
        .filter(|(_, len)| len.is_some())
        .count() as i64;

    // Every untrimmed value is 5 characters, so a differing count is what discriminates
    // old from new behavior.
    assert_ne!(
        expected_count, untrimmed_count,
        "expected trimmed-length count must differ from the untrimmed-length \
         (bug) count of {untrimmed_count} for this test to discriminate \
         old vs. new code"
    );

    let row_sql = format!("SELECT id, LENGTH(c_decimal_a) FROM {t} ORDER BY id");
    let cols = conn.query_columns(&row_sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, LENGTH): {cols:?}");
    assert_eq!(cols[0].len(), 12, "expected all 12 seed rows: {cols:?}");

    for (i, &(expected_id, expected_len)) in expected_lengths.iter().enumerate() {
        let id = parse_int(&cols[0][i]);
        assert_eq!(
            id, expected_id,
            "row {i}: id must be {expected_id}, got {id}"
        );
        match expected_len {
            Some(len) => {
                let actual = parse_int(&cols[1][i]);
                assert_eq!(
                    actual, len,
                    "id={id}: LENGTH(c_decimal_a) must be {len}, got {actual} \
                     (a pre-fix build would return the untrimmed length 5 here)"
                );
            }
            None => {
                assert!(
                    cols[1][i].is_null(),
                    "id={id}: LENGTH(c_decimal_a) must be NULL for a NULL cell, got {:?}",
                    cols[1][i]
                );
            }
        }
    }

    let count_sql = format!("SELECT COUNT(*) FROM {t} WHERE LENGTH(c_decimal_a) > 4");
    let count_cols = conn.query_columns(&count_sql);
    let actual_count = parse_int(&count_cols[0][0]);
    assert_eq!(
        actual_count, expected_count,
        "COUNT(*) WHERE LENGTH(c_decimal_a) > 4 must be {expected_count} \
         (trimmed-string LENGTH semantics), got {actual_count} — a pre-fix \
         build would return {untrimmed_count} (every non-NULL row, via the \
         untrimmed fixed-scale string length)"
    );
}

// String functions over non-string arguments (#210): Exasol converts implicitly, but
// DataFusion refuses. VARCHAR/CHAR pass through, DATE is CAST to VARCHAR, DECIMAL uses
// the trimmed rendering, and other types decline to native Exasol.

/// Scenario: UPPER(c_varchar) still pushes down through the VARCHAR passthrough (#210)
#[test]
fn e2e_upper_varchar_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT UPPER(c_varchar) FROM {} WHERE id = 1",
        vs_typed_table()
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

/// Scenario: UPPER over a DECIMAL(20,0) column returns the plain digit string (#210)
#[test]
fn e2e_upper_id_trims_to_plain_integer_string() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT UPPER(id) FROM {} WHERE id = 4", vs_typed_table());
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

/// Scenario: LTRIM(c_decimal_a) returns the Exasol-trimmed decimal string (#210)
#[test]
fn e2e_ltrim_decimal_trims_trailing_zeros() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, LTRIM(c_decimal_a) FROM {} WHERE id IN (1,4,6) ORDER BY id",
        vs_typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, LTRIM): {cols:?}");
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1,4,6): {cols:?}");

    let ids = [1i64, 4, 6];
    for (i, &expected_id) in ids.iter().enumerate() {
        let id = parse_int(&cols[0][i]);
        assert_eq!(
            id, expected_id,
            "row {i}: id must be {expected_id}, got {id}"
        );

        let unscaled = TYPED_DECIMAL_A_UNSCALED
            .iter()
            .find(|&&(row_id, _)| row_id == expected_id)
            .unwrap_or_else(|| panic!("no TYPED_DECIMAL_A_UNSCALED entry for id={expected_id}"))
            .1
            .unwrap_or_else(|| panic!("id={expected_id} c_decimal_a must not be NULL"));
        let expected = exasol_trim_decimal_string(unscaled, 2);

        let s = cols[1][i]
            .as_str()
            .unwrap_or_else(|| panic!("LTRIM result at row {i} is not a string: {:?}", cols[1][i]));
        assert_eq!(
            s, expected,
            "row {i} (id={id}): LTRIM(c_decimal_a) must be {expected:?}, got {s:?}"
        );
    }
}

/// Scenario: LOWER(c_date) returns Exasol's default YYYY-MM-DD rendering (#210)
#[test]
fn e2e_lower_date_formats_as_iso() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT LOWER(c_date) FROM {} WHERE id = 1",
        vs_typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 column (LOWER(c_date)): {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row (id=1): {cols:?}");

    let lower = cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("LOWER(c_date) not a string: {:?}", cols[0][0]));
    assert_eq!(
        lower, "2024-01-01",
        "LOWER(c_date) for id=1 must be \"2024-01-01\", got {lower:?}"
    );
}

/// Scenario: INSTR(c_decimal_a, '.') returns the position within the trimmed decimal string (#210)
#[test]
fn e2e_instr_decimal_finds_dot_position_in_trimmed_text() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, INSTR(c_decimal_a, '.') FROM {} WHERE id IN (1,4,6) ORDER BY id",
        vs_typed_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (id, INSTR): {cols:?}");
    assert_eq!(cols[0].len(), 3, "expected 3 rows (id 1,4,6): {cols:?}");

    let ids = [1i64, 4, 6];
    for (i, &expected_id) in ids.iter().enumerate() {
        let id = parse_int(&cols[0][i]);
        assert_eq!(
            id, expected_id,
            "row {i}: id must be {expected_id}, got {id}"
        );

        let unscaled = TYPED_DECIMAL_A_UNSCALED
            .iter()
            .find(|&&(row_id, _)| row_id == expected_id)
            .unwrap_or_else(|| panic!("no TYPED_DECIMAL_A_UNSCALED entry for id={expected_id}"))
            .1
            .unwrap_or_else(|| panic!("id={expected_id} c_decimal_a must not be NULL"));
        let trimmed = exasol_trim_decimal_string(unscaled, 2);
        let expected_pos = trimmed.find('.').map(|i| i as i64 + 1).unwrap_or(0);

        let pos = parse_int(&cols[1][i]);
        assert_eq!(
            pos, expected_pos,
            "row {i} (id={id}): INSTR(c_decimal_a, '.') must be {expected_pos} \
             (position within trimmed text {trimmed:?}), got {pos}"
        );
    }
}

// BOOLEAN, DOUBLE and TIMESTAMP string-function arguments decline to native Exasol.
// Each result is compared with an in-session native oracle over a bare literal, so a
// regressed guard either hard-fails or returns DataFusion's divergent formatting.

/// Scenario: UPPER(c_double) declines pushdown and matches a native DOUBLE oracle (#210)
#[test]
fn e2e_upper_double_declines_to_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let vs_sql = format!(
        "SELECT UPPER(c_double) FROM {} WHERE id = 1",
        vs_typed_table()
    );
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

/// Scenario: UPPER(c_ts) declines pushdown and matches a native oracle cast to the engine's timestamp precision (#210)
#[test]
fn e2e_upper_timestamp_declines_to_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let vs_sql = format!("SELECT UPPER(c_ts) FROM {} WHERE id = 1", vs_typed_table());
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

/// Scenario: UPPER(c_bool) declines pushdown and matches a native BOOLEAN oracle (#210)
#[test]
fn e2e_upper_boolean_declines_to_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let vs_sql = format!(
        "SELECT UPPER(c_bool) FROM {} WHERE id = 1",
        vs_typed_table()
    );
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
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT SUBSTR(name, 1, 5), LEFT(name, 5), SUBSTR(name, 7, 2) FROM {} WHERE id = 1",
        vs_table()
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

/// Scenario: select-list INSTR with a start position declines to native Exasol instead of truncating (#228)
#[test]
fn e2e_instr_arity_decline_selectlist_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let oracle = parse_int(&conn.query_columns("SELECT INSTR('aa', 'a', 2)")[0][0]);
    assert_eq!(
        oracle, 2,
        "native oracle INSTR('aa', 'a', 2) must be 2, got {oracle}"
    );

    let vs_sql = format!(
        "SELECT INSTR(c_varchar, 'a', 2) FROM {} WHERE id = 1",
        vs_typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(vs_cols.len(), 1, "expected 1 column (INSTR): {vs_cols:?}");
    assert_eq!(vs_cols[0].len(), 1, "expected 1 row (id=1): {vs_cols:?}");

    let vs_value = parse_int(&vs_cols[0][0]);
    assert_eq!(
        vs_value, 2,
        "INSTR(c_varchar, 'a', 2) for id=1 (\"aa\") must be 2 (native decline), \
         got {vs_value} (a regressed coerce-not-decline build would return 1)"
    );
}

/// Scenario: WHERE-clause INSTR with a start position declines to native Exasol instead of truncating (#228)
#[test]
fn e2e_instr_arity_decline_where_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let oracle = parse_int(&conn.query_columns("SELECT INSTR('bb', 'b', 3)")[0][0]);
    assert_eq!(
        oracle, 0,
        "native oracle INSTR('bb', 'b', 3) must be 0, got {oracle}"
    );

    let vs_sql = format!(
        "SELECT id FROM {} WHERE INSTR(c_varchar, 'b', 3) = 0",
        vs_typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    assert_eq!(vs_cols.len(), 1, "expected 1 column (id): {vs_cols:?}");

    let ids: Vec<i64> = vs_cols[0].iter().map(parse_int).collect();
    assert!(
        ids.contains(&4),
        "WHERE INSTR(c_varchar, 'b', 3) = 0 must include id=4 (\"bb\", native \
         decline gives 0), got ids={ids:?} (a regressed coerce build would \
         compute strpos('bb','b')=1, never matching id=4)"
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
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {sql}"));
    let result_set = resp["responseData"]["results"][0]["resultSet"].clone();
    conn.fetch_result_columns(&result_set)
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .filter_map(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .flat_map(|v| v.as_array().cloned().unwrap_or_default())
        .find_map(|entry| entry.get("pushdownRequest").cloned())
        .unwrap_or_else(|| panic!("EXPLAIN VIRTUAL carried no echoed pushdownRequest for:\n{sql}"))
}

fn int_set(values: &[serde_json::Value]) -> std::collections::HashSet<i64> {
    values.iter().map(parse_int).collect()
}

/// Scenario: row scan with an expression sort key absent from the select list leaks no HIDDEN_COL and orders correctly (#198)
#[test]
fn e2e_order_by_expression_not_selected_leaks_no_hidden_column() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, c_price FROM {} WHERE id<=5 ORDER BY ABS(c_price) DESC",
        vs_typed_table()
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
        vs_typed_table()
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

/// Scenario: a group-key-only select list ordered by an unselected aggregate with LIMIT returns the correct group set (#198)
#[test]
fn e2e_grouped_order_by_aggregate_not_selected_top_n_groups_limit_applies() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id FROM {} GROUP BY id ORDER BY SUM(c_qty) DESC LIMIT 4",
        vs_typed_table()
    );

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
}

/// Scenario: an unselected aggregate sort key leaks no HIDDEN_COL beside another selected aggregate, and a selected sort key keeps the partial/merge path (#198)
#[test]
fn e2e_grouped_order_by_aggregate_not_selected_leaks_no_hidden_column() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT c_bool, COUNT(*) FROM {} GROUP BY c_bool ORDER BY SUM(c_price) DESC",
        vs_typed_table()
    );

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
        vs_typed_table()
    );

    let pushed2 = explain_virtual_sql(&mut conn, &sql2);
    assert!(
        pushed2.contains("\"group_keys\":["),
        "a sort key matching a select-list aggregate must KEEP the \
         partial/merge path — the scan spec must carry group_keys, not fall \
         back to the raw-row wrapper, got:\n{pushed2}"
    );
    assert!(
        pushed2.contains("ORDER BY SUM(\"PARTIAL_sum_"),
        "the merge ORDER BY must reference the merged partial column, not a \
         base column, got:\n{pushed2}"
    );

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

/// Scenario: a sort expression that is also a select-list item survives under its own name
#[test]
fn e2e_order_by_expression_also_selected_control() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, c_price, ABS(c_price) AS a FROM {} ORDER BY ABS(c_price)",
        vs_typed_table()
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

/// Scenario: multi-COUNT(DISTINCT) with an aggregate ORDER BY routes to the qualified wrapper and returns a correct, leak-free result
#[test]
fn e2e_multi_count_distinct_order_by_expression_renders_on_wrapper() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT c_bool), COUNT(DISTINCT id) FROM {} \
         ORDER BY COUNT(DISTINCT id) DESC",
        vs_typed_table()
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

/// Scenario: LIMIT 0 over a one-row aggregate returns zero rows, truncated by Exasol itself
#[test]
fn e2e_order_by_aggregate_with_limit_zero_returns_no_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*) FROM {} ORDER BY COUNT(*) DESC LIMIT 0",
        vs_typed_table()
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

// Dialect-aware rendering in Exasol-parsed wrapper SQL (#209): pre-fix, DataFusion
// function names reached Exasol and were rejected (42000). The TPC-H-shaped repros are
// retargeted onto `events`/`typed_distinct_probe`, each compared with an in-session
// native oracle.

/// `%.f` accepts any fractional digit count, including none.
fn parse_exasol_timestamp(s: &str) -> chrono::NaiveDateTime {
    let normalized = s.replacen(' ', "T", 1);
    let fmt = "%Y-%m-%dT%H:%M:%S%.f";
    chrono::NaiveDateTime::parse_from_str(&normalized, fmt).unwrap_or_else(|e| {
        panic!(
            "failed to parse Exasol timestamp {s:?} (normalized {normalized:?}) with {fmt:?}: {e}"
        )
    })
}

#[test]
fn parse_exasol_timestamp_accepts_space_and_t_separators_with_or_without_fraction() {
    assert_eq!(
        parse_exasol_timestamp("2026-07-28 18:56:00.581000"),
        chrono::NaiveDate::from_ymd_opt(2026, 7, 28)
            .unwrap()
            .and_hms_micro_opt(18, 56, 0, 581000)
            .unwrap()
    );
    assert_eq!(
        parse_exasol_timestamp("2026-07-28T18:56:00.581"),
        chrono::NaiveDate::from_ymd_opt(2026, 7, 28)
            .unwrap()
            .and_hms_milli_opt(18, 56, 0, 581)
            .unwrap()
    );
    assert_eq!(
        parse_exasol_timestamp("2024-01-01T00:00:00"),
        chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    );
}

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

/// Scenario: COUNT(DISTINCT SIGN(c_price - 3)) compiles and matches the native oracle (#209)
#[test]
fn e2e_count_distinct_sign_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 3,
        "COUNT(DISTINCT SIGN(c_price - 3)) over the VS must be 3 (matching the \
         native oracle), got {vs_value}"
    );
}

/// Scenario: COUNT(DISTINCT YEAR(...)) and COUNT(DISTINCT WEEK(...)) compile and match native oracles (#209)
#[test]
fn e2e_count_distinct_date_field_matches_native_oracle() {
    setup_e2e();
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
        vs_table()
    );
    let vs_year = conn.query_scalar_i64(&year_sql);
    assert_eq!(
        vs_year, oracle_year,
        "COUNT(DISTINCT YEAR(event_date)) over the VS must match the native \
         oracle, got vs={vs_year} oracle={oracle_year}"
    );

    let week_sql = format!(
        "SELECT COUNT(DISTINCT WEEK(event_date)) FROM {}",
        vs_table()
    );
    let vs_week = conn.query_scalar_i64(&week_sql);
    assert_eq!(
        vs_week, oracle_week,
        "COUNT(DISTINCT WEEK(event_date)) over the VS must match the native \
         oracle, got vs={vs_week} oracle={oracle_week}"
    );
}

/// Scenario: COUNT(DISTINCT HOURS_BETWEEN(...)) compiles and matches the native oracle (#209)
#[test]
fn e2e_count_distinct_hours_between_matches_native_oracle() {
    setup_e2e();
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
        vs_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, oracle,
        "COUNT(DISTINCT HOURS_BETWEEN(event_ts, ...)) over the VS must match \
         the native oracle, got vs={vs_value} oracle={oracle}"
    );
}

/// Scenario: COUNT(DISTINCT INSTR(c_varchar, 'a')) still matches the native oracle after the dialect rework (#210)
#[test]
fn e2e_count_distinct_instr_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 3,
        "COUNT(DISTINCT INSTR(c_varchar, 'a')) over the VS must be 3 (matching \
         the native oracle), got {vs_value}"
    );
}

/// Scenario: grouped SIGN(SUM(c_price) - 10) compiles and matches the native oracle (#209)
#[test]
fn e2e_grouped_scalar_over_aggregate_sign_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_map = grouped_by_label(&vs_cols[0], &vs_cols[1], parse_int);
    assert_eq!(
        vs_map, oracle_map,
        "grouped SIGN(SUM(c_price) - 10) over the VS must match the native \
         oracle, got vs={vs_map:?} oracle={oracle_map:?}"
    );
}

/// Scenario: grouped YEAR(MIN(c_ts)) compiles and matches the native oracle, including a NULL group (#209)
#[test]
fn e2e_grouped_scalar_over_aggregate_year_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
    );
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_map = grouped_by_label(&vs_cols[0], &vs_cols[1], parse_int_opt);
    assert_eq!(
        vs_map, oracle_map,
        "grouped YEAR(MIN(c_ts)) over the VS must match the native oracle, \
         got vs={vs_map:?} oracle={oracle_map:?}"
    );
}

/// Scenario: a select-list REGEXP_LIKE inside COUNT(DISTINCT ...) compiles and matches the native oracle (#209)
#[test]
fn e2e_count_distinct_regexp_like_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 2,
        "COUNT(DISTINCT (c_varchar REGEXP_LIKE 'a.*')) over the VS must be 2 \
         (matching the native oracle), got {vs_value}"
    );
}

/// Scenario: a CASE with a TIMESTAMP literal comparison inside COUNT(DISTINCT ...) compiles and matches the native oracle (#209)
#[test]
fn e2e_count_distinct_timestamp_literal_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
    );
    let vs_value = conn.query_scalar_i64(&vs_sql);
    assert_eq!(
        vs_value, 2,
        "COUNT(DISTINCT CASE WHEN c_ts > TIMESTAMP '2020-01-01 00:00:00' ...) \
         over the VS must be 2 (matching the native oracle), got {vs_value}"
    );
}

/// Scenario: a select-list SYSTIMESTAMP is statement-constant across shards and matches Exasol's DBTIMEZONE clock
#[test]
fn e2e_now_family_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    // A UTC DBTIMEZONE would make the offset comparison vacuous.
    let tz_cols = conn.query_columns("SELECT DBTIMEZONE");
    let db_timezone = tz_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("DBTIMEZONE not a string: {:?}", tz_cols[0][0]))
        .to_string();
    assert_ne!(
        db_timezone.to_uppercase(),
        "UTC",
        "DBTIMEZONE must not be UTC, or the offset comparison below is \
         vacuous (a whole-zone-offset regression would be invisible), got \
         {db_timezone:?}"
    );

    let vs_sql = format!("SELECT SYSTIMESTAMP FROM {}", vs_typed_table());
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_values: Vec<&str> = vs_cols[0]
        .iter()
        .map(|v| {
            v.as_str()
                .unwrap_or_else(|| panic!("SYSTIMESTAMP not a string: {v:?}"))
        })
        .collect();
    assert!(
        !vs_values.is_empty(),
        "SELECT SYSTIMESTAMP FROM {} returned no rows",
        vs_typed_table()
    );
    let distinct_vs: std::collections::HashSet<&str> = vs_values.iter().copied().collect();
    assert_eq!(
        distinct_vs.len(),
        1,
        "SELECT SYSTIMESTAMP FROM {} must return exactly one distinct value \
         across all rows (statement-constancy Exasol guarantees); a \
         regressed pushdown of now() into the scan instead returns one \
         value PER SHARD (measured as two over this two-file table \
         pre-withdrawal), got {distinct_vs:?}",
        vs_typed_table()
    );
    let vs_ts = parse_exasol_timestamp(vs_values[0]);

    // Loose tolerance: the statements run at different instants, and the defect is a
    // whole-zone offset of an hour or more. CURRENT_TIMESTAMP is not the probe because its
    // TIMESTAMP WITH LOCAL TIME ZONE type never emits a pushed projection.
    let oracle_cols = conn.query_columns("SELECT SYSTIMESTAMP");
    let oracle_str = oracle_cols[0][0].as_str().unwrap_or_else(|| {
        panic!(
            "native oracle SYSTIMESTAMP not a string: {:?}",
            oracle_cols[0][0]
        )
    });
    let oracle_ts = parse_exasol_timestamp(oracle_str);

    let delta = (vs_ts - oracle_ts).num_seconds().abs();
    assert!(
        delta <= 60,
        "VS SYSTIMESTAMP ({vs_ts}) must be within 60s of the native oracle \
         SYSTIMESTAMP ({oracle_ts}); a regressed build pushing now() \
         UTC-naively into the scan would diverge by a whole zone offset \
         (>= 1h) from Exasol's own {db_timezone}-zoned clock, got \
         delta={delta}s"
    );
}

// Declined-filter self-apply (#279): a WHERE predicate the adapter cannot push into the
// scan must be applied in its own Exasol-dialect SQL, since Exasol never re-applies it.

/// Scenario: a declined SECOND(c_ts, 3) filter is applied in the wrapper WHERE and returns 0 rows
#[test]
fn e2e_declined_filter_second_arity_returns_filtered_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE SECOND(C_TS, 3) > 1",
        vs_typed_table()
    );
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

/// Scenario: a declined LIKE over a DECIMAL column is self-applied and returns ids 1, 5, 7
#[test]
fn e2e_declined_filter_like_on_decimal_returns_filtered_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID",
        vs_typed_table()
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

/// Scenario: a declined three-argument INSTR filter is self-applied instead of truncated to strpos
#[test]
fn e2e_declined_filter_instr_three_arg_returns_filtered_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE INSTR(C_VARCHAR, 'a', 2) = 0 ORDER BY ID",
        vs_typed_table()
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

/// Scenario: a declined filter applies before aggregation, so COUNT(*) is 3, not 12
#[test]
fn e2e_declined_filter_under_aggregate_filters_before_aggregating() {
    setup_e2e();
    let mut conn = exa_conn();

    let count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE C_DECIMAL_A LIKE '1%'",
        vs_typed_table()
    ));
    assert_eq!(
        count, 3,
        "COUNT(*) under a declined WHERE filter must be 3 (ids 1, 5, 7), \
         not 12 — the filter must be applied before aggregating"
    );
}

/// Scenario: a declined filter applies before ORDER BY … LIMIT truncation
#[test]
fn e2e_declined_filter_under_order_by_limit_filters_before_truncating() {
    setup_e2e();
    let mut conn = exa_conn();

    let cols = conn.query_columns(&format!(
        "SELECT ID FROM {} WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID LIMIT 2",
        vs_typed_table()
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

/// Scenario: SELECT * over a declined filter returns the full base row with no column-count error
#[test]
fn e2e_declined_filter_select_star_returns_full_row_shape() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT * FROM {} WHERE SECOND(C_TS, 3) > 1",
        vs_typed_table()
    );
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

/// Scenario: a CAST to HASHTYPE refused by both dialects fails with a clean error that leaks no credentials
#[test]
fn e2e_both_dialects_unrenderable_predicate_errors_without_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT ID FROM {} WHERE CAST(C_VARCHAR AS HASHTYPE) = \
         CAST('00000000000000000000000000000000' AS HASHTYPE)",
        vs_typed_table()
    );
    let resp = conn.try_execute(&sql);
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "a WHERE predicate unrenderable under BOTH dialects (CAST to \
         HASHTYPE) must return a clean adapter error, not rows: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        !msg.contains("minioadmin"),
        "adapter error message must not leak storage credentials: {msg}"
    );
    assert!(
        msg.to_ascii_lowercase().contains("neither dialect"),
        "adapter error must name the both-dialects-declined reason, got: {msg}"
    );
}

// #192: CHAR-declared pushdown shapes.

/// Scenario: equal-length CASE keys, CAST AS CHAR(20) and literal GROUP BY keys return correct rows, VARCHAR control unaffected (#192)
#[test]
fn char_declared_pushdown_shapes_match_native() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CASE WHEN score <= 50.0 THEN 'LOW' ELSE 'BIG' END g, COUNT(*) c \
         FROM {} GROUP BY 1 ORDER BY 1",
        vs_table()
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
        pushed_sql.contains("CHAR(3)") && !pushed_sql.contains("VARCHAR(3)"),
        "expected the pushed SQL for the equal-length CASE GROUP BY key to \
         declare CHAR(3) (not VARCHAR(3)), got: {pushed_sql}"
    );

    let sql = format!(
        "SELECT CAST(name AS CHAR(20)) FROM {} WHERE id = 1",
        vs_table()
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

    // Assert the pushdown carries CHAR(20); a declined pushdown would pass the value checks.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("CHAR(20)") && !pushed_sql.contains("VARCHAR(20)"),
        "expected the pushed SQL for CAST(name AS CHAR(20)) to declare CHAR(20) \
         (not VARCHAR(20)), got: {pushed_sql}"
    );

    let sql = format!("SELECT 'X' g, COUNT(*) c FROM {} GROUP BY 1", vs_table());
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

    let sql = format!("SELECT name, COUNT(*) c FROM {} GROUP BY 1", vs_table());
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

/// Scenario: a CHAR(30) cast blank-pads 'ab' and 'ab   ' into one GROUP BY group
#[test]
fn char_group_key_merges_trailing_space_variants_like_native() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST({CHAR_PAD_COL} AS CHAR(30)) g, COUNT(*) c FROM {} GROUP BY 1",
        vs_char_pad_table()
    );

    // Assert the pushdown carries CHAR(30); native Exasol would merge the same way.
    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed_sql.contains("CHAR(30)") && !pushed_sql.contains("VARCHAR(30)"),
        "expected the pushed SQL to declare the group key CHAR(30) (not \
         VARCHAR(30)), proving the blank-pad merge behavior was actually \
         pushed down: {pushed_sql}"
    );

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 2, "expected 2 columns (g, c): {cols:?}");
    assert_eq!(
        cols[0].len(),
        3,
        "expected 3 groups ({CHAR_PAD_TOTAL_ROWS} rows minus the \
         '{CHAR_PAD_SHORT}'/'{CHAR_PAD_SHORT_TRAILING_SPACE}' merge): {cols:?}"
    );

    let groups: Vec<(String, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(g, c)| {
            (
                g.as_str().unwrap_or_default().trim_end().to_string(),
                parse_int(c),
            )
        })
        .collect();

    let merged = groups
        .iter()
        .find(|(g, _)| g == CHAR_PAD_SHORT)
        .unwrap_or_else(|| panic!("expected a merged group for {CHAR_PAD_SHORT:?}: {groups:?}"));
    assert_eq!(
        merged.1, 2,
        "'{CHAR_PAD_SHORT}' and '{CHAR_PAD_SHORT_TRAILING_SPACE}' must merge into ONE \
         group with count 2: {groups:?}"
    );

    let other = groups
        .iter()
        .find(|(g, _)| g == CHAR_PAD_OTHER)
        .unwrap_or_else(|| panic!("expected a singleton group for '{CHAR_PAD_OTHER}': {groups:?}"));
    assert_eq!(
        other.1, 1,
        "'{CHAR_PAD_OTHER}' must remain its own singleton group: {groups:?}"
    );

    let over_length = groups
        .iter()
        .find(|(g, _)| g == CHAR_PAD_OVER_LENGTH)
        .unwrap_or_else(|| {
            panic!("expected a singleton group for the over-length value: {groups:?}")
        });
    assert_eq!(
        over_length.1, 1,
        "the 25-character over-length value must be its own singleton group \
         under CHAR(30): {groups:?}"
    );
}

/// Scenario: an over-length value under a CHAR(20) group key raises a truncation error (22002) instead of merging
#[test]
fn over_length_char_group_key_raises_truncation_error_like_native() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST({CHAR_PAD_COL} AS CHAR(20)) g, COUNT(*) c FROM {} GROUP BY 1",
        vs_char_pad_table()
    );

    // Assert the pushdown carries the CHAR declaration; native Exasol raises the same error class.
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains("CHAR(20)") && !pushed.contains("VARCHAR(20)"),
        "expected the pushed SQL to declare CHAR(20) (not VARCHAR(20)), got: {pushed}"
    );

    let resp = conn.try_execute(&sql);

    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "GROUP BY on a CHAR(20) key must fail for the over-length 25-character \
         value rather than silently truncate it, got: {resp}"
    );

    let sql_code = resp["exception"]["sqlCode"].as_str().unwrap_or_default();
    let message = resp["exception"]["text"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        sql_code.contains("22002"),
        "expected Exasol's UDF-emit truncation error (sqlCode 22002, \"string \
         data, right truncation\"), got sqlCode={sql_code:?} message={message:?}: {resp}"
    );
}

/// Scenario: an over-length value projected through CAST AS CHAR(20) fails with a truncation error at the emit boundary
#[test]
fn over_length_char_projection_fails_cleanly() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT CAST({CHAR_PAD_COL} AS CHAR(20)) FROM {}",
        vs_char_pad_table()
    );

    // Assert the pushdown carries the CHAR declaration; native Exasol raises the same error class.
    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains("CHAR(20)") && !pushed.contains("VARCHAR(20)"),
        "expected the pushed SQL to declare CHAR(20) (not VARCHAR(20)), got: {pushed}"
    );

    let resp = conn.try_execute(&sql);

    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "projecting CAST(val AS CHAR(20)) over the 25-character over-length \
         value must fail rather than silently truncate, got: {resp}"
    );

    let sql_code = resp["exception"]["sqlCode"].as_str().unwrap_or_default();
    let message = resp["exception"]["text"]
        .as_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        sql_code.contains("22002"),
        "expected Exasol's UDF-emit truncation error (sqlCode 22002, \"string \
         data, right truncation\"), got sqlCode={sql_code:?} message={message:?}: {resp}"
    );
}

/// Scenario: C_DECIMAL_A/7 pushes down as full float division, matching the native oracle (#186)
#[test]
fn e2e_float_div_decimal_over_int_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let oracle_cols = conn.query_columns("SELECT CAST(40.99 AS DECIMAL(9,2)) / 7");
    let oracle_value = parse_numeric(&oracle_cols[0][0]);
    let expected = 5.855714285714286;
    assert!(
        (oracle_value - expected).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * expected.abs(),
        "native oracle 40.99/7 must be ~5.855714285714286, got {oracle_value}"
    );

    let vs_sql = format!("SELECT C_DECIMAL_A/7 FROM {} WHERE ID=6", vs_typed_table());
    let vs_cols = conn.query_columns(&vs_sql);
    let vs_value = parse_numeric(&vs_cols[0][0]);
    assert!(
        (vs_value - oracle_value).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * oracle_value.abs(),
        "pushed-down C_DECIMAL_A/7 at id=6 must match the native oracle \
         {oracle_value} (decimal/int FLOAT_DIV must not truncate to scale 6), \
         got {vs_value}"
    );
}

/// Scenario: C_DECIMAL_A/C_DECIMAL_B pushes down as full float division, matching the native oracle (#186)
#[test]
fn e2e_float_div_decimal_over_decimal_matches_native_oracle() {
    setup_e2e();
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
        vs_typed_table()
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
