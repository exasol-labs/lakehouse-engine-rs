//! End-to-end capability-alignment tests for the lakehouse-engine Virtual Schema.
//!
//! Exercises the full advertised → translated → executed path for each newly
//! advertised capability group: math/string/date scalar functions in filters,
//! REGEXP_LIKE, scalar select-list expressions, HAVING, STDDEV/VARIANCE, and
//! CAST / unary-minus (NEG) / WEEK (#104, #105, #107).
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
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_NAMESPACE, E2E_TABLE, E2E_TYPED_TABLE, ExpectedValue,
    seed_events, seed_typed_distinct_probe,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Constants (mirror e2e_scan_test.rs — same stack, same VS)
// ---------------------------------------------------------------------------

const VS_NAME: &str = "MY_LAKEHOUSE";

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
                .expect("seed Iceberg events table");
            // Additive: `typed_distinct_probe` seeds into the SAME namespace
            // (E2E_NAMESPACE) as `events`, so it becomes queryable through this
            // file's single `MY_LAKEHOUSE` virtual schema below without a second
            // VS. Used only by the #211 decimal-string-trimming regression
            // tests further down this file.
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
// 5.2  COUNT(DISTINCT) aggregate-pushdown capability advertisement (live round-trip)
// ---------------------------------------------------------------------------

/// A live `getCapabilities` round-trip against the running VS advertises
/// `FN_AGG_COUNT_DISTINCT` — single-group `COUNT(DISTINCT col)` pushdown
/// (issue #56, revisited by fix-count-distinct-shard-cap).
///
/// Mirrors `e2e_advertises_inner_equi_join_capability`: `EXPLAIN VIRTUAL` of a
/// query over the virtual schema drives Exasol's planner through
/// `getCapabilities`, and its output echoes the adapter's capability response
/// verbatim. Asserting `FN_AGG_COUNT_DISTINCT` is present in that live
/// response — rather than only against the in-process `CAPABILITIES` constant
/// (`capabilities_advertise_count_distinct` in
/// `src/adapter/capabilities.rs`) — proves the deployed `.so` advertises it
/// end to end. `AGGREGATE_SINGLE_GROUP` must also be present since
/// single-group `COUNT(DISTINCT)` pushdown depends on it.
#[test]
fn advertises_count_distinct_capability() {
    setup_e2e();
    let mut conn = exa_conn();

    // A COUNT(DISTINCT) query guarantees the capability is exercised in
    // planning; the capability list itself is echoed regardless of the query
    // shape.
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

/// True if `"projection"` is followed somewhere later by an `"expr"` key —
/// the signal that the pushed scan spec's projection carries a positional
/// `Expr` item, not the full-base-row fallback (bare `Column` string
/// entries, no `"expr"` key at all). Shared by every test in this file that
/// needs this check — `explain_virtual_sql` flattens EXPLAIN's result cells
/// by joining them with a single space, so JSON tokens can end up split
/// across a cell boundary; an exact adjacent-substring match would be flaky.
fn has_expr_after_projection(pushed_sql: &str) -> bool {
    pushed_sql
        .find(r#""projection""#)
        .and_then(|idx| pushed_sql[idx..].find(r#""expr""#))
        .is_some()
}

/// Regression for issue #196: `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, and
/// `<>` select-list items must push down as a positional expression, not
/// widen the derived projection to the full base row. Each predicate runs
/// independently, over `EVENTS` (`vs_table()`) for the id-based predicates
/// and `typed_distinct_probe` (`vs_typed_table()`) for the NULL-column
/// predicates, and asserts BOTH the returned boolean values (computed from
/// the real seeded rows) AND — via `has_expr_after_projection` — that the
/// scan spec carries a positional `Expr` projection for the predicate item.
#[test]
fn e2e_selectlist_predicate_projection_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();

    // IN (...): `id IN (1,2,3)` over ids 1..5.
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

    // BETWEEN ... AND ...: `id BETWEEN 2 AND 4` over ids 1..5.
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

    // IS NULL: `c_decimal_a IS NULL` over typed_distinct_probe ids 1..4
    // (id=3 is the only NULL c_decimal_a among these — see
    // `common/seed.rs`'s `typed_probe()`, reproduced below as
    // `TYPED_DECIMAL_A_UNSCALED`).
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

    // IS NOT NULL: same column, negated.
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

    // <> (not-equal): `id <> 3` over ids 1..5.
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

/// One row of `typed_distinct_probe` (`id`, `c_decimal_b` unscaled scale-4,
/// `c_double`, `c_varchar`, `c_bool`, `c_price`, `c_qty`). `c_decimal_a` is
/// deliberately absent: it's already tracked for all 12 rows by
/// `TYPED_DECIMAL_A_UNSCALED` (section 8.14, below), so `assert_typed_probe_prefix`
/// looks it up from there instead of re-encoding it here. `c_qty` is never
/// NULL; every other field mirrors its column's nullability.
struct TypedProbeRow {
    id: i64,
    decimal_b: Option<i128>,
    double: Option<f64>,
    varchar: Option<&'static str>,
    boolean: Option<bool>,
    price: Option<f64>,
    qty: i64,
}

/// Rows 1..=3 of `typed_distinct_probe`, copied from `common/seed.rs`'s
/// `typed_probe()` (see its module doc for the full 12-row fixture). Row 3
/// (index 2) is NULL in every optional column; `c_qty` is never NULL.
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

/// Assert columns 0..=8 (`id`, `c_decimal_a`, `c_decimal_b`, `c_double`,
/// `c_varchar`, `c_date`, `c_ts`, `c_bool`, `c_price`) of row `i` against
/// `TYPED_ROWS_1_TO_3` (plus `TYPED_DECIMAL_A_UNSCALED` for `c_decimal_a`,
/// looked up by `id`). `c_date`/`c_ts` are checked only for NULL-ness (their
/// exact rendering is covered elsewhere, sections 8.14/8.16); every other
/// nullable column is checked against its real seeded value. Shared by the
/// two coincidental-arity widening tests below, which both project these
/// same nine columns and vary only the tenth (widening) item.
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
    // Coupling note: `c_date`/`c_ts` are checked only for NULL-ness here, using
    // `decimal_a` as the oracle rather than their own real values — this is
    // correct only because row 3 (id=3) is NULL in every optional column of
    // `typed_probe()`'s seed data (see `common/seed.rs`), so `decimal_a`'s
    // null flag happens to double as the oracle for every other nullable
    // column too, including these two. A future seed-data edit that breaks
    // this coupling (a row NULL in `c_decimal_a` but not in `c_date`/`c_ts`,
    // or vice versa) would silently invalidate these two assertions.
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

/// Arity-coincidence repro for issue #196/#234: a 10-item select list over
/// `typed_distinct_probe` (10 columns) whose LAST item is `(c_qty BETWEEN 1
/// AND 3)`. Pre-hardening, the dispatcher's arity-based safety net could not
/// distinguish a widened 10-column projection from a genuine 10-item
/// non-widened select list, and Exasol rejected the mismatched EMITS type at
/// position 10 with `sqlCode 04000` ("Data type mismatch in column number
/// 10 ... Expected BOOLEAN, but got DECIMAL(20,0)"). Task 1 of this plan
/// whitelisted `predicate_between` as a pushable select-list item kind (see
/// `selectlist_between_projects_as_expr` in
/// `crates/lakehouse-engine/src/adapter/pushdown/support.rs`), so this item
/// now projects as a positional `Expr` on the ORDINARY scan path — the
/// projection is never widened, and the query never reaches
/// `qualified_single_table_fallback_pushdown`. That non-widening is exactly
/// what makes the 10-item/10-column arity coincidence harmless here: with no
/// widening, there is nothing for a coincidence to mask. This asserts both
/// the 10 correct columns AND (via `has_expr_after_projection`) the
/// positional-`Expr` projection shape that proves no widening occurred.
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

/// A DIFFERENT widening trigger at the same coincidental arity, proving the
/// hardened routing (#196/#234) is not predicate-specific: `LENGTH(c_double)`
/// widens because issue #210's string-function argument-type guard declines
/// a DOUBLE argument to `LENGTH` (a string function), not because of the
/// predicate whitelist task 1 of this plan extended. Also covers the (#234)
/// shape as a variant: the same widening (`LENGTH(score)`, DOUBLE argument)
/// plus a trailing `ORDER BY id`, over a table (`EVENTS`, 5 columns) whose
/// column count DIFFERS from the select-list arity (10) — the pre-existing
/// arity-mismatch routing task 2.4 of this plan confirmed already includes
/// `ORDER BY` columns.
#[test]
fn e2e_widened_projection_with_declined_order_by_routes_to_wrapper() {
    setup_e2e();
    let mut conn = exa_conn();

    // Base shape: matching arity (10 items, 10 real columns). `ORDER BY id`
    // is only for this test's own deterministic row order, not the (#234)
    // trigger — that variant follows below.
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

            // LENGTH(c_double)'s implicit DOUBLE-to-VARCHAR rendering is
            // Exasol-implementation-defined, so — matching section 8.16's
            // convention — the expected value comes from Exasol's own
            // in-session native oracle, not a hand-computed string length.
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

    // (#234) variant: same widening (`LENGTH(score)`, DOUBLE argument
    // declined by #210's guard) plus a trailing `ORDER BY id`, over `EVENTS`
    // (5 real columns) — a select-list arity (10) that DIFFERS from the
    // table's column count, the shape #234 originally reported.
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

/// Regression for issue #190: a projected constant/literal select-list item
/// must push down without collapsing to the full base-table row. Runs three
/// literal-projection shapes over the full 20-row EVENTS table (no WHERE)
/// and asserts both the emitted column arity and the constant value in every
/// literal position — including BOTH positions of a duplicated literal,
/// which pre-fix collapsed to arity 2 (value-based dedup) or failed with a
/// column-count error (full-row fallback).
#[test]
fn e2e_selectlist_literal_projection_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table();

    // `SELECT 1 FROM <t>`: single literal column, all 20 rows, every value 1.
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

    // `SELECT 1, name FROM <t>`: literal + real column, arity 2.
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

    // `SELECT 1, name, 1 FROM <t>`: duplicated literal — the arity-3 regression
    // case for issue #190. Both literal positions (0 and 2) must independently
    // carry the constant 1; a pre-fix value-based dedup would have collapsed
    // this to arity 2, and the full-row fallback would have errored on arity.
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

/// Regression for issue #205: `COUNT(*)` over a `LIMIT`-bearing derived table
/// must not fail with a pushdown column-count mismatch. Exasol represents its
/// "select any one column" contract for the inner derived-table request as a
/// single-element `literal_null` select list once a LIMIT barrier separates
/// the outer aggregate from the derived table — the same literal-select-list
/// code path #190 fixed (`is_literal_selectlist_item` in
/// `crates/lakehouse-engine/src/adapter/pushdown/support.rs`), so this guards
/// that fix against a regression on the LIMIT-barrier trigger surface.
#[test]
fn e2e_count_star_over_limited_subselect_pushdown() {
    setup_e2e();
    let mut conn = exa_conn();
    let table = vs_table();

    // Primary shape: single projected column behind the LIMIT barrier.
    let primary_sql = format!("SELECT COUNT(*) FROM (SELECT id FROM {table} LIMIT 5)");
    assert_eq!(
        conn.query_scalar_i64(&primary_sql),
        5,
        "COUNT(*) over a single-column LIMITed derived table must be 5: {primary_sql}"
    );

    // Two-column subselect variant.
    let two_col_sql = format!("SELECT COUNT(*) FROM (SELECT id, name FROM {table} LIMIT 5)");
    assert_eq!(
        conn.query_scalar_i64(&two_col_sql),
        5,
        "COUNT(*) over a two-column LIMITed derived table must be 5: {two_col_sql}"
    );

    // WHERE + LIMIT variant.
    let where_limit_sql =
        format!("SELECT COUNT(*) FROM (SELECT id FROM {table} WHERE id <= 10 LIMIT 5)");
    assert_eq!(
        conn.query_scalar_i64(&where_limit_sql),
        5,
        "COUNT(*) over a WHERE+LIMITed derived table must be 5: {where_limit_sql}"
    );

    // Guard the pushdown shape itself, not only the numeric result: the inner
    // derived-table scan for the primary shape must push a positional literal
    // projection, not the full-base-row fallback (which would emit every
    // base column and yield the #205 column-count mismatch). See
    // `has_expr_after_projection`'s doc comment for why this is a substring
    // check rather than an exact match.
    let pushed_sql = explain_virtual_sql(&mut conn, &primary_sql);
    assert!(
        has_expr_after_projection(&pushed_sql),
        "{primary_sql}'s inner derived-table scan must push a positional \
         literal projection (an '\"expr\"' key after 'projection'), \
         not the full-base-row fallback (#205), got:\n{pushed_sql}"
    );
}

/// Regression for issue #190: a query whose predicate prunes every Iceberg
/// data file (`id > 1000` against a max seeded id of 20) must still accept a
/// select list of repeated literals plus a real column. This exercises the
/// `empty_pushdown_sql` path with positional-unique synthetic EMITS aliases
/// for the two `1` literals — proving Exasol accepts the zero-row shape
/// rather than rejecting it on a duplicate-alias or arity error.
#[test]
fn e2e_all_files_pruned_literal_projection_empty_shape() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT 1, name, 1 FROM {} WHERE id > 1000", vs_table());
    // `execute` panics if Exasol rejects the pushdown (a duplicate EMITS alias
    // or a column-count mismatch), so reaching the assertions already proves
    // Exasol accepted the empty-pushdown shape. Assert on the resultSet
    // METADATA, not `query_columns`: that helper returns an empty vec for any
    // zero-row result, so it cannot observe the column count of zero rows.
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

// ---------------------------------------------------------------------------
// 8.9  Filter-pushdown alignment helper (CAST / NEG / WEEK)
// ---------------------------------------------------------------------------

/// Asserts the pushed scan spec carries a non-empty `filter` field — proof
/// the WHERE predicate was translated and pushed into the DataFusion scan
/// (`CommonScanSpec::filter`), rather than falling back to Exasol evaluating
/// the whole WHERE clause itself over an unfiltered raw-row scan (which would
/// omit the `filter` field entirely: it is `#[serde(skip_serializing_if =
/// "Option::is_none")]`).
///
/// Each caller below uses a query whose WHERE clause is a single CAST / NEG /
/// WEEK expression, so field presence alone attributes the pushdown to that
/// expression: if its translation had declined, the whole top-level filter
/// would be dropped (there is nothing else in the clause to push instead).
fn assert_filter_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let pushed_sql = explain_virtual_sql(conn, query_sql);
    assert!(
        pushed_sql.contains("\"filter\":\""),
        "EXPLAIN VIRTUAL output must contain a non-empty 'filter' field in the \
         scan spec (predicate pushdown occurred), not a raw row-scan fallback, \
         got:\n{pushed_sql}"
    );
}

// ---------------------------------------------------------------------------
// 8.10  CAST in a WHERE filter (#104)
// ---------------------------------------------------------------------------

/// `CAST(id AS VARCHAR(2000000))` in a WHERE filter pushes down and returns
/// the correct row.
///
/// `id` is DECIMAL(20,0) in Exasol; `CAST(id AS VARCHAR(2000000)) = '15'`
/// matches only id=15 (score = 5.0*15 = 75.0). Exasol's CAST grammar requires
/// an explicit length for VARCHAR (this project's own data-type convention:
/// VARCHAR(n≤2,000,000)); the DataFusion-facing translation in
/// `render_cast_target` ignores the length and always renders the bare
/// DataFusion `VARCHAR` type, so this is purely an Exasol-facing SQL detail,
/// not a translator concern.
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

// ---------------------------------------------------------------------------
// 8.11  Unary minus (NEG) in a WHERE filter (#105)
// ---------------------------------------------------------------------------

/// Unary minus in a WHERE filter pushes down and returns the correct rows.
///
/// `-score < -50.0` is equivalent to `score > 50.0`. Scores are 5.0*id for
/// id=1..20 (5,10,…,100), so score > 50.0 -> id > 10 -> ids 11..20 -> 10 rows.
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

    // ids 11..20 inclusive -> 10 rows.
    let expected_count = 10i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "-score < -50.0 must return {expected_count} rows, got {}",
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

    // Every returned score must satisfy score > 50.0.
    for v in &cols[1] {
        let s = parse_numeric(v);
        assert!(s > 50.0, "filter violated: score {s} must be > 50.0");
    }
}

// ---------------------------------------------------------------------------
// 8.12  WEEK in a WHERE filter (#107)
// ---------------------------------------------------------------------------

/// `WEEK(event_date)` in a WHERE filter pushes down and returns the correct
/// ISO-8601 week's rows.
///
/// Seed dates: `event_date` = 2024-01-01 + (id-1) days, so day-of-month = id
/// for every row (id 1..20, all January 2024). 2024-01-01 is a Monday, so
/// ISO-8601 week 1 = Jan 1..7 (id 1..7), week 2 = Jan 8..14 (id 8..14), week 3
/// = Jan 15..21 (id 15..20, truncated at the seed's last row). Verified with
/// `date -d 2024-01-08 +%V` = 02 and `date -d 2024-01-14 +%V` = 02 (both
/// Monday-start ISO week 2). `WEEK(event_date) = 2` -> id 8..14 -> 7 rows.
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

    // id 8..14 -> 7 rows.
    let expected_count = 7i64;
    assert_eq!(
        cols[0].len() as i64,
        expected_count,
        "WEEK(event_date) = 2 must return {expected_count} rows, got {}",
        cols[0].len()
    );

    // IDs must be 8..14 in order.
    let ids: Vec<i64> = cols[0].iter().map(parse_int).collect();
    for (pos, &id) in ids.iter().enumerate() {
        let expected = 8 + pos as i64;
        assert_eq!(
            id, expected,
            "id at position {pos} must be {expected}, got {id}"
        );
    }
}

// ---------------------------------------------------------------------------
// 8.12b  Date-difference pushdown parity (#107, task 3.1)
// ---------------------------------------------------------------------------
//
// Only the four *_BETWEEN functions are advertised. ADD_HOURS/ADD_MINUTES were
// WITHDRAWN during this task: the microsecond round-trip renders a fixed
// TIMESTAMP(3), but Exasol infers TIMESTAMP(0) for a DATE argument, so Exasol
// rejects the pushdown of ADD_HOURS(<date>, n) ("Data type mismatch ... Expected
// TIMESTAMP(0), but got TIMESTAMP(3)"). A type-blind string translator cannot
// vary the result precision by argument type — see the plan's disposition table.

/// Run `EXPLAIN VIRTUAL <query_sql>` and assert the pushed SQL contains
/// `fragment`, proving the projection expression pushed down (rather than
/// falling back to a raw column scan).
fn assert_select_pushed_down(conn: &mut ExaConn, query_sql: &str, fragment: &str) {
    let pushed = explain_virtual_sql(conn, query_sql);
    assert!(
        pushed.contains(fragment),
        "expected pushed SQL to contain {fragment:?}, got: {pushed}"
    );
}

/// `DAYS_BETWEEN` pushes down as an `Int64` whole-day date difference and
/// preserves Exasol's sign convention: first argument earlier than the second
/// yields a NEGATIVE result (task 3.1 case e).
///
/// Seed: `event_date` = 2024-01-01 + (id-1) days. id=1 → 2024-01-01, so
/// `DAYS_BETWEEN(event_date, DATE '2024-01-10')` = 2024-01-01 − 2024-01-10 = −9.
/// Verified against native Exasol: `DAYS_BETWEEN(DATE '2024-01-01', DATE
/// '2024-01-10')` = −9.
#[test]
fn e2e_days_between_matches_exasol() {
    setup_e2e();
    let mut conn = exa_conn();
    let t = vs_table();

    let sql = format!("SELECT DAYS_BETWEEN(event_date, DATE '2024-01-10') FROM {t} WHERE id = 1");
    // "AS DATE" alone can also match Exasol's echoed pushdownRequest (e.g. a DATE
    // literal/cast in the original query), so anchor on "- CAST(" instead — that
    // sequence only appears in the adapter's own `(CAST(.. AS DATE) - CAST(.. AS
    // DATE))` rendering, never in the echoed request.
    assert_select_pushed_down(&mut conn, &sql, "- CAST(");

    let cols = conn.query_columns(&sql);
    let value = parse_int(&cols[0][0]);
    assert_eq!(
        value, -9,
        "DAYS_BETWEEN(2024-01-01, 2024-01-10) must be -9 (Exasol sign convention), got {value}"
    );
}

/// `HOURS_BETWEEN`, `MINUTES_BETWEEN`, and `SECONDS_BETWEEN` push down as
/// fractional epoch-second differences and match Exasol's fractional values
/// (task 3.1 case d — fractional 2.5-hour gap).
///
/// Seed: `event_ts` = 2024-01-01T00:00:00 + (id-1) hours. id=6 → 05:00:00.
/// Against the fixed anchor 02:30:00 the gap is exactly 2.5 hours =
/// 150 minutes = 9000 seconds. Native Exasol confirms `HOURS_BETWEEN` over a
/// 2.5-hour gap = 2.5.
#[test]
fn e2e_time_between_matches_exasol() {
    setup_e2e();
    let mut conn = exa_conn();
    let t = vs_table();

    let anchor = "TIMESTAMP '2024-01-01 02:30:00'";

    let hours_sql = format!("SELECT HOURS_BETWEEN(event_ts, {anchor}) FROM {t} WHERE id = 6");
    // The projected expression now lands in the scan spec's JSON `projection`
    // field, which is embedded as the single-quoted `LAKEHOUSE_SCAN('…')`
    // argument, so its single quotes are doubled by SQL-string escaping
    // (`date_part('epoch'` → `date_part(''epoch''`). Before the positional
    // EMITS-naming change (#190) the expression also appeared verbatim as the
    // EMITS identifier; it no longer does — the EMITS name is now `_LH_PROJ_0`.
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

// ---------------------------------------------------------------------------
// 8.13  CAST / EXTRACT / CASE together in the SELECT list (#136)
// ---------------------------------------------------------------------------

/// `CAST`, `EXTRACT`, and `CASE` in the SELECT list all push down together
/// and return correct evaluated values, with no "Expected number of columns"
/// error.
///
/// Regression test for #136: a CAST in a virtual-schema SELECT list broke
/// pushdown with a column-count mismatch. The root cause was that
/// `project_columns` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`)
/// did not dispatch `function_scalar_cast` — nor, by the same gap,
/// `function_scalar_extract` and `function_scalar_case` — into
/// `render_expression_safe`, so a SELECT-list item using CAST/EXTRACT/CASE
/// fell back to projecting the full base row instead of the single evaluated
/// expression column, producing a column-count mismatch against the
/// advertised select list (`query_columns` panics on any adapter error, so
/// this test failing to run at all would itself reproduce #136). All three
/// functions are selected together to prove the fix covers all three
/// dispatch gaps, not only CAST.
///
/// Seed: id 1..20, event_date = 2024-01-01 + (id-1) days (all January 2024).
/// For id <= 3:
///   CAST(id AS VARCHAR(2000000)) = "1", "2", "3"
///   EXTRACT(YEAR FROM event_date) = 2024 for every row
///   CASE WHEN id > 10 THEN 'high' ELSE 'low' END = 'low' for every row (id <= 3)
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

    // Verify CAST(id AS VARCHAR(2000000)) for each id.
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

    // Verify EXTRACT(YEAR FROM event_date) is 2024 for every row.
    for (i, v) in cols[2].iter().enumerate() {
        let year = parse_int(v);
        assert_eq!(
            year, 2024,
            "row {i}: EXTRACT(YEAR FROM event_date) must be 2024, got {year}"
        );
    }

    // Verify CASE WHEN id > 10 THEN 'high' ELSE 'low' END is 'low' for id <= 3.
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

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// 8.12  ORDER BY on a column outside the select list (#225)
// ---------------------------------------------------------------------------

/// Regression test for #225: `ORDER BY <col>` must push down correctly even
/// when `<col>` is not itself a projected select-list item.
///
/// Pre-fix, the adapter widened the projection to the FULL base row whenever a
/// pushed sort key was not a bare projected column, so the returned pushdown
/// query's column count no longer matched the (unwidened) select list —
/// Exasol rejects that positionally with `sqlCode 04000 "Expected number of
/// columns is N but pushdown query has M"`. Post-fix, the sort key is appended
/// as a HIDDEN extra scan/EMITS column and the declined-ORDER-BY wrapper
/// selects only the ORIGINAL select-list items, so the returned arity always
/// matches.
///
/// Case 1 is issue #225's own literal repro: `id` drives the ORDER BY but is
/// not selected at all. Case 2 proves the hidden sort column actually DRIVES
/// the ordering (not just that the query no longer errors) by sorting
/// DESCENDING on `id` while selecting only `name`, and checking the returned
/// row order.
///
/// Seed: id 1..20, score = 5.0 * id, name = "event-NN" (`common/seed.rs`).
#[test]
fn e2e_order_by_unprojected_column_bare_projection() {
    setup_e2e();
    let mut conn = exa_conn();

    // Case 1: #225's literal repro.
    let sql = format!("SELECT score FROM {} WHERE id = 1 ORDER BY id", vs_table());

    // Task 3.3: the pushed scan spec must carry the hidden sort key `ID` as an
    // extra projection column, and the row-scan fan-out must NOT have widened
    // to a full-base-row `SELECT * FROM (...)`. Scoped to the adapter's OWN
    // emitted `"projection":[...]` scan-spec JSON array, not the whole SQL
    // string (the EVENTS Iceberg schema's field names are lowercase, so an
    // uppercase whole-string check like `!contains("EVENT_DATE")` would pass
    // by casing accident rather than by actually proving the projection is
    // narrow).
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

    // Case 2: prove the hidden sort column actually drives the ordering (and
    // is dropped from the visible result) by sorting DESCENDING on `id` while
    // selecting only `name`.
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

// ---------------------------------------------------------------------------
// 8.13  ORDER BY on a column referenced only inside a projected expression (#225)
// ---------------------------------------------------------------------------

/// Regression test for #225's OTHER repro shape: the ORDER BY sort key is
/// referenced only INSIDE a computed select-list expression, never bare
/// projected — exercising the fix via a `ProjectionItem::Expr` instead of a
/// bare `ProjectionItem::Column`.
///
/// `id || '-' || name` pushes down as ONE `Expr` select-list item
/// (`FN_CONCAT` is advertised, `adapter/capabilities.rs:87`, and the
/// translator renders it as DataFusion `concat(...)`,
/// `vs-expression/src/lib.rs:632`), so `id` never appears as a bare projected
/// column even though it also drives the ORDER BY.
///
/// Pre-check (done live against the `typed_distinct_probe` seed table via
/// `scripts/capture-pushdown-payload.sh` before writing this test): concat of
/// an Int64 column (`ID`) with a VARCHAR literal executes correctly end to
/// end (`"1-aa"`, `"2-AA"`, `"3-"` for `ID || '-' || C_VARCHAR`) — DataFusion
/// 54.1's `concat()` coerces the non-string argument cleanly, unlike the
/// LIKE-pushdown type-coercion bug fixed in `a6e829e`. No `CAST(id AS
/// VARCHAR)` fallback is needed; the literal `||` repro is used as specified.
///
/// Seed: id 1..20, name = "event-NN" (`common/seed.rs`).
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

// ---------------------------------------------------------------------------
// 8.14  DECIMAL→string trailing-zero trimming (#211)
// ---------------------------------------------------------------------------
//
// Exasol's own DECIMAL→VARCHAR conversion trims trailing scale zeros — and
// drops the decimal point entirely when the fractional part is all zeros:
// `30.00` becomes `"30"`, not `"30.00"`. Before the #211 fix, `CAST`, `||`
// (CONCAT), and `LENGTH` over a DECIMAL column all silently rendered the
// fixed-scale (untrimmed) string instead, producing silently-wrong results
// rather than an error. This session's STATUS.md documents the live-captured
// pre-fix values: `CAST(c_decimal_a AS VARCHAR(20))` returned `"10.50"` /
// `"30.00"` / `"40.99"` (should be `"10.5"` / `"30"` / `"40.99"`),
// `id||'-'||c_decimal_a` returned `"1-10.50"` / `"4-30.00"` (should be
// `"1-10.5"` / `"4-30"`), and `LENGTH(c_decimal_a)` returned a uniform `5` for
// every row instead of the correct 2/4/5 mix.
//
// Uses `typed_distinct_probe`'s `c_decimal_a` column (Exasol `DECIMAL(9,2)`,
// 12 rows, ids 3 and 10 NULL — see `common/seed.rs`'s `typed_probe()`). The
// raw unscaled values are reproduced below (the seed module keeps
// `typed_probe()` private) so every expected string/length in this section is
// computed independently in Rust from those raw values, never hand-guessed —
// and never by calling the production `format_decimal_exasol_style` helper,
// so this stays an independent oracle rather than a tautology check.

/// `(id, unscaled_value)` for `typed_distinct_probe.c_decimal_a` (scale 2),
/// copied from `common/seed.rs`'s `typed_probe().decimal_a`.
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

/// Independently reproduce Exasol's DECIMAL→VARCHAR trimming rule from a raw
/// unscaled value + scale: render the full fixed-scale digit string, then
/// trim trailing fractional zeros, dropping the decimal point too if the
/// whole fraction is zero. This is a from-scratch implementation (not a call
/// into `vs-expression`'s `format_decimal_exasol_style`), so it serves as this
/// test's own expected-value oracle.
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

/// `exasol_trim_decimal_string` matches the documented Exasol trimming rule
/// for every `c_decimal_a` value used by this section, pinning the oracle
/// itself against the values this session live-captured (STATUS.md) before
/// it is used to derive further expected results.
#[test]
fn exasol_trim_decimal_string_matches_documented_values() {
    assert_eq!(exasol_trim_decimal_string(1050, 2), "10.5");
    assert_eq!(exasol_trim_decimal_string(2025, 2), "20.25");
    assert_eq!(exasol_trim_decimal_string(3000, 2), "30");
    assert_eq!(exasol_trim_decimal_string(4099, 2), "40.99");
    assert_eq!(exasol_trim_decimal_string(5000, 2), "50");
    assert_eq!(exasol_trim_decimal_string(6000, 2), "60");
}

/// Explicit `CAST(c_decimal_a AS VARCHAR(20))` trims trailing scale zeros the
/// way native Exasol does (#211).
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

// ---------------------------------------------------------------------------
// 8.15  Issue #189 cross-verification (same root cause as #225)
// ---------------------------------------------------------------------------

/// Issue #189 ("ORDER BY on a non-projected column generates invalid pushdown
/// (column not found)") reported the shape-equivalent live repro
/// `SELECT c_acctbal FROM CUSTOMER WHERE c_custkey <= 5 ORDER BY c_custkey`
/// against a remote Databricks-backed TPC-H `CUSTOMER` table — not reproducible
/// on this local stack, which has no `CUSTOMER`/`c_acctbal` table. This test
/// verifies the SAME root-cause shape against the locally seeded `dim_customer`
/// table instead: a projected column (`c_name`) with an `ORDER BY` on a
/// DIFFERENT, unprojected column (`c_custkey`).
///
/// Seed: `dim_customer` has 5 rows, `c_custkey` 1..=5, `c_name` =
/// "customer-01".."customer-05" (`common/seed.rs::make_customer_batch`).
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

/// Implicit CONCAT (`||`) over a DECIMAL operand trims trailing scale zeros
/// the way native Exasol does (#211).
///
/// `id` is projected alongside the CONCAT expression (not only embedded
/// inside it) — an unrelated pre-existing pushdown limitation rejects `ORDER
/// BY <col>` when `<col>` is not itself a top-level SELECT-list item (even
/// when referenced inside another projected expression), reproducible on the
/// baseline `events` table with a bare column and no decimal/CONCAT
/// involved. Projecting `id` directly sidesteps that unrelated limitation so
/// this test isolates the #211 CONCAT-trimming behavior only.
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

/// Implicit `LENGTH(c_decimal_a)` reflects the TRIMMED string's length, not
/// the fixed-scale (untrimmed) string's length (#211).
///
/// `id` is projected alongside `LENGTH(...)` for the same reason as
/// `e2e_decimal_concat_trims_trailing_zeros` above: `ORDER BY id` requires
/// `id` to be a top-level SELECT-list item, an unrelated pre-existing
/// pushdown limitation orthogonal to #211.
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

    // "10.5"=4, "30"=2, "40.99"=5. Pre-fix code returned 5 for all three
    // (untrimmed "10.50" / "30.00" / "40.99" are all 5 characters).
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

/// The headline #211 repro: `COUNT(*) FROM ... WHERE LENGTH(c_decimal_a) > N`
/// must match native Exasol's own trimmed-string `LENGTH` semantics, not the
/// untrimmed fixed-scale string's length. Also independently verifies every
/// row's `LENGTH(c_decimal_a)` against a Rust-computed trimmed length, proving
/// the per-row divergence mechanism (not just the final aggregate number) is
/// fixed.
#[test]
fn e2e_decimal_length_where_count_matches_trimmed_semantics() {
    setup_e2e();
    let mut conn = exa_conn();
    let t = vs_typed_table();

    // Expected trimmed LENGTH per row, computed from the seed's own unscaled
    // c_decimal_a values (scale 2) via the independent oracle above — never
    // hardcoded, never derived from the production formatter.
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

    // Every c_decimal_a value renders as exactly 5 characters BEFORE
    // trimming ("XX.XX", scale 2, values 10.50..60.00), so a pre-fix build
    // would match every one of the 10 non-NULL rows here. Asserting the
    // trimmed count differs from that untrimmed count is what makes this
    // test discriminate old vs. new code, rather than passing by accident.
    assert_ne!(
        expected_count, untrimmed_count,
        "expected trimmed-length count must differ from the untrimmed-length \
         (bug) count of {untrimmed_count} for this test to discriminate \
         old vs. new code"
    );

    // Row-by-row check over all 12 seed rows.
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

    // The headline COUNT(*) repro.
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

// ---------------------------------------------------------------------------
// 8.15  String-function argument-type coercion repro (#210)
// ---------------------------------------------------------------------------
//
// `crates/vs-expression/src/lib.rs`'s string-function family (`UPPER`,
// `LOWER`, `TRIM`, `INSTR`, `LOCATE`, ...) used to hand every argument
// straight to DataFusion with zero type inspection. Exasol implicitly
// converts a numeric or DATE argument to VARCHAR before invoking a string
// function; DataFusion refuses, and pre-fix the scan died at plan time with
// `F-UDF-CL-RUST-9001 ... DataFusion SQL error: Error during planning ...
// requires String, but received ...` — a hard error, not a native fallback.
// The new `string_function_arg_type_guard` dispatches each string-position
// argument on its Exasol column type before rendering: VARCHAR/CHAR pass
// through unchanged, DATE is wrapped in `CAST(... AS VARCHAR)`, DECIMAL
// reuses #211's trimmed `decimal_to_varchar_exasol` rendering, and every
// other resolvable type (BOOLEAN, DOUBLE, TIMESTAMP) declines to native
// Exasol evaluation (covered separately in section 8.16 below).
//
// Uses `typed_distinct_probe` (`vs_typed_table()`): `c_varchar`, `id`
// (DECIMAL(20,0)), `c_decimal_a` (DECIMAL(9,2)), `c_date`. Reuses the #211
// `TYPED_DECIMAL_A_UNSCALED` table and `exasol_trim_decimal_string` oracle
// defined in section 8.14 above.

/// `UPPER(c_varchar)` still pushes down and returns the uppercased string,
/// guarding the new dispatch table's VARCHAR-passthrough `Coerce([0])`
/// branch (#210) against a regression.
///
/// Unlike the other tests in this section, `UPPER(c_varchar)` never
/// hard-failed pre-fix: `c_varchar` is already a string, so the type-blind
/// original renderer already passed it straight through to DataFusion's
/// `upper()` with no coercion needed. This test proves the new per-argument
/// type dispatch — added to fix the OTHER (numeric/DATE) cases below — does
/// not silently degrade this already-working VARCHAR case to the full-row
/// fallback.
///
/// Seed: `typed_distinct_probe.c_varchar` for id=1 is `"aa"` (see
/// `common/seed.rs`'s `typed_probe()`).
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

/// `UPPER(id)` over the DECIMAL(20,0) `id` column returns the plain digit
/// string, exercising the new DECIMAL-coercion branch of
/// `string_function_arg_type_guard` (#210) for a scale-0 (integer) DECIMAL.
///
/// Pre-fix, `id` (a numeric argument) hard-failed with
/// `F-UDF-CL-RUST-9001 ... requires String, but received ...` inside the
/// DataFusion scan, the same way #210's `UPPER(c_custkey)` repro did.
/// Post-fix the argument is wrapped in the trimmed decimal-to-string
/// rendering shared with #211; since `id`'s scale is 0 there is no
/// fractional part to trim, so this really just confirms `UPPER('4')` =
/// `'4'` — a bare digit string, no decimal point.
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

/// `LTRIM(c_decimal_a)` returns the Exasol-trimmed decimal string (#210),
/// reusing the #211 `exasol_trim_decimal_string` oracle and
/// `TYPED_DECIMAL_A_UNSCALED` table from section 8.14.
///
/// Pre-fix, `c_decimal_a` (a DECIMAL column) hard-failed with
/// `F-UDF-CL-RUST-9001 ... requires String, but received ...` when passed to
/// `LTRIM`, mirroring #210's `LTRIM(c_acctbal)` repro. Post-fix the argument
/// is wrapped in the same trimmed decimal-to-string rendering #211 already
/// proved for `CAST`/`CONCAT`/`LENGTH`. `LTRIM` strips no characters here —
/// the trimmed string has no leading whitespace — so the expected value is
/// simply the trimmed decimal string itself: id=1 -> "10.5", id=4 -> "30",
/// id=6 -> "40.99" — the same three ids `e2e_decimal_cast_trims_trailing_zeros`
/// uses, for consistency.
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

/// `LOWER(c_date)` returns Exasol's default `YYYY-MM-DD` textual DATE
/// rendering (#210) — the same DATE-cast rationale `guard_like_subject`
/// already applies for #207's `LIKE` subject guard.
///
/// Pre-fix, `c_date` (a DATE column) hard-failed with
/// `F-UDF-CL-RUST-9001 ... requires String, but received ...` when passed to
/// `LOWER`, mirroring #210's `LOWER(l_shipdate)` repro. Post-fix the
/// argument is wrapped in an explicit `CAST(... AS VARCHAR)`, matching
/// Exasol's own `NLS_DATE_FORMAT` default.
///
/// Seed: `c_date` for id=1 is `BASE_DATE + 0` days. `common/seed.rs` defines
/// `BASE_DATE = 19_723` (days since epoch) and separately documents
/// `BASE_DATE + 182` as the literal date 2024-07-01 (`INITDEF_REAL_DATE_DAYS`
/// comment), which confirms `BASE_DATE` itself is 2024-01-01 (Jan 1 + 182
/// days = Jul 1 in the 2024 leap year: 31+29+31+30+31+30 = 182).
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

/// `INSTR(c_decimal_a, '.')` returns the position of `.` WITHIN the trimmed
/// decimal string (#210), not the untrimmed fixed-scale text.
///
/// Pre-fix, `c_decimal_a` hard-failed with `F-UDF-CL-RUST-9001 ... requires
/// String, but received ...` when passed to `INSTR`, mirroring #210's
/// `INSTR(c_custkey, '1')` repro. Post-fix the argument is wrapped in the
/// #211 trimmed rendering before `strpos` is applied, so the returned
/// position reflects the TRIMMED string, not the fixed-scale ("XX.XX")
/// string. Positions are computed in Rust from `exasol_trim_decimal_string`'s
/// output (`s.find('.').map(|i| i as i64 + 1).unwrap_or(0)`), never
/// hardcoded: id=1 "10.5" -> 3, id=4 "30" -> 0 (no '.'), id=6 "40.99" -> 3.
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

// ---------------------------------------------------------------------------
// 8.16  String-function argument-type decline native-oracle parity (#210)
// ---------------------------------------------------------------------------
//
// BOOLEAN, DOUBLE, and TIMESTAMP are non-coercible resolvable types for
// `string_function_arg_type_guard`: they are not VARCHAR/CHAR (pass through),
// not DATE (CAST-wrapped), and not DECIMAL (trim-wrapped), so the guard
// returns `None` and the whole select-list item degrades to the full base
// row projection instead of hard-failing. Exasol's own SQL engine then
// evaluates the string function over the raw returned column natively.
//
// Each comparison below is against an IN-SESSION NATIVE ORACLE: a second
// query over a bare literal value, with NO virtual schema reference, run
// over the SAME connection — so the comparison is not a tautology. A
// regressed guard would either hard-fail (if it stopped declining) or
// return DataFusion's own divergent text formatting (if something coerced
// instead of declining); either way this comparison would catch it.
//
// Exasol's public Type Conversion Rules
// (https://docs.exasol.com/db/latest/sql_references/data_types/typeconversionrules.htm)
// document implicit BOOLEAN/TIMESTAMP-to-VARCHAR conversion as supported,
// which is why `c_ts`/`c_bool` are included here alongside `c_double` rather
// than omitted. If the native-oracle query for either ever fails against a
// live Exasol container (an Exasol-side rejection of its own documented
// implicit conversion, unrelated to this fix), drop that specific test and
// record why here — never weaken its assertion to make it pass regardless.

/// `UPPER(c_double)` over the virtual table declines pushdown and falls back
/// to native Exasol evaluation, matching an in-session native oracle over a
/// bare `DOUBLE` literal (#210).
///
/// Seed: `typed_distinct_probe.c_double` for id=1 is `0.5` (see
/// `common/seed.rs`'s `typed_probe()`).
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

/// `UPPER(c_ts)` over the virtual table declines pushdown the same way
/// `UPPER(c_double)` does (#210) and matches an in-session native oracle over
/// a bare `TIMESTAMP` literal.
///
/// Seed: `typed_distinct_probe.c_ts` for id=1 is `BASE_TS_MICROS + 100ms` =
/// 2024-01-01 00:00:00.100 (see `common/seed.rs`'s `typed_probe()`; its
/// `ts(100)` closure computes `BASE_TS_MICROS + 100 * 1_000` microseconds).
/// See the section note above regarding live-stack verification of this case.
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

    let oracle_cols =
        conn.query_columns("SELECT UPPER(CAST(TIMESTAMP '2024-01-01 00:00:00.100' AS TIMESTAMP))");
    let oracle_value = oracle_cols[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("native oracle not a string: {:?}", oracle_cols[0][0]));

    assert_eq!(
        vs_value, oracle_value,
        "UPPER(c_ts) over the VS must match the native Exasol oracle, got \
         vs={vs_value:?} oracle={oracle_value:?}"
    );
}

/// `UPPER(c_bool)` over the virtual table declines pushdown the same way
/// `UPPER(c_double)` does (#210) and matches an in-session native oracle over
/// a bare `BOOLEAN` literal. Exasol's implicit BOOLEAN-to-VARCHAR conversion
/// renders `TRUE`/`FALSE` (Exasol Type Conversion Rules), so
/// `UPPER(CAST(TRUE AS BOOLEAN))` -> `"TRUE"` is the expected oracle value.
///
/// Seed: `typed_distinct_probe.c_bool` for id=1 is `true` (see
/// `common/seed.rs`'s `typed_probe()`). See the section note above regarding
/// live-stack verification of this case.
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

// ---------------------------------------------------------------------------
// 8.17  INSTR/LOCATE arity decline (#228)
// ---------------------------------------------------------------------------
//
// `INSTR`/`LOCATE` beyond 2 arguments unconditionally decline, regardless of
// argument type: `vs-expression`'s renderer reads only `args[0]`/`args[1]`
// and drops the rest (#228), so coercing index 0 would let a truncated
// rendering plan successfully and return a position computed as if the
// start-position argument had never been given — a silent WRONG ANSWER,
// where a hard DataFusion error would at least have been loud. Both wired
// surfaces (select-list projection and WHERE-clause filter) are covered.

/// Select-list `INSTR` beyond 2 arguments declines to native Exasol
/// evaluation instead of coercing and silently truncating to a 2-argument
/// `strpos` call (#228).
///
/// Seed: `typed_distinct_probe.c_varchar` for id=1 is `"aa"` (see
/// `common/seed.rs`'s `typed_probe()`). Exasol's `INSTR('aa', 'a', 2)`
/// searches for `'a'` starting AT position 2 — the second `'a'` in `"aa"` —
/// and returns `2`. A regressed build that coerced `INSTR(c_varchar, 'a', 2)`
/// down to a 2-argument `strpos(c_varchar, 'a')` (silently dropping the
/// start-position argument) would instead return `1` (the FIRST `'a'`) — a
/// different, silently wrong number, so this test discriminates
/// correct-decline from silently-wrong-coerce.
///
/// The expected value of `2` is independently confirmed against a native
/// in-session oracle (`SELECT INSTR('aa', 'a', 2)`, no virtual schema),
/// pinning the oracle itself before using it to judge the VS result.
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

/// WHERE-clause `INSTR` beyond 2 arguments declines to native Exasol
/// evaluation instead of coercing and silently truncating (#228) — the
/// WHERE-clause counterpart of the select-list case above.
///
/// Seed: `typed_distinct_probe.c_varchar` for id=4 is `"bb"` (length 2), see
/// `common/seed.rs`'s `typed_probe()`. Exasol's `INSTR('bb', 'b', 3)`
/// searches for `'b'` starting at position 3 — beyond the string's length —
/// and returns `0` natively, so `WHERE INSTR(c_varchar, 'b', 3) = 0` matches
/// id=4. A regressed build that coerced down to 2-argument
/// `strpos(c_varchar, 'b')` (ignoring the start position) would compute `1`
/// for id=4 (the first `'b'`), so `1 != 0` would make the predicate NEVER
/// match id=4 — the discriminating check below.
///
/// The expected value of `0` is independently confirmed against a native
/// in-session oracle (`SELECT INSTR('bb', 'b', 3)`, no virtual schema).
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

// ---------------------------------------------------------------------------
// 8.18  ORDER BY on an expression or aggregate outside the select list (#198)
// ---------------------------------------------------------------------------
//
// `ORDER_BY_EXPRESSION` is advertised, so Exasol pushes a structured `orderBy`
// for an expression or aggregate sort key instead of silently appending that
// key to the `selectList` — the append is what surfaced pre-fix as an extra
// result column named `HIDDEN_COL_n`. Every case below asserts BOTH halves:
// no leaked column, AND the pushed ordering genuinely applied. The second half
// is not redundant — advertising the capability with no backing path returns
// rows in raw file order with no error at all, which is strictly worse than
// the leak it replaces (plan decision-log [2], measured live).
//
// All cases run against `typed_distinct_probe` (`vs_typed_table()`), 12 rows,
// `id` 1..12 with one row per `id`. Every expected ordering below is computed
// by hand from `common/seed.rs`'s `typed_probe()`:
//
//   id       1  2     3  4  5  6  |  7  8  9  10    11  12
//   c_price  2  3  NULL  4  2  5  |  2  3  6   4  NULL   5
//   c_qty    3  2     5  1  3  2  |  6  4  1   2     3   4
//
// `c_price` is NULL for id 3 and 11; `c_qty` has no NULLs. Exasol's default
// NULL placement is NULLS FIRST under DESC and NULLS LAST under ASC —
// confirmed against a native in-session oracle
// (`SELECT V FROM (SELECT 1 AS V UNION ALL SELECT NULL UNION ALL SELECT 3)
// ORDER BY V DESC` → NULL, 3, 1) — and the adapter renders whichever
// placement Exasol pushes on the wire, so the NULL rows' positions are part of
// what these tests pin.

/// Run `sql` and return `(column names, column-major data)`.
///
/// `query_columns` drops the result-set metadata, but a `HIDDEN_COL_n` leak is
/// visible ONLY in the column NAMES — the arity alone does not distinguish a
/// leaked sort key from a legitimately selected third column.
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

/// Assert the result leaked no synthetic `HIDDEN_COL_n` column (#198).
fn assert_no_hidden_columns(names: &[String], sql: &str) {
    assert!(
        !names.iter().any(|n| n.starts_with("HIDDEN_COL")),
        "result must not leak a synthetic HIDDEN_COL_n column (#198), got \
         columns {names:?} for:\n{sql}"
    );
}

/// Extract the REAL wire `pushdownRequest` object Exasol sent the adapter for
/// `sql`, as parsed JSON.
///
/// `EXPLAIN VIRTUAL` returns the adapter-generated SQL *and* a column carrying
/// the echoed adapter exchange (`getCapabilities` + `pushdown` request /
/// response) as a JSON array. `explain_virtual_sql` flattens all of that into
/// one blob, which is fine for substring-matching the generated SQL but too
/// coarse to assert on Exasol's wire payload — a `contains("limit")` there also
/// matches the adapter's own snake_case scan-spec keys, and misses an uppercase
/// rendered `LIMIT`. This picks out the `pushdownRequest` object itself so its
/// keys can be asserted directly.
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

/// Collect a column's integer values as a set, for assertions over a group set
/// whose internal row order is not deterministic (ties on the sort measure).
fn int_set(values: &[serde_json::Value]) -> std::collections::HashSet<i64> {
    values.iter().map(parse_int).collect()
}

/// Row scan: an expression sort key absent from the select list leaks no
/// `HIDDEN_COL_n` and still orders the result correctly (#198, tasks 9.1/9.2).
///
/// Case 1 (single key) is the plan's row-scan repro. Its sort expression
/// references only `c_price`, which is ALREADY a visible select-list column,
/// so the declined-ORDER-BY wrapper must append NO extra scan column — the
/// "at most once, never invented" rule.
///
/// Case 2 (two keys) adds a second key over `c_qty`, which is NOT selected, so
/// exactly one hidden base column is appended after the two visible ones while
/// `c_price` is still not duplicated. Both keys must render, with their own
/// direction and NULL placement, or the second key's tie-break ordering below
/// cannot hold.
#[test]
fn e2e_order_by_expression_not_selected_leaks_no_hidden_column() {
    setup_e2e();
    let mut conn = exa_conn();

    // --- Case 1: single expression sort key, not selected (task 9.1) -------
    let sql = format!(
        "SELECT id, c_price FROM {} WHERE id<=5 ORDER BY ABS(c_price) DESC",
        vs_typed_table()
    );

    // The sort expression references only the already-projected C_PRICE, so
    // the scan spec's projection must stay at the two visible columns.
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

    // --- Case 2: two expression sort keys, neither selected (task 9.2) -----
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

/// Grouped, issue #198's own repro shape: a group-key-only select list with an
/// `ORDER BY` over an aggregate that is not selected, plus a `LIMIT` that must
/// genuinely cut groups (task 9.3).
///
/// This routes through `RequestShape::GroupByWrapper` — the qualified
/// single-table wrapper — so it is also the end-to-end coverage for that entry
/// point of the wrapper family (task 1.2).
///
/// `id` has 12 distinct values over the 12-row seed (one group per row), so
/// `LIMIT 4` drops 8 groups. `SUM(c_qty)` per `id` is just `c_qty`, whose top
/// values are 6 (id 7), 5 (id 3), then 4 (ids 8 and 12) — the tie at the 4th
/// position falls ENTIRELY inside the limit, so the returned group SET is
/// deterministic even though the row order within that tie is not. Group-set
/// EQUALITY is what proves the `LIMIT` is both rendered AND applied AFTER the
/// `ORDER BY`: a limit applied before the ordering, or absent, fails it.
/// `SUM(c_price)` is deliberately NOT used — `c_price` is NULL for ids 3 and
/// 11, which would make the assertion depend on NULL placement under `DESC`.
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

/// Grouped: an aggregate sort key absent from the select list leaks no
/// `HIDDEN_COL_n` when a DIFFERENT aggregate is already selected — and the
/// variant whose sort key IS selected keeps the partial/merge path (task 9.4).
///
/// Case 1 reaches `GroupedOrderBy::Unresolvable` with a NON-empty plan list
/// (one `COUNT(*)` plan the sort key does not match), the same wrapper route
/// as the group-key-only shape above but from a different plan-list state.
///
/// Case 2 is the control that must NOT route to the wrapper: `SUM(c_price)` is
/// in the select list, so the sort key resolves against that plan's merged
/// partial expression and the partial/merge decomposition is retained. It is
/// distinguished from Case 1 by the scan spec carrying `group_keys` (a
/// per-shard partial aggregation) and the merge SELECT ordering on
/// `SUM("PARTIAL_sum_…")` rather than on a base column.
///
/// `c_bool` groups: true (ids 1,2,5,6,7,9,11,12), false (ids 4,8), NULL (ids
/// 3,10). `SUM(c_price)` is 25 / 7 / 4 respectively — all non-NULL, so the
/// group order under `DESC` is deterministic without depending on NULL
/// placement.
#[test]
fn e2e_grouped_order_by_aggregate_not_selected_leaks_no_hidden_column() {
    setup_e2e();
    let mut conn = exa_conn();

    // --- Case 1: sort aggregate NOT selected, another aggregate selected ---
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

    // --- Case 2: sort aggregate IS selected → partial/merge path retained --
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

/// Control: the sort expression IS also a select-list item, so nothing is
/// hidden and nothing is stripped (task 9.5).
///
/// The plan proved that this shape and the leaking one push a BYTE-IDENTICAL
/// `selectList` while `ORDER_BY_EXPRESSION` is unadvertised — Exasol picks the
/// client-facing name (`A` here, `HIDDEN_COL_2` there) server-side. So this
/// case exists to prove the fix did not regress the shape that was already
/// correct: the third column must survive, named `A`.
///
/// `ORDER BY ABS(c_price)` is ascending, so NULLs (ids 3 and 11) sort LAST.
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

/// Multi-`COUNT(DISTINCT)` (Case 2/3) with an aggregate `ORDER BY`: the second
/// qualified-wrapper entry point returns a correct, leak-free result (task
/// 9.6).
///
/// This shape declines the partial/merge path and routes to the qualified
/// single-table wrapper, which is the seam task 1.2 relaxed.
///
/// MEASURED CAVEAT, same finding as plan decision-log [10]: this is an
/// `aggregationType: "single_group"` request, and Exasol pushes NO structured
/// `orderBy` for a single-group aggregate even with `ORDER_BY_EXPRESSION`
/// advertised — sorting one row costs nothing to do client-side. Verified live
/// for this exact query: the captured payload carries no `orderBy` key. So
/// what this case pins end to end is that the wrapper route stays correct and
/// leak-free under an aggregate `ORDER BY`; the wrapper's expression-sort-key
/// RENDERING is proven by the unit test on `outer_wrapper_clauses`, which can
/// feed the `orderBy` this shape never receives.
///
/// `c_bool` has 2 distinct non-NULL values, `id` has 12.
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

/// `LIMIT 0` over a one-row aggregate result returns ZERO rows, not one
/// `COUNT = 0` row (task 9.9).
///
/// Pins decision-log [10]: for an `aggregationType: "single_group"` request
/// Exasol keeps BOTH the sort and the truncation client-side, pushing neither
/// wire key — so the zero rows here come from Exasol's own truncation, not from
/// a `LIMIT 0` rendered on the adapter's merge SELECT (`request_limit` is
/// `None`). Task 5.1's render site is covered by the `plan_scan_sql` unit test,
/// which can feed the wire `limit: 0` this shape never receives.
#[test]
fn e2e_order_by_aggregate_with_limit_zero_returns_no_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(*) FROM {} ORDER BY COUNT(*) DESC LIMIT 0",
        vs_typed_table()
    );

    // Assert on the wire payload itself, not on the flattened EXPLAIN blob:
    // these keys DO appear here when Exasol pushes them (verified against a
    // `ORDER BY <col> DESC LIMIT n` row scan on this same table).
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
