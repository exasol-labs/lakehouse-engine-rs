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
    E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_NAMESPACE, E2E_TABLE, E2E_TYPED_TABLE, seed_events,
    seed_typed_distinct_probe,
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
