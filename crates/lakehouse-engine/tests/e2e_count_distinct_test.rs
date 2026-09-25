//! E2E correctness for `COUNT(DISTINCT)` pushdown: the Case 1 native-merge
//! fan-out and the Case 2/3 qualified single-table wrapper.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    DISTINCT_CATEGORY_COL, DISTINCT_CATEGORY_COUNT, DISTINCT_COMMENT_COL,
    DISTINCT_COMMENT_LENGTH_SUM, DISTINCT_REGION_COL, DISTINCT_REGION_COUNT, E2E_DISTINCT_TABLE,
    E2E_HIGH_CARD_TABLE, E2E_NAMESPACE, E2E_TYPED_TABLE, HIGH_CARD_COL, HIGH_CARD_ROWS,
    TYPED_COL_BOOL, TYPED_COL_DATE, TYPED_COL_DECIMAL_A, TYPED_COL_DECIMAL_B, TYPED_COL_DOUBLE,
    TYPED_COL_PRICE, TYPED_COL_QTY, TYPED_COL_TS, TYPED_COL_VARCHAR, seed_distinct_probe,
    seed_events, seed_high_card_probe, seed_typed_distinct_probe, typed_bool_distinct,
    typed_date_distinct, typed_decimal_a_distinct, typed_decimal_b_distinct, typed_double_distinct,
    typed_product_distinct, typed_ts_case_distinct, typed_ts_distinct, typed_varchar_char_distinct,
    typed_varchar_distinct, typed_varchar_upper_distinct,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

use std::sync::OnceLock;

const VS_NAME: &str = "MY_LAKEHOUSE";

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
            seed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg distinct_probe table");
            seed_high_card_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg high_card_probe table");
            seed_typed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed Iceberg typed_distinct_probe table");
        });

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
    });
}

fn distinct_table() -> String {
    format!("{VS_NAME}.{}", E2E_DISTINCT_TABLE.to_uppercase())
}

fn high_card_table() -> String {
    format!("{VS_NAME}.{}", E2E_HIGH_CARD_TABLE.to_uppercase())
}

fn typed_table() -> String {
    format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase())
}

/// Case 2/3 cannot use per-distinct scalar subqueries (Exasol rejects an emitting
/// UDF in a scalar subquery, `04000`) nor a bare row scan (Exasol never
/// re-aggregates a declined pushdown), so it must be the `LHS_T0` wrapper.
fn assert_qualified_wrapper_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.contains("LHS_T0"),
        "EXPLAIN VIRTUAL output must be the qualified single-table wrapper (one \
         aliased raw fan-out subquery, 'AS LHS_T0'), got:\n{pushed_sql}"
    );
    assert!(
        pushed_sql.to_uppercase().contains("COUNT(DISTINCT"),
        "the wrapper must render each COUNT(DISTINCT) verbatim over the materialized \
         scan (Exasol aggregates the returned rows), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains(r#"COUNT(DISTINCT "V")"#),
        "a Case 2/3 request must NOT emit the Case 1 distinct row-scan fan-out \
         (COUNT(DISTINCT \"V\")), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains(r#"(SELECT COUNT(DISTINCT "V")"#),
        "a Case 2/3 request must NOT compose per-distinct SELECT-list scalar \
         subqueries (the blocked 04000 design), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

fn assert_count_distinct_fan_out_pushed_down(conn: &mut ExaConn, query_sql: &str) {
    let explain_sql = format!("EXPLAIN VIRTUAL {query_sql}");
    let resp = conn.execute(&explain_sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);

    let pushed_sql: String = cols
        .iter()
        .flat_map(|col| col.iter())
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        pushed_sql.to_uppercase().contains("COUNT(DISTINCT"),
        "EXPLAIN VIRTUAL output must contain a native COUNT(DISTINCT ...) merge \
         wrapper (the distinct count pushed down as a DISTINCT row-scan fan-out, \
         not a raw-scan fallback), got:\n{pushed_sql}"
    );
    assert!(
        !pushed_sql.contains("SELECT * FROM (SELECT"),
        "EXPLAIN VIRTUAL output must not be a raw row-scan fallback \
         ('SELECT * FROM (SELECT ...)'), got:\n{pushed_sql}"
    );
}

/// Scenario: COUNT(DISTINCT) dedups across shards, excludes NULLs, and returns 0 for empty and all-NULL sets
#[test]
fn count_distinct_dedups_across_shards_excludes_nulls_empty() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {}",
        distinct_table()
    );
    assert_count_distinct_fan_out_pushed_down(&mut conn, &sql);
    let distinct_count = conn.query_scalar_i64(&sql);
    assert_eq!(
        distinct_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) must be {DISTINCT_CATEGORY_COUNT} \
         (values {{A,B,C}}, with A shared across both shards and NULLs excluded), \
         got {distinct_count}"
    );

    // 'AA' matches no row but lies inside both files' min/max range, so no file
    // is pruned and the shards genuinely emit nothing.
    let empty_sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {} WHERE {DISTINCT_CATEGORY_COL} = 'AA'",
        distinct_table()
    );
    let empty_count = conn.query_scalar_i64(&empty_sql);
    assert_eq!(
        empty_count, 0,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) over an empty result set must be 0, \
         got {empty_count}"
    );

    let all_null_sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) FROM {} WHERE {DISTINCT_CATEGORY_COL} IS NULL",
        distinct_table()
    );
    let all_null_count = conn.query_scalar_i64(&all_null_sql);
    assert_eq!(
        all_null_count, 0,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) WHERE {DISTINCT_CATEGORY_COL} IS NULL \
         must be 0 (an all-NULL local set), got {all_null_count}"
    );
}

/// Scenario: a high-cardinality single-shard COUNT(DISTINCT) completes with the exact count (#146)
#[test]
fn high_cardinality_count_distinct_completes() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {HIGH_CARD_COL}) FROM {}",
        high_card_table()
    );

    assert_count_distinct_fan_out_pushed_down(&mut conn, &sql);

    let distinct_count = conn.query_scalar_i64(&sql);
    assert_eq!(
        distinct_count, HIGH_CARD_ROWS as i64,
        "COUNT(DISTINCT {HIGH_CARD_COL}) over {HIGH_CARD_ROWS} unique tokens must \
         complete and equal {HIGH_CARD_ROWS} (the exact single-node distinct \
         count), got {distinct_count}"
    );
}

/// Scenario: the harness reads a handle-backed result set to completion, not just its first fetch
#[test]
fn harness_reads_high_cardinality_result_set_to_completion() {
    // At the default 64 MiB budget the whole scan fits in one response, so a
    // small budget is needed to force multiple fetches.
    const CHUNKED_FETCH_NUM_BYTES: u64 = 65_536;

    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT {HIGH_CARD_COL} FROM {}", high_card_table());
    let resp = conn.execute(&sql);
    let result_set = &resp["responseData"]["results"][0]["resultSet"];

    let (cols, responses) =
        conn.fetch_result_columns_with_num_bytes(result_set, CHUNKED_FETCH_NUM_BYTES);

    let rows = cols.first().map_or(0, |col| col.len());
    assert_eq!(
        rows, HIGH_CARD_ROWS,
        "reading {HIGH_CARD_COL} at a {CHUNKED_FETCH_NUM_BYTES}-byte per-response \
         budget must yield every one of the {HIGH_CARD_ROWS} seeded rows, got \
         {rows} across {responses} fetch response(s) — a short read means the \
         harness stopped at the first response instead of reading to completion"
    );
    assert!(
        responses >= 2,
        "a {CHUNKED_FETCH_NUM_BYTES}-byte per-response budget must split \
         {HIGH_CARD_ROWS} ~100-byte rows across more than one fetch response, so \
         that the read loop is genuinely exercised, got {responses}"
    );
}

/// Scenario: the Q9b multi-COUNT(DISTINCT) shape routes to the qualified wrapper and matches single-node results
#[test]
fn q9b_multi_count_distinct_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT {DISTINCT_CATEGORY_COL}), COUNT(DISTINCT {DISTINCT_REGION_COL}), \
         SUM(LENGTH({DISTINCT_COMMENT_COL})) FROM {}",
        distinct_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 aggregate columns: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let category_count = parse_int(&cols[0][0]);
    assert_eq!(
        category_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT {DISTINCT_CATEGORY_COL}) must be {DISTINCT_CATEGORY_COUNT}, \
         got {category_count}"
    );

    let region_count = parse_int(&cols[1][0]);
    assert_eq!(
        region_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT {DISTINCT_REGION_COL}) must be {DISTINCT_REGION_COUNT}, \
         got {region_count}"
    );

    let comment_length_sum = cols[2][0]
        .as_i64()
        .or_else(|| cols[2][0].as_f64().map(|f| f.round() as i64))
        .or_else(|| {
            cols[2][0]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round() as i64)
        })
        .unwrap_or_else(|| panic!("expected numeric SUM(LENGTH(...)), got: {:?}", cols[2][0]));
    assert_eq!(
        comment_length_sum, DISTINCT_COMMENT_LENGTH_SUM,
        "SUM(LENGTH({DISTINCT_COMMENT_COL})) must be {DISTINCT_COMMENT_LENGTH_SUM}, \
         got {comment_length_sum}"
    );
}

/// Scenario: a lone COUNT(DISTINCT <string expression>) routes to the qualified wrapper and matches single-node results
#[test]
fn count_distinct_string_expression_argument_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let upper_sql = format!(
        "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) FROM {}",
        distinct_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &upper_sql);
    let upper_count = conn.query_scalar_i64(&upper_sql);
    assert_eq!(
        upper_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be \
         {DISTINCT_CATEGORY_COUNT} (string values {{A,B,C}}, NULLs excluded), got \
         {upper_count}"
    );

    let lower_sql = format!(
        "SELECT COUNT(DISTINCT LOWER({DISTINCT_REGION_COL})) FROM {}",
        distinct_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &lower_sql);
    let lower_count = conn.query_scalar_i64(&lower_sql);
    assert_eq!(
        lower_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT LOWER({DISTINCT_REGION_COL})) must be \
         {DISTINCT_REGION_COUNT} (string values {{north,central,south,east}}), got \
         {lower_count}"
    );
}

/// Scenario: an expression-argument COUNT(DISTINCT) combined with other aggregates matches single-node results in the wrapper
#[test]
fn count_distinct_expression_arg_combined_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})), \
         COUNT(DISTINCT {DISTINCT_REGION_COL}), \
         SUM(LENGTH({DISTINCT_COMMENT_COL})) FROM {}",
        distinct_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &sql);

    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 3, "expected 3 aggregate columns: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected 1 row: {cols:?}");

    let category_count = parse_int(&cols[0][0]);
    assert_eq!(
        category_count, DISTINCT_CATEGORY_COUNT,
        "COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be \
         {DISTINCT_CATEGORY_COUNT}, got {category_count}"
    );

    let region_count = parse_int(&cols[1][0]);
    assert_eq!(
        region_count, DISTINCT_REGION_COUNT,
        "COUNT(DISTINCT {DISTINCT_REGION_COL}) must be {DISTINCT_REGION_COUNT}, \
         got {region_count}"
    );

    let comment_length_sum = cols[2][0]
        .as_i64()
        .or_else(|| cols[2][0].as_f64().map(|f| f.round() as i64))
        .or_else(|| {
            cols[2][0]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round() as i64)
        })
        .unwrap_or_else(|| panic!("expected numeric SUM(LENGTH(...)), got: {:?}", cols[2][0]));
    assert_eq!(
        comment_length_sum, DISTINCT_COMMENT_LENGTH_SUM,
        "SUM(LENGTH({DISTINCT_COMMENT_COL})) must be {DISTINCT_COMMENT_LENGTH_SUM}, \
         got {comment_length_sum}"
    );
}

/// Scenario: bare-column COUNT(DISTINCT) matches single-node results for every scan-reachable Exasol type
#[test]
fn count_distinct_bare_column_type_matrix_matches_single_node() {
    // CHAR is absent: no Iceberg type maps to Exasol CHAR, so a bare CHAR column is
    // unreachable; a CHAR cast is covered in the wrapper test instead.
    setup_e2e();
    let mut conn = exa_conn();

    let cases: [(&str, i64); 7] = [
        (TYPED_COL_DECIMAL_A, typed_decimal_a_distinct()),
        (TYPED_COL_DECIMAL_B, typed_decimal_b_distinct()),
        (TYPED_COL_DOUBLE, typed_double_distinct()),
        (TYPED_COL_VARCHAR, typed_varchar_distinct()),
        (TYPED_COL_DATE, typed_date_distinct()),
        (TYPED_COL_TS, typed_ts_distinct()),
        (TYPED_COL_BOOL, typed_bool_distinct()),
    ];

    for (col, expected) in cases {
        let sql = format!("SELECT COUNT(DISTINCT {col}) FROM {}", typed_table());
        assert_count_distinct_fan_out_pushed_down(&mut conn, &sql);
        let got = conn.query_scalar_i64(&sql);
        assert_eq!(
            got, expected,
            "COUNT(DISTINCT {col}) must be {expected} (cross-shard dedup, NULLs \
             excluded), got {got}"
        );
    }
}

/// Scenario: expression-argument COUNT(DISTINCT) via the wrapper dedups natively without string-cast collisions
#[test]
fn count_distinct_expression_arg_via_wrapper_matches_single_node() {
    setup_e2e();
    let mut conn = exa_conn();

    let numeric_sql = format!(
        "SELECT COUNT(DISTINCT {TYPED_COL_PRICE} * {TYPED_COL_QTY}) FROM {}",
        typed_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &numeric_sql);
    let numeric_count = conn.query_scalar_i64(&numeric_sql);
    assert_eq!(
        numeric_count,
        typed_product_distinct(),
        "COUNT(DISTINCT {TYPED_COL_PRICE} * {TYPED_COL_QTY}) must be {} \
         (native product dedup across shards, NULL operands excluded), got {numeric_count}",
        typed_product_distinct()
    );

    let string_sql = format!(
        "SELECT COUNT(DISTINCT UPPER({TYPED_COL_VARCHAR})) FROM {}",
        typed_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &string_sql);
    let string_count = conn.query_scalar_i64(&string_sql);
    assert_eq!(
        string_count,
        typed_varchar_upper_distinct(),
        "COUNT(DISTINCT UPPER({TYPED_COL_VARCHAR})) must be {} (mixed-case values \
         fold together across shards; strictly below the raw distinct count {}), got \
         {string_count}",
        typed_varchar_upper_distinct(),
        typed_varchar_distinct()
    );

    // A CASE, not a bare column, forces the wrapper route.
    let temporal_sql = format!(
        "SELECT COUNT(DISTINCT CASE WHEN {TYPED_COL_BOOL} THEN {TYPED_COL_TS} ELSE NULL END) \
         FROM {}",
        typed_table()
    );
    assert_qualified_wrapper_pushed_down(&mut conn, &temporal_sql);
    let temporal_count = conn.query_scalar_i64(&temporal_sql);
    assert_eq!(
        temporal_count,
        typed_ts_case_distinct(),
        "COUNT(DISTINCT CASE WHEN {TYPED_COL_BOOL} THEN {TYPED_COL_TS} END) must be {} \
         (millisecond-distinct timestamps within one second, deduped natively across \
         shards — a fractional-second-truncating string cast would undercount), got \
         {temporal_count}",
        typed_ts_case_distinct()
    );

    // No pushed-shape assertion: the count must be exact either way.
    let char_sql = format!(
        "SELECT COUNT(DISTINCT CAST({TYPED_COL_VARCHAR} AS CHAR(20))) FROM {}",
        typed_table()
    );
    let char_count = conn.query_scalar_i64(&char_sql);
    assert_eq!(
        char_count,
        typed_varchar_char_distinct(),
        "COUNT(DISTINCT CAST({TYPED_COL_VARCHAR} AS CHAR(20))) must be {} \
         (fixed-width padding is injective over the space-free seeded values), got \
         {char_count}",
        typed_varchar_char_distinct()
    );
}

/// Scenario: an expression-argument COUNT(DISTINCT) over empty, all-NULL, and all-pruned inputs returns a single 0
#[test]
fn count_distinct_expression_arg_empty_returns_zero() {
    setup_e2e();
    let mut conn = exa_conn();

    let scenarios: [(&str, String); 3] = [
        (
            "empty non-pruned",
            format!("{DISTINCT_CATEGORY_COL} = 'AA'"),
        ),
        ("all-NULL", format!("{DISTINCT_CATEGORY_COL} IS NULL")),
        ("all-files-pruned", "id > 1000".to_string()),
    ];

    for (label, predicate) in &scenarios {
        let sql = format!(
            "SELECT COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) FROM {} WHERE {predicate}",
            distinct_table()
        );
        let cols = conn.query_columns(&sql);
        assert_eq!(
            cols.len(),
            1,
            "[{label}] expected 1 aggregate column: {cols:?}"
        );
        assert_eq!(
            cols[0].len(),
            1,
            "[{label}] expected exactly 1 row: {cols:?}"
        );
        let count = parse_int(&cols[0][0]);
        assert_eq!(
            count, 0,
            "[{label}] COUNT(DISTINCT UPPER({DISTINCT_CATEGORY_COL})) must be 0, got {count}"
        );
    }
}

/// Scenario: COUNT(DISTINCT) over an all-files-pruned predicate returns a single 0 row (#57)
#[test]
fn count_distinct_all_files_pruned_returns_zero() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT COUNT(DISTINCT id) FROM {} WHERE id > 1000",
        distinct_table()
    );
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row: {cols:?}");
    let count = parse_int(&cols[0][0]);
    assert_eq!(
        count, 0,
        "COUNT(DISTINCT id) over an all-files-pruned predicate must be 0, got {count}"
    );
}

/// Scenario: SUM over an all-files-pruned predicate returns a single NULL row (#57)
#[test]
fn sum_all_files_pruned_returns_null() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!("SELECT SUM(id) FROM {} WHERE id > 1000", distinct_table());
    let cols = conn.query_columns(&sql);
    assert_eq!(cols.len(), 1, "expected 1 aggregate column: {cols:?}");
    assert_eq!(cols[0].len(), 1, "expected exactly 1 row: {cols:?}");
    assert!(
        cols[0][0].is_null(),
        "SUM(id) over an all-files-pruned predicate must be NULL, got {:?}",
        cols[0][0]
    );
}

/// Scenario: a grouped aggregate over an all-files-pruned predicate returns zero rows (#57)
#[test]
fn grouped_aggregate_all_files_pruned_returns_no_rows() {
    setup_e2e();
    let mut conn = exa_conn();

    let sql = format!(
        "SELECT id, COUNT(*) FROM {} WHERE id > 1000 GROUP BY id",
        distinct_table()
    );
    let cols = conn.query_columns(&sql);
    let total_rows: usize = cols.iter().map(|c| c.len()).sum();
    assert_eq!(
        total_rows, 0,
        "grouped aggregate over an all-files-pruned predicate must return zero rows, got {cols:?}"
    );
}

const GROUND_TRUTH_DISTINCT_TABLE: &str = "GT_DISTINCT_PROBE";

/// Scenario: a scalar-wrapped COUNT(DISTINCT) routes to the wrapper and matches the native oracle
#[test]
fn e2e_scalar_wrapped_count_distinct_routes_to_wrapper_and_matches_native_oracle() {
    setup_e2e();
    let mut conn = exa_conn();

    let select_list = format!("ROUND(COUNT(DISTINCT {DISTINCT_REGION_COL}), 2)");
    let sql = format!("SELECT {select_list} FROM {}", distinct_table());

    let pushed = explain_virtual_sql(&mut conn, &sql);
    assert!(
        pushed.contains(r#"AS "LHS_T0""#),
        "a scalar-wrapped COUNT(DISTINCT) must route to the qualified \
         single-table wrapper (one aliased raw fan-out subquery), got:\n{pushed}"
    );
    assert!(
        pushed.to_uppercase().contains("COUNT(DISTINCT"),
        "the wrapper must render the COUNT(DISTINCT) verbatim over the \
         materialized scan, got:\n{pushed}"
    );
    assert!(
        !pushed.contains("PARTIAL_"),
        "COUNT(DISTINCT) has no partial/merge decomposition, so no partial \
         aggregate column may be pushed for it, got:\n{pushed}"
    );

    conn.execute(&format!(
        "CREATE OR REPLACE TABLE {SCHEMA_NAME}.{GROUND_TRUTH_DISTINCT_TABLE} AS \
         SELECT {DISTINCT_CATEGORY_COL}, {DISTINCT_REGION_COL} FROM {}",
        distinct_table()
    ));

    let actual = conn.query_columns(&sql);
    assert_eq!(actual.len(), 1, "expected 1 column: {actual:?}");
    assert_eq!(
        actual[0].len(),
        1,
        "a scalar-wrapped COUNT(DISTINCT) must return the single-node result — \
         exactly ONE row, not one per shard: {actual:?}"
    );

    let expected = conn.query_columns(&format!(
        "SELECT {select_list} FROM {SCHEMA_NAME}.{GROUND_TRUTH_DISTINCT_TABLE}"
    ));
    let (got, want) = (parse_int(&actual[0][0]), parse_int(&expected[0][0]));
    assert_eq!(
        got, want,
        "scalar-wrapped COUNT(DISTINCT {DISTINCT_REGION_COL}) must equal the \
         native oracle {want}, got {got}"
    );
    assert_eq!(
        got, DISTINCT_REGION_COUNT,
        "the seeded fixture has {DISTINCT_REGION_COUNT} distinct \
         {DISTINCT_REGION_COL} values, got {got}"
    );
}
