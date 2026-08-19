//! Permanent E2E coverage for issue #350: Iceberg `list`/`struct`/`map` columns
//! scan and render as valid JSON end to end, keyed by logical field names, and
//! behave as the declared `VARCHAR(2000000)` in every pushdown shape.
//!
//! Seeds its own `complex_probe` table into the shared `e2e_lakehouse` namespace
//! (`common::seed::seed_complex_types_probe`) — see that seed for the exact row
//! layout `COMPLEX_ROW_POPULATED`/`COMPLEX_ROW_NULL`/`COMPLEX_ROW_EMPTY`/
//! `COMPLEX_ROW_ALT` reference.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    COMPLEX_ROW_ALT, COMPLEX_ROW_EMPTY, COMPLEX_ROW_NULL, COMPLEX_ROW_POPULATED,
    COMPLEX_TOTAL_ROWS, E2E_COMPLEX_JOIN_TABLE, E2E_COMPLEX_TABLE, E2E_NAMESPACE,
    seed_complex_types_join_probe, seed_complex_types_probe,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};
use serde_json::json;
use std::sync::OnceLock;

const VS_NAME: &str = "COMPLEX_TYPE_VS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

/// Seed the fixture and provision the shared VS, once per binary.
fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            seed_complex_types_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed complex-types probe table");
            seed_complex_types_join_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed complex-types join partner table")
        });

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
    });
}

/// The Exasol-served name of the seeded `complex_probe` table.
fn served_table() -> String {
    E2E_COMPLEX_TABLE.to_uppercase()
}

/// The declared `COLUMN_TYPE` for `column`, whitespace-stripped.
fn declared_type(conn: &mut ExaConn, table: &str, column: &str) -> String {
    let ty = conn.query_columns(&format!(
        "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS \
         WHERE COLUMN_SCHEMA='{VS_NAME}' AND COLUMN_TABLE='{table}' AND COLUMN_NAME='{column}'"
    ))[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("{column} has no declared type"))
        .to_string();
    ty.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Assert `cell` parses as JSON equal to `expected`, or is SQL NULL when
/// `expected` is `None`.
fn assert_rendered(cell: &serde_json::Value, expected: Option<serde_json::Value>, context: &str) {
    match (cell, expected) {
        (serde_json::Value::Null, None) => {}
        (serde_json::Value::String(text), Some(expected)) => {
            let parsed: serde_json::Value = serde_json::from_str(text)
                .unwrap_or_else(|e| panic!("{context}: {text} must parse as JSON: {e}"));
            assert_eq!(parsed, expected, "{context}: unexpected rendering {text}");
        }
        (other, expected) => panic!("{context}: expected {expected:?}, got {other:?}"),
    }
}

/// Scenario: An Iceberg table's list, struct, and map columns return valid JSON
/// end to end.
///
/// `complex_probe` carries every shape `datafusion-scan/nested-json-rendering`
/// renders: `list<string>` (`TAGS`), `list<int>` (`NUMS`),
/// `struct<street, city>` (`ADDR`), `map<string, string>` (`ATTRS`),
/// `map<int, string>` (`INT_MAP`), and `list<struct<a>>` (`ITEMS`) — each with a
/// fully-populated row, an all-NULL row, and a row exercising an empty
/// collection plus a NULL struct member.
#[test]
fn iceberg_nested_columns_return_valid_json_end_to_end() {
    setup();
    let mut conn = exa_conn();
    let table = served_table();
    let qualified = format!("{VS_NAME}.{table}");

    for column in ["TAGS", "NUMS", "ADDR", "ATTRS", "INT_MAP", "ITEMS"] {
        assert!(
            declared_type(&mut conn, &table, column).starts_with("VARCHAR(2000000)"),
            "{column} must be declared VARCHAR(2000000), got {}",
            declared_type(&mut conn, &table, column)
        );
    }

    let rows = conn.query_columns(&format!(
        "SELECT ID, TAGS, NUMS, ADDR, ATTRS, INT_MAP, ITEMS FROM {qualified} ORDER BY ID"
    ));
    assert_eq!(rows.len(), 7, "expected 7 projected columns: {rows:?}");
    assert_eq!(
        rows[0].len(),
        COMPLEX_TOTAL_ROWS,
        "expected {COMPLEX_TOTAL_ROWS} rows: {rows:?}"
    );

    let ids: Vec<i64> = rows[0].iter().map(parse_int).collect();
    assert_eq!(
        ids,
        vec![
            COMPLEX_ROW_POPULATED,
            COMPLEX_ROW_NULL,
            COMPLEX_ROW_EMPTY,
            COMPLEX_ROW_ALT
        ]
    );
    let row_index = |id: i64| ids.iter().position(|&x| x == id).unwrap();

    let (tags, nums, addr, attrs, int_map, items) =
        (&rows[1], &rows[2], &rows[3], &rows[4], &rows[5], &rows[6]);

    assert_rendered(
        &tags[row_index(COMPLEX_ROW_POPULATED)],
        Some(json!(["hello", "world"])),
        "TAGS populated",
    );
    assert_eq!(
        tags[row_index(COMPLEX_ROW_POPULATED)].as_str(),
        Some("[\"hello\",\"world\"]"),
        "list<string> must render QUOTED elements, not the Arrow display text [hello, world]"
    );
    assert_rendered(&tags[row_index(COMPLEX_ROW_NULL)], None, "TAGS null");
    assert_rendered(
        &tags[row_index(COMPLEX_ROW_EMPTY)],
        Some(json!([])),
        "TAGS empty",
    );
    assert_rendered(
        &tags[row_index(COMPLEX_ROW_ALT)],
        Some(json!(["foo", "bar", "baz"])),
        "TAGS alt",
    );

    assert_rendered(
        &nums[row_index(COMPLEX_ROW_POPULATED)],
        Some(json!([1, 2, 3])),
        "NUMS populated",
    );
    assert_rendered(&nums[row_index(COMPLEX_ROW_NULL)], None, "NUMS null");
    assert_rendered(
        &nums[row_index(COMPLEX_ROW_EMPTY)],
        Some(json!([])),
        "NUMS empty",
    );
    assert_rendered(
        &nums[row_index(COMPLEX_ROW_ALT)],
        Some(json!([9, 8])),
        "NUMS alt",
    );

    assert_rendered(
        &addr[row_index(COMPLEX_ROW_POPULATED)],
        Some(json!({"street": "Main St", "city": "Berlin"})),
        "ADDR populated",
    );
    assert_rendered(&addr[row_index(COMPLEX_ROW_NULL)], None, "ADDR null");
    assert_rendered(
        &addr[row_index(COMPLEX_ROW_EMPTY)],
        Some(json!({"street": "Empty Ave", "city": null})),
        "ADDR with a null member",
    );
    assert_rendered(
        &addr[row_index(COMPLEX_ROW_ALT)],
        Some(json!({"street": "Second St", "city": "Paris"})),
        "ADDR alt",
    );

    assert_rendered(
        &attrs[row_index(COMPLEX_ROW_POPULATED)],
        Some(json!({"a": "1", "b": "2"})),
        "ATTRS populated",
    );
    assert_rendered(&attrs[row_index(COMPLEX_ROW_NULL)], None, "ATTRS null");
    assert_rendered(
        &attrs[row_index(COMPLEX_ROW_EMPTY)],
        Some(json!({})),
        "ATTRS empty",
    );
    assert_rendered(
        &attrs[row_index(COMPLEX_ROW_ALT)],
        Some(json!({"x": "9"})),
        "ATTRS alt",
    );

    assert_rendered(
        &int_map[row_index(COMPLEX_ROW_POPULATED)],
        Some(json!({"1": "one", "2": "two"})),
        "INT_MAP populated — integer keys stringified into JSON object names",
    );
    assert_rendered(&int_map[row_index(COMPLEX_ROW_NULL)], None, "INT_MAP null");
    assert_rendered(
        &int_map[row_index(COMPLEX_ROW_EMPTY)],
        Some(json!({})),
        "INT_MAP empty",
    );
    assert_rendered(
        &int_map[row_index(COMPLEX_ROW_ALT)],
        Some(json!({"3": "three"})),
        "INT_MAP alt",
    );

    assert_rendered(
        &items[row_index(COMPLEX_ROW_POPULATED)],
        Some(json!([{"a": 1}, {"a": 2}])),
        "ITEMS populated",
    );
    assert_rendered(&items[row_index(COMPLEX_ROW_NULL)], None, "ITEMS null");
    assert_rendered(
        &items[row_index(COMPLEX_ROW_EMPTY)],
        Some(json!([])),
        "ITEMS empty",
    );
    assert_rendered(
        &items[row_index(COMPLEX_ROW_ALT)],
        Some(json!([{"a": 3}])),
        "ITEMS alt",
    );
}

/// Assert `sql` reaches the scan UDF rather than an unaccelerated fallback,
/// naming `shape` in the failure so a shape that stops being pushed is identified
/// without reading the captured plan.
fn assert_pushed_to_scan_udf(conn: &mut ExaConn, sql: &str, shape: &str) {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "{shape} must drive the scan UDF: {pushed}"
    );
}

/// Scenario: Every pushdown shape treats a nested column as the VARCHAR Exasol
/// declared.
///
/// A predicate over a `list` column returned EVERY row before this feature, so the
/// WHERE fixture is chosen to discriminate: it must match exactly one of four rows,
/// and a conjunction with a primitive predicate must narrow further, not widen back
/// to every row.
///
/// Each shape is captured with `EXPLAIN VIRTUAL` as well as executed, so a shape
/// that silently stops reaching the scan UDF is caught even while its rows stay
/// right. The join condition compares the rendered column against a SECOND,
/// distinct table (`complex_join_probe`) rather than an alias of the probe itself.
#[test]
fn nested_columns_push_down_as_the_declared_varchar_in_every_shape() {
    setup();
    let mut conn = exa_conn();
    let table = served_table();
    let qualified = format!("{VS_NAME}.{table}");
    let join_partner = format!("{VS_NAME}.{}", E2E_COMPLEX_JOIN_TABLE.to_uppercase());

    let single_match_sql =
        format!("SELECT ID FROM {qualified} WHERE TAGS = '[\"hello\",\"world\"]'");
    let single_match = conn.query_columns(&single_match_sql);
    let matched_ids: Vec<i64> = single_match[0].iter().map(parse_int).collect();
    assert_eq!(
        matched_ids,
        vec![COMPLEX_ROW_POPULATED],
        "a predicate over TAGS must match only the row it names, not every row: {single_match:?}"
    );
    assert_pushed_to_scan_udf(
        &mut conn,
        &single_match_sql,
        "the WHERE predicate over TAGS",
    );

    let conjunction = conn.query_columns(&format!(
        "SELECT ID FROM {qualified} WHERE ID > 2 AND TAGS = '[\"foo\",\"bar\",\"baz\"]'"
    ));
    let conjunction_ids: Vec<i64> = conjunction[0].iter().map(parse_int).collect();
    assert_eq!(
        conjunction_ids,
        vec![COMPLEX_ROW_ALT],
        "ID > 2 AND TAGS = ... must narrow to the one row matching both: {conjunction:?}"
    );

    let group_by_sql = format!("SELECT TAGS, COUNT(*) FROM {qualified} GROUP BY TAGS");
    let groups = conn.query_columns(&group_by_sql);
    assert_eq!(
        groups[0].len(),
        COMPLEX_TOTAL_ROWS,
        "TAGS must GROUP BY like a VARCHAR, one group per distinct rendered value \
         (including the NULL row's own group): {groups:?}"
    );
    let counts: Vec<i64> = groups[1].iter().map(parse_int).collect();
    assert!(
        counts.iter().all(|&c| c == 1),
        "each TAGS group must hold exactly one row: {counts:?}"
    );
    assert_pushed_to_scan_udf(&mut conn, &group_by_sql, "the GROUP BY over TAGS");

    let order_by_sql = format!("SELECT ID FROM {qualified} WHERE TAGS IS NOT NULL ORDER BY TAGS");
    let ordered = conn.query_columns(&order_by_sql);
    let ordered_ids: Vec<i64> = ordered[0].iter().map(parse_int).collect();
    assert_eq!(
        ordered_ids,
        vec![COMPLEX_ROW_ALT, COMPLEX_ROW_POPULATED, COMPLEX_ROW_EMPTY],
        "ORDER BY TAGS must sort the rendered strings lexically, like a VARCHAR: {ordered:?}"
    );
    assert_pushed_to_scan_udf(&mut conn, &order_by_sql, "the ORDER BY over TAGS");

    let distinct_sql = format!("SELECT COUNT(DISTINCT TAGS) FROM {qualified}");
    assert_eq!(
        conn.query_scalar_i64(&distinct_sql),
        3,
        "COUNT(DISTINCT TAGS) must count the 3 distinct non-NULL rendered documents"
    );
    assert_pushed_to_scan_udf(&mut conn, &distinct_sql, "COUNT(DISTINCT TAGS)");

    let max_sql = format!("SELECT MAX(TAGS) FROM {qualified}");
    let max_tags = conn.query_columns(&max_sql);
    assert_eq!(
        max_tags[0][0].as_str(),
        Some("[]"),
        "MAX(TAGS) must compare the rendered strings as a VARCHAR aggregate argument: {max_tags:?}"
    );
    assert_pushed_to_scan_udf(&mut conn, &max_sql, "TAGS as an aggregate argument");

    let join_sql = format!(
        "SELECT p.ID, j.LABEL FROM {qualified} p \
         JOIN {join_partner} j ON p.TAGS = j.TAG_DOC ORDER BY p.ID"
    );
    let joined = conn.query_columns(&join_sql);
    let joined_ids: Vec<i64> = joined[0].iter().map(parse_int).collect();
    let joined_labels: Vec<&str> = joined[1].iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(
        (joined_ids, joined_labels),
        (
            vec![COMPLEX_ROW_POPULATED, COMPLEX_ROW_ALT],
            vec!["POPULAR", "ALT"]
        ),
        "a JOIN CONDITION over TAGS must pair each row with the partner naming its \
         rendered document, and pair the orphan document with nothing: {joined:?}"
    );
    assert_pushed_to_scan_udf(&mut conn, &join_sql, "the JOIN CONDITION over TAGS");

    let upper_sql =
        format!("SELECT UPPER(TAGS) FROM {qualified} WHERE ID = {COMPLEX_ROW_POPULATED}");
    let upper = conn.query_columns(&upper_sql);
    assert_eq!(
        upper[0][0].as_str(),
        Some("[\"HELLO\",\"WORLD\"]"),
        "a select-list scalar function over TAGS must operate on the rendered VARCHAR: {upper:?}"
    );
    assert_pushed_to_scan_udf(&mut conn, &upper_sql, "the select-list UPPER(TAGS)");
}

/// Scenario: A self-join on a nested JSON-rendered column (issue #361's second
/// repro, `FROM complex_probe a JOIN complex_probe b ON a.TAGS = b.TAGS`) pairs
/// each row only with itself. `COMPLEX_ROW_NULL` must match nothing — including
/// itself — since SQL `NULL = NULL` is never true.
#[test]
fn e2e_self_join_on_nested_json_column_matches_single_node() {
    setup();
    let mut conn = exa_conn();
    let table = served_table();
    let qualified = format!("{VS_NAME}.{table}");

    let join_sql = format!(
        "SELECT a.ID, b.ID FROM {qualified} a JOIN {qualified} b ON a.TAGS = b.TAGS \
         ORDER BY a.ID, b.ID"
    );
    let joined = conn.query_columns(&join_sql);
    let left_ids: Vec<i64> = joined[0].iter().map(parse_int).collect();
    let right_ids: Vec<i64> = joined[1].iter().map(parse_int).collect();
    let actual: Vec<(i64, i64)> = left_ids.into_iter().zip(right_ids).collect();
    assert_eq!(
        actual,
        vec![
            (COMPLEX_ROW_POPULATED, COMPLEX_ROW_POPULATED),
            (COMPLEX_ROW_EMPTY, COMPLEX_ROW_EMPTY),
            (COMPLEX_ROW_ALT, COMPLEX_ROW_ALT),
        ],
        "a self-join on TAGS must pair each row only with itself, and \
         COMPLEX_ROW_NULL must match nothing, not even itself: {actual:?}"
    );
    assert_pushed_to_scan_udf(&mut conn, &join_sql, "the self-join ON TAGS");
}
