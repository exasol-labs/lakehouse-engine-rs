//! The generated `EMITS` clause is the sole authority for scan output types (#399).
//!
//! Does not compare `EMITS` against the SDK's runtime `output_column`: that
//! agreement is the SDK/SLC's own contract.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{
    COMPLEX_ROW_POPULATED, E2E_COMPLEX_TABLE, E2E_NAMESPACE, E2E_TYPED_TABLE, TYPED_COL_BOOL,
    TYPED_COL_DATE, TYPED_COL_DECIMAL_A, TYPED_COL_DOUBLE, TYPED_COL_TS, TYPED_COL_VARCHAR,
    seed_complex_types_probe, seed_typed_distinct_probe,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};
use serde_json::json;
use std::sync::OnceLock;

const VS_NAME: &str = "EMIT_DECL_VS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

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
            seed_typed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed typed_distinct_probe table");
            seed_complex_types_probe(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed complex_probe table");
        });

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));
    });
}

fn typed_table() -> String {
    format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase())
}

fn complex_table() -> String {
    format!("{VS_NAME}.{}", E2E_COMPLEX_TABLE.to_uppercase())
}

/// `PUSHDOWN_SQL` only: the echoed request JSON repeats the user's select list
/// and would satisfy a naive substring probe.
fn explain_virtual_pushdown_sql(conn: &mut ExaConn, query_sql: &str) -> String {
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    cols[1]
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

fn emits_clause(pushed_sql: &str) -> String {
    let marker = "EMITS (";
    let start = pushed_sql
        .find(marker)
        .unwrap_or_else(|| panic!("no EMITS clause in pushed SQL:\n{pushed_sql}"))
        + marker.len();
    let mut depth = 1i32;
    let mut end = pushed_sql.len();
    for (i, c) in pushed_sql[start..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    pushed_sql[start..end].to_string()
}

fn split_top_level(s: &str) -> Vec<String> {
    let mut depth = 0i32;
    let mut parts = Vec::new();
    let mut current = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

/// Types are matched as substrings: an expression item gets a synthetic alias.
fn assert_emits_clause_declares(pushed: &str, expected_types: &[&str]) {
    assert!(
        !pushed.contains("emit_exa_types"),
        "the generated scan spec must carry no 'emit_exa_types' key, got:\n{pushed}"
    );
    let items = split_top_level(&emits_clause(pushed));
    assert_eq!(
        items.len(),
        expected_types.len(),
        "EMITS clause must declare one Exasol type per select-list item, got:\n{pushed}"
    );
    for (item, ty) in items.iter().zip(expected_types) {
        assert!(
            item.contains(ty),
            "EMITS item {item:?} must declare {ty}, got:\n{pushed}"
        );
    }
}

/// Scenario: the scan returns correct values across the type mix with no spec-carried emit types
// date/timestamp/boolean are pinned via WHERE, not cell comparison, so the test
// is independent of the engine-version-dependent TIMESTAMP display precision.
#[test]
fn scan_returns_correct_values_across_type_mix_with_no_spec_carried_emit_types() {
    setup();
    let mut conn = exa_conn();

    let typed_sql = format!(
        "SELECT id, {a}, {d}, {v}, {dt}, {ts}, {b}, CAST(id AS DECIMAL(5,0)) FROM {table} \
         WHERE id = 1 AND {dt} = DATE '2024-01-01' \
         AND {ts} = TIMESTAMP '2024-01-01 00:00:00.100' AND {b} = TRUE",
        a = TYPED_COL_DECIMAL_A,
        d = TYPED_COL_DOUBLE,
        v = TYPED_COL_VARCHAR,
        dt = TYPED_COL_DATE,
        ts = TYPED_COL_TS,
        b = TYPED_COL_BOOL,
        table = typed_table(),
    );

    let pushed = explain_virtual_pushdown_sql(&mut conn, &typed_sql);
    assert_emits_clause_declares(
        &pushed,
        &[
            "DECIMAL(20,0)",
            "DECIMAL(9,2)",
            "DOUBLE PRECISION",
            "VARCHAR(2000000)",
            "DATE",
            "TIMESTAMP", // bare or TIMESTAMP(6)
            "BOOLEAN",
            "DECIMAL(5,0)", // the only reachable Int32-bin probe
        ],
    );

    let cols = conn.query_columns(&typed_sql);
    assert_eq!(
        cols[0].len(),
        1,
        "exactly one row must match id=1 with the seeded date/timestamp/boolean values, got:\n{cols:?}"
    );
    assert_eq!(
        parse_int(&cols[0][0]),
        1,
        "id must be 1, got {:?}",
        cols[0][0]
    );
    assert!(
        (parse_numeric(&cols[1][0]) - 10.50).abs() < 0.001,
        "c_decimal_a must be 10.50, got {:?}",
        cols[1][0]
    );
    assert!(
        (parse_numeric(&cols[2][0]) - 0.5).abs() < 0.001,
        "c_double must be 0.5, got {:?}",
        cols[2][0]
    );
    assert_eq!(
        cols[3][0].as_str(),
        Some("aa"),
        "c_varchar must be \"aa\", got {:?}",
        cols[3][0]
    );
    assert_eq!(
        parse_int(&cols[7][0]),
        1,
        "CAST(id AS DECIMAL(5,0)) must be 1, got {:?}",
        cols[7][0]
    );

    let complex_sql = format!(
        "SELECT id, tags, addr, attrs FROM {} WHERE id = {COMPLEX_ROW_POPULATED}",
        complex_table()
    );
    let pushed_complex = explain_virtual_pushdown_sql(&mut conn, &complex_sql);
    assert_emits_clause_declares(
        &pushed_complex,
        &[
            "DECIMAL(20,0)",
            "VARCHAR(2000000)",
            "VARCHAR(2000000)",
            "VARCHAR(2000000)",
        ],
    );

    let complex_cols = conn.query_columns(&complex_sql);
    assert_eq!(
        complex_cols[0].len(),
        1,
        "exactly one row must match id={COMPLEX_ROW_POPULATED}, got:\n{complex_cols:?}"
    );
    let tags: serde_json::Value = serde_json::from_str(
        complex_cols[1][0]
            .as_str()
            .expect("tags must be a JSON string cell"),
    )
    .expect("tags must be valid JSON");
    assert_eq!(
        tags,
        json!(["hello", "world"]),
        "tags must render the seeded list"
    );
    let addr: serde_json::Value = serde_json::from_str(
        complex_cols[2][0]
            .as_str()
            .expect("addr must be a JSON string cell"),
    )
    .expect("addr must be valid JSON");
    assert_eq!(
        addr,
        json!({"street": "Main St", "city": "Berlin"}),
        "addr must render the seeded struct"
    );
    let attrs: serde_json::Value = serde_json::from_str(
        complex_cols[3][0]
            .as_str()
            .expect("attrs must be a JSON string cell"),
    )
    .expect("attrs must be valid JSON");
    assert_eq!(
        attrs,
        json!({"a": "1", "b": "2"}),
        "attrs must render the seeded map"
    );
}
