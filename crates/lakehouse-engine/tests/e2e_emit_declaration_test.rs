//! Permanent E2E coverage for issue #399: scan output values stay correct now
//! that the scan spec carries no `emit_exa_types` copy of the declared column
//! types — the generated `EMITS` clause is the sole authority, and the scan
//! reads it back via `UdfContext::output_column` at runtime. See
//! `specs/_plans/remove-emit-exa-types/e2e-harness/e2e-harness-scan-correctness`.
//!
//! Deliberately does NOT compare the declared `EMITS` list against the SDK's
//! runtime `output_column` accessors — that agreement is the SDK/SLC's own
//! contract (`language-container-rs` populates both from the one parsed
//! `EMITS` clause the engine sent) and was proven once, at implementation
//! time, against this same local Exasol Docker container (task 1.6). A
//! recurring comparison here would re-test that upstream guarantee rather
//! than this repo's own logic.
//!
//! Seeds `typed_distinct_probe` and `complex_probe` (`common::seed`), the two
//! fixtures that together span the type mix: bare `long`, `decimal`,
//! `double`, `string`, `date`, `timestamp`, and `boolean` columns, a
//! projected `CAST(id AS DECIMAL(5,0))` (the only reachable probe for the
//! `Int32` bin — no catalog-declared column in these fixtures bins there),
//! and `list`/`struct`/`map` columns the adapter declares `VARCHAR(2000000)`
//! and renders as JSON.
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

/// Seed both fixtures and provision the shared VS, once per binary — each E2E
/// binary runs in its own process, so setup is intentionally not shared with
/// the other `e2e_*_test.rs` files (mirrors `e2e_complex_type_test.rs`).
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

/// The generated scan-driving SQL alone (`EXPLAIN VIRTUAL`'s `PUSHDOWN_SQL`
/// column), without Exasol's echoed request JSON — the only surface on which
/// a scan-spec field assertion is meaningful, since the echoed request
/// repeats the user's own select list and would satisfy a naive substring
/// probe. Duplicated from `e2e_scan_test.rs` per this crate's E2E-binary
/// convention: each binary is self-contained.
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

/// The inner text of the top-level `EMITS (...)` clause in `pushed_sql`,
/// depth-aware so a nested `DECIMAL(p,s)` type's own parens do not end the
/// scan early.
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

/// Split `s` on top-level commas only, so a nested `DECIMAL(p,s)` type's own
/// comma is not mistaken for an item separator.
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

/// Assert `pushed`'s scan spec carries no `emit_exa_types` key and its
/// top-level `EMITS (...)` clause declares exactly `expected_types`, one per
/// select-list item, each as a substring of its item (identifiers are not
/// pinned — a bare column keeps its real name, an expression gets a
/// positional synthetic alias).
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

/// Scenario: the scan returns correct values across the type mix with no
/// spec-carried emit types (issue #399).
///
/// `typed_distinct_probe` row `id=1` carries a bare `long`, `decimal`,
/// `double`, `string`, `date`, `timestamp`, and `boolean` column, plus a
/// projected `CAST(id AS DECIMAL(5,0))` reaching the `Int32` bin. The `date`/
/// `timestamp`/`boolean` columns are pinned via an equality `WHERE` predicate
/// rather than a returned-cell string comparison, so the assertion does not
/// depend on the session's TIMESTAMP display precision (bare `TIMESTAMP` vs
/// `TIMESTAMP(6)`, engine-version dependent). `complex_probe` row `id=1`
/// (`COMPLEX_ROW_POPULATED`) carries `list`, `struct`, and `map` columns the
/// adapter declares `VARCHAR(2000000)` and renders as JSON.
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
            "DECIMAL(20,0)",    // id (long) -> ExaType::Int64
            "DECIMAL(9,2)",     // c_decimal_a -> ExaType::Numeric
            "DOUBLE PRECISION", // c_double -> ExaType::Double
            "VARCHAR(2000000)", // c_varchar -> ExaType::String
            "DATE",             // c_date -> ExaType::Date
            "TIMESTAMP",        // c_ts -> ExaType::Timestamp (bare or TIMESTAMP(6))
            "BOOLEAN",          // c_bool -> ExaType::Boolean
            "DECIMAL(5,0)",     // CAST(id AS DECIMAL(5,0)) -> ExaType::Int32
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
            "DECIMAL(20,0)",    // id (long) -> ExaType::Int64
            "VARCHAR(2000000)", // tags (list<string>) -> JSON via VARCHAR
            "VARCHAR(2000000)", // addr (struct) -> JSON via VARCHAR
            "VARCHAR(2000000)", // attrs (map<string,string>) -> JSON via VARCHAR
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
