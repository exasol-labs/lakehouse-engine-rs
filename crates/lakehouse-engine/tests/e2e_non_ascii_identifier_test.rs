//! Seeds its own `e2e_nonascii` namespace so the `straße` table never enters
//! another suite's `createVirtualSchema` table enumeration.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::seed::{
    E2E_NONASCII_NAMESPACE, NONASCII_LIKE_MATCH_COUNT, NONASCII_LIKE_PATTERN, NONASCII_TOTAL_ROWS,
    NONASCII_VALUES, seed_non_ascii_identifier,
};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};

const VS_NAME: &str = "STRASSE_VS";
/// Full-Unicode `to_uppercase` expands `ß` to `SS` for both table and column.
const SERVED_NAME: &str = "STRASSE";

#[test]
fn non_ascii_table_and_column_stay_queryable() {
    wait_for_exasol();
    wait_for_minio();
    wait_for_iceberg_catalog();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        seed_non_ascii_identifier(&iceberg_catalog_url(), "s3://warehouse/")
            .await
            .expect("seed straße table")
    });

    install_slc();
    upload_so();

    let mut conn = exa_conn();
    create_schema_and_scripts(&mut conn);
    create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NONASCII_NAMESPACE));

    let qualified_table = format!("{VS_NAME}.{SERVED_NAME}");

    let tables = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA='{VS_NAME}'"
    ));
    let table_names: Vec<&str> = tables[0].iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(
        table_names,
        vec![SERVED_NAME],
        "expected the straße table declared as {SERVED_NAME}, got {table_names:?}"
    );

    let columns = conn.query_columns(&format!(
        "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='{VS_NAME}' \
         AND COLUMN_TABLE='{SERVED_NAME}' ORDER BY COLUMN_NAME"
    ));
    let column_names: Vec<&str> = columns[0].iter().filter_map(|v| v.as_str()).collect();
    assert!(
        column_names.contains(&SERVED_NAME),
        "expected the straße column declared as {SERVED_NAME}, got {column_names:?}"
    );

    // The uppercased name must resolve through TABLE_MAP to the original-cased identifier.
    let row_count = conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {qualified_table}"));
    assert_eq!(row_count, NONASCII_TOTAL_ROWS);

    let projected = conn.query_columns(&format!(
        "SELECT {SERVED_NAME} FROM {qualified_table} ORDER BY ID"
    ));
    let projected_values: Vec<&str> = projected[0].iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(projected_values, NONASCII_VALUES.to_vec());

    let like_sql = format!(
        "SELECT ID FROM {qualified_table} WHERE {SERVED_NAME} LIKE '{NONASCII_LIKE_PATTERN}'"
    );
    let matched_rows = conn.query_row_count(&like_sql);
    assert_eq!(matched_rows, NONASCII_LIKE_MATCH_COUNT);

    // A decline returns the same rows, so only the pushed scan spec proves the LIKE
    // was pushed. With a single predicate, a decline omits `filter` entirely. Do not
    // substring-match the column or pattern: the echoed `pushdownRequest` in the
    // EXPLAIN blob contains them either way.
    let pushed_sql = explain_virtual_sql(&mut conn, &like_sql);
    assert!(
        pushed_sql.contains("\"filter\":\""),
        "EXPLAIN VIRTUAL output must contain a non-empty 'filter' field (the LIKE \
         predicate must be pushed, not declined), got:\n{pushed_sql}"
    );
}
