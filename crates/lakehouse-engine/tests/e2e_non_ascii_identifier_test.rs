//! Permanent E2E coverage for a non-ASCII (`ß`) Iceberg table and column name,
//! added by `refactor-col-types-guard-dedup` task 7 after a live capture (task 3)
//! showed `resolve_table_schema` uppercases `straße` to `STRASSE` (Rust's full-Unicode
//! `to_uppercase` expands `ß` to `SS`) before Exasol ever sees the name — so the
//! real-world risk worth guarding is "does a non-ASCII-named table/column work at
//! all", not a fold-divergence the same capture proved unreachable (see task 4).
//!
//! Seeds its own `e2e_nonascii` namespace (`common::seed::seed_non_ascii_identifier`)
//! so this table never enters any other suite's `createVirtualSchema` table
//! enumeration, per `vs-adapter/create-virtual-schema`'s added scenario.
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
/// `flatten_table_name`'s full-Unicode fold declares the Iceberg `straße` table
/// (and its `straße` column) as this uppercased, `ß`→`SS`-expanded name.
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

    // 1. SYS.EXA_ALL_TABLES / SYS.EXA_ALL_COLUMNS report both identifiers as the
    // uppercased, ß-to-SS-expanded name — the casing behavior task 3 surfaced.
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

    // 2. Row-count sanity: the uppercased table name still resolves through
    // TABLE_MAP back to the original-cased Iceberg identifier.
    let row_count = conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {qualified_table}"));
    assert_eq!(row_count, NONASCII_TOTAL_ROWS);

    // 3. Projection round-trips the seeded values in full.
    let projected = conn.query_columns(&format!(
        "SELECT {SERVED_NAME} FROM {qualified_table} ORDER BY ID"
    ));
    let projected_values: Vec<&str> = projected[0].iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(projected_values, NONASCII_VALUES.to_vec());

    // 4. A LIKE predicate over the straße column returns the correct row subset.
    let like_sql = format!(
        "SELECT ID FROM {qualified_table} WHERE {SERVED_NAME} LIKE '{NONASCII_LIKE_PATTERN}'"
    );
    let matched_rows = conn.query_row_count(&like_sql);
    assert_eq!(matched_rows, NONASCII_LIKE_MATCH_COUNT);

    // 5. The load-bearing assertion: the same LIKE query was PUSHED, not
    // declined. A decline returns the identical row subset (the recorded
    // `vs-adapter/pushdown-planning-like-type-coercion` spec states three
    // times that a decline is correct), so only the generated pushdown SQL
    // discriminates a resolved `col_types` lookup from a fail-safe decline.
    //
    // Field presence alone settles that: the WHERE clause holds exactly one
    // predicate, so a declined guard drops the whole top-level filter and
    // `CommonScanSpec::filter` (`skip_serializing_if = "Option::is_none"`)
    // vanishes from the scan spec — the same reasoning `assert_filter_pushed_down`
    // in tests/e2e_capability_test.rs relies on. Do NOT bolster it with a
    // substring check on SERVED_NAME, the LIKE pattern or `predicate_like`:
    // `explain_virtual_sql` flattens the echoed `pushdownRequest` Exasol sent
    // into the same blob, where all three appear whether the adapter pushed the
    // filter or declined it, so such an assertion could never fail.
    let pushed_sql = explain_virtual_sql(&mut conn, &like_sql);
    assert!(
        pushed_sql.contains("\"filter\":\""),
        "EXPLAIN VIRTUAL output must contain a non-empty 'filter' field (the LIKE \
         predicate must be pushed, not declined), got:\n{pushed_sql}"
    );
}
