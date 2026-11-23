//! A statement carries no row cap unless its call site sends one (#312).
//!
//! All tests FAIL (never skip) when the stack is unavailable.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::result_row_count;
use crate::common::seed::{TYPED_COL_VARCHAR, TYPED_TABLE_TOTAL_ROWS};
use crate::fixtures::{lakehouse_vs, typed_table};

#[test]
fn undeclared_cap_pushes_no_limit() {
    lakehouse_vs();
    let mut conn = exa_conn();

    let sql = format!("SELECT {TYPED_COL_VARCHAR} FROM {}", typed_table());

    let pushed_sql = explain_virtual_sql(&mut conn, &sql);
    assert!(
        !pushed_sql.contains("\"limit\""),
        "an undeclared-cap connection must push no 'limit' into the generated \
         scan spec, got:\n{pushed_sql}"
    );

    let row_count = conn.query_row_count(&sql);
    assert_eq!(
        row_count, TYPED_TABLE_TOTAL_ROWS as i64,
        "an undeclared-cap connection must return every seeded row, not a \
         truncated prefix, got {row_count}"
    );
}

// Only the delivered count is asserted: `EXPLAIN VIRTUAL` never carries a
// cap-derived pushdown `limit`, so it cannot observe one.
#[test]
fn declared_cap_truncates_returned_row_count() {
    lakehouse_vs();

    const CAP_ROWS: u32 = 5;
    const _: () = assert!((CAP_ROWS as usize) < TYPED_TABLE_TOTAL_ROWS);

    let sql = format!("SELECT {TYPED_COL_VARCHAR} FROM {}", typed_table());
    let mut conn = exa_conn();

    let capped = conn.try_execute_capped(&sql, CAP_ROWS);
    conn.assert_ok(&capped, &sql);
    let capped_rows = result_row_count(&capped)
        .unwrap_or_else(|| panic!("expected numRows from the capped statement: {capped}"));
    let uncapped_rows = conn.query_row_count(&sql);
    assert_eq!(
        capped_rows, CAP_ROWS as i64,
        "a declared cap of {CAP_ROWS} must truncate the delivered result set to \
         exactly {CAP_ROWS} of the fixture's {TYPED_TABLE_TOTAL_ROWS} rows, got \
         {capped_rows}"
    );
    assert_eq!(
        uncapped_rows, TYPED_TABLE_TOTAL_ROWS as i64,
        "the next statement on the same connection declares no cap and must deliver \
         every seeded row, got {uncapped_rows}"
    );
}
