//! Permanent regression coverage for the harness's row-cap default
//! (`fix-e2e-harness-undeclared-limit`, issue #312): `exa_conn()` must declare
//! no row cap unless a call site asks, per `ExaConn::capped_result_sets`'s doc
//! comment.
//!
//! Seeds only `typed_distinct_probe` (12 rows) through the shared
//! `common::e2e_harness` provisioning helpers — the cheapest fixture task 1.2's
//! live capture already exercised for the bare-projection shape — per
//! `e2e-harness/e2e-harness`'s "every E2E binary provisions the scan path from
//! one shared harness definition".
//!
//! All tests FAIL (never skip) when the stack is unavailable — per project rules.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::seed::{
    E2E_NAMESPACE, E2E_TYPED_TABLE, TYPED_COL_VARCHAR, TYPED_TABLE_TOTAL_ROWS,
    seed_typed_distinct_probe,
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

fn typed_table() -> String {
    format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase())
}

/// A bare projection carrying no SQL `LIMIT`, issued through a connection that
/// declares no cap, pushes no `limit` into the generated scan spec and returns
/// every seeded row rather than a prefix — the harness default this plan
/// establishes. Says nothing about a *declared* cap, which does reach the
/// adapter as a pushdown `limit`; see
/// `declared_cap_truncates_returned_row_count` below.
#[test]
fn undeclared_cap_pushes_no_limit() {
    setup_e2e();
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

/// A declared row cap truncates the delivered result set: the same bare
/// projection returns exactly the capped row count through a capped connection
/// and the fixture's full row count through an uncapped one.
///
/// Scoped deliberately to the DELIVERED row count. A declared cap is not inert on
/// the adapter exchange — on a real query execution it reaches the adapter as a
/// pushdown `limit`. That effect is invisible here because `EXPLAIN VIRTUAL` is a
/// separate exchange that never carries a cap-derived limit, so no assertion in
/// this file could observe it. The proof lives elsewhere: direct capture of the
/// adapter's incoming request
/// (`specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md`) for the
/// limit itself.
///
/// A pushed limit is no longer itself a plan-shape consequence: a bare `LIMIT`
/// stays broadcast-eligible, and only a join request's OTHER forcing conditions
/// (aggregate, GROUP BY, ORDER BY, HAVING) fall back to the N-scan wrapper — see
/// `JoinSpec::post_join_limit`.
#[test]
fn declared_cap_truncates_returned_row_count() {
    setup_e2e();

    const CAP_ROWS: u32 = 5;
    const _: () = assert!((CAP_ROWS as usize) < TYPED_TABLE_TOTAL_ROWS);

    let sql = format!("SELECT {TYPED_COL_VARCHAR} FROM {}", typed_table());
    let mut capped = exa_conn().capped_result_sets(CAP_ROWS);
    let mut uncapped = exa_conn();

    let capped_rows = capped.query_row_count(&sql);
    let uncapped_rows = uncapped.query_row_count(&sql);
    assert_eq!(
        capped_rows, CAP_ROWS as i64,
        "a declared cap of {CAP_ROWS} must truncate the delivered result set to \
         exactly {CAP_ROWS} of the fixture's {TYPED_TABLE_TOTAL_ROWS} rows, got \
         {capped_rows}"
    );
    assert_eq!(
        uncapped_rows, TYPED_TABLE_TOTAL_ROWS as i64,
        "the no-cap connection must still deliver every seeded row, got \
         {uncapped_rows}"
    );
}
