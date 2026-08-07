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
/// every seeded row rather than a prefix. Confirmed by task 1.2's live capture
/// (shape 1, bare projection): the uncapped connection's scan spec is
/// byte-identical to a capped one and carries no `limit` key
/// (`specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md`).
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

/// A declared row cap truncates the delivered result set and leaves the adapter
/// exchange untouched: the identical statement issued through a capped and an
/// uncapped connection generates a byte-identical pushed plan, and only the
/// delivered row count differs. Pinned in this shape — rather than as a pushed
/// `limit` — because task 1.2 diffed both variants of all seven statement shapes
/// against the live stack and found none that converts a declared cap into a
/// pushdown `limit`, with controls c1/c3/c6 ruling out the cap simply not
/// arriving (`specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md`,
/// and `docs/debugging-pushdown.md` for the permanent shape matrix).
#[test]
fn declared_cap_truncates_delivered_result_set_not_pushdown_request() {
    setup_e2e();

    const CAP_ROWS: u32 = 5;
    const _: () = assert!((CAP_ROWS as usize) < TYPED_TABLE_TOTAL_ROWS);

    let sql = format!("SELECT {TYPED_COL_VARCHAR} FROM {}", typed_table());
    let mut capped = exa_conn().capped_result_sets(CAP_ROWS);
    let mut uncapped = exa_conn();

    let capped_plan = explain_virtual_sql(&mut capped, &sql);
    let uncapped_plan = explain_virtual_sql(&mut uncapped, &sql);

    assert!(
        uncapped_plan.contains("LAKEHOUSE_SCAN"),
        "the two plans being compared must be generated scan plans, not empty \
         or error output, got:\n{uncapped_plan}"
    );
    assert!(
        !capped_plan.contains("\"limit\""),
        "a declared cap of {CAP_ROWS} must reach neither the pushdown request \
         nor the generated scan spec as a 'limit', got:\n{capped_plan}"
    );
    assert_eq!(
        capped_plan, uncapped_plan,
        "a declared cap must leave the whole adapter exchange untouched — the \
         capped and uncapped plans for the same statement must not differ in \
         any field"
    );

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
