//! Ad-hoc pushdown-payload capture tool, not part of `make test-e2e`. Runs
//! `CAPTURE_SQL` through `EXPLAIN VIRTUAL` and for real; optional
//! `CAPTURE_RESULT_SET_MAX_ROWS` caps the real execution. Usage: see
//! `scripts/capture-pushdown-payload.sh`.

mod common;
use common::e2e_harness::*;
use common::seed::{E2E_NAMESPACE, E2E_TYPED_TABLE, seed_typed_distinct_probe};
use common::stack::{
    iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_seaweedfs,
};

const VS_NAME: &str = "MY_LAKEHOUSE";

#[test]
fn capture_pushdown_payload() {
    let sql = std::env::var("CAPTURE_SQL").expect(
        "set CAPTURE_SQL to the statement to capture, e.g. via scripts/capture-pushdown-payload.sh",
    );

    wait_for_exasol();
    wait_for_seaweedfs();
    wait_for_iceberg_catalog();

    runtime().block_on(async {
        seed_typed_distinct_probe(&iceberg_catalog_url(), "s3://warehouse/")
            .await
            .expect("seed typed_distinct_probe")
    });

    install_slc();
    upload_so();

    let cap: Option<u32> = match std::env::var("CAPTURE_RESULT_SET_MAX_ROWS") {
        Ok(n) => Some(
            n.parse()
                .unwrap_or_else(|_| panic!("CAPTURE_RESULT_SET_MAX_ROWS must be a u32, got {n:?}")),
        ),
        Err(std::env::VarError::NotPresent) => None,
        Err(err) => panic!("CAPTURE_RESULT_SET_MAX_ROWS: {err}"),
    };
    let mut conn = exa_conn();
    create_schema_and_scripts(&mut conn);
    create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));

    let vs_sql = sql.replace(
        "{table}",
        &format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase()),
    );

    println!("\n=== CAPTURE_SQL ===\n{vs_sql}\n");

    println!("=== EXPLAIN VIRTUAL (adapter-generated scan SQL / scan-spec JSON) ===");
    let explain = explain_virtual_sql(&mut conn, &vs_sql);
    println!("{explain}\n");

    println!("=== Real execution ===");
    let resp = match cap {
        Some(n) => conn.try_execute_capped(&vs_sql, n),
        None => conn.try_execute(&vs_sql),
    };
    println!("{}\n", serde_json::to_string_pretty(&resp).unwrap());
}
