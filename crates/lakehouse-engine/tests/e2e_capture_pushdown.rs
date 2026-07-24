//! Ad-hoc pushdown-payload capture tool — NOT part of `make test-e2e`.
//!
//! This repo has no reusable way to inspect what a given SQL statement pushes
//! down through the Virtual Schema, short of a throwaway instrumentation spike
//! (see commit c827d1a) redone from scratch each time. This binary is the
//! reusable replacement: it stands up the shared E2E fixture (`typed_distinct_probe`,
//! which carries VARCHAR/DATE/TIMESTAMP/DECIMAL/DOUBLE/BOOLEAN/INTEGER columns —
//! see `common::seed::E2E_TYPED_TABLE`) against the local Docker stack, then runs
//! a caller-supplied SQL statement through `EXPLAIN VIRTUAL` (showing the SQL the
//! adapter generates, including the literal scan-spec JSON passed to the scan UDF)
//! and, separately, for real (showing the actual runtime error/result).
//!
//! Usage: see `scripts/capture-pushdown-payload.sh` / `docs/debugging-pushdown.md`.
//! Driven entirely by the `CAPTURE_SQL` env var so future issues on this stack
//! (#211, #212, #210, #209) can reuse it without editing this file.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::seed::{E2E_NAMESPACE, E2E_TYPED_TABLE, seed_typed_distinct_probe};
use common::stack::{iceberg_catalog_url, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio};

const VS_NAME: &str = "MY_LAKEHOUSE";

#[test]
fn capture_pushdown_payload() {
    let sql = std::env::var("CAPTURE_SQL")
        .expect("set CAPTURE_SQL to the statement to capture, e.g. via scripts/capture-pushdown-payload.sh");

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
            .expect("seed typed_distinct_probe")
    });

    install_slc();
    upload_so();

    let mut conn = exa_conn();
    create_schema_and_scripts(&mut conn);
    create_virtual_schema(&mut conn, &VsProps::new(VS_NAME, E2E_NAMESPACE));

    let vs_sql = sql.replace(
        &format!("{{table}}"),
        &format!("{VS_NAME}.{}", E2E_TYPED_TABLE.to_uppercase()),
    );

    println!("\n=== CAPTURE_SQL ===\n{vs_sql}\n");

    println!("=== EXPLAIN VIRTUAL (adapter-generated scan SQL / scan-spec JSON) ===");
    let explain = explain_virtual_sql(&mut conn, &vs_sql);
    println!("{explain}\n");

    println!("=== Real execution ===");
    let resp = conn.try_execute(&vs_sql);
    println!("{}\n", serde_json::to_string_pretty(&resp).unwrap());
}
