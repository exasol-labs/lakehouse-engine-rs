//! End-to-end test for the `LAKEHOUSE_VERSION` entry point.
//!
//! Runs against a live Exasol container. FAILS (never skips) when the
//! container is unavailable — per project rules.
//!
//! Unlike the other E2E suites, this one needs no MinIO, no Iceberg REST
//! catalog, no CONNECTION, and no Virtual Schema: `LAKEHOUSE_VERSION()` is a
//! zero-argument, zero-I/O call, so setup is limited to installing the SLC,
//! uploading the `.so`, and creating the schema + scripts.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::stack::wait_for_exasol;

use std::sync::OnceLock;

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
    });
}

#[test]
fn version_udf_returns_the_compiled_crate_version() {
    setup_e2e();
    let mut conn = exa_conn();

    let resp = conn.execute(&format!(
        "SELECT {SCHEMA_NAME}.{VERSION_SCRIPT_NAME}() AS REPORTED_VERSION"
    ));
    let value = resp["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or_else(|| panic!("expected a VARCHAR scalar from LAKEHOUSE_VERSION(), got {resp}"))
        .to_string();

    assert_eq!(
        value,
        env!("CARGO_PKG_VERSION"),
        "LAKEHOUSE_VERSION() must report the compiled crate version"
    );
}
