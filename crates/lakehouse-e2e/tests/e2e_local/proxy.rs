//! Every CONNECTION address here uses a `*-via-proxy` name that only the forward proxy
//! resolves, so a request that bypasses the proxy fails on DNS.

use crate::common::e2e_harness::{
    create_virtual_schema_with_password, exa_conn, try_create_virtual_schema_with_password,
};
use crate::common::exasol_ws::error_text;
use crate::common::pos_delete_fixtures::{
    DELETION_VECTOR_TABLE, FILE_GRANULARITY_REMAINING_ROWS, FILE_GRANULARITY_TABLE,
    deletion_vector_remaining_ids,
};
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_TOTAL_ROWS};
use crate::common::stack::{
    CLOSED_PROXY_URL_INTERNAL, ICEBERG_REST_VIA_PROXY, local_stack_connection_password,
};
use crate::fixtures::{
    PROXY_DIRECT_VS, PROXY_ICEBERG_VS, PROXY_ROLE_VS, count, password_via_proxy, proxy_props,
    proxy_virtual_schemas, setup_proxy, vs_table,
};

const PROXY_PASSWORD: &str = "pr0xy-s3cret";
/// The scripts of the tests that set a broken proxy, so `LHVS_PROXY` keeps the working one.
const LHVS_PROXY_ENV: &str = "LHVS_PROXY_ENV";

/// Scenario: Catalog, storage, and STS requests go through the proxy
#[test]
fn every_request_of_the_adapter_and_the_scan_goes_through_the_proxy() {
    proxy_virtual_schemas();
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table(PROXY_ICEBERG_VS, FILE_GRANULARITY_TABLE)
        )),
        FILE_GRANULARITY_REMAINING_ROWS as i64
    );
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table(PROXY_ICEBERG_VS, DELETION_VECTOR_TABLE)
        )),
        deletion_vector_remaining_ids().count() as i64
    );
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table(PROXY_DIRECT_VS, "rows")
        )),
        3
    );
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table(PROXY_ROLE_VS, E2E_TABLE)
        )),
        SEED_TOTAL_ROWS as i64
    );
}

/// Scenario: NO_PROXY hosts bypass the proxy
#[test]
fn no_proxy_hosts_bypass_an_unreachable_proxy() {
    let env = [
        ("HTTP_PROXY", CLOSED_PROXY_URL_INTERNAL),
        ("NO_PROXY", "seaweedfs,iceberg-rest"),
    ];
    setup_proxy(LHVS_PROXY_ENV, &env, &env);
    create_virtual_schema_with_password(
        &mut exa_conn(),
        &proxy_props("PROXY_BYPASS_VS", E2E_NAMESPACE, "PROXY_BYPASS_CREDS")
            .with_adapter_schema(LHVS_PROXY_ENV),
        "http://iceberg-rest:8181",
        &local_stack_connection_password(),
    );
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table("PROXY_BYPASS_VS", E2E_TABLE)
        )),
        SEED_TOTAL_ROWS as i64
    );
}

/// Scenario: An unreachable proxy fails CREATE without leaking its credentials
#[test]
fn an_unreachable_proxy_fails_create_naming_the_proxy_without_its_password() {
    let proxy = format!("http://proxyuser:{PROXY_PASSWORD}@10.84.0.12:1");
    setup_proxy(LHVS_PROXY_ENV, &[("HTTP_PROXY", &proxy)], &[]);
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &proxy_props("PROXY_CLOSED_VS", E2E_NAMESPACE, "PROXY_CLOSED_CREDS")
            .with_adapter_schema(LHVS_PROXY_ENV),
        ICEBERG_REST_VIA_PROXY,
        &password_via_proxy(),
    );
    let msg = error_text(&resp, "CREATE through an unreachable proxy");
    assert!(msg.contains("10.84.0.12:1"), "{msg}");
    assert!(
        !msg.contains(PROXY_PASSWORD),
        "proxy password leaked: {msg}"
    );
}
