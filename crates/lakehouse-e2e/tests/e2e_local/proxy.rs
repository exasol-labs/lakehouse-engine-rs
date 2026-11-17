//! Every CONNECTION address here uses a `*-via-proxy` name that only the forward proxy
//! resolves, so a request that bypasses the proxy fails on DNS.

use crate::common::e2e_harness::{
    VsProps, create_virtual_schema_with_password, exa_conn, try_create_virtual_schema_with_password,
};
use crate::common::exasol_ws::error_text;
use crate::common::pos_delete_fixtures::{
    DELETION_VECTOR_TABLE, FILE_GRANULARITY_REMAINING_ROWS, FILE_GRANULARITY_TABLE,
    deletion_vector_remaining_ids,
};
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_TOTAL_ROWS};
use crate::common::stack::{
    ICEBERG_REST_VIA_PROXY, closed_proxy_url_internal, local_stack_connection_password,
    proxy_host_internal,
};
use crate::fixtures::{
    PROXY_DIRECT_VS, PROXY_ICEBERG_VS, PROXY_ROLE_VS, count, proxy_virtual_schemas, setup_proxy,
    vs_table,
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
    let closed_proxy = closed_proxy_url_internal();
    let env = [
        ("HTTP_PROXY", closed_proxy.as_str()),
        ("NO_PROXY", "seaweedfs,iceberg-rest"),
    ];
    setup_proxy(LHVS_PROXY_ENV, &env, &env);
    create_virtual_schema_with_password(
        &mut exa_conn(),
        &VsProps::new("PROXY_BYPASS_VS", E2E_NAMESPACE)
            .with_adapter_schema(LHVS_PROXY_ENV)
            .with_catalog_conn_name("PROXY_BYPASS_CREDS"),
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
    let closed = format!("{}:1", proxy_host_internal());
    let proxy = format!("http://proxyuser:{PROXY_PASSWORD}@{closed}");
    setup_proxy(LHVS_PROXY_ENV, &[("HTTP_PROXY", &proxy)], &[]);
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &VsProps::new("PROXY_CLOSED_VS", E2E_NAMESPACE)
            .with_adapter_schema(LHVS_PROXY_ENV)
            .with_catalog_conn_name("PROXY_CLOSED_CREDS"),
        ICEBERG_REST_VIA_PROXY,
        &local_stack_connection_password().via_proxy(),
    );
    let msg = error_text(&resp, "CREATE through an unreachable proxy");
    assert!(msg.contains(&closed), "{msg}");
    assert!(
        !msg.contains(PROXY_PASSWORD),
        "proxy password leaked: {msg}"
    );
}
