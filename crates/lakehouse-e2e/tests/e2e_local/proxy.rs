//! Every CONNECTION address here uses a `*-via-proxy` name that only the forward proxy
//! resolves, so a request that bypasses the proxy fails on DNS.

use crate::common::e2e_harness::{
    VsProps, create_schema_and_scripts_with_env, create_virtual_schema_with_password, exa_conn,
    try_create_virtual_schema_with_password,
};
use crate::common::exasol_ws::error_text;
use crate::common::pos_delete_fixtures::{
    DELETION_VECTOR_TABLE, FILE_GRANULARITY_REMAINING_ROWS, FILE_GRANULARITY_TABLE, NAMESPACE,
    deletion_vector_remaining_ids,
};
use crate::common::raw_parquet::write_parquet_fixture;
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_TOTAL_ROWS};
use crate::common::stack::{
    ASSUME_ROLE_ARN, ASSUME_ROLE_BASE_ACCESS_KEY, ASSUME_ROLE_BASE_SECRET_KEY,
    ASSUME_ROLE_EXTERNAL_ID, CLOSED_PROXY_URL_INTERNAL, CatalogConnectionPassword,
    PROXY_URL_INTERNAL, assert_resolves_only_through_proxy, local_stack_connection_password,
    wait_for_proxy,
};
use crate::fixtures::{events, lakehouse_vs, vs_table};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

const ADAPTER_SCHEMA: &str = "LHVS_PROXY";
const CATALOG_VIA_PROXY: &str = "http://iceberg-rest-via-proxy:8181";
const STORAGE_VIA_PROXY: &str = "http://seaweedfs-via-proxy:8333";
const BASE_DIRECT: &str = "s3://warehouse/proxy_direct/";
const PROXY_PASSWORD: &str = "pr0xy-s3cret";

fn setup(adapter_env: &[(&str, &str)], scan_env: &[(&str, &str)]) {
    lakehouse_vs();
    events();
    wait_for_proxy();
    assert_resolves_only_through_proxy("iceberg-rest-via-proxy");
    assert_resolves_only_through_proxy("seaweedfs-via-proxy");
    create_schema_and_scripts_with_env(&mut exa_conn(), ADAPTER_SCHEMA, adapter_env, scan_env);
}

fn password_via_proxy() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        endpoint: STORAGE_VIA_PROXY.to_string(),
        ..local_stack_connection_password()
    }
}

fn props<'a>(vs_name: &'a str, namespace: &'a str, conn_name: &'a str) -> VsProps<'a> {
    VsProps::new(vs_name, namespace)
        .with_adapter_schema(ADAPTER_SCHEMA)
        .with_catalog_conn_name(conn_name)
}

fn count(sql: &str) -> i64 {
    exa_conn().query_scalar_i64(sql)
}

fn refresh(vs_name: &str) {
    exa_conn().execute(&format!("ALTER VIRTUAL SCHEMA {vs_name} REFRESH"));
}

/// Scenario: Catalog, storage, and STS requests go through the proxy
#[test]
fn every_request_of_the_adapter_and_the_scan_goes_through_the_proxy() {
    let proxy = [("HTTP_PROXY", PROXY_URL_INTERNAL)];
    setup(&proxy, &proxy);
    write_parquet_fixture(
        &format!("{BASE_DIRECT}rows/file1.parquet"),
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("build direct-storage fixture batch"),
    );
    let mut conn = exa_conn();

    create_virtual_schema_with_password(
        &mut conn,
        &props("PROXY_ICEBERG_VS", NAMESPACE, "PROXY_ICEBERG_CREDS"),
        CATALOG_VIA_PROXY,
        &password_via_proxy(),
    );
    refresh("PROXY_ICEBERG_VS");
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table("PROXY_ICEBERG_VS", FILE_GRANULARITY_TABLE)
        )),
        FILE_GRANULARITY_REMAINING_ROWS as i64
    );
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table("PROXY_ICEBERG_VS", DELETION_VECTOR_TABLE)
        )),
        deletion_vector_remaining_ids().count() as i64
    );

    create_virtual_schema_with_password(
        &mut conn,
        &props("PROXY_DIRECT_VS", "", "PROXY_DIRECT_CREDS").with_catalog_kind("DIRECT_STORAGE"),
        BASE_DIRECT,
        &CatalogConnectionPassword {
            warehouse: String::new(),
            ..password_via_proxy()
        },
    );
    refresh("PROXY_DIRECT_VS");
    assert_eq!(count("SELECT COUNT(*) FROM PROXY_DIRECT_VS.ROWS"), 3);

    create_virtual_schema_with_password(
        &mut conn,
        &props("PROXY_ROLE_VS", E2E_NAMESPACE, "PROXY_ROLE_CREDS"),
        CATALOG_VIA_PROXY,
        &CatalogConnectionPassword {
            access_key: ASSUME_ROLE_BASE_ACCESS_KEY.to_string(),
            secret_key: ASSUME_ROLE_BASE_SECRET_KEY.to_string(),
            aws_assume_role_arn: Some(ASSUME_ROLE_ARN.to_string()),
            aws_external_id: Some(ASSUME_ROLE_EXTERNAL_ID.to_string()),
            aws_sts_endpoint: Some(STORAGE_VIA_PROXY.to_string()),
            ..password_via_proxy()
        },
    );
    refresh("PROXY_ROLE_VS");
    assert_eq!(
        count(&format!(
            "SELECT COUNT(*) FROM {}",
            vs_table("PROXY_ROLE_VS", E2E_TABLE)
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
    setup(&env, &env);
    create_virtual_schema_with_password(
        &mut exa_conn(),
        &props("PROXY_BYPASS_VS", E2E_NAMESPACE, "PROXY_BYPASS_CREDS"),
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
    setup(&[("HTTP_PROXY", &proxy)], &[]);
    let resp = try_create_virtual_schema_with_password(
        &mut exa_conn(),
        &props("PROXY_CLOSED_VS", E2E_NAMESPACE, "PROXY_CLOSED_CREDS"),
        CATALOG_VIA_PROXY,
        &password_via_proxy(),
    );
    let msg = error_text(&resp, "CREATE through an unreachable proxy");
    assert!(msg.contains("10.84.0.12:1"), "{msg}");
    assert!(
        !msg.contains(PROXY_PASSWORD),
        "proxy password leaked: {msg}"
    );
}
