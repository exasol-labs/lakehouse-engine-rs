#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, seed_events};
use common::stack::{
    exasol_host, exasol_sql_port, iceberg_catalog_url, iceberg_catalog_url_internal,
    local_stack_connection_password, wait_for_exasol, wait_for_iceberg_catalog, wait_for_minio,
};
use std::sync::{Mutex, MutexGuard, OnceLock};

const CONN_NAME: &str = "CREDEXP_CATALOG_CREDS";
const VS_NAME: &str = "CREDEXP_VS";
const OWNER_USER: &str = "CREDEXP_OWNER";
const OWNER_PASSWORD: &str = "CredExpOwner2026x";
const READER_USER: &str = "CREDEXP_READER";
const READER_PASSWORD: &str = "CredExpReader2026x";

static SETUP_DONE: OnceLock<()> = OnceLock::new();
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

fn owner_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), OWNER_USER, OWNER_PASSWORD)
}

fn reader_conn() -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), READER_USER, READER_PASSWORD)
}

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

fn scan_grant_sql(verb: &str, preposition: &str) -> String {
    format!("{verb} ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} {preposition} {OWNER_USER}")
}

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
            seed_events(&iceberg_catalog_url(), "s3://warehouse/")
                .await
                .expect("seed");
        });

        install_slc();
        upload_so();

        let mut sys = exa_conn();
        create_schema_and_scripts(&mut sys);

        let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {READER_USER} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {OWNER_USER} CASCADE"));
        sys.execute(&format!("DROP CONNECTION IF EXISTS {CONN_NAME}"));

        sys.execute(&format!("CREATE USER {OWNER_USER} IDENTIFIED BY \"{OWNER_PASSWORD}\""));
        for priv_name in ["CREATE SESSION", "CREATE CONNECTION", "CREATE VIRTUAL SCHEMA"] {
            sys.execute(&format!("GRANT {priv_name} TO {OWNER_USER}"));
        }
        for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, DISTRIBUTOR_SCRIPT_NAME] {
            sys.execute(&format!("GRANT EXECUTE ON SCRIPT {SCHEMA_NAME}.{script} TO {OWNER_USER}"));
        }

        sys.execute(&format!("CREATE USER {READER_USER} IDENTIFIED BY \"{READER_PASSWORD}\""));
        sys.execute(&format!("GRANT CREATE SESSION TO {READER_USER}"));

        let mut owner = owner_conn();
        create_virtual_schema_with_password(
            &mut owner,
            &VsProps::new(VS_NAME, E2E_NAMESPACE).with_catalog_conn_name(CONN_NAME),
            &iceberg_catalog_url_internal(),
            &local_stack_connection_password(),
        );
        owner.execute(&format!("GRANT SELECT ON SCHEMA {VS_NAME} TO {READER_USER}"));
    });
}

#[test]
fn a_least_privilege_reader_gets_the_owners_rows_without_a_connection_grant() {
    let _serial = serial();
    setup_e2e();

    let count_sql = format!("SELECT COUNT(*) FROM {}", vs_table());
    let filtered_sql = format!("SELECT COUNT(*) FROM {} WHERE SCORE > 15.0", vs_table());

    let owner_total = owner_conn().query_scalar_i64(&count_sql);
    let owner_filtered = owner_conn().query_scalar_i64(&filtered_sql);
    assert_eq!(owner_total, SEED_TOTAL_ROWS as i64);
    assert_eq!(owner_filtered, SEED_ROWS_SCORE_GT_15 as i64);

    let mut reader = reader_conn();
    assert_eq!(reader.query_scalar_i64(&count_sql), owner_total);
    assert_eq!(reader.query_scalar_i64(&filtered_sql), owner_filtered);

    let mut sys = exa_conn();
    for view in ["EXA_DBA_CONNECTION_PRIVS", "EXA_DBA_ROLE_PRIVS"] {
        assert_eq!(sys.query_row_count(&format!("SELECT * FROM {view} WHERE GRANTEE = '{READER_USER}'")), 0);
    }
}

#[test]
fn the_readers_pushdown_plan_names_the_connection_and_carries_no_credential() {
    let _serial = serial();
    setup_e2e();
    let password = local_stack_connection_password();

    let plan = explain_virtual_sql(
        &mut reader_conn(),
        &format!("SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0", vs_table()),
    );

    assert!(!plan.trim().is_empty(), "EXPLAIN VIRTUAL returned empty");
    assert!(plan.contains(SCAN_SCRIPT_NAME), "no scan script: {plan}");
    assert!(
        plan.contains(&format!(r#""connection":{{"name":"{CONN_NAME}""#)),
        "no connection ref: {plan}"
    );

    for value in [&password.access_key, &password.secret_key] {
        assert!(!value.is_empty());
        assert!(!plan.contains(value.as_str()), "credential leaked: {plan}");
    }
    for key in [r#""access_key""#, r#""secret_key""#, r#""session_token""#, r#""s3":{"#, r#""inline":"#] {
        assert!(!plan.contains(key), "inline token {key} in: {plan}");
    }
}

#[test]
fn revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential() {
    let _serial = serial();
    setup_e2e();
    let password = local_stack_connection_password();
    let query = format!("SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0", vs_table());

    assert_eq!(reader_conn().query_row_count(&query), SEED_ROWS_SCORE_GT_15 as i64);

    let mut owner = owner_conn();
    owner.execute(&scan_grant_sql("REVOKE", "FROM"));
    let denied = reader_conn().try_execute(&query);
    owner.execute(&scan_grant_sql("GRANT", "TO"));

    assert_eq!(denied["status"].as_str(), Some("error"), "revoke must deny: {denied}");
    let msg = denied["exception"]["text"].as_str().unwrap_or("");
    for needle in [CONN_NAME, "GRANT ACCESS ON CONNECTION", &format!("FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}"), "OWNER of the virtual schema"] {
        assert!(msg.contains(needle), "denial must contain {needle:?}: {msg}");
    }
    for value in [&password.access_key, &password.secret_key] {
        assert!(!msg.contains(value.as_str()), "credential leaked: {msg}");
    }

    assert_eq!(reader_conn().query_row_count(&query), SEED_ROWS_SCORE_GT_15 as i64);
}
