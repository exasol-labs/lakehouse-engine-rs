//! Regression coverage for #135, #378: the CONNECTION's storage credential must
//! never reach pushdown SQL; the scan resolves it via grant-gated
//! `ctx.connection()`. Uses a non-DBA VS owner because the pushdown-path
//! CONNECTION check is evaluated against the VS OWNER, not the querying user
//! (verified live on 2025.2.1). All tests FAIL (never skip) without the stack.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS, seed_events};
use common::stack::{
    CatalogConnectionPassword, exasol_host, exasol_sql_port, iceberg_catalog_url,
    iceberg_catalog_url_internal, local_stack_connection_password, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
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
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn catalog_password() -> CatalogConnectionPassword {
    local_stack_connection_password()
}

fn owner_conn() -> ExaConn {
    ExaConn::connect(
        &exasol_host(),
        exasol_sql_port(),
        OWNER_USER,
        OWNER_PASSWORD,
    )
}

fn reader_conn() -> ExaConn {
    ExaConn::connect(
        &exasol_host(),
        exasol_sql_port(),
        READER_USER,
        READER_PASSWORD,
    )
}

fn vs_table() -> String {
    format!("{VS_NAME}.{}", E2E_TABLE.to_uppercase())
}

fn grant_owner_scan_access(owner: &mut ExaConn) {
    owner.execute(&format!(
        "GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         TO {OWNER_USER}"
    ));
}

fn revoke_owner_scan_access(owner: &mut ExaConn) {
    owner.execute(&format!(
        "REVOKE ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         FROM {OWNER_USER}"
    ));
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
                .expect("seed Iceberg events table");
        });

        install_slc();
        upload_so();

        let mut sys = exa_conn();
        create_schema_and_scripts(&mut sys);

        let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {READER_USER} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {OWNER_USER} CASCADE"));
        sys.execute(&format!("DROP CONNECTION IF EXISTS {CONN_NAME}"));

        sys.execute(&format!(
            "CREATE USER {OWNER_USER} IDENTIFIED BY \"{OWNER_PASSWORD}\""
        ));
        sys.execute(&format!("GRANT CREATE SESSION TO {OWNER_USER}"));
        sys.execute(&format!("GRANT CREATE CONNECTION TO {OWNER_USER}"));
        sys.execute(&format!("GRANT CREATE VIRTUAL SCHEMA TO {OWNER_USER}"));
        for script in [
            ADAPTER_SCRIPT_NAME,
            SCAN_SCRIPT_NAME,
            DISTRIBUTOR_SCRIPT_NAME,
        ] {
            sys.execute(&format!(
                "GRANT EXECUTE ON SCRIPT {SCHEMA_NAME}.{script} TO {OWNER_USER}"
            ));
        }

        sys.execute(&format!(
            "CREATE USER {READER_USER} IDENTIFIED BY \"{READER_PASSWORD}\""
        ));
        sys.execute(&format!("GRANT CREATE SESSION TO {READER_USER}"));

        let mut owner = owner_conn();
        create_virtual_schema_with_password(
            &mut owner,
            &VsProps::new(VS_NAME, E2E_NAMESPACE).with_catalog_conn_name(CONN_NAME),
            &iceberg_catalog_url_internal(),
            &catalog_password(),
        );
        owner.execute(&format!(
            "GRANT SELECT ON SCHEMA {VS_NAME} TO {READER_USER}"
        ));
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
    assert_eq!(
        owner_total, SEED_TOTAL_ROWS as i64,
        "the owner must see every seeded row"
    );
    assert_eq!(
        owner_filtered, SEED_ROWS_SCORE_GT_15 as i64,
        "the owner must see every seeded row above the score bound"
    );

    let mut reader = reader_conn();
    assert_eq!(
        reader.query_scalar_i64(&count_sql),
        owner_total,
        "reader (no connection privilege) must get the owner's row count"
    );
    assert_eq!(
        reader.query_scalar_i64(&filtered_sql),
        owner_filtered,
        "reader must get the owner's filtered count"
    );

    let mut sys = exa_conn();
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT GRANTEE FROM EXA_DBA_CONNECTION_PRIVS WHERE GRANTEE = '{READER_USER}'"
        )),
        0, "reader must hold no connection privilege"
    );
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT GRANTED_ROLE FROM EXA_DBA_ROLE_PRIVS WHERE GRANTEE = '{READER_USER}'"
        )),
        0, "reader must hold no role"
    );
}

#[test]
fn the_readers_pushdown_plan_names_the_connection_and_carries_no_credential() {
    let _serial = serial();
    setup_e2e();
    let password = catalog_password();

    let plan = explain_virtual_sql(
        &mut reader_conn(),
        &format!(
            "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
            vs_table()
        ),
    );

    assert!(
        !plan.trim().is_empty(),
        "EXPLAIN VIRTUAL must return a non-empty plan"
    );
    assert!(
        plan.contains(SCAN_SCRIPT_NAME),
        "the plan must drive the scan script, got:\n{plan}"
    );
    assert!(
        plan.contains(&format!(r#""connection":{{"name":"{CONN_NAME}""#)),
        "plan must reference the CONNECTION by name, got:\n{plan}"
    );

    for value in [&password.access_key, &password.secret_key] {
        assert!(
            !value.is_empty(),
            "provisioned credential must be non-empty"
        );
        assert!(
            !plan.contains(value.as_str()),
            "plan must not carry credential {value:?}, got:\n{plan}"
        );
    }

    for key in [
        r#""access_key""#,
        r#""secret_key""#,
        r#""session_token""#,
        r#""s3":{"#,
        r#""inline":"#,
    ] {
        assert!(
            !plan.contains(key),
            "plan must not carry inline-storage token {key}, got:\n{plan}"
        );
    }
}

#[test]
fn revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential() {
    let _serial = serial();
    setup_e2e();
    let password = catalog_password();
    let query = format!(
        "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
        vs_table()
    );

    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64, "query must work before the revoke"
    );

    let mut owner = owner_conn();
    revoke_owner_scan_access(&mut owner);
    let denied = reader_conn().try_execute(&query);
    grant_owner_scan_access(&mut owner);

    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "revoking owner's grant must deny the reader, got: {denied}"
    );
    let msg = denied["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains(CONN_NAME),
        "denial must name the CONNECTION: {msg}"
    );
    assert!(
        msg.contains("GRANT ACCESS ON CONNECTION")
            && msg.contains(&format!("FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}")),
        "denial must name the missing grant: {msg}"
    );
    assert!(
        msg.contains("OWNER of the virtual schema"),
        "denial must name VS OWNER as grantee: {msg}"
    );
    for value in [&password.access_key, &password.secret_key] {
        assert!(
            !msg.contains(value.as_str()),
            "denial must not carry credential {value:?}: {msg}"
        );
    }

    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64,
        "re-granting must restore the reader's query"
    );
}

#[test]
fn a_grant_on_the_reader_is_no_substitute_for_the_owners_grant() {
    let _serial = serial();
    setup_e2e();
    let query = format!("SELECT COUNT(*) FROM {}", vs_table());

    let mut owner = owner_conn();
    owner.execute(&format!(
        "GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         TO {READER_USER}"
    ));
    revoke_owner_scan_access(&mut owner);
    let denied = reader_conn().try_execute(&query);
    grant_owner_scan_access(&mut owner);
    owner.execute(&format!(
        "REVOKE ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} \
         FROM {READER_USER}"
    ));

    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "a grant on the reader must not substitute for the owner's: {denied}"
    );

    assert_eq!(
        reader_conn().query_scalar_i64(&query),
        SEED_TOTAL_ROWS as i64,
        "the restored owner grant must make the reader's query work again"
    );
}
