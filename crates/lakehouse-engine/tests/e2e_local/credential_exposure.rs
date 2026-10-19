use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS};
use crate::common::stack::{exasol_host, exasol_sql_port, local_stack_connection_password};
use crate::fixtures::{credential_exposure_vs, vs_table};
use std::sync::{Mutex, MutexGuard};

pub(crate) const CONN_NAME: &str = "CREDEXP_CATALOG_CREDS";
pub(crate) const VS_NAME: &str = "CREDEXP_VS";
pub(crate) const OWNER_USER: &str = "CREDEXP_OWNER";
pub(crate) const OWNER_PASSWORD: &str = "CredExpOwner2026x";
pub(crate) const READER_USER: &str = "CREDEXP_READER";
pub(crate) const READER_PASSWORD: &str = "CredExpReader2026x";

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

pub(crate) fn owner_conn() -> ExaConn {
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

fn scan_grant_sql(verb: &str, preposition: &str) -> String {
    format!(
        "{verb} ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME} {preposition} {OWNER_USER}"
    )
}

#[test]
fn a_least_privilege_reader_gets_the_owners_rows_without_a_connection_grant() {
    let _serial = serial();
    credential_exposure_vs();

    let count_sql = format!("SELECT COUNT(*) FROM {}", vs_table(VS_NAME, E2E_TABLE));
    let filtered_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE SCORE > 15.0",
        vs_table(VS_NAME, E2E_TABLE)
    );

    let owner_total = owner_conn().query_scalar_i64(&count_sql);
    let owner_filtered = owner_conn().query_scalar_i64(&filtered_sql);
    assert_eq!(owner_total, SEED_TOTAL_ROWS as i64);
    assert_eq!(owner_filtered, SEED_ROWS_SCORE_GT_15 as i64);

    let mut reader = reader_conn();
    assert_eq!(reader.query_scalar_i64(&count_sql), owner_total);
    assert_eq!(reader.query_scalar_i64(&filtered_sql), owner_filtered);

    let mut sys = exa_conn();
    for view in ["EXA_DBA_CONNECTION_PRIVS", "EXA_DBA_ROLE_PRIVS"] {
        assert_eq!(
            sys.query_row_count(&format!(
                "SELECT * FROM {view} WHERE GRANTEE = '{READER_USER}'"
            )),
            0
        );
    }
}

#[test]
fn the_readers_pushdown_plan_names_the_connection_and_carries_no_credential() {
    let _serial = serial();
    credential_exposure_vs();
    let password = local_stack_connection_password();

    let plan = explain_virtual_sql(
        &mut reader_conn(),
        &format!(
            "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
            vs_table(VS_NAME, E2E_TABLE)
        ),
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
    for key in [
        r#""access_key""#,
        r#""secret_key""#,
        r#""session_token""#,
        r#""s3":{"#,
        r#""inline":"#,
    ] {
        assert!(!plan.contains(key), "inline token {key} in: {plan}");
    }
}

#[test]
fn revoking_the_owners_scan_grant_denies_the_reader_without_leaking_the_credential() {
    let _serial = serial();
    credential_exposure_vs();
    let password = local_stack_connection_password();
    let query = format!(
        "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
        vs_table(VS_NAME, E2E_TABLE)
    );

    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64
    );

    let mut owner = owner_conn();
    owner.execute(&scan_grant_sql("REVOKE", "FROM"));
    let denied = reader_conn().try_execute(&query);
    owner.execute(&scan_grant_sql("GRANT", "TO"));

    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "revoke must deny: {denied}"
    );
    let msg = denied["exception"]["text"].as_str().unwrap_or("");
    for needle in [
        CONN_NAME,
        "GRANT ACCESS ON CONNECTION",
        &format!("FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}"),
        "OWNER of the virtual schema",
    ] {
        assert!(
            msg.contains(needle),
            "denial must contain {needle:?}: {msg}"
        );
    }
    for value in [&password.access_key, &password.secret_key] {
        assert!(!msg.contains(value.as_str()), "credential leaked: {msg}");
    }

    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64
    );
}

#[test]
fn the_reader_cannot_execute_the_pushdown_plan_it_captured() {
    let _serial = serial();
    credential_exposure_vs();
    let password = local_stack_connection_password();
    let query = format!(
        "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
        vs_table(VS_NAME, E2E_TABLE)
    );

    let isolated = isolated_pushdown_statement(&mut reader_conn(), &query);

    assert!(
        isolated.contains(&format!(r#""{SCHEMA_NAME}".{SCAN_SCRIPT_NAME}"#)),
        "isolated statement must name the scan script: {isolated}"
    );
    assert!(
        isolated.contains(&format!(r#""{SCHEMA_NAME}".{DISTRIBUTOR_SCRIPT_NAME}"#)),
        "isolated statement must name the distributor script: {isolated}"
    );
    assert!(
        isolated.contains(&format!("s3://warehouse/{E2E_NAMESPACE}/{E2E_TABLE}")),
        "isolated statement must carry the table root: {isolated}"
    );
    for column in ["ID", "NAME", "SCORE"] {
        assert!(
            isolated.contains(&format!(r#""{column}""#)),
            "isolated statement must carry projection column {column}: {isolated}"
        );
    }
    assert!(
        isolated.contains(r#""filter":"#),
        "isolated statement must carry a filter key: {isolated}"
    );
    assert!(
        isolated.contains(r#"\"SCORE\""#),
        "isolated statement's filter must name the filtered column: {isolated}"
    );
    assert!(
        isolated.contains("VALUES"),
        "isolated statement must carry a VALUES file list: {isolated}"
    );

    let mut sys = exa_conn();
    for script in [
        ADAPTER_SCRIPT_NAME,
        SCAN_SCRIPT_NAME,
        DISTRIBUTOR_SCRIPT_NAME,
    ] {
        assert_eq!(
            sys.query_row_count(&format!(
                "SELECT * FROM EXA_DBA_OBJ_PRIVS WHERE GRANTEE = '{READER_USER}' AND OBJECT_NAME = '{script}' AND OBJECT_TYPE = 'SCRIPT' AND PRIVILEGE = 'EXECUTE'"
            )),
            0
        );
    }
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT * FROM EXA_DBA_SYS_PRIVS WHERE GRANTEE = '{READER_USER}' AND PRIVILEGE = 'EXECUTE ANY SCRIPT'"
        )),
        0
    );
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT * FROM EXA_DBA_ROLE_PRIVS WHERE GRANTEE = '{READER_USER}'"
        )),
        0
    );

    let denied = reader_conn().try_execute(&isolated);
    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "reader must be denied: {denied}"
    );
    let msg = denied["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("insufficient privileges for calling script"),
        "denial must state insufficient script privilege: {msg}"
    );
    for value in [&password.access_key, &password.secret_key] {
        assert!(!value.is_empty());
        assert!(!msg.contains(value.as_str()), "credential leaked: {msg}");
    }

    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64
    );
}
