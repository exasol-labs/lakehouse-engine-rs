use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS};
use crate::common::stack::{
    exasol_host, exasol_sql_port, iceberg_catalog_url_internal, local_stack_connection_password,
};
use crate::fixtures::{events, stack, vs_table};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

const CONN_NAME: &str = "CREDEXP_CATALOG_CREDS";
const VS_NAME: &str = "CREDEXP_VS";
const OWNER_USER: &str = "CREDEXP_OWNER";
const OWNER_PASSWORD: &str = "CredExpOwner2026x";
const READER_USER: &str = "CREDEXP_READER";
const READER_PASSWORD: &str = "CredExpReader2026x";
const OWNER_ROLE: &str = "CREDEXP_OWNER_ROLE";
const ROLE_VS_NAME: &str = "CREDEXP_ROLE_VS";
const SCAN_DENIAL: &str = "the scan cannot access CONNECTION";

static SETUP_DONE: OnceLock<()> = OnceLock::new();
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
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

fn scan_grant_sql(verb: &str, preposition: &str) -> String {
    script_grant_sql(verb, preposition, SCAN_SCRIPT_NAME, OWNER_USER)
}

fn script_grant_sql(verb: &str, preposition: &str, script: &str, grantee: &str) -> String {
    format!(
        "{verb} ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{script} {preposition} {grantee}"
    )
}

fn schema_grant_sql(verb: &str, preposition: &str) -> String {
    format!(
        "{verb} ACCESS ON CONNECTION {CONN_NAME} FOR SCHEMA {SCHEMA_NAME} {preposition} {OWNER_USER}"
    )
}

fn count_sql(vs: &str) -> String {
    format!("SELECT COUNT(*) FROM {}", vs_table(vs, E2E_TABLE))
}

fn denial_text(denied: &serde_json::Value) -> String {
    assert_eq!(
        denied["status"].as_str(),
        Some("error"),
        "query must be denied: {denied}"
    );
    let msg = denied["exception"]["text"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let password = local_stack_connection_password();
    for value in [&password.access_key, &password.secret_key] {
        assert!(!msg.contains(value.as_str()), "credential leaked: {msg}");
    }
    msg
}

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        stack();
        events();

        let mut sys = exa_conn();

        for vs in [ROLE_VS_NAME, VS_NAME] {
            let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
        }
        sys.execute(&format!("DROP ROLE IF EXISTS {OWNER_ROLE} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {READER_USER} CASCADE"));
        sys.execute(&format!("DROP USER IF EXISTS {OWNER_USER} CASCADE"));
        sys.execute(&format!("DROP CONNECTION IF EXISTS {CONN_NAME}"));

        sys.execute(&format!(
            "CREATE USER {OWNER_USER} IDENTIFIED BY \"{OWNER_PASSWORD}\""
        ));
        for priv_name in [
            "CREATE SESSION",
            "CREATE CONNECTION",
            "CREATE VIRTUAL SCHEMA",
        ] {
            sys.execute(&format!("GRANT {priv_name} TO {OWNER_USER}"));
        }
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
            &local_stack_connection_password(),
        );
        owner.execute(&format!(
            "GRANT SELECT ON SCHEMA {VS_NAME} TO {READER_USER}"
        ));

        sys.execute(&format!("CREATE ROLE {OWNER_ROLE}"));
        for script in [
            ADAPTER_SCRIPT_NAME,
            SCAN_SCRIPT_NAME,
            DISTRIBUTOR_SCRIPT_NAME,
        ] {
            sys.execute(&format!(
                "GRANT EXECUTE ON SCRIPT {SCHEMA_NAME}.{script} TO {OWNER_ROLE}"
            ));
        }
        for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
            sys.execute(&script_grant_sql("GRANT", "TO", script, OWNER_ROLE));
        }
        // Not the harness helper: it re-creates the CONNECTION, which drops every grant on it.
        sys.execute(&format!(
            "CREATE VIRTUAL SCHEMA {ROLE_VS_NAME} USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH \
             CATALOG_CONNECTION = '{CONN_NAME}' NAMESPACE = '{E2E_NAMESPACE}' ALLOW_HTTP = 'true'"
        ));
        sys.execute(&format!(
            "ALTER VIRTUAL SCHEMA {ROLE_VS_NAME} CHANGE OWNER {OWNER_ROLE}"
        ));
        sys.execute(&format!(
            "GRANT SELECT ON SCHEMA {ROLE_VS_NAME} TO {READER_USER}"
        ));
    });
}

#[test]
fn a_least_privilege_reader_gets_the_owners_rows_without_a_connection_grant() {
    let _serial = serial();
    setup_e2e();

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
    setup_e2e();
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
    setup_e2e();
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
    setup_e2e();
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

/// Scenario: A role can own the virtual schema and hold its grants
#[test]
fn a_role_owned_virtual_schema_serves_a_reader_through_the_roles_grants() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();

    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT * FROM EXA_DBA_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{ROLE_VS_NAME}' AND SCHEMA_OWNER = '{OWNER_ROLE}'"
        )),
        1
    );
    assert_eq!(
        sys.query_row_count(&format!(
            "SELECT * FROM EXA_DBA_RESTRICTED_OBJ_PRIVS WHERE OBJECT_NAME = '{CONN_NAME}' AND FOR_OBJECT_SCHEMA = '{SCHEMA_NAME}' AND FOR_OBJECT_NAME = '{SCAN_SCRIPT_NAME}' AND GRANTEE = '{OWNER_ROLE}'"
        )),
        1
    );
    assert_eq!(
        reader_conn().query_scalar_i64(&count_sql(ROLE_VS_NAME)),
        SEED_TOTAL_ROWS as i64
    );

    let role_scan_grant =
        |verb, preposition| script_grant_sql(verb, preposition, SCAN_SCRIPT_NAME, OWNER_ROLE);
    sys.execute(&role_scan_grant("REVOKE", "FROM"));
    let denied = reader_conn().try_execute(&count_sql(ROLE_VS_NAME));
    sys.execute(&role_scan_grant("GRANT", "TO"));

    let msg = denial_text(&denied);
    assert!(msg.contains(SCAN_DENIAL), "scan denial expected: {msg}");
}

/// Scenario: A schema-scoped CONNECTION grant covers both scripts
#[test]
fn a_schema_scoped_grant_replaces_both_script_grants() {
    let _serial = serial();
    setup_e2e();
    let mut owner = owner_conn();

    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        owner.execute(&script_grant_sql("REVOKE", "FROM", script, OWNER_USER));
    }
    let without_grants = reader_conn().try_execute(&count_sql(VS_NAME));
    owner.execute(&schema_grant_sql("GRANT", "TO"));
    let with_schema_grant = reader_conn().try_execute(&count_sql(VS_NAME));
    owner.execute(&schema_grant_sql("REVOKE", "FROM"));
    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        owner.execute(&script_grant_sql("GRANT", "TO", script, OWNER_USER));
    }

    denial_text(&without_grants);
    assert_eq!(
        with_schema_grant["status"].as_str(),
        Some("ok"),
        "schema-scoped grant must allow the query: {with_schema_grant}"
    );
    assert_eq!(
        reader_conn().query_scalar_i64(&count_sql(VS_NAME)),
        SEED_TOTAL_ROWS as i64
    );
}

/// Scenario: A reader's own CONNECTION grant does not replace the owner's
#[test]
fn a_readers_own_scan_grant_does_not_replace_the_owners() {
    let _serial = serial();
    setup_e2e();
    let mut owner = owner_conn();

    owner.execute(&scan_grant_sql("REVOKE", "FROM"));
    owner.execute(&script_grant_sql(
        "GRANT",
        "TO",
        SCAN_SCRIPT_NAME,
        READER_USER,
    ));
    let denied = reader_conn().try_execute(&count_sql(VS_NAME));
    owner.execute(&script_grant_sql(
        "REVOKE",
        "FROM",
        SCAN_SCRIPT_NAME,
        READER_USER,
    ));
    owner.execute(&scan_grant_sql("GRANT", "TO"));

    let msg = denial_text(&denied);
    assert!(msg.contains(SCAN_DENIAL), "scan denial expected: {msg}");
    assert_eq!(
        reader_conn().query_scalar_i64(&count_sql(VS_NAME)),
        SEED_TOTAL_ROWS as i64
    );
}

/// Scenario: A missing adapter grant fails with the adapter's named error
#[test]
fn revoking_the_owners_adapter_grant_fails_with_the_adapter_error() {
    let _serial = serial();
    setup_e2e();
    let mut owner = owner_conn();

    owner.execute(&script_grant_sql(
        "REVOKE",
        "FROM",
        ADAPTER_SCRIPT_NAME,
        OWNER_USER,
    ));
    let denied = reader_conn().try_execute(&count_sql(VS_NAME));
    owner.execute(&script_grant_sql(
        "GRANT",
        "TO",
        ADAPTER_SCRIPT_NAME,
        OWNER_USER,
    ));

    let msg = denial_text(&denied);
    assert!(
        msg.contains(&format!("CONNECTION '{CONN_NAME}' could not be resolved")),
        "adapter denial expected: {msg}"
    );
    assert!(!msg.contains(SCAN_DENIAL), "not a scan denial: {msg}");
    assert_eq!(
        reader_conn().query_scalar_i64(&count_sql(VS_NAME)),
        SEED_TOTAL_ROWS as i64
    );
}
