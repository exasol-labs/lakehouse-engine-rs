use crate::common::e2e_harness::*;
use crate::common::exasol_ws::{ExaConn, scalar_i64};
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS};
use crate::common::stack::{
    CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    iceberg_catalog_url_internal, local_stack_connection_password,
};
use crate::fixtures::{events, stack, vs_table};
use serde_json::Value;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

const PASSWORD: &str = "CredExpUser2026x";
const CONN_NAME: &str = "CREDEXP_CATALOG_CREDS";
const VS_NAME: &str = "CREDEXP_VS";
const SINGLE_SHARD_VS_NAME: &str = "CREDEXP_SINGLE_SHARD_VS";
const OWNER_USER: &str = "CREDEXP_OWNER";
const READER_USER: &str = "CREDEXP_READER";
const OWNER_ROLE: &str = "CREDEXP_OWNER_ROLE";
const ROLE_VS_NAME: &str = "CREDEXP_ROLE_VS";
const INSTALLER_USER: &str = "CREDEXP_INSTALLER";
const INSTALLER_ROLE: &str = "CREDEXP_INSTALLER_ROLE";
const INSTALLER_SCHEMA: &str = "CREDEXP_INSTALLER_SCRIPTS";
const INSTALLER_VS_NAME: &str = "CREDEXP_INSTALLER_VS";
const REINSTALL_SCHEMA: &str = "CREDEXP_SCRIPTS";
const SCHEMA_GRANTEE_USER: &str = "CREDEXP_SCHEMA_GRANTEE";
const SCHEMA_VS_NAME: &str = "CREDEXP_SCHEMA_VS";
const SCRIPT_VS_NAME: &str = "CREDEXP_SCRIPT_VS";
const ROTATED_CONN_NAME: &str = "CREDEXP_ROTATED_CONN";
const ROTATED_VS_NAME: &str = "CREDEXP_ROTATED_VS";
const MISSING_CONN_NAME: &str = "CREDEXP_NO_SUCH_CONN";
const MISSING_CONN_VS_NAME: &str = "CREDEXP_MISSING_CONN_VS";
const WRONG_SECRET_KEY: &str = "CredExpRotatedSecret2026";
const SCAN_DENIAL: &str = "the scan cannot access CONNECTION";
const ADAPTER_EXECUTE_DENIAL: &str = "insufficient privileges for calling adapter script";
const SCRIPT_EXECUTE_DENIAL: &str = "insufficient privileges for calling script";

static SETUP_DONE: OnceLock<()> = OnceLock::new();
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

fn user_conn(user: &str) -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), user, PASSWORD)
}

fn reader_conn() -> ExaConn {
    user_conn(READER_USER)
}

fn create_user(sys: &mut ExaConn, user: &str, privileges: &[&str]) {
    sys.execute(&format!("CREATE USER {user} IDENTIFIED BY \"{PASSWORD}\""));
    for privilege in privileges {
        sys.execute(&format!("GRANT {privilege} TO {user}"));
    }
}

fn execute_all(conn: &mut ExaConn, statements: impl IntoIterator<Item = String>) {
    for statement in statements {
        conn.execute(&statement);
    }
}

fn access_grant(conn: &str, schema: &str, script: &str, grantee: &str) -> String {
    format!("GRANT ACCESS ON CONNECTION {conn} FOR SCRIPT {schema}.{script} TO {grantee}")
}

fn execute_grant(schema: &str, script: &str, grantee: &str) -> String {
    format!("GRANT EXECUTE ON SCRIPT {schema}.{script} TO {grantee}")
}

/// The per-script owner grants that `docs/security.md` lists.
fn script_grants(schema: &str, conn: &str, grantee: &str) -> Vec<String> {
    [
        ADAPTER_SCRIPT_NAME,
        SCAN_SCRIPT_NAME,
        DISTRIBUTOR_SCRIPT_NAME,
    ]
    .map(|script| execute_grant(schema, script, grantee))
    .into_iter()
    .chain([ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME].map(|s| access_grant(conn, schema, s, grantee)))
    .collect()
}

/// The per-schema owner grants that `docs/security.md` lists.
fn schema_grants(schema: &str, conn: &str, grantee: &str) -> [String; 2] {
    [
        format!("GRANT EXECUTE ON SCHEMA {schema} TO {grantee}"),
        format!("GRANT ACCESS ON CONNECTION {conn} FOR SCHEMA {schema} TO {grantee}"),
    ]
}

fn revoke_sql(grant: &str) -> String {
    let (privilege, grantee) = grant
        .strip_prefix("GRANT ")
        .and_then(|rest| rest.rsplit_once(" TO "))
        .unwrap_or_else(|| panic!("not a GRANT ... TO statement: {grant}"));
    format!("REVOKE {privilege} FROM {grantee}")
}

/// Runs `undo` as SYS on drop, so a panicking test still restores the shared grants.
struct Undo {
    sys: ExaConn,
    sql: String,
}

impl Undo {
    fn after(apply: &str, undo: String) -> Self {
        let mut sys = exa_conn();
        sys.execute(apply);
        Self { sys, sql: undo }
    }
}

impl Drop for Undo {
    fn drop(&mut self) {
        let _ = self.sys.try_execute(&self.sql);
    }
}

fn without_grant<R>(grant: &str, f: impl FnOnce() -> R) -> R {
    let _restore = Undo::after(&revoke_sql(grant), grant.to_string());
    f()
}

fn with_grants<R>(grants: &[String], f: impl FnOnce() -> R) -> R {
    let _revoke: Vec<Undo> = grants
        .iter()
        .map(|grant| Undo::after(grant, revoke_sql(grant)))
        .collect();
    f()
}

fn create_vs_sql(vs: &str, script_schema: &str, conn: &str) -> String {
    format!(
        "CREATE VIRTUAL SCHEMA {vs} USING {script_schema}.{ADAPTER_SCRIPT_NAME} WITH \
         CATALOG_CONNECTION = '{conn}' NAMESPACE = '{E2E_NAMESPACE}' ALLOW_HTTP = 'true'"
    )
}

fn create_readable_vs(owner: &mut ExaConn, vs: &str, script_schema: &str, conn: &str) {
    owner.execute(&create_vs_sql(vs, script_schema, conn));
    owner.execute(&format!("GRANT SELECT ON SCHEMA {vs} TO {READER_USER}"));
}

fn count_sql(vs: &str) -> String {
    format!("SELECT COUNT(*) FROM {}", vs_table(vs, E2E_TABLE))
}

fn filtered_count_sql(vs: &str) -> String {
    format!("{} WHERE SCORE > 15.0", count_sql(vs))
}

fn filtered_rows_sql(vs: &str) -> String {
    format!(
        "SELECT ID, NAME, SCORE FROM {} WHERE SCORE > 15.0",
        vs_table(vs, E2E_TABLE)
    )
}

fn reader_count(vs: &str) -> i64 {
    reader_conn().query_scalar_i64(&count_sql(vs))
}

fn try_reader_count(vs: &str) -> Value {
    reader_conn().try_execute(&count_sql(vs))
}

fn unresolved_text(conn: &str) -> String {
    format!("CONNECTION '{conn}' could not be resolved")
}

fn assert_no_credential(text: &str) {
    let password = local_stack_connection_password();
    for value in [&password.access_key, &password.secret_key] {
        assert!(!value.is_empty());
        assert!(!text.contains(value.as_str()), "credential leaked: {text}");
    }
}

/// The error text of a denied response, checked to carry no credential.
fn denial_text(denied: &Value) -> String {
    let msg = error_text(denied);
    assert_no_credential(&msg);
    msg
}

fn assert_denied(denied: &Value, expected: &str) {
    let msg = denial_text(denied);
    assert!(msg.contains(expected), "{expected:?} expected: {msg}");
}

/// Both CONNECTION errors name the owner's grant, per script and per schema.
fn assert_names_the_owner_grant(msg: &str, script: &str) {
    for needle in [
        format!("GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{script}"),
        format!("(or FOR SCHEMA {SCHEMA_NAME})"),
        "OWNER of the virtual schema".to_string(),
    ] {
        assert!(msg.contains(&needle), "{needle:?} expected: {msg}");
    }
}

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        stack();
        events();

        let mut sys = exa_conn();
        for vs in [
            VS_NAME,
            SINGLE_SHARD_VS_NAME,
            ROLE_VS_NAME,
            INSTALLER_VS_NAME,
            SCHEMA_VS_NAME,
            SCRIPT_VS_NAME,
            ROTATED_VS_NAME,
            MISSING_CONN_VS_NAME,
        ] {
            let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
        }
        for role in [OWNER_ROLE, INSTALLER_ROLE] {
            sys.execute(&format!("DROP ROLE IF EXISTS {role} CASCADE"));
        }
        for user in [OWNER_USER, READER_USER, INSTALLER_USER, SCHEMA_GRANTEE_USER] {
            sys.execute(&format!("DROP USER IF EXISTS {user} CASCADE"));
        }
        for schema in [REINSTALL_SCHEMA, INSTALLER_SCHEMA] {
            sys.execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        }
        sys.execute(&format!("DROP CONNECTION IF EXISTS {ROTATED_CONN_NAME}"));
        sys.execute(&build_create_connection_sql(
            CONN_NAME,
            &iceberg_catalog_url_internal(),
            &local_stack_connection_password(),
        ));

        create_user(&mut sys, READER_USER, &["CREATE SESSION"]);
        create_user(
            &mut sys,
            OWNER_USER,
            &["CREATE SESSION", "CREATE VIRTUAL SCHEMA"],
        );
        execute_all(&mut sys, script_grants(SCHEMA_NAME, CONN_NAME, OWNER_USER));
        let mut owner = user_conn(OWNER_USER);
        create_readable_vs(&mut owner, VS_NAME, SCHEMA_NAME, CONN_NAME);
        owner.execute(&format!(
            "{} PARALLELISM_FACTOR = '1'",
            create_vs_sql(SINGLE_SHARD_VS_NAME, SCHEMA_NAME, CONN_NAME)
        ));
        owner.execute(&format!(
            "GRANT SELECT ON SCHEMA {SINGLE_SHARD_VS_NAME} TO {READER_USER}"
        ));

        sys.execute(&format!("CREATE ROLE {OWNER_ROLE}"));
        execute_all(&mut sys, script_grants(SCHEMA_NAME, CONN_NAME, OWNER_ROLE));
        create_readable_vs(&mut sys, ROLE_VS_NAME, SCHEMA_NAME, CONN_NAME);
        sys.execute(&format!(
            "ALTER VIRTUAL SCHEMA {ROLE_VS_NAME} CHANGE OWNER {OWNER_ROLE}"
        ));
        create_user(
            &mut sys,
            INSTALLER_USER,
            &[
                "CREATE SESSION",
                "CREATE SCHEMA",
                "CREATE SCRIPT",
                "CREATE VIRTUAL SCHEMA",
            ],
        );
        create_user(
            &mut sys,
            SCHEMA_GRANTEE_USER,
            &["CREATE SESSION", "CREATE VIRTUAL SCHEMA"],
        );
    });
}

/// Scenario: The scan spec references the CONNECTION by name
#[test]
fn the_readers_pushdown_plan_names_the_connection_and_carries_no_credential() {
    let _serial = serial();
    setup_e2e();

    let plan = explain_virtual_sql(&mut reader_conn(), &filtered_rows_sql(VS_NAME));

    assert!(!plan.trim().is_empty(), "EXPLAIN VIRTUAL returned empty");
    assert!(plan.contains(SCAN_SCRIPT_NAME), "no scan script: {plan}");
    assert!(
        plan.contains(&format!(r#""connection":{{"name":"{CONN_NAME}""#)),
        "no connection ref: {plan}"
    );
    assert_no_credential(&plan);
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

/// Scenario: The scan script needs the owner's CONNECTION grant
/// Scenario: A reader's own CONNECTION grant does not replace the owner's
#[test]
fn revoking_the_owners_scan_grant_denies_the_reader_even_with_its_own_grant() {
    let _serial = serial();
    setup_e2e();
    let query = filtered_rows_sql(VS_NAME);
    let owner_grant = access_grant(CONN_NAME, SCHEMA_NAME, SCAN_SCRIPT_NAME, OWNER_USER);
    let reader_grant = access_grant(CONN_NAME, SCHEMA_NAME, SCAN_SCRIPT_NAME, READER_USER);

    let denied = without_grant(&owner_grant, || {
        with_grants(&[reader_grant], || reader_conn().try_execute(&query))
    });

    let msg = denial_text(&denied);
    assert!(msg.contains(SCAN_DENIAL), "scan denial expected: {msg}");
    assert_names_the_owner_grant(&msg, SCAN_SCRIPT_NAME);
    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64
    );
}

/// Scenario: A missing adapter grant fails with the adapter's named error
/// Scenario: A reader's own CONNECTION grant does not replace the owner's
#[test]
fn revoking_the_owners_adapter_grant_fails_with_the_adapter_error_even_with_the_readers_grant() {
    let _serial = serial();
    setup_e2e();
    let owner_grant = access_grant(CONN_NAME, SCHEMA_NAME, ADAPTER_SCRIPT_NAME, OWNER_USER);
    let reader_grants = [
        format!("GRANT CONNECTION {CONN_NAME} TO {READER_USER}"),
        access_grant(CONN_NAME, SCHEMA_NAME, ADAPTER_SCRIPT_NAME, READER_USER),
    ];

    let denied = without_grant(&owner_grant, || {
        with_grants(&reader_grants, || try_reader_count(VS_NAME))
    });

    let msg = denial_text(&denied);
    assert!(
        msg.contains(&unresolved_text(CONN_NAME)) && !msg.contains(SCAN_DENIAL),
        "adapter denial expected: {msg}"
    );
    assert_names_the_owner_grant(&msg, ADAPTER_SCRIPT_NAME);
    assert_eq!(reader_count(VS_NAME), SEED_TOTAL_ROWS as i64);
}

/// Scenario: A CONNECTION that does not exist fails with the adapter's named error
#[test]
fn a_connection_that_does_not_exist_fails_with_the_adapter_error() {
    let _serial = serial();
    setup_e2e();

    let denied = exa_conn().try_execute(&create_vs_sql(
        MISSING_CONN_VS_NAME,
        SCHEMA_NAME,
        MISSING_CONN_NAME,
    ));

    assert_denied(
        &denied,
        &format!("{}: it does not exist", unresolved_text(MISSING_CONN_NAME)),
    );
}

/// Scenario: The scan script needs the owner's CONNECTION grant
/// Scenario: A virtual schema reader can read the pushdown plan but cannot execute it
#[test]
fn the_reader_cannot_execute_the_pushdown_plan_it_captured_and_the_owner_can() {
    let _serial = serial();
    setup_e2e();
    let query = filtered_rows_sql(VS_NAME);

    let isolated = isolated_pushdown_statement(&mut reader_conn(), &query);

    for needle in [
        format!(r#""{SCHEMA_NAME}".{SCAN_SCRIPT_NAME}"#),
        format!(r#""{SCHEMA_NAME}".{DISTRIBUTOR_SCRIPT_NAME}"#),
        format!("s3://warehouse/{E2E_NAMESPACE}/{E2E_TABLE}"),
        r#""ID""#.to_string(),
        r#""NAME""#.to_string(),
        r#""SCORE""#.to_string(),
        r#""filter":"#.to_string(),
        r#"\"SCORE\""#.to_string(),
        "VALUES".to_string(),
    ] {
        assert!(
            isolated.contains(&needle),
            "isolated statement must carry {needle}: {isolated}"
        );
    }

    assert_denied(&reader_conn().try_execute(&isolated), SCRIPT_EXECUTE_DENIAL);
    assert_eq!(
        user_conn(OWNER_USER).query_row_count(&isolated),
        SEED_ROWS_SCORE_GT_15 as i64,
        "the owner must run the scan plan directly"
    );
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

    assert_eq!(reader_count(ROLE_VS_NAME), SEED_TOTAL_ROWS as i64);

    let role_scan_grant = access_grant(CONN_NAME, SCHEMA_NAME, SCAN_SCRIPT_NAME, OWNER_ROLE);
    let denied = without_grant(&role_scan_grant, || try_reader_count(ROLE_VS_NAME));

    assert_denied(&denied, SCAN_DENIAL);
}

/// Scenario: A user owner can hold the CONNECTION grants through a role
#[test]
fn the_install_scripts_role_recipe_serves_a_reader() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();
    let mut installer = user_conn(INSTALLER_USER);

    // The grants `print_next_step_template` in deploy/scripts/install.sh prints.
    create_schema_and_scripts_in(&mut installer, INSTALLER_SCHEMA);
    sys.execute(&format!("CREATE ROLE {INSTALLER_ROLE}"));
    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        sys.execute(&access_grant(
            CONN_NAME,
            INSTALLER_SCHEMA,
            script,
            INSTALLER_ROLE,
        ));
    }
    let role_grant = format!("GRANT {INSTALLER_ROLE} TO {INSTALLER_USER}");
    sys.execute(&role_grant);
    create_readable_vs(
        &mut installer,
        INSTALLER_VS_NAME,
        INSTALLER_SCHEMA,
        CONN_NAME,
    );

    assert_eq!(reader_count(INSTALLER_VS_NAME), SEED_TOTAL_ROWS as i64);

    let denied = without_grant(&role_grant, || try_reader_count(INSTALLER_VS_NAME));

    assert_denied(&denied, &unresolved_text(CONN_NAME));
}

/// Scenario: Schema-scoped grants survive a re-install, script-scoped grants do not
#[test]
fn schema_scoped_grants_survive_a_reinstall_and_script_scoped_grants_do_not() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();

    create_schema_and_scripts_in(&mut sys, REINSTALL_SCHEMA);
    execute_all(
        &mut sys,
        schema_grants(REINSTALL_SCHEMA, CONN_NAME, SCHEMA_GRANTEE_USER),
    );
    execute_all(
        &mut sys,
        script_grants(REINSTALL_SCHEMA, CONN_NAME, OWNER_USER),
    );
    create_readable_vs(
        &mut user_conn(SCHEMA_GRANTEE_USER),
        SCHEMA_VS_NAME,
        REINSTALL_SCHEMA,
        CONN_NAME,
    );
    create_readable_vs(
        &mut user_conn(OWNER_USER),
        SCRIPT_VS_NAME,
        REINSTALL_SCHEMA,
        CONN_NAME,
    );
    for vs in [SCHEMA_VS_NAME, SCRIPT_VS_NAME] {
        assert_eq!(reader_count(vs), SEED_TOTAL_ROWS as i64, "{vs}");
    }

    create_schema_and_scripts_in(&mut sys, REINSTALL_SCHEMA);

    assert_eq!(reader_count(SCHEMA_VS_NAME), SEED_TOTAL_ROWS as i64);
    assert_denied(&try_reader_count(SCRIPT_VS_NAME), ADAPTER_EXECUTE_DENIAL);
}

/// Scenario: ALTER CONNECTION rotates the CONNECTION and keeps its grants
#[test]
fn alter_connection_rotates_the_secret_and_keeps_the_grants() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();
    let password = local_stack_connection_password();
    let rotated = CatalogConnectionPassword {
        secret_key: WRONG_SECRET_KEY.to_string(),
        ..local_stack_connection_password()
    };
    let alter_sql = |pw: &CatalogConnectionPassword| {
        build_create_connection_sql(ROTATED_CONN_NAME, &iceberg_catalog_url_internal(), pw)
            .replacen("CREATE OR REPLACE CONNECTION", "ALTER CONNECTION", 1)
    };
    let query = filtered_rows_sql(ROTATED_VS_NAME);

    sys.execute(&build_create_connection_sql(
        ROTATED_CONN_NAME,
        &iceberg_catalog_url_internal(),
        &password,
    ));
    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        sys.execute(&access_grant(
            ROTATED_CONN_NAME,
            SCHEMA_NAME,
            script,
            OWNER_USER,
        ));
    }
    create_readable_vs(
        &mut user_conn(OWNER_USER),
        ROTATED_VS_NAME,
        SCHEMA_NAME,
        ROTATED_CONN_NAME,
    );

    sys.execute(&alter_sql(&rotated));
    let msg = error_text(&reader_conn().try_execute(&query));
    for secret in [password.secret_key.as_str(), WRONG_SECRET_KEY] {
        assert!(!msg.contains(secret), "secret leaked: {msg}");
    }
    sys.execute(&alter_sql(&password));
    assert_eq!(
        reader_conn().query_row_count(&query),
        SEED_ROWS_SCORE_GT_15 as i64
    );
}

/// Scenario: A missing EXECUTE grant of the owner fails the query with Exasol's privilege error
#[test]
fn a_missing_execute_grant_of_the_owner_fails_with_exasols_privilege_error() {
    let _serial = serial();
    setup_e2e();

    let multi_shard = explain_virtual_sql(&mut reader_conn(), &filtered_count_sql(VS_NAME));
    assert!(
        multi_shard.contains(DISTRIBUTOR_SCRIPT_NAME),
        "the default plan must fan out through the distributor: {multi_shard}"
    );
    let single_shard = explain_virtual_sql(
        &mut reader_conn(),
        &filtered_count_sql(SINGLE_SHARD_VS_NAME),
    );
    assert!(
        !single_shard.contains(DISTRIBUTOR_SCRIPT_NAME),
        "a single-shard plan must not call the distributor: {single_shard}"
    );

    for (script, expected) in [
        (ADAPTER_SCRIPT_NAME, ADAPTER_EXECUTE_DENIAL),
        (SCAN_SCRIPT_NAME, SCRIPT_EXECUTE_DENIAL),
        (DISTRIBUTOR_SCRIPT_NAME, SCRIPT_EXECUTE_DENIAL),
    ] {
        let (multi, single) =
            without_grant(&execute_grant(SCHEMA_NAME, script, OWNER_USER), || {
                (
                    reader_conn().try_execute(&filtered_count_sql(VS_NAME)),
                    reader_conn().try_execute(&filtered_count_sql(SINGLE_SHARD_VS_NAME)),
                )
            });

        assert_denied(&multi, expected);
        if script == DISTRIBUTOR_SCRIPT_NAME {
            assert_eq!(
                scalar_i64(&single),
                Some(SEED_ROWS_SCORE_GT_15 as i64),
                "a single-shard query needs no distributor grant: {single}"
            );
        } else {
            assert_denied(&single, expected);
        }
    }
    assert_eq!(
        reader_conn().query_scalar_i64(&filtered_count_sql(VS_NAME)),
        SEED_ROWS_SCORE_GT_15 as i64
    );
}
