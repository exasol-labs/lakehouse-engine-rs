use crate::common::e2e_harness::*;
use crate::common::exasol_ws::{ExaConn, scalar_i64};
use crate::common::seed::{E2E_NAMESPACE, E2E_TABLE, SEED_ROWS_SCORE_GT_15, SEED_TOTAL_ROWS};
use crate::common::stack::{
    build_create_connection_sql, exasol_host, exasol_sql_port, iceberg_catalog_url_internal,
    local_stack_connection_password,
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
const ROLE_MEMBER_USER: &str = "CREDEXP_ROLE_MEMBER";
const INSTALLER_USER: &str = "CREDEXP_INSTALLER";
const INSTALLER_ROLE: &str = "CREDEXP_INSTALLER_ROLE";
const INSTALLER_SCHEMA: &str = "CREDEXP_INSTALLER_SCRIPTS";
const INSTALLER_VS_NAME: &str = "CREDEXP_INSTALLER_VS";
const ANY_PRIVILEGE_USER: &str = "CREDEXP_ANY_PRIVILEGE";
const ANY_PRIVILEGE_VS_NAME: &str = "CREDEXP_ANY_PRIVILEGE_VS";
const REINSTALL_SCHEMA: &str = "CREDEXP_SCRIPTS";
const SCHEMA_OWNER_USER: &str = "CREDEXP_SCHEMA_OWNER";
const SCHEMA_VS_NAME: &str = "CREDEXP_SCHEMA_VS";
const SCRIPT_VS_NAME: &str = "CREDEXP_SCRIPT_VS";
const REPLACE_CONN_NAME: &str = "CREDEXP_REPLACE_CONN";
const REPLACE_VS_NAME: &str = "CREDEXP_REPLACE_VS";
const MISSING_CONN_NAME: &str = "CREDEXP_NO_SUCH_CONN";
const MISSING_CONN_VS_NAME: &str = "CREDEXP_MISSING_CONN_VS";
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

fn with_grant<R>(grant: &str, f: impl FnOnce() -> R) -> R {
    let _revoke = Undo::after(grant, revoke_sql(grant));
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

fn rows(sys: &mut ExaConn, view: &str, filter: &str) -> i64 {
    sys.query_row_count(&format!("SELECT * FROM {view} WHERE {filter}"))
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
            ANY_PRIVILEGE_VS_NAME,
            SCHEMA_VS_NAME,
            SCRIPT_VS_NAME,
            REPLACE_VS_NAME,
            MISSING_CONN_VS_NAME,
        ] {
            let _ = sys.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
        }
        for role in [OWNER_ROLE, INSTALLER_ROLE] {
            sys.execute(&format!("DROP ROLE IF EXISTS {role} CASCADE"));
        }
        for user in [
            OWNER_USER,
            READER_USER,
            ROLE_MEMBER_USER,
            INSTALLER_USER,
            ANY_PRIVILEGE_USER,
            SCHEMA_OWNER_USER,
        ] {
            sys.execute(&format!("DROP USER IF EXISTS {user} CASCADE"));
        }
        for schema in [REINSTALL_SCHEMA, INSTALLER_SCHEMA] {
            sys.execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        }
        sys.execute(&format!("DROP CONNECTION IF EXISTS {REPLACE_CONN_NAME}"));
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
        create_user(&mut sys, ROLE_MEMBER_USER, &["CREATE SESSION", OWNER_ROLE]);

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
            ANY_PRIVILEGE_USER,
            &[
                "CREATE SESSION",
                "CREATE VIRTUAL SCHEMA",
                "EXECUTE ANY SCRIPT",
                "ACCESS ANY CONNECTION",
            ],
        );
        create_user(
            &mut sys,
            SCHEMA_OWNER_USER,
            &["CREATE SESSION", "CREATE VIRTUAL SCHEMA"],
        );
    });
}

/// Scenario: The scan script requires a script-scoped grant; rotation is observed per shard
#[test]
fn a_least_privilege_reader_gets_the_owners_rows_without_a_connection_grant() {
    let _serial = serial();
    setup_e2e();

    let mut owner = user_conn(OWNER_USER);
    let owner_total = owner.query_scalar_i64(&count_sql(VS_NAME));
    let owner_filtered = owner.query_scalar_i64(&filtered_count_sql(VS_NAME));
    assert_eq!(owner_total, SEED_TOTAL_ROWS as i64);
    assert_eq!(owner_filtered, SEED_ROWS_SCORE_GT_15 as i64);

    let mut reader = reader_conn();
    assert_eq!(reader.query_scalar_i64(&count_sql(VS_NAME)), owner_total);
    assert_eq!(
        reader.query_scalar_i64(&filtered_count_sql(VS_NAME)),
        owner_filtered
    );

    let mut sys = exa_conn();
    for view in ["EXA_DBA_CONNECTION_PRIVS", "EXA_DBA_ROLE_PRIVS"] {
        assert_eq!(
            rows(&mut sys, view, &format!("GRANTEE = '{READER_USER}'")),
            0
        );
    }
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

/// Scenario: The scan script requires a script-scoped grant; rotation is observed per shard
/// Scenario: A reader's own CONNECTION grant does not replace the owner's
#[test]
fn revoking_the_owners_scan_grant_denies_the_reader_even_with_its_own_grant() {
    let _serial = serial();
    setup_e2e();
    let query = filtered_rows_sql(VS_NAME);
    let owner_grant = access_grant(CONN_NAME, SCHEMA_NAME, SCAN_SCRIPT_NAME, OWNER_USER);
    let reader_grant = access_grant(CONN_NAME, SCHEMA_NAME, SCAN_SCRIPT_NAME, READER_USER);

    let denied = without_grant(&owner_grant, || {
        with_grant(&reader_grant, || reader_conn().try_execute(&query))
    });

    let msg = denial_text(&denied);
    for needle in [
        SCAN_DENIAL,
        CONN_NAME,
        &format!(
            "GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{SCAN_SCRIPT_NAME}"
        ),
        "OWNER of the virtual schema",
    ] {
        assert!(
            msg.contains(needle),
            "denial must contain {needle:?}: {msg}"
        );
    }
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
        with_grant(&reader_grants[0], || {
            with_grant(&reader_grants[1], || try_reader_count(VS_NAME))
        })
    });

    let msg = denial_text(&denied);
    for needle in [
        unresolved_text(CONN_NAME),
        format!(
            "GRANT ACCESS ON CONNECTION {CONN_NAME} FOR SCRIPT {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME}"
        ),
        "OWNER of the virtual schema".to_string(),
    ] {
        assert!(
            msg.contains(&needle),
            "adapter denial must contain {needle:?}: {msg}"
        );
    }
    assert!(!msg.contains(SCAN_DENIAL), "not a scan denial: {msg}");
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

/// Scenario: A virtual schema reader can read the pushdown plan but cannot execute it
#[test]
fn the_reader_cannot_execute_the_pushdown_plan_it_captured() {
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

    let mut sys = exa_conn();
    let reader = format!("GRANTEE = '{READER_USER}'");
    assert_eq!(
        rows(
            &mut sys,
            "EXA_DBA_OBJ_PRIVS",
            &format!("{reader} AND PRIVILEGE = 'EXECUTE'")
        ),
        0
    );
    assert_eq!(
        rows(
            &mut sys,
            "EXA_DBA_SYS_PRIVS",
            &format!("{reader} AND PRIVILEGE = 'EXECUTE ANY SCRIPT'")
        ),
        0
    );
    assert_eq!(rows(&mut sys, "EXA_DBA_ROLE_PRIVS", &reader), 0);

    assert_denied(&reader_conn().try_execute(&isolated), SCRIPT_EXECUTE_DENIAL);
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

    assert_eq!(
        rows(
            &mut exa_conn(),
            "EXA_DBA_VIRTUAL_SCHEMAS",
            &format!("SCHEMA_NAME = '{ROLE_VS_NAME}' AND SCHEMA_OWNER = '{OWNER_ROLE}'")
        ),
        1
    );
    assert_eq!(reader_count(ROLE_VS_NAME), SEED_TOTAL_ROWS as i64);

    let role_scan_grant = access_grant(CONN_NAME, SCHEMA_NAME, SCAN_SCRIPT_NAME, OWNER_ROLE);
    let denied = without_grant(&role_scan_grant, || try_reader_count(ROLE_VS_NAME));

    assert_denied(&denied, SCAN_DENIAL);
}

/// Scenario: A member of the owning role administers a role-owned virtual schema
#[test]
fn a_member_of_the_owning_role_administers_the_virtual_schema_and_runs_its_plan() {
    let _serial = serial();
    setup_e2e();
    let refresh = format!("ALTER VIRTUAL SCHEMA {ROLE_VS_NAME} REFRESH");
    let set = |factor: &str| {
        format!("ALTER VIRTUAL SCHEMA {ROLE_VS_NAME} SET PARALLELISM_FACTOR = '{factor}'")
    };

    let mut member = user_conn(ROLE_MEMBER_USER);
    member.execute(&refresh);
    member.execute(&set("2"));
    assert_eq!(
        rows(
            &mut exa_conn(),
            "EXA_DBA_VIRTUAL_SCHEMA_PROPERTIES",
            &format!(
                "SCHEMA_NAME = '{ROLE_VS_NAME}' AND PROPERTY_NAME = 'PARALLELISM_FACTOR' \
                 AND PROPERTY_VALUE = '2'"
            )
        ),
        1
    );
    let isolated = isolated_pushdown_statement(&mut member, &count_sql(ROLE_VS_NAME));
    assert_eq!(
        scalar_i64(&member.try_execute(&isolated)),
        Some(SEED_TOTAL_ROWS as i64),
        "a member must run the scan plan directly"
    );

    let mut reader = reader_conn();
    for statement in [refresh, set("3")] {
        error_text(&reader.try_execute(&statement));
    }
    assert_denied(&reader.try_execute(&isolated), SCRIPT_EXECUTE_DENIAL);
    assert_eq!(reader_count(ROLE_VS_NAME), SEED_TOTAL_ROWS as i64);
}

/// Scenario: A user owner can hold the CONNECTION grants through a role
#[test]
fn the_install_scripts_role_recipe_serves_a_reader() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();
    let mut installer = user_conn(INSTALLER_USER);

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

    let installer_grantee = format!("GRANTEE = '{INSTALLER_USER}'");
    for view in ["EXA_DBA_OBJ_PRIVS", "EXA_DBA_RESTRICTED_OBJ_PRIVS"] {
        assert_eq!(rows(&mut sys, view, &installer_grantee), 0, "{view}");
    }
    assert_eq!(reader_count(INSTALLER_VS_NAME), SEED_TOTAL_ROWS as i64);

    let denied = without_grant(&role_grant, || try_reader_count(INSTALLER_VS_NAME));

    assert_denied(&denied, &unresolved_text(CONN_NAME));
}

/// Scenario: An owner with EXECUTE ANY SCRIPT and ACCESS ANY CONNECTION needs no grant
#[test]
fn an_owner_with_the_any_privileges_needs_no_grant() {
    let _serial = serial();
    setup_e2e();

    create_readable_vs(
        &mut user_conn(ANY_PRIVILEGE_USER),
        ANY_PRIVILEGE_VS_NAME,
        SCHEMA_NAME,
        CONN_NAME,
    );

    let grantee = format!("GRANTEE = '{ANY_PRIVILEGE_USER}'");
    let mut sys = exa_conn();
    for view in [
        "EXA_DBA_OBJ_PRIVS",
        "EXA_DBA_RESTRICTED_OBJ_PRIVS",
        "EXA_DBA_ROLE_PRIVS",
    ] {
        assert_eq!(rows(&mut sys, view, &grantee), 0, "{view}");
    }
    assert_eq!(reader_count(ANY_PRIVILEGE_VS_NAME), SEED_TOTAL_ROWS as i64);
}

/// Scenario: Schema-scoped grants cover the scripts and survive a re-install, script-scoped grants do not
#[test]
fn schema_scoped_grants_survive_a_script_reinstall_and_script_scoped_grants_do_not() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();

    create_schema_and_scripts_in(&mut sys, REINSTALL_SCHEMA);
    execute_all(
        &mut sys,
        schema_grants(REINSTALL_SCHEMA, CONN_NAME, SCHEMA_OWNER_USER),
    );
    execute_all(
        &mut sys,
        script_grants(REINSTALL_SCHEMA, CONN_NAME, OWNER_USER),
    );
    create_readable_vs(
        &mut user_conn(SCHEMA_OWNER_USER),
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

    let access_rows = |sys: &mut ExaConn, for_type: &str| {
        rows(
            sys,
            "EXA_DBA_RESTRICTED_OBJ_PRIVS",
            &format!(
                "OBJECT_NAME = '{CONN_NAME}' AND FOR_OBJECT_TYPE = '{for_type}' AND \
                 (FOR_OBJECT_SCHEMA = '{REINSTALL_SCHEMA}' OR FOR_OBJECT_NAME = '{REINSTALL_SCHEMA}')"
            ),
        )
    };
    assert_eq!(access_rows(&mut sys, "SCRIPT"), 0);
    assert_eq!(access_rows(&mut sys, "SCHEMA"), 1);
    assert_eq!(reader_count(SCHEMA_VS_NAME), SEED_TOTAL_ROWS as i64);
    assert_denied(&try_reader_count(SCRIPT_VS_NAME), ADAPTER_EXECUTE_DENIAL);
}

/// Scenario: A schema-scoped CONNECTION grant extends to a script the owner creates
#[test]
fn a_schema_scoped_grant_extends_to_a_script_the_owner_creates() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();

    sys.execute(&format!("CREATE SCHEMA IF NOT EXISTS {REINSTALL_SCHEMA}"));
    sys.execute(&schema_grants(REINSTALL_SCHEMA, CONN_NAME, SCHEMA_OWNER_USER)[1]);
    // The address stands in for the password, so a passing test prints no credential.
    let peeked = with_grant(
        &format!("GRANT CREATE ANY SCRIPT TO {SCHEMA_OWNER_USER}"),
        || {
            let mut schema_owner = user_conn(SCHEMA_OWNER_USER);
            schema_owner.execute(&format!(
                "CREATE OR REPLACE LUA SCALAR SCRIPT {REINSTALL_SCHEMA}.PEEK_CONNECTION() \
                 RETURNS VARCHAR(2000) AS\n\
                 function run(ctx) return exa.get_connection('{CONN_NAME}').address end\n/"
            ));
            schema_owner.try_execute(&format!("SELECT {REINSTALL_SCHEMA}.PEEK_CONNECTION()"))
        },
    );

    assert_eq!(
        peeked["responseData"]["results"][0]["resultSet"]["data"][0][0].as_str(),
        Some(iceberg_catalog_url_internal().as_str()),
        "the owner's own script must read the CONNECTION: {peeked}"
    );
}

/// Scenario: ALTER CONNECTION keeps the CONNECTION grants, CREATE OR REPLACE CONNECTION drops them
#[test]
fn alter_connection_keeps_the_grants_and_create_or_replace_drops_them() {
    let _serial = serial();
    setup_e2e();
    let mut sys = exa_conn();
    let create_sql = build_create_connection_sql(
        REPLACE_CONN_NAME,
        &iceberg_catalog_url_internal(),
        &local_stack_connection_password(),
    );
    let owner_grants = format!("OBJECT_NAME = '{REPLACE_CONN_NAME}' AND GRANTEE = '{OWNER_USER}'");

    sys.execute(&create_sql);
    for script in [ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME] {
        sys.execute(&access_grant(
            REPLACE_CONN_NAME,
            SCHEMA_NAME,
            script,
            OWNER_USER,
        ));
    }
    create_readable_vs(
        &mut user_conn(OWNER_USER),
        REPLACE_VS_NAME,
        SCHEMA_NAME,
        REPLACE_CONN_NAME,
    );

    sys.execute(&create_sql.replacen("CREATE OR REPLACE CONNECTION", "ALTER CONNECTION", 1));
    assert_eq!(
        rows(&mut sys, "EXA_DBA_RESTRICTED_OBJ_PRIVS", &owner_grants),
        2
    );
    assert_eq!(reader_count(REPLACE_VS_NAME), SEED_TOTAL_ROWS as i64);

    sys.execute(&create_sql);
    assert_eq!(
        rows(&mut sys, "EXA_DBA_RESTRICTED_OBJ_PRIVS", &owner_grants),
        0
    );
    assert_denied(
        &try_reader_count(REPLACE_VS_NAME),
        &unresolved_text(REPLACE_CONN_NAME),
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
