//! `EXPORT ... INTO SCRIPT` through the export script. The local stack has one node, so
//! the writer count is `WRITERS_PER_NODE`, capped at 300.

use std::sync::OnceLock;

use crate::common::e2e_harness::{
    DEFAULT_CATALOG_CONN_NAME, EXPORT_SCRIPT_NAME, SCHEMA_NAME, error_text, exa_conn,
};
use crate::common::exasol_ws::{ExaConn, profiled_export_counts};
use crate::common::seed::E2E_TABLE;
use crate::common::stack::{exasol_host, exasol_sql_port, local_stack_connection_password};
use crate::common::timestamp_precision::engine_honors_declared_precision;
use crate::fixtures::{LAKEHOUSE_VS, lakehouse_vs, stack, vs_table};

const EXPORT_SCHEMA: &str = "EXPORT_E2E";
const TYPED_TABLE: &str = "EXPORT_E2E.TYPED";
const TYPED_ROWS: i64 = 1_000;
const LARGE_TABLE: &str = "EXPORT_E2E.ROWS_10K";
const LARGE_ROWS: i64 = 10_000;
const TAGS_TABLE: &str = "EXPORT_E2E.EVENT_TAGS";
const MAX_WRITERS: i64 = 300;
const EXPORTER_PASSWORD: &str = "ExportE2eUser2026x";
const ROLE_EXPORTER: &str = "EXPORT_E2E_ROLE_EXPORTER";
const NO_ACCESS_EXPORTER: &str = "EXPORT_E2E_NO_ACCESS";

/// Every row raises when read, so an error without the read error proves the export
/// failed before reading the source.
const FAILING_SOURCE: &str = "(SELECT 1 / (ID - ID) AS X FROM EXPORT_E2E.TYPED)";
const SOURCE_READ_ERROR: &str = "division by zero";
const INLINE_PASSWORD: &str = "ExportInlineSecret2026";
fn setup() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        lakehouse_vs();
        let mut sys = exa_conn();
        sys.execute(&format!("CREATE SCHEMA IF NOT EXISTS {EXPORT_SCHEMA}"));
        // Exasol 8 refuses TIMESTAMP(9) as `0A000 Feature not supported`.
        let timestamp_9 = if engine_honors_declared_precision(&mut sys) {
            "CAST(ADD_SECONDS(TIMESTAMP '2024-01-01 00:00:00.123456789', LEVEL) AS TIMESTAMP(9)) AS C_TIMESTAMP_9,"
        } else {
            ""
        };
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {TYPED_TABLE} AS SELECT \
               CAST(LEVEL AS DECIMAL(9,0)) AS ID, \
               CASE WHEN MOD(LEVEL, 2) = 0 THEN TRUE ELSE FALSE END AS C_BOOLEAN, \
               ADD_DAYS(DATE '2024-01-01', LEVEL) AS C_DATE, \
               CAST(ADD_SECONDS(TIMESTAMP '2024-01-01 00:00:00.123', LEVEL) AS TIMESTAMP(3)) AS C_TIMESTAMP_3, \
               {timestamp_9} \
               CAST(LEVEL / 4 AS DOUBLE) AS C_DOUBLE, \
               CAST('c' || LEVEL AS CHAR(10)) AS C_CHAR, \
               CAST('v' || LEVEL AS VARCHAR(100)) AS C_VARCHAR, \
               CAST(LEVEL AS DECIMAL(9,0)) AS C_DECIMAL_9, \
               CAST(LEVEL * 1000000000 AS DECIMAL(18,0)) AS C_DECIMAL_18, \
               CAST(LEVEL / 3 AS DECIMAL(36,4)) AS C_DECIMAL_36 \
             FROM DUAL CONNECT BY LEVEL <= {TYPED_ROWS}"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {LARGE_TABLE} AS \
             SELECT CAST(LEVEL AS DECIMAL(9,0)) AS ID FROM DUAL CONNECT BY LEVEL <= {LARGE_ROWS}"
        ));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {TAGS_TABLE} AS \
             SELECT CAST(2 * LEVEL AS DECIMAL(18,0)) AS ID, 'tag' || LEVEL AS TAG \
             FROM DUAL CONNECT BY LEVEL <= 15"
        ));
    });
}

/// `ROLE_EXPORTER` holds the export role; `NO_ACCESS_EXPORTER` holds its privileges
/// directly, except `ACCESS` for the export script.
fn exporters() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        let mut sys = exa_conn();
        let role = format!("LAKEHOUSE_EXPORT_ROLE_{SCHEMA_NAME}");
        sys.execute(&format!("DROP ROLE IF EXISTS {role} CASCADE"));
        for user in [ROLE_EXPORTER, NO_ACCESS_EXPORTER] {
            sys.execute(&format!("DROP USER IF EXISTS {user} CASCADE"));
            sys.execute(&format!(
                "CREATE USER {user} IDENTIFIED BY \"{EXPORTER_PASSWORD}\""
            ));
            sys.execute(&format!("GRANT CREATE SESSION TO {user}"));
            sys.execute(&format!("GRANT SELECT ON SCHEMA {EXPORT_SCHEMA} TO {user}"));
        }
        let script = format!("{SCHEMA_NAME}.{EXPORT_SCRIPT_NAME}");
        let conn = DEFAULT_CATALOG_CONN_NAME;
        // The role statements that `print_next_step_template` in deploy/scripts/install.sh prints.
        let privileges = ["EXPORT".to_string(), format!("EXECUTE ON SCRIPT {script}")];
        let access_grant = format!("ACCESS ON CONNECTION {conn} FOR SCRIPT {script}");
        sys.execute(&format!("CREATE ROLE {role}"));
        for privilege in &privileges {
            sys.execute(&format!("GRANT {privilege} TO {role}"));
            sys.execute(&format!("GRANT {privilege} TO {NO_ACCESS_EXPORTER}"));
        }
        sys.execute(&format!("GRANT {access_grant} TO {role}"));
        sys.execute(&format!("GRANT {role} TO {ROLE_EXPORTER}"));
    });
}

fn exporter_conn(user: &str) -> ExaConn {
    ExaConn::connect(&exasol_host(), exasol_sql_port(), user, EXPORTER_PASSWORD)
}

fn missing_grant_error(user: &str) -> String {
    format!(
        "user {user} cannot read CONNECTION {DEFAULT_CATALOG_CONN_NAME} in script {SCHEMA_NAME}.{EXPORT_SCRIPT_NAME}"
    )
}

fn export_into(source: &str, tail: &str) -> String {
    format!("EXPORT {source} INTO SCRIPT {SCHEMA_NAME}.{EXPORT_SCRIPT_NAME} {tail}")
}

/// A valid export of `source` with `options` after `TABLE_NAME`.
fn export_sql(source: &str, options: &str) -> String {
    export_into(
        source,
        &format!(
            "WITH CATALOG_CONNECTION = '{DEFAULT_CATALOG_CONN_NAME}' TABLE_NAME = 'ns.t' {options}"
        ),
    )
}

#[track_caller]
fn exported_rows(conn: &mut ExaConn, sql: &str) -> i64 {
    let resp = conn.execute(sql);
    resp["responseData"]["results"][0]["rowCount"]
        .as_i64()
        .unwrap_or_else(|| panic!("expected a rowCount from:\n{sql}\n\nResponse: {resp}"))
}

/// The exported row count and the writer count of one export. A fresh session holds only
/// this export, whose writer count is the `OUT_ROWS` of its one `GROUP BY` profile part.
#[track_caller]
fn profiled_export(source: &str, options: &str) -> (i64, i64) {
    profiled_export_counts(&mut exa_conn(), &export_sql(source, options))
}

/// The error text of a failing export, checked to carry no value of the CONNECTION.
#[track_caller]
fn export_error(conn: &mut ExaConn, sql: &str) -> String {
    let error = error_text(&conn.try_execute(sql), sql);
    let password = local_stack_connection_password();
    for secret in [&password.access_key, &password.secret_key] {
        assert!(
            !error.contains(secret.as_str()),
            "a CONNECTION value leaked: {error}"
        );
    }
    error
}

#[track_caller]
fn assert_refused_before_reading(
    label: &str,
    conn: &mut ExaConn,
    sql: &str,
    fragments: &[&str],
) -> String {
    let error = export_error(conn, sql);
    assert!(
        fragments.iter().all(|fragment| error.contains(fragment))
            && !error.contains(SOURCE_READ_ERROR),
        "{label}: {sql} must fail before reading the source, naming {fragments:?}: {error}"
    );
    error
}

/// Scenario: A whole-table export reports every source row
/// Scenario: A filtered query exports only the rows it returns
/// Scenario: A column-list export reports every row of the table
/// Scenario: A join of a virtual schema table and an Exasol table exports the joined rows
/// Scenario: An empty source exports zero rows
/// Scenario: All scripts resolve from the same uploaded artifact
#[test]
fn export_reports_every_source_row() {
    stack();
    setup();
    let mut sys = exa_conn();
    let join = format!(
        "SELECT e.ID, e.NAME, t.TAG FROM {} e JOIN {TAGS_TABLE} t ON e.ID = t.ID",
        vs_table(LAKEHOUSE_VS, E2E_TABLE)
    );
    // The even IDs of the 20 events join one tag each.
    assert_eq!(sys.query_row_count(&join), 10, "the join selects 10 rows");
    let cases = [
        ("whole table", TYPED_TABLE.to_string(), TYPED_ROWS),
        (
            "filtered query",
            format!("(SELECT * FROM {TYPED_TABLE} WHERE MOD(ID, 4) = 0)"),
            TYPED_ROWS / 4,
        ),
        (
            "column list",
            format!("{TYPED_TABLE} (ID, C_VARCHAR)"),
            TYPED_ROWS,
        ),
        ("virtual schema join", format!("({join})"), 10),
        (
            "empty source",
            format!("(SELECT * FROM {TYPED_TABLE} WHERE ID < 0)"),
            0,
        ),
    ];

    for (case, source, expected) in cases {
        assert_eq!(
            exported_rows(&mut sys, &export_sql(&source, "")),
            expected,
            "{case}"
        );
    }
}

/// Scenario: WRITERS_PER_NODE sets the writer count
/// Scenario: The writer count never exceeds 300
/// Scenario: SINGLE_COMMIT runs one writer for the whole cluster
#[test]
fn export_writer_layout_sets_writer_count() {
    stack();
    setup();
    let typed_first = |n: i64| format!("(SELECT * FROM {TYPED_TABLE} WHERE ID <= {n})");
    let large_first = |n: i64| format!("(SELECT * FROM {LARGE_TABLE} WHERE ID <= {n})");
    let per_node_3 = "WRITERS_PER_NODE = '3'";
    let per_node_500 = "WRITERS_PER_NODE = '500'";
    let cases = [
        (
            "3 writers per node over the table",
            TYPED_TABLE.to_string(),
            per_node_3,
            (TYPED_ROWS, 3),
        ),
        (
            "3 writers per node over exactly 3 rows",
            typed_first(3),
            per_node_3,
            (3, 3),
        ),
        (
            "3 writers per node over 2 rows",
            typed_first(2),
            per_node_3,
            (2, 2),
        ),
        (
            "default is 1 writer",
            TYPED_TABLE.to_string(),
            "",
            (TYPED_ROWS, 1),
        ),
        (
            "500 writers per node cap at 300 over the table",
            LARGE_TABLE.to_string(),
            per_node_500,
            (LARGE_ROWS, MAX_WRITERS),
        ),
        (
            "500 writers per node cap at 300 over exactly 300 rows",
            large_first(MAX_WRITERS),
            per_node_500,
            (MAX_WRITERS, MAX_WRITERS),
        ),
        (
            "SINGLE_COMMIT runs 1 writer",
            TYPED_TABLE.to_string(),
            "SINGLE_COMMIT = 'true'",
            (TYPED_ROWS, 1),
        ),
    ];

    for (case, source, options, expected) in cases {
        assert_eq!(profiled_export(&source, options), expected, "{case}");
    }
}

/// Scenario: WRITERS_PER_NODE together with SINGLE_COMMIT is refused
/// Scenario: An invalid option fails the export before any data is read
/// Scenario: An export without CATALOG_CONNECTION fails before any data is read
/// Scenario: An AT clause is refused
/// Scenario: TRUNCATE, REPLACE, and CREATED BY are refused
#[test]
fn export_refuses_invalid_statements_before_reading() {
    stack();
    setup();
    let use_option =
        "name the CONNECTION with CATALOG_CONNECTION = '<connection>' in the WITH options";
    let named_at = format!(
        "AT {DEFAULT_CATALOG_CONN_NAME} WITH CATALOG_CONNECTION = '{DEFAULT_CATALOG_CONN_NAME}' \
         TABLE_NAME = 'ns.t'"
    );
    let inline = format!(
        "AT 'http://catalog:8181' USER 'exporter' IDENTIFIED BY '{INLINE_PASSWORD}' \
         WITH CATALOG_CONNECTION = '{DEFAULT_CATALOG_CONN_NAME}' TABLE_NAME = 'ns.t'"
    );
    let appends_only =
        |clause: &str| format!("{clause} is not supported, because the export only appends rows");
    let cases = [
        (
            "WRITERS_PER_NODE with SINGLE_COMMIT",
            export_sql(
                FAILING_SOURCE,
                "WRITERS_PER_NODE = '2' SINGLE_COMMIT = 'true'",
            ),
            vec!["WRITERS_PER_NODE cannot be combined with SINGLE_COMMIT = 'true'".to_string()],
        ),
        (
            "unknown option",
            export_sql(FAILING_SOURCE, "FILE_FORMAT = 'csv'"),
            vec![
                "unknown option FILE_FORMAT; the accepted options are CATALOG_CONNECTION, TABLE_NAME, WRITERS_PER_NODE, and SINGLE_COMMIT"
                    .to_string(),
            ],
        ),
        (
            "WRITERS_PER_NODE of 0",
            export_sql(FAILING_SOURCE, "WRITERS_PER_NODE = '0'"),
            vec![
                "WRITERS_PER_NODE = '0' is not an integer of at least 1".to_string(),
                "WRITERS_PER_NODE = '4'".to_string(),
            ],
        ),
        (
            "no CATALOG_CONNECTION",
            export_into(FAILING_SOURCE, "WITH TABLE_NAME = 'ns.t'"),
            vec!["option CATALOG_CONNECTION is required".to_string()],
        ),
        (
            "named AT clause",
            export_into(FAILING_SOURCE, &named_at),
            vec![
                "the AT clause is not supported".to_string(),
                use_option.to_string(),
            ],
        ),
        (
            "inline AT clause",
            export_into(FAILING_SOURCE, &inline),
            vec![
                "the AT clause is not supported".to_string(),
                use_option.to_string(),
            ],
        ),
        (
            "TRUNCATE",
            export_sql(FAILING_SOURCE, "TRUNCATE"),
            vec![appends_only("TRUNCATE")],
        ),
        (
            "REPLACE",
            export_sql(FAILING_SOURCE, "REPLACE"),
            vec![appends_only("REPLACE")],
        ),
        (
            "CREATED BY",
            export_sql(FAILING_SOURCE, "CREATED BY 'CREATE TABLE ns.t (x INT)'"),
            vec![appends_only("CREATED BY")],
        ),
    ];

    for (case, sql, fragments) in cases {
        let fragments: Vec<&str> = fragments.iter().map(String::as_str).collect();
        let error = assert_refused_before_reading(case, &mut exa_conn(), &sql, &fragments);
        assert!(
            !error.contains(INLINE_PASSWORD),
            "{case}: the inline password leaked: {error}"
        );
    }
    let read_error = export_error(&mut exa_conn(), &export_sql(FAILING_SOURCE, ""));
    assert!(
        read_error.contains(SOURCE_READ_ERROR),
        "with valid options the export reads the failing source: {read_error}"
    );
}

/// Scenario: A user who holds the export privileges exports rows
#[test]
fn export_role_user_exports_rows() {
    stack();
    setup();
    exporters();

    assert_eq!(
        exported_rows(
            &mut exporter_conn(ROLE_EXPORTER),
            &export_sql(TYPED_TABLE, "")
        ),
        TYPED_ROWS
    );
}

/// Scenario: A missing script-scoped CONNECTION grant fails with a named error
#[test]
fn export_missing_script_grant_fails_named() {
    stack();
    setup();
    exporters();

    assert_refused_before_reading(
        "no script grant",
        &mut exporter_conn(NO_ACCESS_EXPORTER),
        &export_sql(FAILING_SOURCE, ""),
        &[&missing_grant_error(NO_ACCESS_EXPORTER)],
    );
}

/// Scenario: No CONNECTION value appears in the generated SQL or an error
#[test]
fn export_errors_carry_no_connection_password() {
    stack();
    setup();
    exporters();
    // A direct call skips the export hook, so only the writer's own check refuses it.
    let forged_writer_call = format!(
        r#"SELECT {SCHEMA_NAME}.{EXPORT_SCRIPT_NAME}('{{"connection":"{DEFAULT_CATALOG_CONN_NAME}"}}', 1) FROM DUAL"#
    );

    let error = export_error(&mut exporter_conn(NO_ACCESS_EXPORTER), &forged_writer_call);

    assert!(
        error.contains(&missing_grant_error(NO_ACCESS_EXPORTER)),
        "the writer must refuse a direct call without the script grant: {error}"
    );
}
