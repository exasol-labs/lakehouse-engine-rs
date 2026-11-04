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
const UNSUPPORTED_TABLE: &str = "EXPORT_E2E.UNSUPPORTED";
const TAGS_TABLE: &str = "EXPORT_E2E.EVENT_TAGS";
const MAX_WRITERS: i64 = 300;
const EXPORTER_PASSWORD: &str = "ExportE2eUser2026x";
const ROLE_EXPORTER: &str = "EXPORT_E2E_ROLE_EXPORTER";
const NO_ACCESS_EXPORTER: &str = "EXPORT_E2E_NO_ACCESS";

/// Every row raises when read, so an error without the read error proves the export
/// failed before reading the source.
const FAILING_SOURCE: &str = "(SELECT 1 / (ID - ID) AS X FROM EXPORT_E2E.TYPED)";
const SOURCE_READ_ERROR: &str = "division by zero";
const EXASOL_TYPE_ERROR: &str = "Column type not supported";

/// Each column of `UNSUPPORTED_TABLE` but `ID`, with the start of the type its error names.
const UNSUPPORTED_COLUMNS: [(&str, &str); 5] = [
    ("C_GEOMETRY", "GEOMETRY"),
    ("C_HASHTYPE", "HASHTYPE"),
    ("C_INTERVAL_YM", "INTERVAL YEAR"),
    ("C_INTERVAL_DS", "INTERVAL DAY"),
    ("C_TSLTZ", "TIMESTAMP(3) WITH LOCAL TIME ZONE"),
];

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
            "CREATE OR REPLACE TABLE {UNSUPPORTED_TABLE} AS SELECT \
               CAST(1 AS DECIMAL(9,0)) AS ID, \
               CAST('POINT(1 2)' AS GEOMETRY) AS C_GEOMETRY, \
               CAST('550e8400-e29b-11d4-a716-446655440000' AS HASHTYPE) AS C_HASHTYPE, \
               CAST('5-3' AS INTERVAL YEAR TO MONTH) AS C_INTERVAL_YM, \
               CAST('2 12:50:10.123' AS INTERVAL DAY TO SECOND) AS C_INTERVAL_DS, \
               CAST(TIMESTAMP '2024-01-01 12:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE) AS C_TSLTZ \
             FROM DUAL"
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
        let privileges = [
            "EXPORT".to_string(),
            format!("CONNECTION {conn}"),
            format!("EXECUTE ON SCRIPT {script}"),
        ];
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
        &format!("AT {DEFAULT_CATALOG_CONN_NAME} WITH TABLE_NAME = 'ns.t' {options}"),
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
fn assert_refused_before_reading(conn: &mut ExaConn, sql: &str, fragments: &[&str]) -> String {
    let error = export_error(conn, sql);
    assert!(
        fragments.iter().all(|fragment| error.contains(fragment))
            && !error.contains(SOURCE_READ_ERROR),
        "{sql} must fail before reading the source, naming {fragments:?}: {error}"
    );
    error
}

fn guard_message(column: &str, type_start: &str) -> String {
    format!("{EXPORT_SCRIPT_NAME}: column \"{column}\" has type {type_start}")
}

/// Exasol's own UDF type check races the guard, so the refusal carries either message.
#[track_caller]
fn assert_type_refused(source: &str, columns: &[(&str, &str)]) {
    let sql = export_sql(source, "");
    let error = export_error(&mut exa_conn(), &sql);
    assert!(
        error.contains(EXASOL_TYPE_ERROR)
            || columns
                .iter()
                .any(|(column, type_start)| error.contains(&guard_message(column, type_start))),
        "{sql} must be refused with the guard error for one of {columns:?} or `{EXASOL_TYPE_ERROR}`: {error}"
    );
}

/// Scenario: A whole-table export reports every source row
/// Scenario: All scripts resolve from the same uploaded artifact
#[test]
fn export_whole_table_reports_every_row() {
    stack();
    setup();

    assert_eq!(
        exported_rows(&mut exa_conn(), &export_sql(TYPED_TABLE, "")),
        TYPED_ROWS
    );
}

/// Scenario: A filtered query exports only the rows it returns
#[test]
fn export_filtered_query_reports_returned_rows() {
    stack();
    setup();
    let source = format!("(SELECT * FROM {TYPED_TABLE} WHERE MOD(ID, 4) = 0)");

    assert_eq!(
        exported_rows(&mut exa_conn(), &export_sql(&source, "")),
        TYPED_ROWS / 4
    );
}

/// Scenario: A column-list export reports every row of the table
#[test]
fn export_column_list_reports_every_row() {
    stack();
    setup();
    let source = format!("{TYPED_TABLE} (ID, C_VARCHAR)");

    assert_eq!(
        exported_rows(&mut exa_conn(), &export_sql(&source, "")),
        TYPED_ROWS
    );
}

/// Scenario: A join of a virtual schema table and an Exasol table exports the joined rows
#[test]
fn export_vs_join_reports_joined_rows() {
    stack();
    setup();
    let mut sys = exa_conn();
    let query = format!(
        "SELECT e.ID, e.NAME, t.TAG FROM {} e JOIN {TAGS_TABLE} t ON e.ID = t.ID",
        vs_table(LAKEHOUSE_VS, E2E_TABLE)
    );
    let selected = sys.query_row_count(&query);

    let exported = exported_rows(&mut sys, &export_sql(&format!("({query})"), ""));

    assert_eq!(
        selected, 10,
        "the even IDs of the 20 events join one tag each"
    );
    assert_eq!(exported, selected);
}

/// Scenario: An empty source exports zero rows
#[test]
fn export_empty_source_reports_zero() {
    stack();
    setup();
    let source = format!("(SELECT * FROM {TYPED_TABLE} WHERE ID < 0)");

    assert_eq!(exported_rows(&mut exa_conn(), &export_sql(&source, "")), 0);
}

/// Scenario: WRITERS_PER_NODE sets the writer count
#[test]
fn export_writers_per_node_sets_writer_count() {
    stack();
    setup();
    let first_rows = |n: i64| format!("(SELECT * FROM {TYPED_TABLE} WHERE ID <= {n})");

    assert_eq!(
        profiled_export(TYPED_TABLE, "WRITERS_PER_NODE = '3'"),
        (TYPED_ROWS, 3)
    );
    assert_eq!(
        profiled_export(&first_rows(3), "WRITERS_PER_NODE = '3'"),
        (3, 3)
    );
    assert_eq!(
        profiled_export(&first_rows(2), "WRITERS_PER_NODE = '3'"),
        (2, 2)
    );
    assert_eq!(profiled_export(TYPED_TABLE, ""), (TYPED_ROWS, 1));
}

/// Scenario: The writer count never exceeds 300
#[test]
fn export_writer_count_caps_at_300() {
    stack();
    setup();
    let first_300 = format!("(SELECT * FROM {LARGE_TABLE} WHERE ID <= {MAX_WRITERS})");

    assert_eq!(
        profiled_export(LARGE_TABLE, "WRITERS_PER_NODE = '500'"),
        (LARGE_ROWS, MAX_WRITERS)
    );
    assert_eq!(
        profiled_export(&first_300, "WRITERS_PER_NODE = '500'"),
        (MAX_WRITERS, MAX_WRITERS)
    );
}

/// Scenario: SINGLE_COMMIT runs one writer for the whole cluster
#[test]
fn export_single_commit_runs_one_writer() {
    stack();
    setup();

    assert_eq!(
        profiled_export(TYPED_TABLE, "SINGLE_COMMIT = 'true'"),
        (TYPED_ROWS, 1)
    );
}

/// Scenario: WRITERS_PER_NODE together with SINGLE_COMMIT is refused
#[test]
fn export_refuses_writers_per_node_with_single_commit() {
    stack();
    setup();

    assert_refused_before_reading(
        &mut exa_conn(),
        &export_sql(
            FAILING_SOURCE,
            "WRITERS_PER_NODE = '2' SINGLE_COMMIT = 'true'",
        ),
        &["WRITERS_PER_NODE cannot be combined with SINGLE_COMMIT = 'true'"],
    );
}

/// Scenario: An invalid option fails the export before any data is read
#[test]
fn export_refuses_invalid_options_before_reading() {
    stack();
    setup();

    assert_refused_before_reading(
        &mut exa_conn(),
        &export_sql(FAILING_SOURCE, "FILE_FORMAT = 'csv'"),
        &[
            "unknown option FILE_FORMAT; the accepted options are TABLE_NAME, WRITERS_PER_NODE, and SINGLE_COMMIT",
        ],
    );
    assert_refused_before_reading(
        &mut exa_conn(),
        &export_sql(FAILING_SOURCE, "WRITERS_PER_NODE = '0'"),
        &[
            "WRITERS_PER_NODE = '0' is not an integer of at least 1",
            "WRITERS_PER_NODE = '4'",
        ],
    );
    let read_error = export_error(&mut exa_conn(), &export_sql(FAILING_SOURCE, ""));
    assert!(
        read_error.contains(SOURCE_READ_ERROR),
        "with valid options the export reads the failing source: {read_error}"
    );
}

/// Scenario: An export without a named CONNECTION fails before any data is read
#[test]
fn export_refuses_missing_or_inline_connection() {
    stack();
    setup();
    let inline_password = "ExportInlineSecret2026";
    let ask_for_connection =
        "add AT <connection>, naming the CONNECTION object the virtual schema uses";

    assert_refused_before_reading(
        &mut exa_conn(),
        &export_into(FAILING_SOURCE, "WITH TABLE_NAME = 'ns.t'"),
        &[
            "the EXPORT statement names no CONNECTION",
            ask_for_connection,
        ],
    );
    let inline = assert_refused_before_reading(
        &mut exa_conn(),
        &export_into(
            FAILING_SOURCE,
            &format!(
                "AT 'http://catalog:8181' USER 'exporter' IDENTIFIED BY '{inline_password}' \
                 WITH TABLE_NAME = 'ns.t'"
            ),
        ),
        &["an inline AT", "is not supported", ask_for_connection],
    );
    assert!(
        !inline.contains(inline_password),
        "the inline password leaked: {inline}"
    );
}

/// Scenario: TRUNCATE, REPLACE, and CREATED BY are refused
#[test]
fn export_refuses_truncate_replace_created_by() {
    stack();
    setup();

    for (clause, name) in [
        ("TRUNCATE", "TRUNCATE"),
        ("REPLACE", "REPLACE"),
        ("CREATED BY 'CREATE TABLE ns.t (x INT)'", "CREATED BY"),
    ] {
        assert_refused_before_reading(
            &mut exa_conn(),
            &export_sql(FAILING_SOURCE, clause),
            &[&format!(
                "{name} is not supported, because the export only appends rows"
            )],
        );
    }
}

/// Scenario: A column of an unsupported type is refused
#[test]
fn export_refuses_unsupported_column_types() {
    stack();
    setup();

    for (column, type_start) in UNSUPPORTED_COLUMNS {
        let source = format!("{UNSUPPORTED_TABLE} (ID, {column})");
        assert_type_refused(&source, &[(column, type_start)]);
    }
    assert_type_refused(UNSUPPORTED_TABLE, &UNSUPPORTED_COLUMNS);
    assert_type_refused(
        &format!("(SELECT C_GEOMETRY AS SHAPE FROM {UNSUPPORTED_TABLE})"),
        &[("SHAPE", "GEOMETRY")],
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
