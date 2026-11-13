//! `EXPORT ... INTO SCRIPT` through the export script into the local Iceberg REST catalog. The
//! local stack has one node, so the writer count is `WRITERS_PER_NODE`, capped at 300.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, OnceLock};

use futures::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;

use crate::common::e2e_harness::{
    DEFAULT_CATALOG_CONN_NAME, EXPORT_SCRIPT_NAME, ExportRoute, SCHEMA_NAME, SOURCE_READ_ERROR,
    VsProps, assert_holds_rows, assert_refused_before_reading, create_virtual_schema,
    declared_types, exa_conn, export_error, exported_rows, failing_source, local_stack_s3_store,
    run_namespace, runtime,
};
use crate::common::exasol_ws::{ExaConn, profiled_export_counts};
use crate::common::seed::E2E_TABLE;
use crate::common::stack::{
    CatalogConnectionPassword, ICEBERG_REST_VIA_PROXY, PROXY_ADAPTER_SCHEMA, SEAWEEDFS_VIA_PROXY,
    build_create_connection_sql, exasol_host, exasol_sql_port, iceberg_catalog_url_internal,
    local_stack_connection_password, proxy_log_during,
};
use crate::fixtures::{
    LAKEHOUSE_VS, PROXY_ICEBERG_CREDS, lakehouse_vs, proxy_virtual_schemas, seed_catalog, stack,
    vs_table,
};

const EXPORT_SCHEMA: &str = "EXPORT_E2E";
const SOURCE_TABLE: &str = "EXPORT_E2E.SRC_ROWS";
const SOURCE_ROWS: i64 = 1_000;
const LARGE_TABLE: &str = "EXPORT_E2E.ROWS_10K";
const LARGE_ROWS: i64 = 10_000;
const TAGS_TABLE: &str = "EXPORT_E2E.EVENT_TAGS";
const MAX_WRITERS: i64 = 300;
const EXPORTER_PASSWORD: &str = "ExportE2eUser2026x";
const ROLE_EXPORTER: &str = "EXPORT_E2E_ROLE_EXPORTER";
const NO_ACCESS_EXPORTER: &str = "EXPORT_E2E_NO_ACCESS";
const WAREHOUSE_BUCKET: &str = "warehouse";

static FAILING_SOURCE: LazyLock<String> = LazyLock::new(|| failing_source(SOURCE_TABLE));
const INLINE_PASSWORD: &str = "ExportInlineSecret2026";
const AZURE_ACCOUNT_KEY: &str = "ZXhwb3J0LWUyZS1henVyZS1rZXk=";

fn setup() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        lakehouse_vs();
        let mut sys = exa_conn();
        sys.execute(&format!("CREATE SCHEMA IF NOT EXISTS {EXPORT_SCHEMA}"));
        sys.execute(&format!(
            "CREATE OR REPLACE TABLE {SOURCE_TABLE} AS SELECT \
               CAST(LEVEL AS DECIMAL(9,0)) AS ID, \
               CAST('v' || LEVEL AS VARCHAR(100)) AS LABEL, \
               CAST(MOD(LEVEL, 7) AS DECIMAL(9,0)) AS GRP \
             FROM DUAL CONNECT BY LEVEL <= {SOURCE_ROWS}"
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

/// A table no earlier export wrote.
fn new_table() -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!(
        "{}.t{}",
        run_namespace("misc"),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

const DIRECT: ExportRoute = ExportRoute {
    script_schema: SCHEMA_NAME,
    connection: DEFAULT_CATALOG_CONN_NAME,
};

/// The proxy-only scripts and CONNECTION of `proxy_virtual_schemas`, so a `scripts/proxy/filter`
/// line can inject a catalog or storage fault.
const PROXIED: ExportRoute = ExportRoute {
    script_schema: PROXY_ADAPTER_SCHEMA,
    connection: PROXY_ICEBERG_CREDS,
};

fn export_into(source: &str, tail: &str) -> String {
    DIRECT.export_into(source, tail)
}

/// A valid export of `source` into a new table, with `options`.
fn export_sql(source: &str, options: &str) -> String {
    DIRECT.export(source, &new_table(), options)
}

/// The exported row count and the writer count of one export. A fresh session holds only
/// this export, whose writer count is the `OUT_ROWS` of its one `GROUP BY` profile part.
#[track_caller]
fn profiled_export(source: &str, options: &str) -> (i64, i64) {
    profiled_export_counts(&mut exa_conn(), &export_sql(source, options))
}

/// The CONNECTION values no export error may carry.
fn connection_secrets() -> [String; 2] {
    let password = local_stack_connection_password();
    [password.access_key, password.secret_key]
}

fn table_exists(table: &str) -> bool {
    let ident = TableIdent::from_strs(table.split('.'))
        .unwrap_or_else(|e| panic!("{table} is not a table name: {e}"));
    runtime()
        .block_on(seed_catalog(runtime(), "export").table_exists(&ident))
        .unwrap_or_else(|e| panic!("check table {table}: {e}"))
}

fn namespace_exists(namespace: &str) -> bool {
    let ident = NamespaceIdent::new(namespace.to_string());
    runtime()
        .block_on(seed_catalog(runtime(), "export").namespace_exists(&ident))
        .unwrap_or_else(|e| panic!("check namespace {namespace}: {e}"))
}

/// The keys under `prefix` of the warehouse bucket.
fn objects_under(prefix: &str) -> Vec<String> {
    let store = local_stack_s3_store(WAREHOUSE_BUCKET);
    let prefix = ObjectStorePath::from(prefix);
    runtime()
        .block_on(
            store
                .list(Some(&prefix))
                .map_ok(|object| object.location.to_string())
                .try_collect(),
        )
        .unwrap_or_else(|e| panic!("list {WAREHOUSE_BUCKET}/{prefix}: {e}"))
}

/// Scenario: A whole-table export writes every source row
/// Scenario: A filtered query exports only the rows it returns
/// Scenario: A column-list export writes only the listed columns
/// Scenario: A join of a virtual schema table and an Exasol table exports the joined rows
/// Scenario: All scripts resolve from the same uploaded artifact
#[test]
fn export_writes_every_source_row() {
    stack();
    setup();
    let mut sys = exa_conn();
    let namespace = run_namespace("rows");
    let read_vs = "EXPORT_E2E_READ_BACK";
    let join = format!(
        "SELECT e.ID, e.NAME, t.TAG FROM {} e JOIN {TAGS_TABLE} t ON e.ID = t.ID",
        vs_table(LAKEHOUSE_VS, E2E_TABLE)
    );
    // The even IDs of the 20 events join one tag each.
    assert_eq!(sys.query_row_count(&join), 10, "the join selects 10 rows");
    let filtered = format!("SELECT ID, LABEL, GRP FROM {SOURCE_TABLE} WHERE MOD(ID, 4) = 0");
    let cases = [
        (
            "whole_table",
            SOURCE_TABLE.to_string(),
            format!("SELECT ID, LABEL, GRP FROM {SOURCE_TABLE}"),
            SOURCE_ROWS,
        ),
        (
            "filtered_rows",
            format!("({filtered})"),
            filtered.clone(),
            SOURCE_ROWS / 4,
        ),
        (
            "listed_columns",
            format!("{SOURCE_TABLE} (LABEL, ID)"),
            format!("SELECT LABEL, ID FROM {SOURCE_TABLE}"),
            SOURCE_ROWS,
        ),
        ("joined_rows", format!("({join})"), join.clone(), 10),
    ];

    for (table, source, _, expected) in &cases {
        let sql = DIRECT.export(source, &format!("{namespace}.{table}"), "");
        assert_eq!(exported_rows(&mut sys, &sql), *expected, "{table}");
    }
    create_virtual_schema(
        &mut sys,
        &VsProps::new(read_vs, &namespace).with_catalog_conn_name("EXPORT_E2E_READ_CREDS"),
    );

    for (table, _, query, expected) in &cases {
        assert_holds_rows(&mut sys, &vs_table(read_vs, table), query, *expected);
    }
    let listed: Vec<String> = declared_types(&mut sys, read_vs, "listed_columns")
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        listed,
        ["LABEL", "ID"],
        "only the listed columns, in list order"
    );
}

/// Scenario: WRITERS_PER_NODE sets the writer count
/// Scenario: The writer count never exceeds 300
/// Scenario: SINGLE_COMMIT runs one writer for the whole cluster
#[test]
fn export_writer_layout_sets_writer_count() {
    stack();
    setup();
    let typed_first = |n: i64| format!("(SELECT * FROM {SOURCE_TABLE} WHERE ID <= {n})");
    let large_first = |n: i64| format!("(SELECT * FROM {LARGE_TABLE} WHERE ID <= {n})");
    let per_node_3 = "WRITERS_PER_NODE = '3'";
    let per_node_500 = "WRITERS_PER_NODE = '500'";
    let cases = [
        (
            "3 writers per node over the table",
            SOURCE_TABLE.to_string(),
            per_node_3,
            (SOURCE_ROWS, 3),
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
            SOURCE_TABLE.to_string(),
            "",
            (SOURCE_ROWS, 1),
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
            SOURCE_TABLE.to_string(),
            "SINGLE_COMMIT = 'true'",
            (SOURCE_ROWS, 1),
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
    let never_rewrites = |clause: &str| {
        format!("{clause} is not supported, because the export never truncates or replaces a table")
    };
    let cases = [
        (
            "WRITERS_PER_NODE with SINGLE_COMMIT",
            export_sql(
                &FAILING_SOURCE,
                "WRITERS_PER_NODE = '2' SINGLE_COMMIT = 'true'",
            ),
            vec!["WRITERS_PER_NODE cannot be combined with SINGLE_COMMIT = 'true'".to_string()],
        ),
        (
            "unknown option",
            export_sql(&FAILING_SOURCE, "FILE_FORMAT = 'csv'"),
            vec![
                "unknown option FILE_FORMAT; the accepted options are CATALOG_CONNECTION, TABLE_NAME, WRITERS_PER_NODE, SINGLE_COMMIT, and ALLOW_HTTP"
                    .to_string(),
            ],
        ),
        (
            "WRITERS_PER_NODE of 0",
            export_sql(&FAILING_SOURCE, "WRITERS_PER_NODE = '0'"),
            vec![
                "WRITERS_PER_NODE = '0' is not a whole number from 1 to".to_string(),
                "WRITERS_PER_NODE = '4'".to_string(),
            ],
        ),
        (
            "ALLOW_HTTP of yes",
            export_into(
                &FAILING_SOURCE,
                &format!(
                    "WITH CATALOG_CONNECTION = '{DEFAULT_CATALOG_CONN_NAME}' TABLE_NAME = 'ns.t' \
                     ALLOW_HTTP = 'yes'"
                ),
            ),
            vec!["ALLOW_HTTP = 'yes' is not 'true' or 'false'".to_string()],
        ),
        (
            "no CATALOG_CONNECTION",
            export_into(&FAILING_SOURCE, "WITH TABLE_NAME = 'ns.t'"),
            vec!["option CATALOG_CONNECTION is required".to_string()],
        ),
        (
            "named AT clause",
            export_into(&FAILING_SOURCE, &named_at),
            vec![
                "the AT clause is not supported".to_string(),
                use_option.to_string(),
            ],
        ),
        (
            "inline AT clause",
            export_into(&FAILING_SOURCE, &inline),
            vec![
                "the AT clause is not supported".to_string(),
                use_option.to_string(),
            ],
        ),
        (
            "TRUNCATE",
            export_sql(&FAILING_SOURCE, "TRUNCATE"),
            vec![never_rewrites("TRUNCATE")],
        ),
        (
            "REPLACE",
            export_sql(&FAILING_SOURCE, "REPLACE"),
            vec![never_rewrites("REPLACE")],
        ),
        (
            "CREATED BY",
            export_sql(&FAILING_SOURCE, "CREATED BY 'CREATE TABLE ns.t (x INT)'"),
            vec![never_rewrites("CREATED BY")],
        ),
    ];

    for (case, sql, fragments) in cases {
        let fragments: Vec<&str> = fragments.iter().map(String::as_str).collect();
        let error = assert_refused_before_reading(
            case,
            &mut exa_conn(),
            &sql,
            &fragments,
            &connection_secrets(),
        );
        assert!(
            !error.contains(INLINE_PASSWORD),
            "{case}: the inline password leaked: {error}"
        );
    }
    let read_error = export_error(
        &mut exa_conn(),
        &export_sql(&FAILING_SOURCE, ""),
        &connection_secrets(),
    );
    assert!(
        read_error.contains(SOURCE_READ_ERROR),
        "with valid options the export reads the failing source: {read_error}"
    );
}

/// Scenario: Plain HTTP needs ALLOW_HTTP
#[test]
fn export_plain_http_needs_allow_http() {
    stack();
    setup();
    let conn = DEFAULT_CATALOG_CONN_NAME;
    let without_allow_http = export_into(
        &FAILING_SOURCE,
        &format!(
            "WITH CATALOG_CONNECTION = '{conn}' TABLE_NAME = '{}'",
            new_table()
        ),
    );

    assert_refused_before_reading(
        "without ALLOW_HTTP",
        &mut exa_conn(),
        &without_allow_http,
        &[
            &format!("field address of CONNECTION '{conn}' names a plain HTTP address"),
            "ALLOW_HTTP = 'true'",
        ],
        &connection_secrets(),
    );
    assert_eq!(
        exported_rows(
            &mut exa_conn(),
            &export_sql(
                &format!("(SELECT * FROM {SOURCE_TABLE} WHERE ID <= 10)"),
                ""
            )
        ),
        10,
        "with ALLOW_HTTP = 'true' the same export of a readable source succeeds"
    );
}

/// Scenario: A CONNECTION the export does not support is refused before any data is read
/// Scenario: A source column name the new table cannot hold is refused before any data is read
#[test]
fn export_refuses_unwritable_connections_and_column_names() {
    stack();
    setup();
    let mut sys = exa_conn();
    let azure = CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        account_name: Some("exporte2e".to_string()),
        account_key: Some(AZURE_ACCOUNT_KEY.to_string()),
        ..Default::default()
    };
    let sigv4 = CatalogConnectionPassword {
        use_sigv4: true,
        ..local_stack_connection_password()
    };
    let catalog = iceberg_catalog_url_internal();
    sys.execute(&build_create_connection_sql(
        "EXPORT_E2E_AZURE_CREDS",
        &catalog,
        &azure,
    ));
    sys.execute(&build_create_connection_sql(
        "EXPORT_E2E_SIGV4_CREDS",
        &catalog,
        &sigv4,
    ));
    let through = |connection: &'static str, options: &str| {
        let table = new_table();
        let route = ExportRoute {
            connection,
            ..DIRECT
        };
        (route.export(&FAILING_SOURCE, &table, options), table)
    };
    let azure_refusal = "supplies Azure storage field(s) account_name, account_key, but the export writes only to S3-compatible storage";
    let sigv4_refusal =
        "sets use_sigv4, but the export does not sign catalog requests with AWS SigV4";
    let repeated_name = new_table();
    let cases = [
        (
            "Azure storage",
            through("EXPORT_E2E_AZURE_CREDS", ""),
            azure_refusal,
        ),
        (
            "Azure storage, SINGLE_COMMIT",
            through("EXPORT_E2E_AZURE_CREDS", "SINGLE_COMMIT = 'true'"),
            azure_refusal,
        ),
        (
            "SigV4 signing",
            through("EXPORT_E2E_SIGV4_CREDS", ""),
            sigv4_refusal,
        ),
        (
            "SigV4 signing, SINGLE_COMMIT",
            through("EXPORT_E2E_SIGV4_CREDS", "SINGLE_COMMIT = 'true'"),
            sigv4_refusal,
        ),
        // Exasol refuses the repeated name itself, before the export hook runs.
        (
            "repeated column name",
            (
                DIRECT.export(
                    "(SELECT 1 / (ID - ID) AS A, 2 AS A FROM EXPORT_E2E.SRC_ROWS)",
                    &repeated_name,
                    "",
                ),
                repeated_name.clone(),
            ),
            "duplicate column name: A",
        ),
    ];

    for (case, (sql, table), refusal) in cases {
        let error =
            assert_refused_before_reading(case, &mut sys, &sql, &[refusal], &connection_secrets());
        assert!(
            !error.contains(AZURE_ACCOUNT_KEY),
            "{case}: the account key leaked: {error}"
        );
        assert!(!table_exists(&table), "{case}: {table} must not exist");
    }
}

/// Scenario: SINGLE_COMMIT on a catalog without staged creation fails and creates no table
#[test]
fn export_single_commit_on_catalog_refusing_stage_create_fails() {
    stack();
    setup();
    proxy_virtual_schemas();
    // The `scripts/proxy/filter` line answers HTTP 403 to every table creation in this namespace.
    let refused_table = "e2e_export_no_stage.t";

    let error = export_error(
        &mut exa_conn(),
        &PROXIED.export(SOURCE_TABLE, refused_table, "SINGLE_COMMIT = 'true'"),
        &connection_secrets(),
    );

    // The proxy resets the connection after its 403, so the answer's body breaks off.
    assert!(
        error.contains(&format!("staged creation of table {refused_table} failed"))
            && error.contains(&format!("{ICEBERG_REST_VIA_PROXY}/"))
            && error.contains("HTTP 403")
            && error.contains("SINGLE_COMMIT needs a catalog that accepts staged table creation"),
        "the writer must fail naming the catalog and staged creation: {error}"
    );
    assert!(
        !table_exists(refused_table),
        "{refused_table} must not exist"
    );
}

/// Scenario: A failed SINGLE_COMMIT export leaves no table
#[test]
fn export_failed_single_commit_leaves_no_table() {
    stack();
    setup();
    proxy_virtual_schemas();
    let source_error = run_namespace("sc_fail_source");
    let error = export_error(
        &mut exa_conn(),
        &DIRECT.export(
            &format!("(SELECT ID, 1 / (ID - 500) AS X FROM {SOURCE_TABLE})"),
            &format!("{source_error}.t"),
            "SINGLE_COMMIT = 'true'",
        ),
        &connection_secrets(),
    );
    assert!(
        error.contains(SOURCE_READ_ERROR),
        "a source error at row 500 fails the statement: {error}"
    );
    // The `scripts/proxy/filter` line answers HTTP 403 under `<namespace>/t/metadata/`, so the
    // writer fails at its first manifest write, after its data file.
    let manifest_fault = run_namespace("sc_fail_manifest");
    let location = format!("{WAREHOUSE_BUCKET}/{manifest_fault}/t");
    let log = proxy_log_during(|| {
        export_error(
            &mut exa_conn(),
            &PROXIED.export(
                SOURCE_TABLE,
                &format!("{manifest_fault}.t"),
                "SINGLE_COMMIT = 'true'",
            ),
            &connection_secrets(),
        );
    });
    assert!(
        log.lines().any(|line| {
            line.contains(&format!("PUT {SEAWEEDFS_VIA_PROXY}/{location}/data/"))
                && line.contains(".parquet")
        }),
        "the writer must have written a data file under {location}/data/: {log}"
    );

    for namespace in [source_error, manifest_fault] {
        let table = format!("{namespace}.t");
        assert!(!table_exists(&table), "{table} must not exist");
        assert!(
            namespace_exists(&namespace),
            "the namespace {namespace} the export created must remain"
        );
        let data_files = objects_under(&format!("{namespace}/t/data/"));
        assert!(
            data_files.is_empty(),
            "the writer's data files must be gone: {data_files:?}"
        );
    }
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
            &export_sql(SOURCE_TABLE, "")
        ),
        SOURCE_ROWS
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
        &export_sql(&FAILING_SOURCE, ""),
        &[&missing_grant_error(NO_ACCESS_EXPORTER)],
        &connection_secrets(),
    );
}

/// Scenario: No CONNECTION value appears in the generated SQL or an error
#[test]
fn export_errors_carry_no_connection_password() {
    stack();
    setup();
    exporters();
    // A direct call skips the export hook, so only the writer's own check refuses it.
    let writer_spec = serde_json::json!({
        "connection": DEFAULT_CATALOG_CONN_NAME,
        "table": new_table(),
        "export_id": "0".repeat(32),
        "columns": ["X"],
        "single_commit": false,
        "allow_http": true,
    });
    let forged_writer_call =
        format!("SELECT {SCHEMA_NAME}.{EXPORT_SCRIPT_NAME}('{writer_spec}', 1) FROM DUAL");

    let error = export_error(
        &mut exporter_conn(NO_ACCESS_EXPORTER),
        &forged_writer_call,
        &connection_secrets(),
    );

    assert!(
        error.contains(&missing_grant_error(NO_ACCESS_EXPORTER)),
        "the writer must refuse a direct call without the script grant: {error}"
    );
}
