//! `CATALOG_KIND = 'GLUE'` against real AWS Glue and S3 with a local Exasol. Run with
//! `--test-threads=1`: the tests share one Exasol provisioning and one virtual schema name.
#![cfg(feature = "glue-e2e")]

mod common;

use common::cloud_fixture::panic_message;
use common::e2e_harness::{
    SYS_PASSWORD, VARCHAR_JSON, VsProps, assert_binary_values_refused, assert_text_columns,
    assert_type_matrix, create_schema_and_scripts, declared_types, exa_conn, explain_virtual_sql,
    install_slc, int_column, pairs, parse_int, query_error, reads, refuses,
    try_create_virtual_schema_with_password, upload_so, value_to_string,
};
use common::exasol_ws::ExaConn;
use common::glue::{
    ACCESS_KEY_ID_VAR, ALL_TYPES, BINARY_VALUES, FixturePartition, GlueEnv, GlueRun,
    ICEBERG_ORDERS, ORC_INPUT_FORMAT, ORDERS, PARTITIONED, PARTITIONS, PartitionedRow,
    ROUTED_TABLES, SECRET_ACCESS_KEY_VAR, SKIPPED_TABLES, STALE_GLUE_COLUMN, SUCCESS_MARKER,
    glue_assume_role_connection_password, glue_base_identity_connection_password,
    glue_connection_password, metadata_only_keys, partitioned_rows, register_fixture_set,
    register_probe_table,
};
use common::seed::{
    ALL_TYPES_IDS_TEXT, BOOLEAN_VALUES_TEXT, DATE_VALUES_TEXT, DECIMAL_10_2_VALUES_TEXT,
    DECIMAL_38_10_VALUES_TEXT, FLOAT32_VALUES_TEXT, INT_LIST_VALUES_TEXT, INT8_VALUES_TEXT,
    INT16_VALUES_TEXT, INT32_VALUES_TEXT, TEXT_VALUES_TEXT, TIMESTAMP_VALUES_TEXT,
};
use common::stack::{
    CatalogConnectionPassword, build_create_connection_sql, exasol_host, exasol_sql_port,
    wait_for_exasol,
};
use common::timestamp_precision::expected_timestamp_precision;

use futures::FutureExt;
use serde_json::Value;

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;

const VS: &str = "GLUE_LAKEHOUSE";
const CONN: &str = "GLUE_E2E_CREDS";
const ROLE_VS: &str = "GLUE_LAKEHOUSE_ROLE";
const ROLE_CONN: &str = "GLUE_E2E_ROLE_CREDS";
const BASE_VS: &str = "GLUE_LAKEHOUSE_BASE";
const BASE_CONN: &str = "GLUE_E2E_BASE_CREDS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

/// SeaweedFS and the Iceberg REST fixture are deliberately not awaited: this suite's storage and
/// catalog are AWS.
fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        install_slc();
        upload_so();
        create_schema_and_scripts(&mut exa_conn());
    });
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// The environment is read before anything else, so a missing variable leaves no resource, and
/// the local Exasol is prepared next, so an unavailable stack creates no cloud resource.
fn provision() -> GlueRun {
    let env = GlueEnv::from_environment();
    setup();
    let rt = runtime();
    let run = env.expect(rt.block_on(GlueRun::create(&env)), "create the Glue run");
    env.expect(
        rt.block_on(register_fixture_set(&run)),
        &format!("register the fixture set in {}", run.database()),
    );
    assert_glue_vs_created(
        &env,
        VS,
        CONN,
        &run.database(),
        &glue_connection_password(&env),
    );
    run
}

fn vs_table(name: &str) -> String {
    format!("{VS}.{}", name.to_uppercase())
}

fn glue_vs_props<'a>(vs_name: &'a str, conn_name: &'a str, namespace: &'a str) -> VsProps<'a> {
    VsProps::new(vs_name, namespace)
        .with_catalog_conn_name(conn_name)
        .with_catalog_kind("GLUE")
}

/// A redacting connection, because the Exasol error for credential-bearing DDL may echo the
/// statement.
fn redacting_conn() -> ExaConn {
    ExaConn::connect_redacting(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD)
}

fn try_create_glue_vs(
    env: &GlueEnv,
    vs_name: &str,
    conn_name: &str,
    namespace: &str,
    password: &CatalogConnectionPassword,
) -> Value {
    try_create_virtual_schema_with_password(
        &mut redacting_conn(),
        &glue_vs_props(vs_name, conn_name, namespace),
        &env.glue_endpoint(),
        password,
    )
}

fn assert_glue_vs_created(
    env: &GlueEnv,
    vs_name: &str,
    conn_name: &str,
    namespace: &str,
    password: &CatalogConnectionPassword,
) {
    let response = try_create_glue_vs(env, vs_name, conn_name, namespace, password);
    assert_eq!(
        response["status"].as_str(),
        Some("ok"),
        "CREATE VIRTUAL SCHEMA {vs_name} failed: {}",
        env.redact(&response["exception"].to_string())
    );
}

/// The data files of the partitions whose `p_int` is `p_int`, or of every partition for `None`.
fn partition_files(p_int: Option<&str>) -> Vec<&'static str> {
    PARTITIONS
        .iter()
        .filter(|partition| p_int.is_none_or(|p_int| partition.values[0] == p_int))
        .flat_map(|partition| partition.files.iter().map(|file| file.name))
        .collect()
}

fn assert_scan_names_only(pushed: &str, kept: &[&str], context: &str) {
    for file in partition_files(None) {
        assert_eq!(
            pushed.contains(file),
            kept.contains(&file),
            "{context}: the pushed scan must name exactly the kept files {kept:?}, and file \
             {file} breaks that: {pushed}"
        );
    }
    assert!(
        !pushed.contains(SUCCESS_MARKER),
        "{context}: the pushed scan must never name the {SUCCESS_MARKER} marker: {pushed}"
    );
}

/// Scenario: Each run provisions its own Glue database and S3 prefix and removes both, including on panic
#[test]
fn glue_run_resources_are_removed_when_the_scope_ends_including_on_panic() {
    let env = GlueEnv::from_environment();
    let rt = runtime();
    let assert_present = |database: &str, prefix: &str, present: bool, how: &str| {
        let exists = env.expect(
            rt.block_on(env.database_exists(database)),
            &format!("check {database}"),
        );
        assert_eq!(
            exists, present,
            "{how}: does Glue database {database} exist"
        );
        let objects = env.expect(
            rt.block_on(env.object_keys(prefix)),
            &format!("list {prefix}"),
        );
        assert_eq!(
            !objects.is_empty(),
            present,
            "{how}: are there objects under {prefix}: {objects:?}"
        );
    };

    let (database, prefix) = {
        let run = env.expect(rt.block_on(GlueRun::create(&env)), "create a run");
        env.expect(
            rt.block_on(register_probe_table(&run)),
            "register the probe",
        );
        assert_present(
            &run.database(),
            &run.object_prefix(),
            true,
            "a run in scope with its probe",
        );
        (run.database(), run.object_prefix())
    };
    assert_present(&database, &prefix, false, "a scope that ends normally");

    let mut created = None;
    let outcome = rt.block_on(
        AssertUnwindSafe(async {
            let run = env.expect(GlueRun::create(&env).await, "create a run");
            env.expect(register_probe_table(&run).await, "register the probe");
            created = Some((run.database(), run.object_prefix()));
            panic!("deliberate panic to exercise the run's Drop while unwinding inside block_on");
        })
        .catch_unwind(),
    );
    assert!(
        outcome.is_err(),
        "the inner async block must have panicked, which is what this test exercises"
    );
    let (database, prefix) = created.expect("the run was created before the deliberate panic");
    assert_present(&database, &prefix, false, "a scope that ends in a panic");
}

type ReadOnlyCheck = fn(&GlueRun, &mut ExaConn);

/// Every check here only reads the fixture, so one provisioning serves them all. Each check runs
/// even after another failed, and the panic hook has already printed each failure.
#[test]
fn glue_read_only_checks_pass_against_one_provisioned_fixture() {
    let run = provision();
    let checks: [(&str, ReadOnlyCheck); 7] = [
        (
            "listing",
            check_listing_includes_routed_tables_and_records_every_skip,
        ),
        (
            "pushdown rows",
            check_queries_return_expected_rows_through_pushdown,
        ),
        (
            "partition cases",
            check_partition_cases_return_their_glue_values,
        ),
        ("ORC partition", check_orc_partition_fails_loud),
        (
            "partition pruning",
            check_partition_predicate_reduces_the_scan_file_list,
        ),
        (
            "all types",
            check_all_types_declare_and_return_their_mapped_values,
        ),
        (
            "assume role",
            check_assume_role_connection_reads_through_the_role,
        ),
    ];
    let failed: Vec<&str> = checks
        .iter()
        .filter(|(_, check)| {
            std::panic::catch_unwind(AssertUnwindSafe(|| check(&run, &mut exa_conn()))).is_err()
        })
        .map(|(name, _)| *name)
        .collect();
    assert!(failed.is_empty(), "failed read-only checks: {failed:?}");
}

/// Scenario: The listing includes the routed tables and records every skip
fn check_listing_includes_routed_tables_and_records_every_skip(run: &GlueRun, conn: &mut ExaConn) {
    let listed = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = '{VS}' \
         ORDER BY TABLE_NAME"
    ));
    let listed: Vec<String> = listed[0].iter().map(value_to_string).collect();
    let routed: Vec<String> = ROUTED_TABLES.iter().map(|t| t.to_uppercase()).collect();
    assert_eq!(
        listed, routed,
        "the listing admits exactly the routed tables"
    );

    assert_eq!(
        declared_types(conn, VS, ICEBERG_ORDERS),
        pairs(&[
            ("ORDER_ID", "DECIMAL(20,0)"),
            ("CUSTOMER", VARCHAR_JSON),
            ("AMOUNT", "DECIMAL(10,2)"),
            ("ORDER_DATE", "DATE"),
        ]),
        "the Iceberg columns come from metadata.json, never Glue's {STALE_GLUE_COLUMN} copy"
    );
    assert_eq!(
        declared_types(conn, VS, PARTITIONED),
        pairs(&[
            ("ID", "DECIMAL(20,0)"),
            ("V", VARCHAR_JSON),
            ("P_INT", "DECIMAL(10,0)"),
            ("P_DATE", "DATE"),
            ("P_STR", VARCHAR_JSON),
        ]),
        "a Parquet table declares its storage columns, then its partition keys"
    );
    let notes = conn.query_columns(&format!(
        "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = '{VS}'"
    ));
    let notes: Value = serde_json::from_str(&value_to_string(&notes[0][0]))
        .unwrap_or_else(|e| panic!("ADAPTER_NOTES must be JSON: {e}"));
    let skipped = notes["SKIPPED_TABLES"]
        .as_array()
        .unwrap_or_else(|| panic!("ADAPTER_NOTES must carry SKIPPED_TABLES: {notes}"));
    let recorded: BTreeSet<String> = skipped
        .iter()
        .map(|entry| value_to_string(&entry["table"]))
        .collect();
    let database = run.database();
    let expected: BTreeSet<String> = SKIPPED_TABLES
        .iter()
        .map(|(name, _)| format!("{database}.{name}"))
        .collect();
    assert_eq!(
        recorded, expected,
        "SKIPPED_TABLES records every skip: {notes}"
    );
    for (name, reason) in SKIPPED_TABLES {
        let entry = skipped
            .iter()
            .find(|entry| value_to_string(&entry["table"]) == format!("{database}.{name}"))
            .expect("checked above");
        assert!(
            value_to_string(&entry["reason"]).contains(reason),
            "the skip of {name} must state {reason:?}: {entry}"
        );
    }

    let env = run.env();
    let objects = env.expect(
        runtime().block_on(env.object_keys(&run.object_prefix())),
        "list the run prefix",
    );
    for key in metadata_only_keys() {
        let prefix = format!("{}{key}", run.object_prefix());
        assert!(
            !objects.iter().any(|object| object.starts_with(&prefix)),
            "no data file may exist under the metadata-only location {prefix}, so each skip \
             rests on metadata alone"
        );
    }
}

/// Scenario: Queries through pushdown return the expected rows
fn check_queries_return_expected_rows_through_pushdown(_run: &GlueRun, conn: &mut ExaConn) {
    let orders = vs_table(ICEBERG_ORDERS);

    assert_text_columns(
        conn,
        &format!("SELECT ORDER_ID, CUSTOMER, AMOUNT, ORDER_DATE FROM {orders} ORDER BY ORDER_ID"),
        &[
            ORDERS.map(|o| Some(o.order_id.to_string())),
            ORDERS.map(|o| o.customer.map(str::to_string)),
            ORDERS.map(|o| Some(o.amount())),
            ORDERS.map(|o| Some(o.order_date.to_string())),
        ],
    );

    let unpushed_filtered: Vec<i64> = ORDERS
        .iter()
        .filter(|o| o.amount_hundredths > 2000)
        .map(|o| o.order_id)
        .collect();
    let filtered = conn.query_columns(&format!(
        "SELECT ORDER_ID FROM {orders} WHERE AMOUNT > 20 ORDER BY ORDER_ID"
    ));
    assert_eq!(
        int_column(&filtered[0]),
        unpushed_filtered,
        "projection and filter pushdown return the rows the unpushed scan filters to"
    );
    let limited = conn.query_columns(&format!(
        "SELECT ORDER_ID FROM {orders} WHERE AMOUNT > 20 ORDER BY ORDER_ID LIMIT 2"
    ));
    assert_eq!(int_column(&limited[0]), unpushed_filtered[..2]);

    let partitioned = vs_table(PARTITIONED);
    let full = conn.query_columns(&format!(
        "SELECT ID, V FROM {partitioned} WHERE P_INT < 9 ORDER BY ID"
    ));
    let unpushed_filtered: Vec<i64> = (0..full[0].len())
        .filter(|&i| value_to_string(&full[1][i]) != "b")
        .map(|i| parse_int(&full[0][i]))
        .collect();
    let filtered = conn.query_columns(&format!(
        "SELECT ID FROM {partitioned} WHERE P_INT < 9 AND V <> 'b' ORDER BY ID"
    ));
    assert_eq!(int_column(&filtered[0]), unpushed_filtered);
    let limited = conn.query_columns(&format!(
        "SELECT ID FROM {partitioned} WHERE P_INT < 9 ORDER BY ID LIMIT 3"
    ));
    assert_eq!(int_column(&limited[0]), [1, 2, 3]);
}

/// Scenario: Each kept partition's location is listed and its files carry the partition's Glue values
fn check_partition_cases_return_their_glue_values(_run: &GlueRun, conn: &mut ExaConn) {
    let partitioned = vs_table(PARTITIONED);
    let expected = partitioned_rows();
    let column = |cell: fn(&FixturePartition, &PartitionedRow) -> Option<String>| {
        expected
            .iter()
            .map(|(partition, row)| cell(partition, row))
            .collect::<Vec<_>>()
    };

    // P_STR proves the default partition reads NULL, `a b/c` reads decoded, and the out-of-root
    // and s3a:// partitions read their Glue values, never a value parsed from a path.
    assert_text_columns(
        conn,
        &format!(
            "SELECT ID, V, P_INT, P_DATE, P_STR FROM {partitioned} WHERE P_INT < 9 ORDER BY ID"
        ),
        &[
            column(|_, row| Some(row.id.to_string())),
            column(|_, row| Some(row.v.to_string())),
            column(|partition, _| Some(partition.p_int().to_string())),
            column(|partition, _| Some(partition.p_date().to_string())),
            column(|partition, _| partition.p_str().map(str::to_string)),
        ],
    );

    let null_ids = conn.query_columns(&format!(
        "SELECT ID FROM {partitioned} WHERE P_INT < 9 AND P_STR IS NULL ORDER BY ID"
    ));
    assert_eq!(int_column(&null_ids[0]), [4]);
    let slash_ids = conn.query_columns(&format!(
        "SELECT ID FROM {partitioned} WHERE P_STR = 'a b/c' ORDER BY ID"
    ));
    assert_eq!(int_column(&slash_ids[0]), [5]);
}

/// Scenario: Queries through pushdown return the expected rows
fn check_orc_partition_fails_loud(run: &GlueRun, conn: &mut ExaConn) {
    let orc = PARTITIONS
        .iter()
        .find(|partition| partition.input_format == ORC_INPUT_FORMAT)
        .expect("the fixture set has an ORC partition");
    let orc_location = orc.place.location(run);

    let error = query_error(
        conn,
        &format!("SELECT COUNT(*) FROM {}", vs_table(PARTITIONED)),
    );
    for fragment in [
        "p_int=9",
        ORC_INPUT_FORMAT,
        "is not Parquet",
        orc_location.trim_end_matches('/'),
    ] {
        assert!(
            error.contains(fragment),
            "a query reading the ORC partition must fail naming {fragment:?}: {error}"
        );
    }
    assert!(
        !error.contains(run.env().secret_access_key()),
        "the failure must not contain the secret access key"
    );
}

/// Scenario: A partition predicate prunes partitions before their locations are listed
fn check_partition_predicate_reduces_the_scan_file_list(_run: &GlueRun, conn: &mut ExaConn) {
    let partitioned = vs_table(PARTITIONED);
    let pushed = explain_virtual_sql(
        conn,
        &format!("SELECT COUNT(*) FROM {partitioned} WHERE P_INT < 9"),
    );
    assert_scan_names_only(&pushed, &partition_files(None), "P_INT < 9");

    let pushed = explain_virtual_sql(
        conn,
        &format!("SELECT COUNT(*) FROM {partitioned} WHERE P_INT = 1"),
    );
    assert_scan_names_only(&pushed, &partition_files(Some("1")), "P_INT = 1");

    let predicate = "P_INT = 1 AND P_DATE >= DATE '2024-01-02'";
    let pushed = explain_virtual_sql(
        conn,
        &format!("SELECT COUNT(*) FROM {partitioned} WHERE {predicate}"),
    );
    assert_scan_names_only(&pushed, &["20240102_000000_00001_p2"], predicate);

    let count = |conn: &mut ExaConn, predicate: &str| {
        conn.query_scalar_i64(&format!(
            "SELECT COUNT(*) FROM {partitioned} WHERE {predicate}"
        ))
    };
    assert_eq!(count(conn, "P_INT = 1"), 4);
    assert_eq!(count(conn, predicate), 1);
    let limited = conn.query_columns(&format!(
        "SELECT ID FROM {partitioned} WHERE P_INT = 2 LIMIT 1"
    ));
    assert_eq!(int_column(&limited[0]), [5]);
}

/// Scenario: Every Hive type declares and returns its mapped value on a Glue Parquet table
fn check_all_types_declare_and_return_their_mapped_values(_run: &GlueRun, conn: &mut ExaConn) {
    let timestamp = expected_timestamp_precision(conn).declared_column_type;
    assert_type_matrix(
        conn,
        VS,
        ALL_TYPES,
        &[
            reads("ID", "DECIMAL(20,0)", ALL_TYPES_IDS_TEXT),
            reads("H_TINYINT", "DECIMAL(3,0)", INT8_VALUES_TEXT),
            reads("H_SMALLINT", "DECIMAL(5,0)", INT16_VALUES_TEXT),
            reads("H_INT", "DECIMAL(10,0)", INT32_VALUES_TEXT),
            reads("H_INTEGER", "DECIMAL(10,0)", [Some("7"), Some("-7"), None]),
            reads(
                "H_BIGINT",
                "DECIMAL(20,0)",
                [
                    Some("9223372036854775807"),
                    Some("-9223372036854775808"),
                    None,
                ],
            ),
            reads("H_FLOAT", "DOUBLE", FLOAT32_VALUES_TEXT),
            reads("H_DOUBLE", "DOUBLE", [Some("2.5"), Some("-0.125"), None]),
            reads("H_BOOLEAN", "BOOLEAN", BOOLEAN_VALUES_TEXT),
            reads("H_STRING", VARCHAR_JSON, TEXT_VALUES_TEXT),
            reads(
                "H_STRING_OVER_BINARY",
                VARCHAR_JSON,
                [Some("legacy-a"), Some("legacy-b"), None],
            ),
            reads(
                "H_VARCHAR",
                VARCHAR_JSON,
                [Some("short"), Some("text"), None],
            ),
            reads("H_CHAR", VARCHAR_JSON, [Some("abcde"), Some("fghij"), None]),
            reads("H_DECIMAL_10_2", "DECIMAL(10,2)", DECIMAL_10_2_VALUES_TEXT),
            reads(
                "H_DECIMAL",
                "DECIMAL(10,0)",
                [Some("42"), Some("-42"), None],
            ),
            reads("H_DECIMAL_SPACED", VARCHAR_JSON, DECIMAL_38_10_VALUES_TEXT),
            reads("H_DATE", "DATE", DATE_VALUES_TEXT),
            reads("H_TIMESTAMP", timestamp, TIMESTAMP_VALUES_TEXT),
            refuses("H_BINARY", VARCHAR_JSON, &["type 'binary'", "#351"]),
            reads("H_ARRAY_INT", VARCHAR_JSON, INT_LIST_VALUES_TEXT),
            reads(
                "H_ARRAY_STRUCT",
                VARCHAR_JSON,
                [Some(r#"[{"a":1.25}]"#), Some("[]"), None],
            ),
            reads(
                "H_MAP_STRING",
                VARCHAR_JSON,
                [Some(r#"{"k1":1,"k2":2}"#), Some("{}"), None],
            ),
            reads(
                "H_MAP_VARCHAR",
                VARCHAR_JSON,
                [Some(r#"{"a":1}"#), Some("{}"), None],
            ),
            reads(
                "H_STRUCT_XY",
                VARCHAR_JSON,
                [
                    Some(r#"{"x":1,"y":"p"}"#),
                    Some(r#"{"x":2,"y":null}"#),
                    None,
                ],
            ),
            refuses(
                "H_STRUCT_BINARY",
                VARCHAR_JSON,
                &["member 'h_struct_binary.b'", "type 'binary'", "#351"],
            ),
            refuses(
                "H_UNIONTYPE",
                VARCHAR_JSON,
                &["Hive type 'uniontype<int,string>'"],
            ),
            refuses(
                "H_INTERVAL",
                VARCHAR_JSON,
                &["Hive type 'interval_day_time'"],
            ),
            refuses("H_MALFORMED_MAP", VARCHAR_JSON, &["Hive type 'map<int>'"]),
            refuses("H_EMPTY_TYPE", VARCHAR_JSON, &["Hive type ''"]),
        ],
    );
    assert_binary_values_refused(conn, VS, BINARY_VALUES, "Invalid UTF8 sequence");
}

/// Scenario: An assume-role CONNECTION reads through the role and its base identity alone is denied
fn check_assume_role_connection_reads_through_the_role(run: &GlueRun, conn: &mut ExaConn) {
    let env = run.env();
    let database = run.database();
    assert_glue_vs_created(
        env,
        ROLE_VS,
        ROLE_CONN,
        &database,
        &glue_assume_role_connection_password(env),
    );

    let orders = conn.query_columns(&format!(
        "SELECT ORDER_ID FROM {ROLE_VS}.{} ORDER BY ORDER_ID",
        ICEBERG_ORDERS.to_uppercase()
    ));
    assert_eq!(
        int_column(&orders[0]),
        ORDERS.iter().map(|o| o.order_id).collect::<Vec<_>>(),
        "the Iceberg table reads through the role's session"
    );
    let partitioned = conn.query_columns(&format!(
        "SELECT ID FROM {ROLE_VS}.{} WHERE P_INT < 9 ORDER BY ID LIMIT 3",
        PARTITIONED.to_uppercase()
    ));
    assert_eq!(
        int_column(&partitioned[0]),
        [1, 2, 3],
        "the partitioned Parquet table reads through the role's sealed session"
    );

    let response = try_create_glue_vs(
        env,
        BASE_VS,
        BASE_CONN,
        &database,
        &glue_base_identity_connection_password(env),
    );
    let error = value_to_string(&response["exception"]["text"]);
    let mut cleanup = redacting_conn();
    for (vs, connection) in [(ROLE_VS, ROLE_CONN), (BASE_VS, BASE_CONN)] {
        let _ = cleanup.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
        let _ = cleanup.try_execute(&format!("DROP CONNECTION IF EXISTS {connection}"));
    }
    assert_ne!(
        response["status"].as_str(),
        Some("ok"),
        "Glue must deny the base identity without its role"
    );
    assert!(
        error.contains("AccessDenied"),
        "the denial must come from Glue: {}",
        env.redact(&error)
    );
    assert_eq!(
        env.redact(&error),
        error,
        "the denial must contain no credential value"
    );
}

/// Scenario: The suite fails, never skips, when a variable or the stack is missing
#[test]
fn glue_suite_fails_when_stack_unavailable() {
    let message = panic_message(|| {
        ExaConn::connect("127.0.0.1", 1, "sys", SYS_PASSWORD);
    });
    assert!(
        message.contains("127.0.0.1:1"),
        "the failure must name the unavailable stack: {message}"
    );
}

/// Scenario: No credential value appears in output
#[test]
fn glue_credentials_never_appear_in_output() {
    const SENTINEL_ACCESS_KEY_ID: &str = "AKIAGLUEREDACTIONPROBE";
    const SENTINEL_SECRET: &str = "GLUE/REDACTION+PROBE/SECRET/SENTINEL/0123";
    const PROBE_CONN: &str = "GLUE_REDACTION_PROBE";
    const PROBE_VS: &str = "GLUE_REDACTION_PROBE";

    let sentinel_env = GlueEnv::from_lookup(|name| match name {
        ACCESS_KEY_ID_VAR => Some(SENTINEL_ACCESS_KEY_ID.to_string()),
        SECRET_ACCESS_KEY_VAR => Some(SENTINEL_SECRET.to_string()),
        other => std::env::var(other).ok(),
    });
    setup();
    let mut conn = redacting_conn();
    let connection_sql = build_create_connection_sql(
        PROBE_CONN,
        &sentinel_env.glue_endpoint(),
        &glue_connection_password(&sentinel_env),
    );

    let message = panic_message(AssertUnwindSafe(|| {
        conn.execute(&format!(
            "{connection_sql} THIS_TRAILING_TOKEN_MAKES_THE_STATEMENT_INVALID"
        ));
    }));
    assert!(
        message.contains("Exasol execute failed"),
        "the failure must still be reported: {message}"
    );
    assert!(
        !message.contains(SENTINEL_SECRET),
        "a failed CONNECTION DDL must not echo the secret access key: {message}"
    );

    let response = try_create_glue_vs(
        &sentinel_env,
        PROBE_VS,
        PROBE_CONN,
        "lh_e2e_redaction_probe",
        &glue_connection_password(&sentinel_env),
    );
    conn.execute(&format!("DROP CONNECTION IF EXISTS {PROBE_CONN}"));
    assert_ne!(
        response["status"].as_str(),
        Some("ok"),
        "a virtual schema over sentinel credentials must fail"
    );
    let error = value_to_string(&response["exception"]["text"]);
    assert!(
        error.contains("Glue"),
        "the adapter must report the rejected Glue call: {}",
        sentinel_env.redact(&error)
    );
    assert!(
        !error.contains(SENTINEL_SECRET),
        "an adapter failure must not contain the secret access key: {}",
        sentinel_env.redact(&error)
    );
}
