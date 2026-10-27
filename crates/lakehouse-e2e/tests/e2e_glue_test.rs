//! `CATALOG_KIND = 'GLUE'` against real AWS Glue and S3 with a local Exasol. Run with
//! `--test-threads=1`: the tests share one Exasol provisioning and one virtual schema name.

mod common;

use common::cloud_fixture::panic_message;
use common::e2e_harness::{
    SYS_PASSWORD, VARCHAR_JSON, VsProps, assert_binary_values_refused,
    assert_decimal_orders_numerically, assert_notes_well_formed, assert_text_columns,
    assert_type_matrix, column_adapter_notes, create_schema_and_scripts, declared_types,
    error_text, exa_conn, explain_virtual_sql, identifier_note, install_slc, int_column, note_of,
    pairs, parse_int, partition_positions, query_error, reads, refuses, runtime, skipped_entries,
    try_create_virtual_schema_with_password, upload_so, value_to_string,
};
use common::exasol_ws::ExaConn;
use common::glue::{
    ACCESS_KEY_ID_VAR, ALL_TYPES, BINARY_VALUES, BROKEN_ICEBERG, FixturePartition, GlueEnv,
    GlueRun, ICEBERG_ORDERS, ORC_INPUT_FORMAT, ORC_TABLE, ORDERS, PARTITIONED, PARTITIONS,
    PartitionedRow, ROUTED_TABLES, SECRET_ACCESS_KEY_VAR, SKIPPED_TABLES, STALE_GLUE_COLUMN,
    SUCCESS_MARKER, glue_assume_role_connection_password, glue_base_identity_connection_password,
    glue_connection_password, glue_iceberg_rest_connection_password, metadata_only_keys,
    page_padding_views, partitioned_rows, register_broken_iceberg_table, register_fixture_set,
    register_probe_table,
};
use common::seed::{
    ALL_TYPES_IDS_TEXT, BOOLEAN_VALUES_TEXT, DATE_VALUES_TEXT, DECIMAL_10_2_VALUES_TEXT,
    DECIMAL_18_0_VALUES_TEXT, DECIMAL_36_6_VALUES_TEXT, DECIMAL_38_10_VALUES_TEXT,
    FLOAT32_VALUES_TEXT, INT_LIST_VALUES_TEXT, INT8_VALUES_TEXT, INT16_VALUES_TEXT,
    INT32_VALUES_TEXT, TEXT_VALUES_TEXT, TIMESTAMP_VALUES_TEXT,
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
const REST_VS: &str = "GLUE_LAKEHOUSE_REST";
const REST_CONN: &str = "GLUE_E2E_REST_CREDS";

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

/// specs/testing.md § Per-run cloud resources
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

type FixtureCheck = fn(&GlueRun, &mut ExaConn);

/// One provisioning serves every check. Each check but the last only reads the fixture, and the
/// last breaks every later full REFRESH of it, so it runs after them. Each check runs even after
/// another failed, and the panic hook has already printed each failure.
#[test]
fn glue_checks_pass_against_one_provisioned_fixture() {
    let run = provision();
    let checks: [(&str, FixtureCheck); 11] = [
        (
            "listing",
            check_listing_includes_routed_tables_and_records_every_skip,
        ),
        ("column notes", check_glue_columns_persist_source_notes),
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
        (
            "Iceberg REST paging",
            check_iceberg_rest_listing_reaches_past_the_first_page,
        ),
        (
            "refresh tables",
            check_refresh_tables_redeclares_only_the_named_tables,
        ),
        (
            "broken table",
            check_a_broken_unrequested_table_fails_only_a_full_refresh,
        ),
    ];
    let failed: Vec<&str> = checks
        .iter()
        .filter(|(_, check)| {
            std::panic::catch_unwind(AssertUnwindSafe(|| check(&run, &mut exa_conn()))).is_err()
        })
        .map(|(name, _)| *name)
        .collect();
    assert!(failed.is_empty(), "failed checks: {failed:?}");
}

/// Scenario: The client routes a table by its declared table type before its storage descriptor
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
    for table in ROUTED_TABLES {
        assert_eq!(
            identifier_note(conn, VS, &table.to_uppercase()),
            Some(format!("{}.{table}", run.database())),
            "{table}'s note must name its Glue identifier"
        );
    }

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
    let database = run.database();
    assert_records_every_skip(conn, &database);

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

/// Scenario: A scoped refresh lists Glue table names and loads only the requested tables
fn check_refresh_tables_redeclares_only_the_named_tables(run: &GlueRun, conn: &mut ExaConn) {
    let routed: Vec<String> = ROUTED_TABLES.iter().map(|t| t.to_uppercase()).collect();
    let database = run.database();
    let named = ICEBERG_ORDERS.to_uppercase();
    let others: Vec<String> = ROUTED_TABLES
        .iter()
        .filter(|table| **table != ICEBERG_ORDERS)
        .map(|table| table.to_uppercase())
        .collect();
    let named_notes = column_adapter_notes(conn, VS, &named);
    let other_declarations: Vec<_> = others
        .iter()
        .map(|table| declared_types(conn, VS, table))
        .collect();
    let other_answers = count_statuses(conn, &others);
    assert!(
        other_answers.iter().any(|(_, status)| status == "ok"),
        "the unrequested routed tables must include a queryable one: {other_answers:?}"
    );
    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {VS} REFRESH TABLES {named} {}",
        ORC_TABLE.to_uppercase()
    ));
    assert_eq!(
        column_adapter_notes(conn, VS, &named),
        named_notes,
        "REFRESH TABLES {named} re-declares {named} from the unchanged source"
    );
    assert_eq!(
        identifier_note(conn, VS, &named),
        Some(format!("{database}.{ICEBERG_ORDERS}")),
        "{named} keeps its identifier note"
    );
    for (table, declaration) in others.iter().zip(other_declarations) {
        assert_eq!(
            declared_types(conn, VS, table),
            declaration,
            "the unrequested {table} keeps its declaration"
        );
    }
    assert_eq!(
        count_statuses(conn, &others),
        other_answers,
        "every unrequested table answers a query as it did before REFRESH TABLES"
    );
    let listed: Vec<String> = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = '{VS}' \
         ORDER BY TABLE_NAME"
    ))[0]
        .iter()
        .map(value_to_string)
        .collect();
    assert_eq!(
        listed, routed,
        "the requested, skipped {ORC_TABLE} stays undeclared"
    );
    assert_records_every_skip(conn, &database);
}

/// Each table's `SELECT COUNT(*)` outcome, so a check compares queryability before and after.
fn count_statuses(conn: &mut ExaConn, tables: &[String]) -> Vec<(String, String)> {
    tables
        .iter()
        .map(|table| {
            let response = conn.try_execute(&format!("SELECT COUNT(*) FROM {VS}.{table}"));
            (table.clone(), value_to_string(&response["status"]))
        })
        .collect()
}

/// `SKIPPED_TABLES` records exactly the fixture's skips, each stating its reason.
fn assert_records_every_skip(conn: &mut ExaConn, database: &str) {
    let skipped = skipped_entries(conn, VS);
    let recorded: BTreeSet<&str> = skipped.iter().map(|(table, _)| table.as_str()).collect();
    let padding = page_padding_views();
    let expected_skips: Vec<(&str, &str)> = SKIPPED_TABLES
        .into_iter()
        .chain(
            padding
                .iter()
                .map(|name| (name.as_str(), "TableType=VIRTUAL_VIEW")),
        )
        .collect();
    let expected: Vec<String> = expected_skips
        .iter()
        .map(|(name, _)| format!("{database}.{name}"))
        .collect();
    assert_eq!(
        recorded,
        expected.iter().map(String::as_str).collect(),
        "SKIPPED_TABLES records every skip: {skipped:?}"
    );
    for (name, reason) in expected_skips {
        let table = format!("{database}.{name}");
        let (_, recorded_reason) = skipped
            .iter()
            .find(|(skipped_table, _)| *skipped_table == table)
            .expect("checked above");
        assert!(
            recorded_reason.contains(reason),
            "the skip of {name} must state {reason:?}: {recorded_reason}"
        );
    }
}

/// Scenario: A partial refresh loads the metadata of the requested tables only
/// Scenario: A scoped refresh lists Glue table names and loads only the requested tables
fn check_a_broken_unrequested_table_fails_only_a_full_refresh(run: &GlueRun, conn: &mut ExaConn) {
    let env = run.env();
    env.expect(
        runtime().block_on(register_broken_iceberg_table(run)),
        &format!("register {BROKEN_ICEBERG} in {}", run.database()),
    );
    let error = env.redact(&query_error(
        conn,
        &format!("ALTER VIRTUAL SCHEMA {VS} REFRESH"),
    ));
    let broken = format!("{}.{BROKEN_ICEBERG}", run.database());
    assert!(
        error.contains(&broken),
        "a full REFRESH must fail naming {broken}: {error}"
    );
    let partitioned = PARTITIONED.to_uppercase();
    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {VS} REFRESH TABLES {partitioned}"
    ));
    // The predicate prunes the fixture's ORC partition, which fails any unpruned scan by design.
    let ids = conn.query_columns(&format!(
        "SELECT ID FROM {VS}.{partitioned} WHERE P_STR = 'a b/c' ORDER BY ID"
    ));
    assert_eq!(int_column(&ids[0]), [5]);
}

/// Scenario: Every declared column carries its source shape in its adapterNotes
/// Scenario: A Glue Parquet table is planned by the shared catalog-declared Parquet reader
/// Scenario: A Glue Iceberg table is planned from its metadata location by the one Iceberg planner
fn check_glue_columns_persist_source_notes(run: &GlueRun, conn: &mut ExaConn) {
    let notes = column_adapter_notes(conn, VS, &ICEBERG_ORDERS.to_uppercase());
    let declared: Vec<(&str, &Value)> = notes
        .iter()
        .map(|(column, note)| (column.as_str(), &note["sourceType"]))
        .collect();
    assert_eq!(
        declared,
        [
            (
                "AMOUNT",
                &serde_json::json!({"field_id": 3, "name": "amount", "arrow_type": "decimal128(10,2)", "nullable": true})
            ),
            (
                "CUSTOMER",
                &serde_json::json!({"field_id": 2, "name": "customer", "arrow_type": "utf8", "nullable": true})
            ),
            (
                "ORDER_DATE",
                &serde_json::json!({"field_id": 4, "name": "order_date", "arrow_type": "date32", "nullable": true})
            ),
            (
                "ORDER_ID",
                &serde_json::json!({"field_id": 1, "name": "order_id", "arrow_type": "int64", "nullable": false})
            ),
        ],
        "each note is the metadata file's field, never Glue's {STALE_GLUE_COLUMN} copy"
    );
    let parquet = column_adapter_notes(conn, VS, &PARTITIONED.to_uppercase());
    assert_eq!(
        partition_positions(&parquet),
        [
            ("ID", None),
            ("P_DATE", Some(1)),
            ("P_INT", Some(0)),
            ("P_STR", Some(2)),
            ("V", None)
        ],
        "a Parquet table's notes record its partition keys in Glue's key order"
    );
    assert!(
        parquet
            .iter()
            .all(|(_, note)| note["sourceType"]["nullable"] == true
                && note["sourceType"]["field_id"].is_null()),
        "every Parquet column is declared nullable and unbound: {parquet:?}"
    );
    let all_types = column_adapter_notes(conn, VS, &ALL_TYPES.to_uppercase());
    let binary = note_of(&all_types, "H_BINARY");
    assert!(
        binary["refused"]
            .as_str()
            .is_some_and(|reason| reason.contains("#351")),
        "a binary Hive column's refusal is recorded at CREATE: {binary}"
    );

    for notes in [&notes, &parquet] {
        assert_notes_well_formed(notes, run.env().secret_access_key());
    }
}

/// Scenario: Each kept partition's location is listed and its files carry the partition's Glue values
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

/// Scenario: A kept partition the reader cannot read faithfully fails the query naming it
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

/// Scenario: The listing declares each Glue column through the Spark listing mapping
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
            reads("H_DECIMAL_18_0", "DECIMAL(18,0)", DECIMAL_18_0_VALUES_TEXT),
            reads("H_DECIMAL_36_6", "DECIMAL(36,6)", DECIMAL_36_6_VALUES_TEXT),
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
    assert_decimal_orders_numerically(conn, VS, ALL_TYPES, "H_DECIMAL_18_0");
    assert_binary_values_refused(conn, VS, BINARY_VALUES, "Invalid bytestream");
}

/// Scenario: Session credentials sign every native Glue request
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
    let mut cleanup = redacting_conn();
    for (vs, connection) in [(ROLE_VS, ROLE_CONN), (BASE_VS, BASE_CONN)] {
        let _ = cleanup.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {vs} CASCADE"));
        let _ = cleanup.try_execute(&format!("DROP CONNECTION IF EXISTS {connection}"));
    }
    let error = error_text(&response);
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

/// Scenario: Create virtual schema enumerates every table in the configured namespace
fn check_iceberg_rest_listing_reaches_past_the_first_page(run: &GlueRun, conn: &mut ExaConn) {
    let env = run.env();
    let catalog_id = env.expect(runtime().block_on(run.catalog_id()), "read the catalog id");
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let response = try_create_virtual_schema_with_password(
            &mut redacting_conn(),
            &VsProps::new(REST_VS, &run.database()).with_catalog_conn_name(REST_CONN),
            &env.iceberg_rest_endpoint(),
            &glue_iceberg_rest_connection_password(env, &catalog_id),
        );
        assert_eq!(
            response["status"].as_str(),
            Some("ok"),
            "CREATE VIRTUAL SCHEMA {REST_VS} failed: {}",
            env.redact(&response["exception"].to_string())
        );
        let listed: Vec<String> = conn.query_columns(&format!(
            "SELECT TABLE_NAME FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = '{REST_VS}'"
        ))[0]
            .iter()
            .map(value_to_string)
            .collect();
        assert_eq!(
            listed,
            [ICEBERG_ORDERS.to_uppercase()],
            "Glue answers the first page of {} views with no identifier and a next-page-token",
            page_padding_views().len()
        );
    }));
    let mut cleanup = redacting_conn();
    let _ = cleanup.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {REST_VS} CASCADE"));
    let _ = cleanup.try_execute(&format!("DROP CONNECTION IF EXISTS {REST_CONN}"));
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// specs/testing.md § Failure contract
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

/// specs/testing.md § Credentials in tests
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
    let error = error_text(&response);
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
