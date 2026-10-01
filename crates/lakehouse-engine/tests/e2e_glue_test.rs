//! E2E tests of `CATALOG_KIND = 'GLUE'` against a real AWS Glue Data Catalog and S3 bucket, with
//! a local Exasol. Each test provisions its own `GlueRun`, whose `Drop` removes the run's
//! database and prefix when the test returns or panics. Tests run with `--test-threads=1`
//! because they share one Exasol provisioning and one virtual schema name.
#![cfg(feature = "glue-e2e")]

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCHEMA_NAME, SYS_PASSWORD, VARCHAR_JSON, assert_query_fails,
    assert_text_columns, create_schema_and_scripts, declared_types, exa_conn, explain_virtual_sql,
    install_slc, pairs, parse_int, query_error, upload_so, value_to_string,
};
use common::exasol_ws::ExaConn;
use common::glue::{
    A_VIEW, ACCESS_KEY_ID_VAR, ALL_TYPES, BINARY_VALUES, DELTA_TABLE, GlueEnv, GlueRun,
    HIVE_DEFAULT_PARTITION, HIVE_TYPE_COLUMNS, ICEBERG_ORDERS, ORC_INPUT_FORMAT, ORC_TABLE, ORDERS,
    PARTITION_KEYS, PARTITIONED, PARTITIONED_ROWS, PARTITIONS, PROJECTED, ROUTED_TABLES,
    SECRET_ACCESS_KEY_VAR, SKIPPED_TABLES, STALE_GLUE_COLUMN, SUCCESS_MARKER,
    glue_connection_password, metadata_only_keys, register_fixture_set, register_probe_table,
};
use common::stack::{build_create_connection_sql, panic_payload_message, wait_for_exasol};
use common::timestamp_precision::expected_timestamp_precision;

use futures::FutureExt;
use serde_json::Value;

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::{Mutex, OnceLock};

const VS: &str = "GLUE_LAKEHOUSE";
const CONN: &str = "GLUE_E2E_CREDS";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

/// MinIO and the Iceberg REST fixture are deliberately not awaited: this suite's storage and
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

/// Hold as a test-function local: `run`'s `Drop` removes the Glue database and the S3 prefix
/// on return or panic, which a static would never do.
struct GlueFixture {
    run: GlueRun,
}

impl GlueFixture {
    /// Takes the environment its caller read before creating anything, so a missing variable
    /// leaves no resource.
    fn register(env: GlueEnv) -> Self {
        let rt = runtime();
        let run = rt
            .block_on(GlueRun::create(&env))
            .unwrap_or_else(|e| panic!("{}", env.redact(&format!("create the Glue run: {e:#}"))));
        rt.block_on(register_fixture_set(&run)).unwrap_or_else(|e| {
            panic!(
                "{}",
                env.redact(&format!(
                    "register the fixture set in {}: {e:#}",
                    run.database()
                ))
            )
        });
        Self { run }
    }

    /// The local Exasol is prepared first, so an unavailable stack creates no cloud resource.
    fn provision() -> Self {
        let env = GlueEnv::from_environment();
        setup();
        let fixture = Self::register(env);
        create_glue_virtual_schema(fixture.run.env(), &fixture.run.database());
        fixture
    }

    fn table(&self, name: &str) -> String {
        format!("{VS}.{}", name.to_uppercase())
    }
}

/// The Exasol error for credential-bearing DDL may echo the statement, so only its redacted
/// text reaches the panic.
fn execute_redacted(conn: &mut ExaConn, env: &GlueEnv, label: &str, sql: &str) {
    let response = conn.try_execute(sql);
    if response["status"].as_str() != Some("ok") {
        panic!(
            "{}",
            env.redact(&format!(
                "{label} failed: {} (sqlCode {})",
                response["exception"]["text"].as_str().unwrap_or(""),
                response["exception"]["sqlCode"].as_str().unwrap_or("")
            ))
        );
    }
}

fn create_glue_virtual_schema(env: &GlueEnv, namespace: &str) {
    let mut conn = exa_conn();
    execute_redacted(
        &mut conn,
        env,
        "CREATE CONNECTION",
        &build_create_connection_sql(CONN, &env.glue_endpoint(), &glue_connection_password(env)),
    );
    conn.execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS} CASCADE"));
    execute_redacted(
        &mut conn,
        env,
        "CREATE VIRTUAL SCHEMA",
        &format!(
            "CREATE VIRTUAL SCHEMA {VS} USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH \
             CATALOG_CONNECTION = '{CONN}' CATALOG_KIND = 'GLUE' NAMESPACE = '{namespace}'"
        ),
    );
}

fn int_column(column: &[Value]) -> Vec<i64> {
    column.iter().map(parse_int).collect()
}

fn text_column(column: &[Value]) -> Vec<Option<String>> {
    column
        .iter()
        .map(|value| (!value.is_null()).then(|| value_to_string(value)))
        .collect()
}

fn partition_files(p_int: &str) -> Vec<&'static str> {
    PARTITIONS
        .iter()
        .filter(|partition| partition.values[0] == p_int)
        .flat_map(|partition| partition.files.iter().map(|file| file.name))
        .collect()
}

fn every_partition_file() -> Vec<&'static str> {
    PARTITIONS
        .iter()
        .flat_map(|partition| partition.files.iter().map(|file| file.name))
        .collect()
}

fn assert_scan_names_only(pushed: &str, kept: &[&str], context: &str) {
    for file in every_partition_file() {
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
    let assert_removed = |database: &str, prefix: &str, how: &str| {
        let exists = rt
            .block_on(env.database_exists(database))
            .unwrap_or_else(|e| panic!("{}", env.redact(&format!("check {database}: {e:#}"))));
        assert!(!exists, "{how}: Glue database {database} must be deleted");
        let left = rt
            .block_on(env.object_keys(prefix))
            .unwrap_or_else(|e| panic!("{}", env.redact(&format!("list {prefix}: {e:#}"))));
        assert!(
            left.is_empty(),
            "{how}: every object under {prefix} must be deleted, found {left:?}"
        );
    };

    let (database, prefix) = {
        let run = rt
            .block_on(GlueRun::create(&env))
            .unwrap_or_else(|e| panic!("{}", env.redact(&format!("create a run: {e:#}"))));
        rt.block_on(register_probe_table(&run))
            .unwrap_or_else(|e| panic!("{}", env.redact(&format!("register the probe: {e:#}"))));
        assert!(
            rt.block_on(env.database_exists(&run.database()))
                .unwrap_or_else(|e| panic!(
                    "{}",
                    env.redact(&format!("check {}: {e:#}", run.database()))
                )),
            "the run's database {} must exist while the run is in scope",
            run.database()
        );
        assert!(
            !rt.block_on(env.object_keys(&run.object_prefix()))
                .unwrap_or_else(|e| panic!(
                    "{}",
                    env.redact(&format!("list {}: {e:#}", run.object_prefix()))
                ))
                .is_empty(),
            "the probe must have written an object under {}",
            run.object_prefix()
        );
        (run.database(), run.object_prefix())
    };
    assert_removed(&database, &prefix, "a scope that ends normally");

    let created = Mutex::new(None);
    let outcome = rt.block_on(
        AssertUnwindSafe(async {
            let run = GlueRun::create(&env)
                .await
                .unwrap_or_else(|e| panic!("{}", env.redact(&format!("create a run: {e:#}"))));
            register_probe_table(&run).await.unwrap_or_else(|e| {
                panic!("{}", env.redact(&format!("register the probe: {e:#}")))
            });
            *created.lock().expect("the name slot is never poisoned") =
                Some((run.database(), run.object_prefix()));
            panic!("deliberate panic to exercise the run's Drop while unwinding inside block_on");
        })
        .catch_unwind(),
    );
    assert!(
        outcome.is_err(),
        "the inner async block must have panicked, which is what this test exercises"
    );
    let (database, prefix) = created
        .into_inner()
        .expect("the name slot is never poisoned")
        .expect("the run was created before the deliberate panic");
    assert_removed(&database, &prefix, "a scope that ends in a panic");
}

#[derive(Debug)]
struct RegisteredPartition {
    values: Vec<String>,
    location: String,
    input_format: String,
}

/// Scenario: The fixture set covers every routing, partition, and type case
#[test]
fn glue_fixture_set_registers_every_case() {
    let fixture = GlueFixture::register(GlueEnv::from_environment());
    let (env, run) = (fixture.run.env(), &fixture.run);
    let rt = runtime();
    let client = env.glue_client();
    let database = run.database();
    let tables = rt
        .block_on(client.get_tables().database_name(&database).send())
        .unwrap_or_else(|e| panic!("{}", env.glue_failure(&format!("GetTables {database}"), &e)));
    let table = |name: &str| {
        tables
            .table_list()
            .iter()
            .find(|table| table.name() == name)
            .unwrap_or_else(|| panic!("the fixture set must register table {name}"))
    };
    let parameter = |name: &str, key: &str| {
        table(name)
            .parameters()
            .and_then(|parameters| parameters.get(key))
            .cloned()
    };
    let input_format = |name: &str| {
        table(name)
            .storage_descriptor()
            .and_then(|descriptor| descriptor.input_format())
            .map(str::to_string)
    };

    let mut names: Vec<&str> = tables.table_list().iter().map(|t| t.name()).collect();
    names.sort_unstable();
    let mut expected: Vec<&str> = ROUTED_TABLES
        .iter()
        .copied()
        .chain(SKIPPED_TABLES.iter().map(|(name, _)| *name))
        .collect();
    expected.sort_unstable();
    assert_eq!(
        names, expected,
        "the run's database holds exactly the fixture set"
    );

    assert_eq!(
        parameter(ICEBERG_ORDERS, "table_type").as_deref(),
        Some("ICEBERG")
    );
    let metadata_location = parameter(ICEBERG_ORDERS, "metadata_location")
        .expect("iceberg_orders must carry its metadata_location");
    let metadata_key = metadata_location
        .strip_prefix(&format!("s3://{}/", env.fixture_bucket))
        .expect("the metadata file lives in the fixture bucket");
    let objects = rt
        .block_on(env.object_keys(&run.object_prefix()))
        .unwrap_or_else(|e| panic!("{}", env.redact(&format!("list the run prefix: {e:#}"))));
    assert!(
        metadata_key.ends_with(".metadata.json") && objects.iter().any(|key| key == metadata_key),
        "metadata_location {metadata_location} must name a metadata file iceberg-rust wrote"
    );

    let declared = |name: &str| -> Vec<(String, String)> {
        table(name)
            .storage_descriptor()
            .map(|descriptor| descriptor.columns())
            .unwrap_or_default()
            .iter()
            .map(|c| (c.name().to_string(), c.r#type().unwrap_or("").to_string()))
            .collect()
    };
    let hive_types: Vec<(String, String)> = std::iter::once(("id", "bigint"))
        .chain(HIVE_TYPE_COLUMNS.iter().map(|c| (c.column, c.hive_type)))
        .map(|(name, hive_type)| (name.to_string(), hive_type.to_string()))
        .collect();
    assert_eq!(
        declared(ALL_TYPES),
        hive_types,
        "all_types must declare every Hive type of vs-adapter/glue-hive-type-mapping"
    );
    assert_eq!(
        declared(BINARY_VALUES),
        pairs(&[("id", "bigint"), ("c_bytes", "string")])
    );
    for name in [ALL_TYPES, BINARY_VALUES] {
        let data_files: Vec<&String> = objects
            .iter()
            .filter(|key| key.starts_with(&format!("{}{name}/", run.object_prefix())))
            .filter(|key| !key.ends_with(SUCCESS_MARKER))
            .collect();
        assert!(
            !data_files.is_empty()
                && data_files
                    .iter()
                    .all(|key| !key.rsplit('/').next().unwrap_or("").contains('.')),
            "{name}'s data files must carry no file extension: {data_files:?}"
        );
    }

    let partition_keys: Vec<(String, String)> = table(PARTITIONED)
        .partition_keys()
        .iter()
        .map(|c| (c.name().to_string(), c.r#type().unwrap_or("").to_string()))
        .collect();
    assert_eq!(partition_keys, pairs(&PARTITION_KEYS));
    let partitions = rt
        .block_on(
            client
                .get_partitions()
                .database_name(&database)
                .table_name(PARTITIONED)
                .send(),
        )
        .unwrap_or_else(|e| {
            panic!(
                "{}",
                env.glue_failure(&format!("GetPartitions {database}.{PARTITIONED}"), &e)
            )
        });
    let registered: Vec<RegisteredPartition> = partitions
        .partitions()
        .iter()
        .map(|partition| {
            let descriptor = partition.storage_descriptor();
            RegisteredPartition {
                values: partition.values().to_vec(),
                location: descriptor
                    .and_then(|d| d.location())
                    .unwrap_or("")
                    .to_string(),
                input_format: descriptor
                    .and_then(|d| d.input_format())
                    .unwrap_or("")
                    .to_string(),
            }
        })
        .collect();
    assert_eq!(registered.len(), PARTITIONS.len(), "{registered:?}");
    let table_location = run.uri(&format!("{PARTITIONED}/"));
    let has = |check: &dyn Fn(&RegisteredPartition) -> bool, case: &str| {
        assert!(
            registered.iter().any(check),
            "the partitioned table must register {case}: {registered:?}"
        );
    };
    has(
        &|p| p.values[2] == HIVE_DEFAULT_PARTITION,
        "a NULL partition",
    );
    has(
        &|p| p.values[2] == "a b/c" && p.location.contains("p_str=a b%2Fc/"),
        "the value a b/c at its raw key",
    );
    has(
        &|p| p.location.starts_with("s3://") && !p.location.starts_with(&table_location),
        "a partition outside the table location",
    );
    has(&|p| p.location.starts_with("s3a://"), "an s3a:// partition");
    has(
        &|p| p.values[0] == "9" && p.input_format == ORC_INPUT_FORMAT,
        "the p_int=9 ORC partition",
    );
    assert!(
        registered
            .iter()
            .filter(|p| p.values[0] != "9")
            .all(|p| p.values[0].parse::<i32>().is_ok_and(|p_int| p_int < 9)),
        "every partition but the ORC one must have p_int < 9: {registered:?}"
    );

    assert_eq!(
        parameter(PROJECTED, "projection.enabled").as_deref(),
        Some("true")
    );
    assert_eq!(table(A_VIEW).table_type(), Some("VIRTUAL_VIEW"));
    assert_eq!(input_format(ORC_TABLE).as_deref(), Some(ORC_INPUT_FORMAT));
    assert_eq!(
        parameter(DELTA_TABLE, "table_type").as_deref(),
        Some("DELTA")
    );
    for key in metadata_only_keys() {
        let prefix = format!("{}{key}", run.object_prefix());
        assert!(
            !objects.iter().any(|object| object.starts_with(&prefix)),
            "no data file may exist under the metadata-only location {prefix}"
        );
    }
}

/// Scenario: The listing includes the routed tables and records every skip
#[test]
fn glue_listing_includes_routed_tables_and_records_every_skip() {
    let fixture = GlueFixture::provision();
    let mut conn = exa_conn();

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
        declared_types(&mut conn, VS, ICEBERG_ORDERS),
        pairs(&[
            ("ORDER_ID", "DECIMAL(20,0)"),
            ("CUSTOMER", VARCHAR_JSON),
            ("AMOUNT", "DECIMAL(10,2)"),
            ("ORDER_DATE", "DATE"),
        ]),
        "the Iceberg columns come from metadata.json, never Glue's {STALE_GLUE_COLUMN} copy"
    );
    assert_eq!(
        declared_types(&mut conn, VS, PARTITIONED),
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
    let database = fixture.run.database();
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
}

/// Scenario: Queries through pushdown return the expected rows
#[test]
fn glue_queries_return_expected_rows_through_pushdown() {
    let fixture = GlueFixture::provision();
    let mut conn = exa_conn();
    let orders = fixture.table(ICEBERG_ORDERS);

    let full = conn.query_columns(&format!(
        "SELECT ORDER_ID, CUSTOMER, AMOUNT, ORDER_DATE FROM {orders} ORDER BY ORDER_ID"
    ));
    assert_eq!(
        int_column(&full[0]),
        ORDERS.iter().map(|o| o.order_id).collect::<Vec<_>>()
    );
    assert_eq!(
        text_column(&full[1]),
        ORDERS
            .iter()
            .map(|o| o.customer.map(str::to_string))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        text_column(&full[2]),
        ORDERS.iter().map(|o| Some(o.amount())).collect::<Vec<_>>()
    );
    assert_eq!(
        text_column(&full[3]),
        ORDERS
            .iter()
            .map(|o| Some(o.order_date.to_string()))
            .collect::<Vec<_>>()
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

    let partitioned = fixture.table(PARTITIONED);
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
#[test]
fn glue_partition_cases_return_their_glue_values() {
    let fixture = GlueFixture::provision();
    let mut conn = exa_conn();
    let partitioned = fixture.table(PARTITIONED);

    let rows = conn.query_columns(&format!(
        "SELECT ID, V, P_INT, P_DATE, P_STR FROM {partitioned} WHERE P_INT < 9 ORDER BY ID"
    ));
    assert_eq!(
        int_column(&rows[0]),
        PARTITIONED_ROWS.iter().map(|r| r.id).collect::<Vec<_>>()
    );
    assert_eq!(
        text_column(&rows[1]),
        PARTITIONED_ROWS
            .iter()
            .map(|r| Some(r.v.to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        int_column(&rows[2]),
        PARTITIONED_ROWS
            .iter()
            .map(|r| i64::from(r.p_int))
            .collect::<Vec<_>>(),
        "P_INT comes from each partition's Glue values"
    );
    assert_eq!(
        text_column(&rows[3]),
        PARTITIONED_ROWS
            .iter()
            .map(|r| Some(r.p_date.to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        text_column(&rows[4]),
        PARTITIONED_ROWS
            .iter()
            .map(|r| r.p_str.map(str::to_string))
            .collect::<Vec<_>>(),
        "the default partition reads NULL, a b/c reads decoded, and the out-of-root and s3a:// \
         partitions read their Glue values, never a value parsed from a path"
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
#[test]
fn glue_orc_partition_fails_loud() {
    let fixture = GlueFixture::provision();
    let mut conn = exa_conn();
    let orc = PARTITIONS
        .iter()
        .find(|partition| partition.input_format == ORC_INPUT_FORMAT)
        .expect("the fixture set has an ORC partition");
    let orc_location = orc.place.location(&fixture.run);

    let error = query_error(
        &mut conn,
        &format!("SELECT COUNT(*) FROM {}", fixture.table(PARTITIONED)),
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
        !error.contains(fixture.run.env().secret_access_key()),
        "the failure must not contain the secret access key"
    );
}

/// Scenario: A partition predicate prunes partitions before their locations are listed
#[test]
fn glue_partition_predicate_reduces_the_scan_file_list() {
    let fixture = GlueFixture::provision();
    let mut conn = exa_conn();
    let partitioned = fixture.table(PARTITIONED);
    let every_readable: Vec<&str> = every_partition_file();

    let pushed = explain_virtual_sql(
        &mut conn,
        &format!("SELECT COUNT(*) FROM {partitioned} WHERE P_INT < 9"),
    );
    assert_scan_names_only(&pushed, &every_readable, "P_INT < 9");

    let pushed = explain_virtual_sql(
        &mut conn,
        &format!("SELECT COUNT(*) FROM {partitioned} WHERE P_INT = 1"),
    );
    assert_scan_names_only(&pushed, &partition_files("1"), "P_INT = 1");

    let predicate = "P_INT = 1 AND P_DATE >= DATE '2024-01-02'";
    let pushed = explain_virtual_sql(
        &mut conn,
        &format!("SELECT COUNT(*) FROM {partitioned} WHERE {predicate}"),
    );
    assert_scan_names_only(&pushed, &["20240102_000000_00001_p2"], predicate);

    let count = |conn: &mut ExaConn, predicate: &str| {
        conn.query_scalar_i64(&format!(
            "SELECT COUNT(*) FROM {partitioned} WHERE {predicate}"
        ))
    };
    assert_eq!(count(&mut conn, "P_INT = 1"), 4);
    assert_eq!(count(&mut conn, predicate), 1);
    let limited = conn.query_columns(&format!(
        "SELECT ID FROM {partitioned} WHERE P_INT = 2 LIMIT 1"
    ));
    assert_eq!(int_column(&limited[0]), [5]);
}

/// Scenario: Every Hive type declares and returns its mapped value on a Glue Parquet table
#[test]
fn glue_all_types_declare_and_return_their_mapped_values() {
    let fixture = GlueFixture::provision();
    let mut conn = exa_conn();
    let timestamp = expected_timestamp_precision(&mut conn).declared_column_type;
    let all_types = fixture.table(ALL_TYPES);

    let expected_types = [
        ("ID", "DECIMAL(20,0)"),
        ("H_TINYINT", "DECIMAL(3,0)"),
        ("H_SMALLINT", "DECIMAL(5,0)"),
        ("H_INT", "DECIMAL(10,0)"),
        ("H_INTEGER", "DECIMAL(10,0)"),
        ("H_BIGINT", "DECIMAL(20,0)"),
        ("H_FLOAT", "DOUBLE"),
        ("H_DOUBLE", "DOUBLE"),
        ("H_BOOLEAN", "BOOLEAN"),
        ("H_STRING", VARCHAR_JSON),
        ("H_STRING_OVER_BINARY", VARCHAR_JSON),
        ("H_VARCHAR", VARCHAR_JSON),
        ("H_CHAR", VARCHAR_JSON),
        ("H_DECIMAL_10_2", "DECIMAL(10,2)"),
        ("H_DECIMAL", "DECIMAL(10,0)"),
        ("H_DECIMAL_SPACED", VARCHAR_JSON),
        ("H_DATE", "DATE"),
        ("H_TIMESTAMP", timestamp),
        ("H_BINARY", VARCHAR_JSON),
        ("H_ARRAY_INT", VARCHAR_JSON),
        ("H_ARRAY_STRUCT", VARCHAR_JSON),
        ("H_MAP_STRING", VARCHAR_JSON),
        ("H_MAP_VARCHAR", VARCHAR_JSON),
        ("H_STRUCT_XY", VARCHAR_JSON),
        ("H_STRUCT_BINARY", VARCHAR_JSON),
        ("H_UNIONTYPE", VARCHAR_JSON),
        ("H_INTERVAL", VARCHAR_JSON),
        ("H_MALFORMED_MAP", VARCHAR_JSON),
        ("H_EMPTY_TYPE", VARCHAR_JSON),
    ];
    assert_eq!(
        declared_types(&mut conn, VS, ALL_TYPES),
        pairs(&expected_types)
    );
    assert_text_columns(
        &mut conn,
        &format!(
            "SELECT ID, H_TINYINT, H_SMALLINT, H_INT, H_INTEGER, H_BIGINT, H_FLOAT, H_DOUBLE, \
             H_BOOLEAN, H_STRING, H_STRING_OVER_BINARY, H_VARCHAR, H_CHAR, H_DECIMAL_10_2, \
             H_DECIMAL, H_DECIMAL_SPACED, H_DATE, H_TIMESTAMP, H_ARRAY_INT, H_ARRAY_STRUCT, \
             H_MAP_STRING, H_MAP_VARCHAR, H_STRUCT_XY FROM {all_types} ORDER BY ID"
        ),
        &[
            [Some("1"), Some("2"), Some("3")],
            [Some("127"), Some("-128"), None],
            [Some("32767"), Some("-32768"), None],
            [Some("2147483647"), Some("-2147483648"), None],
            [Some("7"), Some("-7"), None],
            [
                Some("9223372036854775807"),
                Some("-9223372036854775808"),
                None,
            ],
            [Some("1.5"), Some("-0.25"), None],
            [Some("2.5"), Some("-0.125"), None],
            [Some("true"), Some("false"), None],
            [Some("h\u{e9}llo"), Some("w\u{f6}rld"), None],
            [Some("legacy-a"), Some("legacy-b"), None],
            [Some("short"), Some("text"), None],
            [Some("abcde"), Some("fghij"), None],
            [Some("12.34"), Some("-0.05"), None],
            [Some("42"), Some("-42"), None],
            [
                Some("1234567890123456789012345678.9012345678"),
                Some("-0.0000000005"),
                None,
            ],
            [Some("2024-01-15"), Some("1970-01-01"), None],
            [
                Some("2024-01-15 10:30:45.123000"),
                Some("1970-01-01 00:00:00.000000"),
                None,
            ],
            [Some("[1,2]"), Some("[]"), None],
            [Some("[{\"a\":1.25}]"), Some("[]"), None],
            [Some("{\"k1\":1,\"k2\":2}"), Some("{}"), None],
            [Some("{\"a\":1}"), Some("{}"), None],
            [
                Some("{\"x\":1,\"y\":\"p\"}"),
                Some("{\"x\":2,\"y\":null}"),
                None,
            ],
        ],
    );
    for (column, fragments) in [
        ("H_BINARY", &["type 'binary'", "#351"][..]),
        (
            "H_STRUCT_BINARY",
            &["member 'h_struct_binary.b'", "type 'binary'", "#351"],
        ),
        ("H_UNIONTYPE", &["Hive type 'uniontype<int,string>'"]),
        ("H_INTERVAL", &["Hive type 'interval_day_time'"]),
        ("H_MALFORMED_MAP", &["Hive type 'map<int>'"]),
        ("H_EMPTY_TYPE", &["Hive type ''"]),
    ] {
        let sql = format!("SELECT {column} FROM {all_types}");
        assert_query_fails(&mut conn, &sql, fragments);
    }

    assert_eq!(
        declared_types(&mut conn, VS, BINARY_VALUES),
        pairs(&[("ID", "DECIMAL(20,0)"), ("C_BYTES", VARCHAR_JSON)])
    );
    let sql = format!("SELECT C_BYTES FROM {}", fixture.table(BINARY_VALUES));
    assert_query_fails(&mut conn, &sql, &["Invalid UTF8 sequence"]);
}

/// Scenario: The suite fails, never skips, when a variable or the stack is missing
#[test]
fn glue_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        ExaConn::connect("127.0.0.1", 1, "sys", SYS_PASSWORD);
    });
    let payload = result.expect_err("connecting to an unavailable Exasol must fail, never skip");
    let message = panic_payload_message(&*payload).unwrap_or_default();
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
    let mut conn = exa_conn();
    let connection_sql = build_create_connection_sql(
        PROBE_CONN,
        &sentinel_env.glue_endpoint(),
        &glue_connection_password(&sentinel_env),
    );

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        execute_redacted(
            &mut conn,
            &sentinel_env,
            "CREATE CONNECTION",
            &format!("{connection_sql} THIS_TRAILING_TOKEN_MAKES_THE_STATEMENT_INVALID"),
        );
    }));
    let payload = result.expect_err("the malformed credential-bearing DDL must fail");
    let message = panic_payload_message(&*payload).unwrap_or_default();
    assert!(
        message.contains("CREATE CONNECTION failed"),
        "the failure must still be reported: {message}"
    );
    assert!(
        !message.contains(SENTINEL_SECRET),
        "a failed CONNECTION DDL must not echo the secret access key: {message}"
    );

    execute_redacted(
        &mut conn,
        &sentinel_env,
        "CREATE CONNECTION",
        &connection_sql,
    );
    conn.execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {PROBE_VS} CASCADE"));
    let error = query_error(
        &mut conn,
        &format!(
            "CREATE VIRTUAL SCHEMA {PROBE_VS} USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH \
             CATALOG_CONNECTION = '{PROBE_CONN}' CATALOG_KIND = 'GLUE' \
             NAMESPACE = 'lh_e2e_redaction_probe'"
        ),
    );
    conn.execute(&format!("DROP CONNECTION IF EXISTS {PROBE_CONN}"));
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
