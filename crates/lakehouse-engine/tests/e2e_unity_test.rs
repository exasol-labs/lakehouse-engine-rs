//! All tests share one virtual schema, so they must run serially (`--test-threads=1`).
//! The OSS Unity Catalog server has authorization disabled and vends no S3 endpoint, so the
//! CONNECTION carries no catalog-auth field but does carry SeaweedFS's endpoint and static keys.

mod common;

use common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, SCHEMA_NAME, SYS_PASSWORD, VARCHAR_JSON, VsProps,
    assert_decimal_orders_numerically, assert_query_fails, assert_type_matrix,
    column_adapter_notes, create_schema_and_scripts, create_virtual_schema_with_password,
    declared_schema_of, declared_types, exa_conn, explain_virtual_sql, has_broadcast_join_block,
    has_two_scan_wrapper, install_slc, local_stack_creds, note_of, pairs, parse_int, parse_numeric,
    partition_positions, query_error, reads, refuses, upload_so, value_to_string,
};
use common::exasol_ws::ExaConn;
use common::pruning_cases::{
    Format, VS_DELTA, assert_cases_hold, assert_one_shaped_file_per_label,
    assert_single_group_aggregates_match_oracle, read_footer,
};
use common::pruning_fixture::{
    PRUNING_FILES, PRUNING_PARTITION_COLUMN, PRUNING_TABLE, labels_named, oracle_statements,
    pruning_data_file, pruning_relative_path,
};
use common::raw_parquet::{
    delete_fixture_prefix, encode_parquet, get_fixture_object, put_fixture_object,
    write_parquet_fixture,
};
use common::seed::{
    ALL_TYPES_IDS_TEXT, BOOLEAN_VALUES_TEXT, DATE_VALUES_TEXT, DECIMAL_10_2_VALUES_TEXT,
    DECIMAL_18_0_VALUES_TEXT, DECIMAL_36_6_VALUES_TEXT, DECIMAL_38_10_VALUES_TEXT,
    FLOAT32_VALUES_TEXT, INT_LIST_VALUES_TEXT, INT8_VALUES_TEXT, INT16_VALUES_TEXT,
    INT32_VALUES_TEXT, TEXT_VALUES_TEXT, TIMESTAMP_VALUES_TEXT, all_types_ids, binary_values,
    boolean_values, date_values, decimal_10_2_values, decimal_18_0_values, decimal_36_6_values,
    decimal_38_10_values, float32_values, int_list_values, int_string_struct_values, int8_values,
    int16_values, int32_values, string_int_map_values, struct_binary_values, text_values,
    timestamp_values,
};
use common::stack::{
    self, ASSUME_ROLE_ARN, ASSUME_ROLE_BASE_ACCESS_KEY, ASSUME_ROLE_BASE_SECRET_KEY,
    ASSUME_ROLE_EXTERNAL_ID, CatalogConnectionPassword, build_create_connection_sql, exasol_host,
    exasol_sql_port, local_stack_connection_password, wait_for_exasol, wait_for_seaweedfs,
    wait_for_url,
};
use common::timestamp_precision::expected_timestamp_precision;

use lakehouse_catalog::{
    CatalogClient, CatalogTableIdent, ConnectionCreds, StorageBackend, UnityCatalogSession,
};
use lakehouse_engine::adapter::connection::storage_block;
use lakehouse_engine::adapter::pushdown::{
    ConnectionStorage, ResolvedScan, ScanSource, format_reader,
};
use lakehouse_engine::scan::spec::{DeleteMechanism, FileEntry};

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const VS_NAME: &str = "UNITY_DELTA_E2E_VS";
const CONN_NAME: &str = "UNITY_CATALOG_CREDS";
const UNITY_NAMESPACE: &str = "unity.delta_e2e";
const UNITY_CATALOG_URI_INTERNAL: &str = "http://unitycatalog:8080";

const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

const EXPECTED_TABLES: &[&str] = &[
    "TABLE_WITH_DV",
    "CM_NAME_MODE",
    "CM_ID_MODE",
    "BASIC_PARTITIONED",
    "MULTI_PART_STATS",
    "STATS_ALL_TYPES",
    "UNSHREDDED_VARIANT",
    "TYPE_WIDENING",
    "SALES_PARQUET",
    "ALL_TYPES_PARQUET",
    "DELTA_EXTRA_TYPES",
];

/// Outside the Delta fixtures' `s3://warehouse/delta/` prefix, so they never collide.
const SALES_PARQUET_LOCATION: &str = "s3://warehouse/unity_parquet/sales_parquet";
const ALL_TYPES_PARQUET_LOCATION: &str = "s3://warehouse/unity_parquet/all_types_parquet";
const DELTA_EXTRA_TYPES_LOCATION: &str = "s3://warehouse/unity_delta/delta_extra_types";
const CM_NAME_MODE_PARTITIONED_LOCATION: &str =
    "s3://warehouse/unity_delta/cm_name_mode_partitioned";

const PRUNING_DELTA_LOCATION: &str = "s3://warehouse/delta_pruning/pruning_cases";
/// Its own schema, so `VS_NAME` never enumerates the pruning table.
const PRUNING_UNITY_NAMESPACE: &str = "unity.e2e_pruning";
const PRUNING_DELTA_CONN: &str = "PRUNING_DELTA_CREDS";

fn unity_port() -> u16 {
    std::env::var("LH_UNITY_PORT")
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(18080)
}

/// A 2xx on `catalogs` proves the server is serving, not merely that the port is open.
fn wait_for_unity_catalog() {
    let url = format!(
        "http://localhost:{}/api/2.1/unity-catalog/catalogs",
        unity_port()
    );
    wait_for_url(&url, READINESS_TIMEOUT);
}

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_seaweedfs();
        wait_for_unity_catalog();

        install_slc();
        upload_so();
        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
        for statement in oracle_statements() {
            conn.execute(&statement);
        }

        seed_sales_parquet_table();
        seed_all_types_parquet_table();
        seed_delta_extra_types_table();
        seed_cm_name_mode_partitioned_table();
        seed_pruning_delta_table();

        create_unity_virtual_schema(&mut conn);
        create_unity_role_virtual_schema(&mut conn);
        create_catalog_copy_virtual_schema(&mut conn);
        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(VS_DELTA, PRUNING_UNITY_NAMESPACE)
                .with_catalog_kind("UNITY_CATALOG")
                .with_catalog_conn_name(PRUNING_DELTA_CONN),
            UNITY_CATALOG_URI_INTERNAL,
            &local_stack_connection_password(),
        );
    });
}

fn create_unity_virtual_schema(conn: &mut ExaConn) {
    let password = local_stack_connection_password();
    let create_conn_sql =
        build_create_connection_sql(CONN_NAME, UNITY_CATALOG_URI_INTERNAL, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!("DROP VIRTUAL SCHEMA IF EXISTS {VS_NAME} CASCADE"));

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION = '{CONN_NAME}'
  CATALOG_KIND       = 'UNITY_CATALOG'
  NAMESPACE  = '{UNITY_NAMESPACE}'
  ALLOW_HTTP         = 'true'"#
    ));
}

/// Over the seeded entries whose catalog metadata deliberately disagrees with their Delta logs.
const COPY_VS_NAME: &str = "UNITY_CATALOG_COPY_VS";

/// Runs CREATE through `conn.execute`, so a CREATE that fails over the unusable entries fails
/// every test.
fn create_catalog_copy_virtual_schema(conn: &mut ExaConn) {
    create_virtual_schema_with_password(
        conn,
        &VsProps::new(COPY_VS_NAME, "unity.delta_catalog_copy")
            .with_catalog_kind("UNITY_CATALOG")
            .with_catalog_conn_name(CONN_NAME),
        UNITY_CATALOG_URI_INTERNAL,
        &local_stack_connection_password(),
    );
}

/// A second Virtual Schema over the SAME seeded `unity.delta_e2e` namespace.
const VS_ROLE_NAME: &str = "UNITY_DELTA_ROLE_VS";
/// Catalog CONNECTION carrying the assume-role identity instead of static keys.
const CONN_ROLE_NAME: &str = "UNITY_ROLE_CATALOG_CREDS";

/// A CONNECTION with no `use_vended_credentials` that instead names the
/// assume-role identity: the base key pair, the role,
/// its external id, and SeaweedFS's own STS as `aws_sts_endpoint`.
fn create_unity_role_virtual_schema(conn: &mut ExaConn) {
    let password = CatalogConnectionPassword {
        access_key: ASSUME_ROLE_BASE_ACCESS_KEY.to_string(),
        secret_key: ASSUME_ROLE_BASE_SECRET_KEY.to_string(),
        aws_assume_role_arn: Some(ASSUME_ROLE_ARN.to_string()),
        aws_external_id: Some(ASSUME_ROLE_EXTERNAL_ID.to_string()),
        aws_sts_endpoint: Some(stack::seaweedfs_url_internal()),
        ..local_stack_connection_password()
    };
    let create_conn_sql =
        build_create_connection_sql(CONN_ROLE_NAME, UNITY_CATALOG_URI_INTERNAL, &password);
    conn.execute(&create_conn_sql);

    let _ = conn.try_execute(&format!(
        "DROP VIRTUAL SCHEMA IF EXISTS {VS_ROLE_NAME} CASCADE"
    ));

    conn.execute(&format!(
        r#"CREATE VIRTUAL SCHEMA {VS_ROLE_NAME}
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION = '{CONN_ROLE_NAME}'
  CATALOG_KIND       = 'UNITY_CATALOG'
  NAMESPACE  = '{UNITY_NAMESPACE}'
  ALLOW_HTTP         = 'true'"#
    ));
}

/// `year`/`region` are partition directories, not in-file columns.
fn seed_sales_parquet_table() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let files: [(&str, &[i64], &[f64]); 3] = [
        ("year=2024/region=eu/p1.parquet", &[1, 2], &[100.0, 200.0]),
        ("year=2024/region=us/p2.parquet", &[3], &[300.0]),
        ("year=2025/region=eu/p3.parquet", &[4], &[400.0]),
    ];
    for (file, ids, amounts) in files {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .expect("sales_parquet batch");
        write_parquet_fixture(&format!("{SALES_PARQUET_LOCATION}/{file}"), batch);
    }

    register_unity_table(&UnityTable {
        namespace: UNITY_NAMESPACE,
        name: "sales_parquet",
        format: "PARQUET",
        location: SALES_PARQUET_LOCATION,
        columns: &[
            ("id", json!("long"), "LONG"),
            ("amount", json!("double"), "DOUBLE"),
            ("year", json!("integer"), "INT"),
            ("region", json!("string"), "STRING"),
        ],
        partitions: &["year", "region"],
        name_mapped: false,
    });
}

fn spark_field(name: &str, spark_type: &Json) -> Json {
    json!({"name": name, "type": spark_type, "nullable": true, "metadata": {}})
}

fn struct_type(members: &[(&str, &str)]) -> Json {
    let fields: Vec<Json> = members
        .iter()
        .map(|(name, member_type)| spark_field(name, &json!(member_type)))
        .collect();
    json!({"type": "struct", "fields": fields})
}

/// A Unity table fixture: `register_unity_table` registers it and `write_delta_table` writes its
/// Delta log from the same columns.
struct UnityTable<'a> {
    namespace: &'a str,
    name: &'a str,
    format: &'a str,
    location: &'a str,
    columns: &'a [(&'a str, Json, &'a str)],
    partitions: &'a [&'a str],
    /// Name-mode column mapping: column `c` is stored as `col-c`, in data files and in
    /// `add.partitionValues` alike (Delta PROTOCOL.md § Column Mapping).
    name_mapped: bool,
}

impl UnityTable<'_> {
    fn field(&self, position: usize, name: &str, spark_type: &Json) -> Json {
        let mut field = spark_field(name, spark_type);
        if self.name_mapped {
            field["metadata"] = json!({
                "delta.columnMapping.id": position + 1,
                "delta.columnMapping.physicalName": format!("col-{name}"),
            });
        }
        field
    }

    /// The Delta table properties, which Unity Catalog carries as the table's `properties`.
    fn configuration(&self) -> Json {
        if self.name_mapped {
            json!({
                "delta.columnMapping.mode": "name",
                "delta.columnMapping.maxColumnId": self.columns.len().to_string(),
            })
        } else {
            json!({})
        }
    }
}

fn split_unity_namespace(namespace: &str) -> (&str, &str) {
    namespace
        .split_once('.')
        .unwrap_or_else(|| panic!("Unity namespace {namespace} is not <catalog>.<schema>"))
}

/// A column's Spark type is the `type` of the `StructField` JSON the reader reads. Creates the
/// schema if absent and replaces any earlier registration, so no fixture meets a stale one.
fn register_unity_table(table: &UnityTable) {
    let UnityTable {
        namespace,
        name,
        format,
        location,
        columns,
        partitions,
        ..
    } = table;
    let columns: Vec<Json> = columns
        .iter()
        .enumerate()
        .map(|(position, (column, spark_type, type_name))| {
            let type_text = spark_type
                .as_str()
                .or(spark_type["type"].as_str())
                .unwrap_or_default();
            let mut entry = json!({
                "name": column, "type_text": type_text, "type_name": type_name,
                "type_json": table.field(position, column, spark_type).to_string(),
                "position": position, "nullable": true
            });
            if let Some(index) = partitions.iter().position(|p| p == column) {
                entry["partition_index"] = index.into();
            }
            entry
        })
        .collect();

    let base = format!("{}/api/2.1/unity-catalog", unity_catalog_url());
    let client = reqwest::blocking::Client::new();
    let (catalog_name, schema_name) = split_unity_namespace(namespace);
    ensure_unity_schema(&client, &base, namespace);

    let delete = client
        .delete(format!("{base}/tables/{namespace}.{name}"))
        .send()
        .unwrap_or_else(|e| panic!("DELETE {name}: {e}"));
    assert!(
        delete.status().is_success() || delete.status() == reqwest::StatusCode::NOT_FOUND,
        "DELETE {name} returned {}",
        delete.status()
    );

    let response = client
        .post(format!("{base}/tables"))
        .json(&json!({
            "name": name, "catalog_name": catalog_name, "schema_name": schema_name,
            "table_type": "EXTERNAL", "data_source_format": format,
            "storage_location": location, "columns": columns,
            "properties": table.configuration()
        }))
        .send()
        .unwrap_or_else(|e| panic!("POST {name}: {e}"));
    let status = response.status();
    assert!(
        status.is_success(),
        "POST {name} failed: {status}: {}",
        response.text().unwrap_or_default()
    );
}

/// The rule of `ensure` in `scripts/unity/seed.sh`: Unity Catalog OSS answers a re-create with
/// HTTP 400 and an `*_ALREADY_EXISTS` error code, which leaves the wanted end state.
fn ensure_unity_schema(client: &reqwest::blocking::Client, base: &str, namespace: &str) {
    let (catalog_name, schema_name) = split_unity_namespace(namespace);
    let response = client
        .post(format!("{base}/schemas"))
        .json(&json!({"name": schema_name, "catalog_name": catalog_name}))
        .send()
        .unwrap_or_else(|e| panic!("POST schema {catalog_name}.{schema_name}: {e}"));
    let status = response.status();
    let body = response.text().unwrap_or_else(|e| {
        panic!("read the POST schema {catalog_name}.{schema_name} response body: {e}")
    });
    let already_exists = serde_json::from_str::<Json>(&body).is_ok_and(|error| {
        error["error_code"]
            .as_str()
            .is_some_and(|code| code.ends_with("_ALREADY_EXISTS"))
    });
    assert!(
        status == reqwest::StatusCode::OK || already_exists,
        "POST schema {catalog_name}.{schema_name} failed: {status}: {body}"
    );
}

/// Every Spark type a Unity Parquet table can declare, except the `long` and `double` that
/// `sales_parquet` carries; `c_variant` has no data, because the reader refuses it at plan time.
fn seed_all_types_parquet_table() {
    let columns = [
        ("id", Some(all_types_ids()), json!("long"), "LONG"),
        ("c_byte", Some(int8_values()), json!("byte"), "BYTE"),
        ("c_short", Some(int16_values()), json!("short"), "SHORT"),
        ("c_int", Some(int32_values()), json!("integer"), "INT"),
        ("c_float", Some(float32_values()), json!("float"), "FLOAT"),
        (
            "c_boolean",
            Some(boolean_values()),
            json!("boolean"),
            "BOOLEAN",
        ),
        ("c_string", Some(text_values()), json!("string"), "STRING"),
        (
            "c_decimal_10_2",
            Some(decimal_10_2_values()),
            json!("decimal(10,2)"),
            "DECIMAL",
        ),
        (
            "c_decimal_18_0",
            Some(decimal_18_0_values()),
            json!("decimal(18,0)"),
            "DECIMAL",
        ),
        (
            "c_decimal_36_6",
            Some(decimal_36_6_values()),
            json!("decimal(36,6)"),
            "DECIMAL",
        ),
        (
            "c_decimal_38_10",
            Some(decimal_38_10_values()),
            json!("decimal(38,10)"),
            "DECIMAL",
        ),
        ("c_date", Some(date_values()), json!("date"), "DATE"),
        (
            "c_timestamp",
            Some(timestamp_values(Some("UTC"))),
            json!("timestamp"),
            "TIMESTAMP",
        ),
        (
            "c_timestamp_ntz",
            Some(timestamp_values(None)),
            json!("timestamp_ntz"),
            "TIMESTAMP_NTZ",
        ),
        (
            "c_binary",
            Some(binary_values(&DataType::Binary)),
            json!("binary"),
            "BINARY",
        ),
        (
            "c_array",
            Some(int_list_values()),
            json!({"type": "array", "elementType": "integer", "containsNull": true}),
            "ARRAY",
        ),
        (
            "c_map",
            Some(string_int_map_values(
                vec!["k1", "k2"],
                vec![1, 2],
                [2, 0, 0],
            )),
            json!({
                "type": "map", "keyType": "string", "valueType": "integer",
                "valueContainsNull": true
            }),
            "MAP",
        ),
        (
            "c_struct",
            Some(int_string_struct_values("a", "b", "x")),
            struct_type(&[("a", "integer"), ("b", "string")]),
            "STRUCT",
        ),
        (
            "c_struct_binary",
            Some(struct_binary_values(None)),
            struct_type(&[("x", "binary")]),
            "STRUCT",
        ),
        ("c_variant", None, json!("variant"), "VARIANT"),
    ];
    let batch =
        RecordBatch::try_from_iter_with_nullable(columns.iter().filter_map(
            |(name, data, _, _)| data.clone().map(|data| (*name, data, *name != "id")),
        ))
        .expect("all_types_parquet batch");
    write_parquet_fixture(
        &format!("{ALL_TYPES_PARQUET_LOCATION}/part-00000.parquet"),
        batch,
    );

    let registered: Vec<_> = columns
        .into_iter()
        .map(|(name, _, spark_type, type_name)| (name, spark_type, type_name))
        .collect();
    register_unity_table(&UnityTable {
        namespace: UNITY_NAMESPACE,
        name: "all_types_parquet",
        format: "PARQUET",
        location: ALL_TYPES_PARQUET_LOCATION,
        columns: &registered,
        partitions: &[],
        name_mapped: false,
    });
}

/// The Delta types `stats_all_types` lacks: the decimal matrix and a binary struct member, in
/// one data file with no reader feature.
fn seed_delta_extra_types_table() {
    let batch = RecordBatch::try_from_iter_with_nullable(vec![
        ("id", all_types_ids(), false),
        ("c_decimal_10_2", decimal_10_2_values(), true),
        ("c_decimal_18_0", decimal_18_0_values(), true),
        ("c_decimal_36_6", decimal_36_6_values(), true),
        ("c_decimal_38_10", decimal_38_10_values(), true),
        ("c_struct_binary", struct_binary_values(None), true),
    ])
    .expect("delta_extra_types batch");

    let columns = [
        ("id", json!("long"), "LONG"),
        ("c_decimal_10_2", json!("decimal(10,2)"), "DECIMAL"),
        ("c_decimal_18_0", json!("decimal(18,0)"), "DECIMAL"),
        ("c_decimal_36_6", json!("decimal(36,6)"), "DECIMAL"),
        ("c_decimal_38_10", json!("decimal(38,10)"), "DECIMAL"),
        ("c_struct_binary", struct_type(&[("x", "binary")]), "STRUCT"),
    ];
    let table = UnityTable {
        namespace: UNITY_NAMESPACE,
        name: "delta_extra_types",
        format: "DELTA",
        location: DELTA_EXTRA_TYPES_LOCATION,
        columns: &columns,
        partitions: &[],
        name_mapped: false,
    };
    write_delta_table(
        &table,
        &[DeltaDataFile {
            path: "part-00000.parquet".to_string(),
            partition_values: json!({}),
            parquet: encode_parquet(&batch),
            stats: json!({"numRecords": 3}),
        }],
    );
    register_unity_table(&table);
}

/// No vendored fixture is both column-mapped and partitioned. Each data file holds only
/// `col-id`, and its `letter` lives in the log under `col-letter`.
fn seed_cm_name_mode_partitioned_table() {
    let columns = [
        ("id", json!("long"), "LONG"),
        ("letter", json!("string"), "STRING"),
    ];
    let table = UnityTable {
        namespace: UNITY_NAMESPACE,
        name: "cm_name_mode_partitioned",
        format: "DELTA",
        location: CM_NAME_MODE_PARTITIONED_LOCATION,
        columns: &columns,
        partitions: &["letter"],
        name_mapped: true,
    };
    let files: Vec<DeltaDataFile> = [(1, "a"), (2, "b")]
        .into_iter()
        .map(|(id, letter)| DeltaDataFile {
            path: format!("col-letter={letter}/part-0.parquet"),
            partition_values: json!({ "col-letter": letter }),
            parquet: encode_parquet(
                &RecordBatch::try_from_iter(vec![(
                    "col-id",
                    Arc::new(Int64Array::from(vec![id])) as _,
                )])
                .expect("cm_name_mode_partitioned batch"),
            ),
            stats: json!({"numRecords": 1}),
        })
        .collect();
    write_delta_table(&table, &files);
    register_unity_table(&table);
}

/// One data file of a `write_delta_table` commit, at `path` relative to the table root.
struct DeltaDataFile {
    path: String,
    partition_values: Json,
    parquet: Bytes,
    stats: Json,
}

/// Drops `location` and writes one commit anew, so no run overwrites a live log entry or data file
/// (Delta PROTOCOL.md § Creation of New Log Entries).
fn write_delta_table(table: &UnityTable, files: &[DeltaDataFile]) {
    let UnityTable {
        location,
        columns,
        partitions: partition_columns,
        ..
    } = table;
    delete_fixture_prefix(location);
    let fields: Vec<Json> = columns
        .iter()
        .enumerate()
        .map(|(position, (name, spark_type, _))| table.field(position, name, spark_type))
        .collect();
    let id = location
        .rsplit('/')
        .next()
        .expect("a table location has a last path segment");
    let mut log = vec![
        json!({"protocol": delta_protocol(table)}),
        json!({"metaData": {
            "id": id, "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": fields}).to_string(),
            "partitionColumns": partition_columns, "configuration": table.configuration(),
            "createdTime": 0
        }}),
    ];
    for file in files {
        put_fixture_object(&format!("{location}/{}", file.path), file.parquet.clone());
        log.push(json!({"add": {
            "path": file.path, "partitionValues": file.partition_values,
            "size": file.parquet.len(), "modificationTime": 0, "dataChange": true,
            "stats": file.stats.to_string()
        }}));
    }
    let log: Vec<String> = log.iter().map(Json::to_string).collect();
    put_fixture_object(&delta_first_commit_uri(location), log.join("\n").into());
}

fn delta_first_commit_uri(location: &str) -> String {
    format!("{location}/_delta_log/00000000000000000000.json")
}

/// Delta PROTOCOL.md § Timestamp without timezone: a `timestamp_ntz` column needs reader
/// version 3, writer version 7, and `timestampNtz` in both feature lists. § Column Mapping:
/// reader version 2 and writer version 5 enable it.
fn delta_protocol(table: &UnityTable) -> Json {
    if table.name_mapped {
        return json!({"minReaderVersion": 2, "minWriterVersion": 5});
    }
    if table
        .columns
        .iter()
        .any(|(_, spark_type, _)| spark_type == "timestamp_ntz")
    {
        json!({
            "minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["timestampNtz"], "writerFeatures": ["timestampNtz"]
        })
    } else {
        json!({"minReaderVersion": 1, "minWriterVersion": 2})
    }
}

/// The Delta copy of the pruning fixture: the Hive copy's data files, with each partition value
/// and the `k` statistics in the log.
fn seed_pruning_delta_table() {
    let columns = [
        ("id", json!("long"), "LONG"),
        (PRUNING_PARTITION_COLUMN, json!("string"), "STRING"),
        ("k", json!("long"), "LONG"),
        ("s", json!("string"), "STRING"),
        ("x", json!("double"), "DOUBLE"),
        ("ts", json!("timestamp_ntz"), "TIMESTAMP_NTZ"),
    ];
    let files: Vec<DeltaDataFile> = PRUNING_FILES
        .iter()
        .map(|file| DeltaDataFile {
            path: pruning_relative_path(file),
            partition_values: json!({ PRUNING_PARTITION_COLUMN: file.partition }),
            parquet: pruning_data_file(file),
            stats: k_stats(&file.k_values()),
        })
        .collect();
    let table = UnityTable {
        namespace: PRUNING_UNITY_NAMESPACE,
        name: PRUNING_TABLE,
        format: "DELTA",
        location: PRUNING_DELTA_LOCATION,
        columns: &columns,
        partitions: &[PRUNING_PARTITION_COLUMN],
        name_mapped: false,
    };
    write_delta_table(&table, &files);
    register_unity_table(&table);
}

/// Delta PROTOCOL.md § Per-file Statistics: a column that is NULL in every row carries its null
/// count and no bound.
fn k_stats(k_values: &[Option<i64>]) -> Json {
    let present: Vec<i64> = k_values.iter().flatten().copied().collect();
    let mut stats = json!({
        "numRecords": k_values.len(),
        "nullCount": {"k": k_values.len() - present.len()},
    });
    if let (Some(min), Some(max)) = (present.iter().min(), present.iter().max()) {
        stats["minValues"] = json!({"k": min});
        stats["maxValues"] = json!({"k": max});
    }
    stats
}

fn enumerated_table_names(conn: &mut ExaConn, vs_name: &str) -> Vec<String> {
    let cols = conn.query_columns(&format!(
        "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA = '{vs_name}'"
    ));
    cols.first()
        .map(|c| {
            c.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_uppercase()))
                .collect()
        })
        .unwrap_or_default()
}

fn assert_col_type(cols: &[(String, String)], column: &str, expected: &str) {
    let (_, actual) = cols
        .iter()
        .find(|(name, _)| name == column)
        .unwrap_or_else(|| panic!("column {column} not declared; got {cols:?}"));
    assert_eq!(actual, expected, "column {column}");
}

#[test]
fn unity_create_virtual_schema_lists_fixture_tables_and_columns() {
    setup();
    let mut conn = exa_conn();

    let tables = enumerated_table_names(&mut conn, VS_NAME);
    for expected in EXPECTED_TABLES {
        assert!(
            tables.iter().any(|t| t == expected),
            "createVirtualSchema must enumerate the seeded '{expected}' fixture table; got {tables:?}"
        );
    }

    let cm_cols = declared_types(&mut conn, VS_NAME, "CM_NAME_MODE");
    assert_col_type(&cm_cols, "ID", "DECIMAL(20,0)");
    assert_col_type(&cm_cols, "NAME", "VARCHAR(2000000)");
    assert_col_type(&cm_cols, "VALUE", "DOUBLE");

    let stats_cols = declared_types(&mut conn, VS_NAME, "STATS_ALL_TYPES");
    assert_col_type(&stats_cols, "ARRAY_COL", "VARCHAR(2000000)");
}

#[test]
fn unity_suite_fails_when_stack_unavailable() {
    let result = std::panic::catch_unwind(|| {
        wait_for_url(
            "http://127.0.0.1:1/api/2.1/unity-catalog/catalogs",
            Duration::from_secs(2),
        );
    });
    assert!(
        result.is_err(),
        "a readiness wait against an unreachable Unity Catalog stack must panic (fail), \
         never return Ok (skip)"
    );
}

#[test]
fn unity_credentials_never_appear_in_output() {
    const SENTINEL_TOKEN: &str = "UC_DUMMY_BEARER_TOKEN_SENTINEL";

    wait_for_exasol();
    let mut conn =
        ExaConn::connect_redacting(&exasol_host(), exasol_sql_port(), "sys", SYS_PASSWORD);

    let sentinel_password = CatalogConnectionPassword {
        token: Some(SENTINEL_TOKEN.to_string()),
        ..Default::default()
    };
    let base_sql = build_create_connection_sql(
        "UC_REDACTION_PROBE",
        UNITY_CATALOG_URI_INTERNAL,
        &sentinel_password,
    );
    let failing_sql = format!("{base_sql} THIS_TRAILING_TOKEN_MAKES_THE_STATEMENT_INVALID");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.execute(&failing_sql);
    }));
    let payload = match result {
        Ok(_) => panic!("expected execute() to fail on the malformed credential-bearing DDL"),
        Err(p) => p,
    };
    let panic_msg = stack::panic_payload_message(&*payload).unwrap_or_default();

    assert!(
        !panic_msg.is_empty(),
        "expected a string panic payload from the failed redacting execute()"
    );
    assert!(
        !panic_msg.contains(&failing_sql),
        "redacting execute() failure must not echo the SQL text: {panic_msg}"
    );
    assert!(
        !panic_msg.contains(SENTINEL_TOKEN),
        "redacting execute() failure must not leak the bearer token: {panic_msg}"
    );
}

fn unity_catalog_url() -> String {
    format!("http://localhost:{}", unity_port())
}

/// Under vending, `endpoint`/`region` still reach the vended backend because the OSS server
/// vends no S3 endpoint of its own.
fn delta_creds(use_vended_credentials: bool) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: String::new(),
        endpoint: stack::seaweedfs_url(),
        region: "us-east-1".to_string(),
        access_key: "lhadmin".to_string(),
        secret_key: "lhadminsecret123".to_string(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: false,
        use_vended_credentials,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
        ..Default::default()
    }
}

fn delta_static_storage() -> StorageBackend {
    storage_block(&delta_creds(false), true)
}

fn delta_e2e_table(name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: vec!["unity".to_string(), "delta_e2e".to_string()],
        name: name.to_string(),
    }
}

async fn resolve_unity_scan(
    table_name: &str,
    use_vended_credentials: bool,
    filter: Option<&serde_json::Value>,
) -> ResolvedScan {
    let creds = delta_creds(use_vended_credentials);
    let session = UnityCatalogSession::new(&unity_catalog_url(), creds.clone());
    let table = session
        .load_table(&delta_e2e_table(table_name))
        .await
        .unwrap_or_else(|e| panic!("load_table({table_name}) failed: {e}"));
    let storage = delta_static_storage();
    let declared = declared_schema_of(&mut exa_conn(), VS_NAME, &table_name.to_uppercase());

    let reader = format_reader(
        ScanSource::Unity {
            session: &session,
            table: &table,
        },
        &ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    )
    .unwrap_or_else(|e| panic!("format_reader({table_name}) failed: {e}"));

    ResolvedScan::resolve(reader.as_ref(), declared, filter)
        .await
        .unwrap_or_else(|e| panic!("resolve_scan({table_name}) failed: {e}"))
}

/// Panics on an empty partition map: comparing two runs cannot detect both silently losing
/// the values, which live only in the transaction log.
fn path_sorted_letter_values(files: &[FileEntry]) -> Vec<Option<String>> {
    let mut carried: Vec<(&str, Option<String>)> = files
        .iter()
        .map(|entry| {
            let mut partition_values = entry.partition_values.iter();
            let (column, value) = partition_values.next().unwrap_or_else(|| {
                panic!(
                    "{} must carry its logged partition value, not an empty map",
                    entry.path
                )
            });
            assert!(
                partition_values.next().is_none(),
                "{} must carry exactly one partition entry: {:?}",
                entry.path,
                entry.partition_values
            );
            assert_eq!(
                column, "letter",
                "`letter` is basic_partitioned's only partition column"
            );
            (entry.path.as_str(), value.clone())
        })
        .collect();
    carried.sort_by(|(left, _), (right, _)| left.cmp(right));
    carried.into_iter().map(|(_, value)| value).collect()
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Scenario: Vended and static credentials resolve the same scan
#[test]
fn unity_delta_planning_agrees_under_vended_and_static_credentials() {
    setup();

    let rt = rt();
    let vended = rt.block_on(resolve_unity_scan("basic_partitioned", true, None));
    let static_creds = rt.block_on(resolve_unity_scan("basic_partitioned", false, None));

    assert!(
        !vended.files.is_empty(),
        "basic_partitioned must resolve at least one active data file"
    );
    assert_eq!(
        vended.table_root, static_creds.table_root,
        "vended and static credential runs must resolve the identical table root"
    );
    assert_eq!(
        vended.files, static_creds.files,
        "vended and static credential runs must agree, entry for entry, on the resolved \
         file list"
    );

    let letters = path_sorted_letter_values(&vended.files);
    assert_eq!(
        letters,
        vec![
            None,
            Some("a".to_string()),
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
            Some("e".to_string()),
        ],
        "the live S3 path must carry every logged partition value, the Hive \
         default-partition file's explicit NULL among them"
    );
    assert!(
        !letters
            .iter()
            .any(|value| value.as_deref() == Some("__HIVE_DEFAULT_PARTITION__")),
        "the Hive default-partition DIRECTORY literal is never carried as a value: \
         {letters:?}"
    );

    let dv_scan = rt.block_on(resolve_unity_scan("table_with_dv", true, None));
    assert_eq!(
        dv_scan.files.len(),
        1,
        "table_with_dv must resolve exactly one active data file: {:?}",
        dv_scan.files
    );
    let has_deletion_vector = dv_scan.files[0]
        .deletes
        .iter()
        .any(|delete| matches!(delete, DeleteMechanism::DeltaDeletionVector { .. }));
    assert!(
        has_deletion_vector,
        "table_with_dv's single active file must carry a deletion-vector reference: {:?}",
        dv_scan.files[0]
    );
}

/// Scenario: Pruning reaches every request shape and changes no result end to end
#[test]
fn unity_delta_filters_prune_the_resolved_file_list() {
    setup();

    let rt = rt();

    let all_partitioned = rt.block_on(resolve_unity_scan("basic_partitioned", true, None));
    assert_eq!(
        all_partitioned.files.len(),
        6,
        "basic_partitioned must resolve 6 unfiltered files: {:?}",
        all_partitioned.files
    );
    let letter_filter = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "LETTER"},
        "right": {"type": "literal_string", "value": "a"}
    });
    let pruned_partitioned = rt.block_on(resolve_unity_scan(
        "basic_partitioned",
        true,
        Some(&letter_filter),
    ));
    assert_eq!(
        pruned_partitioned.files.len(),
        2,
        "LETTER = 'a' must prune basic_partitioned to 2 files: {:?}",
        pruned_partitioned.files
    );

    let all_stats = rt.block_on(resolve_unity_scan("multi_part_stats", true, None));
    assert_eq!(
        all_stats.files.len(),
        5,
        "multi_part_stats must resolve 5 unfiltered files: {:?}",
        all_stats.files
    );
    let id_filter = serde_json::json!({
        "type": "predicate_lessequal",
        "left": {"type": "column", "name": "ID"},
        "right": {"type": "literal_exactnumeric", "value": "2"}
    });
    let pruned_stats = rt.block_on(resolve_unity_scan(
        "multi_part_stats",
        true,
        Some(&id_filter),
    ));
    assert_eq!(
        pruned_stats.files.len(),
        2,
        "ID <= 2 must prune multi_part_stats to 2 files: {:?}",
        pruned_stats.files
    );
}

fn table_ref(table: &str) -> String {
    format!("{VS_NAME}.{table}")
}

/// Returns the JSON value opening at `start` and the index past its closing bracket; brackets
/// inside quoted strings are skipped.
fn json_value_at(text: &str, start: usize) -> (serde_json::Value, usize) {
    let bytes = text.as_bytes();
    let (open, close) = match bytes[start] {
        b'{' => (b'{', b'}'),
        b'[' => (b'[', b']'),
        other => {
            panic!("expected a JSON object or array at offset {start}, found byte {other}: {text}")
        }
    };
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut end = None;
    for (i, &b) in bytes[start..].iter().enumerate() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b if b == open => depth += 1,
            b if b == close => {
                depth -= 1;
                if depth == 0 {
                    end = Some(start + i + 1);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end.unwrap_or_else(|| panic!("unbalanced JSON at offset {start}: {text}"));
    let value = serde_json::from_str(&text[start..end])
        .unwrap_or_else(|e| panic!("expected valid JSON at offset {start} ({e}): {text}"));
    (value, end)
}

/// Scenario: A Delta data file carrying no deletion vector scans unchanged
#[test]
fn unity_delta_delete_free_table_returns_its_rows() {
    setup();
    let mut conn = exa_conn();

    let count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {}",
        table_ref("MULTI_PART_STATS")
    ));
    assert_eq!(
        count, 5,
        "multi_part_stats' five active data files hold five rows in total"
    );

    let cols = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {}",
        table_ref("MULTI_PART_STATS")
    ));
    assert_eq!(cols.len(), 2, "expected ID, VALUE columns: {cols:?}");
    assert_eq!(
        cols[0].len(),
        5,
        "SELECT must return the same 5 rows COUNT(*) reports: {cols:?}"
    );
    assert!(
        cols.iter().all(|col| col.iter().all(|v| !v.is_null())),
        "a delete-free table's rows must carry real column values, not NULL: {cols:?}"
    );
}

/// Scenario: Session credentials are the storage credential when the CONNECTION does not vend
#[test]
fn unity_role_connection_reads_a_delta_table_through_the_session() {
    setup();
    let mut conn = exa_conn();

    let role_table = format!("{VS_ROLE_NAME}.MULTI_PART_STATS");
    let count = conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {role_table}"));
    assert_eq!(
        count, 5,
        "multi_part_stats' five active data files hold five rows in total"
    );

    let cols = conn.query_columns(&format!("SELECT ID, \"VALUE\" FROM {role_table}"));
    assert_eq!(cols.len(), 2, "expected ID, VALUE columns: {cols:?}");
    assert_eq!(
        cols[0].len(),
        5,
        "SELECT must return the same 5 rows COUNT(*) reports: {cols:?}"
    );
    assert!(
        cols.iter().all(|col| col.iter().all(|v| !v.is_null())),
        "a delete-free table's rows must carry real column values, not NULL: {cols:?}"
    );
}

/// Scenario: Deletion vectors compose with projection, filter, LIMIT, and aggregation
#[test]
fn unity_delta_deletion_vector_table_returns_only_live_rows() {
    setup();
    let mut conn = exa_conn();

    let count = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {}",
        table_ref("TABLE_WITH_DV")
    ));
    assert_eq!(
        count, 8,
        "the deletion vector removes 2 of table_with_dv's 10 physical rows"
    );

    let cols = conn.query_columns(&format!(
        "SELECT \"VALUE\" FROM {}",
        table_ref("TABLE_WITH_DV")
    ));
    let values: Vec<i64> = cols[0].iter().map(parse_int).collect();
    assert_eq!(values.len(), 8, "expected 8 live values: {values:?}");
    assert!(
        !values.contains(&0) && !values.contains(&9),
        "the deleted values 0 and 9 must be absent: {values:?}"
    );

    let filtered = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE \"VALUE\" = 0",
        table_ref("TABLE_WITH_DV")
    ));
    assert_eq!(
        filtered, 0,
        "a predicate selecting a deleted row must return no row: the deletion \
         vector is applied beneath the pushed-down filter"
    );
}

/// Scenario: Each logical field carries the binding key its column-mapping mode selects
#[test]
fn unity_delta_column_mapped_tables_return_logical_column_values() {
    setup();
    let mut conn = exa_conn();

    let id_mode = conn.query_columns(&format!(
        "SELECT ID, NAME, \"VALUE\" FROM {} ORDER BY ID",
        table_ref("CM_ID_MODE")
    ));
    let name_mode = conn.query_columns(&format!(
        "SELECT ID, NAME, \"VALUE\" FROM {} ORDER BY ID",
        table_ref("CM_NAME_MODE")
    ));

    assert_eq!(
        id_mode.len(),
        3,
        "expected ID, NAME, VALUE columns: {id_mode:?}"
    );
    assert!(
        !id_mode[0].is_empty(),
        "cm_id_mode must return at least one row"
    );
    assert!(
        !name_mode[0].is_empty(),
        "cm_name_mode must return at least one row"
    );

    for (label, cols) in [("CM_ID_MODE", &id_mode), ("CM_NAME_MODE", &name_mode)] {
        for (col_idx, col_name) in ["ID", "NAME", "VALUE"].iter().enumerate() {
            assert!(
                cols[col_idx].iter().all(|v| !v.is_null()),
                "{label}.{col_name} must never be NULL: a logical-name-only \
                 binding against a col-<uuid> physical name would produce NULL: \
                 {cols:?}"
            );
        }
    }

    let partitioned = table_ref("CM_NAME_MODE_PARTITIONED");
    let letters = conn.query_columns(&format!("SELECT LETTER FROM {partitioned} ORDER BY ID"));
    assert_eq!(
        letters[0],
        [json!("a"), json!("b")],
        "partition values are logged under the physical name `col-letter` and read as LETTER"
    );
    assert_eq!(
        conn.query_scalar_i64(&format!("SELECT ID FROM {partitioned} WHERE LETTER = 'b'")),
        2,
        "a filter on the partition column matches by its physical name"
    );
}

/// Scenario: A partition column absent from the data file is materialized per file
#[test]
fn unity_delta_partitioned_table_returns_partition_values() {
    setup();
    let mut conn = exa_conn();

    let letters: Vec<Option<String>> = conn.query_columns(&format!(
        "SELECT LETTER FROM {}",
        table_ref("BASIC_PARTITIONED")
    ))[0]
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect();
    assert_eq!(
        letters.len(),
        6,
        "basic_partitioned's six files hold six rows: {letters:?}"
    );
    assert!(
        !letters
            .iter()
            .any(|l| l.as_deref() == Some("__HIVE_DEFAULT_PARTITION__")),
        "the Hive default-partition DIRECTORY literal must never surface as a \
         value: {letters:?}"
    );
    assert_eq!(
        letters.iter().filter(|l| l.is_none()).count(),
        1,
        "exactly one row's logged letter is NULL (the default-partition file): \
         {letters:?}"
    );

    let filtered = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE LETTER = 'a'",
        table_ref("BASIC_PARTITIONED")
    ));
    assert_eq!(
        filtered, 2,
        "exactly the rows whose logged partition value is 'a' must match the filter"
    );

    let grouped = conn.query_columns(&format!(
        "SELECT LETTER, COUNT(*) FROM {} GROUP BY LETTER",
        table_ref("BASIC_PARTITIONED")
    ));
    assert_eq!(
        grouped.len(),
        2,
        "expected LETTER, COUNT(*) columns: {grouped:?}"
    );
    let mut group_counts: std::collections::HashMap<Option<String>, i64> = grouped[0]
        .iter()
        .zip(grouped[1].iter())
        .map(|(letter, count)| (letter.as_str().map(str::to_string), parse_int(count)))
        .collect();
    assert_eq!(
        group_counts.remove(&None),
        Some(1),
        "the NULL-letter group must hold exactly 1 row: {group_counts:?}"
    );
    assert_eq!(
        group_counts.remove(&Some("a".to_string())),
        Some(2),
        "group 'a' must hold exactly 2 rows: {group_counts:?}"
    );
    for letter in ["b", "c", "e"] {
        assert_eq!(
            group_counts.remove(&Some(letter.to_string())),
            Some(1),
            "group '{letter}' must hold exactly 1 row: {group_counts:?}"
        );
    }
    assert!(
        group_counts.is_empty(),
        "no group beyond NULL,a,b,c,e is expected: {group_counts:?}"
    );
}

/// Scenario: Every pushdown request shape resolves through the one format-reader seam
#[test]
fn unity_delta_join_and_aggregate_pushdown_return_correct_rows() {
    setup();
    let mut conn = exa_conn();

    let raw_ids: Vec<i64> = conn
        .query_columns(&format!("SELECT ID FROM {}", table_ref("MULTI_PART_STATS")))[0]
        .iter()
        .map(parse_int)
        .collect();
    assert_eq!(
        raw_ids.len(),
        5,
        "multi_part_stats holds 5 rows: {raw_ids:?}"
    );

    let grouped = conn.query_columns(&format!(
        "SELECT ID, COUNT(*) FROM {} GROUP BY ID",
        table_ref("MULTI_PART_STATS")
    ));
    let mut expected_group_counts: std::collections::HashMap<i64, i64> =
        std::collections::HashMap::new();
    for id in &raw_ids {
        *expected_group_counts.entry(*id).or_insert(0) += 1;
    }
    for (id, count) in grouped[0].iter().zip(grouped[1].iter()) {
        let key = parse_int(id);
        let actual_count = parse_int(count);
        let expected_count = expected_group_counts
            .remove(&key)
            .unwrap_or_else(|| panic!("unexpected group key {key}: {grouped:?}"));
        assert_eq!(
            actual_count, expected_count,
            "group {key}: COUNT(*) mismatch"
        );
    }
    assert!(
        expected_group_counts.is_empty(),
        "GROUP BY ID must cover every id the ground-truth scan saw: missing {expected_group_counts:?}"
    );

    let mut expected_top3 = raw_ids.clone();
    expected_top3.sort_unstable_by(|a, b| b.cmp(a));
    expected_top3.truncate(3);
    let top3: Vec<i64> = conn.query_columns(&format!(
        "SELECT ID FROM {} ORDER BY ID DESC LIMIT 3",
        table_ref("MULTI_PART_STATS")
    ))[0]
        .iter()
        .map(parse_int)
        .collect();
    assert_eq!(
        top3, expected_top3,
        "ORDER BY ID DESC LIMIT 3 must match a full sort + truncate"
    );

    // Broadcast goes to the smaller side by active-file bytes; cm_id_mode (5253) outweighs
    // basic_partitioned (4505), keeping the partitioned table on the broadcast side.
    let join_sql = format!(
        "SELECT p.LETTER, c.ID FROM {} p JOIN {} c ON p.NUMBER = c.ID",
        table_ref("BASIC_PARTITIONED"),
        table_ref("CM_ID_MODE")
    );
    let pushed = explain_virtual_sql(&mut conn, &join_sql);
    assert!(
        has_broadcast_join_block(&pushed),
        "the join must drive one broadcast scan UDF, not the two-scan fallback: {pushed}"
    );
    assert!(
        !has_two_scan_wrapper(&pushed),
        "the join must not fall back to the two-scan Exasol-joined shape: {pushed}"
    );
    let common_table_root_idx = pushed
        .find("\"table_root\":\"")
        .unwrap_or_else(|| panic!("expected the surrounding common spec's table_root: {pushed}"));
    let common_table_root_start = common_table_root_idx + "\"table_root\":\"".len();
    let common_table_root_end = pushed[common_table_root_start..]
        .find('"')
        .map(|rel| common_table_root_start + rel)
        .unwrap_or_else(|| panic!("unterminated table_root string: {pushed}"));
    let common_table_root = &pushed[common_table_root_start..common_table_root_end];
    assert!(
        common_table_root.contains("cdf-column-mapping-id-mode"),
        "the sharded (fact) side must be cm_id_mode, the larger of the two: {pushed}"
    );

    let join_key_idx = pushed
        .find("\"join\":{")
        .unwrap_or_else(|| panic!("expected a join block in the pushed SQL: {pushed}"));
    let join_value_start = join_key_idx + "\"join\":".len();
    let (join_value, _) = json_value_at(&pushed, join_value_start);
    let join_table_root = join_value["table_root"]
        .as_str()
        .unwrap_or_else(|| panic!("join block must carry a table_root: {join_value}"));
    assert!(
        join_table_root.contains("basic_partitioned"),
        "the broadcast (dimension) side's join block must carry \
         basic_partitioned's table root, the PARTITIONED table: {pushed}"
    );

    let cm_ids: Vec<i64> = conn
        .query_columns(&format!("SELECT ID FROM {}", table_ref("CM_ID_MODE")))[0]
        .iter()
        .map(parse_int)
        .collect();
    let distinct_cm_ids: std::collections::HashSet<i64> = cm_ids.iter().copied().collect();
    assert_eq!(
        distinct_cm_ids.len(),
        cm_ids.len(),
        "cm_id_mode's ID values must be distinct for the semi-join ground truth \
         (membership via cm_ids.contains) to equal a real join: {cm_ids:?}"
    );
    let base_cols = conn.query_columns(&format!(
        "SELECT LETTER, \"NUMBER\" FROM {}",
        table_ref("BASIC_PARTITIONED")
    ));
    let mut expected_join: Vec<(Option<String>, i64)> = base_cols[0]
        .iter()
        .zip(base_cols[1].iter())
        .map(|(letter, number)| (letter.as_str().map(str::to_string), parse_int(number)))
        .filter(|(_, number)| cm_ids.contains(number))
        .collect();
    expected_join.sort_by(|a, b| a.1.cmp(&b.1));

    let join_cols = conn.query_columns(&join_sql);
    let mut actual_join: Vec<(Option<String>, i64)> = join_cols[0]
        .iter()
        .zip(join_cols[1].iter())
        .map(|(letter, id)| (letter.as_str().map(str::to_string), parse_int(id)))
        .collect();
    actual_join.sort_by(|a, b| a.1.cmp(&b.1));

    assert!(
        !actual_join.is_empty(),
        "the join must return at least one matching row to prove more than an \
         empty-result coincidence: basic_partitioned NUMBER and cm_id_mode ID \
         must overlap"
    );
    assert_eq!(
        actual_join, expected_join,
        "the broadcast join result must match an in-process join over the two \
         tables' full contents, carrying basic_partitioned's LETTER value"
    );
}

/// `byte_decimal` and `short_decimal` are refused per column (decision [15]).
const TYPE_WIDENING_SUPPORTED_COLUMNS: &str = "BYTE_LONG, INT_LONG, FLOAT_DOUBLE, BYTE_DOUBLE, SHORT_DOUBLE, INT_DOUBLE, \
     DECIMAL_DECIMAL_SAME_SCALE, DECIMAL_DECIMAL_GREATER_SCALE, INT_DECIMAL, LONG_DECIMAL, \
     DATE_TIMESTAMP_NTZ";

/// Scenario: A refused table fails the query with a SQL error and leaves the session usable
#[test]
fn unity_delta_unsupported_reader_feature_fails_the_query_loud() {
    setup();
    let mut conn = exa_conn();

    let table = "UNSHREDDED_VARIANT";
    let feature = "variantType-preview";
    let resp = conn.try_execute(&format!("SELECT * FROM {} LIMIT 1", table_ref(table)));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "{table} must fail the query loud, not return a row: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains(feature),
        "{table}'s error must name its actual unsupported reader feature: {msg}"
    );
    assert!(
        !msg.contains("#349"),
        "{table}'s error must not cite issue #349: type widening is no longer \
         refused, so a closed issue cited in a shipped refusal would read as an \
         unfixed gap with no owner: {msg}"
    );
    assert!(
        !msg.contains("#350") && !msg.to_lowercase().contains("does not map at plan time"),
        "{table}'s error must be the protocol-gate refusal, not a column-typed \
         type-mapping error: {msg}"
    );
    assert!(
        !msg.to_lowercase().contains("lhadminsecret123"),
        "{table}'s error text must not contain a credential value: {msg}"
    );

    let survives = conn.query_scalar_i64("SELECT 1 FROM DUAL");
    assert_eq!(
        survives, 1,
        "the connection must survive the refusal: a crashed UDF VM would take \
         the session down, not return a clean SQL error"
    );
}

/// Scenario: A narrow physical column binds to the current wider logical type and is cast per file
#[test]
fn unity_delta_type_widening_returns_the_widened_types_across_both_files() {
    setup();
    let mut conn = exa_conn();
    let table = table_ref("TYPE_WIDENING");

    let count = conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {table}"));
    assert_eq!(
        count, 2,
        "type_widening must carry exactly 2 rows, one from each of its two live \
         data files: {count}"
    );

    let timestamp = expected_timestamp_precision(&mut conn).declared_column_type;
    let cols = declared_types(&mut conn, VS_NAME, "TYPE_WIDENING");
    for (column, expected) in [
        ("BYTE_LONG", "DECIMAL(20,0)"),
        ("INT_LONG", "DECIMAL(20,0)"),
        ("FLOAT_DOUBLE", "DOUBLE"),
        ("BYTE_DOUBLE", "DOUBLE"),
        ("SHORT_DOUBLE", "DOUBLE"),
        ("INT_DOUBLE", "DOUBLE"),
        ("DECIMAL_DECIMAL_SAME_SCALE", "DECIMAL(20,2)"),
        ("DECIMAL_DECIMAL_GREATER_SCALE", "DECIMAL(20,5)"),
        ("BYTE_DECIMAL", "DECIMAL(4,1)"),
        ("SHORT_DECIMAL", "DECIMAL(6,1)"),
        ("INT_DECIMAL", "DECIMAL(11,1)"),
        ("LONG_DECIMAL", "DECIMAL(21,1)"),
        ("DATE_TIMESTAMP_NTZ", timestamp),
    ] {
        assert_col_type(&cols, column, expected);
    }

    let select_sql =
        format!("SELECT {TYPE_WIDENING_SUPPORTED_COLUMNS} FROM {table} ORDER BY INT_LONG");
    let pushed = explain_virtual_sql(&mut conn, &select_sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "the eleven-column projection must drive the scan UDF, not an \
         unaccelerated fallback: {pushed}"
    );

    let rows = conn.query_columns(&select_sql);
    assert_eq!(rows.len(), 11, "expected 11 projected columns: {rows:?}");
    assert_eq!(
        rows[0].len(),
        2,
        "expected both the pre- and post-widening rows, not one skipped or \
         failed: {rows:?}"
    );

    const PRE: usize = 0;
    const POST: usize = 1;

    assert_eq!(
        parse_int(&rows[0][PRE]),
        1,
        "pre-widening BYTE_LONG must be its real logged value, not NULL: {rows:?}"
    );
    assert_eq!(
        parse_int(&rows[0][POST]),
        9_223_372_036_854_775_807,
        "post-widening BYTE_LONG must hold a value no 32-bit width could \
         represent: {rows:?}"
    );

    assert_eq!(
        parse_int(&rows[1][PRE]),
        2,
        "pre-widening INT_LONG must be its real logged value, not NULL: {rows:?}"
    );
    assert_eq!(
        parse_int(&rows[1][POST]),
        9_223_372_036_854_775_807,
        "post-widening INT_LONG must hold a value no 32-bit width could \
         represent: {rows:?}"
    );

    let pre_widening_float = parse_numeric(&rows[2][PRE]);
    assert!(
        (pre_widening_float - f64::from(3.4f32)).abs() < 1e-9,
        "pre-widening FLOAT_DOUBLE must be the stored 32-bit float's exact double \
         expansion, not the decimal literal 3.4: {pre_widening_float}"
    );

    for (index, column, expected_pre) in [
        (3, "BYTE_DOUBLE", 5.0),
        (4, "SHORT_DOUBLE", 6.0),
        (5, "INT_DOUBLE", 7.0),
    ] {
        let pre = parse_numeric(&rows[index][PRE]);
        assert!(
            (pre - expected_pre).abs() < 1e-9,
            "pre-widening {column} must be its real logged value, not NULL: {pre}"
        );
        let post = parse_numeric(&rows[index][POST]);
        assert!(
            (post - 1.234_567_890_123_f64).abs() < 1e-9,
            "post-widening {column} must hold a fractional value no integral \
             width could represent: {post}"
        );
    }

    assert_eq!(
        value_to_string(&rows[6][PRE]),
        "123.45",
        "pre-widening DECIMAL_DECIMAL_SAME_SCALE must be its real logged value, \
         not NULL: {rows:?}"
    );
    assert_eq!(
        value_to_string(&rows[6][POST]),
        "12345678901234.56",
        "post-widening DECIMAL_DECIMAL_SAME_SCALE must hold a value no narrower \
         decimal precision could represent: {rows:?}"
    );

    assert_eq!(
        value_to_string(&rows[7][PRE]),
        "67.89",
        "pre-widening DECIMAL_DECIMAL_GREATER_SCALE must prove the \
         decimal(10,2) -> decimal(20,5) RESCALE, not a re-tag: re-reading the \
         stored unscaled 6789 at scale 5 would render 0.06789: {rows:?}"
    );
    assert_eq!(
        value_to_string(&rows[7][POST]),
        "12345678901.23456",
        "post-widening DECIMAL_DECIMAL_GREATER_SCALE must keep all five \
         fractional digits its widened scale allows: {rows:?}"
    );

    assert_eq!(
        value_to_string(&rows[8][PRE]),
        "3",
        "pre-widening INT_DECIMAL must be its real logged value, not NULL: {rows:?}"
    );
    assert_eq!(
        value_to_string(&rows[9][PRE]),
        "4",
        "pre-widening LONG_DECIMAL must be its real logged value, not NULL: {rows:?}"
    );
    assert_eq!(
        value_to_string(&rows[9][POST]),
        "123456789012345678.9",
        "post-widening LONG_DECIMAL must hold a value no narrower decimal width \
         could represent: {rows:?}"
    );

    let pre_widening_timestamp = rows[10][PRE].as_str().unwrap_or("");
    assert!(
        pre_widening_timestamp.starts_with("2024-09-09 00:00:00"),
        "pre-widening DATE_TIMESTAMP_NTZ must be the midnight instant of its \
         logged date, not NULL: {pre_widening_timestamp}"
    );

    for (column, delta_name, from_type, to_type) in [
        ("BYTE_DECIMAL", "byte_decimal", "byte", "decimal(4,1)"),
        ("SHORT_DECIMAL", "short_decimal", "short", "decimal(6,1)"),
    ] {
        let resp = conn.try_execute(&format!("SELECT {column} FROM {table}"));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "{column} must refuse the query, not return a row or a NULL value: {resp}"
        );
        let msg = resp["exception"]["text"].as_str().unwrap_or("");
        assert!(
            msg.contains(delta_name)
                && msg.contains(&format!("'{from_type}'"))
                && msg.contains(to_type),
            "{column}'s refusal must name its Delta column and both its Delta \
             types: {msg}"
        );
        assert!(
            !msg.to_lowercase().contains("lhadminsecret123"),
            "{column}'s error text must not contain a credential value: {msg}"
        );
    }
}

const STATS_ALL_TYPES_MAPPABLE_COLUMNS: &str = "BYTE_COL, SHORT_COL, INT_COL, LONG_COL, FLOAT_COL, DOUBLE_COL, DATE_COL, \
     TIMESTAMP_COL, TIMESTAMP_NTZ_COL, STRING_COL, DECIMAL_COL, BOOLEAN_COL, ARRAY_COL, \
     MAP_COL, NESTED_STRUCT";

/// Scenario: Every Delta type Exasol represents natively maps to its own Arrow tag
/// Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing
#[test]
fn unity_delta_varied_types_return_their_expected_exasol_types_and_values() {
    setup();
    let mut conn = exa_conn();
    let table = table_ref("STATS_ALL_TYPES");

    let count = conn.query_scalar_i64(&format!("SELECT COUNT(*) FROM {table}"));
    assert_eq!(
        count, 4,
        "stats_all_types must carry exactly 4 rows: {count}"
    );

    let timestamp = expected_timestamp_precision(&mut conn).declared_column_type;
    let cols = declared_types(&mut conn, VS_NAME, "STATS_ALL_TYPES");
    for (column, expected) in [
        ("BYTE_COL", "DECIMAL(3,0)"),
        ("SHORT_COL", "DECIMAL(5,0)"),
        ("INT_COL", "DECIMAL(10,0)"),
        ("LONG_COL", "DECIMAL(20,0)"),
        ("FLOAT_COL", "DOUBLE"),
        ("DOUBLE_COL", "DOUBLE"),
        ("DATE_COL", "DATE"),
        ("TIMESTAMP_COL", timestamp),
        ("TIMESTAMP_NTZ_COL", timestamp),
        ("STRING_COL", "VARCHAR(2000000)"),
        ("DECIMAL_COL", "DECIMAL(10,2)"),
        ("BOOLEAN_COL", "BOOLEAN"),
        ("ARRAY_COL", "VARCHAR(2000000)"),
        ("MAP_COL", "VARCHAR(2000000)"),
        ("NESTED_STRUCT", "VARCHAR(2000000)"),
    ] {
        assert_col_type(&cols, column, expected);
    }

    let select_sql = format!("SELECT {STATS_ALL_TYPES_MAPPABLE_COLUMNS} FROM {table}");
    let pushed = explain_virtual_sql(&mut conn, &select_sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "the mappable-column projection must drive the scan UDF, not an \
         unaccelerated fallback: {pushed}"
    );

    let rows = conn.query_columns(&select_sql);
    assert_eq!(rows.len(), 15, "expected 15 projected columns: {rows:?}");
    assert_eq!(rows[0].len(), 4, "expected 4 rows: {rows:?}");

    let byte_non_null: Vec<i64> = rows[0]
        .iter()
        .filter(|v| !v.is_null())
        .map(parse_int)
        .collect();
    assert_eq!(
        byte_non_null.len(),
        3,
        "BYTE_COL must carry 3 real logged values and 1 NULL, not be silently \
         NULLed by a missing mapping: {:?}",
        rows[0]
    );

    let short_non_null: Vec<i64> = rows[1]
        .iter()
        .filter(|v| !v.is_null())
        .map(parse_int)
        .collect();
    assert_eq!(
        short_non_null.len(),
        3,
        "SHORT_COL must carry 3 real logged values and 1 NULL, not be silently \
         NULLed by a missing mapping: {:?}",
        rows[1]
    );

    let canonical_json = |cell: &serde_json::Value| -> Option<String> {
        cell.as_str().map(|text| {
            let parsed: serde_json::Value = serde_json::from_str(text)
                .unwrap_or_else(|e| panic!("must parse as JSON: {e}: {text}"));
            serde_json::to_string(&parsed).expect("re-serializing a parsed value cannot fail")
        })
    };

    let array_col = &rows[12];
    let mut rendered: Vec<Option<String>> = array_col.iter().map(canonical_json).collect();
    rendered.sort();
    let mut expected_array = vec![
        None,
        canonical_json(&serde_json::json!("[1,2,3]")),
        canonical_json(&serde_json::json!("[4,5]")),
        canonical_json(&serde_json::json!("[6]")),
    ];
    expected_array.sort();
    assert_eq!(
        rendered, expected_array,
        "ARRAY_COL must render each populated array as a strict-JSON array of \
         bare numbers, not the old Arrow display rendering, and keep a NULL \
         array NULL: {array_col:?}"
    );

    let map_col = &rows[13];
    let mut rendered_map: Vec<Option<String>> = map_col.iter().map(canonical_json).collect();
    rendered_map.sort();
    let mut expected_map = vec![
        None,
        canonical_json(&serde_json::json!(r#"{"key1":100}"#)),
        canonical_json(&serde_json::json!(r#"{"key2":200,"key3":300}"#)),
        canonical_json(&serde_json::json!(r#"{"key4":400}"#)),
    ];
    expected_map.sort();
    assert_eq!(
        rendered_map, expected_map,
        "MAP_COL must render each populated row as a JSON object keyed by its \
         own string keys, and keep a NULL map NULL: {map_col:?}"
    );

    let nested_struct = &rows[14];
    for cell in nested_struct.iter().filter_map(|v| v.as_str()) {
        assert!(
            !cell.contains("col-"),
            "NESTED_STRUCT must never surface a col- prefixed physical name: {cell}"
        );
    }
    let mut rendered_struct: Vec<Option<String>> =
        nested_struct.iter().map(canonical_json).collect();
    rendered_struct.sort();
    let mut expected_struct = vec![
        None,
        canonical_json(&serde_json::json!(
            r#"{"inner_int":10,"inner_string":"nested_a","inner_double":1.1}"#
        )),
        canonical_json(&serde_json::json!(
            r#"{"inner_int":50,"inner_string":"nested_c","inner_double":5.5}"#
        )),
        canonical_json(&serde_json::json!(
            r#"{"inner_int":30,"inner_string":null,"inner_double":3.3}"#
        )),
    ];
    expected_struct.sort();
    assert_eq!(
        rendered_struct, expected_struct,
        "NESTED_STRUCT must render each populated row keyed by the LOGICAL inner \
         names, a NULL member as SQL NULL's JSON counterpart, and a NULL struct \
         cell as SQL NULL: {nested_struct:?}"
    );
}

/// Scenario: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR
/// Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing
/// Scenario: An incompatible Unity Catalog column type is declared as VARCHAR rather than failing
#[test]
fn unity_delta_extra_types_declare_and_return_their_mapped_values() {
    setup();
    let mut conn = exa_conn();
    assert_type_matrix(
        &mut conn,
        VS_NAME,
        "DELTA_EXTRA_TYPES",
        &[
            reads("ID", "DECIMAL(20,0)", ALL_TYPES_IDS_TEXT),
            reads("C_DECIMAL_10_2", "DECIMAL(10,2)", DECIMAL_10_2_VALUES_TEXT),
            reads("C_DECIMAL_18_0", "DECIMAL(18,0)", DECIMAL_18_0_VALUES_TEXT),
            reads("C_DECIMAL_36_6", "DECIMAL(36,6)", DECIMAL_36_6_VALUES_TEXT),
            reads("C_DECIMAL_38_10", VARCHAR_JSON, DECIMAL_38_10_VALUES_TEXT),
            refuses("C_STRUCT_BINARY", VARCHAR_JSON, STRUCT_BINARY_REFUSAL),
        ],
    );
    assert_decimal_orders_numerically(&mut conn, VS_NAME, "DELTA_EXTRA_TYPES", "C_DECIMAL_18_0");
}

const STRUCT_BINARY_REFUSAL: &[&str] = &[
    "column 'c_struct_binary'",
    "member 'c_struct_binary.x'",
    "type 'binary'",
    "#351",
];

#[test]
fn unity_delta_refused_column_refuses_only_the_queries_naming_it() {
    setup();
    let mut conn = exa_conn();
    let table = table_ref("STATS_ALL_TYPES");

    let resp = conn.try_execute(&format!("SELECT BINARY_COL FROM {table}"));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "BINARY_COL must refuse the query, not return a row: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        msg.contains("binary_col") && msg.contains("#351") && !msg.contains("#350"),
        "BINARY_COL's refusal must name its Delta column and cite issue #351, not #350: {msg}"
    );

    let star_resp = conn.try_execute(&format!("SELECT * FROM {table}"));
    assert_eq!(
        star_resp["status"].as_str(),
        Some("error"),
        "SELECT * widens to the full base row, so a refused column anywhere in \
         the table must refuse it too: {star_resp}"
    );

    let where_resp = conn.try_execute(&format!(
        "SELECT INT_COL FROM {table} WHERE BINARY_COL IS NOT NULL"
    ));
    assert_eq!(
        where_resp["status"].as_str(),
        Some("error"),
        "a WHERE clause referencing a refused column must refuse it even though \
         the select list names only a mappable column: {where_resp}"
    );

    let nested_resp = conn.query_columns(&format!("SELECT MAP_COL, NESTED_STRUCT FROM {table}"));
    assert_eq!(
        nested_resp.len(),
        2,
        "MAP_COL and NESTED_STRUCT must query successfully, not appear in any \
         refusal: {nested_resp:?}"
    );
    assert_eq!(nested_resp[0].len(), 4, "expected 4 rows: {nested_resp:?}");

    let mappable = conn.query_columns(&format!(
        "SELECT {STATS_ALL_TYPES_MAPPABLE_COLUMNS} FROM {table}"
    ));
    assert_eq!(
        mappable.len(),
        15,
        "the mappable 15-column projection must still succeed on the same \
         connection, proving the refusal above was per-request: {mappable:?}"
    );
    assert_eq!(
        mappable[0].len(),
        4,
        "expected 4 rows from the mappable projection: {mappable:?}"
    );
}

/// Scenario: Pruning reaches every request shape and changes no result end to end
#[test]
fn unity_delta_pruned_queries_return_unchanged_rows() {
    setup();
    let mut conn = exa_conn();

    let partitioned = conn.query_columns(&format!(
        "SELECT LETTER, \"NUMBER\" FROM {}",
        table_ref("BASIC_PARTITIONED")
    ));
    let all_partitioned: Vec<(Option<String>, i64)> = partitioned[0]
        .iter()
        .zip(partitioned[1].iter())
        .map(|(letter, number)| (letter.as_str().map(str::to_string), parse_int(number)))
        .collect();
    assert_eq!(
        all_partitioned.len(),
        6,
        "basic_partitioned's six files hold six rows: {all_partitioned:?}"
    );

    let mut expected_letter_a: Vec<(Option<String>, i64)> = all_partitioned
        .iter()
        .filter(|(letter, _)| letter.as_deref() == Some("a"))
        .cloned()
        .collect();
    expected_letter_a.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(
        expected_letter_a.len(),
        2,
        "letter 'a' must cover exactly 2 rows in the unfiltered scan: {expected_letter_a:?}"
    );
    let letter_a = conn.query_columns(&format!(
        "SELECT LETTER, \"NUMBER\" FROM {} WHERE LETTER = 'a'",
        table_ref("BASIC_PARTITIONED")
    ));
    let mut actual_letter_a: Vec<(Option<String>, i64)> = letter_a[0]
        .iter()
        .zip(letter_a[1].iter())
        .map(|(letter, number)| (letter.as_str().map(str::to_string), parse_int(number)))
        .collect();
    actual_letter_a.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(
        actual_letter_a, expected_letter_a,
        "the partition predicate LETTER = 'a' must return the same rows pruning \
         inert would: {actual_letter_a:?}"
    );

    let stats = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {}",
        table_ref("MULTI_PART_STATS")
    ));
    let all_stats: Vec<(i64, Option<String>)> = stats[0]
        .iter()
        .zip(stats[1].iter())
        .map(|(id, value)| (parse_int(id), value.as_str().map(str::to_string)))
        .collect();
    assert_eq!(
        all_stats.len(),
        5,
        "multi_part_stats' five active data files hold five rows: {all_stats:?}"
    );

    let mut expected_id_le_2: Vec<(i64, Option<String>)> = all_stats
        .iter()
        .filter(|(id, _)| *id <= 2)
        .cloned()
        .collect();
    expected_id_le_2.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        expected_id_le_2.len(),
        2,
        "ID <= 2 must cover exactly 2 rows in the unfiltered scan: {expected_id_le_2:?}"
    );
    let id_le_2 = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {} WHERE ID <= 2",
        table_ref("MULTI_PART_STATS")
    ));
    let mut actual_id_le_2: Vec<(i64, Option<String>)> = id_le_2[0]
        .iter()
        .zip(id_le_2[1].iter())
        .map(|(id, value)| (parse_int(id), value.as_str().map(str::to_string)))
        .collect();
    actual_id_le_2.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        actual_id_le_2, expected_id_le_2,
        "the range predicate ID <= 2 must return the same rows pruning inert \
         would: {actual_id_le_2:?}"
    );

    let expected_id_eq_3: Vec<(i64, Option<String>)> = all_stats
        .iter()
        .filter(|(id, _)| *id == 3)
        .cloned()
        .collect();
    assert_eq!(
        expected_id_eq_3.len(),
        1,
        "ID = 3 must cover exactly 1 row in the unfiltered scan: {expected_id_eq_3:?}"
    );
    let id_eq_3 = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {} WHERE ID = 3",
        table_ref("MULTI_PART_STATS")
    ));
    let actual_id_eq_3: Vec<(i64, Option<String>)> = id_eq_3[0]
        .iter()
        .zip(id_eq_3[1].iter())
        .map(|(id, value)| (parse_int(id), value.as_str().map(str::to_string)))
        .collect();
    assert_eq!(
        actual_id_eq_3, expected_id_eq_3,
        "the equality ID = 3 must return the same row pruning inert would: \
         {actual_id_eq_3:?}"
    );

    let no_match_count = conn.query_row_count(&format!(
        "SELECT LETTER, \"NUMBER\" FROM {} WHERE LETTER = 'z'",
        table_ref("BASIC_PARTITIONED")
    ));
    assert_eq!(
        no_match_count, 0,
        "a predicate matching no partition file must return zero rows, not an error"
    );

    let expected_mix: Vec<(i64, Option<String>)> = all_stats
        .iter()
        .filter(|(id, value)| *id == 3 && value.as_deref().is_some_and(|v| v.starts_with("value_")))
        .cloned()
        .collect();
    assert_eq!(
        expected_mix.len(),
        1,
        "the equality-plus-LIKE mix must cover exactly 1 row in the unfiltered \
         scan: {expected_mix:?}"
    );
    let mix = conn.query_columns(&format!(
        "SELECT ID, \"VALUE\" FROM {} WHERE ID = 3 AND \"VALUE\" LIKE 'value_%'",
        table_ref("MULTI_PART_STATS")
    ));
    let actual_mix: Vec<(i64, Option<String>)> = mix[0]
        .iter()
        .zip(mix[1].iter())
        .map(|(id, value)| (parse_int(id), value.as_str().map(str::to_string)))
        .collect();
    assert_eq!(
        actual_mix, expected_mix,
        "the equality-plus-LIKE mix must return exactly what both predicates \
         together select, even though only the equality drove file pruning: \
         {actual_mix:?}"
    );
}

/// Scenario: Pruning reaches every request shape and changes no result end to end
#[test]
fn unity_delta_pruned_pushdown_sql_carries_fewer_files_and_drives_the_scan_udf() {
    setup();
    let mut conn = exa_conn();

    const BASIC_PARTITIONED_ACTIVE_FILES: usize = 6;
    const LETTER_A_ACTIVE_FILES: usize = 2;

    let select_sql = format!(
        "SELECT LETTER, \"NUMBER\" FROM {} WHERE LETTER = 'a'",
        table_ref("BASIC_PARTITIONED")
    );
    let pushed = explain_virtual_sql(&mut conn, &select_sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "the pruned query must drive the scan UDF, not an unaccelerated \
         fallback: {pushed}"
    );

    let mut embedded_files: Vec<&str> = pushed
        .match_indices(".parquet")
        .map(|(dot, extension)| {
            let end = dot + extension.len();
            let start = pushed[..end].rfind('"').map_or(0, |quote| quote + 1);
            &pushed[start..end]
        })
        .collect();
    embedded_files.sort_unstable();
    embedded_files.dedup();
    assert_eq!(
        embedded_files.len(),
        LETTER_A_ACTIVE_FILES,
        "LETTER = 'a' must embed exactly its own {LETTER_A_ACTIVE_FILES} files out of \
         basic_partitioned's {BASIC_PARTITIONED_ACTIVE_FILES} active ones — never zero — \
         gathered across every shard fragment and every echoed copy of the pushed SQL, \
         got {embedded_files:?}: {pushed}"
    );
}

/// Scenario: Partition columns come from the catalog and partition values from the file paths
/// Scenario: A partition column's note carries its position among the partition columns
#[test]
fn unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values() {
    setup();
    let mut conn = exa_conn();

    assert!(
        enumerated_table_names(&mut conn, VS_NAME).contains(&"SALES_PARQUET".to_string()),
        "createVirtualSchema must enumerate 'sales_parquet'"
    );
    assert_eq!(
        declared_types(&mut conn, VS_NAME, "SALES_PARQUET"),
        pairs(&[
            ("ID", "DECIMAL(20,0)"),
            ("AMOUNT", "DOUBLE"),
            ("YEAR", "DECIMAL(10,0)"),
            ("REGION", VARCHAR_JSON),
        ]),
        "Parquet columns must precede partition columns, in declared order"
    );
    assert_eq!(
        partition_positions(&column_adapter_notes(&mut conn, VS_NAME, "SALES_PARQUET")),
        [
            ("AMOUNT", None),
            ("ID", None),
            ("REGION", Some(1)),
            ("YEAR", Some(0))
        ],
        "CREATE records each partition column at its partition_index"
    );

    let cols = conn.query_columns(&format!(
        "SELECT ID, AMOUNT, \"YEAR\", REGION FROM {} ORDER BY ID",
        table_ref("SALES_PARQUET")
    ));
    let rows: Vec<(i64, f64, i64, String)> = (0..cols[0].len())
        .map(|row| {
            (
                parse_int(&cols[0][row]),
                parse_numeric(&cols[1][row]),
                parse_int(&cols[2][row]),
                value_to_string(&cols[3][row]),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, 100.0, 2024, "eu".to_string()),
            (2, 200.0, 2024, "eu".to_string()),
            (3, 300.0, 2024, "us".to_string()),
            (4, 400.0, 2025, "eu".to_string()),
        ],
        "each row must carry its own file's YEAR and REGION directory values"
    );

    let filtered = conn.query_scalar_i64(&format!(
        "SELECT COUNT(*) FROM {} WHERE REGION = 'eu' AND \"YEAR\" = 2024",
        table_ref("SALES_PARQUET")
    ));
    assert_eq!(
        filtered, 2,
        "a partition-column filter must match exactly year=2024/region=eu"
    );
}

/// Scenario: The logical schema is the catalog's declared column list
/// Scenario: Unity Catalog Spark column types map to Exasol types sufficient for listing
/// Scenario: An incompatible Unity Catalog column type is declared as VARCHAR rather than failing
#[test]
fn unity_parquet_all_types_declare_and_return_their_mapped_values() {
    setup();
    let mut conn = exa_conn();
    let timestamp = expected_timestamp_precision(&mut conn).declared_column_type;
    assert_type_matrix(
        &mut conn,
        VS_NAME,
        "ALL_TYPES_PARQUET",
        &[
            reads("ID", "DECIMAL(20,0)", ALL_TYPES_IDS_TEXT),
            reads("C_BYTE", "DECIMAL(3,0)", INT8_VALUES_TEXT),
            reads("C_SHORT", "DECIMAL(5,0)", INT16_VALUES_TEXT),
            reads("C_INT", "DECIMAL(10,0)", INT32_VALUES_TEXT),
            reads("C_FLOAT", "DOUBLE", FLOAT32_VALUES_TEXT),
            reads("C_BOOLEAN", "BOOLEAN", BOOLEAN_VALUES_TEXT),
            reads("C_STRING", VARCHAR_JSON, TEXT_VALUES_TEXT),
            reads("C_DECIMAL_10_2", "DECIMAL(10,2)", DECIMAL_10_2_VALUES_TEXT),
            reads("C_DECIMAL_18_0", "DECIMAL(18,0)", DECIMAL_18_0_VALUES_TEXT),
            reads("C_DECIMAL_36_6", "DECIMAL(36,6)", DECIMAL_36_6_VALUES_TEXT),
            reads("C_DECIMAL_38_10", VARCHAR_JSON, DECIMAL_38_10_VALUES_TEXT),
            reads("C_DATE", "DATE", DATE_VALUES_TEXT),
            reads("C_TIMESTAMP", timestamp, TIMESTAMP_VALUES_TEXT),
            reads("C_TIMESTAMP_NTZ", timestamp, TIMESTAMP_VALUES_TEXT),
            refuses(
                "C_BINARY",
                VARCHAR_JSON,
                &["column 'c_binary'", "type 'binary'", "#351"],
            ),
            reads("C_ARRAY", VARCHAR_JSON, INT_LIST_VALUES_TEXT),
            reads(
                "C_MAP",
                VARCHAR_JSON,
                [Some(r#"{"k1":1,"k2":2}"#), Some("{}"), None],
            ),
            reads(
                "C_STRUCT",
                VARCHAR_JSON,
                [
                    Some(r#"{"a":1,"b":"x"}"#),
                    Some(r#"{"a":2,"b":null}"#),
                    None,
                ],
            ),
            refuses("C_STRUCT_BINARY", VARCHAR_JSON, STRUCT_BINARY_REFUSAL),
            refuses(
                "C_VARIANT",
                VARCHAR_JSON,
                &["column 'c_variant'", "type 'variant'"],
            ),
        ],
    );
    assert_decimal_orders_numerically(&mut conn, VS_NAME, "ALL_TYPES_PARQUET", "C_DECIMAL_18_0");

    let notes = column_adapter_notes(&mut conn, VS_NAME, "ALL_TYPES_PARQUET");
    let refused: Vec<&str> = notes
        .iter()
        .filter(|(_, note)| !note["refused"].is_null())
        .map(|(column, _)| column.as_str())
        .collect();
    assert_eq!(
        refused,
        ["C_BINARY", "C_STRUCT_BINARY", "C_VARIANT"],
        "CREATE records each refusal in its column's note"
    );
}

/// Scenario: Vended and static credentials resolve the same scan
#[test]
fn unity_parquet_planning_agrees_under_vended_and_static_credentials() {
    setup();

    let rt = rt();
    let vended = rt.block_on(resolve_unity_scan("sales_parquet", true, None));
    let static_creds = rt.block_on(resolve_unity_scan("sales_parquet", false, None));

    assert_eq!(vended.table_root, static_creds.table_root);
    assert_eq!(vended.files, static_creds.files);
    assert_eq!(vended.files.len(), 3, "{:?}", vended.files);
}

fn copy_ref(table: &str) -> String {
    format!("{COPY_VS_NAME}.{table}")
}

/// Scenario: Every declared column carries its source shape in its adapterNotes
#[test]
fn unity_delta_and_parquet_columns_persist_source_notes() {
    setup();
    let mut conn = exa_conn();

    for table in ["CM_NAME_MODE", "STATS_ALL_TYPES", "SALES_PARQUET"] {
        let notes = column_adapter_notes(&mut conn, VS_NAME, table);
        assert_eq!(
            notes.len(),
            declared_types(&mut conn, VS_NAME, table).len(),
            "{table}: every column carries a note"
        );
        for (column, note) in &notes {
            assert!(
                note["sourceType"]["arrow_type"].is_string(),
                "{table}.{column}: {note}"
            );
        }
    }
}

/// Scenario: Create virtual schema declares each Delta column from its type_json and the table's column-mapping mode property
#[test]
fn unity_create_declares_delta_columns_from_catalog_metadata() {
    setup();
    let mut conn = exa_conn();
    let name_mode = column_adapter_notes(&mut conn, VS_NAME, "CM_NAME_MODE");
    let id_mode = column_adapter_notes(&mut conn, VS_NAME, "CM_ID_MODE");
    let partitioned = column_adapter_notes(&mut conn, VS_NAME, "BASIC_PARTITIONED");

    let key = |note: &Json| {
        let source_type = &note["sourceType"];
        (
            source_type["field_id"].clone(),
            source_type["physical_name"].clone(),
        )
    };
    assert_eq!(
        key(note_of(&name_mode, "ID")),
        (
            Json::Null,
            json!("col-80396d42-d765-483e-b86e-7ac1e13ef88c")
        ),
        "name mode records the physical name alone"
    );
    assert_eq!(
        key(note_of(&id_mode, "ID")),
        (json!(1), Json::Null),
        "id mode records the field id alone"
    );
    for (column, note) in &partitioned {
        assert_eq!(
            key(note),
            (Json::Null, Json::Null),
            "{column}: no mode, no key"
        );
    }
    assert_eq!(note_of(&partitioned, "LETTER")["partition"], 0);
    assert_eq!(note_of(&partitioned, "NUMBER").get("partition"), None);
    assert_eq!(
        declared_types(&mut conn, VS_NAME, "CM_NAME_MODE"),
        pairs(&[
            ("ID", "DECIMAL(20,0)"),
            ("NAME", "VARCHAR(2000000)"),
            ("VALUE", "DOUBLE"),
        ])
    );
}

/// Scenario: Enumeration names tables through the shared fold and stops at catalog metadata
#[test]
fn unity_create_declares_a_delta_table_whose_location_holds_no_log() {
    setup();
    let mut conn = exa_conn();

    assert_eq!(
        declared_types(&mut conn, COPY_VS_NAME, "NO_LOG"),
        pairs(&[
            ("LETTER", "VARCHAR(2000000)"),
            ("NUMBER", "DECIMAL(20,0)"),
            ("A_FLOAT", "DOUBLE"),
        ]),
        "CREATE declares the table from catalog metadata without reading a log"
    );
    assert_eq!(
        note_of(
            &column_adapter_notes(&mut conn, COPY_VS_NAME, "NO_LOG"),
            "LETTER"
        )["partition"],
        0
    );
    assert_query_fails(
        &mut conn,
        &format!("SELECT \"NUMBER\" FROM {}", copy_ref("NO_LOG")),
        &["no-log-here"],
    );
}

/// Scenario: A Delta column whose column-mapping metadata is unusable is refused in its note and the table stays listed
#[test]
fn unity_unusable_column_mapping_metadata_refuses_only_its_column() {
    setup();
    let mut conn = exa_conn();
    let unannotated = "VALUE_WITHOUT_PHYSICAL_NAME";

    let tables = enumerated_table_names(&mut conn, COPY_VS_NAME);
    for table in [unannotated, "LABEL_MODE"] {
        assert!(
            tables.iter().any(|t| t == table),
            "{table} is listed: {tables:?}"
        );
    }

    let rows = conn.query_columns(&format!("SELECT ID, NAME FROM {}", copy_ref(unannotated)));
    assert_eq!(
        rows[0].len(),
        3,
        "the other columns stay queryable: {rows:?}"
    );
    let refused = &["'value'", "Name-mode", "delta.columnMapping.physicalName"];
    assert_query_fails(
        &mut conn,
        &format!("SELECT \"VALUE\" FROM {}", copy_ref(unannotated)),
        refused,
    );
    let label = query_error(
        &mut conn,
        &format!("SELECT ID FROM {}", copy_ref("LABEL_MODE")),
    );
    assert!(
        label.contains("'label'") && label.contains("'id'"),
        "{label}"
    );
    assert!(!label.contains(&local_stack_creds().secret_key), "{label}");
}

/// Scenario: The client carries each table's properties verbatim from the list sweep and the single-table load
#[test]
fn unity_session_carries_table_properties_from_list_and_load() {
    setup();
    let session = UnityCatalogSession::new(&unity_catalog_url(), delta_creds(false));
    let namespace = ["unity".to_string(), "delta_e2e".to_string()];
    let expected = BTreeMap::from([
        ("delta.columnMapping.mode".to_string(), "name".to_string()),
        (
            "delta.columnMapping.maxColumnId".to_string(),
            "3".to_string(),
        ),
        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
    ]);

    let rt = rt();
    let listing = rt
        .block_on(session.list_tables(&namespace))
        .expect("the namespace lists");
    let loaded = rt
        .block_on(session.load_table(&delta_e2e_table("cm_name_mode")))
        .expect("the table loads");

    let listed = |name: &str| {
        let table = listing.tables.iter().find(|table| table.ident.name == name);
        table
            .unwrap_or_else(|| panic!("{name} is listed"))
            .properties
            .clone()
    };
    assert_eq!(listed("cm_name_mode"), expected);
    assert_eq!(loaded.properties, expected);
    assert_eq!(listed("sales_parquet"), BTreeMap::new());
}

/// Scenario: A declaration that disagrees with the Delta log fails the query
#[test]
fn unity_delta_query_fails_when_the_catalog_copy_disagrees_with_the_log() {
    setup();
    let mut conn = exa_conn();

    for (table, column, part) in [
        ("NO_MODE", "ID", "column-mapping mode is 'name'"),
        (
            "NO_PARTITION_INDEX",
            "NUMBER",
            r#"partitions by ["letter"]"#,
        ),
    ] {
        assert_query_fails(
            &mut conn,
            &format!("SELECT \"{column}\" FROM {}", copy_ref(table)),
            &[table, part, "type_json", "ALTER VIRTUAL SCHEMA ... REFRESH"],
        );
    }
}

/// Scenario: A declared non-nullable column that the Delta log marks nullable fails the query
#[test]
fn unity_delta_non_nullable_declaration_of_a_nullable_column_fails() {
    setup();
    let mut conn = exa_conn();
    let table = copy_ref("ID_NOT_NULL");

    for sql in [
        format!("SELECT ID FROM {table} WHERE ID IS NULL"),
        format!("SELECT NAME FROM {table}"),
    ] {
        assert_query_fails(
            &mut conn,
            &sql,
            &[
                "'ID'",
                "ID_NOT_NULL",
                "allows NULL",
                "ALTER VIRTUAL SCHEMA ... REFRESH",
            ],
        );
    }
}

/// Scenario: Every recorded Delta type change is validated, and an unsupported one refuses its column
#[test]
fn unity_unsupported_type_change_in_type_json_refuses_its_column() {
    setup();
    let mut conn = exa_conn();

    let notes = column_adapter_notes(&mut conn, VS_NAME, "TYPE_WIDENING");
    let refusal = note_of(&notes, "BYTE_DECIMAL")["refused"].to_string();
    for fragment in [
        "byte_decimal",
        "'byte'",
        "'decimal(4,1)'",
        "delta.typeChanges",
    ] {
        assert!(
            refusal.contains(fragment),
            "'{fragment}' missing: {refusal}"
        );
    }
    assert_eq!(note_of(&notes, "BYTE_LONG").get("refused"), None);
    assert_query_fails(
        &mut conn,
        &format!("SELECT BYTE_DECIMAL FROM {}", table_ref("TYPE_WIDENING")),
        &["byte_decimal", "'byte'", "'decimal(4,1)'"],
    );
}

/// Scenario: Delta pruning keeps every file with a matching row for every filter shape
#[test]
fn delta_pruning_fixture_files_hold_the_row_groups_pages_and_statistics_the_cases_need() {
    setup();

    let log_uri = delta_first_commit_uri(PRUNING_DELTA_LOCATION);
    let log = get_fixture_object(&log_uri);
    let adds: Vec<Json> = std::str::from_utf8(&log)
        .unwrap_or_else(|e| panic!("{log_uri} is not UTF-8: {e}"))
        .lines()
        .map(|line| {
            serde_json::from_str::<Json>(line)
                .unwrap_or_else(|e| panic!("{log_uri} holds a line that is not JSON ({e}): {line}"))
        })
        .filter_map(|action| action.get("add").cloned())
        .collect();
    let uris: Vec<String> = adds
        .iter()
        .map(|add| {
            let path = add["path"]
                .as_str()
                .unwrap_or_else(|| panic!("an add action in {log_uri} has no path: {add}"));
            format!("{PRUNING_DELTA_LOCATION}/{path}")
        })
        .collect();

    assert_one_shaped_file_per_label("Delta", &uris);
    for (add, uri) in adds.iter().zip(&uris) {
        assert_add_describes_its_data_file(add, uri);
    }
}

/// The log's values come from the fixture rows and the footer's from the Parquet writer, so
/// equal values cross-check two independent derivations.
fn assert_add_describes_its_data_file(add: &Json, uri: &str) {
    let cannot = format!(
        "the Delta log disagrees with its data file {uri}, so the fixture cannot prove Delta pruning"
    );
    let label = labels_named(uri)
        .pop_first()
        .unwrap_or_else(|| panic!("{uri} names no fixture label: {cannot}"));
    let file = PRUNING_FILES
        .iter()
        .find(|file| file.label == label)
        .unwrap_or_else(|| panic!("no fixture file is labelled {label}: {cannot}"));
    assert_eq!(
        add["partitionValues"][PRUNING_PARTITION_COLUMN],
        json!(file.partition),
        "partitionValues.{PRUNING_PARTITION_COLUMN}: {cannot}"
    );
    assert_eq!(
        add["size"],
        json!(get_fixture_object(uri).len()),
        "size: {cannot}"
    );

    let stats: Json = add["stats"]
        .as_str()
        .and_then(|stats| serde_json::from_str(stats).ok())
        .unwrap_or_else(|| panic!("stats is not a JSON string: {cannot}"));
    let footer = read_footer(uri);
    assert_eq!(
        stats["numRecords"],
        json!(footer.file_metadata().num_rows()),
        "numRecords: {cannot}"
    );
    let (min, max, null_count) = footer_k_statistics(&footer, &cannot);
    assert_eq!(stats["minValues"]["k"], json!(min), "minValues.k: {cannot}");
    assert_eq!(stats["maxValues"]["k"], json!(max), "maxValues.k: {cannot}");
    assert_eq!(
        stats["nullCount"]["k"],
        json!(null_count),
        "nullCount.k: {cannot}"
    );
}

/// The minimum, the maximum, and the summed null count of `k` over the row-group statistics.
fn footer_k_statistics(footer: &ParquetMetaData, cannot: &str) -> (Option<i64>, Option<i64>, u64) {
    let k = footer
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == "k")
        .unwrap_or_else(|| panic!("the data file has no `k` column: {cannot}"));
    let mut min = None;
    let mut max = None;
    let mut null_count = 0;
    for row_group in footer.row_groups() {
        let Some(Statistics::Int64(k_statistics)) = row_group.column(k).statistics() else {
            panic!("a row group has no INT64 `k` statistics: {cannot}");
        };
        min = min.into_iter().chain(k_statistics.min_opt().copied()).min();
        max = max.into_iter().chain(k_statistics.max_opt().copied()).max();
        null_count += k_statistics
            .null_count_opt()
            .unwrap_or_else(|| panic!("a row group has no `k` null count: {cannot}"));
    }
    (min, max, null_count)
}

/// Scenario: Delta pruning keeps every file with a matching row for every filter shape
#[test]
fn delta_pruning_keeps_every_file_with_a_matching_row() {
    setup();
    assert_cases_hold(&mut exa_conn(), Format::Delta);
}

/// Scenario: Aggregate without GROUP BY with all files pruned returns the native empty-set result
/// Scenario: A single-group request that does not decompose routes to the qualified wrapper
/// Scenario: Single-group HAVING is applied on every path
/// Scenario: A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list
#[test]
fn delta_single_group_aggregates_match_the_oracle() {
    setup();
    assert_single_group_aggregates_match_oracle(&mut exa_conn(), Format::Delta);
}
