use crate::common::e2e_harness::{
    ADAPTER_SCRIPT_NAME, SCAN_SCRIPT_NAME, VsProps, create_proxied_schema_and_scripts,
    create_schema_and_scripts, create_schema_and_scripts_in, create_virtual_schema,
    create_virtual_schema_with_password, exa_conn, explain_virtual_sql, has_broadcast_join_block,
    has_n_scan_wrapper, has_two_scan_wrapper, install_slc, runtime, upload_so,
};
use crate::common::exasol_ws::ExaConn;
use crate::common::raw_parquet::write_parquet_fixture;
use crate::common::seed::{
    E2E_CHAR_PAD_TABLE, E2E_DIM_TABLE, E2E_FACT_TABLE, E2E_JSON_TABLE, E2E_LINEITEM_TABLE,
    E2E_NAMESPACE, E2E_TABLE, E2E_TYPED_TABLE, build_seed_catalog, seed_all_types,
    seed_complex_types_probe, seed_distinct_probe, seed_events, seed_high_card_probe,
    seed_ident_part_bytes, seed_json_docs, seed_typed_distinct_probe,
};
use crate::common::stack::{
    ASSUME_ROLE_ARN, ASSUME_ROLE_BASE_ACCESS_KEY, ASSUME_ROLE_BASE_SECRET_KEY,
    ASSUME_ROLE_EXTERNAL_ID, CatalogConnectionPassword, ICEBERG_REST_VIA_PROXY,
    PROXY_ADAPTER_SCHEMA, PROXY_URL_INTERNAL, SEAWEEDFS_VIA_PROXY, iceberg_catalog_url,
    local_stack_connection_password, wait_for_exasol, wait_for_iceberg_catalog, wait_for_seaweedfs,
};
use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use std::sync::{Arc, OnceLock};

pub const LAKEHOUSE_VS: &str = "MY_LAKEHOUSE";

pub fn stack() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_seaweedfs();
        wait_for_iceberg_catalog();
        install_slc();
        upload_so();
        create_schema_and_scripts(&mut exa_conn());
    });
}

const WAREHOUSE: &str = "s3://warehouse/";

fn seed<T>(what: &str, seeding: impl Future<Output = anyhow::Result<T>>) {
    runtime()
        .block_on(seeding)
        .unwrap_or_else(|e| panic!("seed {what}: {e:?}"));
}

pub fn events() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| seed("events", seed_events(&iceberg_catalog_url(), WAREHOUSE)));
}

pub fn typed_probe() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        seed(
            "typed_distinct_probe",
            seed_typed_distinct_probe(&iceberg_catalog_url(), WAREHOUSE),
        )
    });
}

pub fn complex_probe() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        seed(
            "complex_probe",
            seed_complex_types_probe(&iceberg_catalog_url(), WAREHOUSE),
        )
    });
}

/// Seeds every table its modules query first, because CREATE enumerates the namespace once.
pub fn lakehouse_vs() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        stack();
        events();
        typed_probe();
        let url = iceberg_catalog_url();
        seed("all_types", seed_all_types(&url, WAREHOUSE));
        seed("distinct_probe", seed_distinct_probe(&url, WAREHOUSE));
        seed("high_card_probe", seed_high_card_probe(&url, WAREHOUSE));
        seed("ident_part_bytes", seed_ident_part_bytes(&url, WAREHOUSE));
        seed("json_docs", seed_json_docs(&url, WAREHOUSE));
        create_virtual_schema(&mut exa_conn(), &VsProps::new(LAKEHOUSE_VS, E2E_NAMESPACE));
    });
}

/// The adapter schema of a virtual schema whose properties [`persist_properties`] sets.
pub const STUB_ADAPTER_SCHEMA: &str = "PV_STUB_ADAPTER";

/// Persists whatever `ALTER ... SET` names, as an adapter that predates the check did.
const PERSISTING_ADAPTER_BODY: &str = r#"import json


def adapter_call(request_json):
    kind = json.loads(request_json)["type"]
    if kind == "getCapabilities":
        return json.dumps({"type": kind, "capabilities": []})
    if kind == "setProperties":
        return json.dumps({"type": kind})
    raise Exception("the stub adapter answers only setProperties, got " + kind)
"#;

/// Swaps the stub in, runs `ALTER VIRTUAL SCHEMA <vs> SET <set_clause>`, and puts the engine
/// adapter back, so the properties persist without the engine's property check. `vs` must be
/// created `USING` [`STUB_ADAPTER_SCHEMA`], whose scripts must already exist.
pub fn persist_properties(conn: &mut ExaConn, vs: &str, set_clause: &str) {
    conn.execute(&format!(
        "CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT \
         {STUB_ADAPTER_SCHEMA}.{ADAPTER_SCRIPT_NAME} AS\n{PERSISTING_ADAPTER_BODY}\n/"
    ));
    conn.execute(&format!("ALTER VIRTUAL SCHEMA {vs} SET {set_clause}"));
    create_schema_and_scripts_in(conn, STUB_ADAPTER_SCHEMA);
}

pub fn vs_table(vs_name: &str, table: &str) -> String {
    format!("{vs_name}.{}", table.to_uppercase())
}

pub fn events_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_TABLE)
}

/// Every column of `TYPED_DISTINCT_PROBE` after `ID`, in declared order.
pub const TYPED_COLUMNS: &str =
    "C_DECIMAL_A, C_DECIMAL_B, C_DOUBLE, C_VARCHAR, C_DATE, C_TS, C_BOOL, C_PRICE, C_QTY";

pub fn json_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_JSON_TABLE)
}

pub fn typed_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_TYPED_TABLE)
}

pub fn vs_char_pad_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_CHAR_PAD_TABLE)
}

pub fn vs_dim_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_DIM_TABLE)
}

pub fn vs_fact_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_FACT_TABLE)
}

pub fn vs_lineitem_table() -> String {
    vs_table(LAKEHOUSE_VS, E2E_LINEITEM_TABLE)
}

/// Holds native copies of the fact and dimension tables under their virtual names, so a join
/// query that names its schema `{T}` runs over either.
pub const JOIN_ORACLE: &str = "JOIN_ORACLE";

/// Copies the fact and dimension tables of `source_vs` into [`JOIN_ORACLE`].
pub fn copy_join_tables_natively(conn: &mut ExaConn, source_vs: &str) {
    conn.execute(&format!("CREATE SCHEMA IF NOT EXISTS {JOIN_ORACLE}"));
    for table in [E2E_FACT_TABLE, E2E_DIM_TABLE] {
        conn.execute(&format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM {}",
            vs_table(JOIN_ORACLE, table),
            vs_table(source_vs, table)
        ));
    }
}

/// `PUSHDOWN_SQL` only: the echoed request JSON repeats the user's select list
/// and would satisfy a naive substring probe.
pub fn explain_virtual_pushdown_sql(conn: &mut ExaConn, query_sql: &str) -> String {
    let resp = conn.execute(&format!("EXPLAIN VIRTUAL {query_sql}"));
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let cols = conn.fetch_result_columns(result_set);
    cols[1]
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The messages each pushdown of `query` echoes in the `PUSHDOWN_JSON` of `EXPLAIN VIRTUAL`.
pub fn explain_virtual_messages(conn: &mut ExaConn, query: &str) -> Vec<Vec<serde_json::Value>> {
    const PUSHDOWN_JSON_COLUMN: usize = 2;
    let columns = conn.query_columns(&format!("EXPLAIN VIRTUAL {query}"));
    columns[PUSHDOWN_JSON_COLUMN]
        .iter()
        .map(|cell| {
            cell.as_str()
                .and_then(|text| serde_json::from_str(text).ok())
                .unwrap_or_else(|| panic!("PUSHDOWN_JSON of {query} must be a JSON array: {cell}"))
        })
        .collect()
}

/// The `pushdownRequest` of each pushdown that `EXPLAIN VIRTUAL` echoes for `query`.
pub fn echoed_pushdown_requests(conn: &mut ExaConn, query: &str) -> Vec<serde_json::Value> {
    explain_virtual_messages(conn, query)
        .iter()
        .filter_map(|messages| messages.iter().find_map(|m| m.get("pushdownRequest")))
        .cloned()
        .collect()
}

pub fn assert_pushed_to_scan_udf(conn: &mut ExaConn, sql: &str, shape: &str) {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME),
        "{shape} must drive the scan UDF: {pushed}"
    );
}

/// Exasol computes the query in the qualified single-table wrapper over the sharded scan, and
/// returns that pushdown SQL.
pub fn assert_single_table_wrapper(conn: &mut ExaConn, sql: &str) -> String {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        pushed.contains(SCAN_SCRIPT_NAME) && has_n_scan_wrapper(&pushed, 1),
        "{sql} must run in the qualified single-table wrapper over the scan: {pushed}"
    );
    pushed
}

/// Exasol computes the join in the N-scan wrapper, not in a broadcast scan.
pub fn assert_two_scan_join_wrapper(conn: &mut ExaConn, sql: &str) {
    let pushed = explain_virtual_sql(conn, sql);
    assert!(
        has_two_scan_wrapper(&pushed) && !has_broadcast_join_block(&pushed),
        "{sql} must take the N-scan join wrapper, not the broadcast join: {pushed}"
    );
}

pub fn emits_clause(pushed_sql: &str) -> String {
    let marker = "EMITS (";
    let start = pushed_sql
        .find(marker)
        .unwrap_or_else(|| panic!("no EMITS clause in pushed SQL:\n{pushed_sql}"))
        + marker.len();
    let mut depth = 1i32;
    let mut end = pushed_sql.len();
    for (i, c) in pushed_sql[start..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    pushed_sql[start..end].to_string()
}

pub const BASE_DIRECT: &str = "s3://warehouse/proxy_direct/";
pub const PROXY_ICEBERG_VS: &str = "PROXY_ICEBERG_VS";
pub const PROXY_DIRECT_VS: &str = "PROXY_DIRECT_VS";
pub const PROXY_ROLE_VS: &str = "PROXY_ROLE_VS";

pub fn setup_proxy(adapter_schema: &str, adapter_env: &[(&str, &str)], scan_env: &[(&str, &str)]) {
    lakehouse_vs();
    events();
    create_proxied_schema_and_scripts(
        &mut exa_conn(),
        adapter_schema,
        adapter_env,
        scan_env,
        &[ICEBERG_REST_VIA_PROXY, SEAWEEDFS_VIA_PROXY],
    );
}

/// A CONNECTION that names a role whose STS endpoint only the proxy resolves.
pub fn role_password_via_proxy() -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        access_key: ASSUME_ROLE_BASE_ACCESS_KEY.to_string(),
        secret_key: ASSUME_ROLE_BASE_SECRET_KEY.to_string(),
        aws_assume_role_arn: Some(ASSUME_ROLE_ARN.to_string()),
        aws_external_id: Some(ASSUME_ROLE_EXTERNAL_ID.to_string()),
        aws_sts_endpoint: Some(SEAWEEDFS_VIA_PROXY.to_string()),
        ..local_stack_connection_password().via_proxy()
    }
}

pub fn count(sql: &str) -> i64 {
    exa_conn().query_scalar_i64(sql)
}

pub fn refresh(vs_name: &str) {
    exa_conn().execute(&format!("ALTER VIRTUAL SCHEMA {vs_name} REFRESH"));
}

/// Installs the proxy-only scripts once, then creates the Iceberg REST, direct-storage, and
/// assume-role virtual schemas that reach their catalog and storage only through the proxy.
pub fn proxy_virtual_schemas() {
    static DONE: OnceLock<()> = OnceLock::new();
    DONE.get_or_init(|| {
        let proxy = [("HTTP_PROXY", PROXY_URL_INTERNAL)];
        setup_proxy(PROXY_ADAPTER_SCHEMA, &proxy, &proxy);
        write_parquet_fixture(
            &format!("{BASE_DIRECT}rows/file1.parquet"),
            RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![Field::new(
                    "id",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .expect("build direct-storage fixture batch"),
        );
        let mut conn = exa_conn();

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(PROXY_ICEBERG_VS, E2E_NAMESPACE).proxied("PROXY_ICEBERG_CREDS"),
            ICEBERG_REST_VIA_PROXY,
            &local_stack_connection_password().via_proxy(),
        );
        refresh(PROXY_ICEBERG_VS);

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(PROXY_DIRECT_VS, "")
                .proxied("PROXY_DIRECT_CREDS")
                .with_catalog_kind("DIRECT_STORAGE"),
            BASE_DIRECT,
            &CatalogConnectionPassword {
                warehouse: String::new(),
                ..local_stack_connection_password().via_proxy()
            },
        );
        refresh(PROXY_DIRECT_VS);

        create_virtual_schema_with_password(
            &mut conn,
            &VsProps::new(PROXY_ROLE_VS, E2E_NAMESPACE).proxied("PROXY_ROLE_CREDS"),
            ICEBERG_REST_VIA_PROXY,
            &role_password_via_proxy(),
        );
        refresh(PROXY_ROLE_VS);
    });
}

pub fn seed_catalog(rt: &tokio::runtime::Runtime, label: &str) -> RestCatalog {
    rt.block_on(build_seed_catalog(
        &iceberg_catalog_url(),
        "s3://warehouse/",
        label,
    ))
    .expect("build seed catalog")
}

pub fn id_val_schema(schema_id: i32) -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "val", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .expect("build id+val Iceberg schema")
}

pub fn id_val_batch(id: i64, val: f64) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Float64Array::from(vec![val])),
        ],
    )
    .expect("id+val RecordBatch construction is infallible")
}

pub async fn ensure_id_val_table(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
    id: i64,
    val: f64,
) {
    crate::common::seed::create_and_append(
        catalog,
        namespace,
        table_name,
        id_val_schema(0),
        std::iter::once(id_val_batch(id, val)),
    )
    .await
    .unwrap_or_else(|e| panic!("seed {namespace}.{table_name}: {e}"));
}

/// The Docker warehouse persists across `make test-e2e` runs, so a prior run's tables can leak in.
pub async fn drop_table_if_exists(catalog: &impl Catalog, ns: &NamespaceIdent, table_name: &str) {
    let ident = TableIdent::new(ns.clone(), table_name.to_string());
    if catalog
        .table_exists(&ident)
        .await
        .unwrap_or_else(|e| panic!("check table {}.{table_name}: {e}", ns.join(".")))
    {
        catalog
            .drop_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("drop stale table {}.{table_name}: {e}", ns.join(".")));
    }
}

pub async fn load(catalog: &impl Catalog, namespace: &str, table: &str) -> iceberg::table::Table {
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table.to_string(),
    );
    catalog
        .load_table(&ident)
        .await
        .unwrap_or_else(|e| panic!("load {namespace}.{table}: {e}"))
}

/// Writes one data file under the table's current schema.
pub async fn append_file(catalog: &impl Catalog, namespace: &str, table: &str, batch: RecordBatch) {
    let loaded = load(catalog, namespace, table).await;
    crate::common::seed::write_one_file_append(catalog, &loaded, table, [batch])
        .await
        .unwrap_or_else(|e| panic!("append to {namespace}.{table}: {e}"));
}
