//! Every test uses its own Iceberg namespace and Virtual Schema: these scenarios mutate the
//! catalog out of band, and doing so in the shared `e2e_lakehouse` namespace would break every
//! other E2E binary's `createVirtualSchema` for the rest of the Docker session.
#![cfg(feature = "exasol-e2e")]

mod common;
use common::e2e_harness::*;
use common::exasol_ws::ExaConn;
use common::seed::{build_seed_catalog, rest_replace_current_schema};
use common::stack::{
    CatalogConnectionPassword, build_create_connection_sql, iceberg_catalog_url,
    iceberg_catalog_url_internal, local_stack_connection_password, wait_for_exasol,
    wait_for_iceberg_catalog, wait_for_minio,
};

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema as IcebergSchema, Type, UnboundPartitionSpec,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// Distinct from other binaries' connection so the unreachable-catalog test's
/// `CREATE OR REPLACE CONNECTION` cannot race with them.
const CATALOG_CONN_NAME: &str = "REFRESH_CATALOG_CREDS";

const NS_REENUM: &str = "e2e_refresh_reenum";
const NS_COLCHANGE: &str = "e2e_refresh_colchange";
const NS_SETPROPS_A: &str = "e2e_refresh_setprops_a";
const NS_SETPROPS_B: &str = "e2e_refresh_setprops_b";
const NS_UNREACHABLE: &str = "e2e_refresh_unreachable";
const NS_PARTIAL: &str = "e2e_refresh_partial";
const NS_COLLISION: &str = "e2e_refresh_collision";

static SETUP_DONE: OnceLock<()> = OnceLock::new();

fn setup_e2e() {
    SETUP_DONE.get_or_init(|| {
        wait_for_exasol();
        wait_for_minio();
        wait_for_iceberg_catalog();

        install_slc();
        upload_so();

        let mut conn = exa_conn();
        create_schema_and_scripts(&mut conn);
    });
}

fn create_virtual_schema(conn: &mut ExaConn, vs_name: &str, namespace: &str) {
    common::e2e_harness::create_virtual_schema(
        conn,
        &VsProps::new(vs_name, namespace).with_catalog_conn_name(CATALOG_CONN_NAME),
    );
}

fn vs_table(vs_name: &str, table_name: &str) -> String {
    format!("{vs_name}.{}", table_name.to_uppercase())
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn id_val_schema(schema_id: i32) -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "val", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .expect("build id+val Iceberg schema")
}

fn id_val_schema_with_new_col(schema_id: i32) -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "val", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(3, "new_col", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()
        .expect("build id+val+new_col Iceberg schema")
}

fn id_val_batch(id: i64, val: f64) -> RecordBatch {
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

async fn ensure_id_val_table(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
    id: i64,
    val: f64,
) {
    common::seed::create_and_append(
        catalog,
        namespace,
        table_name,
        id_val_schema(0),
        std::iter::once(id_val_batch(id, val)),
    )
    .await
    .unwrap_or_else(|e| panic!("seed {namespace}.{table_name}: {e}"));
}

/// Existing rows project `new_col` as NULL: no `initial-default` is set.
async fn add_new_col(catalog: &impl Catalog, namespace: &str, table_name: &str) {
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    );
    let table = catalog
        .load_table(&ident)
        .await
        .unwrap_or_else(|e| panic!("load {namespace}.{table_name} before column add: {e}"));
    let current_schema_id = table.metadata().current_schema_id();
    rest_replace_current_schema(
        &iceberg_catalog_url(),
        namespace,
        table_name,
        current_schema_id,
        id_val_schema_with_new_col(current_schema_id + 1),
    )
    .await
    .unwrap_or_else(|e| panic!("add new_col to {namespace}.{table_name}: {e}"));
}

/// Collision detection runs before any per-table resolution, so no data file is needed.
async fn create_empty_table(catalog: &impl Catalog, ns: &NamespaceIdent, table_name: &str) {
    if !catalog
        .namespace_exists(ns)
        .await
        .unwrap_or_else(|e| panic!("check namespace {}: {e}", ns.join(".")))
    {
        let _ = catalog.create_namespace(ns, HashMap::new()).await;
    }
    let ident = TableIdent::new(ns.clone(), table_name.to_string());
    if catalog
        .table_exists(&ident)
        .await
        .unwrap_or_else(|e| panic!("check table {}.{}: {e}", ns.join("."), table_name))
    {
        return;
    }
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(id_val_schema(0))
        .partition_spec(UnboundPartitionSpec::builder().with_spec_id(0).build())
        .properties(HashMap::new())
        .build();
    let _ = catalog.create_table(ns, creation).await;
}

/// The Docker warehouse persists across `make test-e2e` runs, so a prior run's tables can leak in.
async fn drop_table_if_exists(catalog: &impl Catalog, ns: &NamespaceIdent, table_name: &str) {
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

/// Scenario: a table added after CREATE becomes queryable only after REFRESH (#147)
#[test]
fn refresh_reenumerates_namespace() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-reenum",
        ))
        .expect("build seed catalog");

    let reenum_ns = NamespaceIdent::new(NS_REENUM.to_string());
    rt.block_on(drop_table_if_exists(&catalog, &reenum_ns, "t_orig"));
    rt.block_on(drop_table_if_exists(&catalog, &reenum_ns, "t_new"));

    rt.block_on(ensure_id_val_table(&catalog, NS_REENUM, "t_orig", 1, 10.0));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_REENUM_VS", NS_REENUM);

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_REENUM_VS", "t_orig")
    ));
    assert_eq!(id, 1, "T_ORIG must be queryable right after CREATE");

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_REENUM_VS", "t_new")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "T_NEW must be unknown before it exists in the catalog and before REFRESH: {resp}"
    );

    rt.block_on(ensure_id_val_table(&catalog, NS_REENUM, "t_new", 42, 99.0));

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_REENUM_VS REFRESH");

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_REENUM_VS", "t_new")
    ));
    assert_eq!(
        id, 42,
        "T_NEW must be queryable after REFRESH re-enumerates the namespace"
    );
}

/// Scenario: a column added after CREATE becomes selectable (NULL for existing rows) after REFRESH
#[test]
fn refresh_reflects_added_table_and_column_change() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-colchange",
        ))
        .expect("build seed catalog");

    rt.block_on(ensure_id_val_table(&catalog, NS_COLCHANGE, "evt", 1, 10.0));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_COLCHANGE_VS", NS_COLCHANGE);

    let val = conn.query_columns(&format!(
        "SELECT val FROM {}",
        vs_table("REFRESH_COLCHANGE_VS", "evt")
    ));
    assert_eq!(val[0].len(), 1, "EVT must be queryable right after CREATE");

    let resp = conn.try_execute(&format!(
        "SELECT new_col FROM {}",
        vs_table("REFRESH_COLCHANGE_VS", "evt")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "NEW_COL must be unknown to Exasol before REFRESH: {resp}"
    );

    rt.block_on(add_new_col(&catalog, NS_COLCHANGE, "evt"));

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_COLCHANGE_VS REFRESH");

    let cols = conn.query_columns(&format!(
        "SELECT id, new_col FROM {}",
        vs_table("REFRESH_COLCHANGE_VS", "evt")
    ));
    assert_eq!(
        cols[0].len(),
        1,
        "expected exactly the one pre-existing row after REFRESH: {cols:?}"
    );
    assert!(
        cols[1][0].is_null(),
        "NEW_COL for the pre-existing row must be NULL (absent from the data \
         file, no initial-default): {cols:?}"
    );
}

/// Scenario: SET NAMESPACE re-targets the VS and fully rebuilds the table map
#[test]
fn set_properties_retargets_namespace() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-setprops",
        ))
        .expect("build seed catalog");

    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_SETPROPS_A,
        "a_tbl",
        100,
        1.0,
    ));
    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_SETPROPS_B,
        "b_tbl",
        200,
        2.0,
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_SETPROPS_VS", NS_SETPROPS_A);

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "a_tbl")
    ));
    assert_eq!(
        id, 100,
        "A_TBL must be queryable under the original namespace"
    );

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "b_tbl")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "B_TBL must be unknown while the VS targets the A namespace: {resp}"
    );

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA REFRESH_SETPROPS_VS SET NAMESPACE='{NS_SETPROPS_B}'"
    ));

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "b_tbl")
    ));
    assert_eq!(
        id, 200,
        "B_TBL must be queryable after setProperties re-targets the namespace"
    );

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_SETPROPS_VS", "a_tbl")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "A_TBL must no longer be registered — TABLE_MAP is a full rebuild from \
         the newly targeted namespace, never a merge of both: {resp}"
    );
}

/// Scenario: REFRESH against an unreachable catalog errors without leaking credentials
#[test]
fn refresh_unreachable_catalog_redacts_credentials() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-unreachable",
        ))
        .expect("build seed catalog");
    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_UNREACHABLE,
        "u_tbl",
        1,
        1.0,
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_UNREACHABLE_VS", NS_UNREACHABLE);

    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table("REFRESH_UNREACHABLE_VS", "u_tbl")
    ));
    assert_eq!(
        id, 1,
        "U_TBL must be queryable before the connection is broken"
    );

    let bogus_password = CatalogConnectionPassword {
        warehouse: "s3://warehouse/".to_string(),
        endpoint: "http://does-not-exist.invalid:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "SUPER_SECRET_KEY".to_string(),
        secret_key: "SUPER_SECRET_VALUE".to_string(),
        session_token: None,
        path_style: true,
        use_sigv4: false,
        use_vended_credentials: false,
        ..Default::default()
    };
    let bogus_uri = "http://does-not-exist.invalid:8181";
    let replace_conn_sql =
        build_create_connection_sql(CATALOG_CONN_NAME, bogus_uri, &bogus_password);
    conn.execute(&replace_conn_sql);

    let resp = conn.try_execute("ALTER VIRTUAL SCHEMA REFRESH_UNREACHABLE_VS REFRESH");
    assert_eq!(
        resp["status"].as_str(),
        Some("error"),
        "expected an error when REFRESH hits an unreachable catalog: {resp}"
    );
    let msg = resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        !msg.contains("SUPER_SECRET_KEY") && !msg.contains("SUPER_SECRET_VALUE"),
        "refresh error message must not leak credentials: {msg}"
    );

    // Restore so later runs are not left with a broken connection.
    let create_conn_sql = build_create_connection_sql(
        CATALOG_CONN_NAME,
        &iceberg_catalog_url_internal(),
        &local_stack_connection_password(),
    );
    conn.execute(&create_conn_sql);
}

/// Scenario: REFRESH TABLES naming one table still refreshes the whole namespace
#[test]
fn refresh_partial_requested_tables_still_refreshes_whole_namespace() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-partial",
        ))
        .expect("build seed catalog");

    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_PARTIAL,
        "table_one",
        1,
        10.0,
    ));
    rt.block_on(ensure_id_val_table(
        &catalog,
        NS_PARTIAL,
        "table_two",
        2,
        20.0,
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_PARTIAL_VS", NS_PARTIAL);

    for table in ["table_one", "table_two"] {
        let resp = conn.try_execute(&format!(
            "SELECT new_col FROM {}",
            vs_table("REFRESH_PARTIAL_VS", table)
        ));
        assert_eq!(
            resp["status"].as_str(),
            Some("error"),
            "{table} must not expose NEW_COL before any REFRESH: {resp}"
        );
    }

    rt.block_on(add_new_col(&catalog, NS_PARTIAL, "table_one"));
    rt.block_on(add_new_col(&catalog, NS_PARTIAL, "table_two"));

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_PARTIAL_VS REFRESH TABLES TABLE_ONE");

    let cols = conn.query_columns(&format!(
        "SELECT id, new_col FROM {}",
        vs_table("REFRESH_PARTIAL_VS", "table_one")
    ));
    assert_eq!(
        cols[0].len(),
        1,
        "TABLE_ONE must still return its one row after a partial refresh: {cols:?}"
    );
    assert!(
        cols[1][0].is_null(),
        "TABLE_ONE's NEW_COL must be visible (NULL, no initial-default) after \
         REFRESH TABLES TABLE_ONE: {cols:?}"
    );

    // Exasol does not scope REFRESH TABLES to requestedTables — verified live.
    let cols_two = conn.query_columns(&format!(
        "SELECT id, new_col FROM {}",
        vs_table("REFRESH_PARTIAL_VS", "table_two")
    ));
    assert_eq!(
        cols_two[0].len(),
        1,
        "TABLE_TWO must still return its one row after a refresh naming only \
         TABLE_ONE: {cols_two:?}"
    );
    assert!(
        cols_two[1][0].is_null(),
        "TABLE_TWO's NEW_COL must ALSO be visible after REFRESH TABLES \
         TABLE_ONE — Exasol applies the adapter's full-namespace response to \
         every table regardless of requestedTables, so a partial refresh has \
         the same real-world effect as a full REFRESH: {cols_two:?}"
    );
}

/// Scenario: a flatten-name collision surfaced by REFRESH returns the same error as CREATE
#[test]
fn refresh_flatten_collision_returns_same_error_as_create() {
    setup_e2e();
    let rt = rt();
    let catalog_url = iceberg_catalog_url();
    let catalog = rt
        .block_on(build_seed_catalog(
            &catalog_url,
            "s3://warehouse/",
            "refresh-collision",
        ))
        .expect("build seed catalog");

    let top_ns = NamespaceIdent::new(NS_COLLISION.to_string());
    let eu_ns = NamespaceIdent::from_vec(vec![NS_COLLISION.to_string(), "eu".to_string()])
        .expect("build descendant NamespaceIdent");

    rt.block_on(drop_table_if_exists(&catalog, &top_ns, "baseline"));
    rt.block_on(drop_table_if_exists(&catalog, &top_ns, "eu__orders"));
    rt.block_on(drop_table_if_exists(&catalog, &eu_ns, "orders"));

    rt.block_on(create_empty_table(&catalog, &top_ns, "baseline"));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_COLLISION_VS", NS_COLLISION);

    let resp = conn.try_execute(&format!(
        "SELECT * FROM {}",
        vs_table("REFRESH_COLLISION_VS", "baseline")
    ));
    assert_eq!(
        resp["status"].as_str(),
        Some("ok"),
        "BASELINE must be queryable before the collision is introduced: {resp}"
    );

    // Direct `eu__orders` and descendant `eu.orders` both flatten to `EU__ORDERS`.
    rt.block_on(create_empty_table(&catalog, &top_ns, "eu__orders"));
    rt.block_on(create_empty_table(&catalog, &eu_ns, "orders"));

    let refresh_resp = conn.try_execute("ALTER VIRTUAL SCHEMA REFRESH_COLLISION_VS REFRESH");
    assert_eq!(
        refresh_resp["status"].as_str(),
        Some("error"),
        "REFRESH must error once the flatten-name collision exists: {refresh_resp}"
    );
    let refresh_msg = refresh_resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        refresh_msg.contains("EU__ORDERS"),
        "refresh's collision error must name the colliding Exasol table name: {refresh_msg}"
    );
    assert!(
        refresh_msg.contains("collision"),
        "refresh's collision error must mention 'collision': {refresh_msg}"
    );

    let create_resp = conn.try_execute(&format!(
        r#"CREATE VIRTUAL SCHEMA REFRESH_COLLISION_CREATE_VS
USING {SCHEMA_NAME}.{ADAPTER_SCRIPT_NAME} WITH
  CATALOG_CONNECTION  = '{CATALOG_CONN_NAME}'
  NAMESPACE   = '{NS_COLLISION}'
  ALLOW_HTTP          = 'true'"#
    ));
    assert_eq!(
        create_resp["status"].as_str(),
        Some("error"),
        "a fresh createVirtualSchema over the colliding namespace must also error: {create_resp}"
    );
    let create_msg = create_resp["exception"]["text"].as_str().unwrap_or("");
    assert!(
        create_msg.contains("EU__ORDERS") && create_msg.contains("collision"),
        "createVirtualSchema's collision error must have the same shape as \
         refresh's (same colliding name, same 'collision' wording): {create_msg}"
    );
}
