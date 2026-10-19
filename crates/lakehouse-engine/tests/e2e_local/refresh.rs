//! Every test uses its own Iceberg namespace and Virtual Schema: these scenarios mutate the
//! catalog out of band, and doing so in the shared `e2e_lakehouse` namespace would break every
//! other module's `createVirtualSchema` for the rest of the Docker session.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::common::seed::{build_seed_catalog, rest_replace_current_schema};
use crate::common::stack::{
    CatalogConnectionPassword, build_create_connection_sql, iceberg_catalog_url,
    iceberg_catalog_url_internal, local_stack_connection_password,
};
use crate::fixtures::{runtime, stack, vs_table};

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, Type, UnboundPartitionSpec,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use serde_json::Value as Json;

use std::collections::HashMap;
use std::sync::Arc;

/// Distinct from other modules' connection so the unreachable-catalog test's
/// `CREATE OR REPLACE CONNECTION` cannot race with them.
const CATALOG_CONN_NAME: &str = "REFRESH_CATALOG_CREDS";

const NS_REENUM: &str = "e2e_refresh_reenum";
const NS_COLCHANGE: &str = "e2e_refresh_colchange";
const NS_SETPROPS_A: &str = "e2e_refresh_setprops_a";
const NS_SETPROPS_B: &str = "e2e_refresh_setprops_b";
const NS_UNREACHABLE: &str = "e2e_refresh_unreachable";
const NS_PARTIAL: &str = "e2e_refresh_partial";
const NS_COLLISION: &str = "e2e_refresh_collision";
const NS_NOTES: &str = "e2e_refresh_notes";
const NS_RENOTES: &str = "e2e_refresh_renotes";
const NS_RENAME: &str = "e2e_refresh_rename";
const NS_DROP: &str = "e2e_refresh_drop";
const NS_OPTIONAL: &str = "e2e_refresh_optional";
const NS_SWAP: &str = "e2e_refresh_swap";

fn create_virtual_schema(conn: &mut ExaConn, vs_name: &str, namespace: &str) {
    crate::common::e2e_harness::create_virtual_schema(
        conn,
        &VsProps::new(vs_name, namespace).with_catalog_conn_name(CATALOG_CONN_NAME),
    );
}

fn seed_catalog(rt: &tokio::runtime::Runtime, label: &str) -> RestCatalog {
    rt.block_on(build_seed_catalog(
        &iceberg_catalog_url(),
        "s3://warehouse/",
        label,
    ))
    .expect("build seed catalog")
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

/// Existing rows project `new_col` as NULL: no `initial-default` is set.
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

/// Scenario: Refresh re-enumerates the namespace and returns a refresh response
#[test]
fn refresh_reenumerates_namespace() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-reenum");

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

#[test]
fn refresh_reflects_added_table_and_column_change() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-colchange");

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

    rt.block_on(evolve_schema(
        &catalog,
        NS_COLCHANGE,
        "evt",
        id_val_schema_with_new_col,
    ));

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

#[test]
fn set_properties_retargets_namespace() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-setprops");

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

#[test]
fn refresh_unreachable_catalog_redacts_credentials() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-unreachable");
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

#[test]
fn refresh_partial_requested_tables_still_refreshes_whole_namespace() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-partial");

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

    rt.block_on(evolve_schema(
        &catalog,
        NS_PARTIAL,
        "table_one",
        id_val_schema_with_new_col,
    ));
    rt.block_on(evolve_schema(
        &catalog,
        NS_PARTIAL,
        "table_two",
        id_val_schema_with_new_col,
    ));

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

#[test]
fn refresh_flatten_collision_returns_same_error_as_create() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-collision");

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

fn long_field(id: i32, name: &str, required: bool) -> NestedFieldRef {
    NestedField::new(id, name, Type::Primitive(PrimitiveType::Long), required).into()
}

fn schema_of(schema_id: i32, fields: &[NestedFieldRef]) -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(schema_id)
        .with_fields(fields.to_vec())
        .build()
        .expect("a valid Iceberg schema")
}

/// One data file's rows: `long` columns named and nullable as `fields` declares them.
fn long_batch(fields: &[NestedFieldRef], columns: &[&[Option<i64>]]) -> RecordBatch {
    let schema = ArrowSchema::new(
        fields
            .iter()
            .map(|field| Field::new(field.name.as_str(), DataType::Int64, !field.required))
            .collect::<Vec<_>>(),
    );
    let arrays = columns
        .iter()
        .map(|values| Arc::new(Int64Array::from(values.to_vec())) as ArrayRef)
        .collect();
    RecordBatch::try_new(Arc::new(schema), arrays).expect("a valid long RecordBatch")
}

async fn load(catalog: &impl Catalog, namespace: &str, table: &str) -> iceberg::table::Table {
    let ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table.to_string(),
    );
    catalog
        .load_table(&ident)
        .await
        .unwrap_or_else(|e| panic!("load {namespace}.{table}: {e}"))
}

/// Drops a prior run's table, then creates it with `fields` and one data file per batch.
async fn fresh_table(
    catalog: &impl Catalog,
    namespace: &str,
    table: &str,
    fields: &[NestedFieldRef],
    files: Vec<RecordBatch>,
) {
    drop_table_if_exists(catalog, &NamespaceIdent::new(namespace.to_string()), table).await;
    crate::common::seed::create_and_append_files(
        catalog,
        namespace,
        table,
        schema_of(0, fields),
        files.into_iter().map(std::iter::once),
    )
    .await
    .unwrap_or_else(|e| panic!("seed {namespace}.{table}: {e}"));
}

/// Commits `schema(next id)` as the table's current schema, out of band and with no `REFRESH`.
async fn evolve_schema(
    catalog: &impl Catalog,
    namespace: &str,
    table: &str,
    schema: impl FnOnce(i32) -> IcebergSchema,
) {
    let current = load(catalog, namespace, table)
        .await
        .metadata()
        .current_schema_id();
    rest_replace_current_schema(
        &iceberg_catalog_url(),
        namespace,
        table,
        current,
        schema(current + 1),
    )
    .await
    .unwrap_or_else(|e| panic!("evolve {namespace}.{table}: {e}"));
}

/// Writes one data file under the table's current schema.
async fn append_file(catalog: &impl Catalog, namespace: &str, table: &str, batch: RecordBatch) {
    let loaded = load(catalog, namespace, table).await;
    crate::common::seed::write_one_file_append(catalog, &loaded, table, [batch])
        .await
        .unwrap_or_else(|e| panic!("append to {namespace}.{table}: {e}"));
}

fn source_type(notes: &[(String, Json)], column: &str) -> Json {
    note_of(notes, column)["sourceType"].clone()
}

/// Scenario: Every declared column carries its source shape in its adapterNotes
#[test]
fn iceberg_columns_persist_source_notes() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-notes");
    let fields = [long_field(1, "id", true), long_field(2, "score", false)];
    rt.block_on(fresh_table(
        &catalog,
        NS_NOTES,
        "orders",
        &fields,
        vec![long_batch(&fields, &[&[Some(1)], &[Some(10)]])],
    ));

    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_NOTES_VS", NS_NOTES);
    let notes = column_adapter_notes(&mut conn, "REFRESH_NOTES_VS", "ORDERS");

    // Whole-note equality also proves no partition, no refusal, and no credential member.
    assert_eq!(
        notes,
        [
            (
                "ID".to_string(),
                serde_json::json!({"sourceType":
                    {"field_id": 1, "name": "id", "arrow_type": "int64", "nullable": false}})
            ),
            (
                "SCORE".to_string(),
                serde_json::json!({"sourceType":
                    {"field_id": 2, "name": "score", "arrow_type": "int64", "nullable": true}})
            ),
        ]
    );
}

/// Scenario: Refresh and set properties rebuild every column note from the source
#[test]
fn refresh_and_set_properties_rebuild_column_notes_from_the_source() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-renotes");
    let fields = [long_field(1, "id", true), long_field(2, "score", false)];
    rt.block_on(fresh_table(
        &catalog,
        NS_RENOTES,
        "scores",
        &fields,
        vec![long_batch(&fields, &[&[Some(1)], &[Some(10)]])],
    ));
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_RENOTES_VS", NS_RENOTES);
    create_virtual_schema(&mut conn, "SETPROPS_RENOTES_VS", NS_RENOTES);

    let renamed_with_sibling = [
        long_field(1, "id", true),
        long_field(2, "rating", false),
        long_field(3, "extra", false),
    ];
    rt.block_on(evolve_schema(&catalog, NS_RENOTES, "scores", |id| {
        schema_of(id, &renamed_with_sibling)
    }));
    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_RENOTES_VS REFRESH");
    conn.execute("ALTER VIRTUAL SCHEMA SETPROPS_RENOTES_VS SET PARALLELISM_FACTOR = '2'");

    for vs_name in ["REFRESH_RENOTES_VS", "SETPROPS_RENOTES_VS"] {
        let notes = column_adapter_notes(&mut conn, vs_name, "SCORES");
        let columns: Vec<&str> = notes.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(columns, ["EXTRA", "ID", "RATING"], "{vs_name}");
        let rating = source_type(&notes, "RATING");
        assert_eq!(
            (&rating["name"], &rating["field_id"]),
            (&Json::from("rating"), &Json::from(2)),
            "{vs_name}: RATING's note carries the source name now and the id `score` carried"
        );
        assert_eq!(source_type(&notes, "EXTRA")["field_id"], 3, "{vs_name}");
    }
}

/// Scenario: A column renamed at the source after the last REFRESH reads under its declared name
#[test]
fn renamed_iceberg_column_reads_under_its_declared_name() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-rename");
    let fields = [long_field(1, "id", true), long_field(2, "score", false)];
    rt.block_on(fresh_table(
        &catalog,
        NS_RENAME,
        "scores",
        &fields,
        vec![long_batch(
            &fields,
            &[&[Some(1), Some(2)], &[Some(10), Some(20)]],
        )],
    ));
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_RENAME_VS", NS_RENAME);
    let renamed = [long_field(1, "id", true), long_field(2, "rating", false)];
    rt.block_on(evolve_schema(&catalog, NS_RENAME, "scores", |id| {
        schema_of(id, &renamed)
    }));

    let table = vs_table("REFRESH_RENAME_VS", "scores");
    assert_text_columns(
        &mut conn,
        &format!("SELECT ID, SCORE FROM {table} ORDER BY ID"),
        &[[Some("1"), Some("2")], [Some("10"), Some("20")]],
    );
    query_error(&mut conn, &format!("SELECT RATING FROM {table}"));

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_RENAME_VS REFRESH");
    assert_text_columns(
        &mut conn,
        &format!("SELECT ID, RATING FROM {table} ORDER BY ID"),
        &[[Some("1"), Some("2")], [Some("10"), Some("20")]],
    );
}

/// Scenario: A column dropped at the source after the last REFRESH reads what older data files store
#[test]
fn dropped_iceberg_column_reads_what_older_files_store() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-drop");
    let fields = [
        long_field(1, "id", true),
        long_field(2, "note", false),
        long_field(3, "required_marker", true),
    ];
    rt.block_on(fresh_table(
        &catalog,
        NS_DROP,
        "notes_t",
        &fields,
        vec![long_batch(
            &fields,
            &[
                &[Some(1), Some(2)],
                &[Some(10), Some(20)],
                &[Some(5), Some(6)],
            ],
        )],
    ));
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_DROP_VS", NS_DROP);
    let dropped = [long_field(1, "id", true)];
    rt.block_on(evolve_schema(&catalog, NS_DROP, "notes_t", |id| {
        schema_of(id, &dropped)
    }));
    rt.block_on(append_file(
        &catalog,
        NS_DROP,
        "notes_t",
        long_batch(&dropped, &[&[Some(3)]]),
    ));

    let table = vs_table("REFRESH_DROP_VS", "notes_t");
    assert_text_columns(
        &mut conn,
        &format!("SELECT ID, NOTE FROM {table} ORDER BY ID"),
        &[
            [Some("1"), Some("2"), Some("3")],
            [Some("10"), Some("20"), None],
        ],
    );
    assert_text_columns(
        &mut conn,
        &format!("SELECT ID FROM {table} WHERE NOTE = 20"),
        &[[Some("2")]],
    );
    let required_absent = query_error(
        &mut conn,
        &format!("SELECT ID, REQUIRED_MARKER FROM {table}"),
    );
    assert!(
        required_absent.to_lowercase().contains("required_marker"),
        "the dropped required column fails on the file written after the drop: \
         {required_absent}"
    );

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_DROP_VS REFRESH");
    let declared: Vec<String> = declared_types(&mut conn, "REFRESH_DROP_VS", "notes_t")
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(declared, ["ID"], "REFRESH removes the dropped columns");
}

/// Scenario: A declared required column that the current schema marks optional fails the query
#[test]
fn iceberg_column_made_optional_after_refresh_fails_until_refresh() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-optional");
    let fields = [long_field(1, "id", true), long_field(2, "qty", true)];
    rt.block_on(fresh_table(
        &catalog,
        NS_OPTIONAL,
        "qty_t",
        &fields,
        vec![long_batch(&fields, &[&[Some(1)], &[Some(5)]])],
    ));
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_OPTIONAL_VS", NS_OPTIONAL);
    let relaxed = [long_field(1, "id", true), long_field(2, "qty", false)];
    rt.block_on(evolve_schema(&catalog, NS_OPTIONAL, "qty_t", |id| {
        schema_of(id, &relaxed)
    }));
    rt.block_on(append_file(
        &catalog,
        NS_OPTIONAL,
        "qty_t",
        long_batch(&relaxed, &[&[Some(2)], &[None]]),
    ));

    let table = vs_table("REFRESH_OPTIONAL_VS", "qty_t");
    let is_null = format!("SELECT ID FROM {table} WHERE QTY IS NULL");
    for sql in [is_null.clone(), format!("SELECT ID FROM {table}")] {
        assert_query_fails(
            &mut conn,
            &sql,
            &[
                "'QTY'",
                "'QTY_T'",
                "non-nullable",
                "ALTER VIRTUAL SCHEMA ... REFRESH",
            ],
        );
    }

    conn.execute("ALTER VIRTUAL SCHEMA REFRESH_OPTIONAL_VS REFRESH");
    assert_text_columns(&mut conn, &is_null, &[[Some("2")]]);
}

/// Scenario: A filter column resolves through its declared field id
#[test]
fn filter_on_rename_swapped_iceberg_columns_prunes_by_declared_field_id() {
    stack();
    let rt = runtime();
    let catalog = seed_catalog(rt, "refresh-swap");
    let fields = [long_field(1, "a", false), long_field(2, "b", false)];
    rt.block_on(fresh_table(
        &catalog,
        NS_SWAP,
        "swap_t",
        &fields,
        vec![
            long_batch(&fields, &[&[Some(7)], &[Some(100)]]),
            long_batch(&fields, &[&[Some(100)], &[Some(7)]]),
        ],
    ));
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, "REFRESH_SWAP_VS", NS_SWAP);
    let table = vs_table("REFRESH_SWAP_VS", "swap_t");
    let on_a = format!("SELECT A, B FROM {table} WHERE A = 7");
    assert_text_columns(
        &mut conn,
        &format!("SELECT A, B FROM {table} WHERE 7 = A"),
        &[[Some("7")], [Some("100")]],
    );

    let swapped = [long_field(1, "b", false), long_field(2, "a", false)];
    rt.block_on(evolve_schema(&catalog, NS_SWAP, "swap_t", |id| {
        schema_of(id, &swapped)
    }));

    assert_text_columns(&mut conn, &on_a, &[[Some("7")], [Some("100")]]);
    let planned = isolated_pushdown_statement(&mut conn, &on_a);
    assert_eq!(
        planned.matches(".parquet").count(),
        1,
        "pruning reads field id 1's bounds and keeps only the file holding A = 7: {planned}"
    );
}
