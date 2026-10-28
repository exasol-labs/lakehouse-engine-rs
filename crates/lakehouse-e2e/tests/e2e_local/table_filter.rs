//! Every test seeds one shared Iceberg namespace that no other module touches, and uses its own
//! Virtual Schema.

use crate::common::e2e_harness::*;
use crate::common::exasol_ws::ExaConn;
use crate::fixtures::{stack, vs_table};
use crate::refresh::{
    create_empty_table, ensure_id_val_table, long_field, schema_of, seed_catalog,
};

use iceberg_catalog_rest::RestCatalog;

use iceberg::NamespaceIdent;

const CATALOG_CONN_NAME: &str = "TABLE_FILTER_CATALOG_CREDS";
const NAMESPACE: &str = "e2e_table_filter";
const FILTER_VS: &str = "TABLE_FILTER_VS";
const SETPROPS_VS: &str = "TABLE_FILTER_SETPROPS_VS";

fn seed_tables() {
    stack();
    let rt = runtime();
    let catalog: RestCatalog = seed_catalog(rt, "table-filter");
    for (index, table) in ["orders", "orders_2024", "orders_tmp", "audit_log"]
        .into_iter()
        .enumerate()
    {
        let id = i64::try_from(index).expect("table index") + 1;
        rt.block_on(ensure_id_val_table(&catalog, NAMESPACE, table, id, 1.0));
    }
    // The child namespace needs its parent, which the root tables above create.
    let staging = NamespaceIdent::from_strs([NAMESPACE, "staging"]).expect("staging namespace");
    rt.block_on(create_empty_table(
        &catalog,
        &staging,
        "orders",
        schema_of(0, &[long_field(1, "id", false)]),
    ));
}

fn props<'a>(vs_name: &'a str) -> VsProps<'a> {
    VsProps::new(vs_name, NAMESPACE).with_catalog_conn_name(CATALOG_CONN_NAME)
}

fn exposed_tables(conn: &mut ExaConn, vs_name: &str) -> Vec<String> {
    let mut names = enumerated_table_names(conn, vs_name);
    names.sort();
    names
}

#[track_caller]
fn assert_exposed(conn: &mut ExaConn, vs_name: &str, expected: &[&str], context: &str) {
    assert_eq!(exposed_tables(conn, vs_name), expected, "{context}");
}

/// Scenario: Include and exclude patterns choose the exposed tables, and exclude wins
#[test]
fn include_and_exclude_choose_the_exposed_tables() {
    seed_tables();
    let mut conn = exa_conn();

    create_virtual_schema(
        &mut conn,
        &props(FILTER_VS)
            .with_property("TABLE_INCLUDE", "ORDERS*")
            .with_property("TABLE_EXCLUDE", "ORDERS_TMP"),
    );
    assert_exposed(
        &mut conn,
        FILTER_VS,
        &["ORDERS", "ORDERS_2024"],
        "include ORDERS* with exclude ORDERS_TMP",
    );
    let id = conn.query_scalar_i64(&format!("SELECT id FROM {}", vs_table(FILTER_VS, "orders")));
    assert_eq!(id, 1, "an exposed table must stay queryable");

    create_virtual_schema(
        &mut conn,
        &props(FILTER_VS).with_property("TABLE_INCLUDE", "AUDIT_LOG"),
    );
    assert_exposed(&mut conn, FILTER_VS, &["AUDIT_LOG"], "include alone");

    create_virtual_schema(
        &mut conn,
        &props(FILTER_VS).with_property("TABLE_EXCLUDE", "STAGING__*, ORDERS_*"),
    );
    assert_exposed(
        &mut conn,
        FILTER_VS,
        &["AUDIT_LOG", "ORDERS"],
        "exclude alone, matching the child namespace by its flattened name",
    );
}

/// Scenario: Setting either property re-lists the exposed tables
#[test]
fn set_properties_relists_the_exposed_tables() {
    seed_tables();
    let mut conn = exa_conn();
    create_virtual_schema(&mut conn, &props(SETPROPS_VS));
    assert_exposed(
        &mut conn,
        SETPROPS_VS,
        &[
            "AUDIT_LOG",
            "ORDERS",
            "ORDERS_2024",
            "ORDERS_TMP",
            "STAGING__ORDERS",
        ],
        "a schema without a filter",
    );

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {SETPROPS_VS} SET TABLE_EXCLUDE = 'ORDERS_TMP'"
    ));
    let after_set = ["AUDIT_LOG", "ORDERS", "ORDERS_2024", "STAGING__ORDERS"];
    assert_exposed(
        &mut conn,
        SETPROPS_VS,
        &after_set,
        "after SET TABLE_EXCLUDE",
    );
    let id = conn.query_scalar_i64(&format!(
        "SELECT id FROM {}",
        vs_table(SETPROPS_VS, "orders")
    ));
    assert_eq!(id, 1, "ORDERS must stay declared and queryable");

    conn.execute(&format!("ALTER VIRTUAL SCHEMA {SETPROPS_VS} REFRESH"));
    assert_exposed(
        &mut conn,
        SETPROPS_VS,
        &after_set,
        "a full REFRESH applies the persisted filter",
    );

    conn.execute(&format!(
        "ALTER VIRTUAL SCHEMA {SETPROPS_VS} SET TABLE_EXCLUDE = null"
    ));
    assert_exposed(
        &mut conn,
        SETPROPS_VS,
        &[
            "AUDIT_LOG",
            "ORDERS",
            "ORDERS_2024",
            "ORDERS_TMP",
            "STAGING__ORDERS",
        ],
        "after SET TABLE_EXCLUDE = null",
    );
}
