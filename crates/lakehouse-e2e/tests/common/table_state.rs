//! An Iceberg table's state for the export tests: columns, snapshots, and snapshot summary values
//! from its metadata JSON, the warehouse objects under a prefix, and a local REST catalog table.

use std::collections::BTreeSet;

use futures::TryStreamExt;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use serde_json::Value;

use crate::common::e2e_harness::{local_stack_s3_store, runtime};
use crate::common::seed::build_seed_catalog;
use crate::common::stack::iceberg_catalog_url;

/// The SeaweedFS bucket of the local stack's warehouse.
pub const WAREHOUSE_BUCKET: &str = "warehouse";

/// The snapshot summary property that identifies the export statement that committed a snapshot.
pub const EXPORT_ID: &str = "lakehouse.export-id";

/// The name, Iceberg type, and required flag of each column of `metadata`'s current schema.
pub fn iceberg_columns(metadata: &Value) -> Vec<(&str, &str, bool)> {
    let schema_id = &metadata["current-schema-id"];
    let schema = metadata["schemas"]
        .as_array()
        .and_then(|schemas| schemas.iter().find(|s| &s["schema-id"] == schema_id))
        .unwrap_or_else(|| panic!("no current schema: {metadata}"));
    schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| {
            (
                field["name"].as_str().unwrap(),
                field["type"].as_str().unwrap(),
                field["required"].as_bool().unwrap(),
            )
        })
        .collect()
}

pub fn snapshots(metadata: &Value) -> &[Value] {
    metadata["snapshots"].as_array().map_or(&[], Vec::as_slice)
}

/// The values of the snapshot summary property `key`, one per snapshot.
pub fn summary_values(metadata: &Value, key: &str) -> Vec<String> {
    snapshots(metadata)
        .iter()
        .map(|snapshot| {
            snapshot["summary"][key]
                .as_str()
                .unwrap_or_else(|| panic!("a snapshot without {key}: {snapshot}"))
                .to_string()
        })
        .collect()
}

pub fn snapshot_ids(metadata: &Value) -> BTreeSet<String> {
    snapshots(metadata)
        .iter()
        .map(|snapshot| snapshot["snapshot-id"].to_string())
        .collect()
}

/// The snapshots that `after` adds to `before`, asserted to be `count` snapshots of one export
/// whose ID no snapshot of `before` carries, beside every snapshot of `before`, unchanged.
#[track_caller]
pub fn assert_one_new_export<'a>(
    label: &str,
    before: &Value,
    after: &'a Value,
    count: usize,
) -> Vec<&'a Value> {
    let earlier = snapshots(before);
    assert!(
        earlier
            .iter()
            .all(|snapshot| snapshots(after).contains(snapshot)),
        "{label}: the earlier snapshots stay unchanged: {after}"
    );
    let added: Vec<&Value> = snapshots(after)
        .iter()
        .filter(|snapshot| !earlier.contains(snapshot))
        .collect();
    let export_id = |snapshot: &Value| snapshot["summary"][EXPORT_ID].as_str().map(String::from);
    let added_ids: BTreeSet<Option<String>> = added.iter().map(|s| export_id(s)).collect();
    let earlier_ids: BTreeSet<Option<String>> = earlier.iter().map(export_id).collect();
    assert!(
        added.len() == count
            && added_ids.len() == 1
            && added_ids
                .iter()
                .all(|id| id.is_some() && !earlier_ids.contains(id)),
        "{label}: {count} new snapshots with one new export ID, got {added_ids:?} over \
         {} snapshots, earlier {earlier_ids:?}",
        added.len()
    );
    added
}

/// The keys under `prefix` of the warehouse bucket.
pub fn objects_under(prefix: &str) -> Vec<String> {
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

/// `namespace.table` of the local REST catalog.
pub fn load_table(namespace: &str, table: &str) -> iceberg::table::Table {
    runtime().block_on(async {
        let catalog = build_seed_catalog(&iceberg_catalog_url(), "s3://warehouse/", "load")
            .await
            .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog for {table}: {e:#}"));
        let ident = TableIdent::new(NamespaceIdent::new(namespace.into()), table.into());
        catalog
            .load_table(&ident)
            .await
            .unwrap_or_else(|e| panic!("load {namespace}.{table}: {e}"))
    })
}

/// Whether the local REST catalog holds `table`, a `<namespace>.<table>` name.
pub fn table_exists(table: &str) -> bool {
    let ident = TableIdent::from_strs(table.split('.'))
        .unwrap_or_else(|e| panic!("{table} is not a table name: {e}"));
    runtime()
        .block_on(async {
            build_seed_catalog(&iceberg_catalog_url(), "s3://warehouse/", "exists")
                .await
                .unwrap_or_else(|e| panic!("connect to the Iceberg REST catalog: {e:#}"))
                .table_exists(&ident)
                .await
        })
        .unwrap_or_else(|e| panic!("check table {table}: {e}"))
}

/// The metadata JSON of `namespace.table` of the local REST catalog, with every snapshot.
pub fn local_table_metadata(namespace: &str, table: &str) -> Value {
    serde_json::to_value(load_table(namespace, table).metadata())
        .unwrap_or_else(|e| panic!("serialize the metadata of {namespace}.{table}: {e}"))
}

/// Points `main` of local `namespace.table` at the newest snapshot no export committed. Exports
/// only add snapshots, so this finds the Spark snapshot after any number of earlier runs.
pub fn reset_to_unexported_snapshot(namespace: &str, table: &str) {
    let metadata = local_table_metadata(namespace, table);
    let snapshot = snapshots(&metadata)
        .iter()
        .filter(|snapshot| snapshot["summary"][EXPORT_ID].is_null())
        .max_by_key(|snapshot| snapshot["timestamp-ms"].as_i64())
        .unwrap_or_else(|| panic!("{namespace}.{table} has no snapshot that no export wrote"));
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        iceberg_catalog_url()
    );
    let commit = serde_json::json!({
        "requirements": [],
        "updates": [{
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "type": "branch",
            "snapshot-id": snapshot["snapshot-id"],
        }],
    });
    let status = reqwest::blocking::Client::new()
        .post(&url)
        .json(&commit)
        .send()
        .unwrap_or_else(|e| panic!("reset {namespace}.{table}: POST {url} failed to send: {e}"))
        .status();
    assert!(
        status.is_success(),
        "reset {namespace}.{table}: POST {url} returned {status}"
    );
}
