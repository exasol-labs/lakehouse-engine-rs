//! An Iceberg table's state for the export tests: columns, snapshots, and snapshot summary values
//! from its metadata JSON, the warehouse objects under a prefix, and a local REST catalog table.

use std::collections::BTreeSet;

use futures::TryStreamExt;
use iceberg::spec::DataContentType;
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

/// One live data file of an Iceberg table, read through iceberg-rust in the test process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedFile {
    pub path: String,
    /// The ID of the partition spec the file was written under.
    pub spec_id: i32,
    /// Each partition field's name and value as Iceberg renders it in a path, NULL as `None`.
    pub partition: Vec<(String, Option<String>)>,
    /// The values of the file's `rid` column, matched in any letter case.
    pub rids: Vec<i64>,
}

/// Which snapshots' data files [`data_file_partitions`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddedBy {
    /// Snapshots whose summary carries `lakehouse.export-id`.
    Export,
    /// Every other snapshot, such as the ones Spark wrote.
    OtherEngine,
}

/// The live data files of `namespace.table` that snapshots of `added_by` added, with each file's
/// partition values and `rid`s. Exasol shows no file's partition values, so the manifests are the
/// oracle (`specs/testing.md` § Correctness oracles).
pub fn data_file_partitions(
    namespace: &str,
    table: &str,
    added_by: AddedBy,
) -> Vec<PartitionedFile> {
    let table = load_table(namespace, table);
    runtime().block_on(async {
        let metadata = table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Vec::new();
        };
        let fail = |what: &str, e: iceberg::Error| -> ! {
            panic!("{what} of {}: {e}", table.identifier())
        };
        let manifests = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap_or_else(|e| fail("load the manifest list", e));
        let mut files = Vec::new();
        for manifest_file in manifests.entries() {
            let manifest = table
                .manifest_reader()
                .read(manifest_file)
                .await
                .unwrap_or_else(|e| fail("read a manifest", e));
            for entry in manifest.entries() {
                if !entry.is_alive() || entry.content_type() != DataContentType::Data {
                    continue;
                }
                let exported = entry
                    .snapshot_id()
                    .and_then(|id| metadata.snapshot_by_id(id))
                    .is_some_and(|added| {
                        added
                            .summary()
                            .additional_properties
                            .contains_key(EXPORT_ID)
                    });
                if exported != (added_by == AddedBy::Export) {
                    continue;
                }
                let spec_id = manifest_file.partition_spec_id;
                let spec = metadata
                    .partition_spec_by_id(spec_id)
                    .unwrap_or_else(|| panic!("no partition spec {spec_id}"));
                let partition_type = spec
                    .partition_type(metadata.current_schema())
                    .unwrap_or_else(|e| fail("derive the partition type", e));
                let values = entry.data_file().partition();
                let partition = spec
                    .fields()
                    .iter()
                    .zip(partition_type.fields())
                    .enumerate()
                    .map(|(index, (field, typed))| {
                        let value = values[index].as_ref();
                        let text = value
                            .map(|v| field.transform.to_human_string(&typed.field_type, Some(v)));
                        (field.name.clone(), text)
                    })
                    .collect();
                let path = entry.file_path().to_string();
                let rids = read_rids(&table, &path).await;
                files.push(PartitionedFile {
                    path,
                    spec_id,
                    partition,
                    rids,
                });
            }
        }
        files
    })
}

/// The `rid` column of the Parquet file at `path`, cast to `i64`.
async fn read_rids(table: &iceberg::table::Table, path: &str) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let bytes = table
        .file_io()
        .new_input(path)
        .unwrap_or_else(|e| panic!("open {path}: {e}"))
        .read()
        .await
        .unwrap_or_else(|e| panic!("read {path}: {e}"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .and_then(|builder| builder.build())
        .unwrap_or_else(|e| panic!("open the Parquet file {path}: {e}"));
    let mut rids = Vec::new();
    for batch in reader {
        let batch = batch.unwrap_or_else(|e| panic!("read a batch of {path}: {e}"));
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name().eq_ignore_ascii_case("rid"))
            .unwrap_or_else(|| panic!("{path} has no rid column"));
        let column = arrow::compute::cast(batch.column(index), &arrow::datatypes::DataType::Int64)
            .unwrap_or_else(|e| panic!("cast the rid column of {path}: {e}"));
        let column = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an Int64 array");
        rids.extend((0..column.len()).map(|row| column.value(row)));
    }
    rids
}
