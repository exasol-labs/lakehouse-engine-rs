use super::*;
use crate::test_support::{LOAD_TABLE_NO_SNAPSHOTS, STAGE_TABLE, data_file, ident, memory_table};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, MAIN_BRANCH, Struct,
};
use iceberg_catalog_rest::LoadTableResult;

fn tag_summary() -> HashMap<String, String> {
    HashMap::from([("lakehouse.export-id".to_string(), "e1".to_string())])
}

#[tokio::test]
async fn a_fast_append_captures_one_snapshot_on_the_current_main_and_writes_its_manifests() {
    let table = memory_table(LOAD_TABLE_NO_SNAPSHOTS, &ident("ns", "t"));
    let files = vec![data_file(&table, "a"), data_file(&table, "b")];

    let captured = capture_fast_append(&table, files, tag_summary())
        .await
        .unwrap();

    assert_eq!(
        captured.requirements,
        [
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid()
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: None,
            },
        ]
    );
    let [
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef { ref_name, .. },
    ] = captured.updates.as_slice()
    else {
        panic!("one snapshot and its main ref, got {:?}", captured.updates);
    };
    assert_eq!(ref_name, MAIN_BRANCH);
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get("lakehouse.export-id"),
        Some(&"e1".to_string())
    );
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get("added-data-files"),
        Some(&"2".to_string())
    );
    assert!(
        table
            .file_io()
            .exists(snapshot.manifest_list())
            .await
            .unwrap()
    );
}

#[test]
fn the_creation_updates_rebuild_the_staged_metadata_with_its_uuid_first() {
    let staged: LoadTableResult = serde_json::from_str(STAGE_TABLE).unwrap();
    let metadata = &staged.metadata;

    let updates = creation_updates(metadata).unwrap();

    assert_eq!(
        updates[..2],
        [
            TableUpdate::AssignUuid {
                uuid: metadata.uuid()
            },
            TableUpdate::UpgradeFormatVersion {
                format_version: FormatVersion::V2
            },
        ]
    );
    let schemas: Vec<_> = updates
        .iter()
        .filter_map(|update| match update {
            TableUpdate::AddSchema { schema } => Some(schema.as_struct().clone()),
            _ => None,
        })
        .collect();
    assert_eq!(schemas, [metadata.current_schema().as_struct().clone()]);
    assert!(updates.contains(&TableUpdate::SetLocation {
        location: metadata.location().to_string()
    }));
    assert!(updates.contains(&TableUpdate::SetProperties {
        updates: metadata.properties().clone()
    }));
    assert!(
        !updates
            .iter()
            .any(|update| matches!(update, TableUpdate::AddSnapshot { .. }))
    );
}

/// Delete the refusal of a `void` field named like a column in the engine's
/// `refuse_unappendable` once this fails: iceberg-rust then commits to such a table.
#[tokio::test]
async fn iceberg_rust_still_refuses_a_commit_to_a_void_partition_field_named_like_a_column() {
    let mut answer: serde_json::Value = serde_json::from_str(LOAD_TABLE_NO_SNAPSHOTS).unwrap();
    answer["metadata"]["format-version"] = 1.into();
    answer["metadata"]["partition-specs"] = serde_json::json!([
        {"spec-id": 0, "fields": [
            {"name": "V", "transform": "void", "source-id": 2, "field-id": 1000}
        ]}
    ]);
    answer["metadata"]["last-partition-id"] = 1000.into();
    let table = memory_table(&answer.to_string(), &ident("ns", "t"));
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{}/data/a.parquet", table.metadata().location()))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1024)
        .record_count(10)
        .partition_spec_id(0)
        .partition(Struct::from_iter([None::<Literal>]))
        .build()
        .unwrap();

    let refused = capture_fast_append(&table, vec![file], tag_summary())
        .await
        .err()
        .map(|error| error.message().to_string());

    assert_eq!(
        refused.as_deref(),
        Some(
            "Cannot create partition with name: 'V' that conflicts with schema field and is not \
             an identity transform."
        )
    );
}
