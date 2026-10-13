use std::collections::BTreeMap;
use std::path::Path;

use delta_kernel::schema::DataType as DeltaType;
use object_store::local::LocalFileSystem;

use super::super::{ConnectionStorage, ResolvedScan};
use super::*;
use crate::adapter::pushdown::test_support::{
    SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, UNREACHABLE_CATALOG, closed_port_storage,
    declared_notes, delta_commit_zero_key, delta_object_endpoint, involved_notes,
    resolve_from_notes, sample_storage, unity_delta_table, user_message,
};
use lakehouse_catalog::{
    CatalogTable, CatalogTableIdent, CatalogTableType, ConnectionCreds, StorageBackend,
    TableFormat, UnityCatalogSession,
};

const TABLE_NAME: &str = "cat.sch.orders";

const STATIC_SECRET: &str = "minioadmin";

fn creds(use_vended_credentials: bool) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: STATIC_SECRET.into(),
        secret_key: STATIC_SECRET.into(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: true,
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

fn delta_table(
    storage_location: Option<&str>,
    vended_credential_key: Option<&str>,
) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "orders".into(),
        },
        table_type: CatalogTableType::Table,
        storage_location: storage_location.map(str::to_string),
        format: TableFormat::Delta,
        vended_credential_key: vended_credential_key.map(str::to_string),
        partition_columns: Vec::new(),
        metadata_location: None,
        columns: Vec::new(),
        properties: Default::default(),
    }
}

/// The Delta reader of `table`, planned from `notes`.
async fn resolve_noted(
    table: &CatalogTable,
    notes: &[(String, String)],
    creds: &ConnectionCreds,
    storage: &StorageBackend,
    filter: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let connection = ConnectionStorage {
        storage,
        creds,
        allow_http: true,
    };
    let reader = DeltaFormatReader {
        storage: UnityTableStorage::new(&session, table, &connection),
    };
    resolve_from_notes(&reader, &involved_notes(TABLE_NAME, notes), filter).await
}

async fn refusal(table: &CatalogTable, use_vended_credentials: bool) -> String {
    refusal_as(table, &creds(use_vended_credentials), &sample_storage()).await
}

/// [`refusal`] for the effective credential set `creds`, whose static storage is `storage`.
async fn refusal_as(
    table: &CatalogTable,
    creds: &ConnectionCreds,
    storage: &StorageBackend,
) -> String {
    user_message(
        resolve_noted(table, &[], creds, storage, None)
            .await
            .expect_err("resolution must fail, never answer a scan"),
    )
}

/// A role CONNECTION that vends takes the same vended path as a static one; its
/// session is only the effective static credential, so a missing vending key must
/// refuse rather than fall back to it either.
#[tokio::test]
async fn vending_without_a_vending_key_errors_and_never_falls_back_to_static() {
    const SESSION_SECRET: &str = "SESSION_SECRET_SENTINEL";
    const SESSION_TOKEN: &str = "SESSION_TOKEN_SENTINEL";
    let role = ConnectionCreds {
        access_key: "ASIASESSIONKEYSENTINEL".into(),
        secret_key: SESSION_SECRET.into(),
        session_token: Some(SESSION_TOKEN.into()),
        aws_assume_role_arn: Some("arn:aws:iam::123456789012:role/lakehouse-reader".into()),
        ..creds(true)
    };
    let role_storage = crate::adapter::connection::storage_block(&role, true);

    for absent_key in [None, Some("")] {
        let table = delta_table(Some("s3://bucket/cat/sch/orders"), absent_key);
        let role_message = refusal_as(&table, &role, &role_storage).await;
        let message = refusal(&table, true).await;
        assert_eq!(
            role_message, message,
            "a role must leave the vended path's refusal unchanged"
        );
        for secret in [SESSION_SECRET, SESSION_TOKEN] {
            assert!(!role_message.contains(secret), "{role_message}");
        }

        assert!(
            message.contains(TABLE_NAME),
            "the refusal must name the table whose vending key is missing: {message}"
        );
        assert!(
            message.contains("vend"),
            "the refusal must state that no vending key was reported: {message}"
        );
        assert!(
            !message.contains("Delta version") && !message.contains("_delta_log"),
            "reaching the transaction log proves the static credential was used as a \
             fallback: {message}"
        );
        assert!(
            !message.contains(STATIC_SECRET),
            "no error may carry a credential value: {message}"
        );
    }
}

/// Scenario: An empty table storage location is rejected before any object-store access
#[tokio::test]
async fn empty_storage_location_errors_identically_under_both_credential_modes() {
    let mut messages = Vec::new();
    for location in [None, Some(""), Some("   ")] {
        for vending_key in [None, Some("table-id-1")] {
            for use_vended_credentials in [false, true] {
                messages.push(
                    refusal(&delta_table(location, vending_key), use_vended_credentials).await,
                );
            }
        }
    }

    let expected = &messages[0];
    assert!(
        messages.iter().all(|message| message == expected),
        "an empty storage location must report one text for every credential mode and \
         vending key: {messages:?}"
    );
    assert!(
        expected.contains(TABLE_NAME),
        "the refusal must name the table whose location is empty: {expected}"
    );
    assert!(
        expected.contains("EMPTY storage location"),
        "the refusal must name the empty storage location it rejected: {expected}"
    );
    assert!(
        !expected.contains(UNREACHABLE_CATALOG) && !expected.contains("minio:9000"),
        "no CONNECTION-derived address may stand in for the table's own location: \
         {expected}"
    );
}

const DELTA_TABLE_ROOT: &str = "s3://bucket/cat/sch/orders";

#[tokio::test]
async fn a_failed_log_read_reports_no_static_credential_value() {
    let table = delta_table(Some(DELTA_TABLE_ROOT), None);

    let message = refusal_as(&table, &creds(false), &closed_port_storage()).await;

    assert!(
        message.contains(DELTA_TABLE_ROOT),
        "the refusal must name the table root whose log could not be read: {message}"
    );
    assert!(
        !message.contains(SENTINEL_ACCESS_KEY),
        "no error may carry the access key it read through: {message}"
    );
    assert!(
        !message.contains(SENTINEL_SECRET_KEY),
        "no error may carry the secret key it read through: {message}"
    );
}

const PRUNING_FIXTURE_TABLE: &str = "letter_partitioned";

fn pruning_fixture_commit() -> String {
    let protocol = serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = serde_json::json!({"metaData": {
        "id": "pruning-fixture",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::json!({"type": "struct", "fields": [
            {"name": "letter", "type": "string", "nullable": true, "metadata": {}},
        ]}).to_string(),
        "partitionColumns": ["letter"],
        "configuration": {},
        "createdTime": 1,
    }});
    let add_a = serde_json::json!({"add": {
        "path": "letter=a/part-0.parquet",
        "partitionValues": {"letter": "a"},
        "size": 100,
        "modificationTime": 1,
        "dataChange": true,
    }});
    let add_b = serde_json::json!({"add": {
        "path": "letter=b/part-0.parquet",
        "partitionValues": {"letter": "b"},
        "size": 100,
        "modificationTime": 1,
        "dataChange": true,
    }});
    format!("{protocol}\n{metadata}\n{add_a}\n{add_b}\n")
}

async fn pruning_fixture_storage() -> StorageBackend {
    delta_object_endpoint(vec![(
        delta_commit_zero_key(PRUNING_FIXTURE_TABLE),
        pruning_fixture_commit(),
    )])
    .await
}

fn letter_equals_a_filter() -> Json {
    serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "letter"},
        "right": {"type": "literal_string", "value": "a"},
    })
}

/// Scenario: Enabling the kernel's skipping surfaces no statistic to the engine or the wire
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pruning_changes_only_the_file_list_of_the_resolved_scan() {
    let storage = pruning_fixture_storage().await;
    let table = delta_table(Some(&format!("s3://bucket/{PRUNING_FIXTURE_TABLE}")), None);
    let letter = StructField::nullable("letter", DeltaType::STRING);
    let notes = declared_notes(&unity_delta_table(&[letter], None, &["letter"]));
    let creds = creds(false);
    let resolve = |filter| resolve_noted(&table, &notes, &creds, &storage, filter);

    let filter = letter_equals_a_filter();
    let pruned = resolve(Some(&filter))
        .await
        .expect("a filter naming the partition column must resolve a pruned scan");
    let unpruned = resolve(None)
        .await
        .expect("an unfiltered request must resolve every active file");

    let unpruned_paths: Vec<&str> = unpruned
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    let pruned_paths: Vec<&str> = pruned.files.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(
        unpruned_paths,
        vec!["letter=a/part-0.parquet", "letter=b/part-0.parquet"],
        "an unfiltered request must resolve both fixture files"
    );
    assert_eq!(
        pruned_paths,
        vec!["letter=a/part-0.parquet"],
        "the letter = 'a' filter must leave exactly the letter=a file, never zero files"
    );
    assert_eq!(pruned.logical_schema, unpruned.logical_schema);
    assert_eq!(pruned.partition_columns, unpruned.partition_columns);
    assert_eq!(pruned.table_root, unpruned.table_root);
    assert_eq!(pruned.name_mapping, unpruned.name_mapping);
    assert_eq!(pruned.refused_columns, unpruned.refused_columns);
}

const VIRTUAL_TABLE: &str = "CM_NAME_MODE";

fn fixture_root(table: &str) -> String {
    format!(
        "{}/../../scripts/unity/fixtures/{table}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn snapshot(table_root: &str) -> DeltaSnapshot {
    DeltaSnapshot::open(Arc::new(LocalFileSystem::new()), table_root)
        .expect("the table opens past the gate")
}

/// The fields of `table_root`'s current schema, as Unity Catalog's `type_json` copies them.
fn logged_fields(table_root: &str) -> Vec<StructField> {
    snapshot(table_root).schema().fields().cloned().collect()
}

/// What CREATE declares from a Unity Catalog copy of `fields`.
fn declaration(fields: &[StructField], mode: Option<&str>, partitions: &[&str]) -> DeclaredSchema {
    let notes = declared_notes(&unity_delta_table(fields, mode, partitions));
    involved_notes(VIRTUAL_TABLE, &notes)
        .declared_schema()
        .expect("the declaration parses")
}

fn planned(
    table_root: &str,
    declared: &DeclaredSchema,
    filter: Option<&Json>,
) -> Result<Vec<FileEntry>, String> {
    declared_files(&snapshot(table_root), declared, filter).map_err(user_message)
}

/// A table in a fresh temporary directory whose log holds `commits` from version 0, after a
/// copy of the vendored fixture `copied`'s commits when given.
fn temporary_table(name: &str, copied: Option<&str>, commits: &[Vec<Json>]) -> String {
    let root = std::env::temp_dir().join(format!("lh_delta_reader_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let log = root.join("_delta_log");
    std::fs::create_dir_all(&log).expect("table directory");
    let mut version = 0;
    if let Some(copied) = copied {
        let mut logged: Vec<_> =
            std::fs::read_dir(Path::new(&fixture_root(copied)).join("_delta_log"))
                .expect("the fixture log lists")
                .map(|entry| entry.expect("a log entry").path())
                .filter(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "json")
                })
                .collect();
        logged.sort();
        for path in logged {
            std::fs::copy(&path, log.join(path.file_name().expect("a file"))).expect("copy");
            version += 1;
        }
    }
    for actions in commits {
        let body: Vec<String> = actions.iter().map(Json::to_string).collect();
        std::fs::write(log.join(format!("{version:020}.json")), body.join("\n")).expect("commit");
        version += 1;
    }
    root.to_string_lossy().into_owned()
}

fn id_equals_three() -> Json {
    serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "ID"},
        "right": {"type": "literal_exactnumeric", "value": 3},
    })
}

/// Scenario: A declaration that disagrees with the Delta log fails the query
#[test]
fn declaration_check_refuses_a_mode_or_partition_mismatch() {
    let name_mode = fixture_root("cdf-column-mapping-name-mode");
    let id_mode = fixture_root("cdf-column-mapping-id-mode");
    let partitioned = fixture_root("basic_partitioned");
    let disagreements = [
        (&name_mode, None, &[][..], "column-mapping mode is 'name'"),
        (&name_mode, Some("id"), &[], "column-mapping mode is 'name'"),
        (&id_mode, Some("name"), &[], "column-mapping mode is 'id'"),
        (&partitioned, None, &[], r#"partitions by ["letter"]"#),
    ];

    for (root, mode, partitions, part) in disagreements {
        let declared = declaration(&logged_fields(root), mode, partitions);

        let message = planned(root, &declared, None).expect_err("the query fails");

        for fragment in [
            VIRTUAL_TABLE,
            part,
            "type_json",
            "ALTER VIRTUAL SCHEMA ... REFRESH",
        ] {
            assert!(
                message.contains(fragment),
                "'{fragment}' missing: {message}"
            );
        }
    }
    let annotated: Vec<StructField> = logged_fields(&partitioned)
        .into_iter()
        .zip(1..)
        .map(|(field, id)| {
            let physical_name = format!("col-{}", field.name());
            field.with_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(id)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String(physical_name),
                ),
            ])
        })
        .collect();
    let declared = declaration(&annotated, Some("name"), &["letter"]);

    let message = planned(&partitioned, &declared, None).expect_err("a mapped declaration fails");

    for fragment in [
        VIRTUAL_TABLE,
        "column-mapping mode is 'none'",
        "ALTER VIRTUAL SCHEMA ... REFRESH",
    ] {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing: {message}"
        );
    }
    for (root, mode, partitions) in [
        (&name_mode, Some("name"), &[][..]),
        (&id_mode, Some("id"), &[]),
        (&partitioned, None, &["letter"]),
    ] {
        let declared = declaration(&logged_fields(root), mode, partitions);

        let files = planned(root, &declared, None).expect("an agreeing declaration plans");

        assert!(!files.is_empty(), "{root}");
    }
}

/// Scenario: A declared non-nullable column that the Delta log marks nullable fails the query
#[test]
fn declaration_check_refuses_a_non_nullable_declaration_of_a_nullable_field() {
    let root = fixture_root("cdf-column-mapping-name-mode");
    let mut fields = logged_fields(&root);
    fields[0].nullable = false;

    let message = planned(
        &root,
        &declaration(&fields, Some("name"), &[]),
        Some(&id_equals_three()),
    )
    .expect_err("a stale non-nullable declaration fails the query");

    for fragment in [
        "'ID'",
        VIRTUAL_TABLE,
        "allows NULL",
        "ALTER VIRTUAL SCHEMA ... REFRESH",
    ] {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing: {message}"
        );
    }
}

/// Scenario: A filter column resolves through its declared binding key
#[test]
fn filter_resolves_through_the_declared_physical_name_after_a_rename_swap() {
    let fixture = "cdf-column-mapping-name-mode";
    let fields = logged_fields(&fixture_root(fixture));
    let declared = declaration(&fields, Some("name"), &[]);
    let mut gone = fields.clone();
    gone[0] = fields[0].clone().with_metadata([(
        "delta.columnMapping.physicalName",
        MetadataValue::String("col-dropped".to_string()),
    )]);
    let mut metadata = serde_json::json!({
        "id": "rename-swap",
        "format": {"provider": "parquet", "options": {}},
        "partitionColumns": [],
        "configuration": {"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "3"},
        "createdTime": 1,
    });
    let mut swapped = logged_fields(&fixture_root(fixture));
    let (id_name, value_name) = (swapped[0].name.clone(), swapped[2].name.clone());
    swapped[0].name = value_name;
    swapped[2].name = id_name;
    metadata["schemaString"] =
        serde_json::to_string(&StructType::try_new(swapped).expect("distinct names"))
            .expect("a schema serializes")
            .into();
    let root = temporary_table(
        "rename_swap",
        Some(fixture),
        &[vec![serde_json::json!({"metaData": metadata})]],
    );

    let kept = planned(&root, &declared, Some(&id_equals_three())).expect("the swap plans");
    let unconstrained = planned(
        &root,
        &declaration(&gone, Some("name"), &[]),
        Some(&id_equals_three()),
    )
    .expect("a dropped key plans");

    let paths: Vec<&str> = kept.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(
        paths.len(),
        1,
        "ID = 3 keeps only the file whose id is 3: {paths:?}"
    );
    assert!(paths[0].starts_with("part-00015-1238a68f"), "{paths:?}");
    assert_eq!(
        unconstrained.len(),
        3,
        "a key the snapshot lacks constrains nothing"
    );
}

/// `add.partitionValues` is keyed by physical name under a mapped mode, and the scan reads each
/// partition value under the declared column's name.
///
/// Scenario: A declaration that disagrees with the Delta log fails the query
/// Scenario: A declared non-nullable column that the Delta log marks nullable fails the query
#[test]
fn partition_values_are_keyed_by_the_declared_partition_columns_in_the_logs_order() {
    for mode in ["name", "id"] {
        let field = |name: &str, id: i64| {
            serde_json::json!({"name": name, "type": "string", "nullable": false, "metadata": {
                "delta.columnMapping.id": id, "delta.columnMapping.physicalName": format!("col-{name}"),
            }})
        };
        let fields = [field("id", 1), field("p2", 2), field("p1", 3)];
        let schema = serde_json::json!({"type": "struct", "fields": fields});
        let root = temporary_table(
            &format!("partition_keys_{mode}"),
            None,
            &[vec![
                serde_json::json!({"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}}),
                serde_json::json!({"metaData": {
                    "id": "partitioned", "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema.to_string(), "partitionColumns": ["p2", "p1"],
                    "configuration": {"delta.columnMapping.mode": mode, "delta.columnMapping.maxColumnId": "3"},
                    "createdTime": 1,
                }}),
                serde_json::json!({"add": {
                    "path": "p2=x/p1=y/part-0.parquet", "partitionValues": {"col-p2": "x", "col-p1": "y"},
                    "size": 1, "modificationTime": 1, "dataChange": true,
                }}),
            ]],
        );
        let mut declared_fields = logged_fields(&root);
        declared_fields[0].nullable = true;

        let files = planned(
            &root,
            &declaration(&declared_fields, Some(mode), &["p2", "p1"]),
            None,
        )
        .expect("a nullable declaration of a required column plans");
        let reordered = planned(
            &root,
            &declaration(&declared_fields, Some(mode), &["p1", "p2"]),
            None,
        )
        .expect_err("partition columns out of the log's order disagree");

        assert_eq!(
            files[0].partition_values,
            BTreeMap::from([
                ("p1".to_string(), Some("y".to_string())),
                ("p2".to_string(), Some("x".to_string())),
            ]),
            "mode {mode}"
        );
        assert!(
            reordered.contains(r#"partitions by ["p2", "p1"]"#),
            "{reordered}"
        );
    }
}
