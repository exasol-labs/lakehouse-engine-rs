use super::*;
use crate::adapter::pushdown::test_support::{
    SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, closed_port_storage, delta_commit_zero_key,
    delta_object_endpoint, sample_storage,
};
use lakehouse_catalog::{CatalogTableIdent, CatalogTableType, ConnectionCreds, TableFormat};

/// Closed port: a stray credential request fails with a transport error, distinguishable
/// from every refusal asserted here.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

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
        aws_assume_role_arn: None,
        aws_external_id: None,
        aws_sts_endpoint: None,
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
        columns: Vec::new(),
    }
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
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let connection = ConnectionStorage {
        storage,
        creds,
        allow_http: true,
    };
    let reader = DeltaFormatReader::new(&session, table, &connection);

    let error = reader
        .resolve_scan(None)
        .await
        .expect_err("resolution must fail, never answer a scan");

    match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    }
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
    let creds = creds(false);
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let storage = closed_port_storage();
    let table = delta_table(Some(DELTA_TABLE_ROOT), None);
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    let reader = DeltaFormatReader::new(&session, &table, &connection);

    let error = reader
        .resolve_scan(None)
        .await
        .expect_err("a log read against a closed port must fail, never answer a scan");
    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };

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
    let creds = creds(false);
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let storage = pruning_fixture_storage().await;
    let table = delta_table(Some(&format!("s3://bucket/{PRUNING_FIXTURE_TABLE}")), None);
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    let reader = DeltaFormatReader::new(&session, &table, &connection);

    let filter = letter_equals_a_filter();
    let pruned = reader
        .resolve_scan(Some(&filter))
        .await
        .expect("a filter naming the partition column must resolve a pruned scan");
    let unpruned = reader
        .resolve_scan(None)
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
