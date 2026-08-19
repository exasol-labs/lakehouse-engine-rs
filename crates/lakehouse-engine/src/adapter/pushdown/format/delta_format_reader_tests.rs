use super::*;
use crate::adapter::pushdown::test_support::{
    delta_commit_zero_key, delta_object_endpoint, sample_storage,
};
use crate::scan::spec::StorageProps;
use lakehouse_catalog::{CatalogTableIdent, CatalogTableType, TableFormat};

/// A closed port: any credential request the reader issued would fail loudly with a
/// transport error, which is distinguishable from every refusal asserted here.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

const TABLE_NAME: &str = "cat.sch.orders";

/// The static storage credential a forbidden fallback would silently reach for.
const STATIC_SECRET: &str = "minioadmin";

fn creds(use_vended_credentials: bool) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: STATIC_SECRET.into(),
        secret_key: STATIC_SECRET.into(),
        session_token: None,
        path_style: true,
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
        columns: Vec::new(),
    }
}

/// Resolve one table's scan and answer the user-error message it fails with.
async fn refusal(table: &CatalogTable, use_vended_credentials: bool) -> String {
    let creds = creds(use_vended_credentials);
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let storage = sample_storage();
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
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

/// Scenario: Delta planning resolves its storage credential through the table's own
/// catalog.
///
/// With vending enabled and no catalog-assigned vending key, resolution fails naming
/// the table. It MUST NOT reach the CONNECTION's static credential instead: a fallback
/// would have built that store and failed on the transaction log, so a message naming
/// the log — or one carrying the static credential — is the observable signature of the
/// fallback this refusal exists to prevent.
#[tokio::test]
async fn vending_without_a_vending_key_errors_and_never_falls_back_to_static() {
    for absent_key in [None, Some("")] {
        let message = refusal(
            &delta_table(Some("s3://bucket/cat/sch/orders"), absent_key),
            true,
        )
        .await;

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

/// Scenario: An empty table storage location is rejected before any object-store
/// access.
///
/// The location check runs before the vended/static split, so every combination of
/// credential mode and vending key reports the IDENTICAL text. A check placed after
/// the split would answer the vending-key refusal, or a transport error from the
/// credential request, for the vending-enabled rows.
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

/// A closed port on the loopback interface: every S3 request a log read issues is
/// refused locally, so the failure is deterministic and reaches no network.
const CLOSED_PORT_ENDPOINT: &str = "http://127.0.0.1:1";

/// Two DISTINCT sentinel credential values, so a leak names which half of the
/// static credential escaped.
const SENTINEL_ACCESS_KEY: &str = "AKIA-SENTINEL-ACCESS-0001";
const SENTINEL_SECRET_KEY: &str = "sentinel-secret-value-0002";

const DELTA_TABLE_ROOT: &str = "s3://bucket/cat/sch/orders";

/// `sample_storage()`'s shape carrying sentinel credentials and an endpoint nothing
/// listens on. `path_style` is set deliberately: without it `object_store` ignores the
/// endpoint and derives a real AWS host from the region, which would send this test's
/// requests out to the internet.
fn closed_port_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: CLOSED_PORT_ENDPOINT.into(),
        region: "us-east-1".into(),
        access_key: SENTINEL_ACCESS_KEY.into(),
        secret_key: SENTINEL_SECRET_KEY.into(),
        allow_http: true,
        path_style: true,
        ..Default::default()
    })
}

/// Every effective-storage secret is masked, and only the secrets are: redaction is
/// the single guard between an object-store error that echoes a credential verbatim
/// and the text Exasol surfaces, so it must mask each value it was handed while
/// leaving the rest of the message readable enough to act on.
#[test]
fn redacted_masks_every_effective_storage_secret_in_a_raised_error() {
    let secrets = [SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY];
    let raised = UdfError::User(format!(
        "failed to resolve the current Delta version for table root '{DELTA_TABLE_ROOT}': \
         signature mismatch for {SENTINEL_ACCESS_KEY} signed with {SENTINEL_SECRET_KEY}"
    ));

    let message = match redacted(raised, &secrets) {
        UdfError::User(message) => message,
        other => panic!("redaction must answer a user error, got {other:?}"),
    };

    assert!(
        !message.contains(SENTINEL_ACCESS_KEY),
        "the access key must not survive redaction: {message}"
    );
    assert!(
        !message.contains(SENTINEL_SECRET_KEY),
        "the secret key must not survive redaction: {message}"
    );
    assert!(
        message.starts_with("failed to resolve the current Delta version"),
        "the non-secret text must survive verbatim: {message}"
    );
    assert!(
        message.contains(DELTA_TABLE_ROOT),
        "the table root the read failed on must survive: {message}"
    );
}

/// Scenario: Delta planning resolves its storage credential through the table's own
/// catalog.
///
/// The static-credential half, driven to the point where the log is actually read: the
/// storage location and the credential decision both succeed, so this is the one test
/// that enters `read_delta_log` and fails inside it. The refusal must name the table
/// root it could not read and carry NEITHER static credential value, which is the
/// redaction this layer owns rather than a message that never held a secret.
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

fn refused(column_name: &str, reason: &str) -> RefusedColumn {
    RefusedColumn {
        column_name: column_name.to_string(),
        reason: reason.to_string(),
    }
}

// Scenario Coverage (delta-type-mapping): A Delta table with no mappable column is refused as a
// whole
#[test]
fn a_table_whose_every_column_is_refused_is_refused_as_a_whole() {
    let refused_columns = vec![
        refused("binary_col", "binary is refused, see #351"),
        refused("variant_col", "variant renders no meaningful value"),
    ];

    let error = ensure_table_has_a_mappable_column(&[], &refused_columns)
        .expect_err("a table with zero mappable columns must be refused as a whole");

    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };
    assert!(message.contains("binary_col"), "message was: {message}");
    assert!(
        message.contains("binary is refused, see #351"),
        "message was: {message}"
    );
    assert!(message.contains("variant_col"), "message was: {message}");
    assert!(
        message.contains("variant renders no meaningful value"),
        "message was: {message}"
    );
}

/// The `stats_all_types` shape: some columns refused, at least one mappable. The table stays
/// queryable on its mappable columns rather than being refused as a whole.
#[test]
fn a_table_with_at_least_one_mappable_column_is_not_refused_as_a_whole() {
    let logical_schema = vec![LogicalField {
        field_id: None,
        name: "id".to_string(),
        arrow_type: "int64".to_string(),
        nullable: false,
        initial_default: None,
        nested: None,
        physical_name: None,
    }];
    let refused_columns = vec![refused("binary_col", "binary is refused, see #351")];

    ensure_table_has_a_mappable_column(&logical_schema, &refused_columns)
        .expect("a table with a mappable column must not be refused as a whole");
}

/// A table declaring no column at all trivially satisfies "every column is mappable" — there is
/// no refused column to justify a whole-table refusal.
#[test]
fn a_table_with_no_columns_and_no_refusals_is_not_refused_as_a_whole() {
    ensure_table_has_a_mappable_column(&[], &[])
        .expect("an empty schema with nothing refused must not be refused as a whole");
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
