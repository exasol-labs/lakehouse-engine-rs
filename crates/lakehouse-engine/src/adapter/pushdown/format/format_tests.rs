use super::super::test_support::sample_storage;
use super::*;
use crate::adapter::parquet_directory::MergeMode;
use lakehouse_catalog::{CatalogTableIdent, CatalogTableType};

/// Credentials whose SigV4 mode lets `CatalogSession::resolve` build a session
/// without contacting a catalog, so selection is exercised against a closed port.
fn offline_sigv4_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "signing-access-key".into(),
        secret_key: "signing-secret-key".into(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: true,
        use_vended_credentials: false,
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

/// A closed port: any request selection issued would fail loudly rather than
/// silently succeed against a real catalog.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

const TABLE_NAME: &str = "cat.sch.orders";

/// One loaded Unity Catalog table reporting `format`.
fn unity_table(format: TableFormat) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "orders".into(),
        },
        table_type: CatalogTableType::Table,
        storage_location: Some("s3://bucket/cat/sch/orders".into()),
        format,
        vended_credential_key: Some("table-id-1".into()),
        partition_columns: Vec::new(),
        columns: Vec::new(),
    }
}

/// Scenario: The format reader is selected at one site and refuses a mismatched pairing
#[test]
fn format_reader_refuses_an_iceberg_table_under_the_unity_source() {
    let creds = offline_sigv4_creds();
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let table = unity_table(TableFormat::Iceberg);
    let storage = sample_storage();

    let err = format_reader(
        ScanSource::Unity {
            session: &session,
            table: &table,
        },
        &ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    )
    .err()
    .expect("a Unity Catalog table reporting a non-Delta format must be refused");

    let message = match err {
        UdfError::User(m) => m,
        other => panic!("a mismatched pairing must fail as a user error, got {other:?}"),
    };
    assert!(
        message.contains(TABLE_NAME),
        "the refusal must name the table it refused: {message}"
    );
    assert!(
        message.contains("Iceberg"),
        "the refusal must name the format the catalog reported: {message}"
    );
}

/// Scenario: The format reader is selected at one site and refuses a mismatched
/// pairing.
///
/// A Unity Catalog table reporting Delta passes the format check and selects its
/// reader — the refusal above is scoped to the mismatch and does not reject the
/// format this source exists to select. Selection issues no request: the catalog
/// URI names a closed port, so a selection site that resolved the table's log or
/// its credential over the network could not answer `Ok` here.
#[test]
fn format_reader_selects_the_delta_reader_for_a_delta_table_without_contacting_the_catalog() {
    let creds = offline_sigv4_creds();
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let storage = sample_storage();

    for format in [TableFormat::Delta, TableFormat::Parquet] {
        let table = unity_table(format);
        let selected = format_reader(
            ScanSource::Unity {
                session: &session,
                table: &table,
            },
            &ConnectionStorage {
                storage: &storage,
                creds: &creds,
                allow_http: true,
            },
        );

        assert!(
            selected.is_ok(),
            "a Unity Catalog table reporting {format:?} must select its reader without issuing \
             a request"
        );
    }
}

/// Scenario: The format reader is selected at one site and refuses a mismatched
/// pairing.
///
/// An Iceberg REST source selects its reader with no catalog request: the
/// catalog URI names a closed port, so a selection site that resolved anything
/// over the network could not answer `Ok` here.
#[tokio::test]
async fn format_reader_selects_an_iceberg_source_without_contacting_the_catalog() {
    let creds = offline_sigv4_creds();
    let session = CatalogSession::resolve(UNREACHABLE_CATALOG, &creds.warehouse, &creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let catalog_props = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: "db.t".into(),
    };
    let storage = sample_storage();

    let selected = format_reader(
        ScanSource::Iceberg {
            session: &session,
            catalog_props: &catalog_props,
        },
        &ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    );

    assert!(
        selected.is_ok(),
        "an Iceberg REST source must select its reader without issuing a request"
    );
}

// Selecting the reader must not touch the store: it holds no object, so listing or
// footer-parsing here would fail.
#[test]
fn third_scan_source_selects_the_parquet_reader() {
    let creds = offline_sigv4_creds();
    let storage = sample_storage();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let selected = format_reader(
        ScanSource::DirectParquet {
            store: &store,
            table_root: "s3://warehouse/direct/events",
            options: DirectoryOptions {
                merge_mode: MergeMode::FoldEveryFile,
                hive_partitioning: true,
            },
            declared_columns: &[],
        },
        &ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    );

    assert!(
        selected.is_ok(),
        "a raw Parquet directory must select its reader without reading anything"
    );
}

// Scenario Coverage (delta-type-mapping): A Delta table with no mappable column is refused as a
// whole
#[test]
fn a_table_is_refused_as_a_whole_only_when_every_column_is_refused() {
    let refused = |column_name: &str, reason: &str| RefusedColumn {
        column_name: column_name.to_string(),
        reason: reason.to_string(),
    };
    let refused_columns = vec![
        refused("binary_col", "binary is refused, see #351"),
        refused("variant_col", "variant renders no meaningful value"),
    ];
    let id = LogicalField {
        field_id: None,
        name: "id".to_string(),
        arrow_type: "int64".to_string(),
        nullable: false,
        initial_default: None,
        nested: None,
        physical_name: None,
    };

    ensure_table_has_a_mappable_column(&[id], &refused_columns[..1], "Delta")
        .expect("a table with a mappable column must not be refused as a whole");
    ensure_table_has_a_mappable_column(&[], &[], "Delta")
        .expect("an empty schema with nothing refused must not be refused as a whole");
    let error = ensure_table_has_a_mappable_column(&[], &refused_columns, "Delta")
        .expect_err("a table with zero mappable columns must be refused as a whole");

    let message = match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    };
    assert!(
        message.starts_with("Delta table has no mappable column; every column is refused: "),
        "message was: {message}"
    );
    for fragment in [
        "binary_col",
        "binary is refused, see #351",
        "variant_col",
        "variant renders no meaningful value",
    ] {
        assert!(message.contains(fragment), "message was: {message}");
    }
}
