use super::super::test_support::{
    glue_catalog_table, object_endpoint, sample_storage, user_message,
};
use super::*;
use crate::adapter::parquet_directory::MergeMode;
use lakehouse_catalog::{CatalogTableIdent, CatalogTableType};

/// SigV4 mode lets `CatalogSession::resolve` build a session without contacting a catalog.
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

/// Closed port: any request selection issued would fail loudly.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

const TABLE_NAME: &str = "cat.sch.orders";

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
        metadata_location: None,
        columns: Vec::new(),
    }
}

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

    let message = user_message(err);
    assert!(
        message.contains(TABLE_NAME),
        "the refusal must name the table it refused: {message}"
    );
    assert!(
        message.contains("Iceberg"),
        "the refusal must name the format the catalog reported: {message}"
    );
}

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

fn glue_table(format: TableFormat) -> CatalogTable {
    CatalogTable {
        metadata_location: Some("s3://bucket/sales/orders/metadata/v1.json".into()),
        ..glue_catalog_table(
            "sales",
            "orders",
            format,
            "s3://bucket/sales/orders",
            &[("id", "int")],
        )
    }
}

fn offline_glue_session() -> GlueCatalogSession {
    GlueCatalogSession::new(UNREACHABLE_CATALOG, sample_storage(), offline_sigv4_creds())
        .expect("the CONNECTION region signs the session without a request")
}

/// Scenario: A Glue Parquet table is planned by the shared catalog-declared Parquet reader
#[tokio::test]
async fn format_reader_selects_readers_for_a_glue_table_by_format_without_contacting_the_catalog() {
    let creds = offline_sigv4_creds();
    let session = offline_glue_session();
    let storage = object_endpoint(
        "bucket",
        vec![("sales/orders/part-0".to_string(), "rows".to_string())],
    )
    .await;

    for format in [TableFormat::Iceberg, TableFormat::Parquet] {
        let table = glue_table(format);
        let reader = format_reader(
            ScanSource::Glue {
                session: &session,
                table: &table,
            },
            &ConnectionStorage {
                storage: &storage,
                creds: &creds,
                allow_http: true,
            },
        )
        .expect("a Glue table must select its reader without issuing a request");

        let resolved = reader.resolve_scan(None).await;

        match format {
            TableFormat::Parquet => {
                let scan = resolved.expect("the Parquet reader lists the table location");
                let paths: Vec<&str> = scan.files.iter().map(|file| file.path.as_str()).collect();
                assert_eq!(paths, vec!["part-0"]);
            }
            _ => {
                let error = resolved
                    .expect_err("the Iceberg reader reads the metadata file, which is absent")
                    .to_string();
                assert!(
                    error.contains("s3://bucket/sales/orders/metadata/v1.json"),
                    "{error}"
                );
            }
        }
    }
}

#[test]
fn format_reader_refuses_a_delta_table_under_the_glue_source() {
    let creds = offline_sigv4_creds();
    let session = offline_glue_session();
    let table = glue_table(TableFormat::Delta);
    let storage = sample_storage();

    let err = format_reader(
        ScanSource::Glue {
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
    .expect("no Glue reader plans a Delta table");

    let message = user_message(err);
    assert!(message.contains("sales.orders"), "{message}");
    assert!(message.contains("Delta"), "{message}");
}

/// Scenario: A Delta table with no mappable column is refused as a whole
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

    let message = user_message(error);
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
