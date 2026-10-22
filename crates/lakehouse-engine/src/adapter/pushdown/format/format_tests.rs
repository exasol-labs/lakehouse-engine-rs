use super::super::test_support::{
    UNREACHABLE_CATALOG, catalog_table, declared_columns, declared_schema_of, glue_catalog_table,
    load_table_body, no_notes, object_endpoint, sample_storage, sigv4_creds, user_message,
};
use super::*;
use lakehouse_catalog::CatalogTableIdent;

const TABLE_NAME: &str = "cat.sch.orders";

fn unity_table(format: TableFormat) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "orders".into(),
        },
        vended_credential_key: Some("table-id-1".into()),
        ..catalog_table(format, "s3://bucket/cat/sch/orders")
    }
}

#[test]
fn format_reader_refuses_an_iceberg_table_under_the_unity_source() {
    let creds = sigv4_creds();
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
    let creds = sigv4_creds();
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
    let creds = sigv4_creds();
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

/// Scenario: The format reader is selected at one exhaustive site for a direct-storage source
#[test]
fn third_scan_source_selects_the_parquet_reader() {
    let creds = sigv4_creds();
    let storage = sample_storage();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    let selected = format_reader(
        ScanSource::DirectParquet {
            store: &store,
            table_root: "s3://warehouse/direct/events",
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
    GlueCatalogSession::new(UNREACHABLE_CATALOG, sample_storage(), sigv4_creds())
        .expect("the CONNECTION region signs the session without a request")
}

/// Scenario: A Glue Parquet table is planned by the shared catalog-declared Parquet reader
#[tokio::test]
async fn format_reader_selects_readers_for_a_glue_table_by_format_without_contacting_the_catalog() {
    let creds = sigv4_creds();
    let session = offline_glue_session();
    let metadata = load_table_body(serde_json::json!({}))["metadata"].to_string();
    let storage = object_endpoint(
        "bucket",
        vec![
            ("sales/orders/part-0".to_string(), "rows".to_string()),
            ("sales/orders/metadata/v1.json".to_string(), metadata),
        ],
    )
    .await;

    for (format, files) in [
        (TableFormat::Iceberg, vec![]),
        (TableFormat::Parquet, vec!["part-0"]),
    ] {
        let table = glue_table(format);
        let declared = match format {
            TableFormat::Parquet => declared_schema_of(&table),
            _ => no_notes(),
        };
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

        let scan = reader
            .resolve_scan(&declared, None)
            .await
            .unwrap_or_else(|error| panic!("{format:?}: the selected reader plans: {error}"));

        let paths: Vec<&str> = scan.files.iter().map(|file| file.path.as_str()).collect();
        assert_eq!(
            paths, files,
            "{format:?}: only the Parquet reader lists the location's direct children; the \
             Iceberg reader plans the snapshotless metadata file"
        );
    }
}

#[test]
fn format_reader_refuses_a_delta_table_under_the_glue_source() {
    let creds = sigv4_creds();
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

#[test]
fn declare_columns_notes_each_catalog_parquet_column_with_its_partition_position_and_refusal() {
    let table = CatalogTable {
        partition_columns: vec!["year".into(), "region".into()],
        ..glue_catalog_table(
            "sales",
            "orders",
            TableFormat::Parquet,
            "s3://bucket/sales/orders",
            &[
                ("id", "bigint"),
                ("region", "string"),
                ("year", "int"),
                ("blob", "binary"),
            ],
        )
    };

    let declarations = declared_columns(&table);

    let declared: Vec<(&str, Option<usize>, bool)> = declarations
        .iter()
        .map(|declaration| {
            let field = &declaration.source_type;
            (
                field.name.as_str(),
                declaration.partition,
                declaration.refused.is_some(),
            )
        })
        .collect();
    assert_eq!(
        declared,
        vec![
            ("id", None, false),
            ("region", Some(1), false),
            ("year", Some(0), false),
            ("blob", None, true),
        ]
    );
    assert!(
        declarations
            .iter()
            .all(|declaration| declaration.source_type.nullable)
    );
}
