use super::*;
use crate::adapter::pushdown::test_support::{iceberg_column, named_table, unity_column};
use lakehouse_catalog::{CatalogColumn, ColumnSourceType, TableFormat};

type CatalogClientConstruction = fn(
    CatalogKind,
    String,
    StorageBackend,
    ConnectionCreds,
    &Json,
) -> Result<Box<dyn CatalogClient>, UdfError>;

#[test]
fn construction_site_is_exhaustive_and_fallible_for_every_kind() {
    let _pipeline: fn(
        &[String],
        &CatalogListing,
        EngineTimestampSupport,
    ) -> Result<VirtualTables, UdfError> = build_listing_virtual_tables;
    let _constructor: CatalogClientConstruction = construct_catalog_client;

    let glue_creds = |region: &str| ConnectionCreds {
        region: region.to_string(),
        access_key: "AKID".to_string(),
        secret_key: "SECRET".to_string(),
        use_sigv4: true,
        ..ConnectionCreds::default()
    };
    let construct = |address: &str, creds: ConnectionCreds| {
        let storage = connection::storage_block(&creds, false);
        construct_catalog_client(
            CatalogKind::Glue,
            address.to_string(),
            storage,
            creds,
            &json!({}),
        )
    };

    assert!(construct("https://glue.us-east-1.amazonaws.com", glue_creds("")).is_ok());
    assert!(construct("https://glue.example.test", glue_creds("eu-west-1")).is_ok());
    let err = construct("https://glue.example.test", glue_creds(""))
        .err()
        .expect("a Glue address with no region must fail construction")
        .to_string();
    assert!(!err.contains("SECRET"), "{err}");
}

#[test]
fn both_kinds_share_one_listing_pipeline() {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    let configured_ns = vec!["cat".to_string(), "sch".to_string()];

    let one_table_listing = |format: TableFormat, source_type: ColumnSourceType| CatalogListing {
        tables: vec![named_table(
            &["cat", "sch"],
            "orders",
            format,
            vec![CatalogColumn::new("order_id", source_type)],
        )],
        skipped: Vec::new(),
    };

    let iceberg_listing = one_table_listing(
        TableFormat::Iceberg,
        ColumnSourceType::Iceberg(
            NestedField::optional(1, "order_id", Type::Primitive(PrimitiveType::Long)).into(),
        ),
    );
    let unity_listing = one_table_listing(
        TableFormat::Delta,
        ColumnSourceType::Unity {
            type_json: Some(r#"{"name":"id","type":"long","nullable":true,"metadata":{}}"#.into()),
        },
    );

    let (ib_tables, ib_map, _) = build_listing_virtual_tables(
        &configured_ns,
        &iceberg_listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .unwrap();
    let (uc_tables, uc_map, _) = build_listing_virtual_tables(
        &configured_ns,
        &unity_listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .unwrap();

    assert_eq!(ib_tables[0]["name"], "ORDERS");
    assert_eq!(ib_tables[0]["name"], uc_tables[0]["name"]);
    assert_eq!(ib_tables[0]["columns"][0]["name"], "ORDER_ID");
    assert_eq!(
        ib_tables[0]["columns"][0]["name"],
        uc_tables[0]["columns"][0]["name"]
    );
    assert_eq!(
        ib_map,
        vec![("ORDERS".to_string(), "cat.sch.orders".to_string())]
    );
    assert_eq!(ib_map, uc_map);

    assert_eq!(
        ib_tables[0]["columns"][0]["dataType"],
        uc_tables[0]["columns"][0]["dataType"]
    );
}

#[test]
fn build_listing_virtual_tables_declares_timestamp_at_the_given_precision() {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    let configured_ns = vec!["cat".to_string(), "sch".to_string()];
    let ts = NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp));
    let delta_ts = r#"{"name":"delta_ts","type":"timestamp","nullable":true,"metadata":{}}"#;
    let listing = CatalogListing {
        tables: vec![named_table(
            &["cat", "sch"],
            "events",
            TableFormat::Iceberg,
            vec![
                iceberg_column(&ts.into()),
                unity_column("delta_ts", Some(delta_ts.into())),
            ],
        )],
        skipped: Vec::new(),
    };

    let cases = [
        (
            EngineTimestampSupport::DeclaredPrecision,
            json!({"type": "timestamp", "fractionalSecondsPrecision": 6}),
        ),
        (
            EngineTimestampSupport::MillisecondOnly,
            json!({"type": "timestamp"}),
        ),
    ];
    for (engine, expected) in cases {
        let (tables, _, _) =
            build_listing_virtual_tables(&configured_ns, &listing, engine).unwrap();
        let columns = tables[0]["columns"].as_array().unwrap();
        assert_eq!(columns[0]["dataType"], expected, "iceberg on {engine:?}");
        assert_eq!(columns[1]["dataType"], expected, "delta on {engine:?}");
    }
}

/// Direct-storage-shaped columns: only the slot can give a Parquet column a note.
fn declared_listing(declarations: Vec<Option<String>>) -> CatalogListing {
    let columns = declarations
        .into_iter()
        .enumerate()
        .map(|(index, declaration)| CatalogColumn {
            declaration,
            ..CatalogColumn::new(
                format!("c{index}"),
                ColumnSourceType::Parquet("int64".to_string()),
            )
        })
        .collect();
    CatalogListing {
        tables: vec![named_table(
            &["cat", "sch"],
            "sales",
            TableFormat::Parquet,
            columns,
        )],
        skipped: Vec::new(),
    }
}

fn listed_tables(listing: &CatalogListing) -> Result<Vec<Json>, UdfError> {
    let namespaces = vec!["cat".to_string(), "sch".to_string()];
    build_listing_virtual_tables(
        &namespaces,
        listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .map(|(tables, _, _)| tables)
}

#[test]
fn a_column_with_a_declaration_gets_its_note_and_one_without_gets_none() {
    let tables = listed_tables(&declared_listing(vec![
        Some(r#"{"sourceType":{}}"#.to_string()),
        None,
    ]))
    .unwrap();

    let columns = tables[0]["columns"].as_array().unwrap();
    assert_eq!(columns[0]["adapterNotes"], r#"{"sourceType":{}}"#);
    assert!(columns[1].get("adapterNotes").is_none(), "{columns:?}");
}
