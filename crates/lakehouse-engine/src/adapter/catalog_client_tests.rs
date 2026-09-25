use super::*;

type CatalogClientConstruction = fn(
    CatalogKind,
    String,
    StorageBackend,
    ConnectionCreds,
    &Json,
) -> Result<Box<dyn CatalogClient>, UdfError>;

/// Scenario: the listing pipeline's signature names neither `CatalogKind` nor a `CatalogClient`.
#[test]
fn construction_site_is_exhaustive_and_fallible_for_three_kinds() {
    let _pipeline: fn(
        &[String],
        &CatalogListing,
        EngineTimestampSupport,
    ) -> Result<VirtualTables, UdfError> = build_listing_virtual_tables;
    let _constructor: CatalogClientConstruction = construct_catalog_client;
}

/// Scenario: both catalog kinds resolve their tables through the one shared listing pipeline.
#[test]
fn both_kinds_share_one_listing_pipeline() {
    use iceberg::spec::{PrimitiveType, Type};
    use lakehouse_catalog::{
        CatalogColumn, CatalogTable, CatalogTableType, ColumnSourceType, TableFormat,
    };

    let configured_ns = vec!["cat".to_string(), "sch".to_string()];

    let one_table_listing = |format: TableFormat, source_type: ColumnSourceType| CatalogListing {
        tables: vec![CatalogTable {
            ident: CatalogTableIdent {
                namespace: vec!["cat".to_string(), "sch".to_string()],
                name: "orders".to_string(),
            },
            table_type: CatalogTableType::Table,
            storage_location: None,
            format,
            vended_credential_key: None,
            columns: vec![CatalogColumn {
                name: "order_id".to_string(),
                source_type,
            }],
        }],
        skipped: Vec::new(),
    };

    let iceberg_listing = one_table_listing(
        TableFormat::Iceberg,
        ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long)),
    );
    let unity_listing = one_table_listing(
        TableFormat::Delta,
        ColumnSourceType::Unity {
            type_name: "LONG".to_string(),
            precision: 0,
            scale: 0,
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

/// Scenario: the listing pipeline declares a timestamp column at the precision it is handed.
#[test]
fn build_listing_virtual_tables_declares_timestamp_at_the_given_precision() {
    use iceberg::spec::{PrimitiveType, Type};
    use lakehouse_catalog::{
        CatalogColumn, CatalogTable, CatalogTableType, ColumnSourceType, TableFormat,
    };

    let configured_ns = vec!["cat".to_string(), "sch".to_string()];
    let listing = CatalogListing {
        tables: vec![CatalogTable {
            ident: CatalogTableIdent {
                namespace: vec!["cat".to_string(), "sch".to_string()],
                name: "events".to_string(),
            },
            table_type: CatalogTableType::Table,
            storage_location: None,
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            columns: vec![
                CatalogColumn {
                    name: "ts".to_string(),
                    source_type: ColumnSourceType::Iceberg(Type::Primitive(
                        PrimitiveType::Timestamp,
                    )),
                },
                CatalogColumn {
                    name: "delta_ts".to_string(),
                    source_type: ColumnSourceType::Unity {
                        type_name: "TIMESTAMP".to_string(),
                        precision: 0,
                        scale: 0,
                    },
                },
            ],
        }],
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
