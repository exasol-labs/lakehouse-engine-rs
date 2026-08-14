use super::*;

/// The listing pipeline cannot branch on the catalog kind.
///
/// This is a compile-time surface probe: `build_listing_virtual_tables` compiles
/// with a signature naming neither `CatalogKind` nor a `CatalogClient`, so the
/// listing pipeline is structurally incapable of consulting the kind or a client;
/// `construct_catalog_client` is the listing path's sole kind→client construction
/// site. The probe pins only these two signatures — it does not (and cannot) prove
/// the kind is matched nowhere else. `validate_creds` and the pushdown-path early
/// refusal legitimately consult the kind, both OUTSIDE this listing pipeline.
#[test]
fn catalog_kind_is_matched_only_at_the_construction_site() {
    let _pipeline: fn(&[String], &CatalogListing) -> Result<VirtualTables, UdfError> =
        build_listing_virtual_tables;
    let _constructor: fn(
        CatalogKind,
        String,
        StorageBackend,
        ConnectionCreds,
    ) -> Box<dyn CatalogClient> = construct_catalog_client;
}

/// Both catalog kinds resolve their tables through the ONE shared listing
/// pipeline: fed two listings that carry the same table and column names and
/// differ only in the table's format tag and each column's source-tagged type —
/// exactly what a real Iceberg vs Unity client attaches — the pipeline produces
/// the same Exasol table name, the same case-folded column name, and the same
/// `TABLE_MAP`.
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

    let (ib_tables, ib_map, _) =
        build_listing_virtual_tables(&configured_ns, &iceberg_listing).unwrap();
    let (uc_tables, uc_map, _) =
        build_listing_virtual_tables(&configured_ns, &unity_listing).unwrap();

    // One pipeline: identical flatten, case-fold, and TABLE_MAP for both kinds.
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

    // The ONE type-mapping home maps `LONG` identically for both source kinds.
    assert_eq!(
        ib_tables[0]["columns"][0]["dataType"],
        uc_tables[0]["columns"][0]["dataType"]
    );
}
