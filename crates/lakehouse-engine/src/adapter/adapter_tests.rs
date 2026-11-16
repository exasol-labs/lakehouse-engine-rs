use super::*;
use crate::adapter::properties::PROP_DF_BATCH_SIZE;
use crate::adapter::pushdown::test_support::{
    NO_COLUMNS, customer_orders_join, pushdown_request, table_col,
};
use crate::adapter::pushdown::test_support::{
    named_table, with_delta_notes, with_iceberg_notes, with_identifier_notes,
};
use crate::adapter::tuning::DEFAULT_METADATA_READ_CONCURRENCY;
use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::test_support::{DefaultsCtx, TestContext};
use lakehouse_catalog::{CatalogTableIdent, SkipReason, TableFormat};
use std::sync::{Arc, Mutex};

#[path = "parquet_fixture_tests.rs"]
pub(super) mod parquet_fixture;
#[path = "recording_store_tests.rs"]
pub(super) mod recording_store;

#[test]
fn dispatch_get_capabilities() {
    let req = serde_json::json!({"type": "getCapabilities"});
    let resp = dispatch(&mut DefaultsCtx, &req).unwrap();
    assert_eq!(resp["type"].as_str().unwrap(), "getCapabilities");
    let caps = resp["capabilities"].as_array().unwrap();
    assert!(!caps.is_empty());
}

#[test]
fn dispatch_drop_returns_correct_type() {
    let req = serde_json::json!({"type": "dropVirtualSchema"});
    let resp = dispatch(&mut DefaultsCtx, &req).unwrap();
    assert_eq!(resp["type"].as_str().unwrap(), "dropVirtualSchema");
}

#[test]
fn dispatch_unknown_type_errors() {
    let req = serde_json::json!({"type": "unsupported"});
    let err = dispatch(&mut DefaultsCtx, &req).unwrap_err();
    assert!(err.to_string().contains("unsupported"));
}

#[test]
fn refresh_and_set_properties_dispatched_not_unsupported() {
    for req_type in ["refresh", "setProperties"] {
        let req = serde_json::json!({
            "type": req_type,
            "properties": { PROP_CATALOG_CONNECTION: "no_such_conn" },
        });
        let err = dispatch(&mut DefaultsCtx, &req)
            .expect_err("no live catalog is available in a unit test");
        assert!(
            !err.to_string().contains("unsupported"),
            "{req_type} must not be rejected as an unsupported request type, got: {err}"
        );
    }
}

fn schema_request(request_type: &str, properties: serde_json::Value) -> Json {
    match request_type {
        "setProperties" => serde_json::json!({ "type": request_type, "properties": properties }),
        _ => serde_json::json!({
            "type": request_type,
            "schemaMetadataInfo": { "properties": properties },
        }),
    }
}

#[track_caller]
fn assert_every_schema_request_fails_before_the_connection_is_read(
    property: &str,
    value: &str,
    expected_entry: &str,
) {
    for request_type in ["createVirtualSchema", "refresh", "setProperties"] {
        let request = schema_request(
            request_type,
            serde_json::json!({
                PROP_CATALOG_CONNECTION: "NO_SUCH_CONNECTION",
                property: value,
            }),
        );

        let message = dispatch(&mut TestContext::scalar(vec![]), &request)
            .expect_err("an invalid property must fail the request")
            .to_string();

        assert!(
            message.contains(expected_entry),
            "{request_type}: {message}"
        );
        assert!(
            !message.contains("NO_SUCH_CONNECTION"),
            "{request_type} must fail before it reads the CONNECTION: {message}"
        );
    }
}

#[test]
fn an_unknown_property_name_fails_every_schema_request_before_the_connection_is_read() {
    assert_every_schema_request_fails_before_the_connection_is_read(
        "NAMESPCE",
        "sales",
        "unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?)",
    );
}

#[test]
fn a_misspelled_required_property_is_reported_instead_of_the_missing_one() {
    let request = schema_request(
        "createVirtualSchema",
        serde_json::json!({ "CATALOG_CONECTION": "MY_CONN", PROP_NAMESPACE: "sales" }),
    );

    let message = dispatch(&mut TestContext::scalar(vec![]), &request)
        .expect_err("a misspelled CATALOG_CONNECTION must fail")
        .to_string();

    assert!(
        message
            .contains("unknown property 'CATALOG_CONECTION' (did you mean 'CATALOG_CONNECTION'?)"),
        "{message}"
    );
    assert!(
        !message.contains("CATALOG_CONNECTION is required"),
        "the name check precedes the required-property check: {message}"
    );
}

#[test]
fn an_invalid_tuning_value_fails_every_schema_request_before_the_connection_is_read() {
    assert_every_schema_request_fails_before_the_connection_is_read(
        PROP_DF_BATCH_SIZE,
        "0",
        "invalid value '0' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295",
    );
}

#[test]
fn a_set_properties_null_unsets_an_invalid_value_before_the_check() {
    let request = serde_json::json!({
        "type": "setProperties",
        "properties": { PROP_DF_BATCH_SIZE: null },
        "schemaMetadataInfo": { "properties": { PROP_DF_BATCH_SIZE: "0", "PV_MADE_UP": "x" } },
    });

    let message = dispatch(&mut TestContext::scalar(vec![]), &request)
        .expect_err("the stray name is still unknown")
        .to_string();

    assert!(
        message.contains("unknown property 'PV_MADE_UP'"),
        "{message}"
    );
    assert!(
        !message.contains("DATAFUSION_BATCH_SIZE"),
        "a null removes the invalid value from the effective properties: {message}"
    );
}

#[test]
fn a_pushdown_never_runs_the_property_check() {
    let request = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": {
            "name": "PROPERTY_CHECK_VS",
            "properties": {
                PROP_CATALOG_CONNECTION: "NO_SUCH_CONNECTION",
                "NAMESPCE": "sales",
                PROP_DF_BATCH_SIZE: "0",
            },
        },
    });

    let message = dispatch(&mut TestContext::scalar(vec![]), &request)
        .expect_err("the CONNECTION does not exist")
        .to_string();

    assert!(
        !message.contains("invalid virtual schema properties"),
        "a pushdown reads the properties it needs and checks no other: {message}"
    );
}

#[test]
fn build_schema_response_type_mirrors_request() {
    let schema_metadata = serde_json::json!({"tables": [], "adapterNotes": "{}"});
    for req_type in ["createVirtualSchema", "refresh", "setProperties"] {
        let req = serde_json::json!({"type": req_type});
        let resp = build_schema_response(&req, schema_metadata.clone());
        assert_eq!(
            resp["type"].as_str(),
            Some(req_type),
            "response type must equal request type"
        );
    }
}

#[test]
fn build_schema_response_echoes_requested_tables_present_and_absent() {
    let schema_metadata = serde_json::json!({"tables": [], "adapterNotes": "{}"});

    let with = serde_json::json!({
        "type": "refresh",
        "requestedTables": ["T1", "T2"],
    });
    let resp = build_schema_response(&with, schema_metadata.clone());
    assert_eq!(
        resp["requestedTables"],
        serde_json::json!(["T1", "T2"]),
        "requestedTables must be echoed verbatim"
    );

    let without = serde_json::json!({"type": "refresh"});
    let resp = build_schema_response(&without, schema_metadata);
    assert!(
        resp.get("requestedTables").is_none(),
        "requestedTables must be omitted when the request did not include it"
    );
}

#[test]
fn merge_set_properties_new_wins_and_null_unsets() {
    let req = serde_json::json!({
        "type": "setProperties",
        "properties": {
            "NAMESPACE": "new_ns",
            "ALLOW_HTTP": null,
        },
        "schemaMetadataInfo": {
            "properties": {
                "NAMESPACE": "old_ns",
                "ALLOW_HTTP": "true",
                "CATALOG_CONNECTION": "keep_me",
            }
        },
    });
    let merged = merge_set_properties(&req);

    assert_eq!(merged["NAMESPACE"], "new_ns");
    assert!(
        merged.get("ALLOW_HTTP").is_none(),
        "a null request value must unset the property"
    );
    assert_eq!(merged["CATALOG_CONNECTION"], "keep_me");
}

fn password_connection(
    address: &str,
    password: impl Into<String>,
) -> exasol_udf_sdk::ConnectionObject {
    exasol_udf_sdk::ConnectionObject {
        kind: "PASSWORD".into(),
        address: address.into(),
        user: String::new(),
        password: password.into(),
    }
}

fn s3_style_password() -> String {
    serde_json::json!({
        "warehouse": "wh",
        "endpoint": "http://s3.example.com",
        "region": "us-east-1",
        "access_key": "AKID",
        "secret_key": "SECRET",
        "path_style": true,
    })
    .to_string()
}

#[test]
fn set_properties_null_unset_required_property_errors_not_panic() {
    let req = serde_json::json!({
        "type": "setProperties",
        "properties": {
            "NAMESPACE": null,
        },
        "schemaMetadataInfo": {
            "properties": {
                "NAMESPACE": "old_ns",
                "CATALOG_CONNECTION": "MY_CONN",
            }
        },
    });

    let err = dispatch(
        &mut TestContext::scalar(vec![]).with_connection(
            "MY_CONN",
            password_connection("http://catalog.example.com", s3_style_password()),
        ),
        &req,
    )
    .expect_err("null-unsetting a required property must error, not succeed");

    let expected = format!("property '{PROP_NAMESPACE}' is required");
    assert!(
        err.to_string().contains(&expected),
        "expected the required-property error '{expected}', got: {err}"
    );
}

#[test]
fn namespace_is_required_for_catalog_kinds_only() {
    let catalog_req = serde_json::json!({
        "type": "createVirtualSchema",
        "properties": { "CATALOG_CONNECTION": "MY_CONN" },
    });
    let catalog_err = dispatch(
        &mut TestContext::scalar(vec![]).with_connection(
            "MY_CONN",
            password_connection("http://catalog.example.com", s3_style_password()),
        ),
        &catalog_req,
    )
    .expect_err("ICEBERG_REST still requires NAMESPACE");
    let expected = format!("property '{PROP_NAMESPACE}' is required");
    assert!(
        catalog_err.to_string().contains(&expected),
        "expected the required-property error '{expected}', got: {catalog_err}"
    );

    let direct_password = serde_json::json!({
        "access_key": "AKID",
        "secret_key": "SECRET",
        "region": "us-east-1",
        "endpoint": CLOSED_PORT_ADDRESS,
        "path_style": true,
    })
    .to_string();
    let direct_req = serde_json::json!({
        "type": "createVirtualSchema",
        "properties": {
            "CATALOG_CONNECTION": "MY_CONN",
            "CATALOG_KIND": "DIRECT_STORAGE",
        },
    });
    let direct_err = dispatch(
        &mut TestContext::scalar(vec![]).with_connection(
            "MY_CONN",
            password_connection("s3://bucket/lake", direct_password),
        ),
        &direct_req,
    )
    .expect_err("no live object store is reachable in a unit test");
    assert!(
        !direct_err.to_string().contains(&expected),
        "DIRECT_STORAGE must not require NAMESPACE, got: {direct_err}"
    );
}

// A closed local port: connection refused, no DNS, no hang, so resolution fails fast.
const CLOSED_PORT_ADDRESS: &str = "http://127.0.0.1:1";

#[test]

fn unity_kind_pushdown_routes_to_the_unity_catalog_loader() {
    let (req, _) = pushdown_request("ORDERS", &[("ID", ExasolType::decimal(20, 0))])
        .request_field("type", "pushdown")
        .request_field(
            "schemaMetadataInfo",
            serde_json::json!({"name": "UNITY_ROUTING_VS"}),
        )
        .request_field(
            "properties",
            serde_json::json!({"CATALOG_KIND": "UNITY_CATALOG", "CATALOG_CONNECTION": "MY_CONN"}),
        )
        .build();
    let req = with_delta_notes(req, "ORDERS", &[("id", "long")], &[]);
    let req = with_identifier_notes(req, &[("ORDERS", "cat.sch.orders")]);

    let err = dispatch(
        &mut TestContext::scalar(vec![]).with_connection(
            "MY_CONN",
            password_connection(CLOSED_PORT_ADDRESS, s3_style_password()),
        ),
        &req,
    )
    .expect_err("no live Unity Catalog is reachable in a unit test");

    let message = err.to_string();
    assert!(
        message.contains("Unity Catalog load table request failed"),
        "must reach the Unity Catalog load-table call, got: {message}"
    );
    assert!(
        message.contains("unity-catalog/tables"),
        "must name the Unity Catalog load-table endpoint: {message}"
    );
    assert!(
        !message.contains("not yet supported"),
        "the removed pushdown-path refusal must not resurface: {message}"
    );
}

#[test]
fn cluster_nodes_from_context_defaults_to_one_when_node_count_zero() {
    assert_eq!(cluster_nodes_from_context(&DefaultsCtx), 1usize);
    assert_eq!(
        cluster_nodes_from_context(&TestContext::scalar(vec![]).with_node_count(0)),
        1usize
    );
}

#[test]
fn cluster_nodes_from_context_passes_through_reported_node_count() {
    assert_eq!(
        cluster_nodes_from_context(&TestContext::scalar(vec![]).with_node_count(4)),
        4usize
    );
}

fn parsed_notes(request: &Json, skipped: &[SkippedTable]) -> Json {
    let notes = build_adapter_notes(request, &SkippedTables::listed(skipped), None);
    serde_json::from_str(notes.as_str().expect("adapterNotes is a JSON string"))
        .expect("adapterNotes is valid JSON")
}

#[test]
fn build_adapter_notes_writes_or_removes_the_catalog_prefix() {
    let refresh = json!({
        "type": "refresh",
        "schemaMetadataInfo": {"adapterNotes": json!({NOTE_CATALOG_PREFIX: "stale"}).to_string()},
    });
    for (prefix, expected) in [
        (Some("ns-prefix"), json!("ns-prefix")),
        (Some(""), json!("")),
        (None, Json::Null),
    ] {
        let notes = build_adapter_notes(
            &refresh,
            &SkippedTables::default(),
            prefix.map(str::to_string),
        );
        let parsed: Json = serde_json::from_str(notes.as_str().unwrap()).unwrap();

        assert_eq!(parsed[NOTE_CATALOG_PREFIX], expected, "prefix {prefix:?}");
    }
}

#[test]
fn stored_catalog_prefix_keeps_an_empty_prefix() {
    let request = |notes: Json| json!({"schemaMetadataInfo": {"adapterNotes": notes.to_string()}});

    assert_eq!(
        stored_catalog_prefix(&request(json!({NOTE_CATALOG_PREFIX: ""}))),
        Some(String::new())
    );
    assert_eq!(
        stored_catalog_prefix(&request(json!({"PARALLELISM_FACTOR": "1"}))),
        None,
        "a schema created before the note existed discovers the prefix"
    );
}

#[test]
fn build_adapter_notes_merges_existing() {
    let req = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"OTHER_KEY\":\"keep-me\"}"
        },
    });
    let parsed = parsed_notes(&req, &[]);
    assert_eq!(
        parsed["OTHER_KEY"].as_str(),
        Some("keep-me"),
        "pre-existing adapterNotes keys must be preserved"
    );
}

/// Scenario: Refresh rebuilds every identifier note and preserves other adapter notes
#[test]
fn refresh_writes_an_identifier_note_on_every_returned_table() {
    let request = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": serde_json::json!({"OTHER_KEY": "keep-me"}).to_string(),
        },
    });
    let listing = CatalogListing {
        tables: vec![
            named_table(&["ns"], "orders", TableFormat::Iceberg, Vec::new()),
            named_table(&["ns", "eu"], "Events", TableFormat::Iceberg, Vec::new()),
        ],
        skipped: Vec::new(),
    };
    let tables = build_listing_virtual_tables(
        &["ns".to_string()],
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .unwrap();

    let response = build_schema_response(
        &request,
        json!({
            "tables": tables,
            "adapterNotes": build_adapter_notes(
                &request,
                &SkippedTables::listed(&listing.skipped),
                None,
            ),
        }),
    );

    let returned = response["schemaMetadata"]["tables"].as_array().unwrap();
    let notes: Vec<(&str, Json)> = returned
        .iter()
        .map(|table| {
            let note = table["adapterNotes"]
                .as_str()
                .expect("a note on every table");
            (
                table["name"].as_str().unwrap(),
                serde_json::from_str(note).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        notes,
        vec![
            ("ORDERS", json!({"name": "ns.orders"})),
            ("EU__EVENTS", json!({"name": "ns.eu.Events"})),
        ]
    );
    let schema_notes: Json =
        serde_json::from_str(response["schemaMetadata"]["adapterNotes"].as_str().unwrap()).unwrap();
    assert_eq!(schema_notes["OTHER_KEY"], "keep-me");
}

fn pushdown_request_involving(involved: &str) -> Json {
    pushdown_request(involved, NO_COLUMNS)
        .request_field("type", "pushdown")
        .build()
        .0
}

/// Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table
#[test]
fn pushdown_without_an_identifier_note_fails_naming_refresh() {
    let mut request = pushdown_request_involving("UNKNOWN_TABLE");
    request["involvedTables"][0]
        .as_object_mut()
        .expect("an involved table")
        .remove("adapterNotes");

    let message = resolve_pushdown_identifier(&request)
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("UNKNOWN_TABLE") && message.contains("ALTER VIRTUAL SCHEMA ... REFRESH"),
        "error must name the table and the refresh: {message}"
    );
}

#[test]
fn pushdown_known_involved_table_resolves_identifier() {
    let request = with_identifier_notes(
        pushdown_request_involving("ORDERS"),
        &[("ORDERS", "prod.finance.orders")],
    );

    assert_eq!(
        resolve_pushdown_identifier(&request).unwrap(),
        "prod.finance.orders",
        "ORDERS must resolve to prod.finance.orders"
    );
}

/// Scenario: Table naming and the identifier note round trip reuse the shared helpers
#[test]
fn the_identifier_note_records_the_bare_directory_name_and_round_trips() {
    let listing = CatalogListing {
        tables: vec![
            named_table(&[], "Orders", TableFormat::Iceberg, Vec::new()),
            named_table(&[], "events", TableFormat::Iceberg, Vec::new()),
        ],
        skipped: Vec::new(),
    };
    let tables =
        build_listing_virtual_tables(&[], &listing, EngineTimestampSupport::MillisecondOnly)
            .unwrap();

    assert_eq!(
        resolve_pushdown_identifier(&pushdown_over(&tables, "ORDERS")).unwrap(),
        "Orders",
        "the recorded bare directory name round-trips through pushdown resolution"
    );
}

/// A pushdown request over the one listed virtual table named `name`.
fn pushdown_over(tables: &impl serde::Serialize, name: &str) -> Json {
    let tables = serde_json::to_value(tables).unwrap();
    let table = tables
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["name"] == name)
        .unwrap_or_else(|| panic!("no listed table {name}: {tables}"))
        .clone();
    json!({"type": "pushdown", "involvedTables": [table]})
}

fn cat_ident(ns: &[&str], name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: ns.iter().map(|s| s.to_string()).collect(),
        name: name.to_string(),
    }
}

#[test]
fn create_vs_records_the_multi_level_identifier_in_the_table_note() {
    let listing = CatalogListing {
        tables: vec![
            named_table(
                &["prod", "finance"],
                "orders",
                TableFormat::Iceberg,
                Vec::new(),
            ),
            named_table(
                &["prod", "finance", "eu"],
                "orders",
                TableFormat::Iceberg,
                Vec::new(),
            ),
        ],
        skipped: Vec::new(),
    };
    let tables = build_listing_virtual_tables(
        &["prod".to_string(), "finance".to_string()],
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .unwrap();

    assert_eq!(
        resolve_pushdown_identifier(&pushdown_over(&tables, "ORDERS")).unwrap(),
        "prod.finance.orders"
    );
    assert_eq!(
        read_identifier(&pushdown_over(&tables, "EU__ORDERS"), "EU__ORDERS").unwrap(),
        "prod.finance.eu.orders"
    );
}

#[test]
fn a_flatten_collision_among_listed_tables_fails_the_listing() {
    let listing = CatalogListing {
        tables: vec![
            named_table(
                &["prod", "finance"],
                "eu__orders",
                TableFormat::Iceberg,
                Vec::new(),
            ),
            named_table(
                &["prod", "finance", "eu"],
                "orders",
                TableFormat::Iceberg,
                Vec::new(),
            ),
        ],
        skipped: Vec::new(),
    };
    let message = build_listing_virtual_tables(
        &["prod".to_string(), "finance".to_string()],
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .unwrap_err()
    .to_string();
    assert!(
        message.contains("EU__ORDERS") && message.contains("collision"),
        "{message}"
    );
}

#[test]
fn iceberg_listing_is_behavior_identical_behind_the_trait() {
    use crate::adapter::pushdown::test_support::{iceberg_column, named_table};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use lakehouse_catalog::{CatalogTable, TableFormat};

    let configured_ns = vec!["prod".to_string(), "finance".to_string()];
    let order_id = NestedField::required(1, "order_id", Type::Primitive(PrimitiveType::Long));
    let strasse = NestedField::optional(2, "straße", Type::Primitive(PrimitiveType::String));
    let listing = CatalogListing {
        tables: vec![CatalogTable {
            storage_location: Some("s3://warehouse/orders".to_string()),
            ..named_table(
                &["prod", "finance", "eu"],
                "orders",
                TableFormat::Iceberg,
                vec![
                    iceberg_column(&order_id.into()),
                    iceberg_column(&strasse.into()),
                ],
            )
        }],
        skipped: vec![SkippedTable {
            ident: cat_ident(&["prod", "finance"], "hive_events"),
            reason: SkipReason::NotLoadableIcebergTable,
        }],
    };

    let tables_json = build_listing_virtual_tables(
        &configured_ns,
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .unwrap();

    assert_eq!(tables_json.len(), 1);
    assert_eq!(tables_json[0]["name"], "EU__ORDERS");

    let columns = tables_json[0]["columns"].as_array().unwrap();
    assert_eq!(columns[0]["name"], "ORDER_ID");
    assert_eq!(
        columns[0]["dataType"],
        json!({"type": "decimal", "precision": 20, "scale": 0})
    );
    assert_eq!(columns[1]["name"], "STRASSE");
    assert_eq!(
        columns[1]["dataType"],
        json!({"type": "varchar", "size": 2000000})
    );

    assert_eq!(
        tables_json[0]["adapterNotes"],
        r#"{"name":"prod.finance.eu.orders"}"#
    );

    assert_eq!(
        listing.skipped,
        vec![SkippedTable {
            ident: cat_ident(&["prod", "finance"], "hive_events"),
            reason: SkipReason::NotLoadableIcebergTable,
        }]
    );
}

fn skipped_entry(table: &str, reason: SkipReason) -> SkippedTable {
    SkippedTable {
        ident: cat_ident(&["sales"], table),
        reason,
    }
}

/// Scenario: Every skipped table is recorded with its reason under every catalog kind
#[test]
fn skipped_tables_are_recorded_with_reasons_for_every_kind() {
    let skipped = vec![
        skipped_entry("hive_events", SkipReason::NotLoadableIcebergTable),
        skipped_entry(
            "summary",
            SkipReason::NotDeltaBaseTable {
                detail: "table_type=VIEW".to_string(),
            },
        ),
        skipped_entry("empty_dir", SkipReason::NoDataFile),
        skipped_entry(
            "orders_orc",
            SkipReason::NotPlannableGlueTable {
                detail: "InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".to_string(),
            },
        ),
        skipped_entry(
            "projected",
            SkipReason::NotPlannableGlueTable {
                detail: "projection.enabled=true".to_string(),
            },
        ),
    ];

    let parsed = parsed_notes(&json!({"type": "createVirtualSchema"}), &skipped);

    let recorded = parsed["SKIPPED_TABLES"].as_array().expect("an array");
    let expected = [
        (
            "sales.hive_events",
            "non-Iceberg table",
            "catalog reported it is not a loadable Iceberg table",
        ),
        ("sales.summary", "non-Delta-base entry", "table_type=VIEW"),
        ("sales.empty_dir", "directory", "holds no data file"),
        (
            "sales.orders_orc",
            "Glue table",
            "InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        ),
        ("sales.projected", "Glue table", "projection.enabled=true"),
    ];
    assert_eq!(recorded.len(), expected.len());
    for ((table, kind, reason), (entry, source)) in
        expected.iter().zip(recorded.iter().zip(&skipped))
    {
        assert_eq!(entry, &json!({"table": table, "reason": reason}));
        assert_eq!(
            skip_warning(source),
            format!("createVirtualSchema: skipping {kind} '{table}' ({reason})"),
            "the warning line must state the same reason as the note"
        );
    }
}

/// Scenario: A listing with no skip records an empty list that replaces the previous one
#[test]
fn a_listing_without_skips_records_an_empty_list_replacing_the_previous() {
    let request = json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": json!({
                "SKIPPED_TABLES": [
                    {"table": "sales.a", "reason": "holds no data file"},
                    {"table": "sales.b", "reason": "holds no data file"},
                ],
            })
            .to_string()
        },
    });

    let parsed = parsed_notes(&request, &[]);

    assert_eq!(parsed["SKIPPED_TABLES"], json!([]));
}

/// Scenario: A namespace whose every table is skipped still creates an empty virtual schema
#[test]
fn an_all_skipped_namespace_creates_an_empty_schema_recording_every_skip() {
    let listing = CatalogListing {
        tables: Vec::new(),
        skipped: vec![
            skipped_entry("a", SkipReason::NoDataFile),
            skipped_entry("b", SkipReason::NotLoadableIcebergTable),
        ],
    };

    let tables_json = build_listing_virtual_tables(
        &["sales".to_string()],
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .expect("an all-skipped namespace must not fail the listing");
    let parsed = parsed_notes(&json!({"type": "createVirtualSchema"}), &listing.skipped);

    assert!(tables_json.is_empty());
    assert_eq!(parsed["SKIPPED_TABLES"].as_array().unwrap().len(), 2);
}

fn resolved_for(password: Json) -> ResolvedConnectionConfig {
    resolve_config_over(
        "http://catalog.example.com",
        &password,
        &serde_json::json!({"CATALOG_CONNECTION": "MY_CONN"}),
    )
    .expect("the fixture password must be an acceptable CONNECTION")
}

/// `resolve_connection_config` over a `MY_CONN` CONNECTION, on a runtime of its own as each entry point builds one.
fn resolve_config_over(
    address: &str,
    password: &Json,
    props: &Json,
) -> Result<ResolvedConnectionConfig, UdfError> {
    let ctx = TestContext::scalar(vec![]).with_connection(
        "MY_CONN",
        password_connection(address, password.to_string()),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build the request runtime");
    let props = VsProperties::parse(props, Check::Lenient)?;
    resolve_connection_config(&ctx, &props, DEFAULT_METADATA_READ_CONCURRENCY, &rt)
}

#[test]
fn resolved_config_carries_the_catalog_connection_name() {
    let config = resolved_for(serde_json::json!({
        "warehouse": "wh", "region": "us-east-1",
        "access_key": "AK", "secret_key": "SK",
    }));
    assert_eq!(config.connection_name, "MY_CONN");
}

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/lakehouse-reader";
const BASE_AK: &str = "AKIABASEIDENTITY";
const BASE_SK: &str = "BASE_SECRET_SENTINEL";
const EXTERNAL_ID: &str = "EXTERNAL_ID_SENTINEL";
const SESSION_AK: &str = "ASIASESSIONKEYSENTINEL";
const SESSION_SK: &str = "SESSION_SECRET_SENTINEL";
const SESSION_TOKEN: &str = "SESSION_TOKEN_SENTINEL";

const ASSUME_ROLE_RESPONSE: &str = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIASESSIONKEYSENTINEL</AccessKeyId>
      <SecretAccessKey>SESSION_SECRET_SENTINEL</SecretAccessKey>
      <SessionToken>SESSION_TOKEN_SENTINEL</SessionToken>
      <Expiration>2026-09-28T13:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;

const ACCESS_DENIED_RESPONSE: &str = r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>AccessDenied</Code>
    <Message>User is not authorized to perform: sts:AssumeRole</Message>
  </Error>
</ErrorResponse>"#;

/// An empty namespace enumeration page, so a create lists zero tables and succeeds.
const EMPTY_LISTING: &str = r#"{"identifiers":[],"namespaces":[]}"#;

/// A loopback server standing in for both AWS STS and the catalog: an
/// STS-signed request is answered with `sts`, every other request with
/// `catalog`, and each request head is recorded in arrival order.
///
/// It runs on a runtime of its own because `dispatch` blocks on its own
/// current-thread runtime, which a `#[tokio::test]` would nest.
struct StsAndCatalog {
    uri: String,
    heads: Arc<Mutex<Vec<String>>>,
    _runtime: tokio::runtime::Runtime,
}

impl StsAndCatalog {
    fn start(sts: (u16, &'static str), catalog: (u16, &'static str)) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build the stub runtime");
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind a loopback port");
        let uri = format!("http://{}", listener.local_addr().expect("local_addr"));
        let heads = Arc::new(Mutex::new(Vec::new()));
        let recorded = heads.clone();
        runtime.spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut chunk = [0u8; 1024];
                while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut chunk).await.unwrap_or(0);
                    if read == 0 {
                        break;
                    }
                    head.extend_from_slice(&chunk[..read]);
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let (status, body) = if is_assume_role(&head) {
                    sts
                } else {
                    catalog
                };
                recorded.lock().unwrap().push(head);
                let response = format!(
                    "HTTP/1.1 {status} Stub\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        Self {
            uri,
            heads,
            _runtime: runtime,
        }
    }

    fn heads(&self) -> Vec<String> {
        self.heads.lock().unwrap().clone()
    }

    fn sts_requests(&self) -> usize {
        self.heads()
            .iter()
            .filter(|head| is_assume_role(head))
            .count()
    }

    fn catalog_requests(&self) -> Vec<String> {
        self.heads()
            .into_iter()
            .filter(|head| !is_assume_role(head))
            .collect()
    }
}

fn is_assume_role(head: &str) -> bool {
    head.lines()
        .any(|line| line.to_ascii_lowercase().contains("/sts/aws4_request"))
}

fn authorization(head: &str) -> &str {
    head.lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("authorization")
                .then(|| value.trim())
        })
        .unwrap_or_default()
}

/// A SigV4 Glue-style CONNECTION whose catalog is `stub`, naming no role.
fn sigv4_connection(stub: &StsAndCatalog) -> TestContext {
    TestContext::scalar(vec![]).with_connection(
        "MY_CONN",
        password_connection(&stub.uri, sigv4_password().to_string()),
    )
}

/// The same CONNECTION naming a role, with `stub` as its STS endpoint too.
fn sigv4_role_connection(stub: &StsAndCatalog) -> TestContext {
    let mut password = sigv4_password();
    password["aws_assume_role_arn"] = ROLE_ARN.into();
    password["aws_external_id"] = EXTERNAL_ID.into();
    password["aws_sts_endpoint"] = stub.uri.clone().into();
    TestContext::scalar(vec![]).with_connection(
        "MY_CONN",
        password_connection(&stub.uri, password.to_string()),
    )
}

fn sigv4_password() -> Json {
    serde_json::json!({
        "warehouse": "123456789012",
        "region": "us-east-1",
        "access_key": BASE_AK,
        "secret_key": BASE_SK,
        "use_sigv4": true,
    })
}

fn role_create_request() -> Json {
    serde_json::json!({
        "type": "createVirtualSchema",
        "properties": {
            "CATALOG_CONNECTION": "MY_CONN",
            "NAMESPACE": "db",
            "ALLOW_HTTP": "true",
        },
    })
}

/// `virtual_schema` keys the process-wide statement state, so each test names its own.
fn role_join_pushdown_request(virtual_schema: &str) -> Json {
    let (request, _) = pushdown_request("CUSTOMER", &[("C_CUSTKEY", ExasolType::decimal(20, 0))])
        .table("ORDERS", &[("O_CUSTKEY", ExasolType::decimal(20, 0))])
        .field("type", "select")
        .field("from", customer_orders_join())
        .column(table_col("CUSTOMER", "C_CUSTKEY"))
        .column(table_col("ORDERS", "O_CUSTKEY"))
        .request_field("type", "pushdown")
        .request_field(
            "schemaMetadataInfo",
            serde_json::json!({ "name": virtual_schema }),
        )
        .request_field(
            "properties",
            serde_json::json!({ "CATALOG_CONNECTION": "MY_CONN", "ALLOW_HTTP": "true" }),
        )
        .build();
    with_identifier_notes(
        with_iceberg_notes(request),
        &[("CUSTOMER", "db.customer"), ("ORDERS", "db.orders")],
    )
}

fn assert_signed_by_the_session(requests: &[String]) {
    assert!(!requests.is_empty(), "the request must reach the catalog");
    for head in requests {
        let authorization = authorization(head);
        assert!(
            authorization.contains(&format!("Credential={SESSION_AK}/")),
            "every catalog request must be signed by the session: {head}"
        );
        assert!(
            !head.contains(BASE_AK),
            "never by the base key pair: {head}"
        );
        assert!(
            head.to_ascii_lowercase().contains(&format!(
                "x-amz-security-token: {}",
                SESSION_TOKEN.to_ascii_lowercase()
            )),
            "every catalog request must carry the session token: {head}"
        );
    }
}

fn assert_signed_by_the_stated_key_pair(stub: &StsAndCatalog) {
    assert_eq!(stub.sts_requests(), 0, "{:?}", stub.heads());
    let catalog_requests = stub.catalog_requests();
    assert!(
        !catalog_requests.is_empty(),
        "the request must reach the catalog"
    );
    for head in &catalog_requests {
        assert!(
            authorization(head).contains(&format!("Credential={BASE_AK}/")),
            "a no-role CONNECTION signs with its stated key pair: {head}"
        );
    }
}

fn assert_no_credential_value(message: &str) {
    for secret in [BASE_SK, EXTERNAL_ID, SESSION_SK, SESSION_TOKEN] {
        assert!(!message.contains(secret), "{secret} leaked in: {message}");
    }
}

/// The local E2E catalog ignores SigV4, so only a fake catalog shows which key pair signs.
#[test]
fn a_role_connection_signs_every_catalog_request_with_the_session() {
    let create = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (200, EMPTY_LISTING));
    dispatch(&mut sigv4_role_connection(&create), &role_create_request())
        .expect("a create over an empty namespace succeeds through the session");
    assert_signed_by_the_session(&create.catalog_requests());

    let join = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (503, "{}"));
    let err = dispatch(
        &mut sigv4_role_connection(&join),
        &role_join_pushdown_request("ROLE_SESSION_SIGNING_VS"),
    )
    .expect_err("an unavailable catalog fails the join pushdown");
    assert_signed_by_the_session(&join.catalog_requests());
    assert_no_credential_value(&err.to_string());
}

#[test]
fn a_connection_without_a_role_sends_no_sts_request() {
    let create_stub = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (200, EMPTY_LISTING));
    dispatch(&mut sigv4_connection(&create_stub), &role_create_request())
        .expect("a no-role create over an empty namespace succeeds");
    assert_signed_by_the_stated_key_pair(&create_stub);

    let join_stub = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (503, "{}"));
    dispatch(
        &mut sigv4_connection(&join_stub),
        &role_join_pushdown_request("NO_ROLE_SIGNING_VS"),
    )
    .expect_err("an unavailable catalog fails the join pushdown");
    assert_signed_by_the_stated_key_pair(&join_stub);
}

/// Scenario: A failed AssumeRole is a credential-safe error
#[test]
fn an_sts_denial_fails_the_request_before_any_catalog_request() {
    for request in [
        role_create_request(),
        role_join_pushdown_request("STS_DENIAL_VS"),
    ] {
        let stub = StsAndCatalog::start((403, ACCESS_DENIED_RESPONSE), (200, EMPTY_LISTING));
        let err = dispatch(&mut sigv4_role_connection(&stub), &request)
            .expect_err("an STS denial must fail the request");

        let UdfError::User(message) = &err else {
            panic!("an STS denial must be a user error, got {err:?}");
        };
        assert!(message.contains(ROLE_ARN), "{message}");
        assert!(
            message.contains("403") && message.contains("AccessDenied"),
            "{message}"
        );
        assert_no_credential_value(message);
        assert_eq!(stub.sts_requests(), 1, "{:?}", stub.heads());
        assert!(
            stub.catalog_requests().is_empty(),
            "no catalog request may follow a denied AssumeRole: {:?}",
            stub.heads()
        );
    }
}

#[test]
fn validation_and_sealing_key_read_the_stated_credentials() {
    use crate::scan::sealed::{derive_sealed_storage_key, seal_storage, unseal_storage};

    let stub = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (200, EMPTY_LISTING));
    let props = serde_json::json!({"CATALOG_CONNECTION": "MY_CONN", "ALLOW_HTTP": "true"});
    let stated = serde_json::json!({
        "warehouse": "wh",
        "region": "us-east-1",
        "access_key": BASE_AK,
        "secret_key": BASE_SK,
        "use_vended_credentials": true,
        "aws_assume_role_arn": ROLE_ARN,
        "aws_sts_endpoint": stub.uri,
    });

    let config = resolve_config_over(&stub.uri, &stated, &props)
        .expect("a role CONNECTION resolves through the stub's session");

    assert_eq!(stub.sts_requests(), 1, "{:?}", stub.heads());
    assert_eq!(config.creds.access_key, SESSION_AK);
    assert_eq!(config.creds.secret_key, SESSION_SK);
    assert_eq!(config.creds.session_token.as_deref(), Some(SESSION_TOKEN));
    assert_eq!(config.creds.aws_assume_role_arn.as_deref(), Some(ROLE_ARN));
    assert!(config.creds.use_vended_credentials);
    let StorageBackend::S3(storage) = &config.storage else {
        panic!(
            "a key-pair CONNECTION resolves S3 storage, got {:?}",
            config.storage
        );
    };
    assert_eq!(storage.access_key, SESSION_AK);
    assert_eq!(storage.secret_key, SESSION_SK);
    assert_eq!(storage.session_token.as_deref(), Some(SESSION_TOKEN));

    let key = config
        .sealed_storage_key
        .as_ref()
        .expect("a role CONNECTION always carries key material");
    let payload = seal_storage(&config.storage, key).expect("seal the effective storage");
    assert_eq!(
        unseal_storage(&payload, &derive_sealed_storage_key(&stated.to_string()))
            .expect("the key must derive from the stated password"),
        config.storage
    );

    let mut keyless = stated.clone();
    keyless.as_object_mut().unwrap().remove("access_key");
    let err = resolve_config_over(&stub.uri, &keyless, &props)
        .err()
        .expect("a stated role CONNECTION without access_key must be rejected");
    assert!(err.to_string().contains("access_key"), "{err}");
    assert_eq!(
        stub.sts_requests(),
        1,
        "a rejected CONNECTION must send no STS request: {:?}",
        stub.heads()
    );
}
