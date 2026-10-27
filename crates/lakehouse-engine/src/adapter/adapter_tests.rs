use super::*;
use crate::adapter::properties::{
    PROP_DF_BATCH_SIZE, PROP_DF_TARGET_PARTITIONS, PROP_DF_THREADING_MODE, PROP_DF_THREADS_PER_UDF,
    PROP_INSTANCE_OVERHEAD_MB, PROP_JOIN_BROADCAST_MAX_BYTES, PROP_MEMORY_POOL_FRACTION,
    PROP_PARALLELISM_FACTOR, PROP_S3_MAX_CONNECTIONS,
};
use crate::adapter::pushdown::test_support::{named_table, with_identifier_notes};
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

fn checked(props: serde_json::Value) -> TuningProperties {
    check_properties(&props).unwrap_or_else(|e| panic!("{props} must pass the check: {e}"))
}

fn resolved_tuning(props: serde_json::Value, nr_of_cores: u32) -> TuningNotes {
    resolve_tuning_notes(&checked(props), nr_of_cores)
}

#[test]
fn an_unknown_property_name_fails_every_schema_request_before_the_connection_is_read() {
    for request_type in ["createVirtualSchema", "refresh", "setProperties"] {
        let request = schema_request(
            request_type,
            serde_json::json!({
                PROP_CATALOG_CONNECTION: "NO_SUCH_CONNECTION",
                "NAMESPCE": "sales",
            }),
        );

        let err = dispatch(&mut TestContext::scalar(vec![]), &request)
            .expect_err("an unknown property name must fail the request");

        let message = err.to_string();
        assert!(
            message.contains("unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?)"),
            "{request_type}: {message}"
        );
        assert!(
            !message.contains("NO_SUCH_CONNECTION"),
            "{request_type} must fail before it reads the CONNECTION: {message}"
        );
    }
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
    for request_type in ["createVirtualSchema", "refresh", "setProperties"] {
        let request = schema_request(
            request_type,
            serde_json::json!({
                PROP_CATALOG_CONNECTION: "NO_SUCH_CONNECTION",
                PROP_DF_BATCH_SIZE: "0",
            }),
        );

        let message = dispatch(&mut TestContext::scalar(vec![]), &request)
            .expect_err("an invalid tuning value must fail the request")
            .to_string();

        assert!(
            message.contains(
                "invalid value '0' for 'DATAFUSION_BATCH_SIZE': expected a whole number from 1 to 4294967295"
            ),
            "{request_type}: {message}"
        );
    }
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

    assert_eq!(nonempty_str(&merged, "NAMESPACE"), Some("new_ns"));
    assert!(
        merged.get("ALLOW_HTTP").is_none(),
        "a null request value must unset the property"
    );
    assert_eq!(nonempty_str(&merged, "CATALOG_CONNECTION"), Some("keep_me"));
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
    let req = with_identifier_notes(
        serde_json::json!({
            "type": "pushdown",
            "properties": {
                "CATALOG_KIND": "UNITY_CATALOG",
                "CATALOG_CONNECTION": "MY_CONN",
            },
            "involvedTables": [{
                "name": "ORDERS",
                "columns": [{
                    "name": "ID",
                    "dataType": {"type": "decimal", "precision": 20, "scale": 0},
                    "adapterNotes": r#"{"sourceType":{"name":"id","arrow_type":"int64","nullable":true}}"#,
                }],
            }],
        }),
        &[("ORDERS", "cat.sch.orders")],
    );

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

#[test]
fn adapter_note_absent_or_unparseable_yields_none() {
    let bare = serde_json::json!({"type": "pushdown"});
    assert!(adapter_note(&bare, NOTE_PARALLELISM_FACTOR).is_none());

    let garbage = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": "not json" },
    });
    assert!(adapter_note(&garbage, NOTE_PARALLELISM_FACTOR).is_none());

    let empty = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": "" },
    });
    assert!(adapter_note(&empty, NOTE_PARALLELISM_FACTOR).is_none());
}

impl Default for super::TuningNotes {
    fn default() -> Self {
        let (df_target_partitions, df_threads_per_udf) = auto_df_threading(1);
        Self {
            parallelism_factor: DEFAULT_PARALLELISM_FACTOR,
            df_threading_mode: ThreadingMode::Auto,
            df_target_partitions,
            df_threads_per_udf,
            df_batch_size: DEFAULT_DF_BATCH_SIZE,
            memory_pool_fraction: DEFAULT_MEMORY_POOL_FRACTION,
            instance_overhead_mb: DEFAULT_INSTANCE_OVERHEAD_MB,
            s3_max_connections: DEFAULT_S3_MAX_CONNECTIONS,
            join_broadcast_max_bytes: DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        }
    }
}

fn parsed_notes(request: &Json, tuning: &TuningNotes, skipped: &[SkippedTable]) -> Json {
    let notes = build_adapter_notes(request, tuning, &SkippedTables::listed(skipped));
    serde_json::from_str(notes.as_str().expect("adapterNotes is a JSON string"))
        .expect("adapterNotes is valid JSON")
}

#[test]
fn build_adapter_notes_merges_existing() {
    let req = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"OTHER_KEY\":\"keep-me\"}"
        },
    });
    let parsed = parsed_notes(&req, &TuningNotes::default(), &[]);
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
                &TuningNotes::default(),
                &SkippedTables::listed(&listing.skipped),
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

#[test]
fn create_vs_records_parallelism_factor() {
    let tuning = resolved_tuning(serde_json::json!({ PROP_PARALLELISM_FACTOR: "4" }), 1);
    assert_eq!(
        tuning.parallelism_factor, 4,
        "factor must be read from the property"
    );

    let request = serde_json::json!({"type": "createVirtualSchema"});
    let parsed = parsed_notes(&request, &tuning, &[]);
    assert_eq!(
        parsed[NOTE_PARALLELISM_FACTOR].as_str(),
        Some("4"),
        "PARALLELISM_FACTOR must be recorded in adapterNotes"
    );

    let default_factor = resolved_tuning(serde_json::json!({}), 1).parallelism_factor;
    assert_eq!(
        default_factor, DEFAULT_PARALLELISM_FACTOR,
        "must default to {DEFAULT_PARALLELISM_FACTOR} when property absent"
    );
}

#[test]
fn adapter_notes_carry_parallelism_factor() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        &TuningNotes {
            parallelism_factor: 12,
            ..TuningNotes::default()
        },
        &SkippedTables::default(),
    );
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

    let pushdown_req = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": notes_str },
    });
    assert_eq!(
        adapter_note(&pushdown_req, NOTE_PARALLELISM_FACTOR).as_deref(),
        Some("12"),
        "PARALLELISM_FACTOR must round-trip through adapterNotes"
    );
}

#[test]
fn core_count_comes_from_available_parallelism() {
    let nr_of_cores = resolve_nr_of_cores();
    assert!(
        nr_of_cores >= 1,
        "the core count must come from available_parallelism() (>= 1), got {nr_of_cores}"
    );
}

#[test]
fn core_count_defaults_to_one_when_detection_fails() {
    let detection_failed = Err(std::io::Error::other("platform reports no core count"));
    assert_eq!(
        core_count_or_default(detection_failed),
        1,
        "a failed core-count detection must resolve to 1, not to an unknown sentinel"
    );
}

#[test]
fn core_count_uses_the_detected_count_when_detection_succeeds() {
    let detected = Ok(std::num::NonZeroUsize::new(12).unwrap());
    assert_eq!(
        core_count_or_default(detected),
        12,
        "a reported core count must pass through unchanged, not collapse to the \
         detection-failure default of 1"
    );
}

#[test]
fn default_parallelism_factor_is_one() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).parallelism_factor,
        1,
        "the scalar scan runs one VM per node, so one shard per node is the default"
    );
}

#[test]
fn df_target_partitions_uses_supplied_value() {
    let tuning = resolved_tuning(
        serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED", PROP_DF_TARGET_PARTITIONS: "4" }),
        8,
    );
    assert_eq!(
        tuning.df_target_partitions, 4,
        "explicit value must be returned"
    );

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let parsed = parsed_notes(&req, &tuning, &[]);
    assert_eq!(
        parsed[NOTE_DF_TARGET_PARTITIONS].as_str(),
        Some("4"),
        "DF_TARGET_PARTITIONS must round-trip through adapterNotes"
    );
}

#[test]
fn df_batch_size_uses_supplied_value() {
    let tuning = resolved_tuning(serde_json::json!({ PROP_DF_BATCH_SIZE: "4096" }), 1);
    assert_eq!(
        tuning.df_batch_size, 4096,
        "explicit DATAFUSION_BATCH_SIZE must be returned"
    );

    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).df_batch_size,
        DEFAULT_DF_BATCH_SIZE,
        "absent property must return DEFAULT_DF_BATCH_SIZE (8192)"
    );

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(&req, &tuning, &SkippedTables::default());
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

    let pushdown_req = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": notes_str },
    });
    assert_eq!(
        adapter_note(&pushdown_req, NOTE_DF_BATCH_SIZE).as_deref(),
        Some("4096"),
        "DF_BATCH_SIZE must round-trip through adapterNotes"
    );
}

#[test]
fn df_threads_per_udf_uses_supplied_value() {
    let tuning = resolved_tuning(
        serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED", PROP_DF_THREADS_PER_UDF: "2" }),
        8,
    );
    assert_eq!(
        tuning.df_threads_per_udf, 2,
        "explicit value must be returned"
    );

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let parsed = parsed_notes(&req, &tuning, &[]);
    assert_eq!(
        parsed[NOTE_DF_THREADS_PER_UDF].as_str(),
        Some("2"),
        "DF_THREADS_PER_UDF must round-trip through adapterNotes"
    );
}

#[test]
fn resolve_memory_pool_fraction_defaults_and_reads_the_supplied_value() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).memory_pool_fraction,
        DEFAULT_MEMORY_POOL_FRACTION,
        "absent → default 0.6"
    );

    let valid = resolved_tuning(serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0.5" }), 1);
    assert_eq!(valid.memory_pool_fraction, 0.5);

    let one = resolved_tuning(serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.0" }), 1);
    assert_eq!(one.memory_pool_fraction, 1.0);
}

#[test]
fn resolve_instance_overhead_mb_defaults_and_reads_the_supplied_value() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).instance_overhead_mb,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "absent → default 200"
    );

    let zero = resolved_tuning(serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "0" }), 1);
    assert_eq!(zero.instance_overhead_mb, 0, "\"0\" is a valid overhead");

    let valid = resolved_tuning(serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "256" }), 1);
    assert_eq!(valid.instance_overhead_mb, 256);
}

#[test]
fn resolve_join_broadcast_max_bytes_defaults_and_reads_the_supplied_value() {
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).join_broadcast_max_bytes,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "absent → default 128 MiB"
    );
    assert_eq!(
        DEFAULT_JOIN_BROADCAST_MAX_BYTES, 134_217_728,
        "default must be exactly 128 MiB"
    );

    let valid = resolved_tuning(
        serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "67108864" }),
        1,
    );
    assert_eq!(valid.join_broadcast_max_bytes, 67_108_864);
}

#[test]
fn join_broadcast_max_bytes_round_trips_through_adapter_notes() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        &TuningNotes {
            join_broadcast_max_bytes: 67_108_864,
            ..TuningNotes::default()
        },
        &SkippedTables::default(),
    );

    let pushdown_req = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": notes.as_str().unwrap() },
    });

    assert_eq!(
        adapter_note(&pushdown_req, NOTE_JOIN_BROADCAST_MAX_BYTES).as_deref(),
        Some("67108864"),
        "JOIN_BROADCAST_MAX_BYTES must round-trip through adapterNotes"
    );
}

#[test]
fn memory_budget_params_round_trip_through_adapter_notes() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        &TuningNotes {
            memory_pool_fraction: 0.5,
            instance_overhead_mb: 256,
            ..TuningNotes::default()
        },
        &SkippedTables::default(),
    );
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

    let pushdown_req = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": notes_str },
    });
    assert_eq!(
        adapter_note(&pushdown_req, NOTE_MEMORY_POOL_FRACTION).as_deref(),
        Some("0.5"),
        "MEMORY_POOL_FRACTION must round-trip through adapterNotes"
    );
    assert_eq!(
        adapter_note(&pushdown_req, NOTE_INSTANCE_OVERHEAD_MB).as_deref(),
        Some("256"),
        "INSTANCE_OVERHEAD_MB must round-trip through adapterNotes"
    );
}

#[test]
fn threading_mode_parses_case_insensitively() {
    for (value, expected) in [
        ("fixed", ThreadingMode::Fixed),
        ("FiXeD", ThreadingMode::Fixed),
        ("AUTO", ThreadingMode::Auto),
    ] {
        assert_eq!(
            resolved_tuning(serde_json::json!({ PROP_DF_THREADING_MODE: value }), 1)
                .df_threading_mode,
            expected,
            "'{value}' must parse to {expected:?}"
        );
    }
}

#[test]
fn threading_mode_defaults_to_auto() {
    let tuning = resolved_tuning(serde_json::json!({}), 1);
    assert_eq!(
        tuning.df_threading_mode,
        ThreadingMode::Auto,
        "absent property → Auto"
    );

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let parsed = parsed_notes(&req, &tuning, &[]);
    assert_eq!(
        parsed[NOTE_DF_THREADING_MODE].as_str(),
        Some("AUTO"),
        "DF_THREADING_MODE: AUTO must be recorded in adapterNotes"
    );
}

#[test]
fn auto_mode_uses_every_core_with_twice_as_many_partitions() {
    let absent = resolved_tuning(serde_json::json!({}), 8);
    assert_eq!(
        (absent.df_target_partitions, absent.df_threads_per_udf),
        (16, 8),
        "8 cores → 8 threads, 16 target partitions"
    );

    let supplied = resolved_tuning(
        serde_json::json!({
            PROP_DF_THREADING_MODE: "AUTO",
            PROP_DF_TARGET_PARTITIONS: "99",
            PROP_DF_THREADS_PER_UDF: "99",
        }),
        8,
    );
    assert_eq!(
        (supplied.df_target_partitions, supplied.df_threads_per_udf),
        (16, 8),
        "AUTO ignores supplied values"
    );
}

/// Scenario: AUTO mode yields a single thread on a one-core node
#[test]
fn auto_mode_yields_one_thread_on_one_core() {
    let tuning = resolved_tuning(serde_json::json!({}), 1);
    assert_eq!(
        (tuning.df_target_partitions, tuning.df_threads_per_udf),
        (2, 1),
        "one core → 1 thread, 2 target partitions"
    );
}

#[test]
fn fixed_mode_uses_supplied_values() {
    let supplied = resolved_tuning(
        serde_json::json!({
            PROP_DF_THREADING_MODE: "FIXED",
            PROP_DF_TARGET_PARTITIONS: "3",
            PROP_DF_THREADS_PER_UDF: "2",
        }),
        16,
    );
    assert_eq!(
        (supplied.df_target_partitions, supplied.df_threads_per_udf),
        (3, 2),
        "FIXED uses supplied values verbatim"
    );

    let absent = resolved_tuning(serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED" }), 8);
    assert_eq!(
        (absent.df_target_partitions, absent.df_threads_per_udf),
        (16, 8),
        "absent values fall back to the AUTO values"
    );
}

fn pushdown_request_involving(involved: &str) -> Json {
    serde_json::json!({
        "type": "pushdown",
        "involvedTables": [{"name": involved, "columns": []}],
    })
}

/// Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table
#[test]
fn pushdown_without_an_identifier_note_fails_naming_refresh() {
    let request = pushdown_request_involving("UNKNOWN_TABLE");

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

    let request = json!({"type": "pushdown", "involvedTables": tables});
    assert_eq!(
        resolve_pushdown_identifier(&request).unwrap(),
        "Orders",
        "the recorded bare directory name round-trips through pushdown resolution"
    );
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

    let request = json!({"type": "pushdown", "involvedTables": tables});
    assert_eq!(
        resolve_pushdown_identifier(&request).unwrap(),
        "prod.finance.orders"
    );
    assert_eq!(
        read_identifier(&request, "EU__ORDERS").unwrap(),
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

    let parsed = parsed_notes(
        &json!({"type": "createVirtualSchema"}),
        &TuningNotes::default(),
        &skipped,
    );

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

    let parsed = parsed_notes(&request, &TuningNotes::default(), &[]);

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
    let parsed = parsed_notes(
        &json!({"type": "createVirtualSchema"}),
        &TuningNotes::default(),
        &listing.skipped,
    );

    assert!(tables_json.is_empty());
    assert_eq!(parsed["SKIPPED_TABLES"].as_array().unwrap().len(), 2);
}

#[test]
fn resolve_s3_max_connections_fixed_value_wins() {
    assert_eq!(
        resolved_tuning(serde_json::json!({ PROP_S3_MAX_CONNECTIONS: "64" }), 8).s3_max_connections,
        64,
        "explicit S3_MAX_CONNECTIONS must be used verbatim"
    );
}

/// Scenario: AUTO derivation yields the single-core budget when the core count cannot be detected
#[test]
fn resolve_s3_max_connections_auto_scales_with_threads() {
    let two_threads_on_eight_cores = resolved_tuning(
        serde_json::json!({ PROP_DF_THREADING_MODE: "FIXED", PROP_DF_THREADS_PER_UDF: "2" }),
        8,
    );
    assert_eq!(
        two_threads_on_eight_cores.s3_max_connections,
        2 * S3_CONNECTIONS_PER_THREAD,
        "an absent value derives from the resolved threads, not the cores"
    );
    assert_eq!(
        resolved_tuning(serde_json::json!({}), 1).s3_max_connections,
        S3_CONNECTIONS_PER_THREAD
    );
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
    resolve_connection_config(
        &ctx,
        props,
        &PermissionSettings::Off,
        PermissionCheck::Off,
        &rt,
    )
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

fn role_join_pushdown_request() -> Json {
    with_identifier_notes(
        serde_json::json!({
        "type": "pushdown",
        "properties": { "CATALOG_CONNECTION": "MY_CONN", "ALLOW_HTTP": "true" },
        "involvedTables": [
            {"name": "CUSTOMER", "columns": [
                {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0},
                 "adapterNotes": r#"{"sourceType":{"field_id":1,"name":"c_custkey","arrow_type":"int64","nullable":true}}"#},
            ]},
            {"name": "ORDERS", "columns": [
                {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0},
                 "adapterNotes": r#"{"sourceType":{"field_id":1,"name":"o_custkey","arrow_type":"int64","nullable":true}}"#},
            ]},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {
                "type": "join",
                "join_type": "inner",
                "left": {"name": "CUSTOMER", "type": "table"},
                "right": {"name": "ORDERS", "type": "table"},
                "condition": {
                    "type": "predicate_equal",
                    "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                    "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
                },
            },
            "selectList": [
                {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
            ],
        },
        }),
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

/// Scenario: The adapter assumes the role once per request and substitutes the session
#[test]
fn assume_role_sends_one_sts_request_per_create_and_per_join_pushdown() {
    let create = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (200, EMPTY_LISTING));
    dispatch(&mut sigv4_role_connection(&create), &role_create_request())
        .expect("a create over an empty namespace succeeds through the session");
    assert_eq!(create.sts_requests(), 1, "{:?}", create.heads());
    assert!(
        is_assume_role(&create.heads()[0]),
        "STS must precede the catalog"
    );
    assert_signed_by_the_session(&create.catalog_requests());

    let join = StsAndCatalog::start((200, ASSUME_ROLE_RESPONSE), (503, "{}"));
    let err = dispatch(
        &mut sigv4_role_connection(&join),
        &role_join_pushdown_request(),
    )
    .expect_err("an unavailable catalog fails the join pushdown");
    assert_eq!(
        join.sts_requests(),
        1,
        "one session for both legs: {:?}",
        join.heads()
    );
    assert!(
        is_assume_role(&join.heads()[0]),
        "STS must precede the catalog"
    );
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
        &role_join_pushdown_request(),
    )
    .expect_err("an unavailable catalog fails the join pushdown");
    assert_signed_by_the_stated_key_pair(&join_stub);
}

/// Scenario: A failed AssumeRole is a credential-safe error
#[test]
fn an_sts_denial_fails_the_request_before_any_catalog_request() {
    for request in [role_create_request(), role_join_pushdown_request()] {
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
