use super::*;
use exasol_udf_sdk::test_support::{DefaultsCtx, TestContext};
use std::sync::{Arc, Mutex};

#[path = "parquet_fixture_tests.rs"]
pub(super) mod parquet_fixture;

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
) -> exasol_udf_sdk::connect_back::ConnectionObject {
    exasol_udf_sdk::connect_back::ConnectionObject {
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
fn create_virtual_schema_rejects_old_namespace_alias_without_replacement() {
    let req = serde_json::json!({
        "type": "createVirtualSchema",
        "properties": {
            "CATALOG_CONNECTION": "MY_CONN",
            "ICEBERG_NAMESPACE": "old_ns",
        },
    });

    let err = dispatch(
        &mut TestContext::scalar(vec![]).with_connection(
            "MY_CONN",
            password_connection("http://catalog.example.com", s3_style_password()),
        ),
        &req,
    )
    .expect_err(
        "supplying only the old ICEBERG_NAMESPACE alias must not satisfy the NAMESPACE requirement",
    );

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
    let req = serde_json::json!({
        "type": "pushdown",
        "properties": {
            "CATALOG_KIND": "UNITY_CATALOG",
            "CATALOG_CONNECTION": "MY_CONN",
        },
        "involvedTables": [{
            "name": "ORDERS",
            "columns": [{"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}],
        }],
        "schemaMetadataInfo": {
            "adapterNotes": serde_json::json!({"TABLE_MAP": {"ORDERS": "cat.sch.orders"}}).to_string(),
        },
    });

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

#[test]
fn build_adapter_notes_merges_existing() {
    let req = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"OTHER_KEY\":\"keep-me\",\"CLUSTER_NODES\":\"1\"}"
        },
    });
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
    assert_eq!(
        parsed["OTHER_KEY"].as_str(),
        Some("keep-me"),
        "pre-existing adapterNotes keys must be preserved"
    );
    assert_eq!(
        parsed["CLUSTER_NODES"].as_str(),
        Some("1"),
        "pre-existing adapterNotes keys must be preserved, including foreign ones"
    );
}

#[test]
fn adapter_notes_omit_cluster_nodes() {
    let request = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &request,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
    assert!(
        parsed.get("CLUSTER_NODES").is_none(),
        "a freshly built createVirtualSchema response must carry no CLUSTER_NODES key"
    );
    assert_eq!(
        parsed[NOTE_PARALLELISM_FACTOR].as_str(),
        Some(DEFAULT_PARALLELISM_FACTOR.to_string().as_str()),
        "other notes must still be recorded"
    );
}

#[test]
fn refresh_rebuilds_table_map_preserves_notes() {
    let req = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": serde_json::json!({
                "OTHER_KEY": "keep-me",
                "TABLE_MAP": {"OLD_TABLE": "ns.old_table"},
            })
            .to_string(),
        },
    });

    let fresh_table_map = vec![("NEW_TABLE".to_string(), "ns.new_table".to_string())];
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &fresh_table_map,
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");

    assert_eq!(
        parsed["OTHER_KEY"].as_str(),
        Some("keep-me"),
        "an unrelated adapterNotes key must survive a refresh's TABLE_MAP rebuild"
    );

    let table_map = parsed[NOTE_TABLE_MAP]
        .as_object()
        .expect("TABLE_MAP must be an object");
    assert_eq!(
        table_map.len(),
        1,
        "TABLE_MAP must be rebuilt from the fresh enumeration, not merged with the stale one"
    );
    assert_eq!(
        table_map.get("NEW_TABLE").and_then(|v| v.as_str()),
        Some("ns.new_table"),
        "the freshly resolved table must appear in the rebuilt TABLE_MAP"
    );
    assert!(
        table_map.get("OLD_TABLE").is_none(),
        "the stale TABLE_MAP entry must not survive a refresh rebuild"
    );
}

#[test]
fn create_vs_records_parallelism_factor() {
    let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "4" });
    let factor = resolve_parallelism_factor(&props, 16);
    assert_eq!(factor, 4, "factor must be read from the property");

    let request = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &request,
        factor,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
    let parsed: serde_json::Value =
        serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
    assert_eq!(
        parsed[NOTE_PARALLELISM_FACTOR].as_str(),
        Some("4"),
        "PARALLELISM_FACTOR must be recorded in adapterNotes"
    );

    let empty_props = serde_json::json!({});
    let default_factor = resolve_parallelism_factor(&empty_props, 0);
    assert_eq!(
        default_factor, DEFAULT_PARALLELISM_FACTOR,
        "must default to {DEFAULT_PARALLELISM_FACTOR} when property absent and cores=0"
    );

    let zero_props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "0" });
    let zero_factor = resolve_parallelism_factor(&zero_props, 0);
    assert_eq!(
        zero_factor, DEFAULT_PARALLELISM_FACTOR,
        "zero must fall back to default"
    );
}

#[test]
fn adapter_notes_carry_parallelism_factor() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        12,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
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
fn default_parallelism_factor_is_cores_times_two() {
    let props = serde_json::json!({});
    let factor = resolve_parallelism_factor(&props, 10);
    assert_eq!(
        factor, 20,
        "factor must equal nr_of_cores × 2 when that exceeds 8"
    );
}

#[test]
fn default_parallelism_factor_floors_at_eight() {
    let props = serde_json::json!({});
    let factor_one_core = resolve_parallelism_factor(&props, 1);
    assert_eq!(
        factor_one_core, DEFAULT_PARALLELISM_FACTOR,
        "must floor at 8 on a one-core node"
    );

    let factor_small = resolve_parallelism_factor(&props, 2);
    assert_eq!(
        factor_small, DEFAULT_PARALLELISM_FACTOR,
        "must floor at 8 when cores×2 < 8"
    );
}

#[test]
fn explicit_parallelism_factor_overrides_default() {
    let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "5" });
    let factor = resolve_parallelism_factor(&props, 32);
    assert_eq!(
        factor, 5,
        "explicit property must override the nr_of_cores formula"
    );
}

#[test]
fn df_target_partitions_defaults_to_one() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&absent, PROP_DF_TARGET_PARTITIONS, 0),
        1,
        "absent → 1"
    );

    let zero = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "0" });
    assert_eq!(
        resolve_df_fixed_count(&zero, PROP_DF_TARGET_PARTITIONS, 0),
        1,
        "zero → 1"
    );

    let invalid = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "bad" });
    assert_eq!(
        resolve_df_fixed_count(&invalid, PROP_DF_TARGET_PARTITIONS, 0),
        1,
        "invalid → 1"
    );
}

#[test]
fn df_target_partitions_uses_supplied_value() {
    let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "4" });
    let val = resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 0);
    assert_eq!(val, 4, "explicit value must be returned");

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        val,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
    assert_eq!(
        parsed[NOTE_DF_TARGET_PARTITIONS].as_str(),
        Some("4"),
        "DF_TARGET_PARTITIONS must round-trip through adapterNotes"
    );
}

#[test]
fn df_batch_size_uses_supplied_value() {
    let props = serde_json::json!({ PROP_DF_BATCH_SIZE: "4096" });
    let val = resolve_df_batch_size(&props);
    assert_eq!(val, 4096, "explicit DATAFUSION_BATCH_SIZE must be returned");

    let zero_props = serde_json::json!({ PROP_DF_BATCH_SIZE: "0" });
    assert_eq!(
        resolve_df_batch_size(&zero_props),
        1,
        "DATAFUSION_BATCH_SIZE=0 must be clamped to 1"
    );

    let absent = serde_json::json!({});
    assert_eq!(
        resolve_df_batch_size(&absent),
        DEFAULT_DF_BATCH_SIZE,
        "absent property must return DEFAULT_DF_BATCH_SIZE (8192)"
    );

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        val,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
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
fn df_threads_per_udf_defaults_to_one() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&absent, PROP_DF_THREADS_PER_UDF, 0),
        1,
        "absent → 1"
    );

    let zero = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "0" });
    assert_eq!(
        resolve_df_fixed_count(&zero, PROP_DF_THREADS_PER_UDF, 0),
        1,
        "zero → 1"
    );

    let invalid = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "not-a-number" });
    assert_eq!(
        resolve_df_fixed_count(&invalid, PROP_DF_THREADS_PER_UDF, 0),
        1,
        "invalid → 1"
    );
}

#[test]
fn df_threads_per_udf_uses_supplied_value() {
    let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
    let val = resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 0);
    assert_eq!(val, 2, "explicit value must be returned");

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        val,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
    assert_eq!(
        parsed[NOTE_DF_THREADS_PER_UDF].as_str(),
        Some("2"),
        "DF_THREADS_PER_UDF must round-trip through adapterNotes"
    );
}

#[test]
fn resolve_memory_pool_fraction_defaults_and_validates() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_memory_pool_fraction(&absent),
        DEFAULT_MEMORY_POOL_FRACTION,
        "absent → default 0.6"
    );

    let empty = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "" });
    assert_eq!(
        resolve_memory_pool_fraction(&empty),
        DEFAULT_MEMORY_POOL_FRACTION,
        "empty → default 0.6"
    );

    let zero = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0" });
    assert_eq!(
        resolve_memory_pool_fraction(&zero),
        DEFAULT_MEMORY_POOL_FRACTION,
        "\"0\" is out of range → default 0.6"
    );

    let too_large = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.5" });
    assert_eq!(
        resolve_memory_pool_fraction(&too_large),
        DEFAULT_MEMORY_POOL_FRACTION,
        "\"1.5\" is out of range → default 0.6"
    );

    let valid = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0.5" });
    assert_eq!(
        resolve_memory_pool_fraction(&valid),
        0.5,
        "\"0.5\" must be accepted"
    );

    let one = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.0" });
    assert_eq!(
        resolve_memory_pool_fraction(&one),
        1.0,
        "\"1.0\" is exactly at the upper bound and must be accepted"
    );
}

#[test]
fn resolve_instance_overhead_mb_defaults_and_validates() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_instance_overhead_mb(&absent),
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "absent → default 200"
    );

    let empty = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "" });
    assert_eq!(
        resolve_instance_overhead_mb(&empty),
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "empty → default 200"
    );

    let zero = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "0" });
    assert_eq!(
        resolve_instance_overhead_mb(&zero),
        0,
        "\"0\" is a valid overhead (zero)"
    );

    let valid = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "256" });
    assert_eq!(
        resolve_instance_overhead_mb(&valid),
        256,
        "\"256\" must be returned as-is"
    );

    let garbage = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "not-a-number" });
    assert_eq!(
        resolve_instance_overhead_mb(&garbage),
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "unparseable value → default 200"
    );
}

#[test]
fn resolve_join_broadcast_max_bytes_defaults_and_validates() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_join_broadcast_max_bytes(&absent),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "absent → default 128 MiB"
    );
    assert_eq!(
        DEFAULT_JOIN_BROADCAST_MAX_BYTES, 134_217_728,
        "default must be exactly 128 MiB"
    );

    let empty = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&empty),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "empty → default 128 MiB"
    );

    let valid = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "67108864" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&valid),
        67_108_864,
        "\"67108864\" (64 MiB) must be parsed as-is"
    );

    let garbage = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "not-a-number" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&garbage),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "unparseable value → default 128 MiB"
    );

    let zero = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "0" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&zero),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "\"0\" is not positive → default 128 MiB"
    );

    let negative = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "-1" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&negative),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "\"-1\" is negative (unparseable as u64) → default 128 MiB"
    );
}

#[test]
fn join_broadcast_max_bytes_round_trips_through_adapter_notes() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        67_108_864,
        &[],
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
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        0.5,
        256,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
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
fn df_target_partitions_explicit_wins() {
    let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "3" });
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 8),
        3,
        "explicit DATAFUSION_TARGET_PARTITIONS must override nr_of_cores default"
    );
}

#[test]
fn df_target_partitions_defaults_to_nr_of_cores() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 8),
        8,
        "absent property with nr_of_cores=8 must default to 8"
    );
}

#[test]
fn df_target_partitions_one_core_defaults_to_1() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 1),
        1,
        "absent property on a one-core node must default to 1"
    );
}

#[test]
fn df_threads_per_udf_explicit_wins() {
    let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 16),
        2,
        "explicit DATAFUSION_THREADS_PER_UDF must override nr_of_cores default"
    );
}

#[test]
fn df_threads_per_udf_defaults_to_nr_of_cores() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 8),
        8,
        "absent property with nr_of_cores=8 must default to 8"
    );
}

#[test]
fn df_threads_per_udf_one_core_defaults_to_1() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 1),
        1,
        "absent property on a one-core node must default to 1"
    );
}

#[test]
fn threading_mode_parses_case_insensitively() {
    assert_eq!(
        resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "fixed" })),
        ThreadingMode::Fixed,
        "lowercase 'fixed' must parse to Fixed"
    );
    assert_eq!(
        resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "FiXeD" })),
        ThreadingMode::Fixed,
        "mixed-case 'FiXeD' must parse to Fixed"
    );
    assert_eq!(
        resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "AUTO" })),
        ThreadingMode::Auto,
        "'AUTO' must parse to Auto"
    );
}

#[test]
fn threading_mode_defaults_to_auto() {
    assert_eq!(
        resolve_threading_mode(&serde_json::json!({})),
        ThreadingMode::Auto,
        "absent property → Auto"
    );
    assert_eq!(
        resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "" })),
        ThreadingMode::Auto,
        "empty property → Auto"
    );
    assert_eq!(
        resolve_threading_mode(&serde_json::json!({ PROP_DF_THREADING_MODE: "garbage" })),
        ThreadingMode::Auto,
        "unrecognized value → Auto"
    );

    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
    assert_eq!(
        parsed[NOTE_DF_THREADING_MODE].as_str(),
        Some("AUTO"),
        "DF_THREADING_MODE: AUTO must be recorded in adapterNotes"
    );
}

#[test]
fn auto_mode_derives_non_oversubscribing_threads() {
    let (target_partitions, threads) =
        resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 16, 4);
    assert_eq!(threads, 4, "16 cores / 4 instances → 4 threads");
    assert_eq!(
        target_partitions, threads,
        "target_partitions must equal threads (lockstep)"
    );
    assert!(
        4 * threads <= 16,
        "udf_instances_per_node × threads must not exceed nr_of_cores"
    );

    let (tp, th) = resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 10, 3);
    assert_eq!(th, 3, "floor(10/3) = 3");
    assert_eq!(tp, th, "lockstep");
    assert!(3 * th <= 10, "invariant: 3 × 3 = 9 ≤ 10");

    let (tp_ignored, th_ignored) = resolve_df_threading(
        ThreadingMode::Auto,
        &serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "99", PROP_DF_THREADS_PER_UDF: "99" }),
        16,
        4,
    );
    assert_eq!(th_ignored, 4, "AUTO ignores supplied threads");
    assert_eq!(tp_ignored, 4, "AUTO ignores supplied target partitions");
}

#[test]
fn auto_mode_yields_one_thread_on_one_core() {
    let (target_partitions, threads) =
        resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 1, 8);
    assert_eq!(threads, 1, "one core → 1 thread");
    assert_eq!(target_partitions, 1, "one core → 1 target partition");
}

#[test]
fn fixed_mode_uses_supplied_values() {
    let props = serde_json::json!({
        PROP_DF_TARGET_PARTITIONS: "3",
        PROP_DF_THREADS_PER_UDF: "2",
    });
    let (tp, th) = resolve_df_threading(ThreadingMode::Fixed, &props, 16, 4);
    assert_eq!(tp, 3, "FIXED uses supplied target partitions verbatim");
    assert_eq!(th, 2, "FIXED uses supplied threads verbatim");

    let (tp_d, th_d) = resolve_df_threading(ThreadingMode::Fixed, &serde_json::json!({}), 8, 4);
    assert_eq!(tp_d, 8, "absent target partitions → max(cores,1) = 8");
    assert_eq!(th_d, 8, "absent threads → max(cores,1) = 8");

    let (tp_z, th_z) = resolve_df_threading(ThreadingMode::Fixed, &serde_json::json!({}), 0, 4);
    assert_eq!(tp_z, 1, "absent target partitions, cores=0 → 1");
    assert_eq!(th_z, 1, "absent threads, cores=0 → 1");
}

#[test]
fn table_map_round_trips_through_adapter_notes() {
    let table_map = vec![
        ("ORDERS".to_string(), "prod.finance.orders".to_string()),
        (
            "EU__ORDERS".to_string(),
            "prod.finance.eu.orders".to_string(),
        ),
    ];
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &table_map,
    );
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");

    let pushdown_req = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": notes_str },
    });
    let recovered = read_table_map(&pushdown_req);
    assert_eq!(
        recovered.get("ORDERS").map(|s| s.as_str()),
        Some("prod.finance.orders"),
        "ORDERS must map to prod.finance.orders"
    );
    assert_eq!(
        recovered.get("EU__ORDERS").map(|s| s.as_str()),
        Some("prod.finance.eu.orders"),
        "EU__ORDERS must map to prod.finance.eu.orders"
    );
    assert_eq!(recovered.len(), 2, "map must have exactly two entries");
}

#[test]
fn table_map_stored_as_nested_json_object() {
    let table_map = vec![("EVENTS".to_string(), "db.events".to_string())];
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &table_map,
    );
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
    let parsed: serde_json::Value =
        serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");
    assert!(
        parsed[NOTE_TABLE_MAP].is_object(),
        "TABLE_MAP must be a nested JSON object: {parsed}"
    );
    assert_eq!(
        parsed[NOTE_TABLE_MAP]["EVENTS"].as_str(),
        Some("db.events"),
        "TABLE_MAP.EVENTS must equal 'db.events'"
    );
}

#[test]
fn table_map_merges_with_existing_notes() {
    let req = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"CLUSTER_NODES\":\"5\",\"OTHER\":\"preserved\"}"
        },
    });
    let notes = build_adapter_notes(
        &req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &[("T".to_string(), "ns.t".to_string())],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(notes.as_str().unwrap()).expect("valid JSON");
    assert_eq!(parsed["OTHER"].as_str(), Some("preserved"));
    assert_eq!(
        parsed["CLUSTER_NODES"].as_str(),
        Some("5"),
        "pre-existing CLUSTER_NODES must be preserved (merge, not clobber)"
    );
    assert!(parsed[NOTE_TABLE_MAP].is_object());
}

#[test]
fn read_table_map_absent_returns_empty() {
    let req = serde_json::json!({"type": "pushdown"});
    let map = read_table_map(&req);
    assert!(map.is_empty(), "absent TABLE_MAP must return empty map");

    let req2 = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"CLUSTER_NODES\":\"1\"}"
        },
    });
    let map2 = read_table_map(&req2);
    assert!(
        map2.is_empty(),
        "missing TABLE_MAP key must return empty map"
    );
}

fn pushdown_request_with_table_map(table_map: &[(String, String)], involved: &str) -> Json {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        table_map,
    );
    let notes_str = notes.as_str().unwrap().to_string();
    serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": notes_str },
        "involvedTables": [{"name": involved, "columns": []}],
    })
}

#[test]
fn pushdown_unknown_involved_table_errors() {
    let table_map = vec![("EVENTS".to_string(), "db.events".to_string())];
    let request = pushdown_request_with_table_map(&table_map, "UNKNOWN_TABLE");

    let err = resolve_pushdown_identifier(&request).unwrap_err();
    assert!(
        err.to_string().contains("UNKNOWN_TABLE"),
        "error must name the unknown table: {err}"
    );
}

#[test]
fn pushdown_known_involved_table_resolves_identifier() {
    let table_map = vec![("ORDERS".to_string(), "prod.finance.orders".to_string())];
    let request = pushdown_request_with_table_map(&table_map, "ORDERS");

    assert_eq!(
        resolve_pushdown_identifier(&request).unwrap(),
        "prod.finance.orders",
        "ORDERS must resolve to prod.finance.orders"
    );
}

#[test]
fn table_map_records_the_bare_directory_name_and_round_trips() {
    let idents = vec![
        CatalogTableIdent {
            namespace: Vec::new(),
            name: "Orders".to_string(),
        },
        CatalogTableIdent {
            namespace: Vec::new(),
            name: "events".to_string(),
        },
    ];
    let table_map = build_table_map(&[], &idents).unwrap();

    assert_eq!(
        table_map,
        vec![
            ("ORDERS".to_string(), "Orders".to_string()),
            ("EVENTS".to_string(), "events".to_string()),
        ]
    );

    let request = pushdown_request_with_table_map(&table_map, "ORDERS");
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
fn flatten_multilevel_namespace_and_detect_collision() {
    let configured_ns = vec!["prod".to_string(), "finance".to_string()];

    let direct = cat_ident(&["prod", "finance"], "orders");
    let descendant = cat_ident(&["prod", "finance", "eu"], "orders");

    let result = build_table_map(&configured_ns, &[direct, descendant]).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0],
        ("ORDERS".to_string(), "prod.finance.orders".to_string())
    );
    assert_eq!(
        result[1],
        (
            "EU__ORDERS".to_string(),
            "prod.finance.eu.orders".to_string()
        )
    );

    // `prod.finance`.`eu__orders` and `prod.finance.eu`.`orders` both flatten to `EU__ORDERS`.
    let collider_a = cat_ident(&["prod", "finance"], "eu__orders");
    let collider_b = cat_ident(&["prod", "finance", "eu"], "orders");
    let err = build_table_map(&configured_ns, &[collider_a, collider_b]).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("EU__ORDERS"),
        "error must name the colliding Exasol table name: {msg}"
    );
    assert!(
        msg.contains("collision"),
        "error must mention 'collision': {msg}"
    );
}

#[test]
fn create_vs_records_table_map_in_adapter_notes() {
    let configured_ns = vec!["prod".to_string(), "finance".to_string()];
    let idents = vec![
        cat_ident(&["prod", "finance"], "orders"),
        cat_ident(&["prod", "finance", "eu"], "orders"),
    ];
    let table_map = build_table_map(&configured_ns, &idents).unwrap();

    let request = serde_json::json!({
        "type": "createVirtualSchema",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"CLUSTER_NODES\":\"3\"}"
        }
    });
    let notes = build_adapter_notes(
        &request,
        DEFAULT_PARALLELISM_FACTOR,
        ThreadingMode::Auto,
        DEFAULT_DF_TARGET_PARTITIONS,
        DEFAULT_DF_THREADS_PER_UDF,
        DEFAULT_DF_BATCH_SIZE,
        DEFAULT_MEMORY_POOL_FRACTION,
        DEFAULT_INSTANCE_OVERHEAD_MB,
        DEFAULT_S3_MAX_CONNECTIONS,
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        &table_map,
    );
    let notes_str = notes.as_str().expect("adapterNotes is a JSON string");
    let parsed: serde_json::Value =
        serde_json::from_str(notes_str).expect("adapterNotes must be valid JSON");

    let table_map_obj = parsed[NOTE_TABLE_MAP]
        .as_object()
        .expect("TABLE_MAP must be a JSON object");
    assert_eq!(
        table_map_obj.get("ORDERS").and_then(|v| v.as_str()),
        Some("prod.finance.orders"),
        "TABLE_MAP must map ORDERS → prod.finance.orders"
    );
    assert_eq!(
        table_map_obj.get("EU__ORDERS").and_then(|v| v.as_str()),
        Some("prod.finance.eu.orders"),
        "TABLE_MAP must map EU__ORDERS → prod.finance.eu.orders"
    );

    assert_eq!(
        parsed["CLUSTER_NODES"].as_str(),
        Some("3"),
        "pre-existing CLUSTER_NODES must be preserved (merge, not clobber)"
    );
}

#[test]
fn iceberg_listing_is_behavior_identical_behind_the_trait() {
    use iceberg::spec::{PrimitiveType, Type};
    use lakehouse_catalog::{
        CatalogColumn, CatalogTable, CatalogTableType, ColumnSourceType, TableFormat,
    };

    let configured_ns = vec!["prod".to_string(), "finance".to_string()];
    let listing = CatalogListing {
        tables: vec![CatalogTable {
            ident: cat_ident(&["prod", "finance", "eu"], "orders"),
            table_type: CatalogTableType::Table,
            storage_location: Some("s3://warehouse/orders".to_string()),
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            columns: vec![
                CatalogColumn {
                    name: "order_id".to_string(),
                    source_type: ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long)),
                },
                CatalogColumn {
                    name: "straße".to_string(),
                    source_type: ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::String)),
                },
            ],
        }],
        skipped: vec![SkippedTable {
            ident: cat_ident(&["prod", "finance"], "hive_events"),
            reason: SkipReason::NotLoadableIcebergTable,
        }],
    };

    let (tables_json, table_map, skipped) = build_listing_virtual_tables(
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
        table_map,
        vec![(
            "EU__ORDERS".to_string(),
            "prod.finance.eu.orders".to_string()
        )]
    );

    assert_eq!(
        skipped,
        vec![SkippedTable {
            ident: cat_ident(&["prod", "finance"], "hive_events"),
            reason: SkipReason::NotLoadableIcebergTable,
        }]
    );
}

#[test]
fn skip_warning_renders_the_legacy_iceberg_line_and_the_unity_detail_line() {
    assert_eq!(
        skip_warning(&SkippedTable {
            ident: cat_ident(&["prod", "finance"], "hive_events"),
            reason: SkipReason::NotLoadableIcebergTable,
        }),
        "createVirtualSchema: skipping non-Iceberg table 'prod.finance.hive_events' (catalog reported it is not a loadable Iceberg table)"
    );
    assert_eq!(
        skip_warning(&SkippedTable {
            ident: cat_ident(&["prod", "finance"], "orders_summary"),
            reason: SkipReason::NotDeltaBaseTable {
                detail: "table_type=VIEW".to_string(),
            },
        }),
        "createVirtualSchema: skipping non-Delta-base entry 'prod.finance.orders_summary' (table_type=VIEW)"
    );
}

#[test]
fn resolve_s3_max_connections_fixed_value_wins() {
    let props = serde_json::json!({ PROP_S3_MAX_CONNECTIONS: "64" });
    assert_eq!(
        resolve_s3_max_connections(&props, 8, 1),
        64,
        "explicit S3_MAX_CONNECTIONS must be used verbatim"
    );
    assert_eq!(
        resolve_s3_max_connections(&props, 1, 4),
        64,
        "explicit value wins even on a one-core node"
    );
}

#[test]
fn resolve_s3_max_connections_auto_scales_with_cores() {
    let absent = serde_json::json!({});

    assert_eq!(
        resolve_s3_max_connections(&absent, 8, 1),
        8 * S3_CONNECTIONS_PER_THREAD,
        "single instance gets the whole node's core count * multiplier"
    );

    let per_instance = resolve_s3_max_connections(&absent, 8, 8);
    assert_eq!(
        per_instance, S3_CONNECTIONS_PER_THREAD,
        "each of eight instances gets one thread's worth of connections"
    );
    assert_eq!(
        8 * per_instance,
        8 * S3_CONNECTIONS_PER_THREAD,
        "aggregate per-node budget is invariant across the instance/thread split"
    );

    assert_eq!(
        resolve_s3_max_connections(&absent, 16, 1),
        16 * S3_CONNECTIONS_PER_THREAD,
        "budget scales with core count"
    );

    for bad in ["", "0", "not-a-number", "-4"] {
        let props = serde_json::json!({ PROP_S3_MAX_CONNECTIONS: bad });
        assert_eq!(
            resolve_s3_max_connections(&props, 8, 1),
            8 * S3_CONNECTIONS_PER_THREAD,
            "invalid property {bad:?} must AUTO-derive, not pin a bad value"
        );
    }

    assert_eq!(
        resolve_s3_max_connections(&absent, 2, 8),
        S3_CONNECTIONS_PER_THREAD,
        "an instance share above the core count must yield one thread's connection \
         share, not a collapsed budget"
    );
}

#[test]
fn resolve_s3_max_connections_auto_one_core_yields_one_threads_share() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_s3_max_connections(&absent, 1, 1),
        S3_CONNECTIONS_PER_THREAD,
        "a one-core node must get one thread's connection share"
    );
    assert_eq!(
        resolve_s3_max_connections(&absent, 1, 8),
        S3_CONNECTIONS_PER_THREAD,
        "an instance share above the core count must not shrink the budget"
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
    resolve_connection_config(&ctx, props, &rt)
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
    serde_json::json!({
        "type": "pushdown",
        "properties": { "CATALOG_CONNECTION": "MY_CONN", "ALLOW_HTTP": "true" },
        "involvedTables": [
            {"name": "CUSTOMER", "columns": [
                {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            ]},
            {"name": "ORDERS", "columns": [
                {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
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
        "schemaMetadataInfo": {
            "adapterNotes": serde_json::json!({
                "TABLE_MAP": {"CUSTOMER": "db.customer", "ORDERS": "db.orders"}
            }).to_string(),
        },
    })
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
