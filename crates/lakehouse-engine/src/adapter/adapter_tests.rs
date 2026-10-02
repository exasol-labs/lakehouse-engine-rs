use super::*;
use exasol_udf_sdk::test_support::{DefaultsCtx, TestContext};

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

impl Default for super::TuningNotes {
    fn default() -> Self {
        Self {
            parallelism_factor: DEFAULT_PARALLELISM_FACTOR,
            df_threading_mode: ThreadingMode::Auto,
            df_target_partitions: DEFAULT_DF_TARGET_PARTITIONS,
            df_threads_per_udf: DEFAULT_DF_THREADS_PER_UDF,
            df_batch_size: DEFAULT_DF_BATCH_SIZE,
            memory_pool_fraction: DEFAULT_MEMORY_POOL_FRACTION,
            instance_overhead_mb: DEFAULT_INSTANCE_OVERHEAD_MB,
            s3_max_connections: DEFAULT_S3_MAX_CONNECTIONS,
            join_broadcast_max_bytes: DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        }
    }
}

#[test]
fn build_adapter_notes_merges_existing() {
    let req = serde_json::json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"OTHER_KEY\":\"keep-me\",\"CLUSTER_NODES\":\"1\"}"
        },
    });
    let notes = build_adapter_notes(&req, &TuningNotes::default(), &[], &[]);
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
    let notes = build_adapter_notes(&request, &TuningNotes::default(), &[], &[]);
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
    let notes = build_adapter_notes(&req, &TuningNotes::default(), &fresh_table_map, &[]);
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
        &TuningNotes {
            parallelism_factor: factor,
            ..TuningNotes::default()
        },
        &[],
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
        &TuningNotes {
            parallelism_factor: 12,
            ..TuningNotes::default()
        },
        &[],
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
        &TuningNotes {
            df_target_partitions: val,
            ..TuningNotes::default()
        },
        &[],
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
        &TuningNotes {
            df_batch_size: val,
            ..TuningNotes::default()
        },
        &[],
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
        &TuningNotes {
            df_threads_per_udf: val,
            ..TuningNotes::default()
        },
        &[],
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
        &TuningNotes {
            join_broadcast_max_bytes: 67_108_864,
            ..TuningNotes::default()
        },
        &[],
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
        &TuningNotes {
            memory_pool_fraction: 0.5,
            instance_overhead_mb: 256,
            ..TuningNotes::default()
        },
        &[],
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
    let notes = build_adapter_notes(&req, &TuningNotes::default(), &[], &[]);
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
    let notes = build_adapter_notes(&create_req, &TuningNotes::default(), &table_map, &[]);
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
    let notes = build_adapter_notes(&create_req, &TuningNotes::default(), &table_map, &[]);
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
        &TuningNotes::default(),
        &[("T".to_string(), "ns.t".to_string())],
        &[],
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
    let notes = build_adapter_notes(&create_req, &TuningNotes::default(), table_map, &[]);
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
    let notes = build_adapter_notes(&request, &TuningNotes::default(), &table_map, &[]);
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
            metadata_location: None,
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
fn skip_warning_renders_the_glue_detail_line() {
    assert_eq!(
        skip_warning(&SkippedTable {
            ident: cat_ident(&["sales"], "orders_orc"),
            reason: SkipReason::NotPlannableGlueTable {
                detail: "InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".to_string(),
            },
        }),
        "createVirtualSchema: skipping Glue table 'sales.orders_orc' (InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat)"
    );
}

fn skipped_entry(table: &str, reason: SkipReason) -> SkippedTable {
    SkippedTable {
        ident: cat_ident(&["sales"], table),
        reason,
    }
}

fn notes_with_skipped(
    request: &Json,
    table_map: &[(String, String)],
    skipped: &[SkippedTable],
) -> Json {
    let notes = build_adapter_notes(request, &TuningNotes::default(), table_map, skipped);
    serde_json::from_str(notes.as_str().unwrap()).expect("adapterNotes is a JSON string")
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

    let parsed = notes_with_skipped(&json!({"type": "createVirtualSchema"}), &[], &skipped);

    let recorded = parsed["SKIPPED_TABLES"].as_array().expect("an array");
    let expected = [
        (
            "sales.hive_events",
            "catalog reported it is not a loadable Iceberg table",
        ),
        ("sales.summary", "table_type=VIEW"),
        ("sales.empty_dir", "holds no data file"),
        (
            "sales.orders_orc",
            "InputFormat=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        ),
        ("sales.projected", "projection.enabled=true"),
    ];
    assert_eq!(recorded.len(), expected.len());
    for ((table, reason), (entry, source)) in expected.iter().zip(recorded.iter().zip(&skipped)) {
        assert_eq!(entry, &json!({"table": table, "reason": reason}));
        assert!(
            skip_warning(source).contains(&format!("({reason})")),
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
                "TABLE_MAP": {"STALE": "sales.stale"},
            })
            .to_string()
        },
    });
    let table_map = vec![("FRESH".to_string(), "sales.fresh".to_string())];

    let parsed = notes_with_skipped(&request, &table_map, &[]);

    assert_eq!(parsed["SKIPPED_TABLES"], json!([]));
    assert_eq!(parsed["TABLE_MAP"], json!({"FRESH": "sales.fresh"}));
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

    let (tables_json, table_map, skipped) = build_listing_virtual_tables(
        &["sales".to_string()],
        &listing,
        EngineTimestampSupport::MillisecondOnly,
    )
    .expect("an all-skipped namespace must not fail the listing");
    let parsed = notes_with_skipped(
        &json!({"type": "createVirtualSchema"}),
        &table_map,
        &skipped,
    );

    assert!(tables_json.is_empty());
    assert_eq!(parsed["TABLE_MAP"], json!({}));
    assert_eq!(parsed["SKIPPED_TABLES"].as_array().unwrap().len(), 2);
}

/// Scenario: A skipped-table list too long for adapterNotes is capped to the longest prefix that fits
#[test]
fn an_oversized_skipped_list_is_capped_to_the_longest_prefix_that_fits_the_exasol_limit() {
    for filler in ["0", "é"] {
        let total = 15_000;
        let skipped: Vec<SkippedTable> = (0..total)
            .map(|i| {
                skipped_entry(
                    &format!("{i:0>80}{}", filler.repeat(40)),
                    SkipReason::NoDataFile,
                )
            })
            .collect();

        let notes = build_adapter_notes(
            &json!({"type": "createVirtualSchema"}),
            &TuningNotes::default(),
            &[],
            &skipped,
        );
        let text = notes.as_str().expect("adapterNotes is a JSON string");
        let parsed: Json = serde_json::from_str(text).expect("valid JSON");

        let recorded = parsed["SKIPPED_TABLES"].as_array().expect("an array");
        let kept = recorded.len();
        assert!(kept > 0 && kept < total, "{filler}: kept {kept} of {total}");
        assert!(
            text.len() <= ADAPTER_NOTES_MAX_BYTES,
            "{filler}: {} bytes",
            text.len()
        );
        assert_eq!(
            parsed["SKIPPED_TABLES_OMITTED"],
            json!((total - kept).to_string())
        );
        for (entry, source) in recorded.iter().zip(&skipped) {
            assert_eq!(entry["table"], catalog_identifier_string(&source.ident));
        }
        let mut one_more = parsed.clone();
        one_more["SKIPPED_TABLES"]
            .as_array_mut()
            .expect("an array")
            .push(json!({
                "table": catalog_identifier_string(&skipped[kept].ident),
                "reason": skip_kind_and_reason(&skipped[kept]).1,
            }));
        let still_omitted = total - kept - 1;
        if still_omitted == 0 {
            one_more
                .as_object_mut()
                .expect("an object")
                .remove("SKIPPED_TABLES_OMITTED");
        } else {
            one_more["SKIPPED_TABLES_OMITTED"] = json!(still_omitted.to_string());
        }
        let with_one_more = one_more.to_string().len();
        assert!(
            with_one_more > ADAPTER_NOTES_MAX_BYTES,
            "{filler}: one more entry would still fit ({with_one_more} bytes)"
        );
    }
}

/// Scenario: A skipped-table list too long for adapterNotes is capped to the longest prefix that fits
#[test]
fn a_skipped_list_that_fits_keeps_every_entry_and_drops_a_stale_omitted_count() {
    let request = json!({
        "type": "refresh",
        "schemaMetadataInfo": {
            "adapterNotes": json!({ "SKIPPED_TABLES_OMITTED": "7" }).to_string()
        },
    });
    let skipped = vec![skipped_entry("a", SkipReason::NoDataFile)];

    let parsed = notes_with_skipped(&request, &[], &skipped);

    assert_eq!(parsed["SKIPPED_TABLES"].as_array().unwrap().len(), 1);
    assert!(parsed.get("SKIPPED_TABLES_OMITTED").is_none());
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
    let ctx = TestContext::scalar(vec![]).with_connection(
        "MY_CONN",
        password_connection("http://catalog.example.com", password.to_string()),
    );
    resolve_connection_config(&ctx, &serde_json::json!({"CATALOG_CONNECTION": "MY_CONN"}))
        .expect("the fixture password must be an acceptable CONNECTION")
}

#[test]
fn resolved_config_carries_the_catalog_connection_name() {
    let config = resolved_for(serde_json::json!({
        "warehouse": "wh", "region": "us-east-1",
        "access_key": "AK", "secret_key": "SK",
    }));
    assert_eq!(config.connection_name, "MY_CONN");
}
