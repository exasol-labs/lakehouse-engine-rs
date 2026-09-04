use super::*;

#[test]
fn dispatch_get_capabilities() {
    let req = serde_json::json!({"type": "getCapabilities"});
    let resp = dispatch(&mut NoopCtx, &req).unwrap();
    assert_eq!(resp["type"].as_str().unwrap(), "getCapabilities");
    let caps = resp["capabilities"].as_array().unwrap();
    assert!(!caps.is_empty());
}

#[test]
fn dispatch_drop_returns_correct_type() {
    let req = serde_json::json!({"type": "dropVirtualSchema"});
    let resp = dispatch(&mut NoopCtx, &req).unwrap();
    assert_eq!(resp["type"].as_str().unwrap(), "dropVirtualSchema");
}

#[test]
fn dispatch_unknown_type_errors() {
    let req = serde_json::json!({"type": "unsupported"});
    let err = dispatch(&mut NoopCtx, &req).unwrap_err();
    assert!(err.to_string().contains("unsupported"));
}

/// `refresh` and `setProperties` are recognised protocol types: dispatch
/// routes them into `handle_create_virtual_schema`, so they fail (if at all)
/// on connection resolution — never with the `unsupported VS request type`
/// error the dead `refreshVirtualSchema` arm used to produce.
#[test]
fn refresh_and_set_properties_dispatched_not_unsupported() {
    for req_type in ["refresh", "setProperties"] {
        let req = serde_json::json!({
            "type": req_type,
            "properties": { PROP_CATALOG_CONNECTION: "no_such_conn" },
        });
        let err =
            dispatch(&mut NoopCtx, &req).expect_err("no live catalog is available in a unit test");
        assert!(
            !err.to_string().contains("unsupported"),
            "{req_type} must not be rejected as an unsupported request type, got: {err}"
        );
    }
}

/// The response `type` mirrors the request `type` for every enumeration
/// request type (Exasol VS protocol requirement).
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

/// `requestedTables` is echoed verbatim when the request carries it and is
/// absent from the response otherwise (pure pass-through).
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

/// `merge_set_properties`: request props win over persisted props, and an
/// explicit `null` in the request unsets (removes) the property — the
/// inverse precedence of `get_properties`.
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

    // Request value wins over the persisted value.
    assert_eq!(nonempty_str(&merged, "NAMESPACE"), Some("new_ns"));
    // A null request value removes the persisted property entirely.
    assert!(
        merged.get("ALLOW_HTTP").is_none(),
        "a null request value must unset the property"
    );
    // A persisted property the request does not mention is retained.
    assert_eq!(nonempty_str(&merged, "CATALOG_CONNECTION"), Some("keep_me"));
}

// Stub UdfContext whose `connection()` resolves successfully, so a
// `setProperties` dispatch can pass connection resolution and reach the
// downstream required-property check instead of failing earlier on a
// missing/unresolvable CONNECTION.
struct ConnResolvingCtx;
impl UdfContext for ConnResolvingCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(
        &self,
        _name: &str,
    ) -> Result<exasol_udf_sdk::connect_back::ConnectionObject, UdfError> {
        Ok(exasol_udf_sdk::connect_back::ConnectionObject {
            kind: "PASSWORD".into(),
            address: "http://catalog.example.com".into(),
            user: String::new(),
            password: serde_json::json!({
                "warehouse": "wh",
                "endpoint": "http://s3.example.com",
                "region": "us-east-1",
                "access_key": "AKID",
                "secret_key": "SECRET",
            })
            .to_string(),
        })
    }
}

/// [human-requested, PR #153 review, adversarial-review finding A2] A
/// `setProperties` request that null-unsets a required property
/// (`NAMESPACE`) must fail with the normal required-property
/// error — never a panic, and never a silent fallback to the stale
/// persisted value. `merge_set_properties` on its own only proves the key
/// is removed from the merged map; this drives the null-unset through the
/// real `setProperties` dispatch path so the removal is proven to reach
/// `handle_create_virtual_schema`'s required-property check end-to-end.
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

    let err = dispatch(&mut ConnResolvingCtx, &req)
        .expect_err("null-unsetting a required property must error, not succeed");

    let expected = format!("property '{PROP_NAMESPACE}' is required");
    assert!(
        err.to_string().contains(&expected),
        "expected the required-property error '{expected}', got: {err}"
    );
}

/// A `createVirtualSchema` request supplying only the old, now-removed
/// `ICEBERG_NAMESPACE` alias — and no `NAMESPACE` — must fail with the
/// normal required-property error naming `NAMESPACE`. Pins the no-alias
/// contract: if an alias for the renamed property is ever reintroduced,
/// this request would satisfy the `NAMESPACE` requirement and `dispatch`
/// would succeed, which trips `expect_err` below and fails this test.
#[test]
fn create_virtual_schema_rejects_old_namespace_alias_without_replacement() {
    let req = serde_json::json!({
        "type": "createVirtualSchema",
        "properties": {
            "CATALOG_CONNECTION": "MY_CONN",
            "ICEBERG_NAMESPACE": "old_ns",
        },
    });

    let err = dispatch(&mut ConnResolvingCtx, &req).expect_err(
        "supplying only the old ICEBERG_NAMESPACE alias must not satisfy the NAMESPACE requirement",
    );

    let expected = format!("property '{PROP_NAMESPACE}' is required");
    assert!(
        err.to_string().contains(&expected),
        "expected the required-property error '{expected}', got: {err}"
    );
}

// Minimal UdfContext for dispatch tests that need no I/O. Its `node_count()`
// uses the trait default (0), exercising the `0 → 1` topology fallback.
struct NoopCtx;
impl UdfContext for NoopCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
}

// A CONNECTION that resolves successfully, carrying a static-token-free S3-style
// credential payload valid under EITHER catalog kind (see `validate_creds`:
// `warehouse` is required only under `IcebergRest`), and an address that is a
// closed local port — a connection refused, no DNS, no hang — so a request that
// reaches catalog resolution fails fast, deterministically, on the FIRST call
// the resolved kind's client makes.
struct ClosedPortConnCtx;
impl UdfContext for ClosedPortConnCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(
        &self,
        _name: &str,
    ) -> Result<exasol_udf_sdk::connect_back::ConnectionObject, UdfError> {
        Ok(exasol_udf_sdk::connect_back::ConnectionObject {
            kind: "PASSWORD".into(),
            address: "http://127.0.0.1:1".into(),
            user: String::new(),
            password: serde_json::json!({
                "warehouse": "wh",
                "endpoint": "http://s3.example.com",
                "region": "us-east-1",
                "access_key": "AKID",
                "secret_key": "SECRET",
            })
            .to_string(),
        })
    }
}

/// A pushdown request under `CATALOG_KIND: UNITY_CATALOG` is planned as a Delta
/// scan: it reaches the SAME resolver the Iceberg path reaches — no early
/// refusal, no routing through the Iceberg REST file-resolution path.
///
/// No live Unity Catalog is reachable in a unit test, so this proves routing by
/// WHICH failure surfaces: `UnityCatalogSession`'s own "Unity Catalog load table
/// request failed" error, naming its `unity-catalog/tables` endpoint, never the
/// removed "not yet supported" refusal and never an Iceberg-shaped error (no
/// `/v1/config` involved).
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

    let err = dispatch(&mut ClosedPortConnCtx, &req)
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

// Like `NoopCtx` but with a configurable `node_count()`, so tests can drive
// both the `0 → default 1` fallback and a `> 1` real-cluster pass-through.
struct StubCtx {
    node_count: u32,
}
impl UdfContext for StubCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn node_count(&self) -> u32 {
        self.node_count
    }
}

#[test]
fn cluster_nodes_from_context_defaults_to_one_when_node_count_zero() {
    // A context reporting node_count() == 0 (no live handshake — the trait
    // default, as on NoopCtx) maps to 1.
    assert_eq!(cluster_nodes_from_context(&NoopCtx), 1usize);
    assert_eq!(
        cluster_nodes_from_context(&StubCtx { node_count: 0 }),
        1usize
    );
}

#[test]
fn cluster_nodes_from_context_passes_through_reported_node_count() {
    // A live cluster reporting node_count() == N (> 1) is passed through
    // verbatim, widened to usize.
    assert_eq!(
        cluster_nodes_from_context(&StubCtx { node_count: 4 }),
        4usize
    );
}

/// Verifies the default-to-1 fallback when adapterNotes is absent or
/// unparseable on a pushdown request.
#[test]
fn adapter_note_absent_or_unparseable_yields_none() {
    // No schemaMetadataInfo at all.
    let bare = serde_json::json!({"type": "pushdown"});
    assert!(adapter_note(&bare, NOTE_PARALLELISM_FACTOR).is_none());

    // adapterNotes present but not valid JSON.
    let garbage = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": "not json" },
    });
    assert!(adapter_note(&garbage, NOTE_PARALLELISM_FACTOR).is_none());

    // adapterNotes empty string.
    let empty = serde_json::json!({
        "type": "pushdown",
        "schemaMetadataInfo": { "adapterNotes": "" },
    });
    assert!(adapter_note(&empty, NOTE_PARALLELISM_FACTOR).is_none());
}

/// Verifies merge-not-clobber: a pre-existing adapterNotes key survives
/// `build_adapter_notes`.
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
        0,
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

/// Verifies that the createVirtualSchema response's adapterNotes carry no
/// CLUSTER_NODES key at all — the note is no longer written.
#[test]
fn adapter_notes_omit_cluster_nodes() {
    let request = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &request,
        0,
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

/// Refresh re-enumerates the namespace and passes the freshly resolved
/// `table_map` into `build_adapter_notes` on every call; TABLE_MAP must be
/// rebuilt from that fresh map (not merged with whatever was persisted
/// from the prior enumeration), while unrelated adapterNotes keys survive
/// the rewrite untouched.
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
        0,
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

/// Task 2.2 — Adapter records the parallelism factor in the virtual-schema adapterNotes.
/// Covers scenario `create_vs_records_parallelism_factor`.
#[test]
fn create_vs_records_parallelism_factor() {
    // Request with an explicit PARALLELISM_FACTOR property — nr_of_cores is
    // irrelevant because the explicit property wins.
    let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "4" });
    let factor = resolve_parallelism_factor(&props, 16);
    assert_eq!(factor, 4, "factor must be read from the property");

    // Build adapterNotes and verify PARALLELISM_FACTOR is present.
    let request = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &request,
        16,
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

    // Default when property absent and nr_of_cores = 0 → floor at DEFAULT_PARALLELISM_FACTOR.
    let empty_props = serde_json::json!({});
    let default_factor = resolve_parallelism_factor(&empty_props, 0);
    assert_eq!(
        default_factor, DEFAULT_PARALLELISM_FACTOR,
        "must default to {DEFAULT_PARALLELISM_FACTOR} when property absent and cores=0"
    );

    // Zero or invalid value also defaults (explicit "0" is treated as absent).
    let zero_props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "0" });
    let zero_factor = resolve_parallelism_factor(&zero_props, 0);
    assert_eq!(
        zero_factor, DEFAULT_PARALLELISM_FACTOR,
        "zero must fall back to default"
    );
}

/// Task 2.2 — PARALLELISM_FACTOR round-trips through adapterNotes.
/// Covers scenario `adapter_notes_carry_parallelism_factor`.
#[test]
fn adapter_notes_carry_parallelism_factor() {
    // createVirtualSchema records the value.
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        0,
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

    // Exasol persists that string and hands it back on the next pushdown request.
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

// ---------------------------------------------------------------------------
// T5 — NR_OF_CORES note tests
// ---------------------------------------------------------------------------

/// Scenario: Adapter records the per-node core count in the virtual-schema adapterNotes.
#[test]
fn adapter_notes_records_nr_of_cores() {
    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        16,
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
        parsed[NOTE_NR_OF_CORES].as_str(),
        Some("16"),
        "NR_OF_CORES must be written into adapterNotes"
    );
}

/// Scenario: with no NR_OF_CORES override, the core count is auto-detected
/// from `std::thread::available_parallelism()` — a positive, host-sourced
/// value (not injectable, so we assert positivity rather than an exact count).
#[test]
fn nr_of_cores_from_available_parallelism_when_unavailable() {
    let props = serde_json::json!({});
    let nr_of_cores = resolve_nr_of_cores(&props);
    assert!(
        nr_of_cores >= 1,
        "nr_of_cores must be auto-detected from available_parallelism() (>= 1), got {nr_of_cores}"
    );
}

// ---------------------------------------------------------------------------
// T5 — parallelism factor formula tests
// ---------------------------------------------------------------------------

/// Scenario: Default parallelism factor equals NR_OF_CORES × 2 when cores > 4.
#[test]
fn default_parallelism_factor_is_cores_times_two() {
    let props = serde_json::json!({});
    // 10 cores × 2 = 20, which is > DEFAULT_PARALLELISM_FACTOR (8), so 20 wins.
    let factor = resolve_parallelism_factor(&props, 10);
    assert_eq!(
        factor, 20,
        "factor must equal nr_of_cores × 2 when that exceeds 8"
    );
}

/// Scenario: Default parallelism factor is floored at DEFAULT_PARALLELISM_FACTOR (8)
/// when NR_OF_CORES × 2 would produce a smaller value (e.g., 0 or 2).
#[test]
fn default_parallelism_factor_floors_at_eight() {
    let props = serde_json::json!({});
    // 0 cores × 2 = 0; must floor to DEFAULT_PARALLELISM_FACTOR.
    let factor_zero = resolve_parallelism_factor(&props, 0);
    assert_eq!(
        factor_zero, DEFAULT_PARALLELISM_FACTOR,
        "must floor at 8 when cores=0"
    );

    // 2 cores × 2 = 4; still below floor.
    let factor_small = resolve_parallelism_factor(&props, 2);
    assert_eq!(
        factor_small, DEFAULT_PARALLELISM_FACTOR,
        "must floor at 8 when cores×2 < 8"
    );
}

/// Scenario: An explicit PARALLELISM_FACTOR property overrides the default formula.
#[test]
fn explicit_parallelism_factor_overrides_default() {
    let props = serde_json::json!({ PROP_PARALLELISM_FACTOR: "5" });
    // Even with 32 cores (32×2=64 > 8), the explicit prop wins.
    let factor = resolve_parallelism_factor(&props, 32);
    assert_eq!(
        factor, 5,
        "explicit property must override the NR_OF_CORES formula"
    );
}

// ---------------------------------------------------------------------------
// T8 — DF_TARGET_PARTITIONS and DF_THREADS_PER_UDF note tests
// ---------------------------------------------------------------------------

/// Scenario: DF_TARGET_PARTITIONS defaults to 1 when property is absent/zero/invalid
/// and nr_of_cores is 0 (unknown).
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

/// Scenario: An explicit positive DATAFUSION_TARGET_PARTITIONS property is used as-is.
#[test]
fn df_target_partitions_uses_supplied_value() {
    let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "4" });
    let val = resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 0);
    assert_eq!(val, 4, "explicit value must be returned");

    // Verify it round-trips through adapterNotes.
    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        0,
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

/// R1: Supplied DATAFUSION_BATCH_SIZE flows create → adapterNote → pushdown → ScanSpec.
///
/// Verifies the full round-trip: resolve_df_batch_size reads the VS property,
/// build_adapter_notes persists it as NOTE_DF_BATCH_SIZE, and the pushdown path
/// reads it back via adapter_note. Also checks default and zero-clamp behaviour.
#[test]
fn df_batch_size_uses_supplied_value() {
    // Explicit value is returned as-is (clamped to ≥1, but 4096 is already ≥1).
    let props = serde_json::json!({ PROP_DF_BATCH_SIZE: "4096" });
    let val = resolve_df_batch_size(&props);
    assert_eq!(val, 4096, "explicit DATAFUSION_BATCH_SIZE must be returned");

    // Zero is clamped to 1.
    let zero_props = serde_json::json!({ PROP_DF_BATCH_SIZE: "0" });
    assert_eq!(
        resolve_df_batch_size(&zero_props),
        1,
        "DATAFUSION_BATCH_SIZE=0 must be clamped to 1"
    );

    // Absent → default.
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_df_batch_size(&absent),
        DEFAULT_DF_BATCH_SIZE,
        "absent property must return DEFAULT_DF_BATCH_SIZE (8192)"
    );

    // Verify it round-trips through adapterNotes (create → note → pushdown).
    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        0,
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

    // Pushdown reads it back.
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

/// Scenario: DF_THREADS_PER_UDF defaults to 1 when property is absent/zero/invalid
/// and nr_of_cores is 0 (unknown).
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

/// Scenario: An explicit positive DATAFUSION_THREADS_PER_UDF property is used as-is.
#[test]
fn df_threads_per_udf_uses_supplied_value() {
    let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
    let val = resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 0);
    assert_eq!(val, 2, "explicit value must be returned");

    // Verify it round-trips through adapterNotes.
    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        0,
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

// ---------------------------------------------------------------------------
// Task 5.1 — MEMORY_POOL_FRACTION and INSTANCE_OVERHEAD_MB resolver tests
// ---------------------------------------------------------------------------

/// Scenario: resolve_memory_pool_fraction defaults/validates.
#[test]
fn resolve_memory_pool_fraction_defaults_and_validates() {
    // Absent → default.
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_memory_pool_fraction(&absent),
        DEFAULT_MEMORY_POOL_FRACTION,
        "absent → default 0.6"
    );

    // Empty string → default (nonempty_str filters empty strings).
    let empty = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "" });
    assert_eq!(
        resolve_memory_pool_fraction(&empty),
        DEFAULT_MEMORY_POOL_FRACTION,
        "empty → default 0.6"
    );

    // "0" → out of range (must be > 0.0) → default.
    let zero = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0" });
    assert_eq!(
        resolve_memory_pool_fraction(&zero),
        DEFAULT_MEMORY_POOL_FRACTION,
        "\"0\" is out of range → default 0.6"
    );

    // "1.5" → > 1.0, out of range → default.
    let too_large = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.5" });
    assert_eq!(
        resolve_memory_pool_fraction(&too_large),
        DEFAULT_MEMORY_POOL_FRACTION,
        "\"1.5\" is out of range → default 0.6"
    );

    // "0.5" → valid.
    let valid = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "0.5" });
    assert_eq!(
        resolve_memory_pool_fraction(&valid),
        0.5,
        "\"0.5\" must be accepted"
    );

    // "1.0" → exactly 1.0, boundary valid.
    let one = serde_json::json!({ PROP_MEMORY_POOL_FRACTION: "1.0" });
    assert_eq!(
        resolve_memory_pool_fraction(&one),
        1.0,
        "\"1.0\" is exactly at the upper bound and must be accepted"
    );
}

/// Scenario: resolve_instance_overhead_mb defaults/validates.
#[test]
fn resolve_instance_overhead_mb_defaults_and_validates() {
    // Absent → default.
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_instance_overhead_mb(&absent),
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "absent → default 200"
    );

    // Empty string → default (nonempty_str filters empty strings).
    let empty = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "" });
    assert_eq!(
        resolve_instance_overhead_mb(&empty),
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "empty → default 200"
    );

    // "0" → valid (zero overhead is permitted).
    let zero = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "0" });
    assert_eq!(
        resolve_instance_overhead_mb(&zero),
        0,
        "\"0\" is a valid overhead (zero)"
    );

    // "256" → valid.
    let valid = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "256" });
    assert_eq!(
        resolve_instance_overhead_mb(&valid),
        256,
        "\"256\" must be returned as-is"
    );

    // Garbage → default.
    let garbage = serde_json::json!({ PROP_INSTANCE_OVERHEAD_MB: "not-a-number" });
    assert_eq!(
        resolve_instance_overhead_mb(&garbage),
        DEFAULT_INSTANCE_OVERHEAD_MB,
        "unparseable value → default 200"
    );
}

/// Scenario: resolve_join_broadcast_max_bytes defaults/validates.
/// Task 3.6 — property present + valid numeric parses correctly; absent
/// defaults to 128 MiB; invalid (non-numeric or zero/negative) falls back
/// to the default. See backlog BL-001 / plan `add-join-pushdown-broadcast`.
#[test]
fn resolve_join_broadcast_max_bytes_defaults_and_validates() {
    // Absent → default 128 MiB.
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

    // Empty string → default (nonempty_str filters empty strings).
    let empty = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&empty),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "empty → default 128 MiB"
    );

    // Present + valid numeric → parsed correctly.
    let valid = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "67108864" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&valid),
        67_108_864,
        "\"67108864\" (64 MiB) must be parsed as-is"
    );

    // Non-numeric → default.
    let garbage = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "not-a-number" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&garbage),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "unparseable value → default 128 MiB"
    );

    // Zero → invalid (must be positive) → default.
    let zero = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "0" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&zero),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "\"0\" is not positive → default 128 MiB"
    );

    // Negative → invalid (u64 parse fails) → default.
    let negative = serde_json::json!({ PROP_JOIN_BROADCAST_MAX_BYTES: "-1" });
    assert_eq!(
        resolve_join_broadcast_max_bytes(&negative),
        DEFAULT_JOIN_BROADCAST_MAX_BYTES,
        "\"-1\" is negative (unparseable as u64) → default 128 MiB"
    );
}

/// Scenario: JOIN_BROADCAST_MAX_BYTES round-trips through build_adapter_notes →
/// adapter_note (mirroring memory_budget_params_round_trip_through_adapter_notes).
#[test]
fn join_broadcast_max_bytes_round_trips_through_adapter_notes() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        0,
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

/// Scenario: MEMORY_POOL_FRACTION and INSTANCE_OVERHEAD_MB round-trip through
/// build_adapter_notes → adapter_note.
#[test]
fn memory_budget_params_round_trip_through_adapter_notes() {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        0,
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

// ---------------------------------------------------------------------------
// Tasks 2.1–2.8 — NR_OF_CORES property override and cores-driven defaults.
// ---------------------------------------------------------------------------

/// Task 2.1 — NR_OF_CORES VS property ≥ 1 is used directly, overriding the
/// `available_parallelism()` auto-detect (tested via the pure helper and the
/// override-wins path).
#[test]
fn nr_of_cores_property_overrides_auto_detect() {
    // A positive integer property must parse to Some(n).
    let props_4 = serde_json::json!({ PROP_NR_OF_CORES: "4" });
    assert_eq!(
        parse_nr_of_cores_override(&props_4),
        Some(4u32),
        "NR_OF_CORES=4 must return Some(4)"
    );

    let props_1 = serde_json::json!({ PROP_NR_OF_CORES: "1" });
    assert_eq!(
        parse_nr_of_cores_override(&props_1),
        Some(1u32),
        "NR_OF_CORES=1 (minimum valid) must return Some(1)"
    );

    // When the override is present, resolve_nr_of_cores returns it directly
    // instead of auto-detecting.
    let cores = resolve_nr_of_cores(&serde_json::json!({ PROP_NR_OF_CORES: "8" }));
    assert_eq!(cores, 8u32, "NR_OF_CORES override must be returned");
}

/// Task 2.2 — NR_OF_CORES absent, empty, zero, or negative falls back to
/// auto-detect (tested via the pure helper returning None, and the
/// `available_parallelism()` fallback returning a positive count).
#[test]
fn nr_of_cores_property_falls_back_to_auto_detect() {
    // Absent → None.
    assert_eq!(
        parse_nr_of_cores_override(&serde_json::json!({})),
        None,
        "absent NR_OF_CORES must return None"
    );

    // Empty string → None (nonempty_str filters empty strings).
    assert_eq!(
        parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "" })),
        None,
        "empty NR_OF_CORES must return None"
    );

    // Zero → None (fails the ≥ 1 filter).
    assert_eq!(
        parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "0" })),
        None,
        "NR_OF_CORES=0 must return None"
    );

    // Negative (u32 parse fails) → None.
    assert_eq!(
        parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "-1" })),
        None,
        "NR_OF_CORES=-1 must return None"
    );

    // Non-numeric → None.
    assert_eq!(
        parse_nr_of_cores_override(&serde_json::json!({ PROP_NR_OF_CORES: "bad" })),
        None,
        "NR_OF_CORES=bad must return None"
    );

    // With no override, resolve_nr_of_cores auto-detects the core count from
    // available_parallelism() (positive, host-sourced).
    let cores = resolve_nr_of_cores(&serde_json::json!({}));
    assert!(
        cores >= 1,
        "no override must fall back to available_parallelism() (>= 1), got {cores}"
    );
}

/// Task 2.3 — Explicit DATAFUSION_TARGET_PARTITIONS wins over cores-driven default.
#[test]
fn df_target_partitions_explicit_wins() {
    let props = serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "3" });
    // Even with nr_of_cores=8, explicit "3" must win.
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 8),
        3,
        "explicit DATAFUSION_TARGET_PARTITIONS must override nr_of_cores default"
    );
}

/// Task 2.4 — Absent DATAFUSION_TARGET_PARTITIONS with nr_of_cores=8 defaults to 8.
#[test]
fn df_target_partitions_defaults_to_nr_of_cores() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 8),
        8,
        "absent property with nr_of_cores=8 must default to 8"
    );
}

/// Task 2.5 — Absent DATAFUSION_TARGET_PARTITIONS with nr_of_cores=0 defaults to 1.
#[test]
fn df_target_partitions_unknown_cores_defaults_to_1() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_TARGET_PARTITIONS, 0),
        1,
        "absent property with nr_of_cores=0 (unknown) must default to 1"
    );
}

/// Task 2.6 — Explicit DATAFUSION_THREADS_PER_UDF wins over cores-driven default.
#[test]
fn df_threads_per_udf_explicit_wins() {
    let props = serde_json::json!({ PROP_DF_THREADS_PER_UDF: "2" });
    // Even with nr_of_cores=16, explicit "2" must win.
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 16),
        2,
        "explicit DATAFUSION_THREADS_PER_UDF must override nr_of_cores default"
    );
}

/// Task 2.7 — Absent DATAFUSION_THREADS_PER_UDF with nr_of_cores=8 defaults to 8.
#[test]
fn df_threads_per_udf_defaults_to_nr_of_cores() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 8),
        8,
        "absent property with nr_of_cores=8 must default to 8"
    );
}

/// Task 2.8 — Absent DATAFUSION_THREADS_PER_UDF with nr_of_cores=0 defaults to 1.
#[test]
fn df_threads_per_udf_unknown_cores_defaults_to_1() {
    let props = serde_json::json!({});
    assert_eq!(
        resolve_df_fixed_count(&props, PROP_DF_THREADS_PER_UDF, 0),
        1,
        "absent property with nr_of_cores=0 (unknown) must default to 1"
    );
}

// ---------------------------------------------------------------------------
// Task 1 — Threading mode AUTO/FIXED tests
// ---------------------------------------------------------------------------

/// 1.1 — DATAFUSION_THREADING_MODE parses case-insensitively; absent / empty /
/// unrecognized values resolve to AUTO.
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

/// 1.5 — Threading mode defaults to AUTO when the property is absent, empty,
/// or holds an unrecognized value; the resolved mode is recorded in adapterNotes.
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

    // The resolved AUTO mode is recorded in adapterNotes.
    let req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &req,
        0,
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

/// 1.5 — AUTO mode derives a per-instance thread budget that does not
/// oversubscribe a node: instances × threads ≤ NR_OF_CORES, with target
/// partitions held in lockstep with threads.
#[test]
fn auto_mode_derives_non_oversubscribing_threads() {
    // 16 cores, parallelism_factor (= udf_instances_per_node) = 4 → 16/4 = 4.
    let (target_partitions, threads) =
        resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 16, 4);
    assert_eq!(threads, 4, "16 cores / 4 instances → 4 threads");
    assert_eq!(
        target_partitions, threads,
        "target_partitions must equal threads (lockstep)"
    );
    // The oversubscription invariant must hold explicitly.
    assert!(
        4 * threads <= 16,
        "udf_instances_per_node × threads must not exceed NR_OF_CORES"
    );

    // Non-divisible case: 10 cores / 3 instances → floor(10/3) = 3; 3×3=9 ≤ 10.
    let (tp, th) = resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 10, 3);
    assert_eq!(th, 3, "floor(10/3) = 3");
    assert_eq!(tp, th, "lockstep");
    assert!(3 * th <= 10, "invariant: 3 × 3 = 9 ≤ 10");

    // A supplied DATAFUSION_TARGET_PARTITIONS is ignored in AUTO mode.
    let (tp_ignored, th_ignored) = resolve_df_threading(
        ThreadingMode::Auto,
        &serde_json::json!({ PROP_DF_TARGET_PARTITIONS: "99", PROP_DF_THREADS_PER_UDF: "99" }),
        16,
        4,
    );
    assert_eq!(th_ignored, 4, "AUTO ignores supplied threads");
    assert_eq!(tp_ignored, 4, "AUTO ignores supplied target partitions");
}

/// 1.5 — AUTO mode falls back to a single thread / partition when the core
/// count is unknown (NR_OF_CORES = 0).
#[test]
fn auto_mode_falls_back_to_one_when_cores_zero() {
    let (target_partitions, threads) =
        resolve_df_threading(ThreadingMode::Auto, &serde_json::json!({}), 0, 8);
    assert_eq!(threads, 1, "cores=0 → 1 thread");
    assert_eq!(target_partitions, 1, "cores=0 → 1 target partition");
}

/// 1.5 — FIXED mode uses the operator-supplied values verbatim; absent or
/// non-positive values fall back to max(NR_OF_CORES, 1) per field.
#[test]
fn fixed_mode_uses_supplied_values() {
    // Explicit positive values are used verbatim, regardless of cores.
    let props = serde_json::json!({
        PROP_DF_TARGET_PARTITIONS: "3",
        PROP_DF_THREADS_PER_UDF: "2",
    });
    let (tp, th) = resolve_df_threading(ThreadingMode::Fixed, &props, 16, 4);
    assert_eq!(tp, 3, "FIXED uses supplied target partitions verbatim");
    assert_eq!(th, 2, "FIXED uses supplied threads verbatim");

    // Absent values fall back to max(NR_OF_CORES, 1) — the pre-mode behaviour.
    let (tp_d, th_d) = resolve_df_threading(ThreadingMode::Fixed, &serde_json::json!({}), 8, 4);
    assert_eq!(tp_d, 8, "absent target partitions → max(cores,1) = 8");
    assert_eq!(th_d, 8, "absent threads → max(cores,1) = 8");

    // Unknown cores → 1.
    let (tp_z, th_z) = resolve_df_threading(ThreadingMode::Fixed, &serde_json::json!({}), 0, 4);
    assert_eq!(tp_z, 1, "absent target partitions, cores=0 → 1");
    assert_eq!(th_z, 1, "absent threads, cores=0 → 1");
}

// ---------------------------------------------------------------------------
// TABLE_MAP round-trip, pushdown table derivation, and collision tests.
// ---------------------------------------------------------------------------

/// TABLE_MAP round-trips through build_adapter_notes → read_table_map.
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
        0,
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

/// TABLE_MAP is stored as a nested JSON object, not a string.
#[test]
fn table_map_stored_as_nested_json_object() {
    let table_map = vec![("EVENTS".to_string(), "db.events".to_string())];
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        0,
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
    // TABLE_MAP must be a JSON object, not a string.
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

/// TABLE_MAP round-trip preserves other adapterNotes entries (merge, not clobber).
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
        0,
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

/// read_table_map returns an empty map when TABLE_MAP is absent from adapterNotes.
#[test]
fn read_table_map_absent_returns_empty() {
    let req = serde_json::json!({"type": "pushdown"});
    let map = read_table_map(&req);
    assert!(map.is_empty(), "absent TABLE_MAP must return empty map");

    // adapterNotes present but no TABLE_MAP key.
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

/// Build a pushdown request whose adapterNotes carry `table_map` and whose
/// involved virtual table is `involved`.
fn pushdown_request_with_table_map(table_map: &[(String, String)], involved: &str) -> Json {
    let create_req = serde_json::json!({"type": "createVirtualSchema"});
    let notes = build_adapter_notes(
        &create_req,
        0,
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

/// Pushdown with an unknown virtual table name returns a clear error naming it.
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

/// TABLE_MAP lookup succeeds for a known virtual table name.
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

fn cat_ident(ns: &[&str], name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: ns.iter().map(|s| s.to_string()).collect(),
        name: name.to_string(),
    }
}

/// Multi-level namespace flattening is deterministic and collision detection
/// returns a clear error naming the colliding Exasol table name.
///
/// Scenario: configured `prod.finance` namespace.
/// - ns `prod.finance` table `orders`   → Exasol name `ORDERS`
/// - ns `prod.finance.eu` table `orders` → Exasol name `EU__ORDERS`
///
/// Collision pair: ns `prod.finance` table `eu__orders` AND
/// ns `prod.finance.eu` table `orders` both flatten to `EU__ORDERS`.
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

    // Collision: ns `prod.finance` table `eu__orders` clashes with
    // ns `prod.finance.eu` table `orders` — both flatten to `EU__ORDERS`.
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

/// Build a table_map, write it through build_adapter_notes, parse the
/// adapterNotes JSON string, and assert:
/// - TABLE_MAP contains the expected Exasol-name → Iceberg-identifier entries.
/// - A pre-existing note (CLUSTER_NODES) is still present after the merge.
#[test]
fn create_vs_records_table_map_in_adapter_notes() {
    let configured_ns = vec!["prod".to_string(), "finance".to_string()];
    let idents = vec![
        cat_ident(&["prod", "finance"], "orders"),
        cat_ident(&["prod", "finance", "eu"], "orders"),
    ];
    let table_map = build_table_map(&configured_ns, &idents).unwrap();

    // Simulate a request with a pre-existing, foreign adapterNotes key.
    let request = serde_json::json!({
        "type": "createVirtualSchema",
        "schemaMetadataInfo": {
            "adapterNotes": "{\"CLUSTER_NODES\":\"3\"}"
        }
    });
    let notes = build_adapter_notes(
        &request,
        0,
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

    // TABLE_MAP must be a nested object mapping Exasol names → Iceberg identifiers.
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

    // A pre-existing, foreign adapterNotes key must survive the merge.
    assert_eq!(
        parsed["CLUSTER_NODES"].as_str(),
        Some("3"),
        "pre-existing CLUSTER_NODES must be preserved (merge, not clobber)"
    );
}

/// The Iceberg listing output — table names, declared column names and types,
/// `TABLE_MAP`, and skipped identifiers — stays byte-identical behind the shared
/// `CatalogClient` trait, including the full-Unicode `to_uppercase` fold that
/// turns `straße` into `STRASSE`.
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

    let (tables_json, table_map, skipped) =
        build_listing_virtual_tables(&configured_ns, &listing, TimestampPrecision::Millisecond)
            .unwrap();

    assert_eq!(tables_json.len(), 1);
    assert_eq!(tables_json[0]["name"], "EU__ORDERS");

    let columns = tables_json[0]["columns"].as_array().unwrap();
    assert_eq!(columns[0]["name"], "ORDER_ID");
    assert_eq!(
        columns[0]["dataType"],
        json!({"type": "decimal", "precision": 20, "scale": 0})
    );
    // Full-Unicode fold: `ß` expands to `SS`, so `straße` declares as `STRASSE`.
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

    // A skipped identifier passes through verbatim for the handler to warn on.
    assert_eq!(
        skipped,
        vec![SkippedTable {
            ident: cat_ident(&["prod", "finance"], "hive_events"),
            reason: SkipReason::NotLoadableIcebergTable,
        }]
    );
}

/// The Iceberg wording is a recorded byte-identical invariant, and the
/// Delta-base wording carries the client's own neutral detail verbatim — so both
/// rendered lines are pinned here rather than left to an uncaptured log call.
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

// ---------------------------------------------------------------------------
// S3_MAX_CONNECTIONS resolution (Task 2.3 / Scenario Coverage rows 3–5)
// ---------------------------------------------------------------------------

/// Scenario: FIXED value overrides the AUTO derivation at createVirtualSchema.
///
/// An explicit positive-integer property is used verbatim regardless of the
/// node capacity that AUTO would otherwise derive a different budget from.
#[test]
fn resolve_s3_max_connections_fixed_value_wins() {
    let props = serde_json::json!({ PROP_S3_MAX_CONNECTIONS: "64" });
    // Cores/instances would AUTO-derive 8 * 4 = 32; the explicit value must win.
    assert_eq!(
        resolve_s3_max_connections(&props, 8, 1),
        64,
        "explicit S3_MAX_CONNECTIONS must be used verbatim"
    );
    // Independent of node capacity (even the unknown-cores path).
    assert_eq!(
        resolve_s3_max_connections(&props, 0, 4),
        64,
        "explicit value wins even when cores are unknown"
    );
}

/// Scenario: AUTO derivation sizes the per-instance budget from node capacity.
///
/// With no explicit property the budget is `per_instance_threads * mult`, and
/// the aggregate per-node budget (`instances * per_instance`) tracks
/// `nr_of_cores * mult` regardless of the instance/thread split.
#[test]
fn resolve_s3_max_connections_auto_scales_with_cores() {
    let absent = serde_json::json!({});

    // One instance on an 8-core node: 8 threads * 4 = 32 connections.
    assert_eq!(
        resolve_s3_max_connections(&absent, 8, 1),
        8 * S3_CONNECTIONS_PER_THREAD,
        "single instance gets the whole node's core count * multiplier"
    );

    // Eight single-thread instances on the same node: 1 thread * 4 = 4 each,
    // and the aggregate (8 * 4 = 32) matches the one-instance case above.
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

    // A larger node scales the budget up.
    assert_eq!(
        resolve_s3_max_connections(&absent, 16, 1),
        16 * S3_CONNECTIONS_PER_THREAD,
        "budget scales with core count"
    );

    // Empty / zero / invalid property strings all fall through to AUTO.
    for bad in ["", "0", "not-a-number", "-4"] {
        let props = serde_json::json!({ PROP_S3_MAX_CONNECTIONS: bad });
        assert_eq!(
            resolve_s3_max_connections(&props, 8, 1),
            8 * S3_CONNECTIONS_PER_THREAD,
            "invalid property {bad:?} must AUTO-derive, not pin a bad value"
        );
    }

    // Never collapses below 1 (more instances than cores → 1 thread each).
    assert!(
        resolve_s3_max_connections(&absent, 2, 8) >= 1,
        "AUTO budget must never collapse below 1"
    );
}

/// Scenario: AUTO derivation falls back to the default budget when the core
/// count is unknown (the `0` sentinel), rather than producing a zero/negative
/// budget.
#[test]
fn resolve_s3_max_connections_auto_zero_cores_defaults() {
    let absent = serde_json::json!({});
    assert_eq!(
        resolve_s3_max_connections(&absent, 0, 1),
        DEFAULT_S3_MAX_CONNECTIONS,
        "unknown cores (0) must fall back to the built-in default"
    );
    // Instance share is irrelevant once cores are unknown.
    assert_eq!(
        resolve_s3_max_connections(&absent, 0, 8),
        DEFAULT_S3_MAX_CONNECTIONS,
        "0-cores fallback ignores the instance share"
    );
}

// ---------------------------------------------------------------------------
// The sealing key `resolve_connection_config` derives, and the predicate gating it
// ---------------------------------------------------------------------------

/// Stub `UdfContext` resolving ONE CONNECTION whose password each case supplies,
/// so `resolve_connection_config` can be driven over an arbitrary password shape.
struct PasswordCtx(String);

impl UdfContext for PasswordCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&exasol_udf_sdk::value::Value, UdfError> {
        Err(UdfError::Type("none".into()))
    }
    fn emit(&mut self, _values: &[exasol_udf_sdk::value::Value]) -> Result<(), UdfError> {
        Ok(())
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
    fn connection(
        &self,
        _name: &str,
    ) -> Result<exasol_udf_sdk::connect_back::ConnectionObject, UdfError> {
        Ok(exasol_udf_sdk::connect_back::ConnectionObject {
            kind: "PASSWORD".into(),
            address: "http://catalog.example.com".into(),
            user: String::new(),
            password: self.0.clone(),
        })
    }
}

/// Resolve a configuration from a CONNECTION carrying `password`. The properties
/// are the minimum `resolve_connection_config` reads: the CONNECTION's name.
fn resolved_for(password: Json) -> ResolvedConnectionConfig {
    resolve_connection_config(
        &PasswordCtx(password.to_string()),
        &serde_json::json!({"CATALOG_CONNECTION": "MY_CONN"}),
    )
    .expect("the fixture password must be an acceptable CONNECTION")
}

/// The resolved configuration carries the CONNECTION's own NAME, which is what
/// the scan spec references in place of the credential it used to carry.
#[test]
fn resolved_config_carries_the_catalog_connection_name() {
    let config = resolved_for(serde_json::json!({
        "warehouse": "wh",
        "region": "us-east-1",
        "access_key": "AK",
        "secret_key": "SK",
    }));
    assert_eq!(
        config.connection_name, "MY_CONN",
        "the resolved configuration must carry the CATALOG_CONNECTION name verbatim"
    );
}

/// A CONNECTION password carrying none of the six secret-bearing fields yields NO
/// sealing key, so the vended envelope's guarantee cannot be claimed for it.
///
/// The `connection_name` assertion is the positive control: without it, an
/// absent key would also be satisfied by a configuration that failed to resolve.
#[test]
fn sealing_key_is_absent_for_a_password_carrying_no_secret_field() {
    let config = resolved_for(serde_json::json!({"warehouse": "wh"}));
    assert_eq!(
        config.connection_name, "MY_CONN",
        "positive control: the configuration must actually have resolved"
    );
    assert!(
        config.sealed_storage_key.is_none(),
        "a password holding only a warehouse carries no key material"
    );
}

/// Each of the six secret-bearing fields, carried non-empty in its own smallest
/// ACCEPTABLE CONNECTION shape, yields a sealing key.
///
/// The shapes are minimal but not arbitrary: `validate_creds` rejects a
/// `client_secret` without its `client_id`, an Azure credential without its
/// `account_name`, and `use_sigv4` without `access_key`/`secret_key`/`region`, so
/// each case supplies exactly the fields its own field requires and no more.
#[test]
fn sealing_key_is_present_for_each_non_empty_secret_field() {
    let cases = [
        (
            "token",
            serde_json::json!({"warehouse": "wh", "token": "T"}),
        ),
        (
            "client_secret",
            serde_json::json!({"warehouse": "wh", "client_id": "CID", "client_secret": "CS"}),
        ),
        (
            "secret_key under use_sigv4",
            serde_json::json!({
                "warehouse": "wh", "region": "us-east-1",
                "access_key": "AK", "secret_key": "SK", "use_sigv4": true,
            }),
        ),
        (
            // The installer's own default template shape: a static storage
            // secret under a no-auth catalog.
            "static secret_key with no use_sigv4",
            serde_json::json!({
                "warehouse": "wh", "region": "us-east-1",
                "access_key": "AK", "secret_key": "SK",
            }),
        ),
        (
            "account_key",
            serde_json::json!({"warehouse": "wh", "account_name": "acct", "account_key": "AKEY"}),
        ),
        (
            "sas_token",
            serde_json::json!({"warehouse": "wh", "account_name": "acct", "sas_token": "SAS"}),
        ),
    ];
    for (field, password) in cases {
        let config = resolved_for(password);
        assert!(
            config.sealed_storage_key.is_some(),
            "a non-empty {field} must derive a sealing key"
        );
    }
}

/// All six secret-bearing fields PRESENT but EMPTY yield no sealing key: the gate
/// tests non-emptiness, so an empty field is an absent one.
#[test]
fn sealing_key_is_absent_when_every_secret_field_is_present_but_empty() {
    let config = resolved_for(serde_json::json!({
        "warehouse": "wh",
        "token": "",
        "client_secret": "",
        "secret_key": "",
        "session_token": "",
        "account_key": "",
        "sas_token": "",
    }));
    assert_eq!(
        config.connection_name, "MY_CONN",
        "positive control: the configuration must actually have resolved"
    );
    assert!(
        config.sealed_storage_key.is_none(),
        "six present-but-empty secret fields carry no key material"
    );
}

/// A non-empty `access_key` with no `secret_key` yields no sealing key: an AWS
/// access key id is an IDENTIFIER, not a secret, so it cannot carry the
/// envelope's guarantee on its own.
#[test]
fn sealing_key_is_absent_for_an_access_key_without_a_secret_key() {
    let config = resolved_for(serde_json::json!({
        "warehouse": "wh",
        "region": "us-east-1",
        "access_key": "AKIAEXAMPLE",
    }));
    assert_eq!(
        config.connection_name, "MY_CONN",
        "positive control: the configuration must actually have resolved"
    );
    assert!(
        config.sealed_storage_key.is_none(),
        "an access key id alone is an identifier, not key material"
    );
}
