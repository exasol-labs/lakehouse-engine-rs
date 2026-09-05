use super::*;

/// 4.1: a `://`-bearing entry is absolute and passes through unchanged.
#[test]
fn reconstruct_absolute_entry_passes_through() {
    assert_eq!(
        reconstruct_abs_uri(
            "s3://bucket/db/table/data/f.parquet",
            "s3://bucket/db/table"
        ),
        "s3://bucket/db/table/data/f.parquet"
    );
    // Passthrough holds even against an empty root.
    assert_eq!(
        reconstruct_abs_uri("s3://other/x.parquet", ""),
        "s3://other/x.parquet"
    );
}

/// 4.1: a relative entry joins onto the root with exactly one separator,
/// regardless of a trailing `/` on the root or a leading `/` on the entry.
#[test]
fn reconstruct_relative_entry_normalizes_single_separator() {
    let expected = "s3://bucket/db/table/data/f.parquet";
    // Neither side carries the separator.
    assert_eq!(
        reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table"),
        expected
    );
    // Trailing slash on the root only.
    assert_eq!(
        reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table/"),
        expected
    );
    // Leading slash on the entry only.
    assert_eq!(
        reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table"),
        expected
    );
    // Both sides carry the separator — still not doubled.
    assert_eq!(
        reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table/"),
        expected
    );
}

fn sample_spec() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: "s3://warehouse/db/table".into(),
            projection: vec!["id".into(), "name".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://minio:9000".into(),
                region: "us-east-1".into(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                allow_http: true,
                ..Default::default()
            })),
            ..Default::default()
        },
        files: vec![
            FileEntry::new("data/part-00000.parquet", 1024),
            FileEntry::new("data/part-00001.parquet", 2048),
        ],
    }
}

/// Unwraps the S3 payload from an inline [`ScanStorage`] for field-level
/// assertions in tests that predate the wrapper and only ever exercised S3.
fn s3_props(storage: &ScanStorage) -> &StorageProps {
    let ScanStorage::Inline(StorageBackend::S3(props)) = storage else {
        panic!("s3_props is inline-S3-only")
    };
    props
}

/// `CommonScanSpec::default()` — the shared test-construction baseline that
/// `..Default::default()` spreads across the test suite fill in — must track
/// serde's field-absent defaults for every tuning knob that reuses a `default_*`
/// seam, so the two default sources cannot silently drift. `s3_max_connections`
/// is the one deliberate exception (the fixture convention `8`, not the serde
/// field-absent fallback `DEFAULT_S3_MAX_CONNECTIONS` = `16`); this pins that
/// intent so a change to either side is a conscious edit, not an accident.
#[test]
fn default_matches_serde_absent_except_s3_max_connections() {
    // A common blob whose every optional/tuning field is absent from JSON
    // (only the two non-defaulted fields, `projection` and `storage`, present).
    let minimal = r#"{"projection":[],"storage":{"inline":{"s3":{"endpoint":"","region":"","access_key":"","secret_key":""}}}}"#;
    let from_absent = CommonScanSpec::from_json(minimal).unwrap();
    let d = CommonScanSpec::default();

    // The knobs Default shares with serde agree field-for-field.
    assert_eq!(d.table_root, from_absent.table_root);
    assert_eq!(d.df_target_partitions, from_absent.df_target_partitions);
    assert_eq!(d.df_batch_size, from_absent.df_batch_size);
    assert_eq!(d.df_threads_per_udf, from_absent.df_threads_per_udf);
    assert_eq!(d.memory_pool_fraction, from_absent.memory_pool_fraction);
    assert_eq!(d.instance_overhead_mb, from_absent.instance_overhead_mb);
    assert_eq!(
        s3_props(&d.storage).path_style,
        s3_props(&from_absent.storage).path_style
    );
    assert!(!s3_props(&d.storage).allow_http && !s3_props(&from_absent.storage).allow_http);

    // The one deliberate divergence: Default is the fixture value, serde's
    // field-absent fallback is the conservative wire default.
    assert_eq!(d.s3_max_connections, 8);
    assert_eq!(from_absent.s3_max_connections, DEFAULT_S3_MAX_CONNECTIONS);
}

/// Scenario (D.2): Scan-spec round-trips through Value boundary.
/// serialize → Value::String → deserialize equals original;
/// credentials survive round-trip but never appear in error text on malformed input.
#[test]
fn scan_spec_round_trips_through_value_boundary() {
    let spec = sample_spec();

    // Serialize to JSON (→ the Value::String payload that crosses the UDF boundary).
    let json = spec.to_json();
    // The JSON must be valid UTF-8 string (Value::String is a Rust String).
    let _value_string: String = json.clone(); // satisfies Value::String ownership model.

    // The wire form is a compact array of `[path, size]` 2-tuples.
    assert!(
        json.contains(
            r#""files":[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#
        ),
        "files must serialize as compact [path,size] 2-tuples: {json}"
    );

    // Deserialize back: must equal original.
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(back.files.len(), 2);
    assert_eq!(
        back.files,
        vec![
            FileEntry::new("data/part-00000.parquet", 1024),
            FileEntry::new("data/part-00001.parquet", 2048),
        ]
    );
    assert_eq!(back.common.table_root, "s3://warehouse/db/table");
    assert_eq!(back.common.projection, vec!["id", "name"]);
    assert_eq!(back.common.filter.as_deref(), Some("(\"ID\" > 10)"));
    assert_eq!(back.common.limit, Some(100));

    // Credentials survive the round-trip (they must reach the scan UDF).
    assert_eq!(s3_props(&back.common.storage).endpoint, "http://minio:9000");
    assert_eq!(s3_props(&back.common.storage).access_key, "minioadmin");
    assert_eq!(s3_props(&back.common.storage).secret_key, "minioadmin");
    assert!(s3_props(&back.common.storage).path_style);
    assert!(s3_props(&back.common.storage).allow_http);
}

#[test]
fn optional_fields_omitted_when_none() {
    let mut spec = sample_spec();
    spec.common.filter = None;
    spec.common.limit = None;
    let ScanStorage::Inline(StorageBackend::S3(props)) = &mut spec.common.storage else {
        panic!("fixture is inline-S3-only")
    };
    props.session_token = None;
    spec.common.aggregates = None;
    spec.common.group_keys = None;
    let json = spec.to_json();
    assert!(!json.contains("filter"));
    assert!(!json.contains("limit"));
    assert!(!json.contains("session_token"));
    assert!(
        !json.contains("aggregates"),
        "aggregates field must be absent when None: {json}"
    );
    assert!(
        !json.contains("group_keys"),
        "group_keys field must be absent when None: {json}"
    );
}

/// `emit_exa_types` round-trips through JSON, is omitted when empty, and a
/// legacy payload lacking it deserializes to an empty Vec (backward-compatible).
#[test]
fn emit_exa_types_round_trips_and_defaults_to_empty() {
    // Empty (default): the field is omitted from serialized JSON.
    let row_spec = sample_spec();
    assert!(row_spec.common.emit_exa_types.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("emit_exa_types"),
        "empty emit_exa_types must be absent from JSON: {row_json}"
    );

    // Non-empty: the declared EMITS types survive the round-trip in order.
    let mut spec = sample_spec();
    spec.common.emit_exa_types = vec![
        "DECIMAL(20,0)".to_string(),
        "VARCHAR(2000000)".to_string(),
        "DOUBLE PRECISION".to_string(),
    ];
    let json = spec.to_json();
    assert!(
        json.contains("emit_exa_types"),
        "non-empty emit_exa_types must appear in JSON: {json}"
    );
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.emit_exa_types,
        vec![
            "DECIMAL(20,0)".to_string(),
            "VARCHAR(2000000)".to_string(),
            "DOUBLE PRECISION".to_string()
        ]
    );

    // Legacy payload without the field deserializes to an empty Vec.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert!(
        legacy.common.emit_exa_types.is_empty(),
        "missing emit_exa_types must default to empty (backward-compat)"
    );
}

/// Task 4.1: Aggregate plan round-trips through JSON and does not appear in row-scan specs.
#[test]
fn aggregate_plan_round_trips_and_absent_from_row_scan() {
    // Row scan: aggregates must be absent.
    let row_spec = sample_spec();
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("aggregates"),
        "row-scan spec must not carry aggregates field: {row_json}"
    );

    // Aggregate scan: round-trip with all supported kinds.
    let mut agg_spec = sample_spec();
    agg_spec.common.aggregates = Some(vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("ID".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Min,
            column: Some("TS".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Max,
            column: Some("TS".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Avg,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
    ]);
    let agg_json = agg_spec.to_json();
    assert!(
        agg_json.contains("aggregates"),
        "aggregate spec must carry the aggregates field: {agg_json}"
    );

    let back = ScanSpec::from_json(&agg_json).unwrap();
    let plans = back
        .common
        .aggregates
        .expect("aggregates must survive round-trip");
    assert_eq!(plans.len(), 6);
    assert_eq!(plans[0].kind, AggKind::Count);
    assert_eq!(plans[0].column, None);
    assert_eq!(plans[1].kind, AggKind::CountCol);
    assert_eq!(plans[1].column.as_deref(), Some("ID"));
    assert_eq!(plans[2].kind, AggKind::Sum);
    assert_eq!(plans[3].kind, AggKind::Min);
    assert_eq!(plans[4].kind, AggKind::Max);
    assert_eq!(plans[5].kind, AggKind::Avg);
    assert_eq!(plans[5].column.as_deref(), Some("AMOUNT"));
}

/// Task 1.1: `AggregatePlan.arg_expr` round-trips through JSON, is omitted from the
/// wire form when `None` (backward-compatible with bare-column plans), and a plan
/// carrying an expression argument survives the round-trip alongside a bare-column plan.
#[test]
fn arg_expr_round_trips_and_omitted_when_none() {
    // A bare-column plan (arg_expr: None) must not carry the key at all.
    let mut agg_spec = sample_spec();
    agg_spec.common.aggregates = Some(vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("AMOUNT".into()),
        arg_expr: None,
    }]);
    let bare_json = agg_spec.to_json();
    assert!(
        !bare_json.contains("arg_expr"),
        "arg_expr must be absent when None: {bare_json}"
    );
    let back = ScanSpec::from_json(&bare_json).unwrap();
    assert_eq!(back.common.aggregates.unwrap()[0].arg_expr, None);

    // An expression-argument plan carries the rendered SQL fragment and round-trips.
    let mut expr_spec = sample_spec();
    expr_spec.common.aggregates = Some(vec![
        AggregatePlan {
            kind: AggKind::Sum,
            column: None,
            arg_expr: Some("LENGTH(\"L_COMMENT\")".into()),
        },
        AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("L_SHIPMODE".into()),
            arg_expr: None,
        },
    ]);
    let expr_json = expr_spec.to_json();
    assert!(
        expr_json.contains("arg_expr"),
        "non-empty arg_expr must appear in JSON: {expr_json}"
    );

    let back = ScanSpec::from_json(&expr_json).unwrap();
    let plans = back
        .common
        .aggregates
        .expect("aggregates must survive round-trip");
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[0].column, None);
    assert_eq!(plans[0].arg_expr.as_deref(), Some("LENGTH(\"L_COMMENT\")"));
    assert_eq!(plans[1].kind, AggKind::CountCol);
    assert_eq!(plans[1].arg_expr, None);

    // A legacy aggregate payload (predating arg_expr) deserializes with it defaulting
    // to None — bare-column plans serialized before this field existed still parse.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "aggregates": [{"kind": "sum", "column": "AMOUNT"}],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    let legacy_plans = legacy
        .common
        .aggregates
        .expect("legacy aggregates must parse");
    assert_eq!(
        legacy_plans[0].arg_expr, None,
        "missing arg_expr must default to None (backward-compat)"
    );
}

/// Every `AggKind`'s partial column set — its arity AND its order — against
/// LITERAL expected values.
///
/// The expectation is written out rather than read from `partial_columns()`,
/// because a test that derives its expectation from the code under test
/// asserts the descriptor against itself and would pass any consistent
/// renaming or reordering.
#[test]
fn partial_columns_arity_per_agg_kind() {
    assert_eq!(AggKind::Count.partial_columns().len(), 1);
    assert_eq!(
        AggKind::Count.partial_columns(),
        &[PartialAggColumn::CountStar]
    );

    assert_eq!(AggKind::CountCol.partial_columns().len(), 1);
    assert_eq!(
        AggKind::CountCol.partial_columns(),
        &[PartialAggColumn::CountArg]
    );

    assert_eq!(AggKind::Sum.partial_columns().len(), 1);
    assert_eq!(AggKind::Sum.partial_columns(), &[PartialAggColumn::Sum]);

    assert_eq!(AggKind::Min.partial_columns().len(), 1);
    assert_eq!(AggKind::Min.partial_columns(), &[PartialAggColumn::Min]);

    assert_eq!(AggKind::Max.partial_columns().len(), 1);
    assert_eq!(AggKind::Max.partial_columns(), &[PartialAggColumn::Max]);

    assert_eq!(AggKind::Avg.partial_columns().len(), 2);
    assert_eq!(
        AggKind::Avg.partial_columns(),
        &[PartialAggColumn::AvgSum, PartialAggColumn::AvgCnt]
    );

    for kind in [
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::StddevPop,
        AggKind::StddevSamp,
    ] {
        assert_eq!(kind.partial_columns().len(), 3, "{kind:?}");
        assert_eq!(
            kind.partial_columns(),
            &[
                PartialAggColumn::StatCnt,
                PartialAggColumn::StatSum,
                PartialAggColumn::StatSumSq,
            ],
            "{kind:?}"
        );
    }
}

/// `is_counter()` holds for exactly the four counter columns and for none of
/// the six value columns — the property the emit site turns into
/// `Value::Int64(0)` versus `Value::Null` on an empty shard.
#[test]
fn is_counter_marks_the_four_count_columns() {
    for col in [
        PartialAggColumn::CountStar,
        PartialAggColumn::CountArg,
        PartialAggColumn::AvgCnt,
        PartialAggColumn::StatCnt,
    ] {
        assert!(col.is_counter(), "{col:?} must be a counter column");
    }
    for col in [
        PartialAggColumn::Sum,
        PartialAggColumn::Min,
        PartialAggColumn::Max,
        PartialAggColumn::AvgSum,
        PartialAggColumn::StatSum,
        PartialAggColumn::StatSumSq,
    ] {
        assert!(!col.is_counter(), "{col:?} must NOT be a counter column");
    }
}

/// The unquoted `PARTIAL_<role>_<ordinal>` name for all ten partial columns,
/// against literal expected strings — the single owner of every name the
/// scan aliases, the `EMITS` clause declares, and the merge SELECT consumes.
#[test]
fn partial_column_name_renders_role_and_ordinal() {
    assert_eq!(
        partial_column_name(PartialAggColumn::CountStar, 0),
        "PARTIAL_count_0"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::CountArg, 3),
        "PARTIAL_count_3"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::Sum, 7),
        "PARTIAL_sum_7"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::Min, 4),
        "PARTIAL_min_4"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::Max, 5),
        "PARTIAL_max_5"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::AvgSum, 1),
        "PARTIAL_avg_sum_1"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::AvgCnt, 1),
        "PARTIAL_avg_cnt_1"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::StatCnt, 9),
        "PARTIAL_stat_cnt_9"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::StatSum, 6),
        "PARTIAL_stat_sum_6"
    );
    assert_eq!(
        partial_column_name(PartialAggColumn::StatSumSq, 2),
        "PARTIAL_stat_sumsq_2"
    );
}

/// The free [`render_ordered`] IS the direction/NULL seam, and
/// [`SortKey::render_ordered`] is a pure delegator to it: the two agree on every
/// flag combination, and the bare-column element list still renders exactly as
/// before. A second copy of this formatting is what the seam exists to prevent.
#[test]
fn render_ordered_free_fn_and_method_are_one_implementation() {
    for (ascending, nulls_last, expected_suffix) in [
        (true, true, "ASC NULLS LAST"),
        (true, false, "ASC NULLS FIRST"),
        (false, true, "DESC NULLS LAST"),
        (false, false, "DESC NULLS FIRST"),
    ] {
        let key = SortKey {
            column: "IGNORED".into(),
            ascending,
            nulls_last,
        };
        let expr = r#"ABS("C_PRICE")"#;
        assert_eq!(
            render_ordered(expr, ascending, nulls_last),
            format!("{expr} {expected_suffix}"),
            "free render_ordered must append direction + NULL placement"
        );
        assert_eq!(
            key.render_ordered(expr),
            render_ordered(expr, ascending, nulls_last),
            "the method must delegate to the free function, not re-implement it"
        );
    }

    // Regression: the bare-column element list is byte-identical to before.
    assert_eq!(
        render_order_by_clause(&[
            SortKey {
                column: "L_EXTENDEDPRICE".into(),
                ascending: false,
                nulls_last: true,
            },
            SortKey {
                column: "L_ORDERKEY".into(),
                ascending: true,
                nulls_last: false,
            },
        ]),
        r#""L_EXTENDEDPRICE" DESC NULLS LAST, "L_ORDERKEY" ASC NULLS FIRST"#
    );
}

/// Task B1: `order_by` round-trips through JSON, is omitted from the wire form
/// when empty (backward-compatible with every pre-existing spec shape), and a
/// legacy JSON payload with no `order_by` key deserializes to an empty list.
#[test]
fn order_by_round_trips_and_defaults_to_empty() {
    // Empty (default): the field is omitted from serialized JSON.
    let row_spec = sample_spec();
    assert!(row_spec.common.order_by.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("order_by"),
        "empty order_by must be absent from JSON: {row_json}"
    );

    // Non-empty: sort keys survive the round-trip, in order, with direction
    // and NULL placement intact.
    let mut spec = sample_spec();
    spec.common.order_by = vec![
        SortKey {
            column: "L_EXTENDEDPRICE".to_string(),
            ascending: false,
            nulls_last: true,
        },
        SortKey {
            column: "L_ORDERKEY".to_string(),
            ascending: true,
            nulls_last: false,
        },
    ];
    let json = spec.to_json();
    assert!(
        json.contains("order_by"),
        "non-empty order_by must appear in JSON: {json}"
    );

    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(back.common.order_by, spec.common.order_by);
    assert_eq!(back.common.order_by.len(), 2);
    assert_eq!(back.common.order_by[0].column, "L_EXTENDEDPRICE");
    assert!(!back.common.order_by[0].ascending);
    assert!(back.common.order_by[0].nulls_last);
    assert_eq!(back.common.order_by[1].column, "L_ORDERKEY");
    assert!(back.common.order_by[1].ascending);
    assert!(!back.common.order_by[1].nulls_last);

    // Full-spec equality also holds (order_by participates in ScanSpec's PartialEq).
    assert_eq!(back, spec);

    // The split (to_common) / merge (from_parts) path threads order_by through.
    let common = spec.to_common();
    assert_eq!(common.order_by, spec.common.order_by);
    let merged = ScanSpec::from_parts(common, spec.files.clone());
    assert_eq!(merged.common.order_by, spec.common.order_by);

    // A legacy payload without the field deserializes to an empty Vec.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert!(
        legacy.common.order_by.is_empty(),
        "missing order_by must default to empty (backward-compat)"
    );

    // Same for the common blob in isolation.
    let legacy_common_json = r#"{
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
    assert!(
        legacy_common.order_by.is_empty(),
        "missing order_by must default to empty on the common blob (backward-compat)"
    );
}

/// Task 2.1: group_keys round-trips through JSON and is absent from row-scan specs.
#[test]
fn group_keys_round_trips_and_absent_from_row_scan() {
    // Row scan: group_keys must be absent from serialized JSON.
    let row_spec = sample_spec();
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("group_keys"),
        "row-scan spec must not carry group_keys field: {row_json}"
    );

    // Grouped scan: round-trip with Some group keys.
    let mut grouped_spec = sample_spec();
    grouped_spec.common.group_keys = Some(vec![
        "\"REGION\"".to_string(),
        "YEAR(\"EVENT_DATE\")".to_string(),
    ]);
    let grouped_json = grouped_spec.to_json();
    assert!(
        grouped_json.contains("group_keys"),
        "grouped spec must carry group_keys field: {grouped_json}"
    );

    let back = ScanSpec::from_json(&grouped_json).unwrap();
    let keys = back
        .common
        .group_keys
        .expect("group_keys must survive round-trip");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], "\"REGION\"");
    assert_eq!(keys[1], "YEAR(\"EVENT_DATE\")");
}

#[test]
fn bad_json_error_does_not_leak_credentials() {
    let garbled = r#"{"storage": {"access_key": "SECRET", "secret_key": "TOPSECRET"}, incomplete"#;
    let err = ScanSpec::from_json(garbled).unwrap_err();
    // The error must not echo the raw input (which contains credentials).
    assert!(!err.contains("SECRET"));
    assert!(!err.contains("TOPSECRET"));
    // But it should say something useful.
    assert!(err.contains("scan spec deserialization failed"));
}

/// Task 2.2: logical_schema round-trips through JSON (spec WITH the field) and
/// a legacy spec WITHOUT it deserializes correctly (backward-compatible default).
#[test]
fn logical_schema_round_trips_and_defaults_to_empty() {
    // A spec with a populated logical_schema.
    let mut spec = sample_spec();
    spec.common.logical_schema = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int32".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(3),
            name: "label".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(4),
            name: "ts".to_string(),
            arrow_type: "timestamp_us".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(5),
            name: "amount".to_string(),
            arrow_type: "decimal128(18,4)".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];
    let json = spec.to_json();

    // The field must appear in the serialized JSON when non-empty.
    assert!(
        json.contains("logical_schema"),
        "non-empty logical_schema must appear in JSON: {json}"
    );

    // Round-trip: all fields survive.
    let back = ScanSpec::from_json(&json).unwrap();
    let fields = &back.common.logical_schema;
    assert_eq!(fields.len(), 5);
    assert_eq!(fields[0].field_id, Some(1));
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[0].arrow_type, "int32");
    assert!(!fields[0].nullable);
    assert_eq!(fields[1].field_id, Some(2));
    assert_eq!(fields[1].name, "rating");
    assert_eq!(fields[1].arrow_type, "float64");
    assert!(fields[1].nullable);
    assert_eq!(fields[2].arrow_type, "utf8");
    assert_eq!(fields[3].arrow_type, "timestamp_us");
    assert_eq!(fields[4].arrow_type, "decimal128(18,4)");
    assert!(!fields[4].nullable);

    // A spec without logical_schema must omit the field from JSON.
    let row_spec = sample_spec();
    assert!(row_spec.common.logical_schema.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("logical_schema"),
        "empty logical_schema must be absent from JSON: {row_json}"
    );

    // A legacy payload without the field deserializes to an empty Vec.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert!(
        legacy.common.logical_schema.is_empty(),
        "missing logical_schema must default to empty (backward-compat)"
    );
}

/// name_mapping round-trips through JSON (spec WITH the field) and
/// a legacy spec WITHOUT it deserializes correctly (backward-compatible default).
#[test]
fn name_mapping_round_trips_and_defaults_to_empty() {
    // A spec with a populated name_mapping.
    let mut spec = sample_spec();
    spec.common.name_mapping = vec![
        NameMappingEntry {
            name: "id".to_string(),
            field_id: 1,
        },
        NameMappingEntry {
            name: "rating".to_string(),
            field_id: 2,
        },
    ];
    let json = spec.to_json();

    // The field must appear in the serialized JSON when non-empty.
    assert!(
        json.contains("name_mapping"),
        "non-empty name_mapping must appear in JSON: {json}"
    );

    // Round-trip: all entries survive.
    let back = ScanSpec::from_json(&json).unwrap();
    let entries = &back.common.name_mapping;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name, "id");
    assert_eq!(entries[0].field_id, 1);
    assert_eq!(entries[1].name, "rating");
    assert_eq!(entries[1].field_id, 2);

    // A spec without name_mapping must omit the field from JSON.
    let row_spec = sample_spec();
    assert!(row_spec.common.name_mapping.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("name_mapping"),
        "empty name_mapping must be absent from JSON: {row_json}"
    );

    // A legacy payload without the field deserializes to an empty Vec.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert!(
        legacy.common.name_mapping.is_empty(),
        "missing name_mapping must default to empty (backward-compat)"
    );
}

/// T8 — ScanSpec threading fields round-trip and default to 1 when absent.
///
/// Verifies that:
/// 1. Explicit `df_target_partitions` / `df_threads_per_udf` values survive
///    serialize → deserialize.
/// 2. A legacy JSON payload that lacks these fields deserializes with both
///    fields defaulting to 1 (backward-compatible with pre-existing specs).
#[test]
fn scan_spec_threading_fields_round_trip_and_default_to_one() {
    // 1. Explicit values round-trip.
    let mut spec = sample_spec();
    spec.common.df_target_partitions = 4;
    spec.common.df_threads_per_udf = 2;
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.df_target_partitions, 4,
        "df_target_partitions must survive round-trip"
    );
    assert_eq!(
        back.common.df_threads_per_udf, 2,
        "df_threads_per_udf must survive round-trip"
    );

    // 2. The fields are present in the serialized JSON.
    assert!(
        json.contains("df_target_partitions"),
        "serialized JSON must carry df_target_partitions: {json}"
    );
    assert!(
        json.contains("df_threads_per_udf"),
        "serialized JSON must carry df_threads_per_udf: {json}"
    );

    // 3. A legacy payload without these fields deserializes with both defaulting to 1.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert_eq!(
        legacy.common.df_target_partitions, 1,
        "missing df_target_partitions must default to 1 (backward-compat)"
    );
    assert_eq!(
        legacy.common.df_threads_per_udf, 1,
        "missing df_threads_per_udf must default to 1 (backward-compat)"
    );
}

/// Task 4.3: df_batch_size round-trips through JSON and defaults correctly on a legacy spec.
///
/// Verifies that:
/// 1. An explicit `df_batch_size` value survives serialize → deserialize.
/// 2. A legacy JSON payload lacking the field deserializes to 8192 (backward-compatible).
#[test]
fn df_batch_size_round_trips_and_defaults() {
    // 1. Explicit non-default value round-trips.
    let mut spec = sample_spec();
    spec.common.df_batch_size = 4096;
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.df_batch_size, 4096,
        "df_batch_size must survive round-trip"
    );

    // 2. The field is present in the serialized JSON.
    assert!(
        json.contains("df_batch_size"),
        "serialized JSON must carry df_batch_size: {json}"
    );

    // 3. A legacy payload without df_batch_size deserializes to 8192.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert_eq!(
        legacy.common.df_batch_size, 8192,
        "missing df_batch_size must default to 8192 (backward-compat)"
    );
}

/// Task 1.2: memory_pool_fraction and instance_overhead_mb round-trip and default correctly.
///
/// Verifies that:
/// 1. Explicit values survive serialize → deserialize.
/// 2. A legacy JSON payload lacking both fields deserializes to 0.6 / 200.
#[test]
fn scan_spec_memory_fields_round_trip_and_default() {
    // 1. Explicit non-default values round-trip.
    let mut spec = sample_spec();
    spec.common.memory_pool_fraction = 0.5;
    spec.common.instance_overhead_mb = 256;
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.memory_pool_fraction, 0.5,
        "memory_pool_fraction must survive round-trip"
    );
    assert_eq!(
        back.common.instance_overhead_mb, 256,
        "instance_overhead_mb must survive round-trip"
    );

    // 2. Legacy payload without these fields → defaults 0.6 / 200.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert_eq!(
        legacy.common.memory_pool_fraction, 0.6,
        "missing memory_pool_fraction must default to 0.6 (backward-compat)"
    );
    assert_eq!(
        legacy.common.instance_overhead_mb, 200,
        "missing instance_overhead_mb must default to 200 (backward-compat)"
    );
}

/// Task 2.2: s3_max_connections round-trips through JSON and defaults to a
/// conservative built-in budget (clamped to at least 1) when absent.
///
/// Verifies that:
/// 1. An explicit value survives serialize → deserialize.
/// 2. A legacy JSON payload lacking the field deserializes to the built-in
///    default (backward-compatible).
#[test]
fn s3_max_connections_round_trips_and_defaults() {
    // 1. Explicit non-default value round-trips.
    let mut spec = sample_spec();
    spec.common.s3_max_connections = 32;
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.s3_max_connections, 32,
        "s3_max_connections must survive round-trip"
    );

    // 2. The field is present in the serialized JSON.
    assert!(
        json.contains("s3_max_connections"),
        "serialized JSON must carry s3_max_connections: {json}"
    );

    // 3. A legacy payload without the field deserializes to the built-in default.
    // `files` uses the current compact [path, size] 2-tuple wire form (ADR-053).
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 123]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert_eq!(
        legacy.common.s3_max_connections,
        default_s3_max_connections(),
        "missing s3_max_connections must default to the built-in budget (backward-compat)"
    );
    assert!(
        legacy.common.s3_max_connections >= 1,
        "default s3_max_connections must be clamped to at least 1"
    );

    // 4. The default also applies to CommonScanSpec (shard-invariant blob).
    let legacy_common_json = r#"{
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
    assert_eq!(
        legacy_common.s3_max_connections,
        default_s3_max_connections(),
        "missing s3_max_connections must default on CommonScanSpec too (backward-compat)"
    );

    // 5. The value threads through the split (to_common) / merge (from_parts) impls.
    let split = spec.to_common();
    assert_eq!(
        split.s3_max_connections, 32,
        "to_common must carry s3_max_connections through the split"
    );
    let merged = ScanSpec::from_parts(split, spec.files.clone());
    assert_eq!(
        merged.common.s3_max_connections, 32,
        "from_parts must carry s3_max_connections through the merge"
    );
}

/// Task 1.3(a): the common blob serializes WITHOUT `files` but WITH
/// `table_root` (carried once, shard-invariant); the per-shard files list
/// serializes as compact `[path, size]` 2-tuples; and reconstituting via
/// `from_parts` (through JSON) yields a spec equal to the pre-split spec.
#[test]
fn from_parts_reconstitutes_files_tuples_and_table_root() {
    let original = sample_spec();

    // Split into the shard-invariant common blob + the per-shard files list.
    let common_json = original.to_common_json();
    let files_json = ScanSpec::files_json(&original.files);

    // The common blob must NOT carry the per-shard files list (type-level guarantee).
    assert!(
        !common_json.contains("\"files\""),
        "common blob must not contain a files key: {common_json}"
    );
    // Nor may any file path value leak into the common blob.
    assert!(
        !common_json.contains("part-00000.parquet"),
        "common blob must not carry any file path: {common_json}"
    );
    // The common blob DOES carry table_root, once.
    assert!(
        common_json.contains(r#""table_root":"s3://warehouse/db/table""#),
        "common blob must carry table_root: {common_json}"
    );

    // The per-shard files list is a compact array of [path, size] 2-tuples.
    assert_eq!(
        files_json,
        r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#
    );

    // The common blob round-trips on its own.
    let common_back = CommonScanSpec::from_json(&common_json).unwrap();
    assert_eq!(common_back, original.to_common());
    assert_eq!(common_back.table_root, "s3://warehouse/db/table");

    // from_parts_json reconstitutes a spec equal to the pre-split original,
    // with table_root reattached from the common blob and files as tuples.
    let reconstituted = ScanSpec::from_parts_json(&common_json, &files_json).unwrap();
    assert_eq!(reconstituted, original);
    assert_eq!(reconstituted.common.table_root, "s3://warehouse/db/table");
    assert_eq!(
        reconstituted.files,
        vec![
            FileEntry::new("data/part-00000.parquet", 1024),
            FileEntry::new("data/part-00001.parquet", 2048),
        ]
    );

    // The struct-level from_parts is equivalent to the JSON round-trip.
    let via_struct = ScanSpec::from_parts(original.to_common(), original.files.clone());
    assert_eq!(via_struct, original);
}

/// Task 1.3(b): malformed common OR files JSON produces errors that never echo
/// the raw input (which carries credentials).
#[test]
fn malformed_common_or_files_json_does_not_leak_credentials() {
    // Malformed common blob carrying credential-shaped values.
    let garbled_common =
        r#"{"storage": {"access_key": "SECRET", "secret_key": "TOPSECRET"}, incomplete"#;
    let err = CommonScanSpec::from_json(garbled_common).unwrap_err();
    assert!(
        !err.contains("SECRET"),
        "common error leaked a secret: {err}"
    );
    assert!(
        !err.contains("TOPSECRET"),
        "common error leaked a secret: {err}"
    );
    assert!(err.contains("scan common spec deserialization failed"));

    // Malformed files argument.
    let garbled_files = r#"["s3://w/SECRETFILE.parquet", incomplete"#;
    let files_err = ScanSpec::files_from_json(garbled_files).unwrap_err();
    assert!(
        !files_err.contains("SECRETFILE"),
        "files error leaked input: {files_err}"
    );
    assert!(files_err.contains("scan files deserialization failed"));

    // from_parts_json surfaces the common-arg error without leaking either input.
    let combined = ScanSpec::from_parts_json(garbled_common, "[]").unwrap_err();
    assert!(!combined.contains("SECRET"));
    assert!(!combined.contains("TOPSECRET"));
}

/// Task 1.3(d): `table_root` round-trips through JSON, and a legacy payload
/// that predates the field (no `table_root` key) deserializes with it
/// defaulting to the empty string — the documented "treat every path as
/// absolute" case.
#[test]
fn legacy_empty_root_treats_paths_as_absolute() {
    // Explicit table_root survives serialize -> deserialize on both spec kinds.
    let spec = sample_spec();
    assert_eq!(spec.common.table_root, "s3://warehouse/db/table");
    let json = spec.to_json();
    assert!(
        json.contains(r#""table_root":"s3://warehouse/db/table""#),
        "non-empty table_root must appear in JSON: {json}"
    );
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(back.common.table_root, "s3://warehouse/db/table");

    let common = spec.to_common();
    let common_json = common.to_json();
    assert!(
        common_json.contains(r#""table_root":"s3://warehouse/db/table""#),
        "non-empty table_root must appear in the common blob: {common_json}"
    );

    // An empty table_root is omitted from serialized JSON (skip_serializing_if).
    let mut rootless = sample_spec();
    rootless.common.table_root = String::new();
    let rootless_json = rootless.to_json();
    assert!(
        !rootless_json.contains("table_root"),
        "empty table_root must be absent from JSON: {rootless_json}"
    );

    // A legacy full-spec payload without table_root deserializes to empty
    // (all file paths in `files` are then absolute, per field semantics).
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert_eq!(
        legacy.common.table_root, "",
        "missing table_root must default to empty (backward-compat; paths are absolute)"
    );
    assert_eq!(legacy.files, vec![FileEntry::new("s3://w/f0.parquet", 100)]);

    // Same for the common blob in isolation.
    let legacy_common_json = r#"{
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
    assert_eq!(
        legacy_common.table_root, "",
        "missing table_root must default to empty on the common blob (backward-compat)"
    );

    // from_parts reattaches the empty table_root onto the reconstituted spec.
    let reconstituted = ScanSpec::from_parts(
        legacy_common,
        vec![FileEntry::new("s3://w/f0.parquet", 100)],
    );
    assert_eq!(reconstituted.common.table_root, "");
}

/// Task 1.3(c): `catalog` no longer appears in any serialized JSON.
#[test]
fn catalog_absent_from_all_serialized_json() {
    let spec = sample_spec();
    assert!(
        !spec.to_json().contains("catalog"),
        "full spec JSON must not contain a catalog key: {}",
        spec.to_json()
    );
    assert!(
        !spec.to_common_json().contains("catalog"),
        "common blob JSON must not contain a catalog key: {}",
        spec.to_common_json()
    );
}

/// Task 2.1(a): a spec WITHOUT a join block serializes with no `join` key and a
/// legacy payload that predates the field deserializes with `join` defaulting to
/// `None` — existing non-join specs are unchanged (backward-compatible).
#[test]
fn absent_join_block_round_trips_unchanged() {
    // A non-join spec (join: None) must omit the field from serialized JSON on
    // both the full spec and the shard-invariant common blob.
    let spec = sample_spec();
    assert!(spec.common.join.is_none());
    let json = spec.to_json();
    assert!(
        !json.contains("\"join\""),
        "non-join spec must not carry a join key: {json}"
    );
    let common_json = spec.to_common_json();
    assert!(
        !common_json.contains("\"join\""),
        "non-join common blob must not carry a join key: {common_json}"
    );

    // A legacy full-spec payload predating the field deserializes with join = None.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert!(
        legacy.common.join.is_none(),
        "missing join must default to None (backward-compat)"
    );

    // Same for the common blob in isolation.
    let legacy_common_json = r#"{
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
    assert!(
        legacy_common.join.is_none(),
        "missing join must default to None on the common blob (backward-compat)"
    );
}

/// Task 1.1: a legacy `[path, size]` per-shard file entry (every entry ever
/// written before positional-delete support) still deserializes — as a
/// [`FileEntry`] whose `deletes` list is empty — inside a full `ScanSpec`
/// payload, inside the isolated `files_from_json` helper, and as the
/// compact wire form a delete-free [`FileEntry`] serializes back to.
#[test]
fn legacy_file_entry_reconstitutes_empty_deletes() {
    // A whole legacy ScanSpec payload whose `files` array uses the
    // pre-existing bare `[path, size]` 2-tuple wire form.
    let legacy_json = r#"{
        "files": [["s3://w/f0.parquet", 100], ["s3://w/f1.parquet", 200]],
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy = ScanSpec::from_json(legacy_json).unwrap();
    assert_eq!(legacy.files.len(), 2);
    for entry in &legacy.files {
        assert!(
            entry.deletes.is_empty(),
            "legacy [path, size] entry must reconstitute with an empty delete list: {entry:?}"
        );
    }
    assert_eq!(legacy.files[0].path, "s3://w/f0.parquet");
    assert_eq!(legacy.files[0].size, 100);
    assert_eq!(legacy.files[1].path, "s3://w/f1.parquet");
    assert_eq!(legacy.files[1].size, 200);
    assert_eq!(
        legacy.files,
        vec![
            FileEntry::new("s3://w/f0.parquet", 100),
            FileEntry::new("s3://w/f1.parquet", 200),
        ]
    );

    // The same legacy 2-tuple form deserializes through the isolated
    // per-shard `files_from_json` helper the UDF boundary actually uses.
    let files_only_json = r#"[["s3://w/f0.parquet", 100], ["s3://w/f1.parquet", 200]]"#;
    let files = ScanSpec::files_from_json(files_only_json).unwrap();
    assert_eq!(
        files,
        vec![
            FileEntry::new("s3://w/f0.parquet", 100),
            FileEntry::new("s3://w/f1.parquet", 200),
        ]
    );
    assert!(files.iter().all(|f| f.deletes.is_empty()));

    // A delete-free FileEntry serializes back to the SAME compact 2-tuple
    // form (not a 3-tuple with a trailing empty array) — the wire stays
    // minimal for the still-common delete-free case.
    let round_tripped = ScanSpec::files_json(&files);
    assert_eq!(
        round_tripped,
        files_only_json.replace(' ', ""),
        "delete-free entries must round-trip to the compact [path,size] form: {round_tripped}"
    );

    // A FileEntry carrying positional-delete refs serializes as a 3-tuple
    // and deserializes back with the delete refs intact.
    let with_deletes = FileEntry::with_deletes(
        "s3://w/f2.parquet",
        300,
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: "s3://w/deletes/d0.parquet".to_string(),
            size: 50,
        }],
    );
    let mixed_json = ScanSpec::files_json(&[
        FileEntry::new("s3://w/f0.parquet", 100),
        with_deletes.clone(),
    ]);
    assert!(
        mixed_json.contains("s3://w/deletes/d0.parquet"),
        "delete-carrying entry must serialize its delete file path: {mixed_json}"
    );
    let mixed_back = ScanSpec::files_from_json(&mixed_json).unwrap();
    assert_eq!(mixed_back[0].deletes, Vec::new());
    assert_eq!(mixed_back[1], with_deletes);
    assert!(matches!(
        mixed_back[1].deletes[0],
        DeleteMechanism::IcebergPositionalDelete { .. }
    ));
}

/// Task 2.1(b): a spec WITH a join block round-trips through JSON and through the
/// common/per-shard split and merge. The join block (dimension side) is
/// shard-INVARIANT: it rides in the common blob (UDF argument 0), never in the
/// per-shard files list (argument 1), so the fact side's per-shard `files` and
/// the dimension side's `join.files` never collide.
#[test]
fn join_block_round_trips_through_split_and_merge() {
    let mut spec = sample_spec();
    // Deliberately DISTINCT from `sample_spec()`'s `common.storage`
    // (access_key "minioadmin") — proves the dimension side's own backend
    // round-trips independently of the fact side's, not by coincidence.
    let dim_storage = StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "dimkey".into(),
        secret_key: "dimsecret".into(),
        allow_http: true,
        ..Default::default()
    });
    spec.common.join = Some(JoinSpec {
        table_root: "s3://warehouse/db/dim".into(),
        files: vec![
            FileEntry::new("data/dim-00000.parquet", 512),
            FileEntry::new("data/dim-00001.parquet", 1024),
        ],
        logical_schema: vec![LogicalField {
            field_id: Some(1),
            name: "d_key".into(),
            arrow_type: "int64".into(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        }],
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"F_KEY\" = \"D_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: ScanStorage::Inline(dim_storage.clone()),
    });

    // The serialized JSON carries the join block; join_type is a lowercase tag.
    let json = spec.to_json();
    assert!(
        json.contains("\"join\""),
        "join spec must carry the join block: {json}"
    );
    assert!(
        json.contains("\"join_type\":\"inner\""),
        "join_type must serialize as the lowercase tag: {json}"
    );

    // Whole-spec round-trip.
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(back, spec);

    // The join block lives in the shard-invariant common part, so the dimension
    // files ride in the common blob (once), not per shard.
    let common = spec.to_common();
    assert_eq!(common.join, spec.common.join);
    let common_json = spec.to_common_json();
    assert!(
        common_json.contains("dim-00000.parquet"),
        "dimension files must ride in the shard-invariant common blob: {common_json}"
    );

    // The per-shard files list still carries ONLY the fact side's files.
    let files_json = ScanSpec::files_json(&spec.files);
    assert!(
        !files_json.contains("dim-00000.parquet"),
        "per-shard files must not carry dimension files: {files_json}"
    );

    // Reconstitution from the two UDF arguments reattaches the join block.
    let reconstituted = ScanSpec::from_parts_json(&common_json, &files_json).unwrap();
    assert_eq!(reconstituted, spec);
    let jb = reconstituted
        .common
        .join
        .expect("join block must survive reconstitution");
    assert_eq!(jb.table_root, "s3://warehouse/db/dim");
    assert_eq!(
        jb.files,
        vec![
            FileEntry::new("data/dim-00000.parquet", 512),
            FileEntry::new("data/dim-00001.parquet", 1024),
        ]
    );
    assert_eq!(jb.join_type, JoinType::Inner);
    assert_eq!(jb.condition, "\"F_KEY\" = \"D_KEY\"");
    assert_eq!(jb.logical_schema.len(), 1);
    assert_eq!(jb.logical_schema[0].name, "d_key");

    // The dimension side's own backend survives the round-trip intact, and
    // remains distinct from the fact side's `common.storage` — the wire-format
    // guarantee this whole plan depends on.
    assert_eq!(jb.storage, ScanStorage::Inline(dim_storage));
    assert_ne!(jb.storage, reconstituted.common.storage);

    // The struct-level split/merge is equivalent to the JSON round-trip.
    let via_struct = ScanSpec::from_parts(spec.to_common(), spec.files.clone());
    assert_eq!(via_struct, spec);
}

/// `post_join_limit` is additive-optional: a join block serialized without it —
/// every one written before the field existed — loads as `None`, and a cap that
/// IS set survives the common/per-shard split the UDF actually receives.
#[test]
fn join_spec_omitting_post_join_limit_deserializes_to_none() {
    let mut spec = sample_spec();
    let storage = spec.common.storage.clone();
    spec.common.join = Some(JoinSpec {
        table_root: "s3://warehouse/db/dim".into(),
        files: vec![FileEntry::new("data/dim-00000.parquet", 512)],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"F_KEY\" = \"D_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage,
    });

    let uncapped = serde_json::to_value(spec.common.join.as_ref().unwrap()).unwrap();
    assert!(
        uncapped.get("post_join_limit").is_none(),
        "an absent cap must emit no key at all: {uncapped}"
    );
    let back: JoinSpec = serde_json::from_value(uncapped).unwrap();
    assert_eq!(back.post_join_limit, None);

    spec.common.join.as_mut().unwrap().post_join_limit = Some(7);
    let round_tripped = ScanSpec::from_parts_json(&spec.to_common_json(), "[]")
        .expect("the common blob must reconstitute");
    assert_eq!(
        round_tripped.common.join.unwrap().post_join_limit,
        Some(7),
        "a set cap must survive the common/per-shard split"
    );
}

/// `partition_columns` on `JoinSpec` mirrors `CommonScanSpec::partition_columns`
/// (the same neutral concept, needed by the broadcast/dimension side): it
/// defaults to empty and is skipped from JSON when empty, so an Iceberg join
/// spec — which never populates it — serializes byte-identically to before the
/// field existed.
#[test]
fn join_spec_partition_columns_defaults_to_empty_and_iceberg_json_is_byte_identical() {
    let storage = StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "dimkey".into(),
        secret_key: "dimsecret".into(),
        allow_http: true,
        ..Default::default()
    });
    let join = JoinSpec {
        table_root: "s3://warehouse/db/dim".into(),
        files: vec![FileEntry::new("data/dim-00000.parquet", 512)],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"F_KEY\" = \"D_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: ScanStorage::Inline(storage),
    };

    let json = serde_json::to_string(&join).unwrap();
    assert_eq!(
        json,
        r#"{"table_root":"s3://warehouse/db/dim","files":[["data/dim-00000.parquet",512]],"join_type":"inner","condition":"\"F_KEY\" = \"D_KEY\"","storage":{"inline":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"dimkey","secret_key":"dimsecret","allow_http":true,"path_style":true}}}}"#,
        "an Iceberg join spec must serialize byte-identically with partition_columns defaulted and skipped"
    );

    let back: JoinSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(back, join);
}

/// `storage` carries no `#[serde(default)]` on either `JoinSpec` or
/// `CommonScanSpec`, so a payload omitting it must be REJECTED rather than
/// silently defaulting a dimension (or the fact side) to no storage at all.
#[test]
fn join_storage_is_a_required_key() {
    let without_storage = r#"{"table_root":"s3://warehouse/db/dim","files":[["data/dim-00000.parquet",512]],"join_type":"inner","condition":"\"F_KEY\" = \"D_KEY\""}"#;
    assert!(
        serde_json::from_str::<JoinSpec>(without_storage).is_err(),
        "a join payload omitting storage must not deserialize"
    );

    let with_storage = r#"{"table_root":"s3://warehouse/db/dim","files":[["data/dim-00000.parquet",512]],"join_type":"inner","condition":"\"F_KEY\" = \"D_KEY\"","storage":{"connection":{"name":"C","allow_http":false}}}"#;
    assert!(
        serde_json::from_str::<JoinSpec>(with_storage).is_ok(),
        "a join payload carrying storage must deserialize"
    );

    let common_without_storage = r#"{"table_root":"s3://warehouse/db/table","projection":[]}"#;
    assert!(
        CommonScanSpec::from_json(common_without_storage).is_err(),
        "a common spec payload omitting storage must not deserialize"
    );
}

/// The two-argument UDF wire (shard-invariant common blob + per-shard files
/// array) MUST stay byte-for-byte identical after `CommonScanSpec` was embedded
/// into `ScanSpec` via `#[serde(flatten)]`. Flatten reorders `ScanSpec`'s own
/// whole-struct serialization (`files` moves to the end), but production never
/// reconstitutes from a whole-`ScanSpec` JSON — it splits via `to_common_json()`
/// (which serializes `CommonScanSpec`, untouched) and `files_json()` (untouched).
/// This pins both against strings captured from the pre-flatten code, so any
/// future field reorder, dropped `skip_serializing_if`, or default drift in the
/// common blob or files list is caught as a byte diff.
#[test]
fn common_blob_wire_is_byte_stable() {
    let spec = sample_spec();

    let common_wire = r#"{"table_root":"s3://warehouse/db/table","projection":["id","name"],"filter":"(\"ID\" > 10)","limit":100,"storage":{"inline":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true}}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":200,"s3_max_connections":8}"#;
    assert_eq!(spec.to_common_json(), common_wire);

    let files_wire = r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#;
    assert_eq!(ScanSpec::files_json(&spec.files), files_wire);

    // The common blob is structurally free of the per-shard `files` key and the
    // adapter-only `catalog` key (the flatten preserves this guarantee).
    assert!(!common_wire.contains("\"files\""));
    assert!(!common_wire.contains("catalog"));
}

/// A populated S3 backend for the wrapper-encoding assertions below.
fn wrapper_test_backend() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "AK".into(),
        secret_key: "SK".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// [`ScanStorage`] is EXTERNALLY tagged, never `untagged`: each of its three
/// variants encodes under its own lowercase key, and a payload naming a variant
/// whose shape it does not match is REJECTED rather than resolved to whichever
/// variant happens to parse.
///
/// The untagged pin is the bare-backend case: under `#[serde(untagged)]` an
/// unwrapped `{"s3":{…}}` would silently deserialize as `Inline`, so the wire
/// could not distinguish a credential carried inline from a raw backend
/// arriving where a credential reference was expected.
#[test]
fn scan_storage_is_externally_tagged_and_rejects_a_mismatched_payload() {
    let cases = [
        (
            ScanStorage::Connection {
                name: "LAKEHOUSE_CATALOG_CREDS".into(),
                allow_http: false,
            },
            r#"{"connection":{"name":"LAKEHOUSE_CATALOG_CREDS","allow_http":false}}"#,
        ),
        (
            ScanStorage::Sealed {
                name: "LAKEHOUSE_CATALOG_CREDS".into(),
                payload: "AAAAAAAAAAAAAAAA".into(),
            },
            r#"{"sealed":{"name":"LAKEHOUSE_CATALOG_CREDS","payload":"AAAAAAAAAAAAAAAA"}}"#,
        ),
        (
            ScanStorage::Inline(wrapper_test_backend()),
            r#"{"inline":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"AK","secret_key":"SK","allow_http":true,"path_style":true}}}"#,
        ),
    ];
    for (value, wire) in cases {
        assert_eq!(
            serde_json::to_string(&value).expect("ScanStorage serialization is infallible"),
            wire,
            "{value:?} must encode under its own lowercase variant key"
        );
        assert_eq!(
            serde_json::from_str::<ScanStorage>(wire).expect("its own encoding must decode"),
            value
        );
    }

    for mismatched in [
        // `connection` carrying `sealed`'s fields.
        r#"{"connection":{"name":"C","payload":"AAAA"}}"#,
        // `sealed` carrying `connection`'s fields.
        r#"{"sealed":{"name":"C","allow_http":false}}"#,
        // A bare backend with no wrapper tag at all — the untagged pin.
        r#"{"s3":{"endpoint":"","region":"","access_key":"AK","secret_key":"SK"}}"#,
        // A variant key that does not exist.
        r#"{"reference":{"name":"C"}}"#,
    ] {
        assert!(
            serde_json::from_str::<ScanStorage>(mismatched).is_err(),
            "{mismatched} must be rejected, not resolved to a variant that happens to parse"
        );
    }
}

/// The Delta deletion-vector delete mechanism built from the `table-with-dv-small`
/// fixture's descriptor, carried verbatim: nothing here is resolved into a path or
/// applied to a row at plan time.
fn sample_deletion_vector() -> DeleteMechanism {
    DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::UuidRelative,
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".into(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    }
}

/// Every delete-mechanism list production can build: empty, each Iceberg
/// delete-file variant alone, all three together, and each deletion-vector storage
/// kind alone (the inline kind with no offset, since a file holding one vector alone
/// records none).
///
/// A list MIXING a deletion vector with an Iceberg delete-file reference is
/// deliberately EXCLUDED: `ScanSpec::files_from_json` refuses that pair outright,
/// since one data file cannot carry two independent delete mechanisms, so it is no
/// longer a shape the wire round-trips.
fn every_delete_mechanism_list() -> Vec<Vec<DeleteMechanism>> {
    let positional = DeleteMechanism::IcebergPositionalDelete {
        path: "deletes/d0.parquet".into(),
        size: 50,
    };
    let equality = DeleteMechanism::IcebergEqualityDelete {
        path: "deletes/d1.parquet".into(),
        size: 60,
    };
    let puffin = DeleteMechanism::IcebergPuffinDeletionVector {
        path: "deletes/d2.puffin".into(),
        size: 70,
    };
    vec![
        Vec::new(),
        vec![positional.clone()],
        vec![equality.clone()],
        vec![puffin.clone()],
        vec![positional, equality, puffin],
        vec![sample_deletion_vector()],
        vec![DeleteMechanism::DeltaDeletionVector {
            storage: DeltaDeletionVectorStorage::Inline,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".into(),
            offset: None,
            size_in_bytes: 36,
            cardinality: 2,
        }],
        vec![DeleteMechanism::DeltaDeletionVector {
            storage: DeltaDeletionVectorStorage::AbsolutePath,
            path_or_inline_dv: "s3://bucket/wh/db/t/deletion_vector.bin".into(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }],
    ]
}

/// Every `FileEntry` shape production can build: each partition-value kind (none, a
/// value, an explicit NULL, and both mixed) crossed with every delete-mechanism list.
fn every_file_entry_combination() -> Vec<FileEntry> {
    let partition_value_sets = [
        BTreeMap::new(),
        BTreeMap::from([("region".to_string(), Some("eu".to_string()))]),
        BTreeMap::from([("region".to_string(), None)]),
        BTreeMap::from([
            ("year".to_string(), Some("2026".to_string())),
            ("region".to_string(), None),
        ]),
    ];

    let mut entries = Vec::new();
    for (index, partition_values) in partition_value_sets.into_iter().enumerate() {
        for deletes in every_delete_mechanism_list() {
            entries.push(FileEntry {
                path: format!("data/part-{index:05}.parquet"),
                size: 1024 + index as u64,
                deletes,
                partition_values: partition_values.clone(),
            });
        }
    }
    entries
}

/// A logical field carries EXACTLY ONE binding key, or none at all, and each absent
/// key emits no JSON at all — so an Iceberg field still serializes `"field_id":N` as
/// its first key and gains no second one, which is what keeps every committed Iceberg
/// golden encoding passing unedited.
#[test]
fn a_logical_field_carries_at_most_one_binding_key_and_emits_no_key_for_the_other() {
    let field_of = |field_id, physical_name| LogicalField {
        field_id,
        name: "REGION".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name,
    };

    // Field-id bound (Iceberg, and Delta `id` mapping): `field_id` first, no
    // `physical_name` key at all.
    assert_eq!(
        serde_json::to_string(&field_of(Some(7), None)).unwrap(),
        r#"{"field_id":7,"name":"REGION","arrow_type":"utf8","nullable":true}"#
    );

    // Physical-name bound (Delta `name` mapping): no `field_id` key at all.
    assert_eq!(
        serde_json::to_string(&field_of(None, Some("col-a1b2".to_string()))).unwrap(),
        r#"{"name":"REGION","arrow_type":"utf8","nullable":true,"physical_name":"col-a1b2"}"#
    );

    // Identity bound (Delta `none` mapping): NEITHER key — not even an ordinal
    // standing in for a field-id, which no writer ever wrote into a file.
    assert_eq!(
        serde_json::to_string(&field_of(None, None)).unwrap(),
        r#"{"name":"REGION","arrow_type":"utf8","nullable":true}"#
    );

    // Each shape reconstitutes as itself, so a binding key is never invented on the
    // way back in.
    for field in [
        field_of(Some(7), None),
        field_of(None, Some("col-a1b2".to_string())),
        field_of(None, None),
    ] {
        let json = serde_json::to_string(&field).unwrap();
        assert_eq!(
            serde_json::from_str::<LogicalField>(&json).unwrap(),
            field,
            "{field:?} must survive its own encoding"
        );
    }
}

/// The table's ordered partition-column names ride ONCE in the shard-invariant common
/// blob, so a scan of a table with zero active files still knows which logical columns
/// have no physical counterpart. Absent from JSON when empty, and order-preserving.
#[test]
fn partition_columns_round_trip_in_order_and_default_to_empty() {
    let mut spec = sample_spec();
    spec.common.partition_columns = vec!["year".to_string(), "region".to_string()];

    let common_json = spec.to_common_json();
    assert!(
        common_json.contains(r#""partition_columns":["year","region"]"#),
        "declared order must reach the wire verbatim: {common_json}"
    );
    assert_eq!(
        ScanSpec::from_parts_json(&common_json, "[]")
            .expect("the common blob must reconstitute")
            .common
            .partition_columns,
        vec!["year".to_string(), "region".to_string()],
        "partition-column order must survive the split"
    );

    // A blob written before the field existed reconstitutes with it empty.
    assert!(
        CommonScanSpec::from_json(&sample_spec().to_common_json())
            .expect("an unpartitioned blob must reconstitute")
            .partition_columns
            .is_empty()
    );
}

/// The shard-invariant partition columns and the per-file partition values and delete
/// mechanisms survive the two-argument wire losslessly in BOTH directions, and an
/// Iceberg spec — one with every neutral field empty — serializes byte-identically to
/// its pre-consolidation encoding.
///
/// The common blob's own pre-consolidation bytes are pinned by
/// `common_blob_wire_is_byte_stable`, which passes UNEDITED: that is the
/// byte-identity proof for argument 0. What this test adds is that neither argument
/// gains a partition key while the neutral fields are empty, that both tuple
/// file-entry encodings are unchanged, and that every combination the neutral fields
/// admit round-trips without dropping a field to the shortest-form rule.
#[test]
fn neutral_fields_round_trip_losslessly_and_leave_iceberg_encodings_byte_identical() {
    // Direction 1 — value -> JSON -> value is the identity for every file-entry
    // combination. Direction 2 — re-serializing the reconstituted value
    // reproduces the same bytes, so no field survives the round trip only to be
    // dropped by the next serialization.
    for entry in every_file_entry_combination() {
        let json = ScanSpec::files_json(std::slice::from_ref(&entry));
        let back = ScanSpec::files_from_json(&json)
            .unwrap_or_else(|e| panic!("{entry:?} encoded as {json} failed to parse: {e}"));
        assert_eq!(
            back,
            vec![entry.clone()],
            "the round trip must be lossless for {entry:?}, encoded as {json}"
        );
        assert_eq!(
            ScanSpec::files_json(&back),
            json,
            "re-serializing the reconstituted {entry:?} must reproduce the same bytes"
        );
    }

    // The shard-invariant partition columns survive the common/per-shard split and
    // merge, riding in argument 0 alongside a full per-shard file list.
    for partition_columns in [
        Vec::new(),
        vec!["region".to_string()],
        vec!["region".to_string(), "year".to_string()],
    ] {
        let mut spec = sample_spec();
        spec.common.partition_columns = partition_columns.clone();
        spec.files = every_file_entry_combination();

        let merged =
            ScanSpec::from_parts_json(&spec.to_common_json(), &ScanSpec::files_json(&spec.files))
                .expect("a partitioned spec must reconstitute");

        assert_eq!(
            merged, spec,
            "partition columns {partition_columns:?} must survive the split and merge"
        );
    }

    // An Iceberg spec — every neutral field empty — carries no partition key in
    // either argument, and both file-entry tuple encodings are unchanged.
    let iceberg = sample_spec();
    let common_json = iceberg.to_common_json();
    assert!(
        !common_json.contains("partition_columns"),
        "empty partition columns must not appear in the common blob: {common_json}"
    );
    assert_eq!(
        ScanSpec::files_json(&iceberg.files),
        r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#,
        "the legacy 2-tuple encoding is unchanged"
    );
    assert_eq!(
        ScanSpec::files_json(&[FileEntry::with_deletes(
            "data/part-00002.parquet",
            4096,
            vec![DeleteMechanism::IcebergPositionalDelete {
                path: "deletes/d0.parquet".into(),
                size: 50,
            }],
        )]),
        r#"[["data/part-00002.parquet",4096,[{"path":"deletes/d0.parquet","size":50,"content_type":"position_deletes"}]]]"#,
        "the delete-carrying 3-tuple encoding is unchanged"
    );

    // Absent in JSON means absent in the value: a common blob and a file list
    // written before the neutral fields existed reconstitute with both empty.
    let pre_neutral = ScanSpec::from_parts_json(
        &common_json,
        r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#,
    )
    .expect("a pre-consolidation two-argument wire must still reconstitute");
    assert!(pre_neutral.common.partition_columns.is_empty());
    assert!(
        pre_neutral
            .files
            .iter()
            .all(|entry| entry.partition_values.is_empty())
    );
}

/// A file entry carrying partition values is a self-describing JSON OBJECT, so the
/// 2-tuple legacy form and the 3-tuple delete-carrying form keep their exact
/// encodings AND their deserialization precedence: object and tuple shapes are
/// disjoint, so the object variant cannot change which variant a tuple matches.
///
/// The object form is selected by PARTITION VALUES, not by table format — so a Delta
/// entry whose only extra content is a deletion vector rides in the 3-tuple form,
/// correctly, because the delete member itself names its mechanism.
#[test]
fn a_partitioned_file_entry_is_a_json_object_leaving_the_tuple_forms_untouched() {
    let mixed = r#"[
        ["legacy.parquet", 1],
        ["deleted.parquet", 2, [{"path":"d.parquet","size":3,"content_type":"position_deletes"}]],
        {"path":"partitioned.parquet","size":4,"partition_values":{"region":"eu"}}
    ]"#;

    let files = ScanSpec::files_from_json(mixed).expect("all three wire forms must parse");

    assert_eq!(files[0], FileEntry::new("legacy.parquet", 1));
    assert_eq!(
        files[1].deletes.len(),
        1,
        "the 3-tuple keeps its delete refs"
    );
    assert!(files[1].partition_values.is_empty());
    assert_eq!(files[2].path, "partitioned.parquet");
    assert!(
        files[2].deletes.is_empty(),
        "a partitioned entry need carry no delete mechanism"
    );
    assert_eq!(
        files[2].partition_values,
        BTreeMap::from([("region".to_string(), Some("eu".to_string()))])
    );

    // Serialization is the mirror image: only the partitioned entry becomes an object.
    let encoded = ScanSpec::files_json(&files);
    assert_eq!(
        encoded,
        r#"[["legacy.parquet",1],["deleted.parquet",2,[{"path":"d.parquet","size":3,"content_type":"position_deletes"}]],{"path":"partitioned.parquet","size":4,"partition_values":{"region":"eu"}}]"#
    );

    // A deletion vector alone leaves the entry in the 3-tuple form: the mechanism is
    // self-describing, so no object wrapper is needed to tell the reader what it is.
    assert_eq!(
        ScanSpec::files_json(&[FileEntry::with_deletes(
            "dv.parquet",
            5,
            vec![sample_deletion_vector()],
        )]),
        r#"[["dv.parquet",5,[{"storage":"uuid_relative","path_or_inline_dv":"vBn[lx{q8@P<9BNH/isA","offset":1,"size_in_bytes":36,"cardinality":2}]]]"#
    );
}

/// A partition column whose logged value is NULL is a PRESENT key holding no
/// value; a column missing from the map is absent. The scan materializes the
/// first and can detect the second as a planning defect, which collapsing both
/// onto one encoding would make impossible. Keys serialize in a deterministic
/// order regardless of insertion order, so a golden encoding is byte-stable.
#[test]
fn partition_values_distinguish_an_explicit_null_from_an_absent_column() {
    let json = r#"[{"path":"f.parquet","size":1,"partition_values":{"region":null}}]"#;

    let files = ScanSpec::files_from_json(json).expect("an explicit NULL must parse");
    let partition_values = &files[0].partition_values;

    assert_eq!(
        partition_values.get("region"),
        Some(&None),
        "an explicit NULL is a present key holding no value"
    );
    assert_eq!(
        partition_values.get("year"),
        None,
        "a column absent from the map stays absent — never read as NULL"
    );
    assert_eq!(
        ScanSpec::files_json(&files),
        json.replace(' ', ""),
        "an explicit NULL re-serializes as null, not as an omitted key"
    );

    // Insertion order does not reach the wire: the map's key order does.
    let unordered = FileEntry::with_partition_values(
        "f.parquet",
        1,
        BTreeMap::from([
            ("year".to_string(), Some("2026".to_string())),
            ("region".to_string(), None),
        ]),
    );
    assert_eq!(
        ScanSpec::files_json(&[unordered]),
        r#"[{"path":"f.parquet","size":1,"partition_values":{"region":null,"year":"2026"}}]"#
    );
}

/// The deletion-vector storage kind is a CLOSED set of the Delta protocol's three
/// kinds, so a descriptor naming a fourth is refused at reconstitution rather
/// than reaching the scan as an unread string. The refusal identifies scan-files
/// deserialization and echoes none of the input.
#[test]
fn deletion_vector_storage_kind_outside_the_closed_set_is_refused() {
    let json = r#"[["f.parquet",1,[{"storage":"puffin","path_or_inline_dv":"x","size_in_bytes":1,"cardinality":1}]]]"#;

    let err = ScanSpec::files_from_json(json)
        .expect_err("a storage kind outside the closed set must be refused");

    assert!(
        err.contains("scan files deserialization failed"),
        "the refusal identifies scan-files deserialization: {err}"
    );
    assert!(
        !err.contains("path_or_inline_dv"),
        "the refusal must not echo the input: {err}"
    );
}

/// An entry whose ONE delete list mixes a deletion vector with an Iceberg delete-file
/// reference would have the scan apply two independent delete mechanisms to one data
/// file and return wrong rows. The wire refuses it at reconstitution — the same place a
/// deletion-vector storage kind outside the closed set is refused — identifying the
/// offending entry by INDEX, because the entry's path and the raw input are text this
/// message never echoes.
#[test]
fn a_file_entry_mixing_a_deletion_vector_with_an_iceberg_delete_reference_is_refused() {
    let json = r#"[
        ["clean.parquet", 1],
        ["contested.parquet", 2, [
            {"path":"d0.parquet","size":3,"content_type":"position_deletes"},
            {"storage":"uuid_relative","path_or_inline_dv":"vBn[lx","size_in_bytes":36,"cardinality":2}
        ]]
    ]"#;

    let err = ScanSpec::files_from_json(json)
        .expect_err("a delete list mixing both mechanisms must be refused");

    assert!(
        err.contains("scan files deserialization failed"),
        "the refusal identifies scan-files deserialization: {err}"
    );
    assert!(
        err.contains("entry 1"),
        "the refusal names the offending entry's index: {err}"
    );
    assert!(
        !err.contains("contested.parquet") && !err.contains("d0.parquet"),
        "the refusal must echo neither path: {err}"
    );
    assert!(
        !err.contains("vBn[lx"),
        "the refusal must not echo the deletion vector's reference: {err}"
    );
}

/// A delete list holding ONLY Iceberg delete-file references, and one holding ONLY a
/// deletion vector, are both accepted: the gate turns on MIXING the two mechanisms,
/// not on either one appearing.
#[test]
fn a_delete_list_holding_one_mechanism_kind_is_accepted() {
    let iceberg_only = r#"[["f.parquet",1,[
        {"path":"d0.parquet","size":3,"content_type":"position_deletes"},
        {"path":"d1.parquet","size":4,"content_type":"equality_deletes"}
    ]]]"#;
    assert_eq!(
        ScanSpec::files_from_json(iceberg_only)
            .expect("an Iceberg-only delete list must reconstitute")[0]
            .deletes
            .len(),
        2
    );

    let vector_only = r#"[["f.parquet",1,[
        {"storage":"inline","path_or_inline_dv":"x","size_in_bytes":36,"cardinality":2}
    ]]]"#;
    assert_eq!(
        ScanSpec::files_from_json(vector_only)
            .expect("a deletion-vector-only delete list must reconstitute")[0]
            .deletes,
        vec![DeleteMechanism::DeltaDeletionVector {
            storage: DeltaDeletionVectorStorage::Inline,
            path_or_inline_dv: "x".into(),
            offset: None,
            size_in_bytes: 36,
            cardinality: 2,
        }]
    );
}

/// Each delete mechanism names ITSELF on the wire, so the scan reads one list and
/// dispatches on content. The three Iceberg variants keep their pre-unification
/// `{"path":…,"size":…,"content_type":…}` object with its key ORDER, which is what
/// makes every committed 3-tuple golden encoding pass unedited; the deletion vector
/// carries all five of its members in its own key order.
#[test]
fn every_delete_mechanism_serializes_its_own_self_describing_form() {
    let cases = [
        (
            DeleteMechanism::IcebergPositionalDelete {
                path: "deletes/d0.parquet".into(),
                size: 50,
            },
            r#"[["f.parquet",1,[{"path":"deletes/d0.parquet","size":50,"content_type":"position_deletes"}]]]"#,
        ),
        (
            DeleteMechanism::IcebergEqualityDelete {
                path: "deletes/d1.parquet".into(),
                size: 60,
            },
            r#"[["f.parquet",1,[{"path":"deletes/d1.parquet","size":60,"content_type":"equality_deletes"}]]]"#,
        ),
        (
            DeleteMechanism::IcebergPuffinDeletionVector {
                path: "deletes/d2.puffin".into(),
                size: 70,
            },
            r#"[["f.parquet",1,[{"path":"deletes/d2.puffin","size":70,"content_type":"puffin_deletion_vector"}]]]"#,
        ),
        (
            sample_deletion_vector(),
            r#"[["f.parquet",1,[{"storage":"uuid_relative","path_or_inline_dv":"vBn[lx{q8@P<9BNH/isA","offset":1,"size_in_bytes":36,"cardinality":2}]]]"#,
        ),
    ];

    for (mechanism, expected) in cases {
        let entry = FileEntry::with_deletes("f.parquet", 1, vec![mechanism.clone()]);
        let json = ScanSpec::files_json(std::slice::from_ref(&entry));
        assert_eq!(json, expected, "{mechanism:?} must keep its own encoding");
        assert_eq!(
            ScanSpec::files_from_json(&json).expect("its own encoding must reconstitute"),
            vec![entry],
            "{mechanism:?} must survive the round trip"
        );
    }
}

/// Only a mechanism naming a whole delete FILE exposes an object-store path. The
/// deletion vector exposes NONE: its `path_or_inline_dv` is resolved into a path at
/// file registration and is never addressed from the delete list itself, so a caller
/// that fetched, relativized, or claimed store ownership of it as a path here would be
/// wrong — regardless of whether the value looks like a UUID token, an inline
/// payload, or an absolute path.
#[test]
fn only_a_delete_file_mechanism_exposes_an_object_store_path() {
    assert_eq!(
        DeleteMechanism::IcebergPositionalDelete {
            path: "deletes/d0.parquet".into(),
            size: 50,
        }
        .object_store_path(),
        Some("deletes/d0.parquet")
    );
    assert_eq!(
        DeleteMechanism::IcebergEqualityDelete {
            path: "deletes/d1.parquet".into(),
            size: 60,
        }
        .object_store_path(),
        Some("deletes/d1.parquet")
    );
    assert_eq!(
        DeleteMechanism::IcebergPuffinDeletionVector {
            path: "deletes/d2.puffin".into(),
            size: 70,
        }
        .object_store_path(),
        Some("deletes/d2.puffin")
    );
    assert_eq!(
        sample_deletion_vector().object_store_path(),
        None,
        "a deletion vector's path_or_inline_dv is resolved at file registration, never addressed from the delete list as a path"
    );
}

/// An Iceberg delete content type outside the closed set is refused at
/// reconstitution rather than silently read as a supported mechanism — the same
/// treatment an unknown deletion-vector storage kind gets, and for the same reason.
#[test]
fn an_iceberg_delete_content_type_outside_the_closed_set_is_refused() {
    let json =
        r#"[["f.parquet",1,[{"path":"d0.parquet","size":3,"content_type":"dictionary_deletes"}]]]"#;

    let err = ScanSpec::files_from_json(json)
        .expect_err("a content type outside the closed set must be refused");

    assert!(
        err.contains("scan files deserialization failed"),
        "the refusal identifies scan-files deserialization: {err}"
    );
    assert!(
        !err.contains("d0.parquet"),
        "the refusal must not echo the input: {err}"
    );
}

/// The reconstituted scan spec carries no catalog handle — not the table's
/// catalog-assigned credential-vending key, not any other catalog identifier —
/// because the scan UDF never contacts the catalog. Asserted with the neutral
/// partition fields POPULATED, since they are the fields a planning-side identifier
/// would ride in on.
#[test]
fn a_partitioned_scan_spec_carries_no_catalog_identifier() {
    let mut spec = sample_spec();
    spec.common.partition_columns = vec!["region".to_string(), "year".to_string()];
    spec.files = every_file_entry_combination();

    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);

    for (argument, json) in [("common blob", &common_json), ("files list", &files_json)] {
        for identifier in ["vended_credential_key", "table_id", "catalog"] {
            assert!(
                !json.contains(identifier),
                "the {argument} must not carry the catalog identifier `{identifier}`: {json}"
            );
        }
    }
}

/// The deletion-vector descriptor crosses the wire with all five of its members
/// intact and its `pathOrInlineDv` stored exactly as logged — resolved into no
/// path and joined onto no table root, because applying it is the scan side's job.
#[test]
fn deletion_vector_is_carried_verbatim_with_every_member() {
    let entry = FileEntry::with_deletes(
        "data/part-00000.parquet",
        1024,
        vec![sample_deletion_vector()],
    );

    let json = ScanSpec::files_json(std::slice::from_ref(&entry));
    assert_eq!(
        json,
        r#"[["data/part-00000.parquet",1024,[{"storage":"uuid_relative","path_or_inline_dv":"vBn[lx{q8@P<9BNH/isA","offset":1,"size_in_bytes":36,"cardinality":2}]]]"#
    );

    let carried = ScanSpec::from_parts_json(&sample_spec().to_common_json(), &json)
        .expect("a deletion-vector entry must reconstitute")
        .files
        .remove(0)
        .deletes;
    assert_eq!(
        carried,
        vec![sample_deletion_vector()],
        "all five members survive the two-argument wire as one mechanism"
    );
    let DeleteMechanism::DeltaDeletionVector {
        path_or_inline_dv, ..
    } = &carried[0]
    else {
        panic!("the deletion-vector variant survives the two-argument wire")
    };
    assert_eq!(
        path_or_inline_dv, "vBn[lx{q8@P<9BNH/isA",
        "the reference is stored verbatim, never joined onto the table root"
    );
}

/// A logical schema authored before the nested descriptor existed still reconstitutes,
/// so an in-flight spec from an older adapter build is never rejected by the scan.
#[test]
fn a_logical_field_authored_before_the_nested_descriptor_deserializes_unchanged() {
    let old_shape = r#"{"field_id":7,"name":"REGION","arrow_type":"utf8","nullable":true}"#;

    let field: LogicalField =
        serde_json::from_str(old_shape).expect("the pre-descriptor encoding must reconstitute");

    assert_eq!(
        field,
        LogicalField {
            field_id: Some(7),
            name: "REGION".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        "an absent nested key must read as no descriptor, not as an error"
    );
}

/// A primitive column's encoding gains NO key from the descriptor, so every committed
/// golden encoding of a primitive-only logical schema still passes unedited.
#[test]
fn a_primitive_logical_field_serializes_no_nested_key() {
    let primitive = LogicalField {
        field_id: Some(7),
        name: "REGION".to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    };

    assert_eq!(
        serde_json::to_string(&primitive).unwrap(),
        r#"{"field_id":7,"name":"REGION","arrow_type":"utf8","nullable":true}"#
    );
}

/// Every container kind is representable at depth, and each nested field carries the
/// SAME single binding key its top-level counterpart would — a field-id for Iceberg, a
/// physical name for Delta `name` mapping, neither for identity binding.
#[test]
fn a_nested_descriptor_round_trips_every_container_kind_at_depth() {
    // list<struct<street(3), city(4)>> under Iceberg field-id binding.
    let list_of_struct = NestedMembers::List {
        element: Some(Box::new(NestedMembers::Struct {
            fields: vec![
                NestedField {
                    field_id: Some(3),
                    name: "street".to_string(),
                    physical_name: None,
                    nested: None,
                },
                NestedField {
                    field_id: Some(4),
                    name: "city".to_string(),
                    physical_name: None,
                    nested: None,
                },
            ],
        })),
    };
    assert_eq!(
        serde_json::to_string(&list_of_struct).unwrap(),
        r#"{"list":{"element":{"struct":{"fields":[{"field_id":3,"name":"street"},{"field_id":4,"name":"city"}]}}}}"#
    );

    // struct<inner_int> under Delta `name` mapping: the physical name is the binding key.
    let name_mapped_struct = NestedMembers::Struct {
        fields: vec![NestedField {
            field_id: None,
            name: "inner_int".to_string(),
            physical_name: Some("col-7f2f94cf".to_string()),
            nested: None,
        }],
    };
    assert_eq!(
        serde_json::to_string(&name_mapped_struct).unwrap(),
        r#"{"struct":{"fields":[{"name":"inner_int","physical_name":"col-7f2f94cf"}]}}"#
    );

    // map<int, struct<a>>: a positional member carries only its own members, so a map of
    // primitives encodes as an empty object rather than inventing names for key/value.
    let map_of_struct = NestedMembers::Map {
        key: None,
        value: Some(Box::new(NestedMembers::Struct {
            fields: vec![NestedField {
                field_id: None,
                name: "a".to_string(),
                physical_name: None,
                nested: None,
            }],
        })),
    };
    assert_eq!(
        serde_json::to_string(&map_of_struct).unwrap(),
        r#"{"map":{"value":{"struct":{"fields":[{"name":"a"}]}}}}"#
    );
    assert_eq!(
        serde_json::to_string(&NestedMembers::Map {
            key: None,
            value: None
        })
        .unwrap(),
        r#"{"map":{}}"#
    );
    assert_eq!(
        serde_json::to_string(&NestedMembers::List { element: None }).unwrap(),
        r#"{"list":{}}"#
    );

    for members in [
        list_of_struct,
        name_mapped_struct,
        map_of_struct,
        NestedMembers::List { element: None },
        NestedMembers::Map {
            key: None,
            value: None,
        },
    ] {
        let field = LogicalField {
            field_id: Some(2),
            name: "ADDR".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: Some(members),
            physical_name: None,
        };
        let json = serde_json::to_string(&field).unwrap();
        assert!(
            json.contains(r#""arrow_type":"utf8""#),
            "a nested column's logical type stays the utf8 tag: {json}"
        );
        assert_eq!(
            serde_json::from_str::<LogicalField>(&json).unwrap(),
            field,
            "{field:?} must survive its own encoding"
        );
    }
}
