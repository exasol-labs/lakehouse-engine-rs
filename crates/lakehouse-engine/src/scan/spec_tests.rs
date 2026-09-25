use super::*;

/// Scenario: a `://`-bearing entry is absolute and passes through unchanged.
#[test]
fn reconstruct_absolute_entry_passes_through() {
    assert_eq!(
        reconstruct_abs_uri(
            "s3://bucket/db/table/data/f.parquet",
            "s3://bucket/db/table"
        ),
        "s3://bucket/db/table/data/f.parquet"
    );
    assert_eq!(
        reconstruct_abs_uri("s3://other/x.parquet", ""),
        "s3://other/x.parquet"
    );
}

/// Scenario: a relative entry joins onto the root with exactly one separator.
#[test]
fn reconstruct_relative_entry_normalizes_single_separator() {
    let expected = "s3://bucket/db/table/data/f.parquet";
    assert_eq!(
        reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table"),
        expected
    );
    assert_eq!(
        reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table/"),
        expected
    );
    assert_eq!(
        reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table"),
        expected
    );
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

fn s3_props(storage: &ScanStorage) -> &StorageProps {
    let ScanStorage::Inline(StorageBackend::S3(props)) = storage else {
        panic!("s3_props is inline-S3-only")
    };
    props
}

/// Scenario: `CommonScanSpec::default()` matches serde's field-absent defaults except `s3_max_connections`.
#[test]
fn default_matches_serde_absent_except_s3_max_connections() {
    let minimal = r#"{"projection":[],"storage":{"inline":{"s3":{"endpoint":"","region":"","access_key":"","secret_key":""}}}}"#;
    let from_absent = CommonScanSpec::from_json(minimal).unwrap();
    let d = CommonScanSpec::default();

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

    assert_eq!(d.s3_max_connections, 8);
    assert_eq!(from_absent.s3_max_connections, DEFAULT_S3_MAX_CONNECTIONS);
}

/// Scenario: a scan spec round-trips through the `Value::String` boundary with credentials intact.
#[test]
fn scan_spec_round_trips_through_value_boundary() {
    let spec = sample_spec();

    let json = spec.to_json();
    let _value_string: String = json.clone();

    assert!(
        json.contains(
            r#""files":[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#
        ),
        "files must serialize as compact [path,size] 2-tuples: {json}"
    );

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

/// Scenario: the common blob carries no EMITS-type key, and a legacy blob declaring one is inert (#399).
#[test]
fn common_blob_carries_no_emit_type_key() {
    let row_spec = sample_spec();
    assert!(
        !row_spec.common.projection.is_empty(),
        "fixture must be a projecting row scan — the shape that carried the key"
    );

    let common_json = row_spec.common.to_json();
    assert!(
        !common_json.contains("emit_exa_types"),
        "common blob must carry no EMITS-type key: {common_json}"
    );
    assert!(
        !row_spec.to_json().contains("emit_exa_types"),
        "whole-spec JSON must carry no EMITS-type key either"
    );

    let mut legacy: serde_json::Value = serde_json::from_str(&common_json).unwrap();
    legacy.as_object_mut().unwrap().insert(
        "emit_exa_types".to_string(),
        serde_json::json!(["DECIMAL(20,0)", "VARCHAR(2000000)"]),
    );
    assert_eq!(
        CommonScanSpec::from_json(&legacy.to_string()).unwrap(),
        row_spec.common,
        "a blob still declaring EMITS types must reconstitute identically"
    );
}

/// Scenario: an aggregate plan round-trips through JSON and is absent from row-scan specs.
#[test]
fn aggregate_plan_round_trips_and_absent_from_row_scan() {
    let row_spec = sample_spec();
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("aggregates"),
        "row-scan spec must not carry aggregates field: {row_json}"
    );

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

/// Scenario: `AggregatePlan.arg_expr` round-trips and is omitted from JSON when `None`.
#[test]
fn arg_expr_round_trips_and_omitted_when_none() {
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

/// Scenario: every `AggKind`'s partial column set matches literal expected arity and order.
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

/// Scenario: `is_counter()` holds for exactly the four counter columns.
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

/// Scenario: `partial_column_name` renders `PARTIAL_<role>_<ordinal>` for all ten partial columns.
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

/// Scenario: `SortKey::render_ordered` delegates to the free `render_ordered` on every flag combination.
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

/// Scenario: `order_by` round-trips, is omitted when empty, and defaults to empty on a legacy payload.
#[test]
fn order_by_round_trips_and_defaults_to_empty() {
    let row_spec = sample_spec();
    assert!(row_spec.common.order_by.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("order_by"),
        "empty order_by must be absent from JSON: {row_json}"
    );

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

    assert_eq!(back, spec);

    let common = spec.to_common();
    assert_eq!(common.order_by, spec.common.order_by);
    let merged = ScanSpec::from_parts(common, spec.files.clone());
    assert_eq!(merged.common.order_by, spec.common.order_by);

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

/// Scenario: `group_keys` round-trips through JSON and is absent from row-scan specs.
#[test]
fn group_keys_round_trips_and_absent_from_row_scan() {
    let row_spec = sample_spec();
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("group_keys"),
        "row-scan spec must not carry group_keys field: {row_json}"
    );

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
    assert!(!err.contains("SECRET"));
    assert!(!err.contains("TOPSECRET"));
    assert!(err.contains("scan spec deserialization failed"));
}

/// Scenario: `logical_schema` round-trips and defaults to empty on a legacy payload.
#[test]
fn logical_schema_round_trips_and_defaults_to_empty() {
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

    assert!(
        json.contains("logical_schema"),
        "non-empty logical_schema must appear in JSON: {json}"
    );

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

    let row_spec = sample_spec();
    assert!(row_spec.common.logical_schema.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("logical_schema"),
        "empty logical_schema must be absent from JSON: {row_json}"
    );

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

/// Scenario: `name_mapping` round-trips and defaults to empty on a legacy payload.
#[test]
fn name_mapping_round_trips_and_defaults_to_empty() {
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

    assert!(
        json.contains("name_mapping"),
        "non-empty name_mapping must appear in JSON: {json}"
    );

    let back = ScanSpec::from_json(&json).unwrap();
    let entries = &back.common.name_mapping;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name, "id");
    assert_eq!(entries[0].field_id, 1);
    assert_eq!(entries[1].name, "rating");
    assert_eq!(entries[1].field_id, 2);

    let row_spec = sample_spec();
    assert!(row_spec.common.name_mapping.is_empty());
    let row_json = row_spec.to_json();
    assert!(
        !row_json.contains("name_mapping"),
        "empty name_mapping must be absent from JSON: {row_json}"
    );

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

/// Scenario: the threading fields round-trip and default to 1 when absent.
#[test]
fn scan_spec_threading_fields_round_trip_and_default_to_one() {
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

    assert!(
        json.contains("df_target_partitions"),
        "serialized JSON must carry df_target_partitions: {json}"
    );
    assert!(
        json.contains("df_threads_per_udf"),
        "serialized JSON must carry df_threads_per_udf: {json}"
    );

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

/// Scenario: `df_batch_size` round-trips and defaults to 8192 on a legacy payload.
#[test]
fn df_batch_size_round_trips_and_defaults() {
    let mut spec = sample_spec();
    spec.common.df_batch_size = 4096;
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.df_batch_size, 4096,
        "df_batch_size must survive round-trip"
    );

    assert!(
        json.contains("df_batch_size"),
        "serialized JSON must carry df_batch_size: {json}"
    );

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

/// Scenario: `memory_pool_fraction` and `instance_overhead_mb` round-trip and default to 0.6 / 200.
#[test]
fn scan_spec_memory_fields_round_trip_and_default() {
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

/// Scenario: `s3_max_connections` round-trips and defaults to the built-in budget when absent.
#[test]
fn s3_max_connections_round_trips_and_defaults() {
    let mut spec = sample_spec();
    spec.common.s3_max_connections = 32;
    let json = spec.to_json();
    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(
        back.common.s3_max_connections, 32,
        "s3_max_connections must survive round-trip"
    );

    assert!(
        json.contains("s3_max_connections"),
        "serialized JSON must carry s3_max_connections: {json}"
    );

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

/// Scenario: the common blob carries `table_root` but no `files`, and `from_parts_json` reconstitutes the original.
#[test]
fn from_parts_reconstitutes_files_tuples_and_table_root() {
    let original = sample_spec();

    let common_json = original.to_common_json();
    let files_json = ScanSpec::files_json(&original.files);

    assert!(
        !common_json.contains("\"files\""),
        "common blob must not contain a files key: {common_json}"
    );
    assert!(
        !common_json.contains("part-00000.parquet"),
        "common blob must not carry any file path: {common_json}"
    );
    assert!(
        common_json.contains(r#""table_root":"s3://warehouse/db/table""#),
        "common blob must carry table_root: {common_json}"
    );

    assert_eq!(
        files_json,
        r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#
    );

    let common_back = CommonScanSpec::from_json(&common_json).unwrap();
    assert_eq!(common_back, original.to_common());
    assert_eq!(common_back.table_root, "s3://warehouse/db/table");

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

    let via_struct = ScanSpec::from_parts(original.to_common(), original.files.clone());
    assert_eq!(via_struct, original);
}

/// Scenario: malformed common or files JSON produces errors that never echo the raw input.
#[test]
fn malformed_common_or_files_json_does_not_leak_credentials() {
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

    let garbled_files = r#"["s3://w/SECRETFILE.parquet", incomplete"#;
    let files_err = ScanSpec::files_from_json(garbled_files).unwrap_err();
    assert!(
        !files_err.contains("SECRETFILE"),
        "files error leaked input: {files_err}"
    );
    assert!(files_err.contains("scan files deserialization failed"));

    let combined = ScanSpec::from_parts_json(garbled_common, "[]").unwrap_err();
    assert!(!combined.contains("SECRET"));
    assert!(!combined.contains("TOPSECRET"));
}

/// Scenario: `table_root` round-trips, and a legacy payload without it defaults to empty (all paths absolute).
#[test]
fn legacy_empty_root_treats_paths_as_absolute() {
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

    let mut rootless = sample_spec();
    rootless.common.table_root = String::new();
    let rootless_json = rootless.to_json();
    assert!(
        !rootless_json.contains("table_root"),
        "empty table_root must be absent from JSON: {rootless_json}"
    );

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

    let legacy_common_json = r#"{
        "projection": [],
        "storage": {"inline": {"s3": {"endpoint": "http://minio:9000", "region": "us-east-1", "access_key": "k", "secret_key": "s"}}}
    }"#;
    let legacy_common = CommonScanSpec::from_json(legacy_common_json).unwrap();
    assert_eq!(
        legacy_common.table_root, "",
        "missing table_root must default to empty on the common blob (backward-compat)"
    );

    let reconstituted = ScanSpec::from_parts(
        legacy_common,
        vec![FileEntry::new("s3://w/f0.parquet", 100)],
    );
    assert_eq!(reconstituted.common.table_root, "");
}

/// Scenario: `catalog` appears in no serialized JSON.
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

/// Scenario: a spec without a join block omits `join`, and a legacy payload defaults it to `None`.
#[test]
fn absent_join_block_round_trips_unchanged() {
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

/// Scenario: a legacy `[path, size]` file entry reconstitutes with empty `deletes`.
#[test]
fn legacy_file_entry_reconstitutes_empty_deletes() {
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

    // Not a 3-tuple with a trailing empty array: the delete-free wire stays minimal.
    let round_tripped = ScanSpec::files_json(&files);
    assert_eq!(
        round_tripped,
        files_only_json.replace(' ', ""),
        "delete-free entries must round-trip to the compact [path,size] form: {round_tripped}"
    );

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

/// Scenario: a join block rides in the common blob and round-trips through split and merge.
#[test]
fn join_block_round_trips_through_split_and_merge() {
    let mut spec = sample_spec();
    // Distinct from `sample_spec()`'s storage, so the dimension backend round-trips on its own.
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

    let json = spec.to_json();
    assert!(
        json.contains("\"join\""),
        "join spec must carry the join block: {json}"
    );
    assert!(
        json.contains("\"join_type\":\"inner\""),
        "join_type must serialize as the lowercase tag: {json}"
    );

    let back = ScanSpec::from_json(&json).unwrap();
    assert_eq!(back, spec);

    let common = spec.to_common();
    assert_eq!(common.join, spec.common.join);
    let common_json = spec.to_common_json();
    assert!(
        common_json.contains("dim-00000.parquet"),
        "dimension files must ride in the shard-invariant common blob: {common_json}"
    );

    let files_json = ScanSpec::files_json(&spec.files);
    assert!(
        !files_json.contains("dim-00000.parquet"),
        "per-shard files must not carry dimension files: {files_json}"
    );

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

    assert_eq!(jb.storage, ScanStorage::Inline(dim_storage));
    assert_ne!(jb.storage, reconstituted.common.storage);

    let via_struct = ScanSpec::from_parts(spec.to_common(), spec.files.clone());
    assert_eq!(via_struct, spec);
}

/// Scenario: a join block without `post_join_limit` loads as `None`, and a set cap survives the split.
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

/// Scenario: `JoinSpec::partition_columns` defaults to empty and keeps Iceberg join JSON byte-identical.
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

/// Scenario: the common blob and files wire stay byte-identical to strings captured before `#[serde(flatten)]`.
#[test]
fn common_blob_wire_is_byte_stable() {
    let spec = sample_spec();

    let common_wire = r#"{"table_root":"s3://warehouse/db/table","projection":["id","name"],"filter":"(\"ID\" > 10)","limit":100,"storage":{"inline":{"s3":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true}}},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":200,"s3_max_connections":8}"#;
    assert_eq!(spec.to_common_json(), common_wire);

    let files_wire = r#"[["data/part-00000.parquet",1024],["data/part-00001.parquet",2048]]"#;
    assert_eq!(ScanSpec::files_json(&spec.files), files_wire);

    assert!(!common_wire.contains("\"files\""));
    assert!(!common_wire.contains("catalog"));
}

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
        r#"{"connection":{"name":"C","payload":"AAAA"}}"#,
        r#"{"sealed":{"name":"C","allow_http":false}}"#,
        r#"{"s3":{"endpoint":"","region":"","access_key":"AK","secret_key":"SK"}}"#,
        r#"{"reference":{"name":"C"}}"#,
    ] {
        assert!(
            serde_json::from_str::<ScanStorage>(mismatched).is_err(),
            "{mismatched} must be rejected, not resolved to a variant that happens to parse"
        );
    }
}

/// The `table-with-dv-small` fixture's deletion-vector descriptor, carried verbatim.
fn sample_deletion_vector() -> DeleteMechanism {
    DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::UuidRelative,
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".into(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    }
}

/// Every delete-mechanism list production can build. A list mixing a deletion vector
/// with an Iceberg delete-file reference is excluded: `files_from_json` refuses it.
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

/// Every partition-value kind crossed with every delete-mechanism list.
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

/// Scenario: a logical field carries at most one binding key and emits no JSON for an absent one.
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

    assert_eq!(
        serde_json::to_string(&field_of(Some(7), None)).unwrap(),
        r#"{"field_id":7,"name":"REGION","arrow_type":"utf8","nullable":true}"#
    );

    assert_eq!(
        serde_json::to_string(&field_of(None, Some("col-a1b2".to_string()))).unwrap(),
        r#"{"name":"REGION","arrow_type":"utf8","nullable":true,"physical_name":"col-a1b2"}"#
    );

    // No ordinal stands in for a field-id, which no writer ever wrote into a file.
    assert_eq!(
        serde_json::to_string(&field_of(None, None)).unwrap(),
        r#"{"name":"REGION","arrow_type":"utf8","nullable":true}"#
    );

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

/// Scenario: `partition_columns` round-trips in order and defaults to empty.
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

    assert!(
        CommonScanSpec::from_json(&sample_spec().to_common_json())
            .expect("an unpartitioned blob must reconstitute")
            .partition_columns
            .is_empty()
    );
}

/// Scenario: neutral fields round-trip losslessly both ways and leave Iceberg encodings byte-identical.
#[test]
fn neutral_fields_round_trip_losslessly_and_leave_iceberg_encodings_byte_identical() {
    // Re-serializing must reproduce the same bytes, so no field is dropped on the next pass.
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

/// Scenario: a partitioned file entry is a JSON object, leaving the tuple forms and their precedence untouched.
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

    let encoded = ScanSpec::files_json(&files);
    assert_eq!(
        encoded,
        r#"[["legacy.parquet",1],["deleted.parquet",2,[{"path":"d.parquet","size":3,"content_type":"position_deletes"}]],{"path":"partitioned.parquet","size":4,"partition_values":{"region":"eu"}}]"#
    );

    assert_eq!(
        ScanSpec::files_json(&[FileEntry::with_deletes(
            "dv.parquet",
            5,
            vec![sample_deletion_vector()],
        )]),
        r#"[["dv.parquet",5,[{"storage":"uuid_relative","path_or_inline_dv":"vBn[lx{q8@P<9BNH/isA","offset":1,"size_in_bytes":36,"cardinality":2}]]]"#
    );
}

/// Scenario: an explicit NULL partition value stays distinct from an absent column, with deterministic key order.
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

/// Scenario: a deletion-vector storage kind outside the closed set is refused without echoing the input.
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

/// Scenario: an entry mixing a deletion vector with an Iceberg delete reference is refused, identified by index.
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

/// Scenario: a delete list holding only one mechanism kind is accepted.
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

/// Scenario: every delete mechanism serializes its own self-describing form, Iceberg keeping its key order.
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

/// Scenario: only a delete-file mechanism exposes an object-store path; a deletion vector exposes none.
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

/// Scenario: an Iceberg delete content type outside the closed set is refused.
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

/// Scenario: a partitioned scan spec carries no catalog identifier.
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

/// Scenario: a deletion-vector descriptor is carried verbatim with every member.
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

/// Scenario: a logical field authored before the nested descriptor deserializes unchanged.
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

/// Scenario: a primitive logical field serializes no nested key.
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

/// Scenario: a nested descriptor round-trips every container kind at depth with the same binding key.
#[test]
fn a_nested_descriptor_round_trips_every_container_kind_at_depth() {
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

    // A map of primitives encodes as an empty object rather than inventing key/value names.
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
