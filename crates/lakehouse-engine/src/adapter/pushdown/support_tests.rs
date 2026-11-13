use super::super::scalar_over_agg::cast_merge_items;
use super::super::test_support::*;
use super::*;
use crate::scan::spec::{AggKind, DeleteMechanism, ScanStorage, SortKey, StorageProps};
use crate::scan::test_support::WithFiles;
use crate::types::exasol_type::exasol_type_to_json;

fn delete_spec_template() -> ScanSpec {
    CommonScanSpec {
        table_root: "s3://warehouse/db/table".into(),
        projection: vec![ProjectionItem::Column("ID".into())],
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![])
}

/// Scenario: Delete-file references are carried in the per-shard files argument
#[test]
fn adapter_preserves_delete_files_and_deletion_vectors_in_the_shard_files_argument() {
    let file_gran = vec![FileEntry::with_deletes(
        "data/part-0.parquet",
        1000,
        vec![pos_delete("data/deletes/del-0.parquet", 50)],
    )];
    let back = ScanSpec::files_from_json(&shard_files_json(&file_gran)).unwrap();
    assert_eq!(back, file_gran, "file-granularity deletes must round-trip");
    assert_eq!(back[0].deletes.len(), 1);
    assert!(matches!(
        back[0].deletes[0],
        DeleteMechanism::IcebergPositionalDelete { .. }
    ));

    let shared = "data/deletes/part-del.parquet";
    let part_gran = vec![
        FileEntry::with_deletes("data/p0.parquet", 1, vec![pos_delete(shared, 80)]),
        FileEntry::with_deletes("data/p1.parquet", 1, vec![pos_delete(shared, 80)]),
    ];
    let back2 = ScanSpec::files_from_json(&shard_files_json(&part_gran)).unwrap();
    assert_eq!(
        back2, part_gran,
        "both data files must retain the shared partition delete"
    );
    assert_eq!(back2[1].deletes[0].object_store_path(), Some(shared));

    let vector = DeleteMechanism::IcebergPuffinDeletionVector {
        path: "data/deletes/dv.puffin".into(),
        size: 300,
        content_offset: 4,
        content_size_in_bytes: 40,
        record_count: 2,
    };
    let with_vector = vec![FileEntry::with_deletes("data/p2.parquet", 1, vec![vector])];
    assert_eq!(
        ScanSpec::files_from_json(&shard_files_json(&with_vector)).unwrap(),
        with_vector,
        "a deletion vector keeps its blob offset, length, and cardinality"
    );
}

#[test]
fn delete_file_entry_carries_content_type_and_delete_free_stays_compact() {
    let with_del = vec![FileEntry::with_deletes(
        "d.parquet",
        5,
        vec![pos_delete("del.parquet", 2)],
    )];
    let json = shard_files_json(&with_del);
    assert!(
        json.contains("position_deletes"),
        "delete content type must appear on the wire: {json}"
    );
    let back = ScanSpec::files_from_json(&json).unwrap();
    assert!(matches!(
        back[0].deletes[0],
        DeleteMechanism::IcebergPositionalDelete { .. }
    ));

    let free = vec![FileEntry::new("data/part-0.parquet", 1000)];
    assert_eq!(
        shard_files_json(&free),
        r#"[["data/part-0.parquet",1000]]"#,
        "delete-free entry must stay the compact 2-tuple form"
    );
}

#[test]
fn adapter_carries_delete_refs_per_shard_minimal_common_spec() {
    let spec_template = delete_spec_template();
    let shards = vec![vec![FileEntry::with_deletes(
        "data/part-0.parquet",
        1000,
        vec![pos_delete("data/deletes/del-0.parquet", 50)],
    )]];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(
            &[ProjectionItem::Column("ID".into())],
            &[ExasolType::decimal(20, 0)],
        ),
        None,
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        sql.contains("del-0.parquet"),
        "per-shard files argument must carry the delete file: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("del-0.parquet"),
        "common blob must NOT carry per-shard delete refs: {common}"
    );
    assert!(
        !common.contains("BoundPredicate") && !common.contains("bound_predicate"),
        "common blob must carry no serialized iceberg predicate: {common}"
    );
}

#[test]
fn fan_out_primitive_wraps_distributor_in_ungrouped_scalar_scan() {
    let spec = delete_spec_template();
    let shards = vec![
        vec![FileEntry::new("data/part-0.parquet", 1000)],
        vec![FileEntry::new("data/part-1.parquet", 2000)],
    ];
    let emits = r#""ID" DECIMAL(20,0)"#;
    let sql = build_fan_out_inner(&spec, &shards, emits, "SCAN", "DISTRIBUTE");

    assert!(
        sql.contains("DISTRIBUTE(files) FROM (VALUES"),
        "distributor passthrough is called bare (its LUA EMITS is static): {sql}"
    );
    assert!(
        !sql.contains("DISTRIBUTE(files) EMITS"),
        "the statically-defined distributor call MUST NOT carry a query-side EMITS: {sql}"
    );
    assert!(
        sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the GROUP BY shard_key fan-out must live in the distributor subquery: {sql}"
    );
    assert!(
        sql.contains(&format!(
            "SELECT SCAN('{}",
            spec.to_common_json().replace('\'', "''")
        )),
        "the outer scalar scan splices the common blob as its first-arg literal: {sql}"
    );
    assert!(
        sql.contains(", files) EMITS ("),
        "the outer scalar scan reads the bare distributed files column, not a literal: {sql}"
    );
    assert_eq!(
        sql.matches("s3://warehouse/db/table").count(),
        1,
        "common blob must be spliced exactly once, not per shard: {sql}"
    );
}

#[test]
fn single_shard_short_circuits_distributor_fromless() {
    let spec = delete_spec_template();
    let shards = vec![vec![FileEntry::new("data/part-0.parquet", 1000)]];
    let emits = r#""ID" DECIMAL(20,0)"#;
    let sql = build_fan_out_inner(&spec, &shards, emits, "SCAN", "DISTRIBUTE");

    assert!(
        sql.starts_with("SELECT SCAN("),
        "from-less scalar call: {sql}"
    );
    assert!(
        !sql.contains("DISTRIBUTE"),
        "no distributor for one shard: {sql}"
    );
    assert!(
        !sql.contains("GROUP BY shard_key"),
        "no shard_key grouping for one shard: {sql}"
    );
    assert!(!sql.contains("VALUES"), "no driving VALUES relation: {sql}");
    let files_literal = sql_string_literal(&shard_files_json(&shards[0]));
    assert!(
        sql.contains(&format!(", {files_literal}) EMITS (")),
        "the single shard's files must be spliced as a literal: {sql}"
    );
}

#[test]
fn pushdown_carries_table_root_and_sizes_in_common_and_shards() {
    let root = "s3://warehouse/db/events";
    let files = vec![
        (format!("{root}/part-00000.parquet"), 1024u64),
        (format!("{root}/part-00001.parquet"), 2048u64),
    ];
    let sql = build_row_sql_with_root(
        files,
        root,
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        2,
    );

    let common = common_arg_literal(&sql);
    assert!(
        common.contains(&format!(r#""table_root":"{root}""#)),
        "common blob must carry table_root once: {common}"
    );

    assert!(
        sql.contains(r#"[["part-00000.parquet",1024]]"#),
        "shard payload must carry relative path + size for file 0: {sql}"
    );
    assert!(
        sql.contains(r#"[["part-00001.parquet",2048]]"#),
        "shard payload must carry relative path + size for file 1: {sql}"
    );
}

#[test]
fn table_root_stripped_from_under_root_paths_and_carried_once() {
    let root = "s3://warehouse/db/events";
    let files = vec![
        (format!("{root}/part-00000.parquet"), 1024u64),
        (format!("{root}/part-00001.parquet"), 2048u64),
    ];
    let sql = build_row_sql_with_root(
        files,
        root,
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        2,
    );

    assert_eq!(
        sql.matches(root).count(),
        1,
        "table root must appear exactly once (common blob only), never per shard: {sql}"
    );
    assert!(
        common_arg_literal(&sql).contains(root),
        "the sole table-root occurrence must be in the common blob: {sql}"
    );
    assert!(
        sql.contains("part-00000.parquet") && sql.contains("part-00001.parquet"),
        "shards must carry the relative file names: {sql}"
    );
}

#[test]
fn path_not_under_root_stays_absolute() {
    let root = "s3://warehouse/db/events";
    let outside = "s3://other-bucket/external/f.parquet";
    let files = vec![
        (format!("{root}/part-00000.parquet"), 1024u64),
        (outside.to_string(), 2048u64),
    ];
    let sql = build_row_sql_with_root(
        files,
        root,
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        2,
    );

    assert!(
        sql.contains(r#"["part-00000.parquet",1024]"#),
        "under-root path must be relativized: {sql}"
    );
    assert!(
        sql.contains(&format!(r#"["{outside}",2048]"#)),
        "path outside the table root must stay absolute: {sql}"
    );
    assert_eq!(
        sql.matches(root).count(),
        1,
        "table root must appear exactly once even with an out-of-root file: {sql}"
    );
}

#[test]
fn fan_out_carries_root_once_and_path_size_tuples_per_shard() {
    let root = "s3://warehouse/db/events";
    let files = vec![
        (format!("{root}/part-00000.parquet"), 1024u64),
        (format!("{root}/part-00001.parquet"), 2048u64),
    ];
    let sql = build_row_sql_with_root(
        files,
        root,
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        2,
    );

    assert!(
        !sql.contains("IPROC()"),
        "fan-out must not use IPROC(): {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key") && sql.contains("AS shards(shard_key, files)"),
        "fan-out must GROUP BY shard_key over the VALUES table: {sql}"
    );

    assert_eq!(
        sql.matches(root).count(),
        1,
        "root must be serialized once in the common blob: {sql}"
    );

    assert!(
        sql.contains(r#"[["part-00000.parquet",1024]]"#)
            && sql.contains(r#"[["part-00001.parquet",2048]]"#),
        "each shard literal must be a [[path,size],...] tuple array: {sql}"
    );
}

/// Scenario: Pushdown resolves the file list once and builds a scan-driving query
#[test]
fn pushdown_resolves_files_once_builds_scan_sql() {
    let files = vec![
        "s3://warehouse/db/events/part-00000.parquet".into(),
        "s3://warehouse/db/events/part-00001.parquet".into(),
    ];
    let sql = build_sql_for_fixture(
        files.clone(),
        vec!["ID".into(), "NAME".into()],
        vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR],
        None,
        None,
    );

    assert!(
        sql.contains(SCAN_UDF_NAME),
        "SQL must reference the scan UDF: {sql}"
    );
    assert!(
        sql.contains("part-00000.parquet"),
        "SQL must carry assigned files: {sql}"
    );
    assert!(
        sql.contains("part-00001.parquet"),
        "SQL must carry both files: {sql}"
    );
    assert!(
        sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")) && !sql.contains("SELECT * FROM ("),
        "must be a real scalar scan-driving query, no materializing wrapper: {sql}"
    );
}

/// Scenario: Projection is pushed into the scan-driving query
#[test]
fn projection_carried_in_common_literal_and_emits() {
    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["A".into(), "B".into()],
        vec![ExasolType::decimal(10, 0), ExasolType::WIDEST_VARCHAR],
        None,
        None,
    );

    assert!(
        sql.contains("\"A\" DECIMAL(10,0)"),
        "EMITS must carry col A: {sql}"
    );
    assert!(
        sql.contains("\"B\" VARCHAR(2000000)"),
        "EMITS must carry col B: {sql}"
    );

    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""projection":["A","B"]"#),
        "common arg must carry the projection in order: {common}"
    );
    assert!(
        !sql.contains(r#""files""#),
        "no ScanSpec files key must appear (files travel as a bare JSON array): {sql}"
    );
}

#[test]
fn pushdown_translates_or_omits_predicate() {
    let columns = column_types(&[
        ("AGE", ExasolType::decimal(20, 0)),
        ("PRICE", ExasolType::decimal(9, 2)),
    ]);
    let translatable = compare("predicate_greater", col("AGE"), num_lit(18));
    let filter_rendered = scan_filter_sql(&translatable, &columns);
    assert!(
        filter_rendered.is_some(),
        "translatable predicate must produce a filter string"
    );
    let filter_str = filter_rendered.unwrap();
    assert!(
        filter_str.contains(">"),
        "filter must include > operator: {filter_str}"
    );
    assert!(
        filter_str.contains("AGE") || filter_str.contains("\"AGE\""),
        "filter must reference the column: {filter_str}"
    );

    // Kept out of the scan spec only; the adapter still self-applies it (see
    // `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`).
    let untranslatable = like(col("PRICE"), "1%");
    let omitted = scan_filter_sql(&untranslatable, &columns);
    assert!(
        omitted.is_none(),
        "untranslatable predicate must be omitted (None), not mistranslated"
    );

    let sql_no_filter = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["AGE".into()],
        vec![ExasolType::decimal(20, 0)],
        None,
        None,
    );
    assert!(
        sql_no_filter.contains(SCAN_UDF_NAME),
        "SQL must still be valid when filter is omitted"
    );

    let sql_with_filter = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["AGE".into()],
        vec![ExasolType::decimal(20, 0)],
        Some(filter_str),
        None,
    );
    assert!(
        sql_with_filter.contains(">"),
        "filter must survive into the spec literal: {sql_with_filter}"
    );
}

#[test]
fn row_scan_limit_in_common_arg() {
    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        None,
        Some(42),
    );

    assert!(
        sql.contains("LIMIT 42"),
        "outer SQL must carry LIMIT for correctness backstop: {sql}"
    );

    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""limit":42"#),
        "row-scan common arg must carry limit=42: {common}"
    );
}

#[test]
fn limit_extracted_from_pushdown_request() {
    let req = serde_json::json!({"numElements": 42});
    assert_eq!(extract_limit(&req), None); // not nested under "limit"

    let req2 = serde_json::json!({"limit": {"numElements": 42}});
    assert_eq!(extract_limit(&req2), Some(42));
}

#[test]
fn offset_extracted_from_pushdown_request() {
    assert_eq!(extract_offset(&serde_json::json!({})), 0);
    assert_eq!(
        extract_offset(&serde_json::json!({"limit": {"numElements": 42}})),
        0
    );
    assert_eq!(extract_offset(&serde_json::json!({"offset": 3})), 0);
    assert_eq!(
        extract_offset(&serde_json::json!({"limit": {"numElements": 12, "offset": 3}})),
        3
    );
}

#[test]
fn render_limit_offset_covers_absent_zero_and_nonzero_offset() {
    assert_eq!(render_limit_offset(None, 0), "");
    assert_eq!(render_limit_offset(None, 3), "");

    for n in [0_u64, 1, 12, u64::MAX] {
        assert_eq!(render_limit_offset(Some(n), 0), format!(" LIMIT {n}"));
    }

    assert_eq!(render_limit_offset(Some(12), 3), " LIMIT 12 OFFSET 3");
    assert_eq!(render_limit_offset(Some(0), 3), " LIMIT 0 OFFSET 3");
}

#[test]
fn project_columns_fallback_is_duplicate_free() {
    let (names, types, _widened) = projected(
        pushdown_request(
            "EVENTS",
            &[
                ("id", ExasolType::decimal(20, 0)),
                ("name", ExasolType::WIDEST_VARCHAR),
            ],
        )
        .item(like(col("id"), "1%"), ExasolType::Boolean)
        .item(count_star(), ExasolType::decimal(20, 0)),
    );

    let unique: std::collections::HashSet<_> = names.iter().map(|p| p.emit_name()).collect();
    assert_eq!(
        unique.len(),
        names.len(),
        "projection must be duplicate-free, got: {names:?}"
    );
    assert_eq!(
        names,
        vec!["ID", "NAME"],
        "fallback must project the full base-table column set"
    );
    assert_eq!(
        names.len(),
        types.len(),
        "names and types must stay aligned"
    );
}

#[test]
fn aggregate_query_builds_partial_agg_spec() {
    let agg_plans = vec![column_plan(AggKind::Sum, "AMOUNT"), count_plan()];
    let merge_inputs = AggregateMergeInputs::new(
        Vec::new(),
        cast_merge_items(
            &agg_plans,
            &[ExasolType::DoublePrecision, ExasolType::decimal(18, 0)],
            &column_types(NO_COLUMNS),
        ),
        None,
    )
    .expect("a two-aggregate merge SELECT is never empty");
    let spec_template = CommonScanSpec {
        projection: vec!["AMOUNT".into()],
        filter: Some("(\"REGION\" = 'EU')".into()),
        aggregates: Some(agg_plans),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);

    let shards = vec![vec![("s3://warehouse/f.parquet".to_string(), 1u64)]];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&["AMOUNT".into()], &[ExasolType::DoublePrecision]),
        None,
        Some(&merge_inputs),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );

    let spec_json = {
        let mut s = spec_template.clone();
        s.files = vec![FileEntry::new("s3://warehouse/f.parquet", 1)];
        s.to_json()
    };
    let parsed = ScanSpec::from_json(&spec_json).expect("spec must parse");

    let plans = parsed
        .common
        .aggregates
        .expect("aggregates must be in the spec");
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
    assert_eq!(plans[1].kind, AggKind::Count);
    assert!(plans[1].column.is_none());

    assert!(
        parsed.common.filter.is_some(),
        "filter must be carried in aggregate spec"
    );

    assert!(sql.contains(SCAN_UDF_NAME));
}

#[test]
fn fan_out_serializes_common_once_files_per_shard() {
    let files = vec![
        "s3://warehouse/shard0/part-000.parquet".into(),
        "s3://warehouse/shard1/part-001.parquet".into(),
        "s3://warehouse/shard2/part-002.parquet".into(),
    ];
    let sql = build_sql_for_fixture_n(
        files,
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        None,
        None,
        3,
    );

    assert!(
        !sql.contains("IPROC()"),
        "multi-shard SQL must NOT contain IPROC(): {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key"),
        "multi-shard SQL must GROUP BY shard_key: {sql}"
    );

    assert!(
        sql.contains("AS shards(shard_key, files)"),
        "fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
    );
    assert!(
        sql.contains(&format!("{SCAN_UDF_NAME}(")),
        "multi-shard SQL must invoke the scan UDF: {sql}"
    );
    assert!(
        sql.contains(", files) EMITS ("),
        "UDF must take the per-shard files column as its second argument: {sql}"
    );

    // The endpoint and tuning knobs live only in the common blob, so counting them
    // proves the payload is not repeated per shard.
    assert_eq!(
        sql.matches("http://minio:9000").count(),
        1,
        "storage endpoint (common blob) must appear exactly once, not per shard: {sql}"
    );
    assert_eq!(
        sql.matches("memory_pool_fraction").count(),
        1,
        "tuning payload (common blob) must appear exactly once, not per shard: {sql}"
    );

    for file in ["part-000.parquet", "part-001.parquet", "part-002.parquet"] {
        assert_eq!(
            sql.matches(file).count(),
            1,
            "file {file} must appear exactly once (in one VALUES row): {sql}"
        );
    }

    let values_start = sql.find("VALUES").expect("must have VALUES");
    let group_by_start = sql.find("GROUP BY").expect("must have GROUP BY");
    let values_section = &sql[values_start..group_by_start];
    let entry_count = values_section.matches("),(").count() + 1;
    assert_eq!(
        entry_count, 3,
        "must have 3 VALUES entries for 3 shards: {values_section}"
    );
}

#[test]
fn common_spec_carries_s3_max_connections_exactly_once() {
    let files = vec![
        "s3://warehouse/shard0/part-000.parquet".into(),
        "s3://warehouse/shard1/part-001.parquet".into(),
        "s3://warehouse/shard2/part-002.parquet".into(),
    ];
    // Distinct from the default (8) and every other numeric field in the spec.
    let distinctive_s3_max_connections = 37;
    let spec_template = CommonScanSpec {
        projection: vec!["ID".into()],
        storage: ScanStorage::Inline(sample_storage()),
        s3_max_connections: distinctive_s3_max_connections,
        ..Default::default()
    }
    .with_files(vec![]);

    let common = spec_template.to_common();
    assert_eq!(
        common.s3_max_connections, distinctive_s3_max_connections,
        "s3_max_connections must carry from ScanSpec into CommonScanSpec"
    );

    let files_with_sizes: Vec<FileEntry> = files
        .into_iter()
        .map(|p: String| FileEntry::new(p, 1))
        .collect();
    let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, 3);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&["ID".into()], &[ExasolType::decimal(20, 0)]),
        None,
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );

    let needle = format!("\"s3_max_connections\":{distinctive_s3_max_connections}");
    assert_eq!(
        sql.matches(&needle).count(),
        1,
        "s3_max_connections must appear exactly once, in the shard-invariant \
         common blob, not per shard and not dropped: {sql}"
    );
}

/// An empty col_types map defaults aggregate columns to DOUBLE PRECISION.
fn build_agg_sql(
    agg_plans: Vec<AggregatePlan>,
    files: Vec<String>,
    cluster_nodes: usize,
) -> String {
    let merge_select = cast_merge_items(
        &agg_plans,
        &vec![ExasolType::DoublePrecision; agg_plans.len()],
        &column_types(NO_COLUMNS),
    );
    let spec_template = CommonScanSpec {
        aggregates: Some(agg_plans),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards =
        crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
    let merge_inputs = AggregateMergeInputs::new(Vec::new(), merge_select, None)
        .expect("a bare-aggregate merge SELECT is never empty");
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&[], &[]),
        None,
        Some(&merge_inputs),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

#[test]
fn aggregate_merge_renders_having_then_request_limit() {
    let plans = vec![count_plan()];
    let merge_select = cast_merge_items(
        &plans,
        &[ExasolType::decimal(18, 0)],
        &column_types(NO_COLUMNS),
    );
    let spec_template = CommonScanSpec {
        aggregates: Some(plans),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let merge_inputs = AggregateMergeInputs::new(Vec::new(), merge_select, Some(0))
        .expect("a bare-aggregate merge SELECT is never empty")
        .with_having(Some("(1 > 0)".to_string()));
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&[], &[]),
        None,
        Some(&merge_inputs),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(sql.ends_with(") HAVING (1 > 0) LIMIT 0"), "{sql}");
}

#[test]
fn aggregate_wrapper_merges_partials() {
    let plans = vec![
        count_plan(),
        column_plan(AggKind::Sum, "AMOUNT"),
        column_plan(AggKind::Min, "TS"),
        column_plan(AggKind::Max, "TS"),
    ];

    let files = vec![
        "s3://warehouse/f0.parquet".into(),
        "s3://warehouse/f1.parquet".into(),
    ];
    let sql = build_agg_sql(plans, files, 2);

    assert!(
        !sql.contains("IPROC()"),
        "aggregate SQL must NOT use IPROC: {sql}"
    );
    assert!(
        sql.contains("GROUP BY"),
        "aggregate SQL must use GROUP BY: {sql}"
    );
    assert!(
        sql.contains("shard_key"),
        "aggregate SQL must use shard_key fan-out: {sql}"
    );

    assert!(
        sql.contains("SUM("),
        "merge wrapper must contain SUM: {sql}"
    );
    assert!(
        sql.contains("MIN("),
        "merge wrapper must contain MIN: {sql}"
    );
    assert!(
        sql.contains("MAX("),
        "merge wrapper must contain MAX: {sql}"
    );

    assert!(
        sql.contains("PARTIAL_count_0"),
        "must reference partial count column: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_sum_1"),
        "must reference partial sum column: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_min_2"),
        "must reference partial min column: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_max_3"),
        "must reference partial max column: {sql}"
    );

    assert!(
        sql.contains("EMITS"),
        "aggregate SQL must have EMITS: {sql}"
    );

    assert!(
        !sql.contains("SELECT *"),
        "aggregate wrapper must not use SELECT *: {sql}"
    );
}

#[test]
fn aggregate_merge_over_scalar_scan_no_wrapper() {
    let plans = vec![count_plan(), column_plan(AggKind::Sum, "AMOUNT")];
    let sql = build_agg_sql(
        plans,
        vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
        2,
    );

    assert!(
        !sql.contains("SELECT * FROM ("),
        "no materializing wrapper between merge and scan: {sql}"
    );
    assert!(
        sql.starts_with("SELECT ") && sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
        "the outer merge SELECT must read directly from the scalar scan subquery: {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key"),
        "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
    );
}

#[test]
fn aggregate_single_shard_merge_over_fromless_scalar_scan() {
    let plans = vec![count_plan()];
    let sql = build_agg_sql(plans, vec!["s3://w/only.parquet".into()], 1);

    assert!(
        !sql.contains("SELECT * FROM ("),
        "single-shard aggregate must not use a materializing wrapper: {sql}"
    );
    assert!(
        !sql.contains("VALUES") && !sql.contains("GROUP BY shard_key"),
        "single-shard aggregate short-circuits the distributor: {sql}"
    );
    assert!(
        sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
        "the merge reads directly from the from-less scalar scan: {sql}"
    );
}

#[test]
fn aggregate_merge_splices_caller_select_items_and_types_emits_per_plan() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: None,
        arg_expr: Some(r#"("A" * "B")"#.to_string()),
    }];
    let spec_template = CommonScanSpec {
        aggregates: Some(plans),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let merge_select = vec![r#"CAST(ROUND(SUM("PARTIAL_sum_0"), 2) AS DECIMAL(30,4))"#.to_string()];
    let merge_inputs = AggregateMergeInputs::new(
        vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()],
        merge_select,
        None,
    )
    .expect("the fixture's merge SELECT is not empty");
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&[], &[]),
        None,
        Some(&merge_inputs),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        sql.starts_with(r#"SELECT CAST(ROUND(SUM("PARTIAL_sum_0"), 2) AS DECIMAL(30,4)) FROM ("#),
        "the caller's merge item must be spliced verbatim: {sql}"
    );
    assert!(
        sql.contains(r#"EMITS ("PARTIAL_sum_0" DECIMAL(36,4))"#),
        "the declared plan type must still widen the SUM partial's EMITS type: {sql}"
    );
}

#[test]
fn avg_wrapper_divides_sum_by_count_guarded() {
    let plans = vec![column_plan(AggKind::Avg, "SCORE")];
    let files = vec![
        "s3://warehouse/f0.parquet".into(),
        "s3://warehouse/f1.parquet".into(),
    ];
    let sql = build_agg_sql(plans, files, 2);

    assert!(
        sql.contains("NULLIF"),
        "AVG wrapper must contain NULLIF zero-guard: {sql}"
    );

    assert!(
        sql.contains(" / "),
        "AVG wrapper must divide sum by count: {sql}"
    );

    assert!(
        sql.contains("PARTIAL_avg_sum_0"),
        "must reference partial avg sum: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_avg_cnt_0"),
        "must reference partial avg count: {sql}"
    );

    let sum_count = sql.matches("SUM(").count();
    assert!(
        sum_count >= 2,
        "AVG wrapper must SUM both partial_avg_sum and partial_avg_cnt: {sql}"
    );

    assert!(
        sql.contains("NULLIF(") && sql.contains(", 0)"),
        "AVG wrapper NULLIF guard must guard against zero: {sql}"
    );
}

#[test]
fn single_shard_aggregate_still_uses_merge_wrapper() {
    let plans = vec![count_plan(), column_plan(AggKind::Avg, "SCORE")];
    let files = vec!["s3://warehouse/f0.parquet".into()];
    let sql = build_agg_sql(plans, files, 1);

    assert!(
        sql.contains("SUM("),
        "single-shard aggregate must have SUM merge: {sql}"
    );
    assert!(
        sql.contains("NULLIF"),
        "single-shard AVG must have NULLIF guard: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_count_0"),
        "single-shard must reference partial count: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_avg_sum_1"),
        "single-shard must reference partial avg sum: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_avg_cnt_1"),
        "single-shard must reference partial avg count: {sql}"
    );
}

/// Matches the real `handle_pushdown` caller contract: only `files` varies per shard.
fn count_distinct_base_spec() -> ScanSpec {
    CommonScanSpec {
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![])
}

#[test]
fn count_distinct_wrapper_uses_native_count_distinct() {
    let base_spec = count_distinct_base_spec();
    let items = vec![SingleGroupItem::Distinct(DistinctCount {
        column: Some("L_SHIPMODE".into()),
        arg_expr: None,
    })];
    let col_types = vec![("L_SHIPMODE".to_string(), ExasolType::varchar(25))];
    let shards = vec![
        vec![("s3://warehouse/a.parquet".to_string(), 1u64)],
        vec![("s3://warehouse/b.parquet".to_string(), 1u64)],
    ];
    let sql = build_count_distinct_scan_sql(
        &base_spec,
        &shards,
        &items,
        &column_types(&col_types),
        None,
        r#""VS_SCHEMA".LAKEHOUSE_SCAN"#,
        r#""VS_SCHEMA".LAKEHOUSE_DISTRIBUTE_FILES"#,
    );

    assert!(
        sql.starts_with(r#"SELECT COUNT(DISTINCT "V") FROM ("#),
        "Case 1 must be a plain native COUNT(DISTINCT) over one fan-out: {sql}"
    );
    assert!(
        sql.contains(r#""V" VARCHAR(25)"#),
        "the fan-out's single emitted column must be named V, with its native \
         (non-JSON) Exasol type: {sql}"
    );
    assert!(
        sql.contains(r#"\"L_SHIPMODE\" IS NOT NULL"#),
        "the fan-out must exclude NULLs from the distinct argument: {sql}"
    );
    assert!(
        sql.contains(r#""VS_SCHEMA".LAKEHOUSE_SCAN"#)
            && sql.contains(r#""VS_SCHEMA".LAKEHOUSE_DISTRIBUTE_FILES"#),
        "both the scan and distributor UDFs must be schema-qualified from the \
         names passed in: {sql}"
    );
    assert!(
        !sql.to_uppercase().contains("LISTAGG") && !sql.contains("DISTINCT_MERGE"),
        "the removed per-shard JSON-array LISTAGG merge-UDF shape must never \
         appear: {sql}"
    );
}

#[test]
fn multi_count_distinct_declines_to_qualified_wrapper() {
    use super::super::joins::{
        FanOutProjection, build_qualified_single_table_fallback_sql, referenced_columns,
    };
    use super::super::single_group_agg::{has_distinct, is_lone_count_distinct};

    let cdist = |col: &str| agg_item_expr("COUNT", table_col("T", col), true);
    let all_cols = vec![
        ("CATEGORY".to_string(), ExasolType::varchar(25)),
        ("REGION".to_string(), ExasolType::varchar(25)),
        ("IRRELEVANT_COL".to_string(), ExasolType::decimal(20, 0)),
    ];
    let (request, _) = pushdown_request("T", &all_cols)
        .item(cdist("CATEGORY"), ExasolType::decimal(18, 0))
        .item(cdist("REGION"), ExasolType::decimal(18, 0))
        .build();
    let pushdown = parsed_pushdown(&request);

    let items = super::super::detect_aggregates(&pushdown, scan_of(&request))
        .expect("two COUNT(DISTINCT) items are detected as distinct fan-out descriptors");
    assert!(
        has_distinct(&items),
        "a Case 2 select list still carries distinct items"
    );
    assert!(
        !is_lone_count_distinct(&items),
        "more than one COUNT(DISTINCT) is NOT a lone distinct — it must decline the \
         fan-out and route to the qualified single-table wrapper"
    );

    // Mirrors the `mod.rs` Case 2/3 guard.

    let referenced = referenced_columns(pushdown.json(), &column_types(&all_cols));

    let fan_out = FanOutProjection::new(count_distinct_base_spec().common, &referenced);
    let sql = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown,
        &fan_out,
        &[vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
    .expect("Case 2/3 qualified wrapper must build");

    assert!(
        sql.contains(r#"AS "LHS_T0""#) && sql.contains("FROM ("),
        "Case 2/3 must be the qualified single-table wrapper (one aliased raw \
         fan-out subquery): {sql}"
    );
    assert_eq!(
        sql.matches("COUNT(DISTINCT").count(),
        2,
        "both COUNT(DISTINCT) aggregates must be spliced verbatim into the outer \
         wrapper — one per select item: {sql}"
    );
    assert!(
        !sql.contains(r#"COUNT(DISTINCT "V")"#),
        "Case 2/3 must NOT be a distinct row-scan fan-out (the Case 1 shape): {sql}"
    );
    assert!(
        !sql.contains("(SELECT COUNT(DISTINCT"),
        "Case 2/3 must NOT compose per-distinct SELECT-list scalar subqueries (the \
         blocked design, sqlCode 04000 'emitting function in expression'): {sql}"
    );
    assert!(
        !sql.starts_with("SELECT * FROM"),
        "Case 2/3 must NOT be a bare row scan (the 04000 column-count mismatch): {sql}"
    );
    assert!(
        !sql.to_uppercase().contains("LISTAGG") && !sql.contains("DISTINCT_MERGE"),
        "the removed per-shard JSON-array LISTAGG merge-UDF shape must never \
         appear: {sql}"
    );
    assert!(
        !sql.contains("IRRELEVANT_COL"),
        "issue #160: the narrowed inner scan must project only referenced columns \
         (CATEGORY, REGION), never the full base-table schema: {sql}"
    );
}

#[test]
fn count_distinct_fan_out_omits_limit_offset_order_by() {
    // Real callers pass `limit: None, order_by: []`; the builder must strip them regardless.
    let mut poisoned_base_spec = count_distinct_base_spec();
    poisoned_base_spec.common.limit = Some(999);
    poisoned_base_spec.common.order_by = vec![SortKey {
        column: "POISON_KEY".into(),
        ascending: true,
        nulls_last: false,
    }];
    let col_types = vec![
        ("A".to_string(), ExasolType::decimal(20, 0)),
        ("B".to_string(), ExasolType::decimal(20, 0)),
    ];
    let shards = vec![
        vec![("s3://warehouse/a.parquet".to_string(), 1u64)],
        vec![("s3://warehouse/b.parquet".to_string(), 1u64)],
    ];

    let assert_only_outer_limit_no_order_by = |sql: &str, case: &str| {
        assert!(
            !sql.contains("POISON_KEY") && !sql.contains("999"),
            "{case}: a poisoned base_spec's LIMIT/ORDER BY must never leak into \
             any distinct fan-out: {sql}"
        );
        assert_eq!(
            sql.matches("LIMIT").count(),
            1,
            "{case}: exactly one literal LIMIT (the outer wrapper's) may \
             appear — none may leak into a per-shard fan-out subquery: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("LIMIT 1"),
            "{case}: the request-level LIMIT must land on the outermost \
             wrapper, after every fan-out subquery closes: {sql}"
        );
        assert!(
            !sql.contains("ORDER BY"),
            "{case}: no ORDER BY may appear — the fan-out never sorts: {sql}"
        );
    };

    let case1_items = vec![SingleGroupItem::Distinct(DistinctCount {
        column: Some("A".into()),
        arg_expr: None,
    })];
    let sql1 = build_count_distinct_scan_sql(
        &poisoned_base_spec,
        &shards,
        &case1_items,
        &column_types(&col_types),
        Some(1),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert_only_outer_limit_no_order_by(&sql1, "Case 1");
}

#[test]
fn multi_shard_row_scan_appends_outer_limit() {
    let files = vec![
        "s3://warehouse/f0.parquet".into(),
        "s3://warehouse/f1.parquet".into(),
    ];
    let sql = build_sql_for_fixture_n(
        files,
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        None,
        Some(10),
        2,
    );
    assert!(
        !sql.contains("IPROC()"),
        "must NOT use IPROC (uses shard_key): {sql}"
    );
    assert!(
        sql.contains("shard_key"),
        "must be multi-shard (uses shard_key): {sql}"
    );
    assert!(
        sql.contains("LIMIT 10"),
        "multi-shard row scan must append outer LIMIT 10: {sql}"
    );
}

#[test]
fn pushdown_builds_scalar_scan_driving_sql() {
    let sql = build_sql_for_fixture_n(
        vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        None,
        None,
        2,
    );
    assert!(
        !sql.contains("SELECT * FROM ("),
        "the materializing SELECT * wrapper must be gone: {sql}"
    );
    assert!(
        sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")),
        "the outer query is the ungrouped scalar scan itself: {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key"),
        "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
    );
    assert!(
        sql.contains(&format!("{DISTRIBUTE_FILES_UDF_NAME}(files)")),
        "the distributor subquery must carry only the files column: {sql}"
    );
}

#[test]
fn limit_attaches_directly_to_outer_scalar_select() {
    let sql = build_sql_for_fixture_n(
        vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
        vec!["ID".into()],
        vec![ExasolType::decimal(20, 0)],
        None,
        Some(7),
        2,
    );
    assert!(
        !sql.contains("SELECT * FROM ("),
        "no materializing wrapper between LIMIT and the scan: {sql}"
    );
    assert!(
        sql.trim_end().ends_with("LIMIT 7"),
        "LIMIT appends to the outer scalar select: {sql}"
    );
    let limit_pos = sql.rfind("LIMIT 7").expect("LIMIT present");
    let close_pos = sql[..limit_pos]
        .rfind(')')
        .expect("distributor subquery closes");
    assert!(
        close_pos < limit_pos,
        "LIMIT must follow the distributor subquery's closing paren: {sql}"
    );
}

#[test]
fn single_shard_two_arg_common_and_files_once() {
    let files = vec![
        "s3://warehouse/db/events/part-00000.parquet".into(),
        "s3://warehouse/db/events/part-00001.parquet".into(),
    ];
    let sql = build_sql_for_fixture_n(
        files.clone(),
        vec!["ID".into(), "NAME".into()],
        vec![ExasolType::decimal(20, 0), ExasolType::WIDEST_VARCHAR],
        None,
        None,
        1,
    );

    assert!(
        !sql.contains("IPROC"),
        "single-shard SQL must not contain IPROC: {sql}"
    );
    assert!(
        !sql.contains("VALUES"),
        "single-shard SQL must not contain VALUES: {sql}"
    );
    assert!(
        !sql.contains("GROUP BY"),
        "single-shard SQL must not contain GROUP BY: {sql}"
    );

    assert!(
        sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")) && !sql.contains("SELECT * FROM ("),
        "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
    );
    assert!(sql.contains("EMITS"), "must have EMITS clause: {sql}");
    assert!(
        sql.contains(SCAN_UDF_NAME),
        "must invoke the scan UDF: {sql}"
    );

    assert_eq!(
        sql.matches("http://minio:9000").count(),
        1,
        "common blob (storage endpoint) must appear exactly once: {sql}"
    );
    assert_eq!(
        sql.matches("memory_pool_fraction").count(),
        1,
        "common blob (tuning payload) must appear exactly once: {sql}"
    );

    assert_eq!(
        sql.matches("part-00000.parquet").count(),
        1,
        "must carry file 0 exactly once: {sql}"
    );
    assert_eq!(
        sql.matches("part-00001.parquet").count(),
        1,
        "must carry file 1 exactly once: {sql}"
    );
}

#[test]
fn partition_files_g_shards_balanced_disjoint_full_coverage() {
    use std::collections::HashSet;
    // 3 nodes × 4 factor = 12, capped to 10 files → G = 10
    let file_names: Vec<String> = (0..10).map(|i| format!("file-{i}.parquet")).collect();
    let files: Vec<(String, u64)> = file_names
        .iter()
        .enumerate()
        .map(|(i, p)| (p.clone(), (i as u64 + 1) * 100))
        .collect();
    let g = shard_count(3, 4, files.len());
    assert_eq!(g, 10, "G must equal file_count when product > file_count");
    let shards = crate::adapter::sharding::partition_files_by_bytes(files.clone(), g);
    assert_eq!(shards.len(), 10, "must produce exactly G=10 shards");
    for (i, shard) in shards.iter().enumerate() {
        assert!(!shard.is_empty(), "shard {i} must not be empty");
    }
    let all: Vec<String> = shards.iter().flatten().map(|(p, _)| p.clone()).collect();
    let unique: HashSet<&String> = all.iter().collect();
    assert_eq!(
        unique.len(),
        all.len(),
        "files must be disjoint across shards"
    );
    assert_eq!(
        unique,
        file_names.iter().collect::<HashSet<_>>(),
        "all files must be covered"
    );
}

#[test]
fn scan_driving_sql_groups_by_shard_key_not_iproc() {
    let files: Vec<(String, u64)> = (0..3)
        .map(|i| (format!("s3://warehouse/f{i}.parquet"), (i as u64 + 1) * 100))
        .collect();
    let g = shard_count(3, 1, files.len());
    let spec_template = CommonScanSpec {
        projection: vec!["ID".into()],
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&["ID".into()], &[ExasolType::decimal(20, 0)]),
        None,
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        !sql.contains("IPROC()"),
        "multi-shard SQL must NOT contain IPROC(): {sql}"
    );
    assert!(
        sql.contains("GROUP BY"),
        "multi-shard SQL must contain GROUP BY: {sql}"
    );
    assert!(
        sql.contains("shard_key"),
        "multi-shard SQL must use shard_key: {sql}"
    );
}

#[test]
fn single_shard_collapses_to_single_invocation() {
    let files = vec![("s3://warehouse/f0.parquet".to_string(), 500u64)];
    let g = shard_count(1, 1, files.len());
    let spec_template = CommonScanSpec {
        projection: vec!["ID".into()],
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &projected_columns(&["ID".into()], &[ExasolType::decimal(20, 0)]),
        None,
        None,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        !sql.contains("IPROC()"),
        "single-shard SQL must not contain IPROC: {sql}"
    );
    assert!(
        !sql.contains("VALUES"),
        "single-shard SQL must not contain VALUES: {sql}"
    );
    assert!(
        !sql.contains("GROUP BY"),
        "single-shard SQL must not contain GROUP BY: {sql}"
    );
    assert!(
        sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")) && !sql.contains("SELECT * FROM ("),
        "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
    );
}

/// A request over `T(ID DECIMAL(10,0), NAME VARCHAR(100))` with no select list yet.
fn id_name_request() -> PushdownRequestBuilder {
    pushdown_request(
        "T",
        &[
            ("ID", ExasolType::decimal(10, 0)),
            ("NAME", ExasolType::varchar(100)),
        ],
    )
}

/// [`project_first_table`] over the built `request`.
fn projected(request: PushdownRequestBuilder) -> (Vec<ProjectionItem>, Vec<ExasolType>, bool) {
    let (request, _) = request.build();
    project_first_table(&request, &pd(&request)).unwrap()
}

#[test]
fn selectlist_scalar_expression_rendered_in_emits() {
    let upper_expr = scalar_fn("UPPER", [col("NAME")]);
    let (proj_cols, proj_types, _widened) =
        projected(id_name_request().item(upper_expr, ExasolType::WIDEST_VARCHAR));
    // An `Expr`, not a `Column`, so the scan splices it instead of quoting a phantom identifier.
    assert_eq!(proj_cols.len(), 1);
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a rendered scalar expression must be an Expr projection item: {proj_cols:?}"
    );
    let rendered = proj_cols[0].emit_name();
    assert!(
        rendered.contains("UPPER") || rendered.contains("upper"),
        "projection must contain rendered expression: {proj_cols:?}"
    );
    assert_eq!(proj_types[0], ExasolType::WIDEST_VARCHAR);
}

#[test]
fn selectlist_cast_node_rendered_in_emits() {
    let cast_expr = cast_to(ExasolType::varchar(100), col("ID"));
    let (proj_cols, _proj_types, _widened) =
        projected(id_name_request().item(cast_expr, ExasolType::WIDEST_VARCHAR));
    assert_eq!(
        proj_cols.len(),
        1,
        "a function_scalar_cast select-list item must not fall back to the full \
         base row: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a rendered CAST expression must be an Expr projection item: {proj_cols:?}"
    );
    let rendered = proj_cols[0].emit_name();
    assert!(
        rendered.contains(r#"CAST(CAST("ID" AS DECIMAL(38,0)) AS VARCHAR)"#),
        "projection must contain the rendered CAST expression: {proj_cols:?}"
    );
}

#[test]
fn selectlist_extract_node_rendered_in_emits() {
    let extract_expr = extract("YEAR", col("EVENT_DATE"));
    let (proj_cols, _proj_types, _widened) = projected(
        pushdown_request(
            "T",
            &[
                ("ID", ExasolType::decimal(10, 0)),
                ("EVENT_DATE", ExasolType::Date),
            ],
        )
        .item(extract_expr, ExasolType::WIDEST_VARCHAR),
    );
    assert_eq!(
        proj_cols.len(),
        1,
        "a function_scalar_extract select-list item must not fall back to the full \
         base row: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a rendered EXTRACT expression must be an Expr projection item: {proj_cols:?}"
    );
    let rendered = proj_cols[0].emit_name();
    assert!(
        rendered.contains("date_part"),
        "projection must contain the rendered EXTRACT expression: {proj_cols:?}"
    );
}

#[test]
fn selectlist_case_node_rendered_in_emits() {
    let case_expr = case_when(
        [compare("predicate_less", col("SCORE"), num_lit("50"))],
        [str_lit("low"), str_lit("high")],
    );
    let (proj_cols, _proj_types, _widened) = projected(
        pushdown_request(
            "T",
            &[
                ("ID", ExasolType::decimal(10, 0)),
                ("SCORE", ExasolType::decimal(10, 0)),
            ],
        )
        .item(case_expr, ExasolType::WIDEST_VARCHAR),
    );
    assert_eq!(
        proj_cols.len(),
        1,
        "a function_scalar_case select-list item must not fall back to the full \
         base row: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a rendered CASE expression must be an Expr projection item: {proj_cols:?}"
    );
    let rendered = proj_cols[0].emit_name();
    assert!(
        rendered.contains("CASE"),
        "projection must contain the rendered CASE expression: {proj_cols:?}"
    );
}

#[test]
fn selectlist_untranslatable_cast_falls_back_to_full_row() {
    let cast_expr = cast_to(
        ExasolType::TimestampWithLocalTimeZone { precision: 3 },
        col("ID"),
    );
    let (proj_cols, proj_types, _widened) =
        projected(id_name_request().item(cast_expr, ExasolType::WIDEST_VARCHAR));
    assert_eq!(
        proj_cols,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Column("NAME".into()),
        ],
        "an untranslatable CAST target must fall back to the full base row: {proj_cols:?}"
    );
    assert_eq!(
        proj_types,
        vec![ExasolType::decimal(10, 0), ExasolType::varchar(100)]
    );
}

#[test]
fn selectlist_literal_rendered_as_positional_expr() {
    let literal = num_lit(1);
    let (proj_cols, proj_types, _widened) = projected(
        pushdown_request("T", &[("ID", ExasolType::decimal(10, 0))])
            .item(literal, ExasolType::decimal(18, 0)),
    );
    assert_eq!(
        proj_cols.len(),
        1,
        "a single literal must not fall back to the full base row: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a rendered literal must be an Expr projection item: {proj_cols:?}"
    );
    assert_eq!(proj_cols[0].emit_name(), "1");
    assert_eq!(proj_types[0], ExasolType::decimal(18, 0));
}

#[test]
fn selectlist_duplicate_literals_keep_distinct_positions() {
    let literal = num_lit(1);
    let column = col("NAME");
    let (proj_cols, _proj_types, _widened) = projected(
        id_name_request()
            .item(literal.clone(), ExasolType::decimal(18, 0))
            .column(column)
            .item(literal, ExasolType::decimal(18, 0)),
    );

    assert_eq!(
        proj_cols.len(),
        3,
        "the two identical literals must NOT be collapsed: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "position 0 must be a rendered Expr: {proj_cols:?}"
    );
    assert_eq!(proj_cols[1], ProjectionItem::Column("NAME".into()));
    assert!(
        matches!(proj_cols[2], ProjectionItem::Expr { .. }),
        "position 2 must be a rendered Expr: {proj_cols:?}"
    );

    let ident_0 = emits_ident(&proj_cols[0], 0);
    let ident_1 = emits_ident(&proj_cols[1], 1);
    let ident_2 = emits_ident(&proj_cols[2], 2);
    assert_eq!(ident_0, quote_ident("_LH_PROJ_0"));
    assert_eq!(ident_1, quote_ident("NAME"));
    assert_eq!(ident_2, quote_ident("_LH_PROJ_2"));
    assert_ne!(
        ident_0, ident_2,
        "the two literal positions must not collide"
    );
    assert_ne!(ident_0, ident_1);
    assert_ne!(ident_1, ident_2);
}

#[test]
fn selectlist_bare_literal_does_not_fall_back_to_full_row() {
    let literal = str_lit("x");
    let (proj_cols, _proj_types, _widened) =
        projected(id_name_request().item(literal, ExasolType::varchar(100)));
    assert_eq!(
        proj_cols.len(),
        1,
        "a bare literal must project exactly one column, not the full base row: {proj_cols:?}"
    );
}

#[test]
fn selectlist_plain_timestamp_literal_rendered_as_expr() {
    let literal = timestamp_lit("2024-03-01 10:00:00");
    let (proj_cols, proj_types, _widened) = projected(
        pushdown_request("T", &[("ID", ExasolType::decimal(10, 0))])
            .item(literal, ExasolType::BARE_TIMESTAMP),
    );
    assert_eq!(
        proj_cols.len(),
        1,
        "a plain TIMESTAMP literal must not fall back to the full base row: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a plain TIMESTAMP literal must be rendered as an Expr: {proj_cols:?}"
    );
    assert_eq!(proj_types[0], ExasolType::BARE_TIMESTAMP);
}

#[test]
fn project_columns_widens_for_a_cast_to_char_item() {
    let cast_item = cast_to(ExasolType::char(20), col("c_varchar"));
    let (_, _, widened) = projected(
        pushdown_request(
            "T",
            &[
                ("id", ExasolType::decimal(20, 0)),
                ("c_varchar", ExasolType::WIDEST_VARCHAR),
            ],
        )
        .column(col("id"))
        .item(cast_item, ExasolType::char(20)),
    );

    assert!(
        widened,
        "the scan does not blank-pad, so a CAST to CHAR runs in Exasol"
    );
}

#[test]
fn selectlist_untranslatable_item_falls_back_to_column() {
    let bad_expr = agg_item("SUM", Some("AMOUNT"), false);
    let (proj_cols, proj_types, _widened) = projected(
        pushdown_request("T", &[("AMOUNT", ExasolType::decimal(18, 2))])
            .item(bad_expr, ExasolType::WIDEST_VARCHAR),
    );
    assert_eq!(proj_cols.len(), 1);
    assert_eq!(proj_cols[0], "AMOUNT");
    assert_eq!(proj_types[0], ExasolType::decimal(18, 2));
}

/// Scenario: Filter predicate is pushed into the scan spec
#[test]
fn filter_in_common_arg() {
    use crate::adapter::iceberg_predicate::to_iceberg_predicate;
    use iceberg::spec::{NestedField, Schema, Type};
    use std::sync::Arc;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(iceberg::spec::PrimitiveType::Int),
        ))])
        .build()
        .unwrap();

    let filter_json = predicate_equal(col("id"), num_lit(42));

    let df_filter = scan_filter_sql(
        &filter_json,
        &column_types(&[("ID", ExasolType::decimal(10, 0))]),
    );
    assert!(
        df_filter.is_some(),
        "translatable filter must produce a DataFusion SQL string"
    );

    let iceberg_pred =
        to_iceberg_predicate(&filter_json, &declared_logical_schema(&schema), &schema);
    assert!(
        iceberg_pred.is_some(),
        "translatable filter must produce an Iceberg predicate"
    );

    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["ID".into()],
        vec![ExasolType::decimal(10, 0)],
        df_filter,
        None,
    );
    let common = common_arg_literal(&sql);
    assert!(
        common.contains("\"filter\"") && common.contains("42"),
        "filter must be pushed into the common arg: {common}"
    );
}

#[test]
fn nested_like_decline_routes_to_wrapper_where() {
    let filter = and([
        string_equal("T", "STATUS", "OPEN"),
        like(table_col("T", "AMOUNT"), "9%"),
    ]);
    let request = pushdown_request(
        "T",
        &[
            ("STATUS", ExasolType::WIDEST_VARCHAR),
            ("AMOUNT", ExasolType::decimal(9, 2)),
        ],
    )
    .filter(filter)
    .build()
    .0;

    let sql = declined_filter_wrapper_sql(&request);

    let where_at = sql
        .find(r#"AS "LHS_T0" WHERE "#)
        .unwrap_or_else(|| panic!("the declined filter must reach the wrapper: {sql}"));
    assert!(
        sql[where_at..].contains(r#""LHS_T0"."AMOUNT" LIKE '9%'"#)
            && sql[where_at..].contains(r#""LHS_T0"."STATUS" = 'OPEN'"#),
        "the wrapper WHERE must carry the WHOLE declined filter, both conjuncts: {sql}"
    );
}

#[test]
fn declined_like_on_date_column_routes_to_wrapper_where() {
    let request = pushdown_request("T", &[("SHIPPED", ExasolType::Date)])
        .filter(like(table_col("T", "SHIPPED"), "2024%"))
        .build()
        .0;

    let sql = declined_filter_wrapper_sql(&request);

    assert!(
        sql.contains(r#"AS "LHS_T0" WHERE ("LHS_T0"."SHIPPED" LIKE '2024%')"#),
        "the DATE-column LIKE must be applied in the wrapper WHERE: {sql}"
    );
}

fn mixed_col_types() -> Vec<(String, ExasolType)> {
    vec![
        ("C_DECIMAL_A".to_string(), ExasolType::decimal(10, 2)),
        ("ID".to_string(), ExasolType::decimal(20, 0)),
        ("NAME".to_string(), ExasolType::WIDEST_VARCHAR),
        ("D".to_string(), ExasolType::Date),
        ("C_DOUBLE_A".to_string(), ExasolType::DoublePrecision),
        ("C_BOOL_A".to_string(), ExasolType::Boolean),
        ("C_TS_A".to_string(), ExasolType::BARE_TIMESTAMP),
    ]
}

#[test]
fn a_text_to_text_cast_renders_unchanged() {
    let pushdown_req = pushdown_request("T", &mixed_col_types())
        .item(
            cast_to(ExasolType::WIDEST_VARCHAR, col("name")),
            ExasolType::varchar(20),
        )
        .build_pushdown();
    let (items, _types, _widened) =
        project_split(&pushdown_req, &column_types(&mixed_col_types())).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "must project a single expression: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a full-row fallback: {items:?}");
    };
    assert_eq!(
        expr, r#"CAST("NAME" AS VARCHAR)"#,
        "a CAST of text to text renders unchanged: {expr}"
    );
}

#[test]
fn selectlist_items_the_scan_declines_widen_to_the_full_base_row() {
    let col_types = mixed_col_types();
    let text = ExasolType::WIDEST_VARCHAR;
    let cases = [
        (
            "UPPER(DOUBLE)",
            scalar_fn("UPPER", vec![col("c_double_a")]),
            text,
        ),
        (
            "UPPER(DECIMAL(10,2))",
            scalar_fn("UPPER", vec![col("c_decimal_a")]),
            text,
        ),
        ("LOWER(DATE)", scalar_fn("LOWER", vec![col("d")]), text),
        (
            "CAST(DECIMAL(10,2) AS VARCHAR)",
            cast_to(ExasolType::WIDEST_VARCHAR, col("c_decimal_a")),
            text,
        ),
        (
            "CAST(computed DECIMAL AS VARCHAR)",
            cast_to(
                ExasolType::WIDEST_VARCHAR,
                scalar_fn("MULT", [col("c_decimal_a"), num_lit(2)]),
            ),
            text,
        ),
        (
            "nested CONCAT over DECIMAL",
            scalar_fn(
                "CONCAT",
                [
                    col("id"),
                    scalar_fn("CONCAT", [str_lit("-"), col("c_decimal_a")]),
                ],
            ),
            text,
        ),
        (
            "INSTR with a start position",
            scalar_fn("INSTR", vec![col("name"), str_lit("b"), num_lit(3)]),
            ExasolType::decimal(18, 0),
        ),
        (
            "LIKE over DECIMAL(10,2)",
            like(col("c_decimal_a"), "1%"),
            ExasolType::Boolean,
        ),
        ("LIKE over DATE", like(col("d"), "1%"), ExasolType::Boolean),
        (
            "LIKE over DOUBLE",
            like(col("c_double_a"), "1%"),
            ExasolType::Boolean,
        ),
        (
            "LIKE over TIMESTAMP",
            like(col("c_ts_a"), "1%"),
            ExasolType::Boolean,
        ),
    ];
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::from(n.clone()))
        .collect();
    let expected_types: Vec<ExasolType> = col_types.iter().map(|(_, t)| *t).collect();
    for (label, item, declared) in cases {
        let pushdown_req = pushdown_request("T", &col_types)
            .item(item, declared)
            .build_pushdown();
        let (items, types, widened) =
            project_split(&pushdown_req, &column_types(&col_types)).expect("must project");

        assert!(widened, "{label}");
        assert_eq!(items, expected_names, "{label}");
        assert_eq!(types, expected_types, "{label}");
    }
}

/// Scenario: Scalar select-list expression is pushed into the scan-driving query
#[test]
fn selectlist_predicate_node_projects_as_expr() {
    let cases: Vec<(&str, serde_json::Value, &str)> = vec![
        (
            "predicate_in_constlist",
            in_list(col("name"), [str_lit("a"), str_lit("b")]),
            r#"("NAME" IN ('a', 'b'))"#,
        ),
        (
            "predicate_between",
            between(col("id"), num_lit(1), num_lit(10)),
            r#"("ID" BETWEEN 1 AND 10)"#,
        ),
        (
            "predicate_is_null",
            is_null(col("name")),
            r#"("NAME" IS NULL)"#,
        ),
        (
            "predicate_is_not_null",
            is_not_null(col("name")),
            r#"("NAME" IS NOT NULL)"#,
        ),
        (
            "predicate_notequal",
            compare("predicate_notequal", col("id"), num_lit(5)),
            r#"("ID" <> 5)"#,
        ),
    ];

    for (node_type, item, expected_frag) in cases {
        let pushdown_req = pushdown_request("T", &mixed_col_types())
            .item(item, ExasolType::Boolean)
            .build_pushdown();
        let (items, types, _widened) =
            project_split(&pushdown_req, &column_types(&mixed_col_types()))
                .unwrap_or_else(|e| panic!("[{node_type}] must project: {e}"));

        assert_eq!(
            items.len(),
            1,
            "[{node_type}] must project a single expression, not the full base row: {items:?}"
        );
        let ProjectionItem::Expr { expr } = &items[0] else {
            panic!(
                "[{node_type}] must be a rendered expression, not a full-row fallback: {items:?}"
            );
        };
        assert_eq!(
            expr, expected_frag,
            "[{node_type}] rendered fragment mismatch"
        );
        assert_eq!(
            types,
            vec![ExasolType::Boolean],
            "[{node_type}] declared type mismatch"
        );
    }
}

#[test]
fn selectlist_function_aggregate_still_widens_to_full_row() {
    let item = count_star();
    let col_types = mixed_col_types();
    let pushdown_req = pushdown_request("T", &mixed_col_types())
        .item(item, ExasolType::decimal(20, 0))
        .build_pushdown();
    let (items, types, widened) =
        project_split(&pushdown_req, &column_types(&col_types)).expect("must project");

    assert!(
        widened,
        "the widening must be REPORTED, not only performed: the dispatcher routes on \
         this flag alone (#196)"
    );
    assert_eq!(
        items.len(),
        col_types.len(),
        "function_aggregate must widen to the full base row, not project as an Expr: {items:?}"
    );
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::from(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the full-row fallback must project every base column unchanged"
    );
    let expected_types: Vec<ExasolType> = col_types.iter().map(|(_, t)| *t).collect();
    assert_eq!(types, expected_types);
}

#[test]
fn selectlist_like_over_integer_or_boolean_projects_its_text() {
    let col_types = mixed_col_types();
    for (subject, text) in [
        ("id", r#"CAST(CAST("ID" AS DECIMAL(38,0)) AS VARCHAR)"#),
        ("c_bool_a", "'TRUE'"),
    ] {
        let pushdown_req = pushdown_request("T", &col_types)
            .item(like(col(subject), "1%"), ExasolType::Boolean)
            .build_pushdown();
        let (items, types, widened) =
            project_split(&pushdown_req, &column_types(&col_types)).expect("must project");

        assert!(!widened, "{subject}");
        let [ProjectionItem::Expr { expr }] = items.as_slice() else {
            panic!("{subject} must project one expression: {items:?}");
        };
        assert!(expr.contains(text) && expr.contains("LIKE"), "{expr}");
        assert_eq!(types, vec![ExasolType::Boolean]);
    }
}

#[test]
fn selectlist_like_inside_case_over_decimal_falls_back_to_full_row() {
    let col_types = mixed_col_types();
    let case_expr = case_when(
        [like(col("c_decimal_a"), "1%")],
        [str_lit("yes"), str_lit("no")],
    );
    let pushdown_req = pushdown_request("T", &mixed_col_types())
        .item(case_expr, ExasolType::WIDEST_VARCHAR)
        .build_pushdown();
    let (items, types, widened) =
        project_split(&pushdown_req, &column_types(&col_types)).expect("must project (Ok)");

    assert!(
        widened,
        "a LIKE nested inside a CASE over a DECIMAL subject must widen the whole select list"
    );
    assert_eq!(
        items.len(),
        col_types.len(),
        "must fall back to the full base row, not a truncated projection: {items:?}"
    );
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::from(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the full-row fallback must project every base column unchanged"
    );
    let expected_types: Vec<ExasolType> = col_types.iter().map(|(_, t)| *t).collect();
    assert_eq!(types, expected_types);
}

#[test]
fn column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list() {
    let node = col("STRAßE");
    let unicode_folded = [("STRASSE".to_string(), ExasolType::WIDEST_VARCHAR)];
    let ascii_folded = [("STRAßE".to_string(), ExasolType::WIDEST_VARCHAR)];

    assert_eq!(
        column_exa_type(&node, &column_types(&unicode_folded)),
        Some(ExasolType::WIDEST_VARCHAR),
        "`STRAßE`.to_uppercase() is `STRASSE`, the key `involved_columns` builds"
    );
    assert_eq!(
        column_exa_type(&node, &column_types(&ascii_folded)),
        None,
        "`to_ascii_uppercase` leaves `STRAßE`, which the Unicode fold cannot match"
    );
}

fn assert_widens_to_full_base_row(select_item: Json, declared_type: ExasolType, why: &str) {
    let col_types = mixed_col_types();
    let pushdown_req = pushdown_request("T", &col_types)
        .item(select_item, declared_type)
        .build_pushdown();

    let (items, types, widened) =
        project_split(&pushdown_req, &column_types(&col_types)).expect("must project (Ok)");

    assert!(widened, "{why}: {items:?}");
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::from(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the widened projection must be the full base row, not a per-item projection"
    );
    let expected_types: Vec<ExasolType> = col_types.iter().map(|(_, t)| *t).collect();
    assert_eq!(types, expected_types, "EMITS types must be the base row's");
}

#[test]
fn project_columns_widens_on_aggregate_nested_in_scalar_item() {
    let select_item = scalar_fn(
        "ROUND",
        [agg_item("SUM", Some("c_decimal_a"), false), num_lit(2)],
    );
    let declared_type = ExasolType::decimal(18, 2);
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under a scalar item must widen the derived projection",
    );
}

#[test]
fn project_columns_top_level_aggregate_widening_is_unchanged_by_the_subtree_probe() {
    let select_item = agg_item("SUM", Some("c_decimal_a"), false);
    let declared_type = ExasolType::decimal(18, 2);
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "a top-level aggregate must still widen",
    );
}

#[test]
fn project_columns_widens_on_aggregate_nested_in_function_scalar_cast_item() {
    let select_item = cast_to(ExasolType::varchar(100), agg_item("SUM", Some("id"), false));
    let declared_type = ExasolType::varchar(100);
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under a function_scalar_cast item must widen",
    );
}

#[test]
fn project_columns_widens_on_aggregate_nested_in_function_scalar_case_item() {
    let select_item = case_when(
        [compare("predicate_less", col("ID"), num_lit(10))],
        [count_star(), num_lit(0)],
    );
    let declared_type = ExasolType::decimal(20, 0);
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested in a CASE result branch must widen",
    );
}

#[test]
fn project_columns_widens_on_aggregate_nested_in_arithmetic_node() {
    let select_item = scalar_fn(
        "ADD",
        [agg_item("SUM", Some("c_decimal_a"), false), num_lit(1)],
    );
    let declared_type = ExasolType::decimal(18, 2);
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under an arithmetic node must widen",
    );
}

#[test]
fn project_columns_widens_on_aggregate_nested_in_predicate_node() {
    let select_item = compare(
        "predicate_less",
        agg_item("SUM", Some("id"), false),
        num_lit(10),
    );
    let declared_type = ExasolType::Boolean;
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under a predicate node must widen",
    );
}

#[test]
fn project_columns_does_not_widen_when_select_item_has_no_nested_aggregate() {
    let col_types = mixed_col_types();
    let pushdown_req = pushdown_request("T", &col_types)
        .item(
            scalar_fn("ROUND", [col("C_DECIMAL_A"), num_lit(2)]),
            ExasolType::decimal(18, 2),
        )
        .build_pushdown();

    let (items, _types, widened) =
        project_split(&pushdown_req, &column_types(&col_types)).expect("must project (Ok)");

    assert!(
        !widened,
        "a scalar item with no nested aggregate must not widen: {items:?}"
    );
    assert_eq!(
        items.len(),
        1,
        "must project the single rendered expression, not the full base row: {items:?}"
    );
    assert!(matches!(items[0], ProjectionItem::Expr { .. }));
}

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/lakehouse-reader";
const SESSION_AK: &str = "ASIASESSIONKEYSENTINEL";
const SESSION_SK: &str = "SESSION_SECRET_SENTINEL";
const SESSION_TOKEN: &str = "SESSION_TOKEN_SENTINEL";

/// The credential set a request acts as once `resolve_aws_identity` has run: the
/// session key triple, with the role and `use_vended_credentials` as stated.
fn effective_creds(names_role: bool, use_vended_credentials: bool) -> ConnectionCreds {
    ConnectionCreds {
        access_key: SESSION_AK.into(),
        secret_key: SESSION_SK.into(),
        session_token: Some(SESSION_TOKEN.into()),
        use_vended_credentials,
        aws_assume_role_arn: names_role.then(|| ROLE_ARN.to_string()),
        ..unauthenticated_creds()
    }
}

fn select(creds: &ConnectionCreds, effective: &StorageBackend) -> Result<ScanStorage, UdfError> {
    scan_storage_for(
        creds,
        TEST_CONNECTION_NAME,
        true,
        effective,
        Some(&test_sealing_key()),
    )
}

fn sealed_payload(selected: ScanStorage) -> String {
    match selected {
        ScanStorage::Sealed { name, payload } => {
            assert_eq!(name, TEST_CONNECTION_NAME);
            payload
        }
        other => panic!("expected the sealed variant, got {other:?}"),
    }
}

#[test]
fn scan_storage_variant_follows_vending_and_the_role() {
    for (names_role, vending) in [(false, false), (true, false), (false, true), (true, true)] {
        let creds = effective_creds(names_role, vending);
        let selected = select(
            &creds,
            &crate::adapter::connection::storage_block(&creds, true),
        )
        .expect("key material is present");
        match (&selected, names_role || vending) {
            (ScanStorage::Connection { name, allow_http }, false) => {
                assert_eq!(name, TEST_CONNECTION_NAME);
                assert!(*allow_http);
            }
            (ScanStorage::Sealed { .. }, true) => {}
            (other, _) => panic!("role={names_role} vending={vending} selected {other:?}"),
        }
    }

    for (names_role, vending) in [(false, true), (true, false), (true, true)] {
        let creds = effective_creds(names_role, vending);
        let err = scan_storage_for(
            &creds,
            TEST_CONNECTION_NAME,
            true,
            &crate::adapter::connection::storage_block(&creds, true),
            None,
        )
        .expect_err("a credential the CONNECTION does not state is refused without key material");
        let message = err.to_string();
        for secret in [SESSION_SK, SESSION_TOKEN] {
            assert!(!message.contains(secret), "{message}");
        }
    }
}

/// Scenario: The scan receives the session credentials only inside the sealed envelope
#[test]
fn a_role_storage_block_is_sealed_and_unseals_to_the_session_backend() {
    let creds = effective_creds(true, false);
    let session_backend = crate::adapter::connection::storage_block(&creds, true);
    let payload = sealed_payload(select(&creds, &session_backend).expect("seal the session"));
    for secret in [SESSION_AK, SESSION_SK, SESSION_TOKEN] {
        assert!(!payload.contains(secret), "{payload}");
    }
    let opened = crate::scan::sealed::unseal_storage(&payload, &test_sealing_key())
        .expect("the envelope opens under the CONNECTION's key");
    assert_eq!(opened, session_backend);
    let StorageBackend::S3(props) = &opened else {
        panic!("a key-pair CONNECTION seals S3 storage, got {opened:?}");
    };
    assert_eq!(props.access_key, SESSION_AK);
    assert_eq!(props.secret_key, SESSION_SK);
    assert_eq!(props.session_token.as_deref(), Some(SESSION_TOKEN));
    assert_eq!(props.endpoint, "http://minio:9000");
    assert_eq!(props.region, "us-east-1");
    assert!(props.path_style);

    let vended = StorageBackend::S3(StorageProps {
        endpoint: "https://s3.eu-west-2.amazonaws.com".into(),
        region: "eu-west-2".into(),
        access_key: "VENDED_AK_SENTINEL".into(),
        secret_key: "VENDED_SK_SENTINEL".into(),
        session_token: Some("VENDED_TOKEN_SENTINEL".into()),
        ..Default::default()
    });
    let payload =
        sealed_payload(select(&effective_creds(true, true), &vended).expect("seal vended storage"));
    assert_eq!(
        crate::scan::sealed::unseal_storage(&payload, &test_sealing_key())
            .expect("the envelope opens under the CONNECTION's key"),
        vended,
        "a vending role CONNECTION seals the vended backend, not the session"
    );
}

fn shared_name_universe() -> ColumnTypes {
    let decimal = ExasolType::decimal(18, 0);
    let varchar = ExasolType::varchar(10);
    ColumnTypes::per_relation([
        ("LHS_T0".to_string(), &column_types(&[("X", decimal)])),
        ("LHS_T1".to_string(), &column_types(&[("X", varchar)])),
    ])
}

#[test]
fn output_select_list_casts_an_untyped_item_to_every_declared_type() {
    for exa_type in EXASOL_TYPE_SPELLINGS {
        assert_eq!(
            output_select_list([SqlFragment::new("x", None)], &[exa_type]),
            Some(vec![format!("CAST(x AS {exa_type})")]),
            "{exa_type}"
        );
    }
}

#[test]
fn output_select_list_keeps_an_item_that_already_has_its_declared_type() {
    let items = [
        SqlFragment::new(r#""GK_0""#, Some(ExasolType::Date)),
        SqlFragment::new(r#""GK_1""#, Some(ExasolType::decimal(9, 0))),
    ];
    assert_eq!(
        output_select_list(items, &[ExasolType::Date, ExasolType::decimal(18, 0)]),
        Some(vec![
            r#""GK_0""#.to_string(),
            r#"CAST("GK_1" AS DECIMAL(18,0))"#.to_string()
        ])
    );
}

#[test]
fn output_select_list_declines_a_list_of_another_length() {
    let items = [SqlFragment::new("x", None)];
    assert_eq!(
        output_select_list(items, &[ExasolType::Date, ExasolType::Date]),
        None
    );
}

fn layouts_of(column_types: &ColumnTypes) -> Vec<(&str, ExasolType, Option<BinaryLayout>)> {
    column_types
        .iter()
        .map(|(column, column_type)| {
            (
                column.name.as_str(),
                column_type.exa_type,
                column_type.binary,
            )
        })
        .collect()
}

#[test]
fn the_column_type_map_takes_each_binary_layout_from_the_declared_field() {
    let hex = ExasolType::WIDEST_VARCHAR;
    let fixed_hex = ExasolType::varchar(4);
    let number = ExasolType::decimal(20, 0);
    let involved = InvolvedColumns {
        types: vec![
            ("B".to_string(), hex),
            ("U".to_string(), fixed_hex),
            ("S".to_string(), hex),
            ("N".to_string(), number),
        ],
        ..InvolvedColumns::default()
    };
    let declared = [
        nullable_field("b", "binary"),
        nullable_field("u", "fixed_size_binary(2)"),
        nullable_field("s", "utf8"),
    ];

    assert_eq!(
        layouts_of(&ColumnTypes::of(&involved, &declared, false)),
        vec![
            ("B", hex, Some(BinaryLayout::Variable)),
            ("U", fixed_hex, Some(BinaryLayout::Fixed(2))),
            ("S", hex, None),
            ("N", number, None),
        ],
        "the request's types in declaration order; a column no field declares binary reads as text"
    );
    assert_eq!(
        layouts_of(&ColumnTypes::of(&involved, &declared, true)),
        vec![
            ("B", hex, None),
            ("U", fixed_hex, Some(BinaryLayout::Fixed(2))),
            ("S", hex, None),
            ("N", number, None),
        ],
        "BINARY_AS_STRING reads a variable-length column as text and keeps a fixed one as bytes"
    );
}

#[test]
fn the_column_type_map_takes_the_first_declared_field_of_a_case_folded_name() {
    let hex = ExasolType::WIDEST_VARCHAR;
    let involved = InvolvedColumns {
        types: vec![("B".to_string(), hex)],
        ..InvolvedColumns::default()
    };
    let binary_first = [nullable_field("b", "binary"), nullable_field("B", "utf8")];
    let text_first = [nullable_field("B", "utf8"), nullable_field("b", "binary")];

    assert_eq!(
        layouts_of(&ColumnTypes::of(&involved, &binary_first, false)),
        vec![("B", hex, Some(BinaryLayout::Variable))]
    );
    assert_eq!(
        layouts_of(&ColumnTypes::of(&involved, &text_first, false)),
        vec![("B", hex, None)]
    );
}

#[test]
fn a_concatenation_lists_each_map_in_turn() {
    let left = column_types(&[("A", ExasolType::Boolean), ("B", ExasolType::Date)]);
    let right = column_types(&[("C", ExasolType::DoublePrecision)]);

    assert_eq!(
        layouts_of(&ColumnTypes::concat([&left, &right])),
        vec![
            ("A", ExasolType::Boolean, None),
            ("B", ExasolType::Date, None),
            ("C", ExasolType::DoublePrecision, None),
        ]
    );
}

/// The types a request's select list parses to, or a phrase its refusal names.
type ParseOutcome = Result<Vec<ExasolType>, String>;

/// Scenario: A pushdown request with a missing or invalid Exasol type fails the query
#[test]
fn select_list_data_types_check_answers_per_request_shape() {
    let columns = [
        ("ID", ExasolType::decimal(20, 0)),
        ("NAME", ExasolType::varchar(10)),
    ];
    let full_row = vec![ExasolType::decimal(20, 0), ExasolType::varchar(10)];
    let one_item = || pushdown_request("T", &columns).column(col("ID"));
    let invalid_entry = serde_json::json!({"type": "decimal", "precision": 37, "scale": 0});
    let cases: [(&str, PushdownRequestBuilder, ParseOutcome); 7] = [
        (
            "an absent list",
            one_item().select_list_data_types(None),
            Err("selectListDataTypes".into()),
        ),
        (
            "an invalid entry",
            one_item().select_list_data_types(Some(serde_json::json!([invalid_entry]))),
            Err(invalid_entry.to_string()),
        ),
        (
            "more entries than items",
            one_item().select_list_data_types(Some(serde_json::json!([
                exasol_type_to_json(&ExasolType::decimal(20, 0)),
                exasol_type_to_json(&ExasolType::varchar(10)),
            ]))),
            Err("2 selectListDataTypes entries for 1 selectList items".into()),
        ),
        (
            "fewer entries than items",
            one_item().select_list_data_types(Some(serde_json::json!([]))),
            Err("0 selectListDataTypes entries for 1 selectList items".into()),
        ),
        (
            "an absent selectList",
            pushdown_request("T", &columns),
            Ok(full_row.clone()),
        ),
        (
            "a JSON-null selectList",
            pushdown_request("T", &columns).field("selectList", Json::Null),
            Ok(full_row),
        ),
        (
            "an empty selectList",
            pushdown_request("T", &columns).empty_select_list(),
            Ok(Vec::new()),
        ),
    ];
    for (shape, request, expected) in cases {
        let parsed = PushdownRequest::parse(pd(&request.build().0))
            .map(|parsed| parsed.select_types().to_vec());
        match (parsed, expected) {
            (Ok(types), Ok(expected)) => assert_eq!(types, expected, "{shape}"),
            (Err(UdfError::User(message)), Err(named)) => {
                assert!(message.contains(&named), "{shape}: {message}");
            }
            (outcome, expected) => panic!("{shape}: got {outcome:?}, expected {expected:?}"),
        }
    }
}

/// Scenario: A pushdown request with a missing or invalid Exasol type fails the query
#[test]
fn involved_column_with_an_unparsable_type_fails() {
    let invalid = serde_json::json!({"type": "char", "size": 2001});
    let cases = [
        (Some(invalid.clone()), invalid.to_string()),
        (None, "ID declares no dataType".to_string()),
    ];
    for (data_type, named) in cases {
        let (mut request, _) = pushdown_request("T", &[("ID", ExasolType::decimal(20, 0))]).build();
        let column = &mut request["involvedTables"][0]["columns"][0];
        match data_type {
            Some(data_type) => column["dataType"] = data_type,
            None => {
                column
                    .as_object_mut()
                    .expect("a column object")
                    .remove("dataType");
            }
        }
        match involved_columns(&request, |tables| tables.first()).err() {
            Some(UdfError::User(message)) => assert!(message.contains(&named), "{message}"),
            other => panic!("the column must fail with {named}, got {other:?}"),
        }
    }
}

#[test]
fn selectlist_tstz_literal_widens_via_emits_type_gate() {
    let literal = lit("literal_timestamp_utc", "2024-03-01 10:00:00");
    assert!(
        typed_scan(NO_COLUMNS).expression(&literal).is_some(),
        "literal_timestamp_utc must render through the scan translator"
    );
    let (proj_cols, proj_types, widened) = projected(id_name_request().item(
        literal,
        ExasolType::TimestampWithLocalTimeZone { precision: 3 },
    ));
    assert!(
        widened,
        "a TIMESTAMP WITH LOCAL TIME ZONE literal must widen the projection \
         so the RowScan dispatcher routes it to the qualified wrapper"
    );
    assert_eq!(
        proj_cols,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Column("NAME".into()),
        ],
        "a TIMESTAMP WITH LOCAL TIME ZONE literal must widen to the full base row: {proj_cols:?}"
    );
    assert_eq!(
        proj_types,
        vec![ExasolType::decimal(10, 0), ExasolType::varchar(100)]
    );
}

#[test]
fn selectlist_real_wire_name_tstz_literal_widens_and_routes() {
    let literal = lit("literal_timestamputc", "2024-03-01 10:00:00");
    assert!(
        typed_scan(NO_COLUMNS).expression(&literal).is_none(),
        "the DataFusion dialect must keep declining the real wire name (#242): \
         a render here would silently start pushing TSTZ predicates into the \
         scan filter"
    );
    let (proj_cols, proj_types, widened) = projected(id_name_request().item(
        literal,
        ExasolType::TimestampWithLocalTimeZone { precision: 3 },
    ));
    assert!(
        widened,
        "the real-wire-name TSTZ literal must widen the projection so the \
         RowScan dispatcher routes it to the qualified wrapper"
    );
    assert_eq!(
        proj_cols,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Column("NAME".into()),
        ]
    );
    assert_eq!(
        proj_types,
        vec![ExasolType::decimal(10, 0), ExasolType::varchar(100)]
    );
}

#[test]
fn a_translator_over_a_column_that_binds_no_entry_fails_naming_it() {
    let universe = shared_name_universe();
    let undeclared = alias_col("LHS_T0", "Y");
    let wrong_relation = alias_col("LHS_T2", "X");

    for (tree, column) in [(undeclared, "Y"), (wrong_relation, "X")] {
        let error = ScanTranslator::over(&universe, &[&tree])
            .expect_err("a column no entry binds is an adapter bug, never a decline");
        assert_mentions(&user_message(error), &[column]);
    }
}
