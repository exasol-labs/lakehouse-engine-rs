use super::super::scalar_over_agg::cast_merge_items;
use super::super::test_support::*;
use super::*;
use crate::scan::spec::{AggKind, DeleteMechanism, ScanStorage, SortKey};
use vs_expression::render_df_filter_safe;

/// Scenario: `walk_column_nodes` fires once per nested `column` node and never for other nodes.
#[test]
fn walk_column_nodes_visits_every_nested_column_node_once() {
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "PLUS",
        "arguments": [
            {"type": "column", "name": "A", "tableName": "T"},
            {"type": "literal_exactnumeric", "value": 1}
        ],
        "case": {
            "type": "case",
            "results": [
                {"type": "column", "name": "B"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        },
        "predicate": {
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C"},
            "right": {
                "type": "column",
                "name": "D",
                "nested": {"type": "column", "name": "E"}
            }
        }
    });

    let mut visited = Vec::new();
    walk_column_nodes(&expr, &mut |map| {
        visited.push(
            map.get("name")
                .and_then(|n| n.as_str())
                .unwrap()
                .to_string(),
        );
    });
    visited.sort();

    assert_eq!(
        visited,
        vec!["A", "B", "C", "D", "E"],
        "every column node must fire exactly once, including one nested inside another column node"
    );
}

/// Scenario: `walk_column_nodes` is a no-op on null, scalar, and empty-object roots, which callers pass unguarded.
#[test]
fn walk_column_nodes_never_invokes_callback_for_a_non_container_root() {
    let mut invocations: usize = 0;

    walk_column_nodes(&serde_json::Value::Null, &mut |_| invocations += 1);
    walk_column_nodes(&serde_json::json!("REGION"), &mut |_| invocations += 1);
    walk_column_nodes(&serde_json::json!(7), &mut |_| invocations += 1);
    walk_column_nodes(&serde_json::json!({}), &mut |_| invocations += 1);

    assert_eq!(
        invocations, 0,
        "a null, scalar, or empty-object root must be a no-op: groupBy/orderBy/selectList reach walk_column_nodes unguarded"
    );
}

/// Scenario: `strip_table_alias` removes every nested `tableAlias` key, keeping `tableName` and `name` (#193).
#[test]
fn strip_table_alias_removes_alias_preserves_table_name_and_name_recursively() {
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "PLUS",
        "tableAlias": "O",
        "arguments": [
            {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS", "tableAlias": "O"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });

    let stripped = strip_table_alias(&expr);

    assert_eq!(
        stripped,
        serde_json::json!({
            "type": "function_scalar",
            "name": "PLUS",
            "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        }),
        "every tableAlias key must be gone at every depth, while name/tableName survive"
    );
}

/// Scenario: A predicate the DataFusion dialect can express answers `true`.
#[test]
fn datafusion_renderable_true_for_a_rendering_predicate() {
    let expr = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "AGE"},
        "right": {"type": "literal_exactnumeric", "value": 18}
    });

    assert!(datafusion_renderable(&expr));
}

/// Scenario: `SECOND(ts, 3)`, a DataFusion arity refusal Exasol renders, answers `false`.
#[test]
fn datafusion_renderable_false_for_second_with_precision_arity_decline() {
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "SECOND",
        "arguments": [
            {"type": "column", "name": "TS"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });

    assert!(!datafusion_renderable(&expr));
}

/// Scenario: A trivially true `TRUE` literal answers `true`, so omitting it is a no-op.
#[test]
fn datafusion_renderable_true_for_trivially_true_literal() {
    let expr = serde_json::json!({"type": "literal_bool", "value": true});

    assert!(datafusion_renderable(&expr));
}

/// Scenario: `strip_table_alias` does not flip the decline/accept answer, or a conjunct would be silently dropped.
#[test]
fn datafusion_renderable_answer_unchanged_by_strip_table_alias() {
    let with_alias = serde_json::json!({
        "type": "function_scalar",
        "name": "SECOND",
        "tableAlias": "O",
        "arguments": [
            {"type": "column", "name": "TS", "tableName": "T", "tableAlias": "O"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    let stripped = strip_table_alias(&with_alias);

    assert_eq!(
        datafusion_renderable(&with_alias),
        datafusion_renderable(&stripped),
        "stripping tableAlias must not change whether the DataFusion dialect accepts the predicate"
    );
    assert!(
        !datafusion_renderable(&stripped),
        "SECOND(ts, 3) must still decline once table-alias-stripped"
    );

    let renders_with_alias = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "TS", "tableName": "T", "tableAlias": "O"},
        "right": {"type": "literal_exactnumeric", "value": 1}
    });
    let renders_stripped = strip_table_alias(&renders_with_alias);

    assert_eq!(
        datafusion_renderable(&renders_with_alias),
        datafusion_renderable(&renders_stripped),
        "stripping tableAlias must not change whether a RENDERING predicate is still accepted"
    );
    assert!(
        datafusion_renderable(&renders_stripped),
        "TS > 1 must still render once table-alias-stripped"
    );
}

fn delete_spec_template() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: "s3://warehouse/db/table".into(),
            projection: vec![ProjectionItem::Column("ID".into())],
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    }
}

/// Scenario: Positional deletes reach the per-shard spec at both file and partition granularity.
#[test]
fn adapter_preserves_positional_deletes_into_scan_spec() {
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
}

/// Scenario: A delete-carrying entry serializes its content type; a delete-free entry stays `[path, size]`.
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

/// Scenario: Delete refs ride only in the per-shard files argument, never the common blob.
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
        &[ProjectionItem::Column("ID".into())],
        &["DECIMAL(20,0)".to_string()],
        None,
        &[],
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

/// Scenario: The fan-out nests the distributor under an ungrouped scalar scan, splicing the common blob once.
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

/// Scenario: A single-shard plan is a from-less scalar scan on literals, with no distributor.
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

/// Scenario: Shard count oversubscribes the cluster and is capped at 300.
#[test]
fn shard_count_oversubscribes_and_caps_at_300() {
    assert_eq!(shard_count(10, 50, 500), 300, "must be capped at 300");
    assert_eq!(
        shard_count(10, 50, 350),
        300,
        "must be capped at min(files,300)=300"
    );
    assert_eq!(shard_count(1, 300, 1000), 300, "exactly 300 must stay 300");
    assert_eq!(shard_count(1, 301, 1000), 300, "301 must be capped at 300");
}

/// Scenario: Fewer files than G produces one shard per file with no empty shards.
#[test]
fn shard_count_clamped_to_file_count_no_empty_shards() {
    assert_eq!(shard_count(10, 8, 3), 3, "must clamp to file_count=3");
    assert_eq!(shard_count(4, 8, 5), 5, "must clamp to file_count=5");
    assert_eq!(shard_count(1, 1, 1), 1, "single file single shard");
    assert_eq!(shard_count(0, 8, 100), 1, "zero product must clamp to 1");
    assert_eq!(shard_count(5, 0, 100), 1, "zero factor must clamp to 1");
}

/// Scenario: The table root rides once in the common blob and file sizes ride in the shard payloads.
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
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: The table root is stripped from under-root paths and appears exactly once.
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
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: A data-file path outside the table root keeps its full absolute URI.
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
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: A multi-shard fan-out carries the root once and `[[path,size],...]` per shard.
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
        vec!["DECIMAL(20,0)".into()],
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
        vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
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
        vec!["DECIMAL(10,0)".into(), "VARCHAR(2000000)".into()],
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

/// Scenario: A filter predicate is pushed into the scan spec or kept out of it, never mistranslated
#[test]
fn pushdown_translates_or_omits_predicate() {
    let translatable = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "age"},
        "right": {"type": "literal_exactnumeric", "value": 18}
    });
    let filter_rendered = render_df_filter_safe(&translatable);
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
    let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
    let omitted = render_df_filter_safe(&untranslatable);
    assert!(
        omitted.is_none(),
        "untranslatable predicate must be omitted (None), not mistranslated"
    );

    let sql_no_filter = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["AGE".into()],
        vec!["DECIMAL(20,0)".into()],
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
        vec!["DECIMAL(20,0)".into()],
        Some(filter_str),
        None,
    );
    assert!(
        sql_with_filter.contains(">"),
        "filter must survive into the spec literal: {sql_with_filter}"
    );
}

/// Scenario: LIMIT is pushed into the scan spec and also appears at Exasol level
#[test]
fn row_scan_limit_in_common_arg() {
    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: `extract_offset` is 0 when `offset` is absent, as Exasol sends for `OFFSET 0`, else the value.
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

/// Scenario: `render_limit_offset` with offset 0 renders exactly ` LIMIT {n}`.
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
fn sql_string_literal_escapes_quotes() {
    let s = "it's a test";
    let lit = sql_string_literal(s);
    assert_eq!(lit, "'it''s a test'");
}

/// Scenario: An untranslatable scalar plus COUNT(*) falls back to the deduplicated base columns.
#[test]
fn extract_projection_fallback_is_duplicate_free() {
    let request = serde_json::json!({
        "involvedTables": [{
            "name": "EVENTS",
            "columns": [
                {"name": "id", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "name", "dataType": {"type": "varchar", "size": 2000000}},
            ],
        }],
    });
    let pushdown_req = serde_json::json!({
        "selectList": [
            {"type": "function_scalar", "name": "TOTALLY_UNKNOWN_FN", "arguments": [
                {"type": "column", "name": "id"}
            ]},
            {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 20, "scale": 0},
        ],
    });

    let (names, types, _widened) = extract_projection(&request, &pushdown_req).unwrap();

    let unique: std::collections::HashSet<&str> = names.iter().map(|p| p.emit_name()).collect();
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

/// Scenario: An aggregate select list yields a spec carrying the aggregate plans plus the filter.
#[test]
fn aggregate_query_builds_partial_agg_spec() {
    let agg_plans = vec![
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
    ];
    let merge_inputs =
        AggregateMergeInputs::new(Vec::new(), cast_merge_items(&agg_plans, &[]), None)
            .expect("a two-aggregate merge SELECT is never empty");
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["AMOUNT".into()],
            filter: Some("(\"REGION\" = 'EU')".into()),
            aggregates: Some(agg_plans),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };

    let shards = vec![vec![("s3://warehouse/f.parquet".to_string(), 1u64)]];
    let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &["AMOUNT".into()],
        &["DOUBLE PRECISION".to_string()],
        None,
        &col_types,
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

/// Scenario: A multi-shard fan-out serializes the common blob once and only file lists per shard.
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
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: `s3_max_connections` rides once in the common blob and is never dropped.
#[test]
fn common_spec_carries_s3_max_connections_exactly_once() {
    let files = vec![
        "s3://warehouse/shard0/part-000.parquet".into(),
        "s3://warehouse/shard1/part-001.parquet".into(),
        "s3://warehouse/shard2/part-002.parquet".into(),
    ];
    // Distinct from the default (8) and every other numeric field in the spec.
    let distinctive_s3_max_connections = 37;
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: ScanStorage::Inline(sample_storage()),
            s3_max_connections: distinctive_s3_max_connections,
            ..Default::default()
        },
        files: vec![],
    };

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
        &["ID".into()],
        &["DECIMAL(20,0)".to_string()],
        None,
        &[],
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
    let merge_select = cast_merge_items(&agg_plans, &[]);
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(agg_plans),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards =
        crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
    let merge_inputs = AggregateMergeInputs::new(Vec::new(), merge_select, None)
        .expect("a bare-aggregate merge SELECT is never empty");
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        &[],
        Some(&merge_inputs),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

/// Scenario: The aggregate merge renders `LIMIT n` on the wrapper, so `LIMIT 0` returns zero rows (#198).
#[test]
fn aggregate_merge_renders_request_limit_when_some() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
    let merge_select = cast_merge_items(&plans, &[]);
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(plans),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let merge_inputs = AggregateMergeInputs::new(Vec::new(), merge_select, Some(0))
        .expect("a bare-aggregate merge SELECT is never empty");
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        &[],
        Some(&merge_inputs),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        sql.ends_with("LIMIT 0"),
        "aggregate merge must render the request LIMIT: {sql}"
    );
}

/// Scenario: The aggregate wrapper merges per-shard COUNT/SUM/MIN/MAX partials in order.
#[test]
fn aggregate_wrapper_merges_partials() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
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

/// Scenario: The single-group merge SELECT sits directly over the scalar scan, with no `SELECT *` wrapper.
#[test]
fn aggregate_merge_over_scalar_scan_no_wrapper() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
    ];
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

/// Scenario: A single-shard aggregate merges directly over a from-less scalar scan.
#[test]
fn aggregate_single_shard_merge_over_fromless_scalar_scan() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
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

/// Scenario: The caller's merge SELECT is spliced verbatim; `aggregate_types` only drives `EMITS`.
#[test]
fn aggregate_merge_splices_caller_select_items_and_types_emits_per_plan() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: None,
        arg_expr: Some(r#"("A" * "B")"#.to_string()),
    }];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(plans),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let merge_select = vec![r#"CAST(ROUND(SUM("PARTIAL_sum_0"), 2) AS DECIMAL(30,4))"#.to_string()];
    let merge_inputs =
        AggregateMergeInputs::new(vec!["DECIMAL(30,4)".to_string()], merge_select, None)
            .expect("the fixture's merge SELECT is not empty");
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        &[],
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

/// Scenario: The AVG merge divides the merged sum by `NULLIF(SUM(cnt), 0)`.
#[test]
fn avg_wrapper_divides_sum_by_count_guarded() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Avg,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
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

/// Scenario: A single-shard aggregate still gets an outer merge wrapper.
#[test]
fn single_shard_aggregate_still_uses_merge_wrapper() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
    ];
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
    ScanSpec {
        common: CommonScanSpec {
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    }
}

/// Scenario: A lone single-group `COUNT(DISTINCT col)` wraps its DISTINCT fan-out in a native `COUNT(DISTINCT "V")`.
#[test]
fn count_distinct_wrapper_uses_native_count_distinct() {
    let base_spec = count_distinct_base_spec();
    let items = vec![SingleGroupItem::Distinct(DistinctCount {
        column: Some("L_SHIPMODE".into()),
        arg_expr: None,
    })];
    let col_types = vec![("L_SHIPMODE".to_string(), "VARCHAR(25)".to_string())];
    let shards = vec![
        vec![("s3://warehouse/a.parquet".to_string(), 1u64)],
        vec![("s3://warehouse/b.parquet".to_string(), 1u64)],
    ];
    let sql = build_count_distinct_scan_sql(
        &base_spec,
        &shards,
        &items,
        &col_types,
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

/// Scenario: Multiple or mixed COUNT(DISTINCT) aggregates route to the qualified wrapper, not a distinct fan-out.
#[test]
fn multi_count_distinct_declines_to_qualified_wrapper() {
    use super::super::joins::{
        FanOutProjection, build_qualified_single_table_fallback_sql, referenced_column_projection,
    };
    use super::super::single_group_agg::{has_distinct, is_lone_count_distinct};

    let cdist = |col: &str| {
        serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "distinct": true,
            "arguments": [{"type": "column", "name": col, "tableName": "T"}],
        })
    };
    let pushdown_req = serde_json::json!({
        "selectList": [cdist("CATEGORY"), cdist("REGION")],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    });

    let items = super::super::detect_aggregates(&pushdown_req)
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
    let all_cols = vec![
        ("CATEGORY".to_string(), "VARCHAR(25)".to_string()),
        ("REGION".to_string(), "VARCHAR(25)".to_string()),
        ("IRRELEVANT_COL".to_string(), "DECIMAL(20,0)".to_string()),
    ];
    let (proj, proj_types) = referenced_column_projection(&pushdown_req, &all_cols);
    let base = count_distinct_base_spec();
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: proj,
            ..base.common
        },
        files: base.files,
    };
    let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
    let fan_out = FanOutProjection::new(&fan_out_spec, &proj_types).expect("aligned fan-out");
    let sql = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
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

/// Scenario: LIMIT/OFFSET/ORDER BY never leak into a distinct fan-out, only onto the outer wrapper.
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
        ("A".to_string(), "DECIMAL(20,0)".to_string()),
        ("B".to_string(), "DECIMAL(20,0)".to_string()),
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
        &col_types,
        Some(1),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert_only_outer_limit_no_order_by(&sql1, "Case 1");
}

/// Scenario: A multi-shard row scan with LIMIT appends LIMIT to the outer SQL.
#[test]
fn multi_shard_row_scan_appends_outer_limit() {
    let files = vec![
        "s3://warehouse/f0.parquet".into(),
        "s3://warehouse/f1.parquet".into(),
    ];
    let sql = build_sql_for_fixture_n(
        files,
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: A multi-shard row scan is an outer ungrouped scalar scan over the distributor, with no `SELECT *` wrapper.
#[test]
fn pushdown_builds_scalar_scan_driving_sql() {
    let sql = build_sql_for_fixture_n(
        vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: LIMIT attaches directly to the outer scalar select, after the distributor subquery.
#[test]
fn limit_attaches_directly_to_outer_scalar_select() {
    let sql = build_sql_for_fixture_n(
        vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
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

/// Scenario: A single-shard scan is `{udf}('<common>', '<files>')` with each literal once.
#[test]
fn single_shard_two_arg_common_and_files_once() {
    let files = vec![
        "s3://warehouse/db/events/part-00000.parquet".into(),
        "s3://warehouse/db/events/part-00001.parquet".into(),
    ];
    let sql = build_sql_for_fixture_n(
        files.clone(),
        vec!["ID".into(), "NAME".into()],
        vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
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

/// Scenario: Files partition into G balanced, disjoint shards covering every file, none empty.
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

/// Scenario: Multi-shard row-scan SQL uses GROUP BY shard_key, never IPROC().
#[test]
fn scan_driving_sql_groups_by_shard_key_not_iproc() {
    let files: Vec<(String, u64)> = (0..3)
        .map(|i| (format!("s3://warehouse/f{i}.parquet"), (i as u64 + 1) * 100))
        .collect();
    let g = shard_count(3, 1, files.len());
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &["ID".into()],
        &["DECIMAL(20,0)".to_string()],
        None,
        &[],
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

/// Scenario: A single shard collapses to one invocation, with no VALUES or GROUP BY.
#[test]
fn single_shard_collapses_to_single_invocation() {
    let files = vec![("s3://warehouse/f0.parquet".to_string(), 500u64)];
    let g = shard_count(1, 1, files.len());
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &["ID".into()],
        &["DECIMAL(20,0)".to_string()],
        None,
        &[],
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

/// Scenario: A select-list `function_scalar` renders into the projection and EMITS clause.
#[test]
fn selectlist_scalar_expression_rendered_in_emits() {
    let upper_expr = serde_json::json!({
        "type": "function_scalar",
        "name": "UPPER",
        "arguments": [{"type": "column", "name": "NAME"}]
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [upper_expr],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
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
    assert_eq!(proj_types[0], "VARCHAR(2000000)");
}

/// Scenario: A select-list `function_scalar_cast` renders as a `ProjectionItem::Expr` (#136).
#[test]
fn selectlist_cast_node_rendered_in_emits() {
    let cast_expr = serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "ID"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [cast_expr],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, _proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
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
        rendered.contains(r#"CAST("ID" AS VARCHAR)"#),
        "projection must contain the rendered CAST expression: {proj_cols:?}"
    );
}

/// Scenario: A select-list `function_scalar_extract` renders as a `ProjectionItem::Expr` (#136).
#[test]
fn selectlist_extract_node_rendered_in_emits() {
    let extract_expr = serde_json::json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "YEAR",
        "arguments": [{"type": "column", "name": "EVENT_DATE"}]
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "EVENT_DATE", "dataType": {"type": "DATE"}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [extract_expr],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, _proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
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

/// Scenario: A select-list `function_scalar_case` renders as a `ProjectionItem::Expr` (#136).
#[test]
fn selectlist_case_node_rendered_in_emits() {
    let case_expr = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_less",
             "left": {"type": "column", "name": "SCORE"},
             "right": {"type": "literal_exactnumeric", "value": "50"}}
        ],
        "results": [
            {"type": "literal_string", "value": "low"},
            {"type": "literal_string", "value": "high"}
        ]
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "SCORE", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [case_expr],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, _proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
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

/// Scenario: A CAST to an unsupported target type still falls back to the full base row.
#[test]
fn selectlist_untranslatable_cast_falls_back_to_full_row() {
    let cast_expr = serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "ID"}],
        "dataType": {"type": "TIMESTAMP", "withLocalTimeZone": true}
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [cast_expr],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
    assert_eq!(
        proj_cols,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Column("NAME".into()),
        ],
        "an untranslatable CAST target must fall back to the full base row: {proj_cols:?}"
    );
    assert_eq!(proj_types, vec!["DECIMAL(10,0)", "VARCHAR(100)"]);
}

/// Scenario: A single projected literal renders to one positional `Expr` item (#190).
#[test]
fn selectlist_literal_rendered_as_positional_expr() {
    let literal = serde_json::json!({"type": "literal_exactnumeric", "value": 1});
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [literal],
            "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
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
    assert_eq!(proj_types[0], "DECIMAL(18,0)");
}

/// Scenario: `SELECT 1, name, 1` keeps three items with distinct EMITS identifiers (#190).
#[test]
fn selectlist_duplicate_literals_keep_distinct_positions() {
    let literal = serde_json::json!({"type": "literal_exactnumeric", "value": 1});
    let column = serde_json::json!({"type": "column", "name": "NAME"});
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [literal.clone(), column, literal],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 18, "scale": 0},
                {"type": "varchar", "size": 100},
                {"type": "decimal", "precision": 18, "scale": 0},
            ],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, _proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();

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

/// Scenario: A bare literal select-list item projects exactly one column (#190).
#[test]
fn selectlist_bare_literal_does_not_fall_back_to_full_row() {
    let literal = serde_json::json!({"type": "literal_string", "value": "x"});
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [literal],
            "selectListDataTypes": [{"type": "varchar", "size": 100}],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, _proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
    assert_eq!(
        proj_cols.len(),
        1,
        "a bare literal must project exactly one column, not the full base row: {proj_cols:?}"
    );
}

/// Scenario: A `TIMESTAMP WITH LOCAL TIME ZONE` literal widens the projection, since Exasol rejects it in EMITS (#218).
#[test]
fn selectlist_tstz_literal_widens_via_emits_type_gate() {
    let literal = serde_json::json!({
        "type": "literal_timestamp_utc",
        "value": "2024-03-01 10:00:00"
    });
    assert!(
        render_expression_safe(&literal).is_some(),
        "literal_timestamp_utc must render via render_expression_safe"
    );
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [literal],
            "selectListDataTypes": [{"type": "TIMESTAMP", "withLocalTimeZone": true}],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, widened) = extract_projection(&request, &pushdown_req).unwrap();
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
    assert_eq!(proj_types, vec!["DECIMAL(10,0)", "VARCHAR(100)"]);
}

/// Scenario: The real wire name `literal_timestamputc` also widens the projection (#242).
#[test]
fn selectlist_real_wire_name_tstz_literal_widens_and_routes() {
    let literal = serde_json::json!({
        "type": "literal_timestamputc",
        "value": "2024-03-01 10:00:00"
    });
    assert!(
        render_expression_safe(&literal).is_none(),
        "the DataFusion dialect must keep declining the real wire name (#242): \
         a render here would silently start pushing TSTZ predicates into the \
         scan filter"
    );
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [literal],
            "selectListDataTypes": [{"type": "TIMESTAMP", "withLocalTimeZone": true}],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, widened) = extract_projection(&request, &pushdown_req).unwrap();
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
    assert_eq!(proj_types, vec!["DECIMAL(10,0)", "VARCHAR(100)"]);
}

/// Scenario: A plain `TIMESTAMP` literal renders as a positional `Expr`, never matched as a prefix.
#[test]
fn selectlist_plain_timestamp_literal_rendered_as_expr() {
    let literal = serde_json::json!({
        "type": "literal_timestamp",
        "value": "2024-03-01 10:00:00"
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [literal],
            "selectListDataTypes": [{"type": "TIMESTAMP"}],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
    assert_eq!(
        proj_cols.len(),
        1,
        "a plain TIMESTAMP literal must not fall back to the full base row: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[0], ProjectionItem::Expr { .. }),
        "a plain TIMESTAMP literal must be rendered as an Expr: {proj_cols:?}"
    );
    assert_eq!(proj_types[0], "TIMESTAMP");
}

/// Scenario: A select-list `CAST(<col> AS CHAR(20))` projects with a `CHAR(20)` EMITS type (#192).
#[test]
fn project_columns_emits_char_type_for_cast_to_char_item() {
    let cast_item = serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "CHAR", "size": 20, "characterSet": "UTF8"},
        "arguments": [{"type": "column", "name": "c_varchar"}]
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "id", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "c_varchar", "dataType": {"type": "varchar", "size": 2000000}},
            ],
        }],
        "pushdownRequest": {
            "selectList": [
                {"type": "column", "name": "id"},
                cast_item,
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "CHAR", "size": 20, "characterSet": "UTF8"},
            ],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, widened) = extract_projection(&request, &pushdown_req).unwrap();

    assert!(
        !widened,
        "a CAST-to-CHAR item must not raise the full-base-row widening signal (#196)"
    );
    assert_eq!(
        proj_cols.len(),
        2,
        "must project exactly the two select-list items, no full-row fallback: {proj_cols:?}"
    );
    assert!(
        matches!(proj_cols[1], ProjectionItem::Expr { .. }),
        "the CAST-to-CHAR item must stay a rendered expression, not fall back to the \
         full base row: {proj_cols:?}"
    );
    assert_eq!(
        proj_types,
        vec!["DECIMAL(20,0)".to_string(), "CHAR(20)".to_string()],
        "the CAST-to-CHAR item must be declared CHAR(20), not VARCHAR(20): {proj_types:?}"
    );
}

/// Scenario: An untranslatable select-list item falls back to the bare column.
#[test]
fn selectlist_untranslatable_item_falls_back_to_column() {
    let bad_expr = serde_json::json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "column", "name": "AMOUNT"}]
    });
    let request = serde_json::json!({
        "involvedTables": [{
            "columns": [
                {"name": "AMOUNT", "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2}},
            ]
        }],
        "pushdownRequest": {
            "selectList": [bad_expr],
        }
    });
    let pushdown_req = request["pushdownRequest"].clone();
    let (proj_cols, proj_types, _widened) = extract_projection(&request, &pushdown_req).unwrap();
    assert_eq!(proj_cols.len(), 1);
    assert_eq!(proj_cols[0], "AMOUNT");
    assert_eq!(proj_types[0], "DECIMAL(18,2)");
}

/// Scenario: Filter predicate is pushed into the scan spec.
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

    let filter_json = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "id"},
        "right": {"type": "literal_exactnumeric", "value": 42}
    });

    let df_filter = render_df_filter_safe(&filter_json);
    assert!(
        df_filter.is_some(),
        "translatable filter must produce a DataFusion SQL string"
    );

    let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
    assert!(
        iceberg_pred.is_some(),
        "translatable filter must produce an Iceberg predicate"
    );

    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["ID".into()],
        vec!["DECIMAL(10,0)".into()],
        df_filter,
        None,
    );
    let common = common_arg_literal(&sql);
    assert!(
        common.contains("\"filter\"") && common.contains("42"),
        "filter must be pushed into the common arg: {common}"
    );
}

/// Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged.
#[test]
fn like_guard_varchar_subject_unchanged() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result,
        Some(filter),
        "VARCHAR subject must be returned unchanged"
    );
}

/// Scenario: LIKE on a genuine CHAR column pushes down unchanged.
#[test]
fn like_guard_char_subject_unchanged() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "code"},
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let col_types = vec![("CODE".to_string(), "CHAR(3) ASCII".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result,
        Some(filter),
        "CHAR subject must be returned unchanged"
    );
}

/// Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR.
#[test]
fn like_guard_date_subject_wraps_cast() {
    let column = serde_json::json!({"type": "column", "name": "signup_date"});
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": column.clone(),
        "pattern": {"type": "literal_string", "value": "2024%"}
    });
    let col_types = vec![("SIGNUP_DATE".to_string(), "DATE".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    let expected = serde_json::json!({
        "type": "predicate_like",
        "expression": {
            "type": "function_scalar_cast",
            "name": "CAST",
            "dataType": {"type": "VARCHAR"},
            "arguments": [column]
        },
        "pattern": {"type": "literal_string", "value": "2024%"}
    });
    assert_eq!(
        result,
        Some(expected),
        "DATE subject must be rewrapped in CAST(<col> AS VARCHAR)"
    );
}

/// Scenario: LIKE on a DECIMAL column declines the whole filter.
#[test]
fn like_guard_decimal_subject_declines() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "amount"},
        "pattern": {"type": "literal_string", "value": "9%"}
    });
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result, None,
        "DECIMAL subject must decline the whole filter"
    );
}

/// Scenario: `apply_type_rewrites` declines a `predicate_like` over a DECIMAL column.
#[test]
fn type_rewrite_pipeline_runs_like_guard() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "amount"},
        "pattern": {"type": "literal_string", "value": "9%"}
    });
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

    assert_eq!(
        apply_type_rewrites(&filter, &col_types),
        None,
        "the type-rewrite pipeline's LIKE-subject guard must decline a DECIMAL subject"
    );
}

/// Scenario: LIKE on an integer column, which arrives as `DECIMAL(20,0)`, declines the whole filter.
#[test]
fn like_guard_integer_subject_declines() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "quantity"},
        "pattern": {"type": "literal_string", "value": "1%"}
    });
    let col_types = vec![("QUANTITY".to_string(), "DECIMAL(20,0)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result, None,
        "integer (DECIMAL(20,0)) subject must decline the whole filter"
    );
}

/// Scenario: LIKE on a non-column subject is left untouched, regardless of `col_types`.
#[test]
fn like_guard_non_column_subject_untouched() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "amount"}]
        },
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result,
        Some(filter),
        "non-column LIKE subject must be left unchanged"
    );
}

/// Scenario: LIKE on a bare column missing from `col_types` declines the whole filter.
#[test]
fn like_guard_unresolvable_column_declines() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "mystery"},
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let col_types = vec![("OTHER".to_string(), "VARCHAR(2000000)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result, None,
        "unresolvable column subject must decline the whole filter"
    );
}

/// Scenario: A nested non-string LIKE declines the entire enclosing filter.
#[test]
fn like_guard_nested_decimal_declines_whole_filter() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {
                "type": "predicate_equal",
                "left": {"type": "column", "name": "status"},
                "right": {"type": "literal_string", "value": "OPEN"}
            },
            {
                "type": "predicate_and",
                "expressions": [
                    {
                        "type": "predicate_like",
                        "expression": {"type": "column", "name": "amount"},
                        "pattern": {"type": "literal_string", "value": "9%"}
                    }
                ]
            }
        ]
    });
    let col_types = vec![
        ("STATUS".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(9,2)".to_string()),
    ];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result, None,
        "a nested non-string LIKE must decline the whole top-level filter"
    );
}

/// Asserts the fixture genuinely declines, since a quietly rendering fixture would
/// assert nothing.
fn declined_filter_wrapper_sql(filter: &Json, col_types: &[(String, String)]) -> String {
    let (scan_filter, declined) = classify_where_filter(Some(filter), col_types);
    assert!(
        scan_filter.is_none(),
        "fixture precondition: a declining filter must never reach the scan spec: \
         {scan_filter:?}"
    );
    let declined = declined
        .expect("fixture precondition: a declining filter must be handed back for self-applying");
    let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
    let pushdown_req = serde_json::json!({"filter": filter.clone()});
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: col_types
                .iter()
                .map(|(name, _)| ProjectionItem::Column(name.clone()))
                .collect(),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let proj_types: Vec<String> = col_types.iter().map(|(_, ty)| ty.clone()).collect();
    let fan_out = super::super::joins::FanOutProjection::new(&fan_out_spec, &proj_types)
        .expect("aligned fan-out");
    super::super::joins::build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
        &fan_out,
        &[vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        Some(declined),
    )
    .expect("the wrapper must render the declined predicate")
}

/// Scenario: A nested non-string LIKE's whole enclosing filter is applied in the wrapper's `WHERE`.
#[test]
fn nested_like_decline_routes_to_wrapper_where() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {
                "type": "predicate_equal",
                "left": {"type": "column", "name": "STATUS", "tableName": "T"},
                "right": {"type": "literal_string", "value": "OPEN"},
            },
            {
                "type": "predicate_like",
                "expression": {"type": "column", "name": "AMOUNT", "tableName": "T"},
                "pattern": {"type": "literal_string", "value": "9%"},
            },
        ],
    });
    let col_types = vec![
        ("STATUS".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(9,2)".to_string()),
    ];

    let sql = declined_filter_wrapper_sql(&filter, &col_types);

    let where_at = sql
        .find(r#"AS "LHS_T0" WHERE "#)
        .unwrap_or_else(|| panic!("the declined filter must reach the wrapper: {sql}"));
    assert!(
        sql[where_at..].contains(r#""LHS_T0"."AMOUNT" LIKE '9%'"#)
            && sql[where_at..].contains(r#""LHS_T0"."STATUS" = 'OPEN'"#),
        "the wrapper WHERE must carry the WHOLE declined filter, both conjuncts: {sql}"
    );
}

/// Scenario: A declined integer-column LIKE is applied by the adapter in the wrapper.
#[test]
fn declined_like_on_integer_column_routes_to_wrapper_where() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "QUANTITY", "tableName": "T"},
        "pattern": {"type": "literal_string", "value": "1%"},
    });
    let col_types = vec![("QUANTITY".to_string(), "DECIMAL(20,0)".to_string())];

    let sql = declined_filter_wrapper_sql(&filter, &col_types);

    assert!(
        sql.contains(r#"AS "LHS_T0" WHERE ("LHS_T0"."QUANTITY" LIKE '1%')"#),
        "the integer-column LIKE must be applied in the wrapper WHERE: {sql}"
    );
}

/// Scenario: A fail-safe decline on an unresolvable subject type still self-applies in the wrapper.
#[test]
fn declined_like_on_unresolvable_column_routes_to_wrapper_where() {
    let filter = serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "MYSTERY", "tableName": "T"},
        "pattern": {"type": "literal_string", "value": "A%"},
    });
    let col_types = vec![("OTHER".to_string(), "VARCHAR(2000000)".to_string())];

    let sql = declined_filter_wrapper_sql(&filter, &col_types);

    assert!(
        sql.contains(r#"AS "LHS_T0" WHERE ("LHS_T0"."MYSTERY" LIKE 'A%')"#),
        "an unresolvable-subject decline must still be applied by the adapter, \
         never omitted: {sql}"
    );
}

/// Scenario: REGEXP_LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR.
#[test]
fn like_guard_regexp_date_subject_wraps_cast() {
    let column = serde_json::json!({"type": "column", "name": "signup_date"});
    let filter = serde_json::json!({
        "type": "predicate_like_regexp",
        "expression": column.clone(),
        "pattern": {"type": "literal_string", "value": "2024.*"}
    });
    let col_types = vec![("SIGNUP_DATE".to_string(), "DATE".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    let expected = serde_json::json!({
        "type": "predicate_like_regexp",
        "expression": {
            "type": "function_scalar_cast",
            "name": "CAST",
            "dataType": {"type": "VARCHAR"},
            "arguments": [column]
        },
        "pattern": {"type": "literal_string", "value": "2024.*"}
    });
    assert_eq!(
        result,
        Some(expected),
        "REGEXP_LIKE DATE subject must be rewrapped in CAST(<col> AS VARCHAR)"
    );
}

/// Scenario: A DECIMAL LIKE under `predicate_not` declines the whole filter.
#[test]
fn like_guard_not_wrapped_decimal_declines() {
    let filter = serde_json::json!({
        "type": "predicate_not",
        "expression": {
            "type": "predicate_like",
            "expression": {"type": "column", "name": "amount"},
            "pattern": {"type": "literal_string", "value": "9%"}
        }
    });
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result, None,
        "a DECIMAL LIKE inside predicate_not must decline the whole filter"
    );
}

/// Scenario: A DECIMAL LIKE inside a CASE WHEN condition declines the whole filter (#207).
#[test]
fn like_guard_decimal_inside_case_declines() {
    let filter = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {
                "type": "predicate_like",
                "expression": {"type": "column", "name": "amount"},
                "pattern": {"type": "literal_string", "value": "9%"}
            }
        ],
        "results": [
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result, None,
        "a DECIMAL LIKE buried in a function_scalar_case's arguments must decline \
         the whole filter: {result:?}"
    );
}

/// Scenario: A DATE LIKE inside a CASE WHEN condition is rewrapped in place, CASE preserved (#207).
#[test]
fn like_guard_date_inside_case_wraps_cast() {
    let column = serde_json::json!({"type": "column", "name": "signup_date"});
    let then_branch = serde_json::json!({"type": "literal_exactnumeric", "value": 1});
    let else_branch = serde_json::json!({"type": "literal_exactnumeric", "value": 0});
    let filter = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {
                "type": "predicate_like",
                "expression": column.clone(),
                "pattern": {"type": "literal_string", "value": "2024%"}
            }
        ],
        "results": [then_branch.clone(), else_branch.clone()]
    });
    let col_types = vec![("SIGNUP_DATE".to_string(), "DATE".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    let expected = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {
                "type": "predicate_like",
                "expression": {
                    "type": "function_scalar_cast",
                    "name": "CAST",
                    "dataType": {"type": "VARCHAR"},
                    "arguments": [column]
                },
                "pattern": {"type": "literal_string", "value": "2024%"}
            }
        ],
        "results": [then_branch, else_branch]
    });
    assert_eq!(
        result,
        Some(expected),
        "a DATE LIKE buried in a function_scalar_case's arguments must be rewrapped \
         in CAST(<col> AS VARCHAR) in place, with the CASE's results preserved: {result:?}"
    );
}

/// Scenario: A VARCHAR LIKE inside a CASE WHEN condition returns the input tree unchanged.
#[test]
fn like_guard_varchar_inside_case_unchanged() {
    let filter = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {
                "type": "predicate_like",
                "expression": {"type": "column", "name": "name"},
                "pattern": {"type": "literal_string", "value": "A%"}
            }
        ],
        "results": [
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    let col_types = vec![("NAME".to_string(), "VARCHAR(20)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result,
        Some(filter),
        "a VARCHAR LIKE buried in a function_scalar_case's arguments must be \
         returned unchanged: {result:?}"
    );
}

/// Scenario: `rewrite_expr_tree` hands `f` a node whose curated children are already rewritten.
#[test]
fn expr_tree_applies_f_to_children_before_their_parent() {
    let tree = serde_json::json!({
        "type": "outer",
        "expression": {"type": "inner"},
    });

    let out = rewrite_expr_tree(&tree, &|node| {
        let mut out = node.clone();
        if node.get("type").and_then(|t| t.as_str()) == Some("inner") {
            out["type"] = Json::from("inner_rewritten");
        } else {
            out["child_type_seen"] = node["expression"]["type"].clone();
        }
        Some(out)
    })
    .expect("an always-Some closure must never decline");

    assert_eq!(
        out["child_type_seen"],
        Json::from("inner_rewritten"),
        "the parent must see its already-rewritten child: {out}"
    );
}

/// Scenario: A `None` from `f` at any depth declines the whole tree.
#[test]
fn expr_tree_decline_deep_in_the_tree_propagates_to_the_root() {
    let tree = serde_json::json!({
        "type": "root",
        "expressions": [
            {"type": "keep"},
            {"type": "branch", "left": {"type": "decline_here"}},
        ],
    });

    let out = rewrite_expr_tree(&tree, &|node| {
        if node.get("type").and_then(|t| t.as_str()) == Some("decline_here") {
            return None;
        }
        Some(node.clone())
    });

    assert_eq!(
        out, None,
        "a declined descendant must decline the whole tree"
    );
}

/// Scenario: Only curated fields in their grammar shapes are descended; `dataType` and `name` never are.
#[test]
fn expr_tree_recurses_only_into_curated_fields_of_the_expected_shape() {
    let tree = serde_json::json!({
        "type": "root",
        "dataType": {"type": "VARCHAR"},
        "arguments": {"type": "not_an_array"},
        "pattern": "not_an_object",
        "expression": {"type": "curated_single"},
        "results": [{"type": "curated_array"}],
    });

    let out = rewrite_expr_tree(&tree, &|node| {
        let mut out = node.clone();
        out["visited"] = Json::Bool(true);
        Some(out)
    })
    .expect("an always-Some closure must never decline");

    assert_eq!(
        out["expression"]["visited"],
        Json::Bool(true),
        "curated field `expression` must be descended into: {out}"
    );
    assert_eq!(
        out["results"][0]["visited"],
        Json::Bool(true),
        "curated field `results` must be descended into: {out}"
    );
    for skipped in ["dataType", "arguments"] {
        assert_eq!(
            out[skipped]["visited"],
            Json::Null,
            "`{skipped}` must not be descended into: {out}"
        );
    }
    assert_eq!(
        out["pattern"],
        Json::from("not_an_object"),
        "a non-object single-child field must be left untouched: {out}"
    );
}

/// Scenario: A non-object node reaches `f` too, with no leaf early-return.
#[test]
fn expr_tree_applies_f_to_a_non_object_node() {
    for leaf in [
        Json::Null,
        serde_json::json!("UPPER"),
        serde_json::json!(7),
        serde_json::json!([1, 2]),
    ] {
        assert_eq!(
            rewrite_expr_tree(&leaf, &|_| Some(Json::from("touched"))),
            Some(Json::from("touched")),
            "a non-object node must be handed to `f`: {leaf}"
        );
    }
}

fn decimal_rewrite_col_types() -> Vec<(String, String)> {
    vec![
        ("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string()),
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("D".to_string(), "DATE".to_string()),
        ("C_DOUBLE_A".to_string(), "DOUBLE PRECISION".to_string()),
        ("C_BOOL_A".to_string(), "BOOLEAN".to_string()),
        ("C_TS_A".to_string(), "TIMESTAMP".to_string()),
    ]
}

fn decimal_column() -> Json {
    serde_json::json!({"type": "column", "name": "c_decimal_a"})
}

fn cast_to(target: &str, arg: Json) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": target},
        "arguments": [arg],
    })
}

/// Scenario: A non-object node is returned unchanged.
#[test]
fn decimal_rewrite_passes_through_non_object_node() {
    let col_types = decimal_rewrite_col_types();
    for node in [
        Json::Null,
        serde_json::json!("UPPER"),
        serde_json::json!(7),
        serde_json::json!([1, 2]),
    ] {
        assert_eq!(
            rewrite_decimal_stringifications(&node, &col_types),
            node.clone(),
            "a non-object node must be passed through: {node}"
        );
    }
}

/// Scenario: `CAST(<decimal column> AS VARCHAR)` is replaced by a trimming `decimal_to_varchar_exasol` node.
#[test]
fn rewrite_cast_decimal_to_varchar_replaces_whole_node() {
    let node = cast_to("VARCHAR", decimal_column());
    let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());

    assert_eq!(
        out.get("type").and_then(|t| t.as_str()),
        Some("decimal_to_varchar_exasol"),
        "the whole CAST node must be replaced, not nested: {out}"
    );
    let inner = &out["arguments"][0];
    assert_eq!(
        inner.get("name").and_then(|n| n.as_str()),
        Some("c_decimal_a"),
        "the wrapped node must be the original column: {out}"
    );
    let sql = render_expression_safe(&out).expect("must render");
    assert!(
        sql.contains(r#"CAST("C_DECIMAL_A" AS VARCHAR)"#) && sql.contains("regexp_replace"),
        "must render via format_decimal_exasol_style: {sql}"
    );
}

/// Scenario: `CAST(<decimal column> AS CHAR)` is also replaced.
#[test]
fn rewrite_cast_decimal_to_char_replaces_whole_node() {
    let node = cast_to("CHAR", decimal_column());
    let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());
    assert_eq!(
        out.get("type").and_then(|t| t.as_str()),
        Some("decimal_to_varchar_exasol"),
        "CAST AS CHAR over a DECIMAL column must also be rewritten: {out}"
    );
}

/// Scenario: The live nested-CONCAT shape wraps each decimal argument and preserves its structure.
#[test]
fn rewrite_nested_concat_wraps_only_inner_decimal() {
    let node = serde_json::json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "id"},
            {
                "type": "function_scalar",
                "name": "CONCAT",
                "arguments": [
                    {"type": "literal_string", "value": "-"},
                    {"type": "column", "name": "c_decimal_a"}
                ]
            }
        ]
    });
    let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());

    // ID is DECIMAL(20,0), so as a direct outer-CONCAT argument it is wrapped too.
    assert_eq!(out.get("name").and_then(|n| n.as_str()), Some("CONCAT"));
    let outer_args = out["arguments"].as_array().unwrap();
    assert_eq!(
        outer_args.len(),
        2,
        "outer CONCAT arg count preserved: {out}"
    );

    let inner = &outer_args[1];
    assert_eq!(inner.get("name").and_then(|n| n.as_str()), Some("CONCAT"));
    let inner_args = inner["arguments"].as_array().unwrap();
    assert_eq!(
        inner_args[0].get("type").and_then(|t| t.as_str()),
        Some("literal_string"),
        "the '-' literal must be untouched: {out}"
    );
    assert_eq!(
        inner_args[1].get("type").and_then(|t| t.as_str()),
        Some("decimal_to_varchar_exasol"),
        "the inner C_DECIMAL_A must be wrapped (post-order recursion reached it): {out}"
    );
    assert_eq!(
        inner_args[1]["arguments"][0]
            .get("name")
            .and_then(|n| n.as_str()),
        Some("c_decimal_a"),
    );
}

/// Scenario: `CONCAT(NAME, C_DECIMAL_A)` wraps only the DECIMAL column.
#[test]
fn rewrite_concat_wraps_only_decimal_leaves_varchar() {
    let node = serde_json::json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "column", "name": "c_decimal_a"}
        ]
    });
    let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());
    let args = out["arguments"].as_array().unwrap();
    assert_eq!(
        args[0].get("type").and_then(|t| t.as_str()),
        Some("column"),
        "the VARCHAR column must stay a bare column: {out}"
    );
    assert_eq!(
        args[1].get("type").and_then(|t| t.as_str()),
        Some("decimal_to_varchar_exasol"),
        "the DECIMAL column must be wrapped: {out}"
    );
}

/// Scenario: `LENGTH(<decimal column>)` wraps its argument.
#[test]
fn rewrite_length_wraps_decimal_argument() {
    let node = serde_json::json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [decimal_column()]
    });
    let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());
    assert_eq!(out.get("name").and_then(|n| n.as_str()), Some("LENGTH"));
    assert_eq!(
        out["arguments"][0].get("type").and_then(|t| t.as_str()),
        Some("decimal_to_varchar_exasol"),
        "LENGTH's DECIMAL argument must be wrapped: {out}"
    );
}

/// Scenario: A non-DECIMAL bare column argument to CAST, CONCAT, or LENGTH is unchanged.
#[test]
fn rewrite_non_decimal_argument_unchanged() {
    let col_types = decimal_rewrite_col_types();

    let cast_varchar = cast_to(
        "VARCHAR",
        serde_json::json!({"type": "column", "name": "name"}),
    );
    assert_eq!(
        rewrite_decimal_stringifications(&cast_varchar, &col_types),
        cast_varchar,
        "CAST of a VARCHAR column must be unchanged"
    );

    let length_date = serde_json::json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [{"type": "column", "name": "d"}]
    });
    assert_eq!(
        rewrite_decimal_stringifications(&length_date, &col_types),
        length_date,
        "LENGTH of a DATE column must be unchanged"
    );
}

/// Scenario: A computed-expression stringifier argument is left unchanged, a tracked exception (#223).
#[test]
fn rewrite_computed_expression_argument_unchanged() {
    let computed = serde_json::json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [decimal_column(), {"type": "literal_exactnumeric", "value": 2}]
    });
    let col_types = decimal_rewrite_col_types();

    let cast = cast_to("VARCHAR", computed.clone());
    assert_eq!(
        rewrite_decimal_stringifications(&cast, &col_types),
        cast,
        "CAST of a computed DECIMAL expression must be left unchanged: it is not a bare column"
    );

    let concat = serde_json::json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [{"type": "column", "name": "name"}, computed]
    });
    assert_eq!(
        rewrite_decimal_stringifications(&concat, &col_types),
        concat,
        "a computed-expression CONCAT argument must be left unchanged"
    );
}

/// Scenario: A DECIMAL column in a non-stringifying context is not wrapped.
#[test]
fn rewrite_non_stringifying_context_unchanged() {
    let col_types = decimal_rewrite_col_types();

    let cmp = serde_json::json!({
        "type": "predicate_greater",
        "left": decimal_column(),
        "right": {"type": "literal_exactnumeric", "value": 5}
    });
    assert_eq!(
        rewrite_decimal_stringifications(&cmp, &col_types),
        cmp,
        "a DECIMAL column in a comparison must not be wrapped"
    );

    let cast_double = cast_to("DOUBLE", decimal_column());
    assert_eq!(
        rewrite_decimal_stringifications(&cast_double, &col_types),
        cast_double,
        "CAST(decimal AS DOUBLE) must not be wrapped"
    );
}

/// Scenario: A DECIMAL stringification reachable only through a CASE THEN branch is wrapped.
#[test]
fn rewrite_reaches_decimal_inside_case_then_branch() {
    let node = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {
                "type": "predicate_greater",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "literal_exactnumeric", "value": 0}
            }
        ],
        "results": [
            {
                "type": "function_scalar",
                "name": "CONCAT",
                "arguments": [
                    {"type": "literal_string", "value": "x"},
                    {"type": "column", "name": "c_decimal_a"}
                ]
            }
        ]
    });
    let out = rewrite_decimal_stringifications(&node, &decimal_rewrite_col_types());

    let then_concat = &out["results"][0];
    assert_eq!(
        then_concat.get("name").and_then(|n| n.as_str()),
        Some("CONCAT"),
        "the CASE THEN CONCAT must be preserved: {out}"
    );
    assert_eq!(
        then_concat["arguments"][1]
            .get("type")
            .and_then(|t| t.as_str()),
        Some("decimal_to_varchar_exasol"),
        "the DECIMAL inside the CASE THEN CONCAT must be wrapped: {out}"
    );
}

/// Scenario: A select-list `CAST(c_decimal_a AS VARCHAR(20))` projects one trimmed `Expr` at its declared type.
#[test]
fn selectlist_decimal_cast_routed_not_full_row_fallback() {
    let pushdown_req = serde_json::json!({
        "selectList": [ cast_to("VARCHAR", decimal_column()) ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 20} ],
    });
    let (items, types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "the CAST-to-VARCHAR item must project to a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a bare column / full-row fallback: {items:?}");
    };
    assert!(
        expr.contains(r#"CAST("C_DECIMAL_A" AS VARCHAR)"#) && expr.contains("regexp_replace"),
        "the projected expression must render the trimmed DECIMAL→string form: {expr}"
    );
    assert_eq!(
        types,
        vec!["VARCHAR(20)".to_string()],
        "the EMITS type must stay the item's declared selectListDataTypes type"
    );
}

/// Scenario: The nested-CONCAT select-list item wraps `C_DECIMAL_A`'s CAST in the trim form.
#[test]
fn selectlist_nested_concat_decimal_arg_rewritten() {
    let item = serde_json::json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "id"},
            {
                "type": "function_scalar",
                "name": "CONCAT",
                "arguments": [
                    {"type": "literal_string", "value": "-"},
                    decimal_column()
                ]
            }
        ]
    });
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "the nested-CONCAT item must project to a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a bare column / full-row fallback: {items:?}");
    };
    assert!(
        expr.contains(r#"regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR)"#),
        "the inner C_DECIMAL_A argument must be rendered through the trim wrapper: {expr}"
    );
}

/// Scenario: A select-list `LENGTH(c_decimal_a)` renders a trim-wrapped `character_length(...)`.
#[test]
fn selectlist_length_decimal_arg_rewritten() {
    let item = serde_json::json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [decimal_column()]
    });
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 0} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "the LENGTH item must project to a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a bare column / full-row fallback: {items:?}");
    };
    assert!(
        expr.contains(
            "character_length(regexp_replace(regexp_replace(CAST(\"C_DECIMAL_A\" AS VARCHAR)"
        ),
        "LENGTH over a DECIMAL column must render the trim-wrapped character_length: {expr}"
    );
}

/// Scenario: A select-list CAST of a VARCHAR column renders a plain CAST, with no trim.
#[test]
fn stringify_nondecimal_column_unchanged() {
    let pushdown_req = serde_json::json!({
        "selectList": [ cast_to("VARCHAR", serde_json::json!({"type": "column", "name": "name"})) ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 20} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

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
        "a CAST over a non-DECIMAL column must render unchanged, exactly as before this fix: {expr}"
    );
}

/// Scenario: A select-list `CAST(c_decimal_a * 2 AS VARCHAR)` renders untrimmed (#223).
#[test]
fn stringify_computed_decimal_arg_untouched() {
    let computed = serde_json::json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [decimal_column(), {"type": "literal_exactnumeric", "value": 2}]
    });
    let pushdown_req = serde_json::json!({
        "selectList": [ cast_to("VARCHAR", computed) ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "must project a single expression: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a full-row fallback: {items:?}");
    };
    assert_eq!(
        expr, r#"CAST(("C_DECIMAL_A" * 2) AS VARCHAR)"#,
        "a CAST of a computed DECIMAL expression must render unchanged: {expr}"
    );
    assert!(
        !expr.contains("regexp_replace"),
        "a computed-expression CAST must not be trimmed (tracked exception #223): {expr}"
    );
}

/// Scenario: `UPPER(c_decimal_a)` projects a single trimmed expression at its declared type.
#[test]
fn selectlist_upper_decimal_arg_coerced_not_full_row() {
    let item = string_fn("UPPER", vec![decimal_column()]);
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
    });
    let (items, types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "UPPER(c_decimal_a) must project a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a full-row fallback: {items:?}");
    };
    assert!(
        expr.contains(r#"upper(regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR)"#),
        "UPPER's DECIMAL argument must render through the trimmed decimal-to-string form: {expr}"
    );
    assert_eq!(
        types,
        vec!["VARCHAR(2000000)".to_string()],
        "the EMITS type must stay the item's declared selectListDataTypes type"
    );
}

/// Scenario: `LOWER(c_date)` projects a single expression containing `CAST("D" AS VARCHAR)`.
#[test]
fn selectlist_lower_date_arg_cast_to_varchar() {
    let item = string_fn("LOWER", vec![column("d")]);
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "LOWER(c_date) must project a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a full-row fallback: {items:?}");
    };
    assert!(
        expr.contains(r#"CAST("D" AS VARCHAR)"#),
        "LOWER's DATE argument must be wrapped in CAST(<col> AS VARCHAR): {expr}"
    );
}

/// Scenario: `UPPER(c_double)` degrades to the full base row with no error.
#[test]
fn selectlist_string_fn_over_double_falls_back_to_full_row() {
    let col_types = decimal_rewrite_col_types();
    let item = string_fn("UPPER", vec![column("c_double_a")]);
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
    });
    let (items, types, _widened) =
        project_columns(&pushdown_req, col_types.clone()).expect("must project");

    assert_eq!(
        items.len(),
        col_types.len(),
        "UPPER(c_double_a) must fall back to the full base row, not a truncated projection: {items:?}"
    );
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::Column(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the full-row fallback must project every base column unchanged"
    );
    let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(types, expected_types);
}

/// Scenario: `INSTR(c_decimal_a, '.')` coerces the column argument and leaves the literal.
#[test]
fn selectlist_instr_decimal_arg_coerces_first_position_only() {
    let item = string_fn(
        "INSTR",
        vec![
            decimal_column(),
            serde_json::json!({"type": "literal_string", "value": "."}),
        ],
    );
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 0} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert_eq!(
        items.len(),
        1,
        "INSTR(c_decimal_a, '.') must project a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a full-row fallback: {items:?}");
    };
    assert!(
        expr.starts_with(r#"strpos(regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR)"#),
        "INSTR's first (string) argument must render the trimmed decimal form: {expr}"
    );
    assert!(
        expr.ends_with("'.')"),
        "INSTR's second (substring) argument, a literal, must be left untouched: {expr}"
    );
}

/// Scenario: A three-argument `INSTR` degrades to the full base row rather than a truncated `strpos` (#228).
#[test]
fn selectlist_instr_with_start_position_falls_back_to_full_row() {
    let col_types = decimal_rewrite_col_types();
    let item = string_fn(
        "INSTR",
        vec![
            column("name"),
            serde_json::json!({"type": "literal_string", "value": "b"}),
            serde_json::json!({"type": "literal_exactnumeric", "value": 3}),
        ],
    );
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 0} ],
    });
    let (items, _types, _widened) =
        project_columns(&pushdown_req, col_types.clone()).expect("must project");

    assert_eq!(
        items.len(),
        col_types.len(),
        "the arity-decline INSTR must fall back to the full base row, not a truncated strpos: {items:?}"
    );
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::Column(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the full-row fallback must project every base column unchanged"
    );
}

/// Scenario: Each whitelisted select-list predicate node renders as a positional `Expr` (#196).
#[test]
fn selectlist_predicate_node_projects_as_expr() {
    let cases: Vec<(&str, serde_json::Value, &str)> = vec![
        (
            "predicate_in_constlist",
            serde_json::json!({
                "type": "predicate_in_constlist",
                "expression": column("name"),
                "arguments": [
                    {"type": "literal_string", "value": "a"},
                    {"type": "literal_string", "value": "b"},
                ]
            }),
            r#"("NAME" IN ('a', 'b'))"#,
        ),
        (
            "predicate_between",
            serde_json::json!({
                "type": "predicate_between",
                "expression": column("id"),
                "left": {"type": "literal_exactnumeric", "value": 1},
                "right": {"type": "literal_exactnumeric", "value": 10},
            }),
            r#"("ID" BETWEEN 1 AND 10)"#,
        ),
        (
            "predicate_is_null",
            serde_json::json!({
                "type": "predicate_is_null",
                "expression": column("name"),
            }),
            r#"("NAME" IS NULL)"#,
        ),
        (
            "predicate_is_not_null",
            serde_json::json!({
                "type": "predicate_is_not_null",
                "expression": column("name"),
            }),
            r#"("NAME" IS NOT NULL)"#,
        ),
        (
            "predicate_notequal",
            serde_json::json!({
                "type": "predicate_notequal",
                "left": column("id"),
                "right": {"type": "literal_exactnumeric", "value": 5},
            }),
            r#"("ID" <> 5)"#,
        ),
        (
            "predicate_like_regexp",
            serde_json::json!({
                "type": "predicate_like_regexp",
                "expression": column("name"),
                "pattern": {"type": "literal_string", "value": "^a.*"},
            }),
            r#"regexp_like("NAME", '^a.*')"#,
        ),
    ];

    for (node_type, item, expected_frag) in cases {
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "boolean"} ],
        });
        let (items, types, _widened) = project_columns(&pushdown_req, decimal_rewrite_col_types())
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
            vec!["BOOLEAN".to_string()],
            "[{node_type}] declared type mismatch"
        );
    }
}

/// Scenario: A `function_aggregate` select-list item still widens, so it reaches the aggregate planner (#196).
#[test]
fn selectlist_function_aggregate_still_widens_to_full_row() {
    let item = serde_json::json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [],
        "distinct": false
    });
    let col_types = decimal_rewrite_col_types();
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "decimal", "precision": 20, "scale": 0} ],
    });
    let (items, types, widened) =
        project_columns(&pushdown_req, col_types.clone()).expect("must project");

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
        .map(|(n, _)| ProjectionItem::Column(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the full-row fallback must project every base column unchanged"
    );
    let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(types, expected_types);
}

/// Scenario: A select-list `predicate_like` over a DATE rewraps the subject as `CAST("D" AS VARCHAR)` (#219).
#[test]
fn selectlist_like_over_date_projects_cast_expr() {
    let item = serde_json::json!({
        "type": "predicate_like",
        "expression": column("d"),
        "pattern": {"type": "literal_string", "value": "2024%"}
    });
    let pushdown_req = serde_json::json!({
        "selectList": [ item ],
        "selectListDataTypes": [ {"type": "boolean"} ],
    });
    let (items, types, widened) =
        project_columns(&pushdown_req, decimal_rewrite_col_types()).expect("must project");

    assert!(
        !widened,
        "a DATE LIKE subject rewraps, it must not widen to the full base row"
    );
    assert_eq!(
        items.len(),
        1,
        "the DATE LIKE item must project a single expression, not the full base row: {items:?}"
    );
    let ProjectionItem::Expr { expr } = &items[0] else {
        panic!("must be a rendered expression, not a full-row fallback: {items:?}");
    };
    assert!(
        expr.contains(r#"CAST("D" AS VARCHAR)"#) && expr.contains("LIKE"),
        "the DATE subject must be rewrapped in CAST(<col> AS VARCHAR) before the LIKE: {expr}"
    );
    assert_eq!(types, vec!["BOOLEAN".to_string()]);
}

/// Scenario: A select-list LIKE over a non-string or unresolved subject widens to the full base row, never `Err`.
#[test]
fn selectlist_like_over_non_string_subject_falls_back_to_full_row() {
    let col_types = decimal_rewrite_col_types();
    let cases: Vec<(&str, Json)> = vec![
        (
            "c_decimal_a (DECIMAL(10,2))",
            serde_json::json!({
                "type": "predicate_like",
                "expression": column("c_decimal_a"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }),
        ),
        (
            "id (DECIMAL(20,0), integer)",
            serde_json::json!({
                "type": "predicate_like",
                "expression": column("id"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }),
        ),
        (
            "c_double_a (DOUBLE PRECISION)",
            serde_json::json!({
                "type": "predicate_like",
                "expression": column("c_double_a"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }),
        ),
        (
            "c_bool_a (BOOLEAN)",
            serde_json::json!({
                "type": "predicate_like",
                "expression": column("c_bool_a"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }),
        ),
        (
            "c_ts_a (TIMESTAMP)",
            serde_json::json!({
                "type": "predicate_like",
                "expression": column("c_ts_a"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }),
        ),
        (
            "unresolvable column name",
            serde_json::json!({
                "type": "predicate_like",
                "expression": column("not_a_column"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }),
        ),
        (
            "predicate_like_regexp over c_decimal_a",
            serde_json::json!({
                "type": "predicate_like_regexp",
                "expression": column("c_decimal_a"),
                "pattern": {"type": "literal_string", "value": "^1.*"}
            }),
        ),
    ];

    for (label, item) in cases {
        let pushdown_req = serde_json::json!({
            "selectList": [ item ],
            "selectListDataTypes": [ {"type": "boolean"} ],
        });
        let (items, types, widened) = project_columns(&pushdown_req, col_types.clone())
            .unwrap_or_else(|e| panic!("[{label}] must project (Ok), not error: {e}"));

        assert!(
            widened,
            "[{label}] a non-string LIKE subject must widen to the full base row"
        );
        assert_eq!(
            items.len(),
            col_types.len(),
            "[{label}] must fall back to the full base row, not a truncated projection: {items:?}"
        );
        let expected_names: Vec<ProjectionItem> = col_types
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        assert_eq!(
            items, expected_names,
            "[{label}] the full-row fallback must project every base column unchanged"
        );
        let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(types, expected_types, "[{label}] EMITS types mismatch");
    }
}

/// Scenario: A select-list LIKE over a DECIMAL nested in a CASE still widens to the full base row.
#[test]
fn selectlist_like_inside_case_over_decimal_falls_back_to_full_row() {
    let col_types = decimal_rewrite_col_types();
    let case_expr = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {
                "type": "predicate_like",
                "expression": column("c_decimal_a"),
                "pattern": {"type": "literal_string", "value": "1%"}
            }
        ],
        "results": [
            {"type": "literal_string", "value": "yes"},
            {"type": "literal_string", "value": "no"}
        ]
    });
    let pushdown_req = serde_json::json!({
        "selectList": [ case_expr ],
        "selectListDataTypes": [ {"type": "VARCHAR", "size": 2000000} ],
    });
    let (items, types, widened) =
        project_columns(&pushdown_req, col_types.clone()).expect("must project (Ok)");

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
        .map(|(n, _)| ProjectionItem::Column(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the full-row fallback must project every base column unchanged"
    );
    let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(types, expected_types);
}

/// Scenario: Every argument of the pure string functions is a string position at every arity.
#[test]
fn string_position_args_coerces_every_argument_of_all_string_functions() {
    for name in ["CONCAT", "TRIM", "LTRIM", "RTRIM", "REPLACE", "TRANSLATE"] {
        assert_eq!(
            string_position_args(name, 1),
            StringPositionArgs::Coerce(vec![0]),
            "{name}/1 must coerce index 0"
        );
        assert_eq!(
            string_position_args(name, 2),
            StringPositionArgs::Coerce(vec![0, 1]),
            "{name}/2 must coerce both indices"
        );
        assert_eq!(
            string_position_args(name, 3),
            StringPositionArgs::Coerce(vec![0, 1, 2]),
            "{name}/3 must coerce every index"
        );
    }
}

/// Scenario: Only the first argument of these functions is a string position.
#[test]
fn string_position_args_coerces_first_argument_only() {
    for name in [
        "LOWER",
        "UPPER",
        "ASCII",
        "INITCAP",
        "REVERSE",
        "LENGTH",
        "OCTET_LENGTH",
        "UNICODE",
        "SUBSTR",
        "REPEAT",
        "LEFT",
        "RIGHT",
    ] {
        for arg_count in 1..=3 {
            assert_eq!(
                string_position_args(name, arg_count),
                StringPositionArgs::Coerce(vec![0]),
                "{name}/{arg_count} must coerce index 0 only"
            );
        }
    }
}

/// Scenario: `LPAD`/`RPAD` exclude the length argument but coerce the pad string.
#[test]
fn string_position_args_excludes_numeric_arguments() {
    for name in ["LPAD", "RPAD"] {
        assert_eq!(
            string_position_args(name, 2),
            StringPositionArgs::Coerce(vec![0]),
            "{name}/2 has no pad-string argument to coerce"
        );
        assert_eq!(
            string_position_args(name, 3),
            StringPositionArgs::Coerce(vec![0, 2]),
            "{name}/3 must coerce the subject and the pad string, never the length"
        );
    }
}

/// Scenario: `CHR`/`UNICODECHR` and non-string functions are not governed.
#[test]
fn string_position_args_not_governed_for_chr_and_non_string_functions() {
    for name in ["CHR", "UNICODECHR", "ABS", "CASE"] {
        for arg_count in 0..=3 {
            assert_eq!(
                string_position_args(name, arg_count),
                StringPositionArgs::NotGoverned,
                "{name}/{arg_count} must not be governed"
            );
        }
    }
}

/// Scenario: A lowercase function name resolves identically.
#[test]
fn string_position_args_matches_lowercase_function_name() {
    assert_eq!(
        string_position_args("upper", 1),
        string_position_args("UPPER", 1),
        "a lowercase name must resolve like its uppercase form"
    );
    assert_eq!(
        string_position_args("upper", 1),
        StringPositionArgs::Coerce(vec![0])
    );
    assert_eq!(
        string_position_args("instr", 3),
        StringPositionArgs::Decline,
        "a lowercase name must reach the arity decline too"
    );
}

/// Scenario: No returned index addresses past the end of the argument list.
#[test]
fn string_position_args_never_returns_out_of_range_index() {
    let governed = [
        "CONCAT",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REPLACE",
        "TRANSLATE",
        "LOWER",
        "UPPER",
        "ASCII",
        "INITCAP",
        "REVERSE",
        "LENGTH",
        "OCTET_LENGTH",
        "UNICODE",
        "SUBSTR",
        "REPEAT",
        "LEFT",
        "RIGHT",
        "LPAD",
        "RPAD",
        "INSTR",
        "LOCATE",
    ];
    for name in governed {
        for arg_count in 0..=5 {
            if let StringPositionArgs::Coerce(indices) = string_position_args(name, arg_count) {
                for i in indices {
                    assert!(
                        i < arg_count,
                        "{name}/{arg_count} returned out-of-range index {i}"
                    );
                }
            }
        }
    }
}

/// Scenario: `INSTR`/`LOCATE` beyond two arguments decline on arity alone (#228).
#[test]
fn string_position_args_declines_instr_locate_beyond_two_args() {
    assert_eq!(
        string_position_args("INSTR", 3),
        StringPositionArgs::Decline,
        "INSTR/3 drops its start-position argument — must decline"
    );
    assert_eq!(
        string_position_args("INSTR", 4),
        StringPositionArgs::Decline,
        "INSTR/4 drops its start-position and occurrence arguments — must decline"
    );
    assert_eq!(
        string_position_args("LOCATE", 3),
        StringPositionArgs::Decline,
        "LOCATE/3 drops its start-position argument — must decline"
    );
    for name in ["INSTR", "LOCATE"] {
        assert_eq!(
            string_position_args(name, 2),
            StringPositionArgs::Coerce(vec![0, 1]),
            "{name}/2 is rendered faithfully and must coerce both arguments"
        );
    }
}

fn column(name: &str) -> Json {
    serde_json::json!({"type": "column", "name": name})
}

fn string_fn(name: &str, args: Vec<Json>) -> Json {
    serde_json::json!({
        "type": "function_scalar",
        "name": name,
        "arguments": args,
    })
}

fn trimmed_decimal(name: &str) -> Json {
    serde_json::json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [column(name)],
    })
}

fn cast_varchar(name: &str) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "VARCHAR"},
        "arguments": [column(name)],
    })
}

fn equals(left: Json, right: Json) -> Json {
    serde_json::json!({"type": "predicate_equal", "left": left, "right": right})
}

/// Scenario: A non-object node passes through.
#[test]
fn string_fn_guard_passes_through_non_object_node() {
    let col_types = decimal_rewrite_col_types();
    for node in [
        Json::Null,
        serde_json::json!("UPPER"),
        serde_json::json!(7),
        serde_json::json!([1, 2]),
    ] {
        assert_eq!(
            string_function_arg_type_guard(&node, &col_types),
            Some(node.clone()),
            "a non-object node must be passed through: {node}"
        );
    }
}

/// Scenario: A string-position VARCHAR or CHAR column argument pushes down unchanged.
#[test]
fn string_fn_guard_leaves_varchar_argument_unchanged() {
    let col_types = decimal_rewrite_col_types();
    for name in ["UPPER", "LOWER", "TRIM", "LTRIM", "CONCAT", "LENGTH"] {
        let node = string_fn(name, vec![column("name")]);
        assert_eq!(
            string_function_arg_type_guard(&node, &col_types),
            Some(node.clone()),
            "{name} over a VARCHAR column must be unchanged"
        );
    }
    let char_types = vec![("C_CHAR_A".to_string(), "CHAR(10)".to_string())];
    let node = string_fn("UPPER", vec![column("c_char_a")]);
    assert_eq!(
        string_function_arg_type_guard(&node, &char_types),
        Some(node.clone()),
        "a CHAR column argument must be unchanged"
    );
}

/// Scenario: A string-position DECIMAL column argument renders through Exasol's trimmed form.
#[test]
fn string_fn_guard_wraps_decimal_argument_in_trim() {
    let col_types = decimal_rewrite_col_types();

    let out =
        string_function_arg_type_guard(&string_fn("UPPER", vec![decimal_column()]), &col_types);
    assert_eq!(
        out,
        Some(string_fn("UPPER", vec![trimmed_decimal("c_decimal_a")])),
        "UPPER's DECIMAL argument must be wrapped in the trimmed-string node"
    );

    for name in ["TRIM", "LTRIM"] {
        assert_eq!(
            string_function_arg_type_guard(&string_fn(name, vec![decimal_column()]), &col_types),
            Some(string_fn(name, vec![trimmed_decimal("c_decimal_a")])),
            "{name}'s DECIMAL argument must be wrapped"
        );
    }

    assert_eq!(
        string_function_arg_type_guard(&string_fn("UPPER", vec![column("id")]), &col_types),
        Some(string_fn("UPPER", vec![trimmed_decimal("id")])),
        "an integer DECIMAL(p,0) argument must be wrapped too"
    );

    let sql = render_expression_safe(
        &string_function_arg_type_guard(&string_fn("UPPER", vec![decimal_column()]), &col_types)
            .expect("must not decline"),
    )
    .expect("must render");
    assert_eq!(
        sql,
        r#"upper(regexp_replace(regexp_replace(CAST("C_DECIMAL_A" AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', ''))"#,
        "UPPER over a DECIMAL column must render the trimmed form: {sql}"
    );
}

/// Scenario: A string-position DATE argument is wrapped in `CAST(<col> AS VARCHAR)`, matching Exasol's date format.
#[test]
fn string_fn_guard_casts_date_argument_to_varchar() {
    let col_types = decimal_rewrite_col_types();
    assert_eq!(
        string_function_arg_type_guard(&string_fn("LOWER", vec![column("d")]), &col_types),
        Some(string_fn("LOWER", vec![cast_varchar("d")])),
        "LOWER's DATE argument must be wrapped in CAST(<col> AS VARCHAR)"
    );
}

/// Scenario: BOOLEAN, DOUBLE, and TIMESTAMP arguments decline, since their text forms differ between engines.
#[test]
fn string_fn_guard_declines_boolean_double_and_timestamp_arguments() {
    let col_types = decimal_rewrite_col_types();
    for col in ["c_bool_a", "c_double_a", "c_ts_a"] {
        for name in ["UPPER", "TRIM", "CONCAT", "LENGTH"] {
            assert_eq!(
                string_function_arg_type_guard(&string_fn(name, vec![column(col)]), &col_types),
                None,
                "{name} over {col} must decline"
            );
        }
    }
}

/// Scenario: A string-position argument whose column does not resolve declines fail-safe.
#[test]
fn string_fn_guard_declines_unresolved_column_name() {
    let col_types = decimal_rewrite_col_types();
    assert_eq!(
        string_function_arg_type_guard(&string_fn("UPPER", vec![column("mystery")]), &col_types),
        None,
        "an unresolvable column argument must decline"
    );
}

/// Scenario: A `column` node with no `name` field declines fail-safe.
#[test]
fn string_fn_guard_declines_nameless_column_node() {
    let col_types = decimal_rewrite_col_types();
    let node = string_fn("UPPER", vec![serde_json::json!({"type": "column"})]);
    assert_eq!(
        string_function_arg_type_guard(&node, &col_types),
        None,
        "a nameless column argument must decline"
    );
}

/// Scenario: The guard reaches a string function nested under a comparison predicate (#210).
#[test]
fn string_fn_guard_reaches_function_under_comparison_predicate() {
    let col_types = decimal_rewrite_col_types();
    let node = equals(
        string_fn("UPPER", vec![decimal_column()]),
        serde_json::json!({"type": "literal_string", "value": "X"}),
    );
    assert_eq!(
        string_function_arg_type_guard(&node, &col_types),
        Some(equals(
            string_fn("UPPER", vec![trimmed_decimal("c_decimal_a")]),
            serde_json::json!({"type": "literal_string", "value": "X"}),
        )),
        "a string function under `left` must be coerced"
    );
}

/// Scenario: A decline anywhere in the tree propagates to the root.
#[test]
fn string_fn_guard_nested_decline_propagates_to_root() {
    let col_types = decimal_rewrite_col_types();
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            equals(column("name"), serde_json::json!({"type": "literal_string", "value": "X"})),
            {
                "type": "predicate_not",
                "expression": equals(
                    string_fn("UPPER", vec![column("c_double_a")]),
                    serde_json::json!({"type": "literal_string", "value": "X"})
                )
            }
        ]
    });
    assert_eq!(
        string_function_arg_type_guard(&filter, &col_types),
        None,
        "a nested non-coercible string function must decline the whole tree"
    );
}

/// Scenario: Only string-position indices are coerced; numeric positions stay untouched.
#[test]
fn string_fn_guard_leaves_numeric_position_arguments_untouched() {
    let col_types = decimal_rewrite_col_types();

    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("SUBSTR", vec![decimal_column(), column("id"), column("id")]),
            &col_types
        ),
        Some(string_fn(
            "SUBSTR",
            vec![trimmed_decimal("c_decimal_a"), column("id"), column("id")]
        )),
        "SUBSTR's start and length arguments must stay bare columns"
    );

    for name in ["REPEAT", "LEFT", "RIGHT"] {
        assert_eq!(
            string_function_arg_type_guard(
                &string_fn(name, vec![decimal_column(), column("id")]),
                &col_types
            ),
            Some(string_fn(
                name,
                vec![trimmed_decimal("c_decimal_a"), column("id")]
            )),
            "{name}'s numeric argument must stay a bare column"
        );
    }

    // LPAD(str, length, pad): index 0 and 2 coerced, index 1 untouched.
    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("LPAD", vec![decimal_column(), column("id"), column("d")]),
            &col_types
        ),
        Some(string_fn(
            "LPAD",
            vec![
                trimmed_decimal("c_decimal_a"),
                column("id"),
                cast_varchar("d")
            ]
        )),
        "LPAD must coerce the subject and the pad string, never the length"
    );

    let length_literal = serde_json::json!({"type": "literal_exactnumeric", "value": 10});
    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("LPAD", vec![decimal_column(), length_literal.clone()]),
            &col_types
        ),
        Some(string_fn(
            "LPAD",
            vec![trimmed_decimal("c_decimal_a"), length_literal]
        )),
        "a 2-argument LPAD must coerce index 0 only"
    );
}

/// Scenario: `INSTR` and `LOCATE` coerce both of their two arguments.
#[test]
fn string_fn_guard_coerces_both_instr_and_locate_arguments() {
    let col_types = decimal_rewrite_col_types();

    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("INSTR", vec![decimal_column(), column("d")]),
            &col_types
        ),
        Some(string_fn(
            "INSTR",
            vec![trimmed_decimal("c_decimal_a"), cast_varchar("d")]
        )),
        "INSTR must coerce both of its arguments"
    );

    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("LOCATE", vec![column("d"), decimal_column()]),
            &col_types
        ),
        Some(string_fn(
            "LOCATE",
            vec![cast_varchar("d"), trimmed_decimal("c_decimal_a")]
        )),
        "LOCATE must coerce both of its arguments"
    );
}

/// Scenario: `INSTR`/`LOCATE` beyond two arguments decline through the guard even over VARCHAR (#228).
#[test]
fn string_fn_guard_declines_instr_locate_beyond_two_args() {
    let col_types = decimal_rewrite_col_types();
    let start = serde_json::json!({"type": "literal_exactnumeric", "value": 3});

    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("INSTR", vec![column("name"), column("name"), start.clone()]),
            &col_types
        ),
        None,
        "INSTR/3 over VARCHAR arguments must still decline"
    );
    assert_eq!(
        string_function_arg_type_guard(
            &string_fn(
                "INSTR",
                vec![column("name"), column("name"), start.clone(), start.clone()]
            ),
            &col_types
        ),
        None,
        "INSTR/4 over VARCHAR arguments must still decline"
    );
    assert_eq!(
        string_function_arg_type_guard(
            &string_fn("LOCATE", vec![column("name"), column("name"), start]),
            &col_types
        ),
        None,
        "LOCATE/3 over VARCHAR arguments must still decline"
    );
}

/// Scenario: `CHR`/`UNICODECHR` arguments are neither coerced nor declined, but their children are recursed.
#[test]
fn string_fn_guard_excludes_chr_and_unicodechr() {
    let col_types = decimal_rewrite_col_types();
    for name in ["CHR", "UNICODECHR"] {
        for arg in ["id", "c_double_a"] {
            let node = string_fn(name, vec![column(arg)]);
            assert_eq!(
                string_function_arg_type_guard(&node, &col_types),
                Some(node.clone()),
                "{name}({arg}) must be left completely untouched"
            );
        }
    }

    let nested = string_fn("CHR", vec![string_fn("LENGTH", vec![decimal_column()])]);
    assert_eq!(
        string_function_arg_type_guard(&nested, &col_types),
        Some(string_fn(
            "CHR",
            vec![string_fn("LENGTH", vec![trimmed_decimal("c_decimal_a")])]
        )),
        "a governed function under CHR must still be coerced"
    );
}

/// Scenario: A column name in any letter case resolves.
#[test]
fn string_fn_guard_resolves_case_mismatched_column_name() {
    let col_types = decimal_rewrite_col_types();
    let node = string_fn("UPPER", vec![column("C_DeCiMaL_a")]);
    assert_eq!(
        string_function_arg_type_guard(&node, &col_types),
        Some(string_fn("UPPER", vec![trimmed_decimal("C_DeCiMaL_a")])),
        "a mixed-case column name must resolve against the uppercase map"
    );
}

/// Scenario: The `col_types` lookup folds with full-Unicode `to_uppercase`, missing an ASCII-folded list.
#[test]
fn column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list() {
    let node = column("STRAßE");
    let unicode_folded = [("STRASSE".to_string(), "VARCHAR(2000000)".to_string())];
    let ascii_folded = [("STRAßE".to_string(), "VARCHAR(2000000)".to_string())];

    assert_eq!(
        column_exa_type(&node, &unicode_folded),
        Some("VARCHAR(2000000)"),
        "`STRAßE`.to_uppercase() is `STRASSE`, the key `extract_all_column_types` builds"
    );
    assert_eq!(
        column_exa_type(&node, &ascii_folded),
        None,
        "`to_ascii_uppercase` leaves `STRAßE`, which the Unicode fold cannot match"
    );
}

/// Scenario: A non-bare-column string-position argument is left unchanged and does not decline (#223).
#[test]
fn string_fn_guard_leaves_computed_argument_unchanged() {
    let col_types = decimal_rewrite_col_types();

    let literal = string_fn(
        "UPPER",
        vec![serde_json::json!({"type": "literal_string", "value": "x"})],
    );
    assert_eq!(
        string_function_arg_type_guard(&literal, &col_types),
        Some(literal.clone()),
        "a literal argument must be left unchanged without declining"
    );

    let computed = string_fn(
        "UPPER",
        vec![string_fn(
            "MULT",
            vec![
                decimal_column(),
                serde_json::json!({"type": "literal_exactnumeric", "value": 2}),
            ],
        )],
    );
    assert_eq!(
        string_function_arg_type_guard(&computed, &col_types),
        Some(computed.clone()),
        "a computed argument must be left unchanged without declining"
    );
}

/// Scenario: An inner string function's argument is coerced before the outer function's check.
#[test]
fn string_fn_guard_coerces_inner_nested_string_function() {
    let col_types = decimal_rewrite_col_types();
    let node = string_fn("UPPER", vec![string_fn("TRIM", vec![decimal_column()])]);
    assert_eq!(
        string_function_arg_type_guard(&node, &col_types),
        Some(string_fn(
            "UPPER",
            vec![string_fn("TRIM", vec![trimmed_decimal("c_decimal_a")])]
        )),
        "the inner TRIM's DECIMAL argument must be coerced exactly once"
    );
}

/// Scenario: `cast_to_declared_type` casts only when a non-default declared type is present.
#[test]
fn cast_to_declared_type_skips_the_varchar_default_and_absent_type() {
    assert_eq!(
        cast_to_declared_type("SUM(x)", Some("DECIMAL(18,2)")),
        "CAST(SUM(x) AS DECIMAL(18,2))"
    );
    assert_eq!(
        cast_to_declared_type("SUM(x)", Some("VARCHAR(2000000)")),
        "SUM(x)"
    );
    assert_eq!(cast_to_declared_type("SUM(x)", None), "SUM(x)");
}

fn assert_widens_to_full_base_row(select_item: Json, declared_type: Json, why: &str) {
    let col_types = decimal_rewrite_col_types();
    let pushdown_req = serde_json::json!({
        "selectList": [select_item],
        "selectListDataTypes": [declared_type],
    });

    let (items, types, widened) =
        project_columns(&pushdown_req, col_types.clone()).expect("must project (Ok)");

    assert!(widened, "{why}: {items:?}");
    let expected_names: Vec<ProjectionItem> = col_types
        .iter()
        .map(|(n, _)| ProjectionItem::Column(n.clone()))
        .collect();
    assert_eq!(
        items, expected_names,
        "the widened projection must be the full base row, not a per-item projection"
    );
    let expected_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(types, expected_types, "EMITS types must be the base row's");
}

/// Scenario: An aggregate nested under a pushable select-list node widens, or shards return unmerged partials (#194).
#[test]
fn project_columns_widens_on_aggregate_nested_in_scalar_item() {
    let select_item = serde_json::json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [
            agg_item("SUM", Some("c_decimal_a"), false),
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    let declared_type = serde_json::json!({"type": "DECIMAL", "precision": 18, "scale": 2});
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under a scalar item must widen the derived projection",
    );
}

/// Scenario: A top-level aggregate select item widens byte-identically.
#[test]
fn project_columns_top_level_aggregate_widening_is_unchanged_by_the_subtree_probe() {
    let select_item = agg_item("SUM", Some("c_decimal_a"), false);
    let declared_type = serde_json::json!({"type": "DECIMAL", "precision": 18, "scale": 2});
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "a top-level aggregate must still widen",
    );
}

/// Scenario: A `function_scalar_cast` wrapping a nested aggregate widens.
#[test]
fn project_columns_widens_on_aggregate_nested_in_function_scalar_cast_item() {
    let select_item = serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [ agg_item("SUM", Some("id"), false) ],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    let declared_type = serde_json::json!({"type": "VARCHAR", "size": 100});
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under a function_scalar_cast item must widen",
    );
}

/// Scenario: A `function_scalar_case` with a nested aggregate in a THEN branch widens.
#[test]
fn project_columns_widens_on_aggregate_nested_in_function_scalar_case_item() {
    let select_item = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [ {
            "type": "predicate_less",
            "left": {"type": "column", "name": "ID"},
            "right": {"type": "literal_exactnumeric", "value": 10}
        } ],
        "results": [
            agg_item("COUNT", None, false),
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    let declared_type = serde_json::json!({"type": "DECIMAL", "precision": 20, "scale": 0});
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested in a CASE result branch must widen",
    );
}

/// Scenario: A nested aggregate under an arithmetic `function_scalar` widens.
#[test]
fn project_columns_widens_on_aggregate_nested_in_arithmetic_node() {
    let select_item = serde_json::json!({
        "type": "function_scalar",
        "name": "ADD",
        "arguments": [
            agg_item("SUM", Some("c_decimal_a"), false),
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    let declared_type = serde_json::json!({"type": "DECIMAL", "precision": 18, "scale": 2});
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under an arithmetic node must widen",
    );
}

/// Scenario: A nested aggregate under a predicate node widens.
#[test]
fn project_columns_widens_on_aggregate_nested_in_predicate_node() {
    let select_item = serde_json::json!({
        "type": "predicate_less",
        "left": agg_item("SUM", Some("id"), false),
        "right": {"type": "literal_exactnumeric", "value": 10}
    });
    let declared_type = serde_json::json!({"type": "boolean"});
    assert_widens_to_full_base_row(
        select_item,
        declared_type,
        "an aggregate nested under a predicate node must widen",
    );
}

/// Scenario: A scalar item with no nested aggregate does not widen.
#[test]
fn project_columns_does_not_widen_when_select_item_has_no_nested_aggregate() {
    let col_types = decimal_rewrite_col_types();
    let pushdown_req = serde_json::json!({
        "selectList": [ {
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                {"type": "column", "name": "C_DECIMAL_A"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        } ],
        "selectListDataTypes": [ {"type": "DECIMAL", "precision": 18, "scale": 2} ],
    });

    let (items, _types, widened) =
        project_columns(&pushdown_req, col_types).expect("must project (Ok)");

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
