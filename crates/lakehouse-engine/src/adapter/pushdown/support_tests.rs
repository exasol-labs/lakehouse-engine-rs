use super::super::test_support::*;
use super::*;
use crate::scan::spec::{AggKind, DeleteFileContentType, SortKey};
use vs_expression::render_df_filter_safe;

/// `walk_column_nodes` fires its callback exactly once per `column` node
/// wherever one is nested — inside a function's `arguments` array, a `CASE`'s
/// `results`, a comparison predicate's `left`/`right`, and even a `column`
/// node's own child object — and never for a non-`column` object, a scalar,
/// or an array node itself.
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

/// `walk_column_nodes` never invokes its callback for a non-container root —
/// `Json::Null`, a scalar string, or a scalar number fall through the `_ => {}`
/// arm untouched, and an empty object matches `Json::Object` but has no `type`
/// key and no values to recurse into, so it is a no-op too. Production hands
/// the primitive exactly such roots unguarded: `referenced_column_projection`
/// (`joins/sql_builders.rs`) and `referenced_side_columns` (`rendering.rs`)
/// pass `pushdown_req.get("groupBy")` / `get("orderBy")` / `get("selectList")`
/// straight through with no `is_null()` guard.
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

/// `strip_table_alias` removes every `tableAlias` key at any nesting depth
/// (issue #193) while preserving `tableName` and `name`, recursing through
/// both nested objects and arrays.
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

/// A predicate the DataFusion dialect can express answers `true`.
#[test]
fn datafusion_renderable_true_for_a_rendering_predicate() {
    let expr = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "AGE"},
        "right": {"type": "literal_exactnumeric", "value": 18}
    });

    assert!(datafusion_renderable(&expr));
}

/// `SECOND(ts, 3)` is a DataFusion field-shortcut arity refusal (exactly 1
/// argument permitted) — the dialect-asymmetric decline this plan's fix
/// exists to self-apply rather than silently omit.
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

/// A trivially-true `TRUE` literal answers `true`: `render_expression_safe`
/// does not suppress it, so omitting it from the scan spec is a correct
/// no-op, not a decline.
#[test]
fn datafusion_renderable_true_for_trivially_true_literal() {
    let expr = serde_json::json!({"type": "literal_bool", "value": true});

    assert!(datafusion_renderable(&expr));
}

/// `strip_table_alias` must not change the decline/accept answer:
/// `handle_pushdown` screens the un-stripped tree while the N-scan leg
/// renders the `tableAlias`-stripped one, and both must agree. Covers both
/// directions: a predicate that declines under both dialects (below), and
/// one that RENDERS under both (below) — the safety-critical direction,
/// since `build_side_fan_out_sql` strips the alias and re-renders AFTER
/// `renderable_only` screened the un-stripped tree, so a conjunct whose
/// answer flipped `true` -> `false` under stripping would be silently
/// dropped from the leg.
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

// ---------------------------------------------------------------------------
// Task 1.2 — adapter carries positional deletes into the per-shard scan spec
// ---------------------------------------------------------------------------

/// A minimal delete-carrying row-scan `ScanSpec` template (files replaced per
/// shard by the builder), used to assert what the per-shard/common arguments
/// carry.
fn delete_spec_template() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: "s3://warehouse/db/table".into(),
            projection: vec![ProjectionItem::Column("ID".into())],
            emit_exa_types: vec!["DECIMAL(20,0)".into()],
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    }
}

/// Positional deletes survive into the per-shard scan spec for BOTH
/// `write.delete.granularity=file` (one data file → its own delete file) and
/// `partition` (one delete file referenced by multiple data files).
#[test]
fn adapter_preserves_positional_deletes_into_scan_spec() {
    // file granularity: one data file carries its own positional-delete file.
    let file_gran = vec![FileEntry::with_deletes(
        "data/part-0.parquet",
        1000,
        vec![pos_delete("data/deletes/del-0.parquet", 50)],
    )];
    let back = ScanSpec::files_from_json(&shard_files_json(&file_gran)).unwrap();
    assert_eq!(back, file_gran, "file-granularity deletes must round-trip");
    assert_eq!(back[0].deletes.len(), 1);
    assert_eq!(
        back[0].deletes[0].content_type,
        DeleteFileContentType::PositionDeletes
    );

    // partition granularity: the SAME delete file is referenced by two data files.
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
    assert_eq!(back2[1].deletes[0].path, shared);
}

/// A delete-carrying entry serializes with its content type on the wire; a
/// delete-free entry stays the compact `[path, size]` 2-tuple (no wire bloat,
/// backward-compatible with pre-delete payloads).
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
    assert_eq!(
        back[0].deletes[0].content_type,
        DeleteFileContentType::PositionDeletes
    );

    let free = vec![FileEntry::new("data/part-0.parquet", 1000)];
    assert_eq!(
        shard_files_json(&free),
        r#"[["data/part-0.parquet",1000]]"#,
        "delete-free entry must stay the compact 2-tuple form"
    );
}

/// Delete refs ride ONLY in the per-shard files argument, never in the
/// shard-invariant common blob, and the common blob carries no serialized
/// Iceberg schema or bound predicate (the minimal-surface decision).
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
        None,
        &[],
        &[],
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

/// The shared fan-out primitive emits a nested `LAKEHOUSE_DISTRIBUTE_FILES`
/// distributor (`GROUP BY shard_key` over the per-shard file lists) wrapped by an
/// outer UNGROUPED scalar `LAKEHOUSE_SCAN('{common}', files)` select. The
/// shard-invariant common blob is spliced exactly ONCE (the outer scalar's first
/// argument); only the per-shard `files` strings flow through the distributor, so
/// the fan-out payload is data-volume-independent.
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
    // The common blob (which carries table_root) appears exactly once: in the
    // outer scalar's first argument, never repeated per shard in the distributor.
    assert_eq!(
        sql.matches("s3://warehouse/db/table").count(),
        1,
        "common blob must be spliced exactly once, not per shard: {sql}"
    );
}

/// A single-shard plan short-circuits the distributor entirely: a from-less scalar
/// `LAKEHOUSE_SCAN('{common}', '{files}')` call on literals (no distributor, no
/// inner `GROUP BY`, no `VALUES` driving relation).
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

// ---------------------------------------------------------------------------
// shard_count — cap/clamp boundary tests
// ---------------------------------------------------------------------------

/// Scenario: Shard count oversubscribes the cluster and is capped at 300.
/// 10 nodes × 50 factor = 500, capped to 300.
#[test]
fn shard_count_oversubscribes_and_caps_at_300() {
    // 10 × 50 = 500 > 300 files; cap at 300.
    assert_eq!(shard_count(10, 50, 500), 300, "must be capped at 300");
    // 10 × 50 = 500 but only 350 files — still capped at 300 (min(350, 300)=300).
    assert_eq!(
        shard_count(10, 50, 350),
        300,
        "must be capped at min(files,300)=300"
    );
    // Exact cap: 1 × 300 = 300, 1000 files — stays 300.
    assert_eq!(shard_count(1, 300, 1000), 300, "exactly 300 must stay 300");
    // 1 × 301 = 301 > 300; capped at 300.
    assert_eq!(shard_count(1, 301, 1000), 300, "301 must be capped at 300");
}

/// Scenario: Fewer files than G produces one shard per file with no empty shards.
/// node_count × parallelism_factor > file_count => clamp to file_count.
#[test]
fn shard_count_clamped_to_file_count_no_empty_shards() {
    // 10 × 8 = 80 but only 3 files; clamp to 3.
    assert_eq!(shard_count(10, 8, 3), 3, "must clamp to file_count=3");
    // 4 × 8 = 32 but only 5 files; clamp to 5.
    assert_eq!(shard_count(4, 8, 5), 5, "must clamp to file_count=5");
    // 1 × 1 = 1, file_count=1; stays 1.
    assert_eq!(shard_count(1, 1, 1), 1, "single file single shard");
    // Minimum clamp: 0 × 8 = 0, clamp to min(1, …) = 1.
    assert_eq!(shard_count(0, 8, 100), 1, "zero product must clamp to 1");
    // parallelism_factor=0: 5 × 0 = 0, clamp to 1.
    assert_eq!(shard_count(5, 0, 100), 1, "zero factor must clamp to 1");
}

/// Pushdown carries the table root ONCE in the common blob and per-shard file
/// sizes travel into the shard payloads (verification scenario, CHANGED).
#[test]
fn pushdown_carries_table_root_and_sizes_in_common_and_shards() {
    let root = "s3://warehouse/db/events";
    let files = vec![
        (format!("{root}/part-00000.parquet"), 1024u64),
        (format!("{root}/part-00001.parquet"), 2048u64),
    ];
    // Two nodes → two shards (one file each) so a genuine fan-out is emitted.
    let sql = build_row_sql_with_root(
        files,
        root,
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
        2,
    );

    // The table root is carried in the shard-invariant common blob.
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(&format!(r#""table_root":"{root}""#)),
        "common blob must carry table_root once: {common}"
    );

    // Each per-shard payload carries its file's byte size as a [path,size] tuple.
    assert!(
        sql.contains(r#"[["part-00000.parquet",1024]]"#),
        "shard payload must carry relative path + size for file 0: {sql}"
    );
    assert!(
        sql.contains(r#"[["part-00001.parquet",2048]]"#),
        "shard payload must carry relative path + size for file 1: {sql}"
    );
}

/// The table root is stripped from every under-root path and appears EXACTLY
/// ONCE (in the common literal), NEVER in a per-shard VALUES literal (NEW).
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

    // The root string occurs exactly once in the whole statement: in the common
    // blob's table_root. Stripped relative paths never repeat the prefix.
    assert_eq!(
        sql.matches(root).count(),
        1,
        "table root must appear exactly once (common blob only), never per shard: {sql}"
    );
    // That single occurrence lives in the common literal.
    assert!(
        common_arg_literal(&sql).contains(root),
        "the sole table-root occurrence must be in the common blob: {sql}"
    );
    // The per-shard VALUES section (everything after the common literal) carries
    // only relative paths.
    assert!(
        sql.contains("part-00000.parquet") && sql.contains("part-00001.parquet"),
        "shards must carry the relative file names: {sql}"
    );
}

/// A data-file path NOT under the table root is carried as a full absolute URI
/// (NEW).
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

    // The under-root file is emitted relative.
    assert!(
        sql.contains(r#"["part-00000.parquet",1024]"#),
        "under-root path must be relativized: {sql}"
    );
    // The not-under-root file keeps its full absolute URI, with its size.
    assert!(
        sql.contains(&format!(r#"["{outside}",2048]"#)),
        "path outside the table root must stay absolute: {sql}"
    );
    // The table root is still carried exactly once (the absolute outside path
    // does not contain the root prefix).
    assert_eq!(
        sql.matches(root).count(),
        1,
        "table root must appear exactly once even with an out-of-root file: {sql}"
    );
}

/// Multi-shard fan-out carries the root once in the common literal and each
/// per-shard literal is a `[[path,size],...]` tuple array (CHANGED).
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

    // Fan-out shape: GROUP BY shard_key over a VALUES table, never IPROC().
    assert!(
        !sql.contains("IPROC()"),
        "fan-out must not use IPROC(): {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key") && sql.contains("AS shards(shard_key, files)"),
        "fan-out must GROUP BY shard_key over the VALUES table: {sql}"
    );

    // Root carried once (common blob), not repeated per shard.
    assert_eq!(
        sql.matches(root).count(),
        1,
        "root must be serialized once in the common blob: {sql}"
    );

    // Each per-shard files literal is a JSON array of [path,size] 2-tuples.
    assert!(
        sql.contains(r#"[["part-00000.parquet",1024]]"#)
            && sql.contains(r#"[["part-00001.parquet",2048]]"#),
        "each shard literal must be a [[path,size],...] tuple array: {sql}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: Pushdown resolves the file list once and builds a scan-driving query
// ---------------------------------------------------------------------------

/// Pure SQL-building part of the pushdown scenario.
/// The file list comes from a fixture (no catalog I/O).
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

    // The generated SQL must invoke the scan UDF with the spec embedded.
    assert!(
        sql.contains(SCAN_UDF_NAME),
        "SQL must reference the scan UDF: {sql}"
    );
    // The spec JSON (embedded as a SQL literal) contains the file path.
    assert!(
        sql.contains("part-00000.parquet"),
        "SQL must carry assigned files: {sql}"
    );
    assert!(
        sql.contains("part-00001.parquet"),
        "SQL must carry both files: {sql}"
    );
    // Must be the outer ungrouped scalar scan itself (no SELECT * wrapper).
    assert!(
        sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")) && !sql.contains("SELECT * FROM ("),
        "must be a real scalar scan-driving query, no materializing wrapper: {sql}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: Projection is pushed into the scan-driving query
// ---------------------------------------------------------------------------

#[test]
fn projection_carried_in_common_literal_and_emits() {
    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["A".into(), "B".into()],
        vec!["DECIMAL(10,0)".into(), "VARCHAR(2000000)".into()],
        None,
        None,
    );

    // EMITS clause must list exactly the projected columns in order.
    assert!(
        sql.contains("\"A\" DECIMAL(10,0)"),
        "EMITS must carry col A: {sql}"
    );
    assert!(
        sql.contains("\"B\" VARCHAR(2000000)"),
        "EMITS must carry col B: {sql}"
    );

    // The projection lives in the common (arg 0) blob, not the per-shard files arg.
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""projection":["A","B"]"#),
        "common arg must carry the projection in order: {common}"
    );
    // The per-shard files arg must not carry projection metadata.
    assert!(
        !sql.contains(r#""files""#),
        "no ScanSpec files key must appear (files travel as a bare JSON array): {sql}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: Filter predicate is pushed into the scan spec (translatable) or
// kept out of it (untranslatable) — never mistranslated, never omitted from
// the query.
// ---------------------------------------------------------------------------

#[test]
fn pushdown_translates_or_omits_predicate() {
    // Translatable predicate: column > literal.
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

    // Untranslatable predicate (e.g., an aggregate or unknown function):
    // render_df_filter_safe returning None keeps it out of the SCAN SPEC
    // only — the adapter must still self-apply it elsewhere (see
    // declined_filter_routes_every_dispatch_shape_to_qualified_wrapper).
    let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
    let omitted = render_df_filter_safe(&untranslatable);
    assert!(
        omitted.is_none(),
        "untranslatable predicate must be omitted (None), not mistranslated"
    );

    // Confirm the scan SQL stays valid without a scan-spec filter — the
    // adapter applies the predicate itself elsewhere rather than relying
    // on any re-check by Exasol (see
    // declined_filter_routes_every_dispatch_shape_to_qualified_wrapper).
    let sql_no_filter = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["AGE".into()],
        vec!["DECIMAL(20,0)".into()],
        None, // omitted
        None,
    );
    assert!(
        sql_no_filter.contains(SCAN_UDF_NAME),
        "SQL must still be valid when filter is omitted"
    );

    // Confirm carrying the filter includes it in the spec JSON.
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

// ---------------------------------------------------------------------------
// Scenario: LIMIT is pushed into the scan spec; also appears at Exasol level.
// ---------------------------------------------------------------------------

#[test]
fn row_scan_limit_in_common_arg() {
    let sql = build_sql_for_fixture(
        vec!["s3://warehouse/f.parquet".into()],
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
        None,
        Some(42),
    );

    // The outer SQL must contain LIMIT (Exasol-level backstop).
    assert!(
        sql.contains("LIMIT 42"),
        "outer SQL must carry LIMIT for correctness backstop: {sql}"
    );

    // For a row scan the LIMIT is retained in the common (arg 0) blob.
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""limit":42"#),
        "row-scan common arg must carry limit=42: {common}"
    );
}

// ---------------------------------------------------------------------------
// Pre-existing helpers tests (unchanged)
// ---------------------------------------------------------------------------

#[test]
fn limit_extracted_from_pushdown_request() {
    let req = serde_json::json!({"numElements": 42});
    assert_eq!(extract_limit(&req), None); // not nested under "limit"

    let req2 = serde_json::json!({"limit": {"numElements": 42}});
    assert_eq!(extract_limit(&req2), Some(42));
}

/// `extract_offset` is a sibling accessor of `extract_limit`: 0 when the
/// `offset` key is absent (the shape Exasol sends for a bare `LIMIT n` and,
/// verified live, also for an explicit `OFFSET 0`), the value otherwise.
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

/// The one rendering seam every reachable wrapper SELECT routes through.
/// The `offset == 0` arm MUST stay byte-identical to the pre-change
/// ` LIMIT {n}` splice — every existing SQL-shape assertion depends on it.
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

// ---------------------------------------------------------------------------
// extract_projection — row-scan fallback must be duplicate-free
// ---------------------------------------------------------------------------

/// A select list mixing an untranslatable scalar and COUNT(*) must NOT emit
/// repeated `first_col_name` columns (which Exasol rejects as duplicate EMITS).
/// It falls back to the full, deduplicated base-table column set.
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
    // Untranslatable scalar (unknown function) + COUNT(*) aggregate — both items
    // would otherwise hit the first-column fallback arms.
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

// ---------------------------------------------------------------------------
// detect_aggregates — plan translation + fallback
// ---------------------------------------------------------------------------

// -----------------------------------------------------------------------
// Expression-argument aggregates (Task 2.1 / 2.3)
// -----------------------------------------------------------------------

/// An aggregate select-list translates to a ScanSpec carrying
/// the aggregate plan (kind+column) plus any pushed-down filter.
#[test]
fn aggregate_query_builds_partial_agg_spec() {
    // Build a spec_template as handle_pushdown would.
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["AMOUNT".into()],
            filter: Some("(\"REGION\" = 'EU')".into()),
            aggregates: Some(vec![
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
            ]),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };

    // Build single-shard SQL and decode the embedded spec literal.
    let shards = vec![vec![("s3://warehouse/f.parquet".to_string(), 1u64)]];
    let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &["AMOUNT".into()],
        &["DOUBLE PRECISION".to_string()],
        None,
        None,
        &col_types,
        &[],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );

    // The spec JSON is embedded in the SQL literal; extract and parse it.
    // It lives between the first `'` and the matching unescaped `'` after the JSON.
    // Simpler: deserialize directly from the template (which is what gets embedded).
    let spec_json = {
        // Reconstruct the shard spec as the builder would.
        let mut s = spec_template.clone();
        s.files = vec![FileEntry::new("s3://warehouse/f.parquet", 1)];
        s.to_json()
    };
    let parsed = ScanSpec::from_json(&spec_json).expect("spec must parse");

    // The aggregate plan must be present with the right kinds and columns.
    let plans = parsed
        .common
        .aggregates
        .expect("aggregates must be in the spec");
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
    assert_eq!(plans[1].kind, AggKind::Count);
    assert!(plans[1].column.is_none());

    // The filter must also be present.
    assert!(
        parsed.common.filter.is_some(),
        "filter must be carried in aggregate spec"
    );

    // The SQL must reference the UDF.
    assert!(sql.contains(SCAN_UDF_NAME));
}

// ---------------------------------------------------------------------------
// Fan-out SQL shape — multi-shard GROUP BY shard_key, single-shard equivalence
// ---------------------------------------------------------------------------

/// Multi-shard fan-out serializes the shard-INVARIANT common blob EXACTLY ONCE
/// (as the UDF's first argument literal) and carries only the per-shard files
/// list in each `VALUES` row — no credential/tuning payload repeats per shard.
#[test]
fn fan_out_serializes_common_once_files_per_shard() {
    let files = vec![
        "s3://warehouse/shard0/part-000.parquet".into(),
        "s3://warehouse/shard1/part-001.parquet".into(),
        "s3://warehouse/shard2/part-002.parquet".into(),
    ];
    // cluster_nodes=3 forces 3 shards (one file each).
    let sql = build_sql_for_fixture_n(
        files,
        vec!["ID".into()],
        vec!["DECIMAL(20,0)".into()],
        None,
        None,
        3,
    );

    // Must use shard_key GROUP BY for the fan-out, NOT IPROC().
    assert!(
        !sql.contains("IPROC()"),
        "multi-shard SQL must NOT contain IPROC(): {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key"),
        "multi-shard SQL must GROUP BY shard_key: {sql}"
    );

    // The VALUES table exposes the per-shard files column (arg 1), not a full spec.
    assert!(
        sql.contains("AS shards(shard_key, files)"),
        "fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
    );
    // The UDF is called with two args: the common literal, then the files column.
    assert!(
        sql.contains(&format!("{SCAN_UDF_NAME}(")),
        "multi-shard SQL must invoke the scan UDF: {sql}"
    );
    assert!(
        sql.contains(", files) EMITS ("),
        "UDF must take the per-shard files column as its second argument: {sql}"
    );

    // The shard-invariant common blob must appear EXACTLY ONCE. The storage
    // endpoint and the tuning knobs live only in the common blob, so counting
    // them proves the credential/tuning payload is not repeated per shard.
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

    // Each shard's file appears EXACTLY ONCE, in its own VALUES row.
    for file in ["part-000.parquet", "part-001.parquet", "part-002.parquet"] {
        assert_eq!(
            sql.matches(file).count(),
            1,
            "file {file} must appear exactly once (in one VALUES row): {sql}"
        );
    }

    // Exactly 3 VALUES entries (one files literal per shard).
    let values_start = sql.find("VALUES").expect("must have VALUES");
    let group_by_start = sql.find("GROUP BY").expect("must have GROUP BY");
    let values_section = &sql[values_start..group_by_start];
    let entry_count = values_section.matches("),(").count() + 1;
    assert_eq!(
        entry_count, 3,
        "must have 3 VALUES entries for 3 shards: {values_section}"
    );
}

/// The connection-concurrency budget (`s3_max_connections`) is a shard-INVARIANT
/// tuning field — like `df_threads_per_udf` and `memory_pool_fraction` — so it must
/// travel in the common blob (the UDF's first argument), serialized exactly once,
/// never duplicated per shard and never silently dropped from the fan-out SQL.
#[test]
fn common_spec_carries_s3_max_connections_exactly_once() {
    let files = vec![
        "s3://warehouse/shard0/part-000.parquet".into(),
        "s3://warehouse/shard1/part-001.parquet".into(),
        "s3://warehouse/shard2/part-002.parquet".into(),
    ];
    // A distinctive, non-default value so it cannot be confused with the
    // built-in default (8) or any other numeric field in the spec.
    let distinctive_s3_max_connections = 37;
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: sample_storage(),
            s3_max_connections: distinctive_s3_max_connections,
            ..Default::default()
        },
        files: vec![],
    };

    // Confirm the value round-trips through the shard-invariant common split
    // that `handle_pushdown` uses to build the fan-out (`ScanSpec::to_common`).
    let common = spec_template.to_common();
    assert_eq!(
        common.s3_max_connections, distinctive_s3_max_connections,
        "s3_max_connections must carry from ScanSpec into CommonScanSpec"
    );

    // cluster_nodes=3 forces 3 shards (one file each) — the same multi-shard
    // fan-out shape `handle_pushdown` builds via `build_scan_driving_sql`.
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
        None,
        &[],
        &[],
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

// ---------------------------------------------------------------------------
// Aggregate merge wrapper SQL — outer SELECT reconstructing partial results
// ---------------------------------------------------------------------------

/// Helper: build aggregate scan SQL from a set of aggregate plans.
/// Uses an empty col_types map — aggregate columns default to DOUBLE PRECISION
/// (correct for existing tests that use SCORE/AMOUNT as DOUBLE).
fn build_agg_sql(
    agg_plans: Vec<AggregatePlan>,
    files: Vec<String>,
    cluster_nodes: usize,
) -> String {
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(agg_plans),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards =
        crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
    build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        None,
        &[],
        &[],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
}

/// The aggregate merge SELECT renders `LIMIT n` on the outer wrapper when
/// `request_limit` is `Some(n)` — the render site issue #198 needs so a pushed
/// `LIMIT 0` over a one-row aggregate merge returns zero rows instead of being
/// silently dropped (no limit value reachable inside the aggregate sub-path
/// carried the request's raw limit before this parameter existed).
#[test]
fn aggregate_merge_renders_request_limit_when_some() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(plans),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        Some(0),
        &[],
        &[],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        sql.ends_with("LIMIT 0"),
        "aggregate merge must render the request LIMIT: {sql}"
    );
}

/// Aggregate wrapper merges partials: outer SELECT aggregates per-shard COUNT/SUM/MIN/MAX.
/// Given COUNT/SUM/MIN/MAX aggregate plan: wrapper contains fan-out AND outer
/// SUM/MIN/MAX over the partial columns in the right order.
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

    // Multi-shard: use 2 shards to exercise the fan-out + merge wrapper.
    let files = vec![
        "s3://warehouse/f0.parquet".into(),
        "s3://warehouse/f1.parquet".into(),
    ];
    let sql = build_agg_sql(plans, files, 2);

    // Must contain the shard_key fan-out (NOT IPROC).
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

    // Must wrap with outer merge aggregation.
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

    // Must contain partial column names in the EMITS and in the merge.
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

    // The EMITS clause must declare the partial columns.
    assert!(
        sql.contains("EMITS"),
        "aggregate SQL must have EMITS: {sql}"
    );

    // The outer merge must not be SELECT *.
    assert!(
        !sql.contains("SELECT *"),
        "aggregate wrapper must not use SELECT *: {sql}"
    );
}

/// The outer single-group merge SELECT sits DIRECTLY over the scalar scan — no
/// `SELECT * FROM (...)` between the merge and the scan (decision [5]). The scalar
/// scan fires once per shard (the distributor emits one row per shard), so one
/// partial-agg row per shard is produced and the outer SUM/MIN/MAX merge over
/// those partials equals the single-node aggregate (result-equivalence, [7]).
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
    // Multi-shard: a genuine distributor fan-out under the merge.
    let sql = build_agg_sql(
        plans,
        vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
        2,
    );

    assert!(
        !sql.contains("SELECT * FROM ("),
        "no materializing wrapper between merge and scan: {sql}"
    );
    // The merge is the outer SELECT; the scalar scan is the subquery it reads.
    assert!(
        sql.starts_with("SELECT ") && sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
        "the outer merge SELECT must read directly from the scalar scan subquery: {sql}"
    );
    // The `GROUP BY shard_key` fan-out lives in the distributor, not the outer merge.
    assert!(
        sql.contains("GROUP BY shard_key"),
        "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
    );
}

/// Single-shard aggregate: the merge SELECT sits directly over a from-less scalar
/// scan on literals — no distributor, no `SELECT * FROM (...)` wrapper.
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

/// Single-group merge casts each aggregate to its Exasol-declared result type.
/// `SELECT COUNT(score)` merges as `SUM("PARTIAL_count_0")` (DECIMAL(31,0)); Exasol
/// declared DECIMAL(18,0) for the column and strictly validates the adapter's output
/// type, so the merge item must be `CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))`.
#[test]
fn single_group_merge_casts_to_declared_type() {
    let plans = vec![AggregatePlan {
        kind: AggKind::CountCol,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(plans.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let col_types = vec![("SCORE".to_string(), "DECIMAL(18,0)".to_string())];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        None,
        &col_types,
        &aggregate_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        sql.contains(r#"CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))"#),
        "single-group merge must cast COUNT to declared DECIMAL(18,0): {sql}"
    );
}

/// Single-group merge with no declared types emits the bare uncast merge expression.
#[test]
fn single_group_merge_uncast_without_declared_types() {
    let plans = vec![AggregatePlan {
        kind: AggKind::CountCol,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(plans.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &[],
        &[],
        None,
        None,
        &[],
        &[],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    assert!(
        sql.contains(r#"SUM("PARTIAL_count_0")"#) && !sql.contains("CAST(SUM"),
        "single-group merge without declared types must be uncast: {sql}"
    );
}

/// AVG wrapper divides merged sum by count with NULLIF(cnt, 0) guard.
/// Given AVG plan: wrapper computes SUM(partial_avg_sum) / NULLIF(SUM(partial_avg_cnt),0).
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

    // Must contain NULLIF guard for zero-count protection.
    assert!(
        sql.contains("NULLIF"),
        "AVG wrapper must contain NULLIF zero-guard: {sql}"
    );

    // Must divide: the / operator must appear in the outer merge context.
    assert!(
        sql.contains(" / "),
        "AVG wrapper must divide sum by count: {sql}"
    );

    // Must reference the AVG sum and count partial columns.
    assert!(
        sql.contains("PARTIAL_avg_sum_0"),
        "must reference partial avg sum: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_avg_cnt_0"),
        "must reference partial avg count: {sql}"
    );

    // Must use SUM() for both the sum and count parts.
    let sum_count = sql.matches("SUM(").count();
    assert!(
        sum_count >= 2,
        "AVG wrapper must SUM both partial_avg_sum and partial_avg_cnt: {sql}"
    );

    // Must contain NULLIF(..., 0).
    assert!(
        sql.contains("NULLIF(") && sql.contains(", 0)"),
        "AVG wrapper NULLIF guard must guard against zero: {sql}"
    );
}

/// Single-shard aggregate path produces a correct merge wrapper.
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

    // Even single-shard aggregate must have an outer merge.
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

// ---------------------------------------------------------------------------
// Single-group COUNT(DISTINCT) — DISTINCT row-scan fan-out wrapper SQL
// (replaces the removed LISTAGG/merge-UDF SQL shape; Tasks 5.3 / 5.6)
// ---------------------------------------------------------------------------

/// A `base_spec` matching the real caller contract (`handle_pushdown`): no
/// projection/aggregates/limit/order-by, `distinct` false. Only `files` varies
/// per shard; `build_count_distinct_scan_sql` derives each per-distinct fan-out
/// (and any shared ordinary partial-aggregate scan) from this template.
fn count_distinct_base_spec() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    }
}

/// Scenario: Case 1 (single-group `COUNT(DISTINCT col)`, nothing else) wraps
/// its DISTINCT row-scan fan-out in a plain, native `COUNT(DISTINCT "V")` —
/// replacing the removed `'[' || LISTAGG(...) || ']'` merge-UDF SQL shape.
/// Both UDF invocations (scan + distributor) are schema-qualified from the
/// names passed in; there is no third (merge) UDF name to qualify anymore.
#[test]
fn count_distinct_wrapper_uses_native_count_distinct() {
    let base_spec = count_distinct_base_spec();
    let items = vec![SingleGroupItem::Distinct(DistinctCount {
        column: Some("L_SHIPMODE".into()),
        arg_expr: None,
    })];
    let col_types = vec![("L_SHIPMODE".to_string(), "VARCHAR(25)".to_string())];
    // Two shards → a genuine fan-out, not the single-shard short-circuit.
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

// The former `count_distinct_expression_arg_declares_varchar_value_type` test was
// removed with the VARCHAR fan-out arm it exercised: a lone `COUNT(DISTINCT <expr>)`
// no longer fans out at all (`is_lone_count_distinct` now requires a bare-column
// argument). An expression-argument distinct declines to the qualified single-table
// wrapper, where Exasol evaluates the expression and DISTINCT natively over
// exact-typed base columns — covered by
// `single_group_agg::lone_expression_count_distinct_declines_fan_out_to_wrapper`.

// The former `multiple_count_distinct_columns_get_independent_fan_outs` (Case 2
// asserting the `FROM DUAL` scalar-subquery shape) was removed with the Case 2/3
// fan-out composition: only the lone-distinct Case 1 shape reaches
// `build_count_distinct_scan_sql`. Case 2/3 now DECLINES the fan-out and routes to
// the qualified single-table wrapper — asserted below.

/// Scenario (task 6.5): a Case 2/3 select list (more than one `COUNT(DISTINCT)`,
/// or a distinct mixed with an ordinary aggregate) is NOT dispatched to the
/// distinct fan-out (`is_lone_count_distinct` is false) and instead routes to the
/// shared qualified single-table wrapper. The wrapper renders every aggregate —
/// each `COUNT(DISTINCT)` spliced VERBATIM — over a materialized raw scan narrowed
/// to only the referenced columns (issue #160) and aliased `"LHS_T0"`. It is NOT a
/// distinct fan-out (`COUNT(DISTINCT "V")`), NOT a bare row scan (`SELECT * FROM`),
/// NOT a per-distinct SELECT-list scalar subquery (`(SELECT COUNT(DISTINCT "V")` —
/// the blocked design, `sqlCode 04000` "emitting function in expression"), and NOT
/// the removed `LISTAGG`/merge-UDF shape.
#[test]
fn multi_count_distinct_declines_to_qualified_wrapper() {
    use super::super::joins::{
        build_qualified_single_table_fallback_sql, referenced_column_projection,
    };
    use super::super::single_group_agg::{has_distinct, is_lone_count_distinct};

    // A column node carrying `tableName` so the wrapper alias-qualifies it.
    let cdist = |col: &str| {
        serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "distinct": true,
            "arguments": [{"type": "column", "name": col, "tableName": "T"}],
        })
    };
    // Case 2: two independent `COUNT(DISTINCT ...)` columns.
    let pushdown_req = serde_json::json!({
        "selectList": [cdist("CATEGORY"), cdist("REGION")],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    });

    // Dispatch: a Case 2 shape is a distinct request that is NOT a lone distinct,
    // so the `mod.rs` branch declines the fan-out and takes the wrapper guard.
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

    // Build the wrapper exactly as the `mod.rs` Case 2/3 guard does: narrow the
    // inner scan to only the referenced columns, then render the aggregates over it.
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
            emit_exa_types: proj_types,
            ..base.common
        },
        files: base.files,
    };
    let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
    let sql = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
        &fan_out_spec,
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

/// Scenario (plan-review finding): LIMIT/OFFSET/ORDER BY must never leak into
/// a distinct fan-out — the fan-out builder unconditionally strips them from
/// `base_spec` regardless of what a (possibly non-conforming) caller passes,
/// so a leaked per-shard LIMIT can never truncate a shard's local distinct set
/// into a wrong count. Covers Case 1 (a lone single-group `COUNT(DISTINCT)`), the
/// only shape that fans out — Case 2/3 declines to the qualified single-table
/// wrapper. The request-level `limit` argument (the outer `LIMIT` on
/// `SELECT COUNT(DISTINCT c) FROM t LIMIT 1`) lands ONLY on the outer wrapper.
#[test]
fn count_distinct_fan_out_omits_limit_offset_order_by() {
    // A deliberately non-conforming base_spec: real callers (`handle_pushdown`)
    // always pass `limit: None, order_by: []`, but the fan-out builder must
    // strip these unconditionally rather than relying on the caller's contract.
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

    // Case 1: a single distinct count — the only shape that fans out. (The Case 2
    // and Case 3 arms were removed with the Case 2/3 fan-out composition: those
    // shapes now decline to the qualified single-table wrapper, whose own
    // limit/order-by behavior is covered by task 6.5.)
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

// ---------------------------------------------------------------------------
// R.1: EMITS type correctness for SUM/MIN/MAX
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FIX 1: grouped aggregate with invalid agg column type falls back
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// R.2: multi-shard row-scan must append outer LIMIT
// ---------------------------------------------------------------------------

/// R.2: multi-shard row scan with LIMIT must append LIMIT to the outer SQL.
#[test]
fn multi_shard_row_scan_appends_outer_limit() {
    let files = vec![
        "s3://warehouse/f0.parquet".into(),
        "s3://warehouse/f1.parquet".into(),
    ];
    // cluster_nodes=2 forces 2 shards.
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

// ---------------------------------------------------------------------------
// Row scan — outer ungrouped scalar scan, no SELECT * materialization wrapper
// (decision [5]); ORDER BY/LIMIT attach directly to the outer scalar select.
// ---------------------------------------------------------------------------

/// A multi-shard row scan drives an OUTER UNGROUPED scalar `LAKEHOUSE_SCAN` over
/// the nested distributor — with NO `SELECT * FROM (...)` materialization wrapper
/// (decision [5]). The scan itself is the top-level SELECT; the distributor
/// subquery does the `GROUP BY shard_key` fan-out. Result-equivalence (decision
/// [7]): the returned rows are the union of every shard's rows (no outer GROUP BY,
/// so no dedup/aggregation).
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

/// LIMIT attaches DIRECTLY to the outer ungrouped scalar select (after the
/// distributor subquery closes), not to a `SELECT * FROM (...)` wrapper
/// (decision [5]).
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
    // The LIMIT must sit OUTSIDE the distributor subquery — after its closing paren.
    let limit_pos = sql.rfind("LIMIT 7").expect("LIMIT present");
    let close_pos = sql[..limit_pos]
        .rfind(')')
        .expect("distributor subquery closes");
    assert!(
        close_pos < limit_pos,
        "LIMIT must follow the distributor subquery's closing paren: {sql}"
    );
}

/// Single-shard SQL uses the two-argument form `{udf}('<common>', '<files>')`:
/// the common blob and the whole-file-list literal each appear exactly once. The
/// scalar scan is a from-less call on literals with no fan-out markers and no
/// `SELECT * FROM (...)` materialization wrapper (decision [5]).
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
        1, // single node
    );

    // Must NOT contain multi-shard markers.
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

    // Must be the from-less scalar scan itself (no SELECT * materialization
    // wrapper) and invoke the scan UDF.
    assert!(
        sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")) && !sql.contains("SELECT * FROM ("),
        "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
    );
    assert!(sql.contains("EMITS"), "must have EMITS clause: {sql}");
    assert!(
        sql.contains(SCAN_UDF_NAME),
        "must invoke the scan UDF: {sql}"
    );

    // The common blob is serialized ONCE (endpoint + tuning knob appear once each).
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

    // Both files are carried once, together in the single files-list literal
    // (arg 1), which is a JSON array — not repeated across per-shard rows.
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

// ---------------------------------------------------------------------------
// detect_group_by_aggregates — GROUP BY key extraction and aggregate detection
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// detect_group_by_aggregates — select-list order preservation (fix-grouped-agg-select-order)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// partition_files_by_bytes — G shards balanced, disjoint, full coverage
// ---------------------------------------------------------------------------

/// File list partitioned into G shards via shard_count is balanced, disjoint,
/// and covers every file with no empty shards.
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
    // No shard is empty.
    for (i, shard) in shards.iter().enumerate() {
        assert!(!shard.is_empty(), "shard {i} must not be empty");
    }
    // All files covered exactly once (compare by path; sizes travel alongside).
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

// ---------------------------------------------------------------------------
// Row-scan SQL shape — GROUP BY shard_key fan-out, single-shard collapse
// ---------------------------------------------------------------------------

/// Multi-shard row-scan SQL uses GROUP BY shard_key, never IPROC().
#[test]
fn scan_driving_sql_groups_by_shard_key_not_iproc() {
    let files: Vec<(String, u64)> = (0..3)
        .map(|i| (format!("s3://warehouse/f{i}.parquet"), (i as u64 + 1) * 100))
        .collect();
    let g = shard_count(3, 1, files.len());
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: sample_storage(),
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
        None,
        &[],
        &[],
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

/// Single-shard collapses to the single-invocation form (no VALUES, no GROUP BY).
#[test]
fn single_shard_collapses_to_single_invocation() {
    let files = vec![("s3://warehouse/f0.parquet".to_string(), 500u64)];
    let g = shard_count(1, 1, files.len());
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into()],
            storage: sample_storage(),
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
        None,
        &[],
        &[],
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

// ---------------------------------------------------------------------------
// Non-decomposable aggregate fallback to row scan
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// STDDEV / VARIANCE decomposition into sufficient statistics
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// STDDEV/VARIANCE NULL-passthrough — N=0 (pop & samp) and N=1 (samp)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// HAVING must not be silently dropped on grouped-path type-validation failure
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Select-list scalar expression pushdown
// ---------------------------------------------------------------------------

/// A function_scalar in the select list renders to a SQL expression in the
/// scan spec projection and EMITS clause.
#[test]
fn selectlist_scalar_expression_rendered_in_emits() {
    // Simulate a pushdown request with UPPER(name) in the select list.
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
    // The rendered expression should be carried as an Expr projection item, NOT
    // a bare Column — so the scan splices it verbatim instead of quoting it as a
    // phantom identifier.
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
    // Type for an expression falls back to VARCHAR(2000000)
    assert_eq!(proj_types[0], "VARCHAR(2000000)");
}

/// A `function_scalar_cast` in the select list (the real Exasol wire node
/// type for CAST — distinct from the generic `function_scalar`) renders as
/// a `ProjectionItem::Expr`, not the full-base-row fallback (issue #136).
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

/// A `function_scalar_extract` in the select list (the real Exasol wire node
/// type for EXTRACT) renders as a `ProjectionItem::Expr`, not the
/// full-base-row fallback (issue #136).
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

/// A `function_scalar_case` in the select list (the real Exasol wire node
/// type for CASE) renders as a `ProjectionItem::Expr`, not the
/// full-base-row fallback (issue #136).
#[test]
fn selectlist_case_node_rendered_in_emits() {
    // Searched CASE (no `basis`): WHEN arguments are boolean predicates.
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

/// A CAST to an unsupported target type (e.g. TIMESTAMP WITH LOCAL TIME
/// ZONE, which `render_cast_target` deliberately declines — see
/// `crates/vs-expression/src/lib.rs`) still falls back to the full base
/// row: the `None` untranslatable branch is untouched by the #136 fix.
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
    // Full base row fallback: both table columns, as bare Column items —
    // not the single rendered expression.
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

/// A single projected literal renders to ONE positional `Expr` projection
/// item — not the full-base-row fallback (issue #190).
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

/// `SELECT 1, name, 1` yields three positional projection items — the two `1`
/// literals are NOT collapsed by value-based dedup (issue #190) — and
/// `emits_ident` assigns each a distinct EMITS identifier: the real quoted
/// column name for the `column` item, positional synthetic names for the two
/// `Expr` items.
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

/// A bare literal select-list item projects exactly one column — proving the
/// full-row fallback is NOT taken for a plain literal (issue #190), even when
/// the table has more than one column available to fall back to.
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

/// A literal declared `TIMESTAMP WITH LOCAL TIME ZONE`, under the pre-existing
/// synthetic node name `literal_timestamp_utc` (which DOES render via
/// `render_expression_safe`, unlike the real wire name below), widens to the
/// full base row because Exasol rejects that declared type as a UDF EMITS
/// output (sqlCode 22002) — this isolates the EMITS-type-gate reason from
/// the "translator declined" reason the next test covers, mirroring
/// `selectlist_untranslatable_cast_falls_back_to_full_row`. The widened
/// projection routes to the qualified wrapper (`mod.rs`'s
/// `RequestShape::RowScan` arm), not literally "falls back to the full
/// base row" as an end state — `#218`.
#[test]
fn selectlist_tstz_literal_widens_via_emits_type_gate() {
    let literal = serde_json::json!({
        "type": "literal_timestamp_utc",
        "value": "2024-03-01 10:00:00"
    });
    // Confirm the literal actually renders — the widening must be due to the
    // EMITS-type gate, not a render failure.
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

/// The REAL wire name `literal_timestamputc` (#242) also widens the
/// projection, so it reaches the same qualified-wrapper routing as the
/// synthetic name above — this is the mechanism actual live traffic
/// exercises. The reason differs (the DataFusion dialect deliberately
/// declines this wire name, so `render_expression_safe` returns `None` and
/// `project_columns` widens via its "translator declined" arm, not the
/// EMITS-type gate) but the observable routing outcome must be identical.
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

/// A plain `TIMESTAMP` (NOT with-local-time-zone) literal IS rendered as a
/// positional `Expr`, never declined — locking the exact-match boundary in
/// `is_valid_emits_output_type` so `TIMESTAMP` is never treated as a prefix of
/// `TIMESTAMP WITH LOCAL TIME ZONE`.
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

/// A `CAST(<col> AS CHAR(20))` select-list item (`function_scalar_cast`) must
/// project with a `CHAR(20)` EMITS type — through the real `project_columns`
/// entry point (`extract_projection`), not the bare `exasol_type_from_json`
/// function. The declared type comes from `selectListDataTypes`, not from the
/// item's own rendered CAST target (which stays a bare, length-less `VARCHAR`
/// on the DataFusion dialect — a separate, unaffected non-goal of this fix).
/// The item must stay a rendered expression, never falling back to the full
/// base row (issue #192, facet B).
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

/// An untranslatable select-list item falls back to the bare column.
#[test]
fn selectlist_untranslatable_item_falls_back_to_column() {
    // A node type the translator cannot handle
    let bad_expr = serde_json::json!({
        "type": "function_aggregate",  // aggregate in select list -> untranslatable as scalar expr
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
    // Fall back to the first column name
    assert_eq!(proj_cols.len(), 1);
    assert_eq!(proj_cols[0], "AMOUNT");
    assert_eq!(proj_types[0], "DECIMAL(18,2)");
}

// ---------------------------------------------------------------------------
// HAVING predicate — applied in the outer wrapper only, never in shard scan
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Task 4.1 — Pushdown wiring: filter JSON reaches Iceberg predicate and
// ScanSpec.filter (DataFusion string) is preserved on both paths.
// ---------------------------------------------------------------------------

/// Scenario: Filter predicate is pushed into the scan spec.
///
/// For a translatable filter (equality on a typed column):
/// - `ScanSpec.filter` (DataFusion SQL string) is `Some`.
/// - `to_iceberg_predicate` over the same JSON + a matching schema is `Some`.
///
/// Both coexist: Iceberg prunes files; DataFusion enforces row correctness.
#[test]
fn filter_in_common_arg() {
    use crate::adapter::iceberg_predicate::to_iceberg_predicate;
    use iceberg::spec::{NestedField, Schema, Type};
    use std::sync::Arc;

    // Build a minimal schema with an Int column "id".
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

    // DataFusion path: render_df_filter_safe must produce Some.
    let df_filter = render_df_filter_safe(&filter_json);
    assert!(
        df_filter.is_some(),
        "translatable filter must produce a DataFusion SQL string"
    );

    // Iceberg path: to_iceberg_predicate over the same JSON must produce Some.
    let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
    assert!(
        iceberg_pred.is_some(),
        "translatable filter must produce an Iceberg predicate"
    );

    // Confirm the DataFusion string survives into the common (arg 0) blob.
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

// ---------------------------------------------------------------------------
// Scenario: like_subject_type_guard dispatches LIKE subjects by Exasol type
// (issue #207 regression coverage).
// ---------------------------------------------------------------------------

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
/// `like_subject_type_guard` already classifies any `CHAR`-prefixed Exasol type
/// as a string subject alongside VARCHAR (`support.rs:546`), so this is a
/// non-regression CONTROL for the CHAR-type-declaration fix (issue #192): it
/// must pass unchanged both before and after that fix.
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
/// Regression: under the pre-fix code (`filter_json_raw.and_then(render_df_filter_safe)`
/// with no guard) the tree is never mutated, so `expression` would still be the bare
/// `column` node — this assertion on the rewritten `function_scalar_cast` shape is
/// false under that old behavior.
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
/// Regression: the pre-fix code has no decline mechanism at this layer — a DECIMAL
/// subject's `filter_json_raw` would pass straight through into `Some(...)` unmodified,
/// so asserting `None` here is false under that old behavior.
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

/// Scenario: a `predicate_like` over a DECIMAL-typed column pins that
/// [`apply_type_rewrites`] declines it — matching
/// `like_guard_decimal_subject_declines` above, which calls the guard directly
/// rather than through the pipeline. This test fails if the LIKE pass is ever
/// dropped from the pipeline; one pipeline now serves both render surfaces.
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

/// Scenario: LIKE on an integer column declines the whole filter. Exasol has no
/// separate wire "INTEGER" type — an integer column arrives as `DECIMAL(20,0)`
/// (confirmed via live payload capture this session).
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

/// Scenario: LIKE on a non-column subject (a computed scalar expression) is left
/// untouched, regardless of `col_types` — this is out of scope for the guard.
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
    // Even a DECIMAL entry for the underlying column must not trigger a decline,
    // since the LIKE subject itself is not a bare column.
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];

    let result = like_subject_type_guard(&filter, &col_types);
    assert_eq!(
        result,
        Some(filter),
        "non-column LIKE subject must be left unchanged"
    );
}

/// Scenario: LIKE on a bare column whose name is not present in `col_types` (a
/// lookup miss) declines the whole filter (fail-safe).
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

/// Scenario: a nested non-string LIKE (inside a `predicate_and`) declines the
/// entire enclosing filter, not just the LIKE sub-node.
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

/// Route `filter` through the production classification and then through the
/// qualified single-table wrapper, returning the emitted SQL.
///
/// Asserts the fixture genuinely declines before rendering: these tests are about
/// WHERE a declined predicate ends up, so a fixture that quietly renders would
/// assert nothing. The inner scan projects the whole `col_types` universe, which is
/// what the production decline route passes as its projection override.
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
            emit_exa_types: col_types.iter().map(|(_, ty)| ty.clone()).collect(),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    super::super::joins::build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
        &fan_out_spec,
        &[vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        Some(declined),
    )
    .expect("the wrapper must render the declined predicate")
}

/// The declined half of `like_guard_nested_decimal_declines_whole_filter`, carried
/// through to its consequence: a nested non-string LIKE declines the ENTIRE
/// enclosing filter, and that whole filter — the renderable `STATUS = 'OPEN'`
/// conjunct included — is then applied by the adapter in the wrapper's `WHERE`, not
/// omitted — omitting either conjunct would return rows the query excludes.
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

/// The declined half of `like_guard_integer_subject_declines`, carried through to
/// its consequence: an integer column arrives as `DECIMAL(20,0)`, the LIKE declines
/// the filter for the scan, and the adapter applies it itself in the wrapper.
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

/// The declined half of `like_guard_unresolvable_column_declines`, carried through
/// to its consequence: an unresolvable subject type is a FAIL-SAFE decline, and a
/// fail-safe decline must still self-apply. It cannot omit the predicate on the
/// grounds that it could not type it — that reasoning is what returned unfiltered
/// rows.
///
/// The fixture is deliberately unreachable from Exasol, which only sends columns of
/// the request's own `involvedTables`, so the assertion is about the routing
/// decision alone: the emitted SQL names a column this artificial `col_types`
/// universe does not project.
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

/// Scenario: REGEXP_LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR,
/// same as `predicate_like` — both node types dispatch through `guard_like_subject`.
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

/// Scenario: a DECIMAL-typed LIKE wrapped in `predicate_not` declines the whole
/// filter — the decline must propagate through `predicate_not`, not just through
/// `predicate_and`/`predicate_or`.
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

/// Regression (#207 blind spot): a DECIMAL-typed LIKE buried inside a
/// `function_scalar_case`'s `arguments` (a WHEN condition) must decline the whole
/// filter, same as a LIKE nested under `predicate_and`/`predicate_or`/`predicate_not` —
/// a `LIKE` at this non-junction position is type-guarded like any other.
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

/// Regression (#207 blind spot): a DATE-typed LIKE buried inside a
/// `function_scalar_case`'s `arguments` (a WHEN condition) is rewritten in place as
/// `CAST(<col> AS VARCHAR)`, with the enclosing CASE structure (its `results`
/// THEN/ELSE branches) preserved unchanged — a `LIKE` at this non-junction position
/// is type-guarded like any other.
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

/// The widened traversal must not cost a working pushdown: a VARCHAR-typed LIKE
/// buried inside a `function_scalar_case`'s `arguments` is now reached (it was not,
/// pre-migration), but since VARCHAR needs no rewrap the returned tree must equal
/// the input tree exactly, byte for byte.
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

// ---------------------------------------------------------------------------
// rewrite_expr_tree — the shared post-order traversal primitive
// ---------------------------------------------------------------------------

/// Post-order: when `f` runs on a node, that node's curated children are
/// already their rewritten selves. Proven without interior mutability — the
/// closure copies the child's (rewritten) type onto the parent, so the
/// assertion can only hold if the child was rewritten first.
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

/// A `None` from `f` at any depth declines the WHOLE tree — it propagates out
/// through every enclosing level instead of dropping only the declining
/// subtree.
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

/// Only the curated fields are descended into, and only in the shapes the
/// grammar sends: an array field must be a `Json::Array`, a single-child field
/// must be an object. A node's object-valued `dataType` sub-object is never
/// handed to `f`, so no guard can rewrite a declared type; `name` is excluded
/// too, since it always carries a bare identifier string, never an object.
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

/// A non-object node reaches `f` too: the primitive has no leaf early-return,
/// which is what lets the migrated walkers drop theirs.
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

// ---------------------------------------------------------------------------
// rewrite_decimal_stringifications — issue #211 decimal-trim JSON rewrite
// ---------------------------------------------------------------------------

/// The column-type map shared by the rewrite and string-function-guard tests: one
/// DECIMAL, one integer DECIMAL(p,0), one VARCHAR, one DATE, plus the three
/// resolvable-but-non-coercible types the string-function guard must decline on
/// (issue #210). The three additions cannot disturb the #211 rewrite assertions:
/// those reference only the first four names, and every wired `project_columns`
/// test here projects a single expression rather than the full base row.
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

/// A non-object node is returned unchanged: `rewrite_expr_tree` finds no curated
/// child on it, so the always-`Some` closure's catch-all arm clones it.
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

/// `CAST(<decimal column> AS VARCHAR)` → the WHOLE cast node is replaced by a
/// `decimal_to_varchar_exasol` node wrapping the column, which renders through
/// `format_decimal_exasol_style` (the trailing-zero-trim regexp form).
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
    // Rendering proves it goes through the trimming regexp form.
    let sql = render_expression_safe(&out).expect("must render");
    assert!(
        sql.contains(r#"CAST("C_DECIMAL_A" AS VARCHAR)"#) && sql.contains("regexp_replace"),
        "must render via format_decimal_exasol_style: {sql}"
    );
}

/// `CAST(<decimal column> AS CHAR)` is also a stringification → replaced.
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

/// The exact nested-CONCAT shape from the live capture
/// (`CONCAT(ID, CONCAT('-', C_DECIMAL_A))`, i.e. `id||'-'||c_decimal_a`): ONLY
/// `C_DECIMAL_A` (reachable only through the INNER CONCAT) gets wrapped; the
/// non-decimal `ID` column and the `'-'` literal are untouched, and the nested
/// structure is otherwise preserved. Guards the post-order-recursion BLOCKER.
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

    // Outer CONCAT preserved; ID (integer DECIMAL(20,0)) is itself decimal so it
    // IS wrapped as a direct outer-CONCAT argument — but the OUTER structure and
    // its argument count are preserved.
    assert_eq!(out.get("name").and_then(|n| n.as_str()), Some("CONCAT"));
    let outer_args = out["arguments"].as_array().unwrap();
    assert_eq!(
        outer_args.len(),
        2,
        "outer CONCAT arg count preserved: {out}"
    );

    // The inner CONCAT node is still a CONCAT; its literal '-' is untouched and
    // its C_DECIMAL_A argument is now wrapped.
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

/// `CONCAT(NAME, C_DECIMAL_A)` — only the DECIMAL column is wrapped; the VARCHAR
/// column is left as a bare column reference.
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

/// `LENGTH(<decimal column>)` → its single argument is wrapped.
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

/// A non-DECIMAL bare column as a CAST / CONCAT / LENGTH argument is left
/// COMPLETELY unchanged (VARCHAR and DATE both).
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

/// A computed-expression argument (e.g. `c_decimal_a * 2`) to a stringifier is
/// left unchanged — its type is not resolvable from `col_types`, a tracked
/// exception in the plan's scope. The argument is not a bare column, so neither
/// the CAST replacement nor the CONCAT per-argument wrap fires on it.
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

/// A DECIMAL column in a NON-stringifying context is NOT wrapped: neither a
/// comparison predicate (`c_decimal_a > 5`) nor a CAST to a non-string target
/// (`CAST(c_decimal_a AS DOUBLE)`). Proves the recursion does not over-wrap.
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

/// A DECIMAL stringification reachable ONLY through a `function_scalar_case` THEN
/// branch (its `results` field) is still found and wrapped — proves the generic
/// child recursion covers CASE's `results`, not just `arguments`.
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

/// Wiring sanity check: a select-list `CAST(c_decimal_a AS VARCHAR(20))` over a
/// bare DECIMAL column must
/// route through `render_expression_safe` — yielding a SINGLE `ProjectionItem::Expr`
/// carrying the trim, at the item's declared EMITS type — NOT degrade to the full
/// base-row fallback. This proves both wiring changes: the unconditional rewrite of
/// each select-list item AND `decimal_to_varchar_exasol` being recognized by the
/// scalar `item_type` match arm.
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

/// Exhaustive coverage: the exact nested-CONCAT JSON shape confirmed
/// live for `id||'-'||c_decimal_a` (`CONCAT(ID, CONCAT('-', C_DECIMAL_A))`) as a
/// select-list item, through `project_columns`, renders `C_DECIMAL_A`'s CAST
/// fragment specifically wrapped in `format_decimal_exasol_style`'s
/// `regexp_replace` pair — proving the wiring (not just the isolated JSON rewrite
/// already covered by `rewrite_nested_concat_wraps_only_inner_decimal`) reaches
/// the nested inner-CONCAT argument at the `project_columns` level.
///
/// `ID` is itself `DECIMAL(20,0)`, so it too is a direct outer-CONCAT argument and
/// gets the same (harmless, no-op-on-scale-0) trim wrapper — documented behavior
/// already asserted in `rewrite_nested_concat_wraps_only_inner_decimal`. This test
/// only asserts what's specific to `C_DECIMAL_A`: its CAST fragment sits inside
/// the trim wrapper.
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

/// Exhaustive coverage: `LENGTH(c_decimal_a)` as a select-list item,
/// through `project_columns`, renders the trim-wrapped `character_length(...)` —
/// the LENGTH-over-DECIMAL wiring at the projection level (mirrors
/// `rewrite_length_wraps_decimal_argument`'s isolated JSON check).
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

/// Exhaustive coverage: `CAST(<VARCHAR column> AS VARCHAR(20))` through
/// `project_columns` renders EXACTLY as it did before this whole fix — a plain
/// CAST, with no `regexp_replace` / `decimal_to_varchar_exasol` involvement.
/// Proves the fix doesn't touch a non-DECIMAL stringification at the wired level.
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

/// Exhaustive coverage: `CAST(c_decimal_a * 2 AS VARCHAR)` through
/// `project_columns` renders a plain, untrimmed CAST — proving the adapter-level
/// wiring correctly leaves the tracked-exception computed-argument case alone
/// (issue #223's scope), consistent with
/// `rewrite_computed_expression_argument_unchanged` at the wired level.
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

// ---------------------------------------------------------------------------
// project_columns wiring — issue #210 string_function_arg_type_guard, run
// BEFORE rewrite_decimal_stringifications on every select-list item
// ---------------------------------------------------------------------------

/// Scenario: `UPPER(c_decimal_a)` projects to a SINGLE expression carrying the
/// trimmed decimal-to-string form (#211's node, reached through the new guard),
/// at the item's declared `selectListDataTypes` type — not the full base row.
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

/// Scenario: `LOWER(c_date)` (the `d` fixture column, `DATE`-typed) projects a
/// single expression containing `CAST("D" AS VARCHAR)`.
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

/// Scenario: `UPPER(c_double)` (the `c_double_a` fixture column) degrades to the
/// FULL base row with no error — `string_function_arg_type_guard` declines a
/// resolvable-but-non-coercible column type, and `project_columns` falls back
/// exactly like any other untranslatable select-list item.
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

/// Scenario: `INSTR(c_decimal_a, '.')` projects a single expression whose FIRST
/// `strpos` argument is the trimmed decimal form and whose SECOND argument is the
/// untouched string literal `'.'` — `INSTR(string, substring)` -> `strpos(string,
/// substring)`, so index 0 is the column being coerced, index 1 the literal left
/// alone since it is not a bare column.
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

/// Scenario: `INSTR(c_varchar, 'b', 3)` (three arguments, all effectively
/// VARCHAR/literal) degrades to the FULL base row rather than projecting a
/// truncated `strpos` call — the #228 arity-decline path. `vs-expression` reads
/// only `args[0]`/`args[1]` and silently drops the third; coercing index 0 here
/// would let a truncated rendering plan successfully, so
/// `string_position_args("INSTR", 3)` returns `Decline` regardless of every
/// argument already being VARCHAR/literal.
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

// ---------------------------------------------------------------------------
// Select-list predicate node types added to the pushable whitelist (#196)
// ---------------------------------------------------------------------------

/// Each whitelisted select-list predicate node type (issue #196) renders as a
/// positional `ProjectionItem::Expr` carrying the rendered SQL fragment and the
/// declared `selectListDataTypes` type — not the full-base-row fallback.
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

/// A `function_aggregate` select-list item still widens to the full base row —
/// pinning the whitelist's one deliberate exclusion (#196) as intentional, not
/// incidental: an aggregate must reach the aggregate planner, not be evaluated
/// per shard as a projection item.
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

// ---------------------------------------------------------------------------
// like_subject_type_guard wired into apply_type_rewrites — issue #219
// select-list LIKE type coercion
// ---------------------------------------------------------------------------

/// Scenario: `predicate_like` over `d` (`DATE`) projects a SINGLE expression that
/// rewraps the subject as `CAST("D" AS VARCHAR)`, mirroring the filter pipeline's
/// DATE arm — not the full base row.
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

/// Scenario: a `predicate_like`/`predicate_like_regexp` over a subject that
/// resolves to a non-string Exasol type (DECIMAL, integer DECIMAL(p,0), DOUBLE,
/// BOOLEAN, TIMESTAMP) or does not resolve at all widens the WHOLE select list to
/// the full base row — `Ok`, never `Err`. Mirrors
/// `like_guard_decimal_subject_declines`'s dispatch table, now proven wired through
/// `project_columns`.
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

/// Scenario: a `predicate_like` over `c_decimal_a` nested inside a
/// `function_scalar_case` still widens to the full base row — pinning that the
/// guard's [`rewrite_expr_tree`] reach (a LIKE buried under a CASE, not only a
/// bare top-level select-list item) is wired all the way through
/// `project_columns`, not just the isolated `like_subject_type_guard` call.
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

// ---------------------------------------------------------------------------
// string_position_args — issue #210 string-position argument table
// ---------------------------------------------------------------------------

/// Every argument of `CONCAT`/`TRIM`/`LTRIM`/`RTRIM`/`REPLACE`/`TRANSLATE` is a
/// string position, at every arity Exasol can send.
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

/// Only the FIRST argument of these is a string position; any further argument is
/// a genuine number (a start offset, a length, a repeat count).
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

/// `LPAD`/`RPAD`'s numeric length argument (index 1) is always excluded, while
/// their PAD-string argument (index 2, present only at arity > 2) is still
/// coerced — the only mixed string/numeric arity in the table.
/// `SUBSTR`/`REPEAT`/`LEFT`/`RIGHT`'s single-numeric-argument exclusion is already
/// covered by `string_position_args_coerces_first_argument_only` above.
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

/// `CHR`/`UNICODECHR` (their argument is a genuine integer codepoint) and every
/// non-string function are NOT governed — the caller leaves such a node alone and
/// never declines on it.
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

/// The name is uppercased before matching, so a lowercase `fn_name` resolves
/// identically.
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

/// No returned index may address past the end of the argument list — the caller
/// indexes `arguments` with them directly.
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

/// `INSTR`/`LOCATE` beyond two arguments decline on ARITY ALONE, whatever the
/// argument types: `vs-expression` renders only `args[0]`/`args[1]` and silently
/// drops the rest (#228), so coercing index 0 would turn today's loud DataFusion
/// error into a silently wrong position. Exactly two arguments coerce both.
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

// ---------------------------------------------------------------------------
// string_function_arg_type_guard — issue #210 string-function argument typing
// ---------------------------------------------------------------------------

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

/// A non-object node has no children and no function dispatch — passed through.
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

/// Scenario: a string-position VARCHAR or CHAR column argument pushes down
/// unchanged — DataFusion needs no help with a genuine string.
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
    // CHAR is dispatched by the same `starts_with` prefix pair as VARCHAR.
    let char_types = vec![("C_CHAR_A".to_string(), "CHAR(10)".to_string())];
    let node = string_fn("UPPER", vec![column("c_char_a")]);
    assert_eq!(
        string_function_arg_type_guard(&node, &char_types),
        Some(node.clone()),
        "a CHAR column argument must be unchanged"
    );
}

/// Scenario: a string-position DECIMAL column argument renders through Exasol's
/// trimmed decimal-to-string form (#211's `decimal_to_varchar_exasol` node, reused
/// verbatim so decimal formatting keeps a single owner). Integer columns arrive as
/// `DECIMAL(p,0)` on the wire and are covered by the same branch.
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

    // Integer column (DECIMAL(20,0)) — issue #210's `UPPER(c_custkey)` repro shape.
    assert_eq!(
        string_function_arg_type_guard(&string_fn("UPPER", vec![column("id")]), &col_types),
        Some(string_fn("UPPER", vec![trimmed_decimal("id")])),
        "an integer DECIMAL(p,0) argument must be wrapped too"
    );

    // The wrapper is what renders Exasol's shortest form, not a plain CAST.
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

/// Scenario: a string-position DATE column argument is wrapped in an explicit
/// `CAST(<col> AS VARCHAR)` — DataFusion's Date32→Utf8 cast is `YYYY-MM-DD`, which
/// is also Exasol's default `NLS_DATE_FORMAT` (issue #210's `LOWER(l_shipdate)`).
#[test]
fn string_fn_guard_casts_date_argument_to_varchar() {
    let col_types = decimal_rewrite_col_types();
    assert_eq!(
        string_function_arg_type_guard(&string_fn("LOWER", vec![column("d")]), &col_types),
        Some(string_fn("LOWER", vec![cast_varchar("d")])),
        "LOWER's DATE argument must be wrapped in CAST(<col> AS VARCHAR)"
    );
}

/// Scenario: a resolvable but non-coercible column type declines. BOOLEAN, DOUBLE
/// and TIMESTAMP all have text forms that differ between the two engines
/// (`TRUE`/`true`, the space/`T` separator), so a cast would turn a crash into a
/// wrong answer — native Exasol evaluation is the only safe outcome.
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

/// Scenario: a string-position argument whose column name does not resolve in
/// `col_types` declines fail-safe.
#[test]
fn string_fn_guard_declines_unresolved_column_name() {
    let col_types = decimal_rewrite_col_types();
    assert_eq!(
        string_function_arg_type_guard(&string_fn("UPPER", vec![column("mystery")]), &col_types),
        None,
        "an unresolvable column argument must decline"
    );
}

/// A `column` node with no `name` field is unresolvable — same fail-safe decline.
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

/// The guard reaches a string function nested under a COMPARISON predicate (under
/// `left`) — the shape issue #210's WHERE-clause repro takes.
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

/// A decline anywhere in the tree propagates to the ROOT, so the caller declines
/// the whole filter / select-list item rather than pushing a partially-guarded tree.
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

/// Only string-position indices are coerced: `SUBSTR`'s start/length, `REPEAT`'s
/// count, `LEFT`/`RIGHT`'s length and `LPAD`'s length stay untouched, while
/// `LPAD`'s PAD-STRING argument is coerced. The numeric positions here hold a
/// DECIMAL column (`ID`), which WOULD be visibly rewritten if it were passed to
/// the type dispatch — a literal int could not tell the two designs apart.
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

    // A literal-int length is likewise never handed to the type dispatch.
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

/// Scenario: `INSTR` and `LOCATE` coerce BOTH of their two arguments. `LOCATE`'s
/// render-time argument swap (`LOCATE(sub, str)` → `strpos(str, sub)`) happens
/// after this guard, so both indices are string positions in either order.
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

/// Scenario: `INSTR` with 3 or 4 arguments and `LOCATE` with 3 decline THROUGH THE
/// GUARD even when every argument is a VARCHAR column — the arity, not a type, is
/// what declines (`vs-expression` drops the extra arguments, #228). The table-level
/// counterpart is `string_position_args_declines_instr_locate_beyond_two_args`.
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

/// Scenario: `CHR`/`UNICODECHR` are excluded — their single argument is a genuine
/// integer codepoint, so it is neither coerced NOR a reason to decline (the
/// difference between "not governed" and "declines on a bad argument"). Their
/// children are still recursed.
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

    // ... but a governed function nested INSIDE one is still reached.
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

/// A column name in any letter case resolves — [`column_exa_type`] uppercases the
/// name before the `col_types` lookup.
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

/// The one `col_types` lookup folds the node's name with the full-Unicode
/// `to_uppercase`, so it resolves against the Unicode-folded list
/// [`extract_all_column_types`] builds and MISSES a constructed ASCII-folded list
/// that no builder in this codebase produces.
///
/// `STRAßE` is a CONSTRUCTED literal, not a name Exasol delivers: this crate
/// uppercases every Iceberg field name itself before declaring it
/// (`resolve_table_schema`, `file_resolution.rs:640`) and the full-Unicode fold maps
/// `ß` to `SS`, so a real `straße` column reaches this lookup as `STRASSE`. The
/// `ascii_folded` list below is likewise constructed, used here solely because Rust's
/// two folds disagree on `STRAßE`, which is what makes the miss assertion falsifiable.
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

/// A non-bare-column string-position argument (a literal, or a computed
/// `c_decimal_a * 2`) is left unchanged and does NOT decline — a deliberate tracked
/// exception (#223), mirroring #211's convention for computed arguments.
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

/// Post-order: the INNER string function's argument is coerced before the outer
/// function's own check runs, so `UPPER(TRIM(c_decimal_a))` coerces the `TRIM`
/// argument and leaves the (now non-column) `TRIM` node as UPPER's argument.
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

/// `cast_to_declared_type` casts when a declared type is present and not the
/// `VARCHAR(2000000)` default, and returns the expression unwrapped otherwise.
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
