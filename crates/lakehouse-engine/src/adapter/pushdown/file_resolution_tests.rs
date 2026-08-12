use super::super::detect_aggregates;
use super::super::single_group_agg::DistinctCount;
use super::super::support::quote_ident;
use super::super::test_support::*;
use super::*;
use crate::scan::spec::AggregatePlan;
use iceberg::spec::{DataContentType, DataFileFormat};

// ---------------------------------------------------------------------------
// Task 1.3 — fail-loud on unsupported delete/data mechanisms (manifest level)
// ---------------------------------------------------------------------------

/// The two mechanisms this engine CAN apply — a Parquet data file and a
/// Parquet positional-delete file — classify as supported (`Ok`).
#[test]
fn classify_accepts_parquet_data_and_parquet_positional_delete() {
    assert!(
        classify_manifest_file(DataContentType::Data, DataFileFormat::Parquet).is_ok(),
        "Parquet data file must be supported"
    );
    assert!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Parquet).is_ok(),
        "Parquet positional delete must be supported"
    );
}

/// Equality deletes fail loud regardless of file format.
#[test]
fn classify_rejects_equality_deletes() {
    for fmt in [
        DataFileFormat::Parquet,
        DataFileFormat::Avro,
        DataFileFormat::Orc,
    ] {
        assert_eq!(
            classify_manifest_file(DataContentType::EqualityDeletes, fmt),
            Err(UnsupportedDeleteMechanism::EqualityDelete),
            "equality delete ({fmt:?}) must fail loud"
        );
    }
}

/// A position delete stored as a Puffin blob is a v3 deletion vector — the
/// exact case indistinguishable from a Parquet positional delete once
/// `plan_files` has dropped the format discriminator, so it MUST be caught at
/// the manifest level.
#[test]
fn classify_rejects_puffin_deletion_vector() {
    assert_eq!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Puffin),
        Err(UnsupportedDeleteMechanism::DeletionVector),
        "Puffin position delete (deletion vector) must fail loud"
    );
}

/// ORC/Avro data and delete files fail loud.
#[test]
fn classify_rejects_orc_and_avro_data_and_delete_files() {
    assert_eq!(
        classify_manifest_file(DataContentType::Data, DataFileFormat::Orc),
        Err(UnsupportedDeleteMechanism::OrcDataFile),
    );
    assert_eq!(
        classify_manifest_file(DataContentType::Data, DataFileFormat::Avro),
        Err(UnsupportedDeleteMechanism::AvroDataFile),
    );
    assert_eq!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Orc),
        Err(UnsupportedDeleteMechanism::OrcDeleteFile),
    );
    assert_eq!(
        classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Avro),
        Err(UnsupportedDeleteMechanism::AvroDeleteFile),
    );
}

/// The fail-loud error names the mechanism, names the table, and leaks no
/// credential (defensively redacted).
#[test]
fn unsupported_delete_error_names_mechanism_and_redacts() {
    let err = unsupported_delete_error(
        UnsupportedDeleteMechanism::DeletionVector,
        "db.mor_dv_table",
    );
    let msg = match err {
        UdfError::User(m) => m,
        other => panic!("expected UdfError::User, got {other:?}"),
    };
    assert!(
        msg.contains("Iceberg v3 Puffin deletion vectors"),
        "error must name the mechanism: {msg}"
    );
    assert!(
        msg.contains("db.mor_dv_table"),
        "error must name the offending table: {msg}"
    );
    // No credential label may survive the defensive redaction.
    assert!(
        !msg.contains("access_key"),
        "must not leak access_key: {msg}"
    );
    assert!(
        !msg.contains("secret_key"),
        "must not leak secret_key: {msg}"
    );
}

/// A manifest-read error that echoes Azure static credentials verbatim has
/// BOTH literal values stripped — not merely their labels.
///
/// The two credentials fail the label heuristic in different ways, so each
/// independently requires the value-based pass:
///   - the account key is echoed bare inside a string-to-sign, with no
///     recognizable label anywhere near it;
///   - the SAS token carries its OWN `sig=` label, so a label-only pass
///     rewrites the middle of the token and leaves its permission and expiry
///     fields verbatim.
#[test]
fn manifest_read_errors_redact_the_literal_azure_secret_values() {
    let account_key = "Zm9vYmFyYmF6cXV1eGNvcmdlc2VjcmV0QUNDT1VOVEtFWT09";
    let sas_permissions = "sp=racwdlmeop";
    let sas_token = format!(
        "sv=2024-11-04&ss=bf&srt=sco&{sas_permissions}&se=2026-12-31T23:59:59Z&sig=aB3%2FxQ7"
    );
    let raw = format!(
        "AuthenticationFailed: Server failed to authenticate the request. \
         String to sign used was: {account_key}. \
         Request URL: https://acct.dfs.core.windows.net/c/meta/snap.avro?{sas_token}"
    );
    let secrets = [account_key, sas_token.as_str()];

    let surfaced = format!(
        "failed to read Iceberg manifest list for 'ns.tbl': {}",
        redact_error_text(&raw, &secrets)
    );

    assert!(
        !surfaced.contains(account_key),
        "account key value must not survive: {surfaced}"
    );
    assert!(
        !surfaced.contains(&sas_token),
        "SAS token value must not survive: {surfaced}"
    );
    assert!(
        !surfaced.contains(sas_permissions),
        "the SAS token's permission field must not survive either: {surfaced}"
    );
    assert!(
        surfaced.contains("failed to read Iceberg manifest list for 'ns.tbl'"),
        "the actionable context must be preserved: {surfaced}"
    );
}

// ---------------------------------------------------------------------------
// Task 1.2 — adapter carries positional deletes into the per-shard scan spec
// ---------------------------------------------------------------------------

/// `map_delete_content_type` maps the iceberg task-level content type onto the
/// wire enum honestly (position → position; equality → equality).
#[test]
fn map_delete_content_type_maps_position_and_equality() {
    use iceberg::spec::DataContentType;
    assert_eq!(
        map_delete_content_type(DataContentType::PositionDeletes),
        DeleteFileContentType::PositionDeletes
    );
    assert_eq!(
        map_delete_content_type(DataContentType::EqualityDeletes),
        DeleteFileContentType::EqualityDeletes
    );
}

/// A data file's associated positional-delete file paths are relativized by
/// the SAME rule as the data-file path: an under-root path is stripped to a
/// root-relative path, a path not under the root stays absolute. Delete size
/// and content type are preserved.
#[test]
fn delete_file_paths_use_relative_absolute_encoding() {
    let root = "s3://warehouse/db/table";
    let entry = FileEntry::with_deletes(
        format!("{root}/data/part-0.parquet"),
        1000,
        vec![
            // under the table root — must relativize exactly like the data path
            pos_delete(&format!("{root}/data/deletes/del-0.parquet"), 50),
            // not under the root — must stay absolute
            pos_delete("s3://other-bucket/del-x.parquet", 60),
        ],
    );
    let shards = relativize_shards_to_root(vec![vec![entry]], root);
    let e = &shards[0][0];
    assert_eq!(e.path, "data/part-0.parquet", "data path must relativize");
    assert_eq!(
        e.deletes[0].path, "data/deletes/del-0.parquet",
        "under-root delete path must relativize EXACTLY like the data path"
    );
    assert_eq!(e.deletes[0].size, 50, "delete size preserved");
    assert_eq!(
        e.deletes[0].content_type,
        DeleteFileContentType::PositionDeletes,
        "delete content type preserved"
    );
    assert_eq!(
        e.deletes[1].path, "s3://other-bucket/del-x.parquet",
        "a delete path not under the root must stay absolute"
    );
}

/// Mirror of the scan UDF's `reconstruct_abs_uri` join rule, so the round-trip
/// invariant can be asserted here without a cross-crate dependency: an entry that
/// already carries a scheme (`"://"`) is absolute and returned unchanged; any
/// other entry is joined onto the root with exactly one `/`.
fn reconstruct_abs_uri_mirror(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

/// A path that shares the table root only as a bare STRING prefix (no `/`
/// segment boundary) must NOT be relativized: stripping it and rejoining with a
/// single `/` corrupts the URI (finding R.1). Only true under-root paths are
/// stripped; everything else stays absolute and round-trips to itself.
#[test]
fn sibling_prefix_paths_are_not_relativized() {
    let root = "s3://w/db/events";

    // A genuine under-root path IS relativized (existing behavior preserved).
    let under = format!("{root}/data/f.parquet");
    assert_eq!(
        relativize_path_to_root(&under, root),
        "data/f.parquet",
        "under-root path must be relativized"
    );

    // Sibling directories that share the root as a bare prefix but break at no
    // `/` boundary stay ABSOLUTE (not stripped).
    let archive = format!("{root}-archive/f.parquet");
    assert_eq!(
        relativize_path_to_root(&archive, root),
        archive,
        "sibling '-archive' path must stay absolute"
    );
    let sibling2 = format!("{root}2/data/f.parquet");
    assert_eq!(
        relativize_path_to_root(&sibling2, root),
        sibling2,
        "sibling '2' path must stay absolute"
    );

    // A path exactly equal to the root stays absolute (no empty entry).
    assert_eq!(
        relativize_path_to_root(root, root),
        root,
        "path equal to the root must stay absolute, not become an empty entry"
    );

    // Every case round-trips back to the original absolute path through the
    // scan UDF's reconstruct rule.
    for original in [&under, &archive, &sibling2, &root.to_string()] {
        let emitted = relativize_path_to_root(original, root);
        assert_eq!(
            reconstruct_abs_uri_mirror(&emitted, root),
            *original,
            "round-trip must be identity for {original}"
        );
    }
}

/// The `abfss://` scheme carries userinfo (the container name) in its
/// authority (`abfss://<container>@<account>.dfs.core.windows.net/...`),
/// unlike `s3://`'s bare-bucket authority. The relativize/reconstruct round
/// trip must still be lossless: relativizing an under-root `abfss://` file
/// path against its table root and reconstructing via the scan UDF's join
/// rule must reproduce the original URI byte-for-byte, exactly like the
/// `s3://` case above.
#[test]
fn abfss_paths_relativize_and_reconstruct_losslessly() {
    let root = "abfss://container@account.dfs.core.windows.net/db/table";
    let original = format!("{root}/data/part-0.parquet");

    let relative = relativize_path_to_root(&original, root);
    assert_eq!(
        relative, "data/part-0.parquet",
        "abfss path under the root must relativize just like s3"
    );

    let reconstructed = reconstruct_abs_uri_mirror(&relative, root);
    assert_eq!(
        reconstructed, original,
        "reconstructed abfss URI must equal the original byte-for-byte"
    );
}

// ---------------------------------------------------------------------------
// Pre-existing helpers tests (unchanged)
// ---------------------------------------------------------------------------

#[test]
fn empty_file_list_returns_empty_select() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let resp = empty_pushdown_sql(&proj, &types);
    let sql = resp["sql"].as_str().unwrap();
    assert!(sql.contains("WHERE 1=0"));
    assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
}

/// A pruned query with repeated literals in the projection (e.g.
/// `SELECT 1, name, 1 ... WHERE <all files pruned>`) keeps unique EMITS
/// aliases via `emits_ident`: the two `Expr` positions get distinct
/// positional synthetic names, never a duplicated `AS "1"` collision
/// (issue #190).
#[test]
fn empty_pushdown_sql_repeated_literals_unique_aliases() {
    let proj_cols: Vec<ProjectionItem> = vec![
        ProjectionItem::Expr { expr: "1".into() },
        ProjectionItem::Column("NAME".into()),
        ProjectionItem::Expr { expr: "1".into() },
    ];
    let proj_types = vec![
        "DECIMAL(18,0)".to_string(),
        "VARCHAR(2000000)".to_string(),
        "DECIMAL(18,0)".to_string(),
    ];
    let resp = empty_pushdown_sql(&proj_cols, &proj_types);
    let sql = resp["sql"].as_str().unwrap();

    assert_eq!(
        sql.matches("CAST(NULL AS").count(),
        3,
        "must emit three CAST(NULL AS ...) items, one per select-list item: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_0"))),
        "position 0's literal must get a positional-unique alias: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("NAME"))),
        "the column item must keep its real quoted name: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_2"))),
        "position 2's literal must get a distinct positional-unique alias: {sql}"
    );
    assert!(
        !sql.contains(&format!("AS {}", quote_ident("1"))),
        "must never alias a literal by its rendered value text (would collide): {sql}"
    );
}

/// Single-group empty result: one row, per-`AggKind` literal cast to its
/// declared type — COUNT → `0`, SUM → `NULL` — with no `WHERE 1=0` (a bare
/// `FROM DUAL` already yields exactly one row).
#[test]
fn empty_agg_sql_emits_zero_and_null_row_cast_to_declared_types() {
    let items = vec![
        SingleGroupItem::Aggregate(AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }),
        SingleGroupItem::Aggregate(AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }),
    ];
    let types = vec!["DECIMAL(18,0)".to_string(), "DECIMAL(36,2)".to_string()];
    let resp = empty_agg_sql(&items, &types);
    let sql = resp["sql"].as_str().unwrap();
    assert!(sql.contains("FROM DUAL"), "must select from DUAL: {sql}");
    assert!(
        !sql.contains("WHERE 1=0"),
        "single-group empty is one row, not zero rows: {sql}"
    );
    assert!(
        sql.contains("CAST(0 AS DECIMAL(18,0))"),
        "COUNT empty literal must be 0 cast to declared type: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(36,2))"),
        "SUM empty literal must be NULL cast to declared type: {sql}"
    );
}

/// COUNT(DISTINCT) over zero files yields a plain `0` literal row — no distinct
/// fan-out, no scan, and no merge step (with zero files there is nothing to scan
/// or deduplicate).
#[test]
fn empty_agg_sql_count_distinct_emits_zero_no_merge_udf() {
    let items = vec![SingleGroupItem::Distinct(DistinctCount {
        column: Some("ID".into()),
        arg_expr: None,
    })];
    let types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_agg_sql(&items, &types);
    let sql = resp["sql"].as_str().unwrap();
    assert_eq!(
        sql, "SELECT CAST(0 AS DECIMAL(18,0)) FROM DUAL",
        "COUNT(DISTINCT) over zero files must be a plain 0 literal row with no fan-out \
         or merge step: {sql}"
    );
}

/// Issue #57 shape-consistency (task 6.7): when EVERY file is pruned, a Case 2/3
/// single-group request (more than one `COUNT(DISTINCT)`, or a distinct mixed with
/// an ordinary aggregate) must return the SAME N-aggregate-column shape
/// (`empty_agg_sql`, one column per select item) that the non-empty qualified
/// single-table wrapper returns — NEVER the full-row empty shape
/// (`empty_pushdown_sql`), whose different column count trips Exasol's positional
/// pushdown validation (`sqlCode 04000`, "Expected number of columns is N but
/// pushdown query has M"), since Exasol never re-aggregates a declined pushdown.
#[test]
fn empty_case_2_3_matches_non_empty_aggregate_shape() {
    fn count_top_level_cols(select_span: &str) -> usize {
        let mut depth = 0i32;
        let mut cols = 1usize;
        for ch in select_span.chars() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => cols += 1,
                _ => {}
            }
        }
        cols
    }

    // Case 3: two COUNT(DISTINCT) + one ordinary SUM → N = 3 output columns.
    let pushdown_req = serde_json::json!({
        "selectList": [
            agg_item("COUNT", Some("A"), true),
            agg_item("COUNT", Some("B"), true),
            agg_item("SUM", Some("C"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    });
    let col_types = vec![
        ("A".to_string(), "DECIMAL(18,0)".to_string()),
        ("B".to_string(), "DECIMAL(18,0)".to_string()),
        ("C".to_string(), "DECIMAL(36,2)".to_string()),
    ];

    // The fixture must be a Case 2/3 shape: distinct present, but not a lone one
    // (so the non-empty path declines the fan-out and routes to the wrapper).
    let items = detect_aggregates(&pushdown_req).expect("a Case 3 select list detects");
    assert!(
        super::super::single_group_agg::has_distinct(&items)
            && !super::super::single_group_agg::is_lone_count_distinct(&items),
        "the fixture must be a Case 2/3 shape"
    );
    let n = pushdown_req["selectList"].as_array().unwrap().len();

    // A deliberately WIDER full-row projection (5 columns): if the empty dispatch
    // wrongly returned the full-row shape, its column count would be 5, not N = 3.
    let proj_cols: Vec<ProjectionItem> = ["A", "B", "C", "D", "E"]
        .iter()
        .map(|c| ProjectionItem::from(*c))
        .collect();
    let proj_types = vec![
        "DECIMAL(18,0)".to_string(),
        "DECIMAL(18,0)".to_string(),
        "DECIMAL(36,2)".to_string(),
        "VARCHAR(10)".to_string(),
        "VARCHAR(10)".to_string(),
    ];

    let empty = empty_result_sql(&pushdown_req, &proj_cols, &proj_types, false, &col_types)
        .expect("empty Case 2/3 result must build");
    let empty_sql = empty["sql"].as_str().unwrap();

    // Routes to the N-aggregate-column shape (empty_agg_sql), NOT the full-row shape.
    let direct = empty_agg_sql(&items, &aggregate_exasol_types(&pushdown_req));
    assert_eq!(
        empty_sql,
        direct["sql"].as_str().unwrap(),
        "the empty Case 2/3 dispatch must route to empty_agg_sql: {empty_sql}"
    );
    assert_ne!(
        empty_sql,
        empty_pushdown_sql(&proj_cols, &proj_types)["sql"]
            .as_str()
            .unwrap(),
        "the empty Case 2/3 dispatch must NOT return the full-row empty shape (#57): {empty_sql}"
    );

    // Exactly N columns — the same one-per-select-item shape the non-empty wrapper
    // returns, so empty and non-empty column shapes never diverge.
    let select_span = &empty_sql["SELECT ".len()..empty_sql.find(" FROM").expect("has FROM")];
    assert_eq!(
        count_top_level_cols(select_span),
        n,
        "the empty shape must have exactly N={n} aggregate columns (one per select \
         item): {empty_sql}"
    );
    // COUNT(DISTINCT) over zero files → 0; the ordinary SUM → NULL, each cast to
    // its declared type.
    assert!(
        empty_sql.contains("CAST(0 AS DECIMAL(18,0))")
            && empty_sql.contains("CAST(NULL AS DECIMAL(36,2))"),
        "COUNT(DISTINCT) empties to 0 and the ordinary SUM to NULL: {empty_sql}"
    );
}

/// Every non-COUNT `AggKind` maps to the `NULL` empty literal — single-node
/// SQL semantics over zero rows (only the COUNT family yields `0`).
#[test]
fn empty_agg_literal_maps_non_count_kinds_to_null() {
    for kind in [
        AggKind::Sum,
        AggKind::Min,
        AggKind::Max,
        AggKind::Avg,
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::StddevPop,
        AggKind::StddevSamp,
    ] {
        assert_eq!(
            empty_agg_literal(&kind),
            "NULL",
            "{kind:?} empty literal must be NULL"
        );
    }
    for kind in [AggKind::Count, AggKind::CountCol] {
        assert_eq!(
            empty_agg_literal(&kind),
            "0",
            "{kind:?} empty literal must be 0"
        );
    }
}

/// Grouped empty result: zero rows (`WHERE 1=0`) with one `CAST(NULL AS <ty>)`
/// per grouped output column, assembled in select-list order.
#[test]
fn empty_grouped_sql_emits_zero_rows_in_grouped_shape() {
    let select_items = vec![
        GroupedSelectItem::GroupKey {
            group_key_slot: 0,
            select_index: 0,
        },
        GroupedSelectItem::Aggregate {
            plan_slot: 0,
            select_index: 1,
        },
    ];
    let group_key_types = vec!["DECIMAL(20,0)".to_string()];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
    let sql = resp["sql"].as_str().unwrap();
    assert!(
        sql.contains("WHERE 1=0"),
        "grouped empty is zero rows: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(20,0))"),
        "group-key column typed from group_key_types: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(18,0))"),
        "aggregate column typed from aggregate_types: {sql}"
    );
    let select_clause = sql
        .strip_prefix("SELECT ")
        .and_then(|s| s.split(" FROM").next())
        .unwrap();
    assert_eq!(
        select_clause.matches("CAST(NULL AS").count(),
        2,
        "one output column per grouped select item: {sql}"
    );
}

/// A `GroupedSelectItem::Constant` (Exasol's "count the groups" literal
/// rewrite) reuses its already-rendered projection expression verbatim,
/// slotted into select-list order alongside the group-key and aggregate
/// columns — it contributes no aggregate plan and is not re-typed here.
#[test]
fn empty_grouped_sql_includes_constant_projection_column() {
    let select_items = vec![
        GroupedSelectItem::GroupKey {
            group_key_slot: 0,
            select_index: 0,
        },
        GroupedSelectItem::Constant {
            select_index: 1,
            projection: "CAST(NULL AS BOOLEAN)".to_string(),
        },
        GroupedSelectItem::Aggregate {
            plan_slot: 0,
            select_index: 2,
        },
    ];
    let group_key_types = vec!["DECIMAL(20,0)".to_string()];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
    let sql = resp["sql"].as_str().unwrap();
    let select_clause = sql
        .strip_prefix("SELECT ")
        .and_then(|s| s.split(" FROM").next())
        .unwrap();
    let columns: Vec<&str> = select_clause.split(", ").collect();
    assert_eq!(
        columns,
        vec![
            "CAST(NULL AS DECIMAL(20,0))",
            "CAST(NULL AS BOOLEAN)",
            "CAST(NULL AS DECIMAL(18,0))",
        ],
        "constant column is reused verbatim in select-list order: {sql}"
    );
}

/// Dispatch priority mirrors the non-empty path: grouped first, then
/// single-group aggregate (only when `validate_agg_col_types` passes), then
/// row scan.
#[test]
fn empty_result_sql_dispatches_by_plan_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(18,2)".to_string())];

    let grouped = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    });
    let grouped_sql =
        empty_result_sql(&grouped, &proj, &proj_types, false, &col_types).unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
    assert!(
        grouped_sql.contains("WHERE 1=0"),
        "grouped shape is zero rows: {grouped_sql}"
    );

    let single = serde_json::json!({
        "selectList": [agg_item("SUM", Some("amount"), false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    });
    let single_sql =
        empty_result_sql(&single, &proj, &proj_types, false, &col_types).unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
    assert!(
        single_sql.contains("FROM DUAL") && !single_sql.contains("WHERE 1=0"),
        "single-group shape is one row: {single_sql}"
    );
    assert!(single_sql.contains("CAST(NULL AS DECIMAL(36,2))"));

    // Non-numeric SUM target demotes to the row-scan empty shape (gate honored).
    let non_numeric = serde_json::json!({
        "selectList": [agg_item("SUM", Some("name"), false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    });
    let non_numeric_col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
    let row_sql = empty_result_sql(
        &non_numeric,
        &proj,
        &proj_types,
        false,
        &non_numeric_col_types,
    )
    .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        row_sql.contains("CAST(NULL AS DECIMAL(20,0))") && row_sql.contains(&quote_ident("ID")),
        "non-numeric single-group aggregate must fall through to the row-scan shape: {row_sql}"
    );
}

/// A grouped aggregate over a non-numeric column with all files pruned no longer
/// demotes to the full-row empty shape: since issue #82's fix, a grouped request
/// that cannot push down (here, a non-numeric SUM with no HAVING) routes on the
/// NON-empty path to the qualified single-table wrapper, whose output columns are
/// the `selectList` items. The empty path must MIRROR that shape — a zero-row
/// result typed per `selectListDataTypes` (the `selectList` column count/types),
/// NOT the full base row — so the empty and non-empty shapes never diverge.
#[test]
fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let grouped_non_numeric = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("SUM", Some("name"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    });

    let row_sql = empty_result_sql(&grouped_non_numeric, &proj, &proj_types, false, &col_types)
        .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate over zero files must produce the selectList-typed \
         empty shape (matching the qualified wrapper), not the full base row"
    );
}

/// A non-numeric grouped aggregate that also carries a HAVING no longer hard
/// errors: the classifier routes it to `GroupByWrapper` (the HAVING renders
/// natively over the wrapper rather than being dropped), so the empty path must
/// mirror the SAME selectList-typed empty shape as the no-HAVING sibling above,
/// not an `Err`.
#[test]
fn empty_files_grouped_non_numeric_aggregate_with_having_yields_typed_empty() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let grouped_having = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("SUM", Some("name"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
        "having": {"type": "predicate_greater"},
    });

    let row_sql = empty_result_sql(&grouped_having, &proj, &proj_types, false, &col_types).unwrap()
        ["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate with HAVING over zero files must produce the same \
         selectList-typed empty shape as the wrapper it now falls through to, not an error"
    );
}

/// A row-scan request whose derived projection WIDENED to the full base row is
/// routed on the non-empty path to the qualified single-table wrapper, whose
/// output columns are the `selectList` items (#196). The empty path must mirror
/// that shape — one `selectListDataTypes`-typed zero-row column — never the
/// wider full base row, whose column count trips Exasol's positional `04000`
/// check. The widening signal alone decides this: the identical request with a
/// non-widened projection still gets the full-row shape.
#[test]
fn empty_result_sql_widened_row_scan_uses_select_list_types() {
    let pushdown_req = serde_json::json!({
        "selectList": [
            {"type": "function_scalar", "name": "LENGTH", "arguments": [
                {"type": "column", "name": "SCORE", "tableName": "T"}]},
        ],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    });
    let col_types = vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    // No aggregate anywhere, so the shared classifier picks `RowScan` — the arm
    // under test, not the `GroupByWrapper` arm that already emits this shape.
    assert!(
        matches!(
            classify_request_shape(&pushdown_req, &col_types),
            RequestShape::RowScan
        ),
        "the fixture must classify as RowScan for this test to exercise its arm"
    );

    // The widened projection IS the full base row: three columns for one item.
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into(), "SCORE".into()];
    let proj_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();

    let widened = empty_result_sql(&pushdown_req, &proj, &proj_types, true, &col_types)
        .expect("the widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        widened, "SELECT CAST(NULL AS DECIMAL(18,0)) FROM DUAL WHERE 1=0",
        "a widened row-scan projection over zero files must produce ONE \
         selectListDataTypes-typed column, not the 3-column base row: {widened}"
    );

    let not_widened = empty_result_sql(&pushdown_req, &proj, &proj_types, false, &col_types)
        .expect("the non-widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        not_widened,
        empty_pushdown_sql(&proj, &proj_types)["sql"]
            .as_str()
            .unwrap(),
        "the non-widened path must stay byte-identical to the full-row empty \
         shape: {not_widened}"
    );
}

// ---------------------------------------------------------------------------
// Task 2.2 — `parse_name_mapping` flattens `schema.name-mapping.default`
// ---------------------------------------------------------------------------

/// A representative `schema.name-mapping.default` payload — mirroring the
/// Iceberg spec's own example shape — flattens to one `NameMappingEntry` per
/// TOP-LEVEL name. Multi-name entries expand to one entry per name (Avro field
/// aliases); an entry's nested `fields` children are excluded, but the entry's
/// OWN top-level name(s) are still included; an entry with no `field-id` at
/// all (schema-only, not present in imported files) is fully excluded.
#[test]
fn resolves_name_mapping_flat_entries_once() {
    let raw = r#"
    [
        { "field-id": 1, "names": ["id", "record_id"] },
        {
            "field-id": 3,
            "names": ["location"],
            "fields": [
                { "field-id": 4, "names": ["latitude", "lat"] },
                { "field-id": 5, "names": ["longitude", "long"] }
            ]
        },
        { "names": ["schema_only_no_field_id"] }
    ]
    "#;

    let entries = parse_name_mapping(Some(raw)).expect("valid name-mapping JSON must parse");

    assert_eq!(
        entries,
        vec![
            NameMappingEntry {
                name: "id".to_string(),
                field_id: 1,
            },
            NameMappingEntry {
                name: "record_id".to_string(),
                field_id: 1,
            },
            NameMappingEntry {
                name: "location".to_string(),
                field_id: 3,
            },
        ],
        "multi-name entry expands per name; nested `fields` children (lat/lat, \
         long/long) are excluded while the parent's own top-level name is kept; \
         the id-less entry is fully excluded"
    );
}

/// An absent `schema.name-mapping.default` property (`None`) yields an empty
/// mapping, not an error — a table with no name-mapping is the common,
/// fully-supported case.
#[test]
fn absent_name_mapping_is_empty() {
    assert_eq!(
        parse_name_mapping(None).expect("absent property must not error"),
        Vec::new()
    );
}

/// A present-but-malformed `schema.name-mapping.default` value fails loud with
/// a clean, credential-free plan-time error that names the offending property.
#[test]
fn malformed_name_mapping_errors_cleanly() {
    let err = parse_name_mapping(Some("{ not valid json mapping shape"))
        .expect_err("malformed name-mapping JSON must error");

    let msg = match err {
        UdfError::User(m) => m,
        other => panic!("expected UdfError::User, got {other:?}"),
    };
    assert!(
        msg.contains(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING),
        "error must name the offending property: {msg}"
    );
    assert!(
        !msg.contains("access_key") && !msg.contains("secret_key"),
        "error must not leak credentials: {msg}"
    );
}

/// A `loadTable` response body with `location` present but empty — an omitted
/// key fails deserialization earlier and never reaches the guard under test.
fn load_table_body_with_empty_location() -> String {
    serde_json::json!({
        "metadata-location": "s3://bucket/db/t/metadata/v1.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        }
    })
    .to_string()
}

/// SigV4 credentials: `resolve_file_list` issues exactly one catalog request
/// under these (no `/v1/config` lookup), so a single-shot loopback server suffices.
fn one_request_sigv4_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "signing-access-key".into(),
        secret_key: "signing-secret-key".into(),
        session_token: None,
        path_style: true,
        use_sigv4: true,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// Drive `resolve_file_list` against a single-shot loopback catalog serving
/// `body` as its one `loadTable` response, and answer with the EFFECTIVE storage
/// it resolved for `db.t`.
///
/// One response is enough because the credentials are SigV4: `CatalogSession::resolve`
/// contacts no catalog under them, so the `loadTable` GET is the only request issued.
async fn effective_storage_from_loopback_catalog(
    creds: &ConnectionCreds,
    body: String,
) -> Result<StorageBackend, UdfError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let port = listener.local_addr().expect("local_addr").port();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let _n = stream.read(&mut buf).await.expect("read");

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.expect("write");
    });

    let catalog_uri = format!("http://127.0.0.1:{port}");
    let session = CatalogSession::resolve(&catalog_uri, &creds.warehouse, creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let catalog = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: "db.t".into(),
    };

    let result = resolve_file_list(&session, &catalog, &sample_storage(), creds, true, None)
        .await
        .map(|(_, storage, ..)| storage);

    server
        .await
        .expect("the loopback catalog fake must serve its one response without panicking");

    result
}

/// Drive `resolve_file_list` against a single-shot loopback catalog that
/// answers `loadTable` with an empty table location.
async fn resolve_file_list_against_locationless_catalog(
    creds: &ConnectionCreds,
) -> Result<(), UdfError> {
    effective_storage_from_loopback_catalog(creds, load_table_body_with_empty_location())
        .await
        .map(|_| ())
}

/// A `loadTable` response with an empty table `location` is rejected as a
/// `UdfError::User`, with the identical message whether or not vended credentials
/// are requested.
#[tokio::test]
async fn absent_table_location_errors_on_both_vended_and_static_paths() {
    let static_creds = one_request_sigv4_creds();
    let mut vended_creds = one_request_sigv4_creds();
    vended_creds.use_vended_credentials = true;

    let vended_err = resolve_file_list_against_locationless_catalog(&vended_creds)
        .await
        .expect_err("vended path must reject a loadTable response with an empty location");
    let static_err = resolve_file_list_against_locationless_catalog(&static_creds)
        .await
        .expect_err("static path must reject a loadTable response with an empty location");

    let vended_message = match vended_err {
        UdfError::User(m) => m,
        other => panic!("vended path must fail as a user error, got {other:?}"),
    };
    let static_message = match static_err {
        UdfError::User(m) => m,
        other => panic!("static path must fail as a user error, got {other:?}"),
    };

    for (path, message) in [("vended", &vended_message), ("static", &static_message)] {
        assert!(
            message.contains("loadTable"),
            "{path}-path error must name the response the location is absent from: {message}"
        );
        assert!(
            message.contains("location"),
            "{path}-path error must name the absent location field: {message}"
        );
        assert!(
            message.contains("db.t"),
            "{path}-path error must name the table whose loadTable response was malformed: \
             {message}"
        );
        assert!(
            !message.contains("storage backend cannot be resolved"),
            "{path}-path error must not frame the failure as a vended-storage-backend \
             resolution failure: {message}"
        );
    }
    assert_eq!(
        vended_message, static_message,
        "both paths must surface the SAME absent-location error — the vended-credential \
         flag must not change how a malformed catalog response is diagnosed"
    );
}

/// The store address the CONNECTION configures, and the DIFFERENT one the catalog
/// vends for the very same table. Every value is distinct, so which source placed
/// the resolved store is readable off the resolved value alone.
const CONNECTION_ENDPOINT: &str = "https://connection-store.example.com";
const CONNECTION_REGION: &str = "eu-central-1";
const VENDED_ENDPOINT: &str = "https://vended-store.example.com";
const VENDED_REGION: &str = "us-west-2";
const VENDED_ACCESS_KEY: &str = "vended-access-key";
const VENDED_SECRET_KEY: &str = "vended-secret-key";
const VENDED_SESSION_TOKEN: &str = "vended-session-token";

/// A `loadTable` response vending a complete S3 credential set AND a store address
/// of its own, for a table whose metadata carries NO snapshot.
///
/// The absent snapshot is what keeps this a pure unit test: `TableScanBuilder::build`
/// answers an empty `TableScan` when `current_snapshot()` is `None`, so
/// `resolve_file_list` reaches its effective-storage decision and returns without
/// reading a single object from the store that address names.
fn load_table_body_vending_its_own_store_address() -> String {
    serde_json::json!({
        "metadata-location": "s3://bucket/db/t/metadata/v1.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000002",
            "location": "s3://bucket/db/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "snapshots": []
        },
        "config": {
            "s3.access-key-id": VENDED_ACCESS_KEY,
            "s3.secret-access-key": VENDED_SECRET_KEY,
            "s3.session-token": VENDED_SESSION_TOKEN,
            "client.region": VENDED_REGION,
            "s3.endpoint": VENDED_ENDPOINT
        }
    })
    .to_string()
}

/// Under vending, a CONNECTION-configured `endpoint` and `region` place the store
/// while the credentials still come from the catalog alone.
///
/// This is the only test at the layer that PERFORMS the split: `resolve_file_list`
/// is what narrows the CONNECTION down to a `StaticStoreAddress` before handing it
/// to the vended selector. Both vended E2E fixtures carry CONNECTIONs with an empty
/// `endpoint` and `region`, so neither can tell an address that came from the
/// CONNECTION from one that came from nowhere — substituting
/// `&StaticStoreAddress::default()` at that call site fails HERE and nowhere else.
#[tokio::test]
async fn vended_addressing_prefers_the_connection_endpoint_and_region() {
    let mut creds = one_request_sigv4_creds();
    creds.use_vended_credentials = true;
    creds.endpoint = CONNECTION_ENDPOINT.into();
    creds.region = CONNECTION_REGION.into();

    let storage = effective_storage_from_loopback_catalog(
        &creds,
        load_table_body_vending_its_own_store_address(),
    )
    .await
    .expect("a vended key pair over a snapshotless s3:// table must resolve a backend");

    let StorageBackend::S3(props) = storage else {
        panic!("an s3:// table location must resolve an S3 backend");
    };

    assert_eq!(
        props.endpoint, CONNECTION_ENDPOINT,
        "the CONNECTION's endpoint must place the store, not the vended {VENDED_ENDPOINT}"
    );
    assert_eq!(
        props.region, CONNECTION_REGION,
        "the CONNECTION's region must place the store, not the vended {VENDED_REGION}"
    );
    assert_eq!(
        props.access_key, VENDED_ACCESS_KEY,
        "the access key must come from the catalog alone: the CONNECTION reaches this \
         resolution as an ADDRESS, and must never supply a storage credential"
    );
    assert_eq!(
        props.secret_key, VENDED_SECRET_KEY,
        "the secret key must come from the catalog alone: the CONNECTION reaches this \
         resolution as an ADDRESS, and must never supply a storage credential"
    );
    assert_eq!(
        props.session_token.as_deref(),
        Some(VENDED_SESSION_TOKEN),
        "the vended session token must reach the effective storage the scan reads with"
    );
}
