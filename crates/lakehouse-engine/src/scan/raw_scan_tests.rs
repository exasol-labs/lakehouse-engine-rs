use super::*;
use crate::scan::session_config_for_spec;
use crate::scan::test_support::{local_file_size, minimal_spec};

/// Both INT96 call sites (`positional_deletes.rs`'s decode path and this
/// module's legacy schema-inference branch) build their `ParquetFormat` via
/// the SAME shared [`int96_coerced_parquet_format`] helper, so asserting the
/// helper's own output once is sufficient to guard against the two sites
/// drifting apart (a divergence between inferred and decoded time units would
/// be a schema mismatch).
#[test]
fn both_parquet_format_sites_coerce_int96_us_utc() {
    let format = int96_coerced_parquet_format();
    assert_eq!(
        format.coerce_int96(),
        Some("us".to_string()),
        "coerce_int96 must coerce INT96 timestamps to microsecond resolution"
    );
    assert_eq!(
        format.options().global.coerce_int96_tz,
        Some("UTC".to_string()),
        "coerce_int96_tz must treat coerced INT96 instants as UTC"
    );
}

/// A malformed/hand-crafted `ScanSpec` with `s3_max_connections: 0` must not
/// deadlock every delete-file read via `Semaphore::new(0)`.
#[test]
fn delete_path_read_limiter_clamps_zero_connections_to_one() {
    let mut spec = minimal_spec();
    spec.common.s3_max_connections = 0;
    assert_eq!(delete_path_read_limiter(&spec).available_permits(), 1);
}

/// Scenario: scan without a logical schema falls back to first-file inference.
///
/// When `spec.common.logical_schema` is empty (legacy or unset), `register_files`
/// must infer the Arrow schema from the first file and register the table
/// without installing the field-id adapter. The registered table must be
/// queryable and return all rows written to the file.
#[tokio::test]
async fn register_files_falls_back_without_logical_schema() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    // Write a minimal local Parquet file.
    let dir = std::env::temp_dir().join(format!("lh_fallback_inference_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("fallback.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, true),
    ]));
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![Some(10i64), Some(20), None])),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    // Build a spec with empty logical_schema — the fallback inference path.
    // Absolute file:// entry (empty table_root) exercises the passthrough
    // reconstruction branch; the real file size is supplied because the
    // provider builds each file's ObjectMeta from it (no-HEAD design).
    let mut spec = minimal_spec();
    let file_size = local_file_size(&file_url);
    spec.files = vec![FileEntry::new(file_url, file_size)];
    spec.common.logical_schema = Vec::new();

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec)
        .await
        .expect("register_files must succeed on first-file inference path");

    // The table must be registered and queryable.
    let table = ctx
        .table("scan_target")
        .await
        .expect("scan_target must be registered after register_files");
    let schema = table.schema();
    assert_eq!(
        schema.fields().len(),
        2,
        "inferred schema must have 2 fields; got {:?}",
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
    );
}

/// Task B4 (scenario `topn: Ordered top-N preserves descending and NULL ordering`):
/// `build_scan_sql` renders a pushed-down ORDER BY through the SAME shared
/// `render_order_by_clause` the adapter's outer merge uses, so per-shard and
/// merge agree on direction AND explicit NULL placement. Over a local Parquet
/// file whose sort column carries NULLs, a DESC sort yields a bounded,
/// correctly-ordered result, and flipping ONLY the `nulls_last` flag moves the
/// NULLs from the head to the tail — proving the NULL placement is honored
/// explicitly, not left to a DataFusion default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_scan_sql_preserves_desc_and_null_placement() {
    use crate::scan::spec::SortKey;
    use arrow::array::{Array, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::ArrowWriter;

    // price is nullable with NULLs interleaved among descending-comparable values.
    let dir = std::env::temp_dir().join(format!("lh_topn_nulls_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("topn.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, true),
    ]));
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    None,
                    Some(30.0),
                    Some(20.0),
                    None,
                ])),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    // Collect the (id, Option<price>) rows build_scan_sql produces for a given
    // sort direction / NULL placement / limit, IN PLAN ORDER (no test-side re-sort).
    async fn topn_rows(
        file_url: &str,
        ascending: bool,
        nulls_last: bool,
        limit: u64,
    ) -> Vec<(i64, Option<f64>)> {
        let mut spec = minimal_spec();
        let file_size = local_file_size(file_url);
        spec.files = vec![FileEntry::new(file_url, file_size)];
        spec.common.projection = vec!["ID".into(), "PRICE".into()];
        spec.common.order_by = vec![SortKey {
            column: "PRICE".into(),
            ascending,
            nulls_last,
        }];
        spec.common.limit = Some(limit);

        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register local parquet");
        let sql = build_scan_sql(&ctx, "scan_target", &spec)
            .await
            .expect("build_scan_sql");
        let df = ctx.sql(&sql).await.expect("plan scan SQL");
        let batches = df.collect().await.expect("collect");
        let mut rows: Vec<(i64, Option<f64>)> = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("col 0 Int64 (ID)");
            let price_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("col 1 Float64 (PRICE)");
            for r in 0..batch.num_rows() {
                let p = if price_col.is_null(r) {
                    None
                } else {
                    Some(price_col.value(r))
                };
                rows.push((id_col.value(r), p));
            }
        }
        rows
    }

    // DESC + NULLS FIRST, bounded to 3: the two NULLs rank first, then the max.
    let desc_nulls_first = topn_rows(&file_url, false, false, 3).await;
    assert_eq!(
        desc_nulls_first.len(),
        3,
        "LIMIT 3 must bound the result: {desc_nulls_first:?}"
    );
    assert!(
        desc_nulls_first[0].1.is_none() && desc_nulls_first[1].1.is_none(),
        "DESC NULLS FIRST must rank NULLs first: {desc_nulls_first:?}"
    );
    assert_eq!(
        desc_nulls_first[2].1,
        Some(30.0),
        "after the NULLs the largest value comes next: {desc_nulls_first:?}"
    );

    // DESC + NULLS LAST, bounded to 3: flipping ONLY the NULL flag moves the NULLs
    // to the tail, so the top-3 are the descending non-NULL values.
    let desc_nulls_last = topn_rows(&file_url, false, true, 3).await;
    assert_eq!(
        desc_nulls_last.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
        vec![Some(30.0), Some(20.0), Some(10.0)],
        "DESC NULLS LAST must rank non-NULLs descending ahead of NULLs: {desc_nulls_last:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Task 4.3: `build_scan_sql`'s uppercase-alias inner-SELECT wrapper works
/// unchanged over a registered logical (current-name) schema — the table
/// schema DataFusion sees is the logical one, so aliases and projection
/// resolve against the current names.
#[tokio::test]
async fn build_scan_sql_aliases_over_logical_schema() {
    use crate::scan::spec::LogicalField;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;

    let logical = vec![
        LogicalField {
            field_id: 1,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
        },
        LogicalField {
            field_id: 2,
            name: "rating".to_string(),
            arrow_type: "float64".to_string(),
            nullable: true,
            initial_default: None,
        },
    ];
    let logical_schema = build_logical_arrow_schema(&logical);

    // Register the logical schema as the table schema (as register_files
    // does via with_schema), with no rows — build_scan_sql only reads the
    // advertised schema.
    let ctx = SessionContext::new();
    let table = MemTable::try_new(logical_schema.clone(), vec![vec![]]).unwrap();
    ctx.register_table("scan_target", Arc::new(table)).unwrap();

    let mut spec = minimal_spec();
    spec.common.projection = vec!["ID".into(), "RATING".into()];
    spec.common.logical_schema = logical;

    let sql = build_scan_sql(&ctx, "scan_target", &spec).await.unwrap();

    // Inner SELECT aliases each current (lowercase) name to its uppercase form.
    assert!(
        sql.contains(r#""id" AS "ID""#) && sql.contains(r#""rating" AS "RATING""#),
        "inner SELECT must alias current names to uppercase: {sql}"
    );
    // Outer projection references the uppercase aliases.
    assert!(
        sql.contains(r#""ID""#) && sql.contains(r#""RATING""#),
        "outer projection must use uppercase aliases: {sql}"
    );
}

/// A bare column projected alongside an unaliased `CAST` of that SAME
/// column (e.g. `SELECT id, CAST(id AS VARCHAR(2000000)) ...`, issue
/// #136's select-list shape) must not trip DataFusion's "duplicate
/// projection name" check — each `build_scan_sql` select item carries
/// its own explicit positional alias precisely to prevent this.
#[tokio::test]
async fn build_scan_sql_disambiguates_column_and_cast_of_same_column() {
    use crate::scan::spec::ProjectionItem;
    use datafusion::arrow::array::Int64Array;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;

    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int64,
            false,
        ),
    ]));
    let ctx = SessionContext::new();
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("scan_target", Arc::new(table)).unwrap();

    let mut spec = minimal_spec();
    spec.common.projection = vec![
        ProjectionItem::Column("ID".into()),
        ProjectionItem::Expr {
            expr: r#"CAST("ID" AS VARCHAR)"#.into(),
        },
    ];

    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    let df = ctx
        .sql(&sql)
        .await
        .expect("plan scan SQL must not hit a duplicate projection name");
    let batches = df.collect().await.expect("collect");
    assert!(
        batches.iter().any(|b| b.num_rows() > 0),
        "test must exercise at least one actual row"
    );
    assert!(
        batches
            .iter()
            .all(|b| b.column(0).as_any().downcast_ref::<Int64Array>().is_some()),
        "column 0 must remain the bare ID column, unaffected by the alias"
    );
}
