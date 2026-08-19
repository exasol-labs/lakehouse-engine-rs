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

/// The per-table Parquet read options are decided by the presence of a nested
/// member tree, and every table keeps the INT96 coercion either way.
///
/// A table carrying a JSON-rendered nested column reads WITHOUT row-filter
/// pushdown, because DataFusion would approve the pushdown against the `utf8`
/// logical tag and then drop the conjunct against the physical nested type,
/// applying it nowhere. Statistics, page-index, and bloom-filter pruning stay ON
/// for BOTH tables: they cannot prune on the rendered column (proven in
/// `tests/scan_parquet_pruning.rs`), so disabling them would only cost the
/// table's primitive columns their pruning.
#[test]
fn a_nested_carrying_table_reads_without_row_filter_pushdown() {
    use crate::scan::spec::NestedMembers;

    let resolution = |nested: &[(&str, NestedMembers)]| FieldIdResolution {
        name_mapping: Vec::new(),
        declared_physical_names: std::collections::HashMap::new(),
        defaults: std::collections::HashMap::new(),
        nested_members: nested
            .iter()
            .map(|(name, members)| (name.to_string(), members.clone()))
            .collect(),
    };

    let primitive_only = scan_table_parquet_format(&resolution(&[]));
    assert!(
        primitive_only.options().global.pushdown_filters,
        "a table with no nested column must keep Parquet row-filter pushdown"
    );

    let nested = scan_table_parquet_format(&resolution(&[(
        "tags",
        NestedMembers::List { element: None },
    )]));
    assert!(
        !nested.options().global.pushdown_filters,
        "a table carrying a nested column must read without row-filter pushdown"
    );

    for format in [&primitive_only, &nested] {
        let global = &format.options().global;
        assert!(global.pruning, "row-group statistics pruning stays enabled");
        assert!(global.enable_page_index, "page-index pruning stays enabled");
        assert!(
            global.bloom_filter_on_read,
            "bloom-filter probing stays enabled"
        );
        assert_eq!(
            global.coerce_int96,
            Some("us".to_string()),
            "every decode format keeps the INT96 coercion of its base"
        );
    }
}

/// The session-level `pushdown_filters` flag is withheld for a scan that renders
/// a nested column on EITHER side, and only for such a scan.
///
/// It has to be: `ParquetSource::try_pushdown_filters` ORs the session flag with
/// the table's own, so a session-level `true` would re-enable the pushdown for the
/// very table `scan_table_parquet_format` withheld it from. Leaving it off at
/// session level and opting each table back in is what keeps the decision per
/// table — the non-nested side of a broadcast join keeps its pushdown.
#[test]
fn the_session_withholds_pushdown_only_for_a_scan_that_renders_nested_json() {
    use crate::scan::spec::{JoinSpec, JoinType, LogicalField, NestedMembers};

    let field = |name: &str, nested: Option<NestedMembers>| LogicalField {
        field_id: None,
        name: name.into(),
        arrow_type: "utf8".into(),
        nullable: true,
        initial_default: None,
        nested,
        physical_name: None,
    };
    let pushdown_enabled = |spec: &ScanSpec| {
        session_config_for_spec(spec)
            .options()
            .execution
            .parquet
            .pushdown_filters
    };

    let mut spec = minimal_spec();
    spec.common.logical_schema = vec![field("name", None)];
    assert!(
        pushdown_enabled(&spec),
        "a logical schema of primitive columns keeps the session-level pushdown"
    );

    spec.common.logical_schema = vec![
        field("name", None),
        field("tags", Some(NestedMembers::List { element: None })),
    ];
    assert!(
        !pushdown_enabled(&spec),
        "a scan rendering a nested column must not enable pushdown at session level"
    );

    let mut join_spec = minimal_spec();
    join_spec.common.logical_schema = vec![field("name", None)];
    join_spec.common.join = Some(JoinSpec {
        table_root: "s3://test-bucket/dim".into(),
        files: vec![crate::scan::spec::FileEntry::new(
            "s3://test-bucket/dim/part-0.parquet",
            512,
        )],
        logical_schema: vec![field("tags", Some(NestedMembers::List { element: None }))],
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: r#""F_KEY" = "D_KEY""#.into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: join_spec.common.storage.clone(),
    });
    assert!(
        !pushdown_enabled(&join_spec),
        "a nested column on the DIMENSION side must withhold the session-level pushdown too"
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

/// Scenario: scan without a logical schema falls back to first-file inference.
///
/// The fallback is selected by the ABSENCE of a logical schema ALONE: a spec whose
/// logical schema IS present still installs the column-binding adapter even when
/// every field binds by identity (no field-id, no declared physical name). The
/// observable difference from inference is what this asserts — the DECLARED schema
/// becomes the table schema, and a declared column absent from the file NULL-fills
/// instead of being unknown to the query.
#[tokio::test]
async fn a_logical_schema_of_identity_fields_still_installs_the_binding_adapter() {
    use crate::scan::spec::LogicalField;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::ArrowWriter;

    // A file written with NO field-id metadata at all, as a Delta `none`
    // column-mapping table's files are.
    let dir = std::env::temp_dir().join(format!("lh_identity_binding_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("identity.parquet");

    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, true),
    ]));
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, physical_schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    // Every field binds by identity, and `added` is absent from the file.
    let identity_field = |name: &str, nullable: bool| LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: "int64".to_string(),
        nullable,
        initial_default: None,
        nested: None,
        physical_name: None,
    };
    let mut spec = minimal_spec();
    let file_size = local_file_size(&file_url);
    spec.files = vec![FileEntry::new(file_url, file_size)];
    spec.common.logical_schema = vec![
        identity_field("id", false),
        identity_field("val", true),
        identity_field("added", true),
    ];
    spec.common.projection = vec!["ID".into(), "VAL".into(), "ADDED".into()];

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec)
        .await
        .expect("register_files must succeed for an all-identity logical schema");

    // The DECLARED schema is the table schema — first-file inference would have
    // registered the file's own two columns instead.
    let registered = ctx
        .table("scan_target")
        .await
        .expect("scan_target must be registered");
    assert_eq!(
        registered
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "val", "added"],
        "the logical schema must be registered, not one inferred from the file"
    );

    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df.collect().await.expect("identity binding must read rows");

    let mut rows: Vec<(i64, i64, bool)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val column is Int64");
        let added = batch.column(2);
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), vals.value(row), added.is_null(row)));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        rows,
        vec![(1, 10, true), (2, 20, true), (3, 30, true)],
        "identity-bound columns must read their real values, and the declared \
         column absent from the file must NULL-fill"
    );

    let _ = std::fs::remove_dir_all(&dir);
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
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
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

/// Scenario (delta-type-mapping): a Delta type Exasol cannot represent natively is
/// surfaced as a VARCHAR rendering.
///
/// The classifier tags a mappable `array<E>` column `utf8`, so the LOGICAL schema
/// declares `Utf8` while the physical Parquet column is a real `List(Int32)`.
/// `build_scan_sql` emits NO `CAST(... AS VARCHAR)` for a logically-`Utf8` column,
/// so the physical-to-logical adaptation can only come from the scan's OWN
/// [`FieldIdExprAdapter`] — not from DataFusion's default schema adapter, which
/// this provider never installs. That link is what this asserts. The available
/// `List → Utf8` cast produces Arrow display text, which is not JSON.
///
/// The logical field declares its nested member tree, which is the ONE signal the
/// diversion is keyed on — the same one the Parquet row-filter-pushdown withdrawal
/// reads, so a rendered column can never keep a pushdown that would drop a predicate
/// over it.
///
/// The column binds by field-id across a physical-name divergence (Delta `id`
/// column mapping), so the rename and the rendering have to compose: the outer
/// rewrite must restore the physical name UNDER the rendering expression for the
/// opener's name-based lookups.
///
/// NULL and empty lists are covered because the three render differently and the
/// distinction is observable in Exasol: a NULL array must stay NULL rather than
/// collapse to `[]` or the empty string.
#[tokio::test]
async fn a_list_column_tagged_utf8_is_json_rendered_by_the_field_id_expression_adapter() {
    use crate::scan::spec::LogicalField;
    use arrow::array::{Array, Int64Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use std::collections::HashMap;

    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);

    // Physical file: an obfuscated Delta `id`-mapping physical name over a real
    // List(Int32) column, with one populated, one NULL, and one empty list.
    let dir = std::env::temp_dir().join("lh_list_utf8_cast");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("list.parquet");

    let lists = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
        Some(vec![]),
    ]);
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_meta(1)),
        Field::new("col-8f0a", lists.data_type().clone(), true).with_metadata(field_id_meta(2)),
    ]));
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, physical_schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(lists),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    // Logical schema: field-id 2 is the current name `arr_col`, tagged `utf8` —
    // exactly what the Delta classifier emits for `array<integer>`.
    let mut spec = minimal_spec();
    let file_size = local_file_size(&file_url);
    spec.files = vec![FileEntry::new(file_url, file_size)];
    spec.common.logical_schema = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "arr_col".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: Some(crate::scan::spec::NestedMembers::List { element: None }),
            physical_name: None,
        },
    ];
    spec.common.projection = vec!["ID".into(), "ARR_COL".into()];

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec)
        .await
        .expect("register_files must succeed for a utf8-tagged list column");
    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    assert!(
        !sql.contains("CAST("),
        "build_scan_sql must add no SQL cast for a logically-Utf8 column, leaving \
         the physical-to-logical adaptation entirely to the expression adapter: {sql}"
    );

    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df
        .collect()
        .await
        .expect("the expression adapter must render the physical list as the logical Utf8");

    let mut rows: Vec<(i64, Option<String>)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let rendered = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the list column must arrive as Utf8, not as its physical list type");
        for row in 0..batch.num_rows() {
            let value = (!rendered.is_null(row)).then(|| rendered.value(row).to_string());
            rows.push((ids.value(row), value));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, Some("[1,2,3]".to_string())),
            (2, None),
            (3, Some("[]".to_string())),
        ],
        "a populated array must render as strict JSON, an empty array as `[]`, and a \
         NULL array must stay NULL"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (nested-json-rendering): a `struct` and a `map` column are diverted
/// around the physical-to-logical cast too — the two arrow-cast cannot convert to
/// text AT ALL, so before the diversion the scan failed outright rather than
/// returning display text.
///
/// Read end to end through the Parquet opener, because that is where the diversion
/// has to hold: the opener resolves the wrapped column by NAME against the real
/// physical file schema, so a wrapper that lost the physical name would silently
/// project the column away instead of reading it. The members bind by their DECLARED
/// physical names — the Delta `name` column-mapping shape — so the rendered documents
/// prove the JSON is keyed by the table's logical names and not the file's opaque
/// ones.
#[tokio::test]
async fn struct_and_map_columns_render_as_json_through_the_parquet_opener() {
    use crate::scan::spec::{LogicalField, NestedField, NestedMembers};
    use arrow::array::{
        Array, ArrayRef, Int64Array, MapBuilder, StringArray, StringBuilder, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use std::collections::HashMap;

    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);

    let addr = StructArray::try_new(
        Fields::from(vec![
            Arc::new(Field::new("col-street", DataType::Utf8, true)),
            Arc::new(Field::new("col-city", DataType::Utf8, true)),
        ]),
        vec![
            Arc::new(StringArray::from(vec![Some("Main St"), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("Berlin"), None])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![true, false])),
    )
    .expect("struct array");

    let mut attrs_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    attrs_builder.keys().append_value("a");
    attrs_builder.values().append_value("1");
    attrs_builder.append(true).expect("populated map row");
    attrs_builder.append(false).expect("null map row");
    let attrs = attrs_builder.finish();

    let dir = std::env::temp_dir().join("lh_struct_map_render");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("nested.parquet");
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_meta(1)),
        Field::new("col-addr", addr.data_type().clone(), true).with_metadata(field_id_meta(2)),
        Field::new("col-attrs", attrs.data_type().clone(), true).with_metadata(field_id_meta(3)),
    ]));
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, physical_schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(addr),
                Arc::new(attrs),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    let nested_field = |name: &str, physical_name: &str| NestedField {
        field_id: None,
        name: name.to_string(),
        physical_name: Some(physical_name.to_string()),
        nested: None,
    };
    let logical_field = |field_id: i32, name: &str, nested: Option<NestedMembers>| LogicalField {
        field_id: Some(field_id),
        name: name.to_string(),
        arrow_type: "utf8".to_string(),
        nullable: true,
        initial_default: None,
        nested,
        physical_name: None,
    };

    let mut spec = minimal_spec();
    let file_size = local_file_size(&file_url);
    spec.files = vec![FileEntry::new(file_url, file_size)];
    spec.common.logical_schema = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        logical_field(
            2,
            "addr",
            Some(NestedMembers::Struct {
                fields: vec![
                    nested_field("street", "col-street"),
                    nested_field("city", "col-city"),
                ],
            }),
        ),
        logical_field(
            3,
            "attrs",
            Some(NestedMembers::Map {
                key: None,
                value: None,
            }),
        ),
    ];
    spec.common.projection = vec!["ID".into(), "ADDR".into(), "ATTRS".into()];

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec)
        .await
        .expect("register_files must succeed for utf8-tagged struct and map columns");
    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    assert!(
        !sql.contains("CAST("),
        "build_scan_sql must add no SQL cast for a logically-Utf8 column: {sql}"
    );

    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df
        .collect()
        .await
        .expect("the expression adapter must render struct and map columns as JSON");

    let column = |batch: &RecordBatch, index: usize, row: usize| {
        let rendered = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a nested column must arrive as Utf8, not as its physical nested type");
        (!rendered.is_null(row)).then(|| rendered.value(row).to_string())
    };
    let mut rows: Vec<(i64, Option<String>, Option<String>)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), column(batch, 1, row), column(batch, 2, row)));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (
                1,
                Some(r#"{"street":"Main St","city":"Berlin"}"#.to_string()),
                Some(r#"{"a":"1"}"#.to_string()),
            ),
            (2, None, None),
        ],
        "a struct must render keyed by the TABLE's member names, a map as one JSON \
         object, and a NULL nested value must stay NULL"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (nested-json-rendering): the legacy no-logical-schema path routes a
/// nested column through the SAME JSON encoder the field-id path uses, instead of
/// the `List → Utf8` display-text cast that answers `needs_json_fallback`.
#[tokio::test]
async fn build_scan_sql_diverts_a_nested_column_to_the_json_render_function() {
    use arrow::array::{Array, Int64Array, ListBuilder, StringArray, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("hello");
    tags_builder.values().append_value("world");
    tags_builder.append(true);
    let tags = tags_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", tags.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64])), Arc::new(tags)],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("scan_target", Arc::new(table)).unwrap();
    register_nested_json_render_udf(&ctx);

    let mut spec = minimal_spec();
    spec.common.projection = vec!["ID".into(), "TAGS".into()];

    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    assert!(
        sql.contains(&format!("{NESTED_JSON_RENDER_UDF_NAME}(\"TAGS\")")),
        "a nested column must be routed through the JSON render function: {sql}"
    );
    assert!(
        !sql.contains("CAST(\"TAGS\""),
        "a nested column must not fall back to the display-text CAST: {sql}"
    );

    let df = ctx.sql(&sql).await.expect("plan scan SQL");
    let batches = df.collect().await.expect("collect");
    let mut rendered: Option<String> = None;
    for batch in &batches {
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("TAGS must arrive as Utf8, not its physical list type");
        for row in 0..batch.num_rows() {
            if !col.is_null(row) {
                rendered = Some(col.value(row).to_string());
            }
        }
    }
    assert_eq!(
        rendered.as_deref(),
        Some(r#"["hello","world"]"#),
        "the legacy path must render strict JSON, not Arrow display text"
    );
}

/// A non-nested incompatible column (e.g. `Binary`) must keep emitting
/// `CAST(col AS VARCHAR)` byte-identical to before — only the five nested types
/// `needs_nested_json_rendering` owns divert to the JSON render function.
#[tokio::test]
async fn build_scan_sql_keeps_a_non_nested_incompatible_column_cast_unchanged() {
    use arrow::array::BinaryArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BinaryArray::from(vec![Some(b"hi".as_slice())]))],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("scan_target", Arc::new(table)).unwrap();

    let mut spec = minimal_spec();
    spec.common.projection = vec!["PAYLOAD".into()];

    let sql = build_scan_sql(&ctx, "scan_target", &spec)
        .await
        .expect("build_scan_sql");
    assert!(
        sql.contains("CAST(\"PAYLOAD\" AS VARCHAR)"),
        "a non-nested incompatible column must keep the byte-identical CAST: {sql}"
    );
}

#[tokio::test]
async fn inferred_schema_path_renders_nested_columns_through_the_same_encoder() {
    use crate::scan::spec::{LogicalField, NestedMembers};
    use arrow::array::{Array, Int64Array, ListBuilder, StringArray, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use std::collections::HashMap;

    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("hello");
    tags_builder.values().append_value("world");
    tags_builder.append(true);
    let tags = tags_builder.finish();

    let dir = std::env::temp_dir().join("lh_shared_encoder_paths");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("shared.parquet");
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_meta(1)),
        Field::new("tags", tags.data_type().clone(), true).with_metadata(field_id_meta(2)),
    ]));
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, physical_schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            physical_schema,
            vec![Arc::new(Int64Array::from(vec![1i64])), Arc::new(tags)],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();
    let file_size = local_file_size(&file_url);

    async fn rendered_tags(ctx: &SessionContext, sql: &str) -> Option<String> {
        let df = ctx.sql(sql).await.expect("plan scan SQL");
        let batches = df.collect().await.expect("collect");
        let mut value = None;
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            let rendered = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("tags must arrive as Utf8, not its physical list type");
            for row in 0..batch.num_rows() {
                if ids.value(row) == 1 && !rendered.is_null(row) {
                    value = Some(rendered.value(row).to_string());
                }
            }
        }
        value
    }

    // Logical-schema path: the field-id adapter substitutes `NestedJsonRenderExpr`
    // for the physical list column at the physical-plan level.
    let mut logical_spec = minimal_spec();
    logical_spec.files = vec![FileEntry::new(file_url.clone(), file_size)];
    logical_spec.common.logical_schema = vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "tags".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
            nested: Some(NestedMembers::List { element: None }),
            physical_name: None,
        },
    ];
    logical_spec.common.projection = vec!["ID".into(), "TAGS".into()];

    let logical_ctx = SessionContext::new_with_config(session_config_for_spec(&logical_spec));
    register_files(&logical_ctx, "scan_target", &logical_spec)
        .await
        .expect("register_files must succeed for the logical-schema path");
    let logical_sql = build_scan_sql(&logical_ctx, "scan_target", &logical_spec)
        .await
        .expect("build_scan_sql");
    assert!(
        !logical_sql.contains("CAST(") && !logical_sql.contains(NESTED_JSON_RENDER_UDF_NAME),
        "the logical-schema path must route through the expression adapter, not a SQL-level \
         cast or UDF call: {logical_sql}"
    );
    let logical_rendered = rendered_tags(&logical_ctx, &logical_sql).await;

    // Legacy path: no logical schema, so the registered table reports the real
    // physical List type and `build_scan_sql` routes it through the SQL-level
    // `NESTED_JSON_RENDER_UDF_NAME` call instead.
    let mut legacy_spec = minimal_spec();
    legacy_spec.files = vec![FileEntry::new(file_url, file_size)];
    legacy_spec.common.logical_schema = Vec::new();
    legacy_spec.common.projection = vec!["ID".into(), "TAGS".into()];

    let legacy_ctx = SessionContext::new_with_config(session_config_for_spec(&legacy_spec));
    register_files(&legacy_ctx, "scan_target", &legacy_spec)
        .await
        .expect("register_files must succeed for the legacy no-logical-schema path");
    register_nested_json_render_udf(&legacy_ctx);
    let legacy_sql = build_scan_sql(&legacy_ctx, "scan_target", &legacy_spec)
        .await
        .expect("build_scan_sql");
    assert!(
        legacy_sql.contains(&format!("{NESTED_JSON_RENDER_UDF_NAME}(\"TAGS\")")),
        "the legacy path must route through the SQL-level JSON render function call: {legacy_sql}"
    );
    let legacy_rendered = rendered_tags(&legacy_ctx, &legacy_sql).await;

    assert_eq!(
        logical_rendered.as_deref(),
        Some(r#"["hello","world"]"#),
        "the logical-schema path must render strict JSON for the populated list"
    );
    assert_eq!(
        logical_rendered, legacy_rendered,
        "both paths call the same render_nested_column_as_json encoder, so their \
         rendered JSON for identical underlying data must be byte-identical"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
