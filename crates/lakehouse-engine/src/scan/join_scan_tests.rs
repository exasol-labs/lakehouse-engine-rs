use super::*;
use crate::scan::raw_scan::register_nested_json_render_udf;
use crate::scan::spec::{JoinSpec, JoinType};
use crate::scan::test_support::minimal_spec;

/// A non-nested incompatible column (e.g. `Binary`) reaching the join select
/// list must keep the byte-identical `CAST(col AS VARCHAR)` this path has
/// always emitted — only the five nested types `needs_nested_json_rendering`
/// owns divert to the JSON render function.
#[test]
fn render_join_select_item_keeps_a_non_nested_incompatible_column_cast_unchanged() {
    let combined = vec![("PAYLOAD".to_string(), arrow::datatypes::DataType::Binary)];
    let item = ProjectionItem::Column("PAYLOAD".into());

    let rendered = render_join_select_item(&item, &combined);

    assert_eq!(rendered, "CAST(\"PAYLOAD\" AS VARCHAR)");
}

/// A nested column reaching the join select list is rendered by name through
/// the SAME JSON encoder the single-table legacy path uses, never cast to
/// Arrow display text.
#[test]
fn render_join_select_item_diverts_a_nested_column_to_the_json_render_function() {
    let list_type = arrow::datatypes::DataType::List(std::sync::Arc::new(
        arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Utf8, true),
    ));
    let combined = vec![("TAGS".to_string(), list_type)];
    let item = ProjectionItem::Column("TAGS".into());

    let rendered = render_join_select_item(&item, &combined);

    assert_eq!(
        rendered,
        format!("{NESTED_JSON_RENDER_UDF_NAME}(\"TAGS\")"),
        "a nested column must be routed through the JSON render function, not CAST"
    );
}

/// End-to-end: a nested column joined through the legacy (no-logical-schema)
/// broadcast join path renders as strict JSON, not the `List → Utf8` Arrow
/// display-text cast.
#[tokio::test]
async fn build_join_sql_renders_a_nested_column_as_valid_json_end_to_end() {
    use arrow::array::{Array, Int64Array, ListBuilder, StringArray, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;

    let dim_schema = Arc::new(Schema::new(vec![Field::new(
        "d_key",
        DataType::Int64,
        false,
    )]));
    let dim_batch = RecordBatch::try_new(
        dim_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64]))],
    )
    .unwrap();
    let dim_table = MemTable::try_new(dim_schema, vec![vec![dim_batch]]).unwrap();

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("hello");
    tags_builder.values().append_value("world");
    tags_builder.append(true);
    let tags = tags_builder.finish();

    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("f_key", DataType::Int64, false),
        Field::new("tags", tags.data_type().clone(), true),
    ]));
    let fact_batch = RecordBatch::try_new(
        fact_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64])), Arc::new(tags)],
    )
    .unwrap();
    let fact_table = MemTable::try_new(fact_schema, vec![vec![fact_batch]]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table(JOIN_DIM_TABLE, Arc::new(dim_table))
        .unwrap();
    ctx.register_table(JOIN_FACT_TABLE, Arc::new(fact_table))
        .unwrap();
    register_nested_json_render_udf(&ctx);

    let mut spec = minimal_spec();
    let storage = spec.common.storage.clone();
    spec.common.join = Some(JoinSpec {
        table_root: String::new(),
        files: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"D_KEY\" = \"F_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage,
    });

    let sql = build_join_sql(&ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, &spec)
        .await
        .expect("build_join_sql");
    let df = ctx.sql(&sql).await.expect("plan join SQL");
    let batches = df.collect().await.expect("collect");

    let mut rendered_tags: Option<String> = None;
    for batch in &batches {
        let tags_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("TAGS must arrive as Utf8, not its physical list type");
        for row in 0..batch.num_rows() {
            if !tags_col.is_null(row) {
                rendered_tags = Some(tags_col.value(row).to_string());
            }
        }
    }

    assert_eq!(
        rendered_tags.as_deref(),
        Some(r#"["hello","world"]"#),
        "a nested column reached through the legacy join path must render as \
         strict JSON, not Arrow display text"
    );
}
