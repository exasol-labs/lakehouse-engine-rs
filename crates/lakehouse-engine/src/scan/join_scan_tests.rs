use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, Int64Builder, ListBuilder,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;

use super::*;
use crate::scan::raw_scan::register_nested_json_render_udf;
use crate::scan::spec::{JoinSpec, JoinType, SortKey};
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
    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("hello");
    tags_builder.values().append_value("world");
    tags_builder.append(true);
    let ctx = join_session(fact_batch_with("tags", Arc::new(tags_builder.finish())));

    let batches = run_join_sql(&ctx, &post_join_spec(Vec::new(), None)).await;

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

/// A session whose dimension side is one row (`d_key = 1`) and whose fact side is
/// `fact`, both registered under the join scan's table names, with the JSON render
/// function the join select list names.
fn join_session(fact: RecordBatch) -> SessionContext {
    let dim_schema = Arc::new(Schema::new(vec![Field::new(
        "d_key",
        DataType::Int64,
        false,
    )]));
    let dim = RecordBatch::try_new(
        dim_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64]))],
    )
    .unwrap();
    let dim_table = MemTable::try_new(dim_schema, vec![vec![dim]]).unwrap();
    let fact_table = MemTable::try_new(fact.schema(), vec![vec![fact]]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table(JOIN_DIM_TABLE, Arc::new(dim_table))
        .unwrap();
    ctx.register_table(JOIN_FACT_TABLE, Arc::new(fact_table))
        .unwrap();
    register_nested_json_render_udf(&ctx);
    ctx
}

/// A fact batch whose every row joins the one dimension row (`f_key = 1`), carrying
/// `values` as its nullable column `name`.
fn fact_batch_with(name: &str, values: ArrayRef) -> RecordBatch {
    let keys = Int64Array::from(vec![1i64; values.len()]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("f_key", DataType::Int64, false),
        Field::new(name, values.data_type().clone(), true),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(keys), values]).unwrap()
}

/// A join spec on `"D_KEY" = "F_KEY"` projecting every column, carrying `order_by`
/// and `limit` as its post-join bounds.
fn post_join_spec(order_by: Vec<SortKey>, limit: Option<u64>) -> ScanSpec {
    let mut spec = minimal_spec();
    let storage = spec.common.storage.clone();
    spec.common.join = Some(JoinSpec {
        table_root: String::new(),
        files: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"D_KEY\" = \"F_KEY\"".into(),
        post_join_limit: limit,
        post_join_order_by: order_by,
        partition_columns: Vec::new(),
        storage,
    });
    spec
}

fn sort_key(column: &str, ascending: bool, nulls_last: bool) -> SortKey {
    SortKey {
        column: column.into(),
        ascending,
        nulls_last,
    }
}

async fn run_join_sql(ctx: &SessionContext, spec: &ScanSpec) -> Vec<RecordBatch> {
    let sql = build_join_sql(ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, spec)
        .await
        .expect("build_join_sql");
    ctx.sql(&sql)
        .await
        .expect("plan join SQL")
        .collect()
        .await
        .expect("collect")
}

/// A join block carrying a cap but no ordering renders the unordered SQL it always
/// has: no `ORDER BY`, the cap last.
#[tokio::test]
async fn build_join_sql_renders_no_order_by_without_a_post_join_ordering() {
    let ctx = join_session(fact_batch_with(
        "score",
        Arc::new(Float64Array::from(vec![1.0])),
    ));

    let sql = build_join_sql(
        &ctx,
        JOIN_FACT_TABLE,
        JOIN_DIM_TABLE,
        &post_join_spec(Vec::new(), Some(3)),
    )
    .await
    .expect("build_join_sql");

    assert!(
        !sql.contains("ORDER BY"),
        "an empty post-join ordering must render no ORDER BY: {sql}"
    );
    assert!(
        sql.ends_with(" LIMIT 3"),
        "the cap must stay the last clause: {sql}"
    );
}

/// A key the scan emits as JSON text ranks by that text, the value the Exasol-side
/// wrapper ranks. The emitted `"[10]"` sorts before `"[9]"`, while native list order
/// puts `[9]` first, so a shard ranking the native value would cut the row the
/// wrapper's global top-1 needs.
#[tokio::test]
async fn build_join_sql_ranks_a_json_rendered_key_by_its_emitted_text() {
    let mut rank_key = ListBuilder::new(Int64Builder::new());
    rank_key.values().append_value(9);
    rank_key.append(true);
    rank_key.values().append_value(10);
    rank_key.append(true);
    let ctx = join_session(fact_batch_with("rank_key", Arc::new(rank_key.finish())));
    let spec = post_join_spec(vec![sort_key("RANK_KEY", true, true)], Some(1));

    let batches = run_join_sql(&ctx, &spec).await;

    let emitted: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("RANK_KEY must arrive as JSON text");
            column.iter().map(|v| v.unwrap_or_default().to_string())
        })
        .collect();
    assert_eq!(emitted, vec!["[10]".to_string()]);
}

/// A `NaN` in a `Float64` key ranks as the NULL that `emit_batch` emits for it
/// (#246), placed by the key's NULL placement. Native ranking puts `NaN` first under
/// `DESC`, so without the rule the shard's top-1 would be the row the wrapper ranks
/// last.
#[tokio::test]
async fn build_join_sql_ranks_a_nan_float_key_as_null() {
    let ctx = join_session(fact_batch_with(
        "score",
        Arc::new(Float64Array::from(vec![f64::NAN, 1.0])),
    ));
    let spec = post_join_spec(vec![sort_key("SCORE", false, true)], Some(1));

    let batches = run_join_sql(&ctx, &spec).await;

    let emitted: Vec<f64> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("SCORE must arrive as Float64");
            column.values().to_vec()
        })
        .collect();
    assert_eq!(emitted, vec![1.0]);
}

/// The `CAST(... AS VARCHAR)` fallback path (an out-of-range `Decimal128`) also
/// ranks by its emitted text. The emitted `"10"` sorts before `"9"`, while native
/// decimal order puts `9` first, so a shard ranking the native value would cut
/// the row the wrapper's global top-1 needs.
#[tokio::test]
async fn build_join_sql_ranks_a_cast_fallback_key_by_its_emitted_text() {
    let ctx = join_session(fact_batch_with(
        "amount",
        Arc::new(
            Decimal128Array::from(vec![9i128, 10])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
    ));
    let spec = post_join_spec(vec![sort_key("AMOUNT", true, true)], Some(1));

    let batches = run_join_sql(&ctx, &spec).await;

    let emitted: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let text = arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
            let text = text
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("AMOUNT must arrive as text (JSON fallback)");
            text.iter()
                .map(|v| v.unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(emitted, vec!["10".to_string()]);
}
