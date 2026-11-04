use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, Int64Builder, ListBuilder,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;

use super::*;
use crate::scan::raw_scan::NESTED_JSON_RENDER_UDF_NAME;
use crate::scan::raw_scan::register_nested_json_render_udf;
use crate::scan::spec::{ColumnRef, JoinSpec, ProjectionItem, SortKey};
use crate::scan::test_support::{join_spec, minimal_spec};

#[tokio::test]
async fn build_join_sql_renders_a_nested_column_as_valid_json_end_to_end() {
    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("hello");
    tags_builder.values().append_value("world");
    tags_builder.append(true);
    let ctx = join_session(fact_batch_with("tags", Arc::new(tags_builder.finish())));

    let batches = run_join_sql(&ctx, &post_join_spec("TAGS", Vec::new(), None)).await;

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

fn fact_batch_with(name: &str, values: ArrayRef) -> RecordBatch {
    let keys = Int64Array::from(vec![1i64; values.len()]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("f_key", DataType::Int64, false),
        Field::new(name, values.data_type().clone(), true),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(keys), values]).unwrap()
}

const DIM_ALIAS: &str = "LHS_T0";
const FACT_ALIAS: &str = "LHS_T1";

fn post_join_spec(fact_column: &str, order_by: Vec<SortKey>, limit: Option<u64>) -> ScanSpec {
    let mut spec = minimal_spec();
    let storage = spec.common.storage.clone();
    spec.common.projection = vec![
        ProjectionItem::Column(ColumnRef::qualified(DIM_ALIAS, "D_KEY")),
        ProjectionItem::Column(ColumnRef::qualified(FACT_ALIAS, "F_KEY")),
        ProjectionItem::Column(ColumnRef::qualified(FACT_ALIAS, fact_column)),
    ];
    spec.common.join = Some(JoinSpec {
        post_join_limit: limit,
        post_join_order_by: order_by,
        ..join_spec(storage)
    });
    spec
}

fn sort_key(column: &str, ascending: bool, nulls_last: bool) -> SortKey {
    SortKey {
        column: ColumnRef::qualified(FACT_ALIAS, column),
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
        &post_join_spec("SCORE", Vec::new(), Some(3)),
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

#[tokio::test]
async fn build_join_sql_ranks_a_json_rendered_key_by_its_emitted_text() {
    let mut rank_key = ListBuilder::new(Int64Builder::new());
    rank_key.values().append_value(9);
    rank_key.append(true);
    rank_key.values().append_value(10);
    rank_key.append(true);
    let ctx = join_session(fact_batch_with("rank_key", Arc::new(rank_key.finish())));
    let spec = post_join_spec("RANK_KEY", vec![sort_key("RANK_KEY", true, true)], Some(1));

    let batches = run_join_sql(&ctx, &spec).await;

    let values: Vec<Option<&str>> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column must arrive as Utf8")
                .iter()
        })
        .collect();
    assert_eq!(values, vec![Some("[10]")]);
}

#[tokio::test]
async fn build_join_sql_ranks_a_nan_float_key_as_null() {
    let ctx = join_session(fact_batch_with(
        "score",
        Arc::new(Float64Array::from(vec![f64::NAN, 1.0])),
    ));
    let spec = post_join_spec("SCORE", vec![sort_key("SCORE", false, true)], Some(1));

    let batches = run_join_sql(&ctx, &spec).await;

    let values: Vec<Option<f64>> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("column must arrive as Float64");
            (0..column.len()).map(|i| column.is_valid(i).then(|| column.value(i)))
        })
        .collect();
    assert_eq!(values, vec![Some(1.0)]);
}

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
    let spec = post_join_spec("AMOUNT", vec![sort_key("AMOUNT", true, true)], Some(1));

    let batches = run_join_sql(&ctx, &spec).await;

    let values: Vec<Option<String>> = batches
        .iter()
        .flat_map(|batch| {
            let text = arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
            let text = text
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column must arrive as text (JSON fallback)");
            text.iter()
                .map(|v| v.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values, vec![Some("10".to_string())]);
}

/// Both relations carry a column `X`: nested on the dimension, primitive on the fact side. The
/// dimension's JSON texts rank opposite to the fact's integers, so a sort key bound to the wrong
/// relation returns the other row.
fn shared_name_session() -> SessionContext {
    let mut dim_x = ListBuilder::new(Int64Builder::new());
    dim_x.values().append_value(5);
    dim_x.append(true);
    dim_x.values().append_value(1);
    dim_x.append(true);
    let dim_x = dim_x.finish();
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("d_key", DataType::Int64, false),
        Field::new("x", dim_x.data_type().clone(), true),
    ]));
    let dim = RecordBatch::try_new(
        dim_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64, 2])), Arc::new(dim_x)],
    )
    .unwrap();
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("f_key", DataType::Int64, false),
        Field::new("x", DataType::Int64, true),
    ]));
    let fact = RecordBatch::try_new(
        fact_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(Int64Array::from(vec![100i64, 200])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table(
        JOIN_DIM_TABLE,
        Arc::new(MemTable::try_new(dim_schema, vec![vec![dim]]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        JOIN_FACT_TABLE,
        Arc::new(MemTable::try_new(fact_schema, vec![vec![fact]]).unwrap()),
    )
    .unwrap();
    register_nested_json_render_udf(&ctx);
    ctx
}

#[tokio::test]
async fn build_join_sql_resolves_a_shared_column_name_per_relation() {
    let ctx = shared_name_session();
    let mut spec = post_join_spec("X", vec![sort_key("X", true, true)], Some(1));
    spec.common.projection = vec![
        ProjectionItem::Column(ColumnRef::qualified(DIM_ALIAS, "X")),
        ProjectionItem::Column(ColumnRef::qualified(FACT_ALIAS, "X")),
    ];

    let sql = build_join_sql(&ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, &spec)
        .await
        .expect("build_join_sql");

    assert!(
        sql.contains(&format!(
            "{NESTED_JSON_RENDER_UDF_NAME}(\"LHS_T0\".\"X\") AS \"LHS_T0_X\""
        )),
        "the nested relation's X must render as JSON under its own output name: {sql}"
    );
    assert!(
        sql.contains("\"LHS_T1\".\"X\" AS \"LHS_T1_X\""),
        "the primitive relation's X must render bare under its own output name: {sql}"
    );
    assert!(
        sql.contains("ORDER BY \"LHS_T1\".\"X\" ASC NULLS LAST"),
        "the sort key must rank by the primitive relation's X, not the JSON one: {sql}"
    );

    let batches = run_join_sql(&ctx, &spec).await;
    let schema = batches[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["LHS_T0_X", "LHS_T1_X"]);
    let dim_x = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("the dimension's X arrives as JSON text");
    let fact_x = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("the fact side's X stays Int64");
    assert_eq!(
        (dim_x.value(0), fact_x.value(0)),
        ("[5]", 100),
        "ranking by the fact side's X keeps the row with 100, whose dimension X is [5]"
    );
}

#[tokio::test]
async fn build_join_sql_rejects_an_empty_projection() {
    let ctx = shared_name_session();
    let mut spec = post_join_spec("X", Vec::new(), None);
    spec.common.projection = Vec::new();

    let error = build_join_sql(&ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, &spec)
        .await
        .expect_err("an empty join projection must be refused");

    assert!(
        matches!(&error, UdfError::User(message) if message.contains("empty projection")),
        "{error:?}"
    );
}

#[tokio::test]
async fn build_join_sql_rejects_a_column_without_a_relation_alias() {
    let ctx = shared_name_session();
    let mut spec = post_join_spec("X", Vec::new(), None);
    spec.common.projection = vec![ProjectionItem::from("X")];

    let error = build_join_sql(&ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, &spec)
        .await
        .expect_err("an unqualified join column must be refused");

    assert!(
        matches!(&error, UdfError::User(message) if message.contains("no relation alias")),
        "{error:?}"
    );

    let mut spec = post_join_spec("X", Vec::new(), None);
    spec.common.join.as_mut().unwrap().post_join_order_by = vec![SortKey {
        column: "X".into(),
        ascending: true,
        nulls_last: true,
    }];
    let error = build_join_sql(&ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, &spec)
        .await
        .expect_err("an unqualified sort key must be refused");
    assert!(
        matches!(&error, UdfError::User(message) if message.contains("no relation alias")),
        "{error:?}"
    );
}
