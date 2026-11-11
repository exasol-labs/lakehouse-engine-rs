//! Issue #200: BOOLEAN-to-string must render Exasol's `TRUE`/`FALSE`, not
//! DataFusion's lowercase. Executes the real `render_expression` output so the
//! renderer and the executed result cannot drift.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use vs_expression::exasol_type::ExasolType;
use vs_expression::test_support::{
    TypedColumns, cast_to, col, compare, num_lit, scalar_fn, str_lit, typed_columns,
};

/// Field names are uppercase because `render_expression` uppercases and
/// double-quotes column names.
fn context_with_acctbal() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("C_CUSTKEY", DataType::Utf8, false),
        Field::new("C_ACCTBAL", DataType::Float64, true),
    ]));

    let custkey = StringArray::from(vec!["1", "2", "3"]);
    let acctbal = Float64Array::from(vec![Some(100.0), Some(-50.0), None]);
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(custkey), Arc::new(acctbal)]).unwrap();

    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("scan_target", Arc::new(table)).unwrap();
    ctx
}

/// The scan table's columns as the request declares them.
fn scan_target_columns() -> TypedColumns {
    typed_columns(&[
        ("C_CUSTKEY", ExasolType::WIDEST_VARCHAR),
        ("C_ACCTBAL", ExasolType::DoublePrecision),
    ])
}

fn acctbal_greater_than_zero() -> serde_json::Value {
    compare("predicate_greater", col("c_acctbal"), num_lit(0))
}

async fn collect_strings(ctx: &SessionContext, sql: &str, col: &str) -> Vec<Option<String>> {
    let df = ctx.sql(sql).await.expect("query must plan");
    let batches = df.collect().await.expect("query must execute");
    let mut out = Vec::new();
    for batch in &batches {
        let idx = batch.schema().index_of(col).unwrap();
        // `CAST(... AS VARCHAR)` yields Utf8View in DataFusion; the scan's emit coerces either.
        let utf8 = arrow::compute::cast(batch.column(idx), &DataType::Utf8).unwrap();
        let arr = utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column must be text");
        for i in 0..arr.len() {
            out.push((!arr.is_null(i)).then(|| arr.value(i).to_string()));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_cast_bool_to_varchar_renders_exasol_casing() {
    let ctx = context_with_acctbal();
    let expr = cast_to(ExasolType::varchar(10), acctbal_greater_than_zero());
    let fragment = vs_expression::render_scan_expression(&expr, &scan_target_columns())
        .expect("CAST must translate")
        .sql;

    let sql = format!("SELECT {fragment} AS s FROM scan_target ORDER BY \"C_CUSTKEY\"");
    let rows = collect_strings(&ctx, &sql, "s").await;

    assert_eq!(
        rows,
        vec![Some("TRUE".to_string()), Some("FALSE".to_string()), None,]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concat_predicate_matches_exasol_uppercase_not_datafusion_lowercase() {
    let ctx = context_with_acctbal();
    let concat_expr = scalar_fn("CONCAT", [acctbal_greater_than_zero(), str_lit("")]);
    let fragment = vs_expression::render_scan_expression(&concat_expr, &scan_target_columns())
        .expect("CONCAT must translate")
        .sql;

    let sql_true = format!("SELECT \"C_CUSTKEY\" FROM scan_target WHERE {fragment} = 'TRUE'");
    let matched_true = collect_strings(&ctx, &sql_true, "C_CUSTKEY").await;
    assert_eq!(matched_true, vec![Some("1".to_string())]);

    let sql_lower = format!("SELECT \"C_CUSTKEY\" FROM scan_target WHERE {fragment} = 'true'");
    let matched_lower = collect_strings(&ctx, &sql_lower, "C_CUSTKEY").await;
    assert!(matched_lower.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concat_group_by_key_uses_exasol_uppercase_labels() {
    let ctx = context_with_acctbal();
    let concat_expr = scalar_fn("CONCAT", [acctbal_greater_than_zero(), str_lit("")]);
    let fragment = vs_expression::render_scan_expression(&concat_expr, &scan_target_columns())
        .expect("CONCAT must translate")
        .sql;

    let sql = format!(
        "SELECT {fragment} AS g, COUNT(*) AS n FROM scan_target GROUP BY {fragment} ORDER BY g"
    );
    let mut labels = collect_strings(&ctx, &sql, "g").await;
    labels.sort();

    assert_eq!(
        labels,
        vec![None, Some("FALSE".to_string()), Some("TRUE".to_string())]
    );
}
