//! Regression test for issue #200.
//!
//! Converting a BOOLEAN to a string was delegated to DataFusion's cast/concat
//! kernel, which renders lowercase `true`/`false`; Exasol renders
//! `TRUE`/`FALSE`. Drives the ACTUAL `vs_expression::render_expression` output
//! through a real DataFusion `SessionContext` (never a hand-written SQL
//! string) so the renderer and the executed result cannot drift, covering the
//! issue's three repro shapes: explicit `CAST(bool AS VARCHAR)`, `bool || ''`
//! in a predicate, and `bool || ''` as a GROUP BY key. Also asserts a NULL
//! boolean converts to NULL, not `'NULL'` or a coerced `'FALSE'`.
//!
//! Runs under plain `cargo test` (no feature gate, no live DB).

use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde_json::json;

/// A `CUSTOMER`-like table mirroring the issue's repro: `C_ACCTBAL` mixes
/// positive, negative, and NULL values so `c_acctbal > 0` yields TRUE, FALSE,
/// and NULL rows respectively (NULL comparison result exercises the
/// NULL-preservation requirement). Field names are uppercase because
/// `render_expression`'s `"column"` node always uppercases and double-quotes
/// the name (mirroring Exasol's own identifier folding), so the schema must
/// match that casing for the rendered fragment's column references to resolve.
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

/// The `c_acctbal > 0` predicate node shared by all three repro shapes.
fn acctbal_greater_than_zero() -> serde_json::Value {
    json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "c_acctbal"},
        "right": {"type": "literal_exactnumeric", "value": 0}
    })
}

async fn collect_strings(ctx: &SessionContext, sql: &str, col: &str) -> Vec<Option<String>> {
    let df = ctx.sql(sql).await.expect("query must plan");
    let batches = df.collect().await.expect("query must execute");
    let mut out = Vec::new();
    for batch in &batches {
        let idx = batch.schema().index_of(col).unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column must be Utf8");
        for i in 0..arr.len() {
            out.push((!arr.is_null(i)).then(|| arr.value(i).to_string()));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_cast_bool_to_varchar_renders_exasol_casing() {
    let ctx = context_with_acctbal();
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [acctbal_greater_than_zero()],
        "dataType": {"type": "VARCHAR", "size": 10}
    });
    let fragment = vs_expression::render_expression(&expr).expect("CAST must translate");

    let sql = format!("SELECT {fragment} AS s FROM scan_target ORDER BY \"C_CUSTKEY\"");
    let rows = collect_strings(&ctx, &sql, "s").await;

    assert_eq!(
        rows,
        vec![
            Some("TRUE".to_string()),
            Some("FALSE".to_string()),
            None, // NULL c_acctbal -> NULL, never "NULL" or "FALSE"
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concat_predicate_matches_exasol_uppercase_not_datafusion_lowercase() {
    let ctx = context_with_acctbal();
    let concat_expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [acctbal_greater_than_zero(), {"type": "literal_string", "value": ""}]
    });
    let fragment = vs_expression::render_expression(&concat_expr).expect("CONCAT must translate");

    // Exasol: WHERE (c_acctbal>0)||'' = 'TRUE' must match the positive row.
    let sql_true = format!("SELECT \"C_CUSTKEY\" FROM scan_target WHERE {fragment} = 'TRUE'");
    let matched_true = collect_strings(&ctx, &sql_true, "C_CUSTKEY").await;
    assert_eq!(matched_true, vec![Some("1".to_string())]);

    // DataFusion's lowercase rendering must NOT match — 0 rows, not the
    // positive row (the issue's "exactly reversed" symptom).
    let sql_lower = format!("SELECT \"C_CUSTKEY\" FROM scan_target WHERE {fragment} = 'true'");
    let matched_lower = collect_strings(&ctx, &sql_lower, "C_CUSTKEY").await;
    assert!(matched_lower.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concat_group_by_key_uses_exasol_uppercase_labels() {
    let ctx = context_with_acctbal();
    let concat_expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [acctbal_greater_than_zero(), {"type": "literal_string", "value": ""}]
    });
    let fragment = vs_expression::render_expression(&concat_expr).expect("CONCAT must translate");

    let sql = format!(
        "SELECT {fragment} AS g, COUNT(*) AS n FROM scan_target GROUP BY {fragment} ORDER BY g"
    );
    let mut labels = collect_strings(&ctx, &sql, "g").await;
    labels.sort();

    // TRUE for the positive row, FALSE for the negative row, NULL for the
    // NULL c_acctbal row — never lowercase "true"/"false".
    assert_eq!(
        labels,
        vec![None, Some("FALSE".to_string()), Some("TRUE".to_string())]
    );
}
