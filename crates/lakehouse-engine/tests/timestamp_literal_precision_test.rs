//! Regression for #155: a year-9999 timestamp literal from `render_expression`, compared
//! against microsecond columns, must not overflow i64 nanoseconds in DataFusion's
//! `simplify_expressions` pass. The overflow surfaces only on `.collect()`, not `ctx.sql()`.
//! The literal is the actual renderer output so renderer and optimizer cannot drift.

use std::sync::Arc;

use arrow::array::TimestampMicrosecondArray;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde_json::json;

const FAR_FUTURE: &str = "9999-12-31 23:59:59";

/// 2026-01-01 00:00:00 UTC.
const ROW_MICROS: i64 = 1_767_225_600_000_000;

fn context_with_timestamp_columns() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "tstz",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));

    let ts = TimestampMicrosecondArray::from(vec![ROW_MICROS]);
    let tstz = TimestampMicrosecondArray::from(vec![ROW_MICROS]).with_timezone("UTC");
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(tstz)]).unwrap();

    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("scan_target", Arc::new(table)).unwrap();
    ctx
}

fn rendered_fragment(node_type: &str) -> String {
    vs_expression::render_expression(&json!({ "type": node_type, "value": FAR_FUTURE }))
        .expect("render_expression must translate the timestamp literal node")
}

async fn assert_no_nanosecond_overflow(ctx: &SessionContext, sql: &str) -> usize {
    let outcome = async {
        let df = ctx.sql(sql).await?;
        df.collect().await
    }
    .await
    .map_err(|e| e.to_string());

    match outcome {
        Ok(batches) => batches.iter().map(|b| b.num_rows()).sum(),
        Err(message) => {
            assert!(
                !message.contains("Overflow") && !message.contains("Nanosecond"),
                "#155 regression — far-future timestamp literal overflowed \
                 simplify_expressions: {message}"
            );
            panic!("far-future timestamp query failed for a reason unrelated to #155: {message}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn far_future_timestamp_literal_does_not_overflow_simplify_expressions() {
    let ctx = context_with_timestamp_columns();

    let ts_literal = rendered_fragment("literal_timestamp");
    let tstz_literal = rendered_fragment("literal_timestamp_utc");

    // The CASE branch is the #155 clamp-workaround shape.
    let ts_predicate = format!("SELECT ts FROM scan_target WHERE ts < {ts_literal}");
    let ts_case = format!(
        "SELECT CASE WHEN ts < {ts_literal} THEN ts ELSE {ts_literal} END AS clamped \
         FROM scan_target"
    );

    let tstz_predicate = format!("SELECT tstz FROM scan_target WHERE tstz < {tstz_literal}");
    let tstz_case = format!(
        "SELECT CASE WHEN tstz < {tstz_literal} THEN tstz ELSE {tstz_literal} END AS clamped \
         FROM scan_target"
    );

    for sql in [&ts_predicate, &ts_case, &tstz_predicate, &tstz_case] {
        let rows = assert_no_nanosecond_overflow(&ctx, sql).await;
        assert_eq!(
            rows, 1,
            "the in-range row must survive the far-future comparison: {sql}"
        );
    }
}
