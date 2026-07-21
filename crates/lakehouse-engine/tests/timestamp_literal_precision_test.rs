//! Optimizer-overflow regression test for issue #155
//! (plan `fix-timestamp-literal-precision`, task 2.1).
//!
//! Proves that a far-future timestamp literal (year 9999) rendered by
//! `vs_expression::render_expression` survives DataFusion's logical optimizer —
//! specifically the `simplify_expressions` pass — when compared against the
//! scan's microsecond-typed columns. It runs under plain `cargo test` (no
//! feature gate, no live DB).
//!
//! Before the fix, `literal_timestamp` / `literal_timestamp_utc` rendered a bare
//! `TIMESTAMP '9999-12-31 23:59:59'`, which DataFusion's SQL frontend types as
//! `Timestamp(Nanosecond)`. Expressed as nanoseconds-since-epoch that instant is
//! ~2.5e20, which overflows i64 (max ~9.2e18). When `simplify_expressions`
//! unified that nanosecond literal with a microsecond column it constant-folded
//! the literal to nanosecond and failed with `Overflow converting … to
//! Nanosecond` — even for values well inside the microsecond column's range.
//! This is the CASE-WHEN clamp workaround in #155's evidence failing to plan.
//!
//! The fix (Group A, `crates/vs-expression/src/lib.rs`) renders both literals via
//! `arrow_cast(…, 'Timestamp(Microsecond, …)')`, so unification with the engine's
//! microsecond columns never introduces a `Timestamp(Nanosecond)` intermediate.
//!
//! This test embeds the ACTUAL `render_expression` output (never a hand-written
//! `arrow_cast(…)` string) so the renderer and the optimizer cannot drift: a
//! revert of Group A would make `render_expression` emit the bare `TIMESTAMP`
//! form again, reintroducing the overflow this test asserts against
//! (decision-log decision [4]).
//!
//! Note: the overflow surfaces at execution/physical-planning time, not at
//! `ctx.sql()` — `ctx.sql()` builds only the unoptimized logical plan, and
//! `simplify_expressions` runs on `.collect()`. Each scenario is therefore driven
//! all the way to `.collect()`; a bare `ctx.sql()` would be a vacuous check.

use std::sync::Arc;

use arrow::array::TimestampMicrosecondArray;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde_json::json;

/// The far-future value that overflows i64 at nanosecond resolution (~2.5e20 ns)
/// but is representable at microsecond resolution — the #155 trigger.
const FAR_FUTURE: &str = "9999-12-31 23:59:59";

/// A realistic in-range microsecond instant for the table's single row:
/// `2026-01-01 00:00:00` UTC (Unix seconds 1_767_225_600 × 1_000_000). Being far
/// below the far-future literal, it satisfies each `< FAR_FUTURE` comparison, so
/// the row is retained and the literal is genuinely evaluated.
const ROW_MICROS: i64 = 1_767_225_600_000_000;

/// Registers a one-row `MemTable` mirroring the scan's coerced timestamp columns:
/// `ts` is `Timestamp(Microsecond, None)` (Iceberg `timestamp`, decision 009) and
/// `tstz` is `Timestamp(Microsecond, Some("UTC"))` (Iceberg `timestamptz`,
/// decision 007).
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

/// The rendered SQL fragment for a far-future literal node, straight from the
/// translator under test — never a hand-written `arrow_cast(…)` string.
fn rendered_fragment(node_type: &str) -> String {
    vs_expression::render_expression(&json!({ "type": node_type, "value": FAR_FUTURE }))
        .expect("render_expression must translate the timestamp literal node")
}

/// Drives `sql` through DataFusion to an executed result, which runs the full
/// logical optimizer including `simplify_expressions`. A #155 regression makes
/// that pass overflow with a `Nanosecond` cast error; this asserts it does not,
/// and returns the retained row count on success.
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

    // literal_timestamp vs the Timestamp(Microsecond, None) column — as a
    // predicate and as a CASE branch (the #155 clamp-workaround shape).
    let ts_predicate = format!("SELECT ts FROM scan_target WHERE ts < {ts_literal}");
    let ts_case = format!(
        "SELECT CASE WHEN ts < {ts_literal} THEN ts ELSE {ts_literal} END AS clamped \
         FROM scan_target"
    );

    // literal_timestamp_utc vs the Timestamp(Microsecond, Some(\"UTC\")) column.
    // Both now carry the same UTC tz label and stay microsecond, so the
    // comparison type-checks and cannot overflow.
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
