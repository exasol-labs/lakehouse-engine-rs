use super::*;
use crate::scan::scan_session_context;
use arrow::array::{Decimal128Array, Float64Array, Int64Array, RecordBatch};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Schema};
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;

const BIG: i64 = 4_000_000_000_000_000_000;

/// `n` holds `BIG` behind its NULL, so only a kernel that skips NULL slots passes.
fn session() -> SessionContext {
    let ctx = scan_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("l", DataType::Int64, true),
        Field::new("n", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
        Field::new("dec36", DataType::Decimal128(36, 6), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(BIG), Some(3), None])),
            Arc::new(Int64Array::new(
                vec![1, 2, BIG].into(),
                Some(NullBuffer::from(vec![true, true, false])),
            )),
            Arc::new(Float64Array::from(vec![1e300, 0.1, 1e-20])),
            Arc::new(
                Decimal128Array::from(vec![10i128.pow(35), 100_000, 1])
                    .with_precision_and_scale(36, 6)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|r| array_value_to_string(b.column(0), r).unwrap()))
        .collect()
}

/// Scenario: Exact arithmetic and SUM do not wrap
#[tokio::test]
async fn exact_results_take_exasols_type_and_do_not_wrap() {
    let ctx = session();
    for (sql, expected) in [
        (
            "SELECT arrow_typeof(l * 100) FROM t LIMIT 1",
            "Decimal128(22, 0)",
        ),
        (
            "SELECT arrow_typeof(l * l) FROM t LIMIT 1",
            "Decimal128(38, 0)",
        ),
        (
            "SELECT arrow_typeof(dec36 + l) FROM t LIMIT 1",
            "Decimal128(37, 6)",
        ),
        (
            "SELECT arrow_typeof(SUM(dec36)) FROM t",
            "Decimal256(46, 6)",
        ),
        (
            "SELECT CAST(l * 3 AS VARCHAR) FROM t LIMIT 1",
            "12000000000000000000",
        ),
        (
            "SELECT CAST(-l - l AS VARCHAR) FROM t LIMIT 1",
            "-8000000000000000000",
        ),
        (
            "SELECT CAST(SUM(l) AS VARCHAR) FROM t",
            "4000000000000000003",
        ),
        (
            "SELECT CAST(SUM(l * 3) AS VARCHAR) FROM t",
            "12000000000000000009",
        ),
    ] {
        assert_eq!(rows(&ctx, sql).await, [expected], "{sql}");
    }
}

/// Scenario: DOUBLE combined with DECIMAL evaluates in DOUBLE
#[tokio::test]
async fn double_meets_decimal_in_double() {
    let ctx = session();
    for (sql, expected) in [
        ("SELECT COUNT(*) FROM t WHERE d = 0.1", "1"),
        ("SELECT COUNT(*) FROM t WHERE dec36 > d", "1"),
        ("SELECT COUNT(*) FROM t WHERE d IN (dec36, 0.1)", "1"),
        ("SELECT COUNT(*) FROM t WHERE d BETWEEN 0 AND dec36", "2"),
        (
            "SELECT arrow_typeof(CASE WHEN l > 3 THEN dec36 ELSE d END) FROM t LIMIT 1",
            "Float64",
        ),
        (
            "SELECT arrow_typeof(greatest(dec36, d)) FROM t LIMIT 1",
            "Float64",
        ),
    ] {
        assert_eq!(rows(&ctx, sql).await, [expected], "{sql}");
    }
}

/// Scenario: Exact overflow raises Exasol's numeric value out of range
#[tokio::test]
async fn overflow_past_128_bits_raises_and_is_recorded_on_the_session() {
    let ctx = session();
    assert_eq!(session_numeric_overflow(&ctx), None);
    assert_eq!(
        rows(
            &ctx,
            "SELECT CAST(l * l * 10 - l * l * 10 AS VARCHAR) FROM t WHERE l > 3"
        )
        .await,
        ["0"],
        "an intermediate past 36 digits that fits 128 bits must not raise"
    );
    assert_eq!(
        rows(&ctx, "SELECT CAST(n * n * 100 AS VARCHAR) FROM t").await,
        ["100", "400", ""],
        "a NULL slot's value must not raise"
    );

    let error = ctx
        .sql("SELECT l * l * 100 FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("BIG * BIG * 100 exceeds 128 bits");

    assert!(find_numeric_overflow(&error).is_some(), "{error}");
    assert!(session_numeric_overflow(&ctx).is_some_and(|o| {
        o.to_string()
            .starts_with("data exception - numeric value out of range")
    }),);
}
