use std::sync::Arc;

use arrow::array::{
    Decimal128Array, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    UInt64Array,
};
use arrow::datatypes::Int32Type;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;

use crate::scan::raised_failure::session_raised_failure;
use crate::scan::test_support::{first_column as rows, scan_session_over};

/// One row per argument type of a string function's length or position.
fn string_arg_session() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("dec0", DataType::Decimal128(20, 0), true),
        Field::new("dec2", DataType::Decimal128(9, 2), true),
        Field::new("f64", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(3), None])),
            Arc::new(Int64Array::from(vec![Some(3), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(3), None])
                    .with_precision_and_scale(20, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(350), Some(-150)])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(Float64Array::from(vec![Some(3.5), Some(-1.5)])),
        ],
    )
    .unwrap();
    scan_session_over(batch)
}

/// Scenario: A string function's numeric argument rounds to an integer as Exasol does
#[tokio::test]
async fn string_function_numeric_argument_rounds_to_int64() {
    let ctx = string_arg_session();
    for (sql, expected) in [
        ("SELECT rpad('ab', i32) FROM t", ["ab ", ""]),
        ("SELECT rpad('ab', i64) FROM t", ["ab ", ""]),
        ("SELECT rpad('ab', dec0 + 1) FROM t", ["ab  ", ""]),
        ("SELECT rpad('ab', dec2 + 2) FROM t", ["ab    ", "a"]),
        ("SELECT rpad('ab', f64 + 2, '*') FROM t", ["ab****", "a"]),
        ("SELECT lpad('ab', 3.4) FROM t", [" ab", " ab"]),
        ("SELECT rpad('ab', NULL) FROM t", ["", ""]),
        ("SELECT repeat('ab', abs(f64)) FROM t", ["abababab", "abab"]),
        ("SELECT left('abcd', abs(f64) - 1) FROM t", ["abc", "a"]),
        ("SELECT right('abcd', abs(f64) - 1) FROM t", ["bcd", "d"]),
        ("SELECT substr('abcd', abs(f64) - 1) FROM t", ["cd", "abcd"]),
        (
            "SELECT substr('abcd', 1, abs(dec2) - 1) FROM t",
            ["abc", "a"],
        ),
        ("SELECT chr(dec2 + 62) FROM t", ["B", "="]),
    ] {
        assert_eq!(rows(&ctx, sql).await, expected, "{sql}");
    }
    let join = "SELECT a.i64 FROM t a JOIN t b ON left('abcd', a.f64) = left('abcd', b.f64)";
    assert_eq!(
        rows(&ctx, join).await,
        ["3"],
        "-1.5 rounds to -2, a NULL key"
    );
}

/// Scenario: A string function's numeric argument outside Exasol's range fails the query
#[tokio::test]
async fn string_function_numeric_argument_outside_exasols_range_fails() {
    for sql in [
        "SELECT rpad('ab', i64 * 1000000000) FROM t",
        "SELECT rpad('ab', i64 * -1000000000) FROM t",
        "SELECT rpad('ab', f64 * 1000000000) FROM t",
        "SELECT substr('abcd', CAST(dec0 AS DECIMAL(36,0)) * -1000000000) FROM t",
        "SELECT left('abcd', 999999999.5) FROM t",
    ] {
        let ctx = string_arg_session();
        let err = ctx.sql(sql).await.unwrap().collect().await.expect_err(sql);
        assert!(
            err.to_string()
                .contains("data exception - numeric value out of range"),
            "{sql}: {err}"
        );
        assert!(
            session_raised_failure(&ctx)
                .is_some_and(|f| f.contains("is not in [ -999999999 .. 999999999 ]")),
            "{sql}"
        );
    }
    let ctx = string_arg_session();
    assert_eq!(
        rows(&ctx, "SELECT left('abcd', 999999999) FROM t").await,
        ["abcd", "abcd"]
    );
    assert_eq!(
        rows(&ctx, "SELECT substr('abcd', 999999999) FROM t").await,
        ["", ""]
    );
}

/// Scenario: A string function's numeric argument rounds to an integer as Exasol does
/// Scenario: A string function's numeric argument outside Exasol's range fails the query
#[tokio::test]
async fn dictionary_argument_converts_and_unsigned_beyond_int64_fails() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("u64", DataType::UInt64, true),
        Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64)),
            true,
        ),
    ]));
    let dict = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![0]),
        Arc::new(Float64Array::from(vec![3.5])),
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![u64::MAX])), Arc::new(dict)],
    )
    .unwrap();
    let ctx = scan_session_over(batch);
    assert_eq!(rows(&ctx, "SELECT rpad('ab', dict) FROM t").await, ["ab  "]);

    let sql = "SELECT rpad('ab', u64) FROM t";
    let err = ctx.sql(sql).await.unwrap().collect().await.expect_err(sql);
    assert!(
        err.to_string()
            .contains("data exception - numeric value out of range"),
        "{err}"
    );
    assert!(session_raised_failure(&ctx).is_some());
}

/// Scenario: A negative count or length makes a string function return NULL as Exasol does
#[tokio::test]
async fn negative_count_or_length_returns_null_or_refuses_repeat() {
    let ctx = string_arg_session();
    for sql in [
        "SELECT rpad('ab', -abs(f64)) FROM t",
        "SELECT lpad('ab', dec2 - 4, '*') FROM t",
        "SELECT left('abcd', -abs(f64)) FROM t",
        "SELECT right('abcd', -abs(dec2)) FROM t",
        "SELECT substr('abcd', 2, -abs(f64)) FROM t",
    ] {
        assert_eq!(rows(&ctx, sql).await, ["", ""], "{sql}");
    }
    assert_eq!(
        rows(&ctx, "SELECT repeat('ab', abs(f64) - 1.9) FROM t").await,
        ["abab", ""],
        "-0.4 rounds to 0, which is not negative"
    );

    let err = ctx
        .sql("SELECT repeat('ab', f64) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("REPEAT refuses -1.5");
    assert!(
        err.to_string()
            .contains("data exception - negative numbers not allowed in function REPEAT"),
        "{err}"
    );
    assert!(session_raised_failure(&ctx).is_some_and(|f| f.contains("REPEAT")));
}
