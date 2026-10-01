use super::*;
use crate::scan::checked_div::register_checked_float_div_udf;
use crate::scan::raw_scan::{build_scan_sql, register_nested_json_render_udf};
use crate::scan::test_support::minimal_spec;
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
    ListArray, StringArray, StringViewArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::datasource::MemTable;
use serde_json::json;

/// Field names are uppercase because `render_expression` uppercases and double-quotes column
/// names.
fn session_with(columns: Vec<(&str, ArrayRef)>) -> SessionContext {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        columns.into_iter().map(|(_, values)| values).collect(),
    )
    .expect("the columns must form one batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("in-memory table");
    let ctx = SessionContext::new();
    ctx.register_table("scan_target", Arc::new(table))
        .expect("register the in-memory table");
    register_exa_to_varchar_udf(&ctx);
    ctx
}

async fn collect_columns(ctx: &SessionContext, sql: &str, column: &str) -> Vec<ArrayRef> {
    let batches = ctx
        .sql(sql)
        .await
        .expect("query must plan")
        .collect()
        .await
        .expect("query must execute");
    batches
        .iter()
        .map(|batch| Arc::clone(batch.column(batch.schema().index_of(column).expect("selected"))))
        .collect()
}

/// A rendered `CAST(.. AS VARCHAR)` yields `Utf8View`, so any string type is read as text.
async fn collect_texts(ctx: &SessionContext, sql: &str, column: &str) -> Vec<Option<String>> {
    let columns = collect_columns(ctx, sql, column).await;
    columns.iter().flat_map(column_texts).collect()
}

/// Each value of one non-string column, converted by `exa_to_varchar` through a planned query.
async fn converted(values: ArrayRef) -> Vec<Option<String>> {
    let ctx = session_with(vec![("V", values)]);
    let columns = collect_columns(
        &ctx,
        "SELECT exa_to_varchar(\"V\") AS s FROM scan_target",
        "s",
    )
    .await;
    for column in &columns {
        assert_eq!(
            column.data_type(),
            &DataType::Utf8,
            "a converted value must be Utf8"
        );
    }
    columns.iter().flat_map(column_texts).collect()
}

fn texts(values: &[Option<&str>]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

/// Drives `invoke_with_args` as DataFusion does per batch, with the return field the function
/// declares for these arguments.
fn invoke(arguments: Vec<ArrayRef>) -> Result<ArrayRef> {
    let rows = arguments.first().map_or(0, |first| first.len());
    let udf = ScalarUDF::from(ExaToVarcharUdf::new());
    let arg_fields: Vec<FieldRef> = arguments
        .iter()
        .map(|argument| Arc::new(Field::new("arg", argument.data_type().clone(), true)))
        .collect();
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &vec![None; arg_fields.len()],
    })?;
    let args = ScalarFunctionArgs {
        arg_fields,
        args: arguments.into_iter().map(ColumnarValue::Array).collect(),
        number_rows: rows,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    udf.invoke_with_args(args)?.to_array(rows)
}

#[test]
fn exa_to_varchar_return_field_keeps_string_nullability_and_marks_conversions_nullable() {
    let udf = ScalarUDF::from(ExaToVarcharUdf::new());
    let return_field_for = |argument: Field| {
        udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::new(argument)],
            scalar_arguments: &[None],
        })
        .expect("one argument must yield a return field")
    };

    let string = return_field_for(Field::new("a", DataType::Utf8, false));
    assert_eq!(string.data_type(), &DataType::Utf8);
    assert!(
        !string.is_nullable(),
        "a non-nullable string argument must stay non-nullable, since simplify returns it as is"
    );

    let integer = return_field_for(Field::new("a", DataType::Int64, false));
    assert_eq!(integer.data_type(), &DataType::Utf8);
    assert!(
        integer.is_nullable(),
        "a converted argument must be nullable, since a conversion can yield NULL"
    );
}

async fn optimized_plan(ctx: &SessionContext, sql: &str) -> String {
    ctx.sql(sql)
        .await
        .expect("query must plan")
        .into_optimized_plan()
        .expect("query must optimize")
        .display_indent()
        .to_string()
}

/// Scenario: A string argument passes through and the call simplifies away
#[tokio::test]
async fn exa_to_varchar_string_argument_simplifies_away() {
    let ctx = session_with(vec![
        (
            "C_VARCHAR",
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
        ),
        ("C_INT", Arc::new(Int64Array::from(vec![1]))),
    ]);

    let wrapped = optimized_plan(
        &ctx,
        "SELECT upper(exa_to_varchar(\"C_VARCHAR\")) AS s FROM scan_target",
    )
    .await;
    let bare = optimized_plan(&ctx, "SELECT upper(\"C_VARCHAR\") AS s FROM scan_target").await;
    let integer = optimized_plan(
        &ctx,
        "SELECT upper(exa_to_varchar(\"C_INT\")) AS s FROM scan_target",
    )
    .await;

    assert_eq!(wrapped, bare, "a string argument must leave today's plan");
    assert!(
        integer.contains(EXA_TO_VARCHAR_FN),
        "an Int64 argument must keep the conversion call: {integer}"
    );
}

#[test]
fn exa_to_varchar_passes_every_string_type_through_unchanged() {
    let arguments: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("a"), None])),
        Arc::new(LargeStringArray::from(vec![Some("a"), None])),
        Arc::new(StringViewArray::from(vec![Some("a"), None])),
    ];

    for argument in arguments {
        let converted = invoke(vec![Arc::clone(&argument)]).expect("a string must convert");
        assert_eq!(
            &converted,
            &argument,
            "{} must pass through",
            argument.data_type()
        );
    }
}

#[test]
fn exa_to_varchar_rejects_a_call_without_exactly_one_argument() {
    let text: ArrayRef = Arc::new(StringArray::from(vec!["a"]));

    let error = invoke(vec![Arc::clone(&text), text]).expect_err("two arguments must be refused");

    assert!(
        error
            .to_string()
            .contains("exa_to_varchar takes exactly one argument, got 2"),
        "{error}"
    );
}

/// Scenario: An integer argument converts to plain digits
#[tokio::test]
async fn exa_to_varchar_renders_integers_as_digits() {
    let columns: Vec<(ArrayRef, Vec<Option<&str>>)> = vec![
        (
            Arc::new(Int8Array::from(vec![Some(-7), None])),
            vec![Some("-7"), None],
        ),
        (
            Arc::new(Int16Array::from(vec![Some(123)])),
            vec![Some("123")],
        ),
        (
            Arc::new(Int32Array::from(vec![Some(-2_147_483_648)])),
            vec![Some("-2147483648")],
        ),
        (
            Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
            vec![
                Some("-9223372036854775808"),
                Some("9223372036854775807"),
                None,
            ],
        ),
        (
            Arc::new(UInt8Array::from(vec![Some(255)])),
            vec![Some("255")],
        ),
        (
            Arc::new(UInt16Array::from(vec![Some(65_535)])),
            vec![Some("65535")],
        ),
        (
            Arc::new(UInt32Array::from(vec![Some(4_294_967_295)])),
            vec![Some("4294967295")],
        ),
        (
            Arc::new(UInt64Array::from(vec![Some(u64::MAX)])),
            vec![Some("18446744073709551615")],
        ),
    ];

    for (values, expected) in columns {
        let data_type = values.data_type().clone();
        assert_eq!(converted(values).await, texts(&expected), "{data_type}");
    }
}

fn decimal(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .expect("inside Arrow's Decimal128 domain"),
    )
}

/// Scenario: A DECIMAL argument converts with trailing scale zeros removed
#[tokio::test]
async fn exa_to_varchar_trims_decimal_trailing_zeros() {
    let two_digit_scale = decimal(
        vec![Some(-50), Some(10_010), Some(500), Some(0), Some(5), None],
        5,
        2,
    );
    let one_digit_scale = decimal(vec![Some(5)], 3, 1);
    let scale_zero = decimal(vec![Some(1_234_567), Some(-7)], 10, 0);

    assert_eq!(
        converted(two_digit_scale).await,
        texts(&[
            Some("-0.5"),
            Some("100.1"),
            Some("5"),
            Some("0"),
            Some("0.05"),
            None
        ])
    );
    assert_eq!(converted(one_digit_scale).await, texts(&[Some("0.5")]));
    assert_eq!(
        converted(scale_zero).await,
        texts(&[Some("1234567"), Some("-7")])
    );
}

/// Scenario: A BOOLEAN argument converts to TRUE or FALSE
#[tokio::test]
async fn exa_to_varchar_renders_boolean_as_uppercase() {
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));

    assert_eq!(
        converted(values).await,
        texts(&[Some("TRUE"), Some("FALSE"), None])
    );
}

/// Scenario: A DATE argument converts to ISO date text
#[tokio::test]
async fn exa_to_varchar_renders_date32_as_iso() {
    let values: ArrayRef = Arc::new(Date32Array::from(vec![Some(19_858), Some(0), None]));

    assert_eq!(
        converted(values).await,
        texts(&[Some("2024-05-15"), Some("1970-01-01"), None])
    );
}

/// Scenario: A NULL-typed argument converts to NULL text
#[tokio::test]
async fn exa_to_varchar_converts_null_type_to_null_text() {
    let ctx = session_with(vec![(
        "V",
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
    )]);

    let batches = ctx
        .sql("SELECT exa_to_varchar(NULL) AS s FROM scan_target")
        .await
        .expect("a NULL literal must plan")
        .collect()
        .await
        .expect("a NULL literal must convert");

    let values = batches[0].column(0);
    assert_eq!(values.data_type(), &DataType::Utf8);
    assert_eq!(values.null_count(), 2, "every row must be NULL");
}

const MAY_15_2024_10_00_UTC: i64 = 1_715_767_200;

/// Scenario: A TIMESTAMP argument converts to Exasol's default timestamp text
#[tokio::test]
async fn exa_to_varchar_renders_timestamps_with_six_truncated_fraction_digits() {
    let columns: Vec<(ArrayRef, Vec<Option<&str>>)> = vec![
        (
            Arc::new(TimestampSecondArray::from(vec![
                Some(MAY_15_2024_10_00_UTC),
                None,
            ])),
            vec![Some("2024-05-15 10:00:00.000000"), None],
        ),
        (
            Arc::new(TimestampMillisecondArray::from(vec![
                MAY_15_2024_10_00_UTC * 1_000 + 123,
            ])),
            vec![Some("2024-05-15 10:00:00.123000")],
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![
                MAY_15_2024_10_00_UTC * 1_000_000_000 + 999_999_900,
                -1,
            ])),
            vec![
                Some("2024-05-15 10:00:00.999999"),
                Some("1969-12-31 23:59:59.999999"),
            ],
        ),
        (
            Arc::new(
                TimestampMicrosecondArray::from(vec![MAY_15_2024_10_00_UTC * 1_000_000 + 1])
                    .with_timezone("+02:00"),
            ),
            vec![Some("2024-05-15 10:00:00.000001")],
        ),
    ];

    for (values, expected) in columns {
        let data_type = values.data_type().clone();
        assert_eq!(converted(values).await, texts(&expected), "{data_type}");
    }
}

#[test]
fn exa_to_varchar_rejects_a_timestamp_outside_the_calendar() {
    let values: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));

    let error = invoke(vec![values]).expect_err("an unrepresentable instant must be refused");

    assert!(
        error
            .to_string()
            .contains("exa_to_varchar cannot convert the timestamp"),
        "{error}"
    );
}

/// The spec's captured DOUBLE corpus: every Background and DOUBLE-scenario example.
const CAPTURED_DOUBLE_TEXTS: [(f64, &str); 19] = [
    (0.5, "0.5"),
    (-0.5, "-0.5"),
    (5.0, "5"),
    (1.0 / 3.0, "0.333333333333333"),
    (711.56 / 3.0, "237.186666666667"),
    (123_456_789_012_345.6, "123456789012346"),
    (100_000_000_000_000.0, "100000000000000"),
    (1e15, "1e15"),
    (1e20, "1e20"),
    (0.0001, "0.0001"),
    (1e-5, "1e-5"),
    (1.234e-5, "1.234e-5"),
    (1_234_567_890_123_456.0, "1.23456789012346e15"),
    (-0.0, "0"),
    (1e-20, "9.99999999999999e-21"),
    (1e23, "9.99999999999999e22"),
    (1e-16, "9.99999999999999e-17"),
    (999_999_999_999_999.5, "1000000000000000"),
    (f64::MAX, "1.79769313486232e308"),
];

/// Scenario: A DOUBLE argument converts to Exasol's DOUBLE text
#[tokio::test]
async fn exa_to_varchar_renders_double_as_exasol_text() {
    let mut values: Vec<Option<f64>> = CAPTURED_DOUBLE_TEXTS
        .iter()
        .map(|(v, _)| Some(*v))
        .collect();
    values.push(None);
    let mut expected: Vec<Option<&str>> = CAPTURED_DOUBLE_TEXTS
        .iter()
        .map(|(_, t)| Some(*t))
        .collect();
    expected.push(None);
    let widened_float32: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(0.1),
        Some(1.5),
        Some(f32::MAX),
        None,
    ]));

    assert_eq!(
        converted(Arc::new(Float64Array::from(values))).await,
        texts(&expected)
    );
    assert_eq!(
        converted(widened_float32).await,
        texts(&[
            Some("0.100000001490116"),
            Some("1.5"),
            Some("3.40282346638529e38"),
            None
        ])
    );
}

/// Scenario: A DOUBLE argument converts to Exasol's DOUBLE text
#[tokio::test]
async fn exa_to_varchar_maps_nan_to_null_and_rejects_infinity() {
    let with_nan: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, 1.0]));
    let float32_nan: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN]));

    assert_eq!(converted(with_nan).await, texts(&[None, Some("1")]));
    assert_eq!(converted(float32_nan).await, texts(&[None]));
    for infinite in [f64::INFINITY, f64::NEG_INFINITY] {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![infinite]));
        let error = invoke(vec![values]).expect_err("an infinite value must fail the query");
        assert!(
            error.to_string().contains("exa_to_varchar(") && error.to_string().contains("infinite"),
            "{error}"
        );
    }
}

fn column_texts(values: &ArrayRef) -> Vec<Option<String>> {
    let text = cast(values, &DataType::Utf8).expect("an emitted column must be text");
    text.as_string::<i32>()
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

/// Scenario: A JSON-fallback type converts to the text the scan emits for it
#[tokio::test]
async fn exa_to_varchar_matches_scan_emit_text_for_json_fallback_types() {
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), None, Some(3)]),
        None,
    ]));
    let ctx = session_with(vec![
        ("C_DECIMAL_38", decimal(vec![Some(1_234_500), None], 38, 4)),
        ("C_LIST", list),
        (
            "C_BINARY",
            Arc::new(BinaryArray::from_opt_vec(vec![Some(b"abc"), None])),
        ),
        (
            "C_TIME",
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(37_800_000_001),
                None,
            ])),
        ),
    ]);
    register_nested_json_render_udf(&ctx);
    let emit_sql = build_scan_sql(&ctx, "scan_target", &minimal_spec())
        .await
        .expect("the scan projection must build");
    let emitted = ctx
        .sql(&emit_sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");
    let converted = ctx
        .sql(
            "SELECT exa_to_varchar(\"C_DECIMAL_38\"), exa_to_varchar(\"C_LIST\"), \
             exa_to_varchar(\"C_BINARY\"), exa_to_varchar(\"C_TIME\") FROM scan_target",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");

    for column in 0..4 {
        assert_eq!(
            column_texts(converted[0].column(column)),
            column_texts(emitted[0].column(column)),
            "column {column} must convert to the text the scan emits"
        );
    }
    assert_eq!(
        column_texts(converted[0].column(0))[0].as_deref(),
        Some("123.4500"),
        "an out-of-domain decimal keeps every scale digit"
    );
}

fn context_with_acctbal() -> SessionContext {
    session_with(vec![
        (
            "C_CUSTKEY",
            Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
        ),
        (
            "C_ACCTBAL",
            Arc::new(Float64Array::from(vec![Some(100.0), Some(-50.0), None])),
        ),
    ])
}

fn acctbal_greater_than_zero() -> serde_json::Value {
    json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "c_acctbal"},
        "right": {"type": "literal_exactnumeric", "value": 0}
    })
}

/// Scenario: CAST renders the mapped target type per dialect
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
    let rows = collect_texts(&ctx, &sql, "s").await;

    assert_eq!(
        rows,
        vec![Some("TRUE".to_string()), Some("FALSE".to_string()), None,]
    );
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concat_predicate_matches_exasol_uppercase_not_datafusion_lowercase() {
    let ctx = context_with_acctbal();
    let concat_expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [acctbal_greater_than_zero(), {"type": "literal_string", "value": ""}]
    });
    let fragment = vs_expression::render_expression(&concat_expr).expect("CONCAT must translate");

    let sql_true = format!("SELECT \"C_CUSTKEY\" FROM scan_target WHERE {fragment} = 'TRUE'");
    let matched_true = collect_texts(&ctx, &sql_true, "C_CUSTKEY").await;
    assert_eq!(matched_true, vec![Some("1".to_string())]);

    let sql_lower = format!("SELECT \"C_CUSTKEY\" FROM scan_target WHERE {fragment} = 'true'");
    let matched_lower = collect_texts(&ctx, &sql_lower, "C_CUSTKEY").await;
    assert!(matched_lower.is_empty());
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
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
    let mut labels = collect_texts(&ctx, &sql, "g").await;
    labels.sort();

    assert_eq!(
        labels,
        vec![None, Some("FALSE".to_string()), Some("TRUE".to_string())]
    );
}

fn string_cast_of(argument: serde_json::Value) -> String {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [argument],
        "dataType": {"type": "VARCHAR", "size": 40}
    });
    vs_expression::render_expression(&expr).expect("a string CAST must translate")
}

fn exact_numeric(value: &str) -> serde_json::Value {
    json!({"type": "literal_exactnumeric", "value": value})
}

/// Scenario: A value DataFusion computes as Float64 converts with the DOUBLE rule
#[tokio::test]
async fn float64_arithmetic_converts_with_the_double_rule() {
    let ctx = session_with(vec![("C_ACCTBAL", decimal(vec![Some(71_156)], 12, 2))]);
    register_checked_float_div_udf(&ctx);
    let acctbal = json!({"type": "column", "name": "c_acctbal"});
    let times_fractional_literal = string_cast_of(json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [acctbal, exact_numeric("1.5")]
    }));
    let rounded_quotient = string_cast_of(json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [
            {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [acctbal, exact_numeric("3")]},
            exact_numeric("2")
        ]
    }));

    let sql = format!(
        "SELECT {times_fractional_literal} AS product, {rounded_quotient} AS quotient \
         FROM scan_target"
    );

    assert_eq!(
        collect_texts(&ctx, &sql, "product").await,
        texts(&[Some("1067.34")])
    );
    assert_eq!(
        collect_texts(&ctx, &sql, "quotient").await,
        texts(&[Some("237.19")])
    );
}
