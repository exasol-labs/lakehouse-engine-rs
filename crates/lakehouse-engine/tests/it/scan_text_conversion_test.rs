//! #587: the typed scan render converts only integers and booleans to text, and DataFusion must
//! produce Exasol's text for them. Executes the rendered fragment, so renderer and result cannot
//! drift.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde_json::Value as Json;
use vs_expression::exasol_type::ExasolType;
use vs_expression::test_support::{TypedColumns, cast_to, col, scalar_fn, typed_columns};

fn columns() -> TypedColumns {
    typed_columns(&[
        ("QTY", ExasolType::decimal(20, 0)),
        ("D9", ExasolType::decimal(9, 0)),
        ("B", ExasolType::Boolean),
    ])
}

fn context() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("QTY", DataType::Int64, true),
        Field::new("D9", DataType::Decimal128(9, 0), true),
        Field::new("B", DataType::Boolean, true),
    ]));
    let qty = Int64Array::from(vec![Some(-12), Some(0), Some(1_234_567_890_123), None]);
    let d9 = Decimal128Array::from(vec![Some(-5), Some(0), Some(123_456_789), None])
        .with_precision_and_scale(9, 0)
        .unwrap();
    let b = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(qty), Arc::new(d9), Arc::new(b)],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table(
        "scan_target",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn texts(fragment: &str) -> Vec<Option<String>> {
    let df = context()
        .sql(&format!("SELECT {fragment} AS v FROM scan_target"))
        .await
        .unwrap_or_else(|e| panic!("{fragment} must plan: {e}"));
    let batches = df.collect().await.unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        // `CAST(... AS VARCHAR)` yields Utf8View in DataFusion; the scan's emit coerces either.
        let utf8 = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        let arr = utf8.as_any().downcast_ref::<StringArray>().unwrap();
        out.extend((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string())));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_and_boolean_columns_convert_to_exasol_text() {
    let render = |e: &Json| {
        vs_expression::render_scan_expression(e, &columns())
            .unwrap()
            .sql
    };
    assert_eq!(
        texts(&render(&cast_to(ExasolType::WIDEST_VARCHAR, col("QTY")))).await,
        vec![
            Some("-12".into()),
            Some("0".into()),
            Some("1234567890123".into()),
            None
        ]
    );
    assert_eq!(
        texts(&render(&cast_to(ExasolType::WIDEST_VARCHAR, col("D9")))).await,
        vec![
            Some("-5".into()),
            Some("0".into()),
            Some("123456789".into()),
            None
        ]
    );
    assert_eq!(
        texts(&render(&cast_to(ExasolType::WIDEST_VARCHAR, col("B")))).await,
        vec![
            Some("TRUE".into()),
            Some("FALSE".into()),
            None,
            Some("TRUE".into())
        ]
    );
    let upper = scalar_fn("UPPER", [col("QTY")]);
    assert_eq!(
        texts(&render(&upper)).await,
        vec![
            Some("-12".into()),
            Some("0".into()),
            Some("1234567890123".into()),
            None
        ]
    );
}
