//! #587: the typed scan render converts only integers and booleans to text, and DataFusion must
//! produce Exasol's text for them. Executes the rendered fragment, so renderer and result cannot
//! drift.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde_json::{Value as Json, json};
use vs_expression::exasol_type::ExasolType;

struct Columns;

impl vs_expression::ColumnTypeLookup for Columns {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        match column.get("name")?.as_str()? {
            "QTY" => Some(ExasolType::decimal(20, 0)),
            "D9" => Some(ExasolType::decimal(9, 0)),
            "B" => Some(ExasolType::Boolean),
            _ => None,
        }
    }
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
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        out.extend((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string())));
    }
    out
}

fn cast_to_varchar(name: &str) -> Json {
    json!({"type": "function_scalar_cast",
           "dataType": {"type": "VARCHAR", "size": 2000000, "characterSet": "UTF8"},
           "arguments": [{"type": "column", "name": name}]})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_and_boolean_columns_convert_to_exasol_text() {
    let render = |e: &Json| vs_expression::render_scan_expression(e, &Columns).unwrap();
    assert_eq!(
        texts(&render(&cast_to_varchar("QTY"))).await,
        vec![
            Some("-12".into()),
            Some("0".into()),
            Some("1234567890123".into()),
            None
        ]
    );
    assert_eq!(
        texts(&render(&cast_to_varchar("D9"))).await,
        vec![
            Some("-5".into()),
            Some("0".into()),
            Some("123456789".into()),
            None
        ]
    );
    assert_eq!(
        texts(&render(&cast_to_varchar("B"))).await,
        vec![
            Some("TRUE".into()),
            Some("FALSE".into()),
            None,
            Some("TRUE".into())
        ]
    );
    let upper = json!({"type": "function_scalar", "name": "UPPER", "arguments": [{"type": "column", "name": "QTY"}]});
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
