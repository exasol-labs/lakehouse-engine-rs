use super::declared_columns_test_support::{declared, numeric, varchar};
use super::*;
use crate::scan::checked_div::{CheckedFloatDivError, register_checked_float_div_udf};
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use exasol_udf_sdk::value::{ColumnInfo, ExaType, Value};
use futures::stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[test]
fn redact_storage_error_redacts_secret_values_end_to_end() {
    let secret = "minio-super-secret-key";
    let raw = format!("S3 GET failed: signature used key {secret} (403)");
    let err = redact_storage_error(raw, &["minioadmin", secret]);
    let text = err.to_string();
    assert!(
        !text.contains(secret),
        "surfaced error must not contain the literal secret: {text}"
    );
    assert!(
        text.contains("scan failed"),
        "error must keep the user-facing summary: {text}"
    );
}

struct CapturingCtx {
    rows: Vec<Vec<Value>>,
    ipc_batches: Vec<Vec<u8>>,
    output_columns: Vec<exasol_udf_sdk::value::ColumnInfo>,
    /// Arity beyond what `output_column` hands out: a truncated declaration.
    declared_arity_override: Option<usize>,
}

impl CapturingCtx {
    /// Declared list verbatim, for drift a well-formed `EMITS` list cannot express.
    fn with_columns(columns: Vec<ColumnInfo>) -> Self {
        Self {
            rows: Vec::new(),
            ipc_batches: Vec::new(),
            output_columns: columns,
            declared_arity_override: None,
        }
    }

    fn declaring(types: &[(&str, ExaType)]) -> Self {
        Self::with_columns(declared(types))
    }

    fn decoded_batches(&self) -> Vec<RecordBatch> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        self.ipc_batches
            .iter()
            .map(|bytes| {
                StreamReader::try_new(Cursor::new(bytes), None)
                    .expect("IPC bytes must be a valid Arrow IPC stream")
                    .next()
                    .expect("IPC stream must contain exactly one batch")
                    .expect("IPC read must not error")
            })
            .collect()
    }
}

impl exasol_udf_sdk::context::UdfContext for CapturingCtx {
    fn input_column_count(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::User("no input".into()))
    }
    fn input_column(
        &self,
        idx: usize,
    ) -> Result<&exasol_udf_sdk::value::ColumnInfo, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::Type(format!(
            "input column {idx} out of range"
        )))
    }
    fn output_column_count(&self) -> usize {
        self.declared_arity_override
            .unwrap_or(self.output_columns.len())
    }
    fn output_column(
        &self,
        idx: usize,
    ) -> Result<&exasol_udf_sdk::value::ColumnInfo, exasol_udf_sdk::error::UdfError> {
        self.output_columns.get(idx).ok_or_else(|| {
            exasol_udf_sdk::error::UdfError::Type(format!("output column {idx} out of range"))
        })
    }
    /// Must NOT be called on the raw-row emit_stream path.
    fn emit(&mut self, values: Vec<Value>) -> Result<(), exasol_udf_sdk::error::UdfError> {
        self.rows.push(values);
        Ok(())
    }
    fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
        Ok(false)
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), exasol_udf_sdk::error::UdfError> {
        self.ipc_batches.push(ipc.to_vec());
        Ok(())
    }
}

struct VecStream {
    schema: arrow::datatypes::SchemaRef,
    inner:
        Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, DataFusionError>> + Send + 'static>>,
}

impl VecStream {
    fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = batches[0].schema();
        let items: Vec<Result<RecordBatch, DataFusionError>> =
            batches.into_iter().map(Ok).collect();
        Self {
            schema,
            inner: Box::pin(stream::iter(items)),
        }
    }
}

impl RecordBatchStream for VecStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for VecStream {
    type Item = Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

fn make_batch(values: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let arr = Arc::new(Int32Array::from(values.to_vec()));
    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Scenario: emit_stream emits one Arrow IPC batch per RecordBatch — no Vec<Value> intermediate.
#[tokio::test]
async fn emits_batch_by_batch_without_materializing() {
    let input_batches = vec![
        make_batch(&[1, 2]),
        make_batch(&[3, 4]),
        make_batch(&[5, 6]),
    ];
    let stream = Box::pin(VecStream::new(input_batches));

    let mut ctx = CapturingCtx::declaring(&[("x", ExaType::Int32)]);
    let mut timers = PhaseTimers::start();
    let total = emit_stream(&mut ctx, stream, &[], &mut timers)
        .await
        .unwrap();

    assert_eq!(total, 6, "total must equal sum of all batch row counts");

    assert_eq!(
        ctx.ipc_batches.len(),
        3,
        "exactly 3 IPC payloads must be captured (one per input batch)"
    );

    assert!(
        ctx.rows.is_empty(),
        "emit() must not be called on the raw IPC path; got {} row-by-row calls",
        ctx.rows.len()
    );

    let decoded = ctx.decoded_batches();
    assert_eq!(
        decoded.len(),
        3,
        "decoded batch count must match payload count"
    );

    use arrow::array::Int32Array;
    let expected_values = [&[1i32, 2][..], &[3, 4], &[5, 6]];
    for (batch, expected) in decoded.iter().zip(expected_values.iter()) {
        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column 0 must be Int32Array");
        for (row_idx, &expected_val) in expected.iter().enumerate() {
            assert_eq!(
                col.value(row_idx),
                expected_val,
                "IPC-decoded value must match original"
            );
        }
    }
}

/// Scenario: ResourcesExhausted, direct or Context/External-wrapped, surfaces as memory exhaustion without credentials.
#[tokio::test]
async fn resources_exhausted_surfaces_as_memory_error_not_storage_error() {
    let secret = "AKIAIOSFODNN7EXAMPLE";
    let secrets = [secret];

    let direct = DataFusionError::ResourcesExhausted(
        "Failed to allocate additional 256 MiB for HashAggregateExec".to_string(),
    );
    let err_direct = classify_scan_error(direct, &secrets);
    let text_direct = err_direct.to_string();
    assert!(
        text_direct.contains("memory exhausted"),
        "direct: must contain 'memory exhausted': {text_direct}"
    );
    assert!(
        !text_direct.contains("assigned data could not be read"),
        "direct: must NOT be classified as storage error: {text_direct}"
    );
    assert!(
        !text_direct.contains(secret),
        "direct: must not contain secret: {text_direct}"
    );

    // DataFusion 54's sort calls e.context("...") on ResourcesExhausted.
    let context_wrapped = DataFusionError::ResourcesExhausted("pool limit exceeded".to_string())
        .context(format!(
            "External sort failed; secret would be bad: {secret}"
        ));
    let err_ctx = classify_scan_error(context_wrapped, &secrets);
    let text_ctx = err_ctx.to_string();
    assert!(
        text_ctx.contains("memory exhausted"),
        "context-wrapped: must contain 'memory exhausted': {text_ctx}"
    );
    assert!(
        !text_ctx.contains("assigned data could not be read"),
        "context-wrapped: must NOT be classified as storage error: {text_ctx}"
    );
    assert!(
        !text_ctx.contains(secret),
        "context-wrapped: must not contain secret: {text_ctx}"
    );

    let external_wrapped = DataFusionError::External(Box::new(
        DataFusionError::ResourcesExhausted("repartition OOM".to_string()),
    ));
    let err_ext = classify_scan_error(external_wrapped, &secrets);
    let text_ext = err_ext.to_string();
    assert!(
        text_ext.contains("memory exhausted"),
        "external-wrapped: must contain 'memory exhausted': {text_ext}"
    );
    assert!(
        !text_ext.contains("assigned data could not be read"),
        "external-wrapped: must NOT be classified as storage error: {text_ext}"
    );

    let storage_err = DataFusionError::Execution("S3 read failed: 403".to_string());
    let err_storage = classify_scan_error(storage_err, &[]);
    let text_storage = err_storage.to_string();
    assert!(
        text_storage.contains("assigned data could not be read"),
        "non-OOM error must use storage path: {text_storage}"
    );
    assert!(
        !text_storage.contains("memory exhausted"),
        "non-OOM error must NOT look like memory error: {text_storage}"
    );
}

/// Scenario: a checked-division failure in any wrapping surfaces as the arithmetic error, without storage framing.
#[test]
fn classify_scan_error_names_a_checked_division_failure_without_the_storage_prefix() {
    let zero_divisor = || CheckedFloatDivError::ZeroDivisor {
        numerator: 7.0,
        divisor: 0.0,
    };
    let nestings: Vec<(&str, DataFusionError)> = vec![
        (
            "bare External",
            DataFusionError::External(Box::new(zero_divisor())),
        ),
        (
            "Context-wrapped",
            DataFusionError::External(Box::new(zero_divisor()))
                .context("ProjectionExec: evaluating expression"),
        ),
        (
            "through ArrowError::ExternalError",
            DataFusionError::from(ArrowError::ExternalError(Box::new(
                DataFusionError::External(Box::new(zero_divisor())),
            ))),
        ),
    ];

    for (nesting, error) in nestings {
        let text = classify_scan_error(error, &[]).to_string();
        assert!(
            text.contains("division by zero"),
            "{nesting}: must name the division by zero: {text}"
        );
        assert!(
            !text.contains("assigned data could not be read"),
            "{nesting}: must NOT carry the storage-read framing: {text}"
        );
        assert!(
            !text.contains("memory exhausted"),
            "{nesting}: must NOT look like a memory error: {text}"
        );
    }
}

/// Scenario: an overflow keeps its own wording through the classifier.
#[test]
fn classify_scan_error_keeps_an_out_of_range_division_distinct_from_a_zero_divisor() {
    let overflow = DataFusionError::External(Box::new(CheckedFloatDivError::NonFiniteResult {
        numerator: 1e300,
        divisor: 1e-300,
        quotient: f64::INFINITY,
    }));

    let text = classify_scan_error(overflow, &[]).to_string();

    assert!(
        text.contains("numeric value out of range"),
        "an overflow must name an out-of-range value: {text}"
    );
    assert!(
        !text.contains("division by zero"),
        "an overflow must not be reported as a division by zero: {text}"
    );
    assert!(
        !text.contains("assigned data could not be read"),
        "an overflow is arithmetic, not a storage failure: {text}"
    );
}

/// Scenario: a credential value in `secrets` never reaches a checked-division message.
#[test]
fn classify_scan_error_redacts_secrets_from_a_checked_division_failure() {
    let secret = "AKIAIOSFODNN7EXAMPLE";
    let wrapped = DataFusionError::External(Box::new(CheckedFloatDivError::ZeroDivisor {
        numerator: 7.0,
        divisor: 0.0,
    }))
    .context(format!("reading s3://bucket/f.parquet?key={secret}"));

    let text = classify_scan_error(wrapped, &[secret]).to_string();

    assert!(
        !text.contains(secret),
        "the surfaced message must not contain the literal secret: {text}"
    );
    assert!(
        !text.contains("s3://bucket/f.parquet"),
        "the wrapping chain's text must not be surfaced at all, so a \
         credential-bearing fragment cannot reach the user even unredacted: \
         {text}"
    );
    assert!(
        text.contains("division by zero"),
        "the message must still name the division by zero: {text}"
    );
}

/// Scenario: every `ExaType` variant resolves to the Arrow type `emit_batch`'s strict IPC feed accepts.
#[test]
fn coerce_maps_every_exa_type_variant_to_its_arrow_target() {
    use arrow::datatypes::TimeUnit;

    let cases: Vec<(ExaType, DataType)> = vec![
        (ExaType::Boolean, DataType::Boolean),
        (ExaType::Double, DataType::Float64),
        (ExaType::Int32, DataType::Int32),
        (ExaType::Int64, DataType::Int64),
        (numeric(20, 0), DataType::Decimal128(20, 0)),
        (numeric(36, 12), DataType::Decimal128(36, 12)),
        (ExaType::Date, DataType::Date32),
        (
            ExaType::Timestamp { precision: 6 },
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (varchar(), DataType::Utf8),
        (ExaType::Char { size: 10 }, DataType::Utf8),
        (ExaType::Unsupported, DataType::Utf8),
    ];

    for (typ, expected) in cases {
        let column = &declared(&[("C0", typ.clone())])[0];
        let got = target_arrow_type(column).unwrap_or_else(|e| {
            panic!("declared {typ:?} must resolve to an Arrow target, got error: {e}")
        });
        assert_eq!(got, expected, "declared {typ:?} must map to {expected:?}");
    }
}

/// Scenario: a declared `TIMESTAMP(p)` resolves to the Arrow unit of that precision, never `Utf8`.
#[test]
fn exa_type_timestamp_maps_to_the_arrow_unit_of_its_declared_precision() {
    use arrow::datatypes::TimeUnit;

    let cases = [
        (0, TimeUnit::Millisecond),
        (1, TimeUnit::Millisecond),
        (2, TimeUnit::Millisecond),
        (3, TimeUnit::Millisecond),
        (4, TimeUnit::Microsecond),
        (5, TimeUnit::Microsecond),
        (6, TimeUnit::Microsecond),
        (7, TimeUnit::Nanosecond),
        (8, TimeUnit::Nanosecond),
        (9, TimeUnit::Nanosecond),
    ];
    for (precision, expected_unit) in cases {
        let column = &declared(&[("TS", ExaType::Timestamp { precision })])[0];
        assert_eq!(
            target_arrow_type(column).expect("TIMESTAMP must resolve"),
            DataType::Timestamp(expected_unit, None),
            "declared TIMESTAMP({precision})"
        );
    }
}

/// Scenario: a nanosecond column declared `TIMESTAMP(9)` keeps all nine digits through `coerce_column`.
#[test]
fn nanosecond_column_declared_timestamp_9_keeps_every_digit() {
    let instant = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_nano_opt(0, 0, 0, 123_456_789)
        .unwrap();
    let nanos = instant.and_utc().timestamp_nanos_opt().unwrap();
    let source: ArrayRef = Arc::new(arrow::array::TimestampNanosecondArray::from(vec![Some(
        nanos,
    )]));

    let column = &declared(&[("TS", ExaType::Timestamp { precision: 9 })])[0];
    let target = target_arrow_type(column).expect("TIMESTAMP(9) must resolve");
    let coerced = coerce_column(&source, column, &target).expect("coercion must succeed");

    assert_eq!(coerced.data_type(), source.data_type());
    let values = coerced
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .expect("coerced column stays a nanosecond timestamp");
    assert_eq!(values.value(0), nanos);
}

/// Scenario: `coerce_batch_to_exa_types` casts every column to its declared ExaType's Arrow type.
#[test]
fn coerce_batch_casts_every_column_to_declared_exatype() {
    use arrow::array::{
        Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringViewArray, UInt32Array,
    };
    use arrow::datatypes::Field;

    // An Iceberg `int` whose DECIMAL(10,0) declaration was binned to Int64.
    let int32_to_int64: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
    // COUNT(*) in the Int64 bin, produced as Decimal128(10,0).
    let dec10_count_to_int64: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![5i128, 7, 9])
            .with_precision_and_scale(10, 0)
            .unwrap(),
    );
    let int64_to_int32: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let uint32_to_dec20: Arc<dyn arrow::array::Array> =
        Arc::new(UInt32Array::from(vec![10u32, 20, 30]));
    let f32_to_double: Arc<dyn arrow::array::Array> =
        Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5]));
    let f64_double: Arc<dyn arrow::array::Array> =
        Arc::new(Float64Array::from(vec![1.0f64, 2.0, 3.0]));
    let dec_narrow_to_wide: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![100i128, 200, 300])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let date: Arc<dyn arrow::array::Array> = Arc::new(Date32Array::from(vec![0, 1, 2]));
    // The shape `decimal_to_varchar_exasol`'s `regexp_replace(...)` chain produces (#211).
    let utf8view_to_varchar: Arc<dyn arrow::array::Array> =
        Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
    let utf8view_to_char: Arc<dyn arrow::array::Array> =
        Arc::new(StringViewArray::from(vec!["p", "q", "r"]));

    let cases: Vec<(&str, Arc<dyn arrow::array::Array>, ExaType, DataType)> = vec![
        (
            "c_int32_to_int64",
            int32_to_int64,
            ExaType::Int64,
            DataType::Int64,
        ),
        (
            "c_count_to_int64",
            dec10_count_to_int64,
            ExaType::Int64,
            DataType::Int64,
        ),
        (
            "c_int32_bin",
            int64_to_int32,
            ExaType::Int32,
            DataType::Int32,
        ),
        (
            "c_uint_dec20",
            uint32_to_dec20,
            numeric(20, 0),
            DataType::Decimal128(20, 0),
        ),
        ("c_f32", f32_to_double, ExaType::Double, DataType::Float64),
        ("c_f64", f64_double, ExaType::Double, DataType::Float64),
        (
            "c_dec_scaled",
            dec_narrow_to_wide,
            numeric(20, 2),
            DataType::Decimal128(20, 2),
        ),
        ("c_date", date, ExaType::Date, DataType::Date32),
        ("c_str", utf8view_to_varchar, varchar(), DataType::Utf8),
        (
            "c_char",
            utf8view_to_char,
            ExaType::Char { size: 4 },
            DataType::Utf8,
        ),
    ];

    let fields: Vec<Field> = cases
        .iter()
        .map(|(name, col, _, _)| Field::new(*name, col.data_type().clone(), true))
        .collect();
    let columns: Vec<Arc<dyn arrow::array::Array>> =
        cases.iter().map(|(_, col, _, _)| col.clone()).collect();
    let types: Vec<(&str, ExaType)> = cases
        .iter()
        .map(|(name, _, t, _)| (*name, t.clone()))
        .collect();

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();

    let coerced = coerce_batch_to_exa_types(batch, &declared(&types))
        .expect("coercion must succeed for all mapping cases");

    for (idx, (name, _, typ, expected)) in cases.iter().enumerate() {
        let got = coerced.schema().field(idx).data_type().clone();
        assert_eq!(
            &got, expected,
            "column {name} (declared {typ:?}) must coerce to {expected:?}, got {got:?}"
        );
    }

    assert_eq!(coerced.num_rows(), 3);
    let c0 = coerced
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c_int32_to_int64 must now be Int64");
    assert_eq!(c0.value(0), 1);
    assert_eq!(c0.value(2), 3);
    let c1 = coerced
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c_count_to_int64 must now be Int64");
    assert_eq!(c1.value(0), 5);
    assert_eq!(c1.value(2), 9);
}

/// Scenario: a UTC `Timestamp(Microsecond, Some("UTC"))` declared TIMESTAMP loses its zone with the instant unchanged (#118).
#[test]
fn coerce_timestamptz_column_to_plain_timestamp_preserves_utc() {
    use arrow::array::{Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, TimeUnit};

    // Includes the Iceberg-spec example instant and a pre-epoch value, so any zone shift shows.
    let raw_micros: Vec<i64> = vec![1_510_881_034_000_000, 0, -1_000_000];

    let src_arr = TimestampMicrosecondArray::from(raw_micros.clone()).with_timezone("UTC");
    assert_eq!(
        src_arr.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "source column must be a tz-aware UTC timestamp"
    );

    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(src_arr)]).unwrap();

    let coerced = coerce_batch_to_exa_types(
        batch,
        &declared(&[("ts", ExaType::Timestamp { precision: 6 })]),
    )
    .expect("timestamptz→TIMESTAMP coercion must succeed");

    assert_eq!(
        coerced.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "column must be stripped to a timezone-naive TIMESTAMP"
    );

    let out = coerced
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("coerced column must be a TimestampMicrosecondArray");
    for (i, &expected) in raw_micros.iter().enumerate() {
        assert_eq!(
            out.value(i),
            expected,
            "raw micros value at row {i} must be identical after the tz-strip cast"
        );
    }
}

/// Scenario: emit_stream coerces each column to its declared EMITS ExaType before emit_batch.
#[tokio::test]
async fn emit_stream_coerces_columns_to_declared_exatypes_before_emit_batch() {
    use arrow::array::{Int64Array, StringArray, StringViewArray};
    use arrow::datatypes::Field;

    // DataFusion yields Utf8View for strings (schema_force_view_types).
    let view_arr = StringViewArray::from(vec!["event-01", "event-02", "event-03"]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8View, false),
    ]));
    let id_col = Arc::new(Int32Array::from(vec![1i32, 2, 3]));
    let view_batch = RecordBatch::try_new(schema, vec![id_col, Arc::new(view_arr)]).unwrap();

    let stream = Box::pin(VecStream::new(vec![view_batch]));
    // DECIMAL(10,0) is binned to Int64.
    let mut ctx = CapturingCtx::declaring(&[("id", ExaType::Int64), ("name", varchar())]);

    let mut timers = PhaseTimers::start();
    let total = emit_stream(&mut ctx, stream, &[], &mut timers)
        .await
        .expect("emit_stream must succeed and coerce to declared ExaTypes");

    assert_eq!(total, 3, "all 3 rows must be counted");
    assert_eq!(ctx.ipc_batches.len(), 1, "exactly 1 IPC payload");

    let decoded = ctx.decoded_batches();
    let decoded_batch = &decoded[0];

    assert_eq!(
        decoded_batch.schema().field(0).data_type(),
        &DataType::Int64,
        "column 0 must be coerced to Int64 (the DECIMAL(10,0) ExaType target)"
    );
    let int_col = decoded_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("decoded column 0 must be Int64Array");
    assert_eq!(int_col.value(0), 1);
    assert_eq!(int_col.value(2), 3);

    assert_eq!(
        decoded_batch.schema().field(1).data_type(),
        &DataType::Utf8,
        "column 1 must be coerced to Utf8 (Utf8View)"
    );
    let str_col = decoded_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("decoded column 1 must be StringArray");
    assert_eq!(str_col.value(0), "event-01");
    assert_eq!(str_col.value(1), "event-02");
    assert_eq!(str_col.value(2), "event-03");
}

/// Scenario: a relaxed column crosses the emit boundary at its declared Exasol type.
#[test]
fn a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch() {
    use arrow::array::{
        Array, Decimal128Array, Float64Array, Int64Array, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, TimeUnit};

    // `int` -> `long`: narrow-boundary value already in Int64; DECIMAL(20,0) is NUMERIC, a real cast.
    let int_boundary = i32::MAX as i64;
    let long_col: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![int_boundary]));

    // `float` -> `double`: identity target.
    let double_boundary = f32::MAX as f64;
    let double_col: Arc<dyn arrow::array::Array> =
        Arc::new(Float64Array::from(vec![double_boundary]));

    // `decimal(15,5)` -> `decimal(20,5)`: identity target.
    let decimal_boundary: i128 = 999_999_999_999_999;
    let decimal_col: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![decimal_boundary])
            .with_precision_and_scale(20, 5)
            .unwrap(),
    );

    // `date` -> `timestamp`: the Delta-protocol date boundary 9999-12-31 23:59:59 UTC.
    let timestamp_boundary: i64 = 253_402_300_799_000_000;
    let timestamp_col: Arc<dyn arrow::array::Array> =
        Arc::new(TimestampMicrosecondArray::from(vec![timestamp_boundary]));

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_long", DataType::Int64, false),
        Field::new("c_double", DataType::Float64, false),
        Field::new("c_decimal", DataType::Decimal128(20, 5), false),
        Field::new(
            "c_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![long_col, double_col, decimal_col, timestamp_col],
    )
    .unwrap();

    let types = declared(&[
        ("c_long", numeric(20, 0)),
        ("c_double", ExaType::Double),
        ("c_decimal", numeric(20, 5)),
        ("c_timestamp", ExaType::Timestamp { precision: 6 }),
    ]);

    let coerced = coerce_batch_to_exa_types(batch, &types)
        .expect("an already-widened value must coerce without error");

    assert_eq!(coerced.num_rows(), 1);

    let long_out = coerced
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("c_long must coerce to Decimal128 (a NUMERIC-binned DECIMAL(20,0))");
    assert_eq!(long_out.data_type(), &DataType::Decimal128(20, 0));
    assert!(!long_out.is_null(0), "widened long value must not be NULL");
    assert_eq!(
        long_out.value(0),
        int_boundary as i128,
        "long value at the int32 boundary must round-trip unchanged"
    );

    let double_out = coerced
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("c_double must remain Float64 (DOUBLE PRECISION target)");
    assert!(
        !double_out.is_null(0),
        "widened double value must not be NULL"
    );
    assert_eq!(
        double_out.value(0),
        double_boundary,
        "double value at the float32 boundary must round-trip unchanged"
    );

    let decimal_out = coerced
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("c_decimal must remain Decimal128(20,5)");
    assert_eq!(decimal_out.data_type(), &DataType::Decimal128(20, 5));
    assert!(
        !decimal_out.is_null(0),
        "widened decimal value must not be NULL"
    );
    assert_eq!(
        decimal_out.value(0),
        decimal_boundary,
        "decimal value at the decimal(15,5) boundary must round-trip unchanged"
    );

    let timestamp_out = coerced
        .column(3)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("c_timestamp must remain Timestamp(Microsecond, None)");
    assert!(
        !timestamp_out.is_null(0),
        "widened timestamp value must not be NULL"
    );
    assert_eq!(
        timestamp_out.value(0),
        timestamp_boundary,
        "timestamp value at the date boundary must round-trip unchanged"
    );
}

/// Scenario: a division whose type the error chain lost replaces the flattened storage-read framing (#370).
#[tokio::test]
async fn reframe_checked_division_names_a_failure_the_error_chain_lost() {
    let session = session_with_a_raised_division().await;

    // What the row-filter route produces: only the Debug rendering survives in a string.
    let flattened = classify_scan_error(
        DataFusionError::External(Box::new(ArrowError::ComputeError(
            "Error evaluating filter predicate: External(ZeroDivisor { numerator: 7.0, \
             divisor: 0.0 })"
                .to_string(),
        ))),
        &[],
    );
    assert!(
        flattened
            .to_string()
            .contains("assigned data could not be read"),
        "precondition: a flattened predicate error is indistinguishable from a \
         storage failure to the classifier, else this test proves nothing: {flattened}"
    );

    let reframed = reframe_checked_division(&session, flattened, &[]).to_string();

    assert!(
        reframed.starts_with("data exception - division by zero"),
        "the reframed error must LEAD with the division by zero: {reframed}"
    );
    assert!(
        !reframed.contains("assigned data could not be read"),
        "the storage-read framing must not survive ANYWHERE in the message: a \
         support case reading it goes looking at object storage for a failure \
         the user's own SQL caused: {reframed}"
    );
    assert!(
        !reframed.contains(CONCURRENT_FAILURE_PREAMBLE),
        "nothing must be appended on this route: the surfaced error IS the \
         recorded division, so a second failure would be a duplicate of the \
         first under the framing this fix removes: {reframed}"
    );
}

/// Scenario: a storage failure alongside a recorded division is replaced by it (accepted masking trade-off).
#[tokio::test]
async fn reframe_checked_division_replaces_a_generic_storage_failure_with_the_division() {
    let session = session_with_a_raised_division().await;
    let storage = classify_scan_error(
        DataFusionError::Execution("S3 read failed: 403".into()),
        &[],
    );

    let reframed = reframe_checked_division(&session, storage, &[]).to_string();

    assert!(
        reframed.contains("division by zero"),
        "the division the user's own SQL caused must be the reported failure: \
         {reframed}"
    );
    assert!(
        !reframed.contains("assigned data could not be read"),
        "the storage-read framing must not survive: it is indistinguishable \
         from the flattened division's own framing, so keeping it here keeps it \
         on issue #370's route too: {reframed}"
    );
    assert!(
        !reframed.contains("403"),
        "the trade-off this rule accepts, recorded here so it cannot be lost: \
         an unrelated storage failure concurrent with a division IS masked: \
         {reframed}"
    );
}

/// Scenario: a session whose division never raised leaves an unrelated failure untouched.
#[test]
fn reframe_checked_division_leaves_an_unrelated_failure_untouched() {
    let session = SessionContext::new();
    register_checked_float_div_udf(&session);
    let storage = classify_scan_error(
        DataFusionError::Execution("S3 read failed: 403".into()),
        &[],
    );

    let reframed = reframe_checked_division(&session, storage, &[]).to_string();

    assert!(
        reframed.contains("assigned data could not be read"),
        "an unrelated failure must keep its storage-read framing: {reframed}"
    );
    assert!(
        !reframed.contains("division by zero"),
        "an unrelated failure must NOT be renamed a division by zero: {reframed}"
    );
}

/// The only state in which `reframe_checked_division` does anything.
async fn session_with_a_raised_division() -> SessionContext {
    let session = SessionContext::new();
    register_checked_float_div_udf(&session);
    session
        .sql(&format!(
            "SELECT {}(7, 0)",
            vs_expression::CHECKED_FLOAT_DIV_FN
        ))
        .await
        .expect("the statement must plan")
        .collect()
        .await
        .expect_err("a zero divisor must raise");
    session
}

/// Scenario: a scan that divided by zero and exhausted memory reports both, division first.
#[tokio::test]
async fn reframe_checked_division_keeps_a_concurrent_memory_exhaustion_failure_visible() {
    let session = session_with_a_raised_division().await;
    let exhausted = classify_scan_error(
        DataFusionError::ResourcesExhausted("pool of 100 bytes".into()),
        &[],
    );

    let reframed = reframe_checked_division(&session, exhausted, &[]).to_string();

    assert!(
        reframed.starts_with("data exception - division by zero"),
        "the division must be named first: it is the failure the user's own SQL \
         caused and can act on, got: {reframed}"
    );
    assert!(
        reframed.contains("memory exhausted"),
        "the concurrent memory exhaustion must stay visible: {reframed}"
    );
    assert!(
        reframed.contains("pool of 100 bytes"),
        "the memory-exhaustion detail must survive the composition, or the \
         operator loses the pool-sizing signal: {reframed}"
    );
    assert!(
        reframed.contains(CONCURRENT_FAILURE_PREAMBLE),
        "the appended failure must be introduced as a SECOND failure, not read \
         as a continuation of the division's own message: {reframed}"
    );
}

/// Scenario: the appended memory-exhaustion text is redacted with the full dimension-inclusive secret set.
#[tokio::test]
async fn reframe_checked_division_redacts_secrets_from_the_appended_failure() {
    const DIMENSION_SIDE_TOKEN: &str = "tw1l1ght-vended-token";
    let session = session_with_a_raised_division().await;
    // Classified without the token, as a fact-side-only secret set upstream leaves it.
    let exhausted = classify_scan_error(
        DataFusionError::ResourcesExhausted(format!(
            "pool of 100 bytes reading s3://dim/f.parquet?t={DIMENSION_SIDE_TOKEN}"
        )),
        &[],
    );
    assert!(
        exhausted.to_string().contains(DIMENSION_SIDE_TOKEN),
        "precondition: the upstream classification must leave the token in \
         place, else this test proves nothing: {exhausted}"
    );

    let reframed =
        reframe_checked_division(&session, exhausted, &[DIMENSION_SIDE_TOKEN]).to_string();

    assert!(
        !reframed.contains(DIMENSION_SIDE_TOKEN),
        "the appended failure must carry no credential value: {reframed}"
    );
    assert!(
        reframed.contains("division by zero"),
        "redacting the appended failure must not cost the division's own \
         message: {reframed}"
    );
    assert!(
        reframed.contains("s3://dim/f.parquet"),
        "redaction must strip the credential only, leaving the path that names \
         what failed: {reframed}"
    );
}

/// Scenario: a declared column count disagreeing with the batch fails naming both counts, emitting nothing.
#[tokio::test]
async fn emit_stream_fails_when_declared_column_count_disagrees() {
    let cases: Vec<(&str, Vec<(&str, ExaType)>)> = vec![
        ("absent", vec![]),
        (
            "too many",
            vec![
                ("C0", ExaType::Int32),
                ("C1", ExaType::Int32),
                ("C2", ExaType::Int32),
            ],
        ),
    ];

    for (label, types) in cases {
        let stream = Box::pin(VecStream::new(vec![make_batch(&[1, 2])]));
        let mut ctx = CapturingCtx::declaring(&types);
        let mut timers = PhaseTimers::start();
        let err = emit_stream(&mut ctx, stream, &[], &mut timers)
            .await
            .expect_err("a mismatched declaration must fail the call");
        let text = err.to_string();
        assert!(
            text.contains(&types.len().to_string()) && text.contains('1'),
            "{label}: the error must name both counts: {text}"
        );
        assert!(
            ctx.ipc_batches.is_empty(),
            "{label}: no batch may be emitted when the declaration disagrees"
        );
    }
}

/// Scenario: an erroring `output_column(i)` fails the call naming `i`.
#[tokio::test]
async fn emit_stream_fails_when_a_declared_column_cannot_be_read() {
    // Arity 2 but only column 0 available: a truncated declaration.
    let mut ctx = CapturingCtx::with_columns(declared(&[("C0", ExaType::Int32)]));
    ctx.declared_arity_override = Some(2);

    let stream = Box::pin(VecStream::new(vec![make_batch(&[1, 2])]));
    let mut timers = PhaseTimers::start();
    let err = emit_stream(&mut ctx, stream, &[], &mut timers)
        .await
        .expect_err("an unreadable declared column must fail the call");
    let text = err.to_string();
    assert!(
        text.contains('1'),
        "the error must name the column index it could not read: {text}"
    );
    assert!(
        ctx.ipc_batches.is_empty(),
        "no batch may be emitted when a declared column cannot be read"
    );
}

/// Scenario: a `Numeric` column outside `Decimal128`'s range fails naming the column, never `Utf8`.
#[tokio::test]
async fn emit_stream_fails_on_numeric_with_out_of_range_payload() {
    let cases: Vec<(&str, ExaType)> = vec![
        ("zero precision", numeric(0, 0)),
        ("precision above Decimal128", numeric(39, 0)),
        ("scale above Decimal128", numeric(10, 39)),
        ("scale above precision", numeric(10, 12)),
    ];

    for (label, typ) in cases {
        let stream = Box::pin(VecStream::new(vec![make_batch(&[1, 2])]));
        let columns = declared(&[("OFFENDING_COL", typ)]);
        let mut ctx = CapturingCtx::with_columns(columns);
        let mut timers = PhaseTimers::start();

        let err = emit_stream(&mut ctx, stream, &[], &mut timers)
            .await
            .expect_err("an out-of-range NUMERIC must fail the call");
        let text = err.to_string();
        assert!(
            text.contains("OFFENDING_COL"),
            "{label}: the error must name the offending column: {text}"
        );
        assert!(
            ctx.ipc_batches.is_empty(),
            "{label}: no batch may be emitted, and no Utf8 may be substituted"
        );
    }
}

/// Scenario: NUMERIC drift through `target_arrow_type` never yields `Utf8`.
#[test]
fn a_drifted_numeric_never_resolves_to_the_string_target() {
    let drifted = vec![numeric(39, 0), numeric(10, 12)];
    for typ in drifted {
        let column = &declared(&[("C0", typ.clone())])[0];
        let resolved = target_arrow_type(column);
        assert!(
            resolved.is_err(),
            "{typ:?} must fail rather than resolve, got {resolved:?}"
        );
    }
}

/// Scenario: a value the declared target cannot represent fails naming the column; nothing is emitted.
#[tokio::test]
async fn coerce_fails_when_a_value_does_not_fit_its_declared_target() {
    use arrow::array::Decimal128Array;
    use arrow::datatypes::Field;

    // 10^36 needs 37 digits: one more than DECIMAL(36,2), inside a widening SUM's Decimal128(38,2).
    let too_wide: i128 = 10i128.pow(36);
    let column: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![too_wide])
            .with_precision_and_scale(38, 2)
            .expect("source decimal"),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "WIDE_SUM",
        DataType::Decimal128(38, 2),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();

    let mut ctx = CapturingCtx::with_columns(declared(&[("WIDE_SUM", numeric(36, 2))]));
    let stream = Box::pin(VecStream::new(vec![batch]));
    let mut timers = PhaseTimers::start();

    let err = emit_stream(&mut ctx, stream, &[], &mut timers)
        .await
        .expect_err("a value the declared target cannot hold must fail the call");
    let text = err.to_string();
    assert!(
        text.contains("WIDE_SUM"),
        "the error must name the column that could not be coerced: {text}"
    );
    assert!(
        ctx.ipc_batches.is_empty(),
        "no batch may be emitted, and no NULL may be substituted for the value"
    );
}
