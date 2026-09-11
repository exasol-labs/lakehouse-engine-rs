use super::*;
use crate::scan::checked_div::{CheckedFloatDivError, register_checked_float_div_udf};
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use exasol_udf_sdk::value::Value;
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

// ---------------------------------------------------------------------------
// Fake UdfContext that captures Arrow IPC bytes from emit_batch.
// emit() is intentionally left as a no-op trap — if emit_stream calls it on
// the raw-row path, emit_was_called == true will fail the assertion.
// ---------------------------------------------------------------------------
struct CapturingCtx {
    /// Row-by-row emit calls — must be empty after emit_stream on the raw path.
    rows: Vec<Vec<Value>>,
    /// Accumulated IPC byte payloads, one entry per emit_batch call.
    ipc_batches: Vec<Vec<u8>>,
}

impl CapturingCtx {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            ipc_batches: Vec::new(),
        }
    }

    /// Decode all captured IPC payloads back to RecordBatches for assertions.
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
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, exasol_udf_sdk::error::UdfError> {
        Err(exasol_udf_sdk::error::UdfError::User("no input".into()))
    }
    /// Row-by-row emit — must NOT be called on the raw-row emit_stream path.
    fn emit(&mut self, values: &[Value]) -> Result<(), exasol_udf_sdk::error::UdfError> {
        self.rows.push(values.to_vec());
        Ok(())
    }
    fn next(&mut self) -> Result<bool, exasol_udf_sdk::error::UdfError> {
        Ok(false)
    }
    /// Capture the IPC bytes so the test can decode and assert their content.
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), exasol_udf_sdk::error::UdfError> {
        self.ipc_batches.push(ipc.to_vec());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// A SendableRecordBatchStream built from a Vec of RecordBatches.
// ---------------------------------------------------------------------------
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
///
/// Invariants verified:
/// 1. total == 6 (num_rows counted correctly across 3 batches of 2 rows each).
/// 2. Exactly 3 IPC payloads captured — one per input batch, not one per row.
/// 3. No row-by-row emit calls (ctx.rows is empty — emit() was never called).
/// 4. Each IPC payload decodes back to a RecordBatch with the correct values,
///    proving the bytes faithfully round-trip through Arrow IPC.
///
/// The "never holds >1 batch" invariant is structural: emit_stream holds only
/// one RecordBatch reference at a time (counted → emit_batch(&batch) → drop).
#[tokio::test]
async fn emits_batch_by_batch_without_materializing() {
    let input_batches = vec![
        make_batch(&[1, 2]),
        make_batch(&[3, 4]),
        make_batch(&[5, 6]),
    ];
    let stream = Box::pin(VecStream::new(input_batches));

    let mut ctx = CapturingCtx::new();
    let mut timers = PhaseTimers::start();
    let total = emit_stream(&mut ctx, stream, &[], &[], &mut timers)
        .await
        .unwrap();

    // 1. Row count is the sum of num_rows across all batches.
    assert_eq!(total, 6, "total must equal sum of all batch row counts");

    // 2. One IPC payload per batch — never one per row.
    assert_eq!(
        ctx.ipc_batches.len(),
        3,
        "exactly 3 IPC payloads must be captured (one per input batch)"
    );

    // 3. Row-by-row emit must never be called on the raw-row path.
    assert!(
        ctx.rows.is_empty(),
        "emit() must not be called on the raw IPC path; got {} row-by-row calls",
        ctx.rows.len()
    );

    // 4. IPC round-trip: decode and verify values.
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

// ---------------------------------------------------------------------------
// Task 3.2 — ResourcesExhausted surfacing
// ---------------------------------------------------------------------------

/// Scenario: A ResourcesExhausted error surfaces as a memory-exhaustion error,
/// not a storage error, and carries no credential values.
///
/// Verifies all three nesting forms DataFusion 54 can produce:
/// - direct ResourcesExhausted
/// - Context-wrapped ResourcesExhausted (e.g. from sort's .context() call)
/// - External-wrapped ResourcesExhausted
#[tokio::test]
async fn resources_exhausted_surfaces_as_memory_error_not_storage_error() {
    let secret = "AKIAIOSFODNN7EXAMPLE";
    let secrets = [secret];

    // --- 1. Direct ResourcesExhausted ---
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

    // --- 2. Context-wrapped ResourcesExhausted ---
    // Sort in DataFusion 54 calls e.context("...") on ResourcesExhausted.
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

    // --- 3. External-wrapped ResourcesExhausted ---
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

    // --- 4. Non-ResourcesExhausted error still routes to storage path ---
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

/// Scenario: a checked-division failure reaches the user as the arithmetic
/// error it is, WITHOUT the storage-read framing. `scan failed: assigned data
/// could not be read` misnames a user's own division by zero, and it would send
/// a support case looking at object storage.
///
/// Verifies every nesting form the error can arrive in, because recognition is
/// BY TYPE on the source chain rather than by matching message text: bare
/// `External`, `Context`-wrapped, and wrapped through
/// `ArrowError::ExternalError` the way a stream adapter produces.
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

/// Scenario: an overflow keeps its own wording through the classifier, so the
/// two causes a checked division can fail for stay distinguishable in a support
/// case after the error has been framed.
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

/// Scenario: a credential value in `secrets` never reaches a checked-division
/// message. The surfaced text is built from the two operands alone, so the
/// wrapping chain's own text — which can carry a credential-bearing fragment
/// from an outer error layer — is dropped rather than redacted into.
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

// ---------------------------------------------------------------------------
// Task R7 — Utf8View normalization: emit_stream must not crash on view types
// ---------------------------------------------------------------------------

/// Scenario: with NO declared types (`&[]`), `coerce_batch_to_exa_types`
/// normalizes Utf8View → Utf8 and leaves non-view columns untouched — the
/// backward-compatible fallback for specs that predate `emit_exa_types`.
///
/// Invariants:
/// 1. A batch with only non-view types is returned unchanged (fast path).
/// 2. A batch with Utf8View is rebuilt: column type becomes Utf8, values preserved.
/// 3. A mixed batch (Int32 + Utf8View) normalizes only the view column.
#[test]
fn coerce_batch_empty_types_normalizes_utf8view_to_utf8() {
    use arrow::array::{StringArray, StringViewArray};
    use arrow::datatypes::Field;

    // Fast path: no view types — the batch is returned unchanged.
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let col = Arc::new(Int32Array::from(vec![1i32, 2]));
    let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
    let result = coerce_batch_to_exa_types(batch.clone(), &[]).unwrap();
    assert_eq!(
        result.schema(),
        batch.schema(),
        "fast path: schema unchanged"
    );
    assert_eq!(result.num_rows(), 2, "fast path: row count unchanged");

    // Utf8View column → Utf8.
    let view_arr = StringViewArray::from(vec!["hello", "world"]);
    let view_schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Utf8View,
        false,
    )]));
    let view_batch = RecordBatch::try_new(view_schema, vec![Arc::new(view_arr)]).unwrap();
    let normalized = coerce_batch_to_exa_types(view_batch, &[]).unwrap();
    assert_eq!(
        normalized.schema().field(0).data_type(),
        &DataType::Utf8,
        "Utf8View must be normalized to Utf8"
    );
    let str_col = normalized
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("normalized column must be StringArray (Utf8)");
    assert_eq!(str_col.value(0), "hello");
    assert_eq!(str_col.value(1), "world");

    // Mixed: Int32 + Utf8View — only the view column changes.
    let mixed_schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int32, false),
        Field::new("s", DataType::Utf8View, false),
    ]));
    let int_col = Arc::new(Int32Array::from(vec![42i32]));
    let view_col = Arc::new(StringViewArray::from(vec!["abc"]));
    let mixed_batch =
        RecordBatch::try_new(mixed_schema, vec![int_col, view_col as Arc<_>]).unwrap();
    let norm = coerce_batch_to_exa_types(mixed_batch, &[]).unwrap();
    assert_eq!(norm.schema().field(0).data_type(), &DataType::Int32);
    assert_eq!(norm.schema().field(1).data_type(), &DataType::Utf8);
}

// ---------------------------------------------------------------------------
// Coerce-to-declared-ExaType — table-driven over the full mapping
// ---------------------------------------------------------------------------

/// Scenario: `coerce_batch_to_exa_types` casts EVERY output column to the Arrow
/// type the declared EMITS ExaType accepts, across the full mapping table —
/// reproducing Exasol's DECIMAL precision binning (Int32 / Int64 / Decimal128).
///
/// This is the generalized fix for BOTH live bench failures:
///   "Arrow column 0 of type Int32 cannot feed declared ExaType Int64"
///   "Arrow column 0 of type Decimal128(10, 0) cannot feed declared ExaType Int64"
///
/// Each case provides a source Arrow array (the type DataFusion's Parquet scan
/// or aggregate might actually produce) and the declared Exasol EMITS type
/// string; the coerced column's Arrow type must equal the canonical target for
/// that ExaType (`exasol_type_to_arrow`), and must NOT remain the source type.
#[test]
fn coerce_batch_casts_every_column_to_declared_exatype() {
    use crate::types::mapping::exasol_type_to_arrow;
    use arrow::array::{
        Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringViewArray, UInt32Array,
    };
    use arrow::datatypes::Field;

    // (column name, source Arrow array, declared Exasol EMITS type)
    // First live failure: Iceberg `int` declared DECIMAL(10,0) (ExaType Int64),
    // but DataFusion produced Arrow Int32 → must cast Int32→Int64.
    let int32_to_int64: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
    // Second live failure: COUNT(*) declared DECIMAL(10,0) (ExaType Int64),
    // produced as Decimal128(10,0) → must cast Decimal128→Int64.
    let dec10_count_to_int64: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![5i128, 7, 9])
            .with_precision_and_scale(10, 0)
            .unwrap(),
    );
    // Small scale-0 DECIMAL declared DECIMAL(5,0) → ExaType Int32.
    let int64_to_int32: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    // UInt32 declared DECIMAL(20,0) (p>18 → ExaType Numeric/Decimal128).
    let uint32_to_dec20: Arc<dyn arrow::array::Array> =
        Arc::new(UInt32Array::from(vec![10u32, 20, 30]));
    // Float32 declared DOUBLE PRECISION.
    let f32_to_double: Arc<dyn arrow::array::Array> =
        Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5]));
    // Float64 already matches DOUBLE PRECISION (fast path).
    let f64_double: Arc<dyn arrow::array::Array> =
        Arc::new(Float64Array::from(vec![1.0f64, 2.0, 3.0]));
    // Decimal width divergence (scale>0): DECIMAL(10,2) declared DECIMAL(20,2).
    let dec_narrow_to_wide: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![100i128, 200, 300])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let date: Arc<dyn arrow::array::Array> = Arc::new(Date32Array::from(vec![0, 1, 2]));
    // Utf8View → Utf8 for a VARCHAR(2000000)-declared column; this is also the
    // exact shape `decimal_to_varchar_exasol`'s `regexp_replace(...)` chain
    // produces for a projected DECIMAL-column stringification (issue #211).
    let utf8view_to_varchar: Arc<dyn arrow::array::Array> =
        Arc::new(StringViewArray::from(vec!["a", "b", "c"]));

    let cases: Vec<(&str, Arc<dyn arrow::array::Array>, &str)> = vec![
        ("c_int32_to_int64", int32_to_int64, "DECIMAL(10,0)"),
        ("c_count_to_int64", dec10_count_to_int64, "DECIMAL(10,0)"),
        ("c_int32_bin", int64_to_int32, "DECIMAL(5,0)"),
        ("c_uint_dec20", uint32_to_dec20, "DECIMAL(20,0)"),
        ("c_f32", f32_to_double, "DOUBLE PRECISION"),
        ("c_f64", f64_double, "DOUBLE PRECISION"),
        ("c_dec_scaled", dec_narrow_to_wide, "DECIMAL(20,2)"),
        ("c_date", date, "DATE"),
        ("c_str", utf8view_to_varchar, "VARCHAR(2000000)"),
    ];

    let fields: Vec<Field> = cases
        .iter()
        .map(|(name, col, _)| Field::new(*name, col.data_type().clone(), true))
        .collect();
    let columns: Vec<Arc<dyn arrow::array::Array>> =
        cases.iter().map(|(_, col, _)| col.clone()).collect();
    let exa_types: Vec<String> = cases.iter().map(|(_, _, t)| t.to_string()).collect();

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();

    let coerced = coerce_batch_to_exa_types(batch, &exa_types)
        .expect("coercion must succeed for all mapping cases");

    for (idx, (name, _, declared)) in cases.iter().enumerate() {
        let got = coerced.schema().field(idx).data_type().clone();
        match exasol_type_to_arrow(declared) {
            Some(expected) => assert_eq!(
                got, expected,
                "column {name} (declared {declared}) must coerce to {expected:?}, got {got:?}"
            ),
            None => assert_eq!(
                got,
                DataType::Utf8,
                "column {name} (declared {declared}) must coerce to Utf8, got {got:?}"
            ),
        }
    }

    // Explicit bin assertions for the two live-failure columns.
    assert_eq!(
        coerced.schema().field(0).data_type(),
        &DataType::Int64,
        "Int32 declared DECIMAL(10,0) must become Int64 (1st live failure)"
    );
    assert_eq!(
        coerced.schema().field(1).data_type(),
        &DataType::Int64,
        "Decimal128(10,0) COUNT(*) declared DECIMAL(10,0) must become Int64 (2nd live failure)"
    );
    assert_eq!(
        coerced.schema().field(2).data_type(),
        &DataType::Int32,
        "Int64 declared DECIMAL(5,0) must become Int32 (small-precision bin)"
    );

    // Row count and values must survive the Int32→Int64 cast.
    assert_eq!(coerced.num_rows(), 3);
    let c0 = coerced
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c_int32_to_int64 must now be Int64");
    assert_eq!(c0.value(0), 1);
    assert_eq!(c0.value(2), 3);
    // COUNT(*) values survive Decimal128→Int64.
    let c1 = coerced
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c_count_to_int64 must now be Int64");
    assert_eq!(c1.value(0), 5);
    assert_eq!(c1.value(2), 9);
}

/// Scenario: when `exa_types` is empty (no declared schema carried), the batch
/// still falls back to view-type normalization so a Utf8View column does not
/// crash `emit_batch`. Backward-compatible with specs that lack `emit_exa_types`.
#[test]
fn coerce_batch_empty_types_falls_back_to_view_normalization() {
    use arrow::array::StringViewArray;
    use arrow::datatypes::Field;

    let view = Arc::new(StringViewArray::from(vec!["x", "y"]));
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
    let batch = RecordBatch::try_new(schema, vec![view]).unwrap();

    let coerced = coerce_batch_to_exa_types(batch, &[]).expect("empty types must not error");
    assert_eq!(
        coerced.schema().field(0).data_type(),
        &DataType::Utf8,
        "empty types must still normalize Utf8View to Utf8"
    );
}

/// Scenario (#118): a timezone-aware `Timestamp(Microsecond, Some("UTC"))`
/// column declared `EMITS "TIMESTAMP"` is coerced to `Timestamp(Microsecond,
/// None)` with the underlying UTC epoch value preserved bit-for-bit — no shift.
///
/// This is the emit-boundary half of the Iceberg-timestamptz → plain Exasol
/// TIMESTAMP fix: `iceberg_primitive_to_exasol` now declares timestamptz as
/// "TIMESTAMP", so `exasol_type_to_arrow("TIMESTAMP")` = `Timestamp(us, None)`
/// is the coercion target. An Iceberg timestamptz is a UTC instant (stored as
/// UTC, not retaining a source zone), so stripping the timezone must keep the
/// instant unchanged rather than localizing it.
#[test]
fn coerce_timestamptz_column_to_plain_timestamp_preserves_utc() {
    use arrow::array::{Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, TimeUnit};

    // Raw micros-since-epoch values, treated as UTC instants. Includes the
    // Iceberg-spec example instant (2017-11-17 01:10:34 UTC), the epoch, and a
    // pre-epoch value so any spurious timezone shift would move a value.
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

    // Declared EMITS type is plain "TIMESTAMP" (the post-fix timestamptz mapping).
    let exa_types = vec!["TIMESTAMP".to_string()];
    let coerced = coerce_batch_to_exa_types(batch, &exa_types)
        .expect("timestamptz→TIMESTAMP coercion must succeed");

    // The coerced column must be timezone-naive Timestamp(Microsecond, None).
    assert_eq!(
        coerced.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "column must be stripped to a timezone-naive TIMESTAMP"
    );

    // The underlying epoch values must be preserved bit-for-bit (no shift).
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

/// Scenario: emit_stream coerces each column to its declared EMITS ExaType
/// before emit_batch — end-to-end through the IPC round-trip.
///
/// This is the regression test for BOTH live E2E failures:
///   "Arrow column 0 of type Int32 cannot feed declared ExaType Int64"
///   "Arrow column 1 of type Utf8View cannot feed declared ExaType String"
///
/// The source batch is `Int32` + `Utf8View` (what DataFusion's Parquet scan
/// produces); the declared EMITS types are `DECIMAL(10,0)` (which Exasol bins
/// to ExaType Int64) and `VARCHAR(2000000)` (string). After emit_stream the
/// decoded IPC batch must have `Int64` and `Utf8`, with values preserved.
///
/// Invariants:
/// 1. emit_stream does not return an error (no VM crash).
/// 2. Column 0 is coerced Int32 → Int64 (DECIMAL(10,0) bins to ExaType Int64).
/// 3. Column 1 is coerced Utf8View → Utf8; string values survive unchanged.
#[tokio::test]
async fn emit_stream_coerces_columns_to_declared_exatypes_before_emit_batch() {
    use arrow::array::{Int64Array, StringArray, StringViewArray};
    use arrow::datatypes::Field;

    // Build a RecordBatch with Int32 + Utf8View — what DataFusion 58 produces
    // for an Iceberg `int` column, and a string column (schema_force_view_types).
    let view_arr = StringViewArray::from(vec!["event-01", "event-02", "event-03"]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8View, false),
    ]));
    let id_col = Arc::new(Int32Array::from(vec![1i32, 2, 3]));
    let view_batch = RecordBatch::try_new(schema, vec![id_col, Arc::new(view_arr)]).unwrap();

    let stream = Box::pin(VecStream::new(vec![view_batch]));
    let mut ctx = CapturingCtx::new();

    // Declared EMITS types: DECIMAL(10,0) (Exasol bins p≤18,s=0 → ExaType Int64)
    // and VARCHAR. This is the exact shape of the live Q1 failure.
    let exa_types = vec!["DECIMAL(10,0)".to_string(), "VARCHAR(2000000)".to_string()];

    // Must not error — previously crashed with the two "cannot feed" errors.
    let mut timers = PhaseTimers::start();
    let total = emit_stream(&mut ctx, stream, &[], &exa_types, &mut timers)
        .await
        .expect("emit_stream must succeed and coerce to declared ExaTypes");

    assert_eq!(total, 3, "all 3 rows must be counted");
    assert_eq!(ctx.ipc_batches.len(), 1, "exactly 1 IPC payload");

    let decoded = ctx.decoded_batches();
    let decoded_batch = &decoded[0];

    // Column 0: Int32 coerced to Int64 (DECIMAL(10,0) bins to ExaType Int64).
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

    // Column 1: Utf8View coerced to Utf8 (VARCHAR target).
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

/// Scenario: A relaxed column crosses the emit boundary at its declared
/// Exasol type.
///
/// A relaxed column (Delta type widening / Iceberg type promotion) reaches
/// `coerce_batch_to_exa_types` already cast to the table's CURRENT Arrow type
/// by the scan's column-binding adapter, before this function ever runs — the
/// feature adds no relaxation-aware branch, pair table, or allow-list to this
/// path. This test proves the existing generic `safe: true` cast (the same
/// one `coerce_batch_casts_every_column_to_declared_exatype` exercises for an
/// unevolved column) needs none of that: a value sitting at the narrow source
/// type's boundary, already stored under the WIDENED Arrow type, round-trips
/// through the coercion to its declared EMITS type unchanged, with no NULL
/// introduced — across `int`→`long`, `float`→`double`, a scale-preserving
/// decimal widening, and `date`→`timestamp`.
#[test]
fn a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch() {
    use arrow::array::{
        Array, Decimal128Array, Float64Array, Int64Array, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, TimeUnit};

    // `int` -> `long`: the source file's value sits at the narrow `int32`
    // boundary, but the column already carries Arrow `Int64` (the scan's
    // column-binding adapter already cast it). Declared "DECIMAL(20,0)" (the
    // `long` binning) parses to precision 20 — above the Int64 threshold of
    // 18 — so the target is `Decimal128(20,0)`, a genuine cast.
    let int_boundary = i32::MAX as i64;
    let long_col: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![int_boundary]));

    // `float` -> `double`: value at the narrow `float32` boundary, already
    // stored as Arrow `Float64`. Declared "DOUBLE PRECISION" maps to
    // `Float64` — an identity target, proven via the same generic path.
    let double_boundary = f32::MAX as f64;
    let double_col: Arc<dyn arrow::array::Array> =
        Arc::new(Float64Array::from(vec![double_boundary]));

    // `decimal(15,5)` -> `decimal(20,5)`: value at the narrow decimal(15,5)
    // boundary (15 nines), already stored as Arrow `Decimal128(20,5)`.
    // Declared "DECIMAL(20,5)" has scale > 0, so `exasol_type_to_arrow`
    // returns `Decimal128(20,5)` — again an identity target.
    let decimal_boundary: i128 = 999_999_999_999_999;
    let decimal_col: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![decimal_boundary])
            .with_precision_and_scale(20, 5)
            .unwrap(),
    );

    // `date` -> `timestamp without time zone`: value at the Delta-protocol
    // date boundary (9999-12-31 23:59:59 UTC), already stored as Arrow
    // `Timestamp(Microsecond, None)` (the current, widened type). Declared
    // "TIMESTAMP" maps to the same Arrow type — an identity target.
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

    let exa_types = vec![
        "DECIMAL(20,0)".to_string(),
        "DOUBLE PRECISION".to_string(),
        "DECIMAL(20,5)".to_string(),
        "TIMESTAMP".to_string(),
    ];

    let coerced = coerce_batch_to_exa_types(batch, &exa_types)
        .expect("an already-widened value must coerce without error");

    assert_eq!(coerced.num_rows(), 1);

    let long_out = coerced
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("c_long must coerce to Decimal128 (DECIMAL(20,0) precision > 18)");
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

/// Scenario: a checked-division failure whose TYPE the error chain lost
/// REPLACES the flattened storage-read framing, leaving no trace of it.
///
/// This is issue #370's own route. DataFusion's Parquet row filter flattens a
/// predicate error into `ArrowError::ComputeError(format!("...{e:?}"))`, so
/// `classify_scan_error` sees no recognisable type and applies the storage-read
/// framing. The session holds the same division as a typed value, so reframing
/// names the arithmetic error without anyone matching text in that flattened
/// message.
///
/// On this route the surfaced error IS the recorded division wearing the
/// storage-read framing, so appending it would republish the one framing this
/// fix exists to remove. `e2e_float_div_by_zero_in_filter_fails_like_native_exasol`
/// asserts live that it never reaches the user, and this test pins the same
/// guarantee at the unit boundary.
#[tokio::test]
async fn reframe_checked_division_names_a_failure_the_error_chain_lost() {
    let session = session_with_a_raised_division().await;

    // Exactly what the row-filter route produces: the type is gone, only the
    // Debug rendering of it survives inside an opaque compute-error string.
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

/// Scenario: a generic storage failure surfaced alongside a recorded division is
/// REPLACED by the division, and the masking of that storage failure is the
/// accepted trade-off rather than an oversight.
///
/// `classify_scan_error`'s fallback branch frames anything it cannot recognise
/// by type as a storage read failure, and the flattened row-filter division
/// lands in exactly that branch. Nothing separates the two without matching
/// DataFusion's `Error evaluating filter predicate` wording, the coupling this
/// module refuses everywhere. So the branch replaces, and an unrelated storage
/// failure that happens to share the scan is lost. `ResourcesExhausted`, which
/// `classify_scan_error` recognises on the typed error before any text exists,
/// is the one surfaced failure that still survives alongside the division.
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

/// Scenario: a session whose checked division never raised leaves an unrelated
/// scan failure exactly as `classify_scan_error` framed it. The reframing is
/// inert for every scan whose SQL contains no division.
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

/// A session whose registered checked division has already raised a zero
/// divisor. That is the only state in which `reframe_checked_division` does
/// anything at all, so every test of the recording arm starts here.
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

/// Scenario: a scan that both divided by zero and exhausted its memory pool
/// reports BOTH failures, with the division named first.
///
/// DataFusion evaluates partitions concurrently and surfaces exactly one of
/// their errors to the caller, so the recorded division and the surfaced error
/// can come from different partitions. Replacing the surfaced error erases the
/// `ResourcesExhausted` signal the mission's bounded-execution guarantee rests
/// on: the operator fixes the division, re-runs, and meets the same pool limit
/// with no record that it was ever reported.
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

/// Scenario: the appended memory-exhaustion failure is redacted here, because
/// the classification upstream did not necessarily see every secret.
///
/// `raw_scan` and `partial_agg` classify with the fact side's credentials alone
/// (`spec.common.storage.secret_values()`), while the dispatcher redacts with
/// the union `all_secret_values()` that also covers a join's dimension side. A
/// dimension-side credential inside a memory-exhaustion message therefore
/// reaches this function still in place, and composing without redacting would
/// publish it. The guarantee is a property of this function, not of what its
/// callers happened to pass upstream.
#[tokio::test]
async fn reframe_checked_division_redacts_secrets_from_the_appended_failure() {
    const DIMENSION_SIDE_TOKEN: &str = "tw1l1ght-vended-token";
    let session = session_with_a_raised_division().await;
    // Classified WITHOUT the token, exactly as a fact-side-only secret set
    // upstream leaves it.
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
