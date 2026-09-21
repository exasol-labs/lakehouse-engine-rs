use super::declared_columns_test_support::{declared, numeric, varchar};
use super::*;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
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
    /// Row-by-row emit calls — must be empty after emit_stream on the raw path.
    rows: Vec<Vec<Value>>,
    /// Accumulated IPC byte payloads, one entry per emit_batch call.
    ipc_batches: Vec<Vec<u8>>,
    /// Declared output columns, read back through `UdfContext::output_column`.
    output_columns: Vec<exasol_udf_sdk::value::ColumnInfo>,
    /// Reported arity when it must exceed what `output_column` can hand out —
    /// the truncated-declaration shape a well-formed list cannot express.
    declared_arity_override: Option<usize>,
}

impl CapturingCtx {
    /// A context whose declared list is `columns` verbatim, for the drift cases
    /// a well-formed `EMITS` list cannot express.
    fn with_columns(columns: Vec<ColumnInfo>) -> Self {
        Self {
            rows: Vec::new(),
            ipc_batches: Vec::new(),
            output_columns: columns,
            declared_arity_override: None,
        }
    }

    /// A context whose call site declared `types` as its `EMITS` list.
    fn declaring(types: &[(&str, ExaType)]) -> Self {
        Self::with_columns(declared(types))
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
    /// Row-by-row emit — must NOT be called on the raw-row emit_stream path.
    fn emit(&mut self, values: Vec<Value>) -> Result<(), exasol_udf_sdk::error::UdfError> {
        self.rows.push(values);
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

    let mut ctx = CapturingCtx::declaring(&[("x", ExaType::Int32)]);
    let mut timers = PhaseTimers::start();
    let total = emit_stream(&mut ctx, stream, &[], &mut timers)
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

// ---------------------------------------------------------------------------
// Coerce-to-declared-ExaType — table-driven over the full mapping
// ---------------------------------------------------------------------------

/// Scenario: every `ExaType` variant the database can report resolves to the
/// Arrow type `emit_batch`'s strict IPC feed accepts for it.
///
/// The DECIMAL bin is READ, never re-derived: `Int32`, `Int64` and
/// `Numeric { precision, scale }` are three distinct variants the engine
/// already chose between, so nothing here parses a precision out of a type
/// string. Every remaining variant feeds `Utf8`, which subsumes the
/// `Utf8View`/`BinaryView` normalization and preserves what the removed
/// type-string path gave an unrecognized declaration.
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

/// Scenario (`type-mapping-timestamp-precision`): a declared `TIMESTAMP(p)`
/// resolves to the Arrow unit of THAT precision — never a fixed one, which under a
/// `TIMESTAMP(9)` declaration destroys every nanosecond digit through the strict
/// cast, and never the `Utf8` string path, which would stringify the value and
/// violate the declaration.
///
/// The unit is read from the same owner that produces the declaration string, so
/// the two sides cannot disagree about what `TIMESTAMP(9)` means. A precision
/// outside `{3, 6, 9}` resolves to the coarsest unit not coarser than it.
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

/// Scenario (`scan-execution-value-conversion`): a nanosecond column declared
/// `TIMESTAMP(9)` passes through `coerce_column` with all nine digits intact. The
/// fixed microsecond target this replaces destroyed three of them through the
/// strict (`safe: false`) cast, unrecorded.
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

/// Scenario: `coerce_batch_to_exa_types` casts EVERY output column to the Arrow
/// type the declared EMITS ExaType accepts, across the full mapping table —
/// reproducing Exasol's DECIMAL precision binning (Int32 / Int64 / Decimal128).
///
/// This is the generalized fix for BOTH live bench failures:
///   "Arrow column 0 of type Int32 cannot feed declared ExaType Int64"
///   "Arrow column 0 of type Decimal128(10, 0) cannot feed declared ExaType Int64"
///
/// Each case provides a source Arrow array (the type DataFusion's Parquet scan
/// or aggregate might actually produce) and the `ExaType` the database reports
/// for that output column; the coerced column's Arrow type must equal the target
/// for that variant, and must NOT remain the source type.
#[test]
fn coerce_batch_casts_every_column_to_declared_exatype() {
    use arrow::array::{
        Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringViewArray, UInt32Array,
    };
    use arrow::datatypes::Field;

    // First live failure: an Iceberg `int` whose DECIMAL(10,0) declaration the
    // engine binned to Int64, while DataFusion produced Arrow Int32.
    let int32_to_int64: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
    // Second live failure: COUNT(*) in the same Int64 bin, produced as
    // Decimal128(10,0) → must cast Decimal128→Int64.
    let dec10_count_to_int64: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![5i128, 7, 9])
            .with_precision_and_scale(10, 0)
            .unwrap(),
    );
    // A scale-0 DECIMAL the engine binned to Int32.
    let int64_to_int32: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    // UInt32 into a NUMERIC-binned DECIMAL(20,0).
    let uint32_to_dec20: Arc<dyn arrow::array::Array> =
        Arc::new(UInt32Array::from(vec![10u32, 20, 30]));
    let f32_to_double: Arc<dyn arrow::array::Array> =
        Arc::new(Float32Array::from(vec![1.5f32, 2.5, 3.5]));
    // Float64 already matches Double (fast path).
    let f64_double: Arc<dyn arrow::array::Array> =
        Arc::new(Float64Array::from(vec![1.0f64, 2.0, 3.0]));
    // Decimal width divergence (scale>0): Decimal128(10,2) into NUMERIC(20,2).
    let dec_narrow_to_wide: Arc<dyn arrow::array::Array> = Arc::new(
        Decimal128Array::from(vec![100i128, 200, 300])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let date: Arc<dyn arrow::array::Array> = Arc::new(Date32Array::from(vec![0, 1, 2]));
    // Utf8View → Utf8 for a String-declared column; this is also the exact shape
    // `decimal_to_varchar_exasol`'s `regexp_replace(...)` chain produces for a
    // projected DECIMAL-column stringification (issue #211).
    let utf8view_to_varchar: Arc<dyn arrow::array::Array> =
        Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
    // A CHAR-declared column takes the same Utf8 path as VARCHAR.
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

/// Scenario (#118): a timezone-aware `Timestamp(Microsecond, Some("UTC"))`
/// column declared `EMITS "TIMESTAMP"` is coerced to `Timestamp(Microsecond,
/// None)` with the underlying UTC epoch value preserved bit-for-bit — no shift.
///
/// This is the emit-boundary half of the Iceberg-timestamptz → plain Exasol
/// TIMESTAMP fix: `iceberg_primitive_to_exasol` declares timestamptz as
/// "TIMESTAMP", which the engine reports as `ExaType::Timestamp`, whose target
/// is `Timestamp(us, None)`. An Iceberg timestamptz is a UTC instant (stored as
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
    let coerced = coerce_batch_to_exa_types(
        batch,
        &declared(&[("ts", ExaType::Timestamp { precision: 6 })]),
    )
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
    // The call site declared DECIMAL(10,0), which the engine bins to Int64, and
    // VARCHAR. This is the exact shape of the live Q1 failure.
    let mut ctx = CapturingCtx::declaring(&[("id", ExaType::Int64), ("name", varchar())]);

    // Must not error — previously crashed with the two "cannot feed" errors.
    let mut timers = PhaseTimers::start();
    let total = emit_stream(&mut ctx, stream, &[], &mut timers)
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
/// path. This test proves the existing generic strict cast (the same one
/// `coerce_batch_casts_every_column_to_declared_exatype` exercises for an
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
    // column-binding adapter already cast it). "DECIMAL(20,0)" (the `long`
    // binning) is above the engine's Int64 bin, so it is reported as
    // NUMERIC(20,0) and the target is `Decimal128(20,0)`, a genuine cast.
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
    // NUMERIC(20,5) has scale > 0, so the target is `Decimal128(20,5)` —
    // again an identity target.
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

// ---------------------------------------------------------------------------
// Drift in the declared list is reported, never worked around
// ---------------------------------------------------------------------------

/// Scenario: the declared output-column list disagreeing with the batch's
/// column count fails the call, naming both counts, and emits nothing.
///
/// There is no fallback to a source-derived type: every scan call carries a
/// call-site `EMITS` clause, so an absent or wrong-arity declaration is drift.
/// Both directions are covered, plus the absent list, which after the removal
/// of the spec-carried types can only mean the accessors reported nothing.
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

/// Scenario: an `output_column(i)` that errors for a column the batch carries
/// fails the call, naming `i`, rather than degrading to a source-derived type.
#[tokio::test]
async fn emit_stream_fails_when_a_declared_column_cannot_be_read() {
    // The context reports an arity of 2 but can hand out only column 0, the
    // shape a truncated declaration produces.
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

/// Scenario: a `Numeric` column whose precision or scale is outside what
/// `Decimal128` represents fails the call naming that column — and is never
/// substituted with `Utf8`, which would put a string into a numeric column. A
/// valid Exasol NUMERIC declaration always carries both, within range.
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

/// Scenario: the same NUMERIC drift resolved through `target_arrow_type` never
/// yields `Utf8` — the assertion the emit-boundary tests above can only make
/// indirectly, since a failed call emits nothing either way.
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

/// Scenario: a value the declared target cannot represent fails the call naming
/// that column, and nothing is emitted.
///
/// The coercion cast is the last thing between a produced value and the wire, so
/// a lenient cast has no second chance to complain: it writes NULL, the shard
/// emits it, and the Exasol outer wrapper merges that NULL as if the shard had
/// no data for the column. A value that does not fit its declaration is drift,
/// and drift on this boundary is reported.
#[tokio::test]
async fn coerce_fails_when_a_value_does_not_fit_its_declared_target() {
    use arrow::array::Decimal128Array;
    use arrow::datatypes::Field;

    // Unscaled 10^36 needs 37 digits: one more than DECIMAL(36,2) holds, and
    // well inside the Decimal128(38,2) a widening SUM produces.
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
