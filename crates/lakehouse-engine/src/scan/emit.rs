/// Batch-by-batch incremental emit loop using Arrow IPC.
///
/// Streams DataFusion result one RecordBatch at a time: emit via IPC → drop.
/// Never collects all batches in memory simultaneously.
///
/// Architecture rules (CLAUDE.md):
/// - Fetch one batch, call `ctx.emit_batch(&batch)` (Arrow IPC bytes — ABI-safe),
///   drop the batch before fetching the next.
/// - Rely on the SDK's 4,000,000-byte auto-flush; always flush at end.
/// - Only IPC bytes cross the .so boundary — never Arrow types or Value intermediates.
use crate::scan::checked_div::{
    CheckedFloatDivError, find_checked_float_div_error, session_checked_float_div_failure,
};
use crate::scan::diagnostics::PhaseTimers;
use crate::types::mapping::TimestampPrecision;
use arrow::array::ArrayRef;
use arrow::compute::CastOptions;
use arrow::compute::kernels::cast::cast_with_options;
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use exasol_udf_sdk::context::{EmitBatch, UdfContext};
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{ColumnInfo, ExaType};
use futures::StreamExt;
use std::sync::Arc;

/// Emit all rows from a DataFusion stream, batch by batch via Arrow IPC.
///
/// Each batch is emitted via `ctx.emit_batch` (Arrow IPC bytes — ABI-safe),
/// then dropped before the next fetch. No `Vec<Value>` intermediate is created.
/// Returns Ok(rows_emitted) on success; surfaces scan errors as UdfError
/// with credentials redacted. `secrets` are the literal credential values that
/// must be stripped from any surfaced error string.
///
/// The call site's generated `EMITS (...)` clause is the sole declaration of
/// this call's output schema, so the declared columns are read once from `ctx`
/// before the batch loop and every column is coerced to the Arrow type its
/// declared `ExaType` accepts. DataFusion's physical Parquet type can diverge
/// from the logical type the VS declared, and `emit_batch` rejects ANY mismatch.
///
/// `timers` carries the phase accumulators (Task 4): the wait for each
/// `stream.next()` is attributed to the object-storage import phase, and the
/// coercion + `emit_batch` of each batch is attributed to the send-back/emit
/// phase. The timing only reads a monotonic clock around the SAME fetch / emit /
/// drop operations — it does NOT change the streaming discipline (one batch
/// fetched, emitted, dropped before the next).
pub async fn emit_stream(
    ctx: &mut dyn UdfContext,
    mut stream: SendableRecordBatchStream,
    secrets: &[&str],
    timers: &mut PhaseTimers,
) -> Result<u64, UdfError> {
    let declared = declared_output_columns(ctx)?;
    let mut total: u64 = 0;
    // Startup ends at the first batch fetch — seal it as the import loop opens.
    timers.seal_startup();
    loop {
        // --- object-storage import phase: await the next batch ---
        timers.import_started();
        let next = stream.next().await;
        timers.import_ended();

        let Some(result) = next else { break };

        // --- send-back/emit phase: coerce + emit this batch ---
        timers.emit_started();
        let emit_result = emit_one_batch(ctx, result, secrets, &declared);
        timers.emit_ended();
        total += emit_result?;
    }
    Ok(total)
}

/// Coerce and emit one fetched batch, returning the row count it contributed.
///
/// Factored out so the emit phase boundary in [`emit_stream`] brackets exactly
/// the coercion + `emit_batch` work, with the batch dropped before the next
/// fetch — preserving the never-hold-two-batches discipline.
fn emit_one_batch(
    ctx: &mut dyn UdfContext,
    result: Result<RecordBatch, DataFusionError>,
    secrets: &[&str],
    declared: &[ColumnInfo],
) -> Result<u64, UdfError> {
    let batch = result.map_err(|e| classify_scan_error(e, secrets))?;
    let batch = coerce_batch_to_exa_types(batch, declared)?;
    // Count rows before emitting — batch is borrowed by emit_batch.
    let rows = batch.num_rows() as u64;
    ctx.emit_batch(&batch)?;
    drop(batch);
    Ok(rows)
}

/// The output columns this call site declared, read once from the context.
///
/// The generated `EMITS (...)` clause is the only declaration of a scan call's
/// output schema, so both emit paths read it here rather than trusting a second
/// copy carried in the scan spec. The metadata is cloned because the borrow the
/// context hands out cannot outlive the `&mut` borrow `emit_batch` then needs.
/// A column the context cannot hand out is drift, and is reported rather than
/// worked around.
pub(crate) fn declared_output_columns(ctx: &dyn UdfContext) -> Result<Vec<ColumnInfo>, UdfError> {
    (0..ctx.output_column_count())
        .map(|idx| {
            ctx.output_column(idx).cloned().map_err(|e| {
                UdfError::User(format!(
                    "emit failed: declared output column {idx} could not be read: {e}"
                ))
            })
        })
        .collect()
}

/// Reject a produced column count that the declared list does not cover.
///
/// Shared by both emit paths so a declaration that does not match what the scan
/// produced fails identically whichever path is emitting.
pub(crate) fn check_declared_arity(
    declared: &[ColumnInfo],
    produced: usize,
) -> Result<(), UdfError> {
    if declared.len() == produced {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "emit failed: the call declares {} output column(s) but the scan produced {produced}",
        declared.len()
    )))
}

/// Coerce every column of a RecordBatch to the Arrow type the engine's strict
/// `emit_batch` IPC feed accepts for its declared output column.
///
/// `declared[i]` is the metadata Exasol reports for output column `i` of this
/// call's `EMITS (...)` clause, positionally aligned with the batch columns.
/// A column already of its target type is kept as-is (shared `Arc`, zero copy),
/// and a batch whose every column already matches is returned untouched.
pub fn coerce_batch_to_exa_types(
    batch: RecordBatch,
    declared: &[ColumnInfo],
) -> Result<RecordBatch, UdfError> {
    let schema = batch.schema();
    check_declared_arity(declared, schema.fields().len())?;

    // Decide the target Arrow type for each column up front, so a drifted
    // declaration is reported before any column is rebuilt.
    let targets: Vec<DataType> = declared
        .iter()
        .map(target_arrow_type)
        .collect::<Result<_, _>>()?;

    // Fast path: every column already matches its target — no allocation.
    if schema
        .fields()
        .iter()
        .zip(&targets)
        .all(|(f, t)| f.data_type() == t)
    {
        return Ok(batch);
    }

    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (((field, col), column), target) in schema
        .fields()
        .iter()
        .zip(batch.columns())
        .zip(declared)
        .zip(&targets)
    {
        let coerced = coerce_column(col, column, target)?;
        new_fields.push(Field::new(
            field.name(),
            coerced.data_type().clone(),
            field.is_nullable(),
        ));
        new_columns.push(coerced);
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| {
        UdfError::User(format!(
            "emit failed: the coerced batch is not well formed: {e}"
        ))
    })
}

/// Cast one output column to the Arrow type its declared output column requires.
///
/// `target` is the type [`target_arrow_type`] resolved for `declared`; it is
/// passed in so the decision is made once per column by the caller that already
/// needs it for its own fast-path check. A column already at that type is
/// returned as the same shared `Arc`, so the common case costs no copy.
///
/// The cast is deliberately strict (`safe: false`): this boundary is the last
/// thing between a produced value and the wire, so the lenient cast's NULL for
/// an unrepresentable value would leave the shard emitting an absent value where
/// it has a real one — which Exasol's outer wrapper merges as "this shard
/// contributed nothing" rather than surfacing as the error it is.
pub(crate) fn coerce_column(
    column: &ArrayRef,
    declared: &ColumnInfo,
    target: &DataType,
) -> Result<ArrayRef, UdfError> {
    if column.data_type() == target {
        return Ok(column.clone());
    }
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    cast_with_options(column.as_ref(), target, &options).map_err(|e| {
        UdfError::User(format!(
            "emit failed: output column {} could not be coerced from {:?} to {target:?}: {e}",
            declared.name,
            column.data_type()
        ))
    })
}

/// The Arrow type the engine's strict `emit_batch` IPC feed accepts for a column
/// declared with this `ExaType`.
///
/// The reported variant IS the bin Exasol chose for the declaration, so nothing
/// here re-derives that choice from a type string: `Int32`, `Int64` and
/// `Numeric` arrive already distinguished. Every remaining variant feeds `Utf8`,
/// which subsumes the `Utf8View`/`BinaryView` normalization and preserves what
/// an unrecognized declaration used to get.
pub(crate) fn target_arrow_type(declared: &ColumnInfo) -> Result<DataType, UdfError> {
    Ok(match &declared.typ {
        ExaType::Boolean => DataType::Boolean,
        ExaType::Double => DataType::Float64,
        ExaType::Int32 => DataType::Int32,
        ExaType::Int64 => DataType::Int64,
        ExaType::Numeric { precision, scale } => decimal_target(declared, *precision, *scale)?,
        ExaType::Date => DataType::Date32,
        ExaType::Timestamp { precision } => DataType::Timestamp(
            TimestampPrecision::from_declared_digits(*precision).arrow_unit(),
            None,
        ),
        ExaType::String { .. } | ExaType::Char { .. } | ExaType::Unsupported => DataType::Utf8,
    })
}

/// The `Decimal128` a NUMERIC-binned declaration maps to. A pair outside
/// `Decimal128`'s range is drift: fail rather than put text into a numeric column.
fn decimal_target(declared: &ColumnInfo, precision: u32, scale: u32) -> Result<DataType, UdfError> {
    let representable = u8::try_from(precision)
        .ok()
        .zip(i8::try_from(scale).ok())
        .filter(|(p, s)| {
            *p >= 1
                && *p <= DECIMAL128_MAX_PRECISION
                && *s <= DECIMAL128_MAX_SCALE
                && *s <= *p as i8
        });
    match representable {
        Some((p, s)) => Ok(DataType::Decimal128(p, s)),
        None => Err(UdfError::User(format!(
            "emit failed: output column {} is declared NUMERIC with precision {precision} \
             and scale {scale}, which is not a decimal the emit boundary can represent",
            declared.name
        ))),
    }
}

/// Classify a DataFusion scan error and produce a UdfError without credential leaks.
///
/// A checked-division failure is recognised first, BY TYPE through
/// [`find_checked_float_div_error`], because a user's own division by zero is
/// not a storage failure and `scan failed: assigned data could not be read`
/// would send a support case looking at object storage. Recognition never
/// matches message text: that would be a silent coupling breaking on any
/// wording change.
///
/// Otherwise calls `find_root()` on the error chain to detect `ResourcesExhausted`
/// through any nesting of `Context`, `External`, or `ArrowError` wrappers — which
/// DataFusion 54 uses internally (e.g. sort wraps OOM errors with `.context()`).
///
/// - checked-division failure → the arithmetic error itself, unframed
/// - `ResourcesExhausted` → clean memory-exhaustion error (distinct from storage errors)
/// - Everything else → storage-read error via `redact_storage_error`
///
/// Credential redaction is applied in every path.
pub fn classify_scan_error(e: DataFusionError, secrets: &[&str]) -> UdfError {
    if let Some(division) = find_checked_float_div_error(&e) {
        return checked_division_error(division, secrets);
    }
    match e.find_root() {
        DataFusionError::ResourcesExhausted(msg) => resources_exhausted_error(msg, secrets),
        _ => redact_storage_error(e.to_string(), secrets),
    }
}

/// Surface a checked-division failure as the arithmetic error it is.
///
/// Only the division's own message is surfaced. Every wrapping layer's text is
/// dropped, exactly as [`resources_exhausted_error`] drops it and for the same
/// reason: a `.context()` string can carry a credential-bearing fragment from an
/// outer error layer. Redaction still runs over what remains, so the
/// no-credential guarantee is a property of this classifier rather than of what
/// each error variant happens to interpolate.
fn checked_division_error(division: &CheckedFloatDivError, secrets: &[&str]) -> UdfError {
    let safe = redact_credentials(&redact_secret_values(&division.to_string(), secrets));
    UdfError::User(safe)
}

/// Introduces the memory exhaustion a scan surfaced alongside its checked
/// division, so a reader sees two failures rather than one run-on message.
const CONCURRENT_FAILURE_PREAMBLE: &str = "the scan also surfaced:";

/// Labels the memory-exhaustion classification, for both the function that
/// writes it and [`reframe_checked_division`], which reads it back to tell a
/// structurally-recognised `ResourcesExhausted` from every other
/// classification.
///
/// This is not the message-text coupling this module refuses elsewhere. That
/// rule is about DataFusion's wording, which changes under us with no compile
/// error. This wording is our own, and one constant owns it for the writer and
/// the reader alike, so rewording it cannot make them disagree.
const MEMORY_EXHAUSTED_LABEL: &str = "scan failed: memory exhausted (ResourcesExhausted):";

/// Report the checked-division failure `session` recorded as the failure the
/// scan surfaced.
///
/// [`classify_scan_error`] already recognises the division wherever its type
/// survives to it. The type does not survive a predicate pushed into the Parquet
/// row filter, which DataFusion flattens into a message string, and that is
/// issue #370's own route. The session keeps the typed value for exactly that
/// case, so this runs once per scan, at the one dispatcher all three run paths
/// funnel through.
///
/// A recorded division REPLACES the surfaced failure, except a memory
/// exhaustion, which it leads instead. Memory exhaustion is the only surfaced
/// failure separable from the flattened division without matching DataFusion's
/// wording: [`classify_scan_error`] recognises `ResourcesExhausted` on the typed
/// root, before any text exists, and labels it with [`MEMORY_EXHAUSTED_LABEL`].
/// Every other classification lands in the generic storage-read branch, which is
/// exactly where the flattened division's own text lands, so appending it would
/// republish `scan failed: assigned data could not be read` on 100% of issue
/// #370's route, under the one framing this change exists to remove.
///
/// Accepted limitation, chosen rather than overlooked: an unrelated storage
/// failure raised in another partition of a scan that also divided by zero is
/// MASKED. DataFusion evaluates partitions concurrently and surfaces exactly one
/// of their errors, so the collision is real; it is also rarer than issue #370's
/// own route and has never been observed live, whereas the storage framing on a
/// user's own division is measured and is what
/// `e2e_float_div_by_zero_in_filter_fails_like_native_exasol` fails on. Memory
/// exhaustion, the collision the mission's bounded-execution guarantee rests on,
/// is the one kept visible.
///
/// The composed text is redacted here rather than trusted. `raw_scan` and
/// `partial_agg` classify with the fact side's credentials alone, while this
/// dispatcher holds the union that also covers a join's dimension side.
pub fn reframe_checked_division(
    session: &SessionContext,
    error: UdfError,
    secrets: &[&str],
) -> UdfError {
    let Some(recorded) = session_checked_float_div_failure(session) else {
        return error;
    };
    let division = checked_division_error(&recorded, secrets);
    let surfaced = error.to_string();
    if !surfaced.contains(MEMORY_EXHAUSTED_LABEL) {
        return division;
    }
    let safe = redact_credentials(&redact_secret_values(&surfaced, secrets));
    UdfError::User(format!("{division}; {CONCURRENT_FAILURE_PREAMBLE} {safe}"))
}

/// Produce a clean memory-exhaustion UdfError, redacting any credential values.
///
/// Only the innermost `ResourcesExhausted` `msg` is surfaced here. Any wrapping
/// `.context()` message (e.g. from DataFusion sort's OOM path) is intentionally
/// dropped: context strings may carry credential-bearing fragments from outer
/// error layers, so exposing them defeats the redaction guarantee.
///
/// The message opens with [`MEMORY_EXHAUSTED_LABEL`], which is also how
/// [`reframe_checked_division`] recognises this classification.
fn resources_exhausted_error(msg: &str, secrets: &[&str]) -> UdfError {
    let safe = redact_credentials(&redact_secret_values(msg, secrets));
    UdfError::User(format!("{MEMORY_EXHAUSTED_LABEL} {safe}"))
}

/// Map a storage/scan error string to a UdfError that does not leak credentials.
///
/// First strips the literal credential values (`secrets`), then applies the
/// label-based heuristic. The value-based pass catches S3 XML / signature error
/// shapes that embed the raw key without a recognizable label.
pub fn redact_storage_error(msg: String, secrets: &[&str]) -> UdfError {
    // ponytail: regex-free redaction: strip known secret values, then any
    // credential-shaped query/auth params. The full error likely contains S3
    // auth headers.
    let safe = redact_credentials(&redact_secret_values(&msg, secrets));
    UdfError::User(format!(
        "scan failed: assigned data could not be read: {safe}"
    ))
}

pub use lakehouse_catalog::{redact_credentials, redact_secret_values};

#[cfg(test)]
#[path = "emit_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "declared_columns_test_support_tests.rs"]
pub(in crate::scan) mod declared_columns_test_support;
