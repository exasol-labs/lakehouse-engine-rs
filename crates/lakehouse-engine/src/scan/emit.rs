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
use crate::scan::diagnostics::PhaseTimers;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use exasol_udf_sdk::context::{EmitBatch, UdfContext};
use exasol_udf_sdk::error::UdfError;
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
/// `exa_types` is the declared Exasol EMITS type string for each output column
/// (positionally aligned). Every column is coerced to the Arrow type that ExaType
/// accepts before `emit_batch` — DataFusion's physical Parquet type can diverge
/// from the Iceberg logical type the VS declared, and `emit_batch` rejects ANY
/// mismatch. Pass `&[]` to fall back to view-type normalization only.
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
    exa_types: &[String],
    timers: &mut PhaseTimers,
) -> Result<u64, UdfError> {
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
        let emit_result = emit_one_batch(ctx, result, secrets, exa_types);
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
    exa_types: &[String],
) -> Result<u64, UdfError> {
    let batch = result.map_err(|e| classify_scan_error(e, secrets))?;
    // Coerce each column to the Arrow type its declared EMITS ExaType accepts,
    // so emit_batch's strict Arrow→ExaType validation never rejects a column.
    // Generalizes the old Utf8View→Utf8 normalization across the full mapping.
    let batch = coerce_batch_to_exa_types(batch, exa_types)
        .map_err(|e| UdfError::User(format!("emit type coercion failed: {e}")))?;
    // Count rows before emitting — batch is borrowed by emit_batch.
    let rows = batch.num_rows() as u64;
    ctx.emit_batch(&batch)?;
    drop(batch);
    Ok(rows)
}

/// Coerce every column of a RecordBatch to the Arrow type the engine's strict
/// `emit_batch` IPC feed accepts for its declared EMITS ExaType.
///
/// `exa_types[i]` is the Exasol EMITS type string the VS declared for output
/// column `i` (the SAME list the adapter put in the EMITS clause), positionally
/// aligned with the batch columns. For each column:
///
/// - A concrete target ([`exasol_type_to_arrow`] returns `Some`) → cast the
///   column to that Arrow type (Int32→Decimal128, Float32→Float64, narrow
///   Decimal→wide Decimal, etc.). DataFusion's Parquet scan can produce a
///   different physical Arrow type than the Iceberg logical type the VS declared
///   (e.g. an Iceberg `int` widened to `long`), and `emit_batch` rejects ANY such
///   mismatch — so the postcondition is that the fed Arrow type maps back to the
///   declared ExaType.
/// - The string family ([`exasol_type_to_arrow`] returns `None`, i.e. VARCHAR /
///   CHAR) → cast to `Utf8`. Incompatible source types were already pre-cast to a
///   string by the scan SQL (`CAST(col AS VARCHAR)`); this also subsumes the old
///   `Utf8View`/`BinaryView` normalization.
///
/// Fast path: a column already of the target type is kept as-is (shared `Arc`,
/// zero copy). When `exa_types` is empty or shorter than the column count (a spec
/// that predates `emit_exa_types`), unmatched columns fall back to view-type
/// normalization so a `Utf8View` column still does not crash `emit_batch`.
pub fn coerce_batch_to_exa_types(
    batch: RecordBatch,
    exa_types: &[String],
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();

    // Decide the target Arrow type for each column up front.
    let targets: Vec<DataType> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            target_arrow_type(exa_types.get(i).map(String::as_str), field.data_type())
        })
        .collect();

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
    let mut new_columns: Vec<Arc<dyn arrow::array::Array>> =
        Vec::with_capacity(batch.num_columns());

    for ((field, col), target) in schema.fields().iter().zip(batch.columns()).zip(&targets) {
        if field.data_type() == target {
            new_fields.push(field.as_ref().clone());
            new_columns.push(col.clone());
        } else {
            let cast_col = arrow::compute::cast(col.as_ref(), target)?;
            new_fields.push(arrow::datatypes::Field::new(
                field.name(),
                target.clone(),
                field.is_nullable(),
            ));
            new_columns.push(cast_col);
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
}

/// Resolve the target Arrow type for one output column.
///
/// `declared` is the Exasol EMITS type string for this column (None when the
/// spec carries no declared type for this position). `source` is the column's
/// current Arrow type, used only for the no-declared-type fallback.
fn target_arrow_type(declared: Option<&str>, source: &DataType) -> DataType {
    if let Some(exa) = declared {
        match crate::types::mapping::exasol_type_to_arrow(exa) {
            // Concrete numeric / temporal / boolean target.
            Some(t) => t,
            // String family (VARCHAR / CHAR): feed Utf8.
            None => DataType::Utf8,
        }
    } else {
        // No declared type for this column: preserve the source type, but still
        // normalize view types so emit_batch does not reject them.
        match source {
            DataType::Utf8View => DataType::Utf8,
            DataType::BinaryView => DataType::Binary,
            other => other.clone(),
        }
    }
}

/// Classify a DataFusion scan error and produce a UdfError without credential leaks.
///
/// Calls `find_root()` on the error chain to detect `ResourcesExhausted` through any
/// nesting of `Context`, `External`, or `ArrowError` wrappers — which DataFusion 54
/// uses internally (e.g. sort wraps OOM errors with `.context()`).
///
/// - `ResourcesExhausted` → clean memory-exhaustion error (distinct from storage errors)
/// - Everything else → storage-read error via `redact_storage_error`
///
/// Credential redaction is applied in both paths.
pub fn classify_scan_error(e: DataFusionError, secrets: &[&str]) -> UdfError {
    match e.find_root() {
        DataFusionError::ResourcesExhausted(msg) => resources_exhausted_error(msg, secrets),
        _ => redact_storage_error(e.to_string(), secrets),
    }
}

/// Produce a clean memory-exhaustion UdfError, redacting any credential values.
///
/// Only the innermost `ResourcesExhausted` `msg` is surfaced here. Any wrapping
/// `.context()` message (e.g. from DataFusion sort's OOM path) is intentionally
/// dropped: context strings may carry credential-bearing fragments from outer
/// error layers, so exposing them defeats the redaction guarantee.
fn resources_exhausted_error(msg: &str, secrets: &[&str]) -> UdfError {
    let safe = redact_credentials(&redact_secret_values(msg, secrets));
    UdfError::User(format!(
        "scan failed: memory exhausted (ResourcesExhausted): {safe}"
    ))
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
