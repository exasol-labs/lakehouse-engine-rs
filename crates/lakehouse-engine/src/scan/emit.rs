use crate::scan::checked_div::find_checked_float_div_error;
use crate::scan::convert::check_date32_in_exasol_range;
use crate::scan::diagnostics::PhaseTimers;
use crate::scan::exasol_numeric::numeric_overflow;
use crate::scan::raised_failure::session_raised_failure;
use crate::types::mapping::TimestampPrecision;
use arrow::array::{ArrayRef, AsArray, StringArray};
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

/// Each batch is emitted then dropped before the next fetch, never collected. Every column is
/// coerced to the Arrow type its declared `ExaType` accepts: the physical Parquet type can
/// diverge from the VS-declared logical type, and `emit_batch` rejects ANY mismatch. `secrets`
/// are the literal credential values stripped from any surfaced error.
pub async fn emit_stream(
    ctx: &mut dyn UdfContext,
    mut stream: SendableRecordBatchStream,
    secrets: &[&str],
    timers: &mut PhaseTimers,
) -> Result<u64, UdfError> {
    let declared = declared_output_columns(ctx)?;
    let mut total: u64 = 0;
    timers.seal_startup();
    loop {
        timers.import_started();
        let next = stream.next().await;
        timers.import_ended();

        let Some(result) = next else { break };

        timers.emit_started();
        let emit_result = emit_one_batch(ctx, result, secrets, &declared);
        timers.emit_ended();
        total += emit_result?;
    }
    Ok(total)
}

/// Factored out so the emit phase timer brackets exactly coercion + `emit_batch`.
fn emit_one_batch(
    ctx: &mut dyn UdfContext,
    result: Result<RecordBatch, DataFusionError>,
    secrets: &[&str],
    declared: &[ColumnInfo],
) -> Result<u64, UdfError> {
    let batch = result.map_err(|e| classify_scan_error(e, secrets))?;
    let batch = coerce_batch_to_exa_types(batch, declared)?;
    let rows = batch.num_rows() as u64;
    ctx.emit_batch(&batch)?;
    drop(batch);
    Ok(rows)
}

/// The generated `EMITS (...)` clause is the only declaration of the output schema, so it is
/// read here rather than from a second copy in the scan spec. Cloned because the context's
/// borrow cannot outlive the `&mut` borrow `emit_batch` needs.
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

/// Shared by both emit paths so a mismatched declaration fails identically on each.
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

/// Errors name `declared[i].name` for `columns[i]`.
pub(crate) fn check_emitted_dates_in_range(
    columns: &[ArrayRef],
    declared: &[ColumnInfo],
) -> Result<(), UdfError> {
    columns
        .iter()
        .zip(declared)
        .try_for_each(|(column, declared)| check_date32_in_exasol_range(column, &declared.name))
}

/// `declared[i]` is positionally aligned with the batch columns. Matching columns are kept as
/// the same `Arc`, and a fully matching batch is returned untouched.
pub fn coerce_batch_to_exa_types(
    batch: RecordBatch,
    declared: &[ColumnInfo],
) -> Result<RecordBatch, UdfError> {
    let schema = batch.schema();
    check_declared_arity(declared, schema.fields().len())?;

    // Resolved up front so a drifted declaration is reported before any column is rebuilt.
    let targets: Vec<DataType> = declared
        .iter()
        .map(target_arrow_type)
        .collect::<Result<_, _>>()?;

    if schema
        .fields()
        .iter()
        .zip(&targets)
        .zip(declared)
        .all(|((f, t), d)| f.data_type() == t && !matches!(d.typ, ExaType::Char { .. }))
    {
        check_emitted_dates_in_range(batch.columns(), declared)?;
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

    check_emitted_dates_in_range(&new_columns, declared)?;
    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| {
        UdfError::User(format!(
            "emit failed: the coerced batch is not well formed: {e}"
        ))
    })
}

/// Deliberately strict (`safe: false`): a lenient cast's NULL for an unrepresentable value
/// would be merged by Exasol's outer wrapper as "this shard contributed nothing" instead of
/// surfacing as the error it is.
pub(crate) fn coerce_column(
    column: &ArrayRef,
    declared: &ColumnInfo,
    target: &DataType,
) -> Result<ArrayRef, UdfError> {
    let coerced = cast_to_target(column, declared, target)?;
    Ok(match declared.typ {
        ExaType::Char { size } => cut_char_blank_excess(coerced, size as usize),
        _ => coerced,
    })
}

/// Exasol's `CAST(x AS CHAR(n))` drops trailing blanks beyond `n`, but the emit rejects any
/// value longer than `n` (#301). Non-blank excess is kept, so it still fails at the emit.
fn cut_char_blank_excess(column: ArrayRef, size: usize) -> ArrayRef {
    let cut = |s: &str| -> Option<usize> {
        let (byte, _) = s.char_indices().nth(size)?;
        s[byte..].bytes().all(|b| b == b' ').then_some(byte)
    };
    let Some(strings) = column.as_string_opt::<i32>() else {
        return column;
    };
    if strings.iter().flatten().all(|s| cut(s).is_none()) {
        return column;
    }
    let trimmed: StringArray = strings
        .iter()
        .map(|v| v.map(|s| cut(s).map_or(s, |byte| &s[..byte])))
        .collect();
    Arc::new(trimmed)
}

fn cast_to_target(
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
        if let DataType::Decimal128(p, s) = target
            && column.data_type().is_decimal()
        {
            return UdfError::User(numeric_overflow(format!(
                "output column {} exceeds DECIMAL({p},{s}): {e}",
                declared.name
            )));
        }
        UdfError::User(format!(
            "emit failed: output column {} could not be coerced from {:?} to {target:?}: {e}",
            declared.name,
            column.data_type()
        ))
    })
}

/// The reported variant IS the bin Exasol chose, so nothing is re-derived from a type string.
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

/// A pair outside `Decimal128`'s range is drift: fail rather than put text into a numeric column.
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

/// A checked-division failure is recognised first, BY TYPE, so a user's division by zero is not
/// framed as a storage failure. `find_root()` sees `ResourcesExhausted` through any
/// `Context`/`External`/`ArrowError` nesting (DataFusion's sort wraps OOM with `.context()`).
/// Credential redaction applies on every path.
pub fn classify_scan_error(e: DataFusionError, secrets: &[&str]) -> UdfError {
    if let Some(division) = find_checked_float_div_error(&e) {
        return raised_failure_error(&division.to_string(), secrets);
    }
    match e.find_root() {
        DataFusionError::ResourcesExhausted(msg) => resources_exhausted_error(msg, secrets),
        _ => redact_storage_error(e.to_string(), secrets),
    }
}

/// Takes the raised message alone, never a wrapped error: a `.context()` string can carry a
/// credential-bearing fragment.
fn raised_failure_error(failure: &str, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_credentials(&redact_secret_values(failure, secrets)))
}

const CONCURRENT_FAILURE_PREAMBLE: &str = "the scan also surfaced:";

/// Our own text, unlike DataFusion's wording, so [`reframe_raised_failure`] can match on it.
const MEMORY_EXHAUSTED_LABEL: &str = "scan failed: memory exhausted (ResourcesExhausted):";

/// The Parquet row filter flattens a raised failure to text (#370), so the session's record
/// replaces it, keeping only a memory exhaustion after it. Redacts with a join's full secret set.
pub fn reframe_raised_failure(
    session: &SessionContext,
    error: UdfError,
    secrets: &[&str],
) -> UdfError {
    let Some(recorded) = session_raised_failure(session) else {
        return error;
    };
    let raised = raised_failure_error(&recorded, secrets);
    let surfaced = error.to_string();
    if !surfaced.contains(MEMORY_EXHAUSTED_LABEL) {
        return raised;
    }
    let safe = redact_credentials(&redact_secret_values(&surfaced, secrets));
    UdfError::User(format!("{raised}; {CONCURRENT_FAILURE_PREAMBLE} {safe}"))
}

/// Only the innermost `msg` is surfaced: a wrapping `.context()` may carry credential-bearing
/// fragments. Opens with [`MEMORY_EXHAUSTED_LABEL`] for [`reframe_raised_failure`].
fn resources_exhausted_error(msg: &str, secrets: &[&str]) -> UdfError {
    let safe = redact_credentials(&redact_secret_values(msg, secrets));
    UdfError::User(format!("{MEMORY_EXHAUSTED_LABEL} {safe}"))
}

/// Strips literal `secrets` first, catching S3 XML/signature shapes that embed the raw key
/// without a recognizable label, then applies the label-based heuristic.
pub fn redact_storage_error(msg: String, secrets: &[&str]) -> UdfError {
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
