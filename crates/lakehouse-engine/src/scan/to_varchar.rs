//! The scan session function the DataFusion dialect wraps around every value Exasol converts to
//! text (#227). The conversion lives here, not in the adapter, because only the scan session
//! knows a computed argument's Arrow type: DataFusion types `c_acctbal * 1.5` as `Float64` where
//! Exasol types it DECIMAL, which no adapter-side column type can see.

mod double_text;

use crate::scan::render_nested_column_as_json;
use crate::types::mapping::{needs_json_fallback, needs_nested_json_rendering};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, StringArray, StringBuilder, new_null_array,
};
use arrow::compute::{cast, cast_with_options};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, TimeUnit};
use chrono::DateTime;
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::{exec_datafusion_err, exec_err, internal_err};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use double_text::ExasolDoubleText;
use std::fmt::{Display, Write};
use std::sync::Arc;
use vs_expression::EXA_TO_VARCHAR_FN;

/// `Signature::any` because Exasol converts a value of every type, and `Immutable` keeps a
/// wrapped predicate eligible for the Parquet row filter.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExaToVarcharUdf {
    signature: Signature,
}

impl ExaToVarcharUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ExaToVarcharUdf {
    fn name(&self) -> &str {
        EXA_TO_VARCHAR_FN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(text_type_of(only_argument(arg_types)?))
    }

    /// A string argument keeps its own field because [`Self::simplify`] replaces the call with
    /// it, and DataFusion requires a simplified expression to keep type and nullability.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let argument = only_argument(args.arg_fields)?;
        let nullable = !is_text(argument.data_type()) || argument.is_nullable();
        Ok(Arc::new(Field::new(
            self.name(),
            text_type_of(argument.data_type()),
            nullable,
        )))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let [argument] = args.as_slice()
            && is_text(&info.get_data_type(argument)?)
        {
            return Ok(ExprSimplifyResult::Simplified(argument.clone()));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let argument = only_argument(&args.args)?;
        let values = argument.to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(exasol_text(&values)?))
    }
}

/// Exasol's text for each value under the default session settings (#216), one arm per Arrow
/// type the scan can produce.
fn exasol_text(values: &ArrayRef) -> Result<ArrayRef> {
    let data_type = values.data_type();
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(Arc::clone(values)),
        DataType::Null => Ok(new_null_array(&DataType::Utf8, values.len())),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => cast_like_sql(values),
        DataType::Decimal128(..) if !needs_json_fallback(data_type) => trimmed_decimal_text(values),
        DataType::Float32 | DataType::Float64 => double_text(values),
        DataType::Boolean => Ok(boolean_text(values.as_boolean())),
        DataType::Date32 => cast_like_sql(values),
        DataType::Timestamp(unit, _) => timestamp_text(values, *unit),
        _ if needs_json_fallback(data_type) => json_fallback_text(values),
        other => internal_err!(
            "{EXA_TO_VARCHAR_FN} has no text rule for {other}, a type the scan emits as a typed \
             Exasol column rather than through its VARCHAR fallback"
        ),
    }
}

/// Exasol receives such a column as the VARCHAR the scan's projection emits
/// (`raw_scan::build_scan_sql`), so a string function sees that same text.
fn json_fallback_text(values: &ArrayRef) -> Result<ArrayRef> {
    if needs_nested_json_rendering(values.data_type()) {
        return Ok(Arc::new(render_nested_column_as_json(values)?));
    }
    cast_like_sql(values)
}

/// DataFusion's SQL `CAST` options, so a text equals what `CAST(x AS VARCHAR)` yields.
fn cast_like_sql(values: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast_with_options(
        values,
        &DataType::Utf8,
        &DEFAULT_CAST_OPTIONS,
    )?)
}

/// Exasol drops a DECIMAL's trailing scale zeros and a bare decimal point (`-0.50` → `-0.5`),
/// while Arrow prints every scale digit.
fn trimmed_decimal_text(values: &ArrayRef) -> Result<ArrayRef> {
    let untrimmed = cast_like_sql(values)?;
    let trimmed: StringArray = untrimmed
        .as_string::<i32>()
        .iter()
        .map(|text| text.map(without_scale_zeros))
        .collect();
    Ok(Arc::new(trimmed))
}

fn without_scale_zeros(text: &str) -> &str {
    if !text.contains('.') {
        return text;
    }
    let trimmed = text.trim_end_matches('0');
    trimmed.strip_suffix('.').unwrap_or(trimmed)
}

/// A `Float32` widens exactly, as the scan emits it. A NaN yields NULL because the raw scan emits
/// a stored NaN as NULL (#246); an infinite value has no Exasol DOUBLE to convert from.
fn double_text(values: &ArrayRef) -> Result<ArrayRef> {
    let widened = cast(values, &DataType::Float64)?;
    let mut text = StringBuilder::with_capacity(widened.len(), widened.len() * DOUBLE_TEXT_LENGTH);
    for value in widened.as_primitive::<Float64Type>() {
        let Some(value) = value.filter(|value| !value.is_nan()) else {
            text.append_null();
            continue;
        };
        let exasol_text = ExasolDoubleText::of(value).ok_or_else(|| {
            exec_datafusion_err!(
                "data exception - numeric value out of range: {EXA_TO_VARCHAR_FN}({value}) has \
                 no text, because Exasol's DOUBLE admits no infinite value"
            )
        })?;
        write_value(&mut text, exasol_text)?;
    }
    Ok(Arc::new(text.finish()))
}

const DOUBLE_TEXT_LENGTH: usize = "-1.23456789012346e-308".len();

fn write_value(text: &mut StringBuilder, value: impl Display) -> Result<()> {
    write!(text, "{value}")
        .map_err(|e| exec_datafusion_err!("{EXA_TO_VARCHAR_FN} could not write text: {e}"))?;
    text.append_value("");
    Ok(())
}

fn boolean_text(values: &BooleanArray) -> ArrayRef {
    let text: StringArray = values
        .iter()
        .map(|value| value.map(|flag| if flag { "TRUE" } else { "FALSE" }))
        .collect();
    Arc::new(text)
}

/// Exasol's `YYYY-MM-DD HH24:MI:SS.FF6`: a finer fraction is truncated, and the time zone is
/// ignored because the scan emits the stored instant as wall-clock time (`scan/convert.rs`).
fn timestamp_text(values: &ArrayRef, unit: TimeUnit) -> Result<ArrayRef> {
    let raw = cast(values, &DataType::Int64)?;
    let mut text = StringBuilder::with_capacity(raw.len(), raw.len() * TIMESTAMP_TEXT_LENGTH);
    for value in raw.as_primitive::<Int64Type>() {
        let Some(value) = value else {
            text.append_null();
            continue;
        };
        let wall_clock = microseconds(value, unit)
            .and_then(DateTime::from_timestamp_micros)
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "{EXA_TO_VARCHAR_FN} cannot convert the timestamp {value} ({unit:?} since \
                     the epoch) to text: it lies outside the representable calendar range"
                )
            })?;
        write_value(&mut text, wall_clock.format("%Y-%m-%d %H:%M:%S%.6f"))?;
    }
    Ok(Arc::new(text.finish()))
}

const TIMESTAMP_TEXT_LENGTH: usize = "YYYY-MM-DD HH:MI:SS.FFFFFF".len();

/// Flooring division keeps a pre-epoch instant's truncated fraction on the earlier second.
fn microseconds(value: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value.div_euclid(1_000)),
    }
}

fn only_argument<T>(arguments: &[T]) -> Result<&T> {
    match arguments {
        [argument] => Ok(argument),
        _ => exec_err!(
            "{EXA_TO_VARCHAR_FN} takes exactly one argument, got {}",
            arguments.len()
        ),
    }
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn text_type_of(argument: &DataType) -> DataType {
    if is_text(argument) {
        argument.clone()
    } else {
        DataType::Utf8
    }
}

/// Registered unconditionally once per session, like the checked division: every scan path
/// splices the same rendered expressions, and a spec that never calls it is unaffected.
pub(super) fn register_exa_to_varchar_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ExaToVarcharUdf::new()));
}

#[cfg(test)]
#[path = "to_varchar_tests.rs"]
mod tests;
