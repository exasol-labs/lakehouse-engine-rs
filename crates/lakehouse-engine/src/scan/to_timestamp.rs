//! The scan function the DataFusion dialect renders `TO_TIMESTAMP(text, format)` as. DataFusion's
//! `to_timestamp` parses through nanoseconds, so it fails outside 1677 to 2262, and it fails on
//! text that ends before its format, where Exasol reads the missing time fields as 0.

use super::datetime_add::DATETIME_OVERFLOW;
use super::raised_failure::RaisedFailure;
use crate::types::mapping::arrow_time_unit;
use arrow::array::{ArrayRef, AsArray, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use chrono::format::{Item, ParseErrorKind, Parsed, StrftimeItems, parse};
use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vs_expression::TO_TIMESTAMP_FN;
use vs_expression::timestamp::TimestampUnit;

const NANOSECOND_DIGITS: u32 = TimestampUnit::Nanosecond.digits() as u32;
const INVALID_VALUE: &str = "data exception - invalid value";
const INVALID_DATE: &str = "data exception - invalid date value";
const INVALID_TIME: &str = "data exception - invalid time value";

/// Exasol's `TO_TIMESTAMP` of `text` over `items`, the parsed strftime form of its format model,
/// with the fraction cut to its first `digits` digits.
pub(crate) fn parse_timestamp(
    text: &str,
    items: &[Item<'_>],
    digits: u8,
) -> std::result::Result<NaiveDateTime, String> {
    let invalid = |reason: &dyn std::fmt::Display| {
        format!("{INVALID_VALUE}; TO_TIMESTAMP cannot read '{text}': {reason}")
    };
    let mut parsed = Parsed::new();
    match parse(&mut parsed, text, items.iter()) {
        Ok(()) => {}
        // Exasol reads each time field the text ends before as 0. A missing date field fails below.
        Err(e) if e.kind() == ParseErrorKind::TooShort => {}
        Err(e) => return Err(invalid(&e)),
    }
    if parsed.hour_div_12().is_none() {
        parsed.set_hour(0).map_err(|e| invalid(&e))?;
    }
    if parsed.minute().is_none() {
        parsed.set_minute(0).map_err(|e| invalid(&e))?;
    }
    let value = parsed
        .to_naive_datetime_with_offset(0)
        .map_err(|e| invalid(&e))?;
    // chrono reads `SS` = 60 as a leap second, which Exasol rejects.
    if value.nanosecond() >= 1_000_000_000 {
        return Err(format!(
            "{INVALID_TIME}; TO_TIMESTAMP of '{text}' names second 60"
        ));
    }
    if !(1..=9999).contains(&value.year()) {
        return Err(format!(
            "{INVALID_DATE}; TO_TIMESTAMP of '{text}' lies outside 0001-01-01 to 9999-12-31"
        ));
    }
    let step = 10_u32.pow(NANOSECOND_DIGITS - u32::from(digits.min(9)));
    let nanos = value.nanosecond();
    Ok(value
        .with_nanosecond(nanos - nanos % step)
        .expect("a cut fraction stays a valid nanosecond"))
}

fn unit_holding(digits: u8) -> TimeUnit {
    arrow_time_unit(TimestampUnit::holding(digits).max(TimestampUnit::Microsecond))
}

/// `value` as a count of `unit` from 1970-01-01.
fn timestamp_in(value: NaiveDateTime, unit: TimeUnit) -> std::result::Result<i64, String> {
    let value = value.and_utc();
    match unit {
        TimeUnit::Nanosecond => value.timestamp_nanos_opt().ok_or_else(|| {
            format!(
                "{DATETIME_OVERFLOW}; TO_TIMESTAMP of {value} lies outside 1677-09-21 to \
                 2262-04-11, the nanosecond timestamps the scan computes an FF9 format in"
            )
        }),
        _ => Ok(value.timestamp_micros()),
    }
}

/// `Immutable` keeps it eligible for the Parquet row filter, which flattens its error to text, so
/// each failure is also recorded on the session.
#[derive(Debug)]
struct ToTimestampUdf {
    signature: Signature,
    raised: Arc<RaisedFailure>,
}

/// The recorded failure is session state, not identity, so it is excluded from plan comparison.
impl PartialEq for ToTimestampUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for ToTimestampUdf {}

impl Hash for ToTimestampUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

/// The renderer passes the pattern and the digits as literals.
fn literal_digits(value: Option<&ScalarValue>) -> Result<u8> {
    match value.map(|value| value.cast_to(&DataType::UInt8)) {
        Some(Ok(ScalarValue::UInt8(Some(digits)))) => Ok(digits),
        _ => plan_err!("{TO_TIMESTAMP_FN} takes its fractional digits as an integer literal"),
    }
}

fn literal_pattern(value: &ColumnarValue) -> Result<String> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(Some(pattern)) => Ok(pattern.to_string()),
            _ => exec_err!("{TO_TIMESTAMP_FN} takes its pattern as a string literal"),
        },
        ColumnarValue::Array(_) => {
            exec_err!("{TO_TIMESTAMP_FN} takes its pattern as a string literal")
        }
    }
}

impl ToTimestampUdf {
    fn convert(
        texts: &ArrayRef,
        pattern: &str,
        digits: u8,
    ) -> std::result::Result<ArrayRef, String> {
        let items = StrftimeItems::new(pattern)
            .parse()
            .map_err(|e| format!("{INVALID_VALUE}; pattern '{pattern}': {e}"))?;
        let unit = unit_holding(digits);
        let texts = cast(texts, &DataType::Utf8).map_err(|e| e.to_string())?;
        let values = texts
            .as_string::<i32>()
            .iter()
            .map(|text| {
                text.map(|text| timestamp_in(parse_timestamp(text, &items, digits)?, unit))
                    .transpose()
            })
            .collect::<std::result::Result<Vec<Option<i64>>, String>>()?;
        Ok(match unit {
            TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
            _ => Arc::new(TimestampMicrosecondArray::from(values)),
        })
    }
}

impl ScalarUDFImpl for ToTimestampUdf {
    fn name(&self) -> &str {
        TO_TIMESTAMP_FN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("{TO_TIMESTAMP_FN} types its result from its digits literal")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [_, _, digits] = args.scalar_arguments else {
            return plan_err!(
                "{TO_TIMESTAMP_FN} takes three arguments, got {}",
                args.scalar_arguments.len()
            );
        };
        let unit = unit_holding(literal_digits(*digits)?);
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Timestamp(unit, None),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [text, pattern, digits] = args.args.as_slice() else {
            return exec_err!(
                "{TO_TIMESTAMP_FN} takes three arguments, got {}",
                args.args.len()
            );
        };
        let digits = match digits {
            ColumnarValue::Scalar(digits) => literal_digits(Some(digits))?,
            ColumnarValue::Array(_) => {
                return exec_err!("{TO_TIMESTAMP_FN} takes its digits as an integer literal");
            }
        };
        let texts = text.to_array(args.number_rows)?;
        Self::convert(&texts, &literal_pattern(pattern)?, digits)
            .map(ColumnarValue::Array)
            .map_err(|failure| {
                self.raised.record(failure.clone());
                DataFusionError::Execution(failure)
            })
    }
}

/// Registered unconditionally once per session, as every scan path splices the same rendered
/// expressions.
pub(super) fn register_to_timestamp_udf(ctx: &SessionContext, raised: Arc<RaisedFailure>) {
    ctx.register_udf(ScalarUDF::from(ToTimestampUdf {
        signature: Signature::any(3, Volatility::Immutable),
        raised,
    }));
}

#[cfg(test)]
#[path = "to_timestamp_tests.rs"]
mod tests;
