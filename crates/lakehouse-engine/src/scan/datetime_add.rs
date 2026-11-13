//! The scan functions the DataFusion dialect renders `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and
//! `ADD_MINUTES` as. DataFusion's date arithmetic reproduces neither Exasol's count rounding, nor
//! its `ADD_DAYS`/`ADD_WEEKS` count limit, nor its failure outside 0001-01-01 to 9999-12-31.

use super::convert::{EXASOL_DATE_MAX_DAYS, EXASOL_DATE_MIN_DAYS};
use super::exasol_numeric::numeric_overflow;
use super::raised_failure::RaisedFailure;
use crate::types::mapping::predates_calendar_versions;
use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float64Array, TimestampMicrosecondBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Date32Type, Decimal128Type, Float64Type, TimeUnit,
    TimestampMicrosecondType,
};
use arrow::temporal_conversions::timestamp_us_to_datetime;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vs_expression::{ADD_DAYS_FN, ADD_HOURS_FN, ADD_MINUTES_FN, ADD_WEEKS_FN};

const TIMESTAMP_MICROS: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);
const MICROS_PER_MINUTE: i128 = 60_000_000;
const MICROS_PER_HOUR: i128 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i128 = 24 * MICROS_PER_HOUR;
/// Exasol's range as microseconds from 1970-01-01. A whole-day result lies inside it exactly when
/// its date lies inside Exasol's DATE range, so one check serves both result types.
const EXASOL_MIN_MICROS: i128 = EXASOL_DATE_MIN_DAYS as i128 * MICROS_PER_DAY;
const EXASOL_MAX_MICROS: i128 = (EXASOL_DATE_MAX_DAYS as i128 + 1) * MICROS_PER_DAY - 1;
/// Exasol refuses an `ADD_DAYS` or `ADD_WEEKS` count of a larger magnitude.
const DAY_AND_WEEK_COUNT_LIMIT: u32 = 999_999_999;
const NANOS_PER_MICRO: i128 = 1_000;
const MICROS_PER_MILLI: i128 = 1_000;
/// Exasol 8 holds the milliseconds of an `ADD_HOURS` or `ADD_MINUTES` count in 18 digits.
const EXASOL_8_MILLIS_LIMIT: u128 = 999_999_999_999_999_999;
const NUMERIC_OUT_OF_RANGE: &str = "data exception - numeric value out of range";
const DATETIME_OVERFLOW: &str = "data exception - datetime field overflow";
const DATETIME_UNDERFLOW: &str = "data exception - datetime field underflow";

/// The arithmetic an Exasol version adds an `ADD_HOURS` or `ADD_MINUTES` count in. A count
/// beyond it fails as a numeric overflow, not as a datetime overflow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CountArithmetic {
    /// Exasol 8 scales the rounded count to milliseconds.
    Milliseconds,
    /// Exasol 2025 and later scale the rounded count to 128-bit nanoseconds.
    #[default]
    Nanoseconds,
}

impl CountArithmetic {
    pub fn from_database_version(version: &str) -> Self {
        if predates_calendar_versions(version) {
            Self::Milliseconds
        } else {
            Self::Nanoseconds
        }
    }

    /// The largest whole count of `micros_per_count` microseconds the arithmetic holds.
    fn count_limit(self, micros_per_count: i128) -> u128 {
        match self {
            Self::Milliseconds => {
                EXASOL_8_MILLIS_LIMIT / (micros_per_count / MICROS_PER_MILLI).unsigned_abs()
            }
            Self::Nanoseconds => {
                i128::MAX.unsigned_abs() / (micros_per_count * NANOS_PER_MICRO).unsigned_abs()
            }
        }
    }

    fn unit_name(self) -> &'static str {
        match self {
            Self::Milliseconds => "millisecond",
            Self::Nanoseconds => "nanosecond",
        }
    }
}

/// The unit one scan function adds its count in, with the Exasol rules that differ by unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Unit {
    Days,
    Weeks,
    Hours,
    Minutes,
}

impl Unit {
    const ALL: [Self; 4] = [Self::Days, Self::Weeks, Self::Hours, Self::Minutes];

    fn scan_name(self) -> &'static str {
        match self {
            Self::Days => ADD_DAYS_FN,
            Self::Weeks => ADD_WEEKS_FN,
            Self::Hours => ADD_HOURS_FN,
            Self::Minutes => ADD_MINUTES_FN,
        }
    }

    fn exasol_name(self) -> &'static str {
        match self {
            Self::Days => "ADD_DAYS",
            Self::Weeks => "ADD_WEEKS",
            Self::Hours => "ADD_HOURS",
            Self::Minutes => "ADD_MINUTES",
        }
    }

    fn micros_per_count(self) -> i128 {
        match self {
            Self::Days => MICROS_PER_DAY,
            Self::Weeks => 7 * MICROS_PER_DAY,
            Self::Hours => MICROS_PER_HOUR,
            Self::Minutes => MICROS_PER_MINUTE,
        }
    }

    /// Exasol returns a DATE over a DATE from `ADD_DAYS` and `ADD_WEEKS`, and `TIMESTAMP(0)`
    /// from `ADD_HOURS` and `ADD_MINUTES`.
    fn keeps_a_date(self) -> bool {
        matches!(self, Self::Days | Self::Weeks)
    }

    fn count_limit(self) -> Option<u32> {
        self.keeps_a_date().then_some(DAY_AND_WEEK_COUNT_LIMIT)
    }

    /// The largest whole hour or minute count that `arithmetic` holds.
    fn arithmetic_count_limit(self, arithmetic: CountArithmetic) -> Option<u128> {
        (!self.keeps_a_date()).then(|| arithmetic.count_limit(self.micros_per_count()))
    }

    fn result_type(self, arg_types: &[DataType]) -> Result<DataType> {
        let [argument, count] = arg_types else {
            return plan_err!(
                "{} takes two arguments, got {}",
                self.scan_name(),
                arg_types.len()
            );
        };
        if !is_count_type(count) {
            return plan_err!("{} takes a numeric count, got {count}", self.scan_name());
        }
        match argument {
            DataType::Date32 if self.keeps_a_date() => Ok(DataType::Date32),
            DataType::Date32 | DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Ok(TIMESTAMP_MICROS)
            }
            other => plan_err!(
                "{} takes a Date32 or {TIMESTAMP_MICROS} first argument, got {other}",
                self.scan_name()
            ),
        }
    }

    /// The whole count Exasol adds: an exact count rounded half away from zero, a DOUBLE count
    /// `x` as `sign(x) * floor(|x| + 0.5)`. Exasol checks the count limit of a DOUBLE before it
    /// rounds and that of an exact count after.
    fn whole_count(
        self,
        count: Count,
        arithmetic: CountArithmetic,
    ) -> std::result::Result<i128, String> {
        let name = self.exasol_name();
        let rounded = match count {
            Count::Double(x) if !x.is_finite() => {
                return Err(numeric_overflow(format_args!(
                    "{name} count {x} is not finite, and Exasol's DOUBLE admits no non-finite value"
                )));
            }
            Count::Double(x) => {
                if self
                    .count_limit()
                    .is_some_and(|limit| x.abs() > f64::from(limit))
                {
                    return Err(self.count_out_of_range(x));
                }
                // Saturates beyond i128, which every later check treats as out of range.
                (x.signum() * (x.abs() + 0.5).floor()) as i128
            }
            Count::Exact { unscaled, scale } => {
                let rounded = round_half_away_from_zero(unscaled, scale);
                if self
                    .count_limit()
                    .is_some_and(|limit| rounded.unsigned_abs() > u128::from(limit))
                {
                    return Err(self.count_out_of_range(rounded));
                }
                rounded
            }
        };
        if self
            .arithmetic_count_limit(arithmetic)
            .is_some_and(|limit| rounded.unsigned_abs() > limit)
        {
            return Err(format!(
                "{NUMERIC_OUT_OF_RANGE}; {} count {rounded} overflows Exasol's {} arithmetic",
                self.exasol_name(),
                arithmetic.unit_name()
            ));
        }
        Ok(rounded)
    }

    fn count_out_of_range(self, count: impl std::fmt::Display) -> String {
        numeric_overflow(format_args!(
            "{} count {count} lies outside -{DAY_AND_WEEK_COUNT_LIMIT} to \
             {DAY_AND_WEEK_COUNT_LIMIT}",
            self.exasol_name()
        ))
    }

    /// `micros` plus `count` units, as microseconds from 1970-01-01 inside Exasol's range.
    fn add(self, micros: i128, count: i128) -> std::result::Result<i64, String> {
        let shifted = count
            .checked_mul(self.micros_per_count())
            .and_then(|offset| micros.checked_add(offset));
        let after_the_range = match shifted {
            Some(result @ EXASOL_MIN_MICROS..=EXASOL_MAX_MICROS) => return Ok(result as i64),
            Some(result) => result > EXASOL_MAX_MICROS,
            None => count > 0,
        };
        let argument = i64::try_from(micros)
            .ok()
            .and_then(timestamp_us_to_datetime)
            .map_or_else(
                || format!("{micros} microseconds from 1970-01-01"),
                |at| at.to_string(),
            );
        let call = format!("{}({argument}, {count})", self.exasol_name());
        Err(if after_the_range {
            format!("{DATETIME_OVERFLOW}; {call} lies after 9999-12-31")
        } else {
            format!("{DATETIME_UNDERFLOW}; {call} lies before 0001-01-01")
        })
    }
}

fn is_count_type(count: &DataType) -> bool {
    count.is_integer()
        || matches!(
            count,
            DataType::Decimal128(..) | DataType::Float32 | DataType::Float64 | DataType::Null
        )
}

/// `scale` is at most 38 for a `Decimal128`, so `10^scale` fits; a larger one could only round
/// to 0, because `|unscaled| < 10^39 / 2`.
fn round_half_away_from_zero(unscaled: i128, scale: i8) -> i128 {
    let exponent = u32::from(scale.unsigned_abs());
    if scale < 0 {
        return 10_i128
            .checked_pow(exponent)
            .and_then(|factor| unscaled.checked_mul(factor))
            .unwrap_or(unscaled.signum() * i128::MAX);
    }
    let Some(divisor) = 10_i128.checked_pow(exponent) else {
        return 0;
    };
    let (quotient, remainder) = (unscaled / divisor, unscaled % divisor);
    if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
        quotient + unscaled.signum()
    } else {
        quotient
    }
}

/// One count, in the exact or the approximate form that Exasol rounds differently.
#[derive(Clone, Copy)]
enum Count {
    Exact { unscaled: i128, scale: i8 },
    Double(f64),
}

/// A count column: integers and NULL widen to an exact count, `Float32` to a DOUBLE.
enum Counts {
    Exact { values: Decimal128Array, scale: i8 },
    Double(Float64Array),
}

impl Counts {
    fn of(counts: &ArrayRef) -> Result<Self> {
        Ok(match counts.data_type() {
            DataType::Decimal128(_, scale) => Self::Exact {
                values: counts.as_primitive::<Decimal128Type>().clone(),
                scale: *scale,
            },
            DataType::Float32 | DataType::Float64 => Self::Double(
                cast(counts, &DataType::Float64)?
                    .as_primitive::<Float64Type>()
                    .clone(),
            ),
            _ => Self::Exact {
                values: cast(counts, &DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0))?
                    .as_primitive::<Decimal128Type>()
                    .clone(),
                scale: 0,
            },
        })
    }

    fn at(&self, row: usize) -> Option<Count> {
        match self {
            Self::Exact { values, scale } => values.is_valid(row).then(|| Count::Exact {
                unscaled: values.value(row),
                scale: *scale,
            }),
            Self::Double(values) => values
                .is_valid(row)
                .then(|| Count::Double(values.value(row))),
        }
    }
}

/// `Immutable` keeps it eligible for the Parquet row filter, which flattens its error to text, so
/// each failure is also recorded on the session.
#[derive(Debug)]
struct DatetimeAddUdf {
    unit: Unit,
    arithmetic: CountArithmetic,
    signature: Signature,
    raised: Arc<RaisedFailure>,
}

impl DatetimeAddUdf {
    fn new(unit: Unit, arithmetic: CountArithmetic, raised: Arc<RaisedFailure>) -> Self {
        Self {
            unit,
            arithmetic,
            signature: Signature::any(2, Volatility::Immutable),
            raised,
        }
    }

    /// NULL in either argument returns NULL before the count is checked.
    fn shift(
        &self,
        arguments: impl Iterator<Item = Option<i128>>,
        counts: &Counts,
    ) -> std::result::Result<ArrayRef, String> {
        let mut shifted = TimestampMicrosecondBuilder::with_capacity(arguments.size_hint().0);
        for (row, argument) in arguments.enumerate() {
            match argument.zip(counts.at(row)) {
                Some((micros, count)) => {
                    let count = self.unit.whole_count(count, self.arithmetic)?;
                    shifted.append_value(self.unit.add(micros, count)?);
                }
                None => shifted.append_null(),
            }
        }
        Ok(Arc::new(shifted.finish()))
    }
}

/// The recorded failure is session state, not identity, so it is excluded from plan comparison.
impl PartialEq for DatetimeAddUdf {
    fn eq(&self, other: &Self) -> bool {
        self.unit == other.unit
            && self.arithmetic == other.arithmetic
            && self.signature == other.signature
    }
}

impl Eq for DatetimeAddUdf {}

impl Hash for DatetimeAddUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.unit.hash(state);
        self.arithmetic.hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for DatetimeAddUdf {
    fn name(&self) -> &str {
        self.unit.scan_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.unit.result_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument, count] = args.args.as_slice() else {
            return exec_err!(
                "{} takes two arguments, got {}",
                self.unit.scan_name(),
                args.args.len()
            );
        };
        let rows = args.number_rows;
        let (argument, counts) = (
            argument.to_array(rows)?,
            Counts::of(&count.to_array(rows)?)?,
        );
        // Widened before scaling: a Date32 far outside Exasol's range overflows i64 microseconds.
        let shifted = match argument.as_primitive_opt::<Date32Type>() {
            Some(dates) => self.shift(
                dates
                    .iter()
                    .map(|days| days.map(|days| i128::from(days) * MICROS_PER_DAY)),
                &counts,
            ),
            None => self.shift(
                cast(&argument, &TIMESTAMP_MICROS)?
                    .as_primitive::<TimestampMicrosecondType>()
                    .iter()
                    .map(|micros| micros.map(i128::from)),
                &counts,
            ),
        }
        .map_err(|failure| {
            self.raised.record(failure.clone());
            DataFusionError::Execution(failure)
        })?;
        Ok(ColumnarValue::Array(cast(
            &shifted,
            args.return_field.data_type(),
        )?))
    }
}

/// Registered unconditionally once per session, as every scan path splices the same rendered
/// expressions.
pub(super) fn register_datetime_add_udfs(
    ctx: &SessionContext,
    arithmetic: CountArithmetic,
    raised: Arc<RaisedFailure>,
) {
    for unit in Unit::ALL {
        ctx.register_udf(ScalarUDF::from(DatetimeAddUdf::new(
            unit,
            arithmetic,
            raised.clone(),
        )));
    }
}

#[cfg(test)]
#[path = "datetime_add_tests.rs"]
mod tests;
