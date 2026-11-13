//! The scan functions the DataFusion dialect renders `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and
//! `ADD_MINUTES` as. DataFusion's date arithmetic reproduces neither Exasol's count rounding, nor
//! its `ADD_DAYS`/`ADD_WEEKS` count limit, nor its failure outside 0001-01-01 to 9999-12-31.

use super::convert::{EXASOL_DATE_MAX_DAYS, EXASOL_DATE_MIN_DAYS};
use super::exasol_numeric::numeric_overflow;
use super::raised_failure::RaisedFailure;
use crate::types::mapping::predates_calendar_versions;
use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float64Array, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Date32Type, Decimal128Type, Float64Type, TimeUnit,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::temporal_conversions::{timestamp_ns_to_datetime, timestamp_us_to_datetime};
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
const TIMESTAMP_NANOS: DataType = DataType::Timestamp(TimeUnit::Nanosecond, None);
const NANOS_PER_MICRO: i128 = 1_000;
const NANOS_PER_MILLI: i128 = 1_000_000;
const NANOS_PER_MINUTE: i128 = 60_000_000_000;
const NANOS_PER_HOUR: i128 = 60 * NANOS_PER_MINUTE;
const NANOS_PER_DAY: i128 = 24 * NANOS_PER_HOUR;
/// Exasol's range as nanoseconds from 1970-01-01. A whole-day result lies inside it exactly when
/// its date lies inside Exasol's DATE range, so one check serves every result type.
const EXASOL_MIN_NANOS: i128 = EXASOL_DATE_MIN_DAYS as i128 * NANOS_PER_DAY;
const EXASOL_MAX_NANOS: i128 = (EXASOL_DATE_MAX_DAYS as i128 + 1) * NANOS_PER_DAY - 1;
/// Exasol refuses an `ADD_DAYS` or `ADD_WEEKS` count of a larger magnitude.
const DAY_AND_WEEK_COUNT_LIMIT: u32 = 999_999_999;
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

    /// The largest whole count of `nanos_per_count` nanoseconds the arithmetic holds.
    fn count_limit(self, nanos_per_count: i128) -> u128 {
        match self {
            Self::Milliseconds => {
                EXASOL_8_MILLIS_LIMIT / (nanos_per_count / NANOS_PER_MILLI).unsigned_abs()
            }
            Self::Nanoseconds => i128::MAX.unsigned_abs() / nanos_per_count.unsigned_abs(),
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

    fn nanos_per_count(self) -> i128 {
        match self {
            Self::Days => NANOS_PER_DAY,
            Self::Weeks => 7 * NANOS_PER_DAY,
            Self::Hours => NANOS_PER_HOUR,
            Self::Minutes => NANOS_PER_MINUTE,
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
        (!self.keeps_a_date()).then(|| arithmetic.count_limit(self.nanos_per_count()))
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
            DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok(TIMESTAMP_NANOS),
            other => plan_err!(
                "{} takes a Date32, {TIMESTAMP_MICROS}, or {TIMESTAMP_NANOS} first argument, \
                 got {other}",
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

    /// `nanos` plus `count` units, as nanoseconds from 1970-01-01 inside Exasol's range.
    fn add(self, nanos: i128, count: i128) -> std::result::Result<i128, String> {
        let shifted = count
            .checked_mul(self.nanos_per_count())
            .and_then(|offset| nanos.checked_add(offset));
        let after_the_range = match shifted {
            Some(result @ EXASOL_MIN_NANOS..=EXASOL_MAX_NANOS) => return Ok(result),
            Some(result) => result > EXASOL_MAX_NANOS,
            None => count > 0,
        };
        let call = self.call(nanos, count);
        Err(if after_the_range {
            format!("{DATETIME_OVERFLOW}; {call} lies after 9999-12-31")
        } else {
            format!("{DATETIME_UNDERFLOW}; {call} lies before 0001-01-01")
        })
    }

    /// A result over a `TIMESTAMP(7..=9)` as an Arrow nanosecond timestamp, which ends before
    /// Exasol's range does (`docs/semantic-differences.md`).
    fn nanosecond_result(
        self,
        nanos: i128,
        count: i128,
        result: i128,
    ) -> std::result::Result<i64, String> {
        i64::try_from(result).map_err(|_| {
            let (class, bound) = if result > 0 {
                (DATETIME_OVERFLOW, "after 2262-04-11 23:47:16.854775807")
            } else {
                (DATETIME_UNDERFLOW, "before 1677-09-21 00:12:43.145224192")
            };
            format!(
                "{class}; {} lies {bound}, outside the nanosecond timestamps the scan computes \
                 a TIMESTAMP(7) to TIMESTAMP(9) in",
                self.call(nanos, count)
            )
        })
    }

    fn call(self, nanos: i128, count: i128) -> String {
        let argument = i64::try_from(nanos)
            .ok()
            .and_then(timestamp_ns_to_datetime)
            .or_else(|| {
                i64::try_from(nanos.div_euclid(NANOS_PER_MICRO))
                    .ok()
                    .and_then(timestamp_us_to_datetime)
            })
            .map_or_else(
                || format!("{nanos} nanoseconds from 1970-01-01"),
                |at| at.to_string(),
            );
        format!("{}({argument}, {count})", self.exasol_name())
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

    /// `arguments` are nanoseconds from 1970-01-01. NULL in either argument returns NULL before
    /// the count is checked.
    fn shift(
        &self,
        arguments: impl Iterator<Item = Option<i128>>,
        counts: &Counts,
        result_unit: TimeUnit,
    ) -> std::result::Result<ArrayRef, String> {
        let mut shifted = Vec::with_capacity(arguments.size_hint().0);
        for (row, argument) in arguments.enumerate() {
            shifted.push(match argument.zip(counts.at(row)) {
                Some((nanos, count)) => {
                    let count = self.unit.whole_count(count, self.arithmetic)?;
                    let result = self.unit.add(nanos, count)?;
                    Some(match result_unit {
                        TimeUnit::Nanosecond => {
                            self.unit.nanosecond_result(nanos, count, result)?
                        }
                        // Exact: the argument holds whole microseconds, and a count whole minutes.
                        _ => (result / NANOS_PER_MICRO) as i64,
                    })
                }
                None => None,
            });
        }
        Ok(match result_unit {
            TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(shifted)),
            _ => Arc::new(TimestampMicrosecondArray::from(shifted)),
        })
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
        // Widened before scaling: a Date32 far outside Exasol's range overflows i64 nanoseconds.
        let shifted = if let Some(dates) = argument.as_primitive_opt::<Date32Type>() {
            self.shift(
                dates
                    .iter()
                    .map(|days| days.map(|days| i128::from(days) * NANOS_PER_DAY)),
                &counts,
                TimeUnit::Microsecond,
            )
        } else if let Some(nanos) = argument.as_primitive_opt::<TimestampNanosecondType>() {
            self.shift(
                nanos.iter().map(|nanos| nanos.map(i128::from)),
                &counts,
                TimeUnit::Nanosecond,
            )
        } else {
            self.shift(
                cast(&argument, &TIMESTAMP_MICROS)?
                    .as_primitive::<TimestampMicrosecondType>()
                    .iter()
                    .map(|micros| micros.map(|micros| i128::from(micros) * NANOS_PER_MICRO)),
                &counts,
                TimeUnit::Microsecond,
            )
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
