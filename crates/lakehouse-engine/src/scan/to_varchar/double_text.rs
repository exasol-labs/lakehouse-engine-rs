//! Exasol 2025.1's DOUBLE text (`DTM::longdouble_to_char`), replayed bit for bit (decision-log
//! [6]): x87 extended scaling by a power of ten, then `roundl` to 15 digits. Integer arithmetic
//! replays every step but the `logl` exponent, which [`EXPONENTS_EXASOL_ROUNDS_UP`] stands in for.

use std::fmt;

const SIGNIFICANT_DIGITS: u32 = 15;

/// Exasol writes a decimal exponent from -4 to 14 in fixed notation and any other in scientific.
const FIXED_EXPONENTS: std::ops::RangeInclusive<i32> = -4..=14;

/// Exasol clamps a mantissa whose rounding carried into a 16th digit instead of raising the
/// exponent, which is why `1e-20` prints `9.99999999999999e-21`.
const LARGEST_MANTISSA: u64 = 10u64.pow(SIGNIFICANT_DIGITS) - 1;

/// A finite DOUBLE's Exasol text under the default session settings, written by `Display`.
pub(super) struct ExasolDoubleText {
    negative: bool,
    digits: u64,
    notation: Notation,
}

enum Notation {
    Fixed { decimals: u32 },
    Scientific { exponent: i32 },
}

impl ExasolDoubleText {
    /// `None` for NaN and infinity, which Exasol's DOUBLE cannot hold.
    pub(super) fn of(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        let negative = value < 0.0;
        if value == 0.0 {
            return Some(Self {
                negative,
                digits: 0,
                notation: Notation::Fixed { decimals: 0 },
            });
        }
        let exponent = exasol_decimal_exponent(value.abs());
        let magnitude = Extended::from_f64(value.abs());
        let scale = SIGNIFICANT_DIGITS as i32 - 1 - exponent;
        if FIXED_EXPONENTS.contains(&exponent) {
            // Below one Exasol keeps 15 decimals, not 15 significant digits: `0.028980134693129`.
            let decimals = scale.min(SIGNIFICANT_DIGITS as i32);
            return Some(Self {
                negative,
                digits: scaled(magnitude, decimals).round_half_away(),
                notation: Notation::Fixed {
                    decimals: decimals as u32,
                },
            });
        }
        Some(Self {
            negative,
            digits: scaled(magnitude, scale)
                .round_half_away()
                .min(LARGEST_MANTISSA),
            notation: Notation::Scientific { exponent },
        })
    }
}

impl fmt::Display for ExasolDoubleText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative {
            f.write_str("-")?;
        }
        match self.notation {
            Notation::Fixed { decimals } => write_point_number(f, self.digits, decimals),
            Notation::Scientific { exponent } => {
                write_point_number(f, self.digits, SIGNIFICANT_DIGITS - 1)?;
                write!(f, "e{exponent}")
            }
        }
    }
}

/// Exasol drops trailing fraction zeros and a bare decimal point.
fn write_point_number(f: &mut fmt::Formatter<'_>, digits: u64, decimals: u32) -> fmt::Result {
    let unit = 10u64.pow(decimals);
    write!(f, "{}", digits / unit)?;
    let mut fraction = digits % unit;
    if fraction == 0 {
        return Ok(());
    }
    let mut width = decimals as usize;
    while fraction.is_multiple_of(10) {
        fraction /= 10;
        width -= 1;
    }
    write!(f, ".{fraction:0width$}")
}

/// Powers whose nearest double lies just below them, yet Exasol's x87 `floorl(logl(v) / ln 10)`
/// rounds up to them: `1e-14` prints `1e-14`, `1e-16` prints `9.99999999999999e-17`. Captured by
/// running every power of ten through Exasol 2025.1.16's own formatter on Intel x86-64.
const EXPONENTS_EXASOL_ROUNDS_UP: [i32; 54] = [
    -299, -283, -277, -274, -267, -266, -265, -261, -256, -243, -242, -231, -220, -176, -175, -174,
    -171, -170, -160, -159, -137, -132, -131, -116, -78, -73, -55, -14, 52, 65, 89, 98, 129, 145,
    147, 153, 156, 157, 179, 186, 203, 208, 213, 220, 224, 229, 233, 255, 267, 268, 271, 273, 283,
    295,
];

fn exasol_decimal_exponent(magnitude: f64) -> i32 {
    let exponent = decimal_exponent(magnitude);
    let power = exponent + 1;
    let is_largest_double_below_power =
        || Extended::from_f64(magnitude.next_up()) >= power_of_ten(power);
    if EXPONENTS_EXASOL_ROUNDS_UP.binary_search(&power).is_ok() && is_largest_double_below_power() {
        power
    } else {
        exponent
    }
}

/// `floor(log10(magnitude))`, exactly. The float estimate can be one off beside a power of ten;
/// the table comparison cannot, since no double lies strictly between `10^k` and its entry.
fn decimal_exponent(magnitude: f64) -> i32 {
    let estimate = magnitude.log10().floor() as i32;
    let value = Extended::from_f64(magnitude);
    if value < power_of_ten(estimate) {
        estimate - 1
    } else if value >= power_of_ten(estimate + 1) {
        estimate + 1
    } else {
        estimate
    }
}

/// An x87 80-bit extended value, `significand × 2^exponent` with the significand's top bit set.
/// Field order makes the derived ordering numeric for positive values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Extended {
    exponent: i32,
    significand: u64,
}

const DOUBLE_FRACTION_BITS: u32 = 52;

/// The exponent bias plus the fraction width: a double is `integer × 2^(biased - 1075)`.
const DOUBLE_INTEGER_EXPONENT_BIAS: i32 = 1075;

impl Extended {
    /// Exact, as the x87 load of a double is.
    fn from_f64(magnitude: f64) -> Self {
        let bits = magnitude.to_bits();
        let fraction = bits & ((1 << DOUBLE_FRACTION_BITS) - 1);
        let biased = (bits >> DOUBLE_FRACTION_BITS) as i32;
        let (integer, exponent) = if biased == 0 {
            (fraction, 1 - DOUBLE_INTEGER_EXPONENT_BIAS)
        } else {
            (
                fraction | 1 << DOUBLE_FRACTION_BITS,
                biased - DOUBLE_INTEGER_EXPONENT_BIAS,
            )
        };
        let shift = integer.leading_zeros();
        Self {
            exponent: exponent - shift as i32,
            significand: integer << shift,
        }
    }

    /// The x87 multiplication under the default round-to-nearest-even control word.
    fn times(self, other: Self) -> Self {
        let product = u128::from(self.significand) * u128::from(other.significand);
        let dropped = u64::BITS - product.leading_zeros();
        let kept = (product >> dropped) as u64;
        let remainder = product & ((1 << dropped) - 1);
        let half = 1 << (dropped - 1);
        let exponent = self.exponent + other.exponent + dropped as i32;
        if remainder > half || (remainder == half && kept & 1 == 1) {
            Self::rounded_up(kept, exponent)
        } else {
            Self {
                exponent,
                significand: kept,
            }
        }
    }

    const fn rounded_up(significand: u64, exponent: i32) -> Self {
        match significand.checked_add(1) {
            Some(significand) => Self {
                exponent,
                significand,
            },
            None => Self {
                exponent: exponent + 1,
                significand: 1 << 63,
            },
        }
    }

    /// `roundl`, half away from zero, for a value in `[1, 2^63)`.
    fn round_half_away(self) -> u64 {
        let fraction_bits = self.exponent.unsigned_abs();
        (self.significand >> fraction_bits) + ((self.significand >> (fraction_bits - 1)) & 1)
    }
}

/// Exasol's `power_ten_longdouble_base` ends at `10^±308`, so a larger scale first multiplies by
/// `10^308`, as `DTM::safescale` does.
const LARGEST_TABLE_POWER: i32 = 308;

const SMALLEST_POWER: i32 = -324;
const LARGEST_POWER: i32 = 309;
const POWER_COUNT: usize = (LARGEST_POWER - SMALLEST_POWER + 1) as usize;

/// `10^k` rounded to nearest-even: Exasol's table for `-308..=308`, widened to the exponents a
/// subnormal or the largest double is compared against.
static POWERS_OF_TEN: [Extended; POWER_COUNT] = powers_of_ten();

fn power_of_ten(power: i32) -> Extended {
    POWERS_OF_TEN[(power - SMALLEST_POWER) as usize]
}

fn scaled(value: Extended, scale: i32) -> Extended {
    let mut value = value;
    let mut scale = scale;
    while scale > LARGEST_TABLE_POWER {
        value = value.times(power_of_ten(LARGEST_TABLE_POWER));
        scale -= LARGEST_TABLE_POWER;
    }
    value.times(power_of_ten(scale))
}

/// 1152 bits hold `10^310` and keep 75 significant bits of `2^1151 / 10^324`.
const WORDS: usize = 18;
type Wide = [u64; WORDS];
const RECIPROCAL_NUMERATOR_BIT: u32 = u64::BITS * WORDS as u32 - 1;

const fn powers_of_ten() -> [Extended; POWER_COUNT] {
    let mut table = [Extended {
        exponent: 0,
        significand: 0,
    }; POWER_COUNT];
    let mut power: Wide = [0; WORDS];
    power[0] = 1;
    let mut k = 0;
    while k <= LARGEST_POWER {
        table[(k - SMALLEST_POWER) as usize] = nearest_extended(&power, 0, false);
        times_ten(&mut power);
        k += 1;
    }
    let mut reciprocal: Wide = [0; WORDS];
    reciprocal[WORDS - 1] = 1 << 63;
    let mut k = -1;
    while k >= SMALLEST_POWER {
        divide_by_ten(&mut reciprocal);
        // No power of ten divides a power of two, so every quotient drops a nonzero tail.
        table[(k - SMALLEST_POWER) as usize] =
            nearest_extended(&reciprocal, RECIPROCAL_NUMERATOR_BIT, true);
        k -= 1;
    }
    table
}

/// `value × 2^-scale` rounded to nearest-even; `truncated` says a nonzero tail lies below it.
const fn nearest_extended(value: &Wide, scale: u32, truncated: bool) -> Extended {
    let top = highest_set_bit(value);
    if top < u64::BITS {
        let shift = u64::BITS - 1 - top;
        return Extended {
            exponent: -(shift as i32) - scale as i32,
            significand: value[0] << shift,
        };
    }
    let lowest_kept = top - (u64::BITS - 1);
    let kept = bits_from(value, lowest_kept);
    let exponent = lowest_kept as i32 - scale as i32;
    let half = bit_is_set(value, lowest_kept - 1);
    let above_half = truncated || any_bit_below(value, lowest_kept - 1);
    if half && (above_half || kept & 1 == 1) {
        Extended::rounded_up(kept, exponent)
    } else {
        Extended {
            exponent,
            significand: kept,
        }
    }
}

const fn highest_set_bit(value: &Wide) -> u32 {
    let mut word = WORDS;
    while word > 0 {
        word -= 1;
        if value[word] != 0 {
            return word as u32 * u64::BITS + (u64::BITS - 1) - value[word].leading_zeros();
        }
    }
    0
}

const fn bit_is_set(value: &Wide, bit: u32) -> bool {
    (value[(bit / u64::BITS) as usize] >> (bit % u64::BITS)) & 1 == 1
}

const fn bits_from(value: &Wide, lowest: u32) -> u64 {
    let word = (lowest / u64::BITS) as usize;
    let offset = lowest % u64::BITS;
    if offset == 0 {
        value[word]
    } else {
        (value[word] >> offset) | (value[word + 1] << (u64::BITS - offset))
    }
}

const fn any_bit_below(value: &Wide, bit: u32) -> bool {
    let word = (bit / u64::BITS) as usize;
    if value[word] & ((1 << (bit % u64::BITS)) - 1) != 0 {
        return true;
    }
    let mut lower = 0;
    while lower < word {
        if value[lower] != 0 {
            return true;
        }
        lower += 1;
    }
    false
}

const fn times_ten(value: &mut Wide) {
    let mut carry = 0u128;
    let mut word = 0;
    while word < WORDS {
        let product = value[word] as u128 * 10 + carry;
        value[word] = product as u64;
        carry = product >> u64::BITS;
        word += 1;
    }
}

const fn divide_by_ten(value: &mut Wide) {
    let mut remainder = 0u128;
    let mut word = WORDS;
    while word > 0 {
        word -= 1;
        let dividend = (remainder << u64::BITS) | value[word] as u128;
        value[word] = (dividend / 10) as u64;
        remainder = dividend % 10;
    }
}

#[cfg(test)]
#[path = "double_text_tests.rs"]
mod tests;
