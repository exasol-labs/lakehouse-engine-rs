//! Exasol rules that the virtual schema adapter, the scan and the export all follow.
//! It depends on nothing else in the crate, so no consumer depends on another.

use std::num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;

/// The name of the CONNECTION property and of the export's WITH option.
pub(crate) const CATALOG_CONNECTION: &str = "CATALOG_CONNECTION";

/// Exasol distributes groups round-robin up to this count (`max_dynamic_group_count`);
/// above it they are hash-partitioned (unbalanced).
const MAX_GROUP_COUNT: usize = 300;

/// `node_count × per_node`, clamped to at least 1 and at most `min(upper_bound, 300)`.
pub(crate) fn group_count(node_count: usize, per_node: usize, upper_bound: usize) -> usize {
    let upper = upper_bound.clamp(1, MAX_GROUP_COUNT);
    node_count.saturating_mul(per_node).clamp(1, upper)
}

/// A CONNECTION name is required and non-empty; `None` means the caller reports it missing.
pub(crate) fn catalog_connection_name(name: Option<&str>) -> Option<&str> {
    name.filter(|n| !n.is_empty())
}

pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The one spelling rule of every boolean option: `TRUE` or `FALSE` in any letter case.
pub(crate) fn parse_boolean(raw: &str) -> Option<bool> {
    if raw.eq_ignore_ascii_case("TRUE") {
        Some(true)
    } else if raw.eq_ignore_ascii_case("FALSE") {
        Some(false)
    } else {
        None
    }
}

/// A whole-number option type; its range is the range the option accepts, so the check and
/// its message share one definition.
pub(crate) trait WholeNumber: FromStr {
    const MIN: u128;
    const MAX: u128;
}

macro_rules! whole_number {
    ($ty:ty, $min:expr, $max:expr) => {
        impl WholeNumber for $ty {
            const MIN: u128 = $min as u128;
            const MAX: u128 = $max as u128;
        }
    };
}

whole_number!(NonZeroU8, NonZeroU8::MIN.get(), NonZeroU8::MAX.get());
whole_number!(NonZeroU16, NonZeroU16::MIN.get(), NonZeroU16::MAX.get());
whole_number!(NonZeroU32, NonZeroU32::MIN.get(), NonZeroU32::MAX.get());
whole_number!(NonZeroU64, NonZeroU64::MIN.get(), NonZeroU64::MAX.get());
whole_number!(
    NonZeroUsize,
    NonZeroUsize::MIN.get(),
    NonZeroUsize::MAX.get()
);
whole_number!(u16, u16::MIN, u16::MAX);

/// Digits only: `str::parse` also accepts a leading `+`. Every failure, overflow included,
/// maps to the range expectation.
pub(crate) fn parse_whole_number<T: WholeNumber>(raw: &str) -> Result<T, String> {
    let digits = !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit());
    digits
        .then(|| raw.parse().ok())
        .flatten()
        .ok_or_else(|| format!("a whole number from {} to {}", T::MIN, T::MAX))
}

#[cfg(test)]
#[path = "exasol_tests.rs"]
mod tests;
