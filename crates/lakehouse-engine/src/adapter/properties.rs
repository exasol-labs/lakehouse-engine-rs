//! The virtual schema property catalog: every property name the adapter reads, and the value
//! rule of each tuning property.

use super::nonempty_str;
use super::table_filter::{TableFilter, parse_patterns};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;

// Read from plain VS properties, never the CONNECTION password JSON. Absent means Iceberg REST.
pub(super) const PROP_CATALOG_KIND: &str = "CATALOG_KIND";
// CONNECTION whose address is the catalog URI and whose password is the credential JSON.
pub(super) const PROP_CATALOG_CONNECTION: &str = "CATALOG_CONNECTION";
pub(super) const PROP_NAMESPACE: &str = "NAMESPACE";
pub(super) const PROP_ALLOW_HTTP: &str = "ALLOW_HTTP";
pub(super) const PROP_PARALLELISM_FACTOR: &str = "PARALLELISM_FACTOR";
pub(super) const PROP_DF_THREADING_MODE: &str = "DATAFUSION_THREADING_MODE";
pub(super) const PROP_DF_TARGET_PARTITIONS: &str = "DATAFUSION_TARGET_PARTITIONS";
pub(super) const PROP_DF_THREADS_PER_UDF: &str = "DATAFUSION_THREADS_PER_UDF";
pub(super) const PROP_DF_BATCH_SIZE: &str = "DATAFUSION_BATCH_SIZE";
pub(super) const PROP_MEMORY_POOL_FRACTION: &str = "MEMORY_POOL_FRACTION";
pub(super) const PROP_INSTANCE_OVERHEAD_MB: &str = "INSTANCE_OVERHEAD_MB";
// A join's smaller side is broadcast into every shard when its total file bytes are at or below
// this threshold; larger joins fall back to an unaccelerated two-scan join.
pub(super) const PROP_JOIN_BROADCAST_MAX_BYTES: &str = "JOIN_BROADCAST_MAX_BYTES";
// Mirrors the native `IMPORT FROM PARQUET` `MaxConnections` knob.
pub(super) const PROP_S3_MAX_CONNECTIONS: &str = "S3_MAX_CONNECTIONS";
pub(super) const PROP_MERGE_SCHEMA: &str = "MERGE_SCHEMA";
pub(super) const PROP_HIVE_PARTITIONING: &str = "HIVE_PARTITIONING";
// Like Spark's `spark.sql.parquet.binaryAsString`, for writers that store text as binary.
const PROP_BINARY_AS_STRING: &str = "BINARY_AS_STRING";
pub(super) const PERMISSION_CHECK: &str = "PERMISSION_CHECK";
pub(super) const USER_MAPPING: &str = "USER_MAPPING";
const PROP_TABLE_INCLUDE: &str = "TABLE_INCLUDE";
const PROP_TABLE_EXCLUDE: &str = "TABLE_EXCLUDE";

/// Every name a virtual schema may set; the value rules of the properties whose owning module
/// checks the value stay in that module.
const PROPERTY_NAMES: [&str; 20] = [
    PROP_CATALOG_KIND,
    PROP_CATALOG_CONNECTION,
    PROP_NAMESPACE,
    PROP_ALLOW_HTTP,
    PROP_PARALLELISM_FACTOR,
    PROP_DF_THREADING_MODE,
    PROP_DF_TARGET_PARTITIONS,
    PROP_DF_THREADS_PER_UDF,
    PROP_DF_BATCH_SIZE,
    PROP_MEMORY_POOL_FRACTION,
    PROP_INSTANCE_OVERHEAD_MB,
    PROP_JOIN_BROADCAST_MAX_BYTES,
    PROP_S3_MAX_CONNECTIONS,
    PROP_MERGE_SCHEMA,
    PROP_HIVE_PARTITIONING,
    PROP_BINARY_AS_STRING,
    PERMISSION_CHECK,
    USER_MAPPING,
    PROP_TABLE_INCLUDE,
    PROP_TABLE_EXCLUDE,
];

const ERROR_PREFIX: &str = "invalid virtual schema properties: ";
const SUGGESTION_MAX_EDITS: usize = 2;
const ECHO_MAX_CHARS: usize = 100;

/// Planning-time only: just the resulting integers reach the scan UDF, which stays mode-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ThreadingMode {
    Auto,
    Fixed,
}

/// The checked tuning values; `None` marks an absent property, which takes its default.
#[derive(Debug, Default, PartialEq)]
pub(super) struct TuningProperties {
    pub(super) parallelism_factor: Option<NonZeroU8>,
    pub(super) threading_mode: Option<ThreadingMode>,
    pub(super) df_target_partitions: Option<NonZeroU16>,
    pub(super) df_threads_per_udf: Option<NonZeroU16>,
    pub(super) df_batch_size: Option<NonZeroU32>,
    pub(super) memory_pool_fraction: Option<f64>,
    pub(super) instance_overhead_mb: Option<u16>,
    pub(super) join_broadcast_max_bytes: Option<NonZeroU64>,
    pub(super) s3_max_connections: Option<NonZeroU16>,
}

/// The checked values that the create-time resolution reads without parsing a string again.
#[derive(Debug, Default, PartialEq)]
pub(super) struct CheckedProperties {
    pub(super) tuning: TuningProperties,
    pub(super) table_filter: TableFilter,
}

/// Rejects every unknown property name and every invalid tuning or table pattern value in one
/// error. Each value is checked on its own: no property's validity depends on another property.
pub(super) fn check_properties(props: &Json) -> Result<CheckedProperties, UdfError> {
    let mut entries: Vec<String> = unknown_names(props)
        .into_iter()
        .map(unknown_entry)
        .collect();
    match check_values(props) {
        Ok(checked) if entries.is_empty() => return Ok(checked),
        Ok(_) => {}
        Err(invalid) => entries.extend(invalid),
    }
    Err(UdfError::User(format!(
        "{ERROR_PREFIX}{}",
        entries.join("; ")
    )))
}

/// Reads `ALLOW_HTTP` with the TRUE/FALSE rule that `check_properties` applies. A rejected value
/// counts as `false`, which only a pushdown on a schema persisted before the check can reach.
pub(super) fn resolve_allow_http(props: &Json) -> bool {
    nonempty_str(props, PROP_ALLOW_HTTP)
        .and_then(parse_boolean)
        .unwrap_or(false)
}

/// Read at every pushdown, so a SET applies from the next query. A rejected value counts as
/// `false`, as in [`resolve_allow_http`].
pub(super) fn resolve_binary_as_string(props: &Json) -> bool {
    nonempty_str(props, PROP_BINARY_AS_STRING)
        .and_then(parse_boolean)
        .unwrap_or(false)
}

fn unknown_names(props: &Json) -> BTreeSet<&str> {
    props
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| !PROPERTY_NAMES.contains(&name.as_str()))
        .filter(|(name, _)| nonempty_str(props, name).is_some())
        .map(|(name, _)| name.as_str())
        .collect()
}

fn unknown_entry(name: &str) -> String {
    match nearest_name(name, &PROPERTY_NAMES) {
        Some(known) => format!("unknown property '{name}' (did you mean '{known}'?)"),
        None => format!("unknown property '{name}'"),
    }
}

/// The candidate at most [`SUGGESTION_MAX_EDITS`] edits from `unknown`, ignoring case; a tie
/// goes to the alphabetically first.
fn nearest_name<'a>(unknown: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let folded: Vec<char> = unknown.to_uppercase().chars().collect();
    candidates
        .iter()
        .map(|&candidate| {
            let candidate_chars: Vec<char> = candidate.to_uppercase().chars().collect();
            (edit_distance(&folded, &candidate_chars), candidate)
        })
        .filter(|&(distance, _)| distance <= SUGGESTION_MAX_EDITS)
        .min()
        .map(|(_, candidate)| candidate)
}

fn edit_distance(a: &[char], b: &[char]) -> usize {
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, &a_char) in a.iter().enumerate() {
        let mut current = vec![i + 1];
        for (j, &b_char) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(a_char != b_char);
            current.push(substitution.min(previous[j + 1] + 1).min(current[j] + 1));
        }
        previous = current;
    }
    previous[b.len()]
}

/// One entry per rejected value, in alphabetical order of the name.
fn check_values(props: &Json) -> Result<CheckedProperties, Vec<String>> {
    let mut checker = TuningChecker::new(props);
    let tuning = checker.tuning();
    let table_filter = checker.table_filter();
    if checker.invalid.is_empty() {
        Ok(CheckedProperties {
            tuning,
            table_filter,
        })
    } else {
        Err(checker.invalid.into_values().collect())
    }
}

struct TuningChecker<'a> {
    props: &'a Json,
    /// Keyed by property name, so the entries read in alphabetical order.
    invalid: BTreeMap<&'static str, String>,
}

impl<'a> TuningChecker<'a> {
    fn new(props: &'a Json) -> Self {
        Self {
            props,
            invalid: BTreeMap::new(),
        }
    }

    fn tuning(&mut self) -> TuningProperties {
        self.value(PROP_ALLOW_HTTP, check_boolean);
        self.value(PROP_BINARY_AS_STRING, check_boolean);
        TuningProperties {
            parallelism_factor: self.whole_number(PROP_PARALLELISM_FACTOR),
            threading_mode: self.value(PROP_DF_THREADING_MODE, parse_threading_mode),
            df_target_partitions: self.whole_number(PROP_DF_TARGET_PARTITIONS),
            df_threads_per_udf: self.whole_number(PROP_DF_THREADS_PER_UDF),
            df_batch_size: self.whole_number(PROP_DF_BATCH_SIZE),
            memory_pool_fraction: self.value(PROP_MEMORY_POOL_FRACTION, parse_fraction),
            instance_overhead_mb: self.whole_number(PROP_INSTANCE_OVERHEAD_MB),
            join_broadcast_max_bytes: self.whole_number(PROP_JOIN_BROADCAST_MAX_BYTES),
            s3_max_connections: self.whole_number(PROP_S3_MAX_CONNECTIONS),
        }
    }

    fn table_filter(&mut self) -> TableFilter {
        TableFilter::new(
            self.value(PROP_TABLE_INCLUDE, parse_patterns),
            self.value(PROP_TABLE_EXCLUDE, parse_patterns),
        )
    }

    /// Applies `rule` to a present value; a rejection is recorded with the expectation the rule
    /// returns, and the property reads as absent.
    fn value<T>(
        &mut self,
        name: &'static str,
        rule: impl FnOnce(&str) -> Result<T, String>,
    ) -> Option<T> {
        let raw = nonempty_str(self.props, name)?;
        match rule(raw) {
            Ok(value) => Some(value),
            Err(expected) => {
                self.invalid
                    .insert(name, invalid_entry(name, raw, &expected));
                None
            }
        }
    }

    fn whole_number<T: WholeNumber>(&mut self, name: &'static str) -> Option<T> {
        self.value(name, parse_whole_number)
    }
}

fn invalid_entry(name: &str, raw: &str, expected: &str) -> String {
    format!(
        "invalid value '{}' for '{name}': expected {expected}",
        echo(raw)
    )
}

/// The first [`ECHO_MAX_CHARS`] characters, cut before the quotes are doubled, so a long value
/// or a value with a quote keeps the error readable.
fn echo(raw: &str) -> String {
    let mut chars = raw.chars();
    let head: String = chars.by_ref().take(ECHO_MAX_CHARS).collect();
    let mut echoed = head.replace('\'', "\'\'");
    if chars.next().is_some() {
        echoed.push('\u{2026}');
    }
    echoed
}

/// A whole-number property type; its range is the range the property accepts, so the check and
/// its message share one definition.
trait WholeNumber: FromStr {
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
whole_number!(u16, u16::MIN, u16::MAX);

/// Every parse error, overflow included, maps to the range expectation.
fn parse_whole_number<T: WholeNumber>(raw: &str) -> Result<T, String> {
    raw.parse()
        .map_err(|_| format!("a whole number from {} to {}", T::MIN, T::MAX))
}

fn parse_fraction(raw: &str) -> Result<f64, String> {
    raw.parse::<f64>()
        .ok()
        .filter(|&fraction| fraction > 0.0 && fraction <= 1.0)
        .ok_or_else(|| "a number greater than 0 and at most 1".to_string())
}

fn parse_threading_mode(raw: &str) -> Result<ThreadingMode, String> {
    if raw.eq_ignore_ascii_case("AUTO") {
        Ok(ThreadingMode::Auto)
    } else if raw.eq_ignore_ascii_case("FIXED") {
        Ok(ThreadingMode::Fixed)
    } else {
        Err("AUTO or FIXED".to_string())
    }
}

/// The one spelling rule of every boolean property: `TRUE` or `FALSE` in any letter case.
pub(super) fn parse_boolean(raw: &str) -> Option<bool> {
    if raw.eq_ignore_ascii_case("TRUE") {
        Some(true)
    } else if raw.eq_ignore_ascii_case("FALSE") {
        Some(false)
    } else {
        None
    }
}

fn check_boolean(raw: &str) -> Result<bool, String> {
    parse_boolean(raw).ok_or_else(|| "TRUE or FALSE".to_string())
}

#[cfg(test)]
#[path = "properties_tests.rs"]
mod tests;
