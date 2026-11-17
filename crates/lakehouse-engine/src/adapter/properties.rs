//! The virtual schema property catalog: every property name the adapter reads, and the value
//! rule of each tuning property and of EXCLUDED_CAPABILITIES. No other module reads a property
//! by name.

use super::capabilities::{CAPABILITIES, excluded_capability_entries};
use super::catalog_kind::CatalogKind;
use super::permission::PermissionSettings;
use super::table_filter::{TableFilter, parse_patterns};
pub(super) use crate::exasol::parse_boolean;
use crate::exasol::{WholeNumber, parse_whole_number};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroU64};

// Read from plain VS properties, never the CONNECTION password JSON. Absent means Iceberg REST.
pub(super) const PROP_CATALOG_KIND: &str = "CATALOG_KIND";
// CONNECTION whose address is the catalog URI and whose password is the credential JSON.
pub(super) use crate::exasol::CATALOG_CONNECTION as PROP_CATALOG_CONNECTION;
pub(super) const PROP_NAMESPACE: &str = "NAMESPACE";
const PROP_ALLOW_HTTP: &str = "ALLOW_HTTP";
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
const PROP_METADATA_READ_CONCURRENCY: &str = "METADATA_READ_CONCURRENCY";
pub(super) const PROP_MERGE_SCHEMA: &str = "MERGE_SCHEMA";
pub(super) const PROP_HIVE_PARTITIONING: &str = "HIVE_PARTITIONING";
// Like Spark's `spark.sql.parquet.binaryAsString`, for writers that store text as binary.
const PROP_BINARY_AS_STRING: &str = "BINARY_AS_STRING";
pub(super) const PERMISSION_CHECK: &str = "PERMISSION_CHECK";
pub(super) const USER_MAPPING: &str = "USER_MAPPING";
const PROP_TABLE_INCLUDE: &str = "TABLE_INCLUDE";
const PROP_TABLE_EXCLUDE: &str = "TABLE_EXCLUDE";
const PROP_EXCLUDED_CAPABILITIES: &str = "EXCLUDED_CAPABILITIES";

/// Every name a virtual schema may set; the value rules of the properties whose owning module
/// checks the value stay in that module.
const PROPERTY_NAMES: [&str; 22] = [
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
    PROP_METADATA_READ_CONCURRENCY,
    PROP_MERGE_SCHEMA,
    PROP_HIVE_PARTITIONING,
    PROP_BINARY_AS_STRING,
    PERMISSION_CHECK,
    USER_MAPPING,
    PROP_TABLE_INCLUDE,
    PROP_TABLE_EXCLUDE,
    PROP_EXCLUDED_CAPABILITIES,
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
    pub(super) metadata_read_concurrency: Option<NonZeroU8>,
}

/// The values this module checks. A rejected or absent boolean is `false`.
#[derive(Debug, Default, PartialEq)]
pub(super) struct CheckedProperties {
    pub(super) allow_http: bool,
    pub(super) binary_as_string: bool,
    pub(super) tuning: TuningProperties,
    pub(super) table_filter: TableFilter,
}

/// How a request treats an unknown property name or a rejected value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Check {
    /// createVirtualSchema, refresh, and setProperties fail with one error listing every one.
    Strict,
    /// Every other request reads a rejected value as absent, so a virtual schema persisted before
    /// a check existed still plans.
    Lenient,
}

/// A request's virtual schema properties, parsed once per request and never kept, so a SET
/// applies from the next request. The value rules of `MERGE_SCHEMA` and `HIVE_PARTITIONING`
/// belong to direct storage, which alone reads them.
pub(super) struct VsProperties {
    pub(super) catalog_kind: CatalogKind,
    pub(super) catalog_connection: Option<String>,
    pub(super) namespace: Option<String>,
    pub(super) merge_schema: Option<String>,
    pub(super) hive_partitioning: Option<String>,
    /// The raw value: `capabilities` owns the entry syntax.
    pub(super) excluded_capabilities: Option<String>,
    pub(super) permission: PermissionSettings,
    pub(super) checked: CheckedProperties,
}

impl VsProperties {
    pub(super) fn parse(props: &Json, check: Check) -> Result<Self, UdfError> {
        let checked = checked_values(props, check)?;
        let text = |name| nonempty_str(props, name).map(str::to_string);
        let permission = PermissionSettings::parse(
            nonempty_str(props, PERMISSION_CHECK),
            nonempty_str(props, USER_MAPPING),
        )?;
        Ok(Self {
            catalog_kind: CatalogKind::parse(nonempty_str(props, PROP_CATALOG_KIND))?,
            catalog_connection: text(PROP_CATALOG_CONNECTION),
            namespace: text(PROP_NAMESPACE),
            merge_schema: text(PROP_MERGE_SCHEMA),
            hive_partitioning: text(PROP_HIVE_PARTITIONING),
            excluded_capabilities: text(PROP_EXCLUDED_CAPABILITIES),
            permission,
            checked,
        })
    }
}

/// Under [`Check::Strict`], rejects every unknown property name, every invalid tuning or table
/// pattern value, and every unadvertised EXCLUDED_CAPABILITIES entry in one error. Each value is
/// checked on its own: no property's validity depends on another property.
pub(super) fn checked_values(props: &Json, check: Check) -> Result<CheckedProperties, UdfError> {
    let mut checker = TuningChecker::new(props);
    let checked = CheckedProperties {
        allow_http: checker.boolean(PROP_ALLOW_HTTP),
        binary_as_string: checker.boolean(PROP_BINARY_AS_STRING),
        tuning: checker.tuning(),
        table_filter: checker.table_filter(),
    };
    checker.excluded_capabilities();
    if check == Check::Lenient {
        return Ok(checked);
    }
    let mut entries: Vec<String> = unknown_names(props)
        .into_iter()
        .map(unknown_entry)
        .collect();
    entries.extend(checker.invalid.into_values());
    if entries.is_empty() {
        Ok(checked)
    } else {
        Err(UdfError::User(format!(
            "{ERROR_PREFIX}{}",
            entries.join("; ")
        )))
    }
}

fn nonempty_str<'a>(obj: &'a Json, key: &str) -> Option<&'a str> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
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
    format!(
        "unknown property '{name}'{}",
        suggestion(name, &PROPERTY_NAMES)
    )
}

fn unadvertised_capability_entry(entry: &str) -> String {
    let expected = format!(
        "an advertised capability name{}",
        suggestion(entry, CAPABILITIES)
    );
    invalid_entry(PROP_EXCLUDED_CAPABILITIES, entry, &expected)
}

/// ` (did you mean '<nearest>'?)` when [`nearest_name`] finds a candidate, else empty.
fn suggestion(name: &str, candidates: &[&str]) -> String {
    nearest_name(name, candidates)
        .map(|nearest| format!(" (did you mean '{nearest}'?)"))
        .unwrap_or_default()
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

    fn boolean(&mut self, name: &'static str) -> bool {
        self.value(name, check_boolean).unwrap_or(false)
    }

    fn tuning(&mut self) -> TuningProperties {
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
            metadata_read_concurrency: self.whole_number(PROP_METADATA_READ_CONCURRENCY),
        }
    }

    fn table_filter(&mut self) -> TableFilter {
        TableFilter::new(
            self.value(PROP_TABLE_INCLUDE, parse_patterns),
            self.value(PROP_TABLE_EXCLUDE, parse_patterns),
        )
    }

    fn excluded_capabilities(&mut self) {
        let Some(raw) = nonempty_str(self.props, PROP_EXCLUDED_CAPABILITIES) else {
            return;
        };
        let mut unadvertised: Vec<&str> = Vec::new();
        for entry in excluded_capability_entries(raw).filter(|e| !CAPABILITIES.contains(e)) {
            if !unadvertised.contains(&entry) {
                unadvertised.push(entry);
            }
        }
        if !unadvertised.is_empty() {
            let entries: Vec<String> = unadvertised
                .into_iter()
                .map(unadvertised_capability_entry)
                .collect();
            self.invalid
                .insert(PROP_EXCLUDED_CAPABILITIES, entries.join("; "));
        }
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

fn check_boolean(raw: &str) -> Result<bool, String> {
    parse_boolean(raw).ok_or_else(|| "TRUE or FALSE".to_string())
}

#[cfg(test)]
#[path = "properties_tests.rs"]
mod tests;
