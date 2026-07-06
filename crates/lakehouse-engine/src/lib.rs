//! Lakehouse Virtual Schema — single `.so`, three entry points.
//!
//! Entry point #1: VS adapter (`__exa_udf_entry_LAKEHOUSE_ADAPTER`)
//!   Handles the Exasol Virtual Schema JSON protocol: getCapabilities,
//!   createVirtualSchema, refreshVirtualSchema, dropVirtualSchema, pushdown.
//!   Resolves the Iceberg file list ONCE in `pushdown` and returns SQL that
//!   invokes the scan SET UDF with the explicit file list.
//!
//! Entry point #2: DataFusion scan SET UDF (`__exa_udf_entry_LAKEHOUSE_SCAN`)
//!   Reads a ScanSpec JSON from its input column, builds a DataFusion session,
//!   registers only the assigned files over MinIO, applies projection/filter/limit,
//!   converts Arrow batches to SDK `Value` rows, and emits them incrementally.
//!
//! Entry point #3: distinct-merge SCALAR UDF (`__exa_udf_entry_LAKEHOUSE_DISTINCT_MERGE_COUNT`)
//!   Merges per-shard `COUNT(DISTINCT col)` local distinct sets. The outer wrapper
//!   SQL feeds it the concatenation of each shard's JSON-array partial value
//!   (`'[' || LISTAGG(partial_col, ',') || ']'`); it unions all inner values into
//!   one set and returns the global distinct cardinality as a DECIMAL(20,0).
//!
//! Architecture invariants enforced here:
//! - Only SDK `Value` types cross the `.so` boundary — never Arrow types.
//! - The scan UDF is stateless and discovers no files itself.
//! - Credentials never appear in error messages.
//!
//! Build: `make cross-musl-udf-build` (inside `rust:1.94-bookworm`).
//! Never `cargo build --release` on the host — produces an unloadable host-glibc `.so`.

use std::collections::HashSet;

use exasol_udf_macros::exasol_udf;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{Decimal, Value};

pub mod adapter;
pub mod scan;
pub mod types;

// ---------------------------------------------------------------------------
// Entry point #1 — VS Adapter (ADAPTER SCRIPT)
// ---------------------------------------------------------------------------

/// VS adapter hook — delegates to `adapter::adapter_call`.
///
/// The `#[exasol_udf(vs_adapter(adapter::adapter_call))]` annotation wires
/// `adapter::adapter_call` into the `virtual_schema_adapter_call` vtable slot.
/// The exported symbol is `__exa_udf_entry_LAKEHOUSE_ADAPTER`.
#[exasol_udf(vs_adapter(adapter::adapter_call))]
fn lakehouse_adapter(_ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    // The run hook is never called for an ADAPTER SCRIPT — the adapter call
    // goes through `virtual_schema_adapter_call`. This body is unreachable.
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point #2 — DataFusion Scan SET UDF (SET SCRIPT)
// ---------------------------------------------------------------------------

/// DataFusion scan SET UDF.
///
/// Input: two VARCHAR columns — the shard-invariant common blob JSON (arg 0)
/// serialized ONCE per fan-out, and the per-shard files JSON array (arg 1).
/// Emits: the projected columns declared in the adapter's EMITS clause.
///
/// The `emits(...)` annotation is omitted because the actual EMITS are
/// declared dynamically in the SQL string the adapter returns from pushdown —
/// Exasol injects the EMITS at script execution time from the SQL. The macro
/// records a null pointer for `annotated_output_schema`, which the runtime
/// accepts. The `input(common: String, files: String)` annotation documents the
/// two-argument input contract without restricting the dynamic EMITS.
///
/// The exported symbol is `__exa_udf_entry_LAKEHOUSE_SCAN`.
#[exasol_udf(name = "LAKEHOUSE_SCAN", input(common: String, files: String))]
fn lakehouse_scan(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    scan::run_scan(ctx)
}

// ---------------------------------------------------------------------------
// Entry point #3 — distinct-merge SCALAR UDF (SCALAR SCRIPT)
// ---------------------------------------------------------------------------

/// Scalar merge UDF for single-group `COUNT(DISTINCT col)`.
///
/// Input: one VARCHAR argument — the concatenation of every shard's local
/// distinct-set JSON array, wrapped by the outer wrapper SQL into a single
/// JSON array-of-arrays via `'[' || LISTAGG(partial_col, ',') || ']'`.
/// Returns: the global distinct count as DECIMAL(20,0) (declared in the
/// SCALAR SCRIPT DDL; DECIMAL(20,0) covers the full `u64` range).
///
/// A `LISTAGG` over zero shard rows yields NULL, so the argument can arrive
/// NULL — that is treated as a distinct count of zero. Only the SDK `Value`
/// crosses the `.so` boundary.
///
/// The exported symbol is `__exa_udf_entry_LAKEHOUSE_DISTINCT_MERGE_COUNT`.
#[exasol_udf(
    name = "LAKEHOUSE_DISTINCT_MERGE_COUNT",
    input(partials: String),
    // SCALAR SCRIPT return columns are not user-named via DDL — Exasol always
    // reports the return column as `RETURN` at the handshake. The annotated name
    // must match that verbatim (the runtime's schema check is an exact,
    // case-sensitive, positional name+type match), so annotate `RETURN` rather
    // than a made-up name. This keeps the Numeric type-check safety net (catches
    // a future DDL/type drift from DECIMAL(20,0)) while fixing the name mismatch.
    emits(RETURN: Decimal)
)]
fn lakehouse_distinct_merge_count(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    let count = match ctx.get_string(0)? {
        Some(partials) => merge_distinct_count(partials)?,
        None => 0,
    };
    ctx.emit(&[Value::Numeric(Decimal {
        unscaled: i128::from(count),
        scale: 0,
    })])
}

/// Union per-shard local distinct sets into a global distinct count.
///
/// `json` is a JSON array of per-shard JSON arrays (e.g. `[["F","N"],["N","O"]]`).
/// Every non-null inner value is unioned into a single set keyed by its canonical
/// JSON form (robust to string- and numeric-typed distinct columns, since a single
/// `COUNT(DISTINCT col)` column is homogeneously typed across shards), and the set's
/// cardinality is returned. Values that appear in more than one shard are counted
/// once; JSON `null` elements are never counted; an empty/NULL input is count 0.
pub fn merge_distinct_count(json: &str) -> Result<u64, UdfError> {
    let trimmed = json.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let parsed: serde_json::Value = serde_json::from_str(trimmed)
        .map_err(|e| UdfError::User(format!("distinct-merge input is not valid JSON: {e}")))?;
    let shards = match parsed {
        serde_json::Value::Array(shards) => shards,
        serde_json::Value::Null => return Ok(0),
        _ => {
            return Err(UdfError::User(
                "distinct-merge input must be a JSON array of per-shard arrays".into(),
            ));
        }
    };
    let mut distinct: HashSet<String> = HashSet::new();
    for shard in shards {
        match shard {
            serde_json::Value::Array(values) => {
                for value in values {
                    if value.is_null() {
                        continue;
                    }
                    distinct.insert(value.to_string());
                }
            }
            serde_json::Value::Null => continue,
            _ => {
                return Err(UdfError::User(
                    "distinct-merge input must be a JSON array of per-shard arrays".into(),
                ));
            }
        }
    }
    Ok(distinct.len() as u64)
}

#[cfg(test)]
mod distinct_merge_tests {
    use super::merge_distinct_count;

    #[test]
    fn merge_distinct_count_unions_dedups_and_counts() {
        // dedup across shards: {F,N} ∪ {N,O} = {F,N,O}
        assert_eq!(merge_distinct_count(r#"[["F","N"],["N","O"]]"#).unwrap(), 3);
        // normal multi-shard, no overlap
        assert_eq!(merge_distinct_count(r#"[["A"],["B"],["C"]]"#).unwrap(), 3);
        // single shard
        assert_eq!(merge_distinct_count(r#"[["F","N","O"]]"#).unwrap(), 3);
        // numeric-column robustness: dedup on the shared value 2
        assert_eq!(merge_distinct_count(r#"[[1,2],[2,3]]"#).unwrap(), 3);
        // a JSON null inside a shard set is never counted
        assert_eq!(merge_distinct_count(r#"[["F",null],["F"]]"#).unwrap(), 1);
    }

    #[test]
    fn merge_distinct_count_handles_empty_and_degenerate() {
        // empty outer array (no shards)
        assert_eq!(merge_distinct_count("[]").unwrap(), 0);
        // one shard with an empty local set
        assert_eq!(merge_distinct_count("[[]]").unwrap(), 0);
        // empty string (a LISTAGG over zero rows can surface as empty)
        assert_eq!(merge_distinct_count("").unwrap(), 0);
        // whitespace only
        assert_eq!(merge_distinct_count("   ").unwrap(), 0);
        // literal JSON null (defensive)
        assert_eq!(merge_distinct_count("null").unwrap(), 0);
    }

    #[test]
    fn merge_distinct_count_rejects_malformed_json() {
        assert!(merge_distinct_count("not json").is_err());
        assert!(merge_distinct_count(r#"{"a":1}"#).is_err());
    }
}
