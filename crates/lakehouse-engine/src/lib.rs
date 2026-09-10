//! Lakehouse Virtual Schema — single `.so`, three entry points.
//!
//! Entry point #1: VS adapter (`__exa_udf_entry_LAKEHOUSE_ADAPTER`)
//!   Handles the Exasol Virtual Schema JSON protocol: getCapabilities,
//!   createVirtualSchema, refresh, setProperties, dropVirtualSchema, pushdown.
//!   Resolves the Iceberg file list ONCE in `pushdown` and returns SQL that
//!   invokes the scan SCALAR EMIT UDF with the explicit file list.
//!
//! Entry point #2: DataFusion scan SCALAR EMIT UDF (`__exa_udf_entry_LAKEHOUSE_SCAN`)
//!   Invoked once per input row (SDK 0.21.0 scalar dispatch). Each call reads one
//!   row's ScanSpec, builds a DataFusion session on a fresh per-call Tokio runtime,
//!   registers only that row's assigned files over MinIO, applies
//!   projection/filter/limit, converts Arrow batches to SDK `Value` rows, and emits
//!   them incrementally. The union of all per-row calls covers every shard.
//!
//! Entry point #3: version query SCALAR RETURNS UDF (`__exa_udf_entry_LAKEHOUSE_VERSION`)
//!   Zero-argument, zero-I/O call that reports [`ENGINE_VERSION`], the crate
//!   version compiled into this `.so`. Needs no CONNECTION and no Virtual
//!   Schema.
//!
//! Architecture invariants enforced here:
//! - Only SDK `Value` types cross the `.so` boundary — never Arrow types.
//! - The scan UDF is stateless and discovers no files itself.
//! - Credentials never appear in error messages.
//!
//! Build: `make cross-udf-build` (inside `rust:1.94-trixie`).
//! Never `cargo build --release` on the host — produces an unloadable host-glibc `.so`.

use exasol_udf_macros::exasol_udf;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

pub mod adapter;
pub mod scan;
#[cfg(test)]
#[path = "scan_surface_probe_tests.rs"]
mod scan_surface_probe;
#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
pub mod types;

#[exasol_udf(vs_adapter(adapter::adapter_call))]
fn lakehouse_adapter(_ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    Ok(())
}

#[exasol_udf(name = "LAKEHOUSE_SCAN", input(common: String, files: String))]
fn lakehouse_scan(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    scan::run_scan(ctx)
}

const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[exasol_udf(name = "LAKEHOUSE_VERSION")]
fn lakehouse_version(_ctx: &mut dyn UdfContext) -> Result<Option<String>, UdfError> {
    Ok(Some(ENGINE_VERSION.to_string()))
}
