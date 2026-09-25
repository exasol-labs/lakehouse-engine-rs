//! One `.so`, three entry points: VS adapter, DataFusion scan UDF, version UDF.
//!
//! Invariants: only SDK `Value` types (or Arrow IPC bytes) cross the `.so` boundary, never
//! Arrow types; the scan UDF discovers no files itself; credentials never appear in errors.
//! Build only via `make cross-udf-build`; a host `cargo build --release` yields an
//! unloadable host-glibc `.so`.

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
