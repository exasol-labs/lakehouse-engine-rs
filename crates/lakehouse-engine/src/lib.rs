//! Lakehouse Virtual Schema — single `.so`, two entry points.
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
//! Architecture invariants enforced here:
//! - Only SDK `Value` types cross the `.so` boundary — never Arrow types.
//! - The scan UDF is stateless and discovers no files itself.
//! - Credentials never appear in error messages.
//!
//! Build: `make cross-musl-udf-build` (inside `rust:1.94-bookworm`).
//! Never `cargo build --release` on the host — produces an unloadable host-glibc `.so`.

use exasol_udf_macros::exasol_udf;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

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
// Entry point #2 — DataFusion Scan SCALAR EMIT UDF (SCALAR SCRIPT)
// ---------------------------------------------------------------------------

/// DataFusion scan SCALAR EMIT UDF.
///
/// Input: two VARCHAR columns — the shard-invariant common blob JSON (arg 0)
/// serialized ONCE per fan-out, and the per-shard files JSON array (arg 1).
/// Emits: the projected columns declared in the adapter's EMITS clause.
///
/// Exasol drives this scalar UDF once per input row (SDK 0.21.0): each `run()`
/// call handles exactly one row's shard on its own fresh Tokio runtime and never
/// iterates with `ctx.next()` (now a runtime-enforced error in scalar context).
/// The dispatch/runtime contract lives in [`scan::run_scan`].
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
