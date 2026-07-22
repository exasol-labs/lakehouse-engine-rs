//! Packaging integration test: the single built `.so` exports EXACTLY the two
//! UDF entry point symbols (the VS adapter and the DataFusion scan SCALAR EMIT
//! UDF) and nothing more.
//!
//! Covers packaging/single-so-two-entry-points scenario:
//! "One crate exports the adapter and the scan entry points" — the `.so` SHALL
//! export the adapter entry-point symbol and the scan entry-point symbol, and
//! MUST export no third entry point (single-group `COUNT(DISTINCT)` now merges
//! via a native Exasol `COUNT(DISTINCT)`, so the former scalar distinct-merge
//! entry point is gone).
//!
//! Gated under `exasol-e2e` because it inspects the containerized release
//! artifact, which `make test-e2e` guarantees is freshly built (it depends on
//! `cross-musl-udf-build`). When run, it FAILS loudly if the `.so` is missing —
//! it never silently skips.
#![cfg(feature = "exasol-e2e")]

use std::path::PathBuf;
use std::process::Command;

/// Host-side path of the built lakehouse-engine `.so`.
fn so_path() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/lakehouse-engine; the workspace target/ is ../../target.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("navigate to workspace root from CARGO_MANIFEST_DIR")
        .join("target/release/liblakehouse_engine.so")
}

#[test]
fn so_exports_scan_symbol_and_no_distributor_symbol() {
    let so = so_path();
    assert!(
        so.exists(),
        "built artifact not found at {} — run `make cross-musl-udf-build` first",
        so.display()
    );

    let output = Command::new("nm")
        .arg("-D")
        .arg(&so)
        .output()
        .expect("failed to run `nm -D` — is binutils installed?");
    assert!(
        output.status.success(),
        "`nm -D {}` failed: {}",
        so.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let symbols = String::from_utf8_lossy(&output.stdout);

    assert!(
        symbols.contains("__exa_udf_entry_LAKEHOUSE_ADAPTER"),
        "the .so must export the adapter entry symbol __exa_udf_entry_LAKEHOUSE_ADAPTER"
    );
    assert!(
        symbols.contains("__exa_udf_entry_LAKEHOUSE_SCAN"),
        "the .so must export the scan entry symbol __exa_udf_entry_LAKEHOUSE_SCAN"
    );
    // EXACTLY two Rust UDF entry points — the adapter and the scan. The former
    // scalar distinct-merge entry point was removed (single-group COUNT(DISTINCT)
    // now merges via a native Exasol COUNT(DISTINCT)), so any third
    // `__exa_udf_entry_*` symbol is a regression.
    let entry_symbols: Vec<&str> = symbols
        .lines()
        .filter(|line| line.contains("__exa_udf_entry_"))
        .collect();
    assert_eq!(
        entry_symbols.len(),
        2,
        "the .so must export EXACTLY two UDF entry-point symbols (the adapter and \
         the scan), found: {entry_symbols:?}"
    );
    // The file distributor (`LAKEHOUSE_DISTRIBUTE_FILES`) is a plain LUA SET script
    // created by its own DDL — it carries no Rust logic and is NOT a `.so` entry
    // point, so no `__exa_udf_entry_LAKEHOUSE_DISTRIBUTE_FILES` symbol may exist.
    assert!(
        !symbols.contains("__exa_udf_entry_LAKEHOUSE_DISTRIBUTE_FILES"),
        "the .so must NOT export a distributor entry symbol — \
         LAKEHOUSE_DISTRIBUTE_FILES is a LUA SET script, not a Rust `.so` entry point"
    );
}
