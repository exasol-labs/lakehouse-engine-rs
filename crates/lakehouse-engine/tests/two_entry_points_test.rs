//! Packaging integration test: the single built `.so` exports all THREE UDF
//! entry point symbols (the VS adapter, the DataFusion scan SET UDF, and the
//! scalar distinct-merge UDF).
//!
//! Covers packaging/single-so-two-entry-points scenario:
//! "One crate exports both the adapter and the scan entry points" (extended to
//! three) — the `.so` SHALL export the adapter entry-point symbol, the scan
//! entry-point symbol, and the scalar distinct-merge entry-point symbol.
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
fn so_exports_adapter_scan_and_distinct_merge_symbols() {
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
    assert!(
        symbols.contains("__exa_udf_entry_LAKEHOUSE_DISTINCT_MERGE_COUNT"),
        "the .so must export the scalar distinct-merge entry symbol \
         __exa_udf_entry_LAKEHOUSE_DISTINCT_MERGE_COUNT"
    );
}
