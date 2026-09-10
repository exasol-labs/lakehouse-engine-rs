//! Packaging integration test: the single built `.so` exports EXACTLY the three
//! UDF entry point symbols (the VS adapter, the DataFusion scan SCALAR EMIT UDF,
//! and the version query SCALAR RETURNS UDF) and nothing more.
//!
//! Covers packaging/single-so-two-entry-points scenario:
//! "One crate exports the adapter, scan, and version entry points" — the `.so`
//! SHALL export those three entry-point symbols and MUST export no fourth.
//!
//! Gated under `exasol-e2e` because it inspects the containerized release
//! artifact, which `make test-e2e` guarantees is freshly built (it depends on
//! `cross-udf-build`). When run, it FAILS loudly if the `.so` is missing —
//! it never silently skips.
#![cfg(feature = "exasol-e2e")]

use std::path::PathBuf;
use std::process::Command;

fn so_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("navigate to workspace root from CARGO_MANIFEST_DIR")
        .join("target/release/liblakehouse_engine.so")
}

#[test]
fn so_exports_three_entry_point_symbols() {
    let so = so_path();
    assert!(
        so.exists(),
        "built artifact not found at {} — run `make cross-udf-build` first",
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
        symbols.contains("__exa_udf_entry_LAKEHOUSE_VERSION"),
        "the .so must export the version entry symbol __exa_udf_entry_LAKEHOUSE_VERSION"
    );
    let entry_symbols: Vec<&str> = symbols
        .lines()
        .filter(|line| line.contains("__exa_udf_entry_"))
        .collect();
    assert_eq!(
        entry_symbols.len(),
        3,
        "the .so must export EXACTLY three UDF entry-point symbols (adapter, scan, \
         version), found: {entry_symbols:?}"
    );
    assert!(
        !symbols.contains("__exa_udf_entry_LAKEHOUSE_DISTRIBUTE_FILES"),
        "the .so must NOT export a distributor entry symbol — \
         LAKEHOUSE_DISTRIBUTE_FILES is a LUA SET script, not a Rust `.so` entry point"
    );
}
