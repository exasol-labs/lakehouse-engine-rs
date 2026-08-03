//! Pure convention test (no I/O): asserts the catalog crate's own manifest
//! never declares a direct dependency on the execution-engine stack
//! (arrow/parquet/datafusion/object_store/roaring), UDF/tracing plumbing
//! (async-trait/tracing/exasol-udf-macros), or the engine crate itself
//! (lakehouse-engine) -- so `lakehouse-catalog` stays a standalone crate that
//! `lakehouse-engine` depends on one way, never the reverse.
//!
//! Covers crate-boundary scenario:
//! "The catalog access layer lives in a standalone crate the engine depends on
//! one way" -- catalog_manifest_declares_no_execution_engine_dependency.
//!
//! The manifest is embedded at compile time via `include_str!`, so this is a
//! pure test: it reads no files at runtime and needs no live services.

/// This crate's own manifest, embedded at compile time.
/// Path is relative to this source file: crates/lakehouse-catalog/tests -> crate root.
const CATALOG_MANIFEST: &str = include_str!("../Cargo.toml");

const FORBIDDEN_DIRECT_DEPENDENCIES: &[&str] = &[
    "arrow",
    "parquet",
    "datafusion",
    "object_store",
    "roaring",
    "async-trait",
    "tracing",
    "exasol-udf-macros",
    "lakehouse-engine",
];

/// Collects the dependency names declared under any `[*dependencies]` table
/// in a Cargo manifest, ignoring comments and every other section (so a
/// dependency name mentioned only in prose, e.g. a comment citing another
/// crate's path, is not mistaken for a declared dependency).
fn declared_dependency_names(manifest: &str) -> Vec<&str> {
    let mut current_section = "";
    let mut names = Vec::new();

    for raw_line in manifest.lines() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            current_section = line;
            continue;
        }
        if current_section.contains("dependencies")
            && let Some((name, _value)) = line.split_once('=')
        {
            names.push(name.trim());
        }
    }

    names
}

#[test]
fn catalog_manifest_declares_no_execution_engine_dependency() {
    let declared = declared_dependency_names(CATALOG_MANIFEST);

    for forbidden in FORBIDDEN_DIRECT_DEPENDENCIES {
        assert!(
            !declared.contains(forbidden),
            "catalog crate manifest must not declare a direct dependency on `{forbidden}`: \
             lakehouse-catalog must stay free of execution-engine, UDF-macro/tracing, and \
             engine-crate dependencies so lakehouse-engine depends on lakehouse-catalog one way only"
        );
    }
}
