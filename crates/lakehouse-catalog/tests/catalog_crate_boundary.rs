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
    "delta_kernel",
    "delta_kernel_default_engine",
];

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

/// Scenario: The catalog access layer lives in a standalone crate the engine depends on one way
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
