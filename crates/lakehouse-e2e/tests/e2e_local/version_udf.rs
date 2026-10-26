use crate::common::e2e_harness::*;
use crate::fixtures::stack;

const ENGINE_MANIFEST: &str = include_str!("../../../lakehouse-engine/Cargo.toml");

/// The `[package]` version the `.so` was built from; this crate's own version is unrelated.
fn engine_crate_version() -> &'static str {
    ENGINE_MANIFEST
        .lines()
        .find_map(|line| line.strip_prefix("version = \""))
        .and_then(|rest| rest.strip_suffix('"'))
        .expect("crates/lakehouse-engine/Cargo.toml must declare `version = \"...\"`")
}

#[test]
fn version_udf_returns_the_compiled_crate_version() {
    stack();
    let mut conn = exa_conn();

    let resp = conn.execute(&format!(
        "SELECT {SCHEMA_NAME}.{VERSION_SCRIPT_NAME}() AS REPORTED_VERSION"
    ));
    let value = resp["responseData"]["results"][0]["resultSet"]["data"][0][0]
        .as_str()
        .unwrap_or_else(|| panic!("expected a VARCHAR scalar from LAKEHOUSE_VERSION(), got {resp}"))
        .to_string();

    assert_eq!(
        value,
        engine_crate_version(),
        "LAKEHOUSE_VERSION() must report the compiled crate version"
    );
}
